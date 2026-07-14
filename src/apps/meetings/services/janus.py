"""Janus integration helpers built around the shared ``janus_api`` session lifecycle."""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import hashlib
import inspect
import json
import threading
import time
from typing import Any, Iterable, Literal, Mapping, Union

from asgiref.sync import async_to_sync
from django.conf import settings
from django.utils import timezone
from janus_api import Janus
from janus_api.conf._config import settings as janus_settings
from janus_api.models.request import BaseJanusRequest
from janus_api.models.response import JanusBaseResponse, JanusResponse, Jsep, PluginData
from janus_api.session.base import AbstractBaseSession
from janus_api.servers._proxy import JanusSessionManager
from janus_api.transport.websocket import WebsocketTransportClient
from pydantic import TypeAdapter

from apps.meetings.exceptions import JanusGatewayError

_janus_bootstrap_lock = threading.Lock()
_janus_event_loop_lock = threading.Lock()
_janus_event_loop: asyncio.AbstractEventLoop | None = None
_janus_event_loop_thread_id: int | None = None
_worker_local_manager: JanusSessionManager | None = None
_worker_janus_thread: threading.Thread | None = None
_worker_janus_ready = threading.Event()
_worker_janus_start_error: BaseException | None = None
CONTROL_SESSION_STATE_KEY = "_control_janus_session_id"
JANUS_NUMERIC_PROTOCOL_FIELDS = {
    "feed",
    "handle_id",
    "id",
    "private_id",
    "publisher_id",
    "room",
    "session_id",
    "user_id",
}


class _PluginSuccessResponse(JanusBaseResponse):
    """Represent synchronous plugin replies emitted as ``janus: success``."""

    janus: Literal["success"]
    sender: Union[str, int]
    plugindata: PluginData
    jsep: Jsep | None = None


def _install_numeric_janus_id_compatibility() -> None:
    """Keep Janus protocol ids numeric despite the SDK plugin's string-facing id property.

    The installed SDK deliberately exposes ``Plugin.id`` as ``str`` and then
    feeds that value into ``PluginMessageRequest.handle_id``. Janus requires
    session/handle ids to be JSON numbers, so the attach succeeds but every
    subsequent message otherwise fails with ``Handle not found``.
    """

    current_send = AbstractBaseSession.send
    if getattr(current_send, "_synq_numeric_ids", False):
        return

    async def send_with_numeric_ids(self, data):
        for attribute in ("session_id", "handle_id"):
            value = getattr(data, attribute, None)
            if isinstance(value, str) and value.isdecimal():
                setattr(data, attribute, int(value))
        return await current_send(self, data)

    send_with_numeric_ids._synq_numeric_ids = True  # type: ignore[attr-defined]
    AbstractBaseSession.send = send_with_numeric_ids

    current_dump_json = BaseJanusRequest.model_dump_json
    if not getattr(current_dump_json, "_synq_exclude_none", False):
        def model_dump_json_without_nulls(self, *args, **kwargs):
            # Janus rejects an explicit top-level ``jsep: null`` as an invalid
            # JSEP object. Omitted optional fields have the intended meaning.
            kwargs.setdefault("exclude_none", True)
            serialized = current_dump_json(self, *args, **kwargs)
            payload = json.loads(serialized)

            def normalize_protocol_ids(value, field_name: str | None = None):
                if isinstance(value, dict):
                    return {
                        key: normalize_protocol_ids(item, key)
                        for key, item in value.items()
                    }
                if isinstance(value, list):
                    return [normalize_protocol_ids(item) for item in value]
                if (
                    field_name in JANUS_NUMERIC_PROTOCOL_FIELDS
                    and isinstance(value, str)
                    and value.isdecimal()
                ):
                    return int(value)
                return value

            return json.dumps(
                normalize_protocol_ids(payload),
                separators=(",", ":"),
            )

        model_dump_json_without_nulls._synq_exclude_none = True  # type: ignore[attr-defined]
        BaseJanusRequest.model_dump_json = model_dump_json_without_nulls

    current_transport_init = WebsocketTransportClient.__init__
    if not getattr(current_transport_init, "_synq_plugin_success", False):
        def init_with_plugin_success(self, *args, **kwargs):
            current_transport_init(self, *args, **kwargs)
            # The SDK's fallback response model drops ``plugindata`` when Janus
            # returns a synchronous plugin result as ``janus: success``.
            self._response_adapter = TypeAdapter(
                Union[_PluginSuccessResponse, JanusResponse],
            )

        init_with_plugin_success._synq_plugin_success = True  # type: ignore[attr-defined]
        WebsocketTransportClient.__init__ = init_with_plugin_success


_install_numeric_janus_id_compatibility()


def _session_start_timeout() -> float:
    """Return the maximum time to wait for a usable Janus session/proxy."""

    return float(getattr(settings, "JANUS_SESSION_START_TIMEOUT_SECONDS", 15))


def _operation_timeout() -> float:
    """Return the maximum time to wait for one Janus SDK operation."""

    return float(getattr(settings, "JANUS_OPERATION_TIMEOUT_SECONDS", 30))


def register_janus_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Record the loop that owns the process-local Janus transport objects."""

    global _janus_event_loop, _janus_event_loop_thread_id

    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    owner_thread_id = threading.get_ident()
    if loop.is_running() and current_loop is not loop:
        # Registration is normally performed by ASGI/Celery from inside the
        # owner loop. Supporting an external registrar makes the boundary
        # deterministic in tests and in alternative process bootstraps.
        registered = threading.Event()

        def capture_owner_thread() -> None:
            nonlocal owner_thread_id
            owner_thread_id = threading.get_ident()
            registered.set()

        loop.call_soon_threadsafe(capture_owner_thread)
        if not registered.wait(2):
            raise JanusGatewayError("Unable to identify the Janus event-loop thread.")

    with _janus_event_loop_lock:
        _janus_event_loop = loop
        _janus_event_loop_thread_id = owner_thread_id


def unregister_janus_event_loop(
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """Forget an owning loop when its ASGI lifespan or worker is stopping."""

    global _janus_event_loop, _janus_event_loop_thread_id
    with _janus_event_loop_lock:
        if loop is not None and _janus_event_loop is not loop:
            return
        _janus_event_loop = None
        _janus_event_loop_thread_id = None


def _get_janus_event_loop() -> tuple[asyncio.AbstractEventLoop | None, int | None]:
    with _janus_event_loop_lock:
        return _janus_event_loop, _janus_event_loop_thread_id


def _wait_for_janus_session(timeout: float | None = None):
    """Wait for a leader session or follower proxy without blocking its loop."""

    loop, owner_thread_id = _get_janus_event_loop()
    if loop is not None and owner_thread_id == threading.get_ident():
        raise JanusGatewayError(
            "A synchronous meeting service attempted to block the Janus event loop."
        )

    deadline = time.monotonic() + (timeout or _session_start_timeout())
    while time.monotonic() < deadline:
        session = Janus.get_session()
        if session is not None:
            return session
        time.sleep(0.05)
    raise JanusGatewayError("The Janus session manager did not become ready in time.")


def _run_worker_janus_loop() -> None:
    """Own a persistent async loop for Celery's process-local Janus manager."""

    global _worker_janus_start_error, _worker_local_manager

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    register_janus_event_loop(loop)

    # The dependency's follower proxy cannot proxy plugin instances: attributes
    # such as ``session.id``/``session.plugins`` become RPC callables. A worker
    # needs a genuine local Janus session for room-level tasks.
    janus_settings.LEADER_MODE = False
    janus_settings.JANUS_ENABLE_ADMIN = False
    janus_settings.JANUS_ENABLE_EVENTS = False

    async def bootstrap() -> None:
        global _worker_janus_start_error, _worker_local_manager
        try:
            manager = JanusSessionManager()
            _worker_local_manager = manager
            Janus.set_manager(manager)
            await manager.start()

            deadline = loop.time() + _session_start_timeout()
            while manager.get_session() is None and loop.time() < deadline:
                await asyncio.sleep(0.05)
            if manager.get_session() is None:
                raise TimeoutError(
                    "The Celery Janus manager did not elect a leader or create a proxy in time."
                )
        except BaseException as exc:  # surfaced to the synchronous task caller
            _worker_janus_start_error = exc
        finally:
            _worker_janus_ready.set()

    loop.create_task(bootstrap())
    try:
        loop.run_forever()
    finally:
        unregister_janus_event_loop(loop)
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()


def _ensure_worker_janus_loop() -> None:
    """Start the persistent Celery Janus loop once and wait for bootstrap."""

    global _worker_janus_thread, _worker_janus_start_error

    with _janus_bootstrap_lock:
        if _worker_janus_thread is None or not _worker_janus_thread.is_alive():
            _worker_janus_ready.clear()
            _worker_janus_start_error = None
            _worker_janus_thread = threading.Thread(
                target=_run_worker_janus_loop,
                name="synq-janus-worker-loop",
                daemon=True,
            )
            _worker_janus_thread.start()

    if not _worker_janus_ready.wait(_session_start_timeout() + 1):
        raise JanusGatewayError("Timed out while starting the Celery Janus event loop.")
    if Janus.get_session() is None:
        raise JanusGatewayError("Unable to initialize the Celery Janus session manager.") from _worker_janus_start_error


def _shutdown_worker_janus_loop() -> None:
    """Best-effort cleanup for a Celery worker's persistent Janus loop."""

    loop, _ = _get_janus_event_loop()
    thread = _worker_janus_thread
    manager = _worker_local_manager
    if thread is None or loop is None or not loop.is_running():
        return
    try:
        if manager is not None:
            future = asyncio.run_coroutine_threadsafe(manager.stop(), loop)
            future.result(timeout=min(_operation_timeout(), 5))
    except Exception:
        pass
    finally:
        loop.call_soon_threadsafe(loop.stop)
        if thread is not threading.current_thread():
            thread.join(timeout=2)


atexit.register(_shutdown_worker_janus_loop)


def serialize_janus_response(response: Any) -> dict[str, Any]:
    """Convert Janus SDK responses into JSON-serializable dictionaries."""

    if hasattr(response, "model_dump"):
        return response.model_dump(mode="json")
    if hasattr(response, "__dict__"):
        return dict(response.__dict__)
    return {"value": str(response)}


def resolve_maybe_awaitable(result: Any) -> Any:
    """Resolve an SDK awaitable on the loop that owns its Janus transport."""

    if not inspect.isawaitable(result):
        return result

    async def _await_result() -> Any:
        return await result

    loop, owner_thread_id = _get_janus_event_loop()
    if loop is not None and loop.is_running():
        if owner_thread_id == threading.get_ident():
            if inspect.iscoroutine(result):
                result.close()
            raise JanusGatewayError(
                "A synchronous meeting service cannot wait on the Janus owner loop."
            )
        future = asyncio.run_coroutine_threadsafe(_await_result(), loop)
        try:
            return future.result(timeout=_operation_timeout())
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise JanusGatewayError("The Janus operation timed out.") from exc

    return async_to_sync(_await_result)()


def resolve_janus_session(*_args: Any, **_kwargs: Any):
    """Return the current process-local Janus session, bootstrapping worker processes when needed.

    ``janus_api.create_asgi_app`` already provisions the shared session during ASGI startup.
    Celery workers run in separate processes, so they lazily bootstrap their own local
    ``JanusSessionManager`` only when no session is already available.
    """

    if not getattr(settings, "JANUS_ENABLED", True):
        raise JanusGatewayError("Janus integration is disabled for this process.")

    session = Janus.get_session()
    if session is not None:
        return session

    # ASGI registers its owner loop and manager during lifespan startup. If a
    # request arrives during leader election, wait for that manager rather than
    # replacing it with a second process-local manager.
    if Janus.get_manager() is not None:
        return _wait_for_janus_session()

    # Celery has no ASGI lifespan. Give it a durable loop so leader election,
    # SessionProxy's aiohttp client, and all plugin calls stay on one loop.
    _ensure_worker_janus_loop()
    return _wait_for_janus_session(timeout=0.1)


def build_room_payload(session) -> dict[str, Any]:
    """Build the Janus VideoRoom creation payload for a meeting session."""

    room_defaults = dict(getattr(settings, "JANUS_DEFAULT_ROOM_CONFIGURATION", {}))
    room_defaults.update(session.room.janus_room_configuration or {})
    room_defaults.setdefault("room", resolve_janus_room_id(session))
    room_defaults.setdefault("description", session.room.title)
    room_defaults["publishers"] = session.room.max_participants
    room_defaults.setdefault("bitrate", 1_024_000)
    room_defaults.setdefault("audiocodec", "opus")
    room_defaults.setdefault("videocodec", "vp8")
    room_defaults.setdefault("notify_joining", True)
    if session.janus_room_secret:
        room_defaults.setdefault("secret", session.janus_room_secret)
    if session.janus_room_pin:
        room_defaults.setdefault("pin", session.janus_room_pin)
    return room_defaults


def resolve_janus_room_id(session) -> str:
    """Return a stable positive numeric room id accepted by the local Janus gateway."""

    if session.janus_room_id:
        return str(session.janus_room_id)
    digest = hashlib.sha256(str(session.pk).encode("utf-8")).digest()
    # Stay below JavaScript's maximum safe integer because this id crosses the
    # browser/backend boundary even though Janus itself supports uint64 values.
    return str(1 + int.from_bytes(digest[:7], "big") % 9_007_199_254_740_990)


def meeting_session_control_plugin_kwargs(instance, field, raw_id: str | None) -> Mapping[str, Any]:
    """Return constructor kwargs for a session-scoped Janus control plugin."""

    del field, raw_id
    return {
        "room": resolve_janus_room_id(instance),
        "username": f"system-session-{instance.pk}",
    }


def participant_media_handle_identifier(instance, field) -> str:
    """Resolve the Janus plugin identifier from a participant media handle row."""

    del field
    return str(instance.handle_type)


def participant_media_plugin_kwargs(instance, field, raw_id: str | None) -> Mapping[str, Any]:
    """Return constructor kwargs for participant publisher, subscriber, or textroom handles."""

    del field, raw_id
    participant = instance.participant
    return {
        "room": resolve_janus_room_id(participant.session),
        "username": participant.display_name,
    }


def ensure_bound_plugin_attached(bound_handle, *, persist: bool = False, update_fields: Iterable[str] | None = None):
    """Attach a bound Janus plugin handle when needed and return the bound handle."""

    if bound_handle.is_attached:
        return bound_handle

    # Attaching is async, but Django model persistence must happen back on the
    # synchronous caller thread rather than inside the Janus owner loop.
    result = bound_handle.attach(persist=False)
    resolve_maybe_awaitable(result)
    if persist:
        bound_handle.sync_from_plugin(
            persist=True,
            update_fields=list(update_fields or []),
        )
    return bound_handle


def ensure_session_control_handle(session):
    """Attach the reusable session control handle and return the bound plugin wrapper."""

    janus_session = resolve_janus_session()
    janus_session_id = str(getattr(janus_session, "id", "") or "")
    recorded_session_id = str(
        (session.janus_state or {}).get(CONTROL_SESSION_STATE_KEY) or ""
    )
    if session.control_handle_id and recorded_session_id != janus_session_id:
        session.control_handle_id = None
        session.janus_state = {
            key: value
            for key, value in (session.janus_state or {}).items()
            if key != CONTROL_SESSION_STATE_KEY
        }
        session.save(update_fields=["control_handle_id", "janus_state", "updated_at"])

    bound_handle = ensure_bound_plugin_attached(
        session.control_handle,
        persist=True,
        update_fields=["control_handle_id", "updated_at"],
    )
    if recorded_session_id != janus_session_id:
        session.janus_state = {
            **(session.janus_state or {}),
            CONTROL_SESSION_STATE_KEY: janus_session_id,
        }
        session.save(update_fields=["janus_state", "updated_at"])
    return bound_handle


def ensure_participant_media_plugin(media_handle):
    """Attach the Janus plugin for a participant media handle row and persist attachment metadata."""

    session = resolve_janus_session()
    janus_session_id = str(getattr(session, "id", "") or "")
    if (
        media_handle.janus_handle_id
        and (
            str(media_handle.janus_session_id or "") != janus_session_id
            or media_handle.lifecycle_state == "failed"
        )
    ):
        media_handle.janus_handle_id = None
        media_handle.janus_session_id = ""
        media_handle.lifecycle_state = "attaching"
        media_handle.save(
            update_fields=[
                "janus_handle_id",
                "janus_session_id",
                "lifecycle_state",
                "updated_at",
            ],
        )
    bound_handle = ensure_bound_plugin_attached(
        media_handle.handle,
        persist=True,
        update_fields=["janus_handle_id", "updated_at"],
    )
    media_handle.janus_session_id = janus_session_id
    if media_handle.lifecycle_state in {"attaching", "detached", "failed"}:
        media_handle.lifecycle_state = "attached"
    media_handle.last_event_at = timezone.now()
    media_handle.save(
        update_fields=[
            "janus_session_id",
            "lifecycle_state",
            "last_event_at",
            "updated_at",
        ],
    )
    return bound_handle


def call_plugin_method(bound_handle, method_name: str, *args: Any, **kwargs: Any) -> Any:
    """Invoke a Janus plugin method and transparently resolve coroutine results."""

    method = getattr(bound_handle, method_name)
    try:
        response = resolve_maybe_awaitable(method(*args, **kwargs))
        print(response)
        plugin_data = getattr(getattr(response, "plugindata", None), "data", None)
        if plugin_data is not None:
            if hasattr(plugin_data, "model_dump"):
                payload = plugin_data.model_dump(mode="json")
            elif isinstance(plugin_data, Mapping):
                payload = dict(plugin_data)
            else:
                payload = vars(plugin_data) if hasattr(plugin_data, "__dict__") else {}
            if payload.get("error_code") or payload.get("error"):
                error_code = payload.get("error_code")
                error_prefix = f" {error_code}" if error_code else ""
                raise JanusGatewayError(
                    f"Janus plugin error{error_prefix}: "
                    f"{payload.get('error', 'unknown error')}",
                )
        return response
    except JanusGatewayError:
        raise
    except Exception as exc:
        print(exc)
        raise JanusGatewayError(
            f"Unable to execute Janus plugin method '{method_name}'.",
        ) from exc
