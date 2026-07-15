"""Janus Core runtime and VideoRoom integration helpers.

Janus Core 3 deliberately keeps sessions, transports, and plugin handles
process-local and async-only.  Django and Celery call this module from
synchronous code, so one long-lived event loop owns every Janus object in a
process and synchronous callers submit work to that loop.
"""

from __future__ import annotations

import asyncio
import atexit
import inspect
import logging
import os
import threading
from collections.abc import AsyncIterator, Awaitable
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import asynccontextmanager
from typing import Any, Iterable, Mapping

from django.conf import settings
from django.utils import timezone
from janus_api.conf import Janus, configure as configure_janus_settings
from janus_api.servers import JanusSessionManager
from janus_videoroom_plugin import VideoRoomPlugin, VideoRoomReply

from apps.meetings.exceptions import JanusGatewayError

_configuration_lock = threading.Lock()
_configuration_ready = False
logger = logging.getLogger(__name__)


def configure_janus_core() -> None:
    """Bridge the project's Django settings into Janus Core's typed settings.

    Janus Core does not read Django settings and does not load ``.env`` files.
    Keeping this bridge explicit makes ASGI and Celery use the same endpoint,
    credentials, pool sizing, and lifecycle timeouts.
    """

    global _configuration_ready

    if _configuration_ready:
        return

    with _configuration_lock:
        if _configuration_ready:
            return

        configure_janus_settings(
            overrides={
                "JANUS_SESSION_URL": settings.JANUS_SESSION_URL,
                "JANUS_REQUEST_TIMEOUT": settings.JANUS_REQUEST_TIMEOUT,
                "JANUS_SESSION_POOL_SIZE": settings.JANUS_SESSION_POOL_SIZE,
                "JANUS_STARTUP_FAIL_FAST": settings.JANUS_STARTUP_FAIL_FAST,
                "JANUS_KEEPALIVE_INTERVAL": settings.JANUS_KEEPALIVE_INTERVAL,
                "JANUS_KEEPALIVE_FAILURES": settings.JANUS_KEEPALIVE_FAILURES,
                "JANUS_SHUTDOWN_TIMEOUT": settings.JANUS_SHUTDOWN_TIMEOUT,
                "JANUS_DETACH_CONCURRENCY": settings.JANUS_DETACH_CONCURRENCY,
                "JANUS_TOKEN": settings.JANUS_TOKEN,
                "JANUS_API_SECRET": settings.JANUS_API_SECRET,
            },
        )
        _configuration_ready = True


class JanusProcessRuntime:
    """Own or bridge the one persistent Janus event loop for this process."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"

    def __init__(self) -> None:
        self._pid = os.getpid()
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._stop_requested = threading.Event()
        self._state = self.STOPPED
        self._loop: asyncio.AbstractEventLoop | None = None
        self._manager: JanusSessionManager | None = None
        self._manager_lease: object | None = None
        self._owner_thread_id: int | None = None
        self._thread: threading.Thread | None = None
        self._startup_error: BaseException | None = None

    @property
    def manager(self) -> JanusSessionManager | None:
        """Return the active process-local manager, when one has started."""

        with self._lock:
            return self._manager

    @property
    def state(self) -> str:
        """Return the process runtime lifecycle state."""

        with self._lock:
            return self._state

    def reset_after_fork(self) -> None:
        """Discard process-local loops and leases inherited across ``fork``.

        Threads do not survive a POSIX fork, and Janus transports cannot be
        reused in the child.  This method is idempotent and is also called at
        every public acquisition boundary, so prefork workers are safe even
        when their host does not expose a child-initialization hook.
        """

        current_pid = os.getpid()
        if current_pid == self._pid:
            return

        # Replace synchronization primitives as well: a lock copied while a
        # vanished parent thread owned it could otherwise deadlock the child.
        self._pid = current_pid
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._stop_requested = threading.Event()
        self._state = self.STOPPED
        self._loop = None
        self._manager = None
        self._manager_lease = None
        self._owner_thread_id = None
        self._thread = None
        self._startup_error = None
        Janus.set_manager(None)

    def _claim_start(self, *, thread: threading.Thread | None) -> None:
        """Reserve runtime ownership before opening any network resources."""

        self.reset_after_fork()
        with self._lock:
            if self._state not in {self.STOPPED, self.FAILED}:
                raise RuntimeError(
                    f"Janus runtime cannot start while state={self._state}."
                )
            self._state = self.STARTING
            self._startup_error = None
            self._ready.clear()
            self._stop_requested.clear()
            self._thread = thread

    def _bind(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        manager: JanusSessionManager,
        lease: object,
        owner_thread_id: int,
        thread: threading.Thread | None,
    ) -> None:
        with self._lock:
            if self._state != self.STARTING:
                raise RuntimeError(
                    f"Janus runtime cannot bind while state={self._state}."
                )
            if self._manager is not None and self._manager is not manager:
                raise RuntimeError("a Janus runtime is already active in this process")
            self._loop = loop
            self._manager = manager
            self._manager_lease = lease
            self._owner_thread_id = owner_thread_id
            self._thread = thread
            self._startup_error = None
            self._state = self.RUNNING
            self._ready.set()

    def _begin_stop(self, manager: JanusSessionManager) -> None:
        """Reject new acquisitions while an owned manager drains."""

        with self._lock:
            if self._manager is not manager:
                return
            self._state = self.STOPPING

    def _finish_stop(
        self,
        manager: JanusSessionManager | None,
        *,
        error: BaseException | None = None,
    ) -> None:
        """Release process ownership only after manager shutdown completes."""

        with self._lock:
            if (
                manager is not None
                and self._manager is not None
                and self._manager is not manager
            ):
                return
            self._loop = None
            self._manager = None
            self._manager_lease = None
            self._owner_thread_id = None
            self._thread = None
            self._startup_error = error
            self._state = self.FAILED if error is not None else self.STOPPED
            if error is not None:
                self._ready.set()
            else:
                self._ready.clear()

    @asynccontextmanager
    async def lifespan(self, _app: Any) -> AsyncIterator[dict[str, Any]]:
        """Own Janus sessions on the ASGI server's long-lived event loop."""

        self._claim_start(thread=None)
        loop = asyncio.get_running_loop()
        manager: JanusSessionManager | None = None
        lease: object | None = None
        bound = False
        failure: BaseException | None = None
        try:
            configure_janus_core()
            from janus_api.conf import settings as janus_settings

            manager = JanusSessionManager(
                fail_fast=janus_settings.JANUS_STARTUP_FAIL_FAST,
            )
            await manager.start()
            lease = Janus.install_manager(manager)
            self._bind(
                loop=loop,
                manager=manager,
                lease=lease,
                owner_thread_id=threading.get_ident(),
                thread=None,
            )
            bound = True
            yield {"janus": Janus, "session_manager": manager}
        except BaseException as exc:
            failure = exc
            raise
        finally:
            if bound and manager is not None:
                self._begin_stop(manager)
            try:
                if manager is not None:
                    await manager.stop()
            finally:
                if lease is not None:
                    Janus.remove_manager(lease)
                self._finish_stop(manager if bound else None, error=failure)

    def _background_main(self) -> None:
        """Run a Celery/management-command Janus manager until process exit."""

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        manager: JanusSessionManager | None = None
        lease: object | None = None
        bound = False
        failure: BaseException | None = None
        try:
            configure_janus_core()
            from janus_api.conf import settings as janus_settings

            manager = JanusSessionManager(fail_fast=janus_settings.JANUS_STARTUP_FAIL_FAST)
            loop.run_until_complete(manager.start())
            if self._stop_requested.is_set():
                return
            lease = Janus.install_manager(manager)
            self._bind(
                loop=loop,
                manager=manager,
                lease=lease,
                owner_thread_id=threading.get_ident(),
                thread=threading.current_thread(),
            )
            bound = True
            loop.run_forever()
        except BaseException as exc:
            failure = exc
        finally:
            if manager is not None and bound:
                self._begin_stop(manager)
            if manager is not None:
                try:
                    loop.run_until_complete(manager.stop())
                except Exception:
                    logger.exception("Could not completely stop the Janus background manager")
            if lease is not None:
                Janus.remove_manager(lease)
            self._finish_stop(manager if bound else None, error=failure)
            asyncio.set_event_loop(None)
            loop.close()

    def ensure_background(self) -> None:
        """Start the persistent fallback loop used outside an ASGI lifespan."""

        self.reset_after_fork()
        thread_to_start: threading.Thread | None = None
        with self._lock:
            if self._state == self.RUNNING:
                return
            if self._state == self.STOPPING:
                raise JanusGatewayError("The process-local Janus runtime is stopping.")

            thread = self._thread
            if self._state in {self.STOPPED, self.FAILED} and (
                thread is None or not thread.is_alive()
            ):
                thread_to_start = threading.Thread(
                    target=self._background_main,
                    name="janus-process-runtime",
                    daemon=True,
                )
                self._state = self.STARTING
                self._ready.clear()
                self._stop_requested.clear()
                self._startup_error = None
                self._thread = thread_to_start

        if thread_to_start is not None:
            try:
                thread_to_start.start()
            except BaseException as exc:
                self._finish_stop(None, error=exc)
                raise JanusGatewayError(
                    "Unable to start the process-local Janus runtime thread."
                ) from exc

        startup_timeout = float(settings.JANUS_RUNTIME_STARTUP_TIMEOUT)
        if not self._ready.wait(timeout=startup_timeout):
            raise JanusGatewayError(
                f"Janus runtime did not start within {startup_timeout:g} seconds."
            )
        with self._lock:
            startup_error = self._startup_error
            state = self._state
        if startup_error is not None:
            raise JanusGatewayError("Unable to start the process-local Janus runtime.") from startup_error
        if state != self.RUNNING:
            raise JanusGatewayError(
                f"The process-local Janus runtime did not become ready (state={state})."
            )

    def stop_background(self) -> None:
        """Stop a fallback loop owned by this module; ASGI owns its own shutdown."""

        self.reset_after_fork()
        with self._lock:
            loop = self._loop
            thread = self._thread
            if self._state in {self.STARTING, self.RUNNING} and thread is not None:
                self._stop_requested.set()
            if self._state == self.RUNNING and loop is not None and thread is not None:
                self._state = self.STOPPING
        if thread is None or thread is threading.current_thread():
            return
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=float(settings.JANUS_SHUTDOWN_TIMEOUT) + 2.0)
        if thread.is_alive():
            logger.warning("Janus background manager did not stop before the shutdown deadline")

    def session(self, *, key: str | int | None = None):
        """Return a ready session pinned by ``key`` for pool stability."""

        self.reset_after_fork()
        with self._lock:
            manager = self._manager
            state = self._state
        if state == self.STOPPING:
            raise JanusGatewayError("The process-local Janus runtime is stopping.")
        if manager is None or state != self.RUNNING:
            self.ensure_background()
            with self._lock:
                manager = self._manager
                state = self._state
        if state != self.RUNNING:
            raise JanusGatewayError(
                f"The process-local Janus runtime is unavailable (state={state})."
            )
        session = None if manager is None else manager.get_session(key=key)
        if session is None:
            raise JanusGatewayError("No ready Janus session is available in this process.")
        return session

    def run(self, awaitable: Awaitable[Any]) -> Any:
        """Resolve an awaitable on the loop that owns the Janus transport."""

        self.reset_after_fork()
        with self._lock:
            manager = self._manager
            state = self._state
        if state == self.STOPPING:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise JanusGatewayError("The process-local Janus runtime is stopping.")
        if manager is None or state != self.RUNNING:
            try:
                self.ensure_background()
            except BaseException:
                if inspect.iscoroutine(awaitable):
                    awaitable.close()
                raise

        with self._lock:
            loop = self._loop
            owner_thread_id = self._owner_thread_id
        if loop is None or not loop.is_running():
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise JanusGatewayError("The process-local Janus event loop is not running.")
        if owner_thread_id == threading.get_ident():
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise JanusGatewayError(
                "A synchronous Janus call cannot block its owner event loop; await it from async code instead."
            )

        async def _resolve() -> Any:
            return await awaitable

        future = asyncio.run_coroutine_threadsafe(_resolve(), loop)
        call_timeout = float(settings.JANUS_SYNC_CALL_TIMEOUT)
        try:
            return future.result(timeout=call_timeout)
        except FutureTimeoutError as exc:
            future.cancel()
            raise JanusGatewayError(
                f"Janus operation exceeded the {call_timeout:g}-second synchronous bridge timeout."
            ) from exc


janus_runtime = JanusProcessRuntime()
atexit.register(janus_runtime.stop_background)


def _session_key(instance: Any | None) -> str | None:
    """Derive a stable pool key from a meeting-domain object."""

    if instance is None:
        return None

    participant = getattr(instance, "participant", None)
    if participant is not None:
        value = getattr(participant, "session_id", None)
        if value is not None:
            return str(value)

    value = getattr(instance, "session_id", None)
    if value is not None:
        return str(value)

    related_session = getattr(instance, "session", None)
    if related_session is not None:
        value = getattr(related_session, "pk", None)
        if value is not None:
            return str(value)

    value = getattr(instance, "pk", None)
    return None if value is None else str(value)


def serialize_janus_response(response: Any) -> dict[str, Any]:
    """Convert core and typed VideoRoom responses into stable JSON envelopes."""

    if isinstance(response, VideoRoomReply):
        raw = response.raw
        if hasattr(raw, "model_dump"):
            return raw.model_dump(mode="json", by_alias=True, exclude_none=True)
        if isinstance(raw, Mapping):
            return dict(raw)

        data = response.data.model_dump(mode="json", by_alias=True, exclude_none=True)
        payload: dict[str, Any] = {
            "plugindata": {
                "plugin": "janus.plugin.videoroom",
                "data": data,
            }
        }
        if response.jsep is not None:
            payload["jsep"] = response.jsep.model_dump(mode="json", by_alias=True, exclude_none=True)
        if response.transaction:
            payload["transaction"] = response.transaction
        return payload

    if hasattr(response, "model_dump"):
        return response.model_dump(mode="json", by_alias=True, exclude_none=True)
    if isinstance(response, Mapping):
        return dict(response)
    if hasattr(response, "__dict__"):
        return {
            key: value
            for key, value in vars(response).items()
            if not key.startswith("_")
        }
    return {"value": str(response)}


def video_room_reply_data(response: Any) -> Any | None:
    """Return typed VideoRoom data while tolerating legacy Janus envelopes."""

    if isinstance(response, VideoRoomReply):
        return response.data
    return getattr(getattr(response, "plugindata", None), "data", None)


def resolve_maybe_awaitable(result: Any) -> Any:
    """Resolve Janus awaitables on their persistent process-owned loop."""

    if not inspect.isawaitable(result):
        return result
    return janus_runtime.run(result)


def resolve_janus_session(instance: Any | None = None, *_args: Any, **_kwargs: Any):
    """Return a ready process-local Janus session pinned to a meeting key."""

    return janus_runtime.session(key=_session_key(instance))


def resolve_owned_janus_session(instance: Any | None = None):
    """Return an already-running local session without creating a new owner.

    Cleanup callers use this to avoid opening a Celery-local session merely to
    compare it with a handle that was attached by an ASGI process.
    """

    janus_runtime.reset_after_fork()
    if janus_runtime.state != janus_runtime.RUNNING:
        return None
    manager = janus_runtime.manager
    return None if manager is None else manager.get_session(key=_session_key(instance))


def build_room_payload(session) -> dict[str, Any]:
    """Build the Janus VideoRoom creation payload for a meeting session."""

    room_defaults = dict(getattr(settings, "JANUS_DEFAULT_ROOM_CONFIGURATION", {}))
    room_defaults.update(session.room.janus_room_configuration or {})
    room_defaults.setdefault("room", session.janus_room_id or str(session.pk))
    room_defaults.setdefault("description", session.room.title)
    room_defaults.setdefault("publishers", session.room.max_participants)
    room_defaults.setdefault("bitrate", 1_024_000)
    room_defaults.setdefault("audiocodec", "opus")
    room_defaults.setdefault("videocodec", "vp8")
    room_defaults.setdefault("notify_joining", True)
    if session.janus_room_secret:
        room_defaults.setdefault("secret", session.janus_room_secret)
    if session.janus_room_pin:
        room_defaults.setdefault("pin", session.janus_room_pin)
    return room_defaults


def meeting_session_control_plugin_kwargs(instance, field, raw_id: str | None) -> Mapping[str, Any]:
    """Retain historical field construction compatibility for old migrations."""

    del instance, field, raw_id
    return {}


def participant_media_handle_identifier(instance, field) -> str:
    """Resolve both media roles to the one installed VideoRoom plugin."""

    del instance, field
    return "videoroom"


def participant_media_plugin_kwargs(instance, field, raw_id: str | None) -> Mapping[str, Any]:
    """Return constructor metadata accepted by the generic v3 plugin base."""

    del field, raw_id
    return {"meeting_handle_type": str(instance.handle_type)}


def ensure_bound_plugin_attached(
    bound_handle,
    *,
    persist: bool = False,
    update_fields: Iterable[str] | None = None,
    opaque_id: str | None = None,
):
    """Attach/register a bound plugin on its owning session and return it."""

    if bound_handle.is_attached:
        return bound_handle

    result = bound_handle.attach(
        persist=False,
        opaque_id=opaque_id,
    )
    resolve_maybe_awaitable(result)
    if persist:
        bound_handle.sync_from_plugin(
            persist=True,
            update_fields=list(update_fields or []),
        )
    return bound_handle


def ensure_session_control_handle(session):
    """Backward-compatible control-handle accessor.

    New room management code uses :func:`call_video_room_management_method`,
    whose short-lived handles cannot leak across processes.  This accessor is
    retained for callers outside the repository and never persists a v3 handle.
    """

    session.control_handle_id = None
    return ensure_bound_plugin_attached(session.control_handle, persist=False)


def ensure_participant_media_plugin(media_handle):
    """Attach a participant handle lazily on the current process/session."""

    session = resolve_janus_session(media_handle)
    session_id = str(session.id)
    stored_session_id = str(media_handle.janus_session_id or "")

    if stored_session_id != session_id and (
        media_handle.janus_handle_id or stored_session_id
    ):
        media_handle.janus_handle_id = None
        media_handle.janus_session_id = ""
        media_handle.lifecycle_state = "attaching"
        media_handle.selected_streams = []
        media_handle.janus_state = {}
        media_handle.save(
            update_fields=[
                "janus_handle_id",
                "janus_session_id",
                "lifecycle_state",
                "selected_streams",
                "janus_state",
                "updated_at",
            ],
        )
        media_handle.streams.all().delete()
        if str(media_handle.handle_type) == "publisher":
            participant = media_handle.participant
            participant.janus_publisher_id = ""
            participant.janus_private_id = ""
            participant.save(
                update_fields=[
                    "janus_publisher_id",
                    "janus_private_id",
                    "updated_at",
                ],
            )

    bound_handle = media_handle.handle
    was_attached = bound_handle.is_attached
    bound_handle = ensure_bound_plugin_attached(
        bound_handle,
        persist=False,
        opaque_id=media_handle.opaque_id or None,
    )
    media_handle.janus_session_id = session_id
    if not was_attached:
        media_handle.lifecycle_state = "attached"
    media_handle.last_event_at = timezone.now()
    try:
        media_handle.save(
            update_fields=[
                "janus_handle_id",
                "janus_session_id",
                "lifecycle_state",
                "last_event_at",
                "updated_at",
            ],
        )
    except BaseException:
        if not was_attached:
            try:
                resolve_maybe_awaitable(bound_handle.detach(persist=False))
            except Exception:
                logger.exception(
                    "Could not compensate a Janus handle after persistence failed"
                )
        raise
    if not was_attached:
        from apps.meetings.models import MeetingEventType
        from apps.meetings.services.lifecycle import record_session_event

        record_session_event(
            session=media_handle.participant.session,
            event_type=MeetingEventType.JANUS_HANDLE_ATTACHED,
            actor_profile=media_handle.participant.profile,
            actor_participant=media_handle.participant,
            payload={
                "handle_id": str(media_handle.pk),
                "handle_type": str(media_handle.handle_type),
                "janus_session_id": session_id,
            },
        )
    return bound_handle


def call_video_room_management_method(
    instance: Any,
    method_name: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run one VideoRoom management command on a short-lived plugin handle."""

    session = resolve_janus_session(instance)

    async def _invoke() -> Any:
        plugin = VideoRoomPlugin(session=session)
        await plugin.attach()
        try:
            method = getattr(plugin, method_name)
            result = await method(*args, **kwargs)
        except BaseException:
            try:
                await plugin.detach()
            except Exception:
                logger.exception(
                    "Could not detach a VideoRoom management handle after a failed command"
                )
            raise
        try:
            await plugin.detach()
        except Exception:
            # The command already succeeded.  Keep that result authoritative;
            # the owning session will retry local cleanup during shutdown.
            logger.exception(
                "VideoRoom management command succeeded but its temporary handle did not detach"
            )
        return result

    try:
        return janus_runtime.run(_invoke())
    except Exception as exc:
        raise JanusGatewayError(
            f"Unable to execute Janus VideoRoom management method '{method_name}'."
        ) from exc


def call_plugin_method(bound_handle, method_name: str, *args: Any, **kwargs: Any) -> Any:
    """Invoke a bound VideoRoom method on the session owner's event loop."""

    method = getattr(bound_handle, method_name)
    try:
        return resolve_maybe_awaitable(method(*args, **kwargs))
    except Exception as exc:
        raise JanusGatewayError(
            f"Unable to execute Janus plugin method '{method_name}'.",
        ) from exc
