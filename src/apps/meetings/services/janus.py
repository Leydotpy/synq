"""Janus integration helpers built around the shared ``janus_api`` session lifecycle."""

from __future__ import annotations

import inspect
import threading
from typing import Any, Iterable, Mapping

from asgiref.sync import async_to_sync
from django.conf import settings
from django.utils import timezone
from janus_api import Janus
from janus_api.servers._proxy import JanusSessionManager

from apps.meetings.exceptions import JanusGatewayError

_janus_bootstrap_lock = threading.Lock()
_worker_local_manager: JanusSessionManager | None = None


def serialize_janus_response(response: Any) -> dict[str, Any]:
    """Convert Janus SDK responses into JSON-serializable dictionaries."""

    if hasattr(response, "model_dump"):
        return response.model_dump(mode="json")
    if hasattr(response, "__dict__"):
        return dict(response.__dict__)
    return {"value": str(response)}


def resolve_maybe_awaitable(result: Any) -> Any:
    """Resolve a value that may already be concrete or may be awaitable."""

    if not inspect.isawaitable(result):
        return result

    async def _await_result() -> Any:
        return await result

    return async_to_sync(_await_result)()


def resolve_janus_session(*_args: Any, **_kwargs: Any):
    """Return the current process-local Janus session, bootstrapping worker processes when needed.

    ``janus_api.create_asgi_app`` already provisions the shared session during ASGI startup.
    Celery workers run in separate processes, so they lazily bootstrap their own local
    ``JanusSessionManager`` only when no session is already available.
    """

    session = Janus.get_session()
    if session is not None:
        return session

    global _worker_local_manager

    with _janus_bootstrap_lock:
        session = Janus.get_session()
        if session is not None:
            return session

        if _worker_local_manager is None:
            _worker_local_manager = JanusSessionManager()
            Janus.set_manager(_worker_local_manager)
            resolve_maybe_awaitable(_worker_local_manager.start())

    return Janus.get_session()


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
    """Return constructor kwargs for a session-scoped Janus control plugin."""

    del field, raw_id
    return {
        "room": instance.janus_room_id or str(instance.pk),
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
        "room": participant.session.janus_room_id or str(participant.session.pk),
        "username": participant.display_name,
    }


def ensure_bound_plugin_attached(bound_handle, *, persist: bool = False, update_fields: Iterable[str] | None = None):
    """Attach a bound Janus plugin handle when needed and return the bound handle."""

    if bound_handle.is_attached:
        return bound_handle

    result = bound_handle.attach(
        persist=persist,
        update_fields=list(update_fields or []),
    )
    resolve_maybe_awaitable(result)
    return bound_handle


def ensure_session_control_handle(session):
    """Attach the reusable session control handle and return the bound plugin wrapper."""

    resolve_janus_session()
    return ensure_bound_plugin_attached(
        session.control_handle,
        persist=True,
        update_fields=["control_handle_id", "updated_at"],
    )


def ensure_participant_media_plugin(media_handle):
    """Attach the Janus plugin for a participant media handle row and persist attachment metadata."""

    resolve_janus_session()
    bound_handle = ensure_bound_plugin_attached(
        media_handle.handle,
        persist=True,
        update_fields=["janus_handle_id", "updated_at"],
    )
    session = resolve_janus_session()
    media_handle.janus_session_id = str(getattr(session, "id", "") or "")
    media_handle.last_event_at = timezone.now()
    return bound_handle


def call_plugin_method(bound_handle, method_name: str, *args: Any, **kwargs: Any) -> Any:
    """Invoke a Janus plugin method and transparently resolve coroutine results."""

    method = getattr(bound_handle, method_name)
    try:
        return resolve_maybe_awaitable(method(*args, **kwargs))
    except Exception as exc:
        raise JanusGatewayError(
            f"Unable to execute Janus plugin method '{method_name}'.",
        ) from exc
