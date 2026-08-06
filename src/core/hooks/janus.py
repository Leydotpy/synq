"""Generic Janus callback routing helpers used by plugin-backed meeting models."""

from __future__ import annotations

import asyncio
from typing import Any, Mapping

from asgiref.sync import async_to_sync, sync_to_async
from django.db import transaction
from django.utils import timezone
from janus_api import JanusResponse
from janus_api.models.base import Jsep
from janus_api.models.response import WebRTCEvent

from apps.meetings.realtime.events import MeetingSocketEvents
from apps.meetings.realtime.emitter import MeetingSocketEmitter
from apps.meetings.services.signaling import MeetingMediaSignalService
from conf.socketio import get_socket_server
from core.models import JanusPluginField
from core.utils import log_to_terminal

JanusEvent = Mapping[str, Any]


def _normalize_event_payload(event: Any) -> dict[str, Any]:
    """Return a JSON-friendly representation of a Janus callback payload."""

    if isinstance(event, Jsep):
        model = event.model_dump(mode="json")
        model["type"] = f"janus.sdp.{event.type}"
        return model

    if isinstance(event, WebRTCEvent):
        model = event.model_dump(mode="json")
        model["type"] = f"janus.webrtc.{event.janus}"
        return model

    if isinstance(event, JanusResponse):
        model = event.model_dump(mode="json")
        model["type"] = "janus.response"
        return model

    if isinstance(event, Mapping):
        payload = dict(event)
        payload.setdefault("type", "janus.event")
        return payload

    if hasattr(event, "__dict__"):
        payload = {
            key: value
            for key, value in vars(event).items()
            if not key.startswith("_")
        }
        payload.setdefault("type", "janus.event")
        return payload

    return {"type": "janus.event", "raw": repr(event)}


def _collect_socket_ids(instance) -> list[str]:
    """Collect the socket ids most relevant to the current callback-bearing model instance."""

    socket_ids: list[str] = []

    for attribute_name in ("socket_id", "sid"):
        value = getattr(instance, attribute_name, None)
        if value:
            socket_ids.append(str(value))

    connection = getattr(instance, "connection", None)
    if connection is not None:
        socket_id = getattr(connection, "socket_id", None)
        if socket_id:
            socket_ids.append(str(socket_id))

    participant = getattr(instance, "participant", None)
    if participant is not None and hasattr(participant, "connections"):
        socket_ids.extend(
            str(socket_id)
            for socket_id in participant.connections.exclude(socket_id="").values_list("socket_id", flat=True)
        )

    return list(dict.fromkeys(socket_ids))


def _extract_context(instance) -> dict[str, Any]:
    """Extract common meeting-domain identifiers from the callback-bearing model instance."""

    payload: dict[str, Any] = {}

    for attribute_name in (
        "session_id",
        "room_id",
        "participant_id",
        "connection_id",
        "profile_id",
        "handle_type",
        "opaque_id",
    ):
        value = getattr(instance, attribute_name, None)
        if value not in (None, ""):
            payload[attribute_name] = str(value)

    session = getattr(instance, "session", None)
    if session is not None and "session_id" not in payload:
        payload["session_id"] = str(session.pk)

    room = getattr(instance, "room", None)
    if room is not None and "room_id" not in payload:
        payload["room_id"] = str(room.pk)

    participant = getattr(instance, "participant", None)
    if participant is not None and "participant_id" not in payload:
        payload["participant_id"] = str(participant.pk)
        payload.setdefault("session_id", str(participant.session_id))
        payload.setdefault("room_id", str(participant.room_id))
        payload.setdefault("profile_id", str(participant.profile_id))

    profile = getattr(instance, "profile", None)
    if profile is not None and "profile_id" not in payload:
        payload["profile_id"] = str(profile.pk)

    return payload


def _persist_latest_event_snapshot(instance, normalized_event: dict[str, Any]) -> None:
    """Persist the latest Janus callback payload on models that track it."""

    update_payload: dict[str, Any] = {}

    if hasattr(instance, "janus_state"):
        update_payload["janus_state"] = normalized_event
    if hasattr(instance, "last_event_at"):
        update_payload["last_event_at"] = timezone.now()
    if hasattr(instance, "updated_at"):
        update_payload["updated_at"] = timezone.now()

    if update_payload and getattr(instance, "pk", None):
        instance.__class__.objects.filter(pk=instance.pk).update(**update_payload)


def plugin_callback_factory(
    instance,
    field: JanusPluginField[Any],
    raw_id: str | None,
):
    """Build a callback that routes Janus events to the most relevant meeting sockets."""

    def _process_event(event: JanusEvent) -> None:
        normalized_event = _normalize_event_payload(event)
        _persist_latest_event_snapshot(instance, normalized_event)
        try:
            MeetingMediaSignalService.handle_callback_snapshot(instance, normalized_event)
        except Exception:
            # Best-effort lifecycle syncing should not block realtime fan-out.
            pass

        payload = {
            "model": instance._meta.label_lower,
            "pk": str(instance.pk) if getattr(instance, "pk", None) else None,
            "plugin_field": field.name,
            "plugin_attr": field.plugin_attr,
            "plugin_identifier": field.resolve_identifier(instance),
            "plugin_id": field.get_stored_value(instance) or raw_id,
            "event": normalized_event,
            "socket_ids": _collect_socket_ids(instance),
            **_extract_context(instance),
        }
        log_to_terminal("JANUS_EVENT", event)
        dispatch_janus_event(payload)

    def _on_rx_event(event: JanusEvent) -> None:
        """Keep synchronous ORM work off the Janus transport event loop."""

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            _process_event(event)
            return

        loop.create_task(sync_to_async(_process_event, thread_sensitive=True)(event))

    return _on_rx_event


def dispatch_janus_event(payload: dict[str, Any]) -> None:
    """Emit a normalized Janus event to targeted meeting sockets or a session-wide room."""

    socket_ids = [str(socket_id) for socket_id in payload.get("socket_ids", []) if socket_id]
    session_id = payload.get("session_id")

    def _emit() -> None:
        server = get_socket_server()

        for socket_id in socket_ids:
            async_to_sync(server.emit)(
                MeetingSocketEvents.JANUS_EVENT,
                payload,
                to=socket_id,
                namespace=MeetingSocketEmitter.namespace,
            )

        if not socket_ids and session_id:
            async_to_sync(server.emit)(
                MeetingSocketEvents.JANUS_EVENT,
                payload,
                room=MeetingSocketEmitter.session_room_name(session_id),
                namespace=MeetingSocketEmitter.namespace,
            )

    transaction.on_commit(_emit)
