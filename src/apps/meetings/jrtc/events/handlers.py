"""Durable domain reconciliation and Socket.IO fan-out for Janus events."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from jrtc.messaging import JANUS_EVENT_ROUTES

from apps.meetings.jrtc.errors import JrtcEventCorrelationError
from apps.meetings.jrtc.events.schemas import JanusBrokerEvent
from apps.meetings.jrtc.ids import janus_event_to_wire, janus_id_to_wire

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SocketDispatch:
    """One browser-compatible event and its authorized Socket.IO targets."""

    payload: dict[str, Any]
    socket_ids: tuple[str, ...]
    session_room: str


class DjangoJanusEventReconciler:
    """Reconcile logical Janus events against persisted media handles."""

    def __init__(
        self,
        *,
        snapshot_callback: Callable[[Any, dict[str, Any]], None] | None = None,
    ) -> None:
        self._snapshot_callback = snapshot_callback or self._default_snapshot_callback
        self._handlers: dict[
            str,
            Callable[[JanusBrokerEvent], tuple[SocketDispatch, ...]],
        ] = {
            logical_type: self._reconcile_correlated
            for logical_type in JANUS_EVENT_ROUTES.values()
        }

    @property
    def logical_event_types(self) -> frozenset[str]:
        """Expose the JRTC-owned logical routes supported by this dispatcher."""

        return frozenset(self._handlers)

    def reconcile(self, event: JanusBrokerEvent) -> tuple[SocketDispatch, ...]:
        """Apply durable state first, then prepare optional realtime fan-out."""

        try:
            handler = self._handlers[event.event_type]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported JRTC logical event type {event.event_type!r}."
            ) from exc
        return handler(event)

    def _reconcile_correlated(
        self,
        event: JanusBrokerEvent,
    ) -> tuple[SocketDispatch, ...]:
        handles = self._correlated_handles(event)
        if not handles:
            logger.warning(
                "JRTC event had no current persisted handle correlation",
                extra={"janus_event_type": event.event_type},
            )
            return ()

        dispatches: list[SocketDispatch] = []
        for media_handle in handles:
            original_handle_id = media_handle.janus_handle_id
            self._persist_latest_snapshot(media_handle, event.payload)
            if event.janus_type == "detached":
                self._reconcile_detached(media_handle, event.payload)
            else:
                self._snapshot_callback(
                    media_handle,
                    event.payload,
                )

            # Transaction-correlated JSEP has already been returned through
            # the awaiting command Future/Socket.IO ACK.  Reconciliation above
            # remains authoritative, but browser negotiation must happen once.
            if event.suppress_browser_jsep:
                continue
            dispatches.append(
                self._socket_dispatch(
                    media_handle,
                    event,
                    original_handle_id=original_handle_id,
                )
            )
        return tuple(dispatches)

    @staticmethod
    def _correlated_handles(event: JanusBrokerEvent) -> list[Any]:
        from apps.meetings.models import ParticipantMediaHandle

        queryset = ParticipantMediaHandle.objects.select_for_update().select_related(
            "connection",
            "participant__session",
            "participant__room",
        )
        if event.sender is None:
            # JRTC's session timeout response legitimately has no plugin
            # sender.  It applies to every handle owned by that Janus session;
            # all handle-bearing events still require the exact tuple below.
            return list(
                queryset.filter(
                    janus_session_id=event.session_id,
                    janus_handle_id__isnull=False,
                )
            )

        matches = list(
            queryset.filter(
                janus_session_id=event.session_id,
                janus_handle_id=event.sender,
            )[:2]
        )
        if len(matches) > 1:
            raise JrtcEventCorrelationError(
                "Multiple media handles share one Janus session/handle tuple."
            )
        return matches

    @staticmethod
    def _default_snapshot_callback(
        media_handle: Any,
        payload: dict[str, Any],
    ) -> None:
        # Import lazily so the dedicated consumer does not initialize the
        # command-plane JRTC runtime merely by loading handlers.
        from apps.meetings.services.signaling import MeetingMediaSignalService

        MeetingMediaSignalService.handle_callback_snapshot(
            media_handle,
            payload,
        )

    @staticmethod
    def _persist_latest_snapshot(media_handle: Any, payload: dict[str, Any]) -> None:
        from django.utils import timezone

        existing_state = (
            media_handle.janus_state
            if isinstance(media_handle.janus_state, dict)
            else {}
        )
        application_state = existing_state.get("_synq")
        snapshot = dict(payload)
        if isinstance(application_state, dict):
            snapshot["_synq"] = application_state
        observed_at = timezone.now()
        media_handle.__class__.objects.filter(pk=media_handle.pk).update(
            janus_state=snapshot,
            last_event_at=observed_at,
            updated_at=observed_at,
        )
        media_handle.janus_state = snapshot
        media_handle.last_event_at = observed_at
        media_handle.updated_at = observed_at

    @staticmethod
    def _reconcile_detached(media_handle: Any, payload: dict[str, Any]) -> None:
        """Apply detach cleanup using nullable integer semantics."""

        from django.utils import timezone

        from apps.meetings.models import (
            JanusHandleLifecycleState,
            JanusHandleType,
        )

        observed_at = timezone.now()
        snapshot = dict(payload)
        application_state = (
            media_handle.janus_state.get("_synq")
            if isinstance(media_handle.janus_state, dict)
            else None
        )
        if isinstance(application_state, dict):
            snapshot["_synq"] = {
                **application_state,
                "subscriber_joined": False,
            }
        elif media_handle.handle_type == JanusHandleType.SUBSCRIBER:
            snapshot["_synq"] = {"subscriber_joined": False}

        media_handle.streams.all().delete()
        media_handle.__class__.objects.filter(pk=media_handle.pk).update(
            lifecycle_state=JanusHandleLifecycleState.DETACHED,
            janus_session_id=None,
            janus_handle_id=None,
            runtime_owner_id=None,
            runtime_claim_id=None,
            selected_streams=[],
            janus_state=snapshot,
            last_event_at=observed_at,
            updated_at=observed_at,
        )
        if media_handle.handle_type == JanusHandleType.PUBLISHER:
            media_handle.participant.__class__.objects.filter(
                pk=media_handle.participant_id,
            ).update(
                janus_publisher_id=None,
                janus_private_id=None,
                updated_at=observed_at,
            )

    @staticmethod
    def _socket_dispatch(
        media_handle: Any,
        event: JanusBrokerEvent,
        *,
        original_handle_id: int,
    ) -> SocketDispatch:
        from apps.meetings.models import RealtimeConnectionStatus
        from apps.meetings.realtime.emitter import MeetingSocketEmitter

        participant = media_handle.participant
        connection = media_handle.connection
        active_statuses = {
            RealtimeConnectionStatus.CONNECTED,
            RealtimeConnectionStatus.SUBSCRIBED,
            RealtimeConnectionStatus.ACTIVE,
        }
        socket_ids = (
            (str(connection.socket_id),)
            if connection is not None
            and connection.status in active_statuses
            and connection.socket_id
            else ()
        )
        wire_event = janus_event_to_wire(event.payload)
        payload: dict[str, Any] = {
            "event_id": str(event.event_id),
            "model": media_handle._meta.label_lower,
            "pk": str(media_handle.pk),
            "plugin_field": "janus_handle_id",
            "plugin_attr": "handle",
            "plugin_identifier": media_handle.handle_type,
            "plugin_id": janus_id_to_wire(
                original_handle_id,
                name="Janus plugin ID",
            ),
            "event": wire_event,
            "session_id": str(participant.session_id),
            "room_id": str(participant.room_id),
            "participant_id": str(participant.pk),
            "profile_id": str(participant.profile_id),
            "handle_type": media_handle.handle_type,
        }
        if media_handle.connection_id is not None:
            payload["connection_id"] = str(media_handle.connection_id)
        if media_handle.opaque_id:
            payload["opaque_id"] = str(media_handle.opaque_id)
        return SocketDispatch(
            payload=payload,
            socket_ids=socket_ids,
            session_room=MeetingSocketEmitter.session_room_name(
                participant.session_id
            ),
        )


class SocketIoJanusEventEmitter:
    """Await Socket.IO publication without exposing broker details to clients."""

    def __init__(self, server_factory: Callable[[], Any] | None = None) -> None:
        self._server_factory = server_factory or self._default_server_factory

    async def emit_many(self, dispatches: Sequence[SocketDispatch]) -> None:
        """Fan out every prepared event, isolating individual socket failures."""

        if not dispatches:
            return
        from apps.meetings.realtime.emitter import MeetingSocketEmitter
        from apps.meetings.realtime.events import MeetingSocketEvents

        server = self._server_factory()
        for dispatch in dispatches:
            if not dispatch.socket_ids:
                # A handle-specific event can carry private SDP or ICE. Never
                # fall back to a session-wide room when its owner has no live
                # authorized socket; durable reconciliation has already run.
                logger.info(
                    "JRTC event reconciled without an active target socket",
                    extra={
                        "janus_event_type": dispatch.payload.get("event", {}).get(
                            "janus"
                        )
                    },
                )
                continue
            for socket_id in dispatch.socket_ids:
                try:
                    await server.emit(
                        MeetingSocketEvents.JANUS_EVENT,
                        dispatch.payload,
                        to=socket_id,
                        namespace=MeetingSocketEmitter.namespace,
                    )
                except Exception:
                    logger.exception(
                        "Unable to emit a JRTC event to a participant socket"
                    )
                    raise

    @staticmethod
    def _default_server_factory() -> Any:
        from conf.socketio import get_socket_server

        return get_socket_server()


__all__ = [
    "DjangoJanusEventReconciler",
    "SocketDispatch",
    "SocketIoJanusEventEmitter",
]
