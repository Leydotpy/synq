"""Socket.IO emission helpers used by lifecycle services and Celery tasks."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from asgiref.sync import async_to_sync, sync_to_async
from django.db.models import Q

from apps.meetings.models import (
    MeetingJoinRequest,
    MeetingMessage,
    MeetingReaction,
    MeetingSession,
    Participant,
    ParticipantConnection,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.realtime.events import MeetingSocketEvents
from apps.meetings.realtime.context import event_context
from apps.meetings.services.state import MeetingStateBuilder

logger = logging.getLogger(__name__)


class MeetingSocketEmitter:
    """Emit meeting-domain realtime events through the shared Socket.IO server."""

    namespace = "/meetings"

    @staticmethod
    def emit_session_state(*, session: MeetingSession) -> None:
        """Send a permission-aware session snapshot to each live socket."""

        connections = ParticipantConnection.objects.filter(
            session=session,
            status__in=[
                RealtimeConnectionStatus.CONNECTED,
                RealtimeConnectionStatus.SUBSCRIBED,
                RealtimeConnectionStatus.ACTIVE,
            ],
        ).select_related("profile")
        snapshots: dict[str, dict] = {}
        for connection in connections:
            profile_key = str(connection.profile_id)
            if profile_key not in snapshots:
                snapshots[profile_key] = MeetingStateBuilder.build(
                    session=session,
                    authenticated_profile=connection.profile,
                )
            MeetingSocketEmitter._emit(
                event=MeetingSocketEvents.SESSION_STATE,
                payload=snapshots[profile_key],
                room=connection.socket_id,
            )

    @staticmethod
    async def aemit_session_state(*, session: MeetingSession) -> None:
        from conf.socketio import get_socket_server
        connections = ParticipantConnection.objects.filter(session=session,
            status__in=[RealtimeConnectionStatus.CONNECTED, RealtimeConnectionStatus.SUBSCRIBED, RealtimeConnectionStatus.ACTIVE],
        ).select_related("profile")
        snapshots = {}
        async for connection in connections:
            key = str(connection.profile_id)
            if key not in snapshots:
                snapshots[key] = await sync_to_async(MeetingStateBuilder.build)(session=session, authenticated_profile=connection.profile)
            try:
                await get_socket_server().emit(MeetingSocketEvents.SESSION_STATE, snapshots[key],
                    room=connection.socket_id, namespace=MeetingSocketEmitter.namespace)
            except Exception:
                logger.exception("Unable to emit session state", extra={"session_id": str(session.pk)})

    @staticmethod
    def emit_session_ended(*, session: MeetingSession, reason: str = "") -> None:
        """Notify every subscribed attendee that the meeting is terminal."""

        payload = {
            "session_id": str(session.pk),
            "event_context": event_context("session-ended", session.pk, session.pk, session.ended_at),
            "reason": reason,
            "ended_at": session.ended_at.isoformat() if session.ended_at else None,
        }
        just_closed = Q(
            status=RealtimeConnectionStatus.DISCONNECTED,
            disconnected_at=session.ended_at,
        )
        socket_ids = (
            ParticipantConnection.objects.filter(session=session)
            .filter(
                Q(
                    status__in=[
                        RealtimeConnectionStatus.CONNECTED,
                        RealtimeConnectionStatus.SUBSCRIBED,
                        RealtimeConnectionStatus.ACTIVE,
                    ],
                )
                | just_closed,
            )
            .exclude(socket_id="")
            .values_list("socket_id", flat=True)
            .distinct()
        )
        for socket_id in socket_ids:
            MeetingSocketEmitter._emit(
                event=MeetingSocketEvents.SESSION_ENDED,
                payload=payload,
                room=socket_id,
            )

    @staticmethod
    def emit_join_request_created(*, join_request: MeetingJoinRequest) -> None:
        """Broadcast a newly created join request to coordinators and the requester."""

        payload = MeetingStateBuilder.serialize_join_request(join_request)
        payload["event_context"] = event_context("join-request-created", join_request.pk, join_request.session_id,
            join_request.created_at, actor_profile_id=join_request.profile_id)
        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.JOIN_REQUEST_CREATED,
            payload=payload,
            room=MeetingSocketEmitter.coordinator_room_name(join_request.session_id),
        )
        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.JOIN_REQUEST_CREATED,
            payload=payload,
            room=MeetingSocketEmitter.profile_room_name(join_request.profile_id),
        )

    @staticmethod
    def emit_join_request_reviewed(*, join_request: MeetingJoinRequest, participant: Participant | None) -> None:
        """Broadcast a join-request review decision to the session and requester rooms."""

        payload = {
            "event_context": event_context("join-request-reviewed", f"{join_request.pk}:{join_request.status}", join_request.session_id,
                join_request.reviewed_at, actor_profile_id=join_request.reviewed_by_profile_id),
            "join_request": MeetingStateBuilder.serialize_join_request(join_request),
            "participant": MeetingStateBuilder.serialize_participant(participant),
        }
        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.JOIN_REQUEST_REVIEWED,
            payload=payload,
            room=MeetingSocketEmitter.coordinator_room_name(join_request.session_id),
        )
        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.JOIN_REQUEST_REVIEWED,
            payload=payload,
            room=MeetingSocketEmitter.profile_room_name(join_request.profile_id),
        )

    @staticmethod
    def emit_participant_removed(*, session: MeetingSession, participant: Participant, reason: str = "") -> None:
        """Notify active participants and every socket owned by the removed profile."""

        payload = {"participant_id": str(participant.pk), "reason": reason,
            "event_context": event_context("participant-removed", f"{participant.pk}:{participant.left_at}", session.pk,
                participant.left_at, actor_participant_id=participant.pk)}
        for socket_id in MeetingSocketEmitter.active_participant_socket_ids(
            session.pk,
        ):
            MeetingSocketEmitter._emit(
                event=MeetingSocketEvents.PARTICIPANT_REMOVED,
                payload=payload,
                room=socket_id,
            )
        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.PARTICIPANT_REMOVED,
            payload=payload,
            room=MeetingSocketEmitter.profile_room_name(participant.profile_id),
        )

    @staticmethod
    def emit_chat_message(*, message: MeetingMessage) -> None:
        """Broadcast a new chat message only to DB-active participants."""

        payload = MeetingStateBuilder.serialize_message(message)
        payload["event_context"] = event_context("message-created", message.pk, message.session_id,
            message.created_at, actor_participant_id=message.participant_id)
        for socket_id in MeetingSocketEmitter.active_participant_socket_ids(
            message.session_id,
        ):
            MeetingSocketEmitter._emit(
                event=MeetingSocketEvents.CHAT_MESSAGE_CREATED,
                payload=payload,
                room=socket_id,
            )

    @staticmethod
    def emit_reaction(*, reaction: MeetingReaction) -> None:
        """Broadcast a new reaction only to DB-active participants."""

        payload = MeetingStateBuilder.serialize_reaction(reaction)
        payload["event_context"] = event_context("reaction-created", reaction.pk, reaction.session_id,
            reaction.created_at, actor_participant_id=reaction.participant_id)
        for socket_id in MeetingSocketEmitter.active_participant_socket_ids(
            reaction.session_id,
        ):
            MeetingSocketEmitter._emit(
                event=MeetingSocketEvents.REACTION_CREATED,
                payload=payload,
                room=socket_id,
            )

    @staticmethod
    def emit_participant_presence(*, event) -> None:
        """Publish a committed presence transition, never a snapshot difference."""
        payload = {
            "participant_id": str(event.actor_participant_id),
            "status": "joined" if event.event_type == "participant_joined" else "left",
            "event_context": event_context(event.event_type, event.pk, event.session_id,
                event.created_at, actor_profile_id=event.actor_profile_id,
                actor_participant_id=event.actor_participant_id),
        }
        for socket_id in MeetingSocketEmitter.active_participant_socket_ids(event.session_id):
            MeetingSocketEmitter._emit(event=MeetingSocketEvents.PARTICIPANT_PRESENCE_CHANGED, payload=payload, room=socket_id)

    @staticmethod
    def emit_error(*, room: str, message: str, details: dict | None = None) -> None:
        """Broadcast an operational error payload to a targeted room."""

        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.ERROR,
            payload={"message": message, "details": details or {}},
            room=room,
        )

    @staticmethod
    def disconnect_sockets(socket_ids: Iterable[object]) -> None:
        """Force owner-routed namespace disconnects for revoked generations."""

        from conf.socketio import get_socket_server

        server = get_socket_server()
        for socket_id in dict.fromkeys(str(value) for value in socket_ids if value):
            try:
                # AsyncRedisManager publishes this operation to the worker
                # that owns the socket, whose on_disconnect then invalidates
                # connection-tagged process-local JRTC bindings.
                async_to_sync(server.disconnect)(
                    socket_id,
                    namespace=MeetingSocketEmitter.namespace,
                )
            except Exception:
                logger.exception(
                    "Unable to disconnect a revoked meeting socket",
                )

    @staticmethod
    def session_room_name(session_id) -> str:
        """Return the Socket.IO room name used for session-wide fan-out."""

        return f"session:{session_id}"

    @staticmethod
    def coordinator_room_name(session_id) -> str:
        """Return the Socket.IO room name used for waiting-room coordinators."""

        return f"{MeetingSocketEmitter.session_room_name(session_id)}:coordinators"

    @staticmethod
    def profile_room_name(profile_id) -> str:
        """Return the Socket.IO room name used for per-profile fan-out."""

        return f"profile:{profile_id}"

    @staticmethod
    def active_participant_socket_ids(session_id) -> list[str]:
        """Return DB-authorized sockets for participant-only content fan-out."""

        return list(
            ParticipantConnection.objects.filter(
                session_id=session_id,
                status=RealtimeConnectionStatus.ACTIVE,
                participant__status=ParticipantStatus.ACTIVE,
            )
            .exclude(socket_id="")
            .values_list("socket_id", flat=True)
            .distinct(),
        )

    @staticmethod
    def _emit(*, event: str, payload: dict, room: str) -> None:
        """Emit a Socket.IO event through the shared server using sync-safe bridging."""

        from conf.socketio import get_socket_server

        try:
            async_to_sync(get_socket_server().emit)(
                event,
                payload,
                room=room,
                namespace=MeetingSocketEmitter.namespace,
            )
        except Exception:
            logger.exception("Unable to emit Socket.IO event '%s' to room '%s'.", event, room)
