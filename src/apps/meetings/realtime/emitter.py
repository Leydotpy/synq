"""Socket.IO emission helpers used by lifecycle services and Celery tasks."""

from __future__ import annotations

import logging

from asgiref.sync import async_to_sync

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
from apps.meetings.services.state import MeetingStateBuilder

logger = logging.getLogger(__name__)


class MeetingSocketEmitter:
    """Emit meeting-domain realtime events through the shared Socket.IO server."""

    namespace = "/meetings"

    @staticmethod
    def emit_session_state(
        *,
        session: MeetingSession,
        exclude_socket_ids: set[str] | None = None,
    ) -> None:
        """Send a personalized snapshot to every active admitted connection.

        A single anonymous room broadcast cannot represent ``local_participant``
        or coordinator-only waiting-room data correctly.  Targeting Socket.IO
        session ids also works across workers when the configured Redis manager
        is enabled.
        """

        excluded = exclude_socket_ids or set()
        payloads_by_profile: dict[str, dict] = {}
        for socket_id, profile in MeetingSocketEmitter._active_session_targets(session_id=session.pk):
            if socket_id in excluded:
                continue
            profile_key = str(profile.pk)
            payload = payloads_by_profile.get(profile_key)
            if payload is None:
                payload = MeetingStateBuilder.build(session=session, authenticated_profile=profile)
                payloads_by_profile[profile_key] = payload
            MeetingSocketEmitter._emit(
                event=MeetingSocketEvents.SESSION_STATE,
                payload=payload,
                room=socket_id,
            )

    @staticmethod
    def emit_join_request_created(*, join_request: MeetingJoinRequest) -> None:
        """Broadcast a newly created join request to coordinators and the requester."""

        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.JOIN_REQUEST_CREATED,
            payload=MeetingStateBuilder.serialize_join_request(join_request),
            room=MeetingSocketEmitter.coordinator_room_name(join_request.session_id),
        )
        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.JOIN_REQUEST_CREATED,
            payload=MeetingStateBuilder.serialize_join_request(join_request),
            room=MeetingSocketEmitter.profile_room_name(join_request.profile_id),
        )

    @staticmethod
    def emit_session_ended(*, session: MeetingSession, reason: str = "") -> None:
        """Notify admitted and waiting-room sockets that the session is terminal."""

        payload = {
            "session_id": str(session.pk),
            "reason": reason,
            "ended_at": session.ended_at.isoformat() if session.ended_at else None,
        }
        socket_ids = (
            ParticipantConnection.objects.filter(
                session=session,
                status__in=[
                    RealtimeConnectionStatus.CONNECTED,
                    RealtimeConnectionStatus.SUBSCRIBED,
                    RealtimeConnectionStatus.ACTIVE,
                ],
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
    def emit_join_request_reviewed(*, join_request: MeetingJoinRequest, participant: Participant | None) -> None:
        """Broadcast a join-request review decision to the session and requester rooms."""

        payload = {
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
        """Broadcast participant removal to the room and the affected profile room."""

        payload = {"participant_id": str(participant.pk), "reason": reason}
        MeetingSocketEmitter._emit_to_active_session_connections(
            event=MeetingSocketEvents.PARTICIPANT_REMOVED,
            payload=payload,
            session_id=session.pk,
        )
        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.PARTICIPANT_REMOVED,
            payload=payload,
            room=MeetingSocketEmitter.profile_room_name(participant.profile_id),
        )

    @staticmethod
    def emit_chat_message(*, message: MeetingMessage) -> None:
        """Broadcast a new chat message to all sockets subscribed to the session room."""

        MeetingSocketEmitter._emit_to_active_session_connections(
            event=MeetingSocketEvents.CHAT_MESSAGE_CREATED,
            payload=MeetingStateBuilder.serialize_message(message),
            session_id=message.session_id,
        )

    @staticmethod
    def emit_reaction(*, reaction: MeetingReaction) -> None:
        """Broadcast a new reaction to all sockets subscribed to the session room."""

        MeetingSocketEmitter._emit_to_active_session_connections(
            event=MeetingSocketEvents.REACTION_CREATED,
            payload=MeetingStateBuilder.serialize_reaction(reaction),
            session_id=reaction.session_id,
        )

    @staticmethod
    def emit_error(*, room: str, message: str, details: dict | None = None) -> None:
        """Broadcast an operational error payload to a targeted room."""

        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.ERROR,
            payload={"message": message, "details": details or {}},
            room=room,
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
    def _active_session_targets(*, session_id):
        """Return socket/profile pairs for currently admitted participants."""

        connections = (
            ParticipantConnection.objects.filter(
                session_id=session_id,
                participant__status=ParticipantStatus.ACTIVE,
                status=RealtimeConnectionStatus.ACTIVE,
            )
            .exclude(socket_id="")
            .select_related("profile")
        )
        return [(connection.socket_id, connection.profile) for connection in connections]

    @staticmethod
    def _emit_to_active_session_connections(*, event: str, payload: dict, session_id) -> None:
        """Fan an event out only to admitted active sockets, never lobby sockets."""

        socket_ids = ParticipantConnection.objects.filter(
            session_id=session_id,
            participant__status=ParticipantStatus.ACTIVE,
            status=RealtimeConnectionStatus.ACTIVE,
        ).exclude(socket_id="").values_list("socket_id", flat=True).distinct()
        for socket_id in socket_ids:
            MeetingSocketEmitter._emit(event=event, payload=payload, room=socket_id)

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
