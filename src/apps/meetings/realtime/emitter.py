"""Socket.IO emission helpers used by lifecycle services and Celery tasks."""

from __future__ import annotations

import logging

from asgiref.sync import async_to_sync

from apps.meetings.models import MeetingJoinRequest, MeetingMessage, MeetingReaction, MeetingSession, Participant
from apps.meetings.realtime.events import MeetingSocketEvents
from apps.meetings.services.state import MeetingStateBuilder

logger = logging.getLogger(__name__)


class MeetingSocketEmitter:
    """Emit meeting-domain realtime events through the shared Socket.IO server."""

    namespace = "/meetings"

    @staticmethod
    def emit_session_state(*, session: MeetingSession) -> None:
        """Broadcast a fresh session snapshot to all sockets subscribed to the session room."""

        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.SESSION_STATE,
            payload=MeetingStateBuilder.build(session=session),
            room=MeetingSocketEmitter.session_room_name(session.pk),
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
        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.PARTICIPANT_REMOVED,
            payload=payload,
            room=MeetingSocketEmitter.session_room_name(session.pk),
        )
        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.PARTICIPANT_REMOVED,
            payload=payload,
            room=MeetingSocketEmitter.profile_room_name(participant.profile_id),
        )

    @staticmethod
    def emit_chat_message(*, message: MeetingMessage) -> None:
        """Broadcast a new chat message to all sockets subscribed to the session room."""

        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.CHAT_MESSAGE_CREATED,
            payload=MeetingStateBuilder.serialize_message(message),
            room=MeetingSocketEmitter.session_room_name(message.session_id),
        )

    @staticmethod
    def emit_reaction(*, reaction: MeetingReaction) -> None:
        """Broadcast a new reaction to all sockets subscribed to the session room."""

        MeetingSocketEmitter._emit(
            event=MeetingSocketEvents.REACTION_CREATED,
            payload=MeetingStateBuilder.serialize_reaction(reaction),
            room=MeetingSocketEmitter.session_room_name(reaction.session_id),
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
