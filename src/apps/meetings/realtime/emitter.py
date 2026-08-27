"""Socket.IO emission helpers used by lifecycle services and Celery tasks."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from asgiref.sync import async_to_sync
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
    def emit_session_ended(*, session: MeetingSession, reason: str = "") -> None:
        """Notify every subscribed attendee that the meeting is terminal."""

        payload = {
            "session_id": str(session.pk),
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
        """Notify active participants and every socket owned by the removed profile."""

        payload = {"participant_id": str(participant.pk), "reason": reason}
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
        for socket_id in MeetingSocketEmitter.active_participant_socket_ids(
            reaction.session_id,
        ):
            MeetingSocketEmitter._emit(
                event=MeetingSocketEvents.REACTION_CREATED,
                payload=payload,
                room=socket_id,
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
