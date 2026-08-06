"""Socket.IO namespace responsible for meeting presence, waiting-room, and room events."""

from __future__ import annotations

import logging
from asgiref.sync import sync_to_async
from socketio import AsyncNamespace

from django.conf import settings

from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.models import (
    MeetingJoinRequest,
    MeetingSession,
    Participant,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.realtime.auth import extract_ip_address, extract_scope_headers, resolve_socket_user
from apps.meetings.realtime.emitter import MeetingSocketEmitter
from apps.meetings.realtime.events import MeetingSocketEvents
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.services.permissions import MeetingPermissionService
from apps.meetings.services.signaling import MeetingMediaSignalService
from apps.meetings.services.state import MeetingStateBuilder
from core.api.api import _get_or_create_profile_for_user


logger = logging.getLogger(__name__)


class MeetingNamespace(AsyncNamespace):
    """Implement the ``/meetings`` namespace for room subscription and moderation workflows."""

    async def trigger_event(self, event: str, *args):
        """Wrap namespace events so operational errors become structured Socket.IO payloads."""

        try:
            return await super().trigger_event(event, *args)
        except Exception as exc:
            if event == "connect":
                raise
            logger.exception("Socket.IO meeting command '%s' failed.", event)
            sid = args[0] if args else None
            error_payload = {"message": str(exc), "event": event}
            if sid is not None:
                await self.emit(MeetingSocketEvents.ERROR, error_payload, to=sid)
            # Socket.IO does not turn a swallowed handler exception into an
            # acknowledgement error. Return an explicit negative envelope so
            # browser commands fail immediately instead of appearing to work
            # (or, for media, falling back to a fabricated success payload).
            return {"ok": False, "error": error_payload}

    async def on_connect(self, sid: str, environ: dict, auth: dict | None) -> None:
        """Authenticate the connecting socket and place it in a profile-scoped room."""
        user = await sync_to_async(resolve_socket_user)(environ, auth)
        if user is None:
            raise ConnectionRefusedError("Authentication required.")
        profile = await sync_to_async(_get_or_create_profile_for_user)(user)
        await self.save_session(sid, {"user_id": str(user.pk), "profile_id": str(profile.pk)})
        await self.enter_room(sid, MeetingSocketEmitter.profile_room_name(profile.pk))
        if settings.DEBUG:
            logger.debug("socket connected with session %s" % (await self.get_session(sid)))

    async def on_disconnect(self, sid: str) -> None:
        """Mark the backing realtime connection as disconnected."""

        await sync_to_async(MeetingLifecycleService.mark_connection_disconnected)(socket_id=sid)

    async def on_session_subscribe(self, sid: str, data: dict) -> dict:
        """Subscribe a socket to a session room and send a personalized state snapshot."""
        profile = await self._get_profile(sid)
        session = await self._get_session(data["session_id"])
        environ = self.server.get_environ(sid, namespace=self.namespace) or {}
        headers = extract_scope_headers(environ)
        connection = await sync_to_async(MeetingLifecycleService.bind_connection_to_session)(
            socket_id=sid,
            session=session,
            profile=profile,
            transport=data.get("transport", "web"),
            user_agent=headers.get("user-agent", data.get("user_agent", "")),
            ip_address=extract_ip_address(environ),
            client_session_key=data.get("client_session_key", ""),
            metadata=data.get("metadata", {}),
        )
        if settings.DEBUG:
            logger.debug("connection bound to session %s" % connection)
        # Lobby sockets stay out of the full meeting room until admission.  The
        # emitter also targets participant events by active DB connection as a
        # cross-worker enforcement boundary.
        if connection.participant_id and connection.status == RealtimeConnectionStatus.ACTIVE:
            await self.enter_room(sid, MeetingSocketEmitter.session_room_name(session.pk))
        membership = await sync_to_async(MeetingPermissionService.get_room_membership)(session.room, profile)
        if membership and membership.can_manage_waiting_room:
            await self.enter_room(sid, MeetingSocketEmitter.coordinator_room_name(session.pk))
        state = await sync_to_async(MeetingStateBuilder.build)(session=session, authenticated_profile=profile)
        await sync_to_async(MeetingLifecycleService.mark_connection_heartbeat)(socket_id=connection.socket_id)

        if settings.DEBUG:
            logger.debug("State Sync success %s" % state)
        return state

    async def on_session_join_request(self, sid: str, data: dict) -> dict:
        """Create a waiting-room join request for the authenticated profile."""

        profile = await self._get_profile(sid)
        session = await self._get_session(data["session_id"])
        connection = await sync_to_async(lambda: session.connections.filter(socket_id=sid).first())()
        join_request = await sync_to_async(MeetingLifecycleService.request_join)(
            session=session,
            profile=profile,
            requested_display_name=data.get("display_name", ""),
            requested_role=data.get("requested_role", "participant"),
            note=data.get("note", ""),
            client_state=data.get("client_state", {}),
            connection=connection,
            passcode=data.get("passcode"),
            invite_token=data.get("invite_token"),
        )
        return MeetingStateBuilder.serialize_join_request(join_request)

    async def on_session_review_join_request(self, sid: str, data: dict) -> dict:
        """Approve or reject a pending join request as a coordinator."""

        reviewer_profile = await self._get_profile(sid)
        approve = data.get("approve")
        if not isinstance(approve, bool):
            raise MeetingDomainError("'approve' must be a boolean.")
        join_request = await sync_to_async(self._get_join_request)(
            data["join_request_id"],
            data["session_id"],
        )
        participant = await sync_to_async(MeetingLifecycleService.review_join_request)(
            join_request=join_request,
            reviewer_profile=reviewer_profile,
            approve=approve,
            reason=data.get("reason", ""),
        )
        return {
            "ok": True,
            "join_request": MeetingStateBuilder.serialize_join_request(join_request),
            "participant": MeetingStateBuilder.serialize_participant(participant),
        }

    async def on_session_chat_message(self, sid: str, data: dict) -> dict:
        """Persist and broadcast a chat message from the authenticated participant."""

        participant = await sync_to_async(self._get_participant_for_session_socket)(data["session_id"], sid)
        message = await sync_to_async(MeetingLifecycleService.record_chat_message)(
            session=participant.session,
            participant=participant,
            body=data["body"],
            metadata=data.get("metadata", {}),
        )
        return MeetingStateBuilder.serialize_message(message)

    async def on_session_reaction(self, sid: str, data: dict) -> dict:
        """Persist and broadcast a participant reaction."""

        participant = await sync_to_async(self._get_participant_for_session_socket)(data["session_id"], sid)
        reaction = await sync_to_async(MeetingLifecycleService.record_reaction)(
            session=participant.session,
            participant=participant,
            reaction=data["reaction"],
            metadata=data.get("metadata", {}),
            expires_in_seconds=data.get("expires_in_seconds", 8),
        )
        return MeetingStateBuilder.serialize_reaction(reaction)

    async def on_session_update_participant(self, sid: str, data: dict) -> dict:
        """Update participant interaction permissions or moderation flags."""

        actor_profile = await self._get_profile(sid)
        participant = await sync_to_async(self._get_participant_by_id)(
            data["participant_id"],
            data["session_id"],
        )
        participant = await sync_to_async(MeetingLifecycleService.update_participant_permissions)(
            session=participant.session,
            actor_profile=actor_profile,
            participant=participant,
            updates=data.get("updates", {}),
        )
        return {"ok": True, "participant": MeetingStateBuilder.serialize_participant(participant)}

    async def on_session_remove_participant(self, sid: str, data: dict) -> dict:
        """Remove a participant from the live session as a coordinator."""

        actor_profile = await self._get_profile(sid)
        participant = await sync_to_async(self._get_participant_by_id)(
            data["participant_id"],
            data["session_id"],
        )
        participant = await sync_to_async(MeetingLifecycleService.remove_participant)(
            session=participant.session,
            actor_profile=actor_profile,
            participant=participant,
            reason=data.get("reason", ""),
        )
        return {"ok": True, "participant": MeetingStateBuilder.serialize_participant(participant)}

    async def on_session_leave(self, sid: str, data: dict) -> dict:
        """Mark the current profile as intentionally leaving a session."""

        profile = await self._get_profile(sid)
        session = await self._get_session(data["session_id"])
        await sync_to_async(MeetingLifecycleService.leave_participant)(
            session=session,
            profile=profile,
            socket_id=sid,
            reason=data.get("reason", ""),
        )
        await self.leave_room(sid, MeetingSocketEmitter.session_room_name(session.pk))
        await self.leave_room(sid, MeetingSocketEmitter.coordinator_room_name(session.pk))
        return {"ok": True}

    async def on_session_heartbeat(self, sid: str, _: dict | None = None) -> dict:
        """Refresh realtime heartbeat state for the socket connection."""

        connection = await sync_to_async(MeetingLifecycleService.mark_connection_heartbeat)(socket_id=sid)
        return {"ok": connection is not None}

    async def on_session_media_publish(self, sid: str, data: dict) -> dict | None:
        """Publish or reconfigure the local participant's Janus publisher handle from a browser offer."""

        participant, connection = await sync_to_async(self._get_participant_connection_for_session_socket)(data["session_id"], sid)
        return await sync_to_async(MeetingMediaSignalService.publish_offer)(
            participant=participant,
            connection=connection,
            offer=data["offer"],
            tracks=data.get("tracks", []),
        )

    async def on_session_media_unpublish(self, sid: str, data: dict) -> dict | None:
        """Unpublish all currently active local tracks for the authenticated participant."""

        participant, connection = await sync_to_async(self._get_participant_connection_for_session_socket)(data["session_id"], sid)
        return await sync_to_async(MeetingMediaSignalService.unpublish)(
            participant=participant,
            connection=connection,
        )

    async def on_session_media_sync_subscriptions(self, sid: str, data: dict) -> dict | None:
        """Join or update the participant subscriber handle to mirror active remote publishers."""

        if settings.DEBUG:
            logger.debug("on_session_media_sync_subscriptions: %s", data)

        participant, connection = await sync_to_async(self._get_participant_connection_for_session_socket)(data["session_id"], sid)
        return await sync_to_async(MeetingMediaSignalService.sync_subscriptions)(
            participant=participant,
            connection=connection,
        )

    async def on_session_media_start_subscriber(self, sid: str, data: dict) -> dict | None:
        """Complete the subscriber Janus negotiation with the browser's SDP answer."""

        participant, connection = await sync_to_async(self._get_participant_connection_for_session_socket)(data["session_id"], sid)
        return await sync_to_async(MeetingMediaSignalService.start_subscriber)(
            participant=participant,
            connection=connection,
            answer=data["answer"],
        )

    async def on_session_media_trickle(self, sid: str, data: dict) -> dict | None:
        """Forward browser ICE candidates to the backend-owned Janus publisher or subscriber handle."""

        participant, connection = await sync_to_async(self._get_participant_connection_for_session_socket)(data["session_id"], sid)
        return await sync_to_async(MeetingMediaSignalService.trickle)(
            participant=participant,
            connection=connection,
            handle_type=data["handle_type"],
            candidates=data.get("candidates") or (None if data.get("completed") else data.get("candidate", {})),
            completed=bool(data.get("completed", False)),
        )

    async def _get_profile(self, sid: str):
        """Load the authenticated profile bound to the current socket session."""

        session_data = await self.get_session(sid)
        from apps.profiles.models import Profile

        return await sync_to_async(Profile.objects.get)(pk=session_data["profile_id"])

    async def _get_session(self, session_id: str) -> MeetingSession:
        """Load a meeting session for subscription or interaction events."""

        return await sync_to_async(MeetingSession.objects.select_related("room", "started_by_profile").get)(pk=session_id)

    def _get_join_request(self, join_request_id: str, session_id: str) -> MeetingJoinRequest:
        """Load a join request synchronously for ``sync_to_async`` usage."""

        return MeetingJoinRequest.objects.select_related("session", "room", "profile").get(
            pk=join_request_id,
            session_id=session_id,
        )

    def _get_participant_by_id(self, participant_id: str, session_id: str) -> Participant:
        """Load a participant synchronously for ``sync_to_async`` usage."""

        return Participant.objects.select_related("session", "room", "profile").get(
            pk=participant_id,
            session_id=session_id,
        )

    def _get_participant_for_session_socket(self, session_id: str, sid: str) -> Participant:
        """Load the current participant for a socket inside the specified session."""

        participant, _ = self._get_participant_connection_for_session_socket(session_id, sid)
        return participant

    def _get_participant_connection_for_session_socket(self, session_id: str, sid: str):
        """Load and, when possible, repair the active participant/connection pair for a socket."""

        from apps.meetings.models import ParticipantConnection

        connection = (
            ParticipantConnection.objects.select_related("participant", "participant__session", "profile", "session")
            .filter(
                socket_id=sid,
                session_id=session_id,
                status__in=[
                    RealtimeConnectionStatus.CONNECTED,
                    RealtimeConnectionStatus.SUBSCRIBED,
                    RealtimeConnectionStatus.ACTIVE,
                ],
            )
            .first()
        )
        if connection is None:
            raise ValueError("Socket is not subscribed to this meeting session.")
        if (
            connection.participant
            and connection.participant.status == ParticipantStatus.ACTIVE
            and str(connection.participant.session_id) == str(session_id)
        ):
            return connection.participant, connection

        participant = (
            Participant.objects.select_related("session", "room", "profile")
            .filter(session_id=session_id, profile_id=connection.profile_id)
            .exclude(status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED])
            .first()
        )
        if participant is None:
            raise ValueError("Socket is not bound to an admitted participant in this session.")
        if participant.status not in {ParticipantStatus.ADMITTED, ParticipantStatus.ACTIVE, ParticipantStatus.DISCONNECTED}:
            raise ValueError("Socket is not bound to an active participant in this session.")

        participant.mark_joined()
        participant.save(update_fields=["status", "joined_at", "last_seen_at", "updated_at"])
        connection.participant = participant
        connection.status = RealtimeConnectionStatus.ACTIVE
        connection.disconnected_at = None
        connection.save(update_fields=["participant", "status", "disconnected_at", "updated_at"])
        return participant, connection

    def _get_connection_by_socket(self, sid: str):
        """Load the backing realtime connection for a socket, when present."""

        from apps.meetings.models import ParticipantConnection

        return ParticipantConnection.objects.select_related("participant", "session").filter(socket_id=sid).first()
