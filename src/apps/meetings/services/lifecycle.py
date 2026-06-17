"""High-level orchestration for room creation, admission, moderation, and cleanup."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from django.db import transaction
from django.utils import timezone

from apps.meetings.exceptions import MeetingDomainError, MeetingJoinRequestStateError
from apps.meetings.models import (
    JanusHandleLifecycleState,
    JanusHandleType,
    MeetingAccessPolicy,
    MeetingEvent,
    MeetingEventType,
    MeetingJoinRequest,
    MeetingJoinRequestStatus,
    MeetingLifecycleState,
    MeetingMessage,
    MeetingReaction,
    MeetingRole,
    MeetingRoom,
    MeetingRoomMembership,
    MeetingSession,
    Participant,
    ParticipantConnection,
    ParticipantMediaHandle,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.services.permissions import MeetingPermissionService
from apps.meetings.services.invitations import MeetingInvitationService
from core.utils import generate_short_code

logger = logging.getLogger(__name__)


def record_session_event(
    *,
    session: MeetingSession,
    event_type: str,
    actor_profile=None,
    actor_participant: Participant | None = None,
    payload: dict[str, Any] | None = None,
) -> MeetingEvent:
    """Persist a structured meeting event for observability and later debugging."""

    return MeetingEvent.objects.create(
        session=session,
        actor_profile=actor_profile,
        actor_participant=actor_participant,
        event_type=event_type,
        payload=payload or {},
    )


def dispatch_task(task, *args) -> None:
    """Attempt to enqueue a Celery task without breaking the request path on broker errors."""

    try:
        task.delay(*args)
    except Exception:
        logger.warning(
            "Unable to enqueue Celery task '%s'; make sure the broker is running.",
            getattr(task, "name", repr(task)),
        )


class MeetingLifecycleService:
    """Coordinate meeting lifecycle mutations while keeping invariants centralized."""

    @staticmethod
    def create_room(
        *,
        creator_profile,
        title: str,
        description: str = "",
        access_policy: str = MeetingAccessPolicy.APPROVAL_REQUIRED,
        is_waiting_room_enabled: bool = True,
        max_participants: int = 100,
        passcode: str | None = None,
        scheduled_start_at=None,
        scheduled_end_at=None,
        janus_room_configuration: dict[str, Any] | None = None,
        feature_flags: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> MeetingRoom:
        """Create a durable room and seed the creator as its host coordinator."""

        with transaction.atomic():
            room = MeetingRoom(
                title=title,
                description=description,
                created_by_profile=creator_profile,
                access_policy=access_policy,
                is_waiting_room_enabled=is_waiting_room_enabled,
                max_participants=max_participants,
                scheduled_start_at=scheduled_start_at,
                scheduled_end_at=scheduled_end_at,
                janus_room_configuration=janus_room_configuration or {},
                feature_flags=feature_flags or {},
                metadata=metadata or {},
            )
            room.set_passcode(passcode)
            room.save()
            MeetingRoomMembership.objects.create(
                room=room,
                profile=creator_profile,
                role=MeetingRole.HOST,
                invited_by_profile=creator_profile,
            )
            return room

    @staticmethod
    def start_session(*, room: MeetingRoom, started_by_profile, metadata: dict[str, Any] | None = None) -> MeetingSession:
        """Create or reuse the room's live session and seed the host participant."""

        existing_session = room.sessions.live().order_by("-created_at").first()
        if existing_session:
            return existing_session
        membership = MeetingPermissionService.get_room_membership(room=room, profile_or_user=started_by_profile)
        with transaction.atomic():
            session = MeetingSession.objects.create(
                room=room,
                started_by_profile=started_by_profile,
                lifecycle_state=MeetingLifecycleState.PROVISIONING,
                started_at=timezone.now(),
                janus_room_secret=generate_short_code(16),
                janus_room_pin=generate_short_code(8),
                metadata=metadata or {},
            )
            participant = Participant(
                room=room,
                session=session,
                profile=started_by_profile,
                membership=membership,
                role=membership.role if membership else MeetingRole.HOST,
                display_name=started_by_profile.display_name or started_by_profile.handle,
            )
            participant.apply_membership_defaults()
            participant.mark_joined()
            participant.save()
            record_session_event(
                session=session,
                event_type=MeetingEventType.SESSION_CREATED,
                actor_profile=started_by_profile,
                actor_participant=participant,
                payload={"room_id": str(room.pk)},
            )
            MeetingLifecycleService.refresh_session_metrics(session=session)

        def enqueue_follow_up_tasks() -> None:
            """Queue asynchronous Janus work once the transaction commits successfully."""

            from apps.meetings.tasks import attach_participant_media_handles, provision_janus_room_for_session

            dispatch_task(provision_janus_room_for_session, str(session.pk))
            dispatch_task(attach_participant_media_handles, str(participant.pk))

        transaction.on_commit(enqueue_follow_up_tasks)
        return session

    @staticmethod
    def request_join(
        *,
        session: MeetingSession,
        profile,
        requested_display_name: str = "",
        requested_role: str = MeetingRole.PARTICIPANT,
        note: str = "",
        client_state: dict[str, Any] | None = None,
        connection: ParticipantConnection | None = None,
        passcode: str | None = None,
        invite_token: str | None = None,
    ) -> MeetingJoinRequest:
        """Create or reuse a pending waiting-room admission request for a profile."""

        if session.lifecycle_state in {MeetingLifecycleState.ENDING, MeetingLifecycleState.ENDED, MeetingLifecycleState.FAILED}:
            raise MeetingDomainError("Cannot join a session that is ending, ended, or failed.")
        if invite_token:
            MeetingInvitationService.validate_invite_token(session=session, token=invite_token)
        elif not session.room.check_passcode(passcode):
            raise MeetingDomainError("Invalid room passcode.")
        existing_participant = session.participants.filter(profile=profile).exclude(status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED]).first()
        if existing_participant:
            raise MeetingDomainError("Profile is already present in the session.")
        join_request = session.join_requests.filter(profile=profile, status=MeetingJoinRequestStatus.PENDING).first()
        if join_request:
            return join_request
        with transaction.atomic():
            if connection:
                connection.session = session
                connection.status = RealtimeConnectionStatus.SUBSCRIBED
                connection.save(update_fields=["session", "status", "updated_at"])
            join_request = MeetingJoinRequest.objects.create(
                room=session.room,
                session=session,
                profile=profile,
                connection=connection,
                requested_display_name=requested_display_name,
                requested_role=requested_role,
                note=note,
                client_state=client_state or {},
            )
            record_session_event(
                session=session,
                event_type=MeetingEventType.JOIN_REQUEST_CREATED,
                actor_profile=profile,
                payload={"join_request_id": str(join_request.pk)},
            )
            MeetingLifecycleService.refresh_session_metrics(session=session)

        def emit_updates() -> None:
            """Broadcast state changes after the database transaction commits."""

            from apps.meetings.realtime.emitter import MeetingSocketEmitter

            MeetingSocketEmitter.emit_session_state(session=session)
            MeetingSocketEmitter.emit_join_request_created(join_request=join_request)

        transaction.on_commit(emit_updates)
        return join_request

    @staticmethod
    def review_join_request(
        *,
        join_request: MeetingJoinRequest,
        reviewer_profile,
        approve: bool,
        reason: str = "",
    ) -> Participant | None:
        """Admit or reject a waiting-room request while enforcing coordinator permissions."""

        MeetingPermissionService.require_session_permission(
            session=join_request.session,
            profile_or_user=reviewer_profile,
            permission_field="can_manage_waiting_room",
        )
        if join_request.status != MeetingJoinRequestStatus.PENDING:
            raise MeetingJoinRequestStateError("Only pending join requests can be reviewed.")
        participant = None
        with transaction.atomic():
            if approve:
                membership = MeetingPermissionService.get_room_membership(room=join_request.room, profile_or_user=join_request.profile)
                participant, created = Participant.objects.get_or_create(
                    room=join_request.room,
                    session=join_request.session,
                    profile=join_request.profile,
                    defaults={
                        "membership": membership,
                        "join_request": join_request,
                        "role": membership.role if membership else join_request.requested_role,
                        "display_name": join_request.requested_display_name or join_request.profile.display_name or join_request.profile.handle,
                    },
                )
                participant.membership = membership
                participant.join_request = join_request
                participant.role = membership.role if membership else participant.role
                participant.display_name = participant.display_name or join_request.profile.display_name or join_request.profile.handle
                participant.apply_membership_defaults()
                participant.status = ParticipantStatus.ADMITTED
                participant.joined_at = participant.joined_at or timezone.now()
                participant.last_seen_at = timezone.now()
                participant.save()
                if join_request.connection:
                    join_request.connection.session = join_request.session
                    join_request.connection.participant = participant
                    join_request.connection.status = RealtimeConnectionStatus.ACTIVE
                    join_request.connection.save(update_fields=["session", "participant", "status", "updated_at"])
                join_request.mark_reviewed(
                    reviewer=reviewer_profile,
                    status=MeetingJoinRequestStatus.ADMITTED,
                    reason=reason,
                )
                join_request.save(update_fields=["status", "reviewed_by_profile", "reviewed_at", "resolution_reason", "updated_at"])
                record_session_event(
                    session=join_request.session,
                    event_type=MeetingEventType.JOIN_REQUEST_REVIEWED,
                    actor_profile=reviewer_profile,
                    actor_participant=participant,
                    payload={"join_request_id": str(join_request.pk), "approved": True, "participant_id": str(participant.pk), "created": created},
                )
                record_session_event(
                    session=join_request.session,
                    event_type=MeetingEventType.PARTICIPANT_JOINED,
                    actor_profile=join_request.profile,
                    actor_participant=participant,
                    payload={"participant_id": str(participant.pk)},
                )
            else:
                join_request.mark_reviewed(
                    reviewer=reviewer_profile,
                    status=MeetingJoinRequestStatus.REJECTED,
                    reason=reason,
                )
                join_request.save(update_fields=["status", "reviewed_by_profile", "reviewed_at", "resolution_reason", "updated_at"])
                record_session_event(
                    session=join_request.session,
                    event_type=MeetingEventType.JOIN_REQUEST_REVIEWED,
                    actor_profile=reviewer_profile,
                    payload={"join_request_id": str(join_request.pk), "approved": False},
                )
            MeetingLifecycleService.refresh_session_metrics(session=join_request.session)

        def emit_updates() -> None:
            """Broadcast post-review state changes and queue Janus work after commit."""

            from apps.meetings.realtime.emitter import MeetingSocketEmitter

            MeetingSocketEmitter.emit_session_state(session=join_request.session)
            MeetingSocketEmitter.emit_join_request_reviewed(join_request=join_request, participant=participant)
            if participant is not None:
                from apps.meetings.tasks import attach_participant_media_handles

                dispatch_task(attach_participant_media_handles, str(participant.pk))

        transaction.on_commit(emit_updates)
        return participant

    @staticmethod
    def update_participant_permissions(
        *,
        session: MeetingSession,
        actor_profile,
        participant: Participant,
        updates: dict[str, Any],
    ) -> Participant:
        """Update participant interaction permissions or moderation flags."""

        permission_sensitive_fields = {"can_publish_audio", "can_publish_video", "can_share_screen", "can_chat", "can_react"}
        media_sensitive_fields = {"is_muted", "is_camera_blocked"}
        requested_fields = set(updates)
        if requested_fields & permission_sensitive_fields:
            MeetingPermissionService.require_session_permission(session=session, profile_or_user=actor_profile, permission_field="can_manage_permissions")
        if requested_fields & media_sensitive_fields:
            MeetingPermissionService.require_session_permission(session=session, profile_or_user=actor_profile, permission_field="can_manage_media")
        allowed_fields = permission_sensitive_fields | media_sensitive_fields | {"raised_hand_at"}
        with transaction.atomic():
            for field_name, value in updates.items():
                if field_name in allowed_fields:
                    setattr(participant, field_name, value)
            participant.last_seen_at = timezone.now()
            participant.save()
            record_session_event(
                session=session,
                event_type=MeetingEventType.PARTICIPANT_UPDATED,
                actor_profile=actor_profile,
                actor_participant=participant,
                payload={"participant_id": str(participant.pk), "updates": updates},
            )
            MeetingLifecycleService.refresh_session_metrics(session=session)

        transaction.on_commit(lambda: __import__("apps.meetings.realtime.emitter", fromlist=["MeetingSocketEmitter"]).MeetingSocketEmitter.emit_session_state(session=session))
        return participant

    @staticmethod
    def remove_participant(*, session: MeetingSession, actor_profile, participant: Participant, reason: str = "") -> Participant:
        """Remove a participant from a live session and schedule downstream cleanup."""

        MeetingPermissionService.require_session_permission(session=session, profile_or_user=actor_profile, permission_field="can_manage_participants")
        with transaction.atomic():
            participant.mark_left(status=ParticipantStatus.REMOVED)
            participant.save()
            participant.connections.filter(
                status__in=[RealtimeConnectionStatus.CONNECTED, RealtimeConnectionStatus.SUBSCRIBED, RealtimeConnectionStatus.ACTIVE],
            ).update(status=RealtimeConnectionStatus.DISCONNECTED, disconnected_at=timezone.now(), updated_at=timezone.now())
            record_session_event(
                session=session,
                event_type=MeetingEventType.PARTICIPANT_REMOVED,
                actor_profile=actor_profile,
                actor_participant=participant,
                payload={"participant_id": str(participant.pk), "reason": reason},
            )
            MeetingLifecycleService.refresh_session_metrics(session=session)

        def emit_updates() -> None:
            """Broadcast participant removal and queue Janus detach work after commit."""

            from apps.meetings.realtime.emitter import MeetingSocketEmitter
            from apps.meetings.tasks import detach_participant_media_handles

            dispatch_task(detach_participant_media_handles, str(participant.pk))
            MeetingSocketEmitter.emit_participant_removed(session=session, participant=participant, reason=reason)
            MeetingSocketEmitter.emit_session_state(session=session)

        transaction.on_commit(emit_updates)
        return participant

    @staticmethod
    def record_chat_message(*, session: MeetingSession, participant: Participant, body: str, metadata: dict[str, Any] | None = None) -> MeetingMessage:
        """Persist a meeting chat message after enforcing participant capabilities."""

        MeetingPermissionService.require_participant_capability(participant=participant, capability_field="can_chat")
        with transaction.atomic():
            message = MeetingMessage.objects.create(session=session, participant=participant, body=body, metadata=metadata or {})
            record_session_event(
                session=session,
                event_type=MeetingEventType.CHAT_MESSAGE_SENT,
                actor_profile=participant.profile,
                actor_participant=participant,
                payload={"message_id": str(message.pk)},
            )
        transaction.on_commit(lambda: __import__("apps.meetings.realtime.emitter", fromlist=["MeetingSocketEmitter"]).MeetingSocketEmitter.emit_chat_message(message=message))
        return message

    @staticmethod
    def record_reaction(
        *,
        session: MeetingSession,
        participant: Participant,
        reaction: str,
        metadata: dict[str, Any] | None = None,
        expires_in_seconds: int | None = 8,
    ) -> MeetingReaction:
        """Persist a participant reaction after enforcing participant capabilities."""

        MeetingPermissionService.require_participant_capability(participant=participant, capability_field="can_react")
        expires_at = timezone.now() + timedelta(seconds=expires_in_seconds) if expires_in_seconds else None
        with transaction.atomic():
            reaction_record = MeetingReaction.objects.create(
                session=session,
                participant=participant,
                reaction=reaction,
                expires_at=expires_at,
                metadata=metadata or {},
            )
            record_session_event(
                session=session,
                event_type=MeetingEventType.REACTION_SENT,
                actor_profile=participant.profile,
                actor_participant=participant,
                payload={"reaction_id": str(reaction_record.pk), "reaction": reaction},
            )
        transaction.on_commit(lambda: __import__("apps.meetings.realtime.emitter", fromlist=["MeetingSocketEmitter"]).MeetingSocketEmitter.emit_reaction(reaction=reaction_record))
        return reaction_record

    @staticmethod
    def bind_connection_to_session(
        *,
        socket_id: str,
        session: MeetingSession,
        profile,
        transport: str,
        user_agent: str = "",
        ip_address: str | None = None,
        client_session_key: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> ParticipantConnection:
        """Associate a Socket.IO connection with a live session and current participant, if any."""

        participant = session.participants.filter(profile=profile).exclude(status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED]).first()
        with transaction.atomic():
            connection, _ = ParticipantConnection.objects.update_or_create(
                socket_id=socket_id,
                defaults={
                    "session": session,
                    "participant": participant,
                    "profile": profile,
                    "transport": transport,
                    "status": RealtimeConnectionStatus.ACTIVE if participant else RealtimeConnectionStatus.SUBSCRIBED,
                    "user_agent": user_agent,
                    "ip_address": ip_address,
                    "client_session_key": client_session_key,
                    "last_heartbeat_at": timezone.now(),
                    "metadata": metadata or {},
                },
            )
            if participant:
                participant.last_seen_at = timezone.now()
                if participant.status in {ParticipantStatus.ADMITTED, ParticipantStatus.DISCONNECTED}:
                    participant.mark_joined()
                participant.save()
            MeetingLifecycleService.refresh_session_metrics(session=session)
            return connection

    @staticmethod
    def mark_connection_heartbeat(*, socket_id: str) -> ParticipantConnection | None:
        """Refresh heartbeat timestamps for an active realtime connection."""

        connection = ParticipantConnection.objects.filter(socket_id=socket_id).select_related("participant").first()
        if not connection:
            return None
        connection.mark_heartbeat()
        connection.save()
        if connection.participant:
            connection.participant.last_seen_at = timezone.now()
            connection.participant.save(update_fields=["last_seen_at", "updated_at"])
        return connection

    @staticmethod
    def mark_connection_disconnected(*, socket_id: str) -> ParticipantConnection | None:
        """Mark a realtime connection as disconnected and degrade participant presence when necessary."""

        connection = ParticipantConnection.objects.select_related("participant", "session").filter(socket_id=socket_id).first()
        if not connection:
            return None
        with transaction.atomic():
            connection.mark_disconnected()
            connection.save()
            if connection.participant:
                still_active = connection.participant.connections.exclude(socket_id=socket_id).filter(
                    status__in=[RealtimeConnectionStatus.CONNECTED, RealtimeConnectionStatus.SUBSCRIBED, RealtimeConnectionStatus.ACTIVE],
                ).exists()
                if not still_active and connection.participant.status not in {ParticipantStatus.LEFT, ParticipantStatus.REMOVED}:
                    connection.participant.status = ParticipantStatus.DISCONNECTED
                    connection.participant.last_seen_at = timezone.now()
                    connection.participant.save(update_fields=["status", "last_seen_at", "updated_at"])
            if connection.session:
                MeetingLifecycleService.refresh_session_metrics(session=connection.session)
        return connection

    @staticmethod
    def refresh_session_metrics(*, session: MeetingSession) -> MeetingSession:
        """Recompute cached session counters and advance the state version."""

        present_participants = session.participants.exclude(status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED]).count()
        active_publishers = ParticipantMediaHandle.objects.filter(
            participant__session=session,
            handle_type=JanusHandleType.PUBLISHER,
            lifecycle_state__in=[
                JanusHandleLifecycleState.ATTACHED,
                JanusHandleLifecycleState.JOINING,
                JanusHandleLifecycleState.READY,
            ],
        ).count()
        session.participant_count = present_participants
        session.active_publisher_count = active_publishers
        session.last_synced_at = timezone.now()
        session.bump_state_version()
        session.save(update_fields=["participant_count", "active_publisher_count", "last_synced_at", "state_version", "updated_at"])
        return session

    @staticmethod
    def end_session(*, session: MeetingSession, actor_profile=None, reason: str = "") -> MeetingSession:
        """Transition a live session to completion and queue Janus room teardown."""

        if actor_profile is not None:
            MeetingPermissionService.require_session_permission(session=session, profile_or_user=actor_profile, permission_field="can_manage_participants")
        with transaction.atomic():
            session.lifecycle_state = MeetingLifecycleState.ENDED
            session.ended_at = timezone.now()
            session.save(update_fields=["lifecycle_state", "ended_at", "updated_at"])
            record_session_event(
                session=session,
                event_type=MeetingEventType.SESSION_ENDED,
                actor_profile=actor_profile,
                payload={"reason": reason},
            )
            MeetingLifecycleService.refresh_session_metrics(session=session)

        def emit_updates() -> None:
            """Broadcast session completion and queue Janus room teardown after commit."""

            from apps.meetings.realtime.emitter import MeetingSocketEmitter
            from apps.meetings.tasks import destroy_janus_room_for_session

            dispatch_task(destroy_janus_room_for_session, str(session.pk))
            MeetingSocketEmitter.emit_session_state(session=session)

        transaction.on_commit(emit_updates)
        return session
