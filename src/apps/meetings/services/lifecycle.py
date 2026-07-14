"""High-level orchestration for room creation, admission, moderation, and cleanup."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from django.db import transaction
from django.db.models import F
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from apps.meetings.exceptions import MeetingDomainError, MeetingJoinRequestStateError
from apps.meetings.models import (
    JanusHandleLifecycleState,
    JanusHandleType,
    MediaDirection,
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

ACTIVE_CONNECTION_STATUSES = [
    RealtimeConnectionStatus.CONNECTED,
    RealtimeConnectionStatus.SUBSCRIBED,
    RealtimeConnectionStatus.ACTIVE,
]
ACTIVE_PARTICIPANT_CONNECTION_STATUSES = [RealtimeConnectionStatus.ACTIVE]


@dataclass(frozen=True)
class MeetingAdmissionResult:
    """Represent the outcome of an attempt to enter a meeting session."""

    status: str
    participant: Participant | None = None
    join_request: MeetingJoinRequest | None = None
    direct_entry: bool = False


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


def dispatch_task(task, *args) -> Any | None:
    """Attempt to enqueue a Celery task without breaking the request path on broker errors."""

    try:
        return task.delay(*args)
    except Exception:
        logger.exception(
            "Unable to enqueue Celery task '%s'; make sure the broker is running.",
            getattr(task, "name", repr(task)),
        )
        return None


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

        with transaction.atomic():
            # Serialize starts for the same room so concurrent requests cannot
            # create multiple live sessions.
            room = MeetingRoom.objects.select_for_update().get(pk=room.pk)
            existing_session = room.sessions.live().order_by("-created_at").first()
            if existing_session:
                if (
                    existing_session.lifecycle_state
                    == MeetingLifecycleState.PROVISIONING
                    or not existing_session.janus_room_id
                ):
                    from apps.meetings.tasks import provision_janus_room_for_session

                    transaction.on_commit(
                        lambda: dispatch_task(
                            provision_janus_room_for_session,
                            str(existing_session.pk),
                        )
                    )
                return existing_session
            membership = MeetingPermissionService.get_room_membership(room=room, profile_or_user=started_by_profile)
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
        client_session_key: str = "",
        passcode: str | None = None,
        invite_token: str | None = None,
    ) -> MeetingJoinRequest:
        """Create or reuse a pending waiting-room request for a profile that needs review."""

        MeetingLifecycleService._validate_session_joinable(session=session)
        MeetingLifecycleService._validate_profile_not_removed(
            session=session,
            profile=profile,
        )
        MeetingLifecycleService._validate_requested_role(requested_role)
        membership = MeetingPermissionService.get_room_membership(room=session.room, profile_or_user=profile)
        if (
            MeetingLifecycleService._get_present_participant(session=session, profile=profile) is not None
            or MeetingLifecycleService._can_enter_directly(session=session, profile=profile, membership=membership)
        ):
            raise MeetingDomainError("Profile can enter this session directly without a join request.")
        MeetingLifecycleService._validate_join_gate(session=session, passcode=passcode, invite_token=invite_token)
        bound_connection = MeetingLifecycleService._resolve_active_connection(
            session=session,
            profile=profile,
            connection=connection,
            client_session_key=client_session_key,
        )
        join_request = session.join_requests.filter(profile=profile, status=MeetingJoinRequestStatus.PENDING).first()
        if join_request:
            if bound_connection and join_request.connection_id != bound_connection.pk:
                join_request.connection = bound_connection
                join_request.save(update_fields=["connection", "updated_at"])
            return join_request
        with transaction.atomic():
            if bound_connection:
                bound_connection.session = session
                bound_connection.status = RealtimeConnectionStatus.SUBSCRIBED
                if client_session_key and not bound_connection.client_session_key:
                    bound_connection.client_session_key = client_session_key
                    bound_connection.save(update_fields=["session", "status", "client_session_key", "updated_at"])
                else:
                    bound_connection.save(update_fields=["session", "status", "updated_at"])
            join_request = MeetingJoinRequest.objects.create(
                room=session.room,
                session=session,
                profile=profile,
                connection=bound_connection,
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
    def request_admission(
        *,
        session: MeetingSession,
        profile,
        requested_display_name: str = "",
        requested_role: str = MeetingRole.PARTICIPANT,
        note: str = "",
        client_state: dict[str, Any] | None = None,
        connection: ParticipantConnection | None = None,
        client_session_key: str = "",
        passcode: str | None = None,
        invite_token: str | None = None,
    ) -> MeetingAdmissionResult:
        """Admit direct-entry profiles or create a waiting-room request when review is required."""

        MeetingLifecycleService._validate_session_joinable(session=session)
        MeetingLifecycleService._validate_profile_not_removed(
            session=session,
            profile=profile,
        )
        MeetingLifecycleService._validate_requested_role(requested_role)
        membership = MeetingPermissionService.get_room_membership(room=session.room, profile_or_user=profile)
        bound_connection = MeetingLifecycleService._resolve_active_connection(
            session=session,
            profile=profile,
            connection=connection,
            client_session_key=client_session_key,
        )
        existing_participant = MeetingLifecycleService._get_present_participant(session=session, profile=profile)
        durable_direct_entry = MeetingLifecycleService._has_durable_direct_entry(
            room=session.room,
            profile=profile,
            membership=membership,
        )
        policy_direct_entry = MeetingLifecycleService._policy_allows_direct_entry(session=session)

        if existing_participant or durable_direct_entry or policy_direct_entry:
            if not existing_participant and not durable_direct_entry:
                MeetingLifecycleService._validate_join_gate(session=session, passcode=passcode, invite_token=invite_token)
            participant = MeetingLifecycleService._admit_profile_directly(
                session=session,
                profile=profile,
                membership=membership,
                existing_participant=existing_participant,
                requested_display_name=requested_display_name,
                requested_role=requested_role,
                connection=bound_connection,
                client_session_key=client_session_key,
            )
            return MeetingAdmissionResult(status="admitted", participant=participant, direct_entry=True)

        join_request = MeetingLifecycleService.request_join(
            session=session,
            profile=profile,
            requested_display_name=requested_display_name,
            requested_role=requested_role,
            note=note,
            client_state=client_state or {},
            connection=bound_connection,
            client_session_key=client_session_key,
            passcode=passcode,
            invite_token=invite_token,
        )
        return MeetingAdmissionResult(status="waiting", join_request=join_request, direct_entry=False)

    @staticmethod
    def _validate_session_joinable(*, session: MeetingSession) -> None:
        """Reject attempts to enter a session that can no longer accept participants."""

        if session.lifecycle_state in {MeetingLifecycleState.ENDING, MeetingLifecycleState.ENDED, MeetingLifecycleState.FAILED}:
            raise MeetingDomainError("Cannot join a session that is ending, ended, or failed.")

    @staticmethod
    def _validate_profile_not_removed(*, session: MeetingSession, profile) -> None:
        """Keep a moderator removal in force for the lifetime of the session."""

        if session.participants.filter(
            profile=profile,
            status=ParticipantStatus.REMOVED,
        ).exists():
            raise MeetingDomainError(
                "This participant was removed from the session and cannot rejoin."
            )

    @staticmethod
    def _validate_requested_role(requested_role: str) -> None:
        """Only coordinators may derive elevated roles from durable membership."""

        if requested_role != MeetingRole.PARTICIPANT:
            raise MeetingDomainError("Unsupported requested participant role.")

    @staticmethod
    def _validate_join_gate(*, session: MeetingSession, passcode: str | None = None, invite_token: str | None = None) -> None:
        """Validate passcode or invite-token requirements before a non-member can proceed."""

        if invite_token:
            MeetingInvitationService.validate_invite_token(session=session, token=invite_token)
            return
        if session.room.access_policy == MeetingAccessPolicy.INVITE_ONLY:
            raise MeetingDomainError("A valid meeting invitation is required for this room.")
        if not session.room.check_passcode(passcode):
            raise MeetingDomainError("Invalid room passcode.")

    @staticmethod
    def _validate_session_capacity(*, session: MeetingSession) -> None:
        """Reject creation of another present participant once the room is full.

        Callers hold a ``select_for_update`` lock on the session row so capacity
        checks and participant creation remain serialized on databases that
        support row-level locks.
        """

        present_count = session.participants.exclude(
            status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED],
        ).count()
        if present_count >= session.room.max_participants:
            raise MeetingDomainError("Meeting has reached its maximum participant capacity.")

    @staticmethod
    def _get_present_participant(*, session: MeetingSession, profile) -> Participant | None:
        """Return an already-admitted participant record for the profile, if one exists."""

        return (
            session.participants.filter(profile=profile)
            .exclude(status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED])
            .first()
        )

    @staticmethod
    def _can_enter_directly(*, session: MeetingSession, profile, membership: MeetingRoomMembership | None) -> bool:
        """Return whether a profile should bypass waiting-room review for this session."""

        return MeetingLifecycleService._has_durable_direct_entry(
            room=session.room,
            profile=profile,
            membership=membership,
        ) or MeetingLifecycleService._policy_allows_direct_entry(session=session)

    @staticmethod
    def _has_durable_direct_entry(*, room: MeetingRoom, profile, membership: MeetingRoomMembership | None) -> bool:
        """Return whether room ownership or active membership grants direct entry."""

        if profile is not None and str(room.created_by_profile_id) == str(profile.pk):
            return True
        if membership is None:
            return False
        if membership.role in {MeetingRole.HOST, MeetingRole.CO_HOST}:
            return True
        if membership.can_manage_waiting_room:
            return True
        return bool(membership.is_active)

    @staticmethod
    def _policy_allows_direct_entry(*, session: MeetingSession) -> bool:
        """Return whether room policy allows non-members to enter without review."""

        return not session.room.is_waiting_room_enabled or session.room.access_policy == MeetingAccessPolicy.OPEN

    @staticmethod
    def _resolve_active_connection(
        *,
        session: MeetingSession,
        profile,
        connection: ParticipantConnection | None = None,
        client_session_key: str = "",
    ) -> ParticipantConnection | None:
        """Find the current subscribed socket connection for a profile, when one exists."""

        normalized_key = (client_session_key or "").strip()
        if (
            connection is not None
            and str(connection.session_id) == str(session.pk)
            and str(connection.profile_id) == str(profile.pk)
            and connection.status in ACTIVE_CONNECTION_STATUSES
            and (not normalized_key or connection.client_session_key == normalized_key)
        ):
            return connection

        queryset = ParticipantConnection.objects.filter(
            session=session,
            profile=profile,
            status__in=ACTIVE_CONNECTION_STATUSES,
        )
        if normalized_key:
            keyed_connection = queryset.filter(client_session_key=normalized_key).order_by("-last_heartbeat_at", "-connected_at").first()
            if keyed_connection is not None:
                return keyed_connection
        return queryset.order_by("-last_heartbeat_at", "-connected_at").first()

    @staticmethod
    def _disconnect_duplicate_connections(
        *,
        session: MeetingSession,
        profile,
        active_connection: ParticipantConnection | None,
        client_session_key: str = "",
        now=None,
    ) -> None:
        """Disconnect older rows that represent the same browser meeting attempt."""

        if active_connection is None:
            return
        timestamp = now or timezone.now()
        ParticipantConnection.objects.filter(
            session=session,
            profile=profile,
            status__in=ACTIVE_CONNECTION_STATUSES,
        ).exclude(pk=active_connection.pk).update(
            status=RealtimeConnectionStatus.DISCONNECTED,
            disconnected_at=timestamp,
            updated_at=timestamp,
        )

    @staticmethod
    def _admit_profile_directly(
        *,
        session: MeetingSession,
        profile,
        membership: MeetingRoomMembership | None,
        existing_participant: Participant | None,
        requested_display_name: str,
        requested_role: str,
        connection: ParticipantConnection | None,
        client_session_key: str = "",
    ) -> Participant:
        """Create or activate one participant for a profile that does not require review."""

        now = timezone.now()
        participant = existing_participant
        should_attach_media_handles = False
        with transaction.atomic():
            locked_session = MeetingSession.objects.select_for_update().select_related("room").get(pk=session.pk)
            if participant is None:
                participant = (
                    locked_session.participants.filter(profile=profile)
                    .exclude(status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED])
                    .first()
                )
            if participant is None:
                MeetingLifecycleService._validate_session_capacity(session=locked_session)
            bound_connection = MeetingLifecycleService._resolve_active_connection(
                session=session,
                profile=profile,
                connection=connection,
                client_session_key=client_session_key,
            )
            session.join_requests.filter(profile=profile, status=MeetingJoinRequestStatus.PENDING).update(
                status=MeetingJoinRequestStatus.CANCELLED,
                reviewed_by_profile_id=profile.pk,
                reviewed_at=now,
                resolution_reason="Profile can enter directly without waiting-room review.",
                updated_at=now,
            )

            if participant is None:
                participant, _ = Participant.objects.get_or_create(
                    room=session.room,
                    session=session,
                    profile=profile,
                    defaults={
                        "membership": membership,
                        "role": membership.role if membership else requested_role,
                        "display_name": requested_display_name or profile.display_name or profile.handle,
                    },
                )
            if participant.status == ParticipantStatus.REMOVED:
                raise MeetingDomainError(
                    "This participant was removed from the session and cannot rejoin."
                )
            participant.membership = membership or participant.membership
            participant.role = membership.role if membership else participant.role
            participant.display_name = requested_display_name or participant.display_name or profile.display_name or profile.handle
            participant.apply_membership_defaults()
            participant.left_at = None
            participant_was_active = participant.status == ParticipantStatus.ACTIVE and participant.joined_at is not None
            if bound_connection:
                participant.mark_joined()
                should_attach_media_handles = True
            else:
                participant.status = ParticipantStatus.ADMITTED
                participant.last_seen_at = now
            participant.save()

            if bound_connection:
                bound_connection.session = session
                bound_connection.profile = profile
                bound_connection.participant = participant
                bound_connection.status = RealtimeConnectionStatus.ACTIVE
                bound_connection.disconnected_at = None
                if client_session_key and not bound_connection.client_session_key:
                    bound_connection.client_session_key = client_session_key
                bound_connection.last_heartbeat_at = now
                bound_connection.save(
                    update_fields=[
                        "session",
                        "profile",
                        "participant",
                        "status",
                        "disconnected_at",
                        "client_session_key",
                        "last_heartbeat_at",
                        "updated_at",
                    ]
                )
                MeetingLifecycleService._disconnect_duplicate_connections(
                    session=session,
                    profile=profile,
                    active_connection=bound_connection,
                    client_session_key=bound_connection.client_session_key,
                    now=now,
                )
                if not participant_was_active:
                    record_session_event(
                        session=session,
                        event_type=MeetingEventType.PARTICIPANT_JOINED,
                        actor_profile=profile,
                        actor_participant=participant,
                        payload={"participant_id": str(participant.pk), "direct_entry": True},
                    )

            MeetingLifecycleService.refresh_session_metrics(session=session)

        def emit_updates() -> None:
            """Broadcast direct admission and queue media-handle attachment after commit."""

            from apps.meetings.realtime.emitter import MeetingSocketEmitter

            if should_attach_media_handles:
                from apps.meetings.tasks import attach_participant_media_handles

                dispatch_task(attach_participant_media_handles, str(participant.pk))
            MeetingSocketEmitter.emit_session_state(session=session)

        transaction.on_commit(emit_updates)
        return participant

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
        original_join_request = join_request
        participant = None
        participant_became_active = False
        should_attach_media_handles = False
        with transaction.atomic():
            join_request = (
                MeetingJoinRequest.objects.select_for_update()
                .select_related("session", "session__room", "room", "profile", "connection")
                .get(pk=join_request.pk)
            )
            if join_request.status != MeetingJoinRequestStatus.PENDING:
                raise MeetingJoinRequestStateError("Only pending join requests can be reviewed.")
            locked_session = (
                MeetingSession.objects.select_for_update()
                .select_related("room")
                .get(pk=join_request.session_id)
            )
            active_request_connection = MeetingLifecycleService._resolve_active_connection(
                session=join_request.session,
                profile=join_request.profile,
                connection=join_request.connection,
                client_session_key=join_request.connection.client_session_key if join_request.connection_id else "",
            )
            if approve:
                membership = MeetingPermissionService.get_room_membership(room=join_request.room, profile_or_user=join_request.profile)
                participant = Participant.objects.filter(
                    session=join_request.session,
                    profile=join_request.profile,
                ).first()
                if participant and participant.status == ParticipantStatus.REMOVED:
                    raise MeetingDomainError(
                        "This participant was removed from the session and cannot rejoin."
                    )
                MeetingLifecycleService._validate_requested_role(
                    join_request.requested_role
                )
                participant_was_present = bool(
                    participant
                    and participant.status not in {ParticipantStatus.LEFT, ParticipantStatus.REMOVED}
                )
                if not participant_was_present:
                    MeetingLifecycleService._validate_session_capacity(session=locked_session)
                created = participant is None
                if participant is None:
                    participant = Participant.objects.create(
                        room=join_request.room,
                        session=join_request.session,
                        profile=join_request.profile,
                        membership=membership,
                        join_request=join_request,
                        role=membership.role if membership else join_request.requested_role,
                        display_name=join_request.requested_display_name or join_request.profile.display_name or join_request.profile.handle,
                    )
                participant.membership = membership
                participant.join_request = join_request
                participant.role = membership.role if membership else participant.role
                participant.display_name = participant.display_name or join_request.profile.display_name or join_request.profile.handle
                participant.apply_membership_defaults()
                participant.left_at = None
                if active_request_connection:
                    was_active = participant.status == ParticipantStatus.ACTIVE and participant.joined_at is not None
                    participant.mark_joined()
                    participant_became_active = not was_active
                    should_attach_media_handles = True
                else:
                    participant.status = ParticipantStatus.ADMITTED
                    participant.last_seen_at = timezone.now()
                participant.save()
                if active_request_connection:
                    active_request_connection.session = join_request.session
                    active_request_connection.profile = join_request.profile
                    active_request_connection.participant = participant
                    active_request_connection.status = RealtimeConnectionStatus.ACTIVE
                    active_request_connection.disconnected_at = None
                    active_request_connection.last_heartbeat_at = timezone.now()
                    active_request_connection.save(
                        update_fields=[
                            "session",
                            "profile",
                            "participant",
                            "status",
                            "disconnected_at",
                            "last_heartbeat_at",
                            "updated_at",
                        ]
                    )
                    MeetingLifecycleService._disconnect_duplicate_connections(
                        session=join_request.session,
                        profile=join_request.profile,
                        active_connection=active_request_connection,
                        client_session_key=active_request_connection.client_session_key,
                    )
                    join_request.connection = active_request_connection
                join_request.mark_reviewed(
                    reviewer=reviewer_profile,
                    status=MeetingJoinRequestStatus.ADMITTED,
                    reason=reason,
                )
                join_request.save(update_fields=["connection", "status", "reviewed_by_profile", "reviewed_at", "resolution_reason", "updated_at"])
                record_session_event(
                    session=join_request.session,
                    event_type=MeetingEventType.JOIN_REQUEST_REVIEWED,
                    actor_profile=reviewer_profile,
                    actor_participant=participant,
                    payload={"join_request_id": str(join_request.pk), "approved": True, "participant_id": str(participant.pk), "created": created},
                )
                if participant_became_active:
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

        if original_join_request is not join_request:
            original_join_request.refresh_from_db()

        def emit_updates() -> None:
            """Broadcast post-review state changes and queue Janus work after commit."""

            from apps.meetings.realtime.emitter import MeetingSocketEmitter

            MeetingSocketEmitter.emit_session_state(session=join_request.session)
            MeetingSocketEmitter.emit_join_request_reviewed(join_request=join_request, participant=participant)
            if should_attach_media_handles and participant is not None:
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
        allowed_fields = permission_sensitive_fields | media_sensitive_fields | {"raised_hand_at"}
        requested_fields = set(updates)
        unknown_fields = sorted(requested_fields - allowed_fields)
        if unknown_fields:
            raise MeetingDomainError(
                f"Unsupported participant update field(s): {', '.join(unknown_fields)}."
            )
        normalized_updates = dict(updates)
        for field_name in (permission_sensitive_fields | media_sensitive_fields) & requested_fields:
            if not isinstance(normalized_updates[field_name], bool):
                raise MeetingDomainError(f"Participant update '{field_name}' must be a boolean.")
        if "raised_hand_at" in normalized_updates:
            raised_hand_at = normalized_updates["raised_hand_at"]
            if isinstance(raised_hand_at, str):
                parsed_raised_hand_at = parse_datetime(raised_hand_at)
                if parsed_raised_hand_at is None:
                    raise MeetingDomainError("Participant update 'raised_hand_at' must be an ISO datetime or null.")
                raised_hand_at = parsed_raised_hand_at
            if raised_hand_at is not None and not isinstance(raised_hand_at, datetime):
                raise MeetingDomainError("Participant update 'raised_hand_at' must be an ISO datetime or null.")
            if raised_hand_at is not None and timezone.is_naive(raised_hand_at):
                raised_hand_at = timezone.make_aware(raised_hand_at)
            normalized_updates["raised_hand_at"] = raised_hand_at
        if requested_fields & permission_sensitive_fields:
            MeetingPermissionService.require_session_permission(session=session, profile_or_user=actor_profile, permission_field="can_manage_permissions")
        if requested_fields & media_sensitive_fields:
            MeetingPermissionService.require_session_permission(session=session, profile_or_user=actor_profile, permission_field="can_manage_media")
        if "raised_hand_at" in requested_fields and str(participant.profile_id) != str(actor_profile.pk):
            MeetingPermissionService.require_session_permission(
                session=session,
                profile_or_user=actor_profile,
                permission_field="can_manage_participants",
            )
        with transaction.atomic():
            for field_name, value in normalized_updates.items():
                setattr(participant, field_name, value)
            participant.last_seen_at = timezone.now()
            participant.save()
            event_updates = {
                field_name: value.isoformat() if isinstance(value, datetime) else value
                for field_name, value in normalized_updates.items()
            }
            record_session_event(
                session=session,
                event_type=MeetingEventType.PARTICIPANT_UPDATED,
                actor_profile=actor_profile,
                actor_participant=participant,
                payload={"participant_id": str(participant.pk), "updates": event_updates},
            )
            MeetingLifecycleService.refresh_session_metrics(session=session)

        def emit_updates() -> None:
            from apps.meetings.realtime.emitter import MeetingSocketEmitter

            if requested_fields & (permission_sensitive_fields | media_sensitive_fields):
                from apps.meetings.services.signaling import MeetingMediaSignalService

                try:
                    MeetingMediaSignalService.apply_moderation(
                        participant=participant,
                    )
                except Exception:
                    logger.exception(
                        "Unable to apply Janus moderation for participant '%s'.",
                        participant.pk,
                    )
            MeetingSocketEmitter.emit_session_state(session=session)

        transaction.on_commit(emit_updates)
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
            from apps.meetings.services.signaling import MeetingMediaSignalService
            from apps.meetings.tasks import detach_participant_media_handles

            try:
                MeetingMediaSignalService.detach_participant_handles(
                    participant=participant,
                )
            except Exception:
                logger.exception(
                    "Unable to detach media while removing participant '%s'.",
                    participant.pk,
                )
            dispatch_task(detach_participant_media_handles, str(participant.pk))
            MeetingSocketEmitter.emit_participant_removed(session=session, participant=participant, reason=reason)
            MeetingSocketEmitter.emit_session_state(session=session)

        transaction.on_commit(emit_updates)
        return participant

    @staticmethod
    def leave_participant(*, session: MeetingSession, profile, socket_id: str | None = None, reason: str = "") -> Participant | None:
        """Mark the current profile as having explicitly left a live session."""

        connection = None
        if socket_id:
            connection = (
                ParticipantConnection.objects.select_related("participant", "session")
                .filter(socket_id=socket_id, session=session, profile=profile)
                .first()
            )
        participant = connection.participant if connection and connection.participant else None
        if participant is None:
            participant = (
                session.participants.filter(profile=profile)
                .exclude(status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED])
                .first()
            )

        now = timezone.now()
        with transaction.atomic():
            if participant and participant.status not in {ParticipantStatus.LEFT, ParticipantStatus.REMOVED}:
                participant.mark_left(status=ParticipantStatus.LEFT)
                participant.save()
                participant.connections.filter(session=session, status__in=ACTIVE_CONNECTION_STATUSES).update(
                    status=RealtimeConnectionStatus.DISCONNECTED,
                    disconnected_at=now,
                    updated_at=now,
                )
                record_session_event(
                    session=session,
                    event_type=MeetingEventType.PARTICIPANT_LEFT,
                    actor_profile=profile,
                    actor_participant=participant,
                    payload={"participant_id": str(participant.pk), "reason": reason},
                )
            elif connection:
                connection.session = None
                connection.participant = None
                connection.status = RealtimeConnectionStatus.CONNECTED
                connection.disconnected_at = None
                connection.save(update_fields=["session", "participant", "status", "disconnected_at", "updated_at"])
            MeetingLifecycleService.refresh_session_metrics(session=session)

        def emit_updates() -> None:
            """Broadcast explicit departure and detach media resources after commit."""

            from apps.meetings.realtime.emitter import MeetingSocketEmitter

            if participant is not None:
                from apps.meetings.services.signaling import MeetingMediaSignalService
                from apps.meetings.tasks import detach_participant_media_handles

                try:
                    MeetingMediaSignalService.detach_participant_handles(
                        participant=participant,
                    )
                except Exception:
                    logger.exception(
                        "Unable to detach media while participant '%s' leaves.",
                        participant.pk,
                    )
                dispatch_task(detach_participant_media_handles, str(participant.pk))
            MeetingSocketEmitter.emit_session_state(session=session)

        transaction.on_commit(emit_updates)
        return participant

    @staticmethod
    def record_chat_message(*, session: MeetingSession, participant: Participant, body: str, metadata: dict[str, Any] | None = None) -> MeetingMessage:
        """Persist a meeting chat message after enforcing participant capabilities."""

        MeetingLifecycleService._validate_session_joinable(session=session)
        MeetingPermissionService.require_participant_capability(participant=participant, capability_field="can_chat")
        normalized_body = str(body).strip()
        if not normalized_body:
            raise MeetingDomainError("A chat message cannot be empty.")
        if len(normalized_body) > 4_000:
            raise MeetingDomainError("A chat message cannot exceed 4000 characters.")
        with transaction.atomic():
            message = MeetingMessage.objects.create(
                session=session,
                participant=participant,
                body=normalized_body,
                metadata=metadata or {},
            )
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

        MeetingLifecycleService._validate_session_joinable(session=session)
        MeetingPermissionService.require_participant_capability(participant=participant, capability_field="can_react")
        normalized_reaction = str(reaction).strip()
        if not normalized_reaction:
            raise MeetingDomainError("A reaction cannot be empty.")
        if len(normalized_reaction) > 64:
            raise MeetingDomainError("A reaction cannot exceed 64 characters.")
        if expires_in_seconds is not None and not 1 <= expires_in_seconds <= 60:
            raise MeetingDomainError("Reaction expiry must be between 1 and 60 seconds.")
        expires_at = timezone.now() + timedelta(seconds=expires_in_seconds) if expires_in_seconds else None
        with transaction.atomic():
            reaction_record = MeetingReaction.objects.create(
                session=session,
                participant=participant,
                reaction=normalized_reaction,
                expires_at=expires_at,
                metadata=metadata or {},
            )
            record_session_event(
                session=session,
                event_type=MeetingEventType.REACTION_SENT,
                actor_profile=participant.profile,
                actor_participant=participant,
                payload={"reaction_id": str(reaction_record.pk), "reaction": normalized_reaction},
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

        participant = session.participants.filter(
            profile=profile,
            status__in=[
                ParticipantStatus.ADMITTED,
                ParticipantStatus.ACTIVE,
                ParticipantStatus.DISCONNECTED,
            ],
        ).first()
        now = timezone.now()
        should_attach_media_handles = False
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
                    "disconnected_at": None,
                    "last_heartbeat_at": now,
                    "metadata": metadata or {},
                },
            )
            MeetingLifecycleService._disconnect_duplicate_connections(
                session=session,
                profile=profile,
                active_connection=connection,
                client_session_key=client_session_key,
                now=now,
            )
            if participant:
                participant.last_seen_at = now
                if participant.status in {ParticipantStatus.ADMITTED, ParticipantStatus.DISCONNECTED}:
                    participant.mark_joined()
                    should_attach_media_handles = True
                    record_session_event(
                        session=session,
                        event_type=MeetingEventType.PARTICIPANT_JOINED,
                        actor_profile=profile,
                        actor_participant=participant,
                        payload={"participant_id": str(participant.pk)},
                    )
                participant.save()
                MeetingLifecycleService._activate_session_for_live_connection(
                    session=session,
                    now=now,
                )
            MeetingLifecycleService.refresh_session_metrics(session=session)

        def emit_updates() -> None:
            """Broadcast entry state and prepare media handles after the connection is bound."""

            from apps.meetings.realtime.emitter import MeetingSocketEmitter

            if should_attach_media_handles and participant is not None:
                from apps.meetings.tasks import attach_participant_media_handles

                dispatch_task(attach_participant_media_handles, str(participant.pk))
            MeetingSocketEmitter.emit_session_state(
                session=session,
                exclude_socket_ids={socket_id},
            )

        transaction.on_commit(emit_updates)
        return connection

    @staticmethod
    def _activate_session_for_live_connection(*, session: MeetingSession, now=None) -> None:
        """Move a provisioned session to active once a participant socket is live."""

        if session.lifecycle_state != MeetingLifecycleState.WAITING:
            return
        timestamp = now or timezone.now()
        updated = MeetingSession.objects.filter(
            pk=session.pk,
            lifecycle_state=MeetingLifecycleState.WAITING,
        ).update(
            lifecycle_state=MeetingLifecycleState.ACTIVE,
            updated_at=timestamp,
        )
        if updated:
            session.lifecycle_state = MeetingLifecycleState.ACTIVE

    @staticmethod
    def mark_connection_heartbeat(*, socket_id: str) -> ParticipantConnection | None:
        """Refresh heartbeat timestamps for an active realtime connection."""

        connection = ParticipantConnection.objects.filter(socket_id=socket_id).select_related("participant").first()
        if not connection:
            return None
        connection.mark_heartbeat()
        connection.save()
        if connection.participant:
            if (
                connection.status == RealtimeConnectionStatus.ACTIVE
                and connection.participant.status == ParticipantStatus.DISCONNECTED
            ):
                connection.participant.mark_joined()
                connection.participant.save(
                    update_fields=[
                        "status",
                        "joined_at",
                        "last_seen_at",
                        "updated_at",
                    ]
                )
            else:
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
                    status__in=ACTIVE_CONNECTION_STATUSES,
                ).exists()
                if not still_active and connection.participant.status not in {ParticipantStatus.LEFT, ParticipantStatus.REMOVED}:
                    connection.participant.status = ParticipantStatus.DISCONNECTED
                    connection.participant.last_seen_at = timezone.now()
                    connection.participant.save(update_fields=["status", "last_seen_at", "updated_at"])
            if connection.session:
                MeetingLifecycleService.refresh_session_metrics(session=connection.session)
        if connection.session:
            transaction.on_commit(
                lambda: __import__(
                    "apps.meetings.realtime.emitter",
                    fromlist=["MeetingSocketEmitter"],
                ).MeetingSocketEmitter.emit_session_state(session=connection.session)
            )
        return connection

    @staticmethod
    def refresh_session_metrics(*, session: MeetingSession) -> MeetingSession:
        """Recompute cached session counters and advance the state version."""

        active_participants = session.participants.filter(status=ParticipantStatus.ACTIVE).count()
        active_publishers = ParticipantMediaHandle.objects.filter(
            participant__session=session,
            handle_type=JanusHandleType.PUBLISHER,
            lifecycle_state=JanusHandleLifecycleState.READY,
            streams__direction=MediaDirection.OUTBOUND,
            streams__is_active=True,
        ).distinct().count()
        now = timezone.now()
        MeetingSession.objects.filter(pk=session.pk).update(
            participant_count=active_participants,
            active_publisher_count=active_publishers,
            last_synced_at=now,
            state_version=F("state_version") + 1,
            updated_at=now,
        )
        session.refresh_from_db(
            fields=[
                "participant_count",
                "active_publisher_count",
                "last_synced_at",
                "state_version",
                "updated_at",
            ]
        )
        return session

    @staticmethod
    def end_session(*, session: MeetingSession, actor_profile=None, reason: str = "") -> MeetingSession:
        """Transition a live session to completion and queue Janus room teardown."""

        if actor_profile is not None:
            MeetingPermissionService.require_session_permission(session=session, profile_or_user=actor_profile, permission_field="can_manage_participants")
        with transaction.atomic():
            session = MeetingSession.objects.select_for_update().select_related("room").get(pk=session.pk)
            if session.lifecycle_state == MeetingLifecycleState.ENDED:
                return session
            now = timezone.now()
            session.lifecycle_state = MeetingLifecycleState.ENDED
            session.ended_at = now
            session.save(update_fields=["lifecycle_state", "ended_at", "updated_at"])
            session.participants.exclude(
                status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED]
            ).update(status=ParticipantStatus.LEFT, left_at=now, updated_at=now)
            session.join_requests.filter(
                status=MeetingJoinRequestStatus.PENDING
            ).update(
                status=MeetingJoinRequestStatus.CANCELLED,
                reviewed_at=now,
                resolution_reason=reason or "The meeting ended.",
                updated_at=now,
            )
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
            MeetingSocketEmitter.emit_session_ended(session=session, reason=reason)
            MeetingSocketEmitter.emit_session_state(session=session)

        transaction.on_commit(emit_updates)
        return session
