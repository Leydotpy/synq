"""High-level orchestration for room creation, admission, moderation, and cleanup."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from django.db import transaction
from django.utils import timezone
from kombu.exceptions import OperationalError as BrokerOperationalError

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

ACTIVE_CONNECTION_STATUSES = [
    RealtimeConnectionStatus.CONNECTED,
    RealtimeConnectionStatus.SUBSCRIBED,
    RealtimeConnectionStatus.ACTIVE,
]


@dataclass(frozen=True)
class MeetingAdmissionResult:
    """Represent the server's direct-entry or waiting-room decision."""

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


def dispatch_task(task, *args):
    """Attempt to enqueue a Celery task without breaking the request path on broker errors."""

    try:
        return task.delay(*args)
    except BrokerOperationalError as exc:
        logger.warning(
            "Unable to enqueue Celery task '%s': broker unavailable (%s).",
            getattr(task, "name", repr(task)),
            exc,
        )
        return None


def require_json_object(value, *, field_name: str) -> dict[str, Any]:
    """Normalize an optional mapping or raise a stable domain error."""

    if value is None:
        return {}
    if not isinstance(value, dict):
        raise MeetingDomainError(f"{field_name} must be a JSON object.")
    return value


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

        janus_room_configuration = require_json_object(
            janus_room_configuration,
            field_name="janus_room_configuration",
        )
        feature_flags = require_json_object(feature_flags, field_name="feature_flags")
        metadata = require_json_object(metadata, field_name="metadata")
        if (
            scheduled_start_at
            and scheduled_end_at
            and scheduled_end_at < scheduled_start_at
        ):
            raise MeetingDomainError(
                "scheduled_end_at must be greater than or equal to scheduled_start_at.",
            )
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
                janus_room_configuration=janus_room_configuration,
                feature_flags=feature_flags,
                metadata=metadata,
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

        metadata = require_json_object(metadata, field_name="metadata")
        membership = MeetingPermissionService.get_room_membership(room=room, profile_or_user=started_by_profile)
        with transaction.atomic():
            locked_room = MeetingRoom.objects.select_for_update().get(pk=room.pk)
            session = locked_room.sessions.live().order_by("-created_at").first()
            created = session is None
            if created:
                session = MeetingSession.objects.create(
                    room=locked_room,
                    started_by_profile=started_by_profile,
                    lifecycle_state=MeetingLifecycleState.PROVISIONING,
                    started_at=timezone.now(),
                    janus_room_secret=generate_short_code(16),
                    janus_room_pin=generate_short_code(8),
                    metadata=metadata,
                )
                participant = Participant(
                    room=locked_room,
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
                    payload={"room_id": str(locked_room.pk)},
                )
                MeetingLifecycleService.refresh_session_metrics(session=session)
            else:
                participant = session.participants.filter(profile=started_by_profile).first()

        def enqueue_follow_up_tasks() -> None:
            """Queue asynchronous Janus work once the transaction commits successfully."""

            from apps.meetings.tasks import attach_participant_media_handles, provision_janus_room_for_session

            if session.lifecycle_state == MeetingLifecycleState.PROVISIONING and not session.janus_room_id:
                dispatch_task(provision_janus_room_for_session, str(session.pk))
            if created and participant is not None:
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

        client_state = require_json_object(client_state, field_name="client_state")
        with transaction.atomic():
            session = MeetingSession.objects.select_for_update().select_related("room").get(pk=session.pk)
            MeetingLifecycleService._validate_session_joinable(session=session)
            MeetingLifecycleService._validate_requested_role(requested_role)
            MeetingLifecycleService._validate_profile_not_removed(session=session, profile=profile)
            MeetingLifecycleService._validate_join_gate(
                session=session,
                passcode=passcode,
                invite_token=invite_token,
            )
            existing_participant = session.participants.filter(profile=profile).exclude(
                status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED],
            ).first()
            if existing_participant:
                raise MeetingDomainError("Profile is already present in the session.")
            join_request = session.join_requests.select_for_update().filter(
                profile=profile,
                status=MeetingJoinRequestStatus.PENDING,
            ).first()
            if join_request:
                return join_request
            MeetingLifecycleService._validate_session_capacity(session=session)
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
                client_state=client_state,
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
        """Apply room policy and either admit immediately or create a wait request."""

        session = MeetingSession.objects.select_related("room").get(pk=session.pk)
        MeetingLifecycleService._validate_session_joinable(session=session)
        MeetingLifecycleService._validate_requested_role(requested_role)
        MeetingLifecycleService._validate_profile_not_removed(session=session, profile=profile)
        membership = MeetingPermissionService.get_room_membership(
            room=session.room,
            profile_or_user=profile,
        )
        direct_entry = MeetingLifecycleService._has_durable_direct_entry(
            room=session.room,
            profile=profile,
            membership=membership,
        ) or MeetingLifecycleService._policy_allows_direct_entry(session=session)

        if direct_entry:
            if not MeetingLifecycleService._has_durable_direct_entry(
                room=session.room,
                profile=profile,
                membership=membership,
            ):
                MeetingLifecycleService._validate_join_gate(
                    session=session,
                    passcode=passcode,
                    invite_token=invite_token,
                )
            participant = MeetingLifecycleService._admit_profile_directly(
                session=session,
                profile=profile,
                membership=membership,
                requested_display_name=requested_display_name,
                requested_role=requested_role,
                connection=connection,
                client_session_key=client_session_key,
            )
            return MeetingAdmissionResult(
                status="admitted",
                participant=participant,
                direct_entry=True,
            )

        bound_connection = MeetingLifecycleService._resolve_active_connection(
            session=session,
            profile=profile,
            connection=connection,
            client_session_key=client_session_key,
        )
        join_request = MeetingLifecycleService.request_join(
            session=session,
            profile=profile,
            requested_display_name=requested_display_name,
            requested_role=requested_role,
            note=note,
            client_state=client_state,
            connection=bound_connection,
            passcode=passcode,
            invite_token=invite_token,
        )
        return MeetingAdmissionResult(
            status="waiting",
            join_request=join_request,
            direct_entry=False,
        )

    @staticmethod
    def _validate_session_joinable(*, session: MeetingSession) -> None:
        """Reject new activity after a session has begun winding down."""

        if session.lifecycle_state in {
            MeetingLifecycleState.ENDING,
            MeetingLifecycleState.ENDED,
            MeetingLifecycleState.FAILED,
        }:
            raise MeetingDomainError("Cannot join a session that is ending, ended, or failed.")

    @staticmethod
    def _validate_requested_role(requested_role: str) -> None:
        """Prevent public admission requests from escalating their meeting role."""

        if requested_role != MeetingRole.PARTICIPANT:
            raise MeetingDomainError("Unsupported requested participant role.")

    @staticmethod
    def _validate_profile_not_removed(*, session: MeetingSession, profile) -> None:
        """Keep a moderator removal in force for the lifetime of a session."""

        if session.participants.filter(profile=profile, status=ParticipantStatus.REMOVED).exists():
            raise MeetingDomainError("This participant was removed from the session and cannot rejoin.")

    @staticmethod
    def _validate_join_gate(
        *,
        session: MeetingSession,
        passcode: str | None = None,
        invite_token: str | None = None,
    ) -> None:
        """Validate invite-only and passcode gates consistently across join paths."""

        if invite_token:
            MeetingInvitationService.validate_invite_token(session=session, token=invite_token)
            return
        if session.room.access_policy == MeetingAccessPolicy.INVITE_ONLY:
            raise MeetingDomainError("A valid meeting invitation is required for this room.")
        if not session.room.check_passcode(passcode):
            raise MeetingDomainError("Invalid room passcode.")

    @staticmethod
    def _validate_session_capacity(*, session: MeetingSession) -> None:
        """Reject new admission work after the room's participant ceiling is reached."""

        present_count = session.participants.exclude(
            status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED],
        ).count()
        if present_count >= session.room.max_participants:
            raise MeetingDomainError("Meeting has reached its maximum participant capacity.")

    @staticmethod
    def _has_durable_direct_entry(*, room: MeetingRoom, profile, membership) -> bool:
        """Return whether ownership or an active membership bypasses waiting-room review."""

        if str(room.created_by_profile_id) == str(profile.pk):
            return True
        return bool(membership and membership.is_active)

    @staticmethod
    def _policy_allows_direct_entry(*, session: MeetingSession) -> bool:
        """Return whether room configuration permits direct non-member entry."""

        return (
            not session.room.is_waiting_room_enabled
            or session.room.access_policy == MeetingAccessPolicy.OPEN
        )

    @staticmethod
    def _resolve_active_connection(
        *,
        session: MeetingSession,
        profile,
        connection: ParticipantConnection | None = None,
        client_session_key: str = "",
    ) -> ParticipantConnection | None:
        """Resolve the subscribed socket row associated with this admission attempt."""

        normalized_key = (client_session_key or "").strip()
        if (
            connection is not None
            and connection.session_id == session.pk
            and connection.profile_id == profile.pk
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
            keyed = queryset.filter(client_session_key=normalized_key).order_by(
                "-last_heartbeat_at",
                "-connected_at",
            ).first()
            if keyed:
                return keyed
        return queryset.order_by("-last_heartbeat_at", "-connected_at").first()

    @staticmethod
    def _admit_profile_directly(
        *,
        session: MeetingSession,
        profile,
        membership,
        requested_display_name: str,
        requested_role: str,
        connection: ParticipantConnection | None,
        client_session_key: str = "",
    ) -> Participant:
        """Create or reactivate exactly one participant for a direct-entry decision."""

        with transaction.atomic():
            session = MeetingSession.objects.select_for_update().select_related("room").get(pk=session.pk)
            MeetingLifecycleService._validate_session_joinable(session=session)
            participant = Participant.objects.select_for_update().filter(
                session=session,
                profile=profile,
            ).first()
            if participant and participant.status == ParticipantStatus.REMOVED:
                raise MeetingDomainError("This participant was removed from the session and cannot rejoin.")
            was_present = bool(
                participant
                and participant.status not in {ParticipantStatus.LEFT, ParticipantStatus.REMOVED}
            )
            if participant is None or participant.status == ParticipantStatus.LEFT:
                MeetingLifecycleService._validate_session_capacity(session=session)
            bound_connection = MeetingLifecycleService._resolve_active_connection(
                session=session,
                profile=profile,
                connection=connection,
                client_session_key=client_session_key,
            )
            now = timezone.now()
            session.join_requests.filter(
                profile=profile,
                status=MeetingJoinRequestStatus.PENDING,
            ).update(
                status=MeetingJoinRequestStatus.CANCELLED,
                reviewed_at=now,
                resolution_reason="Profile entered without waiting-room review.",
                updated_at=now,
            )
            if participant is None:
                participant = Participant(
                    room=session.room,
                    session=session,
                    profile=profile,
                )
            participant.membership = membership or participant.membership
            participant.role = membership.role if membership else requested_role
            participant.display_name = (
                requested_display_name
                or participant.display_name
                or profile.display_name
                or profile.handle
            )
            participant.apply_membership_defaults()
            participant.left_at = None
            if bound_connection:
                participant.mark_joined()
            else:
                participant.status = ParticipantStatus.ADMITTED
                participant.joined_at = participant.joined_at or now
                participant.last_seen_at = now
            participant.save()
            if bound_connection:
                bound_connection.participant = participant
                bound_connection.status = RealtimeConnectionStatus.ACTIVE
                bound_connection.disconnected_at = None
                bound_connection.last_heartbeat_at = now
                if client_session_key and not bound_connection.client_session_key:
                    bound_connection.client_session_key = client_session_key
                bound_connection.save(
                    update_fields=[
                        "participant",
                        "status",
                        "disconnected_at",
                        "last_heartbeat_at",
                        "client_session_key",
                        "updated_at",
                    ],
                )
                if session.lifecycle_state == MeetingLifecycleState.WAITING:
                    session.lifecycle_state = MeetingLifecycleState.ACTIVE
                    session.save(update_fields=["lifecycle_state", "updated_at"])
            if not was_present:
                record_session_event(
                    session=session,
                    event_type=MeetingEventType.PARTICIPANT_JOINED,
                    actor_profile=profile,
                    actor_participant=participant,
                    payload={"participant_id": str(participant.pk), "direct_entry": True},
                )
            MeetingLifecycleService.refresh_session_metrics(session=session)

        def emit_updates() -> None:
            from apps.meetings.realtime.emitter import MeetingSocketEmitter
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
        with transaction.atomic():
            session = MeetingSession.objects.select_for_update().select_related("room").get(
                pk=join_request.session_id,
            )
            MeetingLifecycleService._validate_session_joinable(session=session)
            join_request = MeetingJoinRequest.objects.select_for_update().select_related(
                "room",
                "session",
                "profile",
            ).get(pk=join_request.pk)
            if join_request.status != MeetingJoinRequestStatus.PENDING:
                raise MeetingJoinRequestStateError("Only pending join requests can be reviewed.")
            if approve:
                existing_participant = Participant.objects.select_for_update().filter(
                    session=session,
                    profile=join_request.profile,
                ).first()
                if existing_participant and existing_participant.status == ParticipantStatus.REMOVED:
                    raise MeetingDomainError("This participant was removed from the session and cannot rejoin.")
                if existing_participant is None or existing_participant.status == ParticipantStatus.LEFT:
                    MeetingLifecycleService._validate_session_capacity(session=session)
                membership = MeetingPermissionService.get_room_membership(room=join_request.room, profile_or_user=join_request.profile)
                participant, created = Participant.objects.get_or_create(
                    room=join_request.room,
                    session=session,
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
                participant.left_at = None
                participant.apply_membership_defaults()
                if join_request.connection:
                    participant.mark_joined()
                else:
                    participant.status = ParticipantStatus.ADMITTED
                    participant.joined_at = participant.joined_at or timezone.now()
                    participant.last_seen_at = timezone.now()
                participant.save()
                if join_request.connection:
                    join_request.connection.session = session
                    join_request.connection.participant = participant
                    join_request.connection.status = RealtimeConnectionStatus.ACTIVE
                    join_request.connection.disconnected_at = None
                    join_request.connection.save(
                        update_fields=[
                            "session",
                            "participant",
                            "status",
                            "disconnected_at",
                            "updated_at",
                        ],
                    )
                    if session.lifecycle_state == MeetingLifecycleState.WAITING:
                        session.lifecycle_state = MeetingLifecycleState.ACTIVE
                        session.save(update_fields=["lifecycle_state", "updated_at"])
                join_request.mark_reviewed(
                    reviewer=reviewer_profile,
                    status=MeetingJoinRequestStatus.ADMITTED,
                    reason=reason,
                )
                join_request.save(update_fields=["status", "reviewed_by_profile", "reviewed_at", "resolution_reason", "updated_at"])
                record_session_event(
                    session=session,
                    event_type=MeetingEventType.JOIN_REQUEST_REVIEWED,
                    actor_profile=reviewer_profile,
                    actor_participant=participant,
                    payload={"join_request_id": str(join_request.pk), "approved": True, "participant_id": str(participant.pk), "created": created},
                )
                record_session_event(
                    session=session,
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
                    session=session,
                    event_type=MeetingEventType.JOIN_REQUEST_REVIEWED,
                    actor_profile=reviewer_profile,
                    payload={"join_request_id": str(join_request.pk), "approved": False},
                )
            MeetingLifecycleService.refresh_session_metrics(session=session)

        def emit_updates() -> None:
            """Broadcast post-review state changes and queue Janus work after commit."""

            from apps.meetings.realtime.emitter import MeetingSocketEmitter

            MeetingSocketEmitter.emit_session_state(session=session)
            MeetingSocketEmitter.emit_join_request_reviewed(join_request=join_request, participant=participant)
            if participant is not None:
                from apps.meetings.tasks import attach_participant_media_handles

                dispatch_task(attach_participant_media_handles, str(participant.pk))

        transaction.on_commit(emit_updates)
        original_join_request.refresh_from_db()
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

        updates = require_json_object(updates, field_name="updates")
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
        cleanup_snapshot: dict[str, object]
        cleanup_socket_ids: tuple[str, ...]
        with transaction.atomic():
            participant.mark_left(status=ParticipantStatus.REMOVED)
            participant.save()
            active_connections = participant.connections.filter(
                status__in=[RealtimeConnectionStatus.CONNECTED, RealtimeConnectionStatus.SUBSCRIBED, RealtimeConnectionStatus.ACTIVE],
            )
            cleanup_socket_ids = tuple(
                active_connections.exclude(socket_id="").values_list(
                    "socket_id",
                    flat=True,
                )
            )
            active_connections.update(status=RealtimeConnectionStatus.DISCONNECTED, disconnected_at=timezone.now(), updated_at=timezone.now())
            record_session_event(
                session=session,
                event_type=MeetingEventType.PARTICIPANT_REMOVED,
                actor_profile=actor_profile,
                actor_participant=participant,
                payload={"participant_id": str(participant.pk), "reason": reason},
            )
            from apps.meetings.tasks import build_participant_media_cleanup_snapshot

            cleanup_handles = list(
                ParticipantMediaHandle.objects.select_for_update(of=("self",))
                .filter(participant=participant)
                .order_by("pk")
            )
            cleanup_snapshot = build_participant_media_cleanup_snapshot(
                participant,
                media_handles=cleanup_handles,
            )
            MeetingLifecycleService.refresh_session_metrics(session=session)

        def emit_updates() -> None:
            """Broadcast participant removal and queue Janus detach work after commit."""

            from apps.meetings.realtime.emitter import MeetingSocketEmitter
            from apps.meetings.tasks import detach_participant_media_handles

            dispatch_task(
                detach_participant_media_handles,
                str(participant.pk),
                cleanup_snapshot,
            )
            MeetingSocketEmitter.emit_participant_removed(session=session, participant=participant, reason=reason)
            MeetingSocketEmitter.emit_session_state(session=session)
            MeetingSocketEmitter.disconnect_sockets(cleanup_socket_ids)

        transaction.on_commit(emit_updates)
        return participant

    @staticmethod
    def leave_participant(
        *,
        session: MeetingSession,
        profile,
        socket_id: str | None = None,
        reason: str = "",
    ) -> Participant | None:
        """Persist an intentional departure instead of waiting for heartbeat expiry."""

        participant = None
        cleanup_snapshot: dict[str, object] | None = None
        cleanup_socket_ids: tuple[str, ...] = ()
        with transaction.atomic():
            session = MeetingSession.objects.select_for_update().get(pk=session.pk)
            connection = None
            if socket_id:
                connection = ParticipantConnection.objects.select_for_update().filter(
                    socket_id=socket_id,
                    session=session,
                    profile=profile,
                ).first()
            if connection and connection.participant_id:
                participant = Participant.objects.select_for_update().filter(
                    pk=connection.participant_id,
                ).first()
            if participant is None:
                participant = Participant.objects.select_for_update().filter(
                    session=session,
                    profile=profile,
                ).exclude(status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED]).first()
            changed = bool(
                participant
                and participant.status not in {ParticipantStatus.LEFT, ParticipantStatus.REMOVED}
            )
            now = timezone.now()
            if changed:
                participant.mark_left(status=ParticipantStatus.LEFT)
                participant.save()
                active_connections = participant.connections.filter(
                    session=session,
                    status__in=ACTIVE_CONNECTION_STATUSES,
                )
                cleanup_socket_ids = tuple(
                    active_connections.exclude(socket_id="").values_list(
                        "socket_id",
                        flat=True,
                    )
                )
                active_connections.update(
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
                from apps.meetings.tasks import build_participant_media_cleanup_snapshot

                cleanup_handles = list(
                    ParticipantMediaHandle.objects.select_for_update(of=("self",))
                    .filter(participant=participant)
                    .order_by("pk")
                )
                cleanup_snapshot = build_participant_media_cleanup_snapshot(
                    participant,
                    media_handles=cleanup_handles,
                )
            elif connection:
                connection.session = None
                connection.participant = None
                connection.status = RealtimeConnectionStatus.CONNECTED
                connection.disconnected_at = None
                connection.save(
                    update_fields=[
                        "session",
                        "participant",
                        "status",
                        "disconnected_at",
                        "updated_at",
                    ],
                )
            MeetingLifecycleService.refresh_session_metrics(session=session)

        def emit_updates() -> None:
            from apps.meetings.realtime.emitter import MeetingSocketEmitter

            if changed and participant is not None:
                from apps.meetings.tasks import detach_participant_media_handles

                dispatch_task(
                    detach_participant_media_handles,
                    str(participant.pk),
                    cleanup_snapshot,
                )
            MeetingSocketEmitter.emit_session_state(session=session)
            MeetingSocketEmitter.disconnect_sockets(cleanup_socket_ids)

        transaction.on_commit(emit_updates)
        return participant

    @staticmethod
    def record_chat_message(*, session: MeetingSession, participant: Participant, body: str, metadata: dict[str, Any] | None = None) -> MeetingMessage:
        """Persist a meeting chat message after enforcing participant capabilities."""

        metadata = require_json_object(metadata, field_name="metadata")
        MeetingLifecycleService._validate_session_joinable(session=session)
        MeetingPermissionService.require_participant_capability(participant=participant, capability_field="can_chat")
        body = str(body).strip()
        if not body:
            raise MeetingDomainError("A chat message cannot be empty.")
        if len(body) > 4000:
            raise MeetingDomainError("A chat message cannot exceed 4000 characters.")
        with transaction.atomic():
            message = MeetingMessage.objects.create(session=session, participant=participant, body=body, metadata=metadata)
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

        metadata = require_json_object(metadata, field_name="metadata")
        MeetingLifecycleService._validate_session_joinable(session=session)
        MeetingPermissionService.require_participant_capability(participant=participant, capability_field="can_react")
        reaction = str(reaction).strip()
        if not reaction:
            raise MeetingDomainError("A reaction cannot be empty.")
        if len(reaction) > 64:
            raise MeetingDomainError("A reaction cannot exceed 64 characters.")
        if expires_in_seconds is not None and not 1 <= int(expires_in_seconds) <= 300:
            raise MeetingDomainError("Reaction expiry must be between 1 and 300 seconds.")
        expires_at = timezone.now() + timedelta(seconds=expires_in_seconds) if expires_in_seconds else None
        with transaction.atomic():
            reaction_record = MeetingReaction.objects.create(
                session=session,
                participant=participant,
                reaction=reaction,
                expires_at=expires_at,
                metadata=metadata,
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

        metadata = require_json_object(metadata, field_name="metadata")
        with transaction.atomic():
            session = MeetingSession.objects.select_for_update().get(pk=session.pk)
            MeetingLifecycleService._validate_session_joinable(session=session)
            participant = session.participants.filter(profile=profile).exclude(
                status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED],
            ).first()
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
                    "metadata": metadata,
                },
            )
            if participant:
                participant.last_seen_at = timezone.now()
                if participant.status in {ParticipantStatus.ADMITTED, ParticipantStatus.DISCONNECTED}:
                    participant.mark_joined()
                participant.save()
                if session.lifecycle_state == MeetingLifecycleState.WAITING:
                    session.lifecycle_state = MeetingLifecycleState.ACTIVE
                    session.save(update_fields=["lifecycle_state", "updated_at"])
            MeetingLifecycleService.refresh_session_metrics(session=session)
            return connection

    @staticmethod
    def mark_connection_heartbeat(*, socket_id: str) -> ParticipantConnection | None:
        """Refresh an active generation without resurrecting a superseded socket."""

        now = timezone.now()
        updated = ParticipantConnection.objects.filter(
            socket_id=socket_id,
            status__in=ACTIVE_CONNECTION_STATUSES,
        ).update(last_heartbeat_at=now, updated_at=now)
        connection = (
            ParticipantConnection.objects.filter(socket_id=socket_id)
            .select_related("participant")
            .first()
        )
        if not connection or not updated:
            return connection
        if connection.participant:
            connection.participant.last_seen_at = now
            connection.participant.save(update_fields=["last_seen_at", "updated_at"])
        return connection

    @staticmethod
    def mark_connection_disconnected(*, socket_id: str) -> ParticipantConnection | None:
        """Mark a realtime connection as disconnected and degrade participant presence when necessary."""

        connection_reference = ParticipantConnection.objects.filter(
            socket_id=socket_id
        ).only("pk").first()
        if connection_reference is None:
            return None
        media_handles_to_release: list[ParticipantMediaHandle] = []
        with transaction.atomic():
            # Media command paths lock handle rows before their connection row.
            # Use the same order here to avoid a PostgreSQL command/disconnect
            # deadlock, then perform network cleanup only after commit.
            media_handles_to_release = list(
                ParticipantMediaHandle.objects.select_for_update(of=("self",))
                .filter(connection_id=connection_reference.pk)
                .select_related("participant")
                .order_by("pk")
            )
            connection = (
                ParticipantConnection.objects.select_for_update(of=("self",))
                .select_related("participant", "session")
                .filter(pk=connection_reference.pk, socket_id=socket_id)
                .first()
            )
            if not connection:
                return None
            connection.mark_disconnected()
            connection.save()
            if connection.participant:
                # Snapshot the exact locally-owned handle generation while the
                # socket and handle rows are locked. Network detach happens
                # only after this transaction commits.
                still_active = connection.participant.connections.exclude(socket_id=socket_id).filter(
                    status__in=[RealtimeConnectionStatus.CONNECTED, RealtimeConnectionStatus.SUBSCRIBED, RealtimeConnectionStatus.ACTIVE],
                ).exists()
                if not still_active and connection.participant.status not in {ParticipantStatus.LEFT, ParticipantStatus.REMOVED}:
                    connection.participant.status = ParticipantStatus.DISCONNECTED
                    connection.participant.last_seen_at = timezone.now()
                    connection.participant.save(update_fields=["status", "last_seen_at", "updated_at"])
            if connection.session:
                MeetingLifecycleService.refresh_session_metrics(session=connection.session)
                session = connection.session
            else:
                session = None
        if media_handles_to_release:
            from apps.meetings.services.janus import (
                release_disconnected_participant_media_plugins,
            )

            release_disconnected_participant_media_plugins(media_handles_to_release)
        from apps.meetings.services.janus import (
            release_local_media_plugins_for_connection,
        )

        release_local_media_plugins_for_connection(connection.pk)
        if session:
            from apps.meetings.realtime.emitter import MeetingSocketEmitter

            MeetingSocketEmitter.emit_session_state(session=session)
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
            session = MeetingSession.objects.select_for_update().select_related("room").get(pk=session.pk)
            if session.lifecycle_state == MeetingLifecycleState.ENDED:
                return session
            now = timezone.now()
            session.lifecycle_state = MeetingLifecycleState.ENDED
            session.ended_at = now
            session.save(update_fields=["lifecycle_state", "ended_at", "updated_at"])
            session.participants.exclude(
                status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED],
            ).update(
                status=ParticipantStatus.LEFT,
                left_at=now,
                last_seen_at=now,
                updated_at=now,
            )
            session.join_requests.filter(status=MeetingJoinRequestStatus.PENDING).update(
                status=MeetingJoinRequestStatus.CANCELLED,
                reviewed_at=now,
                resolution_reason=reason or "The meeting ended.",
                updated_at=now,
            )
            session.connections.filter(status__in=ACTIVE_CONNECTION_STATUSES).update(
                status=RealtimeConnectionStatus.DISCONNECTED,
                disconnected_at=now,
                last_heartbeat_at=now,
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
            from apps.meetings.tasks import queue_janus_room_cleanup

            queue_janus_room_cleanup(str(session.pk))
            MeetingSocketEmitter.emit_session_ended(session=session, reason=reason)
            MeetingSocketEmitter.emit_session_state(session=session)

        transaction.on_commit(emit_updates)
        return session
