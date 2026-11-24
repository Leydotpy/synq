"""High-level orchestration for room creation, admission, moderation, and cleanup."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import uuid4

from django.db import transaction
from django.db.models import F
from django.utils import timezone
from kombu.exceptions import OperationalError as BrokerOperationalError

from apps.meetings.exceptions import MeetingDomainError, MeetingJoinRequestStateError
from apps.meetings.models import (
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
    ParticipantStatus,
)
from apps.meetings.services.gateway_state import project_event
from apps.meetings.services.permissions import MeetingPermissionService
from apps.meetings.services.gateway_tickets import advance_epoch
from apps.meetings.services.gateway_outbox import (
    record_authorization_intent,
    record_meeting_termination,
)
from apps.meetings.services.invitations import MeetingInvitationService

logger = logging.getLogger(__name__)


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
    def start_session(
        *, room: MeetingRoom, started_by_profile, metadata: dict[str, Any] | None = None
    ) -> MeetingSession:
        """Create or reuse the room's live session and seed the host participant."""

        metadata = require_json_object(metadata, field_name="metadata")
        membership = MeetingPermissionService.get_room_membership(
            room=room, profile_or_user=started_by_profile
        )
        with transaction.atomic():
            locked_room = MeetingRoom.objects.select_for_update().get(pk=room.pk)
            session = locked_room.sessions.live().order_by("-created_at").first()
            if session is not None:
                if session.signalling_backend != "gateway":
                    raise MeetingDomainError(
                        "This room still has an active legacy meeting."
                    )
                return session
            from apps.meetings.services.gateway_ownership import require_gateway_start
            from apps.meetings.services.gateway_cutover import require_drained

            if locked_room.sessions.filter(
                signalling_backend="legacy", cleanup_completed_at__isnull=True
            ).exists():
                raise MeetingDomainError(
                    "The prior legacy meeting has no confirmed cleanup."
                )
            previous = locked_room.sessions.order_by("-created_at").first()
            if previous is not None and previous.signalling_backend == "gateway":
                require_drained(previous)
            require_gateway_start(locked_room)
            session = MeetingSession.objects.create(
                room=locked_room,
                started_by_profile=started_by_profile,
                lifecycle_state=MeetingLifecycleState.WAITING,
                signalling_backend="gateway",
                started_at=timezone.now(),
                metadata=metadata,
            )
            participant = Participant(
                room=locked_room,
                session=session,
                profile=started_by_profile,
                membership=membership,
                role=membership.role if membership else MeetingRole.HOST,
                display_name=started_by_profile.display_name
                or started_by_profile.handle,
            )
            participant.apply_membership_defaults()
            participant.status = ParticipantStatus.ADMITTED
            participant.save()
            record_session_event(
                session=session,
                event_type=MeetingEventType.SESSION_CREATED,
                actor_profile=started_by_profile,
                actor_participant=participant,
                payload={"room_id": str(locked_room.pk)},
            )
            MeetingLifecycleService.refresh_session_metrics(session=session)
        return session

    @staticmethod
    @transaction.atomic
    def request_admission(
        *,
        session,
        profile,
        requested_display_name="",
        requested_role=MeetingRole.PARTICIPANT,
        note="",
        client_state=None,
        client_session_key="",
        passcode=None,
        invite_token=None,
    ):
        """Adapt the existing HTTP admission response to the gateway authority."""
        from apps.meetings.services.gateway_admission import request_intent
        from apps.meetings.services.gateway_tickets import GatewayDenied

        MeetingLifecycleService._validate_requested_role(requested_role)
        client_state = require_json_object(client_state, field_name="client_state")
        try:
            result = request_intent(
                session_id=session.pk,
                profile_id=profile.pk,
                display_name=requested_display_name,
                note=note,
                client_state=client_state,
                passcode=passcode,
                invite_token=invite_token,
            )
        except GatewayDenied as exc:
            raise MeetingDomainError(
                "Admission to this meeting is unavailable."
            ) from exc
        participant = session.participants.select_related("join_request").get(
            pk=result["participant_id"]
        )
        admitted = participant.status in {
            ParticipantStatus.ADMITTED,
            ParticipantStatus.ACTIVE,
            ParticipantStatus.DISCONNECTED,
        }
        return MeetingAdmissionResult(
            status="admitted" if admitted else "waiting",
            participant=participant if admitted else None,
            join_request=participant.join_request,
            direct_entry=admitted,
        )

    @staticmethod
    def request_join(**kwargs):
        result = MeetingLifecycleService.request_admission(**kwargs)
        if result.join_request is None:
            raise MeetingDomainError("Profile is already present in the session.")
        return result.join_request

    @staticmethod
    def _validate_session_joinable(*, session: MeetingSession) -> None:
        """Reject new activity after a session has begun winding down."""

        if session.lifecycle_state in {
            MeetingLifecycleState.ENDING,
            MeetingLifecycleState.ENDED,
            MeetingLifecycleState.FAILED,
        }:
            raise MeetingDomainError(
                "Cannot join a session that is ending, ended, or failed."
            )

    @staticmethod
    def _validate_requested_role(requested_role: str) -> None:
        """Prevent public admission requests from escalating their meeting role."""

        if requested_role != MeetingRole.PARTICIPANT:
            raise MeetingDomainError("Unsupported requested participant role.")

    @staticmethod
    def _validate_join_gate(
        *,
        session: MeetingSession,
        passcode: str | None = None,
        invite_token: str | None = None,
    ) -> None:
        """Validate invite-only and passcode gates consistently across join paths."""

        if invite_token:
            MeetingInvitationService.validate_invite_token(
                session=session, token=invite_token
            )
            return
        if session.room.access_policy == MeetingAccessPolicy.INVITE_ONLY:
            raise MeetingDomainError(
                "A valid meeting invitation is required for this room."
            )
        if not session.room.check_passcode(passcode):
            raise MeetingDomainError("Invalid room passcode.")

    @staticmethod
    def _validate_session_capacity(*, session: MeetingSession) -> None:
        """Reject new admission work after the room's participant ceiling is reached."""

        present_count = session.participants.exclude(
            status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED],
        ).count()
        if present_count >= session.room.max_participants:
            raise MeetingDomainError(
                "Meeting has reached its maximum participant capacity."
            )

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
    @transaction.atomic
    def review_join_request(*, join_request, reviewer_profile, approve, reason=""):
        """Apply the same locked, epoch-fenced decision as the gateway HTTP API."""
        from apps.meetings.services.gateway_admission import review_intent
        from apps.meetings.services.gateway_tickets import GatewayDenied

        session = MeetingSession.objects.select_for_update().get(
            pk=join_request.session_id
        )
        MeetingPermissionService.require_session_permission(
            session=session,
            profile_or_user=reviewer_profile,
            permission_field="can_manage_waiting_room",
        )
        participant = Participant.objects.filter(
            session=session, join_request=join_request
        ).first()
        if participant is None:
            raise MeetingJoinRequestStateError(
                "This request has no gateway participant."
            )
        try:
            review_intent(
                session_id=session.pk,
                join_request_id=join_request.pk,
                actor_profile_id=reviewer_profile.pk,
                decision_id=uuid4(),
                approve=approve,
                expected_epoch=participant.authorization_epoch,
                reason=reason,
            )
        except GatewayDenied as exc:
            raise MeetingJoinRequestStateError(
                "This request cannot be reviewed."
            ) from exc
        join_request.refresh_from_db()
        participant.refresh_from_db()
        return participant if approve else None

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
        permission_sensitive_fields = {
            "can_publish_audio",
            "can_publish_video",
            "can_share_screen",
            "can_chat",
            "can_react",
        }
        media_sensitive_fields = {"is_muted", "is_camera_blocked"}
        requested_fields = set(updates)
        if requested_fields & permission_sensitive_fields:
            MeetingPermissionService.require_session_permission(
                session=session,
                profile_or_user=actor_profile,
                permission_field="can_manage_permissions",
            )
        if requested_fields & media_sensitive_fields:
            MeetingPermissionService.require_session_permission(
                session=session,
                profile_or_user=actor_profile,
                permission_field="can_manage_media",
            )
        allowed_fields = (
            permission_sensitive_fields | media_sensitive_fields | {"raised_hand_at"}
        )
        with transaction.atomic():
            session = (
                MeetingSession.objects.select_for_update()
                .select_related("room")
                .get(pk=session.pk)
            )
            if session.signalling_backend != "gateway":
                raise MeetingDomainError("This legacy meeting has no gateway owner.")
            participant = Participant.objects.select_for_update().get(
                pk=participant.pk, session=session
            )
            changed_permissions = any(
                key in permission_sensitive_fields | media_sensitive_fields
                and getattr(participant, key) != value
                for key, value in updates.items()
            )
            if changed_permissions:
                advance_epoch(participant)
            for field_name, value in updates.items():
                if field_name in allowed_fields:
                    setattr(participant, field_name, value)
            participant.last_seen_at = timezone.now()
            participant.save()
            if changed_permissions:
                record_authorization_intent(
                    session=session,
                    participant=participant,
                    action="participant.permissions.update",
                    actor_profile=actor_profile,
                )
            record_session_event(
                session=session,
                event_type=MeetingEventType.PARTICIPANT_UPDATED,
                actor_profile=actor_profile,
                actor_participant=participant,
                payload={"participant_id": str(participant.pk), "updates": updates},
            )
            MeetingLifecycleService.refresh_session_metrics(session=session)
            project_event(session)

        return participant

    @staticmethod
    def remove_participant(
        *,
        session: MeetingSession,
        actor_profile,
        participant: Participant,
        reason: str = "",
    ) -> Participant:
        """Remove a participant from a live session and schedule downstream cleanup."""

        MeetingPermissionService.require_session_permission(
            session=session,
            profile_or_user=actor_profile,
            permission_field="can_manage_participants",
        )
        with transaction.atomic():
            session = (
                MeetingSession.objects.select_for_update()
                .select_related("room")
                .get(pk=session.pk)
            )
            if session.signalling_backend != "gateway":
                raise MeetingDomainError("This legacy meeting has no gateway owner.")
            participant = Participant.objects.select_for_update().get(
                pk=participant.pk, session=session
            )
            if participant.status != ParticipantStatus.REMOVED:
                advance_epoch(participant)
            participant.mark_left(status=ParticipantStatus.REMOVED)
            participant.save()
            record_authorization_intent(
                session=session,
                participant=participant,
                action="participant.disconnect",
                actor_profile=actor_profile,
                reason_code="revoked",
            )
            record_session_event(
                session=session,
                event_type=MeetingEventType.PARTICIPANT_REMOVED,
                actor_profile=actor_profile,
                actor_participant=participant,
                payload={"participant_id": str(participant.pk), "reason": reason},
            )
            MeetingLifecycleService.refresh_session_metrics(session=session)
            project_event(session)

        return participant

    @staticmethod
    def leave_participant(
        *,
        session: MeetingSession,
        profile,
        reason: str = "",
    ) -> Participant | None:
        """Persist an intentional departure instead of waiting for heartbeat expiry."""

        with transaction.atomic():
            session = MeetingSession.objects.select_for_update().get(pk=session.pk)
            if session.signalling_backend != "gateway":
                raise MeetingDomainError("This legacy meeting has no gateway owner.")
            participant = (
                Participant.objects.select_for_update()
                .filter(
                    session=session,
                    profile=profile,
                )
                .exclude(status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED])
                .first()
            )
            if participant:
                advance_epoch(participant)
                participant.mark_left(status=ParticipantStatus.LEFT)
                participant.save()
                record_authorization_intent(
                    session=session,
                    participant=participant,
                    action="participant.disconnect",
                    actor_profile=profile,
                    reason_code="normal",
                )
                record_session_event(
                    session=session,
                    event_type=MeetingEventType.PARTICIPANT_LEFT,
                    actor_profile=profile,
                    actor_participant=participant,
                    payload={"participant_id": str(participant.pk), "reason": reason},
                )
            MeetingLifecycleService.refresh_session_metrics(session=session)
            project_event(session)

        return participant

    @staticmethod
    def record_chat_message(
        *,
        session: MeetingSession,
        participant: Participant,
        body: str,
        metadata: dict[str, Any] | None = None,
    ) -> MeetingMessage:
        """Persist a meeting chat message after enforcing participant capabilities."""

        metadata = require_json_object(metadata, field_name="metadata")
        MeetingLifecycleService._validate_session_joinable(session=session)
        MeetingPermissionService.require_participant_capability(
            participant=participant, capability_field="can_chat"
        )
        body = str(body).strip()
        if not body:
            raise MeetingDomainError("A chat message cannot be empty.")
        if len(body) > 4000:
            raise MeetingDomainError("A chat message cannot exceed 4000 characters.")
        with transaction.atomic():
            session = MeetingSession.objects.select_for_update().get(pk=session.pk)
            MeetingLifecycleService._validate_session_joinable(session=session)
            message = MeetingMessage.objects.create(
                session=session, participant=participant, body=body, metadata=metadata
            )
            record_session_event(
                session=session,
                event_type=MeetingEventType.CHAT_MESSAGE_SENT,
                actor_profile=participant.profile,
                actor_participant=participant,
                payload={"message_id": str(message.pk)},
            )
            project_event(session)
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
        MeetingPermissionService.require_participant_capability(
            participant=participant, capability_field="can_react"
        )
        reaction = str(reaction).strip()
        if not reaction:
            raise MeetingDomainError("A reaction cannot be empty.")
        if len(reaction) > 64:
            raise MeetingDomainError("A reaction cannot exceed 64 characters.")
        if expires_in_seconds is not None and not 1 <= int(expires_in_seconds) <= 300:
            raise MeetingDomainError(
                "Reaction expiry must be between 1 and 300 seconds."
            )
        expires_at = (
            timezone.now() + timedelta(seconds=expires_in_seconds)
            if expires_in_seconds
            else None
        )
        with transaction.atomic():
            session = MeetingSession.objects.select_for_update().get(pk=session.pk)
            MeetingLifecycleService._validate_session_joinable(session=session)
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
            project_event(session)
        return reaction_record

    @staticmethod
    def refresh_session_metrics(*, session: MeetingSession) -> MeetingSession:
        """Recompute cached session counters and advance the state version."""

        present_participants = session.participants.exclude(
            status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED]
        ).count()
        session.participant_count = present_participants
        session.last_synced_at = timezone.now()
        session.bump_state_version()
        session.save(
            update_fields=[
                "participant_count",
                "last_synced_at",
                "state_version",
                "updated_at",
            ]
        )
        return session

    @staticmethod
    def end_session(
        *, session: MeetingSession, actor_profile=None, reason: str = ""
    ) -> MeetingSession:
        """Transition a live session to completion and record the signed termination intent."""

        if actor_profile is not None:
            MeetingPermissionService.require_session_permission(
                session=session,
                profile_or_user=actor_profile,
                permission_field="can_manage_participants",
            )
        with transaction.atomic():
            session = (
                MeetingSession.objects.select_for_update()
                .select_related("room")
                .get(pk=session.pk)
            )
            if session.signalling_backend != "gateway":
                raise MeetingDomainError(
                    "Legacy sessions must be drained by their original owner."
                )
            if session.lifecycle_state == MeetingLifecycleState.ENDED:
                return session
            now = timezone.now()
            session.lifecycle_state = MeetingLifecycleState.ENDED
            session.ended_at = now
            advance_epoch(session)
            session.save(
                update_fields=[
                    "lifecycle_state",
                    "ended_at",
                    "authorization_epoch",
                    "updated_at",
                ]
            )
            record_meeting_termination(session=session, actor_profile=actor_profile)
            gateway_ending = list(
                session.participants.exclude(
                    status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED]
                ).values_list("pk", flat=True)
            )
            session.participants.exclude(
                status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED],
            ).update(
                status=ParticipantStatus.LEFT,
                authorization_epoch=F("authorization_epoch") + 1,
                left_at=now,
                last_seen_at=now,
                updated_at=now,
            )
            for ended_participant in session.participants.filter(pk__in=gateway_ending):
                record_authorization_intent(
                    session=session,
                    participant=ended_participant,
                    action="participant.disconnect",
                    actor_profile=actor_profile,
                    reason_code="meeting_ended",
                )
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
            project_event(session)
            from apps.meetings.services.gateway_cutover import settle_unowned

            settle_unowned(session)

        return session
