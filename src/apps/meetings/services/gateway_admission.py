"""Django-only gateway admission decisions. No Janus work or presence inference."""

from django.db import IntegrityError, transaction

from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.models import (
    GatewayAdmissionDecision,
    MeetingEventType,
    MeetingJoinRequest,
    MeetingJoinRequestStatus,
    MeetingLifecycleState,
    MeetingRole,
    MeetingRoomMembership,
    MeetingSession,
    Participant,
    ParticipantStatus,
)
from apps.meetings.services.gateway_outbox import enabled, record_authorization_intent
from apps.meetings.services.gateway_tickets import GatewayDenied, advance_epoch
from apps.meetings.services.lifecycle import (
    MeetingLifecycleService,
    record_session_event,
)
from apps.profiles.models import Profile


def _session(session_id):
    session = (
        MeetingSession.objects.select_for_update()
        .select_related("room")
        .filter(pk=session_id)
        .first()
    )
    if session is None or not enabled(session):
        raise GatewayDenied("UNAVAILABLE", 404)
    if session.lifecycle_state not in {
        MeetingLifecycleState.PROVISIONING,
        MeetingLifecycleState.WAITING,
        MeetingLifecycleState.ACTIVE,
    }:
        raise GatewayDenied()
    return session


def _profile(profile_id):
    profile = (
        Profile.objects.select_related("user")
        .filter(pk=profile_id, is_active=True, user__is_active=True)
        .first()
    )
    if profile is None:
        raise GatewayDenied()
    return profile


def _membership(session, profile):
    return (
        MeetingRoomMembership.objects.select_for_update()
        .filter(
            room=session.room,
            profile=profile,
            is_active=True,
        )
        .first()
    )


def _snapshot(participant, intent=None, decision=None):
    result = {
        "participant_id": str(participant.pk),
        "participant_status": participant.status,
        "authorization_epoch": participant.authorization_epoch,
        "join_request_id": str(intent.pk) if intent else None,
        "admission_status": intent.status if intent else "admitted",
    }
    if decision:
        result.update(
            decision_id=str(decision.pk),
            decision_epoch=decision.authorization_epoch,
            superseded=participant.authorization_epoch != decision.authorization_epoch,
        )
    outbox = participant.gateway_outbox.filter(
        authorization_epoch=participant.authorization_epoch
    ).first()
    result["enforcement_status"] = "pending" if outbox else "not_requested"
    return result


def request_intent(
    *, session_id, profile_id, display_name="", passcode=None, invite_token=None
):
    """Validate identity/policy before creating a pending or direct admission."""
    try:
        with transaction.atomic():
            session = _session(session_id)
            profile = _profile(profile_id)
            membership = _membership(session, profile)
            existing = (
                Participant.objects.select_for_update()
                .filter(session=session, profile=profile)
                .first()
            )
            if existing is not None:
                if existing.status in {
                    ParticipantStatus.LEFT,
                    ParticipantStatus.REMOVED,
                }:
                    raise GatewayDenied()
                if existing.status in {
                    ParticipantStatus.ADMITTED,
                    ParticipantStatus.ACTIVE,
                    ParticipantStatus.DISCONNECTED,
                }:
                    return _snapshot(existing, existing.join_request)
                if existing.status != ParticipantStatus.WAITING:
                    raise GatewayDenied()
            # A rejected request does not become direct-entry when room policy changes.
            if session.join_requests.filter(
                profile=profile, status=MeetingJoinRequestStatus.REJECTED
            ).exists():
                raise GatewayDenied()
            durable = MeetingLifecycleService._has_durable_direct_entry(
                room=session.room, profile=profile, membership=membership
            )
            if not durable:
                MeetingLifecycleService._validate_join_gate(
                    session=session, passcode=passcode, invite_token=invite_token
                )
            direct = durable or MeetingLifecycleService._policy_allows_direct_entry(
                session=session
            )
            intent = (
                MeetingJoinRequest.objects.select_for_update()
                .filter(
                    session=session,
                    profile=profile,
                    status=MeetingJoinRequestStatus.PENDING,
                )
                .first()
            )
            if existing is not None and (
                intent is None or existing.join_request_id != intent.pk
            ):
                raise GatewayDenied()
            if existing is not None and not direct:
                return _snapshot(existing, intent)
            if existing is None:
                MeetingLifecycleService._validate_session_capacity(session=session)
            if intent is None:
                intent = MeetingJoinRequest.objects.create(
                    room=session.room,
                    session=session,
                    profile=profile,
                    requested_display_name=display_name,
                    requested_role=MeetingRole.PARTICIPANT,
                )
            participant = existing or Participant(
                room=session.room,
                session=session,
                profile=profile,
                join_request=intent,
                display_name=display_name or profile.display_name or profile.handle,
                role=MeetingRole.PARTICIPANT,
                status=ParticipantStatus.WAITING,
            )
            if direct:
                participant.membership = membership
                participant.role = (
                    membership.role if membership else MeetingRole.PARTICIPANT
                )
                participant.apply_membership_defaults()
                participant.status = ParticipantStatus.ADMITTED
                advance_epoch(participant)
                intent.mark_reviewed(
                    reviewer=profile,
                    status=MeetingJoinRequestStatus.ADMITTED,
                    reason="Django direct-entry policy.",
                )
                intent.save(
                    update_fields=[
                        "status",
                        "reviewed_by_profile",
                        "reviewed_at",
                        "resolution_reason",
                        "updated_at",
                    ]
                )
            participant.save()
            if direct:
                record_authorization_intent(
                    session=session,
                    participant=participant,
                    action="participant.admit",
                    actor_profile=profile,
                )
            record_session_event(
                session=session,
                event_type=MeetingEventType.JOIN_REQUEST_REVIEWED
                if direct
                else MeetingEventType.JOIN_REQUEST_CREATED,
                actor_profile=profile,
                actor_participant=participant,
                payload={"join_request_id": str(intent.pk), "direct_entry": direct},
            )
            MeetingLifecycleService.refresh_session_metrics(session=session)
            return _snapshot(participant, intent)
    except MeetingDomainError:
        raise GatewayDenied() from None


def review_intent(
    *,
    session_id,
    join_request_id,
    actor_profile_id,
    decision_id,
    approve,
    expected_epoch,
):
    """Idempotency key and epoch precondition bind each review to its original intent."""
    if (
        type(approve) is not bool
        or type(expected_epoch) is not int
        or not 0 <= expected_epoch <= 2**53 - 1
    ):
        raise GatewayDenied("INVALID_REQUEST", 400)
    with transaction.atomic():
        session = _session(session_id)
        actor = _profile(actor_profile_id)
        membership = _membership(session, actor)
        if membership is None or not membership.can_manage_waiting_room:
            raise GatewayDenied()
        intent = (
            MeetingJoinRequest.objects.select_for_update()
            .filter(pk=join_request_id, session=session)
            .first()
        )
        if intent is None:
            raise GatewayDenied()
        participant = (
            Participant.objects.select_for_update()
            .select_related("profile__user")
            .filter(
                session=session,
                profile_id=intent.profile_id,
                join_request=intent,
            )
            .first()
        )
        if participant is None:
            raise GatewayDenied()
        previous = GatewayAdmissionDecision.objects.filter(pk=decision_id).first()
        if previous is not None:
            if (
                previous.join_request_id != intent.pk
                or previous.actor_profile_id != actor.pk
                or previous.approve != approve
                or previous.expected_epoch != expected_epoch
            ):
                raise GatewayDenied("IDEMPOTENCY_CONFLICT", 409)
            return _snapshot(participant, intent, previous)
        # A new idempotency key cannot repeat/reverse an already resolved intent.
        if (
            intent.status != MeetingJoinRequestStatus.PENDING
            or participant.status != ParticipantStatus.WAITING
        ):
            raise GatewayDenied("STALE_AUTHORIZATION", 409)
        if participant.authorization_epoch != expected_epoch:
            raise GatewayDenied("STALE_AUTHORIZATION", 409)
        if approve:
            # Recheck subject activity and role at the decision, never trust requested_role.
            subject = _profile(intent.profile_id)
            subject_membership = _membership(session, subject)
            participant.membership = subject_membership
            participant.role = (
                subject_membership.role
                if subject_membership
                else MeetingRole.PARTICIPANT
            )
            if participant.role not in MeetingRole.values:
                raise GatewayDenied()
            participant.apply_membership_defaults()
            participant.status = ParticipantStatus.ADMITTED
            # Admission is authorization; joined_at/media-connected remains unset.
            decision_status = MeetingJoinRequestStatus.ADMITTED
        else:
            participant.mark_left(status=ParticipantStatus.LEFT)
            decision_status = MeetingJoinRequestStatus.REJECTED
        advance_epoch(participant)
        participant.save()
        intent.mark_reviewed(reviewer=actor, status=decision_status)
        intent.save(
            update_fields=[
                "status",
                "reviewed_by_profile",
                "reviewed_at",
                "resolution_reason",
                "updated_at",
            ]
        )
        try:
            with transaction.atomic():
                decision = GatewayAdmissionDecision.objects.create(
                    id=decision_id,
                    join_request=intent,
                    actor_profile=actor,
                    participant=participant,
                    approve=approve,
                    expected_epoch=expected_epoch,
                    authorization_epoch=participant.authorization_epoch,
                )
        except IntegrityError:
            raise GatewayDenied("IDEMPOTENCY_CONFLICT", 409) from None
        record_authorization_intent(
            session=session,
            participant=participant,
            action="participant.admit" if approve else "participant.deny",
            actor_profile=actor,
            reason_code="denied",
        )
        record_session_event(
            session=session,
            event_type=MeetingEventType.JOIN_REQUEST_REVIEWED,
            actor_profile=actor,
            actor_participant=participant,
            payload={
                "join_request_id": str(intent.pk),
                "approved": approve,
                "decision_id": str(decision.pk),
            },
        )
        MeetingLifecycleService.refresh_session_metrics(session=session)
        return _snapshot(participant, intent, decision)
