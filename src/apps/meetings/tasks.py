"""Celery tasks for meeting invitations and scheduled domain work."""

from __future__ import annotations

import logging
from datetime import timedelta

from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.db.models import F, Q
from django.utils import timezone
from apps.meetings.models import (
    MeetingEventType,
    MeetingInvitation,
    MeetingJoinRequest,
    MeetingJoinRequestStatus,
    MeetingLifecycleState,
    MeetingSession,
    Participant,
    ParticipantStatus,
)
from apps.meetings.services.gateway_state import project_event
from apps.meetings.services.gateway_outbox import record_authorization_intent
from apps.meetings.services.gateway_tickets import advance_epoch
from apps.meetings.services.lifecycle import (
    MeetingLifecycleService,
    dispatch_task,
    record_session_event,
)

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=5,
    soft_time_limit=20,
    time_limit=30,
)
def send_meeting_invitation_email(
    self,
    invitation_id: str,
    join_url: str = "",
    force_send: bool = False,
) -> dict:
    """Send one due invitation reminder and persist its delivery outcome."""

    del self
    attempted_at = timezone.now()
    try:
        with transaction.atomic():
            invitation = (
                # ``issuer_profile`` is nullable and therefore outer-joined.
                # PostgreSQL must lock only the invitation row, not that join.
                MeetingInvitation.objects.select_for_update(of=("self",))
                .select_related(
                    "issuer_profile",
                    "session__room",
                    "session__started_by_profile",
                )
                .filter(pk=invitation_id)
                .first()
            )
            if invitation is None:
                return {"status": "missing", "invitation_id": invitation_id}
            if invitation.session.lifecycle_state in {
                MeetingLifecycleState.ENDING,
                MeetingLifecycleState.ENDED,
                MeetingLifecycleState.FAILED,
            }:
                return {
                    "status": "meeting_unavailable",
                    "invitation_id": invitation_id,
                }
            ready = (
                invitation.session.room.scheduled_start_at is None
                or invitation.session.room.scheduled_start_at <= attempted_at
            )
            if not ready and not force_send:
                return {"status": "not_due", "invitation_id": invitation_id}
            delivery_sent_at = (
                invitation.ready_email_sent_at
                if ready
                else invitation.initial_email_sent_at
            )
            if delivery_sent_at is not None:
                return {"status": "already_sent", "invitation_id": invitation_id}
            if not join_url:
                from apps.meetings.services.invitations import MeetingInvitationService

                issuer_profile = (
                    invitation.issuer_profile or invitation.session.started_by_profile
                )
                token = MeetingInvitationService.create_invite_token(
                    session=invitation.session,
                    issuer_profile=issuer_profile,
                    expires_in_seconds=invitation.expires_in_seconds,
                )
                join_url = MeetingInvitationService.build_frontend_join_url(
                    session=invitation.session,
                    invite_token=token,
                )
            from apps.meetings.services.invitations import MeetingInvitationService

            sent_count = MeetingInvitationService.send_invitation_email(
                invitation=invitation,
                join_url=join_url,
                ready=ready,
            )
            if sent_count != 1:
                raise RuntimeError(
                    f"Email backend reported {sent_count} delivered messages instead of 1.",
                )
            invitation.delivery_attempts += 1
            invitation.last_delivery_attempt_at = attempted_at
            invitation.last_delivery_error = ""
            invitation.initial_email_sent_at = (
                invitation.initial_email_sent_at or attempted_at
            )
            invitation.ready_email_sent_at = attempted_at if ready else None
            invitation.save(
                update_fields=[
                    "delivery_attempts",
                    "initial_email_sent_at",
                    "ready_email_sent_at",
                    "last_delivery_attempt_at",
                    "last_delivery_error",
                    "updated_at",
                ],
            )
    except Exception as exc:
        MeetingInvitation.objects.filter(pk=invitation_id).update(
            delivery_attempts=F("delivery_attempts") + 1,
            last_delivery_attempt_at=attempted_at,
            last_delivery_error=str(exc)[:2000],
        )
        raise
    return {
        "status": "sent",
        "invitation_id": invitation_id,
        "email_state": "ready" if ready else "scheduled",
        "sent_count": sent_count,
    }


@shared_task(ignore_result=True)
def queue_due_meeting_invitation_emails() -> int:
    """Lease unsent initial invitations and reminders for meetings now ready."""

    now = timezone.now()
    lease_cutoff = now - timedelta(minutes=5)
    batch_size = max(
        1,
        int(getattr(settings, "MEETING_INVITATION_REMINDER_BATCH_SIZE", 500)),
    )
    candidates = list(
        MeetingInvitation.objects.filter(
            session__lifecycle_state__in=[
                MeetingLifecycleState.SCHEDULED,
                MeetingLifecycleState.PROVISIONING,
                MeetingLifecycleState.WAITING,
                MeetingLifecycleState.ACTIVE,
            ],
        )
        .filter(
            Q(initial_email_sent_at__isnull=True)
            | Q(
                ready_email_sent_at__isnull=True,
                session__room__scheduled_start_at__isnull=False,
                session__room__scheduled_start_at__lte=now,
            ),
        )
        .filter(
            Q(last_delivery_attempt_at__isnull=True)
            | Q(last_delivery_attempt_at=F("initial_email_sent_at"))
            | Q(last_delivery_attempt_at__lt=lease_cutoff),
        )
        .order_by("session__room__scheduled_start_at", "created_at")
        .values_list("pk", "last_delivery_attempt_at", "initial_email_sent_at")[
            :batch_size
        ],
    )
    queued = 0
    for invitation_id, previous_attempt_at, initial_email_sent_at in candidates:
        force_initial_send = initial_email_sent_at is None
        claim_filter = MeetingInvitation.objects.filter(pk=invitation_id)
        if force_initial_send:
            claim_filter = claim_filter.filter(initial_email_sent_at__isnull=True)
        else:
            claim_filter = claim_filter.filter(ready_email_sent_at__isnull=True)
        if previous_attempt_at is None:
            claim_filter = claim_filter.filter(last_delivery_attempt_at__isnull=True)
        else:
            claim_filter = claim_filter.filter(
                last_delivery_attempt_at=previous_attempt_at,
            )
        if not claim_filter.update(last_delivery_attempt_at=now):
            continue
        if (
            dispatch_task(
                send_meeting_invitation_email,
                str(invitation_id),
                "",
                force_initial_send,
            )
            is None
        ):
            rollback_filter = MeetingInvitation.objects.filter(
                pk=invitation_id,
                last_delivery_attempt_at=now,
            )
            if force_initial_send:
                rollback_filter = rollback_filter.filter(
                    initial_email_sent_at__isnull=True
                )
            else:
                rollback_filter = rollback_filter.filter(
                    ready_email_sent_at__isnull=True
                )
            rollback_filter.update(last_delivery_attempt_at=previous_attempt_at)
            continue
        queued += 1
    return queued


@shared_task(ignore_result=True)
def end_scheduled_sessions() -> int:
    """End live sessions whose room-level scheduled end timestamp has elapsed."""

    cutoff = timezone.now()
    session_ids = list(
        MeetingSession.objects.filter(
            signalling_backend="gateway",
            room__scheduled_end_at__isnull=False,
            room__scheduled_end_at__lte=cutoff,
        )
        .exclude(
            lifecycle_state__in=[
                MeetingLifecycleState.ENDED,
                MeetingLifecycleState.FAILED,
            ],
        )
        .values_list("pk", flat=True),
    )
    ended = 0
    for session_id in session_ids:
        session = (
            MeetingSession.objects.select_related("room").filter(pk=session_id).first()
        )
        if session is None or session.lifecycle_state == MeetingLifecycleState.ENDED:
            continue
        ended_session = MeetingLifecycleService.end_session(
            session=session,
            reason="The scheduled meeting end time was reached.",
        )
        if ended_session.lifecycle_state == MeetingLifecycleState.ENDED:
            ended += 1
    return ended


@shared_task(ignore_result=True)
def expire_pending_join_requests() -> int:
    """Expire old waiting-room requests with session-first lock ordering."""

    ttl_seconds = max(
        1,
        int(getattr(settings, "MEETING_JOIN_REQUEST_TTL_SECONDS", 900)),
    )
    cutoff = timezone.now() - timedelta(seconds=ttl_seconds)
    session_ids = list(
        MeetingJoinRequest.objects.filter(
            session__signalling_backend="gateway",
            status=MeetingJoinRequestStatus.PENDING,
            created_at__lt=cutoff,
        )
        .values_list("session_id", flat=True)
        .distinct(),
    )
    expired_count = 0
    for session_id in session_ids:
        expired_requests: list[MeetingJoinRequest] = []
        with transaction.atomic():
            session = (
                MeetingSession.objects.select_for_update().filter(pk=session_id).first()
            )
            if session is None:
                continue
            candidates = list(
                MeetingJoinRequest.objects.select_for_update().filter(
                    session=session,
                    status=MeetingJoinRequestStatus.PENDING,
                    created_at__lt=cutoff,
                ),
            )
            now = timezone.now()
            for join_request in candidates:
                changed = MeetingJoinRequest.objects.filter(
                    pk=join_request.pk,
                    status=MeetingJoinRequestStatus.PENDING,
                ).update(
                    status=MeetingJoinRequestStatus.EXPIRED,
                    reviewed_at=now,
                    resolution_reason="The waiting-room request expired before review.",
                    updated_at=now,
                )
                if not changed:
                    continue
                join_request.status = MeetingJoinRequestStatus.EXPIRED
                join_request.reviewed_at = now
                join_request.resolution_reason = (
                    "The waiting-room request expired before review."
                )
                record_session_event(
                    session=session,
                    event_type=MeetingEventType.JOIN_REQUEST_REVIEWED,
                    actor_profile=None,
                    payload={
                        "join_request_id": str(join_request.pk),
                        "approved": False,
                        "status": MeetingJoinRequestStatus.EXPIRED,
                    },
                )
                # Invalidate outstanding lobby tickets and queue physical disconnection.
                participant = (
                    Participant.objects.select_for_update()
                    .filter(
                        session=session,
                        join_request=join_request,
                        status=ParticipantStatus.WAITING,
                    )
                    .first()
                )
                if participant is not None:
                    advance_epoch(participant)
                    participant.mark_left(status=ParticipantStatus.LEFT)
                    participant.save()
                    record_authorization_intent(
                        session=session,
                        participant=participant,
                        action="participant.disconnect",
                        reason_code="revoked",
                    )
                expired_requests.append(join_request)
                expired_count += 1
            if expired_requests:
                MeetingLifecycleService.refresh_session_metrics(session=session)
                project_event(session)

    return expired_count
