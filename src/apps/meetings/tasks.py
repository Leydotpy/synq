"""Celery tasks for invitation delivery, Janus orchestration, and lifecycle work."""

from __future__ import annotations

import logging
from datetime import timedelta

from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.db.models import F
from django.utils import timezone

from apps.meetings.models import (
    JanusHandleLifecycleState,
    JanusHandleType,
    MeetingEventType,
    MeetingInvitation,
    MeetingJoinRequest,
    MeetingJoinRequestStatus,
    MeetingLifecycleState,
    MeetingSession,
    Participant,
    ParticipantConnection,
    ParticipantMediaHandle,
    ParticipantStatus,
    ParticipantStream,
    RealtimeConnectionStatus,
)
from apps.meetings.realtime.emitter import MeetingSocketEmitter
from apps.meetings.services.invitation_email import (
    build_meeting_invitation_email,
    invitation_is_ready,
)
from apps.meetings.services.janus import (
    CONTROL_SESSION_STATE_KEY,
    build_room_payload,
    call_plugin_method,
    ensure_participant_media_plugin,
    ensure_session_control_handle,
    serialize_janus_response,
)
from apps.meetings.services.lifecycle import (
    MeetingLifecycleService,
    dispatch_task,
    record_session_event,
)
from apps.meetings.services.signaling import MeetingMediaSignalService


logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=5,
)
def send_meeting_invitation_email(
    self,
    invitation_id: str,
    join_url: str = "",
    force_send: bool = False,
) -> dict:
    """Send one invitation outside the request path and record its delivery state."""

    del self
    attempted_at = timezone.now()
    try:
        with transaction.atomic():
            invitation = (
                MeetingInvitation.objects.select_for_update()
                .select_related(
                    "issuer_profile",
                    "session__room",
                    "session__started_by_profile",
                )
                .filter(pk=invitation_id)
                .first()
            )
            if invitation is None:
                logger.info(
                    "Skipping email delivery for missing meeting invitation '%s'.",
                    invitation_id,
                )
                return {"status": "missing", "invitation_id": invitation_id}
            if invitation.session.lifecycle_state in {
                MeetingLifecycleState.ENDED,
                MeetingLifecycleState.FAILED,
            }:
                return {"status": "meeting_unavailable", "invitation_id": invitation_id}

            show_join_button = invitation_is_ready(
                invitation,
                at=attempted_at,
            )
            sent_at = (
                invitation.ready_email_sent_at
                if show_join_button
                else invitation.initial_email_sent_at
            )
            if sent_at is not None and not force_send:
                return {
                    "status": "already_sent",
                    "invitation_id": invitation_id,
                    "email_state": "ready" if show_join_button else "scheduled",
                }

            if show_join_button and not join_url:
                from apps.meetings.services.invitations import MeetingInvitationService

                issuer_profile = (
                    invitation.issuer_profile
                    or invitation.session.started_by_profile
                )
                invite_token = MeetingInvitationService.create_invite_token(
                    session=invitation.session,
                    issuer_profile=issuer_profile,
                    expires_in_seconds=invitation.expires_in_seconds,
                )
                join_url = MeetingInvitationService.build_frontend_join_url(
                    session=invitation.session,
                    invite_token=invite_token,
                )

            email = build_meeting_invitation_email(
                invitation=invitation,
                join_url=join_url,
                show_join_button=show_join_button,
            )
            sent_count = email.send(fail_silently=False)
            if sent_count != 1:
                raise RuntimeError(
                    f"Email backend reported {sent_count} delivered messages instead of 1."
                )

            invitation.delivery_attempts += 1
            invitation.last_delivery_attempt_at = attempted_at
            invitation.last_delivery_error = ""
            invitation.initial_email_sent_at = (
                invitation.initial_email_sent_at or attempted_at
            )
            update_fields = [
                "delivery_attempts",
                "initial_email_sent_at",
                "last_delivery_attempt_at",
                "last_delivery_error",
                "updated_at",
            ]
            if show_join_button:
                invitation.ready_email_sent_at = attempted_at
                update_fields.append("ready_email_sent_at")
            invitation.save(update_fields=update_fields)
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
        "email_state": "ready" if show_join_button else "scheduled",
        "sent_count": sent_count,
    }


@shared_task
def queue_due_meeting_invitation_emails() -> int:
    """Queue the actionable follow-up for scheduled meetings that have arrived."""

    batch_size = int(
        getattr(settings, "MEETING_INVITATION_REMINDER_BATCH_SIZE", 500)
    )
    invitation_ids = list(
        MeetingInvitation.objects.filter(
            ready_email_sent_at__isnull=True,
            session__room__scheduled_start_at__isnull=False,
            session__room__scheduled_start_at__lte=timezone.now(),
            session__lifecycle_state__in=[
                MeetingLifecycleState.SCHEDULED,
                MeetingLifecycleState.PROVISIONING,
                MeetingLifecycleState.WAITING,
                MeetingLifecycleState.ACTIVE,
            ],
        )
        .order_by("session__room__scheduled_start_at", "created_at")
        .values_list("pk", flat=True)[:batch_size]
    )
    return sum(
        dispatch_task(
            send_meeting_invitation_email,
            str(invitation_id),
            "",
            False,
        )
        is not None
        for invitation_id in invitation_ids
    )


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def provision_janus_room_for_session(self, session_id: str) -> dict:
    """Provision a Janus VideoRoom for the supplied meeting session."""

    with transaction.atomic():
        session = (
            MeetingSession.objects.select_for_update()
            .select_related("room", "started_by_profile")
            .filter(pk=session_id)
            .first()
        )
        if session is None:
            logger.info("Skipping Janus provisioning for missing session '%s'.", session_id)
            return {}
        if (
            session.janus_room_id
            and session.lifecycle_state
            in {MeetingLifecycleState.WAITING, MeetingLifecycleState.ACTIVE}
        ):
            return session.janus_state

        control_handle = ensure_session_control_handle(session)
        payload = build_room_payload(session)
        room_exists = bool(
            call_plugin_method(
                control_handle,
                "exists",
                room=payload["room"],
            )
        )
        if room_exists:
            response_state = {
                "videoroom": "created",
                "room": payload["room"],
                "reused": True,
            }
        else:
            response = call_plugin_method(control_handle, "create", **payload)
            response_state = serialize_janus_response(response)

        session.janus_room_id = payload["room"]
        session.janus_backend_server = getattr(settings, "JANUS_SESSION_URL", "")
        session.janus_state = {
            **response_state,
            CONTROL_SESSION_STATE_KEY: (session.janus_state or {}).get(
                CONTROL_SESSION_STATE_KEY,
                "",
            ),
        }
        has_live_connection = session.connections.filter(
            status=RealtimeConnectionStatus.ACTIVE,
            participant__status=ParticipantStatus.ACTIVE,
        ).exists()
        session.lifecycle_state = (
            MeetingLifecycleState.ACTIVE
            if has_live_connection
            else MeetingLifecycleState.WAITING
        )
        session.last_synced_at = timezone.now()
        session.save(
            update_fields=[
                "janus_room_id",
                "janus_backend_server",
                "janus_state",
                "lifecycle_state",
                "last_synced_at",
                "updated_at",
            ],
        )
    record_session_event(
        session=session,
        event_type=MeetingEventType.SESSION_PROVISIONED,
        actor_profile=session.started_by_profile,
        payload={"janus_room_id": session.janus_room_id},
    )
    MeetingLifecycleService.refresh_session_metrics(session=session)
    MeetingSocketEmitter.emit_session_state(session=session)
    return session.janus_state


@shared_task
def recover_stale_provisioning_sessions() -> int:
    """Requeue sessions stranded before their Janus room was persisted."""

    stale_after_seconds = int(
        getattr(settings, "MEETING_PROVISIONING_RECOVERY_SECONDS", 60)
    )
    threshold = timezone.now() - timedelta(seconds=stale_after_seconds)
    session_ids = list(
        MeetingSession.objects.filter(
            lifecycle_state=MeetingLifecycleState.PROVISIONING,
            janus_room_id=None,
            updated_at__lt=threshold,
        ).values_list("pk", flat=True)
    )
    for session_id in session_ids:
        provision_janus_room_for_session.delay(str(session_id))
    return len(session_ids)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def attach_participant_media_handles(self, participant_id: str) -> dict:
    """Prepare publisher/subscriber rows; socket signaling attaches process-local handles."""

    participant = Participant.objects.select_related("session", "room", "profile").filter(pk=participant_id).first()
    if participant is None:
        logger.info("Skipping media attachment for missing participant '%s'.", participant_id)
        return {}
    attached: dict[str, str | None] = {}
    for handle_type in (JanusHandleType.PUBLISHER, JanusHandleType.SUBSCRIBER):
        media_handle, _ = ParticipantMediaHandle.objects.get_or_create(
            participant=participant,
            handle_type=handle_type,
            defaults={
                "connection": participant.connections.order_by("-connected_at").first(),
                "opaque_id": f"{participant.pk}:{handle_type}",
            },
        )
        if media_handle.lifecycle_state == JanusHandleLifecycleState.DETACHED:
            # A detached Janus id is not reusable.  Clearing it forces the
            # descriptor to construct and attach a fresh plugin handle.
            media_handle.janus_handle_id = None
        media_handle.connection = media_handle.connection or participant.connections.order_by("-connected_at").first()
        media_handle.lifecycle_state = JanusHandleLifecycleState.ATTACHING
        media_handle.last_event_at = timezone.now()
        media_handle.save(update_fields=["connection", "janus_handle_id", "lifecycle_state", "updated_at", "last_event_at"])
        attached[handle_type] = media_handle.janus_handle_id
    MeetingLifecycleService.refresh_session_metrics(session=participant.session)
    MeetingSocketEmitter.emit_session_state(session=participant.session)
    return attached


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def detach_participant_media_handles(self, participant_id: str) -> None:
    """Detach all Janus handles associated with a participant."""

    participant = Participant.objects.select_related("session").filter(pk=participant_id).first()
    if participant is None:
        logger.info("Skipping media detachment for missing participant '%s'.", participant_id)
        return
    for handle in participant.media_handles.all():
        if handle.janus_handle_id:
            # Janus handle ids are scoped to the ASGI process's Janus session,
            # so Celery cannot safely detach a foreign id. The request/socket
            # path attempts the real detach first. If that path was interrupted,
            # clear the stale descriptor here so it cannot remain DETACHING
            # forever; Janus room teardown is the final resource backstop.
            logger.warning(
                "Clearing undetached foreign Janus handle '%s' for participant '%s'.",
                handle.janus_handle_id,
                participant.pk,
            )
        handle.streams.all().delete()
        handle.janus_handle_id = None
        handle.janus_session_id = ""
        handle.lifecycle_state = JanusHandleLifecycleState.DETACHED
        handle.selected_streams = []
        handle.jsep_offer = {}
        handle.jsep_answer = {}
        handle.last_event_at = timezone.now()
        handle.save(
            update_fields=[
                "janus_handle_id",
                "janus_session_id",
                "lifecycle_state",
                "selected_streams",
                "jsep_offer",
                "jsep_answer",
                "last_event_at",
                "updated_at",
            ]
        )
    participant.janus_publisher_id = ""
    participant.janus_private_id = ""
    participant.janus_state = {}
    participant.save(
        update_fields=["janus_publisher_id", "janus_private_id", "janus_state", "updated_at"],
    )
    MeetingLifecycleService.refresh_session_metrics(session=participant.session)
    MeetingSocketEmitter.emit_session_state(session=participant.session)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def sync_janus_participants(self, session_id: str) -> dict:
    """Synchronize persisted participant state with the latest Janus participant listing."""

    session = MeetingSession.objects.select_related("room", "started_by_profile").filter(pk=session_id).first()
    if session is None:
        logger.info("Skipping Janus sync for missing session '%s'.", session_id)
        return {}
    serialized = MeetingMediaSignalService.sync_publishers(session=session, emit_state=False)
    record_session_event(
        session=session,
        event_type=MeetingEventType.STATE_SYNCED,
        actor_profile=session.started_by_profile,
        payload={"publishers": serialized.get("publishers", [])},
    )
    MeetingSocketEmitter.emit_session_state(session=session)
    return serialized


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def destroy_janus_room_for_session(self, session_id: str) -> dict:
    """Destroy the Janus VideoRoom for a completed session and stamp cleanup metadata."""

    session = MeetingSession.objects.select_related("room", "started_by_profile").filter(pk=session_id).first()
    if session is None:
        logger.info("Skipping Janus cleanup for missing session '%s'.", session_id)
        return {}
    if session.cleanup_completed_at:
        return dict((session.janus_state or {}).get("destroy") or {})
    result = {}
    if session.janus_room_id:
        control_handle = ensure_session_control_handle(session)
        room_exists = bool(
            call_plugin_method(
                control_handle,
                "exists",
                room=session.janus_room_id,
            )
        )
        if room_exists:
            response = call_plugin_method(
                control_handle,
                "destroy",
                room=session.janus_room_id,
                secret=session.janus_room_secret or None,
            )
            result = serialize_janus_response(response)
        else:
            result = {
                "videoroom": "destroyed",
                "room": session.janus_room_id,
                "already_absent": True,
            }
    now = timezone.now()
    with transaction.atomic():
        ParticipantStream.objects.filter(
            participant__session=session,
        ).delete()
        ParticipantMediaHandle.objects.filter(
            participant__session=session,
        ).update(
            janus_handle_id=None,
            janus_session_id="",
            lifecycle_state=JanusHandleLifecycleState.DETACHED,
            selected_streams=[],
            jsep_offer={},
            jsep_answer={},
            last_event_at=now,
            updated_at=now,
        )
        Participant.objects.filter(session=session).update(
            janus_publisher_id="",
            janus_private_id="",
            janus_state={},
            updated_at=now,
        )
        session.cleanup_completed_at = now
        session.janus_state = {**session.janus_state, "destroy": result}
        session.save(update_fields=["cleanup_completed_at", "janus_state", "updated_at"])
    record_session_event(
        session=session,
        event_type=MeetingEventType.CLEANUP_COMPLETED,
        actor_profile=session.started_by_profile,
        payload={"janus_room_id": session.janus_room_id},
    )
    MeetingSocketEmitter.emit_session_state(session=session)
    return result


@shared_task
def cleanup_finished_sessions() -> int:
    """Queue Janus room teardown for finished sessions that have not yet been cleaned up."""

    sessions = MeetingSession.objects.filter(lifecycle_state=MeetingLifecycleState.ENDED, cleanup_completed_at__isnull=True)
    count = 0
    for session in sessions.iterator():
        destroy_janus_room_for_session.delay(str(session.pk))
        count += 1
    return count


@shared_task
def end_scheduled_sessions() -> int:
    """End live sessions whose configured room end time has passed."""

    sessions = MeetingSession.objects.live().filter(
        room__scheduled_end_at__isnull=False,
        room__scheduled_end_at__lte=timezone.now(),
    )
    count = 0
    for session in sessions.select_related("room").iterator():
        MeetingLifecycleService.end_session(
            session=session,
            reason="The scheduled meeting end time was reached.",
        )
        count += 1
    return count


@shared_task
def expire_pending_join_requests() -> int:
    """Expire abandoned waiting-room requests and notify their requesters."""

    ttl_seconds = int(getattr(settings, "MEETING_JOIN_REQUEST_TTL_SECONDS", 900))
    threshold = timezone.now() - timedelta(seconds=ttl_seconds)
    requests = MeetingJoinRequest.objects.filter(
        status=MeetingJoinRequestStatus.PENDING,
        created_at__lt=threshold,
    ).select_related("session", "profile")
    count = 0
    for join_request in requests.iterator():
        now = timezone.now()
        updated = MeetingJoinRequest.objects.filter(
            pk=join_request.pk,
            status=MeetingJoinRequestStatus.PENDING,
        ).update(
            status=MeetingJoinRequestStatus.EXPIRED,
            reviewed_at=now,
            resolution_reason="The waiting-room request expired.",
            updated_at=now,
        )
        if not updated:
            continue
        join_request.refresh_from_db()
        MeetingLifecycleService.refresh_session_metrics(
            session=join_request.session,
        )
        MeetingSocketEmitter.emit_join_request_reviewed(
            join_request=join_request,
            participant=None,
        )
        MeetingSocketEmitter.emit_session_state(session=join_request.session)
        count += 1
    return count


@shared_task
def mark_stale_connections() -> int:
    """Mark stale connections and downgrade participant presence when heartbeats stop arriving."""

    stale_seconds = int(getattr(settings, "MEETING_CONNECTION_STALE_SECONDS", 90))
    threshold = timezone.now() - timedelta(seconds=stale_seconds)
    stale_connections = ParticipantConnection.objects.filter(
        status__in=[RealtimeConnectionStatus.CONNECTED, RealtimeConnectionStatus.SUBSCRIBED, RealtimeConnectionStatus.ACTIVE],
        last_heartbeat_at__lt=threshold,
    ).select_related("participant", "session")
    count = 0
    for connection in stale_connections.iterator():
        connection.status = RealtimeConnectionStatus.STALE
        connection.save(update_fields=["status", "updated_at"])
        if connection.participant and connection.participant.status not in {ParticipantStatus.LEFT, ParticipantStatus.REMOVED}:
            has_live_sibling = connection.participant.connections.exclude(pk=connection.pk).filter(
                status__in=[
                    RealtimeConnectionStatus.CONNECTED,
                    RealtimeConnectionStatus.SUBSCRIBED,
                    RealtimeConnectionStatus.ACTIVE,
                ],
            ).exists()
            if not has_live_sibling:
                connection.participant.status = ParticipantStatus.DISCONNECTED
                connection.participant.last_seen_at = timezone.now()
                connection.participant.save(update_fields=["status", "last_seen_at", "updated_at"])
        if connection.session:
            MeetingLifecycleService.refresh_session_metrics(session=connection.session)
            MeetingSocketEmitter.emit_session_state(session=connection.session)
        count += 1
    return count
