"""Celery tasks for Janus orchestration, synchronization, and lifecycle cleanup."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping, Sequence
from datetime import timedelta

from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.db.models import F, Q
from django.utils import timezone
from jrtc_video import (
    VideoRoomCreateRequest,
    VideoRoomDestroyRequest,
    VideoRoomKickRequest,
)

from apps.meetings.exceptions import JanusGatewayError
from apps.meetings.jrtc.ids import require_janus_id
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
    ParticipantStream,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.realtime.emitter import MeetingSocketEmitter
from apps.meetings.services.janus import (
    build_room_payload,
    call_video_room_management_method,
    janus_room_id_for_session,
    serialize_janus_response,
    video_room_reply_data,
)
from apps.meetings.services.lifecycle import (
    MeetingLifecycleService,
    dispatch_task,
    record_session_event,
)
from apps.meetings.services.signaling import MeetingMediaSignalService

logger = logging.getLogger(__name__)


def _require_positive_janus_id(value: object, *, kind: str) -> int:
    """Validate the strict positive-integer identifier required by JRTC."""

    try:
        return require_janus_id(value, name=kind)
    except TypeError as exc:
        raise JanusGatewayError(f"A {kind} must be a positive integer.") from exc


def build_participant_media_cleanup_snapshot(
    participant: Participant,
    *,
    media_handles: Sequence[ParticipantMediaHandle] | None = None,
) -> dict[str, object]:
    """Serialize the exact participant/handle generation a cleanup may mutate."""

    handles = list(
        media_handles
        if media_handles is not None
        else participant.media_handles.order_by("pk")
    )
    return {
        "participant_status": str(participant.status),
        "janus_publisher_id": participant.janus_publisher_id,
        "janus_private_id": participant.janus_private_id,
        "handles": [
            {
                "id": str(handle.pk),
                "handle_type": str(handle.handle_type),
                "connection_id": (
                    None if handle.connection_id is None else str(handle.connection_id)
                ),
                "janus_session_id": handle.janus_session_id,
                "janus_handle_id": handle.janus_handle_id,
                "runtime_owner_id": handle.runtime_owner_id,
                "runtime_claim_id": (
                    None
                    if handle.runtime_claim_id is None
                    else str(handle.runtime_claim_id)
                ),
            }
            for handle in handles
        ],
    }


def _media_cleanup_snapshot_matches(
    participant: Participant,
    media_handles: Sequence[ParticipantMediaHandle],
    snapshot: Mapping[str, object],
) -> bool:
    """Return whether current rows still equal a queued cleanup generation."""

    if (
        str(participant.status) != str(snapshot.get("participant_status") or "")
        or participant.janus_publisher_id != snapshot.get("janus_publisher_id")
        or participant.janus_private_id != snapshot.get("janus_private_id")
    ):
        return False
    raw_handles = snapshot.get("handles")
    if not isinstance(raw_handles, list):
        return False
    expected = {
        str(item.get("id")): item
        for item in raw_handles
        if isinstance(item, Mapping) and item.get("id")
    }
    if len(expected) != len(raw_handles) or len(media_handles) != len(expected):
        return False
    for handle in media_handles:
        item = expected.get(str(handle.pk))
        if item is None or (
            str(handle.handle_type) != str(item.get("handle_type") or "")
            or (
                None if handle.connection_id is None else str(handle.connection_id)
            )
            != item.get("connection_id")
            or handle.janus_session_id != item.get("janus_session_id")
            or handle.janus_handle_id != item.get("janus_handle_id")
            or handle.runtime_owner_id != item.get("runtime_owner_id")
            or (
                None
                if handle.runtime_claim_id is None
                else str(handle.runtime_claim_id)
            )
            != item.get("runtime_claim_id")
        ):
            return False
    return True


def _load_matching_media_cleanup_generation(
    participant_id: str,
    snapshot: Mapping[str, object],
) -> tuple[Participant, list[ParticipantMediaHandle]] | None:
    """Read and validate a cleanup generation without retaining stale objects."""

    participant = Participant.objects.select_related("session").filter(
        pk=participant_id
    ).first()
    if participant is None:
        return None
    media_handles = list(participant.media_handles.order_by("pk"))
    if not _media_cleanup_snapshot_matches(participant, media_handles, snapshot):
        return None
    return participant, media_handles


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
                    invitation.issuer_profile
                    or invitation.session.started_by_profile
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
        .values_list("pk", "last_delivery_attempt_at", "initial_email_sent_at")[:batch_size],
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
        if dispatch_task(
            send_meeting_invitation_email,
            str(invitation_id),
            "",
            force_initial_send,
        ) is None:
            rollback_filter = MeetingInvitation.objects.filter(
                pk=invitation_id,
                last_delivery_attempt_at=now,
            )
            if force_initial_send:
                rollback_filter = rollback_filter.filter(initial_email_sent_at__isnull=True)
            else:
                rollback_filter = rollback_filter.filter(ready_email_sent_at__isnull=True)
            rollback_filter.update(last_delivery_attempt_at=previous_attempt_at)
            continue
        queued += 1
    return queued


def _video_room_exists(
    session: MeetingSession,
    room_id: int,
) -> tuple[bool | None, object]:
    """Return whether Janus currently reports the configured room."""

    response = call_video_room_management_method(
        session,
        "exists",
        _require_positive_janus_id(room_id, kind="Janus room ID"),
    )
    data = video_room_reply_data(response)
    if isinstance(data, dict):
        exists = data.get("exists")
    else:
        exists = getattr(data, "exists", None)
    return exists if isinstance(exists, bool) else None, response


def _video_room_participants(
    session: MeetingSession,
) -> tuple[list[object] | None, object]:
    """Return an explicit participant listing, or ``None`` if malformed."""

    response = call_video_room_management_method(
        session,
        "list_participants",
        _require_positive_janus_id(
            janus_room_id_for_session(session),
            kind="Janus room ID",
        ),
    )
    data = video_room_reply_data(response)
    if isinstance(data, dict):
        participants = data.get("participants") if "participants" in data else None
    else:
        participants = getattr(data, "participants", None)
    return list(participants) if participants is not None else None, response


def _participant_value(participant: object, field_name: str):
    """Read a field from a typed VideoRoom participant or mapping."""

    if isinstance(participant, dict):
        return participant.get(field_name)
    return getattr(participant, field_name, None)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def provision_janus_room_for_session(self, session_id: str) -> dict:
    """Provision a Janus VideoRoom for the supplied meeting session."""

    session = MeetingSession.objects.select_related("room", "started_by_profile").filter(
        pk=session_id,
    ).first()
    if session is None:
        logger.info("Skipping Janus provisioning for missing meeting session '%s'.", session_id)
        return {}
    if session.lifecycle_state in {
        MeetingLifecycleState.ENDING,
        MeetingLifecycleState.ENDED,
        MeetingLifecycleState.FAILED,
    }:
        return session.janus_state
    if session.janus_room_id and session.lifecycle_state in {
        MeetingLifecycleState.WAITING,
        MeetingLifecycleState.ACTIVE,
    }:
        return session.janus_state

    # Renew the recovery lease at the beginning of every Celery attempt.  The
    # recovery sweep keys off ``updated_at``; without this heartbeat, a slow
    # Janus request or an autoretry countdown can be mistaken for an abandoned
    # provisioning job and start a second retry chain.
    lease_time = timezone.now()
    MeetingSession.objects.filter(
        pk=session_id,
        lifecycle_state=MeetingLifecycleState.PROVISIONING,
        janus_room_id=None,
    ).update(updated_at=lease_time)
    session.updated_at = lease_time

    payload = build_room_payload(session)
    payload["room"] = _require_positive_janus_id(
        payload.get("room"),
        kind="Janus room ID",
    )
    request = VideoRoomCreateRequest(**payload)
    reconciled_existing_room = False
    try:
        response = call_video_room_management_method(session, "create", request)
    except JanusGatewayError as create_error:
        try:
            room_exists, response = _video_room_exists(session, request.room)
        except JanusGatewayError:
            raise create_error
        if room_exists is not True:
            raise create_error
        reconciled_existing_room = True
    plugin_data = video_room_reply_data(response)
    returned_room_id = (
        plugin_data.get("room")
        if isinstance(plugin_data, dict)
        else getattr(plugin_data, "room", None)
    )
    room_id = _require_positive_janus_id(
        request.room if returned_room_id is None else returned_room_id,
        kind="Janus room ID",
    )
    serialized_response = serialize_janus_response(response)
    if reconciled_existing_room:
        serialized_response["reconciled_existing_room"] = True

    with transaction.atomic():
        session = MeetingSession.objects.select_for_update().select_related(
            "room",
            "started_by_profile",
        ).get(pk=session_id)
        if session.lifecycle_state in {
            MeetingLifecycleState.ENDING,
            MeetingLifecycleState.ENDED,
            MeetingLifecycleState.FAILED,
        }:
            # Provisioning may have crossed an explicit end request. Persist the
            # deterministic room identifier and queue teardown so no Janus room
            # is orphaned by that race.
            session.janus_room_id = room_id
            session.janus_backend_server = getattr(settings, "JANUS_SESSION_URL", "")
            session.janus_state = serialized_response
            session.cleanup_completed_at = None
            session.cleanup_requested_at = None
            session.cleanup_request_id = None
            session.last_synced_at = timezone.now()
            session.save(
                update_fields=[
                    "janus_room_id",
                    "janus_backend_server",
                    "janus_state",
                    "cleanup_completed_at",
                    "cleanup_requested_at",
                    "cleanup_request_id",
                    "last_synced_at",
                    "updated_at",
                ],
            )
            transaction.on_commit(
                lambda: queue_janus_room_cleanup(str(session.pk)),
            )
            return session.janus_state
        if session.janus_room_id and session.lifecycle_state in {
            MeetingLifecycleState.WAITING,
            MeetingLifecycleState.ACTIVE,
        }:
            return session.janus_state

        session.janus_room_id = room_id
        session.janus_backend_server = getattr(settings, "JANUS_SESSION_URL", "")
        session.janus_state = serialized_response
        has_active_connection = session.connections.filter(
            status=RealtimeConnectionStatus.ACTIVE,
        ).exists()
        session.lifecycle_state = (
            MeetingLifecycleState.ACTIVE
            if has_active_connection
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
        transaction.on_commit(
            lambda: MeetingSocketEmitter.emit_session_state(session=session),
        )
    return session.janus_state


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def attach_participant_media_handles(self, participant_id: str) -> dict:
    """Prepare publisher/subscriber rows for lazy attachment by the signaling process.

    Janus Core v3 handles belong to their process-local session, so a Celery
    worker must not attach a handle that the ASGI signaling process would then
    be unable to use.  The returned mapping contains persistent media-handle
    row IDs, not remote Janus handle IDs.
    """

    participant_session_id = Participant.objects.filter(pk=participant_id).values_list(
        "session_id",
        flat=True,
    ).first()
    if participant_session_id is None:
        logger.info(
            "Skipping media-handle preparation for missing participant '%s'.",
            participant_id,
        )
        return {}

    prepared: dict[str, str] = {}
    with transaction.atomic():
        session = MeetingSession.objects.select_for_update().get(
            pk=participant_session_id,
        )
        participant = Participant.objects.select_for_update().filter(
            pk=participant_id,
            session=session,
        ).first()
        if participant is None:
            return {}
        if (
            session.cleanup_completed_at is not None
            or session.lifecycle_state
            in {
                MeetingLifecycleState.ENDING,
                MeetingLifecycleState.ENDED,
                MeetingLifecycleState.FAILED,
            }
            or participant.status in {ParticipantStatus.LEFT, ParticipantStatus.REMOVED}
        ):
            return {}

        latest_connection = participant.connections.order_by("-connected_at").first()
        for handle_type in (JanusHandleType.PUBLISHER, JanusHandleType.SUBSCRIBER):
            media_handle, created = ParticipantMediaHandle.objects.get_or_create(
                participant=participant,
                handle_type=handle_type,
                defaults={
                    "connection": latest_connection,
                    "opaque_id": f"{participant.pk}:{handle_type}",
                },
            )
            update_fields: list[str] = []
            if media_handle.connection is None and latest_connection is not None:
                media_handle.connection = latest_connection
                update_fields.append("connection")
            if created or (
                not media_handle.janus_handle_id
                and media_handle.lifecycle_state
                in {
                    JanusHandleLifecycleState.DETACHED,
                    JanusHandleLifecycleState.FAILED,
                }
            ):
                media_handle.lifecycle_state = JanusHandleLifecycleState.ATTACHING
                update_fields.append("lifecycle_state")
            if update_fields:
                media_handle.save(update_fields=[*update_fields, "updated_at"])
            prepared[handle_type] = str(media_handle.pk)
        MeetingLifecycleService.refresh_session_metrics(session=session)
        transaction.on_commit(
            lambda: MeetingSocketEmitter.emit_session_state(session=session),
        )
    return prepared


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def detach_participant_media_handles(
    self,
    participant_id: str,
    cleanup_snapshot: Mapping[str, object] | None = None,
) -> None:
    """Invalidate persisted handles without adopting them in a Celery process.

    Live JRTC plugins belong to the ASGI process and registry that created
    them. This task therefore uses the VideoRoom management plane to remove a
    publisher when necessary, then clears only durable correlation metadata.
    """

    del self
    participant = Participant.objects.select_related("session").filter(
        pk=participant_id
    ).first()
    if participant is None:
        return
    media_handles = list(participant.media_handles.order_by("pk"))
    if cleanup_snapshot is None:
        cleanup_snapshot = build_participant_media_cleanup_snapshot(
            participant,
            media_handles=media_handles,
        )
    elif not isinstance(cleanup_snapshot, Mapping):
        logger.warning(
            "Skipping participant media cleanup with a malformed generation snapshot",
            extra={"participant_id": participant_id},
        )
        return
    matched = _load_matching_media_cleanup_generation(
        participant_id,
        cleanup_snapshot,
    )
    if matched is None:
        logger.info(
            "Skipping superseded participant media cleanup generation",
            extra={"participant_id": participant_id},
        )
        return
    participant, media_handles = matched
    publisher_cleanup_required = participant.janus_publisher_id is not None or any(
        handle.handle_type == JanusHandleType.PUBLISHER
        and any(
            value is not None
            for value in (
                handle.janus_session_id,
                handle.janus_handle_id,
                handle.runtime_owner_id,
            )
        )
        for handle in media_handles
    )
    publisher_removed = not publisher_cleanup_required

    if publisher_cleanup_required:
        janus_room_id = _require_positive_janus_id(
            janus_room_id_for_session(participant.session),
            kind="Janus room ID",
        )
        publisher_id = (
            _require_positive_janus_id(
                participant.janus_publisher_id,
                kind="Janus publisher ID",
            )
            if participant.janus_publisher_id is not None
            else None
        )
        if publisher_id is None:
            participants, _ = _video_room_participants(participant.session)
            for remote_participant in participants or []:
                metadata = _participant_value(remote_participant, "metadata")
                if isinstance(metadata, dict) and str(metadata.get("participant_id")) == str(
                    participant.pk
                ):
                    remote_publisher_id = _participant_value(
                        remote_participant,
                        "id",
                    )
                    if remote_publisher_id is not None:
                        publisher_id = _require_positive_janus_id(
                            remote_publisher_id,
                            kind="Janus publisher ID",
                        )
                        break
            if participants is not None and publisher_id is None:
                publisher_removed = True

        if publisher_id is not None:
            if (
                _load_matching_media_cleanup_generation(
                    participant_id,
                    cleanup_snapshot,
                )
                is None
            ):
                return
            try:
                call_video_room_management_method(
                    participant.session,
                    "kick",
                    VideoRoomKickRequest(
                        room=janus_room_id,
                        id=publisher_id,
                        secret=participant.session.janus_room_secret or None,
                    ),
                )
                publisher_removed = True
            except JanusGatewayError as kick_error:
                try:
                    participants, _ = _video_room_participants(participant.session)
                except JanusGatewayError:
                    raise kick_error
                if participants is None:
                    raise kick_error
                publisher_removed = True
                for remote_participant in participants:
                    remote_publisher_id = _participant_value(remote_participant, "id")
                    if remote_publisher_id is None:
                        continue
                    if (
                        _require_positive_janus_id(
                            remote_publisher_id,
                            kind="Janus publisher ID",
                        )
                        == publisher_id
                    ):
                        publisher_removed = False
                        break
                if not publisher_removed:
                    raise kick_error
        elif not publisher_removed:
            raise JanusGatewayError(
                "Unable to identify the process-owned Janus publisher for removal."
            )

    if not publisher_removed:
        raise JanusGatewayError("The Janus publisher could not be removed.")

    detached_at = timezone.now()
    with transaction.atomic():
        media_handles = list(
            ParticipantMediaHandle.objects.select_for_update(of=("self",))
            .filter(participant_id=participant_id)
            .order_by("pk")
        )
        participant = (
            Participant.objects.select_for_update(of=("self",))
            .select_related("session")
            .get(pk=participant_id)
        )
        if not _media_cleanup_snapshot_matches(
            participant,
            media_handles,
            cleanup_snapshot,
        ):
            return
        ParticipantStream.objects.filter(media_handle__in=media_handles).delete()
        ParticipantMediaHandle.objects.filter(pk__in=[item.pk for item in media_handles]).update(
            janus_handle_id=None,
            janus_session_id=None,
            runtime_owner_id=None,
            runtime_claim_id=None,
            selected_streams=[],
            janus_state={},
            lifecycle_state=JanusHandleLifecycleState.DETACHED,
            last_event_at=detached_at,
            updated_at=detached_at,
        )
        participant.janus_publisher_id = None
        participant.janus_private_id = None
        participant.save(
            update_fields=[
                "janus_publisher_id",
                "janus_private_id",
                "updated_at",
            ],
        )
    MeetingLifecycleService.refresh_session_metrics(session=participant.session)
    MeetingSocketEmitter.emit_session_state(session=participant.session)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def sync_janus_participants(self, session_id: str) -> dict:
    """Synchronize persisted participant state with the latest Janus participant listing."""

    session = MeetingSession.objects.select_related("room", "started_by_profile").filter(
        pk=session_id,
    ).first()
    if session is None:
        logger.info("Skipping Janus sync for missing meeting session '%s'.", session_id)
        return {}
    if session.cleanup_completed_at is not None or session.lifecycle_state in {
        MeetingLifecycleState.ENDING,
        MeetingLifecycleState.ENDED,
        MeetingLifecycleState.FAILED,
    }:
        return {}
    serialized = MeetingMediaSignalService.sync_publishers(session=session, emit_state=False)
    record_session_event(
        session=session,
        event_type=MeetingEventType.STATE_SYNCED,
        actor_profile=session.started_by_profile,
        payload={"participants": serialized.get("publishers", [])},
    )
    MeetingSocketEmitter.emit_session_state(session=session)
    return serialized


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def destroy_janus_room_for_session(
    self,
    session_id: str,
    cleanup_request_id: str = "",
) -> dict:
    """Destroy the Janus VideoRoom for a completed session and stamp cleanup metadata."""

    expected_request_id = None
    if cleanup_request_id:
        try:
            expected_request_id = uuid.UUID(str(cleanup_request_id))
        except (TypeError, ValueError):
            logger.warning(
                "Skipping Janus cleanup for session '%s' with invalid request id '%s'.",
                session_id,
                cleanup_request_id,
            )
            return {"status": "invalid_cleanup_request"}

    session = MeetingSession.objects.select_related("room", "started_by_profile").filter(
        pk=session_id,
    ).first()
    if session is None:
        logger.info("Skipping Janus cleanup for missing meeting session '%s'.", session_id)
        return {}
    if session.cleanup_completed_at:
        session_state = session.janus_state if isinstance(session.janus_state, dict) else {}
        destroy_state = session_state.get("destroy", {})
        return destroy_state if isinstance(destroy_state, dict) else {}
    if expected_request_id is not None and session.cleanup_request_id != expected_request_id:
        return {
            "status": "superseded_cleanup_request",
            "cleanup_request_id": cleanup_request_id,
        }

    room_id = _require_positive_janus_id(
        janus_room_id_for_session(session),
        kind="Janus room ID",
    )
    request = VideoRoomDestroyRequest(
        room=room_id,
        secret=session.janus_room_secret or None,
    )
    try:
        response = call_video_room_management_method(session, "destroy", request)
        result = serialize_janus_response(response)
    except JanusGatewayError as destroy_error:
        try:
            room_exists, response = _video_room_exists(session, room_id)
        except JanusGatewayError:
            raise destroy_error
        if room_exists is not False:
            raise destroy_error
        result = {
            "room": room_id,
            "reconciled_absent_room": True,
            "exists_response": serialize_janus_response(response),
        }

    with transaction.atomic():
        session = MeetingSession.objects.select_for_update().select_related(
            "room",
            "started_by_profile",
        ).get(pk=session_id)
        if session.cleanup_completed_at:
            session_state = session.janus_state if isinstance(session.janus_state, dict) else {}
            destroy_state = session_state.get("destroy", {})
            return destroy_state if isinstance(destroy_state, dict) else {}
        if expected_request_id is not None and session.cleanup_request_id != expected_request_id:
            return {
                "status": "superseded_cleanup_request",
                "cleanup_request_id": cleanup_request_id,
            }
        cleanup_time = timezone.now()
        ParticipantStream.objects.filter(
            media_handle__participant__session=session,
        ).delete()
        ParticipantMediaHandle.objects.filter(participant__session=session).update(
            janus_handle_id=None,
            janus_session_id=None,
            runtime_owner_id=None,
            runtime_claim_id=None,
            selected_streams=[],
            janus_state={},
            lifecycle_state=JanusHandleLifecycleState.DETACHED,
            last_event_at=cleanup_time,
            updated_at=cleanup_time,
        )
        Participant.objects.filter(session=session).update(
            janus_publisher_id=None,
            janus_private_id=None,
            updated_at=cleanup_time,
        )
        session.control_handle_id = None
        session.cleanup_completed_at = cleanup_time
        session_state = session.janus_state if isinstance(session.janus_state, dict) else {}
        session.janus_state = {**session_state, "destroy": result}
        session.save(
            update_fields=[
                "control_handle_id",
                "cleanup_completed_at",
                "janus_state",
                "updated_at",
            ],
        )
        record_session_event(
            session=session,
            event_type=MeetingEventType.CLEANUP_COMPLETED,
            actor_profile=session.started_by_profile,
            payload={"janus_room_id": room_id},
        )
        transaction.on_commit(
            lambda: MeetingSocketEmitter.emit_session_state(session=session),
        )
    return result


def queue_janus_room_cleanup(session_id: str) -> bool:
    """Claim one cleanup lease and dispatch exactly one current teardown task."""

    lease_seconds = max(
        1,
        int(
            getattr(
                settings,
                "MEETING_FINISHED_SESSION_CLEANUP_LEASE_SECONDS",
                900,
            ),
        ),
    )
    lease_time = timezone.now()
    lease_cutoff = lease_time - timedelta(seconds=lease_seconds)
    candidate = (
        MeetingSession.objects.filter(
            pk=session_id,
            lifecycle_state__in=[
                MeetingLifecycleState.ENDED,
                MeetingLifecycleState.FAILED,
            ],
            cleanup_completed_at__isnull=True,
        )
        .filter(
            Q(cleanup_requested_at__isnull=True)
            | Q(cleanup_requested_at__lt=lease_cutoff),
        )
        .values_list("cleanup_requested_at", "cleanup_request_id")
        .first()
    )
    if candidate is None:
        return False

    previous_requested_at, previous_request_id = candidate
    cleanup_request_id = uuid.uuid4()
    claim = MeetingSession.objects.filter(
        pk=session_id,
        lifecycle_state__in=[
            MeetingLifecycleState.ENDED,
            MeetingLifecycleState.FAILED,
        ],
        cleanup_completed_at__isnull=True,
    )
    if previous_requested_at is None:
        claim = claim.filter(cleanup_requested_at__isnull=True)
    else:
        claim = claim.filter(cleanup_requested_at=previous_requested_at)
    if previous_request_id is None:
        claim = claim.filter(cleanup_request_id__isnull=True)
    else:
        claim = claim.filter(cleanup_request_id=previous_request_id)
    if not claim.update(
        cleanup_requested_at=lease_time,
        cleanup_request_id=cleanup_request_id,
    ):
        return False

    if dispatch_task(
        destroy_janus_room_for_session,
        str(session_id),
        str(cleanup_request_id),
    ) is not None:
        return True

    # Broker failure must not hide the session until the full lease expires.
    MeetingSession.objects.filter(
        pk=session_id,
        cleanup_completed_at__isnull=True,
        cleanup_request_id=cleanup_request_id,
    ).update(
        cleanup_requested_at=previous_requested_at,
        cleanup_request_id=previous_request_id,
    )
    return False


@shared_task(ignore_result=True)
def cleanup_finished_sessions() -> int:
    """Queue Janus room teardown for finished sessions that have not yet been cleaned up."""

    session_ids = list(
        MeetingSession.objects.filter(
            lifecycle_state__in=[
                MeetingLifecycleState.ENDED,
                MeetingLifecycleState.FAILED,
            ],
            cleanup_completed_at__isnull=True,
        ).values_list("pk", flat=True),
    )
    count = 0
    for session_id in session_ids:
        if queue_janus_room_cleanup(str(session_id)):
            count += 1
    return count


@shared_task(ignore_result=True)
def mark_stale_connections() -> int:
    """Mark stale connections and downgrade participant presence when heartbeats stop arriving."""

    stale_seconds = int(getattr(settings, "MEETING_CONNECTION_STALE_SECONDS", 90))
    threshold = timezone.now() - timedelta(seconds=stale_seconds)
    active_statuses = [
        RealtimeConnectionStatus.CONNECTED,
        RealtimeConnectionStatus.SUBSCRIBED,
        RealtimeConnectionStatus.ACTIVE,
    ]
    candidates = list(
        ParticipantConnection.objects.filter(
            status__in=active_statuses,
            last_heartbeat_at__lt=threshold,
        ).values_list("pk", "session_id", "participant_id"),
    )
    candidates_by_session: dict[object, list[tuple[object, object | None]]] = {}
    for connection_id, session_id, participant_id in candidates:
        candidates_by_session.setdefault(session_id, []).append(
            (connection_id, participant_id),
    )

    count = 0
    for session_id, session_candidates in candidates_by_session.items():
        session_to_emit = None
        with transaction.atomic():
            session = (
                MeetingSession.objects.select_for_update().filter(pk=session_id).first()
                if session_id is not None
                else None
            )
            claimed_for_session = 0
            for connection_id, participant_id in session_candidates:
                claimed_at = timezone.now()
                claimed = ParticipantConnection.objects.filter(
                    pk=connection_id,
                    status__in=active_statuses,
                    last_heartbeat_at__lt=threshold,
                ).update(
                    status=RealtimeConnectionStatus.STALE,
                    updated_at=claimed_at,
                )
                if not claimed:
                    continue

                claimed_for_session += 1
                count += 1
                if participant_id is None:
                    continue
                participant = Participant.objects.select_for_update().filter(
                    pk=participant_id,
                ).first()
                if participant is None:
                    continue
                has_live_sibling = ParticipantConnection.objects.filter(
                    participant_id=participant_id,
                    status__in=active_statuses,
                ).exclude(pk=connection_id).exists()
                if (
                    not has_live_sibling
                    and participant.status
                    not in {ParticipantStatus.LEFT, ParticipantStatus.REMOVED}
                ):
                    participant.status = ParticipantStatus.DISCONNECTED
                    participant.last_seen_at = claimed_at
                    participant.save(
                        update_fields=["status", "last_seen_at", "updated_at"],
                    )

            if session is not None and claimed_for_session:
                MeetingLifecycleService.refresh_session_metrics(session=session)
                session_to_emit = session
        if session_to_emit is not None:
            MeetingSocketEmitter.emit_session_state(session=session_to_emit)
    return count


@shared_task(ignore_result=True)
def recover_stale_provisioning_sessions() -> int:
    """Lease and requeue sessions left in provisioning after a worker interruption."""

    stale_seconds = max(
        1,
        int(getattr(settings, "MEETING_PROVISIONING_STALE_SECONDS", 60)),
    )
    cutoff = timezone.now() - timedelta(seconds=stale_seconds)
    lease_time = timezone.now()
    candidates = list(
        MeetingSession.objects.filter(
            lifecycle_state=MeetingLifecycleState.PROVISIONING,
            janus_room_id__isnull=True,
            updated_at__lt=cutoff,
        ).values_list("pk", "updated_at"),
    )
    queued = 0
    for session_id, previous_updated_at in candidates:
        claimed = MeetingSession.objects.filter(
            pk=session_id,
            lifecycle_state=MeetingLifecycleState.PROVISIONING,
            janus_room_id__isnull=True,
            updated_at=previous_updated_at,
        ).update(updated_at=lease_time)
        if not claimed:
            continue
        if dispatch_task(provision_janus_room_for_session, str(session_id)) is None:
            # Make a broker outage recoverable on the next sweep instead of
            # hiding the row behind a successful-looking lease.
            MeetingSession.objects.filter(
                pk=session_id,
                lifecycle_state=MeetingLifecycleState.PROVISIONING,
                janus_room_id__isnull=True,
                updated_at=lease_time,
            ).update(updated_at=previous_updated_at)
            continue
        queued += 1
    return queued


@shared_task(ignore_result=True)
def end_scheduled_sessions() -> int:
    """End live sessions whose room-level scheduled end timestamp has elapsed."""

    cutoff = timezone.now()
    session_ids = list(
        MeetingSession.objects.filter(
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
        session = MeetingSession.objects.select_related("room").filter(pk=session_id).first()
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
            session = MeetingSession.objects.select_for_update().filter(pk=session_id).first()
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
                expired_requests.append(join_request)
                expired_count += 1
            if expired_requests:
                MeetingLifecycleService.refresh_session_metrics(session=session)

                def emit_updates(
                    *,
                    current_session=session,
                    current_requests=tuple(expired_requests),
                ) -> None:
                    for current_request in current_requests:
                        MeetingSocketEmitter.emit_join_request_reviewed(
                            join_request=current_request,
                            participant=None,
                        )
                    MeetingSocketEmitter.emit_session_state(session=current_session)

                transaction.on_commit(emit_updates)
    return expired_count
