"""Celery tasks for Janus orchestration, synchronization, and lifecycle cleanup."""

from __future__ import annotations

from datetime import timedelta

from celery import shared_task
from django.conf import settings
from django.utils import timezone
from janus_videoroom_plugin import (
    VideoRoomCreateRequest,
    VideoRoomDestroyRequest,
    VideoRoomKickRequest,
)

from apps.meetings.exceptions import JanusGatewayError
from apps.meetings.models import (
    JanusHandleLifecycleState,
    JanusHandleType,
    MeetingEventType,
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
    call_plugin_method,
    call_video_room_management_method,
    resolve_owned_janus_session,
    serialize_janus_response,
    video_room_reply_data,
)
from apps.meetings.services.lifecycle import MeetingLifecycleService, record_session_event
from apps.meetings.services.signaling import MeetingMediaSignalService


def _video_room_exists(
    session: MeetingSession,
    room_id: str,
) -> tuple[bool | None, object]:
    """Return whether Janus currently reports the configured room."""

    response = call_video_room_management_method(session, "exists", room_id)
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
        session.janus_room_id or str(session.pk),
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

    session = MeetingSession.objects.select_related("room", "started_by_profile").get(pk=session_id)
    payload = build_room_payload(session)
    request = VideoRoomCreateRequest(**payload)
    reconciled_existing_room = False
    try:
        response = call_video_room_management_method(session, "create", request)
    except JanusGatewayError as create_error:
        try:
            room_exists, response = _video_room_exists(session, str(request.room))
        except JanusGatewayError:
            raise create_error
        if room_exists is not True:
            raise create_error
        reconciled_existing_room = True
    plugin_data = video_room_reply_data(response)
    session.janus_room_id = str(getattr(plugin_data, "room", None) or request.room)
    session.janus_backend_server = getattr(settings, "JANUS_SESSION_URL", "")
    session.janus_state = serialize_janus_response(response)
    if reconciled_existing_room:
        session.janus_state["reconciled_existing_room"] = True
    session.lifecycle_state = MeetingLifecycleState.WAITING
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


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def attach_participant_media_handles(self, participant_id: str) -> dict:
    """Prepare publisher/subscriber rows for lazy attachment by the signaling process.

    Janus Core v3 handles belong to their process-local session, so a Celery
    worker must not attach a handle that the ASGI signaling process would then
    be unable to use.  The returned mapping contains persistent media-handle
    row IDs, not remote Janus handle IDs.
    """

    participant = Participant.objects.select_related("session", "room", "profile").get(pk=participant_id)
    latest_connection = participant.connections.order_by("-connected_at").first()
    prepared: dict[str, str] = {}
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
    MeetingLifecycleService.refresh_session_metrics(session=participant.session)
    MeetingSocketEmitter.emit_session_state(session=participant.session)
    return prepared


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def detach_participant_media_handles(self, participant_id: str) -> None:
    """Detach locally owned handles and retain foreign-owned IDs as pending."""

    participant = Participant.objects.select_related("session").get(pk=participant_id)
    media_handles = list(participant.media_handles.all())
    local_session = resolve_owned_janus_session(participant)
    local_session_id = str(local_session.id) if local_session is not None else ""
    pending_remote_detach = False
    pending_publisher_detach = False

    for handle in media_handles:
        detached = not bool(handle.janus_handle_id)
        if (
            handle.janus_handle_id
            and handle.janus_session_id
            and str(handle.janus_session_id) == local_session_id
        ):
            # Materializing the stored bound handle is sufficient for detach;
            # do not call the attachment helper during cleanup.
            try:
                call_plugin_method(handle.handle, "detach")
                detached = True
            except JanusGatewayError:
                detached = False

        if not detached:
            pending_remote_detach = True
            if handle.handle_type == JanusHandleType.PUBLISHER:
                pending_publisher_detach = True
            handle.lifecycle_state = JanusHandleLifecycleState.DETACHING
            handle.last_event_at = timezone.now()
            handle.save(
                update_fields=[
                    "lifecycle_state",
                    "last_event_at",
                    "updated_at",
                ],
            )
            continue

        handle.streams.all().delete()
        handle.janus_handle_id = None
        handle.janus_session_id = ""
        handle.selected_streams = []
        handle.janus_state = {}
        handle.lifecycle_state = JanusHandleLifecycleState.DETACHED
        handle.last_event_at = timezone.now()
        handle.save(
            update_fields=[
                "janus_handle_id",
                "janus_session_id",
                "selected_streams",
                "janus_state",
                "lifecycle_state",
                "last_event_at",
                "updated_at",
            ],
        )
    publisher_removed = not pending_publisher_detach
    if pending_publisher_detach:
        janus_room_id = participant.session.janus_room_id or str(participant.session.pk)
        publisher_id = str(participant.janus_publisher_id or "")
        if not publisher_id:
            participants, _ = _video_room_participants(participant.session)
            for remote_participant in participants or []:
                metadata = _participant_value(remote_participant, "metadata")
                if isinstance(metadata, dict) and str(metadata.get("participant_id")) == str(
                    participant.pk
                ):
                    publisher_id = str(_participant_value(remote_participant, "id") or "")
                    break
            if participants is not None and not publisher_id:
                publisher_removed = True

        if publisher_id:
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
                publisher_removed = all(
                    str(_participant_value(item, "id") or "") != publisher_id
                    for item in participants
                )
                if not publisher_removed:
                    raise kick_error
        elif not publisher_removed:
            raise JanusGatewayError(
                "Unable to identify the foreign-owned Janus publisher for removal."
            )

    if not pending_remote_detach or publisher_removed:
        participant.janus_publisher_id = ""
        participant.janus_private_id = ""
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

    session = MeetingSession.objects.select_related("room", "started_by_profile").get(pk=session_id)
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
def destroy_janus_room_for_session(self, session_id: str) -> dict:
    """Destroy the Janus VideoRoom for a completed session and stamp cleanup metadata."""

    session = MeetingSession.objects.select_related("room", "started_by_profile").get(pk=session_id)
    result = {}
    if session.janus_room_id:
        request = VideoRoomDestroyRequest(
            room=session.janus_room_id or str(session.pk),
            secret=session.janus_room_secret or None,
        )
        try:
            response = call_video_room_management_method(
                session,
                "destroy",
                request,
            )
            result = serialize_janus_response(response)
        except JanusGatewayError as destroy_error:
            try:
                room_exists, response = _video_room_exists(
                    session,
                    session.janus_room_id,
                )
            except JanusGatewayError:
                raise destroy_error
            if room_exists is not False:
                raise destroy_error
            result = {
                "room": session.janus_room_id,
                "reconciled_absent_room": True,
                "exists_response": serialize_janus_response(response),
            }
    cleanup_time = timezone.now()
    ParticipantStream.objects.filter(
        media_handle__participant__session=session,
    ).delete()
    ParticipantMediaHandle.objects.filter(participant__session=session).update(
        janus_handle_id=None,
        janus_session_id="",
        selected_streams=[],
        janus_state={},
        lifecycle_state=JanusHandleLifecycleState.DETACHED,
        last_event_at=cleanup_time,
        updated_at=cleanup_time,
    )
    Participant.objects.filter(session=session).update(
        janus_publisher_id="",
        janus_private_id="",
        updated_at=cleanup_time,
    )
    session.control_handle_id = None
    session.cleanup_completed_at = cleanup_time
    session.janus_state = {**session.janus_state, "destroy": result}
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
            connection.participant.status = ParticipantStatus.DISCONNECTED
            connection.participant.last_seen_at = timezone.now()
            connection.participant.save(update_fields=["status", "last_seen_at", "updated_at"])
        if connection.session:
            MeetingLifecycleService.refresh_session_metrics(session=connection.session)
            MeetingSocketEmitter.emit_session_state(session=connection.session)
        count += 1
    return count
