"""Celery tasks for Janus orchestration, synchronization, and lifecycle cleanup."""

from __future__ import annotations

from datetime import timedelta

from celery import shared_task
from django.conf import settings
from django.utils import timezone

from apps.meetings.models import (
    JanusHandleLifecycleState,
    JanusHandleType,
    MeetingEventType,
    MeetingLifecycleState,
    MeetingSession,
    Participant,
    ParticipantConnection,
    ParticipantMediaHandle,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.realtime.emitter import MeetingSocketEmitter
from apps.meetings.services.janus import (
    build_room_payload,
    call_plugin_method,
    ensure_participant_media_plugin,
    ensure_session_control_handle,
    serialize_janus_response,
)
from apps.meetings.services.lifecycle import MeetingLifecycleService, record_session_event
from apps.meetings.services.signaling import MeetingMediaSignalService


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def provision_janus_room_for_session(self, session_id: str) -> dict:
    """Provision a Janus VideoRoom for the supplied meeting session."""

    session = MeetingSession.objects.select_related("room", "started_by_profile").get(pk=session_id)
    control_handle = ensure_session_control_handle(session)
    payload = build_room_payload(session)
    response = call_plugin_method(control_handle, "create", **payload)
    plugin_data = getattr(getattr(response, "plugindata", None), "data", None)
    session.janus_room_id = str(getattr(plugin_data, "room", payload["room"]))
    session.janus_backend_server = getattr(settings, "JANUS_SESSION_URL", "")
    session.janus_state = serialize_janus_response(response)
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
    """Attach Janus publisher and subscriber handles for an admitted participant."""

    participant = Participant.objects.select_related("session", "room", "profile").get(pk=participant_id)
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
        media_handle.connection = media_handle.connection or participant.connections.order_by("-connected_at").first()
        media_handle.lifecycle_state = JanusHandleLifecycleState.ATTACHING
        media_handle.save(update_fields=["connection", "lifecycle_state", "updated_at"])
        ensure_participant_media_plugin(media_handle)
        media_handle.lifecycle_state = JanusHandleLifecycleState.ATTACHED
        media_handle.last_event_at = timezone.now()
        media_handle.save(update_fields=["janus_session_id", "lifecycle_state", "last_event_at", "updated_at"])
        record_session_event(
            session=participant.session,
            event_type=MeetingEventType.JANUS_HANDLE_ATTACHED,
            actor_profile=participant.profile,
            actor_participant=participant,
            payload={"handle_id": str(media_handle.pk), "handle_type": handle_type},
        )
        attached[handle_type] = media_handle.janus_handle_id
    MeetingLifecycleService.refresh_session_metrics(session=participant.session)
    MeetingSocketEmitter.emit_session_state(session=participant.session)
    return attached


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def detach_participant_media_handles(self, participant_id: str) -> None:
    """Detach all Janus handles associated with a participant."""

    participant = Participant.objects.select_related("session").get(pk=participant_id)
    for handle in participant.media_handles.exclude(janus_handle_id__isnull=True).exclude(janus_handle_id=""):
        call_plugin_method(handle.handle, "detach")
        handle.lifecycle_state = JanusHandleLifecycleState.DETACHED
        handle.last_event_at = timezone.now()
        handle.save(update_fields=["lifecycle_state", "last_event_at", "updated_at"])
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
        payload={"participants": serialized.get("participants", [])},
    )
    MeetingSocketEmitter.emit_session_state(session=session)
    return serialized


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def destroy_janus_room_for_session(self, session_id: str) -> dict:
    """Destroy the Janus VideoRoom for a completed session and stamp cleanup metadata."""

    session = MeetingSession.objects.select_related("room", "started_by_profile").get(pk=session_id)
    result = {}
    if session.janus_room_id:
        control_handle = ensure_session_control_handle(session)
        response = call_plugin_method(
            control_handle,
            "destroy",
            room=session.janus_room_id or str(session.pk),
            secret=session.janus_room_secret or None,
        )
        result = serialize_janus_response(response)
    session.cleanup_completed_at = timezone.now()
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
