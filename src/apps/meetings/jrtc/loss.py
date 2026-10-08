"""Durable projection cleanup; JRTC alone owns session replacement."""
from django.db import transaction
from django.utils import timezone
from apps.meetings.models import ParticipantMediaHandle, ParticipantStream, Participant


def clear_lost_projection(snapshot):
    """CAS protects a newer attach/connection while the worker was awaiting I/O."""
    with transaction.atomic():
        handle = ParticipantMediaHandle.objects.select_for_update().filter(
            pk=snapshot.pk, runtime_owner_id=snapshot.runtime_owner_id,
            connection_id=snapshot.connection_id, janus_session_id=snapshot.janus_session_id,
            janus_handle_id=snapshot.janus_handle_id, runtime_claim_id__isnull=True,
        ).first()
        if handle is None:
            return
        ParticipantStream.objects.filter(media_handle=handle).delete()
        if handle.handle_type == "publisher":
            Participant.objects.filter(pk=handle.participant_id).update(janus_publisher_id=None, janus_private_id=None, updated_at=timezone.now())
        handle.janus_session_id = None
        handle.janus_handle_id = None
        handle.runtime_owner_id = None
        handle.lifecycle_state = "failed"
        handle.selected_streams = []
        handle.janus_state = {}
        handle.last_event_at = timezone.now()
        handle.save(update_fields=["janus_session_id", "janus_handle_id", "runtime_owner_id", "lifecycle_state", "selected_streams", "janus_state", "last_event_at", "updated_at"])
