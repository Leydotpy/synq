"""Report retained legacy work without modifying meetings, schedules or broker data."""

import json

from django.core.management.base import BaseCommand, CommandError
from django_celery_beat.models import PeriodicTask

from apps.meetings.models import (
    GatewayAuthorizationOutbox,
    GatewayCommandRoute,
    JrtcBrowserEventOutbox,
    JrtcEventReceipt,
    MeetingSession,
    ParticipantConnection,
    ParticipantMediaHandle,
)
from apps.meetings.services.gateway_commands import OPEN


class Command(BaseCommand):
    help = (
        "Count unresolved database records before retiring the legacy meeting workers."
    )

    def handle(self, *args, **options):
        legacy = MeetingSession.objects.filter(signalling_backend="legacy")
        retired_tasks = (
            "provision_janus_room_for_session",
            "attach_participant_media_handles",
            "detach_participant_media_handles",
            "sync_janus_participants",
            "destroy_janus_room_for_session",
            "cleanup_finished_sessions",
            "mark_stale_connections",
            "recover_stale_provisioning_sessions",
        )
        counts = {
            "live_legacy_sessions": legacy.live().count(),
            "unconfirmed_legacy_cleanup": legacy.filter(
                cleanup_completed_at__isnull=True
            ).count(),
            "open_socket_connections": ParticipantConnection.objects.exclude(
                status__in=("disconnected", "stale")
            ).count(),
            "unconfirmed_media_handles": ParticipantMediaHandle.objects.exclude(
                lifecycle_state="detached"
            ).count(),
            "unreleased_legacy_routes": GatewayCommandRoute.objects.filter(
                session__in=legacy, released=False
            ).count(),
            "open_legacy_authorization_intents": GatewayAuthorizationOutbox.objects.filter(
                session__in=legacy, status__in=OPEN
            ).count(),
            "unfinished_event_receipts": JrtcEventReceipt.objects.exclude(
                status="processed"
            ).count(),
            "undelivered_browser_events": JrtcBrowserEventOutbox.objects.exclude(
                status__in=("delivered", "discarded")
            ).count(),
            "enabled_retired_schedules": PeriodicTask.objects.filter(
                enabled=True,
                task__in=[f"apps.meetings.tasks.{name}" for name in retired_tasks],
            ).count(),
        }
        self.stdout.write(json.dumps(counts, sort_keys=True))
        if any(counts.values()):
            raise CommandError("Legacy database work requires operator reconciliation.")
