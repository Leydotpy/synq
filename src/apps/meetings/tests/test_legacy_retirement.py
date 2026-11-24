"""Regression checks for historical data and queued work at the retirement boundary."""

from datetime import timedelta
from io import StringIO
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.core.management import call_command, CommandError
from django.test import TestCase, TransactionTestCase
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.models import (
    JrtcBrowserEventOutbox,
    JrtcEventReceipt,
    MeetingSession,
    ParticipantMediaHandle,
)
from apps.meetings.services.gateway_tickets import GatewayDenied, check_ticket
from apps.meetings.services.lifecycle import MeetingLifecycleService as Lifecycle
from apps.meetings.tasks import expire_pending_join_requests
from apps.meetings.tests.test_gateway_tickets import GatewayTicketFixture


class RetirementTests(GatewayTicketFixture, TestCase):
    def test_inventory_preserves_pending_work_and_reconciliation_disables_named_tasks(
        self,
    ):
        receipt = JrtcEventReceipt.objects.create(
            event_id=uuid4(), event_type="fixture"
        )
        row = JrtcBrowserEventOutbox.objects.create(
            receipt=receipt, dispatch_index=0, socket_id="fixture", payload={}
        )
        interval = IntervalSchedule.objects.create(every=30, period="seconds")
        schedule = PeriodicTask.objects.create(
            name="locally renamed sweep",
            interval=interval,
            task="apps.meetings.tasks.mark_stale_connections",
        )
        with self.assertRaises(CommandError):
            call_command("check_legacy_signalling_drain", stdout=StringIO())
        self.assertTrue(PeriodicTask.objects.get(pk=schedule.pk).enabled)
        call_command("reconcile_celery_schedule", stdout=StringIO())
        self.assertFalse(PeriodicTask.objects.get(pk=schedule.pk).enabled)
        row.refresh_from_db()
        self.assertEqual(row.status, "pending")
        JrtcBrowserEventOutbox.objects.filter(pk=row.pk).update(status="delivered")
        JrtcEventReceipt.objects.filter(pk=receipt.pk).update(status="processed")
        call_command("check_legacy_signalling_drain", stdout=StringIO())

    def test_terminal_legacy_room_needs_confirmed_cleanup_before_reuse(self):
        room = Lifecycle.create_room(
            creator_profile=self.host, title="Historical meeting"
        )
        old = MeetingSession.objects.create(
            room=room,
            started_by_profile=self.host,
            signalling_backend="legacy",
            lifecycle_state="ended",
            ended_at=timezone.now(),
        )
        with self.assertRaisesRegex(MeetingDomainError, "confirmed cleanup"):
            Lifecycle.start_session(room=room, started_by_profile=self.host)
        old.cleanup_completed_at = timezone.now()
        old.save(update_fields=["cleanup_completed_at"])
        fresh = Lifecycle.start_session(room=room, started_by_profile=self.host)
        self.assertEqual(fresh.signalling_backend, "gateway")
        old.refresh_from_db()
        self.assertEqual(old.signalling_backend, "legacy")
        self.assertNotEqual(old.pk, fresh.pk)

    def test_expiry_revokes_a_waiting_ticket_and_records_one_command(self):
        intent = self.waiting()
        _, claims = self.issue(self.guest)
        self.session.join_requests.filter(pk=intent.pk).update(
            created_at=timezone.now() - timedelta(days=1)
        )
        self.assertEqual(expire_pending_join_requests(), 1)
        self.assertEqual(expire_pending_join_requests(), 0)
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="session.hello", consume=True)
        self.assertEqual(
            self.session.gateway_outbox.filter(
                participant__profile=self.guest, action="participant.disconnect"
            ).count(),
            1,
        )


class RetirementUpgradeTests(TransactionTestCase):
    def test_upgrade_preserves_historical_session_and_undelivered_work(self):
        from django.db import connection
        from django.db.migrations.executor import MigrationExecutor

        previous = [("meetings", "0021_gateway_room_canary")]
        current = MigrationExecutor(connection).loader.graph.leaf_nodes()
        self.addCleanup(lambda: MigrationExecutor(connection).migrate(current))
        before = MigrationExecutor(connection).migrate(previous).apps
        owner = (
            get_user_model()
            .objects.create_user(
                username="retirement-upgrade",
                email="upgrade@example.invalid",
                clerk_user_id="retirement-upgrade",
            )
            .profile
        )
        room = before.get_model("meetings", "MeetingRoom").objects.create(
            title="Historical meeting", created_by_profile_id=owner.pk
        )
        old = before.get_model("meetings", "MeetingSession").objects.create(
            room=room, started_by_profile_id=owner.pk, janus_room_id=482
        )
        participant = before.get_model("meetings", "Participant").objects.create(
            session=old, room=room, profile_id=owner.pk, display_name="Fixture"
        )
        socket = before.get_model("meetings", "ParticipantConnection").objects.create(
            socket_id="upgrade-fixture",
            profile_id=owner.pk,
            participant=participant,
            session=old,
            status="active",
        )
        handle = before.get_model("meetings", "ParticipantMediaHandle").objects.create(
            participant=participant,
            connection=socket,
            handle_type="publisher",
            janus_handle_id=97,
            lifecycle_state="ready",
        )
        receipt = before.get_model("meetings", "JrtcEventReceipt").objects.create(
            event_id=uuid4(), event_type="fixture"
        )
        queued = before.get_model("meetings", "JrtcBrowserEventOutbox").objects.create(
            receipt=receipt, dispatch_index=0, socket_id=socket.socket_id, payload={}
        )
        self.assertEqual(old.signalling_backend, "legacy")

        MigrationExecutor(connection).migrate(current)
        saved = MeetingSession.objects.get(pk=old.pk)
        self.assertEqual(
            (saved.signalling_backend, saved.janus_room_id), ("legacy", 482)
        )
        stored_handle = ParticipantMediaHandle.objects.get(pk=handle.pk)
        self.assertEqual(
            (stored_handle.connection_id, stored_handle.janus_handle_id),
            (socket.pk, 97),
        )
        pending = JrtcBrowserEventOutbox.objects.get(pk=queued.pk)
        self.assertEqual((pending.receipt_id, pending.status), (receipt.pk, "pending"))
        self.assertEqual(
            MeetingSession._meta.get_field("signalling_backend").get_default(),
            "gateway",
        )
        with self.assertRaises(CommandError):
            call_command("check_legacy_signalling_drain", stdout=StringIO())
