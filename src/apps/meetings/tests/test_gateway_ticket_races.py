"""Real shared-DB races; SQLite cannot establish row-lock acceptance."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

from django.db import close_old_connections, connections
from django.test import TransactionTestCase, skipUnlessDBFeature

from apps.meetings.models import GatewayTicket
from apps.meetings.services.gateway_tickets import GatewayDenied, check_ticket
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.tests.test_gateway_tickets import GatewayTicketFixture


@skipUnlessDBFeature("has_select_for_update")
class GatewayTicketRaceTests(GatewayTicketFixture, TransactionTestCase):
    def setUp(self):
        self.dispatch = patch("apps.meetings.services.lifecycle.dispatch_task")
        self.emitter = patch("apps.meetings.realtime.emitter.MeetingSocketEmitter")
        self.dispatch.start()
        self.emitter.start()
        self.addCleanup(self.dispatch.stop)
        self.addCleanup(self.emitter.stop)
        super().setUp()

    def race(self, *operations):
        barrier = Barrier(len(operations))

        def run(operation):
            close_old_connections()
            try:
                barrier.wait(timeout=5)
                return operation()
            except GatewayDenied:
                return "denied"
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=len(operations)) as pool:
            return list(pool.map(run, operations))

    def test_only_one_worker_consumes_a_ticket(self):
        _, claims = self.issue()

        def consume():
            check_ticket(claims=claims, operation="session.hello", consume=True)
            return "accepted"

        self.assertCountEqual(self.race(consume, consume), ["accepted", "denied"])
        self.assertIsNotNone(GatewayTicket.objects.get(pk=claims["jti"]).consumed_at)

    def test_permission_change_serializes_with_consumption(self):
        _, claims = self.issue()

        def revoke():
            MeetingLifecycleService.update_participant_permissions(
                session=self.session,
                actor_profile=self.host,
                participant=self.participant,
                updates={"can_publish_audio": False},
            )
            return "revoked"

        def consume():
            check_ticket(claims=claims, operation="session.hello", consume=True)
            return "accepted_before_revoke"

        results = self.race(revoke, consume)
        self.assertIn("revoked", results)
        self.assertTrue("denied" in results or "accepted_before_revoke" in results)
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="media.publish", consume=False)
