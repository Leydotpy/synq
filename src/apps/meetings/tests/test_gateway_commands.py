"""Durable command/outcome authority, retry, revocation and race regressions."""

import copy
import uuid
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from asgiref.sync import async_to_sync
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from django.db import OperationalError, transaction
from django.test import (
    TestCase,
    TransactionTestCase,
    override_settings,
    skipUnlessDBFeature,
)
from django.utils import timezone

from apps.meetings.models import (
    GatewayAuthorizationOutbox,
    GatewayCommand,
    GatewayCommandOutcome,
    GatewayCommandQuarantine,
    GatewayCommandRoute,
    GatewayGrant,
)
from apps.meetings.services.gateway_commands import (
    authorize_command,
    prepare_delivery,
    publication_finished,
    receive_outcome,
)
from apps.meetings.services.gateway_dispatcher import GatewayCommandWorker
from apps.meetings.services.lifecycle import MeetingLifecycleService as Lifecycle
from apps.meetings.tests import test_gateway_ticket_races as race_support
from apps.meetings.tests.test_gateway_admission import GatewayAdmissionFixture
from jrtc_signalling.bus import (
    BusConfig,
    BusError,
    Signer,
    TrustedKey,
    Verifier,
)
from jrtc_signalling.command_store import outcome


class GatewayCommandFixture(GatewayAdmissionFixture):
    def setUp(self):
        super().setUp()
        flags = override_settings(GATEWAY_COMMANDS_ENABLED=True)
        flags.enable()
        self.addCleanup(flags.disable)
        self.intent()
        self.decision()
        self.row = GatewayAuthorizationOutbox.objects.get()
        self.now = timezone.now()
        self.route = GatewayCommandRoute.objects.create(
            session=self.session,
            owner_id="owner-1",
            owner_generation=3,
            lease_expires_at=self.now + timedelta(minutes=10),
        )
        self.django_key, self.worker_key = (
            Ed25519PrivateKey.generate(),
            Ed25519PrivateKey.generate(),
        )
        self.config = BusConfig(
            "redis://127.0.0.1:6379/15",
            Signer("django-control", "django-key", self.django_key),
            Verifier(
                {"worker-key": TrustedKey("owner-1", self.worker_key.public_key())}
            ),
        )

    def claim(self, **kwargs):
        return prepare_delivery(self.row.pk, now=kwargs.get("now", self.now))

    def event(self, claim, status="applied", reason="normal"):
        return outcome(claim["envelope"], status, reason, self.now.timestamp())


class GatewayCommandTests(GatewayCommandFixture, TestCase):
    def test_preparation_commits_exact_owner_epoch_binding_and_lease(self):
        claim = self.claim()
        self.assertEqual(claim["envelope"]["authorization_epoch"], 1)
        self.assertEqual(claim["envelope"]["expected_owner_generation"], 3)
        self.assertEqual(claim["envelope"]["trace_id"], str(self.row.pk))
        self.assertEqual(
            claim["envelope"]["participant_session_id"],
            str(
                GatewayGrant.objects.get(
                    participant=self.guest_participant
                ).participant_session_id
            ),
        )
        self.assertTrue(authorize_command(claim["envelope"], now=self.now))
        self.assertIsNone(self.claim())
        self.row.refresh_from_db()
        self.assertEqual(self.row.status, "pending")

    def test_failed_publication_and_lost_publish_ack_reuse_immutable_command(self):
        claim = self.claim()
        publication_finished(claim, success=False, now=self.now)
        second = self.claim(now=self.now + timedelta(seconds=3))
        self.assertEqual(second["envelope"], claim["envelope"])
        # A process dies after publishing but before releasing its lease.
        restarted = self.claim(now=self.now + timedelta(seconds=14))
        self.assertEqual(restarted["command_id"], claim["command_id"])
        self.assertEqual(restarted["envelope"], claim["envelope"])
        publication_finished(second, success=True, now=self.now)
        self.row.refresh_from_db()
        self.assertEqual(self.row.lease_id, restarted["lease_id"])
        self.assertEqual(GatewayCommand.objects.count(), 1)

    def test_no_owner_backoff_retains_intent_then_dead_letters(self):
        self.route.delete()
        for attempt in range(10):
            self.assertIsNone(
                self.claim(now=self.now + timedelta(seconds=30 * attempt))
            )
        self.assertIsNone(self.claim(now=self.now + timedelta(seconds=299)))
        self.row.refresh_from_db()
        self.assertEqual(self.row.status, "failed")
        self.assertEqual(self.row.last_error, "owner_lost")
        self.assertEqual(self.row.delivery_attempts, 10)
        self.assertFalse(GatewayCommand.objects.exists())

    def test_expiry_does_not_extend_on_retry(self):
        claim = self.claim()
        publication_finished(claim, success=True, now=self.now)
        self.assertIsNone(self.claim(now=self.now + timedelta(seconds=61)))
        self.row.refresh_from_db()
        self.assertEqual(self.row.status, "timed_out")
        self.assertFalse(
            authorize_command(claim["envelope"], now=self.now + timedelta(seconds=61))
        )

    def test_owner_change_and_binding_change_cannot_retarget_same_command(self):
        claim = self.claim()
        publication_finished(claim, success=True, now=self.now)
        self.route.owner_generation = 4
        self.route.save()
        self.assertFalse(authorize_command(claim["envelope"], now=self.now))
        self.assertIsNone(self.claim(now=self.now + timedelta(seconds=3)))
        self.row.refresh_from_db()
        self.assertEqual(self.row.status, "failed")
        self.assertEqual(GatewayCommand.objects.get().envelope, claim["envelope"])

    def test_new_connection_binding_fences_old_command(self):
        claim = self.claim()
        GatewayGrant.objects.filter(participant=self.guest_participant).update(
            participant_session_id=uuid.uuid4()
        )
        self.assertFalse(authorize_command(claim["envelope"], now=self.now))

    def test_revocation_supersedes_old_intent_before_send_or_authority_check(self):
        claim = self.claim()
        publication_finished(claim, success=True, now=self.now)
        Lifecycle.remove_participant(
            session=self.session,
            actor_profile=self.host,
            participant=self.guest_participant,
        )
        self.assertFalse(authorize_command(claim["envelope"], now=self.now))
        self.assertIsNone(self.claim(now=self.now + timedelta(seconds=3)))
        self.row.refresh_from_db()
        self.assertEqual(self.row.status, "superseded")
        self.assertTrue(
            GatewayAuthorizationOutbox.objects.filter(
                action="participant.disconnect"
            ).exists()
        )

    def test_preparation_rollback_creates_no_command_or_lease(self):
        with transaction.atomic():
            self.assertIsNotNone(self.claim())
            transaction.set_rollback(True)
        self.assertFalse(GatewayCommand.objects.exists())
        self.row.refresh_from_db()
        self.assertIsNone(self.row.lease_id)

    def test_outcomes_are_idempotent_and_cannot_roll_back_terminal_state(self):
        claim = self.claim()
        applied = self.event(claim)
        self.assertEqual(receive_outcome("owner-1", applied, now=self.now), "recorded")
        self.assertEqual(receive_outcome("owner-1", applied, now=self.now), "duplicate")
        accepted = self.event(claim, "accepted", "requested")
        self.assertEqual(receive_outcome("owner-1", accepted, now=self.now), "stale")
        publication_finished(claim, success=False)
        self.row.refresh_from_db()
        self.assertEqual(self.row.status, "applied")
        self.assertEqual(self.row.last_error, "normal")
        self.assertEqual(GatewayCommandOutcome.objects.count(), 2)
        self.guest_participant.refresh_from_db()
        self.assertEqual(self.guest_participant.status, "admitted")
        self.assertIsNone(self.guest_participant.joined_at)

    def test_accepted_never_claims_applied_and_failure_is_visible(self):
        claim = self.claim()
        receive_outcome(
            "owner-1", self.event(claim, "accepted", "requested"), now=self.now
        )
        publication_finished(claim, success=True, now=self.now)
        self.row.refresh_from_db()
        self.assertEqual(self.row.status, "accepted")
        receive_outcome(
            "owner-1", self.event(claim, "failed", "owner_lost"), now=self.now
        )
        self.row.refresh_from_db()
        self.assertEqual(
            (self.row.status, self.row.last_error), ("failed", "owner_lost")
        )

    def test_wrong_owner_epoch_generation_trace_and_conflicting_event_reject(self):
        claim = self.claim()
        event = self.event(claim)
        for field, value in [
            ("gateway_owner_id", "other"),
            ("gateway_generation", 4),
            ("authorization_epoch", 2),
            ("trace_id", str(uuid.uuid4())),
            ("meeting_id", str(uuid.uuid4())),
        ]:
            with self.subTest(field=field), self.assertRaises(BusError):
                receive_outcome("owner-1", {**event, field: value}, now=self.now)
        with self.assertRaises(BusError):
            receive_outcome("untrusted-owner", event, now=self.now)
        receive_outcome("owner-1", event, now=self.now)
        with self.assertRaises(BusError):
            receive_outcome("owner-1", {**event, "status": "failed"}, now=self.now)

    def test_delayed_outcome_after_revocation_cannot_restore_authority(self):
        claim = self.claim()
        Lifecycle.remove_participant(
            session=self.session,
            actor_profile=self.host,
            participant=self.guest_participant,
        )
        self.assertEqual(
            receive_outcome("owner-1", self.event(claim), now=self.now), "stale"
        )
        self.row.refresh_from_db()
        self.guest_participant.refresh_from_db()
        self.assertEqual(self.row.status, "superseded")
        self.assertEqual(self.guest_participant.status, "removed")

    def test_termination_has_independent_meeting_epoch_and_null_participant(self):
        Lifecycle.end_session(session=self.session, actor_profile=self.host)
        Lifecycle.end_session(session=self.session, actor_profile=self.host)
        terminated = GatewayAuthorizationOutbox.objects.get(action="meeting.terminate")
        claim = prepare_delivery(terminated.pk, now=timezone.now())
        self.assertIsNone(claim["envelope"]["participant_id"])
        self.assertIsNone(claim["envelope"]["participant_session_id"])
        self.assertEqual(claim["envelope"]["authorization_epoch"], 1)
        self.assertTrue(authorize_command(claim["envelope"]))

    def test_worker_publish_failure_keeps_durable_intent(self):
        worker = GatewayCommandWorker(self.config)
        worker.transport = SimpleNamespace(
            publish=AsyncMock(side_effect=OSError("private endpoint"))
        )
        async_to_sync(worker.sweep)()
        self.row.refresh_from_db()
        self.assertEqual(
            (self.row.status, self.row.last_error), ("pending", "unavailable")
        )
        self.assertIsNone(self.row.lease_id)
        self.assertEqual(GatewayCommand.objects.count(), 1)

    def test_worker_only_acks_after_durable_outcome_and_rejects_tampering(self):
        claim = self.claim()
        worker = GatewayCommandWorker(self.config)
        event = self.event(claim)
        signer = Signer("owner-1", "worker-key", self.worker_key)
        packet = signer.sign(event, "command-outcome")
        ack = AsyncMock()
        message = SimpleNamespace(
            payload=packet, acknowledgement=SimpleNamespace(ack=ack)
        )
        with patch(
            "apps.meetings.services.gateway_dispatcher.receive_outcome",
            side_effect=OperationalError("offline"),
        ):
            async_to_sync(worker.handle)(message)
        ack.assert_not_awaited()
        async_to_sync(worker.handle)(message)
        ack.assert_awaited_once()
        self.assertEqual(GatewayCommandOutcome.objects.count(), 1)
        message.payload = packet.replace(b'"applied"', b'"failed"')
        async_to_sync(worker.handle)(message)
        self.assertEqual(GatewayCommandQuarantine.objects.count(), 1)

    def test_feature_disable_and_mutated_command_fail_closed(self):
        with override_settings(GATEWAY_COMMANDS_ENABLED=False):
            self.assertIsNone(self.claim())
        claim = self.claim()
        changed = copy.deepcopy(claim["envelope"])
        changed["authorization_epoch"] = 9
        self.assertFalse(authorize_command(changed))
        with override_settings(GATEWAY_COMMANDS_ENABLED=False):
            self.assertFalse(authorize_command(claim["envelope"]))

    def test_http_authority_and_status_do_not_trust_browser_identity(self):
        claim = self.claim()
        url = "/api/v1/meetings/internal/gateway/commands/authorize/"
        self.client.force_authenticate(user=None)
        self.assertEqual(
            self.client.post(
                url, {"command": claim["envelope"]}, format="json", secure=True
            ).status_code,
            401,
        )
        token = "test-service-" + "x" * 32
        result = self.client.post(
            url,
            {"command": claim["envelope"]},
            format="json",
            secure=True,
            HTTP_AUTHORIZATION="Bearer " + token,
        )
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json(), {"authorized": True})
        status_url = f"/api/v1/meetings/sessions/{self.session.pk}/gateway/commands/{self.row.pk}/"
        self.client.force_authenticate(self.host.user)
        result = self.client.get(status_url, secure=True)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["enforcement_status"], "pending")
        self.assertNotIn("envelope", result.json())
        self.client.force_authenticate(self.profile("outsider").user)
        self.assertEqual(self.client.get(status_url, secure=True).status_code, 403)


@skipUnlessDBFeature("has_select_for_update")
class GatewayCommandRaceTests(GatewayCommandFixture, TransactionTestCase):
    race = race_support.GatewayTicketRaceTests.race

    def setUp(self):
        for target in (
            "apps.meetings.services.lifecycle.dispatch_task",
            "apps.meetings.realtime.emitter.MeetingSocketEmitter",
        ):
            mock = patch(target)
            mock.start()
            self.addCleanup(mock.stop)
        super().setUp()

    def test_two_dispatchers_claim_one_command(self):
        results = self.race(self.claim, self.claim)
        self.assertEqual(sum(result is not None for result in results), 1)
        self.assertEqual(GatewayCommand.objects.count(), 1)

    def test_outcome_racing_revocation_never_reauthorizes_participant(self):
        claim = self.claim()

        def remove():
            Lifecycle.remove_participant(
                session=self.session,
                actor_profile=self.host,
                participant=self.guest_participant,
            )
            return "removed"

        self.assertIn(
            "removed",
            self.race(
                remove,
                lambda: receive_outcome("owner-1", self.event(claim), now=self.now),
            ),
        )
        self.guest_participant.refresh_from_db()
        self.assertEqual(self.guest_participant.status, "removed")
        self.assertFalse(authorize_command(claim["envelope"]))
