"""Signed observation inbox, authority fences, ordering, DLQ and commit-before-ACK."""

import copy
import uuid
from datetime import timedelta
from io import StringIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from asgiref.sync import async_to_sync
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import OperationalError
from django.test import (
    TestCase,
    TransactionTestCase,
    override_settings,
    skipUnlessDBFeature,
)
from django.utils import timezone

from apps.meetings.models import (
    GatewayCommandRoute,
    GatewayGrant,
    GatewayLifecycleQuarantine,
    GatewayLifecycleReceipt,
    GatewaySessionObservation,
    ParticipantStatus,
)
from apps.meetings.services.gateway_event_worker import GatewayEventWorker
from apps.meetings.services.gateway_events import receive_event
from apps.meetings.tests import test_gateway_ticket_races as race_support
from apps.meetings.tests.test_gateway_ownership import OwnershipFixture
from jrtc_signalling.bus import BusError
from jrtc_signalling.lifecycle_events import envelope, snapshot


class EventFixture(OwnershipFixture):
    def setUp(self):
        super().setUp()
        self.request()
        _, self.claims = self.issue()

    def event(self, sequence=1, state="SIGNALING_CONNECTED", **changes):
        kinds = {
            "OFFLINE": (
                "participant.signaling_disconnected",
                {"reason_code": "normal"},
            ),
            "LOBBY_CONNECTED": ("participant.lobby_connected", {}),
            "SIGNALING_CONNECTED": ("participant.signaling_connected", {}),
            "MEDIA_CONNECTED": ("participant.media_connected", {"media_kind": "audio"}),
        }
        kind, data = kinds[state]
        body = envelope(
            {
                "schema_version": 1,
                "meeting_id": str(self.session.pk),
                "participant_id": str(self.participant.pk),
                "participant_session_id": self.claims["participant_session_id"],
                "generation": self.claims["generation"],
                "gateway_owner_id": "owner-a",
                "gateway_generation": 1,
                "authorization_epoch": self.claims["authorization_epoch"],
                "sequence": sequence - 1,
                "trace_id": str(uuid.uuid4()),
            },
            kind,
            snapshot(state, audio=state == "MEDIA_CONNECTED"),
            data,
        )
        body.update(changes)
        return body

    def receive(self, body):
        producer, verified = self.bus.verifier.verify(
            self.owner.sign(body, "lifecycle-event"), "lifecycle-event"
        )
        return receive_event(producer, verified)

    def observed(self):
        return GatewaySessionObservation.objects.get(participant=self.participant).envelope


class GatewayEventTests(EventFixture, TestCase):
    def test_duplicate_reorder_and_gap_reconcile_without_permission_mutation(self):
        original = self.participant.__dict__.copy()
        first = self.event()
        self.assertEqual(self.receive(first), "recorded")
        self.assertEqual(self.receive(first), "duplicate")
        newer = self.event(4, "MEDIA_CONNECTED")
        self.assertEqual(self.receive(newer), "reconciled")
        self.assertEqual(self.receive(self.event(2, "OFFLINE")), "stale")
        self.assertEqual(self.observed(), newer)
        self.assertEqual(GatewaySessionObservation.objects.get().reconciled_gaps, 2)
        self.participant.refresh_from_db()
        for name in (
            "status",
            "role",
            "joined_at",
            "authorization_epoch",
            "can_publish_audio",
            "can_publish_video",
            "can_share_screen",
        ):
            self.assertEqual(getattr(self.participant, name), original[name])
        self.assertEqual(GatewayLifecycleReceipt.objects.count(), 3)

    def test_terminal_snapshot_seals_generation(self):
        offline = self.event(3, "OFFLINE")
        self.assertEqual(self.receive(offline), "reconciled")
        self.assertEqual(self.receive(self.event(4, "MEDIA_CONNECTED")), "stale")
        self.assertEqual(self.observed(), offline)

    def test_new_grant_and_resume_generation_reject_old_disconnect(self):
        old = self.event(2, "OFFLINE")
        self.receive(self.event())
        _, self.claims = self.issue(
            purpose="resume",
            participant_session_id=self.claims["participant_session_id"],
            generation=self.claims["generation"],
        )
        fresh = self.event()
        self.assertEqual(self.receive(fresh), "recorded")
        self.assertEqual(self.receive(old), "stale")
        self.assertEqual(self.observed(), fresh)
        _, self.claims = self.issue()
        newest = self.event()
        self.assertEqual(self.receive(newest), "recorded")
        self.assertEqual(self.receive(old), "duplicate")
        self.assertEqual(self.observed(), newest)

    def test_retired_owner_cannot_rollback_replacement(self):
        old = self.event(2, "OFFLINE")
        self.receive(self.event())
        self.request("release", 1)
        self.incarnation = str(uuid.uuid4())
        self.assertEqual(self.request()["generation"], 2)
        fresh = self.event(gateway_generation=2)
        self.assertEqual(self.receive(fresh), "recorded")
        self.assertEqual(self.receive(old), "stale")
        self.assertEqual(self.observed(), fresh)

    def test_stale_epoch_and_revocation_cannot_promote_presence(self):
        event = self.event(state="MEDIA_CONNECTED")
        self.participant.authorization_epoch += 1
        self.participant.status = ParticipantStatus.REMOVED
        self.participant.save()
        self.assertEqual(self.receive(event), "stale")
        self.assertFalse(GatewaySessionObservation.objects.exists())
        self.participant.refresh_from_db()
        self.assertEqual(self.participant.status, ParticipantStatus.REMOVED)

    def test_waiting_observation_does_not_admit_and_rejects_media(self):
        intent = self.waiting()
        _, self.claims = self.issue(self.guest)
        self.participant = GatewayGrant.objects.get(
            participant_session_id=self.claims["participant_session_id"]
        ).participant
        self.assertEqual(self.receive(self.event(state="LOBBY_CONNECTED")), "recorded")
        self.assertEqual(self.receive(self.event(2, "MEDIA_CONNECTED")), "stale")
        self.participant.refresh_from_db()
        intent.refresh_from_db()
        self.assertEqual(self.participant.status, ParticipantStatus.WAITING)
        self.assertEqual(intent.status, "pending")
        self.assertIsNone(self.participant.joined_at)

    def test_source_association_future_time_and_legacy_cohort_quarantine(self):
        mutations = [
            {"gateway_owner_id": "owner-b"},
            {"gateway_generation": 100},
            {"participant_id": str(uuid.uuid4())},
            {"meeting_id": str(self.legacy_session.pk)},
            {"occurred_at": (timezone.now() + timedelta(minutes=1)).isoformat()},
        ]
        for changes in mutations:
            with self.subTest(changes=list(changes)), self.assertRaises(BusError):
                self.receive(self.event(**changes))
        self.assertFalse(GatewaySessionObservation.objects.exists())

    def test_expired_lease_rejects_online_but_accepts_terminal_replay(self):
        GatewayCommandRoute.objects.filter(session=self.session).update(
            lease_expires_at=timezone.now() - timedelta(seconds=1)
        )
        self.assertEqual(self.receive(self.event()), "stale")
        self.assertEqual(self.receive(self.event(2, "OFFLINE")), "reconciled")
        self.assertEqual(self.observed()["snapshot"]["connection"], "OFFLINE")

    def test_conflicting_ids_and_sequence_reject_without_mutation(self):
        body = self.event()
        self.receive(body)
        conflicting = copy.deepcopy(body)
        conflicting["trace_id"] = "different"
        with self.assertRaises(BusError):
            self.receive(conflicting)
        with self.assertRaises(BusError):
            self.receive(self.event())
        self.assertEqual(self.observed(), body)

    def test_inbox_is_bounded_and_cursor_survives_receipt_pruning(self):
        first = self.event()
        self.receive(first)
        GatewayLifecycleReceipt.objects.bulk_create(
            [
                GatewayLifecycleReceipt(
                    participant=self.participant, digest="0" * 64, disposition="stale"
                )
                for _ in range(260)
            ]
        )
        latest = self.event(300)
        self.receive(latest)
        self.assertEqual(GatewayLifecycleReceipt.objects.count(), 256)
        self.assertEqual(self.receive(first), "stale")
        self.assertEqual(self.observed(), latest)

    def test_ack_failure_replays_committed_receipt_and_poison_has_hash_only_dlq(self):
        worker = GatewayEventWorker(self.bus)
        acknowledgement = SimpleNamespace(ack=AsyncMock(side_effect=RuntimeError("ack lost")))
        event = self.event()
        delivery = SimpleNamespace(
            payload=self.owner.sign(event, "lifecycle-event"),
            acknowledgement=acknowledgement,
        )
        async_to_sync(worker.handle)(delivery)
        self.assertEqual(self.observed(), event)
        acknowledgement.ack.side_effect = None
        async_to_sync(worker.handle)(delivery)
        self.assertEqual(worker.counters["duplicate"], 1)
        delivery.payload = b'{"ticket":"private-poison"}'
        async_to_sync(worker.handle)(delivery)
        self.assertEqual(worker.counters["quarantined"], 1)
        row = GatewayLifecycleQuarantine.objects.get()
        self.assertEqual(len(row.digest), 64)
        self.assertNotIn("private-poison", str(row.__dict__))

    def test_database_failure_never_acknowledges_and_rolls_back(self):
        worker = GatewayEventWorker(self.bus)
        ack = AsyncMock()
        delivery = SimpleNamespace(
            payload=self.owner.sign(self.event(), "lifecycle-event"),
            acknowledgement=SimpleNamespace(ack=ack),
        )
        with patch.object(
            GatewayLifecycleReceipt.objects,
            "create",
            side_effect=OperationalError("db unavailable"),
        ):
            async_to_sync(worker.handle)(delivery)
        ack.assert_not_awaited()
        self.assertFalse(GatewaySessionObservation.objects.exists())
        async_to_sync(worker.handle)(delivery)
        ack.assert_awaited_once()

    @override_settings(GATEWAY_EVENTS_ENABLED=False)
    def test_worker_disabled_by_default(self):
        with self.assertRaisesMessage(CommandError, "disabled"):
            call_command("run_gateway_events", stdout=StringIO())


@skipUnlessDBFeature("has_select_for_update")
class GatewayEventRaceTests(EventFixture, TransactionTestCase):
    race = race_support.GatewayTicketRaceTests.race

    def test_duplicate_workers_commit_once(self):
        event = self.event()
        self.assertCountEqual(
            self.race(lambda: self.receive(event), lambda: self.receive(event)),
            ["recorded", "duplicate"],
        )
        self.assertEqual(GatewayLifecycleReceipt.objects.count(), 1)

    def test_concurrent_reordered_snapshots_keep_highest_sequence(self):
        older, newer = self.event(2, "MEDIA_CONNECTED"), self.event(3, "OFFLINE")
        self.race(lambda: self.receive(older), lambda: self.receive(newer))
        self.assertEqual(self.observed(), newer)

    def test_revocation_and_observation_share_authority_lock(self):
        from apps.meetings.services.lifecycle import MeetingLifecycleService

        event = self.event(state="MEDIA_CONNECTED")

        def revoke():
            MeetingLifecycleService.update_participant_permissions(
                session=self.session,
                actor_profile=self.host,
                participant=self.participant,
                updates={"can_publish_audio": False},
            )
            return "revoked"

        self.assertIn("revoked", self.race(revoke, lambda: self.receive(event)))
        self.participant.refresh_from_db()
        self.assertFalse(self.participant.can_publish_audio)
        # Even a later, larger sequence cannot overwrite after the authority epoch changed.
        self.assertEqual(self.receive(self.event(2, "MEDIA_CONNECTED")), "stale")
