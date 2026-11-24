"""Cohort routing, drain evidence and the PostgreSQL first-claim race."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

from django.db import close_old_connections
from django.test import (
    TestCase,
    TransactionTestCase,
    override_settings,
    skipUnlessDBFeature,
)
from django.utils import timezone

from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.models import GatewayAuthorizationOutbox, GatewayCommandRoute
from apps.meetings.services.gateway_cutover import drain, set_cohort
from apps.meetings.services.gateway_tickets import GatewayDenied, issue_ticket
from apps.meetings.services.lifecycle import MeetingLifecycleService as Lifecycle
from apps.meetings.tests.test_gateway_ownership import OwnershipFixture


class CutoverTests(OwnershipFixture, TestCase):
    def start(self, room):
        return Lifecycle.start_session(room=room, started_by_profile=self.host)

    @override_settings(
        GATEWAY_STAGED_ROUTING=True,
        GATEWAY_STATE_ENABLED=True,
        GATEWAY_EVENTS_ENABLED=True,
    )
    def test_dark_allowlist_pause_and_pinned_lease(self):
        room = Lifecycle.create_room(
            creator_profile=self.host,
            title="New staged room",
            feature_flags=self.room.feature_flags,
        )
        with self.assertRaisesRegex(MeetingDomainError, "paused"):
            self.start(room)
        self.assertFalse(room.sessions.exists())
        set_cohort(room.pk, selected=True)
        canary = self.start(room)
        self.assertEqual(canary.signalling_backend, "gateway")
        generation = self.request(meeting_id=str(canary.pk))["generation"]
        set_cohort(room.pk, selected=False)
        self.assertEqual(self.start(room).pk, canary.pk)
        self.assertEqual(
            self.request("renew", generation, meeting_id=str(canary.pk))["generation"],
            generation,
        )
        with self.assertRaises(GatewayDenied):
            issue_ticket(session_id=self.legacy_session.pk, profile_id=self.host.pk)

    @override_settings(GATEWAY_STAGED_ROUTING=True, GATEWAY_EVENTS_ENABLED=False)
    def test_selected_room_fails_closed_when_stack_is_incomplete(self):
        room = Lifecycle.create_room(creator_profile=self.host, title="No events")
        set_cohort(room.pk, selected=True)
        with self.assertRaises(MeetingDomainError):
            self.start(room)
        self.assertFalse(room.sessions.exists())

    def test_route_discovery_requires_identity_and_current_ownership(self):
        def url(session):
            return f"/api/v1/meetings/sessions/{session.pk}/signalling-route/"

        self.client.force_authenticate(None)
        self.assertIn(
            self.client.get(url(self.session), secure=True).status_code, (401, 403)
        )
        self.client.force_authenticate(user=self.host.user)
        self.assertEqual(
            self.client.get(url(self.session), secure=True).status_code, 503
        )
        self.request()
        response = self.client.get(url(self.session), secure=True)
        self.assertEqual(
            response.json(), {"session_id": str(self.session.pk), "backend": "gateway"}
        )
        self.assertIn("no-store", response["Cache-Control"])
        self.assertEqual(
            self.client.get(url(self.legacy_session), secure=True).status_code,
            410,
        )
        self.request("release", 1)
        self.assertEqual(
            self.client.get(url(self.session), secure=True).status_code, 503
        )
        drain(self.room.pk, self.session.pk)
        self.assertEqual(
            self.client.get(url(self.session), secure=True).status_code, 410
        )

    def test_failback_needs_terminal_commands_physical_cleanup_and_clean_release(self):
        self.request()
        drain(self.room.pk, self.session.pk)
        with self.assertRaisesRegex(MeetingDomainError, "pending"):
            self.start(self.room)
        deadline = GatewayCommandRoute.objects.get(
            session=self.session
        ).lease_expires_at
        self.request("renew", 1)
        self.assertEqual(
            GatewayCommandRoute.objects.get(session=self.session).lease_expires_at,
            deadline,
        )
        with self.assertRaises(GatewayDenied):
            self.request("claim")
        GatewayAuthorizationOutbox.objects.filter(session=self.session).update(
            status="timed_out"
        )
        GatewayCommandRoute.objects.filter(session=self.session).update(
            lease_expires_at=timezone.now()
        )
        with self.assertRaisesRegex(MeetingDomainError, "release"):
            self.start(self.room)
        self.request("release", 1)
        with self.assertRaisesRegex(MeetingDomainError, "VideoRoom"):
            self.start(self.room)
        self.assertEqual(self.room.sessions.count(), 1)

    def test_cold_drain_can_retry_without_touching_replacement(self):
        with self.assertRaises(MeetingDomainError):
            drain(self.room.pk, uuid4())
        drain(self.room.pk, self.session.pk)
        drain(self.room.pk, self.session.pk)
        with self.assertRaisesRegex(MeetingDomainError, "paused"):
            self.start(self.room)
        set_cohort(self.room.pk, selected=True)
        replacement = self.start(self.room)
        self.assertEqual(replacement.signalling_backend, "gateway")
        self.assertNotEqual(replacement.pk, self.session.pk)
        with self.assertRaises(MeetingDomainError):
            drain(self.room.pk, self.session.pk)
        self.assertEqual(self.start(self.room).pk, replacement.pk)
        self.assertFalse(
            GatewayAuthorizationOutbox.objects.filter(
                session=self.session, status__in=("pending", "accepted")
            ).exists()
        )


class CutoverRaceTests(OwnershipFixture, TransactionTestCase):
    @skipUnlessDBFeature("has_select_for_update")
    def test_concurrent_first_claim_and_failback_cannot_share_ownership(self):
        barrier = Barrier(2)

        def claim():
            close_old_connections()
            try:
                barrier.wait(timeout=5)
                try:
                    self.request()
                    return True
                except GatewayDenied:
                    return False
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(claim)
            barrier.wait(timeout=5)
            drain(self.room.pk, self.session.pk)
            owned = result.result(timeout=10)
        set_cohort(self.room.pk, selected=True)
        if owned:
            with self.assertRaises(MeetingDomainError):
                Lifecycle.start_session(room=self.room, started_by_profile=self.host)
            self.assertFalse(
                self.room.sessions.filter(signalling_backend="legacy").exists()
            )
        else:
            self.assertEqual(
                Lifecycle.start_session(
                    room=self.room, started_by_profile=self.host
                ).signalling_backend,
                "gateway",
            )
            self.assertFalse(
                GatewayCommandRoute.objects.filter(session=self.session).exists()
            )
