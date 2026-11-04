"""Immutable cohort and shared-database ownership/replay fences."""

import json
import time
import uuid
from datetime import timedelta
from unittest.mock import patch

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from django.test import (
    TestCase,
    TransactionTestCase,
    override_settings,
    skipUnlessDBFeature,
)
from django.utils import timezone
from jrtc_signalling.bus import BusConfig, Signer, TrustedKey, Verifier
from rest_framework.test import APIClient

from apps.meetings.exceptions import JanusGatewayError
from apps.meetings.models import (
    GatewayCommandRoute,
    GatewayOwnerGeneration,
    MeetingSession,
    ParticipantStatus,
)
from apps.meetings.services.gateway_commands import prepare_delivery
from apps.meetings.services.gateway_ownership import ownership_request
from apps.meetings.services.gateway_tickets import GatewayDenied
from apps.meetings.services.janus import (
    call_video_room_management_method,
    resolve_janus_session,
)
from apps.meetings.services.lifecycle import MeetingLifecycleService as Lifecycle
from apps.meetings.services.signaling import _prepare_media_handle
from apps.meetings.tests import test_gateway_ticket_races as race_support
from apps.meetings.tests.test_gateway_admission import GatewayAdmissionFixture


class OwnershipFixture(GatewayAdmissionFixture):
    def setUp(self):
        self.dispatch = patch("apps.meetings.services.lifecycle.dispatch_task")
        self.dispatcher = self.dispatch.start()
        self.addCleanup(self.dispatch.stop)
        super().setUp()
        self.legacy_session = self.session
        flags = override_settings(
            GATEWAY_RUNTIME_ENABLED=True, GATEWAY_COMMANDS_ENABLED=True
        )
        flags.enable()
        self.addCleanup(flags.disable)
        self.room = Lifecycle.create_room(
            creator_profile=self.host, title="Isolated gateway"
        )
        self.room.feature_flags = {
            "gateway_control_plane": True,
            "gateway_runtime": True,
        }
        self.room.save(update_fields=["feature_flags"])
        self.dispatcher.reset_mock()
        self.session = Lifecycle.start_session(
            room=self.room, started_by_profile=self.host
        )
        self.participant = self.session.participants.get(profile=self.host)
        private = Ed25519PrivateKey.generate()
        self.owner = Signer("owner-a", "owner-key", private)
        self.other = Signer("owner-b", "other-key", private)
        self.incarnation = str(uuid.uuid4())
        self.bus = BusConfig(
            "redis://127.0.0.1:6379/15",
            Signer("django-control", "django-key", private),
            Verifier(
                {
                    "owner-key": TrustedKey("owner-a", private.public_key()),
                    "other-key": TrustedKey("owner-b", private.public_key()),
                }
            ),
        )

    def packet(self, action="claim", generation=0, **changes):
        body = dict(
            request_id=str(uuid.uuid4()),
            meeting_id=str(self.session.pk),
            incarnation=self.incarnation,
            generation=generation,
            action=action,
            issued_at=int(time.time()),
        )
        body.update(changes)
        return self.owner.sign(body, "owner-request")

    def request(self, action="claim", generation=0, **changes):
        return ownership_request(
            self.packet(action, generation, **changes), config=self.bus
        )


class OwnershipTests(OwnershipFixture, TestCase):
    def test_cohort_is_fixed_at_creation_and_host_is_not_media_present(self):
        self.assertEqual(self.session.signalling_backend, "gateway")
        self.assertEqual(self.participant.status, ParticipantStatus.ADMITTED)
        self.assertIsNone(self.participant.joined_at)
        self.dispatcher.assert_not_called()
        self.assertFalse(self.participant.media_handles.exists())
        self.session.signalling_backend = "legacy"
        with self.assertRaises(ValueError):
            self.session.save()
        with self.assertRaises(ValueError):
            MeetingSession.objects.filter(pk=self.session.pk).update(
                signalling_backend="legacy"
            )

    def test_existing_legacy_session_does_not_convert_when_flags_change(self):
        self.legacy_session.room.feature_flags = {
            "gateway_control_plane": True,
            "gateway_runtime": True,
        }
        self.legacy_session.room.save(update_fields=["feature_flags"])
        existing = Lifecycle.start_session(
            room=self.legacy_session.room, started_by_profile=self.host
        )
        self.assertEqual(existing.pk, self.legacy_session.pk)
        self.assertEqual(existing.signalling_backend, "legacy")
        with self.assertRaises(GatewayDenied):
            self.request(meeting_id=str(existing.pk))

    def test_claim_retry_renew_and_clean_restart_advance_generation(self):
        first = self.request()
        route = GatewayCommandRoute.objects.get(session=self.session)
        deadline = route.lease_expires_at
        self.assertEqual(self.request()["generation"], first["generation"])
        route.refresh_from_db()
        self.assertEqual(route.lease_expires_at, deadline)
        self.assertEqual(self.request("renew", 1)["generation"], 1)
        self.assertEqual(self.request("release", 1)["lease_seconds"], 0)
        with self.assertRaises(GatewayDenied):
            self.request()
        self.incarnation = str(uuid.uuid4())
        self.assertEqual(self.request()["generation"], 2)
        self.assertEqual(
            GatewayOwnerGeneration.objects.filter(session=self.session).count(), 2
        )

    def test_old_claim_cannot_return_after_multiple_releases(self):
        old = self.packet()
        ownership_request(old, config=self.bus)
        self.request("release", 1)
        self.incarnation = str(uuid.uuid4())
        self.request()
        self.request("release", 2)
        with self.assertRaises(GatewayDenied):
            ownership_request(old, config=self.bus)

    def test_expired_owner_is_not_proof_of_remote_cleanup(self):
        self.request()
        GatewayCommandRoute.objects.filter(session=self.session).update(
            lease_expires_at=timezone.now() - timedelta(seconds=1)
        )
        with self.assertRaises(GatewayDenied):
            self.request("renew", 1)
        with self.assertRaises(GatewayDenied):
            self.request(incarnation=str(uuid.uuid4()))
        # Only the exact old owner can report completed disposal and release.
        self.request("release", 1)
        self.assertEqual(self.request(incarnation=str(uuid.uuid4()))["generation"], 2)

    def test_stale_or_other_owner_cannot_renew_release_or_steal(self):
        self.request()
        for action, generation, changes in [
            ("renew", 2, {}),
            ("release", 2, {}),
            ("renew", 1, {"incarnation": str(uuid.uuid4())}),
            ("claim", 0, {"incarnation": str(uuid.uuid4())}),
        ]:
            with self.assertRaises(GatewayDenied):
                self.request(action, generation, **changes)
        body = json.loads(self.packet("release", 1))["body"]
        with self.assertRaises(GatewayDenied):
            ownership_request(self.other.sign(body, "owner-request"), config=self.bus)

    def test_tampered_expired_and_disabled_requests_fail_closed(self):
        body = json.loads(self.packet())
        body["body"]["action"] = "release"
        with self.assertRaises(GatewayDenied):
            ownership_request(json.dumps(body).encode(), config=self.bus)
        for issued in [int(time.time()) - 11, int(time.time()) + 6]:
            with self.assertRaises(GatewayDenied):
                self.request(issued_at=issued)
        with (
            override_settings(GATEWAY_RUNTIME_ENABLED=False),
            self.assertRaises(GatewayDenied),
        ):
            self.request()
        self.assertFalse(GatewayCommandRoute.objects.exists())

    def test_legacy_management_and_handle_preparation_stay_fenced_when_flags_off(self):
        with (
            override_settings(GATEWAY_RUNTIME_ENABLED=False),
            patch("apps.meetings.services.janus.janus_runtime") as runtime,
        ):
            with self.assertRaises(JanusGatewayError):
                resolve_janus_session(self.session)
            with self.assertRaises(JanusGatewayError):
                call_video_room_management_method(self.session, "list")
            with self.assertRaises(JanusGatewayError):
                _prepare_media_handle(
                    participant=self.participant, handle_type="publisher"
                )
            runtime.run.assert_not_called()
            runtime.session.assert_not_called()
        self.assertFalse(self.participant.media_handles.exists())

    def test_legacy_celery_entrypoints_skip_gateway_resources(self):
        from apps.meetings import tasks

        with patch("apps.meetings.tasks.call_video_room_management_method") as media:
            for task in [
                tasks.provision_janus_room_for_session,
                tasks.sync_janus_participants,
                tasks.destroy_janus_room_for_session,
            ]:
                self.assertEqual(
                    task.run(str(self.session.pk)), {"status": "gateway_owned"}
                )
            self.assertEqual(
                tasks.attach_participant_media_handles.run(str(self.participant.pk)),
                {"status": "gateway_owned"},
            )
            tasks.detach_participant_media_handles.run(str(self.participant.pk))
            media.assert_not_called()
        self.assertFalse(self.participant.media_handles.exists())

    def test_durable_dispatch_uses_published_owner_and_generation(self):
        self.intent()
        self.decision()
        self.request()
        from apps.meetings.models import GatewayAuthorizationOutbox

        row = GatewayAuthorizationOutbox.objects.get(participant=self.guest_participant)
        claim = prepare_delivery(row.pk)
        self.assertIsNotNone(claim)
        self.assertEqual(claim["envelope"]["target_owner"], "owner-a")
        self.assertEqual(claim["envelope"]["expected_owner_generation"], 1)

    def test_endpoint_requires_https_service_token_and_owner_signature(self):
        client = APIClient()
        url = "/api/v1/meetings/internal/gateway/owners/"
        with patch(
            "apps.meetings.services.gateway_ownership.BusConfig.read",
            return_value=self.bus,
        ):
            self.assertEqual(
                client.post(
                    url, json.loads(self.packet()), format="json", secure=True
                ).status_code,
                401,
            )
            client.credentials(HTTP_AUTHORIZATION="Bearer test-service-" + "x" * 32)
            self.assertEqual(
                client.post(url, json.loads(self.packet()), format="json").status_code,
                403,
            )
            result = client.post(
                url, json.loads(self.packet()), format="json", secure=True
            )
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json()["owner_id"], "owner-a")
            self.assertEqual(result["Cache-Control"], "no-store")


class OwnershipHTTPTests(OwnershipFixture, TransactionTestCase):
    def test_gateway_client_uses_real_signed_django_lease_endpoint(self):
        import asyncio

        import httpx2 as httpx
        from asgiref.sync import async_to_sync
        from django.core.handlers.asgi import ASGIHandler
        from jrtc_signalling.ownership import OwnershipClient, OwnershipError

        gateway = BusConfig(
            self.bus.redis_url,
            self.owner,
            Verifier({}),
            authority_token="test-service-" + "x" * 32,
        )

        async def exercise():
            owner = OwnershipClient(
                gateway,
                "https://testserver/api/v1/meetings/internal/gateway/owners/",
                self.incarnation,
            )
            await owner.client.aclose()
            owner.client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=ASGIHandler())
            )
            try:
                meeting = str(self.session.pk)
                first = await owner.request(meeting, "claim")
                first.require("owner-a", 1, now=asyncio.get_running_loop().time())
                renewed = await owner.request(meeting, "renew", first.generation)
                self.assertEqual(renewed.generation, 1)
                with override_settings(GATEWAY_RUNTIME_ENABLED=False):
                    await owner.request(meeting, "release", first.generation)
                with self.assertRaises(OwnershipError):
                    await owner.request(meeting, "renew", first.generation)
                with self.assertRaises(OwnershipError):
                    await owner.request(meeting, "claim")
            finally:
                await owner.close()

        with patch(
            "apps.meetings.services.gateway_ownership.BusConfig.read",
            return_value=self.bus,
        ):
            async_to_sync(exercise)()
        route = GatewayCommandRoute.objects.get(session=self.session)
        self.assertTrue(route.released)


@skipUnlessDBFeature("has_select_for_update")
class OwnershipRaceTests(OwnershipFixture, TransactionTestCase):
    race = race_support.GatewayTicketRaceTests.race

    def test_competing_incarnations_have_one_live_owner(self):
        def claim(incarnation):
            self.request(incarnation=incarnation)
            return "claimed"

        results = self.race(
            lambda: claim(str(uuid.uuid4())), lambda: claim(str(uuid.uuid4()))
        )
        self.assertCountEqual(results, ["claimed", "denied"])
        self.assertEqual(
            GatewayCommandRoute.objects.filter(session=self.session).count(), 1
        )
        self.assertEqual(
            GatewayOwnerGeneration.objects.filter(session=self.session).count(), 1
        )
