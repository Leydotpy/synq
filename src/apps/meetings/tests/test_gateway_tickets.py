"""Ticket/grant authority regressions with real RSA and the Django database."""

import json
import tempfile
import uuid
from datetime import timedelta
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.db import OperationalError, transaction
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework.test import APIClient

from apps.meetings.models import (
    GatewayGrant,
    GatewayTicket,
    MeetingLifecycleState,
    Participant,
    ParticipantStatus,
)
from apps.meetings.services.gateway_tickets import (
    GatewayDenied,
    check_ticket,
    issue_ticket,
)
from apps.meetings.services.lifecycle import MeetingLifecycleService as Lifecycle


class GatewayTicketFixture:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.temp = tempfile.TemporaryDirectory()
        cls.key_path = Path(cls.temp.name) / "issuer.pem"
        cls.key_path.write_bytes(
            cls.key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        cls.config = override_settings(
            GATEWAY_TICKETS_ENABLED=True,
            GATEWAY_TICKET_ISSUER="https://synq.example",
            GATEWAY_TICKET_KEY_ID="test-key",
            GATEWAY_TICKET_PRIVATE_KEY_FILE=str(cls.key_path),
            GATEWAY_TICKET_TTL_SECONDS=60,
            GATEWAY_AUTHORITY_TOKEN="test-service-" + "x" * 32,
        )
        cls.config.enable()

    @classmethod
    def tearDownClass(cls):
        cls.config.disable()
        cls.temp.cleanup()
        super().tearDownClass()

    def profile(self, name):
        return (
            get_user_model()
            .objects.create_user(
                username=name,
                email=name + "@example.invalid",
                password="test",
                clerk_user_id="clerk_" + name,
            )
            .profile
        )

    def setUp(self):
        self.host, self.guest = self.profile("host"), self.profile("guest")
        self.room = Lifecycle.create_room(
            creator_profile=self.host, title="Gateway tests"
        )
        self.session = Lifecycle.start_session(
            room=self.room, started_by_profile=self.host
        )
        self.participant = self.session.participants.get(profile=self.host)
        self.client = APIClient()
        self.client.force_authenticate(self.host.user)
        self.url = f"/api/v1/meetings/sessions/{self.session.pk}/gateway/tickets/"

    def issue(self, profile=None, **kwargs):
        result = issue_ticket(
            session_id=self.session.pk, profile_id=(profile or self.host).pk, **kwargs
        )
        claims = jwt.decode(
            result["ticket"],
            self.key.public_key(),
            algorithms=["RS256"],
            issuer="https://synq.example",
            audience="jrtc-signalling",
        )
        return result, claims

    def waiting(self):
        return Lifecycle.request_join(session=self.session, profile=self.guest)


class GatewayTicketTests(GatewayTicketFixture, TestCase):
    def test_registered_ticket_has_pinned_header_scope_and_server_binding(self):
        result, claims = self.issue()
        self.assertEqual(
            jwt.get_unverified_header(result["ticket"]),
            {"alg": "RS256", "typ": "JWT", "kid": "test-key"},
        )
        self.assertEqual(claims["sub"], str(self.host.pk))
        self.assertEqual(claims["participant_id"], str(self.participant.pk))
        self.assertEqual(claims["exp"] - claims["iat"], 60)
        self.assertEqual(claims["aud"], "jrtc-signalling")
        self.assertIn("media:publish", claims["scopes"])
        self.assertEqual(GatewayTicket.objects.get(pk=claims["jti"]).claims, claims)
        self.assertNotIn(
            result["ticket"],
            json.dumps(GatewayTicket.objects.values().first(), default=str),
        )

    def test_pending_nonmember_guest_can_only_get_lobby(self):
        intent = self.waiting()
        _, claims = self.issue(self.guest)
        self.assertEqual(claims["scopes"], ["lobby", "session:resume"])
        guest = Participant.objects.get(pk=claims["participant_id"])
        self.assertEqual(guest.join_request_id, intent.pk)
        self.assertEqual(guest.status, ParticipantStatus.WAITING)
        result = check_ticket(claims=claims, operation="session.hello", consume=True)
        self.assertFalse(any(result["permissions"].values()))

    def test_unapproved_nonmember_cannot_issue_ticket(self):
        with self.assertRaises(GatewayDenied):
            self.issue(self.guest)
        self.assertFalse(GatewayGrant.objects.exists())

    def test_admitted_guest_gets_media_only_after_django_decision(self):
        intent = self.waiting()
        _, lobby = self.issue(self.guest)
        participant = Lifecycle.review_join_request(
            join_request=intent, reviewer_profile=self.host, approve=True
        )
        self.assertEqual(participant.status, ParticipantStatus.ADMITTED)
        self.assertGreater(
            participant.authorization_epoch, lobby["authorization_epoch"]
        )
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=lobby, operation="session.hello", consume=True)
        _, claims = self.issue(self.guest)
        self.assertIn("media:publish", claims["scopes"])

    def test_denied_guest_lobby_ticket_cannot_be_consumed(self):
        intent = self.waiting()
        _, claims = self.issue(self.guest)
        Lifecycle.review_join_request(
            join_request=intent, reviewer_profile=self.host, approve=False
        )
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="session.hello", consume=True)
        with self.assertRaises(GatewayDenied):
            self.issue(self.guest)

    def test_removed_ended_and_disabled_identities_are_denied(self):
        for change in (
            {"status": ParticipantStatus.REMOVED},
            {"status": ParticipantStatus.LEFT},
            {"role": "admin"},
        ):
            with self.subTest(change=change), transaction.atomic():
                Participant.objects.filter(pk=self.participant.pk).update(**change)
                with self.assertRaises(GatewayDenied):
                    self.issue()
                transaction.set_rollback(True)
        for state in [
            MeetingLifecycleState.SCHEDULED,
            MeetingLifecycleState.ENDING,
            MeetingLifecycleState.ENDED,
            MeetingLifecycleState.FAILED,
        ]:
            self.session.lifecycle_state = state
            self.session.save(update_fields=["lifecycle_state"])
            with self.assertRaises(GatewayDenied):
                self.issue()
        self.session.lifecycle_state = MeetingLifecycleState.ACTIVE
        self.session.save(update_fields=["lifecycle_state"])
        self.host.is_active = False
        self.host.save(update_fields=["is_active"])
        with self.assertRaises(GatewayDenied):
            self.issue()

    def test_lost_host_membership_cannot_retain_host_role(self):
        self.room.memberships.update(is_active=False)
        with self.assertRaises(GatewayDenied):
            self.issue()

    def test_ticket_consumption_is_one_use_and_checks_current_epoch(self):
        _, claims = self.issue()
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="session.ping", consume=False)
        check_ticket(claims=claims, operation="session.hello", consume=True)
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="session.hello", consume=True)
        Lifecycle.update_participant_permissions(
            session=self.session,
            actor_profile=self.host,
            participant=self.participant,
            updates={"is_muted": True},
        )
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="media.publish", consume=False)

    def test_wrong_identity_and_purpose_never_consume_receipt(self):
        _, claims = self.issue()
        for key in [
            "meeting_id",
            "participant_id",
            "participant_session_id",
            "sub",
            "grant_id",
        ]:
            with self.subTest(key=key), self.assertRaises(GatewayDenied):
                check_ticket(
                    claims={**claims, key: str(uuid.uuid4())},
                    operation="session.hello",
                    consume=True,
                )
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="session.refresh", consume=True)
        self.assertIsNone(GatewayTicket.objects.get(pk=claims["jti"]).consumed_at)

    def test_reconnect_fences_old_generation_and_refresh_preserves_binding(self):
        result, old = self.issue()
        refresh, claims = self.issue(
            purpose="refresh",
            participant_session_id=result["participant_session_id"],
            generation=result["generation"],
        )
        self.assertEqual(refresh["generation"], result["generation"])
        self.assertNotEqual(claims["jti"], old["jti"])
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="session.hello", consume=True)
        _, resumed = self.issue(
            purpose="resume",
            participant_session_id=result["participant_session_id"],
            generation=result["generation"],
        )
        self.assertEqual(resumed["generation"], old["generation"] + 1)
        for stale in (old, claims):
            with self.assertRaises(GatewayDenied):
                check_ticket(claims=stale, operation="session.hello", consume=True)
        with self.assertRaises(GatewayDenied):
            self.issue(
                purpose="refresh",
                participant_session_id=result["participant_session_id"],
                generation=result["generation"],
            )

    def test_refresh_cannot_select_another_participant_binding(self):
        self.waiting()
        result, _ = self.issue(self.guest)
        with self.assertRaises(GatewayDenied):
            self.issue(
                purpose="refresh",
                participant_session_id=result["participant_session_id"],
                generation=result["generation"],
            )

    def test_rate_limit_survives_new_binding_and_rolls_forward(self):
        for _ in range(10):
            self.issue()
        with self.assertRaises(GatewayDenied) as denied:
            self.issue()
        self.assertEqual(denied.exception.status, 429)
        self.assertEqual(GatewayGrant.objects.get().generation, 10)
        GatewayGrant.objects.update(
            issue_window_at=timezone.now() - timedelta(minutes=1)
        )
        self.issue()
        self.assertEqual(GatewayGrant.objects.get().issue_count, 1)

    def test_expired_tickets_and_missing_receipts_fail_closed(self):
        _, claims = self.issue()
        GatewayTicket.objects.update(expires_at=timezone.now() - timedelta(seconds=6))
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="session.hello", consume=True)
        call_command("prune_gateway_tickets", stdout=StringIO())
        self.assertFalse(GatewayTicket.objects.exists())
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="session.hello", consume=True)

    def test_permissions_remove_media_publish_scope(self):
        Lifecycle.update_participant_permissions(
            session=self.session,
            actor_profile=self.host,
            participant=self.participant,
            updates={
                "can_publish_audio": False,
                "can_publish_video": False,
                "can_share_screen": False,
            },
        )
        _, claims = self.issue()
        self.assertNotIn("media:publish", claims["scopes"])
        self.assertIn("media:subscribe", claims["scopes"])

    def test_removal_and_end_advance_epoch_before_enforcement(self):
        before = self.participant.authorization_epoch
        removed = Lifecycle.remove_participant(
            session=self.session, actor_profile=self.host, participant=self.participant
        )
        self.assertEqual(removed.authorization_epoch, before + 1)
        with self.assertRaises(GatewayDenied):
            self.issue()
        self.waiting()
        _, claims = self.issue(self.guest)
        Lifecycle.end_session(session=self.session, actor_profile=self.host)
        guest = Participant.objects.get(pk=claims["participant_id"])
        self.assertGreater(guest.authorization_epoch, claims["authorization_epoch"])
        with self.assertRaises(GatewayDenied):
            check_ticket(claims=claims, operation="session.hello", consume=True)

    def test_permission_transaction_rollback_restores_epoch(self):
        with transaction.atomic():
            Lifecycle.update_participant_permissions(
                session=self.session,
                actor_profile=self.host,
                participant=self.participant,
                updates={"is_muted": True},
            )
            transaction.set_rollback(True)
        self.participant.refresh_from_db()
        self.assertEqual(self.participant.authorization_epoch, 0)
        self.assertFalse(self.participant.is_muted)

    def test_key_configuration_fails_closed_without_leaking_path(self):
        for changes in (
            {"GATEWAY_TICKET_TTL_SECONDS": 121},
            {"GATEWAY_TICKET_KEY_ID": "bad/key"},
            {"GATEWAY_TICKET_PRIVATE_KEY_FILE": "/private-key-must-not-leak"},
        ):
            with (
                self.subTest(changes=changes),
                override_settings(**changes),
                self.assertRaises(GatewayDenied) as error,
            ):
                self.issue()
            self.assertEqual(str(error.exception), "UNAVAILABLE")

    def test_https_authentication_no_store_and_correlation(self):
        result = self.client.post(self.url, {}, format="json", secure=True)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result["Cache-Control"], "no-store")
        uuid.UUID(result["X-Correlation-ID"])
        self.assertEqual(self.client.post(self.url, {}, format="json").status_code, 403)
        self.client.force_authenticate(None)
        self.assertIn(
            self.client.post(self.url, {}, format="json", secure=True).status_code,
            (401, 403),
        )

    def test_clients_cannot_request_roles_scopes_or_other_identities(self):
        for field in ("role", "scopes", "profile_id", "participant_id", "generation"):
            with self.subTest(field=field):
                result = self.client.post(
                    self.url, {field: "host"}, format="json", secure=True
                )
                self.assertEqual(result.status_code, 400)
        self.assertEqual(GatewayTicket.objects.count(), 0)

    def test_duplicate_keys_oversize_nonfinite_and_unknown_fields_reject(self):
        for raw in (
            '{"purpose":"connect","purpose":"refresh"}',
            '{"x":NaN}',
            "[]",
            " " * 8193,
        ):
            self.assertEqual(
                self.client.post(
                    self.url, raw, content_type="application/json", secure=True
                ).status_code,
                400,
            )

    def test_authority_requires_dedicated_service_auth_and_secure_transport(self):
        _, claims = self.issue()
        url = "/api/v1/meetings/internal/gateway/authority/"
        data = {"claims": claims, "operation": "session.hello", "consume": True}
        client = APIClient()
        self.assertEqual(
            client.post(url, data, format="json", secure=True).status_code, 401
        )
        client.credentials(HTTP_AUTHORIZATION="Bearer test-service-" + "x" * 32)
        self.assertEqual(client.post(url, data, format="json").status_code, 403)
        self.assertEqual(
            client.post(url, data, format="json", secure=True).status_code, 200
        )
        self.assertEqual(
            client.post(url, data, format="json", secure=True).status_code, 403
        )

    def test_db_failure_and_feature_disabled_do_not_issue(self):
        with patch(
            "apps.meetings.api.gateway.issue_ticket",
            side_effect=OperationalError("sensitive detail"),
        ):
            result = self.client.post(self.url, {}, format="json", secure=True)
        self.assertEqual(result.status_code, 503)
        self.assertNotIn(b"sensitive", result.content)
        with override_settings(GATEWAY_TICKETS_ENABLED=False):
            self.assertEqual(
                self.client.post(self.url, {}, format="json", secure=True).status_code,
                404,
            )
