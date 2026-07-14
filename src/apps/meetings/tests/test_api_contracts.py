"""Focused API coverage for external bindings and public input boundaries."""

from __future__ import annotations

from datetime import timedelta

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from apps.meetings.models import (
    ExternalMeetingBinding,
    ExternalMeetingProvider,
    MeetingLifecycleState,
    MeetingRoom,
    ParticipantStatus,
)
from apps.meetings.services.lifecycle import MeetingLifecycleService


class LocalCorsContractTests(SimpleTestCase):
    """Keep both common Next.js localhost origins enabled in local settings."""

    def test_local_frontend_origins_are_allowed(self):
        self.assertIn("http://localhost:3000", settings.CORS_ALLOWED_ORIGINS)
        self.assertIn("http://127.0.0.1:3000", settings.CORS_ALLOWED_ORIGINS)


@override_settings(
    MEET_SERVICE_TOKEN="service-contract-secret",
    EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
)
class MeetingApiBoundaryTests(TestCase):
    """Verify public metadata isolation and service binding idempotency."""

    def make_profile(self, handle: str):
        user = get_user_model().objects.create_user(
            username=handle,
            email=f"{handle}@example.com",
            password="password",
            clerk_user_id=f"clerk_{handle}",
        )
        return user.profile

    def authenticated_client(self, handle: str = "browser-user") -> tuple[APIClient, object]:
        profile = self.make_profile(handle)
        client = APIClient()
        client.force_authenticate(user=profile.user)
        return client, profile

    @staticmethod
    def service_client() -> APIClient:
        client = APIClient()
        client.credentials(HTTP_AUTHORIZATION="Bearer service-contract-secret")
        return client

    def test_public_room_creation_rejects_reserved_integration_metadata(self):
        client, _ = self.authenticated_client()

        response = client.post(
            "/api/v1/meetings/rooms/",
            {
                "title": "Browser room",
                "metadata": {"external_id": "consultation:claimed"},
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Reserved integration metadata", str(response.json()["metadata"][0]))
        self.assertFalse(MeetingRoom.objects.filter(title="Browser room").exists())

    def test_public_session_creation_rejects_reserved_integration_metadata(self):
        client, _ = self.authenticated_client("session-browser-user")

        response = client.post(
            "/api/v1/meetings/sessions/",
            {
                "title": "Browser session",
                "metadata": {"provider": "law_firm_workspace"},
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Reserved integration metadata", str(response.json()["metadata"][0]))
        self.assertFalse(MeetingRoom.objects.filter(title="Browser session").exists())

    def test_public_room_start_rejects_reserved_integration_metadata(self):
        client, host = self.authenticated_client("room-start-browser-user")
        room = MeetingLifecycleService.create_room(
            creator_profile=host,
            title="Browser-started room",
        )

        response = client.post(
            f"/api/v1/meetings/rooms/{room.slug}/sessions/",
            {"metadata": {"external_id": "consultation:claimed-at-start"}},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Reserved integration metadata", str(response.json()["metadata"][0]))
        self.assertFalse(room.sessions.exists())

    def test_room_and_service_serializers_reject_inverted_schedules(self):
        start = timezone.now() + timedelta(hours=2)
        end = start - timedelta(hours=1)
        browser_client, _ = self.authenticated_client("schedule-browser-user")

        room_response = browser_client.post(
            "/api/v1/meetings/rooms/",
            {
                "title": "Invalid room schedule",
                "scheduled_start_at": start.isoformat(),
                "scheduled_end_at": end.isoformat(),
            },
            format="json",
        )
        service_response = self.service_client().post(
            "/api/v1/meetings/internal/service-sessions/",
            {
                "external_id": "schedule:invalid",
                "title": "Invalid service schedule",
                "scheduled_start_at": start.isoformat(),
                "scheduled_end_at": end.isoformat(),
            },
            format="json",
        )

        self.assertEqual(room_response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(service_response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("scheduled_end_at", str(room_response.json()["non_field_errors"][0]))
        self.assertIn("scheduled_end_at", str(service_response.json()["non_field_errors"][0]))
        self.assertFalse(ExternalMeetingBinding.objects.exists())

    def test_model_validation_failures_use_a_detail_envelope(self):
        client, _ = self.authenticated_client("model-validation-user")

        response = client.post(
            "/api/v1/meetings/rooms/",
            {"title": "Too small", "max_participants": 1},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(set(response.json()), {"detail"})
        self.assertIn("max_participants must be at least 2", response.json()["detail"])

    def test_service_session_creates_and_reuses_one_external_binding(self):
        client = self.service_client()
        payload = {
            "external_id": "consultation:bound-123",
            "title": "Bound consultation",
            "metadata": {"tenant_id": "tenant-7"},
        }

        first = client.post(
            "/api/v1/meetings/internal/service-sessions/",
            payload,
            format="json",
        )
        second = client.post(
            "/api/v1/meetings/internal/service-sessions/",
            payload,
            format="json",
        )

        self.assertEqual(first.status_code, status.HTTP_201_CREATED)
        self.assertEqual(second.status_code, status.HTTP_201_CREATED)
        self.assertEqual(second.json()["room_id"], first.json()["room_id"])
        self.assertEqual(second.json()["session_id"], first.json()["session_id"])
        binding = ExternalMeetingBinding.objects.select_related(
            "room",
            "service_owner_profile",
        ).get(
            provider=ExternalMeetingProvider.LAW_FIRM_WORKSPACE,
            external_id=payload["external_id"],
        )
        self.assertEqual(str(binding.room_id), first.json()["room_id"])
        self.assertEqual(binding.room.created_by_profile_id, binding.service_owner_profile_id)
        self.assertEqual(binding.room.metadata["external_id"], payload["external_id"])

    def test_service_binding_owned_by_another_profile_returns_detail_400(self):
        attacker = self.make_profile("binding-owner-attacker")
        attacker_room = MeetingLifecycleService.create_room(
            creator_profile=attacker,
            title="Attacker-owned room",
        )
        ExternalMeetingBinding.objects.create(
            provider=ExternalMeetingProvider.LAW_FIRM_WORKSPACE,
            external_id="consultation:owned-elsewhere",
            room=attacker_room,
            service_owner_profile=attacker,
        )

        response = self.service_client().post(
            "/api/v1/meetings/internal/service-sessions/",
            {
                "external_id": "consultation:owned-elsewhere",
                "title": "Trusted service title",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.json(),
            {"detail": "External meeting identity is owned by a different service profile."},
        )
        self.assertEqual(MeetingRoom.objects.count(), 1)

    def test_host_can_end_a_session_idempotently_and_clear_presence(self):
        client, host = self.authenticated_client("ending-host")
        room = MeetingLifecycleService.create_room(
            creator_profile=host,
            title="Session to end",
        )
        session = MeetingLifecycleService.start_session(
            room=room,
            started_by_profile=host,
        )

        first = client.post(
            f"/api/v1/meetings/sessions/{session.pk}/end/",
            {"reason": "Host finished the call."},
            format="json",
        )
        second = client.post(
            f"/api/v1/meetings/sessions/{session.pk}/end/",
            {"reason": "Repeated request."},
            format="json",
        )

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        session.refresh_from_db()
        self.assertEqual(session.lifecycle_state, MeetingLifecycleState.ENDED)
        self.assertEqual(session.participant_count, 0)
        self.assertFalse(
            session.participants.exclude(
                status__in=[ParticipantStatus.LEFT, ParticipantStatus.REMOVED]
            ).exists()
        )

    def test_non_coordinator_cannot_end_a_session(self):
        host = self.make_profile("protected-ending-host")
        room = MeetingLifecycleService.create_room(
            creator_profile=host,
            title="Protected session",
        )
        session = MeetingLifecycleService.start_session(
            room=room,
            started_by_profile=host,
        )
        client, _ = self.authenticated_client("ending-outsider")

        response = client.post(
            f"/api/v1/meetings/sessions/{session.pk}/end/",
            {},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        session.refresh_from_db()
        self.assertNotEqual(session.lifecycle_state, MeetingLifecycleState.ENDED)
