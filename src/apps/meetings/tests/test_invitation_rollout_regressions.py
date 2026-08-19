"""Focused regressions for invitation rollout and meeting creation contracts."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from apps.meetings.api.serializers import (
    MeetingRoomSerializer,
    MeetingServiceSessionCreateSerializer,
    MeetingSessionCreateSerializer,
)
from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.models import (
    ExternalMeetingBinding,
    ExternalMeetingProvider,
    MeetingInvitation,
    MeetingLifecycleState,
    MeetingRoom,
)
from apps.meetings.services.invitations import MeetingInvitationService
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.tasks import queue_due_meeting_invitation_emails
from core.api.service_auth import ServiceTokenAuthentication


class InvitationRolloutRegressionTests(TestCase):
    """Protect delivery state transitions introduced by the invitation rollout."""

    def make_session(self):
        """Create a future meeting without executing transaction-on-commit tasks."""

        service_user = ServiceTokenAuthentication._get_service_user()
        host = service_user.profile
        start = timezone.now() + timedelta(hours=1)
        room = MeetingLifecycleService.create_room(
            creator_profile=host,
            title="Rollout regression meeting",
            scheduled_start_at=start,
            scheduled_end_at=start + timedelta(hours=1),
        )
        session = MeetingLifecycleService.start_session(
            room=room,
            started_by_profile=host,
        )
        return host, room, session

    def test_share_session_rejects_terminal_lifecycle_states(self):
        """Ending or terminal meetings cannot create fresh invitation records."""

        host, _, session = self.make_session()

        for lifecycle_state in (
            MeetingLifecycleState.ENDING,
            MeetingLifecycleState.ENDED,
            MeetingLifecycleState.FAILED,
        ):
            with self.subTest(lifecycle_state=lifecycle_state):
                type(session).objects.filter(pk=session.pk).update(
                    lifecycle_state=lifecycle_state,
                )
                session.refresh_from_db()

                with self.assertRaisesMessage(
                    MeetingDomainError,
                    "Invitations cannot be sent for a meeting that has ended.",
                ):
                    MeetingInvitationService.share_session(
                        session=session,
                        issuer_profile=host,
                        emails=["guest@example.com"],
                    )

        self.assertFalse(MeetingInvitation.objects.exists())

    @override_settings(
        EMAIL_BACKEND="django.core.mail.backends.console.EmailBackend",
    )
    @patch.object(MeetingInvitationService, "send_invitation_email", return_value=1)
    def test_console_backend_reports_previewed(self, send_invitation_email):
        """Console output is reported as a local preview, not real delivery."""

        host, _, session = self.make_session()

        result = MeetingInvitationService.share_session(
            session=session,
            issuer_profile=host,
            emails=["guest@example.com"],
        )

        self.assertEqual(result["delivery_status"], "previewed")
        self.assertEqual(result["sent_count"], 1)
        send_invitation_email.assert_called_once()
        self.assertIsNotNone(
            MeetingInvitation.objects.get().initial_email_sent_at,
        )

    @override_settings(
        EMAIL_BACKEND="django.core.mail.backends.smtp.EmailBackend",
    )
    @patch.object(MeetingInvitationService, "send_invitation_email")
    @patch("apps.meetings.services.lifecycle.dispatch_task")
    def test_external_backend_queues_once_without_synchronous_delivery(
        self,
        dispatch_task,
        send_invitation_email,
    ):
        """External email is queued and an immediate client retry is idempotent."""

        dispatch_task.return_value = object()
        host, _, session = self.make_session()

        first = MeetingInvitationService.share_session(
            session=session,
            issuer_profile=host,
            emails=["guest@example.com"],
        )
        second = MeetingInvitationService.share_session(
            session=session,
            issuer_profile=host,
            emails=["guest@example.com"],
        )

        self.assertEqual(first["delivery_status"], "queued")
        self.assertEqual(first["queued_count"], 1)
        self.assertEqual(second["delivery_status"], "pending")
        self.assertEqual(second["pending_count"], 1)
        self.assertEqual(dispatch_task.call_count, 1)
        send_invitation_email.assert_not_called()
        self.assertEqual(MeetingInvitation.objects.count(), 1)
        invitation = MeetingInvitation.objects.get()
        self.assertIsNone(invitation.initial_email_sent_at)
        self.assertIsNotNone(invitation.last_delivery_attempt_at)

    @override_settings(
        EMAIL_BACKEND="django.core.mail.backends.smtp.EmailBackend",
    )
    def test_broker_failure_stays_pending_and_is_retryable_by_sweep(self):
        """A failed enqueue releases its lease so the next sweep can retry it."""

        host, _, session = self.make_session()
        with patch(
            "apps.meetings.services.lifecycle.dispatch_task",
            return_value=None,
        ):
            result = MeetingInvitationService.share_session(
                session=session,
                issuer_profile=host,
                emails=["guest@example.com"],
            )

        invitation = MeetingInvitation.objects.get()
        self.assertEqual(result["delivery_status"], "pending")
        self.assertEqual(result["pending_count"], 1)
        self.assertIsNone(invitation.initial_email_sent_at)
        self.assertIsNone(invitation.last_delivery_attempt_at)

        with patch(
            "apps.meetings.tasks.dispatch_task",
            return_value=object(),
        ) as dispatch_task:
            self.assertEqual(queue_due_meeting_invitation_emails.run(), 1)

        dispatch_task.assert_called_once()
        invitation.refresh_from_db()
        self.assertIsNone(invitation.initial_email_sent_at)
        self.assertIsNotNone(invitation.last_delivery_attempt_at)


@override_settings(MEET_SERVICE_TOKEN="service-secret")
class LegacyServiceRoomRegressionTests(TestCase):
    """Protect runtime adoption of rooms created before durable bindings."""

    def test_service_endpoint_reuses_and_backfills_legacy_metadata_room(self):
        """A legacy metadata match gains a binding instead of a duplicate room."""

        service_user = ServiceTokenAuthentication._get_service_user()
        external_id = "consultation:legacy-rollout"
        legacy_room = MeetingLifecycleService.create_room(
            creator_profile=service_user.profile,
            title="Legacy consultation",
            metadata={
                "source": "law_firm_workspace",
                "external_id": external_id,
                "legacy_marker": True,
            },
        )
        client = APIClient()

        response = client.post(
            "/api/v1/meetings/internal/service-sessions/",
            {
                "external_id": external_id,
                "title": "Replacement title must not create a room",
                "metadata": {"tenant_id": "tenant-legacy"},
            },
            format="json",
            HTTP_AUTHORIZATION="Bearer service-secret",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.json()["room_id"], str(legacy_room.pk))
        self.assertEqual(
            MeetingRoom.objects.filter(
                metadata__source="law_firm_workspace",
                metadata__external_id=external_id,
            ).count(),
            1,
        )
        binding = ExternalMeetingBinding.objects.get(
            provider=ExternalMeetingProvider.LAW_FIRM_WORKSPACE,
            external_id=external_id,
        )
        self.assertEqual(binding.room_id, legacy_room.pk)
        self.assertEqual(binding.service_owner_profile_id, service_user.profile.pk)


class ScheduledRangeSerializerRegressionTests(TestCase):
    """Keep browser, service, and room APIs aligned on schedule ordering."""

    def test_all_creation_serializers_reject_end_before_start(self):
        """Every creation surface reports the invalid scheduled end field."""

        scheduled_start_at = timezone.now() + timedelta(hours=2)
        scheduled_end_at = scheduled_start_at - timedelta(minutes=1)
        cases = (
            (
                "browser",
                MeetingSessionCreateSerializer,
                {"title": "Browser meeting"},
            ),
            (
                "service",
                MeetingServiceSessionCreateSerializer,
                {"external_id": "schedule:invalid", "title": "Service meeting"},
            ),
            (
                "room",
                MeetingRoomSerializer,
                {"title": "Room meeting"},
            ),
        )

        for surface, serializer_class, required_fields in cases:
            with self.subTest(surface=surface):
                serializer = serializer_class(
                    data={
                        **required_fields,
                        "scheduled_start_at": scheduled_start_at.isoformat(),
                        "scheduled_end_at": scheduled_end_at.isoformat(),
                    },
                )

                self.assertFalse(serializer.is_valid())
                self.assertIn("scheduled_end_at", serializer.errors)
