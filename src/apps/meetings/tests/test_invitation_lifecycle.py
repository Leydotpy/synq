"""Regression coverage for durable invitation delivery and due reminders."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core import mail
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.meetings.models import MeetingInvitation
from apps.meetings.services.invitations import MeetingInvitationService
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.tasks import (
    queue_due_meeting_invitation_emails,
    send_meeting_invitation_email,
)


@override_settings(EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend")
class MeetingInvitationLifecycleTests(TestCase):
    """Keep scheduled invitation state aligned with its Beat reminder."""

    def make_scheduled_session(self):
        """Create a future meeting whose invitation needs a later reminder."""

        user = get_user_model().objects.create_user(
            username="invite-host",
            email="host@example.com",
            password="password",
            clerk_user_id="clerk_invite_host",
        )
        start = timezone.now() + timedelta(hours=1)
        room = MeetingLifecycleService.create_room(
            creator_profile=user.profile,
            title="Scheduled review",
            scheduled_start_at=start,
            scheduled_end_at=start + timedelta(hours=1),
        )
        session = MeetingLifecycleService.start_session(
            room=room,
            started_by_profile=user.profile,
        )
        return user.profile, room, session

    def test_share_persists_initial_delivery_for_future_reminder(self):
        """Sharing retains recipient state instead of losing it after the request."""

        host, _, session = self.make_scheduled_session()

        result = MeetingInvitationService.share_session(
            session=session,
            issuer_profile=host,
            emails=["Guest@Example.com"],
            message="Bring the release plan.",
        )

        invitation = MeetingInvitation.objects.get()
        self.assertEqual(result["delivery_status"], "delivered")
        self.assertEqual(result["sent_count"], 1)
        self.assertEqual(invitation.recipient_email, "guest@example.com")
        self.assertIsNotNone(invitation.initial_email_sent_at)
        self.assertIsNone(invitation.ready_email_sent_at)
        self.assertEqual(len(mail.outbox), 1)

    @patch("apps.meetings.tasks.send_meeting_invitation_email.delay")
    def test_due_sweep_leases_each_reminder_once(self, send_delay):
        """An overlapping Beat tick cannot enqueue the same due reminder twice."""

        send_delay.return_value = object()
        host, room, session = self.make_scheduled_session()
        MeetingInvitationService.share_session(
            session=session,
            issuer_profile=host,
            emails=["guest@example.com"],
        )
        room.scheduled_start_at = timezone.now() - timedelta(seconds=1)
        room.save(update_fields=["scheduled_start_at", "updated_at"])

        self.assertEqual(queue_due_meeting_invitation_emails.run(), 1)
        self.assertEqual(queue_due_meeting_invitation_emails.run(), 0)
        send_delay.assert_called_once()

    def test_due_task_sends_and_marks_ready_email(self):
        """The reminder task is idempotent after a successful ready email."""

        host, room, session = self.make_scheduled_session()
        MeetingInvitationService.share_session(
            session=session,
            issuer_profile=host,
            emails=["guest@example.com"],
        )
        invitation = MeetingInvitation.objects.get()
        room.scheduled_start_at = timezone.now() - timedelta(seconds=1)
        room.save(update_fields=["scheduled_start_at", "updated_at"])

        first = send_meeting_invitation_email.run(str(invitation.pk))
        second = send_meeting_invitation_email.run(
            str(invitation.pk),
            force_send=True,
        )

        invitation.refresh_from_db()
        self.assertEqual(first["status"], "sent")
        self.assertEqual(second["status"], "already_sent")
        self.assertIsNotNone(invitation.ready_email_sent_at)
        self.assertEqual(len(mail.outbox), 2)

    def test_forced_initial_task_does_not_resend_successful_delivery(self):
        """The pre-start override bypasses timing, never a persisted send marker."""

        host, _room, session = self.make_scheduled_session()
        MeetingInvitationService.share_session(
            session=session,
            issuer_profile=host,
            emails=["guest@example.com"],
        )
        invitation = MeetingInvitation.objects.get()

        result = send_meeting_invitation_email.run(
            str(invitation.pk),
            force_send=True,
        )

        invitation.refresh_from_db()
        self.assertEqual(result["status"], "already_sent")
        self.assertIsNotNone(invitation.initial_email_sent_at)
        self.assertIsNone(invitation.ready_email_sent_at)
        self.assertEqual(len(mail.outbox), 1)
