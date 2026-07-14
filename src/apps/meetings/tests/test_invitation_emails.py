"""Coverage for asynchronous, branded meeting invitation delivery."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse

from django.contrib.auth import get_user_model
from django.core import mail
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.meetings.models import (
    MeetingInvitation,
    MeetingLifecycleState,
)
from apps.meetings.services.invitations import MeetingInvitationService
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.tasks import (
    queue_due_meeting_invitation_emails,
    send_meeting_invitation_email,
)


@override_settings(
    EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    MEETING_FRONTEND_BASE_URL="https://meet.example",
)
class MeetingInvitationEmailTests(TestCase):
    """Verify both email states, queuing, reminders, and delivery bookkeeping."""

    def setUp(self):
        """Track unique fixtures when a test creates several meeting sessions."""

        self.profile_sequence = 0

    def make_profile(self, handle: str):
        """Create a user and return its signal-created profile."""

        user = get_user_model().objects.create_user(
            username=handle,
            email=f"{handle}@example.com",
            password="password",
            clerk_user_id=f"clerk_{handle}",
        )
        user.profile.display_name = handle.title()
        user.profile.save(update_fields=["display_name", "updated_at"])
        return user.profile

    def make_session(self, *, scheduled_start_at=None):
        """Create a room/session pair with an optional scheduled start."""

        self.profile_sequence += 1
        host = self.make_profile(
            f"host-{self._testMethodName}-{self.profile_sequence}",
        )
        room = MeetingLifecycleService.create_room(
            creator_profile=host,
            title="Design Review",
            description="Review the final product direction.",
            scheduled_start_at=scheduled_start_at,
            scheduled_end_at=(
                scheduled_start_at + timedelta(hours=1)
                if scheduled_start_at is not None
                else None
            ),
        )
        session = MeetingLifecycleService.start_session(
            room=room,
            started_by_profile=host,
        )
        return host, room, session

    def make_invitation(self, *, scheduled_start_at=None) -> MeetingInvitation:
        """Persist an invitation without invoking the request-path dispatcher."""

        host, _, session = self.make_session(
            scheduled_start_at=scheduled_start_at,
        )
        return MeetingInvitation.objects.create(
            session=session,
            issuer_profile=host,
            issuer_name=host.display_name,
            recipient_email="guest@example.com",
            message="Bring the prototype.",
            expires_in_seconds=3600,
        )

    @staticmethod
    def html_body(message) -> str:
        """Return the HTML alternative across supported Django tuple shapes."""

        for alternative in message.alternatives:
            mimetype = getattr(alternative, "mimetype", alternative[1])
            if mimetype == "text/html":
                return getattr(alternative, "body", None) or getattr(
                    alternative,
                    "content",
                    alternative[0],
                )
        raise AssertionError("HTML email alternative was not attached.")

    @patch.object(send_meeting_invitation_email, "delay", return_value=Mock())
    def test_share_session_persists_and_queues_without_sending_inline(self, delay):
        host, _, session = self.make_session()

        payload = MeetingInvitationService.share_session(
            session=session,
            issuer_profile=host,
            emails=["Guest@Example.com", "guest@example.com"],
            message="Bring the prototype.",
        )

        invitation = MeetingInvitation.objects.get()
        self.assertEqual(invitation.recipient_email, "guest@example.com")
        self.assertEqual(len(mail.outbox), 0)
        self.assertEqual(payload["sent_count"], 0)
        self.assertEqual(payload["queued_count"], 1)
        self.assertEqual(payload["delivery_status"], "queued")
        delay.assert_called_once_with(
            str(invitation.pk),
            payload["join_url"],
            True,
        )

    def test_future_invitation_has_no_join_action_or_link(self):
        invitation = self.make_invitation(
            scheduled_start_at=timezone.now() + timedelta(days=1),
        )

        result = send_meeting_invitation_email.run(
            str(invitation.pk),
            "https://meet.example/meetings/should-not-leak?invite=hidden",
            True,
        )

        invitation.refresh_from_db()
        message = mail.outbox[0]
        html = self.html_body(message)
        self.assertEqual(result["email_state"], "scheduled")
        self.assertIn("You’re invited", html)
        self.assertNotIn("Join meeting", html)
        self.assertNotIn("invite=", html)
        self.assertNotIn("invite=", message.body)
        self.assertIn("send another email", message.body)
        self.assertEqual(len(message.attachments), 2)
        self.assertIsNotNone(invitation.initial_email_sent_at)
        self.assertIsNone(invitation.ready_email_sent_at)

    def test_ready_invitation_has_button_and_plain_text_fallback(self):
        invitation = self.make_invitation()

        result = send_meeting_invitation_email.run(str(invitation.pk))

        invitation.refresh_from_db()
        message = mail.outbox[0]
        html = self.html_body(message)
        self.assertEqual(result["email_state"], "ready")
        self.assertIn("Join meeting", html)
        self.assertIn("https://meet.example/meetings/", html)
        self.assertIn("https://meet.example/meetings/", message.body)
        self.assertIsNotNone(invitation.ready_email_sent_at)

    def test_due_follow_up_generates_a_fresh_valid_join_token_once(self):
        invitation = self.make_invitation(
            scheduled_start_at=timezone.now() + timedelta(days=1),
        )
        first_result = send_meeting_invitation_email.run(str(invitation.pk))
        invitation.session.room.scheduled_start_at = timezone.now() - timedelta(
            minutes=1
        )
        invitation.session.room.save(
            update_fields=["scheduled_start_at", "updated_at"],
        )

        ready_result = send_meeting_invitation_email.run(str(invitation.pk))
        duplicate_result = send_meeting_invitation_email.run(str(invitation.pk))

        self.assertEqual(first_result["email_state"], "scheduled")
        self.assertEqual(ready_result["email_state"], "ready")
        self.assertEqual(duplicate_result["status"], "already_sent")
        self.assertEqual(len(mail.outbox), 2)
        ready_url = next(
            line for line in mail.outbox[-1].body.splitlines() if line.startswith("https://")
        )
        token = parse_qs(urlparse(ready_url).query)["invite"][0]
        MeetingInvitationService.validate_invite_token(
            session=invitation.session,
            token=token,
        )

    @patch("apps.meetings.tasks.dispatch_task", return_value=Mock())
    def test_due_sweep_queues_only_due_available_invitations(self, dispatch):
        due = self.make_invitation(
            scheduled_start_at=timezone.now() - timedelta(minutes=1),
        )
        self.make_invitation(
            scheduled_start_at=timezone.now() + timedelta(hours=1),
        )
        ended = self.make_invitation(
            scheduled_start_at=timezone.now() - timedelta(minutes=2),
        )
        ended.session.lifecycle_state = MeetingLifecycleState.ENDED
        ended.session.save(update_fields=["lifecycle_state", "updated_at"])

        queued_count = queue_due_meeting_invitation_emails.run()

        self.assertEqual(queued_count, 1)
        dispatch.assert_called_once_with(
            send_meeting_invitation_email,
            str(due.pk),
            "",
            False,
        )

    @patch("apps.meetings.tasks.build_meeting_invitation_email")
    def test_delivery_failure_is_recorded_for_retry(self, build_email):
        invitation = self.make_invitation()
        build_email.return_value.send.side_effect = RuntimeError("smtp unavailable")

        with self.assertRaisesMessage(RuntimeError, "smtp unavailable"):
            send_meeting_invitation_email.run(str(invitation.pk))

        invitation.refresh_from_db()
        self.assertEqual(invitation.delivery_attempts, 1)
        self.assertEqual(invitation.last_delivery_error, "smtp unavailable")
        self.assertIsNone(invitation.initial_email_sent_at)
        self.assertIsNone(invitation.ready_email_sent_at)
