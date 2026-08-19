"""Regression coverage for deduplicated terminal-session cleanup dispatch."""

from __future__ import annotations

import uuid
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.meetings.models import MeetingLifecycleState, MeetingSession
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.tasks import (
    cleanup_finished_sessions,
    destroy_janus_room_for_session,
)


class MeetingCleanupLeaseTests(TestCase):
    """Keep Beat overlap and broker outages from duplicating Janus teardown."""

    def make_session(self, handle: str, lifecycle_state: str) -> MeetingSession:
        """Create one isolated session without executing on-commit worker calls."""

        user = get_user_model().objects.create_user(
            username=handle,
            email=f"{handle}@example.com",
            password=None,
            clerk_user_id=f"clerk_{handle}",
        )
        profile = user.profile
        room = MeetingLifecycleService.create_room(
            creator_profile=profile,
            title=f"Cleanup {handle}",
        )
        session = MeetingLifecycleService.start_session(
            room=room,
            started_by_profile=profile,
        )
        MeetingSession.objects.filter(pk=session.pk).update(
            lifecycle_state=lifecycle_state,
        )
        session.refresh_from_db()
        return session

    @override_settings(MEETING_FINISHED_SESSION_CLEANUP_LEASE_SECONDS=900)
    @patch("apps.meetings.tasks.destroy_janus_room_for_session.delay")
    def test_sweep_leases_each_terminal_session_once(self, destroy_delay) -> None:
        """Ended and failed rows queue once while active and cleaned rows stay untouched."""

        destroy_delay.return_value = object()
        ended = self.make_session("cleanup-ended", MeetingLifecycleState.ENDED)
        failed = self.make_session("cleanup-failed", MeetingLifecycleState.FAILED)
        active = self.make_session("cleanup-active", MeetingLifecycleState.ACTIVE)
        cleaned = self.make_session("cleanup-complete", MeetingLifecycleState.ENDED)
        MeetingSession.objects.filter(pk=cleaned.pk).update(
            cleanup_completed_at=timezone.now(),
        )

        self.assertEqual(cleanup_finished_sessions.run(), 2)
        self.assertEqual(cleanup_finished_sessions.run(), 0)

        self.assertEqual(destroy_delay.call_count, 2)
        queued = {
            call.args[0]: uuid.UUID(call.args[1])
            for call in destroy_delay.call_args_list
        }
        self.assertEqual(set(queued), {str(ended.pk), str(failed.pk)})
        for session in (ended, failed):
            session.refresh_from_db()
            self.assertIsNotNone(session.cleanup_requested_at)
            self.assertEqual(session.cleanup_request_id, queued[str(session.pk)])
        active.refresh_from_db()
        cleaned.refresh_from_db()
        self.assertIsNone(active.cleanup_requested_at)
        self.assertIsNone(cleaned.cleanup_requested_at)

    @override_settings(MEETING_FINISHED_SESSION_CLEANUP_LEASE_SECONDS=60)
    @patch("apps.meetings.tasks.destroy_janus_room_for_session.delay")
    def test_expired_lease_is_replaced_and_old_task_is_superseded(
        self,
        destroy_delay,
    ) -> None:
        """Recovery gets a new token, so a late task cannot repeat remote teardown."""

        destroy_delay.return_value = object()
        session = self.make_session("cleanup-expired", MeetingLifecycleState.ENDED)
        old_request_id = uuid.uuid4()
        MeetingSession.objects.filter(pk=session.pk).update(
            cleanup_requested_at=timezone.now() - timedelta(minutes=5),
            cleanup_request_id=old_request_id,
        )

        self.assertEqual(cleanup_finished_sessions.run(), 1)
        session.refresh_from_db()
        self.assertNotEqual(session.cleanup_request_id, old_request_id)
        destroy_delay.assert_called_once_with(
            str(session.pk),
            str(session.cleanup_request_id),
        )

        with patch("apps.meetings.tasks.call_video_room_management_method") as janus_call:
            result = destroy_janus_room_for_session.run(
                str(session.pk),
                str(old_request_id),
            )

        self.assertEqual(result["status"], "superseded_cleanup_request")
        janus_call.assert_not_called()

    @override_settings(MEETING_FINISHED_SESSION_CLEANUP_LEASE_SECONDS=900)
    @patch("apps.meetings.tasks.dispatch_task", return_value=None)
    def test_broker_failure_releases_cleanup_lease(self, dispatch_task) -> None:
        """A failed publish remains eligible for the next periodic sweep."""

        session = self.make_session("cleanup-broker", MeetingLifecycleState.ENDED)

        self.assertEqual(cleanup_finished_sessions.run(), 0)

        dispatch_task.assert_called_once()
        session.refresh_from_db()
        self.assertIsNone(session.cleanup_requested_at)
        self.assertIsNone(session.cleanup_request_id)
