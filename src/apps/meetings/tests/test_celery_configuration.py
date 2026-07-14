"""Coverage for Celery task registration and enqueue behavior."""

from __future__ import annotations

from unittest.mock import Mock

from django.test import SimpleTestCase

from apps.meetings.services.lifecycle import dispatch_task
from conf.celery import app


class CeleryConfigurationTests(SimpleTestCase):
    """Verify meeting tasks are discoverable and dispatched asynchronously."""

    def test_meeting_tasks_are_registered(self):
        app.autodiscover_tasks(force=True)

        self.assertIn("apps.meetings.tasks.provision_janus_room_for_session", app.tasks)
        self.assertIn("apps.meetings.tasks.attach_participant_media_handles", app.tasks)
        self.assertIn("apps.meetings.tasks.mark_stale_connections", app.tasks)
        self.assertIn("apps.meetings.tasks.send_meeting_invitation_email", app.tasks)
        self.assertIn(
            "apps.meetings.tasks.queue_due_meeting_invitation_emails",
            app.tasks,
        )

    def test_dispatch_task_uses_delay(self):
        task = Mock()
        task.name = "tests.fake_task"
        task.delay.return_value = "async-result"

        result = dispatch_task(task, "participant-id")

        self.assertEqual(result, "async-result")
        task.delay.assert_called_once_with("participant-id")

    def test_dispatch_task_logs_broker_failures(self):
        task = Mock()
        task.name = "tests.failing_task"
        task.delay.side_effect = RuntimeError("broker down")

        with self.assertLogs("apps.meetings.services.lifecycle", level="ERROR") as logs:
            result = dispatch_task(task, "session-id")

        self.assertIsNone(result)
        self.assertIn("Unable to enqueue Celery task 'tests.failing_task'", "\n".join(logs.output))
