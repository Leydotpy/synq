"""Tests for source-controlled and persistent Celery Beat reconciliation."""

from __future__ import annotations

import os
from io import StringIO
from unittest.mock import patch

from django.conf import settings
from django.core.management import CommandError, call_command
from django.test import TestCase, override_settings
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask, PeriodicTasks
from django_celery_beat.schedulers import DatabaseScheduler

from apps.management.commands.reconcile_celery_schedule import (
    QUARANTINE_MARKER,
    configured_meeting_schedules,
    registered_meeting_task_names,
)
from apps.meetings.tasks import mark_stale_connections
from conf.celery import app as celery_app
from conf.settings.base import env_positive_int


COMMAND_MODULE = "apps.management.commands.reconcile_celery_schedule"
EXPECTED_SCHEDULES = {
    "recover-stale-meeting-provisioning": (
        "apps.meetings.tasks.recover_stale_provisioning_sessions",
        30.0,
    ),
    "mark-stale-meeting-connections": (
        "apps.meetings.tasks.mark_stale_connections",
        30.0,
    ),
    "cleanup-finished-meeting-sessions": (
        "apps.meetings.tasks.cleanup_finished_sessions",
        300.0,
    ),
    "end-scheduled-meeting-sessions": (
        "apps.meetings.tasks.end_scheduled_sessions",
        30.0,
    ),
    "expire-pending-meeting-join-requests": (
        "apps.meetings.tasks.expire_pending_join_requests",
        60.0,
    ),
    "send-due-meeting-invitation-reminders": (
        "apps.meetings.tasks.queue_due_meeting_invitation_emails",
        60.0,
    ),
}


class CeleryBeatConfigurationTests(TestCase):
    """Keep clean-install schedule materialization aligned with the registry."""

    def test_source_controlled_schedules_match_registry_and_defaults(self) -> None:
        configured = configured_meeting_schedules()
        registered = registered_meeting_task_names()

        self.assertEqual(
            configured,
            {name: task_name for name, (task_name, _seconds) in EXPECTED_SCHEDULES.items()},
        )
        self.assertTrue(set(configured.values()).issubset(registered))
        for name, (_task_name, seconds) in EXPECTED_SCHEDULES.items():
            self.assertEqual(settings.CELERY_BEAT_SCHEDULE[name]["schedule"], seconds)
            self.assertEqual(
                settings.CELERY_BEAT_SCHEDULE[name]["options"]["expire_seconds"],
                seconds,
            )

    def test_sweep_cadence_requires_positive_whole_seconds(self) -> None:
        with patch.dict(os.environ, {"TEST_SWEEP_SECONDS": "30.0"}):
            self.assertEqual(env_positive_int("TEST_SWEEP_SECONDS", 5), 30)
        for invalid in ("0", "-1", "0.5", "nan", "not-a-number"):
            with self.subTest(invalid=invalid), patch.dict(
                os.environ,
                {"TEST_SWEEP_SECONDS": invalid},
            ):
                with self.assertRaises(ValueError):
                    env_positive_int("TEST_SWEEP_SECONDS", 5)

    def test_database_scheduler_materializes_all_schedules_on_empty_database(self) -> None:
        scheduler = DatabaseScheduler(app=celery_app, lazy=True)
        scheduler.update_from_dict(settings.CELERY_BEAT_SCHEDULE)

        schedules = {
            row.name: (
                row.task,
                float(row.interval.every),
                row.interval.period,
                row.expire_seconds,
            )
            for row in PeriodicTask.objects.select_related("interval").all()
        }
        self.assertEqual(
            schedules,
            {
                name: (task_name, seconds, IntervalSchedule.SECONDS, int(seconds))
                for name, (task_name, seconds) in EXPECTED_SCHEDULES.items()
            },
        )
        for row in PeriodicTask.objects.all():
            entry = DatabaseScheduler.Entry(row, app=celery_app)
            self.assertEqual(
                entry.options["expires"],
                int(EXPECTED_SCHEDULES[row.name][1]),
            )
            self.assertNotIn("expire_seconds", entry.options)

    def test_static_schedule_update_preserves_operator_disabled_state(self) -> None:
        task_name, _seconds = EXPECTED_SCHEDULES["mark-stale-meeting-connections"]
        interval = IntervalSchedule.objects.create(
            every=30,
            period=IntervalSchedule.SECONDS,
        )
        schedule = PeriodicTask.objects.create(
            name="mark-stale-meeting-connections",
            task=task_name,
            interval=interval,
            enabled=False,
        )

        scheduler = DatabaseScheduler(app=celery_app, lazy=True)
        scheduler.update_from_dict(
            {
                schedule.name: settings.CELERY_BEAT_SCHEDULE[schedule.name],
            }
        )

        schedule.refresh_from_db()
        self.assertFalse(schedule.enabled)


class ReconcileCeleryScheduleTests(TestCase):
    """Quarantine stale rows and restore only rows safe to reactivate."""

    def setUp(self) -> None:
        self.interval = IntervalSchedule.objects.create(
            every=30,
            period=IntervalSchedule.SECONDS,
        )
        self.registered = set(configured_meeting_schedules().values())

    def create_schedule(
        self,
        *,
        name: str,
        task: str,
        enabled: bool = True,
        description: str = "",
    ) -> PeriodicTask:
        """Create one interval-backed schedule for a command test."""

        return PeriodicTask.objects.create(
            name=name,
            task=task,
            interval=self.interval,
            enabled=enabled,
            description=description,
        )

    def call_reconcile(self, **options) -> StringIO:
        """Run reconciliation against a controlled task registry."""

        output = StringIO()
        with patch(
            f"{COMMAND_MODULE}.registered_meeting_task_names",
            return_value=self.registered,
        ):
            call_command("reconcile_celery_schedule", stdout=output, **options)
        return output

    def test_quarantines_only_enabled_unregistered_meeting_tasks(self) -> None:
        stale = self.create_schedule(
            name="stale-meeting-task",
            task="apps.meetings.tasks.removed_task",
            description="Operator note",
        )
        stale.last_run_at = timezone.now()
        stale.save(update_fields=["last_run_at"])
        registered = self.create_schedule(
            name="registered-meeting-task",
            task=mark_stale_connections.name,
        )
        external = self.create_schedule(
            name="external-task",
            task="another_app.tasks.optional_worker_task",
        )
        already_disabled = self.create_schedule(
            name="already-disabled-meeting-task",
            task="apps.meetings.tasks.another_removed_task",
            enabled=False,
        )
        before_change = PeriodicTasks.last_change()

        with patch.object(
            PeriodicTasks,
            "update_changed",
            wraps=PeriodicTasks.update_changed,
        ) as update_changed:
            output = self.call_reconcile()

        for schedule in (stale, registered, external, already_disabled):
            schedule.refresh_from_db()
        self.assertFalse(stale.enabled)
        self.assertEqual(stale.description, f"Operator note\n{QUARANTINE_MARKER}")
        self.assertIsNone(stale.last_run_at)
        self.assertTrue(registered.enabled)
        self.assertTrue(external.enabled)
        self.assertFalse(already_disabled.enabled)
        self.assertEqual(already_disabled.description, "")
        self.assertEqual(update_changed.call_count, 1)
        after_change = PeriodicTasks.last_change()
        self.assertGreater(after_change, before_change)
        self.assertIn("Disabled 1 stale Celery schedule", output.getvalue())

        with patch.object(PeriodicTasks, "update_changed") as second_update:
            second_output = self.call_reconcile()
        second_update.assert_not_called()
        self.assertEqual(PeriodicTasks.last_change(), after_change)
        self.assertIn("no schedule rows need reconciliation", second_output.getvalue())

    def test_auto_restores_only_marker_owned_registered_rows(self) -> None:
        marked = self.create_schedule(
            name="previously-quarantined",
            task=mark_stale_connections.name,
            enabled=False,
            description=f"Keep this note\n{QUARANTINE_MARKER}",
        )
        intentional = self.create_schedule(
            name="intentionally-disabled",
            task=mark_stale_connections.name,
            enabled=False,
            description="Operator disabled this schedule",
        )
        marker_lookalike = self.create_schedule(
            name="marker-mentioned-inline",
            task=mark_stale_connections.name,
            enabled=False,
            description=f"Incident note mentions {QUARANTINE_MARKER} inline",
        )

        output = self.call_reconcile()

        marked.refresh_from_db()
        intentional.refresh_from_db()
        marker_lookalike.refresh_from_db()
        self.assertTrue(marked.enabled)
        self.assertEqual(marked.description, "Keep this note")
        self.assertFalse(intentional.enabled)
        self.assertEqual(intentional.description, "Operator disabled this schedule")
        self.assertFalse(marker_lookalike.enabled)
        self.assertIn("Re-enabled 1 Celery schedule", output.getvalue())

    def test_explicit_mode_reenables_only_exact_configured_pairs(self) -> None:
        task_name, _seconds = EXPECTED_SCHEDULES["mark-stale-meeting-connections"]
        configured = self.create_schedule(
            name="mark-stale-meeting-connections",
            task=task_name,
            enabled=False,
        )
        same_task_different_name = self.create_schedule(
            name="operator-disabled-copy",
            task=task_name,
            enabled=False,
        )
        invitation = self.create_schedule(
            name="send-due-meeting-invitation-reminders",
            task="apps.meetings.tasks.queue_due_meeting_invitation_emails",
            enabled=False,
        )

        output = self.call_reconcile(reenable_configured=True)

        configured.refresh_from_db()
        same_task_different_name.refresh_from_db()
        invitation.refresh_from_db()
        self.assertTrue(configured.enabled)
        self.assertFalse(same_task_different_name.enabled)
        self.assertTrue(invitation.enabled)
        self.assertIn("Re-enabled 2 Celery schedule", output.getvalue())

    def test_dry_run_reports_all_actions_without_cache_invalidation(self) -> None:
        stale = self.create_schedule(
            name="stale-meeting-task",
            task="apps.meetings.tasks.removed_task",
        )
        marked = self.create_schedule(
            name="previously-quarantined",
            task=mark_stale_connections.name,
            enabled=False,
            description=QUARANTINE_MARKER,
        )
        configured = self.create_schedule(
            name="mark-stale-meeting-connections",
            task=mark_stale_connections.name,
            enabled=False,
        )
        before_change = PeriodicTasks.last_change()

        with patch.object(PeriodicTasks, "update_changed") as update_changed:
            output = self.call_reconcile(
                dry_run=True,
                reenable_configured=True,
            )

        for schedule in (stale, marked, configured):
            schedule.refresh_from_db()
        self.assertTrue(stale.enabled)
        self.assertFalse(marked.enabled)
        self.assertFalse(configured.enabled)
        update_changed.assert_not_called()
        self.assertEqual(PeriodicTasks.last_change(), before_change)
        self.assertIn("Would disable 1 stale Celery schedule", output.getvalue())
        self.assertIn("Would re-enable 2 Celery schedule", output.getvalue())

    @override_settings(
        CELERY_BEAT_SCHEDULE={
            "broken-schedule": {
                "task": "apps.meetings.tasks.not_registered",
                "schedule": 30.0,
            },
        }
    )
    def test_unregistered_configured_task_fails_before_mutation(self) -> None:
        stale = self.create_schedule(
            name="stale-meeting-task",
            task="apps.meetings.tasks.removed_task",
        )

        with patch(
            f"{COMMAND_MODULE}.registered_meeting_task_names",
            return_value=set(),
        ), patch.object(PeriodicTasks, "update_changed") as update_changed:
            with self.assertRaisesMessage(CommandError, "broken-schedule"):
                call_command("reconcile_celery_schedule")

        stale.refresh_from_db()
        self.assertTrue(stale.enabled)
        update_changed.assert_not_called()

    def test_guarded_update_does_not_disable_concurrently_retargeted_row(self) -> None:
        stale = self.create_schedule(
            name="retargeted-during-reconcile",
            task="apps.meetings.tasks.removed_task",
        )

        def retarget_before_update(description: str) -> str:
            PeriodicTask.objects.filter(pk=stale.pk).update(
                task=mark_stale_connections.name,
            )
            return QUARANTINE_MARKER

        with patch(
            f"{COMMAND_MODULE}._add_quarantine_marker",
            side_effect=retarget_before_update,
        ), patch.object(PeriodicTasks, "update_changed") as update_changed:
            output = self.call_reconcile()

        stale.refresh_from_db()
        self.assertTrue(stale.enabled)
        self.assertEqual(stale.task, mark_stale_connections.name)
        self.assertEqual(stale.description, "")
        update_changed.assert_not_called()
        self.assertIn("no schedule rows need reconciliation", output.getvalue())

    def test_configured_retarget_and_marker_restore_finish_in_same_run(self) -> None:
        target_task, _seconds = EXPECTED_SCHEDULES[
            "recover-stale-meeting-provisioning"
        ]
        schedule = self.create_schedule(
            name="recover-stale-meeting-provisioning",
            task="apps.meetings.tasks.legacy_recovery_task",
            enabled=False,
            description=f"Retain this note\n{QUARANTINE_MARKER}",
        )

        output = self.call_reconcile()

        schedule.refresh_from_db()
        self.assertEqual(schedule.task, target_task)
        self.assertTrue(schedule.enabled)
        self.assertEqual(schedule.description, "Retain this note")
        self.assertIn("Retargeted 1 configured Celery schedule", output.getvalue())
        self.assertIn("Re-enabled 1 Celery schedule", output.getvalue())

    def test_guarded_restore_preserves_concurrent_admin_edit(self) -> None:
        schedule = self.create_schedule(
            name="marked-but-edited",
            task=mark_stale_connections.name,
            enabled=False,
            description=QUARANTINE_MARKER,
        )

        def edit_before_restore(description: str) -> str:
            current = PeriodicTask.objects.get(pk=schedule.pk)
            current.queue = "operator-maintenance"
            current.save()
            return ""

        with patch(
            f"{COMMAND_MODULE}._remove_quarantine_marker",
            side_effect=edit_before_restore,
        ):
            output = self.call_reconcile()

        schedule.refresh_from_db()
        self.assertFalse(schedule.enabled)
        self.assertEqual(schedule.queue, "operator-maintenance")
        self.assertEqual(schedule.description, QUARANTINE_MARKER)
        self.assertIn("no schedule rows need reconciliation", output.getvalue())
