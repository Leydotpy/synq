"""Reconcile persistent meeting schedules with the current Celery registry."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from importlib import import_module

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from django_celery_beat.models import PeriodicTask, PeriodicTasks

from conf.celery import app as celery_app


MEETING_TASK_NAMESPACE = "apps.meetings.tasks."
QUARANTINE_MARKER = "[synq:quarantined-unregistered-celery-task]"


@dataclass(frozen=True)
class ScheduleSnapshot:
    """Fields used to apply a race-safe compare-and-update mutation."""

    pk: int
    name: str
    task: str
    description: str
    enabled: bool
    date_changed: datetime


def registered_meeting_task_names() -> set[str]:
    """Return meeting task names registered by the current checkout."""

    # Import explicitly so the command remains reliable even when invoked before
    # a worker has triggered Celery's lazy task autodiscovery.
    import_module("apps.meetings.tasks")
    celery_app.autodiscover_tasks(force=True)
    return {
        task_name
        for task_name in celery_app.tasks
        if task_name.startswith(MEETING_TASK_NAMESPACE)
    }


def configured_meeting_schedules() -> dict[str, str]:
    """Return configured schedule-name to meeting-task mappings."""

    configured = getattr(settings, "CELERY_BEAT_SCHEDULE", {})
    if not isinstance(configured, Mapping):
        raise CommandError("CELERY_BEAT_SCHEDULE must be a mapping.")

    meeting_schedules: dict[str, str] = {}
    for schedule_name, entry in configured.items():
        if not isinstance(schedule_name, str) or not isinstance(entry, Mapping):
            raise CommandError(
                "Every CELERY_BEAT_SCHEDULE entry must have a string name and "
                "a mapping value."
            )
        task_name = entry.get("task")
        if not isinstance(task_name, str):
            raise CommandError(
                f"CELERY_BEAT_SCHEDULE entry '{schedule_name}' has no string task name."
            )
        if task_name.startswith(MEETING_TASK_NAMESPACE):
            meeting_schedules[schedule_name] = task_name
    return meeting_schedules


def _add_quarantine_marker(description: str) -> str:
    """Append the reversible quarantine marker without losing operator notes."""

    if _has_quarantine_marker(description):
        return description
    description = description.rstrip()
    return f"{description}\n{QUARANTINE_MARKER}" if description else QUARANTINE_MARKER


def _remove_quarantine_marker(description: str) -> str:
    """Remove only the marker line and retain the rest of the description."""

    remaining_lines = [
        line
        for line in description.splitlines()
        if line.strip() != QUARANTINE_MARKER
    ]
    return "\n".join(remaining_lines).rstrip()


def _has_quarantine_marker(description: str) -> bool:
    """Return whether the exact marker is present as its own description line."""

    return any(line.strip() == QUARANTINE_MARKER for line in description.splitlines())


def _snapshots(queryset) -> list[ScheduleSnapshot]:
    """Materialize schedule rows needed for diagnostics and guarded writes."""

    return [
        ScheduleSnapshot(*row)
        for row in queryset.values_list(
            "pk",
            "name",
            "task",
            "description",
            "enabled",
            "date_changed",
        )
    ]


class Command(BaseCommand):
    """Quarantine stale schedules and restore only explicitly safe rows."""

    help = (
        "Disables enabled meeting schedules whose tasks are not registered, "
        "automatically restores schedules previously marked by this command, "
        "and validates source-controlled meeting schedules."
    )

    def add_arguments(self, parser) -> None:
        """Add diagnostic and deliberate restoration modes."""

        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report reconciliation changes without writing to the database.",
        )
        parser.add_argument(
            "--reenable-configured",
            action="store_true",
            help=(
                "Re-enable disabled rows whose schedule name and task exactly "
                "match CELERY_BEAT_SCHEDULE. Use deliberately; normal runs "
                "preserve unmarked operator-disabled rows."
            ),
        )

    def handle(self, *args, **options) -> None:
        """Validate configuration and reconcile persistent schedule state."""

        registered_tasks = registered_meeting_task_names()
        configured_schedules = configured_meeting_schedules()
        missing_configured = {
            name: task_name
            for name, task_name in configured_schedules.items()
            if task_name not in registered_tasks
        }
        if missing_configured:
            details = ", ".join(
                f"{name} -> {task_name}"
                for name, task_name in sorted(missing_configured.items())
            )
            raise CommandError(
                "Configured Celery schedules reference unregistered meeting "
                f"tasks: {details}"
            )

        retargets = self._find_configured_retargets(configured_schedules)
        stale_schedules, restore_schedules, explicitly_restorable_ids = (
            self._find_reconciliation_candidates(
                registered_tasks,
                configured_schedules,
                reenable_configured=options["reenable_configured"],
            )
        )
        if options["dry_run"]:
            retarget_ids = {row.pk for row, _target_task in retargets}
            stale_schedules = [
                row for row in stale_schedules if row.pk not in retarget_ids
            ]
            restore_by_pk = {row.pk: row for row in restore_schedules}
            for row, target_task in retargets:
                if row.enabled:
                    continue
                if _has_quarantine_marker(row.description) or options[
                    "reenable_configured"
                ]:
                    restore_by_pk[row.pk] = replace(row, task=target_task)
            restore_schedules = sorted(
                restore_by_pk.values(),
                key=lambda row: row.name,
            )
            self._write_dry_run(retargets, stale_schedules, restore_schedules)
            return

        retargeted: list[tuple[ScheduleSnapshot, str]] = []
        disabled: list[ScheduleSnapshot] = []
        restored: list[ScheduleSnapshot] = []
        changed_at = timezone.now()
        with transaction.atomic():
            for row, target_task in retargets:
                updated = PeriodicTask.objects.filter(
                    pk=row.pk,
                    name=row.name,
                    task=row.task,
                    description=row.description,
                    enabled=row.enabled,
                    date_changed=row.date_changed,
                ).update(
                    task=target_task,
                    date_changed=changed_at,
                )
                if updated:
                    retargeted.append((row, target_task))

            # Retargeting may make a marker-owned row immediately restorable,
            # so derive both action sets from the post-retarget state.
            stale_schedules, restore_schedules, explicitly_restorable_ids = (
                self._find_reconciliation_candidates(
                    registered_tasks,
                    configured_schedules,
                    reenable_configured=options["reenable_configured"],
                )
            )
            for row in stale_schedules:
                updated = (
                    PeriodicTask.objects.filter(
                        pk=row.pk,
                        name=row.name,
                        task=row.task,
                        description=row.description,
                        enabled=True,
                        date_changed=row.date_changed,
                        task__startswith=MEETING_TASK_NAMESPACE,
                    )
                    .exclude(task__in=registered_tasks)
                    .update(
                        enabled=False,
                        description=_add_quarantine_marker(row.description),
                        last_run_at=None,
                        date_changed=changed_at,
                    )
                )
                if updated:
                    disabled.append(row)

            for row in restore_schedules:
                restore_query = PeriodicTask.objects.filter(
                    pk=row.pk,
                    name=row.name,
                    task=row.task,
                    description=row.description,
                    enabled=False,
                    date_changed=row.date_changed,
                    task__in=registered_tasks,
                )
                if row.pk in explicitly_restorable_ids:
                    if configured_schedules.get(row.name) != row.task:
                        continue
                else:
                    restore_query = restore_query.filter(
                        description__contains=QUARANTINE_MARKER,
                    )
                updated = restore_query.update(
                    enabled=True,
                    description=_remove_quarantine_marker(row.description),
                    date_changed=changed_at,
                )
                if updated:
                    restored.append(row)

            if retargeted or disabled or restored:
                # QuerySet.update() bypasses PeriodicTask.save(), so explicitly
                # update the scheduler's cache timestamp in the same transaction;
                # both it and the row changes become visible together at commit.
                PeriodicTasks.update_changed()

        self._write_result(retargeted, disabled, restored)

    @staticmethod
    def _find_configured_retargets(
        configured_schedules: dict[str, str],
    ) -> list[tuple[ScheduleSnapshot, str]]:
        """Find persisted names whose task differs from source control."""

        retargets: list[tuple[ScheduleSnapshot, str]] = []
        for schedule_name, target_task in configured_schedules.items():
            rows = _snapshots(
                PeriodicTask.objects.filter(name=schedule_name)
                .exclude(task=target_task)
                .order_by("name")
            )
            retargets.extend((row, target_task) for row in rows)
        return sorted(retargets, key=lambda item: item[0].name)

    @staticmethod
    def _find_reconciliation_candidates(
        registered_tasks: set[str],
        configured_schedules: dict[str, str],
        *,
        reenable_configured: bool,
    ) -> tuple[list[ScheduleSnapshot], list[ScheduleSnapshot], set[int]]:
        """Find stale and safely restorable rows from current database state."""

        stale_schedules = _snapshots(
            PeriodicTask.objects.filter(
                enabled=True,
                task__startswith=MEETING_TASK_NAMESPACE,
            )
            .exclude(task__in=registered_tasks)
            .order_by("name")
        )
        marker_candidates = _snapshots(
            PeriodicTask.objects.filter(
                enabled=False,
                task__in=registered_tasks,
                description__contains=QUARANTINE_MARKER,
            ).order_by("name")
        )
        restore_by_pk = {
            row.pk: row
            for row in marker_candidates
            if _has_quarantine_marker(row.description)
        }
        explicitly_restorable_ids: set[int] = set()
        if reenable_configured:
            for schedule_name, task_name in configured_schedules.items():
                rows = _snapshots(
                    PeriodicTask.objects.filter(
                        name=schedule_name,
                        task=task_name,
                        enabled=False,
                    )
                )
                for row in rows:
                    restore_by_pk[row.pk] = row
                    explicitly_restorable_ids.add(row.pk)
        return (
            stale_schedules,
            sorted(restore_by_pk.values(), key=lambda row: row.name),
            explicitly_restorable_ids,
        )

    def _write_dry_run(
        self,
        retargets: list[tuple[ScheduleSnapshot, str]],
        stale_schedules: list[ScheduleSnapshot],
        restore_schedules: list[ScheduleSnapshot],
    ) -> None:
        """Describe proposed actions without mutating scheduler state."""

        if retargets:
            self.stdout.write(
                self.style.WARNING(
                    f"Would retarget {len(retargets)} configured Celery "
                    f"schedule(s): {self._retarget_details(retargets)}"
                )
            )
        if stale_schedules:
            self.stdout.write(
                self.style.WARNING(
                    f"Would disable {len(stale_schedules)} stale Celery "
                    f"schedule(s): {self._details(stale_schedules)}"
                )
            )
        if restore_schedules:
            self.stdout.write(
                self.style.WARNING(
                    f"Would re-enable {len(restore_schedules)} Celery "
                    f"schedule(s): {self._details(restore_schedules)}"
                )
            )
        if not retargets and not stale_schedules and not restore_schedules:
            self._write_aligned()

    def _write_result(
        self,
        retargeted: list[tuple[ScheduleSnapshot, str]],
        disabled: list[ScheduleSnapshot],
        restored: list[ScheduleSnapshot],
    ) -> None:
        """Report rows changed after guarded updates complete."""

        if retargeted:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Retargeted {len(retargeted)} configured Celery "
                    f"schedule(s): {self._retarget_details(retargeted)}"
                )
            )
        if disabled:
            self.stdout.write(
                self.style.WARNING(
                    f"Disabled {len(disabled)} stale Celery schedule(s): "
                    f"{self._details(disabled)}"
                )
            )
        if restored:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Re-enabled {len(restored)} Celery schedule(s): "
                    f"{self._details(restored)}"
                )
            )
        if not retargeted and not disabled and not restored:
            self._write_aligned()

    def _write_aligned(self) -> None:
        """Report a no-op reconciliation."""

        self.stdout.write(
            self.style.SUCCESS(
                "Celery schedule is aligned: no schedule rows need reconciliation."
            )
        )

    @staticmethod
    def _details(schedules: list[ScheduleSnapshot]) -> str:
        """Format schedule identities for operator-facing diagnostics."""

        return ", ".join(f"{row.name} -> {row.task}" for row in schedules)

    @staticmethod
    def _retarget_details(
        retargets: list[tuple[ScheduleSnapshot, str]],
    ) -> str:
        """Format configured task-name corrections."""

        return ", ".join(
            f"{row.name}: {row.task} -> {target_task}"
            for row, target_task in retargets
        )
