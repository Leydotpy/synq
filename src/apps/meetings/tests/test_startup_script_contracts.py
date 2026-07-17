"""Regression contracts for the development supervisor and production workers."""

from __future__ import annotations

from pathlib import Path

from django.test import SimpleTestCase


SERVER_SRC = Path(__file__).resolve().parents[3]
SERVER_ROOT = SERVER_SRC.parent


class StartupScriptContractTests(SimpleTestCase):
    """Keep each launcher aligned with the restored Celery lifecycle."""

    def read_server_file(self, relative_path: str) -> str:
        """Read one version-controlled launcher as normalized text."""

        path = SERVER_ROOT / relative_path
        self.assertTrue(path.is_file(), f"Required startup file is missing: {path}")
        return path.read_text(encoding="utf-8")

    def test_development_supervisor_reconciles_before_starting_celery(self) -> None:
        """Beat must not see stale database rows before schedule reconciliation."""

        supervisor = self.read_server_file("scripts/supervisor.ps1")
        migration = supervisor.index('"manage.py", "migrate", "--noinput"')
        reconciliation = supervisor.index('"manage.py", "reconcile_celery_schedule"')
        worker = supervisor.index('Start-LoggedProcess -Name "Celery worker"')
        beat = supervisor.index('Start-LoggedProcess -Name "Celery beat"')

        self.assertLess(migration, reconciliation)
        self.assertLess(reconciliation, worker)
        self.assertLess(reconciliation, beat)

    def test_development_worker_consumes_both_queues_with_fair_prefetch(self) -> None:
        """Invitation email work must not be orphaned or starve lifecycle sweeps."""

        supervisor = self.read_server_file("scripts/supervisor.ps1")
        worker_line = next(
            line for line in supervisor.splitlines() if 'Name "Celery worker"' in line
        )

        self.assertIn('"-Q", "celery,meeting_email"', worker_line)
        self.assertIn('"--prefetch-multiplier=1"', worker_line)
        self.assertIn('"-P", "solo"', worker_line)

    def test_development_beat_uses_the_database_scheduler(self) -> None:
        """The process must consume the reconciled django-celery-beat rows."""

        supervisor = self.read_server_file("scripts/supervisor.ps1")
        beat_line = next(
            line for line in supervisor.splitlines() if 'Name "Celery beat"' in line
        )

        self.assertIn('"-A", "conf.celery", "beat"', beat_line)
        self.assertIn(
            '"--scheduler", "django_celery_beat.schedulers:DatabaseScheduler"',
            beat_line,
        )

    def test_django_readiness_is_owned_by_the_new_runtime(self) -> None:
        """A stale listener must never make a failed Django launch look ready."""

        supervisor = self.read_server_file("scripts/supervisor.ps1")
        port_guard = supervisor.index(
            'Assert-LocalTcpPortAvailable -ServiceName "Django"'
        )
        django_launch = supervisor.index('Start-LoggedProcess -Name "Django"')
        readiness = supervisor.index('Wait-ForProbe -Name "Django"')

        self.assertLess(port_guard, django_launch)
        self.assertLess(django_launch, readiness)
        self.assertIn("Test-ManagedProcessIdentity -Record $djangoRuntime", supervisor)
        self.assertIn(
            'Test-TcpPortOwnedByProcess -Address "127.0.0.1" '
            "-Port $BackendPort -ProcessId $djangoRuntime.Pid",
            supervisor,
        )

    def test_long_lived_python_children_are_recorded_for_shutdown(self) -> None:
        """Cleanup must not depend only on uv wrappers or CIM tree discovery."""

        supervisor = self.read_server_file("scripts/supervisor.ps1")
        stop_script = self.read_server_file("scripts/stop.ps1")
        managed_python = self.read_server_file("scripts/managed-python.py")

        for identity_path in (
            "$DjangoIdentityPath",
            "$CeleryWorkerIdentityPath",
            "$CeleryBeatIdentityPath",
        ):
            self.assertIn(identity_path, supervisor)
        self.assertIn("ownedProcesses = @($entry.OwnedProcesses", supervisor)
        self.assertIn("-SkipChildEnumeration", supervisor)
        self.assertIn("-RecordedLeaf", stop_script)
        self.assertIn('"runToken": run_token', managed_python)
        self.assertIn("os.replace(temporary_path, identity_path)", managed_python)
        self.assertIn("runpy.run_module", managed_python)

    def test_compatibility_launchers_forward_arguments_and_exit_codes(self) -> None:
        """Both documented launch locations must preserve supervisor semantics."""

        root_start = self.read_server_file("scripts/start.ps1")
        src_start = self.read_server_file("src/scripts/start.ps1")
        src_stop = self.read_server_file("src/scripts/stop.ps1")

        self.assertIn('& $supervisorPath @args', root_start)
        self.assertIn('exit $LASTEXITCODE', root_start)
        self.assertIn('..\\..\\scripts\\start.ps1', src_start)
        self.assertIn('& $resolvedStartScript @args', src_start)
        self.assertIn('exit $LASTEXITCODE', src_start)
        self.assertIn('..\\..\\scripts\\stop.ps1', src_stop)
        self.assertIn('& $resolvedStopScript @args', src_stop)
        self.assertIn('exit $LASTEXITCODE', src_stop)

    def test_production_processes_use_production_settings_and_scalable_worker(self) -> None:
        """Production must reconcile Beat and avoid Windows-only solo execution."""

        procfile = self.read_server_file("src/Procfile")
        process_lines = {
            name: command
            for name, command in (
                line.split(": ", 1) for line in procfile.splitlines() if line.strip()
            )
        }

        self.assertEqual(set(process_lines), {"web", "worker", "beat"})
        for command in process_lines.values():
            self.assertIn("DJANGO_SETTINGS_MODULE=conf.settings.production", command)
        worker = process_lines["worker"]
        self.assertIn("-Q celery,meeting_email", worker)
        self.assertIn("--prefetch-multiplier=1", worker)
        self.assertIn("--concurrency=${CELERY_WORKER_CONCURRENCY:-4}", worker)
        self.assertNotIn("-P solo", worker)
        beat = process_lines["beat"]
        self.assertIn("manage.py reconcile_celery_schedule && exec", beat)
        self.assertIn("django_celery_beat.schedulers:DatabaseScheduler", beat)
