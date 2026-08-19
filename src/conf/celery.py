"""Celery application bootstrap for background meeting orchestration tasks."""

from __future__ import annotations

import os

from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown, worker_shutdown

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "conf.settings")

app = Celery("meet")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()


@worker_process_init.connect
def reset_janus_runtime_after_fork(**_kwargs) -> None:
    """Discard any process-local Janus objects inherited by a prefork child."""

    from apps.meetings.services.janus import janus_runtime

    janus_runtime.reset_after_fork()


@worker_process_shutdown.connect
@worker_shutdown.connect
def stop_janus_runtime_for_worker(**_kwargs) -> None:
    """Close worker-owned Janus sessions before Celery exits or recycles."""

    from apps.meetings.services.janus import janus_runtime

    janus_runtime.stop_background()


@app.task(bind=True, ignore_result=True)
def debug_task(self) -> None:
    """Log the current request for quick Celery diagnostics during development."""

    print(f"Celery request: {self.request!r}")
