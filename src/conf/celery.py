"""Celery application bootstrap for background meeting orchestration tasks."""

from __future__ import annotations

import os

from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "conf.settings")

app = Celery("meet")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()


@app.task(bind=True, ignore_result=True)
def debug_task(self) -> None:
    """Log the current request for quick Celery diagnostics during development."""

    print(f"Celery request: {self.request!r}")
