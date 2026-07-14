"""Unauthenticated process probes for deployment orchestration."""

from __future__ import annotations

from django.conf import settings
from django.db import connection
from django.http import JsonResponse


def liveness(_request):
    """Confirm that the Django process can serve a request."""

    return JsonResponse({"status": "ok"})


def readiness(_request):
    """Confirm that stateful dependencies required by the ASGI app are ready."""

    checks = {
        "database": _database_is_ready(),
        "redis": _redis_is_ready(),
        "janus": _janus_is_ready(),
    }
    is_ready = all(checks.values())
    return JsonResponse(
        {"status": "ok" if is_ready else "unavailable", "checks": checks},
        status=200 if is_ready else 503,
    )


def _database_is_ready() -> bool:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return True
    except Exception:
        return False


def _redis_is_ready() -> bool:
    if not getattr(settings, "REDIS_URL", ""):
        return True
    try:
        from django_redis import get_redis_connection

        return bool(get_redis_connection("default").ping())
    except Exception:
        return False


def _janus_is_ready() -> bool:
    if not getattr(settings, "JANUS_ENABLED", True):
        return True
    try:
        from janus_api import Janus

        return Janus.get_session() is not None
    except Exception:
        return False
