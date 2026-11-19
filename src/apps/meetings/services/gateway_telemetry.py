"""Observations at the authority boundary and the SSE producer."""

import time
from datetime import datetime

from django.conf import settings
from django.db.models import Count, Min
from django.http import HttpResponse
from django.views.decorators.http import require_GET
from jrtc_signalling.telemetry import CONTENT_TYPE, Histogram, authorized, trace_ref
from logvista import bind_context, get_logger

from apps.meetings.models import (
    GatewayAuthorizationOutbox,
    GatewayCommandQuarantine,
    GatewayLifecycleQuarantine,
)

logger = get_logger(__name__)
sse_live = Histogram()
sse_replay = Histogram()


def observe_frame(update):
    kind, _, payload = update
    if kind == "state.changed":
        at = datetime.fromisoformat(payload["occurred_at"]).timestamp()
        histogram = sse_replay if payload["context"] == "replay" else sse_live
        histogram.observe(max(0, time.time() - at))


def log_observed_session(meeting, session):
    key = settings.GATEWAY_TRACE_KEY
    if key:
        with bind_context(
            meeting_ref=trace_ref(key, "meeting", meeting),
            session_ref=trace_ref(key, "session", session),
        ):
            logger.info("gateway.session_observed")


@require_GET
def metrics(request):
    headers = {"Cache-Control": "no-store"}
    key = settings.GATEWAY_METRICS_KEY
    if not key:
        return HttpResponse(status=404, headers=headers)
    if request.GET or not authorized(
        key, request.is_secure(), request.headers.get("Authorization", "")
    ):
        return HttpResponse(status=403, headers=headers)
    pending = GatewayAuthorizationOutbox.objects.filter(
        status__in=("pending", "accepted")
    ).aggregate(count=Count("pk"), oldest=Min("created_at"))
    age = (
        max(0, time.time() - pending["oldest"].timestamp()) if pending["oldest"] else 0
    )
    lines = [
        sse_live.render("synq_gateway_sse_live_lag_seconds"),
        sse_replay.render("synq_gateway_sse_replay_lag_seconds"),
        f"synq_gateway_commands_outstanding {pending['count']}",
        f"synq_gateway_oldest_command_seconds {age}",
        f'synq_gateway_dlq_entries{{kind="command"}} {GatewayCommandQuarantine.objects.count()}',
        f'synq_gateway_dlq_entries{{kind="lifecycle"}} {GatewayLifecycleQuarantine.objects.count()}',
    ]
    return HttpResponse(
        "\n".join(lines) + "\n", headers=headers, content_type=CONTENT_TYPE
    )
