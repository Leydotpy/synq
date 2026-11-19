from datetime import UTC, datetime
from unittest.mock import patch

from django.test import TestCase, override_settings
from jrtc_signalling.telemetry import Histogram, trace_ref

from apps.meetings.models import GatewayLifecycleQuarantine
from apps.meetings.services import gateway_telemetry as telemetry


class GatewayTelemetryTests(TestCase):
    path = "/api/v1/meetings/internal/gateway/metrics/"
    key = b"a" * 32

    def test_off_without_configuration(self):
        self.assertEqual(self.client.get(self.path, secure=True).status_code, 404)

    @override_settings(GATEWAY_METRICS_KEY=key)
    def test_https_bearer_and_no_database_work_before_authorization(self):
        header = {"HTTP_AUTHORIZATION": "Bearer " + self.key.decode()}
        with self.assertNumQueries(0):
            for path, args in (
                (self.path, {"secure": True}),
                (self.path, header),
                (self.path + "?token=private", {"secure": True, **header}),
            ):
                self.assertEqual(self.client.get(path, **args).status_code, 403)
        GatewayLifecycleQuarantine.objects.create(
            digest="d" * 64, reason_code="private"
        )
        response = self.client.get(self.path, secure=True, **header)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'synq_gateway_dlq_entries{kind="lifecycle"} 1', response.content)
        self.assertNotIn(b"private", response.content)
        self.assertNotIn(self.key, response.content)

    def test_sse_history_and_initial_snapshots_do_not_inflate_live_lag(self):
        with (
            patch.object(telemetry, "sse_live", Histogram()) as live,
            patch.object(telemetry, "sse_replay", Histogram()) as replay,
            patch.object(telemetry.time, "time", return_value=100),
        ):
            for context in ("live", "replay"):
                telemetry.observe_frame(
                    (
                        "state.changed",
                        "ignored",
                        {
                            "context": context,
                            "occurred_at": datetime.fromtimestamp(99, UTC).isoformat(),
                        },
                    )
                )
            telemetry.observe_frame(("state.snapshot", "ignored", {}))
            for histogram in (live, replay):
                self.assertIn("delay_sum 1.0\n", histogram.render("delay"))
                self.assertIn("delay_count 1", histogram.render("delay"))

    @override_settings(GATEWAY_TRACE_KEY=key)
    def test_logvista_receives_only_matching_keyed_references(self):
        with (
            patch.object(telemetry, "bind_context") as context,
            patch.object(telemetry, "logger") as logger,
        ):
            telemetry.log_observed_session("private-meeting", "private-session")
        context.assert_called_once_with(
            meeting_ref=trace_ref(self.key, "meeting", "private-meeting"),
            session_ref=trace_ref(self.key, "session", "private-session"),
        )
        logger.info.assert_called_once_with("gateway.session_observed")
