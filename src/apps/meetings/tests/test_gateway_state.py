"""Projection authority, bounded replay, actual ASGI streaming and Redis/Broka."""

import asyncio
from dataclasses import replace
from datetime import timedelta
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from unittest import skipUnless
from unittest.mock import patch
import uuid

import jwt

from asgiref.sync import async_to_sync, sync_to_async
from asgiref.testing import ApplicationCommunicator
from django.conf import settings
from django.core.asgi import get_asgi_application
from django.db import close_old_connections, connection, connections, transaction
from django.test import (
    TestCase,
    TransactionTestCase,
    override_settings,
    skipUnlessDBFeature,
)
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from redis.asyncio import Redis

from apps.meetings.api.gateway_state import (
    StreamSlots,
    credentials,
    encode_frame,
    state_stream,
)
from apps.meetings.models import (
    GatewayCommandRoute,
    GatewaySessionObservation,
    GatewayStateChange,
    GatewayStateProjection,
    Participant,
    ParticipantStatus,
)
from apps.meetings.services.gateway_event_worker import GatewayEventWorker
from apps.meetings.services.gateway_presence import PresenceProjector
from apps.meetings.services.gateway_state import next_update, reconcile, snapshot
from apps.meetings.services.gateway_tickets import GatewayDenied
from apps.meetings.services.lifecycle import MeetingLifecycleService as Lifecycle
from apps.meetings.tests.test_gateway_events import EventFixture
from apps.meetings.tests import test_gateway_ticket_races as race_support
from jrtc_signalling.bus import LIFECYCLE
from jrtc_signalling.bus_transport import Streams
from jrtc_signalling.contracts import validate_message


class StateFixture(EventFixture):
    def setUp(self):
        super().setUp()
        self.state_url = f"/api/v1/meetings/sessions/{self.session.pk}/gateway/state/"
        self.stream_url = f"/api/v1/meetings/sessions/{self.session.pk}/gateway/events/"

    def state(self, profile=None, **kw):
        return snapshot(self.session.pk, (profile or self.host).pk, **kw)

    def update(self, cursor, profile=None, **kw):
        return next_update(self.session.pk, (profile or self.host).pk, cursor, **kw)


@override_settings(GATEWAY_STATE_ENABLED=True)
class GatewayStateTests(StateFixture, TestCase):
    def test_bearer_expiry_can_only_shorten_stream_lifetime(self):
        from rest_framework.exceptions import AuthenticationFailed

        for expiry in (time.time() - 1, True, float("nan"), "tomorrow"):
            request = SimpleNamespace(
                auth=jwt.encode({"exp": expiry}, "test-key-" * 4, algorithm="HS256")
            )
            with self.assertRaises(AuthenticationFailed):
                credentials(request)
        expiry = time.time() + 10
        request = SimpleNamespace(
            auth=jwt.encode({"exp": expiry}, "test-key-" * 4, algorithm="HS256")
        )
        deadline, session_key = credentials(request)
        self.assertEqual(deadline, expiry)
        self.assertIsNone(session_key)

    def test_terminal_meeting_clears_presence_and_reports_end(self):
        self.receive(self.event(state="MEDIA_CONNECTED"))
        start = self.state()
        with patch("apps.meetings.tasks.queue_janus_room_cleanup"):
            Lifecycle.end_session(session=self.session, actor_profile=self.host)
        state = self.state()
        self.assertEqual(state["meeting"]["lifecycle"], "ended")
        self.assertFalse(state["can_manage_waiting_room"])
        self.assertEqual(
            state["local_participant"]["presence"]["connection"], "OFFLINE"
        )
        self.assertGreater(state["projection_version"], start["projection_version"])

    def test_disabled_profile_and_removed_host_are_denied_despite_membership(self):
        self.host.is_active = False
        self.host.save(update_fields=["is_active"])
        with self.assertRaises(GatewayDenied):
            self.state()
        self.host.is_active = True
        self.host.save(update_fields=["is_active"])
        Participant.objects.filter(pk=self.participant.pk).update(
            status=ParticipantStatus.REMOVED
        )
        with self.assertRaises(GatewayDenied):
            self.state()

    @override_settings(CORS_ALLOWED_ORIGINS=["https://meet.example"])
    def test_cross_origin_resume_header_has_explicit_allowlist(self):
        response = self.client.options(
            self.stream_url,
            HTTP_ORIGIN="https://meet.example",
            HTTP_ACCESS_CONTROL_REQUEST_METHOD="GET",
            HTTP_ACCESS_CONTROL_REQUEST_HEADERS="authorization,last-event-id",
        )
        self.assertEqual(
            response["Access-Control-Allow-Origin"], "https://meet.example"
        )
        self.assertIn("last-event-id", response["Access-Control-Allow-Headers"])
        response = self.client.options(
            self.stream_url,
            HTTP_ORIGIN="https://untrusted.example",
            HTTP_ACCESS_CONTROL_REQUEST_METHOD="GET",
        )
        self.assertNotIn("Access-Control-Allow-Origin", response)

    def test_approval_does_not_imply_connection(self):
        self.intent()
        self.decision()
        state = self.state(self.guest)
        self.assertEqual(state["local_participant"]["admission"], "admitted")
        self.assertEqual(
            state["local_participant"]["presence"]["connection"], "OFFLINE"
        )
        self.guest_participant.refresh_from_db()
        self.assertIsNone(self.guest_participant.joined_at)

    def test_waiting_user_only_sees_self_and_moderator_sees_queue(self):
        self.intent()
        self.receive(self.event(state="MEDIA_CONNECTED"))
        guest = self.state(self.guest)
        self.assertEqual(guest["participants"], [])
        self.assertFalse(guest["can_manage_waiting_room"])
        self.assertEqual(guest["local_participant"]["admission"], "waiting")
        self.assertNotIn("janus", guest)
        host = self.state()
        self.assertEqual(
            host["participants"][0]["participant_id"], str(self.guest_participant.pk)
        )
        self.assertTrue(host["can_manage_waiting_room"])
        self.assertNotIn("eligible", json.dumps(guest))

    def test_admitted_user_cannot_read_other_waiting_users(self):
        self.intent()
        self.decision()
        third = self.profile("third")
        from apps.meetings.services.gateway_admission import request_intent

        request_intent(session_id=self.session.pk, profile_id=third.pk)
        self.assertEqual(len(self.state(self.guest)["participants"]), 1)
        self.assertEqual(len(self.state()["participants"]), 2)

    def test_semantic_versions_duplicate_refresh_and_read_do_not_write_projection(self):
        self.receive(self.event())
        version = self.state()["projection_version"]
        event = self.event(2)
        with CaptureQueriesContext(connection) as queries:
            self.receive(event)
            self.receive(event)
            for _ in range(3):
                self.state()
        writes = [
            q["sql"]
            for q in queries
            if q["sql"].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            and (
                "gatewaystateprojection" in q["sql"] or "gatewaystatechange" in q["sql"]
            )
        ]
        self.assertEqual(writes, [])
        self.assertEqual(self.state()["projection_version"], version)
        self.receive(self.event(3, "MEDIA_CONNECTED"))
        self.assertEqual(self.state()["projection_version"], version + 1)

    def test_old_session_and_epoch_cannot_restore_online_presence(self):
        self.receive(self.event(state="MEDIA_CONNECTED"))
        old = self.event(3, "MEDIA_CONNECTED")
        before = self.state()["projection_version"]
        self.issue()  # New participant session invalidates the old observation.
        state = self.state()
        self.assertGreater(state["projection_version"], before)
        self.assertEqual(
            state["local_participant"]["presence"]["connection"], "OFFLINE"
        )
        self.assertEqual(self.receive(old), "stale")
        self.assertEqual(self.state(), state)

    def test_revocation_filters_immediately_without_broker_delivery(self):
        self.receive(self.event(state="MEDIA_CONNECTED"))
        Lifecycle.update_participant_permissions(
            session=self.session,
            actor_profile=self.host,
            participant=self.participant,
            updates={"can_publish_audio": False},
        )
        state = self.state()["local_participant"]
        self.assertFalse(state["permissions"]["audio"])
        self.assertFalse(state["presence"]["audio"])
        self.assertEqual(state["presence"]["connection"], "OFFLINE")

    def test_lease_and_observation_expiry_clear_presence(self):
        self.receive(self.event(state="MEDIA_CONNECTED"))
        now = timezone.now()
        GatewayCommandRoute.objects.filter(session=self.session).update(
            lease_expires_at=now + timedelta(minutes=10)
        )
        before = reconcile(self.session.pk, now=now)
        after = reconcile(self.session.pk, now=now + timedelta(seconds=121))
        self.assertTrue(before["presence"])
        self.assertFalse(after["presence"])
        self.assertGreater(after["projection_version"], before["projection_version"])
        GatewayCommandRoute.objects.filter(session=self.session).update(
            lease_expires_at=now - timedelta(seconds=1)
        )
        self.assertFalse(reconcile(self.session.pk)["presence"])

    def test_lost_fanout_replays_stable_ids_and_contract(self):
        start = self.state()
        self.receive(self.event())
        self.receive(self.event(2, "MEDIA_CONNECTED"))
        first = self.update(start["event_id"], replay=True)
        validate_message(first[2], schema="state-notification")
        self.assertEqual(first, self.update(start["event_id"], replay=True))
        second = self.update(first[1], replay=True)
        self.assertEqual(first[2]["context"], "replay")
        self.assertEqual(
            second[2]["projection_version"], first[2]["projection_version"] + 1
        )
        self.assertIsNone(self.update(second[1]))

    def test_waiting_replay_never_leaks_other_participant_ids(self):
        self.intent()
        start = self.state(self.guest)
        self.receive(self.event())
        update = self.update(start["event_id"], self.guest, replay=True)
        self.assertEqual(update[2]["participant_ids"], [])
        self.assertNotIn(str(self.participant.pk), json.dumps(update))

    def test_gap_foreign_future_and_malformed_cursor_get_quiet_snapshot(self):
        start = self.state()
        self.receive(self.event())
        GatewayStateChange.objects.filter(version=start["projection_version"]).delete()
        for cursor in (
            start["event_id"],
            f"1:{uuid.uuid4()}",
            f"999999:{uuid.uuid4()}",
            "x",
            "",
        ):
            result = self.update(cursor)
            self.assertEqual(result[0], "state.snapshot")
            self.assertEqual(result[2]["context"], "snapshot")
            self.assertEqual(
                result[2]["projection_version"], self.state()["projection_version"]
            )

    def test_retention_is_bounded_and_snapshot_survives_trimming(self):
        with patch("apps.meetings.services.gateway_state.RETAIN", 3):
            for number in range(1, 8):
                self.receive(
                    self.event(
                        number,
                        "MEDIA_CONNECTED" if number % 2 else "SIGNALING_CONNECTED",
                    )
                )
        self.assertEqual(GatewayStateChange.objects.count(), 3)
        self.assertEqual(self.update("0:bad")[2]["projection_version"], 7)

    def test_rollback_cannot_leak_projection_or_notification(self):
        start = self.state()
        with self.assertRaises(RuntimeError):
            with transaction.atomic():
                self.receive(self.event())
                raise RuntimeError("rollback")
        self.assertEqual(self.state(), start)
        self.assertFalse(GatewaySessionObservation.objects.exists())
        self.assertIsNone(self.update(start["event_id"]))

    def test_authenticated_http_snapshot_authority_and_flags(self):
        self.assertEqual(self.client.get(self.state_url).status_code, 200)
        self.client.force_authenticate(self.guest.user)
        self.assertEqual(self.client.get(self.state_url).status_code, 403)
        self.intent()
        self.assertEqual(self.client.get(self.state_url).json()["participants"], [])
        Participant.objects.filter(pk=self.guest_participant.pk).update(
            status=ParticipantStatus.REMOVED
        )
        self.assertEqual(self.client.get(self.state_url).status_code, 403)
        self.client.force_authenticate(None)
        self.assertEqual(self.client.get(self.state_url).status_code, 401)
        self.client.force_authenticate(self.host.user)
        with override_settings(GATEWAY_STATE_ENABLED=False):
            self.assertEqual(self.client.get(self.state_url).status_code, 503)
        self.assertEqual(
            self.client.get(
                self.state_url.replace(
                    str(self.session.pk), str(self.legacy_session.pk)
                )
            ).status_code,
            403,
        )

    def test_pagination_is_bounded_and_pinned_to_one_version(self):
        from apps.meetings.services.gateway_admission import request_intent

        for number in range(3):
            profile = self.profile(f"paging{number}")
            request_intent(session_id=self.session.pk, profile_id=profile.pk)
        with patch("apps.meetings.services.gateway_state.PAGE_SIZE", 2):
            first = self.state()
            second = self.state(
                after=first["next_after"], version=first["projection_version"]
            )
        self.assertEqual(len(first["participants"]), 2)
        self.assertEqual(len(second["participants"]), 1)
        self.receive(self.event())
        with self.assertRaises(GatewayDenied) as error:
            self.state(after=first["next_after"], version=first["projection_version"])
        self.assertEqual(error.exception.code, "SNAPSHOT_CHANGED")
        self.assertLess(len(encode_frame(self.update(""))), 128 * 1024)

    def test_legacy_state_entrypoint_uses_same_permission_filter(self):
        from apps.meetings.services.state import MeetingStateBuilder

        self.intent()
        state = MeetingStateBuilder.build(self.session, self.guest)
        self.assertEqual(state["participants"], [])
        self.assertNotIn("janus", state)

    def test_socket_state_emitter_excluded_only_for_enabled_gateway_cohort(self):
        from apps.meetings.realtime.emitter import MeetingSocketEmitter

        with patch(
            "apps.meetings.realtime.emitter.ParticipantConnection.objects.filter"
        ) as query:
            MeetingSocketEmitter.emit_session_state(session=self.session)
            async_to_sync(MeetingSocketEmitter.aemit_session_state)(
                session=self.session
            )
            query.assert_not_called()
            MeetingSocketEmitter.emit_session_state(session=self.legacy_session)
            query.assert_called_once()

    def test_role_visibility_change_advances_version(self):
        self.intent()
        start = self.state()
        self.room.memberships.filter(profile=self.host).update(
            can_manage_waiting_room=False
        )
        state = self.state()
        self.assertGreater(state["projection_version"], start["projection_version"])
        self.assertEqual(state["participants"], [])


@override_settings(GATEWAY_STATE_ENABLED=True)
@skipUnlessDBFeature("has_select_for_update")
class GatewayStateRaceTests(StateFixture, TransactionTestCase):
    race = race_support.GatewayTicketRaceTests.race

    def test_uncommitted_inbox_projection_cannot_be_delivered(self):
        start = self.state()
        event = self.event()
        prepared, release, reading = Event(), Event(), Event()

        def writer():
            close_old_connections()
            try:
                with transaction.atomic():
                    self.receive(event)
                    prepared.set()
                    if not release.wait(5):
                        raise AssertionError("commit barrier timed out")
            finally:
                connections.close_all()

        def reader():
            close_old_connections()
            try:
                reading.set()
                return self.update(start["event_id"])
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=2) as pool:
            pending = pool.submit(writer)
            try:
                self.assertTrue(prepared.wait(5))
                # The independent observer cannot see the uncommitted revision.
                self.assertEqual(
                    GatewayStateProjection.objects.get().version,
                    start["projection_version"],
                )
                output = pool.submit(reader)
                self.assertTrue(reading.wait(5))
                self.assertFalse(output.done())
            finally:
                release.set()
            pending.result(timeout=5)
            update = output.result(timeout=5)
        self.assertEqual(
            update[2]["projection_version"], start["projection_version"] + 1
        )

    def test_stream_http_denies_anonymous_foreign_and_removed_participants(self):
        self.client.force_authenticate(None)
        self.assertEqual(self.client.get(self.stream_url).status_code, 401)
        self.client.force_login(self.guest.user)
        self.assertEqual(self.client.get(self.stream_url).status_code, 403)
        self.intent()
        Participant.objects.filter(pk=self.guest_participant.pk).update(
            status=ParticipantStatus.REMOVED
        )
        self.assertEqual(self.client.get(self.stream_url).status_code, 403)

    def test_stream_limits_release_unstarted_responses(self):
        self.client.force_authenticate(None)
        self.client.force_login(self.host.user)
        responses = [self.client.get(self.stream_url) for _ in range(4)]
        try:
            self.assertEqual([r.status_code for r in responses], [200] * 4)
            self.assertEqual(self.client.get(self.stream_url).status_code, 429)
            self.assertEqual(
                self.client.get(self.stream_url + "?token=not-accepted").status_code,
                400,
            )
            self.assertEqual(
                self.client.get(
                    self.stream_url, HTTP_LAST_EVENT_ID="x" * 81
                ).status_code,
                400,
            )
            self.assertTrue(responses[0].is_async)
        finally:
            for response in responses:
                response.close()
        self.assertEqual(StreamSlots.active, {})

    def test_concurrent_event_and_snapshot_have_one_monotonic_version(self):
        self.state()
        event = self.event(state="MEDIA_CONNECTED")
        self.race(lambda: self.receive(event), lambda: self.receive(event), self.state)
        self.assertEqual(GatewayStateProjection.objects.get().version, 2)
        self.assertEqual(
            list(
                GatewayStateChange.objects.order_by("version").values_list(
                    "version", flat=True
                )
            ),
            [1, 2],
        )

    def test_open_stream_closes_after_revocation_without_old_replay(self):
        self.intent()
        self.decision()

        async def exercise():
            released = []
            stream = state_stream(
                self.session.pk,
                self.guest.pk,
                self.guest.user.pk,
                "",
                time.time() + 5,
                None,
                lambda: released.append(True),
            )
            await anext(stream)
            initial = await anext(stream)
            self.assertIn(b"state.snapshot", initial)
            await sync_to_async(
                Participant.objects.filter(pk=self.guest_participant.pk).update
            )(status=ParticipantStatus.REMOVED)
            with self.assertRaises(StopAsyncIteration):
                await anext(stream)
            self.assertEqual(released, [True])

        async_to_sync(exercise)()

    def test_real_asgi_stream_session_logout_and_disconnect_cleanup(self):
        self.client.force_authenticate(None)
        self.client.force_login(self.host.user)
        session_key = self.client.session.session_key

        async def exercise():
            app = ApplicationCommunicator(
                get_asgi_application(),
                {
                    "type": "http",
                    "asgi": {"version": "3.0"},
                    "http_version": "1.1",
                    "method": "GET",
                    "scheme": "http",
                    "path": self.stream_url,
                    "raw_path": self.stream_url.encode(),
                    "query_string": b"",
                    "headers": [
                        (b"host", b"testserver"),
                        (
                            b"cookie",
                            f"{settings.SESSION_COOKIE_NAME}={session_key}".encode(),
                        ),
                    ],
                    "server": ("testserver", 80),
                    "client": ("127.0.0.1", 1234),
                },
            )
            try:
                await app.send_input(
                    {"type": "http.request", "body": b"", "more_body": False}
                )
                start = await app.receive_output(timeout=5)
                self.assertEqual(start["status"], 200)
                self.assertIn(
                    (b"content-type", b"text/event-stream"),
                    [(name.lower(), value) for name, value in start["headers"]],
                )
                self.assertIn(b"retry:", (await app.receive_output())["body"])
                self.assertIn(b"state.snapshot", (await app.receive_output())["body"])
                await sync_to_async(lambda: self.client.session.delete(session_key))()
                final = await app.receive_output(timeout=5)
                self.assertFalse(final.get("more_body", False))
                await app.wait(timeout=5)
            finally:
                await app.send_input({"type": "http.disconnect"})
                await app.wait(timeout=5)
            self.assertEqual(StreamSlots.active, {})

        async_to_sync(exercise)()

    def test_cancelled_slow_stream_releases_slot(self):
        async def exercise():
            release = StreamSlots.acquire(self.host.user.pk)
            stream = state_stream(
                self.session.pk,
                self.host.pk,
                self.host.user.pk,
                "",
                time.time() + 5,
                None,
                release,
            )
            await anext(stream)
            await anext(stream)
            pending = asyncio.create_task(anext(stream))
            await asyncio.sleep(0)
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            await stream.aclose()
            self.assertEqual(StreamSlots.active, {})

        async_to_sync(exercise)()


@override_settings(GATEWAY_STATE_ENABLED=True)
@skipUnless(os.getenv("GATEWAY_TEST_REDIS_URL"), "real Redis required")
class GatewayStateRedisTests(StateFixture, TransactionTestCase):
    def test_actual_signed_broka_projection_redis_and_lost_delivery_repair(self):
        start = self.state()
        url = os.environ["GATEWAY_TEST_REDIS_URL"]
        body = self.event(state="MEDIA_CONNECTED")

        async def exercise():
            worker = GatewayEventWorker(replace(self.bus, redis_url=url))
            producer = Streams(url, "sg11-producer")
            task = None
            try:
                async with Redis.from_url(url) as client:
                    await client.delete(LIFECYCLE)
                task = asyncio.create_task(worker.run())
                await producer.start()
                await producer.publish(
                    LIFECYCLE,
                    self.owner.sign(body, "lifecycle-event"),
                    body["event_id"],
                )
                async with asyncio.timeout(10):
                    while not worker.counters["recorded"]:
                        await asyncio.sleep(0.02)
                state = await sync_to_async(reconcile)(self.session.pk)
                self.assertTrue(state["presence"])
                await worker.presence.publish(state)
                key = f"synq:gateway:presence:{self.session.pk}"
                async with Redis.from_url(url) as client:
                    cached = json.loads(await client.get(key))
                    self.assertEqual(
                        cached["projection_version"], state["projection_version"]
                    )
                    self.assertTrue(
                        cached["presence"][str(self.participant.pk)]["audio"]
                    )
                    self.assertGreater(await client.ttl(key), 0)
                    await client.delete(key)  # Lost transient fanout/cache.
                update = await sync_to_async(self.update)(
                    start["event_id"], replay=True
                )
                self.assertEqual(update[0], "state.changed")
                self.assertEqual(
                    update[2]["projection_version"], state["projection_version"]
                )
                await worker.presence.publish(state)
                self.assertEqual(
                    await worker.presence.publish({**state, "projection_version": 0}), 0
                )
                self.assertEqual(
                    await worker.presence.publish(
                        {**state, "as_of": state["as_of"] - 1}
                    ),
                    0,
                )
            finally:
                worker.stop.set()
                if task:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await worker.transport.close()
                await worker.presence.close()
                await producer.close()
                async with Redis.from_url(url) as client:
                    await client.delete(
                        LIFECYCLE, f"synq:gateway:presence:{self.session.pk}"
                    )

        async_to_sync(exercise)()

    def test_redis_failure_does_not_block_committed_snapshot(self):
        self.receive(self.event())

        async def exercise():
            projector = PresenceProjector("redis://127.0.0.1:1/15")
            try:
                with self.assertRaises(Exception):
                    await projector.publish(
                        await sync_to_async(reconcile)(self.session.pk)
                    )
                state = await sync_to_async(self.state)()
                self.assertEqual(
                    state["local_participant"]["presence"]["connection"],
                    "SIGNALING_CONNECTED",
                )
            finally:
                await projector.close()

        async_to_sync(exercise)()
