"""Rehearse an isolated room's staged cutover and failback with actual Janus peers."""

import argparse
import asyncio
import json
import os
import sys
import tempfile
from contextlib import AsyncExitStack
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", required=True, type=Path)
    gateway = parser.parse_args().gateway.resolve()
    sys.path[:0] = [
        str(gateway / "src"),
        str(gateway / "tests"),
        str(Path(__file__).resolve().parents[1] / "src"),
    ]
    os.environ.setdefault(
        "DJANGO_SETTINGS_MODULE", "conf.gateway_postgres_test_settings"
    )
    os.environ["SOCKET_IO_REDIS_URL"] = os.environ["GATEWAY_TEST_REDIS_URL"]
    import django

    django.setup()
    import httpx2 as httpx
    from aiortc import (
        AudioStreamTrack,
        RTCConfiguration,
        RTCPeerConnection,
        RTCSessionDescription,
    )
    from asgiref.sync import sync_to_async
    from asgiref.testing import ApplicationCommunicator
    from django.conf import settings
    from django.core.handlers.asgi import ASGIHandler
    from django.db import connections
    from django.test import TransactionTestCase, override_settings
    from django.test.runner import DiscoverRunner
    from jrtc.core.exceptions import JanusErrorResponse
    from jrtc.models.request import KeepAliveRequest
    from jrtc.models.response import ErrorResponse
    from jrtc_video import VideoRoomCreateRequest
    from media_helpers import Browser, publish_payload
    from test_janus_live import JanusServer

    from benchmark_gateway_ice import media_ready
    from check_gateway_state import build_test_gateway
    from apps.meetings.exceptions import JanusGatewayError, MeetingDomainError
    from apps.meetings.models import (
        GatewayAuthorizationOutbox,
        GatewayCommandRoute,
        ParticipantConnection,
    )
    from apps.meetings.realtime.namespace import MeetingNamespace
    from apps.meetings.services.gateway_cutover import drain, set_cohort
    from apps.meetings.services.gateway_dispatcher import GatewayCommandWorker
    from apps.meetings.services.gateway_event_worker import GatewayEventWorker
    from apps.meetings.services.gateway_state import snapshot
    from apps.meetings.services.janus import (
        acall_video_room_management_method as manage,
    )
    from apps.meetings.services.janus import janus_room_id_for_session, janus_runtime
    from apps.meetings.services.lifecycle import MeetingLifecycleService as Lifecycle
    from apps.meetings.tasks import (
        provision_janus_room_for_session,
        attach_participant_media_handles,
    )
    from apps.meetings.tests.test_gateway_ownership import OwnershipFixture

    class Cutover(OwnershipFixture, TransactionTestCase):
        def start(self, room):
            return Lifecycle.start_session(room=room, started_by_profile=self.host)

        @override_settings(
            GATEWAY_STAGED_ROUTING=True,
            GATEWAY_STATE_ENABLED=True,
            GATEWAY_EVENTS_ENABLED=True,
        )
        def test_dark_canary_drain_and_legacy_recreation(self):
            dark = Lifecycle.create_room(
                creator_profile=self.host,
                title="Dark",
                feature_flags=self.room.feature_flags,
            )
            self.dark = self.start(dark)
            self.assertEqual(self.dark.signalling_backend, "legacy")
            self.room = Lifecycle.create_room(
                creator_profile=self.host, title="Staged canary"
            )
            set_cohort(self.room.pk, selected=True)
            self.session = self.start(self.room)
            self.assertEqual(self.session.signalling_backend, "gateway")
            self.participant = self.session.participants.get(profile=self.host)
            self.assertEqual(
                provision_janus_room_for_session.run(str(self.session.pk)),
                {"status": "gateway_owned"},
            )
            self.assertEqual(
                attach_participant_media_handles.run(str(self.participant.pk)),
                {"status": "gateway_owned"},
            )
            self.ticket, self.claims = self.issue()
            self.client.force_login(self.host.user)
            self.cookie = (
                f"{settings.SESSION_COOKIE_NAME}={self.client.session.session_key}"
            )
            with (
                tempfile.TemporaryDirectory() as directory,
                patch(
                    "apps.meetings.services.gateway_ownership.BusConfig.read",
                    return_value=self.bus,
                ),
            ):
                asyncio.run(self.exercise(Path(directory)))

        async def legacy_audio(self, stack, session):
            participant = await sync_to_async(session.participants.get)(
                profile=self.host
            )
            sid = "legacy-" + str(session.pk)
            await sync_to_async(ParticipantConnection.objects.create)(
                session=session,
                profile=self.host,
                participant=participant,
                socket_id=sid,
                status="active",
            )
            await manage(
                session,
                "create",
                VideoRoomCreateRequest(
                    room=janus_room_id_for_session(session),
                    publishers=4,
                    permanent=False,
                ),
            )
            peer = RTCPeerConnection(RTCConfiguration(iceServers=[]))
            stack.push_async_callback(peer.close)
            peer.addTransceiver(AudioStreamTrack(), direction="sendonly")
            await peer.setLocalDescription(await peer.createOffer())
            reply = await MeetingNamespace("/meetings").on_session_media_publish(
                sid,
                {
                    "session_id": str(session.pk),
                    "handle_type": "publisher",
                    "offer": {"type": "offer", "sdp": peer.localDescription.sdp},
                },
            )
            await peer.setRemoteDescription(RTCSessionDescription(**reply["jsep"]))
            await media_ready([peer])
            return peer

        async def replay_terminal(self, old, final):
            path = f"/api/v1/meetings/sessions/{self.session.pk}/gateway/events/"
            stream = ApplicationCommunicator(
                ASGIHandler(),
                {
                    "type": "http",
                    "asgi": {"version": "3.0"},
                    "http_version": "1.1",
                    "method": "GET",
                    "scheme": "https",
                    "path": path,
                    "raw_path": path.encode(),
                    "query_string": b"",
                    "headers": [
                        (b"host", b"testserver"),
                        (b"cookie", self.cookie.encode()),
                        (b"last-event-id", old["event_id"].encode()),
                    ],
                    "server": ("testserver", 443),
                    "client": ("127.0.0.1", 1234),
                },
            )
            await stream.send_input(
                {"type": "http.request", "body": b"", "more_body": False}
            )
            try:
                self.assertEqual(
                    (await stream.receive_output(timeout=5))["status"], 200
                )
                version = old["projection_version"]
                async with asyncio.timeout(10):
                    while version < final["projection_version"]:
                        frame = (await stream.receive_output(timeout=8))["body"]
                        if b"event: state.changed" not in frame:
                            continue
                        item = json.loads(frame.split(b"data: ", 1)[1])
                        self.assertEqual(item["meeting_id"], str(self.session.pk))
                        self.assertEqual(item["context"], "replay")
                        self.assertEqual(item["previous_version"], version)
                        version = item["projection_version"]
                self.assertEqual(version, final["projection_version"])
            finally:
                await stream.send_input({"type": "http.disconnect"})
                await stream.wait(timeout=5)

        async def exercise(self, directory):
            url = os.environ["GATEWAY_TEST_REDIS_URL"]
            server = JanusServer(directory)
            meeting = str(self.session.pk)
            read_state = sync_to_async(snapshot)
            intents = GatewayAuthorizationOutbox.objects.filter(session=self.session)
            async with AsyncExitStack() as stack:
                stack.push_async_callback(sync_to_async(connections.close_all))
                await server.start()
                stack.push_async_callback(server.stop)
                stack.enter_context(
                    override_settings(
                        JANUS_SESSION_URL=f"ws://127.0.0.1:{server.port}/",
                        JANUS_API_SECRET=server.secret,
                        JANUS_STARTUP_FAIL_FAST=True,
                        JRTC_EVENTS_ENABLED=False,
                    )
                )
                await stack.enter_async_context(janus_runtime.lifespan(None))
                dark_peer = await self.legacy_audio(stack, self.dark)
                runtime, app = await build_test_gateway(
                    self, directory, server, url, staged_routing=True
                )
                control = replace(self.bus, redis_url=url)
                event_worker, command_worker = (
                    GatewayEventWorker(control),
                    GatewayCommandWorker(control),
                )
                tasks = []

                async def stop_workers():
                    for worker in (event_worker, command_worker):
                        worker.stop.set()
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

                stack.push_async_callback(stop_workers)
                tasks.append(asyncio.create_task(event_worker.run()))
                async with (
                    app.router.lifespan_context(app),
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app),
                        base_url="https://testserver",
                    ) as http,
                ):
                    async with asyncio.timeout(10):
                        while (await http.get("/health/ready")).status_code != 200:
                            await asyncio.sleep(0.05)
                    browser = Browser(app, self.claims, lambda _: self.ticket["ticket"])
                    stack.push_async_callback(browser.close)
                    self.assertTrue((await browser.open())["ok"])
                    peer = RTCPeerConnection(RTCConfiguration(iceServers=[]))
                    stack.push_async_callback(peer.close)
                    peer.addTransceiver(AudioStreamTrack(), direction="sendonly")
                    await peer.setLocalDescription(await peer.createOffer())
                    payload = publish_payload()
                    payload["offer"]["sdp"] = peer.localDescription.sdp
                    reply = await browser.request("media.publisher.publish", payload)
                    self.assertTrue(reply["ok"], reply.get("error", {}).get("code"))
                    await peer.setRemoteDescription(
                        RTCSessionDescription(**reply["result"]["jsep"])
                    )
                    await media_ready([dark_peer, peer])
                    async with asyncio.timeout(12):
                        while True:
                            old = await read_state(self.session.pk, self.host.pk)
                            if (
                                old["local_participant"]["presence"]["connection"]
                                == "MEDIA_CONNECTED"
                            ):
                                break
                            await asyncio.sleep(0.05)
                    old_room = app.state.auth_endpoint.media.rooms[meeting].id
                    old_janus_session = runtime.session(meeting).id
                    with self.assertRaises(JanusGatewayError):
                        await manage(self.session, "exists", old_room)
                    # The dispatcher is deliberately started after the operator request.
                    await sync_to_async(drain)(self.room.pk, self.session.pk)
                    with self.assertRaisesRegex(MeetingDomainError, "pending"):
                        await sync_to_async(self.start)(self.room)
                    tasks.append(asyncio.create_task(command_worker.run()))
                    async with asyncio.timeout(20):
                        while await sync_to_async(
                            intents.filter(status__in=("pending", "accepted")).exists
                        )():
                            await asyncio.sleep(0.05)
                    self.assertTrue(
                        await sync_to_async(
                            intents.filter(
                                action="meeting.terminate", status="applied"
                            ).exists
                        )()
                    )
                    self.assertFalse(runtime.registry.bindings)
                    self.assertFalse(
                        (await manage(self.dark, "exists", old_room)).data.exists
                    )
                    await asyncio.wait_for(browser.task, 5)
                    with self.assertRaisesRegex(MeetingDomainError, "release"):
                        await sync_to_async(self.start)(self.room)
                    self.assertEqual((await http.get("/health/ready")).status_code, 503)
                self.assertTrue(
                    await sync_to_async(
                        GatewayCommandRoute.objects.filter(
                            session=self.session, released=True
                        ).exists
                    )()
                )
                observer = janus_runtime.adapter.get_session(str(self.dark.pk))
                try:
                    result = await observer.send(
                        KeepAliveRequest(session_id=old_janus_session), timeout=2
                    )
                except JanusErrorResponse as exc:
                    self.assertEqual(exc.code, 458)
                else:
                    self.assertIsInstance(result, ErrorResponse)
                    self.assertEqual(result.error.code, 458)
                new = await sync_to_async(self.start)(self.room)
                self.assertEqual(new.signalling_backend, "legacy")
                self.assertNotEqual(new.pk, self.session.pk)
                replacement_peer = await self.legacy_audio(stack, new)
                await media_ready([dark_peer, replacement_peer])
                self.assertFalse((await manage(new, "exists", old_room)).data.exists)
                final = await read_state(self.session.pk, self.host.pk)
                self.assertEqual(final["meeting"]["lifecycle"], "ended")
                self.assertEqual(
                    final["local_participant"]["presence"]["connection"], "OFFLINE"
                )
                self.assertGreater(
                    final["projection_version"], old["projection_version"]
                )
                await self.replay_terminal(old, final)
                print(
                    "SG17_CANARY_OK legacy and gateway audio, pending commands blocked, signed termination applied, Janus 458, clean release, fresh legacy audio and terminal SSE replay",
                    flush=True,
                )

    Cutover.__module__ = __name__
    globals()["Cutover"] = Cutover
    raise SystemExit(
        bool(DiscoverRunner(verbosity=1).run_tests([__name__ + ".Cutover"]))
    )


if __name__ == "__main__":
    main()
