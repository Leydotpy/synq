"""SG-11 actual Janus -> Broka -> Django projection -> authenticated ASGI SSE.

Run in Synq's Python 3.14 environment with this gateway, pytest and aiortc installed.
Requires GATEWAY_TEST_REDIS_URL, Janus and conf.gateway_postgres_test_settings for
PostgreSQL acceptance. Test keys and media stay in memory or temporary directories.
"""

import argparse
import asyncio
import os
import sys
import tempfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch


async def cancellable_django(scope, receive, send):
    """Model an actual disconnect when an in-process HTTP client is cancelled.

    httpx's ASGI transport cancels the application task rather than sending the
    disconnect an ASGI server would send. Let Django close its request children.
    The SSE probe below uses its own explicit ASGI disconnect unchanged.
    """
    from django.core.handlers.asgi import ASGIHandler

    disconnected = asyncio.Event()

    async def read():
        incoming = asyncio.create_task(receive())
        cancelled = asyncio.create_task(disconnected.wait())
        try:
            done, _ = await asyncio.wait(
                (incoming, cancelled), return_when=asyncio.FIRST_COMPLETED
            )
            return (
                {"type": "http.disconnect"} if cancelled in done else incoming.result()
            )
        finally:
            for child in (incoming, cancelled):
                if not child.done():
                    child.cancel()
            await asyncio.gather(incoming, cancelled, return_exceptions=True)

    task = asyncio.create_task(ASGIHandler()(scope, read, send))
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        disconnected.set()
        await asyncio.wait_for(task, 5)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    sys.path.insert(0, str(args.gateway.resolve() / "tests"))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "conf.test_settings")
    import django

    django.setup()
    import httpx2 as httpx
    from aiortc import (
        AudioStreamTrack,
        RTCConfiguration,
        RTCPeerConnection,
        RTCSessionDescription,
    )
    from apps.meetings.models import GatewaySessionObservation, Participant
    from apps.meetings.services.gateway_event_worker import GatewayEventWorker
    from apps.meetings.tests.test_gateway_ownership import OwnershipFixture
    from asgiref.sync import async_to_sync, sync_to_async
    from django.core.handlers.asgi import ASGIHandler
    from django.test import TransactionTestCase, override_settings
    from asgiref.testing import ApplicationCommunicator
    from django.conf import settings
    from apps.meetings.services.gateway_state import next_update
    from rest_framework.test import APIClient
    from django.test.runner import DiscoverRunner
    from media_helpers import Browser, publish_payload
    from redis.asyncio import Redis
    from test_janus_live import JanusServer

    from jrtc_signalling.app import create_app
    from jrtc_signalling.auth import AuthConfig, HTTPAuthority
    from jrtc_signalling.bus import LIFECYCLE, BusConfig, TrustedKey, Verifier
    from jrtc_signalling.config import Settings
    from jrtc_signalling.janus_runtime import JanusConfig, JanusRuntime

    @override_settings(GATEWAY_STATE_ENABLED=True)
    class StateIntegration(OwnershipFixture, TransactionTestCase):
        def test_real_media_lobby_projection_sse_and_broker_stall(self):
            from aioice.ice import get_host_addresses

            try:
                get_host_addresses(use_ipv4=True, use_ipv6=False)
            except PermissionError:
                # Restricted local runners cannot enumerate interfaces; actual
                # media still crosses UDP to Janus on the available loopback.
                interfaces = patch(
                    "aioice.ice.get_host_addresses", return_value=["127.0.0.1"]
                )
                interfaces.start()
                self.addCleanup(interfaces.stop)
            issued, claims = self.issue()
            self.waiting()
            lobby_ticket, lobby_claims = self.issue(self.guest)
            before = Participant.objects.values().get(pk=self.participant.pk)
            self.client.force_authenticate(None)
            self.client.force_login(self.host.user)
            host_cookie = (
                f"{settings.SESSION_COOKIE_NAME}={self.client.session.session_key}"
            )
            guest_client = APIClient()
            guest_client.force_login(self.guest.user)
            guest_cookie = (
                f"{settings.SESSION_COOKIE_NAME}={guest_client.session.session_key}"
            )
            state_path = f"/api/v1/meetings/sessions/{self.session.pk}/gateway/state/"
            events_path = f"/api/v1/meetings/sessions/{self.session.pk}/gateway/events/"

            async def exercise(directory):
                url = os.environ["GATEWAY_TEST_REDIS_URL"]
                server = JanusServer(directory)
                await server.start()
                bus = BusConfig(
                    url,
                    self.owner,
                    Verifier(
                        {
                            self.bus.signer.key_id: TrustedKey(
                                "django-control", self.bus.signer.key.public_key()
                            )
                        }
                    ),
                    str(directory / "inbox.sqlite3"),
                    "https://testserver/api/v1/meetings/internal/gateway/commands/authorize/",
                    "test-service-" + "x" * 32,
                )
                runtime = JanusRuntime(
                    bus,
                    JanusConfig(
                        f"ws://127.0.0.1:{server.port}/",
                        server.secret,
                        "https://testserver/api/v1/meetings/internal/gateway/owners/",
                        (str(self.session.pk),),
                    ),
                )
                await runtime.authority.client.aclose()
                runtime.authority.client = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=cancellable_django)
                )
                await runtime.control.authority.client.aclose()
                runtime.control.authority.client = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=cancellable_django)
                )
                auth = AuthConfig(
                    "https://synq.example",
                    {"test-key": self.key.public_key()},
                    frozenset({"https://meet.example"}),
                    "https://testserver/api/v1/meetings/internal/gateway/authority/",
                    bus.authority_token,
                )
                app = create_app(
                    Settings(
                        media_rehearsal=True, lifecycle_events=True, shutdown_timeout=30
                    ),
                    runtime_factory=lambda: runtime,
                    auth_config=auth,
                    authority_factory=lambda config: HTTPAuthority(
                        config,
                        httpx.AsyncClient(
                            transport=httpx.ASGITransport(app=cancellable_django)
                        ),
                    ),
                )
                worker = GatewayEventWorker(replace(self.bus, redis_url=url))
                task = asyncio.create_task(worker.run())
                peer = RTCPeerConnection(RTCConfiguration(iceServers=[]))
                http = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=cancellable_django),
                    base_url="http://testserver",
                )
                sse = ApplicationCommunicator(
                    ASGIHandler(),
                    {
                        "type": "http",
                        "asgi": {"version": "3.0"},
                        "http_version": "1.1",
                        "method": "GET",
                        "scheme": "http",
                        "path": events_path,
                        "raw_path": events_path.encode(),
                        "query_string": b"",
                        "headers": [
                            (b"host", b"testserver"),
                            (b"cookie", host_cookie.encode()),
                        ],
                        "server": ("testserver", 80),
                        "client": ("127.0.0.1", 1234),
                    },
                )
                await sse.send_input(
                    {"type": "http.request", "body": b"", "more_body": False}
                )
                self.assertEqual((await sse.receive_output(timeout=5))["status"], 200)
                self.assertIn(b"retry:", (await sse.receive_output())["body"])
                initial = await sse.receive_output()
                self.assertIn(b"state.snapshot", initial["body"])
                import json

                initial_state = json.loads(initial["body"].split(b"data: ", 1)[1])
                self.assertEqual(
                    initial_state["local_participant"]["presence"]["connection"],
                    "OFFLINE",
                )

                async def public_state(cookie):
                    response = await http.get(state_path, headers={"Cookie": cookie})
                    self.assertEqual(response.status_code, 200)
                    return response.json()

                async def observed(participant, state):
                    async with asyncio.timeout(12):
                        while True:
                            row = await sync_to_async(
                                lambda: (
                                    GatewaySessionObservation.objects.filter(
                                        participant_id=participant
                                    )
                                    .values_list("envelope", flat=True)
                                    .first()
                                )
                            )()
                            if row and row["snapshot"]["connection"] == state:
                                return row
                            await asyncio.sleep(0.02)

                try:
                    async with app.router.lifespan_context(app):
                        browser = Browser(app, claims, lambda _: issued["ticket"])
                        lobby = Browser(
                            app, lobby_claims, lambda _: lobby_ticket["ticket"]
                        )
                        try:
                            self.assertTrue((await lobby.open())["ok"])
                            await observed(
                                lobby_claims["participant_id"], "LOBBY_CONNECTED"
                            )
                            waiting_state = await public_state(guest_cookie)
                            self.assertEqual(waiting_state["participants"], [])
                            self.assertEqual(
                                waiting_state["local_participant"]["presence"][
                                    "connection"
                                ],
                                "LOBBY_CONNECTED",
                            )
                            self.assertFalse(runtime.registry.bindings)
                            self.assertTrue((await browser.open())["ok"])
                            await observed(
                                claims["participant_id"], "SIGNALING_CONNECTED"
                            )
                            peer.addTransceiver(
                                AudioStreamTrack(), direction="sendonly"
                            )
                            await peer.setLocalDescription(await peer.createOffer())
                            payload = publish_payload()
                            payload["offer"]["sdp"] = peer.localDescription.sdp
                            response = await browser.request(
                                "media.publisher.publish", payload
                            )
                            self.assertTrue(response["ok"])
                            await peer.setRemoteDescription(
                                RTCSessionDescription(**response["result"]["jsep"])
                            )
                            connected = await observed(
                                claims["participant_id"], "MEDIA_CONNECTED"
                            )
                            self.assertTrue(connected["snapshot"]["audio"])
                            public_connected = await public_state(host_cookie)
                            self.assertEqual(
                                public_connected["local_participant"]["presence"][
                                    "connection"
                                ],
                                "MEDIA_CONNECTED",
                            )
                            self.assertTrue(
                                public_connected["local_participant"]["presence"][
                                    "audio"
                                ]
                            )
                            async with asyncio.timeout(5):
                                while True:
                                    frame = (await sse.receive_output(timeout=5))[
                                        "body"
                                    ]
                                    if b"state.changed" not in frame:
                                        continue
                                    notification = json.loads(
                                        frame.split(b"data: ", 1)[1]
                                    )
                                    if (
                                        notification["projection_version"]
                                        == public_connected["projection_version"]
                                    ):
                                        break
                            self.assertGreater(
                                notification["projection_version"],
                                initial_state["projection_version"],
                            )
                            # Reconnect uses the durable log; no transient delivery is required.
                            replay = await sync_to_async(next_update)(
                                self.session.pk,
                                self.host.pk,
                                initial_state["event_id"],
                                replay=True,
                            )
                            self.assertEqual(replay[2]["context"], "replay")
                            # An actual broker outage cannot block Janus transactions
                            # or terminal persistence. Recovery delivers the snapshot.
                            async with Redis.from_url(url) as redis:
                                await redis.execute_command(
                                    "CLIENT", "PAUSE", 3000, "ALL"
                                )
                            await asyncio.wait_for(
                                runtime.management(
                                    str(self.session.pk), 1, "list_rooms"
                                ),
                                1,
                            )
                            await asyncio.wait_for(browser.close(), 1)
                            offline = await observed(
                                claims["participant_id"], "OFFLINE"
                            )
                            self.assertGreater(
                                offline["sequence"], connected["sequence"]
                            )
                            public_offline = await public_state(host_cookie)
                            self.assertEqual(
                                public_offline["local_participant"]["presence"][
                                    "connection"
                                ],
                                "OFFLINE",
                            )
                            self.assertGreater(
                                public_offline["projection_version"],
                                public_connected["projection_version"],
                            )
                            self.assertGreater(runtime.lifecycle.counters["retry"], 0)
                            await lobby.close()
                            await observed(lobby_claims["participant_id"], "OFFLINE")
                        finally:
                            await browser.close()
                            await lobby.close()
                finally:
                    await sse.send_input({"type": "http.disconnect"})
                    await sse.wait(timeout=5)
                    await http.aclose()
                    await peer.close()
                    worker.stop.set()
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    await server.stop()
                    async with Redis.from_url(url) as redis:
                        await redis.delete(LIFECYCLE)

            with (
                tempfile.TemporaryDirectory() as directory,
                patch(
                    "apps.meetings.services.gateway_ownership.BusConfig.read",
                    return_value=self.bus,
                ),
            ):
                async_to_sync(exercise)(Path(directory))
            after = Participant.objects.values().get(pk=self.participant.pk)
            for key in (
                "status",
                "role",
                "authorization_epoch",
                "joined_at",
                "can_publish_audio",
                "can_publish_video",
                "can_share_screen",
            ):
                self.assertEqual(before[key], after[key])
            guest = Participant.objects.get(pk=lobby_claims["participant_id"])
            self.assertEqual(guest.status, "waiting")
            self.assertIsNone(guest.joined_at)

    StateIntegration.__module__ = __name__
    globals()["StateIntegration"] = StateIntegration
    raise SystemExit(
        bool(DiscoverRunner(verbosity=1).run_tests([__name__ + ".StateIntegration"]))
    )


if __name__ == "__main__":
    main()
