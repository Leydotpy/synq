"""Real Chromium SDK/React -> HTTPS/SSE + WSS -> Janus/TURN/Broka integration.

Run with PostgreSQL/Redis, Janus/coturn, Node/Playwright and a built frontend
test/browser/gateway.tsx bundle. Test sessions, TLS keys and cookies are ephemeral.
"""

import argparse
import asyncio
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
from unittest.mock import patch

from check_gateway_state import cancellable_django


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", type=Path, required=True)
    parser.add_argument("--frontend", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    sys.path.insert(0, str(args.gateway.resolve() / "tests"))
    os.environ.setdefault(
        "DJANGO_SETTINGS_MODULE", "conf.gateway_postgres_test_settings"
    )
    import django

    django.setup()
    import httpx2 as httpx
    import uvicorn
    from asgiref.sync import async_to_sync
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.x509.oid import NameOID
    from django.conf import settings
    from django.core.handlers.asgi import ASGIHandler
    from django.http import HttpRequest
    from django.middleware.csrf import get_token
    from django.test import TransactionTestCase, override_settings
    from django.test.runner import DiscoverRunner
    from rest_framework.test import APIClient
    from apps.meetings.models import ParticipantConnection, ParticipantMediaHandle
    from apps.meetings.services.gateway_dispatcher import GatewayCommandWorker
    from apps.meetings.services.gateway_event_worker import GatewayEventWorker
    from apps.meetings.tests.test_gateway_ownership import OwnershipFixture
    from jrtc_signalling.app import create_app
    from jrtc_signalling.auth import AuthConfig, HTTPAuthority
    from jrtc_signalling.bus import BusConfig, TrustedKey, Verifier
    from jrtc_signalling.config import Settings
    from jrtc_signalling.janus_runtime import JanusConfig, JanusRuntime
    from live_ice_helpers import TurnServer
    from test_janus_live import JanusServer
    from aioice.ice import get_host_addresses
    from jrtc_video.plugin import parse_videoroom_response
    from pydantic import ValidationError

    def observed_response(payload):
        try:
            return parse_videoroom_response(payload)
        except Exception as error:
            if isinstance(error.__cause__, ValidationError):
                # Diagnose schema mismatches without logging responses, SDP,
                # candidate addresses, credentials or validation input values.
                print(
                    json.dumps(
                        {
                            "janus_validation": [
                                {"loc": item["loc"], "type": item["type"]}
                                for item in error.__cause__.errors(
                                    include_input=False, include_context=False
                                )
                            ]
                        }
                    ),
                    flush=True,
                )
            raise

    @override_settings(
        GATEWAY_STATE_ENABLED=True,
        ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"],
    )
    class BrowserIntegration(OwnershipFixture, TransactionTestCase):
        def test_browser_admission_media_state_and_revocation(self):
            observer = patch(
                "jrtc_video.plugin.parse_videoroom_response",
                new=observed_response,
            )
            observer.start()
            self.addCleanup(observer.stop)
            api_port, gateway_port = free_port(), free_port()
            api = f"https://127.0.0.1:{api_port}"
            gateway = f"wss://127.0.0.1:{gateway_port}/ws"
            identities = {}
            for name, profile in (("host", self.host), ("guest", self.guest)):
                client = APIClient()
                client.force_login(profile.user)
                identities[name] = {
                    "profile": str(profile.pk),
                    "cookie": client.session.session_key,
                }
            request = HttpRequest()
            csrf = get_token(request)

            async def exercise(directory):
                counters = Counter()
                js = args.bundle.read_bytes()
                janus_dir, turn_dir = directory / "janus", directory / "turn"
                janus_dir.mkdir()
                turn_dir.mkdir()
                janus = JanusServer(janus_dir)
                # Match SG-09's real TURN rehearsal: Janus uses host-interface
                # candidates on CI, so relay allocations must share that
                # interface. The TURN listener remains private on loopback.
                try:
                    addresses = get_host_addresses(use_ipv4=True, use_ipv6=False)
                except PermissionError:
                    addresses = []
                turn = TurnServer(turn_dir, (addresses or ["127.0.0.1"])[0])
                await janus.start()
                await turn.start()
                bus = BusConfig(
                    os.environ["GATEWAY_TEST_REDIS_URL"],
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
                        f"ws://127.0.0.1:{janus.port}/",
                        janus.secret,
                        "https://testserver/api/v1/meetings/internal/gateway/owners/",
                        (str(self.session.pk),),
                    ),
                )
                for authority in (runtime.authority, runtime.control.authority):
                    await authority.client.aclose()
                    authority.client = httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=cancellable_django)
                    )
                auth = AuthConfig(
                    "https://synq.example",
                    {"test-key": self.key.public_key()},
                    frozenset({api}),
                    "https://testserver/api/v1/meetings/internal/gateway/authority/",
                    bus.authority_token,
                )
                gateway_app = create_app(
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
                workers = [
                    GatewayEventWorker(replace(self.bus, redis_url=bus.redis_url)),
                    GatewayCommandWorker(replace(self.bus, redis_url=bus.redis_url)),
                ]
                worker_tasks = [asyncio.create_task(worker.run()) for worker in workers]

                async def application(scope, receive, send):
                    path = scope.get("path", "")
                    if path in {"/", "/sdk.js"}:
                        body = (
                            js
                            if path == "/sdk.js"
                            else b'<!doctype html><html><body><div id="root"></div><script src="/sdk.js"></script></body></html>'
                        )
                        await send(
                            {
                                "type": "http.response.start",
                                "status": 200,
                                "headers": [
                                    (
                                        b"content-type",
                                        b"text/javascript"
                                        if path == "/sdk.js"
                                        else b"text/html",
                                    )
                                ],
                            }
                        )
                        await send({"type": "http.response.body", "body": body})
                        return
                    if "/socket.io" in path:
                        raise AssertionError(
                            "Gateway SDK unexpectedly opened Socket.IO"
                        )
                    if path.endswith("/tickets/"):
                        counters["tickets"] += 1
                    if not path.endswith("/events/"):
                        await ASGIHandler()(scope, receive, send)
                        return
                    counters["sse_requests"] += 1
                    if dict(scope["headers"]).get(b"last-event-id"):
                        counters["sse_cursors"] += 1
                    cut = counters["sse_requests"] == 1
                    ended = asyncio.Event()

                    async def stream_receive():
                        incoming, cancelled = (
                            asyncio.create_task(receive()),
                            asyncio.create_task(ended.wait()),
                        )
                        try:
                            done, _ = await asyncio.wait(
                                (incoming, cancelled),
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            return (
                                {"type": "http.disconnect"}
                                if cancelled in done
                                else incoming.result()
                            )
                        finally:
                            incoming.cancel()
                            cancelled.cancel()
                            await asyncio.gather(
                                incoming, cancelled, return_exceptions=True
                            )

                    async def stream_send(message):
                        if ended.is_set():
                            return
                        if (
                            cut
                            and message["type"] == "http.response.body"
                            and b"state.changed" in message.get("body", b"")
                        ):
                            counters["sse_forced_drop"] += 1
                            await send({**message, "more_body": False})
                            ended.set()
                        else:
                            await send(message)

                    await ASGIHandler()(scope, stream_receive, stream_send)

                subject = x509.Name(
                    [x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]
                )
                cert = (
                    x509.CertificateBuilder()
                    .subject_name(subject)
                    .issuer_name(subject)
                    .public_key(self.key.public_key())
                    .serial_number(x509.random_serial_number())
                    .not_valid_before(datetime.now(timezone.utc) - timedelta(minutes=1))
                    .not_valid_after(datetime.now(timezone.utc) + timedelta(hours=1))
                    .add_extension(
                        x509.SubjectAlternativeName(
                            [
                                x509.DNSName("localhost"),
                                x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                            ]
                        ),
                        critical=False,
                    )
                    .sign(self.key, hashes.SHA256())
                )
                cert_path = directory / "test-cert.pem"
                cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
                servers = [
                    uvicorn.Server(
                        uvicorn.Config(
                            app,
                            host="127.0.0.1",
                            port=port,
                            ssl_certfile=str(cert_path),
                            ssl_keyfile=str(self.key_path),
                            lifespan=lifespan,
                            access_log=False,
                            log_level="error",
                        )
                    )
                    for app, port, lifespan in (
                        (application, api_port, "off"),
                        (gateway_app, gateway_port, "on"),
                    )
                ]
                server_tasks = [
                    asyncio.create_task(server.serve()) for server in servers
                ]
                try:
                    async with asyncio.timeout(20):
                        while not all(server.started for server in servers):
                            for task in server_tasks:
                                if task.done():
                                    task.result()
                                    raise AssertionError(
                                        "ASGI server stopped during startup"
                                    )
                            await asyncio.sleep(0.02)
                    config = {
                        "api": api,
                        "gateway": gateway,
                        "meeting": str(self.session.pk),
                        "cookieName": settings.SESSION_COOKIE_NAME,
                        "csrf": csrf,
                        "csrfCookie": request.META["CSRF_COOKIE"],
                        **identities,
                        "iceServers": [
                            {
                                "urls": f"turn:127.0.0.1:{turn.port}?transport=udp",
                                "username": turn.username,
                                "credential": turn.password,
                            }
                        ],
                    }
                    config_path = directory / "browser-config.json"
                    config_path.write_text(json.dumps(config))
                    config_path.chmod(0o600)
                    process = await asyncio.create_subprocess_exec(
                        "node",
                        str(
                            args.frontend.resolve()
                            / "packages/client/test/browser/run.mjs"
                        ),
                        str(config_path),
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    try:
                        output, error = await asyncio.wait_for(
                            process.communicate(), 100
                        )
                    except BaseException:
                        process.kill()
                        await process.wait()
                        raise
                    print(output.decode(), flush=True)
                    if process.returncode:
                        print(error.decode()[-6000:], flush=True)
                    self.assertEqual(process.returncode, 0, "Browser acceptance failed")
                    self.assertGreaterEqual(counters["tickets"], 5)
                    self.assertEqual(counters["sse_forced_drop"], 1)
                    self.assertGreaterEqual(counters["sse_cursors"], 3)
                    print(
                        "HTTPS ticket refresh, SSE drop/resume and broker delivery:",
                        dict(counters),
                        flush=True,
                    )
                finally:
                    for server in servers:
                        server.should_exit = True
                    await asyncio.wait_for(
                        asyncio.gather(*server_tasks, return_exceptions=True), 40
                    )
                    for worker in workers:
                        worker.stop.set()
                    for task in worker_tasks:
                        task.cancel()
                    await asyncio.gather(*worker_tasks, return_exceptions=True)
                    await turn.close()
                    await janus.stop()

            with (
                tempfile.TemporaryDirectory() as directory,
                patch(
                    "apps.meetings.services.gateway_ownership.BusConfig.read",
                    return_value=self.bus,
                ),
                override_settings(
                    GATEWAY_TICKET_TTL_SECONDS=24, CSRF_TRUSTED_ORIGINS=[api]
                ),
            ):
                async_to_sync(exercise)(Path(directory))
            self.assertFalse(
                ParticipantConnection.objects.filter(session=self.session).exists()
            )
            self.assertFalse(
                ParticipantMediaHandle.objects.filter(
                    participant__session=self.session
                ).exists()
            )

    BrowserIntegration.__module__ = __name__
    globals()["BrowserIntegration"] = BrowserIntegration
    raise SystemExit(
        bool(DiscoverRunner(verbosity=1).run_tests([__name__ + ".BrowserIntegration"]))
    )


if __name__ == "__main__":
    main()
