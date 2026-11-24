"""Exercise two actual gateway processes against TLS Django, PostgreSQL, Redis and Janus."""

import argparse
import asyncio
import json
import os
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import uuid4


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


from check_gateway_state import use_available_interfaces


def main():
    parser = argparse.ArgumentParser()
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
    import django

    django.setup()
    import httpx2 as httpx
    import uvicorn
    from aiortc import (
        AudioStreamTrack,
        RTCConfiguration,
        RTCPeerConnection,
        RTCSessionDescription,
    )
    from apps.meetings.services.lifecycle import MeetingLifecycleService
    from asgiref.sync import sync_to_async
    from cryptography.hazmat.primitives import serialization
    from django.conf import settings
    from django.core.handlers.asgi import ASGIHandler
    from django.db import connections
    from django.test import TransactionTestCase, override_settings
    from django.test.runner import DiscoverRunner
    from django.utils import timezone
    from jrtc_signalling.bus import EVENTS, command_route
    from jrtc_signalling.bus_transport import Streams
    from jrtc_signalling.command_store import outcome
    from media_helpers import Browser, publish_payload
    from redis.asyncio import Redis
    from test_janus_live import JanusServer
    from websockets.asyncio.client import connect

    from apps.meetings.models import (
        GatewayAuthorizationOutbox,
        GatewayCommandRoute,
        GatewayOwnerGeneration,
        GatewaySessionObservation,
    )
    from apps.meetings.services.gateway_commands import (
        authorize_command,
        prepare_delivery,
        receive_outcome,
    )
    from apps.meetings.services.gateway_dispatcher import GatewayCommandWorker
    from apps.meetings.services.gateway_event_worker import GatewayEventWorker
    from apps.meetings.services.gateway_events import receive_event
    from apps.meetings.tests.test_gateway_ownership import OwnershipFixture

    class Integration(OwnershipFixture, TransactionTestCase):
        @override_settings(
            GATEWAY_MULTIWORKER_ENABLED=True, ALLOWED_HOSTS=["127.0.0.1"]
        )
        def test_rolling_drain_expired_live_owner_and_broker_loss(self):
            use_available_interfaces(self)
            self.intent()
            self.decision()
            outbox = GatewayAuthorizationOutbox.objects.get(
                session=self.session, action="participant.admit"
            )
            with tempfile.TemporaryDirectory() as name:
                directory = Path(name)
                subprocess.run(
                    [
                        "openssl",
                        "req",
                        "-x509",
                        "-newkey",
                        "rsa:2048",
                        "-nodes",
                        "-subj",
                        "/CN=localhost",
                        "-addext",
                        "subjectAltName=IP:127.0.0.1",
                        "-keyout",
                        str(directory / "tls.key"),
                        "-out",
                        str(directory / "tls.pem"),
                        "-days",
                        "1",
                    ],
                    check=True,
                    capture_output=True,
                )
                asyncio.run(self.exercise(directory, outbox.pk))

        async def exercise(self, directory, outbox_id):
            def write(name, data):
                path = directory / name
                path.write_bytes(data if isinstance(data, bytes) else data.encode())
                return str(path)

            def document(name, value):
                return write(name, json.dumps(value))

            def public(key):
                return key.public_key().public_bytes(
                    serialization.Encoding.PEM,
                    serialization.PublicFormat.SubjectPublicKeyInfo,
                )

            def private(key):
                return key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                )

            meeting = str(self.session.pk)
            host = "127.0.0.1"
            redis_url = os.environ["GATEWAY_TEST_REDIS_URL"]
            owners = {s.producer: s for s in (self.owner, self.other)}
            ports = {owner: free_port() for owner in owners}
            targets = {owner: f"wss://{host}:{p}/ws" for owner, p in ports.items()}
            dport = free_port()
            authority = f"https://{host}:{dport}/api/v1/meetings/internal/gateway"
            redis_file = write("redis", redis_url)
            token_file = write("token", settings.GATEWAY_AUTHORITY_TOKEN)
            tls = str(directory / "tls.pem")
            tls_key = str(directory / "tls.key")
            context = ssl.create_default_context(cafile=tls)
            trusted = {
                s.key_id: {
                    "producer_id": s.producer,
                    "public_key_file": write(s.key_id + ".pub", public(s.key)),
                }
                for s in (*owners.values(), self.bus.signer)
            }

            def bus(signer, peers, **extra):
                return document(
                    signer.producer + ".json",
                    dict(
                        redis_url_file=redis_file,
                        producer_id=signer.producer,
                        signing_key_id=signer.key_id,
                        private_key_file=write(
                            signer.producer + ".key", private(signer.key)
                        ),
                        trusted_keys={s.key_id: trusted[s.key_id] for s in peers},
                        **extra,
                    ),
                )

            django_bus = bus(self.bus.signer, owners.values())
            config = document(
                "auth.json",
                {
                    "issuer": settings.GATEWAY_TICKET_ISSUER,
                    "public_keys": {"test-key": public(self.key).decode()},
                    "origins": ["https://meet.example"],
                    "authority_url": authority + "/authority/",
                    "authority_token_file": token_file,
                },
            )
            janus = JanusServer(directory)
            await janus.start()
            runtime_config = document(
                "runtime.json",
                {
                    "janus_url": f"ws://{host}:{janus.port}/",
                    "api_secret_file": write("janus-api", janus.secret),
                    "meetings": [meeting],
                    "ownership_url": authority + "/owners/",
                    "workers": targets,
                },
            )
            metrics_key = uuid4().hex
            env = {
                **os.environ,
                "SSL_CERT_FILE": tls,
                "NO_PROXY": host,
                "PYTHONPATH": str(gateway / "src"),
                "JRTC_SIGNALLING_AUTH_CONFIG_FILE": config,
                "JRTC_SIGNALLING_RUNTIME_CONFIG_FILE": runtime_config,
                "JRTC_SIGNALLING_METRICS_TOKEN_FILE": write("scrape-key", metrics_key),
                "JRTC_SIGNALLING_MEDIA_REHEARSAL": "true",
                "JRTC_SIGNALLING_LIFECYCLE_EVENTS": "true",
                "JRTC_SIGNALLING_SHUTDOWN_TIMEOUT": "30",
                "JRTC_SIGNALLING_TLS_CERT_FILE": tls,
                "JRTC_SIGNALLING_TLS_KEY_FILE": tls_key,
            }
            env.pop("DJANGO_SETTINGS_MODULE", None)
            workers = {}
            sockets = []
            peers = []
            authority_server = uvicorn.Server(
                uvicorn.Config(
                    ASGIHandler(),
                    host=host,
                    port=dport,
                    lifespan="off",
                    ssl_keyfile=tls_key,
                    ssl_certfile=tls,
                    access_log=False,
                    log_level="error",
                )
            )
            control_bus = replace(self.bus, redis_url=redis_url)
            projection = GatewayEventWorker(control_bus)
            commands = GatewayCommandWorker(control_bus)
            tasks = []
            redis = Redis.from_url(redis_url, decode_responses=True)
            streams = Streams(redis_url, "sg15-verification")
            key = "synq:gateway:owner:v1:" + meeting

            async def launch(owner):
                signer = owners[owner]
                bus_file = bus(
                    signer,
                    [self.bus.signer],
                    inbox_path=str(directory / (owner + ".sqlite3")),
                    authority_url=authority + "/commands/authorize/",
                    authority_token_file=token_file,
                )
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "jrtc_signalling",
                    env={
                        **env,
                        "JRTC_SIGNALLING_PORT": str(ports[owner]),
                        "JRTC_SIGNALLING_BUS_CONFIG_FILE": bus_file,
                    },
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                workers[owner] = process

            async def route(generation):
                async with asyncio.timeout(55):
                    while True:
                        raw = await redis.get(key)
                        if (
                            raw
                            and len(raw.split("/")) == 3
                            and raw.endswith("/" + str(generation))
                        ):
                            row = await GatewayCommandRoute.objects.aget(
                                session_id=meeting
                            )
                            if (
                                not row.released
                                and row.owner_id == raw.split("/")[0]
                                and row.owner_generation == generation
                            ):
                                return row
                        await asyncio.sleep(0.1)

            async def stop(owner):
                process = workers.pop(owner)
                process.send_signal(signal.SIGCONT)
                process.terminate()
                await asyncio.wait_for(process.wait(), 35)
                self.assertEqual(process.returncode, -signal.SIGTERM)

            async def request(ws, wire, op, payload=None):
                frame = wire.message(op, payload)
                await ws.send(json.dumps(frame))
                async with asyncio.timeout(10):
                    while True:
                        reply = json.loads(await ws.recv())
                        if reply.get("id") == frame["id"]:
                            self.assertTrue(reply["ok"], reply.get("error"))
                            return reply["result"]

            async def browser(owner, prev=None):
                fields = (
                    {
                        "purpose": "resume",
                        "participant_session_id": prev["participant_session_id"],
                        "generation": prev["generation"],
                    }
                    if prev
                    else {}
                )
                ticket, claims = await sync_to_async(self.issue)(**fields)
                wire = Browser(None, claims, None)
                ws = await connect(
                    targets[owner],
                    origin="https://meet.example",
                    subprotocols=["jrtc.signalling.v1"],
                    ssl=context,
                    proxy=None,
                    close_timeout=1,
                )
                sockets.append(ws)
                payload = {"ticket": ticket["ticket"]}
                if prev:
                    payload["last_sequence"] = 0
                reply = await request(
                    ws, wire, "session.resume" if prev else "session.hello", payload
                )
                await request(ws, wire, "media.publisher.join")
                peer = RTCPeerConnection(RTCConfiguration(iceServers=[]))
                peers.append(peer)
                peer.addTransceiver(AudioStreamTrack(), direction="sendonly")
                await peer.setLocalDescription(await peer.createOffer())
                payload = publish_payload()
                payload["offer"]["sdp"] = peer.localDescription.sdp
                published = await request(ws, wire, "media.publisher.publish", payload)
                await peer.setRemoteDescription(
                    RTCSessionDescription(**published["jsep"])
                )
                return ws, claims, reply, peer

            async def observed(generation):
                async with asyncio.timeout(12):
                    while True:
                        row = await GatewaySessionObservation.objects.filter(
                            participant=self.participant
                        ).afirst()
                        if (
                            row
                            and row.envelope["gateway_generation"] == generation
                            and row.envelope["snapshot"]["connection"]
                            == "MEDIA_CONNECTED"
                        ):
                            return row.envelope
                        await asyncio.sleep(0.1)

            async def janus_request(session_id):
                async with connect(
                    f"ws://{host}:{janus.port}/",
                    subprotocols=["janus-protocol"],
                    proxy=None,
                ) as ws:
                    await ws.send(
                        json.dumps(
                            {
                                "janus": "keepalive",
                                "session_id": session_id,
                                "apisecret": janus.secret,
                                "transaction": str(uuid4()),
                            }
                        )
                    )
                    return json.loads(await asyncio.wait_for(ws.recv(), 3))

            with override_settings(GATEWAY_BUS_CONFIG_FILE=django_bus):
                try:
                    tasks.extend(
                        [
                            asyncio.create_task(authority_server.serve()),
                            asyncio.create_task(projection.run()),
                        ]
                    )
                    async with asyncio.timeout(5):
                        while not authority_server.started:
                            await asyncio.sleep(0.02)
                    await streams.start()
                    await commands.transport.start()
                    await commands.transport.subscribe(EVENTS, commands.handle)
                    await asyncio.gather(*(launch(owner) for owner in owners))
                    one = await route(1)
                    owner = one.owner_id
                    standby = next(key for key in owners if key != owner)
                    # Both listeners must accept before forcing entry through the losing worker.
                    async with asyncio.timeout(15):
                        for port in ports.values():
                            while True:
                                try:
                                    _, writer = await asyncio.open_connection(
                                        host, port, ssl=context
                                    )
                                except OSError:
                                    await asyncio.sleep(0.05)
                                else:
                                    writer.close()
                                    await writer.wait_closed()
                                    break
                    first, claims, ack, _ = await browser(standby)
                    self.assertEqual(
                        (ack["gateway_owner_id"], ack["gateway_generation"]), (owner, 1)
                    )
                    print("Verified routed WSS admission at generation 1", flush=True)
                    self.assertEqual(
                        (await janus_request(one.janus_session_id))["janus"], "ack"
                    )
                    old_observation = await observed(1)
                    footprint = {}
                    async with httpx.AsyncClient(
                        verify=context, trust_env=False
                    ) as telemetry:
                        for role, worker in (("owner", owner), ("standby", standby)):
                            response = await telemetry.get(
                                f"https://{host}:{ports[worker]}/metrics",
                                headers={"Authorization": "Bearer " + metrics_key},
                            )
                            response.raise_for_status()
                            footprint[role] = {
                                name: int(value)
                                for name, value in (
                                    line.split()
                                    for line in response.text.splitlines()
                                    if line.startswith(
                                        (
                                            "jrtc_janus_sessions ",
                                            "jrtc_janus_handles ",
                                            "jrtc_owned_meetings ",
                                        )
                                    )
                                )
                            }
                    self.assertEqual(
                        [
                            footprint[role]["jrtc_janus_sessions"]
                            for role in ("owner", "standby")
                        ],
                        [1, 1],
                    )
                    self.assertEqual(
                        [
                            footprint[role]["jrtc_owned_meetings"]
                            for role in ("owner", "standby")
                        ],
                        [1, 0],
                    )
                    self.assertGreaterEqual(footprint["owner"]["jrtc_janus_handles"], 1)
                    print(
                        "SG16_POOL=" + json.dumps(footprint, sort_keys=True), flush=True
                    )
                    command = (await sync_to_async(prepare_delivery)(outbox_id))[
                        "envelope"
                    ]
                    await stop(owner)
                    await asyncio.wait_for(first.wait_closed(), 8)
                    self.assertEqual(first.close_code, 1012)
                    two = await route(2)
                    self.assertEqual(two.owner_id, standby)
                    self.assertEqual(
                        (await janus_request(one.janus_session_id))["error"]["code"],
                        458,
                    )
                    second, claims, ack, _ = await browser(standby, claims)
                    self.assertEqual(ack["gateway_generation"], 2)
                    await observed(2)
                    print(
                        "Verified SIGTERM handoff and resume at generation 2",
                        flush=True,
                    )
                    # Replay the original signed command on the wire and submit its delayed signed ACK.
                    old_packet = self.bus.signer.sign(command, "command")
                    await streams.publish(
                        command_route(owner, str(one.owner_incarnation)),
                        old_packet,
                        command["command_id"],
                    )
                    self.assertFalse(await sync_to_async(authorize_command)(command))
                    event = outcome(
                        command, "applied", "normal", timezone.now().timestamp()
                    )
                    signer = owners[owner]
                    producer, body = self.bus.verifier.verify(
                        signer.sign(event, "command-outcome"), "command-outcome"
                    )
                    self.assertEqual(
                        await sync_to_async(receive_outcome)(producer, body), "stale"
                    )
                    self.assertEqual(
                        await sync_to_async(receive_outcome)(producer, body),
                        "duplicate",
                    )
                    await launch(owner)
                    # Freeze a still-connected Janus owner, expire both leases and recover while it is frozen.
                    workers[standby].send_signal(signal.SIGSTOP)
                    await GatewayCommandRoute.objects.filter(pk=two.pk).aupdate(
                        lease_expires_at=timezone.now() - timedelta(seconds=1)
                    )
                    await redis.pexpire(key, 1)
                    three = await route(3)
                    self.assertEqual(three.owner_id, owner)
                    self.assertEqual(
                        (await janus_request(two.janus_session_id))["error"]["code"],
                        458,
                    )
                    workers[standby].send_signal(signal.SIGCONT)
                    third, claims, ack, _ = await browser(owner, claims)
                    self.assertEqual(ack["gateway_generation"], 3)
                    await observed(3)
                    print(
                        "Verified physical session fencing at generation 3", flush=True
                    )
                    # A delayed snapshot from the original owner cannot restore old presence.
                    old_observation["event_id"] = str(uuid4())
                    old_observation["sequence"] += 1
                    self.assertEqual(
                        await sync_to_async(receive_event)(owner, old_observation),
                        "stale",
                    )
                    self.assertEqual(
                        await GatewaySessionObservation.objects.filter(
                            participant=self.participant
                        ).acount(),
                        1,
                    )
                    self.assertEqual(
                        await GatewayOwnerGeneration.objects.filter(
                            session=self.session
                        ).acount(),
                        3,
                    )
                    await asyncio.wait_for(second.wait_closed(), 12)
                    # Drive a fresh intent through the regular Django dispatcher to this incarnation.
                    await sync_to_async(MeetingLifecycleService.remove_participant)(
                        session=self.session,
                        actor_profile=self.host,
                        participant=self.guest_participant,
                    )
                    await commands.sweep()
                    async with asyncio.timeout(12):
                        while True:
                            row = (
                                await GatewayAuthorizationOutbox.objects.select_related(
                                    "command"
                                ).aget(
                                    session=self.session,
                                    action="participant.disconnect",
                                )
                            )
                            if row.status == "applied":
                                break
                            await asyncio.sleep(0.1)
                    delivered = row.command.envelope
                    self.assertEqual(delivered["expected_owner_generation"], 3)
                    signed = self.bus.signer.sign(delivered, "command")
                    await streams.publish(
                        command_route(owner, str(three.owner_incarnation)),
                        signed,
                        delivered["command_id"],
                    )
                    self.assertEqual(
                        await GatewayAuthorizationOutbox.objects.filter(
                            pk=row.pk, status="applied"
                        ).acount(),
                        1,
                    )
                    # Outlast the owner's renewal interval with an actual broker outage.
                    await redis.execute_command("CLIENT", "PAUSE", 8500, "ALL")
                    await asyncio.wait_for(third.wait_closed(), 18)
                    await stop(standby)
                    await launch(standby)
                    four = await route(4)
                    self.assertEqual(four.owner_id, standby)
                    _, _, ack, _ = await browser(standby, claims)
                    self.assertEqual(ack["gateway_generation"], 4)
                    await observed(4)
                finally:
                    for process in workers.values():
                        if process.returncode is None:
                            process.send_signal(signal.SIGCONT)
                    await asyncio.gather(
                        *(ws.close() for ws in sockets), return_exceptions=True
                    )
                    await asyncio.gather(
                        *(stop(owner) for owner in list(workers)),
                        return_exceptions=True,
                    )
                    authority_server.should_exit = True
                    projection.stop.set()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    await commands.transport.close()
                    await streams.close()
                    await asyncio.gather(*(peer.close() for peer in peers))
                    await redis.aclose()
                    await janus.stop()
                    await sync_to_async(connections.close_all)()

    Integration.__module__ = __name__
    globals()["Integration"] = Integration
    raise SystemExit(
        bool(DiscoverRunner(verbosity=1).run_tests([__name__ + ".Integration"]))
    )


if __name__ == "__main__":
    main()
