"""Paired legacy/gateway ICE ACK benchmark with isolated local media and faults."""

import argparse
import asyncio
import json
import math
import platform
import secrets
import shutil
import socket
import sys
import tempfile
import time
from contextlib import AsyncExitStack, ExitStack
from pathlib import Path
from traceback import extract_tb
from threading import Lock
from unittest.mock import patch

from check_gateway_state import build_test_gateway


class SqlMeter:
    """Count real SQL across Django's sync worker threads only during timed waves."""

    def __init__(self):
        self.lock = Lock()
        self.value = None

    def install(self, sender, connection, **kwargs):
        if self not in connection.execute_wrappers:
            connection.execute_wrappers.append(self)

    def __call__(self, execute, sql, params, many, context):
        with self.lock:
            value = self.value
        if value is None:
            return execute(sql, params, many, context)
        start = time.perf_counter()
        try:
            return execute(sql, params, many, context)
        finally:
            with self.lock:
                value["statements"] += 1
                value["sql_ms"] += 1000 * (time.perf_counter() - start)
                if sql.lstrip().split(None, 1)[0].upper() in {
                    "INSERT",
                    "UPDATE",
                    "DELETE",
                }:
                    value["writes"] += 1


async def measure(flows, addresses, meter):
    samples = []
    tally = {"statements": 0, "writes": 0, "sql_ms": 0.0}

    async def wave(send, base, ports):
        for i, port in enumerate(ports):
            # Low-priority additional host candidates. Every destination is a
            # bound UDP socket on this test host. The original audio pair stays live.
            candidate = {
                **base,
                "candidate": f"candidate:{i + 100} 1 udp 1 127.0.0.1 {port} typ host",
            }
            start = time.perf_counter()
            await send([candidate], False)
            samples.append((time.perf_counter() - start) * 1000)

    with meter.lock:
        meter.value = tally
    start = time.perf_counter()
    try:
        async with asyncio.TaskGroup() as tasks:
            for (send, base), ports in zip(flows, addresses, strict=True):
                tasks.create_task(wave(send, base, ports))
    finally:
        elapsed = time.perf_counter() - start
        with meter.lock:
            meter.value = None
    samples.sort()
    assert len(samples) == sum(map(len, addresses))
    return {
        "acks": len(samples),
        "batches_s": round(len(samples) / elapsed, 2),
        **{
            f"p{q}_ms": round(samples[math.ceil(q * len(samples) / 100) - 1], 3)
            for q in (50, 95, 99)
        },
        **tally,
        "sql_ms": round(tally["sql_ms"], 2),
    }


async def media_ready(peers):
    async with asyncio.timeout(12):
        for peer in peers:
            while True:
                stats = await peer.getStats()
                if peer.connectionState == "connected" and any(
                    item.type == "outbound-rtp" and item.packetsSent > 0
                    for item in stats.values()
                ):
                    break
                await asyncio.sleep(0.05)


async def private_redis(directory, stack):
    from redis.asyncio import Redis

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    process = await asyncio.create_subprocess_exec(
        "redis-server",
        "--bind",
        "127.0.0.1",
        "--port",
        str(port),
        "--dir",
        str(directory),
        "--save",
        "",
        "--appendonly",
        "no",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )

    async def stop():
        if process.returncode is None:
            process.terminate()
        await asyncio.wait_for(process.wait(), 5)

    stack.push_async_callback(stop)
    url = f"redis://127.0.0.1:{port}/0"
    redis = await stack.enter_async_context(Redis.from_url(url))
    from redis.exceptions import ConnectionError as RedisConnectionError

    async with asyncio.timeout(5):
        while True:
            try:
                await redis.ping()
                return redis, url
            except RedisConnectionError:
                await asyncio.sleep(0.05)


def main():
    import os

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", required=True, type=Path)
    parser.add_argument("--participants", type=int, choices=range(2, 9), default=4)
    parser.add_argument("--batches", type=int, choices=range(8, 49), default=32)
    parser.add_argument("--first", choices=("legacy", "gateway"), default="legacy")
    args = parser.parse_args()
    for command in ("janus", "redis-server"):
        if not shutil.which(command):
            parser.error(f"{command} is required")
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    sys.path.insert(0, str(args.gateway.resolve() / "tests"))
    os.environ.setdefault(
        "DJANGO_SETTINGS_MODULE", "conf.gateway_postgres_test_settings"
    )
    import django
    import httpx2 as httpx

    django.setup()
    from aiortc import (
        AudioStreamTrack,
        RTCConfiguration,
        RTCPeerConnection,
        RTCSessionDescription,
    )
    from asgiref.sync import sync_to_async
    from django.db import connections
    from django.db.backends.signals import connection_created
    from django.test import TransactionTestCase, override_settings
    from django.test.runner import DiscoverRunner
    from jrtc_video import VideoRoomCreateRequest
    from live_ice_helpers import split_trickle
    from media_helpers import Browser, publish_payload
    from test_janus_live import JanusServer
    from apps.meetings.models import Participant, ParticipantConnection
    from apps.meetings.realtime.namespace import MeetingNamespace
    from apps.meetings.services.janus import (
        acall_video_room_management_method,
        janus_room_id_for_session,
        janus_runtime,
    )
    from apps.meetings.tests.test_gateway_ownership import OwnershipFixture

    meter = SqlMeter()
    connection_created.connect(meter.install, weak=False)

    class Benchmark(OwnershipFixture, TransactionTestCase):
        def test_paired_media(self):
            self.people = [self.host] + [
                self.profile(f"load{i}") for i in range(1, args.participants)
            ]
            for i, profile in enumerate(self.people):
                for session in (self.session, self.legacy_session):
                    participant, _ = Participant.objects.get_or_create(
                        session=session,
                        profile=profile,
                        defaults={
                            "room": session.room,
                            "display_name": f"Peer {i}",
                            "status": "admitted",
                        },
                    )
                    if session == self.legacy_session:
                        ParticipantConnection.objects.create(
                            session=session,
                            profile=profile,
                            participant=participant,
                            socket_id=f"load-{i}",
                            status="active",
                        )
            with (
                tempfile.TemporaryDirectory() as directory,
                ExitStack() as ports,
                patch(
                    "apps.meetings.services.gateway_ownership.BusConfig.read",
                    return_value=self.bus,
                ),
            ):
                addresses = []
                for _ in self.people:
                    row = []
                    for _ in range(args.batches):
                        sock = ports.enter_context(
                            socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        )
                        sock.bind(("127.0.0.1", 0))
                        row.append(sock.getsockname()[1])
                    addresses.append(row)
                result = asyncio.run(self.run_pair(Path(directory), addresses))
            print(
                "SG16_RESULT="
                + json.dumps(
                    {
                        "python": platform.python_version(),
                        "database": connections["default"].vendor,
                        "first": args.first,
                        "participants": args.participants,
                        "batches_per_peer": args.batches,
                        **result,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

        async def run_pair(self, directory, addresses):
            server = JanusServer(directory)
            async with AsyncExitStack() as stack:
                stack.push_async_callback(sync_to_async(connections.close_all))
                await server.start()
                stack.push_async_callback(server.stop)
                redis, url = await private_redis(directory, stack)
                result = {}
                order = [args.first, "gateway" if args.first == "legacy" else "legacy"]
                for mode in order:
                    async with asyncio.timeout(90), AsyncExitStack() as phase:
                        peers, flows = [], []
                        if mode == "legacy":
                            phase.enter_context(
                                override_settings(
                                    JANUS_SESSION_URL=f"ws://127.0.0.1:{server.port}/",
                                    JANUS_API_SECRET=server.secret,
                                    JANUS_STARTUP_FAIL_FAST=True,
                                    JRTC_EVENTS_ENABLED=False,
                                )
                            )
                            await phase.enter_async_context(
                                janus_runtime.lifespan(None)
                            )
                            await acall_video_room_management_method(
                                self.legacy_session,
                                "create",
                                VideoRoomCreateRequest(
                                    room=janus_room_id_for_session(self.legacy_session),
                                    publishers=16,
                                    permanent=False,
                                ),
                            )
                            namespace = MeetingNamespace("/meetings")
                        else:
                            token = secrets.token_hex(32)
                            token_file = directory / "metrics.key"
                            token_file.write_text(token)
                            runtime, app = await build_test_gateway(
                                self,
                                directory,
                                server,
                                url,
                                metrics_token_file=str(token_file),
                            )
                            await phase.enter_async_context(
                                app.router.lifespan_context(app)
                            )

                        def report_failure(kind, exc, tb, mode=mode):
                            if exc is not None:
                                last = extract_tb(tb)[-1]
                                print(
                                    "SG16_CHECK_FAILURE",
                                    mode,
                                    kind.__name__,
                                    Path(last.filename).name,
                                    last.lineno,
                                    flush=True,
                                )

                        phase.push(report_failure)
                        for i, profile in enumerate(self.people):
                            peer = RTCPeerConnection(RTCConfiguration(iceServers=[]))
                            phase.push_async_callback(peer.close)
                            peers.append(peer)
                            peer.addTransceiver(
                                AudioStreamTrack(), direction="sendonly"
                            )
                            await peer.setLocalDescription(await peer.createOffer())
                            sdp, real = split_trickle(peer.localDescription.sdp)
                            if mode == "legacy":
                                binding = {
                                    "session_id": str(self.legacy_session.pk),
                                    "handle_type": "publisher",
                                }
                                sid = f"load-{i}"
                                ack = await namespace.on_session_media_publish(
                                    sid,
                                    {**binding, "offer": {"type": "offer", "sdp": sdp}},
                                )

                                async def send(
                                    items, completed, sid=sid, binding=binding
                                ):
                                    result = await namespace.on_session_media_trickle(
                                        sid,
                                        {
                                            **binding,
                                            "candidates": items,
                                            "completed": completed,
                                        },
                                    )
                                    assert result["action"] == "trickle"
                            else:
                                issued, claims = await sync_to_async(self.issue)(
                                    profile
                                )
                                browser = Browser(
                                    app,
                                    claims,
                                    lambda _, ticket=issued["ticket"]: ticket,
                                )
                                phase.push_async_callback(browser.close)
                                assert (await browser.open())["ok"]
                                payload = publish_payload()
                                payload["offer"]["sdp"] = sdp
                                reply = await browser.request(
                                    "media.publisher.publish", payload
                                )
                                assert reply["ok"], reply.get("error", {}).get("code")
                                ack = reply["result"]

                                async def send(
                                    items,
                                    completed,
                                    browser=browser,
                                    negotiation=ack["negotiation_id"],
                                ):
                                    reply = await browser.request(
                                        "media.trickle",
                                        {
                                            "handle_type": "publisher",
                                            "negotiation_id": negotiation,
                                            "candidates": items,
                                            "completed": completed,
                                        },
                                    )
                                    assert reply["ok"], reply.get("error", {}).get(
                                        "code"
                                    )

                            await peer.setRemoteDescription(
                                RTCSessionDescription(**ack["jsep"])
                            )
                            await send(real, False)
                            flows.append((send, real[0]))
                        await media_ready(peers)
                        if mode == "gateway":
                            timing = app.state.auth_endpoint.metrics.operations[
                                "media.trickle"
                            ]
                            queued_before, time_before = (
                                timing.queue_seconds,
                                timing.total_seconds,
                            )
                        result[mode] = await measure(flows, addresses, meter)
                        print(
                            f"SG16_WAVE {mode} acks={result[mode]['acks']}", flush=True
                        )
                        if mode == "gateway":
                            result[mode]["mean_dispatch_ms"] = round(
                                1000
                                * (timing.total_seconds - time_before)
                                / result[mode]["acks"],
                                3,
                            )
                            result[mode]["mean_queue_ms"] = round(
                                1000
                                * (timing.queue_seconds - queued_before)
                                / result[mode]["acks"],
                                3,
                            )
                            async with httpx.AsyncClient(
                                transport=httpx.ASGITransport(app=app),
                                base_url="https://testserver",
                            ) as client:
                                scrape = await client.get(
                                    "/metrics",
                                    headers={"Authorization": "Bearer " + token},
                                )
                                assert scrape.status_code == 200
                                assert (
                                    f"jrtc_ice_ack_seconds_count {result[mode]['acks'] + len(peers)}\n"
                                    in scrape.text
                                )
                            # This Redis process and Janus process belong exclusively to this script.
                            await redis.execute_command("CLIENT", "PAUSE", 2500, "ALL")
                            start = time.perf_counter()
                            await asyncio.gather(*(send([], True) for send, _ in flows))
                            result["broker_pause_all_peer_acks_ms"] = round(
                                1000 * (time.perf_counter() - start), 2
                            )
                            assert result["broker_pause_all_peer_acks_ms"] < 2500
                            await redis.ping()
                            await media_ready(peers)
                            result["ready_sessions"] = len(
                                [s for s in runtime.manager.sessions if s.ready]
                            )
                            result["attached_handles"] = sum(
                                len(s.plugins)
                                for s in runtime.manager.sessions
                                if s.ready
                            )
                            meeting = str(self.session.pk)
                            old = runtime.session(meeting)
                            start = time.perf_counter()
                            await server.stop()
                            async with asyncio.timeout(15):
                                while old.ready or runtime.registry.bindings:
                                    await asyncio.sleep(0.05)
                            result["janus_invalidation_ms"] = round(
                                1000 * (time.perf_counter() - start), 2
                            )
                            await server.start()
                            async with asyncio.timeout(25):
                                while (
                                    not runtime.manager.ready
                                    or runtime.session(meeting) is old
                                    or runtime.registry.uncertain
                                    or app.state.auth_endpoint.sessions
                                ):
                                    await asyncio.sleep(0.05)
                            await runtime.management(
                                meeting,
                                runtime.leases[meeting].generation,
                                "list_rooms",
                            )
                            assert not runtime.registry.cleanup_failed
                            result["janus_reconciled_ms"] = round(
                                1000 * (time.perf_counter() - start), 2
                            )
                            print("SG16_RECOVERY verified", flush=True)
                        else:
                            await asyncio.gather(*(send([], True) for send, _ in flows))
                return result

    Benchmark.__module__ = __name__
    globals()["Benchmark"] = Benchmark
    raise SystemExit(
        bool(DiscoverRunner(verbosity=1).run_tests([__name__ + ".Benchmark"]))
    )


if __name__ == "__main__":
    main()
