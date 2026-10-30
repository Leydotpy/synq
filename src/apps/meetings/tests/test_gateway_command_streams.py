"""Real Redis/Broka plus real Django ASGI authority and gateway inbox integration."""

import asyncio
import os
import tempfile
import uuid
from dataclasses import replace
from pathlib import Path
from unittest import skipUnless
from unittest.mock import patch

import httpx2 as httpx
from asgiref.sync import async_to_sync, sync_to_async
from django.core.asgi import get_asgi_application
from django.test import TransactionTestCase
from redis.asyncio import Redis

from apps.meetings.models import GatewayAuthorizationOutbox, GatewayCommandOutcome
from apps.meetings.services.gateway_dispatcher import GatewayCommandWorker
from apps.meetings.tests.test_gateway_commands import GatewayCommandFixture
from jrtc_signalling.bus import (
    EVENTS,
    BusConfig,
    Signer,
    TrustedKey,
    Verifier,
    command_route,
)
from jrtc_signalling.commands import CommandRuntime


@skipUnless(os.getenv("GATEWAY_TEST_REDIS_URL"), "real Redis required")
class GatewayCommandStreamsTests(GatewayCommandFixture, TransactionTestCase):
    def setUp(self):
        for target in (
            "apps.meetings.services.lifecycle.dispatch_task",
            "apps.meetings.realtime.emitter.MeetingSocketEmitter",
        ):
            mock = patch(target)
            mock.start()
            self.addCleanup(mock.stop)
        super().setUp()

    def test_broker_stall_recovers_committed_command_and_records_signed_failure(self):
        owner = "test-" + uuid.uuid4().hex
        self.route.owner_id = owner
        self.route.save()
        url = os.environ["GATEWAY_TEST_REDIS_URL"]
        django_config = replace(
            self.config,
            redis_url=url,
            verifier=Verifier(
                {"worker-key": TrustedKey(owner, self.worker_key.public_key())}
            ),
        )

        async def exercise(inbox):
            gateway_config = BusConfig(
                url,
                Signer(owner, "worker-key", self.worker_key),
                Verifier(
                    {
                        "django-key": TrustedKey(
                            "django-control", self.django_key.public_key()
                        )
                    }
                ),
                inbox,
                "https://testserver/api/v1/meetings/internal/gateway/commands/authorize/",
                "test-service-" + "x" * 32,
            )
            gateway = CommandRuntime(gateway_config)
            await gateway.authority.client.aclose()
            gateway.authority.client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=get_asgi_application())
            )
            worker = GatewayCommandWorker(django_config)
            task = None
            try:
                await worker.transport.start()
                # A real Redis stall exceeds the two-second publish budget. Its
                # eventual XADD may be ambiguous, so recovery must keep the ID.
                async with Redis.from_url(url) as client:
                    await client.execute_command("CLIENT", "PAUSE", 2500, "ALL")
                await worker.sweep()
                pending = await sync_to_async(GatewayAuthorizationOutbox.objects.get)(
                    pk=self.row.pk
                )
                self.assertEqual(pending.status, "pending")
                self.assertEqual(pending.last_error, "unavailable")
                await gateway.start()
                task = asyncio.create_task(worker.run())
                async with asyncio.timeout(15):
                    while True:
                        row = await sync_to_async(
                            GatewayAuthorizationOutbox.objects.get
                        )(pk=self.row.pk)
                        if row.status == "failed":
                            break
                        await asyncio.sleep(0.02)
                self.assertEqual(row.last_error, "owner_lost")
                self.assertEqual(
                    await sync_to_async(GatewayCommandOutcome.objects.count)(), 2
                )
                self.assertEqual(gateway.store.inspect()["inbox"], 1)
                self.assertEqual(worker.counters["publish_failed"], 1)
            finally:
                worker.stop.set()
                if task:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await worker.transport.close()
                await gateway.aclose()
                async with Redis.from_url(url) as client:
                    await client.delete(command_route(owner))
                    # This CI database is isolated and has only this test's events.
                    await client.delete(EVENTS)

        with tempfile.TemporaryDirectory() as temporary:
            async_to_sync(exercise)(str(Path(temporary) / "inbox.sqlite3"))
        self.guest_participant.refresh_from_db()
        self.assertEqual(self.guest_participant.status, "admitted")
        self.assertIsNone(self.guest_participant.joined_at)
