"""Dedicated Django background worker for signed command/outcome streams."""

import asyncio
from collections import Counter
from contextlib import suppress

from asgiref.sync import sync_to_async
from django.db.models import Q
from django.utils import timezone

from apps.meetings.models import GatewayAuthorizationOutbox
from apps.meetings.services.gateway_commands import (
    OPEN,
    prepare_delivery,
    publication_finished,
    quarantine,
    receive_outcome,
)
from jrtc_signalling.bus import EVENTS, BusError, command_route, digest
from jrtc_signalling.bus_transport import Streams


def candidates():
    now = timezone.now()
    return list(
        GatewayAuthorizationOutbox.objects.filter(
            Q(lease_until__isnull=True) | Q(lease_until__lte=now),
            status__in=OPEN,
            next_attempt_at__lte=now,
        )
        .order_by("created_at")
        .values_list("pk", flat=True)[:20]
    )


class GatewayCommandWorker:
    def __init__(self, config):
        self.config = config
        self.transport = Streams(config.redis_url, "django-control-v1")
        self.stop = asyncio.Event()
        self.counters = Counter()

    async def sweep(self):
        for row_id in await sync_to_async(candidates)():
            claim = await sync_to_async(prepare_delivery)(row_id)
            if claim is None:
                continue
            success = False
            try:
                packet = self.config.signer.sign(claim["envelope"], "command")
                await self.transport.publish(
                    command_route(claim["envelope"]["target_owner"]),
                    packet,
                    str(claim["command_id"]),
                )
                success = True
                self.counters["published"] += 1
            except Exception:
                self.counters["publish_failed"] += 1
            await sync_to_async(publication_finished)(claim, success=success)

    async def handle(self, delivery):
        try:
            try:
                producer, event = self.config.verifier.verify(
                    delivery.payload, "command-outcome"
                )
                result = await sync_to_async(receive_outcome)(producer, event)
                self.counters[result] += 1
            except BusError:
                await sync_to_async(quarantine)(
                    digest(delivery.payload), "untrusted_outcome"
                )
                self.counters["quarantined"] += 1
            async with asyncio.timeout(2):
                await delivery.acknowledgement.ack()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.counters["pending_failure"] += 1

    async def run(self, *, once=False):
        reconnects = 0
        while not self.stop.is_set():
            try:
                await self.transport.start()
                await self.transport.subscribe(EVENTS, self.handle)
                while not self.stop.is_set():
                    await self.sweep()
                    if not await self.transport.healthy():
                        raise BusError("Control transport unavailable.")
                    reconnects = 0
                    if once:
                        return
                    with suppress(TimeoutError):
                        await asyncio.wait_for(self.stop.wait(), 1)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.counters["transport_failure"] += 1
                reconnects += 1
                if once:
                    raise BusError("Control worker unavailable.") from None
            finally:
                with suppress(Exception):
                    await self.transport.close()
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    self.stop.wait(), min(30, 2 ** min(reconnects, 5))
                )
