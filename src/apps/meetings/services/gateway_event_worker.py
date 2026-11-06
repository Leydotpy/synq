"""Separate lifecycle subscription; leaves command outcomes and raw janus.events intact."""

import asyncio
from collections import Counter
from contextlib import suppress

from asgiref.sync import sync_to_async

from apps.meetings.services.gateway_events import quarantine, receive_event
from jrtc_signalling.bus import LIFECYCLE, BusError, digest
from jrtc_signalling.bus_transport import Streams


class GatewayEventWorker:
    def __init__(self, config):
        self.config = config
        self.transport = Streams(config.redis_url, "django-lifecycle-v1")
        self.stop = asyncio.Event()
        self.counters = Counter()

    async def handle(self, delivery):
        try:
            try:
                producer, event = self.config.verifier.verify(
                    delivery.payload, "lifecycle-event"
                )
                result = await sync_to_async(receive_event)(producer, event)
                self.counters[result] += 1
            except BusError:
                await sync_to_async(quarantine)(digest(delivery.payload))
                self.counters["quarantined"] += 1
            async with asyncio.timeout(2):
                await delivery.acknowledgement.ack()
        except asyncio.CancelledError:
            raise
        except Exception:
            # Transaction/ACK failure leaves Redis pending for claim/replay.
            self.counters["pending_failure"] += 1

    async def run(self):
        while not self.stop.is_set():
            try:
                await self.transport.start()
                await self.transport.subscribe(LIFECYCLE, self.handle)
                while not self.stop.is_set():
                    if not await self.transport.healthy():
                        raise BusError("Lifecycle transport unavailable.")
                    with suppress(TimeoutError):
                        await asyncio.wait_for(self.stop.wait(), 1)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.counters["transport_failure"] += 1
            finally:
                with suppress(Exception):
                    await self.transport.close()
            with suppress(TimeoutError):
                await asyncio.wait_for(self.stop.wait(), 1)
