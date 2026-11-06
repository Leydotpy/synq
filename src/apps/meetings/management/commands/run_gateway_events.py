"""Opt-in observation worker; never starts a Janus runtime or changes admission."""

import asyncio
import signal

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from apps.meetings.services.gateway_event_worker import GatewayEventWorker
from jrtc_signalling.bus import BusConfig, BusError


class Command(BaseCommand):
    help = "Consume authenticated gateway lifecycle snapshots."

    def handle(self, *args, **options):
        if not settings.GATEWAY_EVENTS_ENABLED:
            raise CommandError("Gateway lifecycle events are disabled.")
        try:
            if (
                settings.JRTC_EVENT_BROKER_ENGINE != "redis"
                or settings.JRTC_REDIS_MODE != "streams"
            ):
                raise BusError("Lifecycle events require Redis Streams.")
            config = BusConfig.read(settings.GATEWAY_BUS_CONFIG_FILE, gateway=False)

            async def run():
                worker = GatewayEventWorker(config)
                loop = asyncio.get_running_loop()
                for signum in (signal.SIGINT, signal.SIGTERM):
                    loop.add_signal_handler(signum, worker.stop.set)
                self.stdout.write("Gateway lifecycle worker started.")
                try:
                    await worker.run()
                finally:
                    for signum in (signal.SIGINT, signal.SIGTERM):
                        loop.remove_signal_handler(signum)
                self.stdout.write("Gateway lifecycle worker stopped.")

            asyncio.run(run())
        except (BusError, OSError, ValueError):
            raise CommandError(
                "Gateway lifecycle configuration or transport unavailable."
            ) from None
