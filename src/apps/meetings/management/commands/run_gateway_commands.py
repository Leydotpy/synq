"""One separate command/outcome worker; never started inside Django/Janus ASGI."""

import asyncio
import signal

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from apps.meetings.services.gateway_dispatcher import GatewayCommandWorker
from jrtc_signalling.bus import BusConfig, BusError


class Command(BaseCommand):
    help = "Relay durable gateway commands and record authenticated outcomes."

    def add_arguments(self, parser):
        parser.add_argument(
            "--once", action="store_true", help="Run one bounded dispatch sweep."
        )

    def handle(self, *args, **options):
        if not settings.GATEWAY_COMMANDS_ENABLED:
            raise CommandError("Gateway commands are disabled.")
        try:
            if (
                settings.JRTC_EVENT_BROKER_ENGINE != "redis"
                or settings.JRTC_REDIS_MODE != "streams"
            ):
                raise BusError(
                    "Gateway commands require the configured Redis Streams backend."
                )
            config = BusConfig.read(settings.GATEWAY_BUS_CONFIG_FILE, gateway=False)

            async def run():
                worker = GatewayCommandWorker(config)
                loop = asyncio.get_running_loop()
                for signum in (signal.SIGINT, signal.SIGTERM):
                    loop.add_signal_handler(signum, worker.stop.set)
                self.stdout.write("Gateway command worker started.")
                try:
                    await worker.run(once=options["once"])
                finally:
                    for signum in (signal.SIGINT, signal.SIGTERM):
                        loop.remove_signal_handler(signum)
                self.stdout.write("Gateway command worker stopped.")

            asyncio.run(run())
        except BusError, OSError, ValueError:
            raise CommandError(
                "Gateway command worker configuration or transport unavailable."
            ) from None
