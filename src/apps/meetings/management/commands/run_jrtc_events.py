"""Run the dedicated authoritative JRTC broker consumer."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from apps.meetings.jrtc.config import JrtcEventConfig, load_event_config
from apps.meetings.jrtc.events.consumer import JrtcEventConsumer, build_event_consumer
from apps.meetings.jrtc.ownership import new_runtime_owner_id

ConsumerReadyCallback = Callable[[JrtcEventConsumer], None]


class Command(BaseCommand):
    """Own one long-lived Broka subscription in a dedicated process."""

    help = (
        "Consumes JRTC Janus events from the shared physical destination, "
        "applies idempotent meeting state updates, and forwards client events."
    )

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--consumer-name",
            default=None,
            help=(
                "Override the unique live broker consumer identity. The durable "
                "group/queue identity remains controlled by settings."
            ),
        )

    def handle(self, *args: object, **options: object) -> None:
        del args
        consumer_name = options.get("consumer_name") or new_runtime_owner_id()
        try:
            config = load_event_config(consumer_name=str(consumer_name))
            if not config.enabled:
                self.stdout.write(
                    self.style.WARNING(
                        "JRTC event consumption is disabled by JRTC_EVENTS_ENABLED."
                    )
                )
                return
            consumer = build_event_consumer(config)
            asyncio.run(
                run_until_stopped(
                    consumer,
                    on_ready=self._report_ready,
                )
            )
            self.stdout.write(
                self.style.SUCCESS("JRTC event consumer stopped cleanly.")
            )
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("JRTC event consumer interrupted."))
        except Exception as exc:
            raise CommandError("JRTC event consumer terminated with an error.") from exc

    def _report_ready(self, consumer: JrtcEventConsumer) -> None:
        """Write and flush feedback only after the subscription is active."""

        self.stdout.write(
            self.style.SUCCESS(consumer_readiness_message(consumer.config))
        )
        self.stdout.flush()


def consumer_readiness_message(config: JrtcEventConfig) -> str:
    """Describe the active subscription without exposing connection secrets."""

    details = [
        f"backend={config.engine!r}",
        f"route={config.physical_route!r}",
        f"consumer={config.consumer_name!r}",
    ]
    if config.engine == "redis":
        mode = str(config.engine_options.get("mode", "streams"))
        details.insert(1, f"mode={mode!r}")
        if mode == "streams":
            details.append(f"group={config.consumer_group!r}")
    elif config.engine == "rabbitmq":
        details.append(f"queue={config.consumer_group!r}")
    elif config.engine == "kafka":
        details.append(f"group={config.consumer_group!r}")
    return (
        "JRTC event consumer running and listening for events "
        f"({', '.join(details)})."
    )


async def run_until_stopped(
    consumer: JrtcEventConsumer,
    *,
    stop_event: asyncio.Event | None = None,
    on_ready: ConsumerReadyCallback | None = None,
) -> None:
    """Run one consumer until SIGINT/SIGTERM or an injected stop event."""

    selected_stop_event = stop_event or asyncio.Event()
    cleanup_signals: Callable[[], None] = lambda: None
    await consumer.start()
    try:
        if on_ready is not None:
            on_ready(consumer)
        if stop_event is None:
            cleanup_signals = _install_signal_handlers(selected_stop_event)
        await selected_stop_event.wait()
    finally:
        cleanup_signals()
        await consumer.stop()


def _install_signal_handlers(stop_event: asyncio.Event) -> Callable[[], None]:
    """Install portable stop callbacks and return their cleanup function."""

    loop = asyncio.get_running_loop()
    loop_signals: list[signal.Signals] = []
    fallback_signals: list[tuple[signal.Signals, Any]] = []

    def request_stop(*_args: object) -> None:
        loop.call_soon_threadsafe(stop_event.set)

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_event.set)
            loop_signals.append(signum)
            continue
        except (NotImplementedError, RuntimeError):
            pass
        try:
            previous = signal.getsignal(signum)
            signal.signal(signum, request_stop)
            fallback_signals.append((signum, previous))
        except (OSError, RuntimeError, ValueError):
            # A non-main-thread consumer can still be stopped by cancellation
            # or by an injected event in tests/embedded deployments.
            continue

    def cleanup() -> None:
        for signum in loop_signals:
            with suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(signum)
        for signum, previous in fallback_signals:
            with suppress(OSError, RuntimeError, ValueError):
                signal.signal(signum, previous)

    return cleanup


__all__ = [
    "Command",
    "consumer_readiness_message",
    "run_until_stopped",
]
