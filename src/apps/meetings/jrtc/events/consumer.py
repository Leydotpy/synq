"""Dedicated Broka consumer for Synq's authoritative JRTC event plane."""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from contextlib import suppress
from inspect import iscoroutinefunction
from time import perf_counter
from typing import Any

from broka import (
    AcknowledgementMode,
    Broker,
    Delivery,
    Subscription,
    SubscriptionOptions,
)
from jrtc.messaging import DEFAULT_PHYSICAL_ROUTE
from logvista import bind_context

from apps.meetings.jrtc.broker import build_event_broker
from apps.meetings.jrtc.config import JrtcEventConfig, load_event_config
from apps.meetings.jrtc.errors import (
    JrtcBrokerConsumerFailure,
    JrtcBrowserDispatchFailure,
)
from apps.meetings.jrtc.events.dispatcher import JrtcEventDispatcher
from apps.meetings.jrtc.events.schemas import event_from_delivery

logger = logging.getLogger(__name__)


class JrtcEventConsumer:
    """Own one broker connection and one physical-route subscription."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"

    def __init__(
        self,
        broker: Broker,
        config: JrtcEventConfig,
        *,
        dispatcher: JrtcEventDispatcher | None = None,
    ) -> None:
        self.broker = broker
        self.config = config
        self.dispatcher = dispatcher or JrtcEventDispatcher()
        self.subscription: Subscription[Any] | None = None
        self.state = self.STOPPED
        self._counters: Counter[str] = Counter()
        self._event_type_counts: Counter[str] = Counter()
        self._handler_duration_total_ms = 0.0
        self._handler_duration_max_ms = 0.0
        self._handler_duration_last_ms = 0.0
        self._outbox_stop: asyncio.Event | None = None
        self._outbox_task: asyncio.Task[None] | None = None

    def inspect(self) -> dict[str, Any]:
        """Return a low-cardinality process-local health/metrics snapshot."""

        handled = self._counters["handled"]
        return {
            "state": self.state,
            "received": self._counters["received"],
            "handled": handled,
            "acknowledged": self._counters["acknowledged"],
            "failures": self._counters["failures"],
            "ack_failures": self._counters["ack_failures"],
            "duplicates": self._counters["duplicates"],
            "retries": self._counters["retries"],
            "browser_dispatches": self._counters["browser_dispatches"],
            "socketio_failures": self._counters["socketio_failures"],
            "correlated_handles": self._counters["correlated_handles"],
            "correlation_misses": self._counters["correlation_misses"],
            "outbox_retry_cycles": self._counters["outbox_retry_cycles"],
            "outbox_retry_attempts": self._counters["outbox_retry_attempts"],
            "outbox_retry_delivered": self._counters["outbox_retry_delivered"],
            "outbox_retry_discarded": self._counters["outbox_retry_discarded"],
            "outbox_retry_failures": self._counters["outbox_retry_failures"],
            "outbox_sweep_failures": self._counters["outbox_sweep_failures"],
            "event_types": dict(self._event_type_counts),
            "handler_duration_ms": {
                "last": round(self._handler_duration_last_ms, 3),
                "max": round(self._handler_duration_max_ms, 3),
                "average": round(
                    self._handler_duration_total_ms / handled,
                    3,
                )
                if handled
                else 0.0,
            },
        }

    async def start(self) -> None:
        """Start the broker, then establish exactly one physical subscription."""

        if self.state == self.RUNNING:
            return
        if self.state not in {self.STOPPED, self.FAILED}:
            raise JrtcBrokerConsumerFailure(
                f"JRTC event consumer cannot start while state={self.state}."
            )
        self.state = self.STARTING
        broker_started = False
        try:
            await self.broker.startup()
            broker_started = True
            self.subscription = await self.broker.subscribe(
                self.config.physical_route or DEFAULT_PHYSICAL_ROUTE,
                self.handle_delivery,
                options=subscription_options(self.config),
            )
            self.state = self.RUNNING
            self._start_outbox_relay()
            self._log_lifecycle(
                logging.INFO,
                "JRTC event consumer subscription is ready",
            )
        except asyncio.CancelledError:
            if broker_started:
                with suppress(Exception):
                    await self.broker.shutdown()
            self.state = self.STOPPED
            raise
        except Exception as exc:
            if broker_started:
                with suppress(Exception):
                    await self.broker.shutdown()
            self.subscription = None
            self.state = self.FAILED
            self._log_lifecycle(
                logging.ERROR,
                "JRTC event consumer failed to start",
                error_type=type(exc).__name__,
            )
            raise JrtcBrokerConsumerFailure(
                "Unable to start the authoritative JRTC event consumer."
            ) from exc

    async def stop(self) -> None:
        """Stop admission before shutting down the owned broker connection."""

        if self.state == self.STOPPED:
            return
        self.state = self.STOPPING
        self._log_lifecycle(
            logging.INFO,
            "JRTC event consumer is stopping",
        )
        first_error: Exception | None = None
        subscription, self.subscription = self.subscription, None
        if subscription is not None:
            try:
                await subscription.close()
            except Exception as exc:
                first_error = exc
                logger.exception("Could not close the JRTC event subscription")
        try:
            await self._stop_outbox_relay()
        except Exception as exc:
            first_error = first_error or exc
            logger.exception("Could not stop the JRTC browser-outbox relay")
        try:
            await self.broker.shutdown()
        except Exception as exc:
            first_error = first_error or exc
            logger.exception("Could not shut down the JRTC event broker")

        self.state = self.FAILED if first_error is not None else self.STOPPED
        if first_error is not None:
            self._log_lifecycle(
                logging.ERROR,
                "JRTC event consumer stopped with errors",
                error_type=type(first_error).__name__,
            )
            raise JrtcBrokerConsumerFailure(
                "The JRTC event consumer did not shut down cleanly."
            ) from first_error
        self._log_lifecycle(
            logging.INFO,
            "JRTC event consumer stopped",
        )

    def _log_lifecycle(
        self,
        level: int,
        message: str,
        **context: object,
    ) -> None:
        """Emit lifecycle fields through LogVista's structured context."""

        with bind_context(**self._lifecycle_log_context(), **context):
            logger.log(level, message)

    def _lifecycle_log_context(self) -> dict[str, object]:
        """Return non-secret, process-stable fields for lifecycle records."""

        context: dict[str, object] = {
            "jrtc_event_broker_engine": self.config.engine,
            "jrtc_event_physical_route": self.config.physical_route,
            "jrtc_event_consumer_name": self.config.consumer_name,
            "jrtc_event_consumer_group": self.config.consumer_group,
            "jrtc_event_consumer_state": self.state,
        }
        if self.config.engine == "redis":
            context["jrtc_event_redis_mode"] = str(
                self.config.engine_options.get("mode", "streams")
            )
        return context

    def _start_outbox_relay(self) -> None:
        """Start a durable retry sweep independent of broker redelivery."""

        retry = getattr(
            self.dispatcher,
            "retry_pending_browser_dispatches",
            None,
        )
        if retry is None or not iscoroutinefunction(retry):
            return
        self._outbox_stop = asyncio.Event()
        self._outbox_task = asyncio.create_task(
            self._run_outbox_relay(retry),
            name="synq-jrtc-browser-outbox",
        )

    async def _stop_outbox_relay(self) -> None:
        """Let an active sweep finish, then cancel it at the drain deadline."""

        task, self._outbox_task = self._outbox_task, None
        stop, self._outbox_stop = self._outbox_stop, None
        if task is None:
            return
        if stop is not None:
            stop.set()
        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=self.config.drain_timeout,
            )
        except TimeoutError:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def _run_outbox_relay(self, retry: Any) -> None:
        """Continuously relay eligible database rows with bounded batches."""

        stop = self._outbox_stop
        if stop is None:
            return
        while not stop.is_set():
            try:
                outcome = await retry(
                    limit=self.config.outbox_batch_size,
                    retry_delay=self.config.outbox_retry_delay,
                    lease_timeout=self.config.outbox_lease_timeout,
                )
                self._counters["outbox_retry_cycles"] += 1
                self._counters["outbox_retry_attempts"] += outcome.attempted
                self._counters["outbox_retry_delivered"] += outcome.delivered
                self._counters["outbox_retry_discarded"] += outcome.discarded
                self._counters["outbox_retry_failures"] += outcome.failed
                if outcome.attempted:
                    logger.info(
                        "JRTC browser-outbox retry sweep completed",
                        extra={
                            "outbox_attempted": outcome.attempted,
                            "outbox_delivered": outcome.delivered,
                            "outbox_discarded": outcome.discarded,
                            "outbox_failed": outcome.failed,
                        },
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                self._counters["outbox_sweep_failures"] += 1
                logger.exception("JRTC browser-outbox retry sweep failed")

            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self.config.outbox_poll_interval,
                )
            except TimeoutError:
                continue

    async def handle_delivery(self, delivery: Delivery[Any]) -> None:
        """Await all application work and only then manually acknowledge it."""

        started_at = perf_counter()
        event_type = "unknown"
        try:
            event = event_from_delivery(delivery)
            event_type = event.event_type
            self._counters["received"] += 1
            self._event_type_counts[event_type] += 1
            if event.delivery_attempt > 1:
                self._counters["retries"] += 1
            outcome = await self.dispatcher.dispatch(event)
            if outcome is not None:
                if outcome.duplicate:
                    self._counters["duplicates"] += 1
                else:
                    self._counters[
                        "correlated_handles"
                    ] += outcome.correlated_handles
                    if outcome.correlated_handles == 0:
                        self._counters["correlation_misses"] += 1
                self._counters["browser_dispatches"] += outcome.browser_dispatches
            try:
                await delivery.ack()
            except Exception:
                self._counters["ack_failures"] += 1
                raise
            self._counters["acknowledged"] += 1
        except Exception as exc:
            if isinstance(exc, JrtcBrowserDispatchFailure):
                self._counters["socketio_failures"] += 1
            self._counters["failures"] += 1
            logger.exception(
                "JRTC event delivery failed before acknowledgement",
                extra={"janus_event_type": event_type},
            )
            raise
        finally:
            duration_ms = (perf_counter() - started_at) * 1000.0
            self._counters["handled"] += 1
            self._handler_duration_last_ms = duration_ms
            self._handler_duration_total_ms += duration_ms
            self._handler_duration_max_ms = max(
                self._handler_duration_max_ms,
                duration_ms,
            )
        logger.info(
            "JRTC event delivery acknowledged",
            extra={
                "janus_event_type": event_type,
                "handler_duration_ms": round(
                    self._handler_duration_last_ms,
                    3,
                ),
                "duplicate": bool(
                    outcome is not None and outcome.duplicate
                ),
                "browser_dispatches": (
                    0 if outcome is None else outcome.browser_dispatches
                ),
            },
        )

    async def __aenter__(self) -> JrtcEventConsumer:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        del exc_type, exc, traceback
        await self.stop()


def subscription_options(config: JrtcEventConfig) -> SubscriptionOptions:
    """Build capabilities-safe manual acknowledgement options per backend."""

    redis_streams = (
        config.engine == "redis"
        and str(config.engine_options.get("mode", "streams")) == "streams"
    )
    durable = redis_streams or config.engine in {"rabbitmq", "kafka"}
    supports_consumer_groups = redis_streams or config.engine == "kafka"
    return SubscriptionOptions(
        acknowledgement_mode=AcknowledgementMode.MANUAL,
        durable=durable,
        consumer_group=(
            config.consumer_group if supports_consumer_groups else None
        ),
        consumer_id=config.consumer_name,
        concurrency=config.consumer_concurrency,
        capacity=config.consumer_capacity,
    )


def build_event_consumer(
    config: JrtcEventConfig | None = None,
    *,
    broker: Broker | None = None,
    dispatcher: JrtcEventDispatcher | None = None,
) -> JrtcEventConsumer:
    """Construct an unstarted consumer with a process-owned broker."""

    selected = config or load_event_config()
    selected_dispatcher = dispatcher or JrtcEventDispatcher(
        outbox_lease_timeout=selected.outbox_lease_timeout,
    )
    return JrtcEventConsumer(
        broker or build_event_broker(selected),
        selected,
        dispatcher=selected_dispatcher,
    )


__all__ = [
    "JrtcEventConsumer",
    "build_event_consumer",
    "subscription_options",
]
