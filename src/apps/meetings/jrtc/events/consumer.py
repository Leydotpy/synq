"""Dedicated Broka consumer for Synq's authoritative JRTC event plane."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import Any

from broka import (
    AcknowledgementMode,
    Broker,
    Delivery,
    Subscription,
    SubscriptionOptions,
)
from jrtc.messaging import DEFAULT_PHYSICAL_ROUTE

from apps.meetings.jrtc.broker import build_event_broker
from apps.meetings.jrtc.config import JrtcEventConfig, load_event_config
from apps.meetings.jrtc.errors import JrtcBrokerConsumerFailure
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
            raise JrtcBrokerConsumerFailure(
                "Unable to start the authoritative JRTC event consumer."
            ) from exc

    async def stop(self) -> None:
        """Stop admission before shutting down the owned broker connection."""

        if self.state == self.STOPPED:
            return
        self.state = self.STOPPING
        first_error: Exception | None = None
        subscription, self.subscription = self.subscription, None
        if subscription is not None:
            try:
                await subscription.close()
            except Exception as exc:
                first_error = exc
                logger.exception("Could not close the JRTC event subscription")
        try:
            await self.broker.shutdown()
        except Exception as exc:
            first_error = first_error or exc
            logger.exception("Could not shut down the JRTC event broker")

        self.state = self.FAILED if first_error is not None else self.STOPPED
        if first_error is not None:
            raise JrtcBrokerConsumerFailure(
                "The JRTC event consumer did not shut down cleanly."
            ) from first_error

    async def handle_delivery(self, delivery: Delivery[Any]) -> None:
        """Await all application work and only then manually acknowledge it."""

        event = event_from_delivery(delivery)
        await self.dispatcher.dispatch(event)
        await delivery.ack()

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
    return JrtcEventConsumer(
        broker or build_event_broker(selected),
        selected,
        dispatcher=dispatcher,
    )


__all__ = [
    "JrtcEventConsumer",
    "build_event_consumer",
    "subscription_options",
]
