"""Declarative Dispio coordination for inbound Janus responses."""

from __future__ import annotations

import inspect
import time
from collections.abc import Iterable
from contextlib import suppress

from broka import MetricsProvider
from dispio import Dispatcher, ExactMatcher
from logvista import VisualLogger, get_logger

from jrtc.messaging.constants import (
    DISPATCH_DURATION_SECONDS,
    DISPATCH_FAILURES_TOTAL,
    DISPATCH_TOTAL,
    DISPATCHABLE_JANUS_TYPES,
)
from jrtc.messaging.listeners import LocalListenerRegistry, ResponseCallback
from jrtc.messaging.metrics import LogVistaMetrics
from jrtc.messaging.publisher import JanusEventPublisher, JanusIdentifier
from jrtc.models import JanusResponse


class JanusResponseDispatcher:
    """Resolve every Janus response exactly once through Dispio.

    Dispatchable messages run the generic transaction callback, then local
    listeners, then bounded broker admission. ACK and error use their dedicated
    callbacks. Unknown and ordinary transactional responses use the generic
    callback. Callback failures are logged and isolated from transport loops.
    """

    dispatchable_events = DISPATCHABLE_JANUS_TYPES

    def __init__(
        self,
        *,
        publisher: JanusEventPublisher | None = None,
        on_transaction: ResponseCallback | None = None,
        on_ack: ResponseCallback | None = None,
        on_error: ResponseCallback | None = None,
        on_message: ResponseCallback | Iterable[ResponseCallback] | None = None,
        listeners: LocalListenerRegistry | None = None,
        metrics: MetricsProvider | None = None,
        logger: VisualLogger | None = None,
    ) -> None:
        self.publisher = publisher
        self.on_transaction = on_transaction
        self.on_ack = on_ack
        self.on_error = on_error
        self.logger = logger or get_logger("jrtc.messaging.dispatcher")
        publisher_metrics = getattr(publisher, "metrics", None)
        self.metrics = metrics or publisher_metrics or LogVistaMetrics(self.logger)
        self.listeners = listeners or LocalListenerRegistry(
            metrics=self.metrics,
            logger=self.logger,
        )
        if on_message is not None:
            callbacks = (on_message,) if callable(on_message) else tuple(on_message)
            for callback in callbacks:
                self.listeners.add("*", callback)

        self._dispatcher = Dispatcher(name="janus.responses")
        self._dispatcher.add(
            ExactMatcher("ack"),
            self._handle_ack,
            name="janus-ack",
        )
        self._dispatcher.add(
            ExactMatcher("error"),
            self._handle_error,
            name="janus-error",
        )
        for janus_type in sorted(DISPATCHABLE_JANUS_TYPES):
            self._dispatcher.add(
                ExactMatcher(janus_type),
                self._handle_dispatchable,
                name=f"janus-{janus_type}",
            )
        self._dispatcher.default(self._handle_transaction, name="janus-transaction")
        self._dispatcher.registry.freeze()

    def add_listener(self, event: str, callback: ResponseCallback) -> None:
        """Add a local callback for an exact Janus type or ``"*"``."""

        self.listeners.add(event, callback)

    def remove_listener(self, event: str, callback: ResponseCallback) -> bool:
        """Remove a previously added local callback."""

        return self.listeners.remove(event, callback)

    async def dispatch(
        self,
        response: JanusResponse,
        *,
        session_id: JanusIdentifier | None = None,
        sender: JanusIdentifier | None = None,
    ) -> bool:
        """Resolve and execute one response, returning broker admission state."""

        started = time.perf_counter()
        resolved = await self._dispatcher.resolve_async(response, key=response.janus)
        safe_type = (
            response.janus
            if response.janus
            in {
                *DISPATCHABLE_JANUS_TYPES,
                "ack",
                "error",
                "success",
                "keepalive",
                "pong",
                "server_info",
            }
            else "unknown"
        )
        failed = False
        try:
            value = resolved.handler(
                response,
                session_id=session_id,
                sender=sender,
            )
            return bool(await value if inspect.isawaitable(value) else value)
        except Exception as exc:
            failed = True
            with suppress(Exception):
                self.metrics.increment(
                    DISPATCH_FAILURES_TOTAL,
                    labels={"janus_type": safe_type},
                )
            self.logger.error(
                "Response dispatch failed",
                "A selected Janus response handler raised and was isolated",
                context={
                    "error_type": type(exc).__name__,
                    "handler": resolved.registration.name,
                    "janus_type": safe_type,
                },
                exc_info=exc,
            )
            return False
        finally:
            duration = max(0.0, time.perf_counter() - started)
            with suppress(Exception):
                self.metrics.increment(
                    DISPATCH_TOTAL,
                    labels={
                        "janus_type": safe_type,
                        "result": "failed" if failed else "handled",
                    },
                )
                self.metrics.observe(
                    DISPATCH_DURATION_SECONDS,
                    duration,
                    labels={"janus_type": safe_type},
                )
            self.logger.debug(
                "Response dispatched",
                "Dispio selected one Janus response handler",
                context={
                    "duration_ms": round(duration * 1000, 3),
                    "handler": resolved.registration.name,
                    "janus_type": safe_type,
                    "match": resolved.match.reason,
                    "score": resolved.match.score,
                },
            )

    async def _handle_ack(
        self,
        response: JanusResponse,
        **_context: object,
    ) -> bool:
        await self._invoke(self.on_ack, response, callback="ack")
        return False

    async def _handle_error(
        self,
        response: JanusResponse,
        **_context: object,
    ) -> bool:
        await self._invoke(self.on_error, response, callback="error")
        return False

    async def _handle_transaction(
        self,
        response: JanusResponse,
        **_context: object,
    ) -> bool:
        await self._invoke(self.on_transaction, response, callback="transaction")
        return False

    async def _handle_dispatchable(
        self,
        response: JanusResponse,
        *,
        session_id: JanusIdentifier | None = None,
        sender: JanusIdentifier | None = None,
    ) -> bool:
        await self._invoke(self.on_transaction, response, callback="transaction")
        await self.listeners.notify(response.janus, response)
        if self.publisher is None:
            return False
        return await self.publisher.admit(
            response,
            session_id=session_id,
            sender=sender,
        )

    async def _invoke(
        self,
        callback_fn: ResponseCallback | None,
        response: JanusResponse,
        *,
        callback: str,
    ) -> None:
        if callback_fn is None:
            return
        try:
            result = callback_fn(response)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            with suppress(Exception):
                self.metrics.increment(
                    DISPATCH_FAILURES_TOTAL,
                    labels={"janus_type": callback},
                )
            self.logger.error(
                "Response callback failed",
                "A Janus dispatcher callback raised and was isolated",
                context={"callback": callback, "error_type": type(exc).__name__},
                exc_info=exc,
            )


__all__ = ["JanusResponseDispatcher"]
