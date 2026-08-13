"""Reusable, deterministic local listeners for typed Janus responses."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from contextlib import suppress
from threading import RLock

from broka import MetricsProvider
from logvista import VisualLogger, get_logger

from jrtc.messaging.constants import (
    DISPATCHABLE_JANUS_TYPES,
    LISTENER_FAILURES_TOTAL,
)
from jrtc.messaging.metrics import LogVistaMetrics
from jrtc.models import JanusResponse

type ResponseCallback = Callable[[JanusResponse], Awaitable[None] | None]


class LocalListenerRegistry:
    """Maintain ordered per-event and wildcard callbacks.

    Listener failures are isolated so one local integration cannot prevent
    transaction resolution or external publication. Cancellation is never
    swallowed.
    """

    def __init__(
        self,
        *,
        metrics: MetricsProvider | None = None,
        logger: VisualLogger | None = None,
    ) -> None:
        self.logger = logger or get_logger("jrtc.messaging.listeners")
        self.metrics = metrics or LogVistaMetrics(self.logger)
        self._listeners: dict[str, list[ResponseCallback]] = {}
        self._lock = RLock()

    def add(self, event: str, callback: ResponseCallback) -> None:
        """Register ``callback`` once for an exact event or ``"*"``."""

        event = self._validate_event(event)
        if not callable(callback):
            raise TypeError("listener must be callable")
        with self._lock:
            listeners = self._listeners.setdefault(event, [])
            if callback not in listeners:
                listeners.append(callback)

    def remove(self, event: str, callback: ResponseCallback) -> bool:
        """Remove a callback, returning whether it was registered."""

        event = self._validate_event(event)
        with self._lock:
            listeners = self._listeners.get(event)
            if not listeners:
                return False
            try:
                listeners.remove(callback)
            except ValueError:
                return False
            if not listeners:
                self._listeners.pop(event, None)
            return True

    def clear(self, event: str | None = None) -> None:
        """Remove listeners for one event, or all listeners when omitted."""

        with self._lock:
            if event is None:
                self._listeners.clear()
            else:
                self._listeners.pop(self._validate_event(event), None)

    def snapshot(self, event: str) -> tuple[ResponseCallback, ...]:
        """Return the exact deterministic invocation order for ``event``."""

        event = self._validate_event(event)
        with self._lock:
            callbacks = [*self._listeners.get(event, ()), *self._listeners.get("*", ())]
        unique: list[ResponseCallback] = []
        for callback in callbacks:
            if callback not in unique:
                unique.append(callback)
        return tuple(unique)

    async def notify(self, event: str, response: JanusResponse) -> None:
        """Invoke a stable listener snapshot sequentially."""

        safe_event = event if event in DISPATCHABLE_JANUS_TYPES else "unknown"
        for callback in self.snapshot(event):
            try:
                result = callback(response)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                with suppress(Exception):
                    self.metrics.increment(
                        LISTENER_FAILURES_TOTAL,
                        labels={"janus_type": safe_event},
                    )
                self.logger.error(
                    "Local listener failed",
                    "A Janus response listener raised and was isolated",
                    context={
                        "error_type": type(exc).__name__,
                        "janus_type": safe_event,
                    },
                    exc_info=exc,
                )

    @staticmethod
    def _validate_event(event: str) -> str:
        if not isinstance(event, str) or not event.strip():
            raise ValueError("listener event must be a non-empty string")
        return event.strip()


__all__ = ["LocalListenerRegistry", "ResponseCallback"]
