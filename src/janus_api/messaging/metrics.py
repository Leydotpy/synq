"""LogVista-backed, snapshot-retaining metrics for Janus messaging."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from broka.observability.metrics import InMemoryMetrics, Labels
from logvista import VisualLogger, get_logger

_LOGGER_NAME: Final = "janus_api.messaging.metrics"
_EXTRA_HIGH_CARDINALITY_LABELS: Final = frozenset(
    {
        "handle",
        "handle_id",
        "sender",
        "session",
        "session_id",
        "transaction",
        "user",
        "user_id",
    }
)


class LogVistaMetrics(InMemoryMetrics):
    """Retain metric snapshots and emit each update as a debug diagnostic.

    The provider deliberately rejects identifiers that would create unbounded
    metric series. Message bodies, plugin data, and JSEP are never accepted or
    emitted by this API.
    """

    def __init__(self, logger: VisualLogger | None = None) -> None:
        super().__init__(allow_high_cardinality=False)
        self.logger = logger or get_logger(_LOGGER_NAME)

    @staticmethod
    def _validate_labels(labels: Labels | None) -> None:
        if not labels:
            return
        forbidden = {name.casefold() for name in labels} & _EXTRA_HIGH_CARDINALITY_LABELS
        if forbidden:
            names = ", ".join(sorted(forbidden))
            raise ValueError(f"high-cardinality metric labels are not allowed: {names}")

    @staticmethod
    def _log_labels(labels: Labels | None) -> dict[str, str]:
        return {str(name): str(value) for name, value in (labels or {}).items()}

    def _debug(
        self,
        kind: str,
        name: str,
        value: float,
        labels: Labels | None,
    ) -> None:
        self.logger.debug(
            "Messaging metric",
            f"Janus messaging {kind} updated",
            context={
                "kind": kind,
                "metric": name,
                "value": float(value),
                "labels": self._log_labels(labels),
            },
        )

    def increment(
        self,
        name: str,
        value: float = 1.0,
        *,
        labels: Labels | None = None,
    ) -> None:
        self._validate_labels(labels)
        super().increment(name, value, labels=labels)
        self._debug("counter", name, value, labels)

    increment_counter = increment

    def set_gauge(
        self,
        name: str,
        value: float,
        *,
        labels: Labels | None = None,
    ) -> None:
        self._validate_labels(labels)
        super().set_gauge(name, value, labels=labels)
        self._debug("gauge", name, value, labels)

    gauge = set_gauge

    def observe(
        self,
        name: str,
        value: float,
        *,
        labels: Labels | None = None,
    ) -> None:
        self._validate_labels(labels)
        super().observe(name, value, labels=labels)
        self._debug("observation", name, value, labels)

    histogram = observe


def metric_labels(**values: object) -> Mapping[str, str]:
    """Build a compact label mapping while omitting absent values."""

    return {name: str(value) for name, value in values.items() if value is not None}


__all__ = ["LogVistaMetrics", "metric_labels"]
