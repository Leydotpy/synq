"""Construction of Synq-owned Broka publishers and consumers.

The publisher runtime and dedicated consumer process each own a separate
broker connection.  Both map JRTC logical event types onto one configured
physical destination, normally the Redis Stream ``janus.events``.
"""

from __future__ import annotations

from broka import Broker
from jrtc.messaging import JanusEventPublisher, create_broker

from apps.meetings.jrtc.config import JrtcEventConfig, load_event_config
from apps.meetings.jrtc.errors import JrtcBrokerUnavailable


def build_event_broker(config: JrtcEventConfig | None = None) -> Broker:
    """Return an unstarted broker for the configured physical destination."""

    selected = config or load_event_config()
    try:
        return create_broker(
            engine=selected.engine,
            physical_route=selected.physical_route,
            engine_options=selected.engine_options,
        )
    except Exception as exc:
        raise JrtcBrokerUnavailable("Unable to construct the JRTC event broker.") from exc


def build_event_publisher(
    config: JrtcEventConfig | None = None,
) -> JanusEventPublisher | None:
    """Build an unstarted bounded publisher, or ``None`` when explicitly disabled."""

    selected = config or load_event_config()
    if not selected.enabled:
        return None
    broker = build_event_broker(selected)
    return JanusEventPublisher(
        broker,
        physical_route=selected.physical_route,
        workers=selected.publish_workers,
        queue_capacity=selected.publish_queue_capacity,
        admission_timeout=selected.publish_admission_timeout,
        publish_timeout=selected.publish_timeout,
        owns_broker=True,
    )


__all__ = ["build_event_broker", "build_event_publisher"]
