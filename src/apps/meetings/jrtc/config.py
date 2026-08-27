"""Validated Django-to-JRTC and Broka configuration.

Janus connectivity remains under ``JANUS_*`` settings.  Application event
transport selection and consumer/publisher tuning use ``JRTC_EVENT_*``.  This
keeps command connectivity separate from the authoritative event plane.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

from django.conf import settings as django_settings
from jrtc.conf import configure as configure_jrtc_settings
from jrtc.messaging import DEFAULT_PHYSICAL_ROUTE

BrokerEngine = Literal["memory", "local", "redis", "rabbitmq", "kafka"]
SUPPORTED_ENGINES = frozenset({"memory", "local", "redis", "rabbitmq", "kafka"})


@dataclass(frozen=True, slots=True)
class JrtcEventConfig:
    """Complete process-safe event publisher/consumer configuration."""

    enabled: bool
    engine: BrokerEngine
    physical_route: str
    publish_workers: int
    publish_queue_capacity: int
    publish_admission_timeout: float
    publish_timeout: float
    drain_timeout: float
    consumer_concurrency: int
    consumer_capacity: int
    consumer_group: str
    consumer_name: str
    engine_options: dict[str, object]
    outbox_poll_interval: float = 1.0
    outbox_retry_delay: float = 2.0
    outbox_lease_timeout: float = 30.0
    outbox_batch_size: int = 100


def _positive_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_number(name: str, value: object) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive number")
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{name} must be finite and greater than zero")
    return normalized


def configure_jrtc_core() -> None:
    """Install explicit, typed Janus connection settings into local JRTC."""

    configure_jrtc_settings(
        overrides={
            "JANUS_SESSION_URL": django_settings.JANUS_SESSION_URL,
            "JANUS_REQUEST_TIMEOUT": django_settings.JANUS_REQUEST_TIMEOUT,
            "JANUS_SESSION_POOL_SIZE": django_settings.JANUS_SESSION_POOL_SIZE,
            "JANUS_KEEPALIVE_INTERVAL": django_settings.JANUS_KEEPALIVE_INTERVAL,
            "JANUS_KEEPALIVE_FAILURES": django_settings.JANUS_KEEPALIVE_FAILURES,
            "JANUS_SHUTDOWN_TIMEOUT": django_settings.JANUS_SHUTDOWN_TIMEOUT,
            "JANUS_DETACH_CONCURRENCY": django_settings.JANUS_DETACH_CONCURRENCY,
            "JANUS_TOKEN": django_settings.JANUS_TOKEN,
            "JANUS_API_SECRET": django_settings.JANUS_API_SECRET,
        },
    )


def load_event_config(*, consumer_name: str | None = None) -> JrtcEventConfig:
    """Read and validate the configured backend without exposing credentials."""

    raw_engine = str(django_settings.JRTC_EVENT_BROKER_ENGINE).strip().lower()
    if raw_engine not in SUPPORTED_ENGINES:
        supported = ", ".join(sorted(SUPPORTED_ENGINES))
        raise ValueError(f"unsupported JRTC event broker {raw_engine!r}; choose {supported}")
    engine = raw_engine
    route = str(
        getattr(django_settings, "JRTC_EVENT_PHYSICAL_ROUTE", DEFAULT_PHYSICAL_ROUTE)
    ).strip()
    if not route or any(character.isspace() for character in route):
        raise ValueError("JRTC_EVENT_PHYSICAL_ROUTE must not be empty or contain whitespace")

    redis_group = str(django_settings.JRTC_REDIS_GROUP).strip()
    configured_name = consumer_name or django_settings.JRTC_REDIS_CONSUMER_NAME
    name = str(configured_name).strip()
    if not redis_group or not name:
        raise ValueError("JRTC event consumer group and name must be non-empty")

    logical_group = redis_group

    engine_options: dict[str, object]
    if engine == "redis":
        mode = str(django_settings.JRTC_REDIS_MODE).strip().lower()
        if mode not in {"streams", "pubsub"}:
            raise ValueError("JRTC_REDIS_MODE must be 'streams' or 'pubsub'")
        engine_options = {
            "url": django_settings.JRTC_REDIS_URL,
            "mode": mode,
            # Broka 0.0.2 does not forward SubscriptionOptions identities to
            # Redis Streams, so these must live on the engine itself.
            "group": redis_group,
            "consumer_name": name,
            "max_length": django_settings.JRTC_REDIS_MAX_LENGTH,
            "claim_idle_ms": django_settings.JRTC_REDIS_CLAIM_IDLE_MS,
            "claim_interval": django_settings.JRTC_REDIS_CLAIM_INTERVAL,
        }
    elif engine == "rabbitmq":
        logical_group = str(django_settings.JRTC_RABBITMQ_QUEUE).strip()
        if not logical_group:
            raise ValueError("JRTC_RABBITMQ_QUEUE must be non-empty")
        engine_options = {
            "url": django_settings.JRTC_RABBITMQ_URL,
            "exchange": django_settings.JRTC_RABBITMQ_EXCHANGE,
            "queue": django_settings.JRTC_RABBITMQ_QUEUE,
            "dead_letter_exchange": django_settings.JRTC_RABBITMQ_DLX or None,
            "durable": True,
            "auto_delete": False,
            "publisher_confirms": True,
            "mandatory": True,
        }
    elif engine == "kafka":
        logical_group = str(django_settings.JRTC_KAFKA_GROUP_ID).strip()
        if not logical_group:
            raise ValueError("JRTC_KAFKA_GROUP_ID must be non-empty")
        engine_options = {
            "bootstrap_servers": list(django_settings.JRTC_KAFKA_BOOTSTRAP_SERVERS),
            "group_id": django_settings.JRTC_KAFKA_GROUP_ID,
            "group_instance_id": name,
            "security_protocol": django_settings.JRTC_KAFKA_SECURITY_PROTOCOL,
        }
        mechanism = django_settings.JRTC_KAFKA_SASL_MECHANISM
        username = django_settings.JRTC_KAFKA_USERNAME
        password = django_settings.JRTC_KAFKA_PASSWORD
        if mechanism:
            engine_options["sasl_mechanism"] = mechanism
        if username:
            engine_options["sasl_plain_username"] = username
        if password:
            engine_options["sasl_plain_password"] = password
    else:
        engine_options = {}

    return JrtcEventConfig(
        enabled=bool(django_settings.JRTC_EVENTS_ENABLED),
        engine=engine,  # type: ignore[arg-type]
        physical_route=route,
        publish_workers=_positive_int(
            "JRTC_EVENT_PUBLISH_WORKERS", django_settings.JRTC_EVENT_PUBLISH_WORKERS
        ),
        publish_queue_capacity=_positive_int(
            "JRTC_EVENT_PUBLISH_QUEUE_CAPACITY",
            django_settings.JRTC_EVENT_PUBLISH_QUEUE_CAPACITY,
        ),
        publish_admission_timeout=_positive_number(
            "JRTC_EVENT_PUBLISH_ADMISSION_TIMEOUT",
            django_settings.JRTC_EVENT_PUBLISH_ADMISSION_TIMEOUT,
        ),
        publish_timeout=_positive_number(
            "JRTC_EVENT_PUBLISH_TIMEOUT", django_settings.JRTC_EVENT_PUBLISH_TIMEOUT
        ),
        drain_timeout=_positive_number(
            "JRTC_EVENT_DRAIN_TIMEOUT", django_settings.JRTC_EVENT_DRAIN_TIMEOUT
        ),
        consumer_concurrency=_positive_int(
            "JRTC_EVENT_CONSUMER_CONCURRENCY",
            django_settings.JRTC_EVENT_CONSUMER_CONCURRENCY,
        ),
        consumer_capacity=_positive_int(
            "JRTC_EVENT_CONSUMER_CAPACITY", django_settings.JRTC_EVENT_CONSUMER_CAPACITY
        ),
        consumer_group=logical_group,
        consumer_name=name,
        engine_options={key: value for key, value in engine_options.items() if value is not None},
        outbox_poll_interval=_positive_number(
            "JRTC_EVENT_OUTBOX_POLL_INTERVAL",
            django_settings.JRTC_EVENT_OUTBOX_POLL_INTERVAL,
        ),
        outbox_retry_delay=_positive_number(
            "JRTC_EVENT_OUTBOX_RETRY_DELAY",
            django_settings.JRTC_EVENT_OUTBOX_RETRY_DELAY,
        ),
        outbox_lease_timeout=_positive_number(
            "JRTC_EVENT_OUTBOX_LEASE_TIMEOUT",
            django_settings.JRTC_EVENT_OUTBOX_LEASE_TIMEOUT,
        ),
        outbox_batch_size=_positive_int(
            "JRTC_EVENT_OUTBOX_BATCH_SIZE",
            django_settings.JRTC_EVENT_OUTBOX_BATCH_SIZE,
        ),
    )


__all__ = [
    "BrokerEngine",
    "JrtcEventConfig",
    "SUPPORTED_ENGINES",
    "configure_jrtc_core",
    "load_event_config",
]
