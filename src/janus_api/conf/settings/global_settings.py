"""Secure environment-backed defaults for the Janus client core."""

from __future__ import annotations

import json
import math
import os
from typing import Any


def _boolean(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


def _integer(name: str, default: int, *, minimum: int = 0) -> int:
    value = int(os.getenv(name, str(default)))
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def _number(name: str, default: float, *, minimum: float = 0.0) -> float:
    value = float(os.getenv(name, str(default)))
    if not math.isfinite(value) or value < minimum:
        raise ValueError(f"{name} must be finite and at least {minimum}")
    return value


def _optional(name: str) -> str | None:
    value = os.getenv(name)
    return value if value else None


def _json_object(name: str) -> dict[str, Any]:
    raw = os.getenv(name, "{}")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must contain valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{name} must contain a JSON object")
    return value


DEBUG = _boolean("JANUS_DEBUG", False)

# Client runtime
JANUS_SESSION_URL = os.getenv("JANUS_SESSION_URL", "ws://localhost:8188/janus")
JANUS_REQUEST_TIMEOUT = _number("JANUS_REQUEST_TIMEOUT", 15.0, minimum=0.001)
JANUS_SESSION_POOL_SIZE = _integer("JANUS_SESSION_POOL_SIZE", 1, minimum=1)
JANUS_KEEPALIVE_INTERVAL = _number("JANUS_KEEPALIVE_INTERVAL", 25.0, minimum=0.001)
JANUS_KEEPALIVE_FAILURES = _integer("JANUS_KEEPALIVE_FAILURES", 3, minimum=1)
JANUS_SHUTDOWN_TIMEOUT = _number("JANUS_SHUTDOWN_TIMEOUT", 10.0, minimum=0.001)
JANUS_DETACH_CONCURRENCY = _integer("JANUS_DETACH_CONCURRENCY", 16, minimum=1)
JANUS_TOKEN = _optional("JANUS_TOKEN")
JANUS_API_SECRET = _optional("JANUS_API_SECRET")

# Transport-originated WebRTC events.  All logical ``janus.*`` event types are
# mapped to one portable physical destination so Redis Streams, RabbitMQ and
# Kafka subscribers share the same contract.
JANUS_BROKER_ENGINE = os.getenv("JANUS_BROKER_ENGINE", "memory").strip().lower()
if JANUS_BROKER_ENGINE not in {"memory", "local", "redis", "rabbitmq", "kafka"}:
    raise ValueError("JANUS_BROKER_ENGINE is invalid")
JANUS_BROKER_ROUTE = os.getenv("JANUS_BROKER_ROUTE", "janus.events").strip()
if not JANUS_BROKER_ROUTE or any(character.isspace() for character in JANUS_BROKER_ROUTE):
    raise ValueError("JANUS_BROKER_ROUTE must be a non-empty route without whitespace")
JANUS_BROKER_ENGINE_OPTIONS = _json_object("JANUS_BROKER_ENGINE_OPTIONS")
JANUS_BROKER_OPTIONS = _json_object("JANUS_BROKER_OPTIONS")
JANUS_BROKER_PUBLISH_WORKERS = _integer("JANUS_BROKER_PUBLISH_WORKERS", 4, minimum=1)
JANUS_BROKER_QUEUE_CAPACITY = _integer("JANUS_BROKER_QUEUE_CAPACITY", 4096, minimum=1)
JANUS_BROKER_ADMISSION_TIMEOUT = _number("JANUS_BROKER_ADMISSION_TIMEOUT", 0.05, minimum=0.001)
JANUS_BROKER_PUBLISH_TIMEOUT = _number("JANUS_BROKER_PUBLISH_TIMEOUT", 5.0, minimum=0.001)
JANUS_BROKER_DRAIN_TIMEOUT = _number("JANUS_BROKER_DRAIN_TIMEOUT", 10.0, minimum=0.001)
