"""Hardened Kafka engine for the Broka 0.0.2 service-provider contract.

Broka's bundled Kafka adapter intentionally exposes only a small subset of
``aiokafka``. This adapter keeps Broka's acknowledgement implementation while
providing an explicit, validated client-option surface suitable for secured
deployments. No arbitrary ``aiokafka`` keyword arguments are accepted.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import math
import os
import re
import ssl
from collections.abc import Iterator, Mapping
from contextlib import suppress
from importlib import import_module
from types import MappingProxyType
from typing import Any, Final

from broka.engines import kafka as _broka_kafka
from broka.engines.base import (
    EngineConsumer,
    EngineDeliveryCallback,
    EngineHealth,
    EnginePublishContext,
    EnginePublishResult,
    EngineSubscription,
)
from broka.engines.kafka import KafkaEngine as _BrokaKafkaEngine

SUPPORTED_KAFKA_CONFIG_KEYS: Final = frozenset(
    {
        "api_version",
        "auto_offset_reset",
        "bootstrap_servers",
        "check_crcs",
        "client_id",
        "client_rack",
        "compression_type",
        "connections_max_idle_ms",
        "consumer_timeout_ms",
        "enable_idempotence",
        "exclude_internal_topics",
        "fetch_max_bytes",
        "fetch_max_wait_ms",
        "fetch_min_bytes",
        "group_id",
        "group_instance_id",
        "heartbeat_interval_ms",
        "isolation_level",
        "linger_ms",
        "max_batch_size",
        "max_partition_fetch_bytes",
        "max_poll_interval_ms",
        "max_poll_records",
        "max_request_size",
        "metadata_max_age_ms",
        "publish_timeout",
        "rebalance_timeout_ms",
        "request_timeout_ms",
        "retry_backoff_ms",
        "sasl_mechanism",
        "sasl_plain_password",
        "sasl_plain_username",
        "security_protocol",
        "session_timeout_ms",
        "ssl_ca_file",
        "ssl_cert_file",
        "ssl_key_file",
        "ssl_key_password",
    }
)

DEFAULT_KAFKA_PUBLISH_TIMEOUT: Final = 30.0

_REDACTED_CONFIG_KEYS: Final = frozenset(
    {
        "bootstrap_servers",
        "sasl_plain_password",
        "sasl_plain_username",
        "ssl_key_password",
    }
)
_SECURITY_PROTOCOLS: Final = frozenset({"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"})
_SSL_PROTOCOLS: Final = frozenset({"SSL", "SASL_SSL"})
_SASL_PROTOCOLS: Final = frozenset({"SASL_PLAINTEXT", "SASL_SSL"})
_SASL_MECHANISMS: Final = frozenset({"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"})
_COMPRESSION_TYPES: Final = frozenset({"gzip", "snappy", "lz4", "zstd"})

_BOOLEAN_OPTIONS: Final = frozenset({"check_crcs", "enable_idempotence", "exclude_internal_topics"})
_NON_NEGATIVE_INTEGER_OPTIONS: Final = frozenset(
    {"connections_max_idle_ms", "consumer_timeout_ms", "fetch_min_bytes", "linger_ms"}
)
_POSITIVE_INTEGER_OPTIONS: Final = frozenset(
    {
        "fetch_max_bytes",
        "fetch_max_wait_ms",
        "heartbeat_interval_ms",
        "max_batch_size",
        "max_partition_fetch_bytes",
        "max_poll_interval_ms",
        "max_poll_records",
        "max_request_size",
        "metadata_max_age_ms",
        "rebalance_timeout_ms",
        "request_timeout_ms",
        "retry_backoff_ms",
        "session_timeout_ms",
    }
)
_OPTIONAL_STRING_OPTIONS: Final = frozenset(
    {"client_id", "client_rack", "group_id", "group_instance_id"}
)

_SHARED_CLIENT_OPTIONS: Final = (
    "api_version",
    "client_id",
    "connections_max_idle_ms",
    "metadata_max_age_ms",
    "request_timeout_ms",
    "retry_backoff_ms",
)
_PRODUCER_OPTIONS: Final = (
    "compression_type",
    "linger_ms",
    "max_batch_size",
    "max_request_size",
)
_CONSUMER_OPTIONS: Final = (
    "check_crcs",
    "client_rack",
    "consumer_timeout_ms",
    "exclude_internal_topics",
    "fetch_max_bytes",
    "fetch_max_wait_ms",
    "fetch_min_bytes",
    "heartbeat_interval_ms",
    "isolation_level",
    "max_partition_fetch_bytes",
    "max_poll_interval_ms",
    "rebalance_timeout_ms",
    "session_timeout_ms",
)

_DNS_LABEL = re.compile(r"[A-Za-z0-9_](?:[A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?\Z")


def _validate_port(value: str) -> None:
    if not value or not value.isascii() or not value.isdigit():
        raise ValueError("Kafka bootstrap server ports must be decimal integers")
    port = int(value)
    if not 1 <= port <= 65_535:
        raise ValueError("Kafka bootstrap server ports must be between 1 and 65535")


def _validate_hostname(value: str) -> None:
    try:
        ipaddress.ip_address(value)
        return
    except ValueError:
        pass
    try:
        ascii_value = value.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise ValueError("Kafka bootstrap servers must contain valid host names") from exc
    candidate = ascii_value[:-1] if ascii_value.endswith(".") else ascii_value
    if (
        not candidate
        or len(ascii_value) > 253
        or any(not _DNS_LABEL.fullmatch(label) for label in candidate.split("."))
    ):
        raise ValueError("Kafka bootstrap servers must contain valid host names")


def _validate_bootstrap_server(value: str) -> str:
    if (
        not value
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
        or any(marker in value for marker in ("@", "/", "?", "#", "\\"))
    ):
        raise ValueError("Kafka bootstrap servers must use host[:port] syntax without credentials")
    if value.startswith("["):
        closing = value.find("]")
        if closing < 0:
            raise ValueError("Kafka bracketed bootstrap servers must contain a valid IPv6 address")
        host = value[1:closing]
        suffix = value[closing + 1 :]
        try:
            ipaddress.IPv6Address(host)
        except ValueError as exc:
            raise ValueError(
                "Kafka bracketed bootstrap servers must contain a valid IPv6 address"
            ) from exc
        if suffix:
            if not suffix.startswith(":"):
                raise ValueError("Kafka bootstrap servers must use host[:port] syntax")
            _validate_port(suffix[1:])
        return value
    if value.count(":") > 1:
        try:
            ipaddress.IPv6Address(value)
        except ValueError as exc:
            raise ValueError(
                "Kafka IPv6 bootstrap servers with ports must use bracket notation"
            ) from exc
        return value
    host, separator, port = value.partition(":")
    _validate_hostname(host)
    if separator:
        _validate_port(port)
    return value


def _validate_bootstrap_servers(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        servers: object = value.split(",")
    else:
        servers = value
    if not isinstance(servers, (list, tuple)) or not servers:
        raise ValueError("Kafka bootstrap_servers must contain at least one server")
    if not all(isinstance(server, str) for server in servers):
        raise TypeError("Kafka bootstrap_servers must contain only strings")
    return tuple(_validate_bootstrap_server(server) for server in servers)


def _optional_string(config: Mapping[str, object], name: str) -> str | None:
    value = config.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Kafka {name} must be a non-empty string or null")
    return value


def _path(config: Mapping[str, object], name: str) -> str | None:
    value = config.get(name)
    if value is None:
        return None
    if not isinstance(value, (str, os.PathLike)):
        raise TypeError(f"Kafka {name} must be a filesystem path or null")
    normalized = os.fspath(value)
    if not normalized:
        raise ValueError(f"Kafka {name} must not be empty")
    return normalized


def _normalize_config(config: Mapping[str, object] | None) -> dict[str, object]:
    values = dict(config or {})
    if not all(isinstance(name, str) for name in values):
        raise TypeError("Kafka configuration keys must be strings")
    unsupported = set(values) - SUPPORTED_KAFKA_CONFIG_KEYS
    if unsupported:
        names = ", ".join(sorted(unsupported))
        raise ValueError(f"Unsupported Kafka engine configuration key(s): {names}")

    values["bootstrap_servers"] = _validate_bootstrap_servers(
        values.get("bootstrap_servers", "localhost:9092")
    )
    for name in _BOOLEAN_OPTIONS:
        if name in values and not isinstance(values[name], bool):
            raise TypeError(f"Kafka {name} must be a boolean")
    for name in _NON_NEGATIVE_INTEGER_OPTIONS | _POSITIVE_INTEGER_OPTIONS:
        if name not in values or values[name] is None:
            continue
        value = values[name]
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"Kafka {name} must be an integer")
        minimum = 1 if name in _POSITIVE_INTEGER_OPTIONS else 0
        if value < minimum:
            raise ValueError(f"Kafka {name} must be at least {minimum}")
    for name in _OPTIONAL_STRING_OPTIONS:
        if name in values:
            values[name] = _optional_string(values, name)

    publish_timeout = values.get("publish_timeout", DEFAULT_KAFKA_PUBLISH_TIMEOUT)
    if (
        isinstance(publish_timeout, bool)
        or not isinstance(publish_timeout, (int, float))
        or not math.isfinite(publish_timeout)
        or publish_timeout <= 0
    ):
        raise ValueError("Kafka publish_timeout must be finite and greater than zero")
    values["publish_timeout"] = float(publish_timeout)

    protocol = values.get("security_protocol", "PLAINTEXT")
    if not isinstance(protocol, str) or protocol.upper() not in _SECURITY_PROTOCOLS:
        raise ValueError("Kafka security_protocol is invalid")
    protocol = protocol.upper()
    values["security_protocol"] = protocol

    mechanism_was_configured = "sasl_mechanism" in values
    mechanism = values.get("sasl_mechanism", "PLAIN")
    if not isinstance(mechanism, str) or mechanism.upper() not in _SASL_MECHANISMS:
        raise ValueError("Kafka sasl_mechanism is invalid")
    values["sasl_mechanism"] = mechanism.upper()

    username = _optional_string(values, "sasl_plain_username")
    password = _optional_string(values, "sasl_plain_password")
    if protocol in _SASL_PROTOCOLS and (username is None or password is None):
        raise ValueError("Kafka SASL username and password are required for SASL security")
    if protocol not in _SASL_PROTOCOLS and (
        mechanism_was_configured or username is not None or password is not None
    ):
        raise ValueError("Kafka SASL options require a SASL security protocol")

    ca_file = _path(values, "ssl_ca_file")
    cert_file = _path(values, "ssl_cert_file")
    key_file = _path(values, "ssl_key_file")
    key_password = _optional_string(values, "ssl_key_password")
    if (cert_file is None) != (key_file is None):
        raise ValueError("Kafka TLS certificate and key must be configured together")
    if key_password is not None and cert_file is None:
        raise ValueError("Kafka ssl_key_password requires a TLS certificate and key")
    if protocol not in _SSL_PROTOCOLS and any((ca_file, cert_file, key_file, key_password)):
        raise ValueError("Kafka TLS options require an SSL security protocol")

    compression = values.get("compression_type")
    if compression is not None and (
        not isinstance(compression, str) or compression.lower() not in _COMPRESSION_TYPES
    ):
        raise ValueError("Kafka compression_type is invalid")
    if isinstance(compression, str):
        values["compression_type"] = compression.lower()

    api_version = values.get("api_version")
    valid_version_tuple = (
        isinstance(api_version, tuple)
        and len(api_version) in {2, 3}
        and all(isinstance(part, int) and not isinstance(part, bool) for part in api_version)
    )
    if api_version is not None and not isinstance(api_version, str) and not valid_version_tuple:
        raise TypeError("Kafka api_version must be a string, version tuple, or null")

    offset = values.get("auto_offset_reset", "earliest")
    if offset not in {"earliest", "latest", "none"}:
        raise ValueError("Kafka auto_offset_reset is invalid")
    values["auto_offset_reset"] = offset

    isolation = values.get("isolation_level", "read_uncommitted")
    if isolation not in {"read_uncommitted", "read_committed"}:
        raise ValueError("Kafka isolation_level is invalid")
    values["isolation_level"] = isolation

    idempotent = values.get("enable_idempotence", True)
    values["enable_idempotence"] = idempotent
    return values


def _redacted_config(config: Mapping[str, object]) -> MappingProxyType[str, object]:
    return MappingProxyType(
        {
            name: "<redacted>" if name in _REDACTED_CONFIG_KEYS and value is not None else value
            for name, value in config.items()
        }
    )


class _ClientConfig(Mapping[str, object]):
    """Retain client values while keeping accidental representations log-safe."""

    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, object]) -> None:
        self._values = dict(values)

    def __getitem__(self, name: str) -> object:
        return self._values[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return repr(_redacted_config(self._values))


class KafkaEngine(_BrokaKafkaEngine):
    """Broka Kafka engine with bounded, allowlisted secured client configuration."""

    def __init__(self, config: Mapping[str, object] | None = None) -> None:
        normalized = _normalize_config(config)
        super().__init__(normalized)
        self._client_config = _ClientConfig(normalized)
        # Broka exposes ``engine.config`` in diagnostics. Keep that public view
        # useful without retaining credentials in its representation.
        self.config = _redacted_config(normalized)
        self.bootstrap_servers = normalized["bootstrap_servers"]
        publish_timeout = normalized["publish_timeout"]
        assert isinstance(publish_timeout, (int, float)) and not isinstance(publish_timeout, bool)
        self.publish_timeout = float(publish_timeout)
        self._ssl_context: ssl.SSLContext | None = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(config={dict(self.config)!r})"

    @staticmethod
    def _load_aiokafka() -> Any:
        return import_module("aiokafka")

    def _build_ssl_context(self) -> ssl.SSLContext | None:
        if self._client_config["security_protocol"] not in _SSL_PROTOCOLS:
            return None
        ca_file = self._client_config.get("ssl_ca_file")
        context = ssl.create_default_context(cafile=str(ca_file) if ca_file else None)
        cert_file = self._client_config.get("ssl_cert_file")
        if cert_file:
            password = self._client_config.get("ssl_key_password")
            assert password is None or isinstance(password, str)
            context.load_cert_chain(
                certfile=str(cert_file),
                keyfile=str(self._client_config["ssl_key_file"]),
                password=password,
            )
        return context

    def _security_options(self) -> dict[str, object]:
        protocol = str(self._client_config["security_protocol"])
        options: dict[str, object] = {"security_protocol": protocol}
        if protocol in _SSL_PROTOCOLS:
            options["ssl_context"] = self._ssl_context
        if protocol in _SASL_PROTOCOLS:
            options.update(
                sasl_mechanism=self._client_config["sasl_mechanism"],
                sasl_plain_username=self._client_config["sasl_plain_username"],
                sasl_plain_password=self._client_config["sasl_plain_password"],
            )
        return options

    def _selected_options(self, names: tuple[str, ...]) -> dict[str, object]:
        return {
            name: self._client_config[name]
            for name in names
            if name in self._client_config and self._client_config[name] is not None
        }

    def _producer_client_options(self) -> dict[str, object]:
        options: dict[str, object] = {
            "acks": "all",
            "bootstrap_servers": self.bootstrap_servers,
            "enable_idempotence": self._client_config["enable_idempotence"],
        }
        options.update(self._selected_options(_SHARED_CLIENT_OPTIONS))
        options.update(self._selected_options(_PRODUCER_OPTIONS))
        options.update(self._security_options())
        return options

    def _group_instance_id(self, subscription: EngineSubscription) -> str | None:
        configured = self._client_config.get("group_instance_id")
        if not isinstance(configured, str):
            return None
        suffix = hashlib.sha256(subscription.id.encode("utf-8")).hexdigest()[:16]
        return f"{configured}:{suffix}"

    def _consumer_client_options(self, subscription: EngineSubscription) -> dict[str, object]:
        configured_max = self._client_config.get("max_poll_records")
        max_poll_records = (
            min(subscription.capacity, configured_max)
            if isinstance(configured_max, int)
            else subscription.capacity
        )
        options: dict[str, object] = {
            "auto_offset_reset": self._client_config["auto_offset_reset"],
            "bootstrap_servers": self.bootstrap_servers,
            "enable_auto_commit": False,
            "group_id": self._client_config.get("group_id") or f"pyev-{subscription.id}",
            "max_poll_records": max_poll_records,
        }
        options.update(self._selected_options(_SHARED_CLIENT_OPTIONS))
        options.update(self._selected_options(_CONSUMER_OPTIONS))
        group_instance_id = self._group_instance_id(subscription)
        if group_instance_id is not None:
            options["group_instance_id"] = group_instance_id
        options.update(self._security_options())
        return options

    async def connect(self) -> None:
        if self._producer is not None:
            return
        if not self.is_available(self._client_config):
            raise RuntimeError("aiokafka is unavailable; install a compatible Kafka client")
        aiokafka = self._load_aiokafka()
        self._ssl_context = self._build_ssl_context()
        producer: Any | None = None
        try:
            producer = aiokafka.AIOKafkaProducer(**self._producer_client_options())
            await producer.start()
        except BaseException:
            if producer is not None:
                with suppress(Exception):
                    await producer.stop()
            self._ssl_context = None
            raise
        self._aiokafka = aiokafka
        self._producer = producer

    async def disconnect(self) -> None:
        try:
            await super().disconnect()
        finally:
            self._ssl_context = None
            self._aiokafka = None

    async def publish(
        self,
        destination: str,
        payload: bytes,
        context: EnginePublishContext,
    ) -> EnginePublishResult:
        producer = self._producer
        if producer is None:
            raise RuntimeError("Kafka engine is not connected")
        requested_timeout = context.timeout
        if requested_timeout is not None and (
            isinstance(requested_timeout, bool)
            or not isinstance(requested_timeout, (int, float))
            or not math.isfinite(requested_timeout)
            or requested_timeout <= 0
        ):
            raise ValueError("Kafka publish timeout must be finite and greater than zero")
        timeout = self.publish_timeout if requested_timeout is None else float(requested_timeout)
        routing_key = context.partition_key or context.ordering_key
        if routing_key is not None and not isinstance(routing_key, str):
            raise TypeError("Kafka partition and ordering keys must be strings or null")
        key = routing_key.encode("utf-8") if routing_key is not None else None
        if not all(
            isinstance(name, str) and isinstance(value, str)
            for name, value in context.headers.items()
        ):
            raise TypeError("Kafka headers must contain string names and values")
        headers = [(name, value.encode("utf-8")) for name, value in context.headers.items()]

        async def send() -> Any:
            return await producer.send_and_wait(
                destination,
                payload,
                key=key,
                headers=headers,
            )

        async with asyncio.timeout(timeout):
            metadata = await send()
        return EnginePublishResult(
            transport_id=f"{metadata.topic}:{metadata.partition}:{metadata.offset}"
        )

    async def create_consumer(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> EngineConsumer:
        if self._producer is None or self._aiokafka is None:
            raise RuntimeError("Kafka engine is not connected")
        existing = self._consumers.get(subscription.id)
        if existing is not None and not existing.closed:
            raise ValueError(f"consumer {subscription.id!r} is already registered")
        self._consumers.pop(subscription.id, None)
        consumer = self._aiokafka.AIOKafkaConsumer(**self._consumer_client_options(subscription))
        if "*" in subscription.pattern:
            consumer.subscribe(pattern=_broka_kafka._topic_pattern(subscription.pattern))
        else:
            consumer.subscribe(topics=[subscription.destination])
        # Broka 0.0.2 does not export its acknowledgement-aware consumer handle.
        # The dependency is pinned, and this is the only private compatibility seam.
        handle = _broka_kafka._KafkaConsumer(subscription.id, consumer, callback)
        await handle.start()
        self._consumers[subscription.id] = handle
        return handle

    async def healthcheck(self) -> EngineHealth:
        producer = self._producer
        producer_error = self._producer_error(producer)
        connected = producer is not None and producer_error is None
        errors = {
            consumer.id: consumer.last_error.partition(":")[0]
            for consumer in self._consumers.values()
            if consumer.last_error is not None
        }
        return EngineHealth(
            self.name,
            connected=connected,
            healthy=connected and not errors,
            # This is a non-blocking local state probe, not a Kafka round trip.
            latency_ms=None,
            details=MappingProxyType(
                {
                    "active_consumers": sum(
                        not consumer.closed for consumer in self._consumers.values()
                    ),
                    "errors": errors,
                    **({"producer_error": producer_error} if producer_error is not None else {}),
                }
            ),
        )

    @staticmethod
    def _producer_error(producer: object | None) -> str | None:
        """Return a non-secret terminal producer state, if one is observable."""

        if producer is None:
            return "not-connected"
        if getattr(producer, "_closed", False) is True:
            return "closed"
        sender = getattr(producer, "_sender", None)
        # aiokafka 0.14 exposes ``sender_task`` as a property over
        # ``_sender_task``. Keep the fallback for compatible patch releases.
        task = getattr(sender, "sender_task", None)
        if task is None:
            task = getattr(sender, "_sender_task", None)
        if task is None:
            return None
        done = getattr(task, "done", None)
        if not callable(done) or not done():
            return None
        cancelled = getattr(task, "cancelled", None)
        if callable(cancelled) and cancelled():
            return "sender-cancelled"
        exception = getattr(task, "exception", None)
        if not callable(exception):
            return "sender-stopped"
        try:
            error = exception()
        except BaseException as caught:
            return type(caught).__name__
        return "sender-stopped" if error is None else type(error).__name__


__all__ = ["DEFAULT_KAFKA_PUBLISH_TIMEOUT", "SUPPORTED_KAFKA_CONFIG_KEYS", "KafkaEngine"]
