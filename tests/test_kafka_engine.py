from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from broka.engines.base import Availability, EnginePublishContext, EngineSubscription

from janus_api.messaging.engines import KafkaEngine
from janus_api.messaging.engines import kafka as kafka_module


class _FakeSSLContext:
    def __init__(self, ca_file: str | None) -> None:
        self.ca_file = ca_file
        self.cert_chain: tuple[str, str, object] | None = None

    def load_cert_chain(
        self,
        certfile: str,
        keyfile: str,
        password: object = None,
    ) -> None:
        self.cert_chain = (certfile, keyfile, password)


class _FakeProducer:
    def __init__(self, options: dict[str, object]) -> None:
        self.options = options
        self.started = False
        self.stopped = False
        self._closed = False
        self._sender = SimpleNamespace(sender_task=None)
        self.block = False
        self.sent: list[tuple[str, bytes, bytes | None, list[tuple[str, bytes]]]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True
        self._closed = True

    async def send_and_wait(
        self,
        destination: str,
        payload: bytes,
        *,
        key: bytes | None,
        headers: list[tuple[str, bytes]],
    ) -> Any:
        self.sent.append((destination, payload, key, headers))
        if self.block:
            await asyncio.Event().wait()
        return SimpleNamespace(topic=destination, partition=3, offset=42)


class _FakeConsumer:
    def __init__(self, options: dict[str, object]) -> None:
        self.options = options
        self.started = False
        self.stopped = False
        self.subscription: dict[str, object] = {}

    def subscribe(self, **options: object) -> None:
        self.subscription = options

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def assignment(self) -> tuple[object, ...]:
        return ()

    def pause(self, *_partitions: object) -> None:
        return None

    def resume(self, *_partitions: object) -> None:
        return None

    def __aiter__(self) -> _FakeConsumer:
        return self

    async def __anext__(self) -> Any:
        await asyncio.Event().wait()
        raise StopAsyncIteration


class _FakeAiokafka:
    def __init__(self) -> None:
        self.producers: list[_FakeProducer] = []
        self.consumers: list[_FakeConsumer] = []

    def AIOKafkaProducer(self, **options: object) -> _FakeProducer:
        producer = _FakeProducer(dict(options))
        self.producers.append(producer)
        return producer

    def AIOKafkaConsumer(self, **options: object) -> _FakeConsumer:
        consumer = _FakeConsumer(dict(options))
        self.consumers.append(consumer)
        return consumer


def _install_fake_client(monkeypatch: pytest.MonkeyPatch) -> _FakeAiokafka:
    client = _FakeAiokafka()
    monkeypatch.setattr(
        KafkaEngine,
        "is_available",
        classmethod(lambda cls, config=None: Availability(True)),
    )
    monkeypatch.setattr(KafkaEngine, "_load_aiokafka", staticmethod(lambda: client))
    return client


async def test_secured_client_options_are_allowlisted_redacted_and_shared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _install_fake_client(monkeypatch)
    ssl_context = _FakeSSLContext("ca.pem")
    monkeypatch.setattr(
        kafka_module.ssl,
        "create_default_context",
        lambda *, cafile=None: ssl_context,
    )
    password = "not-for-diagnostics"
    engine = KafkaEngine(
        {
            "bootstrap_servers": ["kafka-a:9093", "kafka-b:9093"],
            "client_id": "janus-ops",
            "compression_type": "gzip",
            "connections_max_idle_ms": 120_000,
            "enable_idempotence": True,
            "group_id": "janus-monitor-v1",
            "heartbeat_interval_ms": 3_000,
            "max_poll_records": 200,
            "metadata_max_age_ms": 60_000,
            "request_timeout_ms": 15_000,
            "retry_backoff_ms": 250,
            "sasl_mechanism": "SCRAM-SHA-512",
            "sasl_plain_password": password,
            "sasl_plain_username": "janus-service",
            "security_protocol": "SASL_SSL",
            "session_timeout_ms": 10_000,
            "ssl_ca_file": "ca.pem",
            "ssl_cert_file": "client.pem",
            "ssl_key_file": "client.key",
            "ssl_key_password": "key-password",
            "publish_timeout": 12.5,
        }
    )

    assert password not in repr(engine)
    assert "kafka-a:9093" not in repr(engine)
    assert password not in repr(engine.config)
    assert password not in repr(engine._client_config)
    assert engine.config["sasl_plain_password"] == "<redacted>"
    assert engine.config["bootstrap_servers"] == "<redacted>"
    await engine.connect()

    producer = client.producers[0]
    assert producer.started
    assert producer.options["bootstrap_servers"] == ("kafka-a:9093", "kafka-b:9093")
    assert producer.options["acks"] == "all"
    assert producer.options["enable_idempotence"] is True
    assert producer.options["client_id"] == "janus-ops"
    assert producer.options["compression_type"] == "gzip"
    assert producer.options["request_timeout_ms"] == 15_000
    assert producer.options["retry_backoff_ms"] == 250
    assert "publish_timeout" not in producer.options
    assert producer.options["security_protocol"] == "SASL_SSL"
    assert producer.options["ssl_context"] is ssl_context
    assert producer.options["sasl_plain_password"] == password
    assert ssl_context.ca_file == "ca.pem"
    assert ssl_context.cert_chain == ("client.pem", "client.key", "key-password")

    async def callback(_message: object) -> None:
        return None

    handle = await engine.create_consumer(
        EngineSubscription(
            id="monitor-1",
            pattern="janus.events",
            destination="janus.events",
            capacity=50,
        ),
        callback,
    )
    consumer = client.consumers[0]
    assert consumer.started
    assert consumer.options["enable_auto_commit"] is False
    assert consumer.options["group_id"] == "janus-monitor-v1"
    assert consumer.options["max_poll_records"] == 50
    assert consumer.options["client_id"] == "janus-ops"
    assert consumer.options["request_timeout_ms"] == 15_000
    assert consumer.options["ssl_context"] is ssl_context
    assert consumer.subscription == {"topics": ["janus.events"]}
    assert password not in repr((await engine.healthcheck()).details)

    await handle.close()
    await engine.disconnect()
    assert consumer.stopped
    assert producer.stopped


@pytest.mark.parametrize(
    ("config", "message"),
    [
        ({"ssl_cert_file": "client.pem"}, "certificate and key"),
        (
            {"security_protocol": "SASL_SSL", "sasl_plain_username": "user"},
            "username and password",
        ),
        (
            {"security_protocol": "PLAINTEXT", "ssl_ca_file": "ca.pem"},
            "require an SSL security protocol",
        ),
        (
            {"security_protocol": "PLAINTEXT", "sasl_mechanism": "SCRAM-SHA-256"},
            "require a SASL security protocol",
        ),
        ({"enable_idempotence": "yes"}, "must be a boolean"),
        ({"api_version": object()}, "must be a string, version tuple, or null"),
        ({"publish_timeout": 0}, "finite and greater than zero"),
        ({"publish_timeout": float("inf")}, "finite and greater than zero"),
        ({"transactional_id": "janus-events"}, "Unsupported Kafka engine"),
        ({"transaction_timeout_ms": 30_000}, "Unsupported Kafka engine"),
        ({"unknown_password": "do-not-log"}, "Unsupported Kafka engine"),
    ],
)
def test_invalid_or_unallowlisted_configuration_fails_closed(
    config: dict[str, object],
    message: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=message) as error:
        KafkaEngine(config)
    assert "do-not-log" not in str(error.value)


@pytest.mark.parametrize(
    "bootstrap_servers",
    [
        "kafka://alice:do-not-log@broker:9092",
        "alice:do-not-log@broker:9092",
        "broker:0",
        "broker:65536",
        "broker:not-a-port",
        "bad host:9092",
        "[not-ipv6]:9092",
        ["broker:9092", ""],
    ],
)
def test_bootstrap_servers_reject_urls_credentials_and_malformed_addresses(
    bootstrap_servers: object,
) -> None:
    with pytest.raises((TypeError, ValueError)) as error:
        KafkaEngine({"bootstrap_servers": bootstrap_servers})
    assert "do-not-log" not in str(error.value)


async def test_publish_preserves_headers_partitioning_and_enforces_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _install_fake_client(monkeypatch)
    engine = KafkaEngine({"bootstrap_servers": "kafka:9092"})
    await engine.connect()
    producer = client.producers[0]
    context = EnginePublishContext(
        message_id="event-1",
        headers={"janus-event-id": "dedupe-1"},
        partition_key="session-42",
        ordering_key="ignored-fallback",
        timeout=1.0,
    )

    result = await engine.publish("janus.eventhandler", b"payload", context)

    assert result.transport_id == "janus.eventhandler:3:42"
    assert producer.sent[0] == (
        "janus.eventhandler",
        b"payload",
        b"session-42",
        [("janus-event-id", b"dedupe-1")],
    )

    producer.block = True
    with pytest.raises(TimeoutError):
        await engine.publish(
            "janus.eventhandler",
            b"blocked",
            EnginePublishContext(message_id="event-2", timeout=0.01),
        )
    with pytest.raises(ValueError, match="greater than zero"):
        await engine.publish(
            "janus.eventhandler",
            b"invalid",
            EnginePublishContext(message_id="event-3", timeout=0),
        )
    await engine.disconnect()


async def test_publish_uses_finite_engine_timeout_when_context_has_no_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _install_fake_client(monkeypatch)
    engine = KafkaEngine(
        {
            "bootstrap_servers": "kafka:9092",
            "publish_timeout": 0.01,
        }
    )
    await engine.connect()
    producer = client.producers[0]
    producer.block = True

    with pytest.raises(TimeoutError):
        await engine.publish(
            "janus.eventhandler",
            b"blocked",
            EnginePublishContext(message_id="event-without-caller-timeout"),
        )

    await engine.disconnect()


async def test_group_instance_id_is_unique_for_each_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _install_fake_client(monkeypatch)
    engine = KafkaEngine(
        {
            "bootstrap_servers": "kafka:9092",
            "group_id": "janus-monitor-v1",
            "group_instance_id": "monitor-instance-1",
        }
    )
    await engine.connect()

    async def callback(_message: object) -> None:
        return None

    handles = []
    for subscription_id in ("events", "audit"):
        handles.append(
            await engine.create_consumer(
                EngineSubscription(
                    id=subscription_id,
                    pattern=f"janus.{subscription_id}",
                    destination=f"janus.{subscription_id}",
                ),
                callback,
            )
        )

    instance_ids = [consumer.options["group_instance_id"] for consumer in client.consumers]
    assert len(set(instance_ids)) == 2
    assert all(str(instance_id).startswith("monitor-instance-1:") for instance_id in instance_ids)

    await asyncio.gather(*(handle.close() for handle in handles))
    await engine.disconnect()


async def test_healthcheck_detects_closed_and_failed_producer_sender(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _install_fake_client(monkeypatch)
    engine = KafkaEngine({"bootstrap_servers": "kafka:9092"})
    await engine.connect()
    producer = client.producers[0]

    health = await engine.healthcheck()
    assert health.connected
    assert health.healthy
    assert health.latency_ms is None
    assert "producer_error" not in health.details

    sender_task = asyncio.get_running_loop().create_future()
    sender_task.set_exception(RuntimeError("sensitive broker failure detail"))
    producer._sender.sender_task = sender_task
    health = await engine.healthcheck()
    assert not health.connected
    assert not health.healthy
    assert health.details["producer_error"] == "RuntimeError"
    assert "sensitive broker failure detail" not in repr(health.details)

    producer._sender.sender_task = None
    producer._closed = True
    health = await engine.healthcheck()
    assert not health.connected
    assert health.details["producer_error"] == "closed"

    await engine.disconnect()


async def test_failed_producer_start_is_cleaned_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _install_fake_client(monkeypatch)

    async def fail_start() -> None:
        raise RuntimeError("authentication failed")

    engine = KafkaEngine()
    original_factory = client.AIOKafkaProducer

    def failing_factory(**options: object) -> _FakeProducer:
        producer = original_factory(**options)
        producer.start = fail_start  # type: ignore[method-assign]
        return producer

    monkeypatch.setattr(client, "AIOKafkaProducer", failing_factory)
    with pytest.raises(RuntimeError, match="authentication failed"):
        await engine.connect()
    assert client.producers[0].stopped
    assert engine._producer is None
