# Janus broker events

This guide describes the cross-process event contract implemented by
`jrtc.messaging`. It targets Python 3.12 or newer and the versions pinned by
this project: Broka 0.0.2 and Dispio 0.0.2.

The short version is:

- Dispio decides what an inbound Janus response means inside the transport.
- Broka publishes supported asynchronous Janus events to third-party applications.
- Every backend uses one exact physical destination, `janus.events`.
- The logical event name is `delivery.envelope.type`, not `delivery.route`.
- JSEP stays in the original response at `delivery.envelope.payload["jsep"]`; there
  is no separate SDP event.

## Architecture and ownership

`WebsocketTransportClient` and `HttpTransportClient` use the same pipeline:

```text
Janus Gateway
    -> validate one response
    -> JanusResponseDispatcher (Dispio)
       -> ACK/error/transaction resolution
       -> local session and plugin listeners
       -> bounded JanusEventPublisher admission, for supported events only
    -> Broka logical route (janus.event, janus.media, ...)
    -> Broka Router maps janus.* to the exact physical destination janus.events
    -> Redis, RabbitMQ, Kafka, or an in-process engine
    -> third-party Broka subscription to janus.events
    -> optional application Dispio dispatcher keyed by delivery.envelope.type
```

This is not an Rx pipeline. A response is not passed through a chain of plugins on
its way to an external application. Local plugin delivery and external publication
are separate consumers of the already validated response.

The transport's Dispio registry has exact handlers for `ack`, `error`, and every
published asynchronous type, plus one default transaction handler. It is frozen
after construction. This replaces the former growing `if`/`elif` decision tree
without making dispatch mutable at runtime.

ACK, error, success, keepalive, pong, server-info, and ordinary transaction
responses are resolved locally and are not broker events. The following Janus
types are published:

| Janus `janus` value | Logical envelope type |
| --- | --- |
| `event` | `janus.event` |
| `webrtcup` | `janus.webrtcup` |
| `media` | `janus.media` |
| `slowlink` | `janus.slowlink` |
| `hangup` | `janus.hangup` |
| `detached` | `janus.detached` |
| `trickle` | `janus.trickle` |
| `timeout` | `janus.timeout` |

Use `JANUS_EVENT_ROUTES` and `DISPATCHABLE_JANUS_TYPES` from
`jrtc.messaging` instead of duplicating this table in application code.

## Physical destination and envelope contract

There are two intentionally different addresses:

| Value | Meaning | Default |
| --- | --- | --- |
| Logical route | The kind of Janus event | `janus.event`, `janus.media`, etc. |
| Physical destination | The backend channel, stream, queue binding, or topic | `janus.events` |

Publishers call Broka with a logical `janus.*` route. `create_broker()` installs a
Broka `Router` rule that maps every logical route to `janus.events`. Consumers must
subscribe to the exact physical destination:

```python
subscription = await broker.subscribe("janus.events", handler)
```

Do not create backend subscriptions to `janus.event`, `janus.media`, or
`janus.*`. Do not use `delivery.route` to select application behavior:

```python
assert delivery.route == "janus.events"          # physical
logical_type = delivery.envelope.type             # e.g. "janus.event"
event = delivery.message                          # decoded payload mapping
```

The published payload is the complete Pydantic response dump, using Janus aliases
and omitting only `None` values. A representative envelope is:

```json
{
  "envelope_version": 1,
  "id": "90e9b967-0c41-4290-85d8-5c45e547d9cc",
  "type": "janus.event",
  "version": 1,
  "timestamp": "2026-08-12T20:00:00Z",
  "source": null,
  "headers": {},
  "partition_key": "74122891:77334455",
  "ordering_key": "74122891:77334455",
  "payload": {
    "janus": "event",
    "transaction": "client-transaction",
    "session_id": 74122891,
    "sender": 77334455,
    "plugindata": {
      "plugin": "janus.plugin.videoroom",
      "data": {"videoroom": "event"}
    },
    "jsep": {
      "type": "offer",
      "sdp": "v=0..."
    }
  }
}
```

The exact optional envelope fields are owned by Broka. Consumers should depend on
`type`, `version`, `id`, `timestamp`, `payload`, and `headers`, and tolerate new
optional fields.

### JSEP

JSEP is never emitted as `janus.sdp` or as a second event. It remains in the
complete `janus.event` response:

```python
jsep = delivery.message.get("jsep")
# Equivalent immutable envelope view:
jsep = delivery.envelope.payload.get("jsep")
```

This prevents duplicate negotiation work and preserves the transaction, session,
sender, plugin data, and JSEP as one atomic application event.

## Create a subscriber

Use the project helper rather than constructing Broka's default registry. The
helper returns an unstarted `broka.Broker`; startup and shutdown remain explicit
and asynchronous.

The following reusable subscriber performs one physical subscription and uses
Dispio for logical event selection:

```python
from __future__ import annotations

import asyncio
from collections.abc import Mapping

from broka import AcknowledgementMode, Broker, Delivery, SubscriptionOptions
from dispio import Dispatcher, ExactMatcher

from jrtc.messaging import DEFAULT_PHYSICAL_ROUTE, create_broker

events = Dispatcher(name="third-party.janus-events")


async def plugin_event(delivery: Delivery[object]) -> None:
    event = delivery.message
    if not isinstance(event, Mapping):
        raise TypeError("Janus event payload must be a mapping")

    plugin_data = event.get("plugindata")
    jsep = event.get("jsep")
    # Persist or enqueue the complete event. JSEP is processed here, if present.
    await application_service.handle_plugin_event(plugin_data, jsep=jsep)


async def peer_connection_up(delivery: Delivery[object]) -> None:
    await application_service.mark_peer_connection_up(delivery.message)


async def unhandled_event(delivery: Delivery[object]) -> None:
    # Choose explicitly whether an unknown logical event should be acknowledged
    # (return) or retried/dead-lettered (raise).
    application_logger.warning(
        "Unhandled Janus event",
        extra={"event_type": delivery.envelope.type},
    )


events.add(ExactMatcher("janus.event"), plugin_event, name="plugin-event")
events.add(ExactMatcher("janus.webrtcup"), peer_connection_up, name="webrtc-up")
events.default(unhandled_event, name="unhandled-janus-event")
events.registry.freeze()


async def dispatch_delivery(delivery: Delivery[object]) -> None:
    # This key MUST come from the logical envelope type. delivery.route is the
    # physical value "janus.events" for every message.
    await events.dispatch_async(
        delivery,
        __dispatch_key=delivery.envelope.type,
        __dispatch_headers=delivery.envelope.headers,
        __dispatch_metadata={
            "physical_route": delivery.route,
            "attempt": delivery.attempt,
        },
    )


async def serve(broker: Broker, *, durable: bool, instance_id: str) -> None:
    async with broker:  # calls broker.startup(), then broker.shutdown()
        subscription = await broker.subscribe(
            DEFAULT_PHYSICAL_ROUTE,
            dispatch_delivery,
            options=SubscriptionOptions(
                acknowledgement_mode=AcknowledgementMode.AUTO,
                durable=durable,
                consumer_id=instance_id,
                concurrency=4,
                capacity=256,
            ),
        )
        try:
            await asyncio.Event().wait()  # replace with the application's stop event
        finally:
            await subscription.close()
```

`AUTO` acknowledges only after the async handler returns successfully. If the
handler raises, Broka applies its handler retry policy and then its dead-letter
path. Do not start a background task and return before that task commits its work;
that acknowledges too early.

The Janus process and subscribers must agree on the backend endpoint, exact
physical destination, and RabbitMQ exchange. Consumer identity is application
specific: replicas of one consumer application share its stable stream group,
RabbitMQ queue, or Kafka group, while independent applications use different
groups or queues so each application receives a copy.

## Backend-specific subscriptions

### Memory and local

`memory` is a bounded asynchronous in-process queue. `local` invokes matching
consumers directly in the publisher's process. Neither engine communicates
between broker instances or processes and neither is durable. A separately
deployed third-party application cannot subscribe to either one.

They are useful for tests and for an embedded integration that shares the exact
same `Broker` instance with `JanusEventPublisher`:

```python
from jrtc.messaging import JanusEventPublisher, create_broker

broker = create_broker(
    engine="memory",
    engine_options={
        "overflow_policy": "block",  # "block", "reject", or "drop-newest"
        "drain_timeout": 10.0,
    },
)
publisher = JanusEventPublisher(broker, owns_broker=False)

# Start broker once, attach both the subscriber and publisher, then stop them in
# reverse order. Passing this publisher to either Janus transport enables events.
await broker.startup()
subscription = await broker.subscribe("janus.events", dispatch_delivery)
await publisher.start()
```

`fail_publish_count` is a failure-injection setting for tests, not a production
tuning option. With `local`, a slow handler directly increases publish latency;
with `memory`, `SubscriptionOptions.capacity` bounds each consumer queue.

### Redis Pub/Sub

Redis Pub/Sub provides live fan-out with at-most-once delivery. Every connected
subscriber sees a copy, but messages published while a subscriber is disconnected
are lost. ACK and reject are no-ops; requeue and defer are unsupported. Always use
`durable=False`.

```python
import os

from jrtc.messaging import create_broker

broker = create_broker(
    engine="redis",
    engine_options={
        "mode": "pubsub",
        "url": os.environ["JANUS_REDIS_URL"],
    },
)

await serve(broker, durable=False, instance_id="analytics-api-01")
```

The adapter uses Redis `PSUBSCRIBE`; this integration still supplies the exact
pattern `janus.events`. Use a `rediss://` URL and backend credentials where the
deployment requires TLS. Pub/Sub is suitable only when loss during disconnects is
acceptable.

### Redis Streams

Redis Streams is the recommended Redis mode when events must survive subscriber
disconnects. It uses `XGROUP`, `XREADGROUP`, `XACK`, and `XAUTOCLAIM` for
at-least-once delivery and pending-entry recovery.

```python
import os

from jrtc.messaging import create_broker

broker = create_broker(
    engine="redis",
    engine_options={
        "mode": "streams",
        "url": os.environ["JANUS_REDIS_URL"],
        "group": "analytics-janus-v1",  # stable per logical application
        "consumer_name": "analytics-api-01",  # unique per live replica
        "max_length": 1_000_000,
        "claim_idle_ms": 60_000,
        "claim_interval": 30.0,
    },
)

await serve(broker, durable=True, instance_id="analytics-api-01")
```

Replicas of one application share a stable `group` and use distinct
`consumer_name` values. They compete for work. Applications that each require a
copy must use different stable group names. Omitting `group` uses Broka's generic
`pyev` default and can accidentally mix unrelated applications.

`max_length` enables approximate stream trimming. Size it from the maximum outage
window and event rate; trimming an entry before every required group consumes it
is data loss. A requeue appends the same encoded envelope as a new stream entry and
then acknowledges the old entry, so the envelope ID remains the idempotency key.

### RabbitMQ

RabbitMQ uses one topic exchange and a queue bound with `janus.events`. Configure
a stable queue name. Replicas using the same queue are competing consumers;
different third-party applications need different queues to each receive a copy.

```python
import os

from jrtc.messaging import create_broker

broker = create_broker(
    engine="rabbitmq",
    engine_options={
        "url": os.environ["JANUS_AMQP_URL"],
        "exchange": "janus",
        "queue": "analytics.janus.events.v1",
        "durable": True,
        "auto_delete": False,
        "publisher_confirms": True,
        "mandatory": True,
        "dead_letter_exchange": "janus.dlx",
    },
)

await serve(broker, durable=True, instance_id="analytics-api-01")
```

Use an `amqps://` URL outside a trusted development network. Provision the topic
exchange, dead-letter exchange, permissions, and capacity as infrastructure. A
stable durable queue plus persistent publishing provides at-least-once behavior;
handler idempotency is still required. RabbitMQ preserves queue order under simple
conditions, but multiple consumers, rejection, retry, and redelivery can reorder
completion.

### Kafka

Kafka maps the physical destination to the topic `janus.events`. A stable
`group_id` is mandatory for restart continuity. Replicas in one application share
a group; independent applications use different groups.

```python
import os

from jrtc.messaging import create_broker

broker = create_broker(
    engine="kafka",
    engine_options={
        "bootstrap_servers": os.environ["JANUS_KAFKA_BOOTSTRAP_SERVERS"],
        "security_protocol": "SASL_SSL",
        "ssl_ca_file": os.environ["JANUS_KAFKA_CA_FILE"],
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_plain_username": os.environ["JANUS_KAFKA_USERNAME"],
        "sasl_plain_password": os.environ["JANUS_KAFKA_PASSWORD"],
        "enable_idempotence": True,
        "publish_timeout": 30.0,
        "group_id": "analytics-janus-v1",
        "auto_offset_reset": "earliest",
    },
)

await serve(broker, durable=True, instance_id="analytics-api-01")
```

The publisher supplies `session_id:sender` as both Broka's partition key and
ordering key. Kafka therefore keeps one Janus session/handle key on one partition,
subject to normal Kafka partitioning. Ordering is only guaranteed within a
partition, not across the topic, and parallel handlers may finish out of order.
Offsets are explicitly committed after acknowledgement. Requeue seeks to the
record offset.

The compatibility adapter rejects `transactional_id` and
`transaction_timeout_ms`. Broka's engine interface has no transaction lifecycle,
and aiokafka rejects sends from a transactional producer until a transaction is
explicitly begun. Producer idempotence is enabled, but the application side
effect and offset commit are not one Kafka transaction; do not describe this as
exactly-once delivery.

Because Broka 0.0.2's bundled Kafka adapter does not forward TLS/SASL or publish
deadlines, `create_broker()` registers a project-owned compatibility adapter for
Kafka. It forwards a strict allowlist of producer/consumer settings, validates
TLS certificate/key pairing and SASL credentials, requests producer idempotence
with `acks=all`, and redacts credentials and bootstrap infrastructure from
diagnostics. Bootstrap entries must use `host[:port]` syntax; URLs, userinfo,
control characters, invalid hostnames, and invalid ports fail closed.

Every `send_and_wait` has a finite deadline. A per-call Broka timeout takes
precedence; otherwise the Kafka engine's `publish_timeout` applies and defaults
to 30 seconds. This fallback is required because aiokafka intentionally retries
idempotent batches without expiring them. Broka may still retry a timed-out
publish, so consumers must remain idempotent. A configured `group_instance_id`
is treated as an application-instance prefix and receives a deterministic suffix
per Broka subscription, preventing two subscriptions in one process from fencing
each other. Unknown options fail closed rather than being silently ignored. Keep
the Broka pin and adapter tests together until an upstream release provides
equivalent behavior.

## Acknowledgement and idempotency

Broka exposes four acknowledgement modes:

| Mode | Behavior |
| --- | --- |
| `AUTO` | ACK after the async handler returns; retry/dead-letter on failure. Recommended default. |
| `MANUAL` | The handler must call `ack()`, `nack()`, `reject()`, `requeue()`, or `defer()`. |
| `NONE` | No application acknowledgement lifecycle. Appropriate only for at-most-once transports. |
| `BATCH` | Broka 0.0.2 currently preserves semantics with individual ACKs; do not assume a native atomic batch. |

For durable backends, assume at-least-once delivery. Use `delivery.message_id` or
`delivery.envelope.id` as the unique idempotency key. A robust consumer stores that
ID with the application state in the same transaction:

```python
from broka import AcknowledgementMode, Delivery, SubscriptionOptions


async def apply_once(delivery: Delivery[object]) -> None:
    async with database.transaction() as transaction:
        inserted = await transaction.try_insert_event_id(delivery.message_id)
        if inserted:
            await transaction.apply_janus_event(delivery.message)
    await delivery.ack()


options = SubscriptionOptions(
    acknowledgement_mode=AcknowledgementMode.MANUAL,
    durable=True,
    consumer_id="analytics-api-01",
    capacity=256,
)
```

In manual mode, never return with a delivery still in `PROCESSING`. Redis Pub/Sub
cannot requeue. Redis Streams, RabbitMQ, and Kafka support requeue with different
native mechanics. `touch()` is not supported by these three alpha adapters.

Broka retries a failing handler in-process before its dead-letter path. A handler
that performs a side effect and then raises can perform that side effect again on
retry, which is why transport acknowledgements alone do not provide idempotency.
The default Broka dead-letter store is in memory; RabbitMQ's configured
`dead_letter_exchange` provides a backend-native durable rejection path. Design a
durable failure workflow explicitly for Redis and Kafka.

## Producer lifecycle and backpressure

`JanusEventPublisher` decouples a transport receive loop from backend latency. Its
defaults are four workers, a global capacity of 1,024 admitted events, and a
50-millisecond admission timeout.

```python
from jrtc.messaging import JanusEventPublisher, create_broker
from jrtc.transport.websocket import WebsocketTransportClient

broker = create_broker(engine="redis", engine_options=redis_options)
publisher = JanusEventPublisher(
    broker,
    workers=8,
    queue_capacity=4096,
    admission_timeout=0.05,
    owns_broker=True,
)
transport = WebsocketTransportClient(event_publisher=publisher)

await publisher.start()
try:
    await transport.start()
    # Run the application.
finally:
    await transport.stop()
    await publisher.stop(drain=True, timeout=30.0)
```

Accepted events are assigned to a deterministic worker shard by
`session_id:sender`, preserving per-key publish order within one publisher
process. Different keys publish concurrently. Backend failure is isolated from
the Janus receive loop. After Broka exhausts its publish retry policy, however,
the publisher records a failure and releases the item; its queue is not a durable
outbox.

Capacity protects the Janus receive loop from unbounded memory growth. Admission
timeout, shutdown cancellation, and a full/rejecting backend are observable drops.
Size capacity from measured peak event rate and backend latency. Graceful shutdown
should stop transport admission first, drain the publisher, and shut down the
broker last.

`SubscriptionOptions.capacity` is interpreted differently by the alpha adapters:
it is a memory queue bound, Redis claim/read count input, RabbitMQ prefetch, and
Kafka `max_poll_records`. Treat it as a tuning hint, benchmark each backend, and do
not assume identical throughput or concurrency behavior.

## LogVista metrics and debugging

`create_broker()` installs `LogVistaMetrics` by default. It keeps an in-memory
snapshot and writes every counter, gauge, and observation update as a structured
LogVista debug diagnostic. Payloads, plugin data, JSEP, session IDs, handle IDs,
transactions, and other high-cardinality identifiers are not metric labels or log
fields.

The Janus-specific metrics are:

| Metric | Type | Purpose |
| --- | --- | --- |
| `janus_event_admission_total` | counter | Accepted, timed-out, or invalid publisher admission |
| `janus_event_published_total` | counter | Backend-accepted events |
| `janus_event_publish_failures_total` | counter | Rejected, failed, or shutdown publication |
| `janus_event_dropped_total` | counter | Unsupported, stopped, timed-out, cancelled, or discarded events |
| `janus_event_queue_depth` | gauge | Admitted events not yet completed |
| `janus_event_queue_latency_seconds` | histogram | Admission-to-worker delay |
| `janus_event_publish_latency_seconds` | histogram | Broka publish duration |
| `janus_dispatch_total` | counter | Dispio response dispatch outcomes |
| `janus_dispatch_failures_total` | counter | Selected handler/callback failures |
| `janus_dispatch_duration_seconds` | histogram | Dispio dispatch duration |
| `janus_listener_failures_total` | counter | Isolated local listener failures |

Broka also records its own `pyev_*` publish, consume, retry, handler, ACK/NACK,
in-flight, and latency series through the same provider.

Inject one metrics instance to correlate broker, publisher, and dispatcher state:

```python
from jrtc.messaging import LogVistaMetrics, JanusEventPublisher, create_broker

metrics = LogVistaMetrics()
broker = create_broker(engine="redis", engine_options=redis_options, metrics=metrics)
publisher = JanusEventPublisher(broker, metrics=metrics)

snapshot = metrics.snapshot()
```

Metric logging occurs at debug level and can be verbose under load. Configure
LogVista level, sampling, and sinks for the environment. For a production metrics
system, pass another implementation of Broka's `MetricsProvider` while retaining
structured error and lifecycle logs.

Useful alerts include a non-zero drop or publish-failure rate, sustained queue
depth, queue latency approaching the admission/outage budget, handler failures,
consumer lag, dead letters, and reconnect/recovery churn.

## Native clients and Broka wire framing

Using Broka on both ends is recommended because it validates the envelope,
serializer allowlist, size limit, lifecycle, and acknowledgements. A native
backend client receives a Broka wire frame, not plain Janus JSON.

With the integration's default JSON serializer, the frame is:

```text
bytes "PYEV\x01" | one-byte serializer-name length | ASCII "json" | envelope JSON
```

The `PYEV` spelling is a Broka 0.0.2 compatibility artifact. A bounded decoder is:

```python
from broka import Envelope


WIRE_MAGIC = b"PYEV\x01"
MAX_FRAME_BYTES = 16 * 1024 * 1024


def decode_janus_envelope(value: bytes) -> Envelope:
    raw = bytes(value)
    if len(raw) > MAX_FRAME_BYTES:
        raise ValueError("Broka frame exceeds configured limit")

    # Broka also accepts its older unframed canonical JSON representation.
    if not raw.startswith(WIRE_MAGIC):
        return Envelope.from_bytes(raw, max_size=MAX_FRAME_BYTES)

    offset = len(WIRE_MAGIC)
    if len(raw) <= offset:
        raise ValueError("truncated Broka frame")
    name_size = raw[offset]
    start = offset + 1
    end = start + name_size
    if name_size == 0 or end > len(raw):
        raise ValueError("invalid Broka serializer header")

    serializer = raw[start:end].decode("ascii")
    if serializer != "json":
        raise ValueError(f"unsupported Broka serializer: {serializer!r}")
    return Envelope.from_bytes(raw[end:], max_size=MAX_FRAME_BYTES)
```

Extract the frame from the native client as follows:

| Backend | Raw frame location |
| --- | --- |
| Redis Pub/Sub | `message["data"]` |
| Redis Streams | `fields[b"payload"]` (or `fields["payload"]`) |
| RabbitMQ | `message.body` |
| Kafka | `record.value` |

After decoding, route on `envelope.type` and read `envelope.payload`. Redis Stream
headers are stored separately in the stream's `headers` field, while the canonical
application headers also remain in the decoded envelope. Native clients must
implement backend acknowledgement and recovery rules themselves.

If `broker_options` changes the serializer from JSON, this decoder must be replaced
with an allowlisted implementation for that serializer. Pin the wire contract and
test fixtures before operating a non-Broka consumer; the native frame is part of
an alpha dependency.

## Broka 0.0.2 registry workaround

Broka 0.0.2's `create_default_registry()` probes stale `pyev.engines.*` module
paths for Redis, RabbitMQ, and Kafka. Optional-engine discovery can therefore fail
even though `broka.engines.*` is installed. `jrtc.messaging.create_broker()`
avoids this defect with an isolated, lazy `EngineRegistry` that explicitly loads
the five supported engine classes.

Third-party applications should use `create_broker()`. If a standalone application
must construct Broka directly, explicitly register the selected engine and router:

```python
from broka import Broker, Destination, EngineRegistry, Router
from broka.engines.redis import RedisEngine

registry = EngineRegistry()
registry.register(RedisEngine)

router = Router()
router.map_destination(
    "janus.*",
    Destination("janus.events", engine="redis"),
)

broker = Broker(
    {
        "engine": "redis",
        "engines": {"redis": redis_options},
    },
    registry=registry,
    router=router,
)
```

Do not copy Broka's stale default entry-point strings into application code.

The 0.0.2 distribution metadata and this project's dependency pin are the version
authority. The installed package currently reports `broka.__version__ == "0.1.0"`
internally, retains several `pyev` names, and declares backend/framework packages
both unconditionally and as extras. Treat those as alpha packaging defects, not
as permission to float the dependency version.

## Security and production constraints

The current integration provides bounded processing, typed validation, isolated
registries, structured diagnostics, backend acknowledgements, and graceful
lifecycle management. It does not provide an application security boundary.

- Broka does not encrypt or sign the event payload and does not authorize tenants.
- Janus events can contain SDP, ICE candidates, plugin data, and stable identifiers.
  Classify them as sensitive and set retention accordingly.
- Restrict publishers and consumers to the exact destination with Redis, RabbitMQ,
  or Kafka ACLs. Do not grant broad administrative credentials.
- Store broker URLs and credentials in a secret manager or environment injection;
  never include them in source, exception messages, metric labels, or support dumps.
- Redis and RabbitMQ 0.0.2 security configuration is URL-based. Validate
  certificates and server identity in the actual client/deployment configuration.
- Kafka must be configured through this project's allowlisted compatibility
  adapter; do not instantiate Broka's bundled 0.0.2 Kafka engine directly.
- There is no durable producer outbox. A process crash after Janus reception but
  before broker acceptance can lose an event.
- Redis Pub/Sub, memory, and local are intentionally non-durable.
- Redis Streams, RabbitMQ, and Kafka are at-least-once, not end-to-end exactly-once.
- The default Broka dead-letter store is process memory unless a native RabbitMQ
  dead-letter exchange or an application-owned durable failure store is configured.

Load-test event bursts, broker outages, redelivery, rolling restarts, queue/stream
retention, and graceful shutdown before production. Pin Broka and Dispio until an
upgrade has passed wire-compatibility and failure-semantics tests.

## Migration from Reactivex

There is no Reactivex compatibility layer in the new design.

| Previous concept | Current equivalent |
| --- | --- |
| `Subject.on_next(response)` | `await JanusEventPublisher.admit(response, ...)` |
| `Observable.subscribe(on_next=...)` | `await broker.subscribe("janus.events", async_handler)` |
| Disposable subscription | `await subscription.close()` |
| Rx filtering by event enum | Dispio exact/glob/type matchers keyed by `delivery.envelope.type` |
| Plugin chain before external delivery | Independent local listener and Broka publication paths |
| Separate SDP event | `delivery.envelope.payload["jsep"]` on `janus.event` |
| Scheduler ownership | Explicit asyncio `startup()`/`shutdown()` and application task ownership |

Migration checklist:

1. Remove Rx imports, subjects, operators, schedulers, disposables, and compatibility
   properties from application and plugin code.
2. Create one Broka broker/publisher at application startup and pass the publisher
   into the selected Janus transport or session manager.
3. Subscribe third-party applications to the exact physical route `janus.events`.
4. Route logical behavior with Dispio using `delivery.envelope.type`.
5. Move SDP handlers into the `janus.event` handler and read its `jsep` field.
6. Give every durable application a stable Redis group, RabbitMQ queue, or Kafka
   group, and give live replicas appropriate distinct consumer identities.
7. Add transactional idempotency using the envelope ID.
8. Exercise disconnect, duplicate, retry, poison-event, backpressure, and shutdown
   scenarios and alert on the LogVista/Broka metrics.

## Upstream references

This project pins published artifacts; upstream `main` can change after a release.
Use these links for implementation context and verify behavior against the pinned
0.0.2 wheel or source distribution:

- [Broka 0.0.2 on PyPI](https://pypi.org/project/broka/0.0.2/)
- [Broka / pyev repository](https://github.com/Leydotpy/pyev)
- [Broka broker facade](https://github.com/Leydotpy/pyev/blob/94f720174d0c2bf3ea083cac141c3161b3642615/src/broka/broker.py)
- [engine registry](https://github.com/Leydotpy/pyev/blob/94f720174d0c2bf3ea083cac141c3161b3642615/src/broka/registry.py)
- [configuration](https://github.com/Leydotpy/pyev/blob/94f720174d0c2bf3ea083cac141c3161b3642615/src/broka/config.py)
- [wire envelope](https://github.com/Leydotpy/pyev/blob/94f720174d0c2bf3ea083cac141c3161b3642615/src/broka/envelope.py)
- [subscriptions](https://github.com/Leydotpy/pyev/blob/94f720174d0c2bf3ea083cac141c3161b3642615/src/broka/subscription.py)
- [acknowledgements](https://github.com/Leydotpy/pyev/blob/94f720174d0c2bf3ea083cac141c3161b3642615/src/broka/acknowledgements.py)
- [routing](https://github.com/Leydotpy/pyev/blob/94f720174d0c2bf3ea083cac141c3161b3642615/src/broka/routing/router.py)
- [memory engine](https://github.com/Leydotpy/pyev/blob/94f720174d0c2bf3ea083cac141c3161b3642615/src/broka/engines/memory.py)
- [Redis engine](https://github.com/Leydotpy/pyev/blob/94f720174d0c2bf3ea083cac141c3161b3642615/src/broka/engines/redis.py)
- [RabbitMQ engine](https://github.com/Leydotpy/pyev/blob/94f720174d0c2bf3ea083cac141c3161b3642615/src/broka/engines/rabbitmq.py)
- [Kafka engine](https://github.com/Leydotpy/pyev/blob/94f720174d0c2bf3ea083cac141c3161b3642615/src/broka/engines/kafka.py)
- [Dispio 0.0.2 on PyPI](https://pypi.org/project/dispio/0.0.2/)
- [Dispio dispatcher](https://github.com/Leydotpy/pyev-dispatch/blob/adc63ca982888ca9a86f78aee97643d1b7782d87/src/dispio/dispatcher.py)
- [Dispio repository](https://github.com/Leydotpy/pyev-dispatch)
