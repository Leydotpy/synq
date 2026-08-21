# JRTC architecture and operations

Synq separates Janus work into a direct command plane, an authoritative event
plane, and process-local handle ownership. Persisted Janus identifiers are
correlation data; they are never reconstructed into live network objects.

## Command plane

Socket.IO signaling calls `VideoRoomAdapter` directly. The adapter resolves a
live `VideoRoomPlugin` from the current process registry and awaits JRTC's
transaction future. An initial Janus ACK does not complete VideoRoom requests
that wait for the final event. Publisher `join_and_configure` remains one Janus
operation and its JSEP is returned in the existing Socket.IO ACK.

```text
Socket.IO -> Synq signaling -> VideoRoomAdapter -> live VideoRoomPlugin
          <- command ACK/JSEP <- JRTC transaction future <- Janus
```

Commands are not RPC messages on Redis, RabbitMQ, or Kafka.

## Event plane

Every JRTC-owning runtime passes a bounded `JanusEventPublisher` to its
`JanusSessionManager`. JRTC publishes supported asynchronous Janus types onto
one physical destination, `janus.events`. The dedicated Django command
`python manage.py run_jrtc_events` subscribes once to that destination and
selects logical handlers with `delivery.envelope.type`.

```text
Janus -> JRTC dispatcher -> JanusEventPublisher -> Broka backend
      -> janus.events -> run_jrtc_events -> domain reconciliation -> Socket.IO
```

Supported logical types come from JRTC's exported route map and currently
include `janus.event`, `janus.webrtcup`, `janus.media`, `janus.slowlink`,
`janus.hangup`, `janus.detached`, `janus.trickle`, and `janus.timeout`.

Broka freezes nested JSON containers. The consumer recursively copies mappings
before validation, persistence, duplicate-JSEP filtering, or Socket.IO output.
A packet containing both `transaction` and a JSEP still reconciles safe state
metadata, but its JSEP is not forwarded to the browser because the command ACK
already delivered the negotiated result.

## Delivery and idempotency

Redis Streams, RabbitMQ, and Kafka are treated as at-least-once transports.
The consumer uses manual acknowledgement and does not return before durable
database work and awaited Socket.IO work finish. A `JrtcEventReceipt` row keyed
by the JRTC envelope UUID is admitted in the same transaction as domain state
changes. Redelivery therefore skips domain side effects. This is application
idempotency, not an exactly-once claim.

Backend identities must be configured on the engine because Broka 0.0.2 does
not derive them from `SubscriptionOptions`:

- Redis Streams: stable `JRTC_REDIS_GROUP`, unique live consumer name.
- RabbitMQ: stable durable `JRTC_RABBITMQ_QUEUE` and configured DLX.
- Kafka: stable `JRTC_KAFKA_GROUP_ID`, unique group-instance prefix.

Redis Pub/Sub remains supported only for intentionally lossy development or
low-criticality deployments. It is not the production default.

## Runtime and handle ownership

Each ASGI or worker process that issues commands owns its own event loop,
session manager, WebSocket transports, event publisher, registry, pending
transaction futures, and unique runtime owner ID. Startup order is publisher,
then manager; shutdown first stops transports, then drains/stops the publisher,
then clears the registry.

`ParticipantMediaHandle.runtime_owner_id` records diagnostic ownership. A
binding is usable only when all of these remain true:

- its runtime owner is this process;
- its JRTC session is ready and has the recorded integer session ID;
- its plugin has the recorded integer handle ID;
- `session.plugins.get(handle_id)` is the same plugin object.

On restart, fork, session loss, transport replacement, or ownership mismatch,
the registry treats database IDs as stale, attaches a fresh plugin when the
workflow permits, and persists the new correlation IDs. It never constructs
`VideoRoomPlugin(session=new_session, plugin_id=old_database_id)`.

Initial deployments require Socket.IO connection affinity: commands for a
participant's active connection must reach the web process that owns its live
handles. There is intentionally no cross-process command RPC bus. Celery may
provision rooms, reconcile state, kick publishers with short-lived management
handles, and clear stale ownership; it must not adopt participant handles.

## Identifier boundary

JRTC, Python, and database identifiers are strict positive integers. Nullable
database values use `None`; empty strings are invalid. Browser JSON uses
canonical decimal strings because Janus IDs may exceed JavaScript's safe
integer range. Conversion occurs only in state, command-ACK, and event
serialization boundaries. Persisted/client JSON feed strings are explicitly
parsed back to integers before creating JRTC models.

## Configuration

Janus connection settings remain `JANUS_*`. Application event settings are
separate:

- `JRTC_EVENTS_ENABLED`
- `JRTC_EVENT_BROKER_ENGINE` (`redis` by default; also `rabbitmq`, `kafka`,
  `memory`, or `local`)
- `JRTC_EVENT_PHYSICAL_ROUTE`
- `JRTC_EVENT_PUBLISH_*`, `JRTC_EVENT_DRAIN_TIMEOUT`
- `JRTC_EVENT_CONSUMER_CONCURRENCY`, `JRTC_EVENT_CONSUMER_CAPACITY`
- `JRTC_REDIS_*`, `JRTC_RABBITMQ_*`, and `JRTC_KAFKA_*`

Use memory/local only in tests or deliberate same-process development.

## Deployment and shutdown

Run four independent units:

```text
synq-web         python manage.py runjanus
synq-worker      celery -A conf worker ...
synq-beat        celery -A conf.celery beat ...
synq-jrtc-events python manage.py run_jrtc_events
```

The event consumer handles SIGINT/SIGTERM, closes its subscription, then shuts
down its broker connection. Deploy at least one consumer per durable group;
scale with backend-aware replicas rather than starting an authoritative
consumer in every web worker.

## Security and retention

Janus events may contain SDP, ICE candidates, transport IDs, and participant
metadata. Production logging must include event type, outcome, and timings but
must not include full SDP, credentials, or broker URLs. Use TLS and broker
authentication, grant producers and consumers access only to the configured
stream/topic/exchange and queue, and separate permissions where supported.

Set Redis Stream trimming, Kafka topic retention, or RabbitMQ TTL/DLX policies
to the organization's media-metadata retention requirement. Redis credentials,
RabbitMQ URLs, Kafka SASL secrets, Janus tokens, and API secrets must come from
the deployment secret store and must not be returned to clients.

Operational dashboards should track runtime readiness, active/stale handles,
publisher admission/drop/failure counts and queue depth, consumer lag/retries/
duplicates/dead letters/correlation failures, per-type handler latency, and
command timeouts/protocol failures. Never use session, handle, participant, or
room IDs as metric labels.
