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

Browser fan-out uses a durable `JrtcBrowserEventOutbox` row per authorized
socket target. An initial emit failure leaves that row pending and fails the
broker handler, but recovery does not depend on the broker delivering the
envelope again. The dedicated consumer continuously claims eligible rows in
bounded batches and retries them from the database. A conditional
`delivering` status plus `updated_at` is the cross-process lease; an abandoned
lease becomes eligible after `JRTC_EVENT_OUTBOX_LEASE_TIMEOUT`.

Before every immediate or delayed emit, the relay rechecks the persisted
connection ID, session ID, socket ID, and active connection state. A target
that is no longer authorized is terminally marked `discarded`. Successful
targets are never replayed during a partial fan-out retry. The browser also
deduplicates the envelope `event_id`, covering the narrow case where an emit
finishes after its database lease expires.

An immediate Socket.IO failure is translated to
`JrtcBrowserDispatchFailure` with the original exception chained for server
diagnostics; broker or Socket.IO implementation exceptions are never exposed
as a browser contract.

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
- `JRTC_EVENT_OUTBOX_POLL_INTERVAL`, `JRTC_EVENT_OUTBOX_RETRY_DELAY`
- `JRTC_EVENT_OUTBOX_LEASE_TIMEOUT`, `JRTC_EVENT_OUTBOX_BATCH_SIZE`
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

After the broker subscription is established, the consumer prints a readiness
line beginning with `JRTC event consumer running and listening for events`.
The line includes only the backend, physical route, live consumer identity,
and durable group/queue; broker URLs and credentials are never printed. Treat
that line (or the equivalent structured readiness log), rather than process
existence alone, as the consumer's startup signal.

The event consumer handles SIGINT/SIGTERM, closes its subscription, lets the
browser-outbox relay finish its active bounded sweep, then shuts down its
broker connection. Deploy at least one consumer per durable group; scale with
backend-aware replicas rather than starting an authoritative consumer in every
web worker. Each replica may sweep the shared outbox safely because claims use
conditional database leases.

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

The database receipt/outbox replay horizon defaults to 30 days through
`JRTC_EVENT_RECEIPT_RETENTION_DAYS`. Keep it at least as long as the broker and
dead-letter replay horizon, then schedule this command daily:

```bash
python manage.py prune_jrtc_event_history
```

The command deletes in bounded batches and never removes a receipt that still
has a pending or leased browser delivery. Use `--dry-run`, `--days`, and
`--batch-size` when validating a deployment-specific policy.

`run_jrtc_events` writes low-cardinality structured records for acknowledged
events, failures, and non-empty outbox sweeps. Embedded health integrations can
read `JrtcEventConsumer.inspect()` for consumer/outbox counters and handler
latency, and `consumer.broker.metrics.snapshot()` for Broka's existing lag,
in-flight, retry, dead-letter, and acknowledgement series. JRTC's publisher
metrics are emitted through its configured LogVista metrics provider. None of
these metric labels contain session, handle, participant, socket, or room IDs.

Operational dashboards should track runtime readiness, active/stale handles,
publisher admission/drop/failure counts and queue depth, consumer lag/retries/
duplicates/dead letters/correlation failures, per-type handler latency, and
command timeouts/protocol failures. Never use session, handle, participant, or
room IDs as metric labels.
