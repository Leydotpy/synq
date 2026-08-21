# AGENTS.md

## 1. Mission

This repository migration replaces Synq's current Janus integration based on the obsolete/intermediate `janus_api` / `janus-api-core` / `janus-videoroom-plugin` stack with the new `jrtc` core runtime and the new independently packaged JRTC plugins, especially `jrtc-video`.

This work is not a mechanical import rename.

The coding agent must treat the migration as an architectural refactor with three separate responsibilities:

1. **Command plane**
   - Synq/Django calls JRTC and `jrtc-video` methods directly.
   - Command responses are returned through JRTC's transaction/future machinery.
   - Examples: `join_and_configure`, `publish`, `configure_publisher`, `join_subscriber`, `update_subscription`, `start`, `trickle`, `hangup`, and VideoRoom management operations.

2. **Event plane**
   - Asynchronous Janus/WebRTC events are published by `jrtc.messaging.JanusEventPublisher`.
   - Broka fans those events out through the configured backend.
   - Synq/Django consumes the shared physical destination `janus.events`.
   - Supported backends must remain configurable: Redis Streams, Redis Pub/Sub, RabbitMQ, Kafka, with memory/local reserved for tests or same-process development.

3. **Local JRTC plugin lifecycle**
   - JRTC still has local per-plugin event delivery through `Plugin.on_event`.
   - Synq must not treat `on_event` as the authoritative Django application event bus.
   - `on_event` may be used only for process-local lifecycle, tracing, instrumentation, tests, or narrowly scoped internal callbacks.

The final architecture must be production-grade, async-safe, process-safe, scalable, observable, idempotent, and explicit about ownership.

---

## 2. Source repositories and target branches

### Backend

Repository:

```text
https://github.com/Leydotpy/synq
```

Target branch:

```text
v4
```

### Frontend

Repository:

```text
https://github.com/Leydotpy/synq.js
```

Target branch:

```text
codex/new-ui-implementation
```

### JRTC core

Repository:

```text
https://github.com/Leydotpy/jrtc
```

Primary package:

```text
jrtc
```

Expected compatible version:

```text
>=3.1,<4
```

### JRTC plugins monorepo

Repository:

```text
https://github.com/Leydotpy/jrtc-plugins
```

VideoRoom package:

```text
jrtc-video
```

Expected compatible version:

```text
>=3,<4
```

Optional future TextRoom package:

```text
jrtc-text
```

Do not retain runtime dependencies on:

```text
janus_api
janus-api-core
janus_videoroom_plugin
janus-videoroom-plugin
```

Historical migrations may still reference compatibility dotted paths. Those historical migration files must not be rewritten.

---

## 3. Non-negotiable architectural principles

### 3.1 Internal Janus IDs are integers

All Janus protocol identifiers inside Python and the database must be strict positive integers.

This includes:

```text
session_id
handle_id
room_id
publisher_id
private_id
feed_id
sender
```

The new JRTC `JanusId` contract is strict and must be respected.

Wrong:

```python
SubscribeTarget(feed="12345")
```

Correct:

```python
SubscribeTarget(feed=12345)
```

Do not pass numeric strings into JRTC models.

### 3.2 JavaScript-facing Janus IDs are strings

Janus IDs can exceed JavaScript's safe integer range.

Therefore:

```text
Python/JRTC/database: int
JSON/browser/TypeScript: decimal string
```

The string conversion must happen deliberately at the application boundary.

Examples:

```python
payload["janus_room_id"] = str(room_id) if room_id is not None else None
payload["janus_publisher_id"] = str(publisher_id) if publisher_id is not None else None
payload["janus_feed_id"] = str(feed_id)
payload["plugin_id"] = str(plugin_id) if plugin_id is not None else None
```

Do not globally stringify IDs inside the domain or JRTC adapter.

---

## 4. Critical clarification about JRTC response dispatch

The coding agent must understand this distinction before changing any code.

Every valid Janus response received by `WebsocketTransportClient` is passed through JRTC's internal `JanusResponseDispatcher`.

That does **not** mean every response is published through Broka.

The dispatcher performs multiple functions:

```text
Janus response
    |
    v
JanusResponseDispatcher
    |
    +--> ACK handling
    +--> error handling
    +--> ordinary transaction resolution
    +--> local session/plugin event delivery
    +--> supported asynchronous event publication to Broka
```

Only configured dispatchable Janus asynchronous event types are published externally.

Current supported asynchronous event types include:

```text
event
webrtcup
media
slowlink
hangup
detached
trickle
timeout
```

These correspond to logical Broka envelope types:

```text
janus.event
janus.webrtcup
janus.media
janus.slowlink
janus.hangup
janus.detached
janus.trickle
janus.timeout
```

Ordinary transaction responses such as `success`, `ack`, `error`, `keepalive`, `pong`, and `server_info` are not application broker events merely because they pass through the internal dispatcher.

---

## 5. Command plane

### 5.1 Commands must call JRTC directly

The response from operations such as:

```python
await plugin.join_and_configure(...)
await plugin.publish(...)
await plugin.configure_publisher(...)
await plugin.join_subscriber(...)
await plugin.update_subscription(...)
await plugin.start(...)
```

must remain direct method results.

Do not redesign these operations as request/response over Kafka, Redis, RabbitMQ, or any other broker during this migration.

### 5.2 Command response lifecycle

The conceptual flow must remain:

```text
Synq
 |
 v
VideoRoomAdapter
 |
 v
live VideoRoomPlugin
 |
 v
JRTC Plugin.send()
 |
 v
JRTC session.send()
 |
 v
WebsocketTransportClient.send()
 |
 +--> create transaction id
 +--> create pending Future
 +--> send Janus request
 |
 v
Janus Gateway
 |
 v
response carrying same transaction
 |
 v
JanusResponseDispatcher
 |
 v
resolve pending Future
 |
 v
VideoRoomPlugin.request()
 |
 v
parse_videoroom_response()
 |
 v
VideoRoomReply
 |
 v
Synq caller
```

This is the authoritative response path for commands.

### 5.3 ACK versus final event

For plugin requests using `wait_for_event=True`, JRTC may receive an initial Janus ACK and later a transaction-correlated Janus `event`.

The final transaction-correlated event is what resolves the method's pending Future.

Do not mistakenly treat the initial ACK as the final `join_and_configure`, publish, or subscriber response.

---

## 6. Event plane

### 6.1 Broka is the authoritative Synq application event stream

Synq must configure a `JanusEventPublisher` and pass it into the process-local JRTC session manager.

Asynchronous Janus events must be consumed by a Synq-owned Broka subscriber.

The authoritative event path is:

```text
Janus Gateway
    |
    v
JRTC transport
    |
    v
JanusResponseDispatcher
    |
    v
JanusEventPublisher
    |
    v
Broka
    |
    +--> Redis
    +--> RabbitMQ
    +--> Kafka
    |
    v
physical destination: janus.events
    |
    v
Synq JRTC Event Consumer
    |
    v
Synq event dispatcher
    |
    +--> domain state updates
    +--> topology reconciliation
    +--> Socket.IO events
    +--> metrics/audit
```

### 6.2 Subscribe to the physical destination

Use:

```python
from jrtc.messaging import DEFAULT_PHYSICAL_ROUTE
```

Do not create separate backend subscriptions for `janus.event`, `janus.media`, `janus.webrtcup`, etc.

Logical event selection must use:

```python
delivery.envelope.type
```

not:

```python
delivery.route
```

### 6.3 Application event dispatcher

Create a Synq-owned dispatcher, preferably using Dispio, keyed by `delivery.envelope.type`.

Suggested handlers:

```text
janus.event      -> VideoRoom/plugin event handler
janus.webrtcup   -> peer connection up handler
janus.media      -> media status handler
janus.slowlink   -> congestion/quality handler
janus.hangup     -> hangup/lifecycle handler
janus.detached   -> handle lifecycle handler
janus.trickle    -> remote ICE handler
janus.timeout    -> session timeout/recovery handler
```

Do not duplicate JRTC's logical route constants. Use exported JRTC constants where possible.

---

## 7. Transaction-correlated events must not cause duplicate negotiation

A Janus packet can legitimately serve both planes.

Example:

```json
{
  "janus": "event",
  "transaction": "abc123",
  "session_id": 100,
  "sender": 200,
  "jsep": {
    "type": "answer",
    "sdp": "..."
  }
}
```

This can:

1. resolve the pending JRTC command Future; and
2. be broker-published because its Janus type is `event`.

Synq must not send the same negotiated JSEP to the browser twice.

Required rule:

```python
if payload.get("transaction") and isinstance(payload.get("jsep"), dict):
    # Command plane handled this negotiation result.
    return
```

This filtering must be documented and tested.

Do not discard all transaction-correlated events blindly if some non-JSEP data is required for reconciliation.

Where necessary:

```text
transaction-correlated JSEP
    -> no duplicate negotiation

transaction-correlated state metadata
    -> safe reconciliation may still occur
```

---

## 8. Replace `JanusPluginField` as a live plugin materializer

### 8.1 Reason

The ORM must not masquerade an integer database identifier as a live async network object.

A live `VideoRoomPlugin` is:

```text
event-loop-bound
session-bound
transport-bound
process-local
temporary
invalidatable
not safely serializable
not safely reconstructible from an integer alone
```

A Django database field is persistent state.

These responsibilities must be separated.

### 8.2 Replace with ordinary numeric fields

For active models, prefer:

```python
models.PositiveBigIntegerField(
    null=True,
    blank=True,
    db_index=True,
)
```

for:

```text
janus_session_id
janus_handle_id
janus_room_id
janus_publisher_id
janus_private_id
janus_feed_id
```

Use nullable semantics consistently. Never store `""` into nullable integer fields. Use `None`.

### 8.3 Why keep session and handle IDs?

The IDs remain important for:

1. diagnostics;
2. broker-event correlation;
3. ownership reconciliation;
4. stale-handle detection;
5. operational debugging.

A broker payload contains `session_id` and `sender`, which naturally maps to:

```python
ParticipantMediaHandle.objects.get(
    janus_session_id=payload["session_id"],
    janus_handle_id=payload["sender"],
)
```

### 8.4 Historical migration compatibility

Do not rewrite historical migrations that reference:

```text
apps.meetings.services.janus.NativeJanusIdVideoRoomPlugin
```

Keep a compatibility symbol until migrations are squashed:

```python
NativeJanusIdVideoRoomPlugin = VideoRoomPlugin
```

Mark it clearly as migration compatibility only. Do not use this alias in new runtime code.

---

## 9. Introduce an explicit JRTC handle registry

Eliminating `JanusPluginField` does not eliminate live plugin handles.

The live handles must move into an explicit runtime registry such as `JrtcHandleRegistry` or `VideoRoomHandleRegistry`.

### 9.1 Responsibilities

The registry should:

- associate a Synq domain handle/model identity with a live JRTC plugin;
- retain the owning session;
- retain the Janus session ID;
- retain the Janus plugin handle ID;
- validate that the session is still active;
- detect stale plugin references;
- invalidate bindings when the session/transport is lost;
- remove bindings on detach/hangup/participant cleanup;
- never automatically adopt a persisted handle on a different session;
- expose safe inspection for debugging and metrics.

### 9.2 Suggested key

Prefer a stable domain key such as `ParticipantMediaHandle.pk`.

Suggested runtime record:

```python
@dataclass(slots=True)
class BoundVideoRoomHandle:
    model_id: int
    session_id: int
    handle_id: int
    plugin: VideoRoomPlugin
    owner_id: str
```

### 9.3 Resolve semantics

A method such as:

```python
await registry.resolve(media_handle)
```

must:

1. return the current live plugin if owned by this process and still valid;
2. reject a stale DB-only handle;
3. attach a fresh plugin when the workflow permits recreation;
4. update the database with new session/handle IDs;
5. never silently bind a stale persisted handle ID to a newly created JRTC session.

### 9.4 JRTC's own plugin registry

JRTC sessions already maintain live plugin ownership. Use that registry where appropriate:

```python
session.plugins.get(handle_id)
```

The Synq registry should add domain identity and ownership semantics rather than unnecessarily duplicating JRTC's internal structures.

---

## 10. Introduce a Synq-owned JRTC integration package

Recommended structure:

```text
src/apps/meetings/jrtc/
    __init__.py

    runtime.py
    config.py
    broker.py
    ownership.py
    handles.py
    errors.py

    events/
        __init__.py
        consumer.py
        dispatcher.py
        handlers.py
        idempotency.py
        schemas.py

    videoroom/
        __init__.py
        adapter.py
        rooms.py
        publisher.py
        subscriber.py
```

Exact filenames may be adjusted to existing repository conventions.

Do not scatter raw JRTC imports throughout models, tasks, serializers, namespace handlers, views, hooks, and middleware.

Domain code should prefer Synq-owned adapters.

---

## 11. VideoRoom adapter

Create a Synq-owned adapter around `jrtc-video`.

Suggested operations:

```text
get_session
create_room
room_exists
destroy_room
list_participants
kick_participant

attach_publisher
attach_subscriber

join_and_configure
publish
configure_publisher
unpublish

join_subscriber
update_subscription
start_subscriber

trickle
complete_trickle
hangup
detach
```

The adapter is responsible for:

- obtaining the correct process-local session;
- resolving/attaching the correct plugin;
- strict integer ID normalization;
- converting Synq input into JRTC/JRTC-video models;
- parsing JRTC-video responses;
- translating package exceptions into Synq domain exceptions;
- updating runtime ownership;
- updating persisted correlation IDs where appropriate;
- keeping JRTC-specific details out of frontend-facing code.

Do not expose `jrtc_video` Pydantic models directly in the browser/API contract.

---

## 12. Preserve `join_and_configure`

Do not blindly replace Synq's publisher flow with `VideoRoomService` if doing so degrades signaling semantics.

Synq currently benefits from the VideoRoom plugin's direct `join_and_configure(...)` operation.

That must remain available.

The agent may use lifecycle ideas from `VideoRoomService`, but must not mechanically convert every flow to the high-level service if the result:

- adds extra Janus round trips;
- changes negotiation ordering;
- breaks existing Socket.IO ACK semantics;
- creates publisher/subscriber lifecycle mismatches.

Behavioral parity takes precedence during migration.

---

## 13. Process-local JRTC session manager

JRTC 3.x intentionally uses process-local session ownership.

Do not reintroduce the old Redis leader/follower RPC architecture from earlier `janus_api` versions.

Each process that owns JRTC control connections owns:

```text
JanusSessionManager
Janus sessions
WebSocket transports
plugin instances
plugin registries
pending transaction Futures
```

These objects are not portable between processes.

### 13.1 Current runtime pattern

Synq's existing `JanusProcessRuntime` architecture is directionally valid:

- ASGI lifespan owns JRTC on the ASGI loop;
- sync/Celery contexts can use a dedicated background loop where appropriate;
- forked processes must reset inherited runtime state.

Keep this model unless a superior implementation preserves the same ownership guarantees.

### 13.2 Extend runtime ownership

The runtime must now own:

```text
event broker
JanusEventPublisher
JanusSessionManager
handle registry
process instance identity
```

Suggested lifecycle state:

```text
STOPPED
STARTING
RUNNING
STOPPING
FAILED
```

### 13.3 Startup order

```text
configure JRTC
    |
    v
build event broker
    |
    v
start JanusEventPublisher
    |
    v
construct JanusSessionManager(event_publisher=publisher)
    |
    v
start session manager
    |
    v
install optional compatibility Janus manager
    |
    v
mark Synq runtime RUNNING
```

### 13.4 Shutdown order

```text
stop accepting new Synq commands
    |
    v
stop Janus session manager / transports
    |
    v
drain JanusEventPublisher
    |
    v
stop publisher
    |
    v
shutdown broker
    |
    v
clear handle registry
    |
    v
mark runtime STOPPED
```

The Janus transport must stop producing new events before the publisher is drained.

---

## 14. JRTC event broker configuration

Create one application-level configuration abstraction.

Do not hard-code Redis.

Suggested settings:

```text
JRTC_EVENTS_ENABLED

JRTC_EVENT_BROKER_ENGINE
JRTC_EVENT_PHYSICAL_ROUTE

JRTC_EVENT_PUBLISH_WORKERS
JRTC_EVENT_PUBLISH_QUEUE_CAPACITY
JRTC_EVENT_PUBLISH_ADMISSION_TIMEOUT
JRTC_EVENT_PUBLISH_TIMEOUT

JRTC_EVENT_CONSUMER_CONCURRENCY
JRTC_EVENT_CONSUMER_CAPACITY

JRTC_REDIS_URL
JRTC_REDIS_MODE
JRTC_REDIS_GROUP
JRTC_REDIS_CONSUMER_NAME
JRTC_REDIS_MAX_LENGTH
JRTC_REDIS_CLAIM_IDLE_MS
JRTC_REDIS_CLAIM_INTERVAL

JRTC_RABBITMQ_URL
JRTC_RABBITMQ_EXCHANGE
JRTC_RABBITMQ_QUEUE
JRTC_RABBITMQ_DLX

JRTC_KAFKA_BOOTSTRAP_SERVERS
JRTC_KAFKA_GROUP_ID
JRTC_KAFKA_SECURITY_PROTOCOL
JRTC_KAFKA_SASL_MECHANISM
JRTC_KAFKA_USERNAME
JRTC_KAFKA_PASSWORD
```

Keep existing `JANUS_*` settings for Janus gateway/session behavior.

Do not rename all Janus settings to `JRTC_*`.

Examples that should remain conceptually `JANUS_*`:

```text
JANUS_SESSION_URL
JANUS_REQUEST_TIMEOUT
JANUS_SESSION_POOL_SIZE
JANUS_KEEPALIVE_INTERVAL
JANUS_KEEPALIVE_FAILURES
JANUS_SHUTDOWN_TIMEOUT
JANUS_DETACH_CONCURRENCY
JANUS_TOKEN
JANUS_API_SECRET
```

Those describe Janus connectivity, not application broker selection.

---

## 15. Recommended default event backend

For Synq's default production deployment, prefer:

```text
Redis Streams
```

Reasons:

- Synq already uses Redis;
- durable consumer groups;
- disconnect survival;
- pending-entry recovery;
- at-least-once semantics;
- relatively low additional operational complexity.

Do not use Redis Pub/Sub as the default authoritative production event transport because messages are lost during subscriber disconnects.

Redis Pub/Sub may remain supported for development, low-criticality deployments, or intentionally at-most-once semantics.

Kafka and RabbitMQ must remain first-class configurable options.

---

## 16. Dedicated Django event-consumer process

Prefer a separate process for authoritative broker consumption.

Suggested command:

```bash
python manage.py run_jrtc_events
```

Suggested deployment units:

```text
synq-web
synq-celery
synq-celery-beat
synq-jrtc-events
```

The JRTC event consumer should:

- initialize Django normally;
- build the configured Broka consumer;
- subscribe to `DEFAULT_PHYSICAL_ROUTE`;
- use a stable logical consumer-group/queue identity;
- use a unique live instance identity;
- process events asynchronously;
- handle graceful shutdown;
- expose metrics and health state.

Do not automatically run one authoritative durable event consumer inside every web worker unless there is a documented, tested operational reason.

---

## 17. Broker acknowledgement semantics

For durable backends, assume at-least-once delivery.

Preferred default is `AUTO` only if the handler does not return before its durable application work is complete.

Do not:

```python
async def handler(delivery):
    asyncio.create_task(do_database_work())
    return
```

That can acknowledge before the work commits.

Where stronger control is required, use manual acknowledgement.

The agent must understand each backend's semantics.

---

## 18. Idempotency is mandatory

Durable broker delivery can duplicate events.

Implement application-level idempotency.

Suggested database model:

```python
class JrtcEventReceipt(models.Model):
    event_id = models.UUIDField(unique=True)
    event_type = models.CharField(max_length=...)
    received_at = models.DateTimeField(auto_now_add=True)
```

A different durable store may be used if better aligned with the repository.

Required processing semantics:

```text
BEGIN DATABASE TRANSACTION
    |
    v
insert broker envelope ID
    |
    +--> already exists -> duplicate -> no side effects
    |
    v
apply domain state changes
    |
    v
COMMIT
    |
    v
ACK broker event
```

Do not claim exactly-once delivery.

The target is:

```text
at-least-once transport
+
idempotent application side effects
```

---

## 19. Handle ownership across workers

This must be explicit.

A live JRTC plugin belongs to the process/session/transport that created it.

Persist a runtime owner identity where useful, for example:

```text
runtime_owner_id
```

which may be a worker UUID, instance UUID, pod UUID, or process UUID.

### Initial recommended strategy

Use connection affinity where practical:

- a participant's active Socket.IO connection is handled by one web worker;
- that worker owns the participant's JRTC plugin handles;
- media commands for that live connection execute in that process.

### Do not implement a cross-process command bus unless required

A future architecture may route commands:

```text
arbitrary worker
    |
    v
command broker/RPC
    |
    v
owning worker
    |
    v
live VideoRoomPlugin
```

Do not add this complexity to the initial migration unless current Synq behavior demonstrably requires it.

Document the limitation and ownership contract.

---

## 20. Stale handles

Persisted Janus IDs are not proof of a live handle.

On process restart, transport loss, session replacement, or ownership mismatch:

```text
persisted handle ID
    !=
live plugin
```

Never construct:

```python
VideoRoomPlugin(
    session=new_session,
    plugin_id=old_persisted_handle_id,
)
```

unless JRTC/Janus has explicitly reclaimed the same valid session and ownership has been verified.

Default recovery:

```text
detect stale binding
    |
    v
invalidate runtime record
    |
    v
attach fresh plugin
    |
    v
store new session/handle IDs
```

---

## 21. Backend dependency migration

In `synq/pyproject.toml`, remove:

```text
janus-api-core
janus-videoroom-plugin
```

Add compatible constraints similar to:

```text
jrtc>=3.1,<4
jrtc-video>=3.0,<4
```

If TextRoom is genuinely used, add:

```text
jrtc-text>=3,<4
```

Do not add plugin packages merely because an old enum exists.

After dependency changes:

- regenerate the lock file;
- ensure old distributions disappear from the resolved environment;
- verify no transitive dependency accidentally reintroduces them;
- run import scans.

---

## 22. `services/janus.py` refactor

Current responsibilities are too broad.

Refactor into the new integration package while preserving compatibility imports where needed.

Required changes:

- replace `janus_api` imports with `jrtc`;
- replace `janus_videoroom_plugin` imports with `jrtc_video`;
- remove the numeric-ID runtime workaround because JRTC now preserves strict native integer IDs;
- keep a compatibility alias for historical migrations;
- configure JRTC through the correct configuration API;
- construct event broker/publisher;
- pass publisher into `JanusSessionManager`;
- extend runtime lifecycle to own publisher/broker;
- move response normalization to the Synq adapter;
- remove support for named/non-decimal Janus IDs;
- convert `janus_room_id_for_session` and related helpers to integer-only semantics;
- remove stale comments referring to the old core-v3 package implementation.

---

## 23. `core/models/fields/janus.py`

This module should be retired from active live-plugin materialization.

If temporarily retained:

- fix all incorrect `str`/`int` typing;
- use `int | None`, not `str | None`;
- rename `on_rx_event` concepts to `on_event`;
- do not use old package names in system-check IDs;
- remove incorrect `from_db_value` conversion to string;
- remove misleading error messages about raw string IDs.

Final target:

- active models use normal `PositiveBigIntegerField`;
- live JRTC plugins are resolved through the runtime registry;
- serializers explicitly convert between int and decimal string;
- the legacy custom field can eventually be deleted after migrations are safely squashed or made independent of it.

---

## 24. Model migration

Review and migrate Janus identifiers.

Likely targets include:

```text
MeetingSession.janus_room_id
Participant.janus_publisher_id
Participant.janus_private_id
ParticipantMediaHandle.janus_session_id
ParticipantMediaHandle.janus_handle_id
ParticipantStream.janus_feed_id
```

Prefer `PositiveBigIntegerField` where the value is a Janus-generated positive identifier.

Add appropriate indexes.

Consider a composite index on:

```text
(janus_session_id, janus_handle_id)
```

for event correlation.

Do not rewrite old migrations. Create new forward migrations.

---

## 25. Signaling service refactor

`MeetingMediaSignalService` is a critical migration hotspot.

### Replace package imports

Use `jrtc` and `jrtc_video` rather than old package modules.

### Preserve current signaling protocol

Do not rename browser Socket.IO command names unless necessary.

Existing commands such as:

```text
session_media_publish
session_media_unpublish
session_media_sync_subscriptions
session_media_start_subscriber
session_media_trickle
```

should remain stable.

### Strict ID handling

Remove mixed string/int flows.

Internal logic must use integers.

For JSON fields such as stored stream selections, parse feed IDs back to integers before constructing JRTC models.

Fix `SubscribeTarget(feed=...)`, `UnsubscribeTarget(feed=...)`, `PublisherJoinAndConfigureRequest(...)`, and `SubscriberJoinRequest(...)` to receive strict integers.

Do not assign decimal strings to integer model fields.

---

## 26. Async strategy

Do not mix the package migration with a complete async rewrite in one step.

### Phase 1

Preserve behavior.

It is acceptable for the initial migration to keep the existing pattern:

```text
async Socket.IO
    ->
sync service bridge
    ->
JanusProcessRuntime.run()
    ->
async JRTC
```

if tests confirm parity.

### Phase 2

After migration stability:

- convert media signaling operations to native async;
- directly `await` JRTC from the ASGI loop;
- use Django async ORM where mature and appropriate;
- isolate unavoidable sync ORM calls with narrow `sync_to_async` boundaries.

Do not force the phase-2 async rewrite into the initial compatibility migration.

---

## 27. Tasks/Celery

Preserve the existing rule:

**Celery must not attach or assume ownership of live participant plugin handles that belong to ASGI workers.**

Tasks may:

- prepare database rows;
- provision rooms through short-lived management handles;
- perform cleanup using server-side management operations;
- mark stale ownership;
- reconcile persistent state.

Tasks must not pretend a persisted participant handle ID is a portable Python plugin object.

### Cleanup

When cleaning up a handle owned by another process:

- prefer VideoRoom server management operations such as kick where appropriate;
- clear database ownership/state using `None`;
- do not reattach a foreign handle just to call detach.

---

## 28. Management handles

VideoRoom management handles for operations such as `create`, `exists`, `destroy`, `list_participants`, and `kick` should be short-lived.

Preferred pattern:

```python
async with VideoRoomPlugin(session=session) as plugin:
    ...
```

or a Synq adapter abstraction with equivalent attach/invoke/detach semantics.

Do not persist management plugin handles unless there is an explicit, justified requirement.

Existing legacy `control_handle_id` fields should be reviewed for deprecation/removal.

---

## 29. Remove Janus middleware coupling

The current middleware that globally injects a Janus session into every Django request is unnecessary.

Remove or deprecate:

```text
request.janus
request.janus_session
```

for ordinary HTTP requests.

Media/domain services should explicitly acquire JRTC through the integration layer.

Benefits:

- clearer ownership;
- fewer hidden side effects;
- easier multi-backend routing;
- less accidental session creation;
- better testability.

Do not remove middleware until all call sites are migrated and tests confirm no hidden consumers remain.

---

## 30. State serialization

Fix the backend/frontend ID boundary.

Backend state builders must stringify Janus IDs before sending to TypeScript.

Examples:

```text
janus_room_id
janus_publisher_id
janus_feed_id
plugin_id
```

must be strings in frontend-facing JSON.

Do not change TypeScript types to `number` merely because Python uses integers.

---

## 31. Frontend migration

The frontend should remain largely package-agnostic.

No direct `jrtc` package dependency should be introduced into `synq.js`.

Keep application-level contracts such as:

```text
MediaSignalAck
JsepPayload
JanusEventEnvelope
meeting/session state payloads
```

### Known frontend bug

Fix topology detection:

```text
joinings
```

to:

```text
joining
```

where the new JRTC VideoRoom event model uses the singular Janus field.

### Preserve peer connection behavior

`PublisherPeerConnection` and `SubscriberPeerConnection` should continue to:

- send local offers/answers through Synq Socket.IO commands;
- consume ACK JSEP from the command plane;
- consume unsolicited Janus events from the event plane;
- process trickle/hangup/media topology events.

Do not expose Broka or backend broker concepts to the browser.

---

## 32. Event normalization

Create one Synq-owned normalization layer.

It should convert broker/JRTC event payloads into the existing `JanusEventEnvelope` or a cleaned successor.

Responsibilities:

- preserve `janus`;
- preserve `transaction`;
- stringify `session_id` and `sender` for browser output;
- preserve JSEP;
- preserve `plugindata`;
- preserve trickle candidates;
- tolerate additive optional envelope fields;
- avoid leaking broker implementation details;
- avoid leaking credentials or internal topology.

The frontend should not care whether the backend is Redis, Kafka, or RabbitMQ.

---

## 33. Security

Treat Janus event payloads as sensitive.

They may contain SDP, ICE candidates, session IDs, handle IDs, plugin metadata, and participant media state.

Requirements:

- never log complete SDP in production by default;
- never log broker credentials;
- never expose Redis/RabbitMQ/Kafka endpoints to clients;
- use TLS/authentication appropriate to each broker;
- restrict broker ACLs to the exact required destination/topic/queue;
- separate producer and consumer permissions if supported;
- validate broker options;
- fail closed on unsupported security configuration;
- redact secrets from diagnostics;
- document retention policy for broker streams/topics.

---

## 34. Observability

Add structured metrics/logging for:

### JRTC runtime

```text
runtime state
session pool readiness
session replacement count
transport reconnects
active plugin handles
stale handle invalidations
```

### Event publisher

```text
admission count
drop count
queue depth
publish failures
queue latency
publish latency
```

Use JRTC's existing metrics where available rather than duplicating them.

### Event consumer

```text
consumer lag
handler duration
event type counts
duplicates
retries
dead letters
failed correlation
unknown handle events
Socket.IO forwarding failures
```

### Command plane

```text
method latency
Janus request timeout
Janus error response
JSEP negotiation failure
handle resolution failure
ownership mismatch
```

Do not use high-cardinality IDs as metric labels.

---

## 35. Error taxonomy

Translate infrastructure/package errors into stable Synq errors.

Suggested categories:

```text
JrtcRuntimeUnavailable
JrtcSessionUnavailable
JrtcHandleUnavailable
JrtcHandleOwnershipError
JrtcStaleHandleError
JrtcEventCorrelationError
JrtcBrokerUnavailable
JrtcBrokerPublishFailure
JrtcBrokerConsumerFailure
VideoRoomCommandError
VideoRoomProtocolError
```

Do not leak raw Broka/JRTC exceptions directly to browser clients.

Preserve detailed exception chaining in server logs.

---

## 36. Tests

Rewrite the existing old-package contract tests.

The new suite must test the architecture, not obsolete package names.

### Dependency tests

Assert:

```text
jrtc installed
jrtc-video installed
old janus-api-core absent
old janus-videoroom-plugin absent
```

### ID tests

Assert:

- JRTC rejects numeric strings;
- Synq uses int internally;
- DB uses bigint-compatible fields;
- browser JSON uses strings;
- bool is not accepted as a Janus ID;
- null semantics use `None`.

### Command-plane tests

Cover:

```text
join_and_configure
publish
configure_publisher
join_subscriber
update_subscription
start
trickle
hangup
```

Verify direct method return values resolve through transaction Futures.

### ACK tests

Verify:

```text
ACK does not prematurely complete wait_for_event=True plugin requests
```

### Broker publication tests

Assert only supported async Janus event types are externally published.

Do not assert that ordinary success/ACK/error responses are broker events.

### Duplicate negotiation tests

Given a transaction-correlated `janus.event` containing JSEP:

- command plane returns the JSEP once;
- broker consumer does not trigger duplicate browser negotiation.

### Unsolicited JSEP tests

Given an unsolicited Janus event with JSEP and no relevant command transaction:

- broker consumer forwards negotiation to the proper browser flow.

### Handle registry tests

Test:

- attach and register;
- resolve same handle;
- stale session detection;
- process owner mismatch;
- transport loss invalidation;
- detach removal;
- process restart behavior;
- no automatic stale-handle adoption.

### Event correlation tests

Map `(session_id, sender)` to `ParticipantMediaHandle` and verify unknown/stale events are safely handled.

### Idempotency tests

Send the same envelope ID multiple times. Assert domain side effects occur once.

### Broker backend contract tests

At minimum include reusable contract tests for memory, Redis Streams, RabbitMQ, and Kafka where CI/environment support exists.

Redis Pub/Sub tests should confirm its intentionally at-most-once behavior.

### Frontend tests

Verify:

- Janus IDs are strings;
- `joining` triggers topology refresh;
- command ACK JSEP still works;
- broker-forwarded unsolicited JSEP still works;
- trickle candidate shape remains compatible;
- publisher/subscriber lifecycle remains stable.

---

## 37. Integration and load testing

Before production rollout, test:

```text
high participant counts
rapid joins/leaves
renegotiation storms
ICE candidate bursts
broker slowdown
broker outage
Redis restart
RabbitMQ reconnect
Kafka rebalance
Janus restart
JRTC transport reconnect
ASGI rolling restart
consumer rolling restart
Celery restart
stale handle cleanup
duplicate broker delivery
poison events
consumer lag
graceful shutdown under load
```

Measure:

```text
event latency
command latency
queue depth
consumer lag
drop rate
duplicate rate
reconnect rate
handle recreation rate
Socket.IO delivery latency
```

---

## 38. Rollout strategy

Implement in controlled phases.

### Phase 0 — Baseline

- capture current behavior;
- make existing tests deterministic;
- document current Socket.IO protocol;
- document current DB state transitions.

### Phase 1 — Dependency and adapter foundation

- add `jrtc`;
- add `jrtc-video`;
- create integration package;
- implement strict ID helpers;
- implement VideoRoom adapter;
- retain current external behavior.

### Phase 2 — JRTC runtime migration

- migrate `JanusProcessRuntime`;
- configure JRTC settings;
- remove numeric ID shim from runtime;
- retain migration alias;
- preserve ASGI lifespan;
- preserve sync background bridge where still required.

### Phase 3 — Handle registry and model migration

- introduce runtime handle registry;
- migrate active models to bigint ID fields;
- stop materializing live plugins through ORM fields;
- keep event-correlation IDs.

### Phase 4 — Signaling migration

- move publisher/subscriber/trickle operations to VideoRoom adapter;
- preserve Socket.IO protocol;
- preserve direct command response semantics;
- enforce int-internal/string-wire policy.

### Phase 5 — Broker event plane

- construct `JanusEventPublisher`;
- pass it to `JanusSessionManager`;
- add `run_jrtc_events`;
- implement Dispio event dispatcher;
- implement state updates and Socket.IO forwarding;
- implement transaction-correlated JSEP deduplication;
- implement idempotency.

### Phase 6 — Frontend fixes

- fix `joining`;
- validate IDs remain strings;
- validate async event envelope compatibility.

### Phase 7 — Cleanup

- remove active `JanusPluginField`;
- remove Janus middleware;
- remove obsolete hooks;
- remove old package imports;
- remove obsolete settings;
- remove dead compatibility code no longer needed by migrations.

### Phase 8 — Async optimization

Only after parity:

- make signaling services native async;
- remove avoidable `sync_to_async -> sync -> run_coroutine_threadsafe` hops;
- benchmark before and after.

---

## 39. Prohibited shortcuts

The coding agent must not:

1. mechanically rename `janus_api` imports to `jrtc` and call the migration complete;
2. pass numeric strings into JRTC models;
3. convert frontend Janus IDs to JavaScript numbers;
4. use `Plugin.on_event` as Synq's authoritative cross-process event bus;
5. assume every internally dispatched JRTC response is published through Broka;
6. redesign command responses as broker RPC;
7. persist Python plugin objects;
8. reconstruct stale plugin handles merely from stored handle IDs;
9. attach participant handles in Celery if they belong to ASGI workers;
10. reuse an old handle ID on a different Janus session;
11. rewrite historical Django migrations;
12. hard-code Redis as the only supported event backend;
13. use Redis Pub/Sub as the default durable production event mechanism;
14. acknowledge broker events before database work commits;
15. claim exactly-once semantics;
16. emit a transaction-correlated JSEP to the browser twice;
17. expose JRTC/Broka implementation details to `synq.js`;
18. introduce `jrtc` directly into the TypeScript client;
19. retain the old `NativeJanusIdVideoRoomPlugin` runtime workaround;
20. store empty strings in nullable integer fields;
21. use the database as the live plugin registry;
22. make ordinary Django HTTP requests implicitly acquire Janus sessions;
23. implement cross-process JRTC command routing unless required by a concrete current workflow;
24. mix a full async rewrite into the first compatibility migration without tests;
25. remove compatibility symbols still required to reconstruct historical migrations.

---

## 40. Documentation requirements

Every new major module must have comprehensive documentation.

Document:

```text
module purpose
ownership
event-loop assumptions
process assumptions
startup/shutdown lifecycle
public classes
public functions
settings
error semantics
retry semantics
idempotency
security assumptions
backend-specific behavior
failure recovery
testing strategy
```

Update developer documentation with:

1. target JRTC architecture;
2. command-plane diagram;
3. event-plane diagram;
4. broker configuration examples;
5. Redis Streams deployment;
6. RabbitMQ deployment;
7. Kafka deployment;
8. local development mode;
9. `run_jrtc_events` usage;
10. troubleshooting;
11. metrics;
12. stale-handle recovery;
13. rolling restart behavior;
14. browser ID serialization contract.

---

## 41. Architecture diagrams

### Command plane

```text
synq.js
   |
   | Socket.IO media command
   v
MeetingNamespace
   |
   v
MeetingMediaSignalService
   |
   v
Synq VideoRoom Adapter
   |
   v
JRTC Handle Registry
   |
   v
live VideoRoomPlugin
   |
   v
JRTC transaction machinery
   |
   v
Janus Gateway
   |
   v
transaction-correlated response
   |
   v
pending Future
   |
   v
VideoRoomReply
   |
   v
Socket.IO ACK
   |
   v
synq.js
```

### Event plane

```text
Janus Gateway
   |
   v
JRTC transport
   |
   v
JanusResponseDispatcher
   |
   v
JanusEventPublisher
   |
   v
Broka
   |
   +--> Redis Streams
   +--> Redis Pub/Sub
   +--> RabbitMQ
   +--> Kafka
   |
   v
janus.events
   |
   v
Synq JRTC Event Consumer
   |
   v
Dispio logical dispatcher
   |
   +--> state reconciliation
   +--> event correlation
   +--> idempotency
   +--> Socket.IO forwarding
   |
   v
synq.js
```

### Local JRTC lifecycle path

```text
JRTC transport
   |
   v
WebsocketSession
   |
   v
PluginManager
   |
   v
Plugin local event queue
   |
   v
Plugin.on_event
```

This third path is process-local and is not the Synq application event bus.

---

## 42. Definition of done

The migration is complete only when all of the following are true:

- `synq` no longer depends at runtime on `janus-api-core`;
- `synq` no longer depends at runtime on `janus-videoroom-plugin`;
- `jrtc` is the core Janus runtime;
- `jrtc-video` is the VideoRoom implementation;
- all Janus protocol IDs are integers internally;
- all browser-facing Janus IDs are strings;
- `JanusPluginField` is no longer used to materialize live plugin instances;
- a process-local handle registry owns live JRTC plugin mappings;
- stale persisted handles cannot be silently adopted;
- JRTC session ownership is process-local and explicit;
- `JanusEventPublisher` is configured;
- the configured broker backend receives supported async Janus events;
- Django has an explicit Broka consumer for `janus.events`;
- logical event routing uses `delivery.envelope.type`;
- durable event processing is idempotent;
- transaction-correlated JSEP does not cause duplicate browser negotiation;
- unsolicited async JSEP still reaches the appropriate client flow;
- command methods still return direct JRTC/JRTC-video responses;
- Celery does not own ASGI participant handles;
- frontend signaling command names remain stable unless intentionally versioned;
- frontend topology logic uses `joining`, not `joinings`;
- historical Django migrations remain valid;
- migration and integration tests pass;
- broker outage/reconnect tests pass;
- rolling restart tests pass;
- documentation is updated.

---

## 43. Coding-agent execution guidance

Before editing:

1. inspect the current `v4` backend tree;
2. inspect the current `codex/new-ui-implementation` frontend tree;
3. identify all old `janus_api` / `janus_videoroom_plugin` imports;
4. identify every `JanusPluginField` use;
5. identify all historical migration references;
6. identify all Janus ID serialization/deserialization paths;
7. identify all `plugin_callback_factory` / `on_rx_event` / `on_event` usage;
8. identify all Celery code touching plugin handles;
9. identify all frontend assumptions about Janus topology events;
10. run the existing tests before making changes.

Then implement the migration in small, reviewable commits.

Prefer this order:

```text
dependency changes
-> adapter foundation
-> runtime migration
-> handle registry
-> model migration
-> signaling migration
-> event publisher/consumer
-> frontend fixes
-> tests
-> cleanup
-> async optimization
```

At every phase:

- preserve existing behavior first;
- add tests before removing compatibility code;
- document any intentional behavior change;
- do not move to the next phase with unexplained failing tests.

---

## 44. Quality bar

The final implementation must be:

```text
production-grade
async-safe
process-safe
type-safe
well-tested
observable
fault-tolerant
secure-by-default
backend-configurable
idempotent
documented
scalable
performant
maintainable
```

Favor explicit ownership and narrow interfaces over framework magic.

The most important conceptual rule for this migration is:

> **Persistent Django state, live JRTC plugin ownership, synchronous command responses, and asynchronous broker events are four different concerns and must remain separate.**

That separation is the foundation of the new Synq/JRTC architecture.
