# AGENTS.md — Synq v4 Async Signaling, ICE Fast Path, and JRTC Integration Upgrade

## 0. Mission

This repository owns Synq's meeting application/control plane.

Refactor the meeting realtime/JRTC integration so that:

- Socket.IO/ASGI media signaling is async-first;
- ordinary ORM work in async code uses Django's async ORM APIs where appropriate;
- synchronous transaction/locking code is isolated into small transactional islands;
- JRTC calls are directly awaited on the ASGI/JRTC owner loop;
- ICE trickle supports frontend candidate batches correctly;
- final candidate batches are never discarded;
- empty candidate arrays never accidentally signal ICE completion;
- high-frequency ICE trickle avoids unnecessary durable DB locks/writes;
- process-local live-handle registry lookups become the common path;
- state-changing media commands retain correctness fencing;
- JRTC/jrtc-video package improvements are consumed rather than reimplemented;
- database/event/broker work cannot block the Janus command path;
- realtime event payloads contain enough identity metadata for the frontend sound-cue policy.

Target audited branch: `v4`.

If the current working branch has advanced, apply the architectural rules to the current implementations rather than restoring old code.

---

# PART I — ARCHITECTURAL BOUNDARY

## 1. Preserve the media-plane/control-plane split

WebRTC RTP/RTCP media is:

```text
browser <-> Janus
```

Synq is not the media relay.

Synq owns:

- authentication;
- meeting state;
- permissions;
- participant/session persistence;
- Socket.IO signaling;
- handle ownership/generation;
- JRTC integration;
- normalized domain/realtime events.

Therefore optimize the Python control/signaling path. Do not introduce server-side media forwarding into these services.

---

## 2. Primary code targets

Audit and modify, where appropriate:

- `src/apps/meetings/realtime/namespace.py`
- `src/apps/meetings/services/signaling.py`
- `src/apps/meetings/services/janus.py`
- `src/apps/meetings/jrtc/runtime.py`
- `src/apps/meetings/jrtc/handles.py`
- `src/apps/meetings/jrtc/videoroom/adapter.py`
- `src/conf/asgi.py`
- `src/conf/settings/base.py`
- meeting/JRTC tests, especially:
  - `src/apps/meetings/tests/test_jrtc_signaling_contracts.py`
  - `src/apps/meetings/tests/test_jrtc_contracts.py`
  - persistence/lifecycle/concurrency tests.

Do not create a second parallel media signaling stack. Refactor the current one.

---

# PART II — ASYNC-FIRST SIGNALING

## 3. Current anti-pattern to remove

The current media path is conceptually:

```text
Socket.IO async handler
    -> sync_to_async(whole service)
    -> worker thread
    -> ORM/transaction
    -> janus_runtime.run(...)
    -> run_coroutine_threadsafe(...)
    -> ASGI/JRTC owner loop
    -> JRTC
    -> return to worker thread
    -> ORM persistence
    -> return to ASGI handler
```

This causes avoidable:

- event-loop/thread context switches;
- thread-pool pressure;
- latency under signaling concurrency;
- complexity around runtime ownership.

Target:

```text
Socket.IO async handler
    -> async MeetingMediaSignalService
    -> async ORM where safe
    -> narrow sync transaction island where required
    -> await VideoRoomAdapter/JRTC directly
    -> narrow persistence transaction island where required
    -> Socket.IO response
```

---

## 4. Convert `MeetingMediaSignalService` to async-first orchestration

Media methods such as:

- publish/configure;
- unpublish;
- subscription sync/update;
- subscriber start;
- trickle;
- relevant handle lifecycle operations;

should become `async def` orchestration entry points.

Socket.IO handlers should call:

```python
return await MeetingMediaSignalService.trickle(...)
```

instead of:

```python
return await sync_to_async(MeetingMediaSignalService.trickle)(...)
```

The same applies to the other media commands.

Do not wrap an entire media workflow in `sync_to_async()` just because one step needs `transaction.atomic()`.

---

## 5. Await JRTC directly from ASGI media workflows

The JRTC runtime lifespan already belongs to the ASGI process/event-loop architecture.

Realtime media workflows should not bounce into a worker thread and then use:

```python
janus_runtime.run(...)
```

to come back to the owner loop.

Prefer direct native calls to async adapter/service methods:

```python
response = await adapter.trickle(...)
```

or an equivalent async Synq integration API.

Retain a synchronous compatibility façade only for genuinely synchronous callers such as:

- selected Celery tasks;
- management commands;
- migrations/legacy code;

when still required.

Realtime Socket.IO code must use the async path.

---

# PART III — DJANGO ASYNC ORM RULES

## 6. Use async ORM for ordinary execution

From async Socket.IO/ASGI code use Django's asynchronous ORM methods when transactional row-lock semantics are not required.

Examples:

```python
participant = await (
    Participant.objects
    .select_related("session", "room", "profile")
    .aget(pk=participant_id)
)
```

```python
connection = await (
    ParticipantConnection.objects
    .select_related(
        "participant",
        "participant__session",
        "session",
    )
    .aget(socket_id=sid)
)
```

Use, as appropriate:

- `aget`;
- `afirst`;
- `aexists`;
- `acount`;
- `acreate`;
- `aupdate`;
- `adelete`;
- `asave`;
- async iteration.

---

## 7. Query construction remains normal

Methods that construct a QuerySet but do not execute SQL remain normal:

```python
qs = (
    ParticipantConnection.objects
    .filter(...)
    .select_related(...)
)
connection = await qs.aget(...)
```

Do not invent async versions of lazy QuerySet-building methods.

---

## 8. Prevent lazy related-object queries in async code

Async code must not accidentally access a relation that triggers a synchronous lazy query.

Use deliberate:

- `select_related`;
- `prefetch_related`.

Before changing a loader, inspect every related object used later in the async workflow.

For hot-path media objects, prefer fetching the complete small relationship graph in one query over hidden follow-up queries.

---

## 9. Keep transactions as small synchronous islands

Django's Python-level transaction management/`select_for_update()` remains a synchronous island in this architecture.

Keep synchronous helpers similar to:

```python
@sync_to_async
def claim_media_generation(...):
    with transaction.atomic():
        ...
```

or use `database_sync_to_async`/the project-approved equivalent if that is the established convention.

The helper must do only the transaction/locking work.

Do not perform:

- Janus/JRTC network I/O;
- broker publication;
- Socket.IO emission;
- sleep/retries;
- expensive serialization;

while holding the transaction open.

---

## 10. Do not mechanically replace every ORM call

Code requiring:

- `transaction.atomic`;
- `select_for_update`;
- multi-row invariants;
- compare-and-set generation ownership;
- application-level transaction callbacks;

must remain in a properly scoped synchronous transaction helper until Django provides safe native semantics for that operation.

Correctness fencing is more important than eliminating every thread hop.

---

## 11. Async ORM caveat

Even when Django's public async ORM API is used, current Django implementations may internally bridge to synchronous database execution.

The goal here is still valid:

- keep application orchestration async;
- remove coarse app-owned `sync_to_async` around whole workflows;
- allow native async JRTC calls;
- keep ORM execution boundaries explicit;
- future-proof the code.

Do not claim that `aget()` guarantees zero internal thread transitions.

---

# PART IV — DATABASE CONNECTION POLICY

## 12. Preserve `CONN_MAX_AGE=0` for ASGI

The existing default:

```python
"CONN_MAX_AGE": 0
```

is appropriate for the async deployment model.

Do not increase it as an ad-hoc performance fix.

Instead review PostgreSQL/psycopg pooling.

---

## 13. Configure and benchmark a real DB pool if required

The project already has psycopg pool capability in its dependency direction.

Audit whether pooling is actually enabled/configured in deployment.

Tune:

- minimum pool size;
- maximum pool size;
- acquisition timeout;
- idle lifetime;
- worker/process count;
- database server max connections.

Measure under realistic simultaneous meeting signaling load.

Do not allow each ASGI worker to create an unbounded pool.

---

# PART V — ICE BATCHING CORRECTNESS

## 14. Canonical frontend-to-backend payload

The new preferred Socket.IO payload is:

```json
{
  "session_id": "<uuid>",
  "handle_type": "publisher",
  "candidates": [
    {
      "candidate": "...",
      "sdpMid": "0",
      "sdpMLineIndex": 0
    }
  ],
  "completed": false
}
```

The same contract applies to subscriber handles.

`completed=true` means:

> Process every candidate contained in this message first, then signal end-of-candidates for this handle/ICE generation.

That semantic rule is mandatory.

---

## 15. Fix the current final-batch bug

Current logic conceptually does:

```python
if completed or not serialized_candidates:
    complete_trickle()
else:
    trickle(serialized_candidates)
```

This drops the final queued candidate batch if the frontend sends:

```json
{
  "candidates": [A, B, C],
  "completed": true
}
```

Replace it with independent actions:

```python
if serialized_candidates:
    await trickle(serialized_candidates)

if completed:
    await complete_trickle()
```

The ordering MUST be:

```text
final candidates
    -> Janus trickle(candidates=[...])
    -> Janus completed marker
```

Never completion first.

---

## 16. Empty batch is not completion

This payload:

```json
{
  "candidates": [],
  "completed": false
}
```

MUST NOT cause `complete_trickle()`.

Prefer rejecting it as an invalid/no-op signaling request:

```python
if not candidate_payloads and not completed:
    raise VideoRoomProtocolError(
        "ICE trickle requires at least one candidate or completed=true."
    )
```

A pure completion payload is valid:

```json
{
  "candidates": [],
  "completed": true
}
```

and should call only `complete_trickle()`.

---

## 17. Normalize singular legacy input safely

During migration, the Socket.IO boundary may continue accepting the old shape:

```json
{
  "candidate": { ... }
}
```

Do not rely on:

```python
list(candidate_dict)
```

because it produces dictionary keys.

Normalize explicitly:

```python
def normalize_candidates(value):
    if value is None:
        return []
    if isinstance(value, dict):
        return [value]
    if isinstance(value, (str, bytes)):
        raise VideoRoomProtocolError(...)
    return list(value)
```

Longer term, make the service contract plural-only:

```python
Sequence[IceCandidate] | None
```

and keep singular compatibility only in the Socket.IO boundary.

---

## 18. Reject malformed batches atomically

Current batch serialization skips malformed entries and forwards valid ones.

Change this.

A malformed batch should be rejected with the failing index:

```text
Invalid ICE candidate at index 3: ...
```

Do not silently discard an entry because it might be the only:

- relay/TURN candidate;
- server-reflexive candidate;
- useful candidate for a network path.

Atomic rejection makes network-specific failures diagnosable.

---

## 19. Preserve candidate order

The order received from the frontend must be the order forwarded to JRTC.

Do not:

- sort;
- deduplicate based on candidate string unless deliberately specified later;
- parallelize individual candidates;
- split a batch into per-candidate JRTC calls.

One frontend batch should normally become one JRTC `trickle(sequence)` call.

---

## 20. Add a Synq defensive batch limit

JRTC has a much larger protocol safety ceiling.

Synq should define an application limit such as:

```python
MEETING_ICE_MAX_BATCH_SIZE = 32
```

or `64` after benchmark evidence.

The frontend default is expected to be much smaller, around 16.

Validate the batch size before expensive conversion/JRTC calls.

Do not blindly mirror JRTC's 256 hard ceiling as the normal frontend limit.

---

## 21. Decide the `usernameFragment` contract explicitly

Synq's ICE typed shape currently acknowledges browser `usernameFragment`, but JRTC's canonical candidate model does not currently forward it.

Choose one:

1. remove `usernameFragment` from the Synq internal canonical model; or
2. accept it at the browser boundary but document that it is not forwarded.

Do not imply end-to-end support for a field that is silently discarded.

Do not extend JRTC protocol models without confirming Janus wire expectations.

---

# PART VI — ICE FAST PATH

## 22. Treat ICE trickle as ephemeral transport signaling

ICE differs from stateful commands.

Stateful commands include:

- publish;
- configure;
- unpublish;
- subscriber join/update;
- subscriber start;
- detach;
- ownership handoff.

These should retain durable generation fencing where required.

ICE candidates/completion are:

- high frequency;
- ephemeral;
- not durable application state.

Do not use the full durable command-claim/persist cycle for every ICE batch once the fast path is implemented.

---

## 23. Target ICE path

Healthy steady-state path:

```text
Socket.IO handler
    -> validate active participant/connection
    -> resolve live binding from process-local registry
    -> verify binding generation/owner/session/handle/connection
    -> await JRTC trickle(batch)
    -> return lightweight acknowledgement
```

No row lock should be required on the ordinary happy path.

Use durable DB reconciliation when:

- live registry miss;
- generation mismatch;
- stale handle;
- runtime restart;
- ownership handoff;
- session recovery.

---

## 24. Do not update `last_event_at` on every ICE batch

The current path writes activity around every trickle operation.

Replace with one of:

- coalesced persistence every 2–5 seconds;
- existing socket heartbeat;
- last-write threshold:

```python
if now - last_persisted_activity >= threshold:
    schedule/update activity
```

Do not lose lifecycle observability; simply avoid turning ICE into write amplification.

Benchmark the threshold.

---

## 25. Keep a state-command fence separate from ICE

Conceptual per-handle model:

```text
media handle
├── state_command_lock/fence
│   ├── publish
│   ├── configure
│   ├── subscription update
│   ├── start
│   └── detach
└── ICE fast path
    ├── batch
    ├── batch
    └── completed
```

Do not let a redesign allow detach to race unsafely with a trickle on a destroyed handle.

The process-local registry/generation check must still make stale bindings fail safely.

---

# PART VII — LIVE HANDLE REGISTRY FAST PATH

## 26. Optimize `ensure_participant_media_plugin`

The current robust DB claim/finalize flow is valuable for creation/recovery but expensive as a mandatory resolver.

Add a healthy-binding fast path:

1. lookup by media-handle/domain ID in `JrtcHandleRegistry`;
2. verify:
   - owner ID;
   - Janus session ID;
   - Janus handle ID;
   - participant connection generation;
   - selected session health;
3. if all match, return immediately;
4. fall back to durable claim/reconciliation only on miss/mismatch/stale state.

Target:

```text
common path: registry -> JRTC
recovery path: DB claim -> attach/reconcile -> registry -> DB finalize
```

Do not weaken cross-process ownership checks.

---

# PART VIII — VIDEO ROOM MANAGEMENT PATH

## 27. Consume `jrtc-video` reusable management service

Once `jrtc-video` implements a lazy persistent management handle, Synq must stop attaching a fresh temporary VideoRoom management plugin for every command.

Refactor `VideoRoomAdapter.management_command()` or its replacement to use the reusable `VideoRoomService`.

Rules:

- one service/management handle per selected Janus session/generation;
- invalidate it when that JRTC session is replaced/lost;
- close services during JRTC runtime shutdown;
- do not create Synq's own duplicate management-plugin abstraction if `jrtc-video` owns it.

Participant publisher/subscriber handles remain in Synq's registry as before.

---

# PART IX — JRTC EVENT PLANE INTEGRATION

## 28. Keep JRTC command transport independent from Broka/event backpressure

JRTC will be upgraded so its WebSocket reader uses non-blocking global event admission.

Synq must not undo this by installing callbacks that synchronously:

- write Django rows;
- publish to Broka;
- call Redis/Kafka/RabbitMQ;
- emit expensive Socket.IO snapshots;
- run analytics.

Application event processing belongs in background/event consumer workers.

The direct Janus command path must remain low-latency.

---

## 29. Normalize event work outside transport callbacks

If Synq consumes JRTC events:

```text
JRTC background event publisher
    -> Broka/event route
    -> Synq consumer
    -> persistence/domain side effects
```

or another bounded async application queue may be used.

Never make the Janus WebSocket reader wait for a durable Synq event receipt.

---

# PART X — SOUND-CUE SUPPORT AT THE BACKEND EVENT CONTRACT

## 30. Sound playback remains frontend-owned

Do not play or select audio assets in Django.

Synq backend's only responsibility is to emit sufficiently rich normalized realtime events.

The frontend needs semantic events for:

- join request received;
- participant joined;
- reaction received;
- message received;
- participant left;
- meeting ended.

---

## 31. Ensure event payloads identify actor/source and event identity

For frontend suppression/deduplication, the relevant normalized event should include, where applicable:

- stable event ID or sequence/correlation ID;
- session ID;
- actor participant/profile ID;
- target participant/profile ID when meaningful;
- created/observed timestamp;
- event type;
- whether the event is a live realtime event rather than an initial state snapshot.

Do not add duplicate bespoke "play_sound" events.

The frontend should derive sound policy from normal meeting events.

---

## 32. Backend must not generate replay sound semantics

State hydration/reconnect snapshots are not "new notification" events.

Ensure the frontend can distinguish:

- initial/current state;
- replayed/recovered event history;
- newly delivered live event.

Prefer normal event metadata/sequence state over a sound-specific backend flag.

---

# PART XI — SESSION RECOVERY

## 33. Use notification-first JRTC recovery

Coordinate with JRTC changes:

```text
transport loss
    -> session marked lost
    -> manager owns recovery/replacement
    -> Synq runtime/registry invalidates affected bindings
```

Periodic sweeps remain safety nets.

Avoid two Synq components independently attempting to recreate the same JRTC session.

---

## 34. Keep ownership generation explicit

Whenever a JRTC session is replaced:

- invalidate registry bindings for the old session generation;
- clear/reconcile durable handle ownership;
- lazily recreate participant handles when needed;
- invalidate cached `jrtc-video` management service for that session;
- do not silently trust persisted Janus handle IDs from the dead generation.

---

# PART XII — SESSION POOL

## 35. Do not blindly increase `JANUS_SESSION_POOL_SIZE`

The default pool size of 1 is not automatically a bottleneck because a WebSocket can correlate multiple outstanding transactions.

First implement:

1. async-first signaling;
2. ICE batching and fast path;
3. non-blocking JRTC event ingress;
4. management-handle reuse;
5. live-registry fast paths.

Then benchmark:

```text
1 vs 2 vs 4 sessions
```

under the same meeting workloads.

Prefer horizontal process/Janus scaling over an arbitrarily large local session pool.

---

# PART XIII — LOGGING

## 36. Hot-path logging policy

Do not log full:

- SDP offers/answers;
- ICE candidate strings;
- Janus envelopes;
- tokens/secrets.

Use structured fields:

- meeting/session domain ID;
- participant ID;
- connection ID;
- media handle ID;
- Janus session ID;
- Janus handle ID;
- handle type;
- operation;
- candidate batch size;
- completion flag;
- duration;
- result/error class.

For ICE specifically log metadata such as:

```text
batch_size=12
completed=false
handle_type=publisher
```

not the candidates.

---

# PART XIV — SOCKET.IO ICE BOUNDARY

## 37. Replace clever fallback expression with explicit normalization

Current handler compatibility logic should become an explicit function with tests.

Required accepted migration forms:

### Preferred

```json
{
  "candidates": [ ... ],
  "completed": false
}
```

### Legacy temporary compatibility

```json
{
  "candidate": { ... },
  "completed": false
}
```

### Completion only

```json
{
  "candidates": [],
  "completed": true
}
```

Reject ambiguous invalid shapes.

Add a deprecation path for singular `candidate` after frontend rollout.

---

# PART XV — TEST REQUIREMENTS

## 38. ICE contract tests

Add tests proving:

1. one candidate -> one JRTC `trickle` invocation;
2. 16 candidates -> one JRTC `trickle` invocation containing 16 models;
3. candidate order is preserved;
4. final batch of 3 + `completed=True` -> `trickle(3)` then `complete_trickle()`;
5. empty + `completed=True` -> only `complete_trickle()`;
6. empty + `completed=False` -> validation error and never completion;
7. malformed element -> whole batch rejected with failing index;
8. oversized batch -> rejected before JRTC;
9. publisher and subscriber handles both work;
10. old singular `candidate` Socket.IO payload normalizes correctly during migration;
11. completion cannot overtake the final candidate batch.

---

## 39. Async boundary tests

Prove that realtime media handlers:

- do not wrap whole `MeetingMediaSignalService` methods in `sync_to_async`;
- directly await async media services;
- directly await JRTC adapter operations;
- use transaction helpers only where necessary.

Where practical, use mocks that fail if `janus_runtime.run()` is called from the async realtime path.

---

## 40. Transaction correctness tests

Do not lose existing generation/ownership safety.

Test races:

- publish vs disconnect;
- subscriber update vs detach;
- ownership handoff;
- JRTC session loss during a stateful command;
- stale binding during ICE;
- new connection superseding old generation.

Stateful commands must not become unsafe merely because ICE gets a fast path.

---

## 41. Registry fast-path tests

Test:

- healthy matching registry binding -> no durable handle-resolution transaction;
- owner mismatch -> fallback/error;
- session mismatch -> invalidate/fallback;
- connection generation mismatch -> reject;
- lost session -> no stale invocation;
- registry miss -> recovery path works.

---

## 42. Event-contract tests for frontend sound policy

For the relevant domain/socket events verify enough metadata exists to let the frontend determine:

- who initiated the event;
- whether current user is the initiator;
- stable event identity for deduplication;
- session identity;
- live event vs hydration/replay context where necessary.

Do not test actual sound playback in backend tests.

---

# PART XVI — PERFORMANCE TESTS

## 43. Compare old vs new signaling path

Measure:

- media command p50/p95/p99 latency;
- thread-pool queueing;
- number of thread crossings if observable;
- DB queries per operation;
- DB writes per operation;
- Janus requests per ICE gathering cycle;
- event-loop lag.

---

## 44. ICE benchmark matrix

For a representative gathered candidate count, compare:

```text
per-candidate
batch size 4
batch size 8
batch size 16
batch size 32
```

Track:

- Socket.IO requests;
- Synq handler CPU;
- DB queries/writes;
- JRTC calls;
- Janus trickle transactions;
- time to ICE connected;
- failure rate under TURN-only/network-restricted scenarios.

The goal is not simply fewer requests. Connection establishment latency must not regress materially.

---

## 45. DB pool benchmark

Under concurrent meetings test configured pool sizes while tracking:

- acquisition latency;
- open connections;
- DB CPU;
- request latency;
- pool timeouts.

Do not increase pool limits without evidence.

---

# PART XVII — IMPLEMENTATION ORDER

Implement in this order:

1. Add ICE regression tests for final-batch loss and empty-batch completion.
2. Fix ICE batch/completion correctness and payload normalization.
3. Convert Socket.IO media handlers + `MeetingMediaSignalService` to async-first.
4. Convert safe ordinary ORM operations to async ORM methods.
5. Isolate synchronous transaction/locking islands.
6. Replace realtime `janus_runtime.run()` bridges with direct awaits.
7. Implement live-registry healthy-binding fast path.
8. Implement ICE no-durable-claim fast path and coalesced activity persistence.
9. Consume JRTC non-blocking event ingress changes.
10. Consume `jrtc-video` persistent management service.
11. Add notification-driven recovery integration.
12. Review PostgreSQL pooling.
13. Benchmark JRTC session pool 1/2/4.
14. Profile serialization/logging only after structural fixes.

---

# PART XVIII — DEFINITION OF DONE

The Synq upgrade is complete only when:

- media Socket.IO handlers are async-first;
- entire media services are no longer wrapped in `sync_to_async`;
- safe ORM execution uses Django async APIs;
- transaction/row-lock work remains small and synchronous;
- JRTC calls are awaited directly on the async path;
- final ICE candidate batches cannot be lost;
- empty batches cannot accidentally mark ICE complete;
- one frontend candidate batch becomes one JRTC batch call;
- ordinary ICE no longer requires a durable DB claim/write cycle;
- stateful commands retain generation/ownership correctness;
- healthy handle lookup uses the live registry fast path;
- management commands consume `jrtc-video` reusable management handles;
- broker/event pressure cannot block the Janus reader;
- sound-related realtime events carry sufficient actor/event metadata;
- `CONN_MAX_AGE=0` remains the default;
- tests cover concurrency, recovery, and batch semantics;
- performance evidence accompanies the implementation.
