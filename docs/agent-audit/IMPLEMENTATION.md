# Implementation review — 2026-10-08

S-T01 through S-T05 have concrete implementations and local regression evidence,
ready for maintainer review. S-T06 remains blocked on live integration and load
acceptance. This report supersedes the implementation status in the historical
2026-10-07 audit; it does not claim production readiness or maintainer acceptance.

## Behavior

- Realtime publish, subscribe, start, unpublish and trickle directly await the
  runtime owner loop. Only named DB operations cross the synchronous bridge;
  claim/network/finalize and compensation fences remain intact. Worker compatibility
  entry points still use runtime.run; realtime tests forbid that bridge.
- ICE validation is atomic, before authorization/resource side effects: 32 entries,
  64 KiB compact UTF-8 request JSON, strict boolean completion, no ambiguous
  singular/plural input, and no empty incomplete batch. Final candidates precede
  explicit completion under one live-binding invocation fence. Authorization is
  checked again after candidate ACK before completion.
- Healthy ICE reuses the registry binding without claims or activity writes. The
  existing heartbeat owns activity updates. A registry miss cannot adopt a durable
  Janus ID. Bindings also check the actual session generation.
- Management commands reuse the installed VideoRoomService per actual session and
  generation. Replacement creates a new service. Runtime shutdown owns a shielded
  cleanup task and finishes service cleanup before manager stop and publisher drain,
  even when its caller is cancelled repeatedly.
- Semantic event metadata identifies live events; all state responses are snapshots.
  Committed participant presence events provide a stable MeetingEvent identity.
  Admission review echoes precede reconciliation snapshots. JRTC loss observers
  only set one coalescing event; an owned worker performs conditional durable cleanup.
  JRTC remains the sole recovery owner.
- A forward migration aligns the already-declared positive-ID checks, outbox status
  and retry index with the database. Receipt pruning locks terminal records and
  preserves unfinished outbox delivery. Historical migrations are unchanged.

## Reproduction and results

Final consumer environment: Python 3.14.8, Django 6.0.7, Broka 0.0.2, Dispio 0.0.2,
LogVista 0.1.0, python-socketio 5.16.3, JRTC 3.2.0, jrtc-video 3.0.3. The uv lock
pins the reviewed Git commits, which uv built and installed into site-packages.
The observer and service close APIs were verified on those installed artifacts.

```sh
uv sync --frozen --python 3.14
uv run python src/manage.py test apps.meetings.tests.test_jrtc_signaling_contracts apps.meetings.tests.test_jrtc_contracts apps.meetings.tests.test_jrtc_persistence_contracts apps.meetings.tests.test_runtime_lifecycle_contracts apps.meetings.tests.test_jrtc_event_plane apps.meetings.tests.test_realtime_namespace apps.meetings.tests.test_ice_batches apps.meetings.tests.test_async_media apps.meetings.tests.test_service_lifecycle --settings=conf.test_settings --noinput
uv run python src/manage.py test apps.meetings.tests --settings=conf.test_settings --noinput
uv run python src/manage.py makemigrations --check --dry-run --settings=conf.test_settings
```

- Required contracts plus added regressions: 91 passed.
- Entire meetings suite: 166 ran, 159 passed, seven existing startup-file tests failed.
  The source branch is missing scripts/supervisor.ps1, scripts/start.ps1 and
  src/Procfile; the failures were not hidden or changed to passes.
- Migration consistency: no changes detected.
- Real ORM/registry/adapter tests exercise all media actions. Both publisher and
  subscriber final ICE batches make **four service-level reads and zero writes**
  (handle, authorization, final authorization, stream ACK). Socket authentication
  adds its own reads; this number is not a whole HTTP/socket request budget.
- Actual installed service/plugin integration: five management commands reuse one
  attach; a replacement object with the same numeric ID receives a new service;
  repeated shutdown cancellation cannot stop the manager before detach completes.
- A 1,000-notification observer burst retains one registration and one worker/event.
  A durable cleanup CAS cannot clear a replacement handle. Presence emits only
  after commit, and snapshots carry an explicit snapshot marker.
- A SQLite/locmem test configuration isolates external infrastructure. It also
  disables the pre-existing conf.logging.configure_logging reference because that
  module is absent from the source tree. This does not verify production startup.

Machine-readable evidence: [implementation-verification.json](evidence/implementation-verification.json).

## Rollout and remaining gates

Use [CROSS_REPO_CONTRACT.md](CROSS_REPO_CONTRACT.md) revision 2026-10-08.1.
Development pins are JRTC b0998af03c0702302855cc7c52c3dd53ac7a32af and
VideoRoom 87bbb9b79870724a91fc98f8711c55f5cb4519e2. Published package verification
remains a maintainer release gate; no release was published by this work.

Before applying migration 0013, inspect the seven affected Janus ID columns for
non-null zero values and reconcile them with their domain owner; the new checks
intentionally reject those values. Do not silently rewrite IDs. Test the migration
and row-lock races on PostgreSQL, then restore/verify the production startup and
logging configuration missing from the baseline checkout.

Deploy compatible packages/backend before the new client. Live Janus/broker
failures, rolling restarts, multi-worker handoff, TURN-only media, p50/p95/p99,
loop lag, CPU/RSS, pool 1/2/4 comparison and actual browser ICE-connected times are
not verified. Pool and database connection defaults were not increased.

Rollback requires the prior tested code and dependency lock together, with the
matching client. The additive constraints/index can generally remain during code
rollback; any schema reversal needs a separately reviewed migration. No merge,
deployment or data modification outside the isolated test database occurred.

Review PRs: [Synq #2](https://github.com/Leydotpy/synq/pull/2), [synq.js #7](https://github.com/Leydotpy/synq.js/pull/7), [JRTC #2](https://github.com/Leydotpy/jrtc/pull/2), [VideoRoom #2](https://github.com/Leydotpy/jrtc-plugins/pull/2).

Code commit: `eaa2a3f8ec5b4fbc70e488b971c531ed2be44fb0`. Review branch: `codex/implement-agent-plan-2026-10-08`.
