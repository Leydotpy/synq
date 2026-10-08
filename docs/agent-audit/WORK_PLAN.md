# Synq backend — concern-by-concern work plan

Implementation update 2026-10-08: local implementations and evidence are ready for review; the final live acceptance task remains blocked. See [IMPLEMENTATION.md](IMPLEMENTATION.md). The audit-only statement below describes the historical documentation change.

The audit/instruction change is ready for review. The implementation tasks below are **not started by this documentation change**. Existing code is credited in requirements.csv; tasks describe the remaining corrections or verification, not a request to repeat completed features.

Task IDs are unique across the four repositories: S=Synq, F=frontend, C=core, V=VideoRoom. A dependency in another repository refers to that repository's WORK_PLAN.md. Dependencies gate integration/release; isolated test preparation may proceed earlier. No task requires automatic delegation to other agents.

| Task | Priority | Dependencies | State |
| --- | --- | --- | --- |
| S-T01 — Fix the ICE wire contract | P0 | None | ready_for_review |
| S-T02 — Make media orchestration async | P1 | S-T01 | ready_for_review |
| S-T03 — Add the healthy binding and ICE fast path | P1 | S-T02 | ready_for_review |
| S-T04 — Consume a session-owned VideoRoomService | P1 | S-T02, V-T01 | ready_for_review |
| S-T05 — Complete event identity and loss observation | P1 | S-T02, C-T02 | ready_for_review |
| S-T06 — Run integrated acceptance and measurements | P2 | S-T01, S-T02, S-T03, S-T04, S-T05, F-T05 | blocked |

## S-T01 — Fix the ICE wire contract

**Scope:** namespace.py; services/signaling.py; ICE contract tests.

**Acceptance:** For publisher and subscriber: [A,B,C]+true forwards one ordered batch then one completion; []+false, mixed invalid entries, ambiguous singular/plural, non-boolean completed and oversized payloads reject before handle/DB side effects. Retain one-candidate legacy compatibility only at the boundary. Add an application limit of 32 and a separately documented byte limit; change limits only with evidence.

**Review evidence:** exact code commit, affected requirement IDs, regression results, external checks/limitations and rollback instructions. Set ready_for_review only after the selected scope is concrete; accepted requires maintainer review.

## S-T02 — Make media orchestration async

**Scope:** realtime namespace; signaling and janus services; async boundary tests.

**Acceptance:** Directly await adapter calls on the owner loop. Ordinary ORM execution uses async APIs with relations loaded explicitly. Transactions remain narrow synchronous helpers with no network I/O. Existing publish/disconnect and ownership tests still pass; realtime tests fail if janus_runtime.run is invoked.

**Review evidence:** exact code commit, affected requirement IDs, regression results, external checks/limitations and rollback instructions. Set ready_for_review only after the selected scope is concrete; accepted requires maintainer review.

## S-T03 — Add the healthy binding and ICE fast path

**Scope:** handle registry; authorization and activity accounting.

**Acceptance:** Validate active socket, participant, process owner, connection generation and live session/handle under an invocation fence. Healthy ICE adds zero handle-resolution claims/writes and zero per-batch activity writes; authentication reads may remain. Detach/handoff/revocation cannot bypass authorization. Demonstrate the miss/recovery path and bounded coalesced activity updates.

**Review evidence:** exact code commit, affected requirement IDs, regression results, external checks/limitations and rollback instructions. Set ready_for_review only after the selected scope is concrete; accepted requires maintainer review.

## S-T04 — Consume a session-owned VideoRoomService

**Scope:** VideoRoom adapter; runtime lifecycle; dependency lock.

**Acceptance:** Cache by actual session object and generation, not just a reusable integer ID. Keep participant handles separate. Create a new service for a replacement session; do not mutate service.session. Close services before destroying their owning sessions. Prove repeated command reuse, replacement and cancellation-safe shutdown. Verify the selected package release actually contains the API.

**Review evidence:** exact code commit, affected requirement IDs, regression results, external checks/limitations and rollback instructions. Set ready_for_review only after the selected scope is concrete; accepted requires maintainer review.

## S-T05 — Complete event identity and loss observation

**Scope:** event schemas; runtime; registry; frontend contracts.

**Acceptance:** Publish stable semantic event identity, actor, session and occurrence time; distinguish snapshots/replay from new events without a play_sound command. Reconcile admission decisions across coordinators. Observe loss without replacing the manager loss handler or creating a second recovery owner; fence old bindings immediately and reconcile durable state off the reader.

**Review evidence:** exact code commit, affected requirement IDs, regression results, external checks/limitations and rollback instructions. Set ready_for_review only after the selected scope is concrete; accepted requires maintainer review.

## S-T06 — Run integrated acceptance and measurements

**Scope:** test deployment; version lock; benchmark evidence.

**Acceptance:** Run Python 3.14+ with the repository supported Django/Postgres/broker versions. Record query/write budgets, p50/p95/p99, lag, CPU/RSS, 1/2/4-session comparison and TURN-only browser success. Test rolling restarts and broker stalls. Retain rollback configuration and verify old and new client compatibility before enabling combined final batches.

**Review evidence:** exact code commit, affected requirement IDs, regression results, external checks/limitations and rollback instructions. Set ready_for_review only after the selected scope is concrete; accepted requires maintainer review.

## Updating the tracker

When starting, set only the selected task to in_progress and record its branch. Record blocked reasons when a real dependency prevents progress. On completion, add implementation_commit and verification_evidence with commands, runtime/version, observed results and evidence paths; update the relevant requirement rows. Preserve this audit baseline, add a dated assessment for a new head, and never overwrite historical evidence as if it were obtained at the new commit. For a docs-only change, leave application task states unchanged.
