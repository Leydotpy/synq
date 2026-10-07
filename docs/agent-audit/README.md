# Synq backend — AGENTS implementation audit

Audit date: 2026-10-07. **Not fully implemented. The earlier JRTC migration is substantially present, but the supplied async/ICE performance phase is mostly outstanding. Final-batch handling is a release blocker for the audited frontend.**

This PR improves instructions and adds an evidence/tracking pack. It does **not** change runtime application code, fix the listed defects, or declare the remaining implementation tasks complete. Review and merge the instruction update independently; select implementation concerns from WORK_PLAN.md afterward.

## Audited baselines

| Repository scope | Requested branch | Audited commit |
| --- | --- | --- |
| Synq backend | `v4` | `2d8682701300f27ab3771e9ea72413b3f7631a49` |
| Synq browser client | `codex/batched-ice-meeting-sounds` | `da471c92989ccf28a26de14d6ec4d1356c647bed` |
| JRTC core | `main` | `de64c1e02b550049c8253fdef7d47a87b765c9e8` |
| jrtc-video | `main` | `bec03a533ab3af33c1deef822e8249f123a007dc` |

The branch heads were rechecked and unchanged before preparing this update. All code evidence links are pinned to the audited commits. The attached standalone AGENTS.md duplicates AGENTS(5).md and is not an additional scope. Uploaded package metadata was treated as supporting context; repository source at the requested refs is authoritative for implementation status. Secret material was excluded from retrieval and commits.

The four repository trees were inspected, and 302 relevant source, test, configuration and documentation files were retrieved initially (plus the frontend provider/layout follow-up). Generated/vendor snapshots and unrelated plugins are outside the implementation scope. This is a requirement-focused code audit, not a claim that every line of every repository received a security review.

## Meaning of the statuses

- **present**: relevant implementation found; verification is reported separately.
- **partial**: some implementation exists, but a defect, omitted behavior or acceptance evidence remains.
- **missing**: required implementation was not found or the current path still implements the superseded behavior.
- **policy**: ownership/rollout constraint, not a standalone feature completion.
- **deferred**: the original brief explicitly postpones the work.

source_review means code/tests were inspected. runtime_subset_passed covers only the named executed subset. reproduced_gap records a controlled observation of a defect. external_not_verified means required live/deployment evidence is absent. No completion percentage is manufactured from these categories.

## Requirement coverage

Every numbered section in the supplied file is mapped in [requirements.csv](requirements.csv), with stable section IDs, separate implementation/verification status, findings and pinned evidence links. Rows share a group assessment where the same code serves several requirements; the CSV does not imply that every sub-bullet has an individual passing test. Unnumbered mission/order/definition-of-done text is assessed by this verdict and the work plan.

| Group | Original sections | Concern | Implementation | Verification |
| --- | --- | --- | --- | --- |
| S01 | 0, 1, 2 | Ownership and active implementation paths | policy | source_review |
| S02 | 3, 4, 5, 6, 39 | Native async media orchestration | missing | source_review |
| S03 | 7, 8, 9, 10, 11, 40 | ORM boundaries and transactional safety | partial | source_review |
| S04 | 12, 13, 45 | Database connection and pool policy | partial | external_not_verified |
| S05 | 14, 15, 16, 17, 18, 19, 20, 21, 37, 38 | ICE payload validation and final-batch semantics | partial | reproduced_gap |
| S06 | 22, 23, 24, 25, 26, 41 | Live binding and ephemeral ICE fast path | missing | source_review |
| S07 | 27 | Reuse the VideoRoom management service | missing | source_review |
| S08 | 28, 29 | Separate command and broker event paths | present | source_review |
| S09 | 30, 31, 32, 42 | Event identity for sound and notification policy | partial | source_review |
| S10 | 33, 34 | Recovery integration and generation invalidation | partial | source_review |
| S11 | 35 | Session-pool tuning policy | policy | external_not_verified |
| S12 | 36 | Metadata-only hot-path logging | present | source_review |
| S13 | 43, 44 | Application signaling and ICE benchmarks | missing | external_not_verified |

## Principal findings

The highest priority is S05: the source contains `if completed or not serialized_candidates: complete_trickle()` followed by an `else` that sends candidates. Therefore the uploaded final-batch fix is still absent. The focused reproduction observes only completion for three final candidates. The empty-incomplete finding is specifically the service behavior; Socket.IO's current fallback can instead introduce an invalid empty candidate, which is a separate normalization defect. This report does not claim every browser input takes the same path.

The original repository AGENTS file was 44,132 bytes and still prescribed short-lived management handles and a temporary whole-service sync bridge. It has been preserved in migration-baseline.md; the concise current root explicitly supersedes those two phase-specific rules while keeping migration/ownership/idempotency obligations.

### S01 — Ownership and active implementation paths

Keep the existing application control plane and integration package. This audit does not request a second signaling stack.

Evidence: [adapter.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/jrtc/videoroom/adapter.py); [namespace.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/realtime/namespace.py).

### S02 — Native async media orchestration

Socket handlers still wrap complete synchronous media methods in sync_to_async; services call janus_runtime.run through call_plugin_method. Direct async orchestration and its regression checks are absent.

Evidence: [namespace.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/realtime/namespace.py#L240); [signaling.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/services/signaling.py#L1788); [janus.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/services/janus.py#L646).

### S03 — ORM boundaries and transactional safety

Claim/network/finalize separation and ownership tests exist. Preserve them during the async rewrite; async relation-loading and the complete new race matrix are not established.

Evidence: [janus.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/services/janus.py#L386); [signaling.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/services/signaling.py#L92); [test_jrtc_persistence_contracts.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/tests/test_jrtc_persistence_contracts.py).

### S04 — Database connection and pool policy

CONN_MAX_AGE defaults to zero and psycopg pool capability is declared. Enabled deployment pooling, worker budgets and measured acquisition latency are not demonstrated.

Evidence: [base.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/conf/settings/base.py#L179); [pyproject.toml](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/pyproject.toml).

### S05 — ICE payload validation and final-batch semantics

Final candidates with completed=true are skipped; the service treats an empty incomplete batch as completion. Mixed invalid entries are skipped rather than atomically rejected. Namespace fallback is ambiguous, completed uses truthiness, no application batch limit was found, and usernameFragment is not forwarded. Normal non-final arrays do retain order.

Evidence: [signaling.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/services/signaling.py#L1788); [signaling.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/services/signaling.py#L1187); [namespace.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/realtime/namespace.py#L240); [test_jrtc_signaling_contracts.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/tests/test_jrtc_signaling_contracts.py).

### S06 — Live binding and ephemeral ICE fast path

Every trickle still resolves through the durable claim flow, claims a media command and writes last_event_at. The registry has local lookup/invocation fencing, but the application service does not use it as the requested healthy fast path.

Evidence: [signaling.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/services/signaling.py#L1788); [janus.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/services/janus.py#L386); [handles.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/jrtc/handles.py#L354).

### S07 — Reuse the VideoRoom management service

VideoRoomAdapter.management_command still attaches and detaches a temporary plugin on each command. It does not consume VideoRoomService.

Evidence: [adapter.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/jrtc/videoroom/adapter.py#L96).

### S08 — Separate command and broker event paths

Runtime constructs the publisher/manager and an explicit Broka consumer processes application events. Production pressure isolation still requires the integrated load run.

Evidence: [runtime.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/jrtc/runtime.py#L214); [consumer.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/jrtc/events/consumer.py); [dispatcher.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/jrtc/events/dispatcher.py); [test_jrtc_event_plane.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/tests/test_jrtc_event_plane.py).

### S09 — Event identity for sound and notification policy

Semantic meeting events and domain object IDs exist. A complete live/replay/snapshot contract and dedicated coverage for all six sound categories are not demonstrated; participant cues currently depend on frontend snapshot differences.

Evidence: [events.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/realtime/events.py); [emitters.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/realtime/emitter.py); [test_realtime_namespace.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/tests/test_realtime_namespace.py).

### S10 — Recovery integration and generation invalidation

Registry validation rejects stale bindings and JRTC owns session replacement. Synq runtime does not establish the requested proactive loss observation plus management-service invalidation. Do not replace JRTC manager recovery ownership to add notifications.

Evidence: [runtime.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/jrtc/runtime.py); [handles.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/jrtc/handles.py#L158).

### S11 — Session-pool tuning policy

Retain the existing default until matched 1/2/4-session application workloads are recorded. No Synq performance results satisfy this gate.

Evidence: [config.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/jrtc/config.py).

### S12 — Metadata-only hot-path logging

Relevant signaling diagnostics use identities and error metadata rather than routine SDP/candidate dumps. Retain this behavior and add log-capture checks when changing error normalization.

Evidence: [signaling.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/services/signaling.py); [adapter.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/jrtc/videoroom/adapter.py).

### S13 — Application signaling and ICE benchmarks

No before/after application benchmark with DB query/write counts, thread crossings, browser ICE timing and TURN-only success is present in the inspected tree.

Evidence: [test_jrtc_signaling_contracts.py](https://github.com/Leydotpy/synq/blob/2d8682701300f27ab3771e9ea72413b3f7631a49/src/apps/meetings/tests/test_jrtc_signaling_contracts.py).

## Verification actually performed

Full Django tests were not run: this environment has Python 3.12.14, whereas Synq declares Python >=3.14, and Django/Broka dependencies are unavailable. A focused AST extraction executed the actual trickle method with infrastructure doubles; final-three and empty-incomplete both called only complete_trickle. This establishes control flow, not database or Janus integration.

Focused reproduction: [script](evidence/reproduce_backend.py), [output](evidence/backend-reproductions.txt). Run `python docs/agent-audit/evidence/reproduce_backend.py` from the repository root. This AST-extracted method uses infrastructure/serialization doubles and proves only the stated control-flow defect; it is not the Django test suite. A successful process exit is not a conformance pass.

The GitHub check-runs endpoint returned zero checks for each audited SHA at inspection time. This is not evidence that tests failed or passed; no CI result is claimed. No live Janus, external broker, production database or browser session was exercised. Source assertions and prior repository benchmark prose are not substitutes for those checks.

## Improvements implemented in the instructions

1. A concise root policy replaces ambiguous baseline-as-current wording and records exact repository ownership, branch and precedence.
2. The original supplied requirements are preserved, hashed in progress.json and mapped section-by-section; Synq's earlier migration safety requirements are retained separately.
3. Shared ICE, event identity, lifecycle and release ordering are explicit, including the backend/frontend compatibility blocker.
4. Cancellation, stale async continuations, failure outcomes, queue/byte budgets, notification persistence and real package API checks are turned into concrete acceptance work.
5. Concern IDs, dependencies and review states let the user inspect one job at a time without incorrectly marking documentation or code inspection as implementation success.
6. Verification commands, limits and reproducible observations make claims reviewable; dependency/runtime blockers remain visible.

Read [WORK_PLAN.md](WORK_PLAN.md) for the implementation order and [progress.json](progress.json) for the current task states. The source brief remains in [source-requirements.md](source-requirements.md); proposed stronger behavior is governed by the root decisions and [shared contract](CROSS_REPO_CONTRACT.md).
