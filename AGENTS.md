# AGENTS.md — Synq backend

## Scope and reading order

This file governs work in this repository. The current task is an evidence-backed refinement of the supplied requirements. These instructions do not assert that the requested application work is already complete.

1. Read [the audit](docs/agent-audit/README.md) and its pinned source baseline.
2. Select the relevant concern in [WORK_PLAN.md](docs/agent-audit/WORK_PLAN.md); inspect the current code before editing.
3. Read the affected numbered sections of [source-requirements.md](docs/agent-audit/source-requirements.md) and the [shared contract](docs/agent-audit/CROSS_REPO_CONTRACT.md). The original brief is preserved for traceability. Non-conflicting requirements remain applicable; the explicit decisions below supersede obsolete or ambiguous wording.
4. Update [progress.json](docs/agent-audit/progress.json) and the matching [requirements.csv](docs/agent-audit/requirements.csv) with evidence, not estimates.

Audited target: `Leydotpy/synq` at `v4`, commit `2d8682701300f27ab3771e9ea72413b3f7631a49`. Work from the requested branch/current authorized successor; never silently switch to the default branch. Evidence for an older commit must be rechecked before marking a current task complete.

## Current decisions and stronger acceptance rules

- The older migration instructions allowed whole-service sync bridges during compatibility work and required temporary management handles. Those phase-specific recommendations are superseded for this performance phase: use direct async orchestration and package-owned reusable management services, preserving the existing transactional fences.
- Keep the legacy migration obligations in `docs/agent-audit/migration-baseline.md`: strict positive integer Janus IDs internally, decimal strings at browser boundaries, historical migration aliases, direct command ACK/JSEP results, Broka event receipts/outbox semantics and process-local ownership. Do not rewrite historical migrations.
- The frontend target for this audit is `codex/batched-ice-meeting-sounds`; the older `codex/new-ui-implementation` reference is historical.
- Fix correctness before performance. Validate the complete ICE request before claiming or creating a handle. `completed` must be a real boolean. Non-empty candidates and completion are independent ordered actions; an empty array is never implicit completion.
- A local registry hit does not replace authorization. Keep a race-safe active connection/generation fence, including revocation and handoff, and retain durable reconciliation for misses. Do not remove state-command fences merely to meet an ICE query budget.
- JRTC owns recovery. Its single loss handler already belongs to the manager. Introduce/use a separate supported observer contract if needed; never overwrite that handler or run duplicate replacement loops.
- ASGI shutdown closes application-owned services while their sessions still exist, then stops session producers, then drains/stops the publisher. Preserve finite budgets and cancellation cleanup.
- `CONN_MAX_AGE=0` remains the default. No pool or session-count increase without measured process/database budgets.

## Reviewable execution

- Work on a separate branch. Keep one concern and its necessary regression checks reviewable in each commit/PR; preserve unrelated changes.
- Follow the user-authorized scope. This audit does not pre-authorize future runtime changes. For later implementation, the user can select one concern, several concerns or the whole plan; do not add redundant permission gates for work already authorized. Do not merge/deploy unless authorized.
- If the user selects one concern, report its result and pause before starting another. Hand back changed paths, behavioral effect, exact verification commands/results, known limitations, dependency effects and rollback steps.
- Use task states `not_started`, `in_progress`, `blocked`, `ready_for_review`, `accepted`. Only the user/maintainer accepts work. A passing unit test or merged documentation is not implementation completion.
- Keep implementation state (`present`, `partial`, `missing`, `policy`, `deferred`) separate from verification state (`source_review`, `runtime_subset_passed`, `reproduced_gap`, `external_not_verified`). New instructions stay pending until code and the relevant acceptance evidence exist.
- Preserve source section IDs; add improvement IDs instead of silently deleting requirements. Record blockers explicitly. Unavailable infrastructure is a verification limitation, not a pass or a code failure.
- Use deterministic barriers/fake clocks for lifecycle races. Do not replace meaningful assertions with source-string matching or fake external dependencies merely to make a suite pass.
- Never include tokens, credentials, auth payloads, full SDP or raw ICE addresses in audit artifacts, commits or routine logs.

## Verification commands

Run from the repository root with the declared runtime/dependencies and required services:

```sh
uv run python src/manage.py test apps.meetings.tests.test_jrtc_signaling_contracts apps.meetings.tests.test_jrtc_contracts apps.meetings.tests.test_jrtc_persistence_contracts apps.meetings.tests.test_runtime_lifecycle_contracts apps.meetings.tests.test_jrtc_event_plane apps.meetings.tests.test_realtime_namespace
```

These are required follow-up gates, not claims that they passed in this audit. See the audit for what actually ran. A final completion report must include cross-repository compatibility and any live checks required by the selected concern.
