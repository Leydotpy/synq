# Synq legacy signalling retirement — single Codex agent task

> **Task ID:** SYNQ-LEGACY-RETIRE-01  
> **Repository:** Leydotpy/synq  
> **Working baseline:** current authorized successor of `codex/gateway-control-plane` (never silently use the repository default branch)  
> **Cross-repository dependencies:** Leydotpy/jrtc-signalling (`codex/gateway-foundation` or its authorized successor), Leydotpy/synq.js (`codex/gateway-transport` or its authorized successor)  
> **Status when drafted (2026-11-22): BLOCKED FOR DESTRUCTIVE CLEANUP.** Gateway tracker SG-17 was IN_PROGRESS and SG-18 was NOT_STARTED. This is a snapshot, **not** a permanent status.  
> **Desired outcome:** Django is the authoritative control plane with HTTP, authenticated snapshots/SSE and durable gateway commands/events. The separate gateway alone owns Janus sessions/handles, SDP, ICE and media signalling. No Django Socket.IO runtime survives.

## How to invoke

Read this document in full alongside the repository's existing root `AGENTS.md`. Execute **one self-contained cleanup task with ordered internal checkpoints**, not a new large agent pack. Make changes only when the live dependency gate passes. Do not mistake instructions, earlier test reports or passing unit tests for deployment approval.

**Non-negotiable:** No implementation is authorized on `main`, the default branch, or unrelated users' work. Use a dedicated review branch based on the latest approved Synq gateway-control-plane work; record its parent SHA. Do not merge, deploy, switch production routing, drain real users, change secrets or drop production data. Do not alter the gateway tracker to manufacture readiness.

## Gate 0 — verify readiness before deleting anything

At execution time, inspect the **live** gateway tracker, reports and commits rather than relying on the snapshot above:

- `jrtc-signalling/docs/implementation/tracking/progress.json`
- `jrtc-signalling/docs/implementation/PROGRESS.md`
- `jrtc-signalling/docs/implementation/tracking/reports/SG-17.md`
- `jrtc-signalling/docs/implementation/tracking/reports/SG-18.md`
- `jrtc-signalling/docs/STAGED_CUTOVER.md` and `docs/implementation/checklists/ACCEPTANCE.md`
- Synq's live branch, `docs/gateway-control-plane.md`, `docs/gateway-recovery.md`, `docs/gateway-state.md`, and the deployed-client contracts in Synq.js.

If either SG-17 or SG-18 is not DONE with substantive, reproducible cross-repository evidence, **STOP before destructive edits**. Return status `BLOCKED: SG-17/18 incomplete`, identify missing evidence, and leave the existing legacy runtime, Celery tasks, fallback and routes unchanged. Do not try to finish SG-17/18 under this cleanup instruction. If gateway or frontend branch pointers have moved, find the authorized successors before proceeding.

When SG-17 and SG-18 are DONE, check actual staging/integration evidence for authentication, waiting-room admission, direct publishing/subscribing, ICE/SDP, permission updates, removal, bans, chat/reactions, reconnection, multiworker ownership, broker/Janus failure recovery, SSE replay and session termination. Ensure there is **never** simultaneous legacy and gateway ownership of one meeting. Identify existing gateway/legacy cohorts, session routes, active handles and pending legacy event/command outbox records. Do not assume a record's age means it is safe to discard.

**Permission distinction:** Passing the gate authorizes implementation on a review branch. It does not authorize merging/deploying a gateway-only build or permanently removing production failback. Before merge/deployment, obtain explicit operator cutover/decommission approval and independent evidence that no live legacy session or undispatched legacy work remains; otherwise report `READY_FOR_REVIEW — DEPLOYMENT BLOCKED`.

## Checkpoint 1 — create an evidence-backed dependency inventory

Inspect before editing; record exact paths, callers, imports, dynamic imports, task registrations, settings, startup scripts, process supervisors, frontend transport selection and historical migration references. Search the full tree, not only normal import statements:

    rg -n 'socketio|socket\.io|MeetingSocketEmitter|MeetingNamespace|realtime\.' src scripts .github
    rg -n 'janus_runtime|MeetingMediaSignalService|apps\.meetings\.jrtc|services\.janus|JRTC_EVENT|SOCKET_IO|JANUS_' src scripts .github
    rg -n 'signalling_backend|gateway_canary|require_legacy|runjanus|gateway_cutover' src scripts .github
    rg -n 'core\.models\.fields\.janus|core\.hooks\.janus|services\.janus' src/apps/meetings/migrations

Paths may be absent; inspect actual tree and handle failed searches without treating them as proof of absence.

Build a short matrix **symbol/path → current callers → replacement → disposition (remove/refactor/retain shim/retain)**. In particular inspect:

- `src/conf/asgi.py`, `src/conf/socketio.py`, `src/conf/celery.py`, `src/conf/settings/base.py`, custom `runserver`/`runjanus` commands.
- `src/apps/meetings/realtime/{auth,namespace,emitter,events,context}.py`.
- `src/apps/meetings/services/{signaling,janus,lifecycle,state}.py`, `src/apps/meetings/tasks.py`.
- `src/apps/meetings/jrtc/` including runtime, handles, video-room adapter, old raw Janus event dispatcher, `SocketIoJanusEventEmitter`, durable browser forwarding and background consumer.
- `src/core/{hooks,middleware,models/fields,serializers}/janus.py`; `src/apps/meetings/models.py`; all migration files.
- Gateway API, outbox/worker, ticket, permission, ownership, state projection and SSE implementations; `src/apps/meetings/api/urls.py`.
- `pyproject.toml`, `uv.lock`, environment templates, reverse-proxy/process deployment instructions, Celery Beat DB-backed PeriodicTask registrations, monitoring, and Synq.js's remaining Socket.IO fallback.

**Do not delete because of a filename alone.** Preserve referenced functionality, prove that its replacement behaves correctly, then remove the obsolete implementation.

## Checkpoint 2 — implement the gateway-only backend without losing app behaviour

1. **ASGI:** replace the Starlette/Socket.IO composite and `janus_runtime.lifespan` with Django's `get_asgi_application()`; **retain ASGI** because authenticated SSE is served by Django. Remove the old `/socket.io` ingress only when coordinated with the client and deployment.
2. **Socket.IO:** eliminate `conf/socketio.py` and the obsolete `meetings/realtime/` package **only after** migrating every caller. Replace all `MeetingSocketEmitter` calls, including dynamic `__import__` references, post-commit callbacks, presence and socket disconnect operations. Do not lose chat, reactions, moderation, waiting-room decisions, participant lifecycle events or meeting-end visibility. Django gateway state projection + authenticated SSE/snapshot, and signed gateway commands for physical enforcement, are the target; preserve authorization and transactionality. Verify committed state becomes observable/replayable without old sockets.
3. **Janus:** eliminate direct Janus command/SDP/ICE/video-room ownership in `services/signaling.py`, `services/janus.py`, `apps/meetings/jrtc/` and Django Celery tasks. Remove process-local JRTC manager/handle registry, old raw `janus.events` consumer and `JrtcBrowserEventOutbox` delivery implementation **only when their work is replaced, drained and migration-safe**. Do not remove the new gateway's Broka command/outcome/event workers or Django state projection. Avoid importing a Janus session/plugin from any new Django code.
4. **Celery:** remove Janus runtime fork/reset/shutdown hooks and legacy jobs such as Janus room provisioning/destruction, publisher/subscriber handle attachment/detachment, old Janus synchronization and socket heartbeat sweeps after tracing their callers. Preserve invitation email/reminders, scheduled meeting ending, waiting-room expiry and legitimate background domain work. Replace any task scheduling that still assumes a legacy provisioned Janus room. Reconcile DB-backed Celery Beat schedules, not just Python settings.
5. **Contracts:** preserve gateway ticket issuance, admission/permissions epochs, revocation, authoritative ownership leases and fencing, command outbox/inbox, signed outcome handling, observation projections and SSE. Preserve the immutability of a session's selected backend and route discovery contract until Synq.js and historical-session behaviour have been deliberately migrated. Never send SDP/ICE via Django/Broka and never let gateway observations confer admission.
6. **Dependencies/configuration:** remove `python-socketio`, unused `starlette`, and direct `jrtc`/`jrtc-video` Django dependencies only after static and dynamic references are gone. Keep `uvicorn` if Django ASGI still uses it; keep `jrtc-signalling[bus]`, Broka, Redis and Celery as needed by the control plane. Remove only unused `SOCKET_IO_*`, legacy `JANUS_*`, `JRTC_EVENT_*`, JRTC consumer startup commands, deployment wiring and lock entries. Do not strip gateway or ordinary Redis config.

**Business models to retain:** MeetingRoom, MeetingSession, Participant, memberships, invitations, join requests, moderation permissions, MeetingMessage, MeetingReaction, MeetingEvent, gateway grants/tickets/routes/epochs, owner generations, command/outcome/quarantine, lifecycle receipts and projections. Keep auth, HTTP API and invitation flows working.

**Schema safety:** Do NOT drop `ParticipantConnection`, `ParticipantMediaHandle`, `ParticipantStream`, `JrtcEventReceipt`, `JrtcBrowserEventOutbox`, `signalling_backend`, `gateway_canary`, or persisted Janus IDs merely to make the repo look cleaner. For each, check live references, FK constraints, audit/retention needs, history, pending outbox rows and migrations. If safe in this task, create additive/forward Django migrations with tested data treatment; otherwise **retain it and document the residual schema debt**. Do not rewrite, delete or squash historical migration files in this task.

**Historical migration shims:** Migration `0001_initial.py` and `0005_native_janus_videoroom_plugin.py` import `core.models.fields.janus.JanusPluginField` and contain dotted references to `services.janus` and `core.hooks.janus`. Preserve importable, minimal, inert compatibility symbols as required to load/replay historical migrations without reinstating the Janus runtime. Verify imports and deconstruction on a completely fresh database. Do not retain runtime JRTC dependencies solely because an old migration needs a dotted-path string.

## Checkpoint 3 — hard verification and final handoff

Run the project's exact checks using pinned dependencies and required isolated PostgreSQL, Redis, Janus, TURN and browser services where applicable. Adapt the following commands to the repo's current documented runner (do not claim a command passed without running it):

    uv run --frozen python src/manage.py check
    uv run --frozen python src/manage.py makemigrations --check --dry-run
    uv run --frozen python src/manage.py test apps.meetings

Run clean-install migration replay **and** upgrade of a representative pre-cleanup database copy; assert no missing custom field/serializer/class import. Run current Synq gateway integration tests, end-to-end cutover checker and Synq.js browser/React publishing, SSE, reconnection, denial, moderation and media tests. Use the task reports to discover exact available commands; do not substitute mocks for live-owner/Janus/failure-path checks. Check formatting, lint, types, frozen dependency resolution and application boot. Verify scheduled Celery tasks register without missing imports.

Exercise all of: ticket auth/expiry/replay, direct/lobby join/approve/deny, broadcast permissions, published audio and video, ICE restart and ordered candidate completion, multiple peers, host removal and immediate revocation, chat and reactions, waiting-room/participant state, SSE resubscribe/replay/privacy, forced disconnect/resume, gateway multiworker failover/fencing, Janus and Redis outage, meeting termination, invitation reminders and scheduled ending. Check no duplicate-owner or old `/socket.io` traffic is required for the gateway cohort.

Search again for stale runtime references and dynamic strings. If any are genuinely necessary for migrations or historical data, report them explicitly rather than hiding them. Ensure no operational Django code constructs a Janus session/plugin or emits Socket.IO, while allowing persisted identifiers and controlled historical shims.

**No test cheating:** never delete/skip/weaken gateway tests just because a legacy suite was removed. Retire obsolete tests only when a new gateway-only behavioural test covers the same enduring domain requirement. Never change application security or failure semantics to satisfy a source-string assertion.

**Output and delivery:** Keep this as **one** work item. Use one review branch and logically grouped commits as needed for safe rollback; do not create a new multi-task pack. Add a concise `docs/legacy-signalling-cleanup-report.md` containing:

- baseline branches/SHAs, live SG-17/18 evidence, files/functions removed/refactored/retained and the dependency inventory;
- exact commands, test counts, failures/skips, clean DB and upgrade evidence, cross-repo client/gateway validation and any unavailable infrastructure;
- verified legacy event/outbox drain and unresolved historical-session or migration limitations;
- residual schema debt, remaining deprecated imports, deployment prerequisites and rollback via Git/redeployment;
- final status: `BLOCKED`, `READY_FOR_REVIEW` or `PARTIAL` (never self-accept).

Follow the existing root `AGENTS.md` and update applicable existing audit tracking honestly without rewriting historical evidence. Commit/push reviewable changes to the authorized feature branch **only**; do not merge, deploy, mutate live credentials or erase production data. If a mandatory gate fails, stop, explain precisely why, and keep the repository in a buildable state.

## Definition of done

- Both SG-17 and SG-18 verified DONE with their underlying evidence.
- Gateway-only Django + frontend flows pass, with no second Janus runtime or Socket.IO in active Django code.
- Business/control-plane semantics, security fences, SSE and Broker durability remain intact.
- Clean and upgrade migrations work with justified historical shims and preserved data.
- Cross-repo tests and documented live integration run or blockers are stated plainly.
- A review branch, auditable commits, report and rollback notes exist.
- **Production decommission/merge is separately authorized.**
