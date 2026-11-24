# Synq legacy signalling retirement

**SYNQ-LEGACY-RETIRE-01 · 24 November 2026 · READY_FOR_REVIEW — DEPLOYMENT BLOCKED**

New meetings use the dedicated gateway. Django owns admission, permissions, invitations, application events and authenticated HTTP/SSE. The gateway owns Janus sessions and handles, WebRTC negotiation and physical media enforcement. This review contains no production drain evidence or release approval.

## Baseline and readiness

`codex/retire-legacy-signalling` started at the verified `codex/gateway-control-plane` commit `0bff1ab929054e56697c2a7a0410d2e76221e555`. The cleanup instructions are on the separate instructions branch. [Task specification](https://github.com/Leydotpy/synq/blob/cb02435b99bf961a7ce69ca34c162d18b892f84c/docs/agents/LEGACY_SIGNALLING_CLEANUP.md)

| Gate | Accepted revisions (gateway / Synq / synq.js) | Evidence checked before removal |
| --- | --- | --- |
| SG-17, DONE | `8ad22c0` / `b36ba62` / `0affac3` | The staging rehearsal carried real audio on two separate cohorts, blocked premature room reuse, confirmed physical Janus disposal and replayed the terminal SSE event. [SG-17 report](https://github.com/Leydotpy/jrtc-signalling/blob/463fb6e168f90da57caa95cf3188e756dba312d2/docs/implementation/tracking/reports/SG-17.md) |
| SG-18, DONE | `6d2536e` / `0bff1ab` / `35927e5` | PostgreSQL and Redis authority checks, browser admission and TURN media, removal and denial, lost broker/Janus, multiple owners and signed termination. [SG-18 report](https://github.com/Leydotpy/jrtc-signalling/blob/463fb6e168f90da57caa95cf3188e756dba312d2/docs/implementation/tracking/reports/SG-18.md) |

For the checks on this branch, Synq's lock installed gateway runtime `8ad22c05f13a87d04a16189a8ec08bcc0276cec6`. Integration used the gateway checkout at `463fb6e168f90da57caa95cf3188e756dba312d2` and the React client from `codex/gateway-transport` at `35927e514d035d8bcf7dbb6f1404a86f4513ee4f`. [Gateway tracker](https://github.com/Leydotpy/jrtc-signalling/blob/463fb6e168f90da57caa95cf3188e756dba312d2/docs/implementation/tracking/progress.json)

## Dependency inventory

| Component | Former callers | Disposition and replacement |
| --- | --- | --- |
| `conf/asgi.py`, `conf/socketio.py`, `meetings/realtime/` | ASGI startup, namespace authentication and media handlers, dynamic emitter imports and lifecycle callbacks | ASGI now starts plain Django. Removed the Socket.IO endpoint, Redis socket manager, namespace and callbacks. Application events use a committed, permission-filtered projection and authenticated SSE. |
| `meetings/jrtc/`, `services/signaling.py`, `runjanus`, `run_jrtc_events`, `prune_jrtc_event_history` | Django ASGI, media Celery tasks, raw Janus consumer, browser forwarding and old process commands | Removed the in-process manager, handle registry, VideoRoom adapter, ICE/SDP handling and raw event worker. Gateway WSS carries SDP/ICE. The Django command and event workers retain signed Broka Redis Streams and durable outcomes. |
| `services/lifecycle.py`, `state.py`, `gateway_admission.py`, `gateway_tickets.py`, `gateway_sdk.py`, `api/gateway_application.py` | Existing HTTP admission, start, review, chat, moderation, state and discovery endpoints | Both admission entry points use one locked authority. Waiting-room expiry and revocations fence tickets and record durable commands. Chat, reactions and moderation advance committed state. HTTP and SSE apply the same visibility rules. |
| `conf/celery.py`, `conf/settings/base.py`, `meetings/tasks.py` | Celery startup/shutdown, legacy provisioning, attach/detach, room destruction, synchronisation and stale-connection sweeps | Removed runtime hooks and media jobs, including three periodic schedules. Invitation email delivery, invitation reminders, scheduled endings and waiting-room expiry remain registered. Existing `reconcile_celery_schedule` disables stale DB-backed jobs even under operator-defined schedule names. |
| `services/gateway_{ownership,cutover,outbox}.py`, `gateway_cutover` | New-session selection, route discovery, termination and operational recovery | New sessions default to gateway. A paused room, incomplete stack, live legacy session or unconfirmed prior cleanup blocks creation. Old sessions keep their backend. Lookup returns 410 for legacy or ended sessions and 503 for a live gateway session awaiting its owner. Room reuse waits for command settlement, physical termination and release. |
| `services/janus.py`, `core/hooks/janus.py`, `core/models/fields/janus.py`, `meetings/models.py` | Historical migrations `0001`, `0005` and `0009`, persisted IDs, field deconstruction | Retained importable inert callables and strict numeric field support. Removed unused Janus middleware and serializer. Migration `0022_default_to_gateway` changes the default for new rows only. |
| `pyproject.toml`, `uv.lock`, process wiring | Normal Django install, Celery, ASGI and live integration scripts | Removed direct Socket.IO, JRTC, VideoRoom and obsolete web-stack dependencies from the Django install. Uvicorn, Celery, Redis and the gateway bus remain. The separate `gateway-integration` group installs the media runtime for tests. The parent has no tracked reverse-proxy, Windows launcher or Procfile to update. |
| Synq.js transport selection | React meeting page and SDK | Use server-side `SYNQ_STAGED_ROUTING=true` with `NEXT_PUBLIC_SYNQ_GATEWAY_URL` set to the WSS gateway. The matched client still contains a legacy mode for older deployments. With authenticated route discovery enabled, a failed lookup shows the unavailable screen. |

## Verification on the review branch

GitHub Actions passed on implementation commit `78d76a3052295ee63bbbe06678f9f2821166099e`. The hosted run repeated the 205 Django tests against PostgreSQL 16, all 63 gateway ICE checks, the migration and drain checks, and all five live integration probes. The separate paired-client results below were collected locally. [Hosted verification](https://github.com/Leydotpy/synq/actions/runs/45454502343)

Checks used disposable PostgreSQL 17, Redis, Janus 1.1.2, coturn, Chromium 143 and Python 3.14.10. No production credentials, records or service endpoints were used. The commands below take the test database and Redis settings from the environment. `../gateway` and `../client` refer to the revisions above.

| Check | Result |
| --- | --- |
| Django boot, frozen resolution, migration drift and Ruff | Passed. Default dependencies import the gateway bus without `socketio`, `jrtc` or `jrtc_video`. Django serves HTTP and the former Engine.IO path returns 404. The three configured business schedules resolve to registered Celery tasks. |
| PostgreSQL meeting tests | All 205 tests selected by the branch workflow passed. Full discovery ran 212, with 205 passes and seven launcher failures. The same seven failed on the unmodified parent because `scripts/start.ps1`, `scripts/supervisor.ps1` and `src/Procfile` are absent. No tests were skipped. |
| Fresh and upgrade migrations | Fresh replay through `0022` passed, with every legacy inventory count zero. On a separately seeded parent database, migration preserved the legacy backend, numeric room and handle IDs, connection, receipt, pending browser outbox row and both schedules. The read-only command flagged seven outstanding categories. Reconciliation disabled exactly the retired schedule and preserved the invitation schedule and pending event. A migration regression reproduces the historical session, connection, handle and pending outbox upgrade in CI. |
| Five live integration checks | Passed lifecycle, state/SSE, two-worker failover, ICE benchmark and cutover. Four publishers acknowledged 128 timed ICE batches. Real media recovered after broker and Janus faults. The drain refused early reuse, independently observed Janus error 458 for the former session and absence of its VideoRoom, replayed terminal SSE and published RTP in the replacement session. |
| Frontend build, types and browser | `bun run verify` passed all 78 SDK, five packaging and 19 app tests. Chromium passed 18 scenarios and the disconnect-teardown regression. Four real peer connections sent/received RTP over TURN. The suite exercised approval, denial, reconnect, ticket expiry, chat, reaction, restart, revocation and meeting end with zero Socket.IO traffic. Its initial run timed out waiting for RTP on this loopback-only host. Allowing loopback peer connections passed on both the branch and the parent. |
| Shared ICE fixtures and authority | All 63 tests in the gateway's native ICE and signalling suites passed. Five of six checks in the older gateway authority script passed against this Django revision. The sixth still imports the deleted Django ICE normalizer. The same client batches were exercised in the gateway suite and real browser. Updating that historical comparator belongs to the gateway repository. |

```sh
uv sync --frozen
uv lock --check
uv run --frozen python src/manage.py check --settings=conf.settings
uv run --frozen python src/manage.py makemigrations --check --dry-run --settings=conf.gateway_postgres_test_settings
export DJANGO_SETTINGS_MODULE=conf.gateway_postgres_test_settings
uv run --frozen python src/manage.py migrate --noinput
uv run --frozen python src/manage.py check_legacy_signalling_drain
uv run --frozen python src/manage.py test apps.meetings.tests.test_legacy_retirement --noinput
uv run --frozen python src/manage.py test apps.meetings --noinput
labels=$(sed -n 's/^          \(apps\.meetings\.tests\.test_[a-z_]*\)$/\1/p' .github/workflows/gateway-control-plane.yml)
uv run --frozen python src/manage.py test $labels --noinput
uv run --frozen --group gateway-integration python ../gateway/scripts/check_django_authority.py --synq .
uv run --frozen --group gateway-integration python -m pytest -q ../gateway/tests/test_ice.py ../gateway/tests/test_ice_signalling.py
uv run --frozen --group gateway-integration python ../gateway/scripts/check_django_lifecycle.py --synq .
uv run --frozen --group gateway-integration python scripts/check_gateway_state.py --gateway ../gateway
uv run --frozen --group gateway-integration python scripts/check_gateway_multiworker.py --gateway ../gateway
uv run --frozen --group gateway-integration python scripts/check_gateway_cutover.py --gateway ../gateway
uv run --frozen --group gateway-integration python scripts/benchmark_gateway_ice.py --gateway ../gateway
(cd ../client && bun install --frozen-lockfile && bun run verify && bun build packages/client/test/browser/gateway.tsx --target browser --outfile "$BROWSER_BUNDLE")
uv run --frozen --group gateway-integration python scripts/check_gateway_browser.py --gateway ../gateway --frontend ../client --bundle "$BROWSER_BUNDLE"
```

Provide an absolute temporary `BROWSER_BUNDLE` path, `PLAYWRIGHT_MODULE` for Playwright 1.57 and `CHROMIUM_PATH` for the browser. On a loopback-only host the Chromium launcher used `--allow-loopback-in-peer-connection`. The pinned frontend's previous runner provisions the old combined Django/media environment. Use the explicit `gateway-integration` group above against this branch. [Workflow and exact Ruff commands](../.github/workflows/gateway-control-plane.yml)

## Production decision and rollback

Keep `ParticipantConnection`, `ParticipantMediaHandle`, `ParticipantStream`, `JrtcEventReceipt`, `JrtcBrowserEventOutbox`, immutable session backends, cohort selectors, Janus identifiers, owner generations, command/outcome records and historical migrations. These records identify ownership and unfinished work. Deletion requires a separate retention and reconciliation decision.

Before deployment, operators must verify the target database with `check_legacy_signalling_drain`, account for the old broker's pending deliveries, active Janus resources and queued Celery work, and record the result. Run the check from this revision against a copy or with read-only access. Where unfinished work exists, use the parent release's workers to resolve it and preserve the receipts. Review `reconcile_celery_schedule --dry-run` before applying it. The nine database counters cannot certify an external broker, Janus process or production drain.

The target must have the signed authority, both gateway workers, broker and WSS endpoint configured. New meetings require `GATEWAY_TICKETS_ENABLED`, `GATEWAY_ADMISSION_ENABLED`, `GATEWAY_COMMANDS_ENABLED`, `GATEWAY_RUNTIME_ENABLED`, `GATEWAY_EVENTS_ENABLED` and `GATEWAY_STATE_ENABLED`. `GATEWAY_STAGED_ROUTING` on Django limits starts to explicitly selected rooms when enabled. Coordinate the matched client's route discovery and remove the old proxy ingress only after the drain and operator approval.

For source rollback, return to `0bff1ab929054e56697c2a7a0410d2e76221e555` or revert the retirement commit. For a release rollback, pause new sessions, drain each live gateway owner on its current gateway, verify termination and release, then restore the matched parent application, workers, client/proxy settings and Beat schedules. Migration `0022` can be reversed to `0021`. Its reversal changes the default only. Existing session IDs and backend assignments retain their meaning. A new session can select the parent release's legacy path only after the previous owner has finished. Never reassign a running session.
