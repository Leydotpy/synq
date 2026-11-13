# Gateway state and SSE (SG-11)

Implemented behind `GATEWAY_STATE_ENABLED=False` on the gateway control-plane
branch. This is a server contract for SG-12, not a frontend cutover. Run migration
`0019_gateway_state_projection` before enabling the flag in an isolated gateway
cohort. Keep `GATEWAY_EVENTS_ENABLED`, the authenticated lifecycle bus configuration
and the separate `run_gateway_events` worker configured as described in
[gateway-control-plane.md](gateway-control-plane.md).

## Ownership and projection

`GatewaySessionObservation` remains the authenticated SG-10 input. An accepted
event projects inside the same transaction, under the existing meeting row lock.
`GatewayStateProjection` stores permission-aware state and a monotonic
`projection_version`, separate from the legacy `MeetingSession.state_version`.
`GatewayStateChange` is the committed, uniquely versioned notification log. It
retains the latest 256 changes per meeting. There is no external I/O inside the
projection transaction. SSE pulls only after its transaction returns.

Each projection rechecks Django admission, active profile/user, host/co-host
membership, publishing permissions, authorization epoch, current participant
session/generation and gateway owner/generation. No event changes those authority
fields, `joined_at`, or admission. Approval therefore displays `admitted` with
`OFFLINE` until a valid connection observation arrives. Revocation immediately
affects the next snapshot/stream pull even if a gateway command is still pending.

The worker also reconciles ten live/recently terminal meetings per sweep using a rotating UUID cursor.
Snapshot requests and SSE pulls reconcile independently, repairing missed fanout,
permission changes and expired observations even when Redis is unavailable.
Semantically identical reads and gateway refreshes do not write the projection or
notification log. SDP, ICE candidates, packet statistics and Janus plugin payloads
never enter this path. SG-10 retains its bounded lifecycle inbox/receipt writes.

Online observations are valid only until the earlier of the current owner lease
expiry and `occurred_at + 120 seconds`. Live gateway clients must refresh their
at-most-120-second tickets; refresh generates a fresh observation. Replaying an
old immutable event cannot extend this deadline. Missing/expired evidence becomes
`OFFLINE`, which means no current connection evidence, not removal. This can
conservatively display offline during an extended event delivery outage.

## Public HTTP contract

Both routes use existing session/Clerk authentication and require the current
authorized participant. Active moderators must also be participants. Removed,
denied, inactive and unrelated users cannot subscribe. Existing non-removed
attendees may read their own terminal meeting state after completion.

| Route | Result |
| --- | --- |
| `GET /api/v1/meetings/sessions/{meeting_id}/gateway/state/` | Current personalized snapshot, `Cache-Control: no-store` |
| `GET /api/v1/meetings/sessions/{meeting_id}/gateway/events/` | Async SSE, `text/event-stream`, `no-store`, proxy buffering disabled |

A snapshot contains `schema_version`, minimal `meeting` identity/lifecycle/epoch,
`projection_version`, `event_id`, `context: snapshot`,
`can_manage_waiting_room`, `local_participant`, `participants`, and `next_after`.
Each participant has separate `admission` and `presence` fields, role, display
name, participant-session identity and generation. Own/moderator views also have
publishing permissions, authorization epoch and the join-request ID/status. No
email, credential, SDP, ICE address, raw Janus ID or private room topology is sent.

Waiting participants see only themselves and minimal meeting metadata. Admitted
participants see eligible admitted peers; waiting-room moderators additionally see
eligible waiting participants. Losing moderator permission changes the projection
version and removes that visibility. Global version/cursor metadata is shared;
it does not disclose hidden participant identities or payloads.

Snapshots include at most 50 other participants per page. Fetch additional pages
with `?after={next_after}&version={projection_version}`. A concurrent change gives
409 `SNAPSHOT_CHANGED`: discard partial pages and restart. Never combine pages
from different versions. Admission/generation fields are observations of current
Django authority, not reusable grants for gateway access.

## SSE delivery and recovery

Use cookie authentication or a fetch-based SSE client with the verified Clerk
Bearer header. Never place credentials in query strings. Resume with the
`Last-Event-ID` header; it is allowed by the existing CORS origin allowlist.
Native EventSource cannot set an Authorization header, so bearer clients must use
the fetch-based adapter in SG-12. Do not broaden origins for this feature.

The SSE ID is `{projection_version}:{notification_uuid}`, scoped to the meeting.
Normal `state.changed` frames contain only the existing gateway
`state-notification` v1 schema: event ID, meeting ID, version/previous version,
currently visible changed participant IDs, occurrence time and `live`/`replay`
context. These are **invalidations**, not historical participant patches. Fetch a
new snapshot on every accepted invalidation, including an empty participant list
(meeting-wide changes and changes no longer visible use that form).

Replay walks the retained log in version order, authorizing again before each
frame. It never sends historical private participant payloads. An absent, malformed,
foreign, future or trimmed cursor yields `state.snapshot` with a quiet current
snapshot and a new cursor. A slow client never owns an unbounded server queue;
it resumes from the log or receives a fresh snapshot after retention overflow.

Clients keep the maximum applied projection version, ignore older/equal
invalidations after a newer snapshot, and never derive admission or media presence
from approval alone. Snapshots/replays are silent; this API does not command sound
playback. The existing frontend implementation remains SG-12.

Delivery deliberately uses one-second durable polling instead of an additional
Redis Pub/Sub state channel. It remains correct after lost transient delivery or
a process restart and continuously rechecks authority. Streams send keepalive
comments after 15 idle seconds, advertise a one-second reconnect delay, and close
after at most 60 seconds or the already-verified bearer expiry, whichever comes
first. Django sessions are revalidated on each pull; local permission revocation
is checked for bearer users on every pull. Remote Clerk session revocation follows
the existing authentication/cache policy and reconnection; there is no additional
remote identity-provider call for every frame.

There are at most 32 streams per ASGI worker and four per user per worker; excess
requests receive 429. These are process limits, not a deployment-wide quota.
Frames are capped at 128 KiB. Cancellation/logout/revocation releases stream slots;
unstarted HTTP responses release on close too. Database connections are released
between polls with the existing `CONN_MAX_AGE=0` setting. Deploy under ASGI; a WSGI
server's synchronous streaming fallback is not a supported SSE deployment.

## Redis and compatibility

`run_gateway_events` writes disposable JSON presence under
`synq:gateway:presence:{meeting_id}` only after database commit. It includes the
projection version, participant session/generation, authorization epoch, owner
generation, media flags and **absolute per-participant `expires_at`**. Consumers
must discard expired entries; key existence never proves online presence. The key
has a five-minute TTL to retain a short high-water fence. Lua replacement rejects
lower versions or older reconciliation timestamps. Redis loss cannot grant access
or erase the durable log. The public endpoints revalidate from Django and do not
trust this cache for permissions or authoritative snapshots.

With the flag enabled for gateway meetings, Socket.IO session-state emission and
subscription are excluded. The older HTTP state builder delegates to the same
permission-aware snapshot. Legacy meeting cohorts retain their existing behavior.
No JRTC manager, media path, broker envelope, schema export, or gateway package pin
changes are needed; the installed gateway remains `16a3272ca4625b133f24dbbf0e8fdd1e036c8694`.

Rollback: disable `GATEWAY_STATE_ENABLED`, allow streams to close on their next
pull, and leave the additive migration/data in place. Do not roll back the
ownership fence or route gateway meetings to legacy media while doing this.
Production enablement, proxy timeouts, global connection budgets and scale/load
qualification remain SG-16/17; the per-meeting projection currently recomputes
participant state on each poll and has not been load-qualified for large meetings.

## Verification

The CI workflow runs the new state tests alongside all gateway control-plane and
legacy regressions using PostgreSQL 16/Redis 7.4, plus both live integrations:

```sh
uv run --frozen python src/manage.py test apps.meetings.tests.test_gateway_state --settings=conf.gateway_postgres_test_settings
uv run --frozen python src/manage.py makemigrations --check --dry-run --settings=conf.test_settings
uv run --frozen --with pytest --with aiortc python scripts/check_gateway_state.py --gateway gateway-integration
```

The last command needs the pinned gateway checkout's test helpers and actual Janus.
It proves lobby isolation, actual audio WebRTC-up, signed lifecycle publication,
Django HTTP snapshots/SSE, reconnect replay and broker-stalled disconnect repair.
Task reports record exact executed counts and immutable commit/CI evidence.

## SG-12 SDK compatibility (2026-11-07)

`GET gateway/state/?sdk=1` adds a `sdk` member with the existing browser SDK's
room, session, real profile identities, coordinator permissions, participant
permissions, waiting queue, ten recent messages and 25 unexpired reactions.
Participants and requests remain scoped to the same paginated projection version.
Waiting callers see their own request and no meeting content. The optional shape
never reads legacy connection/media-handle ownership into the gateway cohort;
`gateway_presence` stays separate from admission. Responses above 128 KiB fail
closed. The original compact snapshot/SSE shape is unchanged.

`POST gateway/chat/`, `gateway/reactions/` and `gateway/leave/` provide application
operations without a Socket.IO binding. They require HTTPS, existing Django
authentication/CSRF, current gateway cohort/admission and participant capability
checks under the meeting lock. The strict 8 KiB parser rejects media/unknown fields.
Leave increments the authorization epoch and queues the existing disconnect command.
The existing participant PATCH/DELETE and end/share HTTPS endpoints remain in use.

Snapshots include up to 64 permission-filtered committed domain identities for
notifications. The client establishes an anchor during quiet hydration and emits
only unseen canonical events following contiguous **live** SSE invalidations.
Replay, missing anchors, permission transitions and repair stay quiet. Actual
ordered gateway media-connected/disconnected observations can create presence
event records; they never set Participant.status or joined_at. Reconciled gaps
do not manufacture presence notifications. This is bounded delivery, not an
unbounded message-history API.

Local validation: Python 3.14.9, PostgreSQL 16.2 (UTF-8), Redis 7.0.15: 239 tests
passed in 22.980s, zero skips. Existing real Janus lifecycle integration passed
in 4.582s; real Janus/Broka/projection/SSE integration passed in 5.728s. Migration
drift and targeted Ruff checks pass. `scripts/check_gateway_browser.py` adds real
Chromium/React acceptance with isolated HTTPS/WSS, TURN, Janus, Redis and Django;
its browser result is pending CI because local Chromium cannot create a Unix
socket in this environment. SG-12 is still in progress until that gate passes.

SG-12 dependency correction (2026-11-13): the backend and its CI checkout now use
gateway `23f6513d12f4707ba93529ddd56460ce184634ce` with jrtc-video
`eed430960cc88fa574370d62eae502093757a9f1`. Real Chromium exposed subscription
updates racing an in-flight authorization check and Janus inactive slots without
a source ID. Gateway topology now waits for the pending authority result; the
typed plugin allows missing feeds only for inactive media or data aggregates.
Gateway Actions 43474949853 passed all six jobs, including six actual Janus/TURN
tests and the Python 3.12/3.13/3.14 matrix. Synq's matched installed dependencies
passed 242 PostgreSQL/Redis tests locally on Python 3.14.10 (38.841s, zero skips).
The complete browser rerun remains the SG-12 acceptance gate.
