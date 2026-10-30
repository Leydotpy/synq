# Dedicated gateway control plane

SG-03 is additive to the existing meeting APIs. `GATEWAY_TICKETS_ENABLED` defaults
to false. It does not select a media transport, start Janus, move a room owner,
or enable production gateway traffic. SG-04 verifies tickets; SG-05 supplies a
gateway-specific admission/outbox path; SG-07/08 and later tasks own live media.

## Identity and grants

POST `/api/v1/meetings/sessions/{session_id}/gateway/tickets/` over HTTPS using
the existing session/Clerk authentication. Session authentication retains CSRF.
`session_id` is a MeetingSession ID. No role, scope, profile or participant ID
can be supplied. Responses are no-store and include a generated
`X-Correlation-ID`; credentials never go into query parameters.

Registered attendees and authenticated non-member guests reuse Profile,
MeetingJoinRequest and Participant. The existing backend has no anonymous guest
credential flow; anonymous requests remain denied. An invitation is a join gate,
not authentication. A pending, Django-validated request can create a WAITING
participant and receive only `lobby` plus `session:resume`. Only an existing
admitted/active/disconnected participant can receive media scopes. A disabled
profile/user, removed/left participant, invalid coordinator role, rejected or
expired lobby request, or non-live meeting fails closed. Subscribe and publish
are distinct; publish additionally depends on current audio/video/screen grants.

Request purposes:

| purpose | request fields | effect |
| --- | --- | --- |
| connect (default) | none | new server-generated participant_session_id and newer generation |
| resume | participant_session_id, generation | same binding, newer generation; invalidates old connection |
| refresh | participant_session_id, generation | same current binding and generation, new one-use jti |

One GatewayGrant per Participant represents the current connection generation;
multiple simultaneous tabs are not enabled by this phase. A refresh ticket is
only consumable as `session.refresh` on the same authenticated gateway binding,
never as a new hello. A stale generation or another profile's binding is denied.
Issuance is limited to 10 successful tickets per participant per minute using
a locked database counter shared across workers. Failures return generic codes;
the deployment edge must additionally bound HTTP connection/request rates.

## Keys and configuration

Set `GATEWAY_TICKET_ISSUER` to a stable pinned issuer, `GATEWAY_TICKET_KEY_ID` to a
deployment key ID, and `GATEWAY_TICKET_PRIVATE_KEY_FILE` to a mounted RSA private
key (2048 bits minimum, filesystem permissions restricted to the issuer). Keep
keys out of the repository and logs. RS256 is fixed. `GATEWAY_TICKET_TTL_SECONDS`
defaults to 60 and must be 1–120. Missing/invalid configuration denies issuance.
Claims conform to the gateway v1 ticket schema: iss/aud/sub/jti/iat/nbf/exp,
meeting/participant/session/grant IDs, generation, authorization_epoch, scopes.

Distribute public keys and issuer to gateway workers through trusted deployment
configuration; never fetch a JWT-supplied key URL. For ordinary rotation, install
the next public key on all workers, switch the issuer's signing key/kid, wait
at least 125 seconds after the last old-key issuance, then remove the old key.
For compromise, revoke the old key immediately and require fresh authentication.
Only Django receives the private key. Real keys are not provisioned by this work.

## Service authority and revocation

POST `/api/v1/meetings/internal/gateway/authority/` over authenticated HTTPS using
the dedicated `GATEWAY_AUTHORITY_TOKEN` bearer credential (at least 32 characters;
generate a random secret). The existing external meeting-provider service token
is intentionally insufficient. Restrict network ingress to gateway workers.
The body is `{claims, operation, consume}`. It contains validated claims, not
the JWT. With consume=true, operation must match the issuance purpose; with
consume=false, the receipt must already have been consumed. A receipt cannot
be recreated from a JWT or from arbitrary client assertions.

The reply contains the exact current claims, effective audio/video/screen
permissions and `valid_until`. The meeting lock serializes ticket issuance and
consumption against permission updates/removal/end. Grant generation, current
epoch, identity, meeting state, scopes and receipt expiry are checked on every
authority call. A one-use receipt and its consumption share the database across
workers and restarts. Missing receipt, replay, uncertain storage, or mismatched
authority denies access. Permission changes increment Participant.authorization_epoch
transactionally; observation/presence updates do not grant authority. A revoke
after a successful check requires gateway invalidation/fresh transition checks;
this RPC alone is not an atomic remote Janus authorization fence.

Migration 0014 adds the participant epoch, the current connection grant, and
short-lived issuance/replay receipts. Receipts store claims and timestamps only,
never private keys or JWTs. Run `prune_gateway_tickets --limit 1000` periodically
(repeat bounded batches if needed) to delete receipts expired for at least five
seconds. Missing receipts remain denied, so cleanup cannot enable replay.
Roll back by disabling the feature first; keep the additive migration while old
tickets drain. No historical migration or legacy media protocol is rewritten.

## Verification

Python 3.14.8 with frozen dependencies (Django 6.0.7, PyJWT 2.13.0,
cryptography 48.0.1): the SG-03 ticket, admission, lifecycle-hardening and six
required JRTC/runtime/realtime suites ran 129 tests: 127 passed, two PostgreSQL
race tests skipped on SQLite. The PostgreSQL workflow runs the same tests with
real row locks; its result is tracked in the gateway task report. Migration drift,
focused Ruff checks and cross-repository ticket schema validation also run.
No live Janus, browser, deployment or production identity-provider check is claimed.

## SG-05 admission and durable permission intents

The new HTTPS endpoints additionally require `GATEWAY_ADMISSION_ENABLED=true` and
the room's server-managed `feature_flags.gateway_control_plane=true`. Both are
off by default. This selects control-plane rehearsal, **not** a runtime owner;
legacy Janus ownership/dispatch is unchanged until SG-07's explicit fencing.

POST `/api/v1/meetings/sessions/{id}/gateway/join-intents/` using the existing
authenticated user. Optional fields are display_name, passcode, invite_token.
Role/profile/participant/connection IDs are forbidden. Django reuses its room
passcode, invitation, membership, capacity and direct-entry policy. It creates or
reuses a pending request and WAITING participant. Repeating an unresolved intent
does not duplicate events or change permission epochs. If Django's direct-entry
policy authorizes this identity, it commits ADMITTED and an outbox record.
No gateway connection event can perform either decision.

POST `/api/v1/meetings/sessions/{id}/gateway/join-intents/{request_id}/decision/`:

```json
{"decision_id": "client-generated-uuid", "approve": true, "expected_epoch": 0}
```

The actor comes exclusively from the authenticated profile. An active durable
membership with can_manage_waiting_room is required and locked during review;
this includes hosts/co-hosts and explicitly delegated moderators. Requested roles
are never authority. The subject's current membership supplies role defaults.
The request must belong to the meeting, remain pending, and match the participant's
current epoch. Boolean/integer values are strict. Stale/conflicting decisions
return 409. Inactive users, expired requests, removed participants and ended
meetings cannot be admitted.

Keep the same decision_id, action and expected_epoch when retrying a lost response.
The principal-bound GatewayAdmissionDecision receipt makes retries idempotent.
A key cannot be moved to another actor/request/action. A new key cannot reverse
an already resolved request. A retry after a later removal returns the current
participant status/epoch with superseded=true; it does not restore the old grant.
The same meeting lock serializes approve/revoke, and the newer epoch wins.
Denial revokes the lobby grant and marks the request rejected. Opening the room
later does not bypass that denial. Explicit re-entry after a terminal departure
is reserved for the SG-14 reconnect/rejoin policy.

Approval sets ADMITTED without setting joined_at, creating a media handle, emitting
PARTICIPANT_JOINED or queueing Janus work. Connected/publishing state is a later
gateway observation/projector concern. Responses expose admission_status,
participant_status, authorization_epoch and enforcement_status. Pending means
the decision is durable, not that media has connected or removal has been enforced.
Waiting-room projections should reconcile these authoritative records; SG-11 owns
the SSE projector. Existing snapshots/legacy notifications remain compatible.

Migration 0015 adds the decision receipt and GatewayAuthorizationOutbox, unique
per participant/authorization_epoch. Admission, denial, permission changes,
removal, voluntary leave and meeting end record intents in the same transaction
for selected rooms. Outbox failure rolls back the decision. There is no broker
network I/O or on-commit-only delivery gap. Records contain typed command data,
actor, participant, meeting and epoch; no JWT, SDP, ICE or raw passcode is stored.

These rows are **permission intents**, not complete wire commands: SG-06 must
resolve the current participant binding and SG-07 owner lease, revalidate the
epoch, add the v1 command ID/trace/time/expiry/owner fence, publish durably and
track outcomes. No owner identity is invented here. All rows remain pending;
no dispatcher or physical-enforcement success is claimed by SG-05. Disabling
the feature stops new rehearsal decisions; retain pending rows and migrations
for reconciliation rather than discarding authorization history.

Local SG-03/05 plus required legacy regression suites: 149 tests, 145 passed and
four PostgreSQL-only races skipped on SQLite. PostgreSQL CI runs all 149 including
concurrent decision retries and approval/removal races; the final run/commit are
recorded in the gateway SG-05 report. Migration drift and focused Ruff checks pass.

The gateway's `scripts/check_django_authority.py` also passed four local checks
against this checkout: actual signed-ticket verification, replay and revocation,
lobby approval and bound refresh, and SG-05 admit/deny/permissions/disconnect
outbox payloads validated against the SG-01 gateway schemas. This verifies the
Django authority handoff and command data, not Broka delivery or live Janus media.

Published SG-05 implementation: `8000bc062579b760fb373387d5945243a568ea59`. PostgreSQL 16 CI
[run 41013971484](https://github.com/Leydotpy/synq/actions/runs/41013971484)
passed all 149 tests with zero skips, including simultaneous decision retries and
approval/removal races. Migration drift check passed. Gateway compatibility
[run 41014052042](https://github.com/Leydotpy/jrtc-signalling/actions/runs/41014052042)
passed all five jobs, including four handoff/contract tests, 273 gateway tests
on each supported Python version, and container smoke checks.
