# SG-14: resume authority and recovery acceptance

The existing ticket service is the authority for resumption. A resume request
requires the exact current participant session and generation, rechecks admission,
membership and permissions under database locks, then advances the generation.
It does not create another waiting-room review. Expiry of a transport ticket does
not revoke a participant's durable approval; the authenticated Django request can
issue a fresh ticket. Changed permissions apply to that ticket immediately.

SG-14 adds PostgreSQL races for two simultaneous resume requests and a permission
change concurrent with resume. It also verifies an approved guest resumes after
ticket expiry without a new review, and that an old disconnect cannot downgrade
the new generation's MEDIA_CONNECTED observation. No model or migration change
is required. JRTC and VideoRoom pins remain unchanged.

The frozen gateway dependency and CI checkout now use
`4676d03bb108d048bbae726f23e3b4c52a14d5f8`. Its generation fence waits for confirmed
old-handle cleanup before fresh media and rechecks current authority after waiting.
See that repository's `docs/RECONNECT.md` for close codes and failure budgets.

`scripts/check_gateway_browser.py` supplies an isolated Janus restart route only
inside its ephemeral test application. The ESM browser probe is loaded as a module
to preserve native Window properties. Bounded restart diagnostics contain only
runtime flags, resource counts and close codes. The Chromium probe exercises automatic
resume, offline/online recovery, expired tickets and Janus replacement, in addition
to existing admission, TURN RTP, state/SSE and revocation checks. The fixture runs
its independent Django requests on the event loop's Django executor and closes
those database connections during teardown; it does not serialize all requests
behind an outer `async_to_sync` thread. Its Socket.IO cleanup uses the isolated
test Redis endpoint.

Verification on 2026-11-17: [Actions 43985742300](https://github.com/Leydotpy/synq/actions/runs/43985742300)
passed 245 PostgreSQL/Redis tests (29.552s), both actual Janus integrations
(lifecycle 19.219s; projection/SSE 6.389s), targeted lint/format and migration drift
at `68b0db204b1e25915a45210f2394dd73aaf7239a`. The initial local run also passed 245 tests
with PostgreSQL 16.2, Redis 7.0.15 and Python 3.14.10.

The matched [browser run 44133663381](https://github.com/Leydotpy/synq.js/actions/runs/44133663381)
passed the complete Chromium/React/TURN scenario at frontend
`f43abaf4248e0d56222ccee87288f163c8792c06`: admission, renewal, SSE repair, automatic resume,
offline/online recovery, ticket expiry, Janus restart, removal denial and meeting
end. The browser deliberately receives gateway revocation before its end-response
acknowledgement. The client completes that bounded HTTP operation and validates
the final Django snapshot without replay or applying it to a newer session.

SG-14 is ready for maintainer review. Feature cohorts, production settings, merges
and deployment remain unchanged. Roll back the matched frontend and gateway
dependency together after draining gateway sessions.
