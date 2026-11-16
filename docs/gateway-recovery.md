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
inside its ephemeral test application. The Chromium probe exercises automatic
resume, offline/online recovery, expired tickets and Janus replacement, in addition
to existing admission, TURN RTP, state/SSE and revocation checks. The fixture runs
its independent Django requests on the event loop's Django executor and closes
those database connections during teardown; it does not serialize all requests
behind an outer `async_to_sync` thread. Its Socket.IO cleanup uses the isolated
test Redis endpoint.

Initial local verification: 245 PostgreSQL 16.2/Redis 7.0.15 tests passed on Python
3.14.10, with no skips. Final installed dependency and browser evidence will be
recorded after the corresponding CI run. This work is for review; feature cohorts,
production settings, merges and deployment remain unchanged. Roll back the matched
frontend and gateway dependency together after draining gateway sessions.
