# Shared Synq/JRTC contract — revision 2026-10-08.1

These coordinated instructions describe required behavior. The audit reports describe what currently exists. Identical copies are included in each repository for review; future revisions must update all consumers and this revision identifier together.

| Concern | Owner | Required boundary |
| --- | --- | --- |
| Browser batching and audio | synq.js | No Janus wire protocol or broker imports; app owns notification presentation. |
| Auth, meeting state and signaling | synq | Active connection/generation authorization; direct async JRTC commands; durable state fences remain. |
| Generic protocol, transport and recovery | jrtc | Transaction correlation, bounded background event publication, one recovery owner. |
| VideoRoom typed commands and service | jrtc-video | Reusable management handle per service/session; dedicated participant handles. |

## ICE acceptance table

The Socket.IO event name remains `session_media_trickle`. `session_id` is the Synq meeting-session identity, not a Janus session ID. `handle_type` is publisher or subscriber. New browser requests use `candidates` arrays. `completed` is a strict boolean, default false when absent. Janus protocol IDs are positive integers internally and decimal strings at browser boundaries; booleans are not IDs.

| Browser input | Backend action | Janus/JRTC calls |
| --- | --- | --- |
| [A], false | Validate/authorize then forward | trickle([A]) once |
| [A, B, C], false | Preserve order and the array | trickle([A, B, C]) once |
| [A, B, C], true | All candidates before completion | trickle([A, B, C]); then complete_trickle() |
| [], true | Explicit completion only | complete_trickle() once |
| [], false | Reject invalid request | None |
| Malformed element, ambiguous singular/plural, non-boolean completion, oversize | Reject the entire request before resource/DB side effects | None |
| Legacy singular candidate only | Explicit boundary normalization during rollout | One ordered sequence, then completion if explicitly requested |

Both final operations must execute on the same authorized binding/generation without detach or handoff between them. A failure of the candidate operation must not send completion. An ACK timeout can leave the remote result unknown; stop/reconcile that generation rather than blindly replaying. End-of-candidates is not evidence of ICE-connected success.

Starting browser batch policy remains 25 ms / 16 candidates. The review implementation enforces a Synq ceiling of 32 candidates and 64 KiB of compact UTF-8 JSON for the entire request; JRTC's 256 ceiling remains a protocol safety limit. Browser defaults bound queued plus in-flight work to 256 candidates, 256 KiB and 32 batches, with a 48 KiB candidate chunk budget, a 5-second send deadline and a 6-second close deadline. A failed or unknown generation cannot send completion or replay automatically. Late or abandoned work is counted. Diagnostic complete mode must not create an unbounded array or bypass server limits. Accepting usernameFragment at the browser boundary does not prove Janus forwards it: the audited JRTC model omits it. Preserve it for local generation checks where appropriate and document that wire limitation.

## Event identity and ownership

Domain events used for sounds/toasts must expose stable event/domain identity, meeting session, actor, occurrence time and enough sequence/source context to distinguish new events from hydration, replay and reconciliation. Snapshots stay silent. No bespoke play_sound instruction is required. The authoritative Admission Queue survives notification overflow, reload and disconnect. Cross-coordinator moderation ACK and event echoes reconcile idempotently.

The JRTC manager owns replacement. Do not overwrite its sole loss callback to observe it. JRTC 3.2.0 adds independently removable loss observers (maximum 16 per session); callbacks are synchronous, exception-isolated and must remain constant-time. The manager loss handler remains untouched. Synq coalesces notifications into one event/worker and unregisters on shutdown. Review builds use exact Git commits; production consumers must verify the released artifact before rollout. A replacement session object gets new plugin/service ownership; a persisted handle ID cannot revive a lost generation. JRTC try_admit=False may mean telemetry coalescing and does not necessarily mean no state was retained. Protected queues are best effort; durability requires a separately owned durable mechanism.

## Release and verification order

1. Verify compatible JRTC/package artifacts; fix V-T01 cancellation-safe service shutdown before consuming its lifecycle in Synq.
2. Complete and test S-T01 backend final-batch/validation support before enabling the frontend combined-final-batch behavior against it.
3. Complete async signaling, ownership fast paths, event identity and service integration with their narrow transactional/authorization gates.
4. Complete frontend lifecycle/audio corrections and product notifications; perform a combined browser → Socket.IO → JRTC → Janus check.
5. Record exact commits, resolved dependency versions, Janus/broker/browser versions and observed results. A source branch or broad dependency constraint is not proof that the deployed wheel has the new API.

Synthetic benchmarks prove only their measured control-flow/request-count properties. Record connection timing, loss/recovery, p50/p95/p99, loop lag, CPU/RSS, query/write counts and TURN-only behavior separately. Choose measurable latency tolerances from an actual baseline; never invent performance pass thresholds after seeing results. Keep the previous tested dependency/feature configuration available for rollback.


## Review implementation protocol — 2026-10-08

Every session-state response is a snapshot (`event_context.kind = "snapshot"`), including REST refresh, first hydration and reconnect. Notifications and remote-presence sounds require `event_context.kind = "live"`. Live context contains `event_id`, `session_id`, actor profile/participant IDs when available, and ISO `occurred_at`. Stable domain identities deduplicate join requests, messages, reactions, removal and meeting end. The new `participant_presence_changed` event uses a committed MeetingEvent identity and carries `participant_id` and `status` (`joined` or `left`). No client infers presence events by diffing snapshots. Metadata does not replace existing server authorization.

A generation diagnostic counter is never a server authorization token. Browser callbacks are checked against the actual peer and ICE username fragment; Synq checks the active socket/connection, participant, runtime owner, session object/generation and registry binding, rechecking authorization before final completion. `usernameFragment` is validated at the boundary but is not forwarded by the generic JRTC candidate model.

The coordinated review uses JRTC commit `b0998af03c0702302855cc7c52c3dd53ac7a32af` (3.2.0) and jrtc-video commit `87bbb9b79870724a91fc98f8711c55f5cb4519e2` (3.0.3). Synq's uv sources and lock pin both commits. The VideoRoom workspace's obsolete machine-local core source override was removed so the subdirectory can be installed reproducibly. Both packages were built and imported from the frozen consumer environment; no PyPI publication has occurred.

Rollout order remains packages, Synq, then client/app. The new backend accepts the documented legacy singular ICE form and plural arrays; deploy it before clients sending combined final batches. A new client with an old backend lacks live metadata and keeps semantic cues/admission toasts quiet; the durable Admission Queue remains usable. This is a compatibility limitation, not a certified mixed-version deployment. Do not roll back the backend underneath a client that depends on corrected final-batch semantics. Roll back the tested pairing and dependency locks together.

Goey 0.5.0 and its framer-motion 12.42.2 peer are locked. `patches/goey-toast@0.5.0.patch` uses Bun's patchedDependencies mechanism to keep loading promises expanded until settlement and restart the result timer afterwards. The app is a private workspace; public release packages remain limited to the existing six package directories. Pending admission controllers are capped at three independently of the visible toast queue, and the durable waiting-room route includes the pending count.

Review status is not deployment acceptance. Live PostgreSQL locking, multi-user browser/media/OGG/autoplay/accessibility, Janus/broker failure and TURN-only checks, matched latency/resource measurements and maintainer package release remain external gates. Each repository's IMPLEMENTATION.md records what actually ran and what remains blocked.
