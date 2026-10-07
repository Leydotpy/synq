# Shared Synq/JRTC contract — revision 2026-10-07.1

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

Starting browser batch policy remains 25 ms / 16 candidates. Proposed Synq operational ceiling is 32, verified before deployment; JRTC's 256 ceiling is a protocol safety limit. Define byte and outstanding-work limits separately. Diagnostic complete mode must not create an unbounded array or bypass server limits. Accepting usernameFragment at the browser boundary does not prove Janus forwards it: the audited JRTC model omits it. Preserve it for local generation checks where appropriate and document that wire limitation.

## Event identity and ownership

Domain events used for sounds/toasts must expose stable event/domain identity, meeting session, actor, occurrence time and enough sequence/source context to distinguish new events from hydration, replay and reconciliation. Snapshots stay silent. No bespoke play_sound instruction is required. The authoritative Admission Queue survives notification overflow, reload and disconnect. Cross-coordinator moderation ACK and event echoes reconcile idempotently.

The JRTC manager owns replacement. Do not overwrite its sole loss callback to observe it. A new application observer API, if needed, is additive and must be released before consumers depend on it. A replacement session object gets new plugin/service ownership; a persisted handle ID cannot revive a lost generation. JRTC try_admit=False may mean telemetry coalescing and does not necessarily mean no state was retained. Protected queues are best effort; durability requires a separately owned durable mechanism.

## Release and verification order

1. Verify compatible JRTC/package artifacts; fix V-T01 cancellation-safe service shutdown before consuming its lifecycle in Synq.
2. Complete and test S-T01 backend final-batch/validation support before enabling the frontend combined-final-batch behavior against it.
3. Complete async signaling, ownership fast paths, event identity and service integration with their narrow transactional/authorization gates.
4. Complete frontend lifecycle/audio corrections and product notifications; perform a combined browser → Socket.IO → JRTC → Janus check.
5. Record exact commits, resolved dependency versions, Janus/broker/browser versions and observed results. A source branch or broad dependency constraint is not proof that the deployed wheel has the new API.

Synthetic benchmarks prove only their measured control-flow/request-count properties. Record connection timing, loss/recovery, p50/p95/p99, loop lag, CPU/RSS, query/write counts and TURN-only behavior separately. Choose measurable latency tolerances from an actual baseline; never invent performance pass thresholds after seeing results. Keep the previous tested dependency/feature configuration available for rollback.
