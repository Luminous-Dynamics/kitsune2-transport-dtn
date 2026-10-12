# DTN Envelope v1 — Proposed Protocol Contract

**Status:** design proposal; not implemented; not an interoperability or security claim.  
**Scope:** application envelope carried inside BPv7 payloads by `kitsune2-transport-dtn`.  
**Related work:** [issue #5](https://github.com/Luminous-Dynamics/kitsune2-transport-dtn/issues/5); receive journal qualification is tracked separately in [PR #4](https://github.com/Luminous-Dynamics/kitsune2-transport-dtn/pull/4).

This document freezes a candidate wire contract for review before implementation. The CDDL and test vector in this branch are proposals. The current transport still forwards opaque Kitsune2 frames and does not verify this envelope.

## 1. Layer boundaries and claims

The following are distinct events and MUST NOT be reported as equivalent:

| Event | What it proves | What it does not prove |
|---|---|---|
| Local `/send` returns success | The local dtn7 daemon accepted a submission request | Remote delivery, receiver authentication, handler acceptance, application commit |
| Receiver commits raw bytes to the local journal | The bytes read from the destructive endpoint are locally retained | Sender identity, valid envelope, handler acceptance |
| COSE signature and local credential policy validate | The resolved key signed the encoded envelope and is locally bound to its claimed principal | Permission for every payload operation |
| Durable inbox/dedup entry commits | The receiver has persisted the message identity before dispatch | Application side effects completed |
| Kitsune2 `recv_data()` returns success | The transport handler accepted the frame | Durable application completion or exactly-once side effects |
| Application reports commit | Only the application's explicitly defined transaction/idempotency contract | Anything outside that transaction |

The existing receive journal has a remaining destructive-pop-to-local-commit loss window. This proposal does not close it. Replay after journal commit remains at-least-once. Do not claim exactly-once delivery without a transaction boundary or idempotent handler contract.

Use the standard Bundle Protocol and BPSec for their respective bundle-layer semantics. This envelope is an application-layer identity/integrity profile; it is not BPSec and does not add confidentiality.

## 2. Wire profile

A v1 message is a **tagged COSE_Sign1** object (CBOR tag 18). Its payload is a deterministic CBOR encoding conforming to `envelope-v1` in [`dtn-envelope-v1.cddl`](dtn-envelope-v1.cddl).

The protected COSE header MUST contain exactly:
- `alg = -8` (COSE EdDSA); this v1 profile accepts Ed25519 keys only;
- `content-type = "application/vnd.luminous.dtn-envelope-v1+cbor"`;
- `kid`, a 32-byte credential identifier resolved by the local trust store.

The unprotected header MUST be an empty map. The COSE `Sig_structure` external AAD is the empty byte string. Protected headers and the signed payload MUST use RFC 8949 core deterministic CBOR. Receivers MUST reject duplicate map keys, indefinite-length items within this profile, non-deterministic encodings, unsupported versions, unknown v1 fields, invalid lengths, and trailing bytes.

A receiver MUST bound the raw envelope length **before** allocating payload-sized buffers. The proposed initial profile cap is **8 MiB per signed envelope**; it is intentionally below the existing journal's 64 MiB record cap. The earlier 1–5 MiB real-daemon observations do not qualify the proposed 8 MiB ceiling, so implementation must test both a payload near 5 MiB and the configured limit before claiming that limit. The limit can be lowered by deployment policy. Increasing it requires fresh memory-bound and exact-head qualification.

### Envelope fields

| Key | Field | Required rule |
|---:|---|---|
| 1 | `version` | Integer `1` |
| 2 | `message_id` | 16 cryptographically random bytes, minted once and preserved byte-for-byte across retries |
| 3 | `sender_id` | 32-byte opaque principal identifier from the identity layer |
| 4 | `created_at_ms` | Sender-asserted Unix time in milliseconds; signed but not independently trusted |
| 5 | `expires_at_ms` | Signed absolute expiry time in Unix milliseconds |
| 6 | `traffic_class` | `0=ephemeral`, `1=durable`, `2=bulk`, `3=control` |
| 7 | `payload_type` | Lowercase ASCII media type, 1–127 bytes; v1 disallows parameters |
| 8 | `payload` | Opaque bytes; bounded by the raw envelope cap |
| 9 | `payload_sha256` | SHA-256 of the exact payload bytes in field 8 |

The COSE `kid` is not a principal and a valid public key is not automatically trusted. The local credential resolver MUST bind `kid` to both the public key and the exact `sender_id`; it MUST apply key-validity, revocation, and policy checks locally. A BPv7 source EID, Kitsune2 URL, embedded key, or syntactically valid identity string MUST NOT be treated as proof of the sender's identity.

A message is valid only if the signature verifies, the resolved credential is authorized for the claimed `sender_id`, the payload digest matches, the version/schema is exact, and all local resource and policy limits pass. No network governance/consensus round trip is required on the forwarding hot path; the verifier uses locally available trust state and fails closed when required state is absent or too stale.

## 3. Delivery class and expiry rules

Expiry MUST be checked on initial receive, every replay after restart, and immediately before dispatch. A stored bundle does not retain permission to execute forever.

The proposed baseline policy defaults are local policy values, not negotiated wire values:

| Class | Meaning | Proposed maximum lifetime | Rule |
|---|---|---:|---|
| 0 — ephemeral | Interactive/session-sensitive data | 60 s | MUST NOT be routed through DTN store-and-forward; a receiver seeing it over DTN rejects it |
| 1 — durable | Ordinary durable messages | 7 days | May be stored and forwarded until expiry |
| 2 — bulk | Non-urgent data/artifacts | 30 days | May be stored and forwarded with lower scheduling priority |
| 3 — control | Commands, revocation/consent changes, authority-bearing input | 30 s | MUST NOT directly authorize execution; requires a fresh application-level capability and replay guard |

Deployments may choose stricter limits. A receiver MUST reject messages whose declared lifetime exceeds its configured maximum for that class. Any profile that increases maximum lifetime must explicitly document its time and key-retention assumptions.

The receiver MUST have usable trusted wall-clock time to make a time-based dispatch decision. It MUST reject an already-expired message and a `created_at_ms` too far in the future (proposed default tolerance: 120 s). It MUST NOT rely on sender time alone as proof of freshness. If time quality is unknown, time-sensitive messages MUST remain un-dispatched until clock validity is restored; it is unsafe to assume a message is still fresh. Clock quality and the injected time source must be visible in diagnostics and tests.

Control messages need more than expiry. Their application payload must carry an authorization/capability, scope, authority epoch or monotonic sequence, and nonce according to the relevant application protocol. Envelope deduplication does not replace authorization, consent/revocation checks, or a domain-specific replay guard. Stale control input MUST NOT become current authority merely because a partition ended.

## 4. Deduplication and durable state

Deduplication key: **(`sender_id`, `message_id`)**. It is identity-scoped, not credential-key-scoped, so legitimate key rotation does not create a new message namespace.

For each authenticated envelope, compute `envelope_digest = SHA-256(the exact deterministic CBOR payload bytes carried by COSE_Sign1)`. This digest binds the signed fields, not only the application payload.

1. A previously unseen ID is eligible for admission only after signature, identity, expiry, class, digest, and resource checks pass.
2. The receiver MUST durably reserve the dedup key and inbox state before dispatching the payload.
3. Same key and same envelope digest is a duplicate: MUST NOT be dispatched again solely because it was retransmitted; return the already persisted receipt state.
4. Same key but different envelope digest is an ID conflict: MUST NOT dispatch either variant as a new message; record a security diagnostic and return a stable conflict rejection when the sender can be authenticated.
5. The sender MUST retry the same signed bytes with the same message ID. Extending expiry or changing any signed field requires a new message ID.
6. The receiver MUST retain unexpired dedup tombstones and receipts for the configured retention window. It MUST NOT evict an unexpired dedup entry to make room for a new message and then dispatch that message without replay protection.
7. If durable dedup/inbox state cannot be reserved, the receiver MUST fail closed for that admission and emit an observable resource-limit result; it MUST NOT silently dispatch.

A crash after handler success but before durable dispatch state/receipt commit can cause redelivery. The adapter's current handler interface does not expose an application transaction, so its strongest receipt is `handler_accepted`, not `application_committed`.

## 5. Receipts: durable progress, not a vague ACK

Receipts are separately signed COSE_Sign1 objects with content type `application/vnd.luminous.dtn-receipt-v1+cbor`; their schema is in the CDDL file. They bind the original `message_id`, `envelope_digest`, the receiving principal, a receipt ID, a disposition, a reason code, and receiver time when trustworthy.

Disposition values:
- `0 durable_inbox_committed`: authenticated message and dedup/inbox state are durably committed;
- `1 handler_accepted`: the transport's Kitsune2 receive handler returned success; not an application commit;
- `2 application_committed`: reserved for a higher layer that can prove its application transaction/idempotency boundary; the current adapter MUST NOT emit it;
- `3 rejected`: durable terminal rejection with a stable reason code;
- `4 expired`: the message expired before dispatch.

A receipt MUST be made durable before transmission and be reproducible after restart. On duplicate message delivery the receiver returns its current persisted receipt rather than creating a new application dispatch. Duplicate receipts are idempotent at the sender. Receipts do not require receipts of their own.

The local `/send` response is never an end-to-end receipt. A successful TCP/HTTP request to the local daemon is not evidence of remote acceptance.

## 6. Poison messages, capacity, and availability

The current journal intentionally applies strict head-of-line backpressure when a record cannot be dispatched or durably removed. It retains the record rather than silently discarding it. That is a defensible fail-closed journal rule but creates a node-wide availability risk if a poison record sits at the head of the queue.

DTN-3 MUST NOT claim both “no silent loss” and “unrelated traffic continues” until the implementation has a qualified policy for this case. The preferred design is a bounded, durable quarantine/dead-letter state: retain original bytes (or a separately protected immutable archive plus digest), record the reason and transition receipt, never dispatch the quarantined record, and resume other independently bounded lanes only after the quarantine state is durably committed. Quarantine storage itself must have a hard quota and observable full-state behavior; reaching that quota pauses destructive polls with an explicit alarm rather than deleting evidence. Never overwrite or silently prune poison records. The exact transition and crash semantics need fault-injection tests before implementation.

This design proposal does not change PR #4's current global backpressure behavior or qualify its availability properties.

## 7. Xenia, Kitsune2, and Mycelix boundaries

- DTN message identity and durable dedup state are independent of Xenia Wire's live-session replay window. A delayed bundle is not proof that an old session/key remains valid.
- Long-term envelope credential verification and policy checks happen against locally available trust state. A message does not resurrect an expired Xenia session, bypass preflight, or create a connected-peer fact.
- This envelope does not implement Kitsune2 preflight, peer-connect/disconnect semantics, BPSec, Holochain conductor integration, or confidentiality.
- Mycelix may provide principal/key bindings, capabilities, and revocation state, but a distributed governance action MUST NOT be synchronously required for every packet. Freshness requirements for cached trust state are policy-dependent; control-class traffic fails closed if required authority/revocation state is stale.
- COSE_Sign1 provides signature integrity/authentication only when the credential-to-principal binding is trusted. It does not encrypt the payload. Apply BPSec where bundle-layer security is required, and separately specify any end-to-end encryption/key-management profile.

## 8. Normative state table and crash behavior

This table is the required protocol state model. Implementations MUST persist the named durable transitions before exposing the corresponding disposition. A transition failing to commit leaves the previous durable state authoritative. A process restart MUST resume from durable state, not from an inferred log line or a volatile counter.

| Durable state | Event and guard | Required action before advancing | Next state / retry behavior |
|---|---|---|---|
| `raw_journaled` | Complete bundle bytes have been committed to the local receive journal | Decode within the configured bound; parse exact v1 envelope; validate signature, credential-to-principal binding, digest, class, expiry, and policy | `verified`; invalid content goes to bounded `quarantine_committed` or remains blocking if quarantine cannot commit |
| `verified` | No existing (`sender_id`, `message_id`) entry and all quotas allow admission | Atomically and durably reserve the dedup key, envelope digest, immutable received bytes/reference, expiry, and initial inbox status before dispatch | `inbox_reserved`; if reserve fails, do not dispatch |
| `verified` | Existing dedup key with the same envelope digest | Read the durable prior status/receipt; do not call the handler merely because a duplicate bundle arrived | `duplicate_observed`; return/retry the persisted receipt or current persisted disposition |
| `verified` | Existing dedup key but a different envelope digest | Persist conflict diagnosis and bounded quarantine state; do not dispatch the conflicting variant | `quarantine_committed`; emit conflict rejection only if the sender credential is authenticated and policy allows a response |
| `inbox_reserved` or `dispatch_pending` | Worker attempts dispatch | Preserve the durable inbox entry and original message ID across attempts. Apply application-specific idempotency key where the receiving application supports one | Remains pending until handler call returns success; on failure, retry according to local bounded backoff and surface diagnostics |
| `inbox_reserved` or `dispatch_pending` | Process crashes before or during handler execution | On restart, retry pending dispatch; do not infer that it executed or did not execute from absence of a completion marker | At-least-once; duplicate side effects remain possible unless the application operation is transactional or idempotent |
| `dispatch_pending` | Handler returns success | Durably store `handler_accepted` status and a stable signed receipt before reporting that disposition to the sender | `receipt_durable`; this is not an application-commit claim |
| `receipt_durable` | Receipt transmission is attempted but lost, delayed, or duplicated | Retain the exact signed receipt bytes and retry them; duplicate receipt delivery must be idempotent | Remains `receipt_durable` until retained/acknowledged under the selected receipt-transport policy |
| `receipt_durable` | Retiring local raw journal record | Delete/retire the raw record only after durable inbox/dedup state and the corresponding receipt state exist; sync the relevant storage boundary | `retired`; recovery must not require the raw record to reconstruct a committed receipt |
| Any state with an invalid/unknown clock or stale required authority state | Message is time-sensitive or authority-bearing | Fail closed for dispatch; keep durable evidence and report clock/trust freshness in diagnostics | Remain pending or transition to durable quarantine/rejection according to policy; never treat unknown time as fresh |
| Any state | Journal, inbox, receipt store, or bounded quarantine has no capacity / write fails | Do not dispatch an unreserved message, prune unexpired dedup entries, overwrite evidence, or claim a receipt that was not committed | Pause the affected lane and emit a bounded, observable fault; operator recovery must be explicit |

### Crash ordering rules

1. **Before raw journal commit:** the daemon's destructive `/endpoint` pop can still lose a bundle if the body read, process, or storage path fails. This remains a known limitation of the current dtn7 API.
2. **After raw journal commit but before inbox reservation:** restart re-reads the raw journal and repeats validation; no handler dispatch has occurred.
3. **After inbox reservation but before handler success:** the durable `dispatch_pending` state is retried. The handler may not have seen the message, or it may have partially acted; do not infer which from the crash.
4. **After handler success but before the handler-accepted marker/receipt is durably stored:** redelivery may occur. The handler-accepted receipt MUST NOT be fabricated from memory or logs after restart.
5. **After receipt is durably stored but before it is sent:** resend the same signed receipt bytes. The receiver MUST NOT create a new receipt ID for a retry of the same persisted transition.
6. **After receipt transmission but before local retirement:** a restart may retransmit the same receipt; the sender processes repeated receipts idempotently.
7. `application_committed` is unreachable from the current Kitsune2 transport alone. Only an application adapter with a defined durable transaction or idempotent command contract may emit that disposition.

### Quarantine is a durability boundary

Quarantine is not “delete and log.” A poison, invalid, or conflicting record may leave the active dispatch lane only after a bounded durable quarantine record containing the original bytes (or an immutable archive reference plus digest), observed reason, relevant identity/digest when trusted, and transition state has committed. If quarantine commit fails or its quota is exhausted, stop destructive polling for that lane and emit an operator-visible health failure. Never respond to unauthenticated input in a way that amplifies traffic; an authenticated, policy-approved sender may receive a stable rejection receipt after that receipt is durable.

A lane is only independently resumable if storage, counters, retention, and scheduler isolation for that lane are separately bounded and tested. Until then, global head-of-line blocking is the honest behavior; this spec does not promise that unrelated traffic will continue around a poison record.

## 9. Required qualification before implementation is called complete

The test-vector file is the canonical byte-level fixture for this proposal. A future implementation must consume it in automated tests rather than regenerate expected values with the code under test.

Required exact-head and fault-injection scenarios:
1. Valid vector verifies and decodes to the exact fields; signature tamper, payload tamper, protected-header tamper, bad digest, wrong key, and sender/key binding mismatch fail closed.
2. Non-deterministic CBOR, duplicate map keys, unknown fields/version, indefinite-length items, trailing bytes, oversized raw envelope, and oversized payload reject before unbounded allocation.
3. Same sender/message ID/same envelope is duplicate without a second dispatch; same ID with different envelope digest is rejected as conflict.
4. Crash after durable inbox commit, after handler success, before receipt commit, and before receipt send; retries and restarts yield the declared at-least-once semantics.
5. Lost, duplicated, delayed, and reordered receipts; sender resends identical message bytes; duplicate receipt processing is idempotent.
6. Expiry before receive, while queued, after restart, and immediately before dispatch; unknown/bad clock state; future timestamps; TTL limits by class.
7. Key rotation and revocation across long partitions, unrecognized/revoked credential, and stale Mycelix trust state.
8. Dedup/inbox full, journal full, quarantine full, unwritable directory, corrupt journal, poison record, and recovery collisions; no committed record is silently dropped.
9. Out-of-order arrival beyond the Xenia replay window and across key rotation; the adapter does not treat DTN identity as an active session.
10. Payloads from tiny inputs through the 5 MiB previously observed range and near the proposed 8 MiB cap, including memory/resource measurements.

A PASS must name the exact commit, deterministic fixtures, toolchain, test stages, and explicit non-claims. Focused envelope tests, full CI, real-daemon transport tests, BPSec qualification, and real Holochain conductor integration are separate evidence items.

## References

- [RFC 9171 — Bundle Protocol Version 7](https://www.rfc-editor.org/rfc/rfc9171.html)
- [RFC 9172 — Bundle Protocol Security (BPSec)](https://www.rfc-editor.org/rfc/rfc9172.html)
- [RFC 8949 — Concise Binary Object Representation (CBOR)](https://www.rfc-editor.org/rfc/rfc8949.html)
- [RFC 9052 — COSE Structures and Process](https://www.rfc-editor.org/rfc/rfc9052.html)
- [RFC 9053 — COSE Initial Algorithms](https://www.rfc-editor.org/rfc/rfc9053.html)
- [RFC 8032 — Edwards-Curve Digital Signature Algorithm](https://www.rfc-editor.org/rfc/rfc8032.html)
- [Xenia Wire](https://github.com/Luminous-Dynamics/xenia-wire)
