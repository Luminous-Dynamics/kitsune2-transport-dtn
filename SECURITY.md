# Security model

This repository is a research prototype. It has not received an independent security audit and does not yet implement the complete Kitsune2 peer-session lifecycle.

## Trusted boundary

The adapter talks only to a `dtn7-rs` HTTP API bound on `127.0.0.1`. The local daemon, its configuration, its storage directory, the DTN receive journal, and processes able to reach or modify these local resources are inside the trusted computing base.

A successful `/send` or `/register` response means only that the local daemon accepted the request. It does not prove remote delivery or remote authorization.

## Peer identity

The adapter maps a BPv7 source EID of the form `dtn://<node>/` into a nominal Kitsune2 URL. This is a logical routing identity, not cryptographic authentication.

The current implementation does not:

- bind the DTN node name to a Kitsune2 agent or node key;
- verify BPSec blocks;
- perform Kitsune2 preflight exchange;
- establish an authenticated transport session;
- prevent an authorized local or DTN-layer sender from claiming another syntactically valid source EID where the surrounding deployment permits that.

Applications must not treat the peer URL produced by this adapter as independently authenticated identity.

## Destructive receive and durable handoff

`dtn7-rs`'s `/endpoint` operation removes the next bundle from the application-agent queue while returning its HTTP response. Because this adapter sits above that API, a loss window remains between the daemon's destructive pop and completion of the bounded HTTP read plus durable local journal commit.

After the raw response is successfully journaled, the adapter retains it until Kitsune2 dispatch succeeds and journal deletion is durably completed. Decode failure, missing payload, unsupported EID, handler failure, or process restart therefore no longer intentionally discards an already committed journal record.

The receiver fails closed/backpressures when durable handoff is not clean:

- journal readiness is checked before endpoint registration;
- incomplete/corrupt crash-temp records block startup and are retained;
- recovery never overwrites an existing pending record;
- malformed, oversized, wrong-length, or checksum-invalid committed records are not dispatched;
- handler failure retains the oldest pending record and prevents newer destructive polls;
- journal read failure or cleanup failure stops the receiver instead of continuing to pop bundles.

This provides **at-least-once replay after journal commit**, not exactly-once delivery. A crash after handler success but before durable journal deletion can cause duplicate application delivery after restart.

## Journal integrity and confidentiality

On Unix, the journal directory is normalized to mode `0700` and newly created record files to `0600`.

Each record commits its expected byte length and a CRC32-IEEE checksum in its local storage name. The checksum is only an accidental-corruption/torn-state detector. It is **not** a MAC, signature, or authentication mechanism. An attacker who can write the journal or control the host remains inside the trusted computing base and can alter, replace, delete, or replay records.

Journal record identity is deliberately opaque local storage identity. It must not be promoted into network/application message identity or used as proof of uniqueness.

## Resource bounds

The adapter currently enforces:

- a five-second HTTP connection timeout;
- a thirty-second HTTP request timeout;
- a 64 MiB maximum `/endpoint` response body, enforced while streaming;
- a bounded receive journal (512 MiB default, configurable with `KITSUNE2_DTN_JOURNAL_MAX_BYTES` and required to be at least one maximum-size bundle);
- exact per-record maximum size, length verification, and checksum verification;
- URI-unreserved ASCII node and service identifiers of at most 255 bytes;
- nonzero daemon port, bundle lifetime, and polling interval.

The journal root defaults to `.kitsune2-dtn-journal` and may be relocated with `KITSUNE2_DTN_JOURNAL_ROOT`. Deployments should place it on an appropriate persistent local filesystem with host-level access controls and backup/retention policy consistent with the sensitivity of transported application data.

The adapter does not bound the daemon's persisted bundle store, number of queued bundles, peer table, retry schedule, or network bandwidth. Expiry, storage pressure, routing loops, fragmentation, and daemon-level denial of service remain properties of the surrounding `dtn7-rs` deployment.

A poison record that consistently fails application dispatch intentionally causes receive backpressure. That is preferable to silently skipping the record, but it is also a denial-of-service surface until DTN-3 defines authenticated message identity, disposition/acknowledgement semantics, and an explicit operator policy for invalid messages.

## Ordering, replay, and duplicates

Delivery ordering is not guaranteed. Real-daemon tests observed different reorderings across repeated runs.

The adapter relies on `dtn7-rs` for duplicate BPv7 bundle-ID suppression before application retrieval, but local at-least-once journal replay can still duplicate a logical application delivery across a crash between handler success and journal deletion.

The current bridge adds no application-level stable message id, replay window, sequence number, acknowledgement, or deduplication key. DTN-3 should bind message identity, expiry, acknowledgement/disposition, and peer identity explicitly rather than using local journal identity.

## Remaining non-claims

The durable journal does not prove:

- protection against the pre-journal destructive-pop window;
- exactly-once delivery;
- authenticity or authorization of a BPv7 source EID;
- Byzantine/tamper-resistant journal integrity;
- full Kitsune2 preflight/session semantics;
- full Holochain conductor compatibility.

## Reporting security problems

For sensitive findings, use GitHub's private vulnerability reporting for this repository when available. For non-sensitive prototype defects, open a normal repository issue with a minimal reproduction and the pinned revisions involved.
