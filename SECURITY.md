# Security model

This repository is a research prototype. It has not received an independent security audit and does not yet implement the complete Kitsune2 peer-session lifecycle.

## Trusted boundary

The adapter talks only to a `dtn7-rs` HTTP API bound on `127.0.0.1`. The local daemon, its configuration, its storage directory, and every process able to reach that local API are inside the trusted computing base.

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

## Destructive receive boundary

`dtn7-rs`'s `/endpoint` operation removes the next bundle from the application-agent queue as it returns it. The adapter cannot requeue that bundle. The following failures therefore cause permanent loss after retrieval:

- an oversized response;
- malformed BPv7 encoding;
- a missing payload block;
- an unsupported source EID;
- Kitsune2 frame decoding or permission failure;
- an error from the registered Kitsune2 handler;
- process termination between pop and dispatch.

Exactly-once or retryable application delivery requires an acknowledgement and replay protocol above the current API.

## Resource bounds

The adapter currently enforces:

- a five-second HTTP connection timeout;
- a thirty-second HTTP request timeout;
- a 64 MiB maximum `/endpoint` response body, enforced while streaming;
- URI-unreserved ASCII node and service identifiers of at most 255 bytes;
- nonzero daemon port, bundle lifetime, and polling interval.

The adapter does not bound the daemon's persisted bundle store, number of queued bundles, peer table, retry schedule, or network bandwidth. Expiry, storage pressure, routing loops, fragmentation, and daemon-level denial of service remain properties of the surrounding `dtn7-rs` deployment.

## Ordering and replay

Delivery ordering is not guaranteed. Real-daemon tests observed different reorderings across repeated runs.

The adapter relies on `dtn7-rs` for duplicate BPv7 bundle-ID suppression and adds no application-level sequence number, replay window, or deduplication key. A future protocol envelope should bind message identity, sequence, acknowledgement, expiry, and peer identity explicitly.

## Reporting security problems

For sensitive findings, use GitHub's private vulnerability reporting for this repository when available. For non-sensitive prototype defects, open a normal repository issue with a minimal reproduction and the pinned revisions involved.
