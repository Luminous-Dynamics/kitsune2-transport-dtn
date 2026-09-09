# Protocol status

This crate is a research transport bridge, not yet a drop-in replacement for Kitsune2's connection-oriented transports.

## What is implemented

- `TxImp::send()` hands encoded Kitsune2 frames to a local `dtn7-rs` daemon as BPv7 bundle payloads.
- A receiver task polls the registered DTN application endpoint and hands decoded payloads to `TxImpHnd::recv_data()`.
- Endpoint registration is a load-bearing prerequisite: transport creation fails on HTTP registration errors.
- A successfully read raw `/endpoint` response is durably journaled **before** BPv7 decoding or Kitsune2 dispatch.
- Pending journal records are replayed in deterministic local storage order after restart and before further destructive endpoint polls.
- A blocked pending record applies backpressure: the receiver does not destructively pop a newer bundle until the older record dispatches and its journal entry is durably removed.
- Journal records are bounded, length-checked, protected with a local accidental-corruption checksum, and permission-hardened to `0700` directories / `0600` files on Unix.
- Real-daemon scenarios have demonstrated delayed store-and-forward delivery, simultaneous bidirectional traffic, unordered complete delivery in the tested runs, and byte-identical payloads through 5 MB.

## Receive durability boundary

`dtn7-rs`'s `/endpoint` call is destructive. The daemon removes the application bundle as it returns the HTTP response, so this adapter cannot eliminate every receive-side loss window from outside the daemon.

The current sequence is:

```text
dtn7 application queue
        |
        | destructive /endpoint response begins
        v
bounded HTTP body read
        |
        | remaining unavoidable pre-journal loss window
        v
durable local journal commit
        |
        +--> BPv7 decode
        +--> source/payload validation
        +--> Kitsune2 recv_data()
        |
        v
durable journal deletion
```

Once the raw response is committed to the local journal, decode failure, unsupported EID, missing payload, handler failure, or process restart no longer silently discards that local record. It remains pending and backpressures newer destructive receives.

This is **at-least-once local replay**, not exactly-once delivery. A crash after the Kitsune2 handler succeeds but before the journal deletion becomes durable can replay the logical message after restart.

Journal record identity is deliberately **not** application message identity. DTN-3 should add an explicit protocol message identifier and duplicate-safe application semantics rather than smuggling deduplication into a filesystem filename.

## Poison and corruption behavior

The journal fails closed when its durable handoff state is ambiguous:

- incomplete crash-temp records block journal startup and are retained for inspection;
- a temp/pending recovery collision is not overwritten;
- malformed or oversized reserved pending records block startup/processing;
- exact record length and checksum are verified before dispatch;
- failure to read or durably delete a record stops destructive receiving;
- handler failure retains the record and retries later without polling another bundle.

The checksum detects accidental local corruption only. It is not authentication and does not protect against an attacker with write access to the journal or host.

## What is not implemented

### Preflight and logical peer sessions

Kitsune2's connection transports call `TxImpHnd::peer_connect()` and exchange preflight data before ordinary data frames. This bridge currently delivers ordinary encoded frames directly to `recv_data()` and has not established equivalent DTN control-bundle semantics.

### Authenticated transport identity

A BPv7 source EID is mapped to a nominal Kitsune2 URL. This mapping is a logical address, not by itself cryptographic authentication of the remote Kitsune2 peer. A production design must state how EIDs, Kitsune2 identities, and any BPSec or application-layer credentials are bound.

### Connected-peer reporting

`get_connected_peers()` returns an empty list because DTN reachability is eventual and time-varying rather than a live socket state. Before conductor integration, every Kitsune2 consumer of this method must be traced and its required semantics resolved.

### Disconnect and responsiveness

There is no live connection to close. The adapter currently ignores the optional disconnect payload and does not define a logical-session or unresponsive-peer model for DTN.

### Application message identity and acknowledgement

The journal makes post-commit retry possible, but it does not define a stable application message id, sequence, replay window, receiver acknowledgement, sender retry policy, or exactly-once semantics. These belong in a versioned envelope above the raw local journal.

## Proposed path to a complete transport

1. **DTN-2:** qualify the durable receive-before-dispatch journal and crash/backpressure semantics at an exact head.
2. **DTN-3:** define a versioned envelope with message identity, duplicate-safe replay, expiry, and acknowledgements.
3. Define a DTN control envelope distinguishing preflight, data, disconnect, and acknowledgement messages.
4. On first communication with a peer, call `TxImpHnd::peer_connect()`, carry the resulting preflight over a bounded-lifetime control message, and validate the peer response before releasing queued data.
5. Cryptographically bind logical DTN EID to the peer identity expected by Kitsune2.
6. Define session expiry and re-preflight behavior after long partitions or process restarts.
7. Determine how Kitsune2 should interpret connected, reachable, and unresponsive under delay-tolerant semantics.
8. Test with non-noop handlers and then with two real Holochain conductors across a genuine partition.

## Claim boundary

The strongest implementation claim of the DTN-2 branch is:

> Serialized Kitsune2 protocol messages can traverse a real BPv7/dtn7-rs store-and-forward path, and raw bundles that are successfully read from the destructive application endpoint are durably journaled before decode/dispatch with at-least-once local replay and strict backpressure on retained records.

This does **not** claim zero-loss delivery across the destructive-pop-to-journal window, exactly-once semantics, authenticated peer identity, full Kitsune2 session equivalence, or Holochain conductor compatibility.
