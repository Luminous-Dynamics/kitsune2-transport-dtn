# Protocol status

This crate is a research transport bridge, not yet a drop-in replacement for Kitsune2's connection-oriented transports.

## What is implemented

- `TxImp::send()` hands encoded Kitsune2 frames to a local `dtn7-rs` daemon as BPv7 bundle payloads.
- A receiver task destructively pops bundles from the registered DTN application endpoint, decodes BPv7, and passes the payload to `TxImpHnd::recv_data()`.
- Real-daemon scenarios have demonstrated delayed store-and-forward delivery, simultaneous bidirectional traffic, unordered complete delivery in the tested runs, and byte-identical payloads through 5 MB.
- Endpoint registration is treated as a load-bearing prerequisite and transport creation now fails on HTTP registration errors.

## What is not implemented

### Preflight and logical peer sessions

Kitsune2's connection transports call `TxImpHnd::peer_connect()` and exchange preflight data before ordinary data frames. This bridge currently delivers ordinary encoded frames directly to `recv_data()` and has not established equivalent DTN control-bundle semantics.

### Authenticated transport identity

A BPv7 source EID is mapped to a nominal Kitsune2 URL. This mapping is a logical address, not by itself a cryptographic authentication of the remote Kitsune2 peer. A production design must state how EIDs, Kitsune2 identities, and any BPSec or application-layer credentials are bound.

### Connected-peer reporting

`get_connected_peers()` returns an empty list because DTN reachability is eventual and time-varying rather than a live socket state. Before conductor integration, every Kitsune2 consumer of this method must be traced and its required semantics resolved.

### Disconnect and responsiveness

There is no live connection to close. The adapter currently ignores the optional disconnect payload and does not define a logical-session or unresponsive-peer model for DTN.

### Reliable application dispatch

`dtn7-rs` removes a bundle from the application queue when `/endpoint` returns it. Decode failure, unsupported source EID, missing payload, or `recv_data()` failure therefore loses the bundle. Exactly-once or retryable delivery would require an acknowledgement/requeue protocol above the current API.

## Proposed path to a complete transport

1. Define a versioned DTN control envelope distinguishing preflight, data, disconnect, and acknowledgement bundles.
2. On first communication with a peer, call `TxImpHnd::peer_connect()`, carry the resulting preflight over a bounded-lifetime control bundle, and validate the peer response before releasing queued data.
3. Bind the logical DTN EID to the peer identity expected by Kitsune2.
4. Define session expiry and re-preflight behavior after long partitions or process restarts.
5. Determine how Kitsune2 should interpret connected, reachable, and unresponsive under delay-tolerant semantics.
6. Test with non-noop handlers and then with two real Holochain conductors across a genuine partition.

## Claim boundary

The strongest current claim is:

> Serialized Kitsune2 protocol messages can traverse a real BPv7/dtn7-rs store-and-forward path and reach registered Kitsune2 handlers.

The repository does not yet demonstrate that a full Holochain conductor can substitute this transport while preserving all preflight, discovery, peer-state, gossip, and security behavior.
