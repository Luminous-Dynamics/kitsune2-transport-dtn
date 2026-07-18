# kitsune2-transport-dtn

A [Kitsune2](https://github.com/holochain/kitsune2) `TxImp`/`TransportFactory`
implementation backed by a local [`dtn7-rs`](https://github.com/dtn7/dtn7-rs)
daemon's HTTP API, instead of WebRTC.

Kitsune2 is the peer-to-peer / DHT communication framework underlying
Holochain 0.6's networking. Its default transport is WebRTC, negotiated via
a live signal server — which fundamentally assumes both peers are reachable
at roughly the same time. This crate is a proof-of-concept second transport,
carrying the same Kitsune2 protocol over
[BPv7](https://www.rfc-editor.org/rfc/rfc9171)/DTN's store-and-forward
model instead, so that peers who are *not* simultaneously reachable can
still exchange Kitsune2 traffic — the bundle is held and forwarded whenever
the recipient becomes reachable, with no live connection required at send
time.

**What this actually demonstrates, precisely**: a Kitsune2 message can be
delivered byte-identically through a real BPv7/dtn7-rs store-and-forward
path when the two peers are not continuously connected. It does **not**
yet demonstrate long-duration partitions, process restarts with persisted
state, duplicate delivery, bundle expiry under load, reordering, or
reconciliation after divergent application state — see "Not yet tested"
below for the honest list.

> **Invariant, load-bearing, not optional**: a receiving endpoint must be
> registered (`DtnTransportFactory::create()` completing, which calls
> dtn7's `/register`) *before* the peer relationship that makes it
> reachable is established. This adapter does **not** buffer or retry
> bundles addressed to an endpoint that doesn't exist yet — they are
> silently dropped by the daemon with no observable error. See "A real bug
> found" below for how this was discovered, and "Delivery semantics" for
> the full guarantee (or lack of one).

## Why this is possible at all

Reading Holochain's conductor config surface first suggested this wasn't
viable — `NetworkConfig` exposes only a `bootstrap_url`/`signal_url` pair,
with WebRTC as the only apparent data path. Going one layer deeper into
Kitsune2 itself changed that: it already ships a second real, in-tree
transport backend (`crates/transport_iroh`, using
[iroh](https://github.com/n0-computer/iroh) instead of WebRTC), proving the
abstraction genuinely supports swapping backends. Its actual `TxImp` trait
(`kitsune2_api::transport`) is small — four required methods
(`url()`, `send()`, `disconnect()`, `get_connected_peers()`) plus a
`TransportFactory` — because Kitsune2's `DefaultTransport` wrapper handles
all higher-level space/module routing, blocking, and preflight logic on top
of whatever the low-level implementation provides. This crate follows the
same pattern `transport_iroh` uses for encoding a non-network peer identity
inside Kitsune2's scheme-restricted `Url` type (which only accepts
`ws`/`wss`/`http`/`https`): the DTN node name is carried as the last path
segment of a nominal `ws://` URL that is never actually dialed.

## How it works

- `send()` issues `POST http://127.0.0.1:<web_port>/send?dst=<eid>&lifetime=<secs>`
  against the local `dtnd` daemon, with the outgoing bytes as the request
  body. dtn7 handles the actual store-and-forward delivery.
- A background poll loop calls `GET /endpoint?<service>` (dtn7's
  pop-next-bundle call) at a configurable interval, decodes any delivered
  bundle via the real [`bp7`](https://crates.io/crates/bp7) crate, extracts
  the payload block, and hands it to Kitsune2 via `TxImpHnd::recv_data()`.
- `DtnTransportFactory::create()` self-registers the local DTN endpoint
  before returning, so the transport is ready to receive as soon as it's
  constructed.

## Delivery semantics

Stated explicitly rather than left implicit, since DTN + polling raises
questions a normal live-socket transport doesn't:

- **At-most-once to the Kitsune2 handler, verified in this crate's own
  code, not assumed.** `GET /endpoint` *pops* the next bundle from dtn7's
  application-agent queue as part of that same HTTP call (confirmed by
  reading dtn7-rs's `endpoint()` handler directly) — the bundle is already
  gone from the daemon before this crate even tries to decode it. If CBOR
  decoding fails, if the payload block is missing, or if
  `TxImpHnd::recv_data()` itself returns an error, **there is no retry and
  no re-queueing**. The bundle is lost at that point, silently, with only
  a `tracing::warn!` log line.
- **Network-level duplicates are deduplicated by dtn7-rs itself**, verified
  by reading `core::processing::receive()`: bundles are identified by their
  real BPv7 bundle ID (source + creation timestamp + fragment info), and
  `store_add_bundle_if_unknown()` drops (and counts in a `dups` stat) any
  bundle whose ID has already been seen — so a retransmitted duplicate at
  the daemon layer will not reach `/endpoint` twice. This crate does not
  add any deduplication of its own on top; it relies entirely on dtn7-rs's.
- **Ordering is not guaranteed.** Nothing in this transport or in dtn7-rs's
  `/endpoint` pop order was verified to preserve send order across
  reconnects or multiple in-flight bundles — untested, not claimed.
- **Poller crash between pop and dispatch** would lose the bundle (it's
  already removed from dtn7's queue). Not yet tested, but follows directly
  from the pop-before-decode design above.

## Peer/endpoint identity

The Kitsune2 `Url` this transport reports for a node (`node_url()`) encodes
exactly two things: the DTN node name (`dtnd -n <name>`) and this crate's
fixed service name, as the last path segment of a nominal `ws://` URL. It
does **not** encode any physical daemon address (host/port) — those are
purely local config (`DtnConfig::web_port`) never exposed in the URL. This
means the identity Kitsune2 sees is a stable *logical* DTN identity, not a
transient network location — which is arguably the right property for an
intermittently-connected system (reconnecting doesn't change who a peer
*is*), but it also means this transport cannot currently distinguish "the
same DTN node, restarted" from "the same DTN node, still running" — both
present the identical Kitsune2 `Url`.

## Lifecycle (the bug, illustrated)

Before the fix — node1 already knows how to reach node2 (static peer) the
moment node2's daemon process starts, which is faster than this crate's
own registration call:

```text
node1                    dtn7 node1              dtn7 node2         node2 (this crate)
  |                          |                        |                    |
  |                          |                    (starts, MTCP up)        |
  | send_space_notify() ---->|                        |                    |
  |                          | --- bundle, real MTCP send --------------->  |
  |                          |                        | no "kitsune2"      |
  |                          |                        | endpoint yet:      |
  |                          |                        | dropped, silent    |
  |                          |                        |                    |
  |                          |                        |     factory2.create()
  |                          |                        |<---- /register ----|
  |                          |                        |   (too late)       |
```

After the fix — registration happens first, peer relationship is added
only afterward:

```text
node1                    dtn7 node1              dtn7 node2         node2 (this crate)
  |                          |                        |                    |
  |                          |                        |     factory2.create()
  |                          |                        |<---- /register ----|
  |                          |                        |  "kitsune2" exists |
  | send_space_notify() ---->|                        |                    |
  | /peers/add (node2) ----->|                        |                    |
  |                          | --- bundle, real MTCP send --------------->  |
  |                          |                        | delivers to        |
  |                          |                        | "kitsune2" ---------> recv_data()
```

## Honest limitations

- **`get_connected_peers()` always returns empty.** DTN is store-and-forward,
  not connection-oriented — there is no live "currently connected" set to
  report. Any Kitsune2 logic that depends on knowing which peers are
  reachable *right now* (as opposed to "reachable eventually") won't get a
  meaningful answer from this transport.
- **Receiving is poll-based**, not push-based — dtn7's `/endpoint` is a
  pop-the-next-bundle HTTP call, not a subscription. Latency is bounded by
  the poll interval, not by real bundle delivery time.
- **Peer/endpoint registration ordering matters and is not free** (see
  below) — this is a real operational property of the underlying daemon,
  not something this crate smooths over.
- Only tested at small (tens-of-bytes) payload sizes. The interplay between
  DTN bundle lifetimes/chunking and Kitsune2's own message sizing at real
  Holochain scale is unexplored.
- This is the transport primitive only, not a Holochain conductor
  integration — wiring the actual `holochain` conductor to select this
  transport instead of its built-in WebRTC path is a separate, unstarted
  piece of work (the conductor's config doesn't currently expose a
  transport-choice field at all).

## A real bug found (and fixed) while building the disruption test

Building `kitsune2_message_survives_receiver_outage` (see
`tests/integration.rs`) surfaced a genuine ordering hazard, not a flake.
The first version of the test statically pre-configured node1's peer
relationship to node2 at daemon startup. Node1's automatic retry delivered
a bundle to node2's daemon (a real, successful MTCP transfer) about one
second *before* this crate's own `/register?<service>` HTTP call had
completed — so the bundle arrived addressed to an endpoint that didn't
exist yet, and was silently lost. No delivery log line, no error, nothing.

**Fix**: don't pre-configure the peer relationship statically. Register the
local endpoint first, and only then add the peer dynamically via dtn7's
`GET /peers/add` API — guaranteeing, by construction, that registration
happens before the daemon can possibly route anything to that endpoint.

**The takeaway for real deployments**: endpoint registration has to happen
before peer reachability is established, or bundles that arrive in that
window are lost with zero signal that it happened — the same
zero-observability theme found independently in dtn7-rs itself (see
`https://github.com/dtn7/dtn7-rs/issues/85`), just one layer higher, at
the transport-bootstrap level rather than the bundle-expiry level.

## Running the tests

Requires a built `dtn7-rs` `dtnd` binary (tested against
[`dtn7-rs` @ `c30181b4b1`](https://github.com/dtn7/dtn7-rs/commit/c30181b4b111e2adc5538931c797c0f7190acc4c)):

```bash
git clone https://github.com/dtn7/dtn7-rs.git
cd dtn7-rs && git checkout c30181b4b111e2adc5538931c797c0f7190acc4c
cargo build --release -p dtn7
```

**Clean round-trip test** (`kitsune2_message_crosses_real_dtn_transport`) —
start both daemons bidirectionally peered first:

```bash
dtnd -n node1 -W ./node1 -D sled -C mtcp:port=16162 -w 3000 --disable_nd -j 2s -s mtcp://127.0.0.1:16163/node2 &
dtnd -n node2 -W ./node2 -D sled -C mtcp:port=16163 -w 3001 --disable_nd -j 2s -s mtcp://127.0.0.1:16162/node1 &
cargo test --test integration kitsune2_message_crosses_real_dtn_transport -- --nocapture
```

**Disruption test** (`kitsune2_message_survives_receiver_outage`) — start
only node1 (no static peer), the test spawns node2 itself partway through:

```bash
dtnd -n node1 -W ./node1 -D sled -C mtcp:port=16162 -w 3000 --disable_nd -j 2s &
DTN_BIN_DIR=/path/to/dtn7-rs/target/release \
DTN_NODE2_WORKDIR=/path/to/a/fresh/empty/dir \
cargo test --test integration kitsune2_message_survives_receiver_outage -- --nocapture
```

## Not yet tested

Listed explicitly so scope isn't implied by silence. None of these are
started — a real backlog, not a to-do buried in prose:

1. **Receiver registers late** (beyond the one race already found and
   fixed) — is there any scenario where a late-arriving bundle *should* be
   retained rather than dropped, and if so, at which layer?
2. **Receiver process restarts** with dtn7's `-D sled` persistent backend —
   do queued-but-undelivered bundles survive, and does Kitsune2-level
   identity (the `Url`) remain stable across the restart?
3. **Handler fails after retrieval** — confirmed above that there's no
   retry; not yet exercised as an actual test.
4. **Duplicate bundle delivery** at the Kitsune2 layer specifically (dtn7's
   own dedup is verified; whether anything above it could still observe a
   duplicate is not).
5. **Out-of-order delivery** across alternating connectivity.
6. **Bundle expiry and bounded daemon storage under load** — this session's
   broader DTN research separately found a real dtn7-rs issue in this area
   (expired bundles bypass discard accounting/reporting —
   [dtn7-rs#85](https://github.com/dtn7/dtn7-rs/issues/85)), not yet
   re-tested through this Kitsune2 layer specifically.
7. **Bidirectional simultaneous traffic**, both nodes sending and receiving
   during a reconnect window.
8. **Payload size boundary** — only tested at tens of bytes; the real
   practical limit (interaction with DTN bundle lifetime/chunking and
   Kitsune2's own message sizing) is unknown.

## Origin

Built as part of a research session exploring partition-tolerant
verifiable computing (`/srv/luminous-dynamics/MASTER_ROADMAP.md`,
Workstream D — Holochain DHT/networking through a DTN bridge). See
`WORKSTREAM_D_KITSUNE2_DTN_TRANSPORT.md` in that repository for the full
narrative, including the investigation path that established Kitsune2's
transport layer was genuinely pluggable before any code was written.
