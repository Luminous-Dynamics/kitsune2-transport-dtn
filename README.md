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

## Origin

Built as part of a research session exploring partition-tolerant
verifiable computing (`/srv/luminous-dynamics/MASTER_ROADMAP.md`,
Workstream D — Holochain DHT/networking through a DTN bridge). See
`WORKSTREAM_D_KITSUNE2_DTN_TRANSPORT.md` in that repository for the full
narrative, including the investigation path that established Kitsune2's
transport layer was genuinely pluggable before any code was written.
