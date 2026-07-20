# Experimental evidence

This is the evidence ledger for the repository's real-daemon claims. It does not imply that the adapter already implements every Kitsune2 transport semantic.

## Pinned environment

- Kitsune2: `62cf3446e829eaed7f3948ed29eff6dfb712413c`
- dtn7-rs: `c30181b4b111e2adc5538931c797c0f7190acc4c`
- path exercised: `send_space_notify` -> `TxImp::send` -> local `dtnd` -> BPv7/MTCP -> remote `dtnd` -> `/endpoint` -> BPv7 decode -> `TxImpHnd::recv_data` -> registered space handler

All real-daemon tests require the `real-dtn-tests` feature. Ordinary `cargo test` remains hermetic.

## Results

| Scenario | Observed result | Boundary |
|---|---|---|
| Clean round trip | One notification delivered exactly once and byte-identically through two real daemons | Basic message carriage |
| Receiver absent at send | The sender daemon retained the bundle; after the receiver started, registered, and became reachable, delivery completed without an application resend | One disruption case, not a general partition theorem |
| Registration after reachability | The bundle reached the receiver daemon before its application endpoint existed and was lost | Registration must precede reachability in the tested configuration |
| Ordering | Ten messages were complete but out of order on two runs, with a different permutation each run | Ordering must not be assumed |
| Simultaneous bidirectional traffic | One message each way arrived byte-identically with no cross-contamination | Concurrent bidirectional use in the tested case |
| Payload tiers | 1 KB, 50 KB, 500 KB, 2 MB, and 5 MB arrived byte-identically | No boundary found through 5 MB |
| Harnessed smoke | CI allocated ports, started both pinned daemons, waited for readiness, registered endpoints before reachability, delivered a byte-identical notification, stopped both daemons, and removed temporary state | Automated clean-carriage proof only; no preflight or conductor semantics |

## Ordering observations

Sent:

```text
msg-000, msg-001, msg-002, msg-003, msg-004,
msg-005, msg-006, msg-007, msg-008, msg-009
```

Run one:

```text
msg-000, msg-005, msg-006, msg-001, msg-003,
msg-007, msg-002, msg-004, msg-009, msg-008
```

Run two:

```text
msg-000, msg-002, msg-001, msg-003, msg-005,
msg-004, msg-006, msg-009, msg-008, msg-007
```

The differing permutations show nondeterministic delivery in the exercised path; they do not identify which underlying layer reordered the bundles.

## Registration-lag finding

The original outage test accidentally allowed reachability before `/register?kitsune2` completed. The sender's retry transferred the bundle into the receiver daemon while no matching application agent existed, and the bundle disappeared without an application-visible error.

The generalized test started both daemons without peers, added reachability while the receiver endpoint was absent, sent the message, and registered the endpoint later. A repeated run checked receiver debug logs directly: bundle receipt and dispatch preceded registration by almost two seconds, and no later delivery/removal event followed.

## Delivery semantics established

- `send_space_notify` succeeds after backend handoff; it does not prove remote delivery.
- `/endpoint` destructively pops the next application bundle.
- Decode, source-EID, payload, or handler failure after the pop cannot be retried by this adapter.
- dtn7-rs suppresses already-known BPv7 bundle IDs; the adapter adds no application-level replay window.
- The adapter currently has no connected-peer set or Kitsune2 preflight/session protocol.

## Build the pinned daemon

```bash
git clone https://github.com/dtn7/dtn7-rs.git
cd dtn7-rs
git checkout c30181b4b111e2adc5538931c797c0f7190acc4c
cargo build --release -p dtn7
```

## Preferred reproduction: self-contained smoke

```bash
DTN_BIN=/absolute/path/to/dtn7-rs/target/release/dtnd \
cargo test --test harnessed_smoke --features real-dtn-tests \
  -- --ignored --nocapture
```

The harness allocates ports, creates fresh state, starts both daemons, probes readiness, registers endpoints before adding peer reachability, verifies one byte-identical notification, then reaps both daemons and removes state. GitHub Actions builds the exact pinned daemon and runs this test in `real-daemon-smoke`.

## Historical manual scenarios

The broader fixed-port scenarios remain available for targeted reproduction. Run them separately with fresh daemon state; they are not parallel-safe.

```bash
# With two bidirectionally peered daemons on web ports 3000/3001:
cargo test --test integration kitsune2_message_crosses_real_dtn_transport \
  --features real-dtn-tests -- --nocapture
cargo test --test integration kitsune2_messages_ordering_is_observed_not_assumed \
  --features real-dtn-tests -- --nocapture
cargo test --test integration kitsune2_bidirectional_simultaneous_traffic \
  --features real-dtn-tests -- --nocapture
cargo test --test integration kitsune2_payload_size_boundary \
  --features real-dtn-tests -- --nocapture
cargo test --test payload_regression \
  --features real-dtn-tests -- --nocapture
```

The receiver-outage scenario additionally uses `DTN_BIN_DIR` and `DTN_NODE2_WORKDIR`; the registration-lag scenario starts both daemons without static peers. Their exact setup is documented in `tests/integration.rs`.

## Remaining experiments

- queued delivery and logical identity across daemon and adapter restarts;
- explicit failing-handler regression after destructive retrieval;
- application-level duplicate observation;
- expiry and bounded storage under load;
- long-duration or multi-hop partitions;
- preflight, session establishment, disconnect, and peer-state semantics;
- two real Holochain conductors exchanging application data across a partition.
