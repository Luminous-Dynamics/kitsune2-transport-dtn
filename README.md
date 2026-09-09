# kitsune2-transport-dtn

A research [`Kitsune2`](https://github.com/holochain/kitsune2) `TxImp` / `TransportFactory` backed by a local [`dtn7-rs`](https://github.com/dtn7/dtn7-rs) daemon and BPv7 store-and-forward transport.

The goal is narrow: test whether Kitsune2 protocol traffic can use a delay-tolerant transport when peers are not simultaneously reachable. This repository is **not yet a drop-in Holochain conductor transport** and does not claim complete Kitsune2 session or security parity.

## Current claim boundary

Real-daemon experiments have demonstrated that serialized Kitsune2 traffic can traverse a BPv7/dtn7-rs store-and-forward path and reach registered Kitsune2 handlers, including delayed delivery, simultaneous bidirectional traffic, nondeterministic ordering, and byte-identical payloads through 5 MB in the tested scenarios.

The DTN-2 branch additionally implements a durable receive-before-dispatch journal:

> Raw bundle bytes that are successfully read from dtn7's destructive application endpoint are durably committed locally before BPv7 decoding or Kitsune2 dispatch. Pending records are replayed at least once after restart, and retained records backpressure newer destructive receives.

This still does **not** prove zero-loss delivery, exactly-once semantics, authenticated peer identity, full preflight/session equivalence, or Holochain conductor compatibility.

## Why a journal is necessary

`dtn7-rs` exposes application delivery through `GET /endpoint?<service>`, which removes the next application bundle as it returns it. Without another durable boundary, a process crash or handler failure after that pop loses the message.

DTN-2 changes the receive path to:

```text
dtn7 application queue
        |
        | destructive pop
        v
bounded HTTP response read
        |
        | unavoidable remaining loss window
        v
DURABLE LOCAL JOURNAL
        |
        +--> BPv7 decode
        +--> source/payload validation
        +--> Kitsune2 recv_data()
        |
        v
durable journal deletion
```

The journal cannot close the window **before** it receives the complete response body; that would require a change below this adapter, such as non-destructive/acknowledged application delivery in the daemon.

## Delivery semantics

### After journal commit: at-least-once

A committed record remains present until dispatch succeeds and journal deletion is durably completed. Process restart, BPv7 decode failure, source/payload validation failure, and handler failure do not intentionally discard the record.

If dispatch fails, the receiver retains the oldest pending record and stops issuing new destructive `/endpoint` polls until that record can progress. This is deliberate backpressure rather than silent loss.

### Duplicate window remains

A crash after the Kitsune2 handler has successfully processed a message but before the journal deletion is durable can replay that logical message after restart.

Therefore the current semantics are **at-least-once local replay**, not exactly-once application delivery.

Journal record names are local storage identities only. They are intentionally **not** application message IDs and are not used for duplicate suppression. A later protocol layer should define stable message identity, acknowledgements, replay windows, and duplicate-safe disposition.

### Ordering is not guaranteed

Real-daemon runs have observed complete delivery with different reorderings of back-to-back messages. Applications using this transport must not infer FIFO ordering unless a higher protocol layer provides it.

### Daemon duplicate handling is not application idempotency

`dtn7-rs` has its own BPv7 bundle-ID duplicate handling, but that is distinct from duplicate application delivery caused by local journal replay after a crash boundary. DTN-3 should address application-level identity explicitly.

## Journal safety properties

The DTN-2 journal is intentionally conservative:

- bounded total pending bytes (512 MiB default);
- 64 MiB maximum individual received bundle;
- file `sync_all()` before commit rename;
- directory sync after commit/removal on Unix;
- incomplete crash-temp records fail journal startup and remain for inspection;
- temp/pending recovery collisions are never overwritten;
- exact declared record length is checked before dispatch;
- CRC32-IEEE detects accidental local byte corruption;
- malformed/corrupt reserved journal state fails closed;
- Unix journal directory permissions are normalized to `0700` and new records to `0600`;
- read/cleanup failure stops destructive receive rather than continuing past uncertain durable state.

The checksum is **not** authentication or tamper resistance. A process or attacker that can write the journal is inside the current trusted computing base.

The default root is `.kitsune2-dtn-journal`. It can be relocated with:

```text
KITSUNE2_DTN_JOURNAL_ROOT
```

The maximum pending bytes can be configured with:

```text
KITSUNE2_DTN_JOURNAL_MAX_BYTES
```

The configured bound must be large enough for at least one maximum-size bundle.

## Endpoint registration invariant

A receiving application endpoint must exist before peer reachability permits a bundle to arrive. Real testing found that a reachable daemon can receive a bundle before its application endpoint is registered and silently lose it.

`DtnTransportFactory::create()` therefore treats `/register` success as load-bearing and does not return a transport after registration failure.

Deployments must still establish peer reachability in an order consistent with that invariant.

## How it works

### Send

`TxImp::send()` submits the encoded Kitsune2 bytes to the local daemon:

```text
POST http://127.0.0.1:<web_port>/send?dst=<eid>&lifetime=<secs>
```

`dtn7-rs` owns BPv7 storage, routing, retry, and forwarding after local acceptance.

### Receive

The background receiver:

1. replays and clears durable pending journal records before touching dtn7 again;
2. polls `GET /endpoint?<service>` only when the journal is unblocked;
3. reads the response under the 64 MiB bound;
4. durably journals the raw response;
5. decodes BPv7 and maps its source EID into the nominal Kitsune2 URL;
6. calls `TxImpHnd::recv_data()`;
7. durably removes the journal record only after successful handler return.

## Identity and session limitations

The source mapping:

```text
dtn://<node>/  ->  nominal Kitsune2 Url
```

is a logical routing mapping, **not cryptographic peer authentication**.

The current adapter does not yet provide:

- authenticated EID-to-Kitsune2 identity binding;
- BPSec verification;
- Kitsune2 preflight exchange over DTN;
- logical peer-session establishment/expiry;
- meaningful `get_connected_peers()` semantics (`[]` is currently returned);
- logical disconnect signalling;
- application message IDs, acknowledgements, or replay windows;
- Holochain conductor transport selection/integration.

See [`SECURITY.md`](SECURITY.md) for the trust boundary and [`docs/PROTOCOL_STATUS.md`](docs/PROTOCOL_STATUS.md) for the protocol gap analysis.

## Tests

Default CI is intended to cover formatting, locked compilation, clippy, unit tests, and hermetic HTTP/runtime-contract tests. Feature-gated real-daemon scenarios exercise a pinned `dtn7-rs` build.

The DTN-2 focused tests include journal persistence/reopen, duplicate local records for identical raw bytes, capacity backpressure, crash-temp recovery, corrupt/incomplete record rejection, recovery collision rejection, Unix permissions, and a hermetic proof that one retained invalid record prevents a second destructive endpoint poll.

Historical and real-daemon evidence is documented in [`docs/EVIDENCE.md`](docs/EVIDENCE.md).

## Research observations already established

The repository has previously exercised:

- clean Kitsune2 message round-trip through two real `dtnd` daemons;
- sending while a peer daemon is absent and eventual delivery after it becomes reachable;
- the endpoint-registration-before-reachability failure mode;
- multiple-message ordering as observed rather than assumed;
- simultaneous bidirectional traffic;
- deterministic payload patterns from 1 KB through 5 MB with byte-identical delivery.

These observations are scoped to their exact test conditions. They do not establish general partition tolerance, arbitrary-duration persistence, exactly-once semantics, or conductor compatibility.

## Next protocol tranches

The clean progression is:

1. **DTN-2 — durable receive journal:** qualify receive-before-dispatch persistence and strict backpressure at an exact head.
2. **DTN-3 — message envelope:** add stable message identity, expiry, duplicate-safe replay, explicit dispositions/acknowledgements, and bounded replay state.
3. **DTN-4 — authenticated identity:** bind DTN EID to Kitsune2/Xenia identity without treating routing labels as authentication.
4. **DTN-5 — conductor experiment:** only after the lower layers are explicit, test selectable transport integration with real Holochain conductors.

That ordering keeps local storage identity, network message identity, authenticated peer identity, and higher-level authority as separate proof boundaries.

## Running the externally managed scenarios

The real-daemon suite is feature-gated:

```bash
cargo test --features real-dtn-tests
```

The self-contained smoke harness expects `DTN_BIN` to identify the pinned `dtnd` executable. See the test sources and [`docs/EVIDENCE.md`](docs/EVIDENCE.md) for exact scenario setup and evidence boundaries.

## License

Apache-2.0.
