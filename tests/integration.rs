//! Real end-to-end proof: two Kitsune2 [`Transport`] instances, backed by
//! two real `dtnd` daemons, exchanging a `send_space_notify` message.
//!
//! Requires two real `dtnd` processes already running (node1 web-port 3000,
//! node2 web-port 3001, bidirectionally statically peered) -- this test does
//! not spawn them itself, matching this session's existing dtn7 testing
//! conventions.

use bytes::Bytes;
use kitsune2_api::{
    BoxFut, K2Result, SpaceId, TransportFactory, TxBaseHandler, TxHandler, TxSpaceHandler, Url,
};
use kitsune2_transport_dtn::{DtnConfig, DtnTransportFactory};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug)]
struct NoopTxHandler;
impl TxBaseHandler for NoopTxHandler {}
impl TxHandler for NoopTxHandler {}

#[derive(Debug, Default)]
struct RecordingSpaceHandler {
    received: Arc<Mutex<Vec<(Url, Bytes)>>>,
}

impl TxBaseHandler for RecordingSpaceHandler {}
impl TxSpaceHandler for RecordingSpaceHandler {
    fn recv_space_notify(&self, peer: Url, _space_id: SpaceId, data: Bytes) -> K2Result<()> {
        println!(
            "RECEIVED via DTN transport: {} bytes from {peer}",
            data.len()
        );
        self.received.lock().unwrap().push((peer, data));
        Ok(())
    }

    fn is_any_agent_at_url_blocked(&self, _peer_url: &Url) -> K2Result<bool> {
        Ok(false)
    }

    fn has_local_agents(&self) -> BoxFut<'_, K2Result<bool>> {
        Box::pin(async { Ok(true) })
    }
}

#[tokio::test]
async fn kitsune2_message_crosses_real_dtn_transport() {
    tracing_subscriber::fmt::try_init().ok();

    let space_id = SpaceId::from(Bytes::from_static(b"workstream-d-proof"));

    let factory1 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3000,
            node_name: "node1".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(200),
        },
    };
    let factory2 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3001,
            node_name: "node2".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(200),
        },
    };

    // default_test_builder() ships kitsune2's own in-memory mock transport;
    // create() ignores the builder anyway, so this only needs to exist to
    // satisfy the TransportFactory::create() signature.
    let builder = Arc::new(kitsune2_core::default_test_builder());

    let transport1 = factory1
        .create(builder.clone(), Arc::new(NoopTxHandler))
        .await
        .expect("create transport1");
    let transport2 = factory2
        .create(builder, Arc::new(NoopTxHandler))
        .await
        .expect("create transport2");

    let recorder = Arc::new(RecordingSpaceHandler::default());
    transport2.register_space_handler(space_id.clone(), recorder.clone());
    transport1.register_space_handler(space_id.clone(), Arc::new(RecordingSpaceHandler::default()));

    let node2_url = kitsune2_transport_dtn::node_url("node2").unwrap();

    let payload = Bytes::from_static(b"hello over a real BPv7/DTN bundle, from kitsune2");
    transport1
        .send_space_notify(node2_url, space_id, payload.clone())
        .await
        .expect("send_space_notify");

    // Give the poll-based receiver loop time to pick it up.
    for _ in 0..25 {
        if !recorder.received.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let received = recorder.received.lock().unwrap();
    assert_eq!(received.len(), 1, "expected exactly one message received");
    assert_eq!(received[0].1, payload, "payload must match exactly");
    println!(
        "PROOF: Kitsune2 send_space_notify -> recv_space_notify round-tripped \
         through two real dtnd daemons, byte-identical payload."
    );
}

/// Disruption test: `send_space_notify` is called while node2's daemon does
/// not exist yet at all (not just "unreachable" -- the process hasn't
/// started, matching the clean-state methodology from this session's
/// Workstream B Test 3, which avoids a real stale-MTCP-connection bug found
/// earlier this session). Proves the *Kitsune2-level* call survives a real
/// outage and eventually delivers once the peer comes online, not just that
/// raw dtn7 bundles do.
///
/// Requires node1's dtnd already running (web-port 3000, statically peered
/// to a not-yet-running node2 on mtcp port 16163) -- this test itself starts
/// node2's dtnd partway through, matching the disruption scenario, and does
/// not tear down either daemon (left running for inspection / the next
/// disruption test in this session).
#[tokio::test]
async fn kitsune2_message_survives_receiver_outage() {
    tracing_subscriber::fmt::try_init().ok();

    let space_id = SpaceId::from(Bytes::from_static(b"workstream-d-disruption"));

    let factory1 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3000,
            node_name: "node1".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(200),
        },
    };

    let builder = Arc::new(kitsune2_core::default_test_builder());

    let transport1 = factory1
        .create(builder.clone(), Arc::new(NoopTxHandler))
        .await
        .expect("create transport1");
    transport1.register_space_handler(space_id.clone(), Arc::new(RecordingSpaceHandler::default()));

    let node2_url = kitsune2_transport_dtn::node_url("node2").unwrap();
    let payload = Bytes::from_static(
        b"sent while node2's daemon did not exist yet -- must survive the outage",
    );

    // The send itself must succeed (dtn7's /send just enqueues locally and
    // returns; it does not wait for actual delivery) even though node2 is
    // completely absent.
    transport1
        .send_space_notify(node2_url, space_id.clone(), payload.clone())
        .await
        .expect("send_space_notify must succeed even though the peer is down");

    // Confirm real disruption, not a race: give it a couple of real retry
    // cycles worth of time with node2 still absent, nothing to observe on
    // our side since we have no node2 transport yet -- this just lets
    // dtnd's own janitor log a few genuine failed-connection attempts.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Now bring node2's daemon online for the first time.
    let bin_dir = std::env::var("DTN_BIN_DIR")
        .expect("set DTN_BIN_DIR to the dtn7-rs target/release directory before running this test");
    let work_dir = std::env::var("DTN_NODE2_WORKDIR")
        .expect("set DTN_NODE2_WORKDIR to a fresh, empty directory for node2's dtnd state");
    //
    // Deliberately NOT given a static peer back to node1 at startup: node1
    // must not be able to reach node2 at the transport level until *after*
    // node2's "kitsune2" endpoint is registered below. A first attempt at
    // this test found a real race here -- node1's static-peer retry loop
    // delivered the bundle to node2's daemon (transport-level MTCP success)
    // about a second before this test's own `/register?kitsune2` HTTP call
    // completed, so the bundle had nowhere registered to go and was lost.
    // Fixed by establishing the peer relationship dynamically, after
    // registration, instead of statically at daemon startup.
    let mut node2_daemon = tokio::process::Command::new(format!("{bin_dir}/dtnd"))
        .args([
            "-n",
            "node2",
            "-W",
            &work_dir,
            "-D",
            "sled",
            "-C",
            "mtcp:port=16163",
            "-w",
            "3001",
            "--disable_nd",
            "-j",
            "2s",
        ])
        .kill_on_drop(true)
        .spawn()
        .expect("failed to start node2's dtnd");

    // Wait for node2's web port to come up before registering our factory.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let factory2 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3001,
            node_name: "node2".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(200),
        },
    };
    let transport2 = factory2
        .create(builder, Arc::new(NoopTxHandler))
        .await
        .expect("create transport2 once node2 is up");
    let recorder = Arc::new(RecordingSpaceHandler::default());
    transport2.register_space_handler(space_id, recorder.clone());

    // Only now -- strictly after node2's "kitsune2" endpoint is registered
    // -- tell node1 how to reach node2. This is what actually triggers
    // node1's automatic retry/delivery.
    let http = reqwest::Client::new();
    http.get("http://127.0.0.1:3000/peers/add?p=mtcp://127.0.0.1:16163/node2&p_t=STATIC")
        .send()
        .await
        .expect("failed to add node2 as a peer of node1");

    // dtn7's own janitor retries automatically now that node1 knows about
    // node2 -- wait out a few retry cycles for genuine, automatic recovery
    // (no manual resend from this test).
    let mut delivered = false;
    for _ in 0..25 {
        if !recorder.received.lock().unwrap().is_empty() {
            delivered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        delivered,
        "message sent during the outage must be automatically delivered once node2 recovers"
    );
    let received = recorder.received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].1, payload,
        "payload must survive the outage byte-identical"
    );
    println!(
        "PROOF: a Kitsune2 send_space_notify() call issued while the peer's \
         daemon did not exist yet was automatically, correctly delivered \
         byte-identical once the peer came online and its endpoint was \
         registered -- one specific, narrow disruption case (peer absent at \
         send time), not a general claim about partitions, restarts, \
         duplicates, or reordering. See README.md 'Not yet tested' for what \
         remains open."
    );

    let _ = node2_daemon.start_kill();
}

/// Generalizes the race found in `kitsune2_message_survives_receiver_outage`:
/// that test found registration lagging *daemon startup*. This test asks
/// whether the invariant holds more generally -- does registration lagging
/// *established peer reachability* (regardless of why) lose the bundle the
/// same way? Peer reachability is established first via `/peers/add`, then
/// the send happens, then the endpoint is registered *afterward* -- if the
/// documented invariant is correct, this should also lose the message.
///
/// Requires node1's dtnd already running (web-port 3000, mtcp 16162, no
/// static peer) and node2's dtnd already running (web-port 3001, mtcp
/// 16163, no static peer) -- both started fresh, neither aware of the
/// other yet.
#[tokio::test]
async fn kitsune2_message_lost_when_registration_lags_established_reachability() {
    tracing_subscriber::fmt::try_init().ok();

    let space_id = SpaceId::from(Bytes::from_static(b"workstream-d-reglag"));
    let builder = Arc::new(kitsune2_core::default_test_builder());

    let factory1 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3000,
            node_name: "node1".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(200),
        },
    };
    let transport1 = factory1
        .create(builder.clone(), Arc::new(NoopTxHandler))
        .await
        .expect("create transport1");
    transport1.register_space_handler(space_id.clone(), Arc::new(RecordingSpaceHandler::default()));

    // Establish reachability FIRST, with node2's "kitsune2" endpoint not
    // registered yet (node2's daemon is running, but no transport object has
    // been created for it).
    let http = reqwest::Client::new();
    http.get("http://127.0.0.1:3000/peers/add?p=mtcp://127.0.0.1:16163/node2&p_t=STATIC")
        .send()
        .await
        .expect("failed to add node2 as a peer of node1");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let node2_url = kitsune2_transport_dtn::node_url("node2").unwrap();
    let payload =
        Bytes::from_static(b"sent while peer is reachable but its endpoint isn't registered yet");
    transport1
        .send_space_notify(node2_url, space_id.clone(), payload.clone())
        .await
        .expect("send_space_notify must succeed at the transport layer regardless");

    // Give node1 time to actually attempt delivery -- reachability is
    // already established, so this should happen quickly, well before we
    // register node2's endpoint below.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Register node2's endpoint LATE, after the send (and likely delivery
    // attempt) already happened.
    let factory2 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3001,
            node_name: "node2".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(200),
        },
    };
    let transport2 = factory2
        .create(builder, Arc::new(NoopTxHandler))
        .await
        .expect("create transport2");
    let recorder = Arc::new(RecordingSpaceHandler::default());
    transport2.register_space_handler(space_id, recorder.clone());

    // Wait to see whether it ever arrives.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let received = recorder.received.lock().unwrap();
    println!(
        "received count after late registration (general case): {}",
        received.len()
    );
    assert!(
        received.is_empty(),
        "expected the documented invariant to hold generally (registration \
         lagging established reachability loses the bundle), but the message \
         WAS delivered -- the invariant as documented may be narrower than \
         reality, needs re-investigating rather than just updating this \
         assertion"
    );
    println!(
        "CONFIRMED: registration-after-reachability loses the bundle in the \
         general case too, not only at daemon startup -- matches the \
         documented invariant."
    );
}

/// Sends several distinct messages back-to-back and checks whether delivery
/// order matches send order. The README documents this as explicitly
/// unguaranteed -- this test doesn't presuppose the answer: it hard-asserts
/// only that every message arrives (completeness), and separately reports
/// whether order happened to be preserved as an observed finding.
///
/// Requires both daemons already running and bidirectionally statically
/// peered (same setup as `kitsune2_message_crosses_real_dtn_transport`).
#[tokio::test]
async fn kitsune2_messages_ordering_is_observed_not_assumed() {
    tracing_subscriber::fmt::try_init().ok();

    let space_id = SpaceId::from(Bytes::from_static(b"workstream-d-ordering"));
    let builder = Arc::new(kitsune2_core::default_test_builder());

    let factory1 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3000,
            node_name: "node1".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(100),
        },
    };
    let factory2 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3001,
            node_name: "node2".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(100),
        },
    };
    let transport1 = factory1
        .create(builder.clone(), Arc::new(NoopTxHandler))
        .await
        .expect("create transport1");
    let transport2 = factory2
        .create(builder, Arc::new(NoopTxHandler))
        .await
        .expect("create transport2");

    let recorder = Arc::new(RecordingSpaceHandler::default());
    transport2.register_space_handler(space_id.clone(), recorder.clone());
    transport1.register_space_handler(space_id.clone(), Arc::new(RecordingSpaceHandler::default()));

    let node2_url = kitsune2_transport_dtn::node_url("node2").unwrap();

    const N: usize = 10;
    let sent: Vec<Bytes> = (0..N)
        .map(|i| Bytes::from(format!("msg-{i:03}").into_bytes()))
        .collect();
    for payload in &sent {
        transport1
            .send_space_notify(node2_url.clone(), space_id.clone(), payload.clone())
            .await
            .expect("send_space_notify");
    }

    for _ in 0..50 {
        if recorder.received.lock().unwrap().len() >= N {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let received = recorder.received.lock().unwrap();
    assert_eq!(
        received.len(),
        N,
        "completeness is required regardless of order: all {N} messages must arrive"
    );

    let recv_order: Vec<Bytes> = received.iter().map(|(_, d)| d.clone()).collect();
    let recv_set: std::collections::HashSet<Vec<u8>> =
        recv_order.iter().map(|b| b.to_vec()).collect();
    let sent_set: std::collections::HashSet<Vec<u8>> = sent.iter().map(|b| b.to_vec()).collect();
    assert_eq!(
        recv_set, sent_set,
        "every sent message must be present exactly once"
    );

    if recv_order == sent {
        println!("OBSERVED: delivery order matched send order for this run ({N} messages).");
    } else {
        println!(
            "OBSERVED: delivery order did NOT match send order for this run. \
             sent:     {:?}\n received: {:?}",
            sent.iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect::<Vec<_>>(),
            recv_order
                .iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect::<Vec<_>>()
        );
    }
}

/// Both nodes send to each other concurrently. Nothing in this session's
/// tests so far has exercised both directions at once -- every prior test
/// was strictly one-directional (node1 sends, node2 receives).
///
/// Requires both daemons already running and bidirectionally statically
/// peered (same setup as `kitsune2_message_crosses_real_dtn_transport`).
#[tokio::test]
async fn kitsune2_bidirectional_simultaneous_traffic() {
    tracing_subscriber::fmt::try_init().ok();

    let space_id = SpaceId::from(Bytes::from_static(b"workstream-d-bidirectional"));
    let builder = Arc::new(kitsune2_core::default_test_builder());

    let factory1 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3000,
            node_name: "node1".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(200),
        },
    };
    let factory2 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3001,
            node_name: "node2".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(200),
        },
    };
    let transport1 = factory1
        .create(builder.clone(), Arc::new(NoopTxHandler))
        .await
        .expect("create transport1");
    let transport2 = factory2
        .create(builder, Arc::new(NoopTxHandler))
        .await
        .expect("create transport2");

    let recorder1 = Arc::new(RecordingSpaceHandler::default());
    let recorder2 = Arc::new(RecordingSpaceHandler::default());
    transport1.register_space_handler(space_id.clone(), recorder1.clone());
    transport2.register_space_handler(space_id.clone(), recorder2.clone());

    let node1_url = kitsune2_transport_dtn::node_url("node1").unwrap();
    let node2_url = kitsune2_transport_dtn::node_url("node2").unwrap();

    let payload_1_to_2 = Bytes::from_static(b"node1-to-node2");
    let payload_2_to_1 = Bytes::from_static(b"node2-to-node1");

    let (r1, r2) = tokio::join!(
        transport1.send_space_notify(node2_url, space_id.clone(), payload_1_to_2.clone()),
        transport2.send_space_notify(node1_url, space_id.clone(), payload_2_to_1.clone()),
    );
    r1.expect("node1 -> node2 send");
    r2.expect("node2 -> node1 send");

    for _ in 0..50 {
        if !recorder1.received.lock().unwrap().is_empty()
            && !recorder2.received.lock().unwrap().is_empty()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let recv1 = recorder1.received.lock().unwrap();
    let recv2 = recorder2.received.lock().unwrap();
    assert_eq!(
        recv1.len(),
        1,
        "node1 must receive exactly one message from node2"
    );
    assert_eq!(recv1[0].1, payload_2_to_1);
    assert_eq!(
        recv2.len(),
        1,
        "node2 must receive exactly one message from node1"
    );
    assert_eq!(recv2[0].1, payload_1_to_2);
    println!(
        "PROOF: simultaneous bidirectional send_space_notify -- both \
         directions delivered correctly, byte-identical, no cross-contamination."
    );
}

/// Escalates payload size to find a practical boundary, rather than only
/// asserting "small payloads work." This exercises a code path the earlier,
/// separately-run raw-dtn7 large-payload test (a 1.9MB .happ file, sent via
/// the `dtnsend` CLI) did not: this crate's own HTTP client
/// (`reqwest::Client::post` with a `Vec<u8>` body) and its own poll-based
/// receiver, not dtn7's CLI tooling -- a genuinely different code path that
/// could have different limits (e.g. an HTTP framework body-size default).
///
/// Requires both daemons already running and bidirectionally statically
/// peered (same setup as `kitsune2_message_crosses_real_dtn_transport`).
#[tokio::test]
async fn kitsune2_payload_size_boundary() {
    tracing_subscriber::fmt::try_init().ok();

    let space_id = SpaceId::from(Bytes::from_static(b"workstream-d-size"));
    let builder = Arc::new(kitsune2_core::default_test_builder());

    let factory1 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3000,
            node_name: "node1".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(200),
        },
    };
    let factory2 = DtnTransportFactory {
        cfg: DtnConfig {
            web_port: 3001,
            node_name: "node2".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(200),
        },
    };
    let transport1 = factory1
        .create(builder.clone(), Arc::new(NoopTxHandler))
        .await
        .expect("create transport1");
    let transport2 = factory2
        .create(builder, Arc::new(NoopTxHandler))
        .await
        .expect("create transport2");

    let recorder = Arc::new(RecordingSpaceHandler::default());
    transport2.register_space_handler(space_id.clone(), recorder.clone());
    transport1.register_space_handler(space_id.clone(), Arc::new(RecordingSpaceHandler::default()));
    let node2_url = kitsune2_transport_dtn::node_url("node2").unwrap();

    // Deterministic, corruption-detecting fill pattern (not all-zero, so a
    // truncation or byte-order bug would actually be caught by the
    // byte-identical assertion below).
    fn make_payload(size: usize) -> Bytes {
        Bytes::from((0..size).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
    }

    for size in [1_000usize, 50_000, 500_000, 2_000_000, 5_000_000] {
        let payload = make_payload(size);
        recorder.received.lock().unwrap().clear();

        let send_result = transport1
            .send_space_notify(node2_url.clone(), space_id.clone(), payload.clone())
            .await;
        if let Err(e) = send_result {
            println!(
                "size {size} bytes: send_space_notify itself failed: {e:?} -- boundary found here"
            );
            break;
        }

        let mut delivered = false;
        for _ in 0..75 {
            if !recorder.received.lock().unwrap().is_empty() {
                delivered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        if !delivered {
            println!(
                "size {size} bytes: FAILED to deliver within timeout -- \
                 practical boundary found near here"
            );
            break;
        }

        let received = recorder.received.lock().unwrap();
        assert_eq!(
            received[0].1, payload,
            "size {size} bytes: delivered payload must be byte-identical, not just same length"
        );
        println!("size {size} bytes: OK, byte-identical");
    }
}
