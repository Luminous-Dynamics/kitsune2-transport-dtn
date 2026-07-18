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
