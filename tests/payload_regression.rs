//! Hard regression guard for the documented 5 MB payload capability.
//!
//! Requires two externally managed, bidirectionally peered `dtnd` daemons on
//! web ports 3000 and 3001. Enable with `--features real-dtn-tests`.

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
struct Recorder(Mutex<Vec<(Url, Bytes)>>);
impl TxBaseHandler for Recorder {}
impl TxSpaceHandler for Recorder {
    fn recv_space_notify(&self, peer: Url, _space: SpaceId, data: Bytes) -> K2Result<()> {
        self.0.lock().expect("recorder poisoned").push((peer, data));
        Ok(())
    }

    fn is_any_agent_at_url_blocked(&self, _peer: &Url) -> K2Result<bool> {
        Ok(false)
    }

    fn has_local_agents(&self) -> BoxFut<'_, K2Result<bool>> {
        Box::pin(async { Ok(true) })
    }
}

fn config(port: u16, node: &str) -> DtnConfig {
    DtnConfig {
        web_port: port,
        node_name: node.into(),
        service: "kitsune2".into(),
        lifetime_secs: 3600,
        poll_interval: Duration::from_millis(100),
    }
}

#[tokio::test]
async fn five_megabyte_payload_remains_byte_identical() {
    let builder = Arc::new(kitsune2_core::default_test_builder());
    let tx = DtnTransportFactory {
        cfg: config(3000, "node1"),
    }
    .create(builder.clone(), Arc::new(NoopTxHandler))
    .await
    .expect("create sender");
    let rx = DtnTransportFactory {
        cfg: config(3001, "node2"),
    }
    .create(builder, Arc::new(NoopTxHandler))
    .await
    .expect("create receiver");

    let space = SpaceId::from(Bytes::from_static(b"payload-regression"));
    let recorder = Arc::new(Recorder::default());
    tx.register_space_handler(space.clone(), Arc::new(Recorder::default()));
    rx.register_space_handler(space.clone(), recorder.clone());

    let expected = Bytes::from(
        (0..5_000_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    tx.send_space_notify(
        kitsune2_transport_dtn::node_url("node2").expect("node2 URL"),
        space,
        expected.clone(),
    )
    .await
    .expect("5 MB send must be accepted");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if !recorder.0.lock().expect("recorder poisoned").is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "5 MB payload was not delivered before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let received = recorder.0.lock().expect("recorder poisoned");
    assert_eq!(received.len(), 1, "expected exactly one delivery");
    assert_eq!(received[0].1, expected, "payload must be byte-identical");
}
