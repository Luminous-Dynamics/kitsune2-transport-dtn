//! Self-contained real-daemon smoke test.
//!
//! Set `DTN_BIN` to the pinned `dtnd` executable and run with
//! `--features real-dtn-tests -- --ignored --nocapture`.

use bytes::Bytes;
use kitsune2_api::{
    BoxFut, K2Result, SpaceId, TransportFactory, TxBaseHandler, TxHandler, TxSpaceHandler, Url,
};
use kitsune2_transport_dtn::{DtnConfig, DtnTransportFactory};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

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

fn reserve_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    listener.local_addr().expect("reserved address").port()
}

fn test_root() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    std::env::temp_dir().join(format!("kitsune2-dtn-smoke-{}-{nonce}", std::process::id()))
}

fn spawn_daemon(binary: &Path, node: &str, workdir: &Path, web_port: u16, mtcp_port: u16) -> Child {
    Command::new(binary)
        .args([
            "-n",
            node,
            "-W",
            workdir.to_str().expect("UTF-8 workdir"),
            "-D",
            "sled",
            "-C",
            &format!("mtcp:port={mtcp_port}"),
            "-w",
            &web_port.to_string(),
            "--disable_nd",
            "-j",
            "2s",
        ])
        .kill_on_drop(true)
        .spawn()
        .expect("spawn dtnd")
}

async fn wait_for_port(port: u16) {
    tokio::time::timeout(Duration::from_secs(10), async move {
        loop {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("dtnd web port did not become ready");
}

fn config(web_port: u16, node_name: &str) -> DtnConfig {
    DtnConfig {
        web_port,
        node_name: node_name.into(),
        service: "kitsune2".into(),
        lifetime_secs: 3600,
        poll_interval: Duration::from_millis(50),
    }
}

#[tokio::test]
#[ignore = "requires DTN_BIN pointing to a built dtnd executable"]
async fn harness_starts_registers_connects_and_cleans_up() {
    let binary = PathBuf::from(std::env::var("DTN_BIN").expect("set DTN_BIN"));
    let root = test_root();
    let node1_dir = root.join("node1");
    let node2_dir = root.join("node2");
    std::fs::create_dir_all(&node1_dir).expect("create node1 state");
    std::fs::create_dir_all(&node2_dir).expect("create node2 state");

    let web1 = reserve_port();
    let web2 = reserve_port();
    let mtcp1 = reserve_port();
    let mtcp2 = reserve_port();
    let mut daemon1 = spawn_daemon(&binary, "node1", &node1_dir, web1, mtcp1);
    let mut daemon2 = spawn_daemon(&binary, "node2", &node2_dir, web2, mtcp2);
    wait_for_port(web1).await;
    wait_for_port(web2).await;

    let builder = Arc::new(kitsune2_core::default_test_builder());
    let transport1 = DtnTransportFactory {
        cfg: config(web1, "node1"),
    }
    .create(builder.clone(), Arc::new(NoopTxHandler))
    .await
    .expect("register node1 endpoint");
    let transport2 = DtnTransportFactory {
        cfg: config(web2, "node2"),
    }
    .create(builder, Arc::new(NoopTxHandler))
    .await
    .expect("register node2 endpoint");

    let http = reqwest::Client::new();
    for (web, peer) in [
        (web1, format!("mtcp://127.0.0.1:{mtcp2}/node2")),
        (web2, format!("mtcp://127.0.0.1:{mtcp1}/node1")),
    ] {
        http.get(format!("http://127.0.0.1:{web}/peers/add"))
            .query(&[("p", peer.as_str()), ("p_t", "STATIC")])
            .send()
            .await
            .expect("add peer request")
            .error_for_status()
            .expect("add peer status");
    }

    let space = SpaceId::from(Bytes::from_static(b"harnessed-smoke"));
    let recorder = Arc::new(Recorder::default());
    transport1.register_space_handler(space.clone(), Arc::new(Recorder::default()));
    transport2.register_space_handler(space.clone(), recorder.clone());
    let payload = Bytes::from_static(b"self-contained real-daemon smoke");
    transport1
        .send_space_notify(
            kitsune2_transport_dtn::node_url("node2").expect("node2 URL"),
            space,
            payload.clone(),
        )
        .await
        .expect("send smoke message");

    tokio::time::timeout(Duration::from_secs(15), async {
        while recorder.0.lock().expect("recorder poisoned").is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("smoke message was not delivered");
    let received = recorder.0.lock().expect("recorder poisoned");
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].1, payload);
    drop(received);
    drop((transport1, transport2));

    daemon1.start_kill().expect("stop node1");
    daemon2.start_kill().expect("stop node2");
    let _ = daemon1.wait().await;
    let _ = daemon2.wait().await;
    std::fs::remove_dir_all(root).expect("remove smoke state");
}
