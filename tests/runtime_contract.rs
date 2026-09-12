//! Hermetic runtime-contract tests using a minimal local HTTP server.

use kitsune2_api::{TransportFactory, TxBaseHandler, TxHandler};
use kitsune2_transport_dtn::{DtnConfig, DtnTransportFactory};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
struct NoopTxHandler;
impl TxBaseHandler for NoopTxHandler {}
impl TxHandler for NoopTxHandler {}

fn config(port: u16, poll_interval: Duration) -> DtnConfig {
    DtnConfig {
        web_port: port,
        node_name: "node-1".into(),
        service: "kitsune2".into(),
        lifetime_secs: 3600,
        poll_interval,
    }
}

async fn read_request(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await.expect("read HTTP request");
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..read]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        assert!(bytes.len() < 16 * 1024, "unexpectedly large test request");
    }
    String::from_utf8(bytes).expect("request must be UTF-8 HTTP headers")
}

async fn respond(stream: &mut TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write HTTP response");
    stream.shutdown().await.expect("shutdown response stream");
}

#[tokio::test]
async fn registration_error_status_fails_transport_creation() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let port = listener.local_addr().expect("test address").port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept registration");
        let request = read_request(&mut stream).await;
        assert!(request.starts_with("GET /register?kitsune2 "));
        respond(&mut stream, "500 Internal Server Error", "").await;
    });

    let factory = DtnTransportFactory {
        cfg: config(port, Duration::from_millis(25)),
    };
    let result = factory
        .create(
            Arc::new(kitsune2_core::default_test_builder()),
            Arc::new(NoopTxHandler),
        )
        .await;

    assert!(result.is_err(), "HTTP 500 registration must fail closed");
    server.await.expect("registration server task");
}

#[tokio::test]
async fn dropping_transport_stops_endpoint_polling() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let port = listener.local_addr().expect("test address").port();
    let polls = Arc::new(AtomicUsize::new(0));
    let server_polls = polls.clone();

    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let request = read_request(&mut stream).await;
            if request.starts_with("GET /register?kitsune2 ") {
                respond(&mut stream, "200 OK", "registered").await;
            } else if request.starts_with("GET /endpoint?kitsune2 ") {
                server_polls.fetch_add(1, Ordering::SeqCst);
                respond(&mut stream, "200 OK", "Nothing to receive").await;
            } else {
                respond(&mut stream, "404 Not Found", "").await;
            }
        }
    });

    let transport = DtnTransportFactory {
        cfg: config(port, Duration::from_millis(20)),
    }
    .create(
        Arc::new(kitsune2_core::default_test_builder()),
        Arc::new(NoopTxHandler),
    )
    .await
    .expect("create transport");

    tokio::time::timeout(Duration::from_secs(2), async {
        while polls.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("receiver should poll the endpoint");

    drop(transport);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let after_drop = polls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        polls.load(Ordering::SeqCst),
        after_drop,
        "poll count advanced after transport drop"
    );

    server.abort();
}
