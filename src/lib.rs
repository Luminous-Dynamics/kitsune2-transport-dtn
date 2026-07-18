//! A Kitsune2 [`TxImp`]/[`TransportFactory`] backed by a local `dtn7-rs`
//! daemon's HTTP API, instead of WebRTC.
//!
//! Workstream D proof-of-concept: real Kitsune2 traffic carried over a real
//! BPv7/DTN store-and-forward transport. Peer addressing is DTN node name,
//! encoded as the last path segment of a nominal `ws://` URL (matching the
//! precedent set by kitsune2's own `transport_iroh` crate, which encodes an
//! iroh EndpointId the same way inside a relay URL's path).
//!
//! Honest limitations, not hidden:
//! - `get_connected_peers()` always returns empty: DTN is store-and-forward,
//!   not connection-oriented, so there is no live "connected" set to report.
//! - Receiving is poll-based (dtn7's `/endpoint` is a pop-next-bundle HTTP
//!   call, not a push/subscribe API), so latency is bounded by the poll
//!   interval, not by real bundle delivery time.
//! - No retry/backoff tuning beyond dtn7's own; this crate is a thin bridge,
//!   not a reimplementation of DTN semantics.

use bytes::Bytes;
use kitsune2_api::{
    BoxFut, Builder, Config, DefaultTransport, DynTransport, DynTxHandler, DynTxImp, K2Error,
    K2Result, TransportConnectionStats, TransportFactory, TransportStats, TxImp, TxImpHnd, Url,
};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the DTN-backed transport.
#[derive(Clone, Debug)]
pub struct DtnConfig {
    /// The local dtn7-rs daemon's HTTP web-port (e.g. 3000).
    pub web_port: u16,
    /// This node's DTN node name (must match `dtnd -n <name>`).
    pub node_name: String,
    /// The local endpoint service name to register/poll (e.g. "kitsune2").
    pub service: String,
    /// Bundle lifetime, in seconds.
    pub lifetime_secs: u64,
    /// How often to poll `/endpoint` for a new bundle.
    pub poll_interval: Duration,
}

/// Build the nominal Kitsune2 [`Url`] for a given DTN node name.
///
/// Host/port are placeholders to satisfy Kitsune2's Url parser (which does
/// no DNS resolution, only syntax checks) -- the real addressing information
/// is the DTN node name, carried as the last path segment.
pub fn node_url(node_name: &str) -> K2Result<Url> {
    Url::from_str(format!("ws://dtn.local:1/{node_name}"))
}

struct DtnTxImp {
    cfg: DtnConfig,
    client: reqwest::Client,
    my_url: Url,
}

impl std::fmt::Debug for DtnTxImp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DtnTxImp {{ node: {} }}", self.cfg.node_name)
    }
}

fn peer_node_name(url: &Url) -> K2Result<&str> {
    url.peer_id()
        .ok_or_else(|| K2Error::other("dtn peer url has no node-name path segment"))
}

impl TxImp for DtnTxImp {
    fn url(&self) -> Option<Url> {
        Some(self.my_url.clone())
    }

    fn send(&self, peer: Url, data: Bytes) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            let peer_name = peer_node_name(&peer)?;
            let dst = format!("dtn://{}/{}", peer_name, self.cfg.service);
            let send_url = format!(
                "http://127.0.0.1:{}/send?dst={}&lifetime={}s",
                self.cfg.web_port,
                urlencoding::encode(&dst),
                self.cfg.lifetime_secs
            );
            let resp = self
                .client
                .post(&send_url)
                .body(data.to_vec())
                .send()
                .await
                .map_err(|e| K2Error::other_src("dtn /send failed", e))?;
            if !resp.status().is_success() {
                return Err(K2Error::other(format!(
                    "dtn /send returned status {}",
                    resp.status()
                )));
            }
            Ok(())
        })
    }

    fn disconnect(&self, _peer: Url, _payload: Option<(String, Bytes)>) -> BoxFut<'_, ()> {
        // DTN has no live connection to close -- store-and-forward, not
        // connection-oriented. Best-effort payload delivery isn't
        // meaningful here either; a real disconnect notice would just be
        // another bundle, which callers can send via `send()` directly.
        Box::pin(async {})
    }

    fn get_connected_peers(&self) -> BoxFut<'_, K2Result<Vec<Url>>> {
        // Honest limitation: no live "connected" concept over DTN.
        Box::pin(async { Ok(vec![]) })
    }

    fn dump_network_stats(&self) -> BoxFut<'_, K2Result<TransportStats>> {
        let my_url = self.my_url.clone();
        Box::pin(async move {
            Ok(TransportStats {
                backend: "dtn7".into(),
                peer_urls: vec![my_url],
                connections: Vec::<TransportConnectionStats>::new(),
            })
        })
    }
}

fn extract_payload(bndl: &bp7::Bundle) -> Option<Vec<u8>> {
    let block = bndl.extension_block_by_type(bp7::canonical::PAYLOAD_BLOCK)?;
    match block.data() {
        bp7::canonical::CanonicalData::Data(data) => Some(data.clone()),
        _ => None,
    }
}

fn spawn_receiver(cfg: DtnConfig, client: reqwest::Client, hnd: Arc<TxImpHnd>) {
    tokio::spawn(async move {
        let poll_url = format!("http://127.0.0.1:{}/endpoint?{}", cfg.web_port, cfg.service);
        loop {
            match client.get(&poll_url).send().await {
                Ok(resp) => {
                    if let Ok(raw) = resp.bytes().await {
                        if raw.as_ref() != b"Nothing to receive" && !raw.is_empty() {
                            match bp7::Bundle::try_from(raw.to_vec()) {
                                Ok(bndl) => {
                                    if let Some(payload) = extract_payload(&bndl) {
                                        let src = bndl.primary.source.to_string();
                                        // EndpointID Display is typically
                                        // "dtn://<name>/" -- pull out <name>.
                                        let node_name = src
                                            .trim_start_matches("dtn://")
                                            .trim_end_matches('/')
                                            .to_string();
                                        match node_url(&node_name) {
                                            Ok(peer_url) => {
                                                if let Err(e) = hnd
                                                    .recv_data(peer_url, Bytes::from(payload))
                                                    .await
                                                {
                                                    tracing::warn!(?e, "recv_data failed");
                                                }
                                            }
                                            Err(e) => {
                                                tracing::warn!(?e, %src, "could not build peer url from dtn source eid")
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        ?e,
                                        "failed to decode received bundle as bp7 Bundle"
                                    )
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::trace!(?e, "dtn /endpoint poll failed");
                }
            }
            tokio::time::sleep(cfg.poll_interval).await;
        }
    });
}

/// [`TransportFactory`] that builds a [`Transport`] backed by a local
/// dtn7-rs daemon.
#[derive(Debug)]
pub struct DtnTransportFactory {
    pub cfg: DtnConfig,
}

impl TransportFactory for DtnTransportFactory {
    fn default_config(&self, _config: &mut Config) -> K2Result<()> {
        Ok(())
    }

    fn validate_config(&self, _config: &Config) -> K2Result<()> {
        Ok(())
    }

    fn create(
        &self,
        _builder: Arc<Builder>,
        handler: DynTxHandler,
    ) -> BoxFut<'static, K2Result<DynTransport>> {
        let cfg = self.cfg.clone();
        Box::pin(async move {
            let my_url = node_url(&cfg.node_name)?;
            let hnd = TxImpHnd::new(handler);
            let client = reqwest::Client::new();

            // Register our local endpoint on the dtn7 daemon so bundles
            // addressed to us are queued for /endpoint to pop.
            let register_url =
                format!("http://127.0.0.1:{}/register?{}", cfg.web_port, cfg.service);
            client
                .get(&register_url)
                .send()
                .await
                .map_err(|e| K2Error::other_src("dtn /register failed", e))?;

            spawn_receiver(cfg.clone(), client.clone(), hnd.clone());

            let imp: DynTxImp = Arc::new(DtnTxImp {
                cfg,
                client,
                my_url,
            });
            Ok(DefaultTransport::create(&hnd, imp))
        })
    }
}
