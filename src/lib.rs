//! A proof-of-concept Kitsune2 [`TxImp`]/[`TransportFactory`] backed by a
//! local `dtn7-rs` daemon's HTTP API.
//!
//! This crate demonstrates that serialized Kitsune2 protocol messages can be
//! carried over a real BPv7/DTN store-and-forward path. It does not yet
//! implement Kitsune2's full logical connection lifecycle: in particular,
//! preflight exchange, authenticated peer-session establishment, disconnect
//! signalling, and meaningful connected-peer reporting remain open design
//! work.
//!
//! Honest limitations, not hidden:
//! - `get_connected_peers()` always returns empty: DTN is store-and-forward,
//!   not connection-oriented, so there is no live "connected" set to report.
//! - Receiving is poll-based (dtn7's `/endpoint` is a destructive
//!   pop-next-bundle HTTP call), so latency is bounded by the poll interval.
//! - Successfully read raw bundles are journaled before BPv7 decoding and
//!   Kitsune2 dispatch, so committed records can be replayed after restart.
//! - There is still an unavoidable loss window after dtn7's destructive pop
//!   and before the HTTP body is completely read and durably journaled.
//! - Replay is intentionally at-least-once. A crash after handler success but
//!   before journal deletion can deliver the same logical message again; this
//!   crate does not yet define idempotent message identity or exactly-once
//!   semantics.
//! - No retry/backoff tuning beyond dtn7's own; this crate is a thin bridge,
//!   not a reimplementation of DTN semantics.

mod journal;

use bytes::{Bytes, BytesMut};
use journal::InboundJournal;
use kitsune2_api::{
    BoxFut, Builder, Config, DefaultTransport, DynTransport, DynTxHandler, DynTxImp, K2Error,
    K2Result, TransportConnectionStats, TransportFactory, TransportStats, TxImp, TxImpHnd, Url,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BUNDLE_BYTES: usize = 64 * 1024 * 1024;
const MAX_DTN_SEGMENT_BYTES: usize = 255;
const DEFAULT_JOURNAL_MAX_BYTES: u64 = 512 * 1024 * 1024;
const JOURNAL_ROOT_ENV: &str = "KITSUNE2_DTN_JOURNAL_ROOT";
const JOURNAL_MAX_BYTES_ENV: &str = "KITSUNE2_DTN_JOURNAL_MAX_BYTES";

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
    /// How often to poll `/endpoint` after the application-agent queue is
    /// observed empty or after a poll error.
    pub poll_interval: Duration,
}

fn validate_dtn_segment(kind: &str, value: &str) -> K2Result<()> {
    let valid = !value.is_empty()
        && value.len() <= MAX_DTN_SEGMENT_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~'));
    if valid {
        Ok(())
    } else {
        Err(K2Error::other(format!(
            "dtn {kind} must be 1-{MAX_DTN_SEGMENT_BYTES} bytes of URI-unreserved ASCII"
        )))
    }
}

fn validate_dtn_config(cfg: &DtnConfig) -> K2Result<()> {
    if cfg.web_port == 0 {
        return Err(K2Error::other("dtn web port must be non-zero"));
    }
    validate_dtn_segment("node name", &cfg.node_name)?;
    validate_dtn_segment("service", &cfg.service)?;
    if cfg.lifetime_secs == 0 {
        return Err(K2Error::other("dtn bundle lifetime must be non-zero"));
    }
    if cfg.poll_interval.is_zero() {
        return Err(K2Error::other("dtn poll interval must be non-zero"));
    }
    Ok(())
}

fn inbound_journal_settings(cfg: &DtnConfig) -> K2Result<(PathBuf, u64)> {
    let base = std::env::var_os(JOURNAL_ROOT_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".kitsune2-dtn-journal"));
    let root = base
        .join(format!("{}-{}", cfg.node_name, cfg.web_port))
        .join(&cfg.service);

    let max_pending_bytes = match std::env::var(JOURNAL_MAX_BYTES_ENV) {
        Ok(value) => value.parse::<u64>().map_err(|_| {
            K2Error::other(format!(
                "{JOURNAL_MAX_BYTES_ENV} must be an unsigned byte count"
            ))
        })?,
        Err(std::env::VarError::NotPresent) => DEFAULT_JOURNAL_MAX_BYTES,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(K2Error::other(format!(
                "{JOURNAL_MAX_BYTES_ENV} must be valid Unicode digits"
            )));
        }
    };

    if max_pending_bytes < MAX_BUNDLE_BYTES as u64 {
        return Err(K2Error::other(format!(
            "dtn inbound journal capacity must be at least {MAX_BUNDLE_BYTES} bytes"
        )));
    }

    Ok((root, max_pending_bytes))
}

/// Build the nominal Kitsune2 [`Url`] for a given DTN node name.
///
/// Host/port are placeholders to satisfy Kitsune2's URL parser. The real
/// addressing information is the DTN node name in the final path segment.
pub fn node_url(node_name: &str) -> K2Result<Url> {
    validate_dtn_segment("node name", node_name)?;
    Url::from_str(format!("ws://dtn.local:1/{node_name}"))
}

struct DtnTxImp {
    cfg: DtnConfig,
    client: reqwest::Client,
    my_url: Url,
    receiver_abort: tokio::task::AbortHandle,
}

impl std::fmt::Debug for DtnTxImp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DtnTxImp {{ node: {} }}", self.cfg.node_name)
    }
}

impl Drop for DtnTxImp {
    fn drop(&mut self) {
        self.receiver_abort.abort();
    }
}

fn peer_node_name(url: &Url) -> K2Result<&str> {
    let node_name = url
        .peer_id()
        .ok_or_else(|| K2Error::other("dtn peer url has no node-name path segment"))?;
    validate_dtn_segment("peer node name", node_name)?;
    Ok(node_name)
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
            self.client
                .post(&send_url)
                .body(data)
                .send()
                .await
                .map_err(|e| K2Error::other_src("dtn /send failed", e))?
                .error_for_status()
                .map_err(|e| K2Error::other_src("dtn /send returned an error status", e))?;
            Ok(())
        })
    }

    fn disconnect(&self, _peer: Url, _payload: Option<(String, Bytes)>) -> BoxFut<'_, ()> {
        // DTN has no live connection to close. A future full Kitsune2 mapping
        // may carry a logical disconnect as a short-lived control bundle.
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

fn source_node_name(source_eid: &str) -> Option<&str> {
    source_eid
        .strip_prefix("dtn://")
        .and_then(|rest| rest.strip_suffix('/'))
        .filter(|name| validate_dtn_segment("source node name", name).is_ok())
}

async fn read_bounded_body(mut response: reqwest::Response) -> K2Result<Bytes> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_BUNDLE_BYTES as u64)
    {
        return Err(K2Error::other(format!(
            "dtn /endpoint response exceeds {MAX_BUNDLE_BYTES} bytes"
        )));
    }

    let mut body = BytesMut::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| K2Error::other_src("failed to read dtn /endpoint response body", e))?
    {
        if body.len().saturating_add(chunk.len()) > MAX_BUNDLE_BYTES {
            return Err(K2Error::other(format!(
                "dtn /endpoint response exceeds {MAX_BUNDLE_BYTES} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

async fn dispatch_bundle(raw: Bytes, hnd: &Arc<TxImpHnd>) -> K2Result<()> {
    let bndl = bp7::Bundle::try_from(raw.to_vec()).map_err(|error| {
        K2Error::other(format!("failed to decode received bundle as bp7 Bundle: {error:?}"))
    })?;
    let payload = extract_payload(&bndl)
        .ok_or_else(|| K2Error::other("received BPv7 bundle without a payload block"))?;
    let source_eid = bndl.primary.source.to_string();
    let node_name = source_node_name(&source_eid).ok_or_else(|| {
        K2Error::other(format!(
            "unsupported dtn source EID {source_eid:?}; expected dtn://<URI-unreserved-node>/"
        ))
    })?;
    let peer_url = node_url(node_name)?;
    hnd.recv_data(peer_url, Bytes::from(payload)).await?;
    Ok(())
}

async fn replay_pending(journal: &InboundJournal, hnd: &Arc<TxImpHnd>) -> K2Result<()> {
    for record in journal.pending().await? {
        let raw = match journal.read(&record).await {
            Ok(raw) => raw,
            Err(error) => {
                tracing::warn!(
                    ?error,
                    path = ?record.path(),
                    "could not read pending dtn journal record; retaining it"
                );
                continue;
            }
        };

        match dispatch_bundle(raw, hnd).await {
            Ok(()) => {
                if let Err(error) = journal.mark_delivered(&record).await {
                    tracing::warn!(
                        ?error,
                        path = ?record.path(),
                        "dtn handler succeeded but journal cleanup failed; replay may duplicate delivery"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    path = ?record.path(),
                    "pending dtn journal dispatch failed; retaining record for later replay"
                );
            }
        }
    }
    Ok(())
}

fn spawn_receiver(
    cfg: DtnConfig,
    client: reqwest::Client,
    hnd: Arc<TxImpHnd>,
    journal: InboundJournal,
) -> tokio::task::AbortHandle {
    let task = tokio::spawn(async move {
        let poll_url = format!(
            "http://127.0.0.1:{}/endpoint?{}",
            cfg.web_port,
            urlencoding::encode(&cfg.service)
        );
        loop {
            // Replay retained records before destructively popping more data.
            // Repeating this once per poll cycle also permits application
            // handlers registered shortly after transport creation to consume
            // records that were not dispatchable during the first pass.
            if let Err(error) = replay_pending(&journal, &hnd).await {
                tracing::error!(
                    ?error,
                    "dtn inbound journal replay failed; stopping receiver before further destructive pops"
                );
                return;
            }

            // Drain the application-agent queue without sleeping between
            // bundles. Sleep only once the queue is empty or polling fails.
            loop {
                let response = match client.get(&poll_url).send().await {
                    Ok(response) => response,
                    Err(error) => {
                        tracing::trace!(?error, "dtn /endpoint poll failed");
                        break;
                    }
                };

                let response = match response.error_for_status() {
                    Ok(response) => response,
                    Err(error) => {
                        tracing::warn!(?error, "dtn /endpoint returned an error status");
                        break;
                    }
                };

                let raw = match read_bounded_body(response).await {
                    Ok(raw) => raw,
                    Err(error) => {
                        tracing::warn!(
                            ?error,
                            "failed to read bounded dtn /endpoint body; bundle may already be popped before journaling"
                        );
                        break;
                    }
                };

                if raw.as_ref() == b"Nothing to receive" || raw.is_empty() {
                    break;
                }

                let record = match journal.persist(&raw).await {
                    Ok(record) => record,
                    Err(error) => {
                        tracing::error!(
                            ?error,
                            "could not durably journal destructively popped dtn bundle; stopping receiver to avoid popping additional bundles"
                        );
                        return;
                    }
                };

                match dispatch_bundle(raw, &hnd).await {
                    Ok(()) => {
                        if let Err(error) = journal.mark_delivered(&record).await {
                            tracing::warn!(
                                ?error,
                                path = ?record.path(),
                                "dtn handler succeeded but journal cleanup failed; replay may duplicate delivery"
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            ?error,
                            path = ?record.path(),
                            "dtn dispatch failed after durable journal commit; retaining record for replay"
                        );
                    }
                }
            }

            tokio::time::sleep(cfg.poll_interval).await;
        }
    });
    task.abort_handle()
}

/// [`TransportFactory`] that builds a Kitsune2 transport backed by a local
/// `dtn7-rs` daemon.
#[derive(Debug)]
pub struct DtnTransportFactory {
    /// Transport configuration.
    pub cfg: DtnConfig,
}

impl TransportFactory for DtnTransportFactory {
    fn default_config(&self, _config: &mut Config) -> K2Result<()> {
        Ok(())
    }

    fn validate_config(&self, _config: &Config) -> K2Result<()> {
        validate_dtn_config(&self.cfg)?;
        inbound_journal_settings(&self.cfg).map(|_| ())
    }

    fn create(
        &self,
        _builder: Arc<Builder>,
        handler: DynTxHandler,
    ) -> BoxFut<'static, K2Result<DynTransport>> {
        let cfg = self.cfg.clone();
        Box::pin(async move {
            validate_dtn_config(&cfg)?;
            let (journal_root, journal_max_bytes) = inbound_journal_settings(&cfg)?;
            // Journal readiness is load-bearing. Refuse to register the
            // destructive receive endpoint if durable handoff cannot be opened.
            let journal =
                InboundJournal::open(journal_root, journal_max_bytes, MAX_BUNDLE_BYTES).await?;

            let my_url = node_url(&cfg.node_name)?;
            let hnd = TxImpHnd::new(handler);
            let client = reqwest::Client::builder()
                .connect_timeout(HTTP_CONNECT_TIMEOUT)
                .timeout(HTTP_REQUEST_TIMEOUT)
                .build()
                .map_err(|e| K2Error::other_src("failed to build dtn HTTP client", e))?;

            // Registration is load-bearing: do not return a transport unless
            // the daemon confirms the application endpoint was registered.
            let register_url = format!(
                "http://127.0.0.1:{}/register?{}",
                cfg.web_port,
                urlencoding::encode(&cfg.service)
            );
            client
                .get(&register_url)
                .send()
                .await
                .map_err(|e| K2Error::other_src("dtn /register failed", e))?
                .error_for_status()
                .map_err(|e| K2Error::other_src("dtn /register returned an error status", e))?;

            let receiver_abort =
                spawn_receiver(cfg.clone(), client.clone(), hnd.clone(), journal);

            let imp: DynTxImp = Arc::new(DtnTxImp {
                cfg,
                client,
                my_url,
                receiver_abort,
            });
            Ok(DefaultTransport::create(&hnd, imp))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> DtnConfig {
        DtnConfig {
            web_port: 3000,
            node_name: "node-1".into(),
            service: "kitsune2".into(),
            lifetime_secs: 3600,
            poll_interval: Duration::from_millis(100),
        }
    }

    #[test]
    fn node_url_rejects_invalid_node_names() {
        for invalid in [
            "",
            "node/child",
            "node child",
            "node?query",
            "node#fragment",
        ] {
            assert!(
                node_url(invalid).is_err(),
                "accepted invalid node name {invalid:?}"
            );
        }
    }

    #[test]
    fn node_url_round_trips_peer_name() {
        let url = node_url("node-1").expect("valid node URL");
        assert_eq!(peer_node_name(&url).expect("peer name"), "node-1");
    }

    #[test]
    fn source_eid_parsing_is_strict() {
        assert_eq!(source_node_name("dtn://node-1/"), Some("node-1"));
        for invalid in [
            "node-1",
            "dtn://node-1",
            "dtn://node-1/service",
            "dtn://node child/",
            "ipn:1.1",
        ] {
            assert_eq!(source_node_name(invalid), None, "accepted {invalid:?}");
        }
    }

    #[test]
    fn config_validation_rejects_unsafe_or_inert_values() {
        let mut cfg = valid_config();
        assert!(validate_dtn_config(&cfg).is_ok());

        cfg.web_port = 0;
        assert!(validate_dtn_config(&cfg).is_err());
        cfg = valid_config();
        cfg.service = "bad/service".into();
        assert!(validate_dtn_config(&cfg).is_err());
        cfg = valid_config();
        cfg.lifetime_secs = 0;
        assert!(validate_dtn_config(&cfg).is_err());
        cfg = valid_config();
        cfg.poll_interval = Duration::ZERO;
        assert!(validate_dtn_config(&cfg).is_err());
    }
}
