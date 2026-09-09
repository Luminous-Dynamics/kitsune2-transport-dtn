//! Durable receive-before-dispatch journal for destructively popped BPv7 bundles.
//!
//! This module deliberately gives journal records an opaque local identity.
//! A journal record id is **not** an application message id and must not be used
//! for duplicate suppression. Idempotent message identity belongs to a later
//! protocol layer.

use bytes::Bytes;
use kitsune2_api::{K2Error, K2Result};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;

static RECORD_COUNTER: AtomicU64 = AtomicU64::new(0);
const RECORD_PREFIX: &str = "record-";
const TEMP_SUFFIX: &str = ".tmp";
const PENDING_SUFFIX: &str = ".bundle";

/// One opaque local journal record.
///
/// Its filename is a storage identity only. Equal raw bundles intentionally get
/// distinct records; duplicate-safe application semantics are not implemented
/// here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JournaledBundle {
    path: PathBuf,
}

impl JournaledBundle {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

/// A bounded durable queue of bundles already removed from dtn7 but not yet
/// acknowledged as successfully dispatched to Kitsune2.
#[derive(Clone, Debug)]
pub(crate) struct InboundJournal {
    root: PathBuf,
    max_pending_bytes: u64,
    max_record_bytes: usize,
}

impl InboundJournal {
    /// Open or create a journal and recover any fully written temporary records
    /// left by a crash between file sync and atomic rename.
    pub(crate) async fn open(
        root: PathBuf,
        max_pending_bytes: u64,
        max_record_bytes: usize,
    ) -> K2Result<Self> {
        if root.as_os_str().is_empty() {
            return Err(K2Error::other("dtn inbound journal root must not be empty"));
        }
        if max_pending_bytes == 0 {
            return Err(K2Error::other(
                "dtn inbound journal max pending bytes must be non-zero",
            ));
        }
        if max_record_bytes == 0 {
            return Err(K2Error::other(
                "dtn inbound journal max record bytes must be non-zero",
            ));
        }

        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|error| K2Error::other_src("failed to create dtn inbound journal", error))?;

        let journal = Self {
            root,
            max_pending_bytes,
            max_record_bytes,
        };
        journal.recover_complete_temps().await?;
        Ok(journal)
    }

    /// Persist raw bytes before any BPv7 decoding or Kitsune2 dispatch.
    ///
    /// The record is written to a unique temporary file, file-synced, renamed
    /// into the pending set, and then the containing directory is synced on
    /// platforms where directory fsync is available.
    pub(crate) async fn persist(&self, raw: &Bytes) -> K2Result<JournaledBundle> {
        if raw.len() > self.max_record_bytes {
            return Err(K2Error::other(format!(
                "dtn inbound journal record exceeds {} bytes",
                self.max_record_bytes
            )));
        }

        let pending = self.pending_bytes().await?;
        let projected = pending.saturating_add(raw.len() as u64);
        if projected > self.max_pending_bytes {
            return Err(K2Error::other(format!(
                "dtn inbound journal capacity exceeded: projected {projected} > {} bytes",
                self.max_pending_bytes
            )));
        }

        // create_new is the final collision guard. The counter makes collision
        // extremely unlikely, but correctness does not depend on probability.
        for _ in 0..32 {
            let stem = new_record_stem(raw.len());
            let temp_path = self.root.join(format!("{stem}{TEMP_SUFFIX}"));
            let pending_path = self.root.join(format!("{stem}{PENDING_SUFFIX}"));

            let mut file = match tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
                .await
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(K2Error::other_src(
                        "failed to create dtn inbound journal temp record",
                        error,
                    ));
                }
            };

            file.write_all(raw).await.map_err(|error| {
                K2Error::other_src("failed to write dtn inbound journal record", error)
            })?;
            file.sync_all().await.map_err(|error| {
                K2Error::other_src("failed to sync dtn inbound journal record", error)
            })?;
            drop(file);

            tokio::fs::rename(&temp_path, &pending_path)
                .await
                .map_err(|error| {
                    K2Error::other_src("failed to commit dtn inbound journal record", error)
                })?;
            sync_directory(&self.root)?;

            return Ok(JournaledBundle { path: pending_path });
        }

        Err(K2Error::other(
            "failed to allocate unique dtn inbound journal record id",
        ))
    }

    /// List pending records in deterministic filename order.
    pub(crate) async fn pending(&self) -> K2Result<Vec<JournaledBundle>> {
        let mut records = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.root)
            .await
            .map_err(|error| K2Error::other_src("failed to read dtn inbound journal", error))?;

        while let Some(entry) = entries.next_entry().await.map_err(|error| {
            K2Error::other_src("failed to enumerate dtn inbound journal", error)
        })? {
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                tracing::warn!(path = ?entry.path(), "ignoring non-UTF8 dtn journal entry");
                continue;
            };
            if !is_pending_name(file_name) {
                continue;
            }

            let metadata = entry.metadata().await.map_err(|error| {
                K2Error::other_src("failed to stat dtn inbound journal record", error)
            })?;
            if !metadata.is_file() {
                continue;
            }
            if metadata.len() > self.max_record_bytes as u64 {
                tracing::warn!(
                    path = ?entry.path(),
                    bytes = metadata.len(),
                    max = self.max_record_bytes,
                    "retaining oversized dtn journal record without dispatch"
                );
                continue;
            }

            records.push(JournaledBundle { path: entry.path() });
        }

        records.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(records)
    }

    /// Read one pending record with a second size bound before allocation.
    pub(crate) async fn read(&self, record: &JournaledBundle) -> K2Result<Bytes> {
        if record.path.parent() != Some(self.root.as_path()) || !record_is_pending(record) {
            return Err(K2Error::other(
                "dtn inbound journal record is outside the pending journal set",
            ));
        }

        let metadata = tokio::fs::metadata(&record.path).await.map_err(|error| {
            K2Error::other_src("failed to stat dtn inbound journal record", error)
        })?;
        if metadata.len() > self.max_record_bytes as u64 {
            return Err(K2Error::other(format!(
                "dtn inbound journal record exceeds {} bytes",
                self.max_record_bytes
            )));
        }

        let bytes = tokio::fs::read(&record.path).await.map_err(|error| {
            K2Error::other_src("failed to read dtn inbound journal record", error)
        })?;
        if bytes.len() > self.max_record_bytes {
            return Err(K2Error::other(format!(
                "dtn inbound journal record exceeds {} bytes after read",
                self.max_record_bytes
            )));
        }
        Ok(Bytes::from(bytes))
    }

    /// Remove a record only after successful Kitsune2 dispatch.
    ///
    /// A crash after handler success but before this unlink can cause replay on
    /// restart. That is intentional at-least-once behavior, not exactly-once.
    pub(crate) async fn mark_delivered(&self, record: &JournaledBundle) -> K2Result<()> {
        if record.path.parent() != Some(self.root.as_path()) || !record_is_pending(record) {
            return Err(K2Error::other(
                "dtn inbound journal record is outside the pending journal set",
            ));
        }

        tokio::fs::remove_file(&record.path).await.map_err(|error| {
            K2Error::other_src("failed to remove delivered dtn journal record", error)
        })?;
        sync_directory(&self.root)?;
        Ok(())
    }

    async fn recover_complete_temps(&self) -> K2Result<()> {
        let mut entries = tokio::fs::read_dir(&self.root)
            .await
            .map_err(|error| K2Error::other_src("failed to read dtn inbound journal", error))?;
        let mut promoted_any = false;

        while let Some(entry) = entries.next_entry().await.map_err(|error| {
            K2Error::other_src("failed to enumerate dtn inbound journal", error)
        })? {
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            let Some(expected_len) = temp_expected_len(file_name) else {
                continue;
            };

            let metadata = entry.metadata().await.map_err(|error| {
                K2Error::other_src("failed to stat dtn journal temp record", error)
            })?;
            if !metadata.is_file() {
                continue;
            }

            if metadata.len() != expected_len as u64 || expected_len > self.max_record_bytes {
                tracing::error!(
                    path = ?entry.path(),
                    actual_bytes = metadata.len(),
                    expected_bytes = expected_len,
                    "retaining incomplete dtn journal temp record; the destructively popped bundle may be unrecoverable"
                );
                continue;
            }

            let pending_name = file_name
                .strip_suffix(TEMP_SUFFIX)
                .expect("validated temp suffix");
            let pending_path = self.root.join(format!("{pending_name}{PENDING_SUFFIX}"));
            tokio::fs::rename(entry.path(), &pending_path)
                .await
                .map_err(|error| {
                    K2Error::other_src("failed to recover complete dtn journal temp record", error)
                })?;
            promoted_any = true;
        }

        if promoted_any {
            sync_directory(&self.root)?;
        }
        Ok(())
    }

    async fn pending_bytes(&self) -> K2Result<u64> {
        let mut total = 0_u64;
        let mut entries = tokio::fs::read_dir(&self.root)
            .await
            .map_err(|error| K2Error::other_src("failed to read dtn inbound journal", error))?;

        while let Some(entry) = entries.next_entry().await.map_err(|error| {
            K2Error::other_src("failed to enumerate dtn inbound journal", error)
        })? {
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            if !(is_pending_name(file_name) || temp_expected_len(file_name).is_some()) {
                continue;
            }
            let metadata = entry.metadata().await.map_err(|error| {
                K2Error::other_src("failed to stat dtn inbound journal entry", error)
            })?;
            if metadata.is_file() {
                total = total.saturating_add(metadata.len());
            }
        }
        Ok(total)
    }
}

fn new_record_stem(raw_len: usize) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let counter = RECORD_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "{RECORD_PREFIX}{nanos:032x}-{:08x}-{counter:016x}-{raw_len}",
        std::process::id()
    )
}

fn is_pending_name(file_name: &str) -> bool {
    file_name.starts_with(RECORD_PREFIX) && file_name.ends_with(PENDING_SUFFIX)
}

fn record_is_pending(record: &JournaledBundle) -> bool {
    record
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(is_pending_name)
}

fn temp_expected_len(file_name: &str) -> Option<usize> {
    let stem = file_name
        .strip_prefix(RECORD_PREFIX)?
        .strip_suffix(TEMP_SUFFIX)?;
    let (_, len) = stem.rsplit_once('-')?;
    len.parse().ok()
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> K2Result<()> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| K2Error::other_src("failed to sync dtn inbound journal directory", error))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> K2Result<()> {
    // Directory fsync is not portable through std on all supported platforms.
    // File contents are still sync_all()'d before rename; the protocol docs do
    // not claim Unix-equivalent rename durability on non-Unix filesystems.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "kitsune2-dtn-journal-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn persists_across_reopen_until_marked_delivered() {
        let root = test_root("reopen");
        let raw = Bytes::from_static(b"raw-bundle");
        let journal = InboundJournal::open(root.clone(), 1024, 512)
            .await
            .expect("open journal");
        journal.persist(&raw).await.expect("persist");
        drop(journal);

        let reopened = InboundJournal::open(root.clone(), 1024, 512)
            .await
            .expect("reopen journal");
        let pending = reopened.pending().await.expect("pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(reopened.read(&pending[0]).await.unwrap(), raw);
        reopened
            .mark_delivered(&pending[0])
            .await
            .expect("mark delivered");
        assert!(reopened.pending().await.unwrap().is_empty());

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[tokio::test]
    async fn identical_raw_bundles_get_distinct_journal_records() {
        let root = test_root("duplicates");
        let raw = Bytes::from_static(b"same-bundle");
        let journal = InboundJournal::open(root.clone(), 1024, 512)
            .await
            .expect("open journal");

        let first = journal.persist(&raw).await.expect("first");
        let second = journal.persist(&raw).await.expect("second");
        assert_ne!(first.path(), second.path());
        assert_eq!(journal.pending().await.unwrap().len(), 2);

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[tokio::test]
    async fn capacity_limit_fails_closed() {
        let root = test_root("capacity");
        let journal = InboundJournal::open(root.clone(), 8, 8)
            .await
            .expect("open journal");

        journal
            .persist(&Bytes::from_static(b"12345678"))
            .await
            .expect("first record fits");
        assert!(journal.persist(&Bytes::from_static(b"x")).await.is_err());

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[tokio::test]
    async fn complete_temp_record_is_promoted_on_reopen() {
        let root = test_root("temp-recovery");
        std::fs::create_dir_all(&root).expect("create root");
        let temp = root.join("record-deadbeef-3.tmp");
        let mut file = std::fs::File::create(&temp).expect("create temp");
        use std::io::Write as _;
        file.write_all(b"abc").expect("write temp");
        file.sync_all().expect("sync temp");
        drop(file);

        let journal = InboundJournal::open(root.clone(), 1024, 512)
            .await
            .expect("recover journal");
        let pending = journal.pending().await.expect("pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(journal.read(&pending[0]).await.unwrap(), Bytes::from_static(b"abc"));

        std::fs::remove_dir_all(root).expect("cleanup");
    }
}
