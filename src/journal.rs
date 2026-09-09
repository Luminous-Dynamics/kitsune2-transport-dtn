//! Durable receive-before-dispatch journal for destructively popped BPv7 bundles.
//!
//! This module deliberately gives journal records an opaque local identity.
//! A journal record id is **not** an application message id and must not be used
//! for duplicate suppression. Idempotent message identity belongs to a later
//! protocol layer.
//!
//! The journal provides local crash durability and accidental-corruption
//! detection. CRC32 is used only to detect damaged/torn local records; it is not
//! authentication and provides no adversarial tamper resistance.

use bytes::Bytes;
use kitsune2_api::{K2Error, K2Result};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

static RECORD_COUNTER: AtomicU64 = AtomicU64::new(0);
const RECORD_PREFIX: &str = "record-";
const TEMP_SUFFIX: &str = ".tmp";
const PENDING_SUFFIX: &str = ".bundle";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RecordMetadata {
    expected_len: usize,
    crc32: u32,
}

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
    ///
    /// Ambiguous or corrupt records fail closed. A receiver must not continue
    /// destructively popping bundles while its durable handoff state is suspect.
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
        let root_metadata = tokio::fs::metadata(&root)
            .await
            .map_err(|error| K2Error::other_src("failed to stat dtn inbound journal", error))?;
        if !root_metadata.is_dir() {
            return Err(K2Error::other("dtn inbound journal root is not a directory"));
        }
        set_private_directory_permissions(&root).await?;

        let journal = Self {
            root,
            max_pending_bytes,
            max_record_bytes,
        };
        journal.recover_complete_temps().await?;
        let pending_bytes = journal.pending_bytes().await?;
        if pending_bytes > max_pending_bytes {
            return Err(K2Error::other(format!(
                "existing dtn inbound journal exceeds configured capacity: {pending_bytes} > {max_pending_bytes} bytes"
            )));
        }
        // Force a structural scan now so malformed/oversized pending records
        // prevent endpoint registration rather than surfacing after more pops.
        let _ = journal.pending().await?;
        Ok(journal)
    }

    /// Persist raw bytes before any BPv7 decoding or Kitsune2 dispatch.
    ///
    /// The record is written to a unique temporary file, permission-hardened,
    /// file-synced, atomically renamed into the pending set, and then the
    /// containing directory is synced where supported.
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

        let crc32 = crc32fast::hash(raw);
        // create_new is the final collision guard. The counter makes collision
        // extremely unlikely, but correctness does not depend on probability.
        for _ in 0..32 {
            let stem = new_record_stem(raw.len(), crc32);
            let temp_path = self.root.join(format!("{stem}{TEMP_SUFFIX}"));
            let pending_path = self.root.join(format!("{stem}{PENDING_SUFFIX}"));

            if path_exists(&pending_path).await? {
                continue;
            }

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
            set_private_file_permissions(&temp_path).await?;

            file.write_all(raw).await.map_err(|error| {
                K2Error::other_src("failed to write dtn inbound journal record", error)
            })?;
            file.sync_all().await.map_err(|error| {
                K2Error::other_src("failed to sync dtn inbound journal record", error)
            })?;
            drop(file);

            // Never permit rename-overwrite semantics to decide correctness.
            if path_exists(&pending_path).await? {
                return Err(K2Error::other(
                    "dtn inbound journal pending-record collision detected",
                ));
            }
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
    ///
    /// Reserved-name corruption fails closed instead of being skipped. Unknown
    /// unrelated files remain outside the journal namespace and are ignored.
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
                continue;
            };
            if !file_name.starts_with(RECORD_PREFIX) || !file_name.ends_with(PENDING_SUFFIX) {
                continue;
            }
            let record_meta = parse_record_metadata(file_name, PENDING_SUFFIX).ok_or_else(|| {
                K2Error::other(format!(
                    "malformed dtn inbound journal pending record name: {file_name}"
                ))
            })?;
            if record_meta.expected_len > self.max_record_bytes {
                return Err(K2Error::other(format!(
                    "dtn inbound journal pending record declares {} bytes above max {}",
                    record_meta.expected_len, self.max_record_bytes
                )));
            }

            let file_type = entry.file_type().await.map_err(|error| {
                K2Error::other_src("failed to inspect dtn inbound journal record", error)
            })?;
            if !file_type.is_file() {
                return Err(K2Error::other(format!(
                    "dtn inbound journal reserved pending path is not a regular file: {file_name}"
                )));
            }
            let metadata = entry.metadata().await.map_err(|error| {
                K2Error::other_src("failed to stat dtn inbound journal record", error)
            })?;
            if metadata.len() != record_meta.expected_len as u64 {
                return Err(K2Error::other(format!(
                    "dtn inbound journal pending record length mismatch for {file_name}: {} != {}",
                    metadata.len(), record_meta.expected_len
                )));
            }

            records.push(JournaledBundle { path: entry.path() });
        }

        records.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(records)
    }

    /// Read one pending record and verify its length and accidental-corruption
    /// checksum before returning it for dispatch.
    pub(crate) async fn read(&self, record: &JournaledBundle) -> K2Result<Bytes> {
        let record_meta = self.validate_record_path(record)?;
        let file_type = tokio::fs::symlink_metadata(&record.path)
            .await
            .map_err(|error| K2Error::other_src("failed to inspect dtn journal record", error))?
            .file_type();
        if !file_type.is_file() {
            return Err(K2Error::other(
                "dtn inbound journal pending path is not a regular file",
            ));
        }

        let metadata = tokio::fs::metadata(&record.path).await.map_err(|error| {
            K2Error::other_src("failed to stat dtn inbound journal record", error)
        })?;
        if metadata.len() != record_meta.expected_len as u64
            || record_meta.expected_len > self.max_record_bytes
        {
            return Err(K2Error::other(
                "dtn inbound journal record length does not match committed metadata",
            ));
        }

        let bytes = tokio::fs::read(&record.path).await.map_err(|error| {
            K2Error::other_src("failed to read dtn inbound journal record", error)
        })?;
        if bytes.len() != record_meta.expected_len {
            return Err(K2Error::other(
                "dtn inbound journal record length changed during read",
            ));
        }
        let actual_crc32 = crc32fast::hash(&bytes);
        if actual_crc32 != record_meta.crc32 {
            return Err(K2Error::other(format!(
                "dtn inbound journal record checksum mismatch: {actual_crc32:08x} != {:08x}",
                record_meta.crc32
            )));
        }
        Ok(Bytes::from(bytes))
    }

    /// Remove a record only after successful Kitsune2 dispatch.
    ///
    /// A crash after handler success but before this unlink can cause replay on
    /// restart. That is intentional at-least-once behavior, not exactly-once.
    pub(crate) async fn mark_delivered(&self, record: &JournaledBundle) -> K2Result<()> {
        let _ = self.validate_record_path(record)?;
        tokio::fs::remove_file(&record.path).await.map_err(|error| {
            K2Error::other_src("failed to remove delivered dtn journal record", error)
        })?;
        sync_directory(&self.root)?;
        Ok(())
    }

    fn validate_record_path(&self, record: &JournaledBundle) -> K2Result<RecordMetadata> {
        if record.path.parent() != Some(self.root.as_path()) {
            return Err(K2Error::other(
                "dtn inbound journal record is outside the pending journal set",
            ));
        }
        let file_name = record
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| K2Error::other("dtn inbound journal record name is not UTF-8"))?;
        parse_record_metadata(file_name, PENDING_SUFFIX).ok_or_else(|| {
            K2Error::other("dtn inbound journal record is outside the pending journal namespace")
        })
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
            if !file_name.starts_with(RECORD_PREFIX) || !file_name.ends_with(TEMP_SUFFIX) {
                continue;
            }
            let record_meta = parse_record_metadata(file_name, TEMP_SUFFIX).ok_or_else(|| {
                K2Error::other(format!(
                    "malformed dtn inbound journal temp record name: {file_name}"
                ))
            })?;
            if record_meta.expected_len > self.max_record_bytes {
                return Err(K2Error::other(format!(
                    "dtn journal temp record declares {} bytes above max {}",
                    record_meta.expected_len, self.max_record_bytes
                )));
            }

            let file_type = entry.file_type().await.map_err(|error| {
                K2Error::other_src("failed to inspect dtn journal temp record", error)
            })?;
            if !file_type.is_file() {
                return Err(K2Error::other(format!(
                    "dtn journal reserved temp path is not a regular file: {file_name}"
                )));
            }
            let metadata = entry.metadata().await.map_err(|error| {
                K2Error::other_src("failed to stat dtn journal temp record", error)
            })?;
            if metadata.len() != record_meta.expected_len as u64 {
                return Err(K2Error::other(format!(
                    "incomplete dtn journal temp record retained at {:?}: {} != {} bytes",
                    entry.path(), metadata.len(), record_meta.expected_len
                )));
            }

            let bytes = tokio::fs::read(entry.path()).await.map_err(|error| {
                K2Error::other_src("failed to verify dtn journal temp record", error)
            })?;
            if crc32fast::hash(&bytes) != record_meta.crc32 {
                return Err(K2Error::other(format!(
                    "corrupt dtn journal temp record retained at {:?}",
                    entry.path()
                )));
            }

            let pending_name = file_name
                .strip_suffix(TEMP_SUFFIX)
                .expect("validated temp suffix");
            let pending_path = self.root.join(format!("{pending_name}{PENDING_SUFFIX}"));
            if path_exists(&pending_path).await? {
                return Err(K2Error::other(format!(
                    "dtn journal recovery collision: both temp and pending record exist for {pending_name}"
                )));
            }
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
            let reserved = (file_name.starts_with(RECORD_PREFIX)
                && file_name.ends_with(PENDING_SUFFIX))
                || (file_name.starts_with(RECORD_PREFIX) && file_name.ends_with(TEMP_SUFFIX));
            if !reserved {
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

fn new_record_stem(raw_len: usize, crc32: u32) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let counter = RECORD_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "{RECORD_PREFIX}{nanos:032x}-{:08x}-{counter:016x}-{raw_len}-{crc32:08x}",
        std::process::id()
    )
}

fn parse_record_metadata(file_name: &str, suffix: &str) -> Option<RecordMetadata> {
    let stem = file_name.strip_prefix(RECORD_PREFIX)?.strip_suffix(suffix)?;
    let (prefix, crc32_hex) = stem.rsplit_once('-')?;
    let (_, len) = prefix.rsplit_once('-')?;
    if crc32_hex.len() != 8 {
        return None;
    }
    Some(RecordMetadata {
        expected_len: len.parse().ok()?,
        crc32: u32::from_str_radix(crc32_hex, 16).ok()?,
    })
}

async fn path_exists(path: &Path) -> K2Result<bool> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(K2Error::other_src(
            "failed to inspect dtn inbound journal path",
            error,
        )),
    }
}

#[cfg(unix)]
async fn set_private_directory_permissions(path: &Path) -> K2Result<()> {
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .await
        .map_err(|error| K2Error::other_src("failed to protect dtn inbound journal directory", error))
}

#[cfg(not(unix))]
async fn set_private_directory_permissions(_path: &Path) -> K2Result<()> {
    Ok(())
}

#[cfg(unix)]
async fn set_private_file_permissions(path: &Path) -> K2Result<()> {
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .await
        .map_err(|error| K2Error::other_src("failed to protect dtn inbound journal record", error))
}

#[cfg(not(unix))]
async fn set_private_file_permissions(_path: &Path) -> K2Result<()> {
    Ok(())
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
    use std::io::Write as _;

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

    fn temp_name(raw: &[u8]) -> String {
        format!(
            "record-test-{}-{:08x}{TEMP_SUFFIX}",
            raw.len(),
            crc32fast::hash(raw)
        )
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
    async fn complete_temp_record_is_verified_and_promoted_on_reopen() {
        let root = test_root("temp-recovery");
        std::fs::create_dir_all(&root).expect("create root");
        let raw = b"abc";
        let temp = root.join(temp_name(raw));
        let mut file = std::fs::File::create(&temp).expect("create temp");
        file.write_all(raw).expect("write temp");
        file.sync_all().expect("sync temp");
        drop(file);

        let journal = InboundJournal::open(root.clone(), 1024, 512)
            .await
            .expect("recover journal");
        let pending = journal.pending().await.expect("pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(journal.read(&pending[0]).await.unwrap(), Bytes::from_static(raw));

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[tokio::test]
    async fn incomplete_temp_record_stops_recovery() {
        let root = test_root("incomplete-temp");
        std::fs::create_dir_all(&root).expect("create root");
        let raw = b"abcdef";
        let temp = root.join(temp_name(raw));
        std::fs::write(&temp, b"abc").expect("write partial temp");

        assert!(InboundJournal::open(root.clone(), 1024, 512).await.is_err());
        assert!(temp.exists(), "forensic temp record must be retained");
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[tokio::test]
    async fn pending_record_bitflip_is_detected_before_dispatch() {
        let root = test_root("bitflip");
        let raw = Bytes::from_static(b"abcdef");
        let journal = InboundJournal::open(root.clone(), 1024, 512)
            .await
            .expect("open journal");
        let record = journal.persist(&raw).await.expect("persist");
        std::fs::write(record.path(), b"abcdeg").expect("corrupt pending record");

        assert!(journal.read(&record).await.is_err());
        assert!(record.path().exists(), "corrupt record must be retained");
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[tokio::test]
    async fn recovery_refuses_temp_pending_collision() {
        let root = test_root("collision");
        std::fs::create_dir_all(&root).expect("create root");
        let raw = b"abc";
        let temp_name = temp_name(raw);
        let temp = root.join(&temp_name);
        let pending = root.join(format!(
            "{}{}",
            temp_name.strip_suffix(TEMP_SUFFIX).unwrap(),
            PENDING_SUFFIX
        ));
        std::fs::write(&temp, raw).expect("write temp");
        std::fs::write(&pending, raw).expect("write pending");

        assert!(InboundJournal::open(root.clone(), 1024, 512).await.is_err());
        assert!(temp.exists());
        assert!(pending.exists());
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn journal_directory_and_records_are_private() {
        let root = test_root("permissions");
        let journal = InboundJournal::open(root.clone(), 1024, 512)
            .await
            .expect("open journal");
        let record = journal
            .persist(&Bytes::from_static(b"secret-ish-payload"))
            .await
            .expect("persist");

        let dir_mode = std::fs::metadata(&root).unwrap().permissions().mode() & 0o777;
        let file_mode = std::fs::metadata(record.path()).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700);
        assert_eq!(file_mode, 0o600);
        std::fs::remove_dir_all(root).expect("cleanup");
    }
}