use crate::FixedGaugeRollup;
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::segment::{Segment, SegmentStats};
use crate::transaction::{IngressIdentity, Record, Transaction};
use crc32fast::hash;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DATABASE_MAGIC: &[u8; 8] = b"FTWDB001";
const DATABASE_VERSION: u16 = 1;
const DATABASE_HEADER_BYTES: usize = 16;
const FRAME_MAGIC: &[u8; 4] = b"WBAT";
const FRAME_VERSION: u16 = 1;
const FRAME_HEADER_BYTES: usize = 24;
const POINT_BYTES: usize = 72;
const FRAME_KIND_LEGACY_POINTS: u16 = 0;
const FRAME_KIND_TRANSACTION: u16 = 1;
/// A transaction frame carrying a client-supplied idempotency identifier.
///
/// Format evolution follows the existing frame-kind precedent (kinds 0 and 1
/// already coexist): a new kind keeps every old log byte-for-byte decodable
/// and keeps identifier-less commits writing exactly the kind-1 frames they
/// always wrote. The payload is the 16-byte little-endian commit identifier
/// followed by an unmodified kind-1 transaction payload, so the identifier
/// lives in the same checksummed durable unit as the records it protects —
/// a separate identifier frame would reintroduce the torn-window problem
/// this feature exists to close.
const FRAME_KIND_IDENTIFIED_TRANSACTION: u16 = 2;
const COMMIT_ID_BYTES: usize = 16;
/// An ordered ingress transaction.
///
/// Its payload starts with `source_id: u128`, `sequence: u64`, and
/// `commit_id: u128`, all little-endian, followed by the canonical kind-1
/// transaction payload. One frame checksum therefore covers both identity
/// and data. Kinds 0 through 2 remain unchanged.
const FRAME_KIND_INGRESS_TRANSACTION: u16 = 3;
/// Marks the durable prefix that a later manifest generation sealed into an
/// immutable raw segment. Recovery drops live-index points accumulated before
/// a checkpoint whose generation is published, so reopen stays bounded by the
/// unsealed tail even when reclaim has not yet rewritten `active.wlog`.
const FRAME_KIND_SEAL_CHECKPOINT: u16 = 4;
/// Compact identity receipts after WAL reclaim. Payload is postcard-encoded
/// receipt metadata plus exact retry bytes; recovery does not rebuild point
/// records into the live query index.
const FRAME_KIND_IDENTITY_INDEX: u16 = 5;
const IDENTITY_INDEX_MAGIC_V2: &[u8; 8] = b"WIDX0002";
/// A single retained transaction may already use the configured transaction
/// limit. Kind 5 needs a small amount of receipt metadata around those exact
/// bytes, while the writer splits multiple receipts across frames.
const IDENTITY_INDEX_ENTRY_OVERHEAD_BYTES: usize = 1024;
const INGRESS_IDENTITY_BYTES: usize = 16 + 8 + 16;
const SEAL_CHECKPOINT_BYTES: usize = 16;
const TRANSACTION_MAGIC: &[u8; 4] = b"WTXN";
const TRANSACTION_VERSION: u16 = 1;
const TRANSACTION_HEADER_BYTES: usize = 12;
const RECORD_HEADER_BYTES: usize = 8;
const RECORD_ENTITY: u8 = 1;
const RECORD_RELATION: u8 = 2;
const RECORD_SERIES: u8 = 3;
const RECORD_RUN: u8 = 4;
const RECORD_PLAN: u8 = 5;
const RECORD_POINTS: u8 = 6;

/// A value in three-dimensional energy time plus provenance.
///
/// All timestamps are UTC microseconds since Unix epoch. `valid_time` is when
/// the value applies, `knowledge_time` is when it became known, and
/// `change_time` is when this revision was recorded. `valid_time_end` equals
/// `valid_time` for an instant and is exclusive for interval values.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct Point {
    pub series_id: u64,
    pub valid_time: i64,
    pub valid_time_end: i64,
    pub knowledge_time: i64,
    pub change_time: i64,
    /// Zero means that no run was supplied. Non-zero IDs link forecasts,
    /// optimization plans, imports, and outcomes to catalog provenance.
    pub run_id: u128,
    pub value: f64,
    pub quality: u32,
    pub flags: u32,
}

impl Point {
    #[must_use]
    pub const fn actual(series_id: u64, timestamp: i64, value: f64) -> Self {
        Self {
            series_id,
            valid_time: timestamp,
            valid_time_end: timestamp,
            knowledge_time: timestamp,
            change_time: timestamp,
            run_id: 0,
            value,
            quality: 0,
            flags: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    /// Sync every committed batch before returning. Safest and hardest on
    /// flash media.
    #[default]
    Always,
    /// Sync when at least this many frame bytes have accumulated. Recent
    /// acknowledged batches may be lost on power failure.
    EveryBytes(u64),
    /// Only sync when `flush` or `close` is called.
    Manual,
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub durability: Durability,
    pub max_batch_points: usize,
    pub max_transaction_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            durability: Durability::Always,
            max_batch_points: 262_144,
            max_transaction_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Commit {
    pub frame_offset: u64,
    pub points: usize,
    pub records: usize,
    pub bytes_written: u64,
    pub durable: bool,
    /// True when the transaction's [`Transaction::with_commit_id`] identifier
    /// was already committed, so this call wrote nothing: the original
    /// commit's records and points are stored exactly once. Legacy identified
    /// replays return zero counts. Ordered ingress replays return the original
    /// frame offset, counts, and byte count as a durable receipt.
    pub deduplicated: bool,
}

/// Durable progress for one ordered ingress source.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IngressWatermarks {
    /// Highest sequence present in the current process view.
    pub accepted_through: Option<u64>,
    /// Highest sequence covered by a successful sync.
    pub durable_through: Option<u64>,
}

/// Read-only proof that one ordered ingress frame exists in the current log.
///
/// `durable` reflects the current handle's proven durable watermark. A
/// read-only opener cannot prove that a prior writer synced a recovered frame,
/// so it reports `false` until a writable opener has synced that prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngressReceipt {
    pub identity: IngressIdentity,
    pub frame_offset: u64,
    pub records: usize,
    pub points: usize,
    pub bytes_written: u64,
    pub durable: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct IngressKey {
    source_id: u128,
    sequence: u64,
}

impl From<IngressIdentity> for IngressKey {
    fn from(identity: IngressIdentity) -> Self {
        Self {
            source_id: identity.source_id,
            sequence: identity.sequence,
        }
    }
}

#[derive(Clone, Debug)]
struct StoredIngressReceipt {
    identity: IngressIdentity,
    canonical_payload_offset: u64,
    canonical_payload_len: u32,
    canonical_payload_crc32: u32,
    /// Exact canonical bytes retained by a compact identity index. Live
    /// receipts instead point at their original frame in `active.wlog`.
    compact_payload: Option<Arc<[u8]>>,
    commit: Commit,
}

#[derive(Clone, Debug)]
struct StoredIdentifiedReceipt {
    payload_offset: u64,
    payload_len: u32,
    payload_crc32: u32,
    /// Exact identified-frame payload retained by a compact identity index.
    compact_payload: Option<Arc<[u8]>>,
    commit: Commit,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CompactIdentifiedReceipt {
    commit_id: u128,
    payload_len: u32,
    payload_crc32: u32,
    points: u64,
    records: u64,
    /// Empty only when reading an index written by the first kind-5 format,
    /// which stored CRC metadata but no bytes. Such receipts stay known but
    /// fail closed on retry instead of treating CRC equality as exact proof.
    #[serde(default)]
    payload: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CompactIngressReceipt {
    source_id: u128,
    sequence: u64,
    commit_id: u128,
    canonical_payload_len: u32,
    canonical_payload_crc32: u32,
    points: u64,
    records: u64,
    /// See [`CompactIdentifiedReceipt::payload`].
    #[serde(default)]
    canonical_payload: Vec<u8>,
    #[serde(default)]
    frame_offset: u64,
    #[serde(default)]
    bytes_written: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct CompactIdentityIndex {
    identified: Vec<CompactIdentifiedReceipt>,
    ingress: Vec<CompactIngressReceipt>,
}

/// Old writers created this kind-5 payload before they retained exact retry
/// bytes. Keep a decoder so those stores still open; their known identifiers
/// fail closed on retry because this format cannot prove byte equality.
#[derive(Deserialize)]
struct LegacyCompactIdentifiedReceipt {
    commit_id: u128,
    payload_len: u32,
    payload_crc32: u32,
    points: u64,
    records: u64,
}

#[derive(Deserialize)]
struct LegacyCompactIngressReceipt {
    source_id: u128,
    sequence: u64,
    commit_id: u128,
    canonical_payload_len: u32,
    canonical_payload_crc32: u32,
    points: u64,
    records: u64,
}

#[derive(Deserialize)]
struct LegacyCompactIdentityIndex {
    identified: Vec<LegacyCompactIdentifiedReceipt>,
    ingress: Vec<LegacyCompactIngressReceipt>,
}

impl From<LegacyCompactIdentityIndex> for CompactIdentityIndex {
    fn from(index: LegacyCompactIdentityIndex) -> Self {
        Self {
            identified: index
                .identified
                .into_iter()
                .map(|receipt| CompactIdentifiedReceipt {
                    commit_id: receipt.commit_id,
                    payload_len: receipt.payload_len,
                    payload_crc32: receipt.payload_crc32,
                    points: receipt.points,
                    records: receipt.records,
                    payload: Vec::new(),
                })
                .collect(),
            ingress: index
                .ingress
                .into_iter()
                .map(|receipt| CompactIngressReceipt {
                    source_id: receipt.source_id,
                    sequence: receipt.sequence,
                    commit_id: receipt.commit_id,
                    canonical_payload_len: receipt.canonical_payload_len,
                    canonical_payload_crc32: receipt.canonical_payload_crc32,
                    points: receipt.points,
                    records: receipt.records,
                    canonical_payload: Vec::new(),
                    frame_offset: 0,
                    bytes_written: 0,
                })
                .collect(),
        }
    }
}

/// Why the final bytes of a log were recovered.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RecoveredTail {
    /// The log ended at a frame boundary.
    #[default]
    None,
    /// The log ended before its final frame header was complete.
    IncompleteHeader,
    /// The final frame header was complete, but its payload was short.
    IncompletePayload,
}

/// Why salvage stopped after its longest fully validated raw-log prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SalvageStopReason {
    CleanEof,
    IncompleteFrameHeader,
    InvalidFrameMagic,
    UnsupportedFrameVersion,
    FrameHeaderChecksumMismatch,
    InvalidLegacyFrameSize,
    InvalidLegacyPoint,
    TransactionFrameTooLarge,
    IdentifiedTransactionTooShort,
    IngressTransactionTooShort,
    UnknownFrameKind,
    IncompleteFramePayload,
    PayloadChecksumMismatch,
    DuplicateCommitId,
    DuplicateIngressSequence,
    InvalidIngressSequence,
    InvalidTransaction,
    TransactionPointCountTooLarge,
    InvalidCatalogTransaction,
    SealCheckpointInvalid,
    IdentityIndexInvalid,
}

impl fmt::Display for SalvageStopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::CleanEof => "clean-eof",
            Self::IncompleteFrameHeader => "incomplete-frame-header",
            Self::InvalidFrameMagic => "invalid-frame-magic",
            Self::UnsupportedFrameVersion => "unsupported-frame-version",
            Self::FrameHeaderChecksumMismatch => "frame-header-checksum-mismatch",
            Self::InvalidLegacyFrameSize => "invalid-legacy-frame-size",
            Self::InvalidLegacyPoint => "invalid-legacy-point",
            Self::TransactionFrameTooLarge => "transaction-frame-too-large",
            Self::IdentifiedTransactionTooShort => "identified-transaction-too-short",
            Self::IngressTransactionTooShort => "ingress-transaction-too-short",
            Self::UnknownFrameKind => "unknown-frame-kind",
            Self::IncompleteFramePayload => "incomplete-frame-payload",
            Self::PayloadChecksumMismatch => "payload-checksum-mismatch",
            Self::DuplicateCommitId => "duplicate-commit-id",
            Self::DuplicateIngressSequence => "duplicate-ingress-sequence",
            Self::InvalidIngressSequence => "invalid-ingress-sequence",
            Self::InvalidTransaction => "invalid-transaction",
            Self::TransactionPointCountTooLarge => "transaction-point-count-too-large",
            Self::InvalidCatalogTransaction => "invalid-catalog-transaction",
            Self::SealCheckpointInvalid => "seal-checkpoint-invalid",
            Self::IdentityIndexInvalid => "identity-index-invalid",
        };
        f.write_str(name)
    }
}

impl std::fmt::Display for RecoveredTail {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::None => "none",
            Self::IncompleteHeader => "incomplete-header",
            Self::IncompletePayload => "incomplete-payload",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Stats {
    pub points: u64,
    pub commits: u64,
    pub series: usize,
    pub catalog_records: u64,
    pub file_bytes: u64,
    pub recovered_tail_bytes: u64,
    pub recovered_tail: RecoveredTail,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlanOutcome {
    pub valid_time: i64,
    pub planned: Option<Point>,
    pub actual: Option<Point>,
    /// `actual - planned` when both sides exist.
    pub difference: Option<f64>,
}

/// A single-writer embedded database.
pub struct Database {
    path: PathBuf,
    file: File,
    config: Config,
    read_only: bool,
    index: HashMap<u64, Vec<Point>>,
    /// Immutable raw segments published by the store. Queries merge these
    /// with the live tail; they are never rebuilt into `index`.
    sealed: Vec<Segment>,
    sealed_points: u64,
    /// True when recovery dropped sealed frames from the live index because
    /// a published checkpoint is still sitting in an unreclaimed log.
    pending_reclaim: bool,
    catalog: Catalog,
    /// Every client-supplied commit identifier in the log, rebuilt on open by
    /// `scan_and_recover` so idempotency survives a crash or reopen. Growth is
    /// one `u128` plus hash overhead per identified commit for the life of the
    /// log — acceptable because the whole log is already replayed into memory
    /// by design (the README acknowledges that ceiling).
    commit_ids: HashSet<u128>,
    /// Payload receipts for legacy [`Transaction::with_commit_id`] frames.
    /// A replay compares encoded bytes (and re-reads the stored frame) so a
    /// reused identifier with different records cannot silently deduplicate.
    identified_receipts: HashMap<u128, StoredIdentifiedReceipt>,
    /// One receipt per ordered ingress frame. Live receipts read canonical
    /// transaction bytes from the log. Reclaim retains those exact bytes in
    /// kind 5 so CRC equality never becomes the retry decision.
    ingress_receipts: HashMap<IngressKey, StoredIngressReceipt>,
    ingress_commit_ids: HashMap<u128, IngressKey>,
    ingress_last_sequences: HashMap<u128, u64>,
    ingress_durable_sequences: HashMap<u128, u64>,
    commits: u64,
    points: u64,
    catalog_records: u64,
    recovered_tail_bytes: u64,
    recovered_tail: RecoveredTail,
    bytes_since_sync: u64,
    poisoned: bool,
}

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_SYNC: std::cell::Cell<Option<std::io::ErrorKind>> = const {
        std::cell::Cell::new(None)
    };

    static MUTATE_SALVAGE_SOURCE_AFTER_IDENTITY_CHECKS: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn fail_next_sync(kind: std::io::ErrorKind) {
    FAIL_NEXT_SYNC.with(|failure| failure.set(Some(kind)));
}

#[cfg(test)]
pub(crate) fn mutate_salvage_source_after_identity_checks(passed_checks: usize) {
    MUTATE_SALVAGE_SOURCE_AFTER_IDENTITY_CHECKS.with(|remaining| {
        remaining.set(Some(passed_checks));
    });
}

fn sync_database_file(file: &File) -> std::io::Result<()> {
    #[cfg(test)]
    if let Some(kind) = FAIL_NEXT_SYNC.with(std::cell::Cell::take) {
        return Err(std::io::Error::new(kind, "injected sync failure"));
    }
    file.sync_data()
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, Config::default())
    }

    pub fn open_with(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        Self::open_mode(path.as_ref(), config, false, &HashSet::new())
    }

    /// Opens an existing database without any possibility of mutating it.
    ///
    /// The file is opened without write access and is never created, torn-tail
    /// recovery is simulated in memory instead of physically truncating the
    /// file, and every writer API (`append`, `commit`, `flush`) fails with
    /// [`Error::ReadOnly`]. A shared advisory lock replaces the exclusive one,
    /// so concurrent read-only openers coexist while a live exclusive writer
    /// still blocks (and is blocked by) read-only opens, preventing reads of a
    /// mid-write file.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_mode(path.as_ref(), Config::default(), true, &HashSet::new())
    }

    pub(crate) fn open_with_published_seals(
        path: impl AsRef<Path>,
        config: Config,
        published_seals: &HashSet<u64>,
    ) -> Result<Self> {
        Self::open_mode(path.as_ref(), config, false, published_seals)
    }

    pub(crate) fn open_read_only_with_published_seals(
        path: impl AsRef<Path>,
        published_seals: &HashSet<u64>,
    ) -> Result<Self> {
        Self::open_mode(path.as_ref(), Config::default(), true, published_seals)
    }

    fn open_mode(
        path: &Path,
        config: Config,
        read_only: bool,
        published_seals: &HashSet<u64>,
    ) -> Result<Self> {
        validate_config(config)?;
        let mut file = if read_only {
            open_regular_file_read_only(path)?
        } else {
            open_regular_file_read_write(path)?
        };

        // Take an advisory lock before recovery so a writer and a reader can
        // never interleave: the exclusive writer lock keeps a second opener
        // from interleaving frames or truncating this handle's in-flight
        // tail, while the shared read-only lock admits other inspectors but
        // excludes any exclusive writer. The lock lives exactly as long as
        // the handle: closing or dropping the database releases it, even on
        // panic or crash.
        let lock_result = if read_only {
            file.try_lock_shared()
        } else {
            file.try_lock()
        };
        match lock_result {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(Error::Locked {
                    path: path.to_path_buf(),
                });
            }
            Err(TryLockError::Error(error)) => return Err(Error::Io(error)),
        }

        if read_only {
            // A writable open initializes an empty file; a read-only open
            // must instead reject anything too short to hold a header.
            if file.metadata()?.len() < DATABASE_HEADER_BYTES as u64 {
                return Err(Error::InvalidHeader);
            }
        } else if file.metadata()?.len() == 0 {
            write_database_header(&mut file)?;
            // The header is durable inside the file, but a freshly created
            // file only survives power loss once its parent directory entry
            // is synced as well — otherwise a database that acknowledged
            // durable commits can vanish wholesale. A pre-existing empty
            // file takes the same path, which at worst repeats a directory
            // sync that was already durable.
            sync_parent_directory(path)?;
        }

        let mut scan = scan_and_recover(
            &mut file,
            config.max_batch_points,
            config.max_transaction_bytes,
            read_only,
            published_seals,
        )?;
        let ingress_durable_sequences = if read_only {
            // A complete frame proves only that the kernel can read it. The
            // prior writer may have used Manual or EveryBytes durability and
            // died before syncing, so a read-only opener cannot tell a client
            // that it is safe to discard its source copy.
            HashMap::new()
        } else {
            // Recovery can see complete frames left in the page cache by a
            // process crash. Sync the recovered prefix before publishing a
            // durable watermark or replay receipt. This also makes a tail
            // truncation durable before the writer accepts more frames.
            sync_database_file(&file)?;
            for receipt in scan.ingress_receipts.values_mut() {
                receipt.commit.durable = true;
            }
            scan.ingress_last_sequences.clone()
        };
        Ok(Self {
            path: path.to_path_buf(),
            file,
            config,
            read_only,
            index: scan.index,
            sealed: Vec::new(),
            sealed_points: 0,
            pending_reclaim: scan.pending_reclaim,
            catalog: scan.catalog,
            commit_ids: scan.commit_ids,
            identified_receipts: scan.identified_receipts,
            ingress_receipts: scan.ingress_receipts,
            ingress_commit_ids: scan.ingress_commit_ids,
            ingress_last_sequences: scan.ingress_last_sequences,
            ingress_durable_sequences,
            commits: scan.commits,
            points: scan.points,
            catalog_records: scan.catalog_records,
            recovered_tail_bytes: scan.recovered_tail_bytes,
            recovered_tail: scan.recovered_tail,
            bytes_since_sync: 0,
            poisoned: false,
        })
    }

    /// Appends one atomic, checksummed batch.
    ///
    /// This is the catalog-less fast path: points may refer to series and
    /// runs that no catalog record defines, because callers ingesting raw
    /// telemetry use it without ever creating a catalog. Only the
    /// catalog-independent point invariants are enforced — an interval must
    /// not end before it starts, rejected with the same error a transaction
    /// commit gives. Series existence and run provenance are checked
    /// exclusively by [`Database::commit`].
    pub fn append(&mut self, points: &[Point]) -> Result<Commit> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if points.len() > self.config.max_batch_points {
            return Err(Error::BatchTooLarge {
                points: points.len(),
                maximum: self.config.max_batch_points,
            });
        }
        crate::catalog::validate_point_intervals(points)?;
        if points.is_empty() {
            return Ok(Commit {
                frame_offset: self.file.seek(SeekFrom::End(0))?,
                points: 0,
                records: 0,
                bytes_written: 0,
                durable: self.bytes_since_sync == 0,
                deduplicated: false,
            });
        }

        let payload_len = points
            .len()
            .checked_mul(POINT_BYTES)
            .ok_or(Error::BatchTooLarge {
                points: points.len(),
                maximum: self.config.max_batch_points,
            })?;
        let payload_len_u32 = u32::try_from(payload_len).map_err(|_| Error::BatchTooLarge {
            points: points.len(),
            maximum: self.config.max_batch_points,
        })?;
        let point_count = u32::try_from(points.len()).map_err(|_| Error::BatchTooLarge {
            points: points.len(),
            maximum: self.config.max_batch_points,
        })?;

        let mut payload = Vec::with_capacity(payload_len);
        for point in points {
            encode_point(*point, &mut payload);
        }
        let frame_header = encode_frame_header(
            FRAME_KIND_LEGACY_POINTS,
            point_count,
            payload_len_u32,
            hash(&payload),
        );
        let bytes_written = (FRAME_HEADER_BYTES + payload.len()) as u64;
        let frame_offset = self.file.seek(SeekFrom::End(0))?;

        let write_result = (|| -> Result<bool> {
            self.file.write_all(&frame_header)?;
            self.file.write_all(&payload)?;
            self.bytes_since_sync += bytes_written;

            let should_sync = match self.config.durability {
                Durability::Always => true,
                Durability::EveryBytes(threshold) => self.bytes_since_sync >= threshold,
                Durability::Manual => false,
            };
            if should_sync {
                sync_database_file(&self.file)?;
                self.bytes_since_sync = 0;
            }
            Ok(should_sync)
        })();

        let durable = match write_result {
            Ok(durable) => durable,
            Err(error) => {
                self.poisoned = true;
                return Err(error);
            }
        };
        if durable {
            self.mark_ingress_durable();
        }

        for point in points {
            insert_indexed_point(&mut self.index, *point);
        }
        self.commits += 1;
        self.points += points.len() as u64;

        Ok(Commit {
            frame_offset,
            points: points.len(),
            records: 1,
            bytes_written,
            durable,
            deduplicated: false,
        })
    }

    /// Atomically commits catalog changes and point batches in one frame.
    ///
    /// When the transaction carries a [`Transaction::with_commit_id`]
    /// identifier that is already in the log, the encoded payload is checked
    /// against the original frame. An exact retry writes nothing and reports
    /// [`Commit::deduplicated`]. Reusing the identifier with different
    /// records returns [`Error::IngressCommitIdConflict`]. Prefer
    /// [`Self::commit_ingress`] at a production writer boundary so retries
    /// carry a source cursor and cannot drop a mutated payload. An empty
    /// transaction writes no frame, so its identifier (if any) is not
    /// recorded; there is nothing a replay could duplicate.
    pub fn commit(&mut self, transaction: Transaction) -> Result<Commit> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if let Some(identity) = transaction.ingress_identity {
            return self.commit_ordered_ingress(identity, transaction);
        }
        if let Some(commit_id) = transaction.commit_id {
            if self.ingress_commit_ids.contains_key(&commit_id) {
                return Err(Error::IngressCommitIdConflict { commit_id });
            }
            if self.commit_ids.contains(&commit_id) {
                let payload = encode_transaction(&transaction)?;
                if let Some(receipt) = self.identified_receipts.get(&commit_id).cloned() {
                    if !self.identified_payload_matches(receipt, &payload)? {
                        return Err(Error::IngressCommitIdConflict { commit_id });
                    }
                    return Ok(Commit {
                        frame_offset: self.file.seek(SeekFrom::End(0))?,
                        points: 0,
                        records: 0,
                        bytes_written: 0,
                        durable: self.bytes_since_sync == 0,
                        deduplicated: true,
                    });
                }
                return Err(Error::IngressCommitIdConflict { commit_id });
            }
        }
        if transaction.is_empty() {
            return Ok(Commit {
                frame_offset: self.file.seek(SeekFrom::End(0))?,
                points: 0,
                records: 0,
                bytes_written: 0,
                durable: self.bytes_since_sync == 0,
                deduplicated: false,
            });
        }
        let point_count = transaction.point_count();
        if point_count > self.config.max_batch_points {
            return Err(Error::BatchTooLarge {
                points: point_count,
                maximum: self.config.max_batch_points,
            });
        }

        let candidate_catalog = self.catalog.apply_records(&transaction.records)?;
        let payload = encode_transaction(&transaction)?;
        if payload.len() > self.config.max_transaction_bytes {
            return Err(Error::InvalidModel(format!(
                "transaction has {} encoded bytes; maximum is {}",
                payload.len(),
                self.config.max_transaction_bytes
            )));
        }
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| Error::Serialization("transaction exceeds u32 length".to_owned()))?;
        let record_count = u32::try_from(transaction.record_count())
            .map_err(|_| Error::Serialization("too many transaction records".to_owned()))?;
        let frame_kind = if transaction.commit_id.is_some() {
            FRAME_KIND_IDENTIFIED_TRANSACTION
        } else {
            FRAME_KIND_TRANSACTION
        };
        let frame_header =
            encode_frame_header(frame_kind, record_count, payload_len, hash(&payload));
        let bytes_written = (FRAME_HEADER_BYTES + payload.len()) as u64;
        let frame_offset = self.file.seek(SeekFrom::End(0))?;
        let durable = self.write_frame(&frame_header, &payload, bytes_written)?;

        for record in &transaction.records {
            if let Record::Points(points) = record {
                for point in points {
                    insert_indexed_point(&mut self.index, *point);
                }
            }
        }
        if let Some(catalog) = candidate_catalog {
            self.catalog = catalog;
        }
        if let Some(commit_id) = transaction.commit_id {
            self.commit_ids.insert(commit_id);
            self.identified_receipts.insert(
                commit_id,
                StoredIdentifiedReceipt {
                    payload_offset: frame_offset + FRAME_HEADER_BYTES as u64,
                    payload_len,
                    payload_crc32: hash(&payload),
                    compact_payload: None,
                    commit: Commit {
                        frame_offset,
                        points: point_count,
                        records: transaction.record_count(),
                        bytes_written,
                        durable,
                        deduplicated: false,
                    },
                },
            );
        }
        self.commits += 1;
        self.points += point_count as u64;
        let metadata_records = transaction.record_count()
            - transaction
                .records
                .iter()
                .filter(|record| matches!(record, Record::Points(_)))
                .count();
        self.catalog_records += metadata_records as u64;

        Ok(Commit {
            frame_offset,
            points: point_count,
            records: transaction.record_count(),
            bytes_written,
            durable,
            deduplicated: false,
        })
    }

    /// Commits one ordered producer transaction with two durable retry keys.
    ///
    /// FTWDB stores `identity` and the canonical transaction bytes in one
    /// checksummed frame. An exact replay returns the original frame receipt
    /// with `deduplicated: true` and writes nothing. Reusing either key with
    /// different bytes returns a nonfatal ingress conflict. After a source's
    /// first commit, only a strictly greater source cursor is accepted. The
    /// cursor may contain gaps; source reconciliation detects missing data.
    pub fn commit_ingress(
        &mut self,
        identity: IngressIdentity,
        mut transaction: Transaction,
    ) -> Result<Commit> {
        transaction.with_ingress_identity(identity);
        self.commit(transaction)
    }

    fn commit_ordered_ingress(
        &mut self,
        identity: IngressIdentity,
        transaction: Transaction,
    ) -> Result<Commit> {
        if identity.source_id == 0 {
            return Err(Error::InvalidArgument("ingress source id zero is reserved"));
        }
        let canonical = encode_canonical_transaction(&transaction)?;
        let key = IngressKey::from(identity);

        if let Some(receipt) = self.ingress_receipts.get(&key).cloned() {
            if receipt.identity.commit_id != identity.commit_id
                || !self.ingress_payload_matches(receipt.clone(), &canonical)?
            {
                return Err(Error::IngressSourceSequenceConflict {
                    source_id: identity.source_id,
                    sequence: identity.sequence,
                });
            }
            let mut replay = receipt.commit;
            replay.deduplicated = true;
            // The source watermark, not an empty dirty-byte counter, is the
            // durability proof. A read-only recovery has no such proof.
            replay.durable = self
                .ingress_durable_sequences
                .get(&identity.source_id)
                .is_some_and(|sequence| *sequence >= identity.sequence);
            return Ok(replay);
        }

        if self.commit_ids.contains(&identity.commit_id) {
            return Err(Error::IngressCommitIdConflict {
                commit_id: identity.commit_id,
            });
        }

        if let Some(last) = self
            .ingress_last_sequences
            .get(&identity.source_id)
            .copied()
            && identity.sequence <= last
        {
            return Err(Error::IngressSequenceNotIncreasing {
                source_id: identity.source_id,
                previous: last,
                actual: identity.sequence,
            });
        }

        let point_count = transaction.point_count();
        if point_count > self.config.max_batch_points {
            return Err(Error::BatchTooLarge {
                points: point_count,
                maximum: self.config.max_batch_points,
            });
        }
        let payload_len = INGRESS_IDENTITY_BYTES
            .checked_add(canonical.len())
            .ok_or_else(|| {
                Error::Serialization("ingress transaction length overflows".to_owned())
            })?;
        if payload_len > self.config.max_transaction_bytes {
            return Err(Error::InvalidModel(format!(
                "transaction has {payload_len} encoded bytes; maximum is {}",
                self.config.max_transaction_bytes
            )));
        }

        let candidate_catalog = self.catalog.apply_records(&transaction.records)?;
        let payload_len_u32 = u32::try_from(payload_len)
            .map_err(|_| Error::Serialization("transaction exceeds u32 length".to_owned()))?;
        let canonical_len_u32 = u32::try_from(canonical.len())
            .map_err(|_| Error::Serialization("transaction exceeds u32 length".to_owned()))?;
        let record_count = u32::try_from(transaction.record_count())
            .map_err(|_| Error::Serialization("too many transaction records".to_owned()))?;
        let mut payload = Vec::with_capacity(payload_len);
        payload.extend_from_slice(&identity.source_id.to_le_bytes());
        payload.extend_from_slice(&identity.sequence.to_le_bytes());
        payload.extend_from_slice(&identity.commit_id.to_le_bytes());
        payload.extend_from_slice(&canonical);
        let frame_header = encode_frame_header(
            FRAME_KIND_INGRESS_TRANSACTION,
            record_count,
            payload_len_u32,
            hash(&payload),
        );
        let bytes_written = (FRAME_HEADER_BYTES + payload.len()) as u64;
        let frame_offset = self.file.seek(SeekFrom::End(0))?;
        let durable = self.write_frame(&frame_header, &payload, bytes_written)?;

        for record in &transaction.records {
            if let Record::Points(points) = record {
                for point in points {
                    insert_indexed_point(&mut self.index, *point);
                }
            }
        }
        if let Some(catalog) = candidate_catalog {
            self.catalog = catalog;
        }
        self.commits += 1;
        self.points += point_count as u64;
        let metadata_records = transaction.record_count()
            - transaction
                .records
                .iter()
                .filter(|record| matches!(record, Record::Points(_)))
                .count();
        self.catalog_records += metadata_records as u64;

        let commit = Commit {
            frame_offset,
            points: point_count,
            records: transaction.record_count(),
            bytes_written,
            durable,
            deduplicated: false,
        };
        let receipt = StoredIngressReceipt {
            identity,
            canonical_payload_offset: frame_offset
                + FRAME_HEADER_BYTES as u64
                + INGRESS_IDENTITY_BYTES as u64,
            canonical_payload_len: canonical_len_u32,
            canonical_payload_crc32: hash(&canonical),
            compact_payload: None,
            commit,
        };
        self.commit_ids.insert(identity.commit_id);
        self.ingress_receipts.insert(key, receipt);
        self.ingress_commit_ids.insert(identity.commit_id, key);
        self.ingress_last_sequences
            .insert(identity.source_id, identity.sequence);
        if durable {
            self.ingress_durable_sequences
                .insert(identity.source_id, identity.sequence);
        }
        Ok(commit)
    }

    fn identified_payload_matches(
        &mut self,
        receipt: StoredIdentifiedReceipt,
        candidate: &[u8],
    ) -> Result<bool> {
        let result = self.identified_payload_matches_at(receipt, candidate);
        if matches!(result, Err(Error::Io(_) | Error::Corruption { .. })) {
            self.poisoned = true;
        }
        result
    }

    fn identified_payload_matches_at(
        &self,
        receipt: StoredIdentifiedReceipt,
        candidate: &[u8],
    ) -> Result<bool> {
        if candidate.len() != receipt.payload_len as usize
            || hash(candidate) != receipt.payload_crc32
        {
            return Ok(false);
        }
        if receipt.payload_offset == 0 {
            // An old compact index may lack exact bytes. Treat it as a known
            // identifier that conflicts on retry; CRC equality alone cannot
            // prove that the transaction is the same.
            return Ok(receipt.compact_payload.as_deref() == Some(candidate));
        }
        let expected_payload_offset = receipt
            .commit
            .frame_offset
            .checked_add(FRAME_HEADER_BYTES as u64)
            .ok_or_else(|| Error::Serialization("identified replay offset overflows".to_owned()))?;
        if receipt.payload_offset != expected_payload_offset {
            return corruption(
                receipt.commit.frame_offset,
                "stored identified receipt offset is invalid",
            );
        }
        let record_count = u32::try_from(receipt.commit.records).map_err(|_| {
            Error::Serialization("identified replay record count overflows".to_owned())
        })?;
        let expected_header = encode_frame_header(
            FRAME_KIND_IDENTIFIED_TRANSACTION,
            record_count,
            receipt.payload_len,
            receipt.payload_crc32,
        );
        let mut stored_header = [0_u8; FRAME_HEADER_BYTES];
        self.file
            .read_exact_at(&mut stored_header, receipt.commit.frame_offset)?;
        if stored_header != expected_header {
            return corruption(
                receipt.commit.frame_offset,
                "stored identified frame header changed after open",
            );
        }

        let mut stored = [0_u8; 8 * 1024];
        let mut candidate_offset = 0_usize;
        while candidate_offset < candidate.len() {
            let chunk_len = stored.len().min(candidate.len() - candidate_offset);
            let file_offset = receipt
                .payload_offset
                .checked_add(u64::try_from(candidate_offset).map_err(|_| {
                    Error::Serialization("identified replay offset overflows".to_owned())
                })?)
                .ok_or_else(|| {
                    Error::Serialization("identified replay offset overflows".to_owned())
                })?;
            self.file
                .read_exact_at(&mut stored[..chunk_len], file_offset)?;
            if stored[..chunk_len] != candidate[candidate_offset..candidate_offset + chunk_len] {
                return corruption(
                    receipt.commit.frame_offset,
                    "stored identified payload changed after open",
                );
            }
            candidate_offset += chunk_len;
        }
        Ok(true)
    }

    fn ingress_payload_matches(
        &mut self,
        receipt: StoredIngressReceipt,
        candidate: &[u8],
    ) -> Result<bool> {
        let result = self.ingress_payload_matches_at(receipt, candidate);
        if matches!(result, Err(Error::Io(_) | Error::Corruption { .. })) {
            // The in-memory catalog and index may no longer match the log.
            // Stop all later writes until a fresh scan proves a sound prefix.
            self.poisoned = true;
        }
        result
    }

    fn ingress_payload_matches_at(
        &self,
        receipt: StoredIngressReceipt,
        candidate: &[u8],
    ) -> Result<bool> {
        if candidate.len() != receipt.canonical_payload_len as usize
            || hash(candidate) != receipt.canonical_payload_crc32
        {
            return Ok(false);
        }
        if receipt.canonical_payload_offset == 0 {
            return Ok(receipt.compact_payload.as_deref() == Some(candidate));
        }
        let expected_canonical_offset = receipt
            .commit
            .frame_offset
            .checked_add((FRAME_HEADER_BYTES + INGRESS_IDENTITY_BYTES) as u64)
            .ok_or_else(|| Error::Serialization("ingress replay offset overflows".to_owned()))?;
        if receipt.canonical_payload_offset != expected_canonical_offset {
            return corruption(
                receipt.commit.frame_offset,
                "stored ingress receipt offset is invalid",
            );
        }
        let mut identity_bytes = [0_u8; INGRESS_IDENTITY_BYTES];
        identity_bytes[..16].copy_from_slice(&receipt.identity.source_id.to_le_bytes());
        identity_bytes[16..24].copy_from_slice(&receipt.identity.sequence.to_le_bytes());
        identity_bytes[24..40].copy_from_slice(&receipt.identity.commit_id.to_le_bytes());
        let mut expected_payload_checksum = crc32fast::Hasher::new();
        expected_payload_checksum.update(&identity_bytes);
        expected_payload_checksum.update(candidate);
        let payload_len = u32::try_from(INGRESS_IDENTITY_BYTES + candidate.len())
            .map_err(|_| Error::Serialization("ingress replay length overflows".to_owned()))?;
        let record_count = u32::try_from(receipt.commit.records).map_err(|_| {
            Error::Serialization("ingress replay record count overflows".to_owned())
        })?;
        let expected_header = encode_frame_header(
            FRAME_KIND_INGRESS_TRANSACTION,
            record_count,
            payload_len,
            expected_payload_checksum.finalize(),
        );

        let mut stored_header = [0_u8; FRAME_HEADER_BYTES];
        self.file
            .read_exact_at(&mut stored_header, receipt.commit.frame_offset)?;
        if stored_header != expected_header {
            return corruption(
                receipt.commit.frame_offset,
                "stored ingress frame header changed after open",
            );
        }

        let identity_offset = receipt
            .commit
            .frame_offset
            .checked_add(FRAME_HEADER_BYTES as u64)
            .ok_or_else(|| Error::Serialization("ingress replay offset overflows".to_owned()))?;
        let mut stored_identity = [0_u8; INGRESS_IDENTITY_BYTES];
        self.file
            .read_exact_at(&mut stored_identity, identity_offset)?;
        if stored_identity != identity_bytes {
            return corruption(
                receipt.commit.frame_offset,
                "stored ingress identity changed after open",
            );
        }

        let mut stored = [0_u8; 8 * 1024];
        let mut stored_checksum = crc32fast::Hasher::new();
        let mut equal = true;
        let mut candidate_offset = 0_usize;
        while candidate_offset < candidate.len() {
            let chunk_len = stored.len().min(candidate.len() - candidate_offset);
            let file_offset = receipt
                .canonical_payload_offset
                .checked_add(u64::try_from(candidate_offset).map_err(|_| {
                    Error::Serialization("ingress replay offset overflows".to_owned())
                })?)
                .ok_or_else(|| {
                    Error::Serialization("ingress replay offset overflows".to_owned())
                })?;
            self.file
                .read_exact_at(&mut stored[..chunk_len], file_offset)?;
            stored_checksum.update(&stored[..chunk_len]);
            equal &=
                stored[..chunk_len] == candidate[candidate_offset..candidate_offset + chunk_len];
            candidate_offset += chunk_len;
        }
        if stored_checksum.finalize() != receipt.canonical_payload_crc32 {
            return corruption(
                receipt.commit.frame_offset,
                "stored ingress transaction checksum changed after open",
            );
        }
        Ok(equal)
    }

    /// Checks one expected ingress transaction against its exact stored
    /// canonical bytes without changing the file cursor or writing the store.
    pub(crate) fn verify_ingress_payload(
        &self,
        identity: IngressIdentity,
        transaction: &Transaction,
    ) -> Result<Option<bool>> {
        let Some(receipt) = self
            .ingress_receipts
            .get(&IngressKey::from(identity))
            .cloned()
        else {
            return Ok(None);
        };
        if receipt.identity != identity {
            return Ok(Some(false));
        }
        let canonical = encode_canonical_transaction(transaction)?;
        self.ingress_payload_matches_at(receipt, &canonical)
            .map(Some)
    }

    /// Whether a [`Transaction::with_commit_id`] identifier has been applied
    /// by this handle — recovered from the log on open or committed since.
    #[must_use]
    pub fn contains_commit_id(&self, commit_id: u128) -> bool {
        self.commit_ids.contains(&commit_id)
    }

    /// Returns accepted and synced progress for one ordered source.
    #[must_use]
    pub fn ingress_watermarks(&self, source_id: u128) -> IngressWatermarks {
        IngressWatermarks {
            accepted_through: self.ingress_last_sequences.get(&source_id).copied(),
            durable_through: self.ingress_durable_sequences.get(&source_id).copied(),
        }
    }

    /// Returns the stored identity and frame receipt for one source sequence.
    ///
    /// This does not reread the frame payload. Open-time recovery already
    /// checked it; integrity checks and exact retry perform the stronger byte
    /// validation when required.
    #[must_use]
    pub fn ingress_receipt(&self, source_id: u128, sequence: u64) -> Option<IngressReceipt> {
        let stored = self.ingress_receipts.get(&IngressKey {
            source_id,
            sequence,
        })?;
        Some(IngressReceipt {
            identity: stored.identity,
            frame_offset: stored.commit.frame_offset,
            records: stored.commit.records,
            points: stored.commit.points,
            bytes_written: stored.commit.bytes_written,
            durable: self
                .ingress_durable_sequences
                .get(&source_id)
                .is_some_and(|durable| *durable >= sequence),
        })
    }

    /// Returns every known ingress source in stable source-ID order.
    #[must_use]
    pub fn all_ingress_watermarks(&self) -> BTreeMap<u128, IngressWatermarks> {
        self.ingress_last_sequences
            .keys()
            .copied()
            .map(|source_id| (source_id, self.ingress_watermarks(source_id)))
            .collect()
    }

    fn write_frame(
        &mut self,
        frame_header: &[u8; FRAME_HEADER_BYTES],
        payload: &[u8],
        bytes_written: u64,
    ) -> Result<bool> {
        let write_result = (|| -> Result<bool> {
            self.file.write_all(frame_header)?;
            self.file.write_all(payload)?;
            self.bytes_since_sync += bytes_written;
            let should_sync = match self.config.durability {
                Durability::Always => true,
                Durability::EveryBytes(threshold) => self.bytes_since_sync >= threshold,
                Durability::Manual => false,
            };
            if should_sync {
                sync_database_file(&self.file)?;
                self.bytes_since_sync = 0;
            }
            Ok(should_sync)
        })();
        match write_result {
            Ok(durable) => {
                if durable {
                    self.mark_ingress_durable();
                }
                Ok(durable)
            }
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
        }
    }

    fn mark_ingress_durable(&mut self) {
        self.ingress_durable_sequences
            .clone_from(&self.ingress_last_sequences);
    }

    /// Makes all prior appends durable according to the operating system.
    pub fn flush(&mut self) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let sync_result = sync_database_file(&self.file);
        if let Err(error) = sync_result {
            // A failed fsync may still have marked the dirty pages clean, so
            // a retried sync could succeed without the data ever reaching
            // media (fsyncgate). Poison the writer, exactly as a failed
            // frame write does, so durability can only be claimed again by
            // reopening and re-reading what is actually on disk.
            self.poisoned = true;
            return Err(Error::Io(error));
        }
        self.bytes_since_sync = 0;
        self.mark_ingress_durable();
        Ok(())
    }

    /// Flushes and closes the database. A read-only handle has nothing to
    /// flush and simply releases its shared lock.
    pub fn close(mut self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        self.flush()
    }

    #[must_use]
    pub const fn is_read_only(&self) -> bool {
        self.read_only
    }

    #[must_use]
    pub const fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Returns every revision in deterministic temporal order.
    ///
    /// Sealed raw segments are merged with the live tail. A sealed-segment
    /// read error is fail-closed: the call returns [`Error::Corruption`]
    /// rather than a silent partial history.
    pub fn query_history(&self, series_id: u64, start: i64, end: i64) -> Result<Vec<Point>> {
        let mut result = self.collect_raw_points(series_id, start, end)?;
        result.sort_by_key(|point| (point.valid_time, point.knowledge_time, point.change_time));
        Ok(result)
    }

    /// Live-index revisions only. Sealed history is not included.
    pub(crate) fn series_points(&self, series_id: u64) -> &[Point] {
        self.index.get(&series_id).map_or(&[], Vec::as_slice)
    }

    #[must_use]
    pub fn live_index_len(&self) -> usize {
        self.index.values().map(Vec::len).sum()
    }

    #[must_use]
    pub const fn sealed_point_count(&self) -> u64 {
        self.sealed_points
    }

    #[must_use]
    pub(crate) const fn pending_reclaim(&self) -> bool {
        self.pending_reclaim
    }

    pub(crate) fn series_revision_count(&self, series_id: u64) -> usize {
        let live = self.series_points(series_id).len();
        let sealed = self
            .sealed
            .iter()
            .map(|segment| segment.series_point_count(series_id) as usize)
            .sum::<usize>();
        live.saturating_add(sealed)
    }

    pub(crate) fn series_valid_bounds(&self, series_id: u64) -> Option<(i64, i64)> {
        let mut min_time = i64::MAX;
        let mut max_time = i64::MIN;
        if let Some((first, last)) = self
            .series_points(series_id)
            .first()
            .zip(self.series_points(series_id).last())
        {
            min_time = min_time.min(first.valid_time);
            max_time = max_time.max(last.valid_time);
        }
        for segment in &self.sealed {
            if let Some((start, end)) = segment.series_bounds(series_id) {
                min_time = min_time.min(start);
                max_time = max_time.max(end);
            }
        }
        (min_time <= max_time).then_some((min_time, max_time))
    }

    fn collect_raw_points(&self, series_id: u64, start: i64, end: i64) -> Result<Vec<Point>> {
        // Segment order follows seal order. Read those older revisions before
        // the live tail so the stable sort below keeps append order when two
        // revisions have identical bitemporal keys. `winning_revisions` uses
        // the later item as the tie-breaker, matching the documented
        // `(knowledge_time, change_time, append_order)` rule.
        let mut points = Vec::new();
        for segment in &self.sealed {
            if !segment.overlaps(series_id, start, end) {
                continue;
            }
            points.extend(segment.query(series_id, start, end)?);
        }
        points.extend_from_slice(series_time_slice(self.series_points(series_id), start, end));
        points.sort_by_key(|point| (point.valid_time, point.knowledge_time, point.change_time));
        Ok(points)
    }

    /// Returns the winning revision for each valid timestamp.
    pub fn query_latest(&self, series_id: u64, start: i64, end: i64) -> Result<Vec<Point>> {
        self.query_with_cutoffs(series_id, start, end, None, None)
    }

    /// Replays what was visible at one historical instant.
    pub fn query_as_of(
        &self,
        series_id: u64,
        start: i64,
        end: i64,
        as_of: i64,
    ) -> Result<Vec<Point>> {
        self.query_with_cutoffs(series_id, start, end, Some(as_of), Some(as_of))
    }

    /// Returns the latest correction within one forecast/optimization run.
    pub fn query_run(
        &self,
        series_id: u64,
        run_id: u128,
        start: i64,
        end: i64,
    ) -> Result<Vec<Point>> {
        Ok(winning_revisions(
            &self.collect_raw_points(series_id, start, end)?,
            start,
            end,
            |point| point.run_id == run_id,
            |candidate, current| candidate.change_time >= current.change_time,
        ))
    }

    /// Exact-time plan-versus-actual alignment. Higher-level resampling uses
    /// persistent rollups before calling this primitive.
    pub fn compare_plan_to_actual(
        &self,
        planned_series_id: u64,
        actual_series_id: u64,
        run_id: u128,
        start: i64,
        end: i64,
    ) -> Result<Vec<PlanOutcome>> {
        let mut aligned = BTreeMap::<i64, (Option<Point>, Option<Point>)>::new();
        for point in self.query_run(planned_series_id, run_id, start, end)? {
            aligned.entry(point.valid_time).or_default().0 = Some(point);
        }
        for point in self.query_run(actual_series_id, 0, start, end)? {
            aligned.entry(point.valid_time).or_default().1 = Some(point);
        }
        Ok(aligned
            .into_iter()
            .map(|(valid_time, (planned, actual))| PlanOutcome {
                valid_time,
                difference: planned
                    .zip(actual)
                    .map(|(planned, actual)| actual.value - planned.value),
                planned,
                actual,
            })
            .collect())
    }

    /// Separates forecast issue-time and correction-time cutoffs for strict
    /// point-in-time backtests.
    pub fn query_with_cutoffs(
        &self,
        series_id: u64,
        start: i64,
        end: i64,
        maximum_knowledge_time: Option<i64>,
        maximum_change_time: Option<i64>,
    ) -> Result<Vec<Point>> {
        Ok(winning_revisions(
            &self.collect_raw_points(series_id, start, end)?,
            start,
            end,
            |point| {
                maximum_knowledge_time.is_none_or(|cutoff| point.knowledge_time <= cutoff)
                    && maximum_change_time.is_none_or(|cutoff| point.change_time <= cutoff)
            },
            |candidate, current| {
                (candidate.knowledge_time, candidate.change_time)
                    >= (current.knowledge_time, current.change_time)
            },
        ))
    }

    /// Materializes fixed UTC gauge buckets from the winning revisions in a
    /// range. Persistent background rollups will use the same bucket state.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] when `resolution_micros` is not
    /// positive or `max_gap_micros` is negative.
    pub fn rollup_gauge(
        &self,
        series_id: u64,
        start: i64,
        end: i64,
        resolution_micros: i64,
        max_gap_micros: i64,
    ) -> Result<FixedGaugeRollup> {
        FixedGaugeRollup::build(
            &self.query_latest(series_id, start, end)?,
            resolution_micros,
            max_gap_micros,
        )
    }

    /// Writes the current raw point snapshot as an immutable compressed
    /// segment. Log reclamation is intentionally separate and requires a
    /// durable manifest in M3.
    pub fn create_segment(
        &self,
        path: impl AsRef<Path>,
        block_points: usize,
    ) -> Result<SegmentStats> {
        let points: Vec<_> = self.index.values().flatten().copied().collect();
        Segment::create(path, &points, block_points)
    }

    pub(crate) fn attach_sealed_segments(&mut self, segments: Vec<Segment>) {
        self.sealed_points = segments.iter().map(|segment| segment.stats().points).sum();
        self.sealed = segments;
    }

    pub(crate) fn live_points_snapshot(&self) -> Vec<Point> {
        self.index.values().flatten().copied().collect()
    }

    pub(crate) fn clear_live_index(&mut self) {
        self.index.clear();
        self.points = 0;
    }

    pub(crate) fn write_seal_checkpoint(&mut self, generation: u64, points: u64) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let mut payload = [0_u8; SEAL_CHECKPOINT_BYTES];
        payload[..8].copy_from_slice(&generation.to_le_bytes());
        payload[8..16].copy_from_slice(&points.to_le_bytes());
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| Error::Serialization("seal checkpoint exceeds u32 length".to_owned()))?;
        let header =
            encode_frame_header(FRAME_KIND_SEAL_CHECKPOINT, 0, payload_len, hash(&payload));
        let bytes_written = (FRAME_HEADER_BYTES + payload.len()) as u64;
        self.file.seek(SeekFrom::End(0))?;
        self.write_frame(&header, &payload, bytes_written)?;
        self.flush()?;
        self.commits += 1;
        Ok(())
    }

    fn compact_identity_index(&self) -> Result<CompactIdentityIndex> {
        let mut identified = Vec::with_capacity(self.identified_receipts.len());
        for (commit_id, receipt) in &self.identified_receipts {
            let payload = if receipt.payload_offset == 0 {
                receipt
                    .compact_payload
                    .as_deref()
                    .map(<[u8]>::to_vec)
                    .unwrap_or_default()
            } else {
                let mut payload = vec![0_u8; receipt.payload_len as usize];
                self.file
                    .read_exact_at(&mut payload, receipt.payload_offset)?;
                if !self.identified_payload_matches_at(receipt.clone(), &payload)? {
                    return corruption(
                        receipt.commit.frame_offset,
                        "stored identified payload changed before reclaim",
                    );
                }
                payload
            };
            identified.push(CompactIdentifiedReceipt {
                commit_id: *commit_id,
                payload_len: receipt.payload_len,
                payload_crc32: receipt.payload_crc32,
                points: receipt.commit.points as u64,
                records: receipt.commit.records as u64,
                payload,
            });
        }
        identified.sort_by_key(|receipt| receipt.commit_id);

        let mut ingress = Vec::with_capacity(self.ingress_receipts.len());
        for receipt in self.ingress_receipts.values() {
            let canonical_payload = if receipt.canonical_payload_offset == 0 {
                receipt
                    .compact_payload
                    .as_deref()
                    .map(<[u8]>::to_vec)
                    .unwrap_or_default()
            } else {
                let mut payload = vec![0_u8; receipt.canonical_payload_len as usize];
                self.file
                    .read_exact_at(&mut payload, receipt.canonical_payload_offset)?;
                if !self.ingress_payload_matches_at(receipt.clone(), &payload)? {
                    return corruption(
                        receipt.commit.frame_offset,
                        "stored ingress payload changed before reclaim",
                    );
                }
                payload
            };
            ingress.push(CompactIngressReceipt {
                source_id: receipt.identity.source_id,
                sequence: receipt.identity.sequence,
                commit_id: receipt.identity.commit_id,
                canonical_payload_len: receipt.canonical_payload_len,
                canonical_payload_crc32: receipt.canonical_payload_crc32,
                points: receipt.commit.points as u64,
                records: receipt.commit.records as u64,
                canonical_payload,
                frame_offset: receipt.commit.frame_offset,
                bytes_written: receipt.commit.bytes_written,
            });
        }
        // Recovery advances one cursor per source while it reads this list.
        // HashMap iteration has no stable order, so sort before serializing.
        ingress.sort_by_key(|receipt| (receipt.source_id, receipt.sequence));

        Ok(CompactIdentityIndex {
            identified,
            ingress,
        })
    }

    /// Rewrites `active.wlog` to catalog + identity receipts + the live tail.
    ///
    /// The exclusive lock moves with the new inode: the compact file is locked
    /// before it is renamed over the live name, so a concurrent opener never
    /// sees an unlocked `active.wlog`.
    pub(crate) fn reclaim_live_log(&mut self) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.flush()?;
        let identity_index = self.compact_identity_index()?;
        let compact_path = compact_log_path(&self.path)?;
        if let Err(error) = write_compact_log(
            &compact_path,
            &self.catalog,
            &identity_index,
            &self.index,
            self.config.max_batch_points,
            self.config.max_transaction_bytes,
        ) {
            let _ = std::fs::remove_file(&compact_path);
            return Err(error);
        }

        let mut new_file = open_regular_file_read_write(&compact_path)?;
        match new_file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                let _ = std::fs::remove_file(&compact_path);
                return Err(Error::Locked { path: compact_path });
            }
            Err(TryLockError::Error(error)) => {
                let _ = std::fs::remove_file(&compact_path);
                return Err(Error::Io(error));
            }
        }
        let mut scan = match scan_and_recover(
            &mut new_file,
            self.config.max_batch_points,
            self.config.max_transaction_bytes,
            false,
            &HashSet::new(),
        ) {
            Ok(scan) => scan,
            Err(error) => {
                drop(new_file);
                let _ = std::fs::remove_file(&compact_path);
                return Err(error);
            }
        };
        if let Err(error) = sync_database_file(&new_file) {
            self.poisoned = true;
            drop(new_file);
            let _ = std::fs::remove_file(&compact_path);
            return Err(Error::Io(error));
        }
        for receipt in scan.ingress_receipts.values_mut() {
            receipt.commit.durable = true;
        }

        if let Err(error) = std::fs::rename(&compact_path, &self.path) {
            self.poisoned = true;
            drop(new_file);
            let _ = std::fs::remove_file(&compact_path);
            return Err(Error::Io(error));
        }
        if let Err(error) = sync_parent_directory(&self.path) {
            self.poisoned = true;
            return Err(error);
        }

        let old = std::mem::replace(&mut self.file, new_file);
        drop(old);
        self.index = scan.index;
        self.catalog = scan.catalog;
        self.commit_ids = scan.commit_ids;
        self.identified_receipts = scan.identified_receipts;
        self.ingress_receipts = scan.ingress_receipts;
        self.ingress_commit_ids = scan.ingress_commit_ids;
        self.ingress_last_sequences = scan.ingress_last_sequences;
        self.ingress_durable_sequences = self.ingress_last_sequences.clone();
        self.commits = scan.commits;
        self.points = scan.points;
        self.catalog_records = scan.catalog_records;
        self.recovered_tail_bytes = 0;
        self.recovered_tail = RecoveredTail::None;
        self.bytes_since_sync = 0;
        self.pending_reclaim = false;
        Ok(())
    }

    pub fn stats(&self) -> Result<Stats> {
        // A read-only open leaves a torn tail on disk, so its physical length
        // is reduced by the simulated truncation to report the same logical
        // length a writable recovery would have left behind.
        let physical_bytes = self.file.metadata()?.len();
        let file_bytes = if self.read_only {
            physical_bytes.saturating_sub(self.recovered_tail_bytes)
        } else {
            physical_bytes
        };
        Ok(Stats {
            points: self.points.saturating_add(self.sealed_points),
            commits: self.commits,
            series: {
                let mut ids: HashSet<u64> = self.index.keys().copied().collect();
                for segment in &self.sealed {
                    ids.extend(segment.series_ids());
                }
                ids.len()
            },
            catalog_records: self.catalog_records,
            file_bytes,
            recovered_tail_bytes: self.recovered_tail_bytes,
            recovered_tail: self.recovered_tail,
        })
    }

    #[must_use]
    pub const fn durability(&self) -> Durability {
        self.config.durability
    }
}

fn validate_config(config: Config) -> Result<()> {
    if config.max_batch_points == 0 {
        return Err(Error::InvalidConfig("max_batch_points must be positive"));
    }
    if config.max_batch_points > u32::MAX as usize {
        return Err(Error::InvalidConfig(
            "max_batch_points exceeds the on-disk u32 count",
        ));
    }
    if config.max_transaction_bytes < TRANSACTION_HEADER_BYTES + RECORD_HEADER_BYTES {
        return Err(Error::InvalidConfig(
            "max_transaction_bytes is too small for one record",
        ));
    }
    if config.max_transaction_bytes > u32::MAX as usize {
        return Err(Error::InvalidConfig(
            "max_transaction_bytes exceeds the on-disk u32 length",
        ));
    }
    if matches!(config.durability, Durability::EveryBytes(0)) {
        return Err(Error::InvalidConfig(
            "EveryBytes durability threshold must be positive",
        ));
    }
    Ok(())
}

fn insert_indexed_point(index: &mut HashMap<u64, Vec<Point>>, point: Point) {
    let series = index.entry(point.series_id).or_default();
    match series.last() {
        Some(last) if last.valid_time > point.valid_time => {
            let at = series.partition_point(|existing| existing.valid_time <= point.valid_time);
            series.insert(at, point);
        }
        _ => series.push(point),
    }
}

fn series_time_slice(points: &[Point], start: i64, end: i64) -> &[Point] {
    let lo = points.partition_point(|point| point.valid_time < start);
    let hi = lo + points[lo..].partition_point(|point| point.valid_time < end);
    &points[lo..hi]
}

fn winning_revisions(
    points: &[Point],
    start: i64,
    end: i64,
    keep: impl Fn(&Point) -> bool,
    prefer: impl Fn(&Point, &Point) -> bool,
) -> Vec<Point> {
    let mut winners: Vec<Point> = Vec::new();
    for point in series_time_slice(points, start, end) {
        if !keep(point) {
            continue;
        }
        match winners.last_mut() {
            Some(current) if current.valid_time == point.valid_time => {
                if prefer(point, current) {
                    *current = *point;
                }
            }
            _ => winners.push(*point),
        }
    }
    winners
}

fn compact_log_path(active: &Path) -> Result<PathBuf> {
    let parent = parent_directory(active);
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    Ok(parent.join(format!(
        ".active.wlog.reclaim-{}-{nonce}",
        std::process::id()
    )))
}

fn write_compact_log(
    path: &Path,
    catalog: &Catalog,
    identity_index: &CompactIdentityIndex,
    live_index: &HashMap<u64, Vec<Point>>,
    max_batch_points: usize,
    max_transaction_bytes: usize,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    write_database_header(&mut file)?;

    let catalog_records = catalog.snapshot_records()?;
    if !catalog_records.is_empty() {
        let mut transaction = Transaction::new();
        transaction.records = catalog_records;
        write_standalone_transaction(&mut file, &transaction, max_transaction_bytes)?;
    }

    if !identity_index.identified.is_empty() || !identity_index.ingress.is_empty() {
        write_identity_index_frames(&mut file, identity_index, max_transaction_bytes)?;
    }

    let tail: Vec<Point> = live_index.values().flatten().copied().collect();
    if tail.len() > max_batch_points {
        return Err(Error::BatchTooLarge {
            points: tail.len(),
            maximum: max_batch_points,
        });
    }
    if !tail.is_empty() {
        let mut transaction = Transaction::new();
        transaction.records.push(Record::Points(tail));
        write_standalone_transaction(&mut file, &transaction, max_transaction_bytes)?;
    }

    file.sync_all()?;
    Ok(())
}

fn identity_index_frame_limit(max_transaction_bytes: usize) -> usize {
    max_transaction_bytes
        .saturating_add(IDENTITY_INDEX_ENTRY_OVERHEAD_BYTES)
        .min(u32::MAX as usize)
}

fn write_identity_index_frames(
    file: &mut File,
    index: &CompactIdentityIndex,
    max_transaction_bytes: usize,
) -> Result<()> {
    let limit = identity_index_frame_limit(max_transaction_bytes);
    let mut chunk = CompactIdentityIndex::default();
    let mut estimated_bytes = 0_usize;

    for receipt in &index.identified {
        let singleton = CompactIdentityIndex {
            identified: vec![receipt.clone()],
            ingress: Vec::new(),
        };
        let bytes = encoded_identity_index(&singleton)?.len();
        if bytes > limit {
            return Err(Error::Serialization(
                "one identified receipt exceeds the identity-index frame limit".to_owned(),
            ));
        }
        if estimated_bytes > 0 && estimated_bytes.saturating_add(bytes) > limit {
            write_identity_index_frame(file, &chunk, limit)?;
            chunk = CompactIdentityIndex::default();
            estimated_bytes = 0;
        }
        chunk.identified.push(receipt.clone());
        estimated_bytes = estimated_bytes.saturating_add(bytes);
    }
    for receipt in &index.ingress {
        let singleton = CompactIdentityIndex {
            identified: Vec::new(),
            ingress: vec![receipt.clone()],
        };
        let bytes = encoded_identity_index(&singleton)?.len();
        if bytes > limit {
            return Err(Error::Serialization(
                "one ingress receipt exceeds the identity-index frame limit".to_owned(),
            ));
        }
        if estimated_bytes > 0 && estimated_bytes.saturating_add(bytes) > limit {
            write_identity_index_frame(file, &chunk, limit)?;
            chunk = CompactIdentityIndex::default();
            estimated_bytes = 0;
        }
        chunk.ingress.push(receipt.clone());
        estimated_bytes = estimated_bytes.saturating_add(bytes);
    }
    if !chunk.identified.is_empty() || !chunk.ingress.is_empty() {
        write_identity_index_frame(file, &chunk, limit)?;
    }
    Ok(())
}

fn encoded_identity_index(index: &CompactIdentityIndex) -> Result<Vec<u8>> {
    let encoded = postcard::to_stdvec(index)
        .map_err(|error| Error::Serialization(format!("identity index encode failed: {error}")))?;
    let mut payload = Vec::with_capacity(IDENTITY_INDEX_MAGIC_V2.len() + encoded.len());
    payload.extend_from_slice(IDENTITY_INDEX_MAGIC_V2);
    payload.extend_from_slice(&encoded);
    Ok(payload)
}

fn write_identity_index_frame(
    file: &mut File,
    index: &CompactIdentityIndex,
    limit: usize,
) -> Result<()> {
    let payload = encoded_identity_index(index)?;
    if payload.len() > limit {
        return Err(Error::Serialization(
            "identity index chunk exceeds its frame limit".to_owned(),
        ));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| Error::Serialization("identity index exceeds u32 length".to_owned()))?;
    let header = encode_frame_header(FRAME_KIND_IDENTITY_INDEX, 0, payload_len, hash(&payload));
    file.write_all(&header)?;
    file.write_all(&payload)?;
    Ok(())
}

fn write_standalone_transaction(
    file: &mut File,
    transaction: &Transaction,
    max_transaction_bytes: usize,
) -> Result<()> {
    let payload = encode_transaction(transaction)?;
    if payload.len() > max_transaction_bytes {
        return Err(Error::InvalidModel(format!(
            "transaction has {} encoded bytes; maximum is {max_transaction_bytes}",
            payload.len()
        )));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| Error::Serialization("transaction exceeds u32 length".to_owned()))?;
    let record_count = u32::try_from(transaction.record_count())
        .map_err(|_| Error::Serialization("too many transaction records".to_owned()))?;
    let header = encode_frame_header(
        FRAME_KIND_TRANSACTION,
        record_count,
        payload_len,
        hash(&payload),
    );
    file.write_all(&header)?;
    file.write_all(&payload)?;
    Ok(())
}

fn apply_identity_index(
    payload: &[u8],
    offset: u64,
    commit_ids: &mut HashSet<u128>,
    identified_receipts: &mut HashMap<u128, StoredIdentifiedReceipt>,
    ingress_receipts: &mut HashMap<IngressKey, StoredIngressReceipt>,
    ingress_commit_ids: &mut HashMap<u128, IngressKey>,
    ingress_last_sequences: &mut HashMap<u128, u64>,
) -> Result<()> {
    let index = decode_identity_index(payload, offset)?;
    for receipt in index.identified {
        if receipt.payload_len < (COMMIT_ID_BYTES + TRANSACTION_HEADER_BYTES) as u32 {
            return corruption(offset, "identified receipt payload is too short");
        }
        let points = usize::try_from(receipt.points).map_err(|_| Error::Corruption {
            offset,
            reason: "identity index point count overflows usize".to_owned(),
        })?;
        let records = usize::try_from(receipt.records).map_err(|_| Error::Corruption {
            offset,
            reason: "identity index record count overflows usize".to_owned(),
        })?;
        let compact_payload = if receipt.payload.is_empty() {
            None
        } else {
            if receipt.payload.len() != receipt.payload_len as usize
                || hash(&receipt.payload) != receipt.payload_crc32
                || receipt.payload.len() < COMMIT_ID_BYTES
                || u128::from_le_bytes(receipt.payload[..COMMIT_ID_BYTES].try_into().unwrap())
                    != receipt.commit_id
            {
                return corruption(offset, "identified receipt bytes do not match their index");
            }
            validate_compact_transaction(
                &receipt.payload[COMMIT_ID_BYTES..],
                records,
                points,
                offset,
            )?;
            Some(Arc::<[u8]>::from(receipt.payload))
        };
        if !commit_ids.insert(receipt.commit_id) {
            return corruption(offset, "duplicate commit identifier");
        }
        identified_receipts.insert(
            receipt.commit_id,
            StoredIdentifiedReceipt {
                payload_offset: 0,
                payload_len: receipt.payload_len,
                payload_crc32: receipt.payload_crc32,
                compact_payload,
                commit: Commit {
                    frame_offset: offset,
                    points,
                    records,
                    bytes_written: 0,
                    durable: false,
                    deduplicated: false,
                },
            },
        );
    }
    for receipt in index.ingress {
        if receipt.canonical_payload_len < TRANSACTION_HEADER_BYTES as u32 {
            return corruption(offset, "ingress receipt payload is too short");
        }
        let points = usize::try_from(receipt.points).map_err(|_| Error::Corruption {
            offset,
            reason: "identity index point count overflows usize".to_owned(),
        })?;
        let records = usize::try_from(receipt.records).map_err(|_| Error::Corruption {
            offset,
            reason: "identity index record count overflows usize".to_owned(),
        })?;
        let compact_payload = if receipt.canonical_payload.is_empty() {
            None
        } else {
            if receipt.canonical_payload.len() != receipt.canonical_payload_len as usize
                || hash(&receipt.canonical_payload) != receipt.canonical_payload_crc32
            {
                return corruption(offset, "ingress receipt bytes do not match their index");
            }
            validate_compact_transaction(&receipt.canonical_payload, records, points, offset)?;
            Some(Arc::<[u8]>::from(receipt.canonical_payload))
        };
        let identity = IngressIdentity {
            source_id: receipt.source_id,
            sequence: receipt.sequence,
            commit_id: receipt.commit_id,
        };
        if identity.source_id == 0 {
            return corruption(offset, "ingress source id zero is reserved");
        }
        let key = IngressKey::from(identity);
        if ingress_receipts.contains_key(&key) {
            return corruption(offset, "duplicate ingress source sequence");
        }
        if !commit_ids.insert(identity.commit_id) {
            return corruption(offset, "duplicate commit identifier");
        }
        if let Some(last) = ingress_last_sequences.get(&identity.source_id).copied()
            && identity.sequence <= last
        {
            return corruption(offset, "ingress source cursor is not strictly increasing");
        }
        ingress_receipts.insert(
            key,
            StoredIngressReceipt {
                identity,
                canonical_payload_offset: 0,
                canonical_payload_len: receipt.canonical_payload_len,
                canonical_payload_crc32: receipt.canonical_payload_crc32,
                compact_payload,
                commit: Commit {
                    frame_offset: receipt.frame_offset,
                    points,
                    records,
                    bytes_written: receipt.bytes_written,
                    durable: false,
                    deduplicated: false,
                },
            },
        );
        ingress_commit_ids.insert(identity.commit_id, key);
        ingress_last_sequences.insert(identity.source_id, identity.sequence);
    }
    Ok(())
}

fn decode_identity_index(payload: &[u8], offset: u64) -> Result<CompactIdentityIndex> {
    if let Some(encoded) = payload.strip_prefix(IDENTITY_INDEX_MAGIC_V2) {
        return postcard::from_bytes(encoded).map_err(|error| Error::Corruption {
            offset,
            reason: format!("identity index v2 decode failed: {error}"),
        });
    }
    postcard::from_bytes::<LegacyCompactIdentityIndex>(payload)
        .map(Into::into)
        .map_err(|error| Error::Corruption {
            offset,
            reason: format!("legacy identity index decode failed: {error}"),
        })
}

fn validate_compact_transaction(
    payload: &[u8],
    expected_records: usize,
    expected_points: usize,
    offset: u64,
) -> Result<()> {
    let records = decode_transaction(payload, expected_records, offset)?;
    let points = records
        .iter()
        .map(|record| match record {
            Record::Points(points) => points.len(),
            _ => 0,
        })
        .try_fold(0_usize, usize::checked_add)
        .ok_or_else(|| Error::Corruption {
            offset,
            reason: "identity index point count overflows".to_owned(),
        })?;
    if points != expected_points {
        return corruption(offset, "identity index point count does not match payload");
    }
    for record in &records {
        if let Record::Points(points) = record
            && crate::catalog::validate_point_intervals(points).is_err()
        {
            return corruption(offset, "identity index contains invalid point values");
        }
    }
    Ok(())
}

fn write_database_header(file: &mut File) -> Result<()> {
    let mut header = [0_u8; DATABASE_HEADER_BYTES];
    header[..8].copy_from_slice(DATABASE_MAGIC);
    header[8..10].copy_from_slice(&DATABASE_VERSION.to_le_bytes());
    // Bytes 10..12 are reserved format flags.
    let checksum = hash(&header[..12]);
    header[12..16].copy_from_slice(&checksum.to_le_bytes());
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    file.sync_all()?;
    Ok(())
}

struct Scan {
    index: HashMap<u64, Vec<Point>>,
    catalog: Catalog,
    commit_ids: HashSet<u128>,
    identified_receipts: HashMap<u128, StoredIdentifiedReceipt>,
    ingress_receipts: HashMap<IngressKey, StoredIngressReceipt>,
    ingress_commit_ids: HashMap<u128, IngressKey>,
    ingress_last_sequences: HashMap<u128, u64>,
    commits: u64,
    points: u64,
    catalog_records: u64,
    recovered_tail_bytes: u64,
    recovered_tail: RecoveredTail,
    validated_bytes: u64,
    salvage_stop_reason: Option<SalvageStopReason>,
    pending_reclaim: bool,
}

enum ScanMode {
    Recover { simulate: bool },
    Salvage,
}

/// Replays the log, recovering from a torn tail. With `simulate_recovery` the
/// torn tail is skipped and accounted exactly as if it had been truncated, but
/// the file is never written; without it the tail is physically removed.
fn scan_and_recover(
    file: &mut File,
    max_batch_points: usize,
    max_transaction_bytes: usize,
    simulate_recovery: bool,
    published_seals: &HashSet<u64>,
) -> Result<Scan> {
    scan_log(
        file,
        max_batch_points,
        max_transaction_bytes,
        ScanMode::Recover {
            simulate: simulate_recovery,
        },
        published_seals,
    )
}

fn scan_log(
    file: &mut File,
    max_batch_points: usize,
    max_transaction_bytes: usize,
    mode: ScanMode,
    published_seals: &HashSet<u64>,
) -> Result<Scan> {
    let original_len = file.metadata()?.len();
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut database_header = [0_u8; DATABASE_HEADER_BYTES];
    reader.read_exact(&mut database_header)?;
    if &database_header[..8] != DATABASE_MAGIC {
        return Err(Error::InvalidHeader);
    }
    let version = u16::from_le_bytes(database_header[8..10].try_into().unwrap());
    if version != DATABASE_VERSION {
        return Err(Error::UnsupportedVersion(version));
    }
    let expected_checksum = u32::from_le_bytes(database_header[12..16].try_into().unwrap());
    if hash(&database_header[..12]) != expected_checksum || database_header[10..12] != [0, 0] {
        return Err(Error::InvalidHeader);
    }

    let mut offset = DATABASE_HEADER_BYTES as u64;
    let mut index = HashMap::<u64, Vec<Point>>::new();
    let mut catalog = Catalog::default();
    let mut commit_ids = HashSet::<u128>::new();
    let mut identified_receipts = HashMap::<u128, StoredIdentifiedReceipt>::new();
    let mut ingress_receipts = HashMap::<IngressKey, StoredIngressReceipt>::new();
    let mut ingress_commit_ids = HashMap::<u128, IngressKey>::new();
    let mut ingress_last_sequences = HashMap::<u128, u64>::new();
    let mut commits = 0_u64;
    let mut points = 0_u64;
    let mut catalog_records = 0_u64;
    let mut recovered_tail_bytes = 0_u64;
    let mut recovered_tail = RecoveredTail::None;
    let mut salvage_stop_reason = None;
    let mut pending_reclaim = false;
    let mut payload = Vec::new();

    macro_rules! stop_or_corruption {
        ($reason:expr, $message:expr) => {
            if matches!(mode, ScanMode::Salvage) {
                salvage_stop_reason = Some($reason);
                break;
            }
            return corruption(offset, $message);
        };
    }

    while offset < original_len {
        let remaining = original_len - offset;
        if remaining < FRAME_HEADER_BYTES as u64 {
            if let ScanMode::Recover { simulate } = mode {
                recovered_tail_bytes = remaining;
                recovered_tail = RecoveredTail::IncompleteHeader;
                if !simulate {
                    reader.seek(SeekFrom::Start(offset))?;
                    truncate_recovered_tail(reader.get_mut(), offset)?;
                }
            } else {
                salvage_stop_reason = Some(SalvageStopReason::IncompleteFrameHeader);
            }
            break;
        }

        let mut frame_header = [0_u8; FRAME_HEADER_BYTES];
        reader.read_exact(&mut frame_header)?;
        if &frame_header[..4] != FRAME_MAGIC {
            stop_or_corruption!(SalvageStopReason::InvalidFrameMagic, "invalid frame magic");
        }
        let frame_version = u16::from_le_bytes(frame_header[4..6].try_into().unwrap());
        if frame_version != FRAME_VERSION {
            stop_or_corruption!(
                SalvageStopReason::UnsupportedFrameVersion,
                "unsupported frame version"
            );
        }
        let header_checksum = u32::from_le_bytes(frame_header[20..24].try_into().unwrap());
        if hash(&frame_header[..20]) != header_checksum {
            stop_or_corruption!(
                SalvageStopReason::FrameHeaderChecksumMismatch,
                "frame header checksum mismatch"
            );
        }

        let frame_kind = u16::from_le_bytes(frame_header[6..8].try_into().unwrap());
        let item_count = u32::from_le_bytes(frame_header[8..12].try_into().unwrap()) as usize;
        let payload_len = u32::from_le_bytes(frame_header[12..16].try_into().unwrap()) as usize;
        let payload_checksum = u32::from_le_bytes(frame_header[16..20].try_into().unwrap());
        if frame_kind == FRAME_KIND_LEGACY_POINTS {
            let Some(expected_payload_len) = item_count.checked_mul(POINT_BYTES) else {
                stop_or_corruption!(
                    SalvageStopReason::InvalidLegacyFrameSize,
                    "frame point count overflows"
                );
            };
            if item_count > max_batch_points || payload_len != expected_payload_len {
                stop_or_corruption!(
                    SalvageStopReason::InvalidLegacyFrameSize,
                    "invalid legacy point frame size"
                );
            }
        } else if frame_kind == FRAME_KIND_TRANSACTION
            || frame_kind == FRAME_KIND_IDENTIFIED_TRANSACTION
            || frame_kind == FRAME_KIND_INGRESS_TRANSACTION
        {
            if payload_len > max_transaction_bytes {
                stop_or_corruption!(
                    SalvageStopReason::TransactionFrameTooLarge,
                    "transaction frame exceeds configured maximum"
                );
            }
            if frame_kind == FRAME_KIND_IDENTIFIED_TRANSACTION && payload_len < COMMIT_ID_BYTES {
                stop_or_corruption!(
                    SalvageStopReason::IdentifiedTransactionTooShort,
                    "identified transaction frame is too short"
                );
            }
            if frame_kind == FRAME_KIND_INGRESS_TRANSACTION && payload_len < INGRESS_IDENTITY_BYTES
            {
                stop_or_corruption!(
                    SalvageStopReason::IngressTransactionTooShort,
                    "ingress transaction frame is too short"
                );
            }
        } else if frame_kind == FRAME_KIND_SEAL_CHECKPOINT {
            if item_count != 0 || payload_len != SEAL_CHECKPOINT_BYTES {
                stop_or_corruption!(
                    SalvageStopReason::SealCheckpointInvalid,
                    "seal checkpoint header has an item count or wrong payload length"
                );
            }
        } else if frame_kind == FRAME_KIND_IDENTITY_INDEX {
            if item_count != 0 {
                stop_or_corruption!(
                    SalvageStopReason::IdentityIndexInvalid,
                    "identity index header has an item count"
                );
            }
            if payload_len > identity_index_frame_limit(max_transaction_bytes) {
                stop_or_corruption!(
                    SalvageStopReason::TransactionFrameTooLarge,
                    "identity index frame exceeds configured maximum"
                );
            }
        } else {
            stop_or_corruption!(SalvageStopReason::UnknownFrameKind, "unknown frame kind");
        }

        let frame_len = FRAME_HEADER_BYTES as u64 + payload_len as u64;
        if remaining < frame_len {
            if let ScanMode::Recover { simulate } = mode {
                recovered_tail_bytes = remaining;
                recovered_tail = RecoveredTail::IncompletePayload;
                if !simulate {
                    reader.seek(SeekFrom::Start(offset))?;
                    truncate_recovered_tail(reader.get_mut(), offset)?;
                }
            } else {
                salvage_stop_reason = Some(SalvageStopReason::IncompleteFramePayload);
            }
            break;
        }

        payload.clear();
        payload.resize(payload_len, 0);
        reader.read_exact(&mut payload)?;
        if hash(&payload) != payload_checksum {
            let reason = if remaining == frame_len {
                "payload checksum mismatch in complete final frame"
            } else {
                "payload checksum mismatch before valid tail"
            };
            stop_or_corruption!(SalvageStopReason::PayloadChecksumMismatch, reason);
        }

        if frame_kind == FRAME_KIND_LEGACY_POINTS {
            let recovered: Vec<_> = payload
                .chunks_exact(POINT_BYTES)
                .map(decode_point)
                .collect();
            if crate::catalog::validate_point_intervals(&recovered).is_err() {
                stop_or_corruption!(
                    SalvageStopReason::InvalidLegacyPoint,
                    "legacy point frame violates point invariants"
                );
            }
            for point in recovered {
                index.entry(point.series_id).or_default().push(point);
            }
            points += item_count as u64;
        } else if frame_kind == FRAME_KIND_SEAL_CHECKPOINT {
            let generation = u64::from_le_bytes(payload[..8].try_into().unwrap());
            let sealed_points = u64::from_le_bytes(payload[8..16].try_into().unwrap());
            if generation == 0 || sealed_points != points {
                stop_or_corruption!(
                    SalvageStopReason::SealCheckpointInvalid,
                    "seal checkpoint generation or point count is invalid"
                );
            }
            if published_seals.contains(&generation) {
                index.clear();
                points = 0;
                pending_reclaim = true;
            }
        } else if frame_kind == FRAME_KIND_IDENTITY_INDEX {
            match apply_identity_index(
                &payload,
                offset,
                &mut commit_ids,
                &mut identified_receipts,
                &mut ingress_receipts,
                &mut ingress_commit_ids,
                &mut ingress_last_sequences,
            ) {
                Ok(()) => {}
                Err(error) if matches!(mode, ScanMode::Salvage) => {
                    let _ = error;
                    salvage_stop_reason = Some(SalvageStopReason::IdentityIndexInvalid);
                    break;
                }
                Err(error) => return Err(error),
            }
        } else {
            let mut ingress_identity = None;
            let mut identified_commit_id = None;
            let transaction_payload = if frame_kind == FRAME_KIND_IDENTIFIED_TRANSACTION {
                let commit_id = u128::from_le_bytes(payload[..COMMIT_ID_BYTES].try_into().unwrap());
                // The writer refuses to append a frame whose identifier is
                // already in the log, so a duplicate here can only come from
                // tampering (say, concatenated logs); failing closed keeps
                // the exactly-once promise instead of silently replaying.
                if !commit_ids.insert(commit_id) {
                    stop_or_corruption!(
                        SalvageStopReason::DuplicateCommitId,
                        "duplicate commit identifier"
                    );
                }
                identified_commit_id = Some(commit_id);
                &payload[COMMIT_ID_BYTES..]
            } else if frame_kind == FRAME_KIND_INGRESS_TRANSACTION {
                let identity = IngressIdentity {
                    source_id: u128::from_le_bytes(payload[..16].try_into().unwrap()),
                    sequence: u64::from_le_bytes(payload[16..24].try_into().unwrap()),
                    commit_id: u128::from_le_bytes(payload[24..40].try_into().unwrap()),
                };
                if identity.source_id == 0 {
                    stop_or_corruption!(
                        SalvageStopReason::InvalidIngressSequence,
                        "ingress source id zero is reserved"
                    );
                }
                let key = IngressKey::from(identity);
                if ingress_receipts.contains_key(&key) {
                    stop_or_corruption!(
                        SalvageStopReason::DuplicateIngressSequence,
                        "duplicate ingress source sequence"
                    );
                }
                if !commit_ids.insert(identity.commit_id) {
                    stop_or_corruption!(
                        SalvageStopReason::DuplicateCommitId,
                        "duplicate commit identifier"
                    );
                }
                if let Some(last) = ingress_last_sequences.get(&identity.source_id).copied()
                    && identity.sequence <= last
                {
                    stop_or_corruption!(
                        SalvageStopReason::InvalidIngressSequence,
                        "ingress source cursor is not strictly increasing"
                    );
                }
                ingress_identity = Some(identity);
                &payload[INGRESS_IDENTITY_BYTES..]
            } else {
                &payload[..]
            };
            let records = match decode_transaction(transaction_payload, item_count, offset) {
                Ok(records) => records,
                Err(error) if matches!(mode, ScanMode::Salvage) => {
                    let _ = error;
                    salvage_stop_reason = Some(SalvageStopReason::InvalidTransaction);
                    break;
                }
                Err(error) => return Err(error),
            };
            if let Err(error) = catalog.apply_recovered(&records, offset) {
                if matches!(mode, ScanMode::Salvage) {
                    let _ = error;
                    salvage_stop_reason = Some(SalvageStopReason::InvalidCatalogTransaction);
                    break;
                }
                return Err(error);
            }
            let recovered_point_count: usize = records
                .iter()
                .map(|record| match record {
                    Record::Points(recovered_points) => recovered_points.len(),
                    _ => 0,
                })
                .sum();
            if recovered_point_count > max_batch_points {
                stop_or_corruption!(
                    SalvageStopReason::TransactionPointCountTooLarge,
                    "transaction point count exceeds maximum"
                );
            }
            for record in &records {
                match record {
                    Record::Points(recovered_points) => {
                        points += recovered_points.len() as u64;
                        for point in recovered_points {
                            index.entry(point.series_id).or_default().push(*point);
                        }
                    }
                    _ => catalog_records += 1,
                }
            }
            if let Some(commit_id) = identified_commit_id {
                identified_receipts.insert(
                    commit_id,
                    StoredIdentifiedReceipt {
                        payload_offset: offset + FRAME_HEADER_BYTES as u64,
                        payload_len: u32::try_from(payload.len()).unwrap(),
                        payload_crc32: payload_checksum,
                        compact_payload: None,
                        commit: Commit {
                            frame_offset: offset,
                            points: recovered_point_count,
                            records: records.len(),
                            bytes_written: frame_len,
                            durable: false,
                            deduplicated: false,
                        },
                    },
                );
            }
            if let Some(identity) = ingress_identity {
                let key = IngressKey::from(identity);
                let receipt = StoredIngressReceipt {
                    identity,
                    canonical_payload_offset: offset
                        + FRAME_HEADER_BYTES as u64
                        + INGRESS_IDENTITY_BYTES as u64,
                    canonical_payload_len: u32::try_from(transaction_payload.len()).unwrap(),
                    canonical_payload_crc32: hash(transaction_payload),
                    compact_payload: None,
                    commit: Commit {
                        frame_offset: offset,
                        points: recovered_point_count,
                        records: records.len(),
                        bytes_written: frame_len,
                        // A scan proves framing and checksums, not persistence
                        // across power loss. A writable open syncs the full
                        // recovered prefix and upgrades these receipts before
                        // exposing the handle; a read-only open leaves them
                        // conservative.
                        durable: false,
                        deduplicated: false,
                    },
                };
                ingress_receipts.insert(key, receipt);
                ingress_commit_ids.insert(identity.commit_id, key);
                ingress_last_sequences.insert(identity.source_id, identity.sequence);
            }
        }
        commits += 1;
        offset += frame_len;
    }

    if matches!(mode, ScanMode::Salvage) && salvage_stop_reason.is_none() {
        salvage_stop_reason = Some(SalvageStopReason::CleanEof);
    }

    for series in index.values_mut() {
        series.sort_by_key(|point| point.valid_time);
    }

    Ok(Scan {
        index,
        catalog,
        commit_ids,
        identified_receipts,
        ingress_receipts,
        ingress_commit_ids,
        ingress_last_sequences,
        commits,
        points,
        catalog_records,
        recovered_tail_bytes,
        recovered_tail,
        validated_bytes: offset,
        salvage_stop_reason,
        pending_reclaim,
    })
}

pub(crate) struct SalvageSource {
    pub(crate) file: File,
    pub(crate) path: std::path::PathBuf,
    pub(crate) source_bytes: u64,
    pub(crate) recovered_prefix_bytes: u64,
    pub(crate) recovered_commits: u64,
    pub(crate) recovered_points: u64,
    pub(crate) stop_reason: SalvageStopReason,
    identity: ReadOnlyFileIdentity,
    root: File,
    root_path: std::path::PathBuf,
    root_identity: ReadOnlyDirectoryIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadOnlyFileIdentity {
    device: u64,
    inode: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadOnlyDirectoryIdentity {
    device: u64,
    inode: u64,
}

impl ReadOnlyDirectoryIdentity {
    fn read(metadata: &std::fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

impl ReadOnlyFileIdentity {
    fn read(metadata: &std::fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        }
    }
}

impl SalvageSource {
    pub(crate) fn open(
        root_path: &Path,
        file_name: &str,
        published_seals: &HashSet<u64>,
    ) -> Result<Self> {
        use rustix::fs::{Mode, OFlags, open, openat};

        let root_descriptor = open(
            root_path,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
            Mode::empty(),
        )
        .map_err(|error| Error::Io(error.into()))?;
        let root = File::from(root_descriptor);
        let root_metadata = root.metadata()?;
        let root_path_metadata = std::fs::symlink_metadata(root_path)?;
        let root_identity = ReadOnlyDirectoryIdentity::read(&root_metadata);
        if !root_path_metadata.file_type().is_dir()
            || ReadOnlyDirectoryIdentity::read(&root_path_metadata) != root_identity
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "salvage source is not a stable directory: {}",
                    root_path.display()
                ),
            )));
        }

        let path = root_path.join(file_name);
        let path_metadata = std::fs::symlink_metadata(&path)?;
        if !path_metadata.file_type().is_file() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("read-only path is not a regular file: {}", path.display()),
            )));
        }
        let descriptor = openat(
            &root,
            file_name,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|error| Error::Io(error.into()))?;
        let mut file = File::from(descriptor);
        let opened_metadata = file.metadata()?;
        if !opened_metadata.file_type().is_file()
            || opened_metadata.dev() != path_metadata.dev()
            || opened_metadata.ino() != path_metadata.ino()
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("read-only path changed while opening: {}", path.display()),
            )));
        }
        match file.try_lock_shared() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(Error::Locked {
                    path: path.to_owned(),
                });
            }
            Err(TryLockError::Error(error)) => return Err(Error::Io(error)),
        }
        let metadata = file.metadata()?;
        if metadata.len() < DATABASE_HEADER_BYTES as u64 {
            return Err(Error::InvalidHeader);
        }
        let scan = scan_log(
            &mut file,
            Config::default().max_batch_points,
            Config::default().max_transaction_bytes,
            ScanMode::Salvage,
            published_seals,
        )?;
        Ok(Self {
            file,
            path: path.to_owned(),
            source_bytes: metadata.len(),
            recovered_prefix_bytes: scan.validated_bytes,
            recovered_commits: scan.commits,
            recovered_points: scan.points,
            stop_reason: scan.salvage_stop_reason.unwrap(),
            identity: ReadOnlyFileIdentity::read(&metadata),
            root,
            root_path: root_path.to_owned(),
            root_identity,
        })
    }

    pub(crate) fn ensure_unchanged(&self) -> Result<()> {
        #[cfg(test)]
        let mutate = MUTATE_SALVAGE_SOURCE_AFTER_IDENTITY_CHECKS.with(|remaining| {
            let Some(checks) = remaining.get() else {
                return false;
            };
            if checks == 0 {
                remaining.set(None);
                true
            } else {
                remaining.set(Some(checks - 1));
                false
            }
        });
        #[cfg(test)]
        if mutate {
            let mut writer = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path)?;
            writer.seek(SeekFrom::End(-1))?;
            let mut byte = [0_u8; 1];
            writer.read_exact(&mut byte)?;
            writer.seek(SeekFrom::End(-1))?;
            writer.write_all(&[byte[0] ^ 0xff])?;
            writer.sync_all()?;
        }

        let descriptor_identity = ReadOnlyFileIdentity::read(&self.file.metadata()?);
        let path_metadata = std::fs::symlink_metadata(&self.path)?;
        let path_identity = ReadOnlyFileIdentity::read(&path_metadata);
        let root_descriptor_identity = ReadOnlyDirectoryIdentity::read(&self.root.metadata()?);
        let root_path_metadata = std::fs::symlink_metadata(&self.root_path)?;
        let root_path_identity = ReadOnlyDirectoryIdentity::read(&root_path_metadata);
        if !path_metadata.file_type().is_file()
            || descriptor_identity != self.identity
            || path_identity != self.identity
            || !root_path_metadata.file_type().is_dir()
            || root_descriptor_identity != self.root_identity
            || root_path_identity != self.root_identity
        {
            return Err(Error::SourceChanged {
                path: self.path.clone(),
            });
        }
        Ok(())
    }
}

/// Opens or creates one regular file without following a final symlink or
/// blocking on a FIFO or device. Existing paths use the same identity check
/// as [`open_regular_file_read_only`]; a missing path is created as `0600`.
fn open_regular_file_read_write(path: &Path) -> Result<File> {
    use rustix::fs::{Mode, OFlags, open};

    match std::fs::symlink_metadata(path) {
        Ok(path_metadata) => {
            if !path_metadata.file_type().is_file() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("writable path is not a regular file: {}", path.display()),
                )));
            }
            let descriptor = open(
                path,
                OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|error| Error::Io(error.into()))?;
            let file = File::from(descriptor);
            let opened_metadata = file.metadata()?;
            if !opened_metadata.file_type().is_file()
                || opened_metadata.dev() != path_metadata.dev()
                || opened_metadata.ino() != path_metadata.ino()
            {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("writable path changed while opening: {}", path.display()),
                )));
            }
            Ok(file)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let descriptor = open(
                path,
                OFlags::RDWR
                    | OFlags::CLOEXEC
                    | OFlags::NOFOLLOW
                    | OFlags::CREATE
                    | OFlags::NONBLOCK,
                Mode::RUSR | Mode::WUSR,
            )
            .map_err(|error| Error::Io(error.into()))?;
            let file = File::from(descriptor);
            if !file.metadata()?.file_type().is_file() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("writable path is not a regular file: {}", path.display()),
                )));
            }
            Ok(file)
        }
        Err(error) => Err(Error::Io(error)),
    }
}

/// Opens one existing regular file without following a final symlink or
/// blocking on a FIFO or device. The identity check closes the metadata/open
/// race for the final path component.
pub(crate) fn open_regular_file_read_only(path: &Path) -> Result<File> {
    use rustix::fs::{Mode, OFlags, open};

    let path_metadata = std::fs::symlink_metadata(path)?;
    if !path_metadata.file_type().is_file() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("read-only path is not a regular file: {}", path.display()),
        )));
    }
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(|error| Error::Io(error.into()))?;
    let file = File::from(descriptor);
    let opened_metadata = file.metadata()?;
    if !opened_metadata.file_type().is_file()
        || opened_metadata.dev() != path_metadata.dev()
        || opened_metadata.ino() != path_metadata.ino()
    {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("read-only path changed while opening: {}", path.display()),
        )));
    }
    Ok(file)
}

/// Makes a directory's entries durable after creating, linking, renaming, or
/// removing an entry. FTWDB v0.1 only builds on Unix because this guarantee
/// depends on opening and syncing the directory itself.
pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

/// Syncs the directory whose entry makes `path` durable. Only this one level
/// is synced, matching what segment and manifest publication guarantee.
pub(crate) fn sync_parent_directory(path: &Path) -> Result<()> {
    sync_directory(parent_directory(path))
}

/// The directory that holds `path`'s entry. A bare file name lives in the
/// current directory, which `Path::parent` reports as an empty path that
/// cannot be opened.
fn parent_directory(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

fn truncate_recovered_tail(file: &mut File, length: u64) -> Result<()> {
    file.set_len(length)?;
    file.sync_data()?;
    Ok(())
}

fn corruption<T>(offset: u64, reason: &str) -> Result<T> {
    Err(Error::Corruption {
        offset,
        reason: reason.to_owned(),
    })
}

fn encode_frame_header(
    frame_kind: u16,
    item_count: u32,
    payload_len: u32,
    payload_checksum: u32,
) -> [u8; 24] {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    header[..4].copy_from_slice(FRAME_MAGIC);
    header[4..6].copy_from_slice(&FRAME_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&frame_kind.to_le_bytes());
    header[8..12].copy_from_slice(&item_count.to_le_bytes());
    header[12..16].copy_from_slice(&payload_len.to_le_bytes());
    header[16..20].copy_from_slice(&payload_checksum.to_le_bytes());
    let header_checksum = hash(&header[..20]);
    header[20..24].copy_from_slice(&header_checksum.to_le_bytes());
    header
}

fn encode_transaction(transaction: &Transaction) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    // An identified transaction (frame kind 2) is the 16-byte little-endian
    // commit identifier followed by the unchanged kind-1 payload, keeping the
    // identifier in the same checksummed durable unit as the records.
    if let Some(commit_id) = transaction.commit_id {
        payload.extend_from_slice(&commit_id.to_le_bytes());
    }
    payload.extend_from_slice(&encode_canonical_transaction(transaction)?);
    Ok(payload)
}

fn encode_canonical_transaction(transaction: &Transaction) -> Result<Vec<u8>> {
    let record_count = u32::try_from(transaction.record_count())
        .map_err(|_| Error::Serialization("too many transaction records".to_owned()))?;
    let mut payload = Vec::new();
    payload.extend_from_slice(TRANSACTION_MAGIC);
    payload.extend_from_slice(&TRANSACTION_VERSION.to_le_bytes());
    payload.extend_from_slice(&0_u16.to_le_bytes());
    payload.extend_from_slice(&record_count.to_le_bytes());

    for record in &transaction.records {
        let (kind, body) = match record {
            Record::Entity(value) => (RECORD_ENTITY, serialize(value)?),
            Record::Relation(value) => (RECORD_RELATION, serialize(value)?),
            Record::Series(value) => (RECORD_SERIES, serialize(value)?),
            Record::Run(value) => (RECORD_RUN, serialize(value)?),
            Record::Plan(value) => (RECORD_PLAN, serialize(value)?),
            Record::Points(points) => {
                let count = u32::try_from(points.len())
                    .map_err(|_| Error::Serialization("too many points".to_owned()))?;
                let body_capacity = 4_usize
                    .checked_add(points.len().checked_mul(POINT_BYTES).ok_or_else(|| {
                        Error::Serialization("point record length overflows".to_owned())
                    })?)
                    .ok_or_else(|| {
                        Error::Serialization("point record length overflows".to_owned())
                    })?;
                let mut body = Vec::with_capacity(body_capacity);
                body.extend_from_slice(&count.to_le_bytes());
                for point in points {
                    encode_point(*point, &mut body);
                }
                (RECORD_POINTS, body)
            }
        };
        let body_len = u32::try_from(body.len())
            .map_err(|_| Error::Serialization("transaction record exceeds u32".to_owned()))?;
        payload.push(kind);
        payload.push(1); // record version
        payload.extend_from_slice(&0_u16.to_le_bytes());
        payload.extend_from_slice(&body_len.to_le_bytes());
        payload.extend_from_slice(&body);
    }
    Ok(payload)
}

fn decode_transaction(payload: &[u8], expected_records: usize, offset: u64) -> Result<Vec<Record>> {
    if payload.len() < TRANSACTION_HEADER_BYTES || &payload[..4] != TRANSACTION_MAGIC {
        return corruption(offset, "invalid transaction header");
    }
    let version = u16::from_le_bytes(payload[4..6].try_into().unwrap());
    if version != TRANSACTION_VERSION {
        return corruption(offset, "unsupported transaction version");
    }
    if payload[6..8] != [0, 0] {
        return corruption(offset, "transaction reserved flags are non-zero");
    }
    let record_count = u32::from_le_bytes(payload[8..12].try_into().unwrap()) as usize;
    if record_count != expected_records {
        return corruption(offset, "transaction record count mismatch");
    }
    if record_count > payload.len().saturating_sub(TRANSACTION_HEADER_BYTES) / RECORD_HEADER_BYTES {
        return corruption(offset, "transaction record count exceeds payload bounds");
    }

    let mut cursor = TRANSACTION_HEADER_BYTES;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        let header_end =
            cursor
                .checked_add(RECORD_HEADER_BYTES)
                .ok_or_else(|| Error::Corruption {
                    offset,
                    reason: "transaction cursor overflow".to_owned(),
                })?;
        if header_end > payload.len() {
            return corruption(offset, "truncated transaction record header");
        }
        let kind = payload[cursor];
        let record_version = payload[cursor + 1];
        if record_version != 1 {
            return corruption(offset, "unsupported transaction record version");
        }
        if payload[cursor + 2..cursor + 4] != [0, 0] {
            return corruption(offset, "transaction record reserved flags are non-zero");
        }
        let body_len =
            u32::from_le_bytes(payload[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
        let body_end = header_end
            .checked_add(body_len)
            .ok_or_else(|| Error::Corruption {
                offset,
                reason: "transaction record length overflow".to_owned(),
            })?;
        if body_end > payload.len() {
            return corruption(offset, "truncated transaction record body");
        }
        let body = &payload[header_end..body_end];
        let record = match kind {
            RECORD_ENTITY => Record::Entity(deserialize(body, offset)?),
            RECORD_RELATION => Record::Relation(deserialize(body, offset)?),
            RECORD_SERIES => Record::Series(deserialize(body, offset)?),
            RECORD_RUN => Record::Run(deserialize(body, offset)?),
            RECORD_PLAN => Record::Plan(deserialize(body, offset)?),
            RECORD_POINTS => Record::Points(decode_point_record(body, offset)?),
            _ => return corruption(offset, "unknown transaction record kind"),
        };
        records.push(record);
        cursor = body_end;
    }
    if cursor != payload.len() {
        return corruption(offset, "trailing transaction bytes");
    }
    Ok(records)
}

fn decode_point_record(body: &[u8], offset: u64) -> Result<Vec<Point>> {
    if body.len() < 4 {
        return corruption(offset, "truncated point record count");
    }
    let count = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
    let expected = 4_usize
        .checked_add(
            count
                .checked_mul(POINT_BYTES)
                .ok_or_else(|| Error::Corruption {
                    offset,
                    reason: "point record count overflows".to_owned(),
                })?,
        )
        .ok_or_else(|| Error::Corruption {
            offset,
            reason: "point record length overflows".to_owned(),
        })?;
    if body.len() != expected {
        return corruption(offset, "point record length mismatch");
    }
    Ok(body[4..]
        .chunks_exact(POINT_BYTES)
        .map(decode_point)
        .collect())
}

fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    postcard::to_stdvec(value).map_err(|error| Error::Serialization(error.to_string()))
}

fn deserialize<T>(body: &[u8], offset: u64) -> Result<T>
where
    T: for<'de> serde::Deserialize<'de>,
{
    postcard::from_bytes(body).map_err(|error| Error::Corruption {
        offset,
        reason: format!("invalid transaction record: {error}"),
    })
}

fn encode_point(point: Point, destination: &mut Vec<u8>) {
    destination.extend_from_slice(&point.series_id.to_le_bytes());
    destination.extend_from_slice(&point.valid_time.to_le_bytes());
    destination.extend_from_slice(&point.valid_time_end.to_le_bytes());
    destination.extend_from_slice(&point.knowledge_time.to_le_bytes());
    destination.extend_from_slice(&point.change_time.to_le_bytes());
    destination.extend_from_slice(&point.run_id.to_le_bytes());
    destination.extend_from_slice(&point.value.to_bits().to_le_bytes());
    destination.extend_from_slice(&point.quality.to_le_bytes());
    destination.extend_from_slice(&point.flags.to_le_bytes());
}

fn decode_point(raw: &[u8]) -> Point {
    Point {
        series_id: u64::from_le_bytes(raw[0..8].try_into().unwrap()),
        valid_time: i64::from_le_bytes(raw[8..16].try_into().unwrap()),
        valid_time_end: i64::from_le_bytes(raw[16..24].try_into().unwrap()),
        knowledge_time: i64::from_le_bytes(raw[24..32].try_into().unwrap()),
        change_time: i64::from_le_bytes(raw[32..40].try_into().unwrap()),
        run_id: u128::from_le_bytes(raw[40..56].try_into().unwrap()),
        value: f64::from_bits(u64::from_le_bytes(raw[56..64].try_into().unwrap())),
        quality: u32::from_le_bytes(raw[64..68].try_into().unwrap()),
        flags: u32::from_le_bytes(raw[68..72].try_into().unwrap()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Config, DATABASE_HEADER_BYTES, Database, Durability, FRAME_HEADER_BYTES,
        FRAME_KIND_IDENTIFIED_TRANSACTION, FRAME_KIND_TRANSACTION, FRAME_VERSION, Point,
        SalvageSource, SalvageStopReason, ScanMode, encode_frame_header, encode_transaction,
        fail_next_sync, scan_log,
    };
    use crate::transaction::IngressIdentity;
    use crate::{
        Entity, EntityId, Error, Plan, PlanStatus, RollupPolicy, Run, RunId, RunKind, RunStatus,
        SeriesDefinition, SeriesSemantics, Transaction,
    };
    use std::collections::{BTreeMap, HashSet};
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    fn point(valid_time: i64, knowledge_time: i64, change_time: i64, value: f64) -> Point {
        Point {
            series_id: 7,
            valid_time,
            valid_time_end: valid_time,
            knowledge_time,
            change_time,
            run_id: knowledge_time as u128 + 1,
            value,
            quality: 0,
            flags: 0,
        }
    }

    fn home() -> Entity {
        Entity {
            id: EntityId(1),
            kind: "home".to_owned(),
            name: "Home".to_owned(),
            parent: None,
            valid_from: 0,
            valid_to: None,
            properties: BTreeMap::new(),
        }
    }

    fn power_series() -> SeriesDefinition {
        SeriesDefinition {
            id: 7,
            owner_entity: Some(EntityId(1)),
            owner_relation: None,
            name: "battery_power".to_owned(),
            physical_quantity: "power".to_owned(),
            canonical_unit: "W".to_owned(),
            semantics: SeriesSemantics::Gauge,
            maximum_gap_micros: Some(10_000_000),
            rollup_policy: RollupPolicy {
                raw_retain_for_micros: None,
                tiers: Vec::new(),
            },
        }
    }

    fn header_only(path: &std::path::Path) {
        Database::open(path).unwrap().close().unwrap();
        assert_eq!(
            std::fs::metadata(path).unwrap().len(),
            DATABASE_HEADER_BYTES as u64
        );
    }

    fn append_raw_frame(path: &std::path::Path, kind: u16, items: u32, payload: &[u8]) -> u64 {
        let offset = std::fs::metadata(path).unwrap().len();
        let header = encode_frame_header(
            kind,
            items,
            u32::try_from(payload.len()).unwrap(),
            crc32fast::hash(payload),
        );
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(&header).unwrap();
        file.write_all(payload).unwrap();
        file.sync_all().unwrap();
        offset
    }

    fn legacy_log(path: &std::path::Path, frames: usize) -> Vec<u64> {
        let mut database = Database::open(path).unwrap();
        let mut offsets = Vec::new();
        for frame in 0..frames {
            offsets.push(database.stats().unwrap().file_bytes);
            database
                .append(&[point(
                    frame as i64,
                    frame as i64,
                    frame as i64,
                    frame as f64,
                )])
                .unwrap();
        }
        database.close().unwrap();
        offsets
    }

    fn rewrite_frame_header(
        path: &std::path::Path,
        offset: u64,
        rewrite: impl FnOnce(&mut [u8; FRAME_HEADER_BYTES]),
    ) {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut header = [0_u8; FRAME_HEADER_BYTES];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut header).unwrap();
        rewrite(&mut header);
        let checksum = crc32fast::hash(&header[..20]);
        header[20..24].copy_from_slice(&checksum.to_le_bytes());
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&header).unwrap();
        file.sync_all().unwrap();
    }

    #[test]
    fn newly_created_database_file_is_owner_only() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("private.ftwdb");
        Database::open(&path).unwrap().close().unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn rejects_non_zero_reserved_database_transaction_and_record_flags() {
        let directory = tempdir().unwrap();

        let database_path = directory.path().join("database-flags.ftwdb");
        header_only(&database_path);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)
            .unwrap();
        let mut header = [0_u8; DATABASE_HEADER_BYTES];
        file.read_exact(&mut header).unwrap();
        header[10] = 1;
        let checksum = crc32fast::hash(&header[..12]);
        header[12..16].copy_from_slice(&checksum.to_le_bytes());
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&header).unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert!(matches!(
            Database::open(&database_path),
            Err(Error::InvalidHeader)
        ));

        let transaction_path = directory.path().join("transaction-flags.ftwdb");
        header_only(&transaction_path);
        let mut payload = encode_transaction(&Transaction::new()).unwrap();
        payload[6] = 1;
        append_raw_frame(&transaction_path, FRAME_KIND_TRANSACTION, 0, &payload);
        assert_eq!(
            salvage_scan(&transaction_path, Config::default().max_batch_points).salvage_stop_reason,
            Some(SalvageStopReason::InvalidTransaction)
        );

        let record_path = directory.path().join("record-flags.ftwdb");
        header_only(&record_path);
        let mut transaction = Transaction::new();
        transaction.upsert_entity(home());
        let mut payload = encode_transaction(&transaction).unwrap();
        payload[super::TRANSACTION_HEADER_BYTES + 2] = 1;
        append_raw_frame(&record_path, FRAME_KIND_TRANSACTION, 1, &payload);
        assert_eq!(
            salvage_scan(&record_path, Config::default().max_batch_points).salvage_stop_reason,
            Some(SalvageStopReason::InvalidTransaction)
        );
    }

    #[test]
    fn salvage_rejects_invalid_legacy_points_and_control_frame_counts() {
        let directory = tempdir().unwrap();

        let legacy_path = directory.path().join("invalid-legacy-point.ftwdb");
        header_only(&legacy_path);
        let mut payload = Vec::new();
        super::encode_point(Point::actual(1, 1, f64::INFINITY), &mut payload);
        append_raw_frame(&legacy_path, super::FRAME_KIND_LEGACY_POINTS, 1, &payload);
        assert_eq!(
            salvage_scan(&legacy_path, Config::default().max_batch_points).salvage_stop_reason,
            Some(SalvageStopReason::InvalidLegacyPoint)
        );

        let checkpoint_path = directory.path().join("invalid-checkpoint.ftwdb");
        header_only(&checkpoint_path);
        let mut checkpoint = [0_u8; super::SEAL_CHECKPOINT_BYTES];
        checkpoint[..8].copy_from_slice(&1_u64.to_le_bytes());
        append_raw_frame(
            &checkpoint_path,
            super::FRAME_KIND_SEAL_CHECKPOINT,
            1,
            &checkpoint,
        );
        assert_eq!(
            salvage_scan(&checkpoint_path, Config::default().max_batch_points).salvage_stop_reason,
            Some(SalvageStopReason::SealCheckpointInvalid)
        );

        let checkpoint_points_path = directory.path().join("invalid-checkpoint-points.ftwdb");
        header_only(&checkpoint_points_path);
        checkpoint[8..16].copy_from_slice(&1_u64.to_le_bytes());
        append_raw_frame(
            &checkpoint_points_path,
            super::FRAME_KIND_SEAL_CHECKPOINT,
            0,
            &checkpoint,
        );
        assert_eq!(
            salvage_scan(&checkpoint_points_path, Config::default().max_batch_points,)
                .salvage_stop_reason,
            Some(SalvageStopReason::SealCheckpointInvalid)
        );

        let index_path = directory.path().join("invalid-index-count.ftwdb");
        header_only(&index_path);
        let index = postcard::to_stdvec(&super::CompactIdentityIndex::default()).unwrap();
        append_raw_frame(&index_path, super::FRAME_KIND_IDENTITY_INDEX, 1, &index);
        assert_eq!(
            salvage_scan(&index_path, Config::default().max_batch_points).salvage_stop_reason,
            Some(SalvageStopReason::IdentityIndexInvalid)
        );
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn config_rejects_limits_that_the_file_format_cannot_encode() {
        let directory = tempdir().unwrap();
        for config in [
            Config {
                max_batch_points: u32::MAX as usize + 1,
                ..Config::default()
            },
            Config {
                max_transaction_bytes: u32::MAX as usize + 1,
                ..Config::default()
            },
        ] {
            assert!(matches!(
                Database::open_with(directory.path().join("limit.ftwdb"), config),
                Err(Error::InvalidConfig(_))
            ));
        }
    }

    fn salvage_scan(path: &std::path::Path, max_batch_points: usize) -> super::Scan {
        let mut file = OpenOptions::new().read(true).open(path).unwrap();
        scan_log(
            &mut file,
            max_batch_points,
            Config::default().max_transaction_bytes,
            ScanMode::Salvage,
            &HashSet::new(),
        )
        .unwrap()
    }

    #[test]
    fn salvage_stops_before_the_first_bad_frame_and_never_resynchronizes() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("bad-middle-frame.ftwdb");
        let offsets = legacy_log(&path, 3);
        let source_bytes = std::fs::metadata(&path).unwrap().len();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(offsets[1] + FRAME_HEADER_BYTES as u64))
            .unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x80;
        file.seek(SeekFrom::Start(offsets[1] + FRAME_HEADER_BYTES as u64))
            .unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();

        let scan = salvage_scan(&path, Config::default().max_batch_points);
        assert_eq!(
            scan.salvage_stop_reason,
            Some(SalvageStopReason::PayloadChecksumMismatch)
        );
        assert_eq!(scan.validated_bytes, offsets[1]);
        assert_eq!(scan.commits, 1);
        assert_eq!(scan.points, 1);
        assert_eq!(
            source_bytes - scan.validated_bytes,
            source_bytes - offsets[1]
        );
    }

    #[test]
    fn salvage_classifies_frame_headers_bounds_and_short_ends_as_partial() {
        let directory = tempdir().unwrap();

        let complete_bad_crc = directory.path().join("complete-bad-crc.ftwdb");
        let offsets = legacy_log(&complete_bad_crc, 2);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&complete_bad_crc)
            .unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 1;
        file.seek(SeekFrom::End(-1)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
        let scan = salvage_scan(&complete_bad_crc, Config::default().max_batch_points);
        assert_eq!(
            scan.salvage_stop_reason,
            Some(SalvageStopReason::PayloadChecksumMismatch)
        );
        assert_eq!(scan.validated_bytes, offsets[1]);
        assert_eq!((scan.commits, scan.points), (1, 1));

        let short_header = directory.path().join("short-header.ftwdb");
        let offsets = legacy_log(&short_header, 2);
        OpenOptions::new()
            .write(true)
            .open(&short_header)
            .unwrap()
            .set_len(offsets[1] + 7)
            .unwrap();
        let scan = salvage_scan(&short_header, Config::default().max_batch_points);
        assert_eq!(
            scan.salvage_stop_reason,
            Some(SalvageStopReason::IncompleteFrameHeader)
        );
        assert_eq!(scan.validated_bytes, offsets[1]);
        assert_eq!((scan.commits, scan.points), (1, 1));

        let short_payload = directory.path().join("short-payload.ftwdb");
        let offsets = legacy_log(&short_payload, 2);
        let length = std::fs::metadata(&short_payload).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&short_payload)
            .unwrap()
            .set_len(length - 7)
            .unwrap();
        let scan = salvage_scan(&short_payload, Config::default().max_batch_points);
        assert_eq!(
            scan.salvage_stop_reason,
            Some(SalvageStopReason::IncompleteFramePayload)
        );
        assert_eq!(scan.validated_bytes, offsets[1]);
        assert_eq!((scan.commits, scan.points), (1, 1));

        let bad_magic = directory.path().join("bad-magic.ftwdb");
        let offset = legacy_log(&bad_magic, 1)[0];
        rewrite_frame_header(&bad_magic, offset, |header| {
            header[..4].copy_from_slice(b"NOPE");
        });
        assert_eq!(
            salvage_scan(&bad_magic, Config::default().max_batch_points).salvage_stop_reason,
            Some(SalvageStopReason::InvalidFrameMagic)
        );

        let unsupported_version = directory.path().join("unsupported-version.ftwdb");
        let offset = legacy_log(&unsupported_version, 1)[0];
        rewrite_frame_header(&unsupported_version, offset, |header| {
            header[4..6].copy_from_slice(&(FRAME_VERSION + 1).to_le_bytes());
        });
        assert_eq!(
            salvage_scan(&unsupported_version, Config::default().max_batch_points)
                .salvage_stop_reason,
            Some(SalvageStopReason::UnsupportedFrameVersion)
        );

        let bad_header_crc = directory.path().join("bad-header-crc.ftwdb");
        let offset = legacy_log(&bad_header_crc, 1)[0];
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&bad_header_crc)
            .unwrap();
        file.seek(SeekFrom::Start(offset + 8)).unwrap();
        file.write_all(&2_u32.to_le_bytes()).unwrap();
        file.sync_all().unwrap();
        assert_eq!(
            salvage_scan(&bad_header_crc, Config::default().max_batch_points).salvage_stop_reason,
            Some(SalvageStopReason::FrameHeaderChecksumMismatch)
        );

        let invalid_legacy_size = directory.path().join("legacy-size.ftwdb");
        let offset = legacy_log(&invalid_legacy_size, 1)[0];
        rewrite_frame_header(&invalid_legacy_size, offset, |header| {
            header[8..12].copy_from_slice(&2_u32.to_le_bytes());
        });
        assert_eq!(
            salvage_scan(&invalid_legacy_size, Config::default().max_batch_points)
                .salvage_stop_reason,
            Some(SalvageStopReason::InvalidLegacyFrameSize)
        );

        let unknown_kind = directory.path().join("unknown-kind.ftwdb");
        let offset = legacy_log(&unknown_kind, 1)[0];
        rewrite_frame_header(&unknown_kind, offset, |header| {
            header[6..8].copy_from_slice(&99_u16.to_le_bytes());
        });
        assert_eq!(
            salvage_scan(&unknown_kind, Config::default().max_batch_points).salvage_stop_reason,
            Some(SalvageStopReason::UnknownFrameKind)
        );

        let oversized = directory.path().join("oversized-transaction.ftwdb");
        header_only(&oversized);
        let offset = append_raw_frame(&oversized, FRAME_KIND_TRANSACTION, 0, b"bad");
        rewrite_frame_header(&oversized, offset, |header| {
            let length = u32::try_from(Config::default().max_transaction_bytes + 1).unwrap();
            header[12..16].copy_from_slice(&length.to_le_bytes());
        });
        assert_eq!(
            salvage_scan(&oversized, Config::default().max_batch_points).salvage_stop_reason,
            Some(SalvageStopReason::TransactionFrameTooLarge)
        );
    }

    #[test]
    fn salvage_classifies_all_transaction_failures_after_a_valid_header_as_partial() {
        let directory = tempdir().unwrap();

        let invalid_transaction = directory.path().join("invalid-transaction.ftwdb");
        header_only(&invalid_transaction);
        append_raw_frame(&invalid_transaction, FRAME_KIND_TRANSACTION, 0, b"bad");
        let scan = salvage_scan(&invalid_transaction, Config::default().max_batch_points);
        assert_eq!(
            scan.salvage_stop_reason,
            Some(SalvageStopReason::InvalidTransaction)
        );
        assert_eq!(scan.validated_bytes, DATABASE_HEADER_BYTES as u64);
        assert_eq!((scan.commits, scan.points), (0, 0));

        let invalid_catalog = directory.path().join("invalid-catalog.ftwdb");
        header_only(&invalid_catalog);
        let mut transaction = Transaction::new();
        transaction.define_series(power_series());
        let payload = encode_transaction(&transaction).unwrap();
        append_raw_frame(&invalid_catalog, FRAME_KIND_TRANSACTION, 1, &payload);
        let scan = salvage_scan(&invalid_catalog, Config::default().max_batch_points);
        assert_eq!(
            scan.salvage_stop_reason,
            Some(SalvageStopReason::InvalidCatalogTransaction)
        );
        assert_eq!(scan.validated_bytes, DATABASE_HEADER_BYTES as u64);
        assert_eq!((scan.commits, scan.points), (0, 0));

        let duplicate = directory.path().join("duplicate-commit-id.ftwdb");
        header_only(&duplicate);
        let mut identified = Transaction::new();
        identified.with_commit_id(77);
        let payload = encode_transaction(&identified).unwrap();
        append_raw_frame(&duplicate, FRAME_KIND_IDENTIFIED_TRANSACTION, 0, &payload);
        let duplicate_offset =
            append_raw_frame(&duplicate, FRAME_KIND_IDENTIFIED_TRANSACTION, 0, &payload);
        let scan = salvage_scan(&duplicate, Config::default().max_batch_points);
        assert_eq!(
            scan.salvage_stop_reason,
            Some(SalvageStopReason::DuplicateCommitId)
        );
        assert_eq!(scan.validated_bytes, duplicate_offset);
        assert_eq!((scan.commits, scan.points), (1, 0));

        let too_many_points = directory.path().join("too-many-points.ftwdb");
        header_only(&too_many_points);
        let mut catalog = Transaction::new();
        catalog.upsert_entity(home()).define_series(power_series());
        let payload = encode_transaction(&catalog).unwrap();
        append_raw_frame(&too_many_points, FRAME_KIND_TRANSACTION, 2, &payload);
        let mut first = point(1, 1, 1, 1.0);
        first.run_id = 0;
        let mut second = point(2, 2, 2, 2.0);
        second.run_id = 0;
        let mut transaction = Transaction::new();
        transaction.append_points(vec![first, second]);
        let payload = encode_transaction(&transaction).unwrap();
        let rejected_offset =
            append_raw_frame(&too_many_points, FRAME_KIND_TRANSACTION, 1, &payload);
        let scan = salvage_scan(&too_many_points, 1);
        assert_eq!(
            scan.salvage_stop_reason,
            Some(SalvageStopReason::TransactionPointCountTooLarge)
        );
        assert_eq!(scan.validated_bytes, rejected_offset);
        assert_eq!((scan.commits, scan.points), (1, 0));

        let identified_too_short = directory.path().join("identified-too-short.ftwdb");
        header_only(&identified_too_short);
        append_raw_frame(
            &identified_too_short,
            FRAME_KIND_IDENTIFIED_TRANSACTION,
            0,
            b"short",
        );
        assert_eq!(
            salvage_scan(&identified_too_short, Config::default().max_batch_points)
                .salvage_stop_reason,
            Some(SalvageStopReason::IdentifiedTransactionTooShort)
        );

        let header_only_path = directory.path().join("header-only.ftwdb");
        header_only(&header_only_path);
        let scan = salvage_scan(&header_only_path, Config::default().max_batch_points);
        assert_eq!(scan.salvage_stop_reason, Some(SalvageStopReason::CleanEof));
        assert_eq!(scan.validated_bytes, DATABASE_HEADER_BYTES as u64);
        assert_eq!((scan.commits, scan.points), (0, 0));
    }

    #[test]
    fn normal_recovery_keeps_catalog_validation_before_the_point_limit() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("recovery-policy.ftwdb");
        header_only(&path);
        let mut transaction = Transaction::new();
        transaction.append_points(vec![point(1, 1, 1, 1.0), point(2, 2, 2, 2.0)]);
        let payload = encode_transaction(&transaction).unwrap();
        append_raw_frame(&path, FRAME_KIND_TRANSACTION, 1, &payload);

        match Database::open_with(
            &path,
            Config {
                max_batch_points: 1,
                ..Config::default()
            },
        ) {
            Err(Error::Corruption { reason, .. }) => {
                assert!(reason.contains("invalid recovered transaction"));
            }
            Err(error) => panic!("wrong normal recovery error: {error}"),
            Ok(_) => panic!("normal recovery accepted invalid catalog dependencies"),
        }
    }

    #[test]
    fn salvage_source_identity_detects_an_uncooperative_in_place_writer() {
        let directory = tempdir().unwrap();
        let source_root = directory.path().join("source");
        std::fs::create_dir(&source_root).unwrap();
        let active = source_root.join("active.wlog");
        legacy_log(&active, 1);
        let source = SalvageSource::open(&source_root, "active.wlog", &HashSet::new()).unwrap();

        let mut writer = OpenOptions::new().write(true).open(&active).unwrap();
        writer.seek(SeekFrom::End(-1)).unwrap();
        writer.write_all(&[0xff]).unwrap();
        writer.sync_all().unwrap();

        assert!(matches!(
            source.ensure_unchanged(),
            Err(Error::SourceChanged { path }) if path == active
        ));
    }

    fn optimization_run() -> Run {
        Run {
            id: RunId(9),
            kind: RunKind::Optimization,
            status: RunStatus::Succeeded,
            created_at: 10,
            knowledge_time: 10,
            workflow: "home-dispatch".to_owned(),
            model: "mpc".to_owned(),
            model_version: "1".to_owned(),
            parent_run: None,
            input_snapshot: None,
            attributes: BTreeMap::new(),
        }
    }

    fn plan() -> Plan {
        Plan {
            id: 11,
            run_id: RunId(9),
            status: PlanStatus::Candidate,
            horizon_start: 100,
            horizon_end: 200,
            resolution_micros: 10,
            scenario: "base".to_owned(),
            objective_terms: BTreeMap::new(),
            objective_value: Some(42.0),
            supersedes: None,
            attributes: BTreeMap::new(),
        }
    }

    #[test]
    fn round_trips_batches_and_revisions() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("round-trip.ftwdb");
        {
            let mut database = Database::open(&path).unwrap();
            database
                .append(&[
                    point(100, 10, 11, 1.0),
                    point(200, 10, 11, 2.0),
                    point(100, 20, 21, 3.0),
                ])
                .unwrap();
            database.close().unwrap();
        }

        let database = Database::open(&path).unwrap();
        assert_eq!(database.stats().unwrap().points, 3);
        let latest = database.query_latest(7, 0, 1_000).unwrap();
        assert_eq!(latest.len(), 2);
        assert_eq!(latest[0].value, 3.0);
        assert_eq!(database.query_as_of(7, 0, 1_000, 15).unwrap()[0].value, 1.0);
    }

    #[test]
    fn recovered_late_valid_times_remain_visible_to_range_queries() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("late-valid-time.ftwdb");
        {
            let mut database = Database::open(&path).unwrap();
            database
                .append(&[point(100, 10, 11, 1.0), point(200, 10, 11, 2.0)])
                .unwrap();
            database.append(&[point(50, 20, 21, 3.0)]).unwrap();
            assert_eq!(database.query_history(7, 0, 75).unwrap().len(), 1);
            database.close().unwrap();
        }

        let database = Database::open(&path).unwrap();
        let early = database.query_history(7, 0, 75).unwrap();
        assert_eq!(early.len(), 1);
        assert_eq!(early[0].valid_time, 50);
        assert_eq!(early[0].value, 3.0);
        assert_eq!(database.query_history(7, 0, 150).unwrap().len(), 2);
        assert_eq!(database.query_latest(7, 0, 1_000).unwrap().len(), 3);
    }

    #[test]
    fn append_rejects_inverted_intervals_with_the_commit_error() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("append-validation.ftwdb");
        let mut database = Database::open(&path).unwrap();
        let mut inverted = point(100, 10, 11, 1.0);
        inverted.valid_time_end = 50;

        let before = database.stats().unwrap().file_bytes;
        let append_error = match database.append(&[point(1, 1, 1, 1.0), inverted]) {
            Err(error @ Error::InvalidModel(_)) => error,
            other => panic!("expected Error::InvalidModel, got {other:?}"),
        };
        // The whole batch is rejected before anything reaches the log.
        assert_eq!(database.stats().unwrap().file_bytes, before);
        assert_eq!(database.stats().unwrap().points, 0);

        // A transaction commit rejects the same point with the same error.
        let mut transaction = Transaction::new();
        transaction
            .upsert_entity(home())
            .define_series(power_series())
            .upsert_run(optimization_run());
        database.commit(transaction).unwrap();
        let mut committed = Transaction::new();
        let mut invalid = inverted;
        invalid.series_id = 7;
        invalid.run_id = 9;
        committed.append_points(vec![invalid]);
        let commit_error = match database.commit(committed) {
            Err(error @ Error::InvalidModel(_)) => error,
            other => panic!("expected Error::InvalidModel, got {other:?}"),
        };
        assert_eq!(append_error.to_string(), commit_error.to_string());
    }

    #[test]
    fn append_and_commit_reject_non_finite_values_before_writing() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("non-finite.ftwdb");
        let mut database = Database::open(&path).unwrap();
        let before = database.stats().unwrap().file_bytes;

        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut invalid = point(100, 10, 11, value);
            invalid.valid_time_end = invalid.valid_time;
            match database.append(&[invalid]) {
                Err(Error::InvalidModel(reason)) => {
                    assert_eq!(reason, "point value must be finite");
                }
                other => panic!("expected finite-value rejection, got {other:?}"),
            }
        }
        assert_eq!(database.stats().unwrap().file_bytes, before);
        assert_eq!(database.stats().unwrap().points, 0);

        let mut catalog = Transaction::new();
        catalog
            .upsert_entity(home())
            .define_series(power_series())
            .upsert_run(optimization_run());
        database.commit(catalog).unwrap();
        let mut committed = Transaction::new();
        let mut invalid = point(100, 10, 11, f64::NAN);
        invalid.series_id = 7;
        invalid.run_id = 9;
        committed.append_points(vec![invalid]);
        match database.commit(committed) {
            Err(Error::InvalidModel(reason)) => {
                assert_eq!(reason, "point value must be finite");
            }
            other => panic!("expected finite-value rejection, got {other:?}"),
        }
        assert!(database.append(&[point(1, 1, 1, 1.0)]).is_ok());
    }

    #[test]
    fn append_remains_a_catalog_less_fast_path() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("append-catalog-less.ftwdb");
        {
            let mut database = Database::open(&path).unwrap();
            // No catalog exists: the series is undefined and the run
            // unresolvable, yet the legacy path accepts the batch because
            // only commit() enforces catalog references.
            let mut telemetry = point(100, 10, 11, 1.0);
            telemetry.run_id = 42;
            database.append(&[telemetry]).unwrap();
            database.close().unwrap();
        }
        // The legacy frame recovers unchanged on reopen.
        let database = Database::open(&path).unwrap();
        assert_eq!(database.stats().unwrap().points, 1);
        assert_eq!(database.query_latest(7, 0, 1_000).unwrap().len(), 1);
    }

    #[test]
    fn rollup_gauge_rejects_invalid_arguments_instead_of_panicking() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("rollup-arguments.ftwdb");
        let mut database = Database::open(&path).unwrap();
        database.append(&[point(100, 10, 11, 1.0)]).unwrap();

        assert!(matches!(
            database.rollup_gauge(7, 0, 1_000, 0, 10),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            database.rollup_gauge(7, 0, 1_000, -5, 10),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            database.rollup_gauge(7, 0, 1_000, 300, -1),
            Err(Error::InvalidArgument(_))
        ));
        let rollup = database.rollup_gauge(7, 0, 1_000, 300, 10).unwrap();
        assert_eq!(rollup.buckets().len(), 1);
    }

    #[test]
    fn parent_directory_resolves_nested_and_bare_paths() {
        use super::parent_directory;
        use std::path::Path;
        assert_eq!(
            parent_directory(Path::new("/store/active.wlog")),
            Path::new("/store")
        );
        assert_eq!(
            parent_directory(Path::new("relative/active.wlog")),
            Path::new("relative")
        );
        // `Path::parent` reports an unopenable empty parent for a bare file
        // name; the creation sync must target the current directory instead.
        assert_eq!(parent_directory(Path::new("active.wlog")), Path::new("."));
    }

    #[test]
    fn directory_sync_helpers_accept_directories_and_parent_paths() {
        let directory = tempdir().unwrap();
        let nested = directory.path().join("nested");
        std::fs::create_dir(&nested).unwrap();

        super::sync_directory(&nested).unwrap();
        super::sync_parent_directory(&nested.join("new-entry")).unwrap();
    }

    #[test]
    fn pre_created_empty_file_is_initialized_like_a_new_database() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("pre-created.ftwdb");
        drop(std::fs::File::create(&path).unwrap());
        // A zero-length file takes the same creation path as a brand-new
        // database: header write, then the parent directory entry sync.
        {
            let mut database = Database::open(&path).unwrap();
            database.append(&[point(1, 1, 1, 1.0)]).unwrap();
            database.close().unwrap();
        }
        let database = Database::open(&path).unwrap();
        assert_eq!(database.stats().unwrap().points, 1);
    }

    #[test]
    fn second_opener_fails_until_the_first_handle_closes() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("locked.ftwdb");
        let first = Database::open(&path).unwrap();
        match Database::open(&path) {
            Err(Error::Locked { path: reported }) => assert_eq!(reported, path),
            Err(other) => panic!("expected Error::Locked, got {other:?}"),
            Ok(_) => panic!("expected Error::Locked, got a second open database"),
        }
        drop(first);
        let mut reopened = Database::open(&path).unwrap();
        reopened.append(&[point(1, 1, 1, 1.0)]).unwrap();
        reopened.close().unwrap();
    }

    #[test]
    fn read_only_open_simulates_recovery_without_mutating_the_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("read-only-torn.ftwdb");
        let first_length;
        {
            let mut database = Database::open_with(
                &path,
                Config {
                    durability: Durability::Manual,
                    ..Config::default()
                },
            )
            .unwrap();
            database.append(&[point(1, 1, 1, 1.0)]).unwrap();
            database.flush().unwrap();
            first_length = database.stats().unwrap().file_bytes;
            database.append(&[point(2, 2, 2, 2.0)]).unwrap();
        }
        let full_length = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full_length - 10).unwrap();
        drop(file);
        let bytes_before = std::fs::read(&path).unwrap();

        let database = Database::open_read_only(&path).unwrap();
        let stats = database.stats().unwrap();
        assert!(stats.recovered_tail_bytes > 0);
        assert_eq!(
            stats.recovered_tail,
            super::RecoveredTail::IncompletePayload
        );
        assert_eq!(stats.file_bytes, first_length);
        assert_eq!(database.query_latest(7, 0, 10).unwrap().len(), 1);
        database.close().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), bytes_before);
    }

    #[test]
    fn read_only_open_does_not_create_a_missing_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("absent.ftwdb");
        match Database::open_read_only(&path) {
            Err(Error::Io(error)) => assert_eq!(error.kind(), std::io::ErrorKind::NotFound),
            Err(other) => panic!("expected a not-found error, got {other:?}"),
            Ok(_) => panic!("expected a not-found error, got an open database"),
        }
        assert!(!path.exists());
    }

    #[test]
    fn writer_apis_fail_cleanly_on_a_read_only_handle() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("read-only-writers.ftwdb");
        {
            let mut database = Database::open(&path).unwrap();
            database.append(&[point(1, 1, 1, 1.0)]).unwrap();
            database.close().unwrap();
        }
        let mut database = Database::open_read_only(&path).unwrap();
        assert!(database.is_read_only());
        assert!(matches!(
            database.append(&[point(2, 2, 2, 2.0)]),
            Err(Error::ReadOnly)
        ));
        assert!(matches!(
            database.commit(Transaction::new()),
            Err(Error::ReadOnly)
        ));
        assert!(matches!(database.flush(), Err(Error::ReadOnly)));
        assert_eq!(database.query_latest(7, 0, 10).unwrap().len(), 1);
        database.close().unwrap();
    }

    #[test]
    fn shared_and_exclusive_locks_exclude_each_other() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("lock-interplay.ftwdb");
        {
            let mut database = Database::open(&path).unwrap();
            database.append(&[point(1, 1, 1, 1.0)]).unwrap();
            database.close().unwrap();
        }

        // Read-only openers share the lock with each other...
        let first_reader = Database::open_read_only(&path).unwrap();
        let second_reader = Database::open_read_only(&path).unwrap();
        // ...but exclude an exclusive writer.
        assert!(matches!(Database::open(&path), Err(Error::Locked { .. })));
        drop(first_reader);
        assert!(matches!(Database::open(&path), Err(Error::Locked { .. })));
        drop(second_reader);

        // A live exclusive writer excludes read-only openers.
        let writer = Database::open(&path).unwrap();
        assert!(matches!(
            Database::open_read_only(&path),
            Err(Error::Locked { .. })
        ));
        drop(writer);
        Database::open_read_only(&path).unwrap().close().unwrap();
    }

    #[test]
    fn incomplete_final_payload_is_removed_as_one_atomic_batch() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("torn-tail.ftwdb");
        let first_length;
        {
            let mut database = Database::open_with(
                &path,
                Config {
                    durability: Durability::Manual,
                    ..Config::default()
                },
            )
            .unwrap();
            database.append(&[point(1, 1, 1, 1.0)]).unwrap();
            database.flush().unwrap();
            first_length = database.stats().unwrap().file_bytes;
            database.append(&[point(2, 2, 2, 2.0)]).unwrap();
        }
        let full_length = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full_length - 10).unwrap();

        let database = Database::open(&path).unwrap();
        assert_eq!(database.query_latest(7, 0, 10).unwrap().len(), 1);
        let stats = database.stats().unwrap();
        assert_eq!(stats.file_bytes, first_length);
        assert!(stats.recovered_tail_bytes > 0);
        assert_eq!(
            stats.recovered_tail,
            super::RecoveredTail::IncompletePayload
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn incomplete_final_header_is_removed_and_reported() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("short-header.ftwdb");
        let first_length;
        {
            let mut database = Database::open(&path).unwrap();
            database.append(&[point(1, 1, 1, 1.0)]).unwrap();
            first_length = database.stats().unwrap().file_bytes;
            database.append(&[point(2, 2, 2, 2.0)]).unwrap();
            database.close().unwrap();
        }
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(first_length + 7).unwrap();
        drop(file);
        let bytes_before = std::fs::read(&path).unwrap();

        let database = Database::open_read_only(&path).unwrap();
        let stats = database.stats().unwrap();
        assert_eq!(stats.file_bytes, first_length);
        assert_eq!(stats.recovered_tail_bytes, 7);
        assert_eq!(stats.recovered_tail, super::RecoveredTail::IncompleteHeader);
        assert_eq!(database.query_latest(7, 0, 10).unwrap().len(), 1);
        database.close().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), bytes_before);

        let database = Database::open(&path).unwrap();
        let stats = database.stats().unwrap();
        assert_eq!(stats.file_bytes, first_length);
        assert_eq!(stats.recovered_tail_bytes, 7);
        assert_eq!(stats.recovered_tail, super::RecoveredTail::IncompleteHeader);
        assert_eq!(database.query_latest(7, 0, 10).unwrap().len(), 1);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn failed_flush_poisons_the_writer_instead_of_permitting_a_retry() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("fsyncgate.ftwdb");
        let mut database = Database::open_with(
            &path,
            Config {
                durability: Durability::Manual,
                ..Config::default()
            },
        )
        .unwrap();
        database.append(&[point(1, 1, 1, 1.0)]).unwrap();

        // A pipe cannot be synchronized, so `sync_data` on it fails exactly
        // where a dying disk would fail the real log file's fsync.
        let (_reader, writer) = std::io::pipe().unwrap();
        let real_file = std::mem::replace(
            &mut database.file,
            std::fs::File::from(std::os::fd::OwnedFd::from(writer)),
        );

        assert!(matches!(database.flush(), Err(Error::Io(_))));
        // The kernel may have marked the unwritten pages clean during the
        // failed sync, so no retry may be able to report durability: every
        // writer API must now fail with `Error::Poisoned`.
        assert!(matches!(database.flush(), Err(Error::Poisoned)));
        assert!(matches!(
            database.append(&[point(2, 2, 2, 2.0)]),
            Err(Error::Poisoned)
        ));
        assert!(matches!(
            database.commit(Transaction::new()),
            Err(Error::Poisoned)
        ));
        drop(real_file);
    }

    #[test]
    fn always_append_sync_failure_preserves_kind_and_poisons_writer() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("append-sync-failure.ftwdb");
        let mut database = Database::open_with(
            &path,
            Config {
                durability: Durability::Always,
                ..Config::default()
            },
        )
        .unwrap();

        fail_next_sync(std::io::ErrorKind::StorageFull);
        match database.append(&[point(1, 1, 1, 1.0)]) {
            Err(Error::Io(error)) => {
                assert_eq!(error.kind(), std::io::ErrorKind::StorageFull)
            }
            other => panic!("expected injected append sync error, got {other:?}"),
        }
        assert!(matches!(
            database.append(&[point(2, 2, 2, 2.0)]),
            Err(Error::Poisoned)
        ));
        assert!(matches!(database.flush(), Err(Error::Poisoned)));
    }

    #[test]
    fn always_commit_sync_failure_preserves_kind_and_poisons_writer() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("commit-sync-failure.ftwdb");
        let mut database = Database::open_with(
            &path,
            Config {
                durability: Durability::Always,
                ..Config::default()
            },
        )
        .unwrap();
        let mut transaction = Transaction::new();
        transaction.upsert_entity(home());

        fail_next_sync(std::io::ErrorKind::PermissionDenied);
        match database.commit(transaction) {
            Err(Error::Io(error)) => {
                assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied)
            }
            other => panic!("expected injected commit sync error, got {other:?}"),
        }
        assert!(matches!(
            database.commit(Transaction::new()),
            Err(Error::Poisoned)
        ));
        assert!(matches!(database.flush(), Err(Error::Poisoned)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn dev_full_write_reports_storage_full_and_poisons_writer() {
        let full = std::path::Path::new("/dev/full");
        if !full.exists() {
            eprintln!("skipping /dev/full test: /dev/full is absent");
            return;
        }

        let directory = tempdir().unwrap();
        let path = directory.path().join("dev-full.ftwdb");
        let mut database = Database::open_with(
            &path,
            Config {
                durability: Durability::Always,
                ..Config::default()
            },
        )
        .unwrap();
        let full_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(full)
            .unwrap();
        let real_file = std::mem::replace(&mut database.file, full_file);

        match database.append(&[point(1, 1, 1, 1.0)]) {
            Err(Error::Io(error)) => {
                assert_eq!(error.kind(), std::io::ErrorKind::StorageFull)
            }
            other => panic!("expected /dev/full storage error, got {other:?}"),
        }
        assert!(matches!(
            database.append(&[point(2, 2, 2, 2.0)]),
            Err(Error::Poisoned)
        ));
        drop(real_file);
    }

    #[test]
    fn corruption_before_the_tail_is_not_silently_discarded() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("corrupt.ftwdb");
        {
            let mut database = Database::open(&path).unwrap();
            database.append(&[point(1, 1, 1, 1.0)]).unwrap();
            database.append(&[point(2, 2, 2, 2.0)]).unwrap();
            database.close().unwrap();
        }
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(16 + 24 + 8)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.sync_data().unwrap();

        assert!(matches!(
            Database::open(&path),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn catalog_plan_and_points_commit_and_recover_atomically() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("transaction.ftwdb");
        let mut planned = point(100, 10, 10, 5_000.0);
        planned.valid_time_end = 110;
        planned.run_id = 9;

        {
            let mut database = Database::open(&path).unwrap();
            let mut transaction = Transaction::new();
            transaction
                .upsert_entity(home())
                .define_series(power_series())
                .upsert_run(optimization_run())
                .upsert_plan(plan())
                .append_points(vec![planned]);
            let commit = database.commit(transaction).unwrap();
            assert_eq!(commit.records, 5);
            assert_eq!(commit.points, 1);
            let mut actual = point(100, 20, 20, 4_800.0);
            actual.valid_time_end = 110;
            actual.run_id = 0;
            let mut outcome_transaction = Transaction::new();
            outcome_transaction.append_points(vec![actual]);
            database.commit(outcome_transaction).unwrap();
            database.close().unwrap();
        }

        let database = Database::open(&path).unwrap();
        assert_eq!(database.catalog().entity(EntityId(1)), Some(&home()));
        assert_eq!(database.catalog().series(7), Some(&power_series()));
        assert_eq!(database.catalog().run(RunId(9)), Some(&optimization_run()));
        assert_eq!(database.catalog().plan(11), Some(&plan()));
        assert_eq!(database.query_run(7, 9, 0, 1_000).unwrap(), vec![planned]);
        let comparison = database.compare_plan_to_actual(7, 7, 9, 0, 1_000).unwrap();
        assert_eq!(comparison.len(), 1);
        assert_eq!(comparison[0].difference, Some(-200.0));
        assert_eq!(database.stats().unwrap().catalog_records, 4);
    }

    #[test]
    fn identified_commit_replays_as_a_no_op_after_reopen() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("idempotent.ftwdb");
        let mut telemetry = point(100, 10, 10, 1.0);
        telemetry.run_id = 0;
        let build = |value: Point, commit_id: u128| {
            let mut transaction = Transaction::new();
            transaction
                .upsert_entity(home())
                .define_series(power_series())
                .append_points(vec![value])
                .with_commit_id(commit_id);
            transaction
        };
        {
            let mut database = Database::open(&path).unwrap();
            // Zero is an ordinary identifier, not a sentinel.
            let commit = database.commit(build(telemetry, 0)).unwrap();
            assert!(!commit.deduplicated);
            assert_eq!(commit.points, 1);
            database.close().unwrap();
        }

        // The crash-and-retry scenario: the acknowledgement was lost, but the
        // frame was durable, so the retried commit must not write again.
        let mut database = Database::open(&path).unwrap();
        assert!(database.contains_commit_id(0));
        let commit = database.commit(build(telemetry, 0)).unwrap();
        assert!(commit.deduplicated);
        assert_eq!(commit.points, 0);
        assert_eq!(commit.records, 0);
        assert_eq!(commit.bytes_written, 0);
        assert_eq!(database.stats().unwrap().points, 1);
        assert_eq!(database.query_history(7, 0, 1_000).unwrap().len(), 1);

        // A different identifier is an independent commit; retrying it within
        // the same session is deduplicated without a reopen.
        let mut revision = point(100, 20, 20, 2.0);
        revision.run_id = 0;
        let mut second = Transaction::new();
        second.append_points(vec![revision]).with_commit_id(1);
        assert!(!database.commit(second.clone()).unwrap().deduplicated);
        assert!(database.commit(second).unwrap().deduplicated);
        assert_eq!(database.stats().unwrap().points, 2);
        assert_eq!(database.query_history(7, 0, 1_000).unwrap().len(), 2);
        database.close().unwrap();
    }

    #[test]
    fn identified_commit_rejects_a_reused_id_with_different_bytes() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("identified-conflict.ftwdb");
        let mut first_point = point(100, 10, 10, 1.0);
        first_point.run_id = 0;
        let mut second_point = point(200, 20, 20, 2.0);
        second_point.run_id = 0;
        let mut database = Database::open(&path).unwrap();
        let mut catalog = Transaction::new();
        catalog.upsert_entity(home()).define_series(power_series());
        database.commit(catalog).unwrap();

        let mut first = Transaction::new();
        first.append_points(vec![first_point]).with_commit_id(42);
        assert!(!database.commit(first).unwrap().deduplicated);

        let mut mutated = Transaction::new();
        mutated.append_points(vec![second_point]).with_commit_id(42);
        assert!(matches!(
            database.commit(mutated),
            Err(Error::IngressCommitIdConflict { commit_id: 42 })
        ));
        assert_eq!(database.stats().unwrap().points, 1);
        assert_eq!(database.query_history(7, 0, 1_000).unwrap().len(), 1);

        let mut exact = Transaction::new();
        exact.append_points(vec![first_point]).with_commit_id(42);
        assert!(database.commit(exact).unwrap().deduplicated);

        database.close().unwrap();
        let mut reopened = Database::open(&path).unwrap();
        let mut mutated_after_reopen = Transaction::new();
        mutated_after_reopen
            .append_points(vec![second_point])
            .with_commit_id(42);
        assert!(matches!(
            reopened.commit(mutated_after_reopen),
            Err(Error::IngressCommitIdConflict { commit_id: 42 })
        ));
        let mut exact_after_reopen = Transaction::new();
        exact_after_reopen
            .append_points(vec![first_point])
            .with_commit_id(42);
        assert!(reopened.commit(exact_after_reopen).unwrap().deduplicated);
        assert_eq!(reopened.stats().unwrap().points, 1);
    }

    #[test]
    fn compact_receipts_compare_exact_bytes_even_when_crc32_collides() {
        let directory = tempdir().unwrap();
        let database = Database::open(directory.path().join("collision.ftwdb")).unwrap();
        let original = b"plumless";
        let collision = b"buckeroo";
        assert_eq!(original.len(), collision.len());
        assert_eq!(crc32fast::hash(original), crc32fast::hash(collision));

        let receipt = super::StoredIdentifiedReceipt {
            payload_offset: 0,
            payload_len: original.len() as u32,
            payload_crc32: crc32fast::hash(original),
            compact_payload: Some(std::sync::Arc::from(original.as_slice())),
            commit: super::Commit {
                frame_offset: 0,
                points: 0,
                records: 0,
                bytes_written: 0,
                durable: true,
                deduplicated: false,
            },
        };

        assert!(
            !database
                .identified_payload_matches_at(receipt, collision)
                .unwrap()
        );
    }

    #[test]
    fn reclaim_sorts_ingress_receipts_and_preserves_exact_replay_receipts() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ordered-compact-index.ftwdb");
        let mut database = Database::open(&path).unwrap();
        let identities = [
            IngressIdentity::new(2, 4, 204),
            IngressIdentity::new(1, 10, 110),
            IngressIdentity::new(1, 12, 112),
            IngressIdentity::new(2, 9, 209),
            IngressIdentity::new(1, 20, 120),
        ];
        let mut originals = Vec::new();
        for identity in identities {
            originals.push((
                identity,
                database
                    .commit_ingress(identity, Transaction::new())
                    .unwrap(),
            ));
        }

        database.reclaim_live_log().unwrap();
        for (identity, original) in &originals {
            let replay = database
                .commit_ingress(*identity, Transaction::new())
                .unwrap();
            assert!(replay.deduplicated);
            assert_eq!(replay.frame_offset, original.frame_offset);
            assert_eq!(replay.bytes_written, original.bytes_written);
        }
        database.close().unwrap();

        let mut reopened = Database::open(&path).unwrap();
        for (identity, original) in originals {
            let replay = reopened
                .commit_ingress(identity, Transaction::new())
                .unwrap();
            assert!(replay.deduplicated);
            assert_eq!(replay.frame_offset, original.frame_offset);
            assert_eq!(replay.bytes_written, original.bytes_written);
        }
    }

    #[test]
    fn reclaim_splits_large_exact_identity_indexes_into_bounded_frames() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("chunked-compact-index.ftwdb");
        let config = Config {
            max_transaction_bytes: 64,
            ..Config::default()
        };
        let mut database = Database::open_with(&path, config).unwrap();
        let identities: Vec<_> = (0_u64..80)
            .map(|sequence| {
                IngressIdentity::new(9, sequence, u128::from(sequence).saturating_add(1_000))
            })
            .collect();
        for identity in &identities {
            database
                .commit_ingress(*identity, Transaction::new())
                .unwrap();
        }
        database.reclaim_live_log().unwrap();
        database.close().unwrap();

        let mut reopened = Database::open_with(&path, config).unwrap();
        for identity in identities {
            assert!(
                reopened
                    .commit_ingress(identity, Transaction::new())
                    .unwrap()
                    .deduplicated
            );
        }
    }

    #[test]
    fn legacy_compact_identity_index_decodes_without_exact_payloads() {
        #[derive(serde::Serialize)]
        struct LegacyIdentified {
            commit_id: u128,
            payload_len: u32,
            payload_crc32: u32,
            points: u64,
            records: u64,
        }
        #[derive(serde::Serialize)]
        struct LegacyIngress {
            source_id: u128,
            sequence: u64,
            commit_id: u128,
            canonical_payload_len: u32,
            canonical_payload_crc32: u32,
            points: u64,
            records: u64,
        }
        #[derive(serde::Serialize)]
        struct LegacyIndex {
            identified: Vec<LegacyIdentified>,
            ingress: Vec<LegacyIngress>,
        }

        let encoded = postcard::to_stdvec(&LegacyIndex {
            identified: vec![LegacyIdentified {
                commit_id: 1,
                payload_len: 28,
                payload_crc32: 2,
                points: 3,
                records: 4,
            }],
            ingress: vec![LegacyIngress {
                source_id: 5,
                sequence: 6,
                commit_id: 7,
                canonical_payload_len: 12,
                canonical_payload_crc32: 8,
                points: 9,
                records: 0,
            }],
        })
        .unwrap();
        let decoded = super::decode_identity_index(&encoded, 0).unwrap();
        assert!(decoded.identified[0].payload.is_empty());
        assert!(decoded.ingress[0].canonical_payload.is_empty());
        assert_eq!(decoded.ingress[0].frame_offset, 0);
        assert_eq!(decoded.ingress[0].bytes_written, 0);

        let directory = tempdir().unwrap();
        let path = directory.path().join("legacy-index.ftwdb");
        header_only(&path);
        let mut transaction = Transaction::new();
        transaction.with_commit_id(77);
        let payload = encode_transaction(&transaction).unwrap();
        let legacy = postcard::to_stdvec(&LegacyIndex {
            identified: vec![LegacyIdentified {
                commit_id: 77,
                payload_len: payload.len() as u32,
                payload_crc32: crc32fast::hash(&payload),
                points: 0,
                records: 0,
            }],
            ingress: Vec::new(),
        })
        .unwrap();
        append_raw_frame(&path, super::FRAME_KIND_IDENTITY_INDEX, 0, &legacy);

        let mut database = Database::open(&path).unwrap();
        assert!(database.contains_commit_id(77));
        assert!(matches!(
            database.commit(transaction),
            Err(Error::IngressCommitIdConflict { commit_id: 77 })
        ));
    }

    #[test]
    fn writable_open_rejects_symlink_fifo_and_directory_without_blocking() {
        for kind in ["symlink", "fifo", "directory"] {
            let directory = tempdir().unwrap();
            let path = directory.path().join("not-a-regular-file.ftwdb");
            match kind {
                "symlink" => std::os::unix::fs::symlink("outside", &path).unwrap(),
                "fifo" => create_fifo(&path),
                "directory" => std::fs::create_dir(&path).unwrap(),
                _ => unreachable!(),
            }
            match Database::open(&path) {
                Ok(_) => panic!("{kind}: writable open must reject a non-file"),
                Err(Error::Io(io_error)) => {
                    assert!(
                        io_error.to_string().contains("not a regular file"),
                        "{kind}: {io_error}"
                    );
                }
                Err(other) => panic!("{kind}: expected I/O rejection, got {other}"),
            }
        }
    }

    #[test]
    fn writable_open_creates_a_private_regular_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("created.ftwdb");
        let database = Database::open(&path).unwrap();
        drop(database);
        let metadata = std::fs::symlink_metadata(&path).unwrap();
        assert!(metadata.file_type().is_file());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }

    #[cfg(target_os = "linux")]
    fn create_fifo(path: &std::path::Path) {
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            path,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
    }

    #[cfg(target_os = "macos")]
    fn create_fifo(path: &std::path::Path) {
        use std::os::unix::ffi::OsStrExt;

        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `path` is a live NUL-terminated string and mode has no
        // platform-dependent bits beyond user read/write permissions.
        let result = unsafe { libc::mkfifo(path.as_ptr(), 0o600) };
        if result != 0 {
            panic!("mkfifo failed: {}", std::io::Error::last_os_error());
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn create_fifo(_path: &std::path::Path) {
        panic!("FIFO open tests require Linux or macOS");
    }

    #[test]
    fn ingress_replay_after_reopen_returns_the_original_receipt() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ingress-replay.ftwdb");
        let identity = IngressIdentity::new(11, 400, 9001);
        let mut telemetry = point(100, 10, 10, 1.0);
        telemetry.run_id = 0;
        let build = || {
            let mut transaction = Transaction::new();
            transaction.append_points(vec![telemetry]);
            transaction
        };

        let original = {
            let mut database = Database::open(&path).unwrap();
            let mut catalog = Transaction::new();
            catalog.upsert_entity(home()).define_series(power_series());
            database.commit(catalog).unwrap();
            let receipt = database.commit_ingress(identity, build()).unwrap();
            assert!(!receipt.deduplicated);
            database.close().unwrap();
            receipt
        };

        let mut database = Database::open(&path).unwrap();
        let replay = database.commit_ingress(identity, build()).unwrap();
        assert!(replay.deduplicated);
        assert_eq!(replay.frame_offset, original.frame_offset);
        assert_eq!(replay.points, original.points);
        assert_eq!(replay.records, original.records);
        assert_eq!(replay.bytes_written, original.bytes_written);
        assert!(replay.durable);
        assert_eq!(database.stats().unwrap().points, 1);
        assert_eq!(database.query_history(7, 0, 1_000).unwrap().len(), 1);
    }

    #[test]
    fn ingress_identity_conflicts_are_exact_and_nonfatal() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ingress-conflict.ftwdb");
        let mut database = Database::open(&path).unwrap();
        let mut catalog = Transaction::new();
        catalog.upsert_entity(home()).define_series(power_series());
        database.commit(catalog).unwrap();

        let mut first = Transaction::new();
        first.append_points(vec![Point::actual(7, 100, 1.0)]);
        database
            .commit_ingress(IngressIdentity::new(1, 8, 80), first)
            .unwrap();

        let mut changed = Transaction::new();
        changed.append_points(vec![Point::actual(7, 100, 2.0)]);
        assert!(matches!(
            database.commit_ingress(IngressIdentity::new(1, 8, 80), changed),
            Err(Error::IngressSourceSequenceConflict {
                source_id: 1,
                sequence: 8
            })
        ));

        let mut reused_commit_id = Transaction::new();
        reused_commit_id.append_points(vec![Point::actual(7, 200, 3.0)]);
        assert!(matches!(
            database.commit_ingress(IngressIdentity::new(2, 1, 80), reused_commit_id),
            Err(Error::IngressCommitIdConflict { commit_id: 80 })
        ));

        let mut next = Transaction::new();
        next.append_points(vec![Point::actual(7, 200, 3.0)]);
        assert!(
            !database
                .commit_ingress(IngressIdentity::new(1, 9, 81), next)
                .unwrap()
                .deduplicated
        );
        assert_eq!(database.stats().unwrap().points, 2);
    }

    #[test]
    fn ingress_replay_detects_storage_changes_and_poisons_later_writes() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ingress-mutated-after-open.ftwdb");
        let identity = IngressIdentity::new(6, 20, 200);
        let mut database = Database::open(&path).unwrap();
        database
            .commit_ingress(identity, Transaction::new())
            .unwrap();

        let receipt = database
            .ingress_receipts
            .get(&super::IngressKey::from(identity))
            .unwrap();
        let offset = receipt.canonical_payload_offset;
        let frame_offset = receipt.commit.frame_offset;
        let mut mutator = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        mutator.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0_u8; 1];
        mutator.read_exact(&mut byte).unwrap();
        mutator.seek(SeekFrom::Start(offset)).unwrap();
        mutator.write_all(&[byte[0] ^ 0xff]).unwrap();
        mutator.sync_data().unwrap();

        assert!(matches!(
            database.commit_ingress(identity, Transaction::new()),
            Err(Error::Corruption { offset, .. }) if offset == frame_offset
        ));
        assert!(matches!(
            database.commit_ingress(IngressIdentity::new(6, 21, 201), Transaction::new()),
            Err(Error::Poisoned)
        ));
    }

    #[test]
    fn ingress_replay_detects_identity_changes_after_open() {
        let directory = tempdir().unwrap();
        let path = directory
            .path()
            .join("ingress-identity-mutated-after-open.ftwdb");
        let identity = IngressIdentity::new(7, 30, 300);
        let mut database = Database::open(&path).unwrap();
        let commit = database
            .commit_ingress(identity, Transaction::new())
            .unwrap();

        let identity_offset = commit.frame_offset + super::FRAME_HEADER_BYTES as u64;
        let mut mutator = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        mutator.seek(SeekFrom::Start(identity_offset)).unwrap();
        let mut byte = [0_u8; 1];
        mutator.read_exact(&mut byte).unwrap();
        mutator.seek(SeekFrom::Start(identity_offset)).unwrap();
        mutator.write_all(&[byte[0] ^ 0xff]).unwrap();
        mutator.sync_data().unwrap();

        assert!(matches!(
            database.commit_ingress(identity, Transaction::new()),
            Err(Error::Corruption { offset, .. }) if offset == commit.frame_offset
        ));
        assert!(matches!(
            database.commit_ingress(IngressIdentity::new(7, 31, 301), Transaction::new()),
            Err(Error::Poisoned)
        ));
    }

    #[test]
    fn ingress_cursor_allows_gaps_and_rejects_regression_after_reopen() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ingress-sequence.ftwdb");
        {
            let mut database = Database::open(&path).unwrap();
            database
                .commit_ingress(IngressIdentity::new(7, 50, 500), Transaction::new())
                .unwrap();
            database.close().unwrap();
        }

        let mut database = Database::open(&path).unwrap();
        assert!(
            !database
                .commit_ingress(IngressIdentity::new(7, 52, 502), Transaction::new())
                .unwrap()
                .deduplicated
        );
        database.close().unwrap();

        let mut database = Database::open(&path).unwrap();
        assert!(
            database
                .commit_ingress(IngressIdentity::new(7, 52, 502), Transaction::new())
                .unwrap()
                .deduplicated
        );
        assert!(matches!(
            database.commit_ingress(IngressIdentity::new(7, 51, 501), Transaction::new()),
            Err(Error::IngressSequenceNotIncreasing {
                source_id: 7,
                previous: 52,
                actual: 51
            })
        ));
        assert_eq!(database.stats().unwrap().commits, 2);
    }

    #[test]
    fn ingress_watermarks_are_per_source_and_advance_on_flush() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ingress-watermarks.ftwdb");
        let mut database = Database::open_with(
            &path,
            Config {
                durability: Durability::Manual,
                ..Config::default()
            },
        )
        .unwrap();

        database
            .commit_ingress(IngressIdentity::new(1, 10, 100), Transaction::new())
            .unwrap();
        database
            .commit_ingress(IngressIdentity::new(2, 50, 200), Transaction::new())
            .unwrap();
        assert_eq!(
            database.ingress_watermarks(1),
            super::IngressWatermarks {
                accepted_through: Some(10),
                durable_through: None,
            }
        );
        assert_eq!(
            database.ingress_watermarks(2),
            super::IngressWatermarks {
                accepted_through: Some(50),
                durable_through: None,
            }
        );

        database.flush().unwrap();
        assert_eq!(database.ingress_watermarks(1).durable_through, Some(10));
        assert_eq!(database.ingress_watermarks(2).durable_through, Some(50));

        database
            .commit_ingress(IngressIdentity::new(1, 11, 101), Transaction::new())
            .unwrap();
        assert_eq!(
            database.ingress_watermarks(1),
            super::IngressWatermarks {
                accepted_through: Some(11),
                durable_through: Some(10),
            }
        );
        database.close().unwrap();

        let database = Database::open_read_only(&path).unwrap();
        assert_eq!(
            database.ingress_watermarks(1),
            super::IngressWatermarks {
                accepted_through: Some(11),
                durable_through: None,
            }
        );
        assert_eq!(
            database.ingress_watermarks(2),
            super::IngressWatermarks {
                accepted_through: Some(50),
                durable_through: None,
            }
        );
    }

    #[test]
    fn append_sync_advances_ingress_durable_watermarks() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("append-sync-watermarks.ftwdb");
        let mut database = Database::open_with(
            &path,
            Config {
                durability: Durability::EveryBytes(2_048),
                ..Config::default()
            },
        )
        .unwrap();

        database
            .commit_ingress(IngressIdentity::new(3, 7, 70), Transaction::new())
            .unwrap();
        assert_eq!(
            database.ingress_watermarks(3),
            super::IngressWatermarks {
                accepted_through: Some(7),
                durable_through: None,
            }
        );

        let points: Vec<_> = (0..64)
            .map(|index| point(index, 1, 1, index as f64))
            .collect();
        let commit = database.append(&points).unwrap();
        assert!(commit.durable);
        assert_eq!(database.ingress_watermarks(3).durable_through, Some(7));
    }

    #[test]
    fn writable_reopen_syncs_recovered_ingress_before_claiming_durability() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ingress-reopen-durability.ftwdb");
        let identity = IngressIdentity::new(4, 70, 700);

        {
            let mut database = Database::open_with(
                &path,
                Config {
                    durability: Durability::Manual,
                    ..Config::default()
                },
            )
            .unwrap();
            let commit = database
                .commit_ingress(identity, Transaction::new())
                .unwrap();
            assert!(!commit.durable);
            assert_eq!(database.ingress_watermarks(4).durable_through, None);
            // Dropping models a process exit that did not call flush.
        }

        let mut database = Database::open_with(
            &path,
            Config {
                durability: Durability::Manual,
                ..Config::default()
            },
        )
        .unwrap();
        assert_eq!(database.ingress_watermarks(4).durable_through, Some(70));
        let replay = database
            .commit_ingress(identity, Transaction::new())
            .unwrap();
        assert!(replay.durable);
        assert!(replay.deduplicated);
    }

    #[test]
    fn writable_reopen_does_not_publish_durability_when_startup_sync_fails() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ingress-reopen-sync-failure.ftwdb");
        {
            let mut database = Database::open_with(
                &path,
                Config {
                    durability: Durability::Manual,
                    ..Config::default()
                },
            )
            .unwrap();
            database
                .commit_ingress(IngressIdentity::new(5, 80, 800), Transaction::new())
                .unwrap();
        }

        fail_next_sync(std::io::ErrorKind::Other);
        assert!(matches!(
            Database::open_with(
                &path,
                Config {
                    durability: Durability::Manual,
                    ..Config::default()
                }
            ),
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::Other
        ));
    }

    #[test]
    fn ingress_rejects_zero_source_without_poisoning_the_writer() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ingress-zero-source.ftwdb");
        let mut database = Database::open(&path).unwrap();
        assert!(matches!(
            database.commit_ingress(IngressIdentity::new(0, 1, 1), Transaction::new()),
            Err(Error::InvalidArgument("ingress source id zero is reserved"))
        ));
        assert!(
            !database
                .commit_ingress(IngressIdentity::new(1, 1, 1), Transaction::new())
                .unwrap()
                .deduplicated
        );
    }

    #[test]
    fn torn_ingress_frame_forgets_identity_sequence_and_data_together() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("torn-ingress.ftwdb");
        let identity = IngressIdentity::new(3, 90, 900);
        {
            let mut database = Database::open(&path).unwrap();
            let mut catalog = Transaction::new();
            catalog.upsert_entity(home()).define_series(power_series());
            database.commit(catalog).unwrap();
            let mut transaction = Transaction::new();
            transaction.append_points(vec![Point::actual(7, 100, 1.0)]);
            database.commit_ingress(identity, transaction).unwrap();
            database.close().unwrap();
        }
        let full_length = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full_length - 7).unwrap();
        drop(file);

        let mut database = Database::open(&path).unwrap();
        assert_eq!(database.stats().unwrap().points, 0);
        let mut retry = Transaction::new();
        retry.append_points(vec![Point::actual(7, 100, 1.0)]);
        assert!(
            !database
                .commit_ingress(identity, retry.clone())
                .unwrap()
                .deduplicated
        );
        assert!(
            database
                .commit_ingress(identity, retry)
                .unwrap()
                .deduplicated
        );
        assert_eq!(database.stats().unwrap().points, 1);
    }

    #[test]
    fn identifier_less_commits_stay_at_least_once() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("at-least-once.ftwdb");
        let mut database = Database::open(&path).unwrap();
        let mut telemetry = point(100, 10, 10, 1.0);
        telemetry.run_id = 0;
        let mut first = Transaction::new();
        first
            .upsert_entity(home())
            .define_series(power_series())
            .append_points(vec![telemetry]);
        database.commit(first).unwrap();
        // Without an identifier there is no dedup key: an identical retried
        // commit stores the point twice. This documented at-least-once path
        // is exactly today's behavior.
        let mut retry = Transaction::new();
        retry.append_points(vec![telemetry]);
        let commit = database.commit(retry).unwrap();
        assert!(!commit.deduplicated);
        assert_eq!(commit.points, 1);
        assert_eq!(database.stats().unwrap().points, 2);
        assert_eq!(database.query_history(7, 0, 1_000).unwrap().len(), 2);
    }

    #[test]
    fn torn_identified_frame_forgets_its_identifier() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("torn-identified.ftwdb");
        let mut telemetry = point(100, 10, 10, 1.0);
        telemetry.run_id = 0;
        {
            let mut database = Database::open(&path).unwrap();
            let mut catalog = Transaction::new();
            catalog.upsert_entity(home()).define_series(power_series());
            database.commit(catalog).unwrap();
            let mut identified = Transaction::new();
            identified.append_points(vec![telemetry]).with_commit_id(77);
            database.commit(identified).unwrap();
            database.close().unwrap();
        }
        // Tear the identified frame: identifier and points vanish together,
        // because they share one durable unit.
        let full_length = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full_length - 7).unwrap();
        drop(file);

        let mut database = Database::open(&path).unwrap();
        assert!(!database.contains_commit_id(77));
        assert_eq!(database.stats().unwrap().points, 0);
        // The retry is a genuine first commit and stores the point once.
        let mut retry = Transaction::new();
        retry.append_points(vec![telemetry]).with_commit_id(77);
        assert!(!database.commit(retry).unwrap().deduplicated);
        assert_eq!(database.stats().unwrap().points, 1);
        assert_eq!(database.query_history(7, 0, 1_000).unwrap().len(), 1);
    }

    #[test]
    fn identified_and_identifier_less_frames_coexist_in_one_log() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("mixed-kinds.ftwdb");
        let mut telemetry = point(100, 10, 10, 1.0);
        telemetry.run_id = 0;
        {
            let mut database = Database::open(&path).unwrap();
            // Kind 0: the legacy point frame.
            database.append(&[point(50, 5, 5, 0.5)]).unwrap();
            // Kind 1: an identifier-less transaction, byte-identical to what
            // the code wrote before identified frames existed.
            let mut plain = Transaction::new();
            plain
                .upsert_entity(home())
                .define_series(power_series())
                .append_points(vec![telemetry]);
            database.commit(plain).unwrap();
            // Kind 2: an identified transaction.
            let mut revision = point(100, 20, 20, 2.0);
            revision.run_id = 0;
            let mut identified = Transaction::new();
            identified.append_points(vec![revision]).with_commit_id(9);
            database.commit(identified).unwrap();
            database.close().unwrap();
        }
        let database = Database::open(&path).unwrap();
        let stats = database.stats().unwrap();
        assert_eq!(stats.points, 3);
        assert_eq!(stats.commits, 3);
        assert_eq!(stats.catalog_records, 2);
        assert!(database.contains_commit_id(9));
        assert!(!database.contains_commit_id(77));
    }

    #[test]
    fn invalid_catalog_reference_writes_nothing() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("invalid-transaction.ftwdb");
        let mut database = Database::open(&path).unwrap();
        let before = database.stats().unwrap().file_bytes;
        let mut transaction = Transaction::new();
        transaction.define_series(power_series());

        assert!(matches!(
            database.commit(transaction),
            Err(Error::InvalidModel(_))
        ));
        assert_eq!(database.stats().unwrap().file_bytes, before);
        assert_eq!(database.catalog().stats().series, 0);
    }

    #[test]
    fn torn_mixed_transaction_leaves_neither_metadata_nor_points() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("torn-transaction.ftwdb");
        let mut planned = point(100, 10, 10, 5_000.0);
        planned.valid_time_end = 110;
        planned.run_id = 9;

        let mut database = Database::open_with(
            &path,
            Config {
                durability: Durability::Manual,
                ..Config::default()
            },
        )
        .unwrap();
        let before = database.stats().unwrap().file_bytes;
        let mut transaction = Transaction::new();
        transaction
            .upsert_entity(home())
            .define_series(power_series())
            .upsert_run(optimization_run())
            .upsert_plan(plan())
            .append_points(vec![planned]);
        database.commit(transaction).unwrap();
        drop(database);

        let full_length = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full_length - 7).unwrap();

        let recovered = Database::open(&path).unwrap();
        assert_eq!(recovered.stats().unwrap().file_bytes, before);
        assert_eq!(recovered.catalog().stats().entities, 0);
        assert!(recovered.query_latest(7, 0, 1_000).unwrap().is_empty());
    }
}
