use crate::FixedGaugeRollup;
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::segment::{Segment, SegmentStats};
use crate::transaction::{Record, Transaction};
use crc32fast::hash;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const DATABASE_MAGIC: &[u8; 8] = b"FTWDB001";
const DATABASE_VERSION: u16 = 1;
const DATABASE_HEADER_BYTES: usize = 16;
const FRAME_MAGIC: &[u8; 4] = b"WBAT";
const FRAME_VERSION: u16 = 1;
const FRAME_HEADER_BYTES: usize = 24;
const POINT_BYTES: usize = 72;
const FRAME_KIND_LEGACY_POINTS: u16 = 0;
const FRAME_KIND_TRANSACTION: u16 = 1;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    /// Sync every committed batch before returning. Safest and hardest on
    /// flash media.
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Stats {
    pub points: u64,
    pub commits: u64,
    pub series: usize,
    pub catalog_records: u64,
    pub file_bytes: u64,
    pub recovered_tail_bytes: u64,
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
    file: File,
    config: Config,
    index: HashMap<u64, Vec<Point>>,
    catalog: Catalog,
    commits: u64,
    points: u64,
    catalog_records: u64,
    recovered_tail_bytes: u64,
    bytes_since_sync: u64,
    poisoned: bool,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, Config::default())
    }

    pub fn open_with(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        validate_config(config)?;
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        // Take an exclusive advisory lock before recovery so a second opener
        // can neither interleave frames nor truncate this handle's in-flight
        // tail. The lock lives exactly as long as the handle: closing or
        // dropping the database releases it, even on panic or crash.
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(Error::Locked {
                    path: path.to_path_buf(),
                });
            }
            Err(TryLockError::Error(error)) => return Err(Error::Io(error)),
        }

        if file.metadata()?.len() == 0 {
            write_database_header(&mut file)?;
        }

        let scan = scan_and_recover(
            &mut file,
            config.max_batch_points,
            config.max_transaction_bytes,
        )?;
        Ok(Self {
            file,
            config,
            index: scan.index,
            catalog: scan.catalog,
            commits: scan.commits,
            points: scan.points,
            catalog_records: scan.catalog_records,
            recovered_tail_bytes: scan.recovered_tail_bytes,
            bytes_since_sync: 0,
            poisoned: false,
        })
    }

    /// Appends one atomic, checksummed batch.
    pub fn append(&mut self, points: &[Point]) -> Result<Commit> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if points.len() > self.config.max_batch_points {
            return Err(Error::BatchTooLarge {
                points: points.len(),
                maximum: self.config.max_batch_points,
            });
        }
        if points.is_empty() {
            return Ok(Commit {
                frame_offset: self.file.seek(SeekFrom::End(0))?,
                points: 0,
                records: 0,
                bytes_written: 0,
                durable: self.bytes_since_sync == 0,
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
                self.file.sync_data()?;
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

        for point in points {
            self.index.entry(point.series_id).or_default().push(*point);
        }
        self.commits += 1;
        self.points += points.len() as u64;

        Ok(Commit {
            frame_offset,
            points: points.len(),
            records: 1,
            bytes_written,
            durable,
        })
    }

    /// Atomically commits catalog changes and point batches in one frame.
    pub fn commit(&mut self, transaction: Transaction) -> Result<Commit> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if transaction.is_empty() {
            return Ok(Commit {
                frame_offset: self.file.seek(SeekFrom::End(0))?,
                points: 0,
                records: 0,
                bytes_written: 0,
                durable: self.bytes_since_sync == 0,
            });
        }
        let point_count = transaction.point_count();
        if point_count > self.config.max_batch_points {
            return Err(Error::BatchTooLarge {
                points: point_count,
                maximum: self.config.max_batch_points,
            });
        }

        let candidate_catalog = self.catalog.validate_and_apply(&transaction.records)?;
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
        let frame_header = encode_frame_header(
            FRAME_KIND_TRANSACTION,
            record_count,
            payload_len,
            hash(&payload),
        );
        let bytes_written = (FRAME_HEADER_BYTES + payload.len()) as u64;
        let frame_offset = self.file.seek(SeekFrom::End(0))?;
        let durable = self.write_frame(&frame_header, &payload, bytes_written)?;

        for record in &transaction.records {
            if let Record::Points(points) = record {
                for point in points {
                    self.index.entry(point.series_id).or_default().push(*point);
                }
            }
        }
        self.catalog = candidate_catalog;
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
        })
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
                self.file.sync_data()?;
                self.bytes_since_sync = 0;
            }
            Ok(should_sync)
        })();
        match write_result {
            Ok(durable) => Ok(durable),
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
        }
    }

    /// Makes all prior appends durable according to the operating system.
    pub fn flush(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.file.sync_data()?;
        self.bytes_since_sync = 0;
        Ok(())
    }

    /// Flushes and closes the database.
    pub fn close(mut self) -> Result<()> {
        self.flush()
    }

    #[must_use]
    pub const fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Returns every revision in deterministic temporal order.
    #[must_use]
    pub fn query_history(&self, series_id: u64, start: i64, end: i64) -> Vec<Point> {
        let mut result: Vec<_> = self
            .index
            .get(&series_id)
            .into_iter()
            .flatten()
            .filter(|point| point.valid_time >= start && point.valid_time < end)
            .copied()
            .collect();
        result.sort_by_key(|point| (point.valid_time, point.knowledge_time, point.change_time));
        result
    }

    /// Returns the winning revision for each valid timestamp.
    #[must_use]
    pub fn query_latest(&self, series_id: u64, start: i64, end: i64) -> Vec<Point> {
        self.query_with_cutoffs(series_id, start, end, None, None)
    }

    /// Replays what was visible at one historical instant.
    #[must_use]
    pub fn query_as_of(&self, series_id: u64, start: i64, end: i64, as_of: i64) -> Vec<Point> {
        self.query_with_cutoffs(series_id, start, end, Some(as_of), Some(as_of))
    }

    /// Returns the latest correction within one forecast/optimization run.
    #[must_use]
    pub fn query_run(&self, series_id: u64, run_id: u128, start: i64, end: i64) -> Vec<Point> {
        let mut winners = BTreeMap::<i64, Point>::new();
        for point in self.index.get(&series_id).into_iter().flatten() {
            if point.run_id != run_id || point.valid_time < start || point.valid_time >= end {
                continue;
            }
            match winners.get(&point.valid_time) {
                Some(current) if current.change_time > point.change_time => {}
                _ => {
                    winners.insert(point.valid_time, *point);
                }
            }
        }
        winners.into_values().collect()
    }

    /// Exact-time plan-versus-actual alignment. Higher-level resampling uses
    /// persistent rollups before calling this primitive.
    #[must_use]
    pub fn compare_plan_to_actual(
        &self,
        planned_series_id: u64,
        actual_series_id: u64,
        run_id: u128,
        start: i64,
        end: i64,
    ) -> Vec<PlanOutcome> {
        let mut aligned = BTreeMap::<i64, (Option<Point>, Option<Point>)>::new();
        for point in self.query_run(planned_series_id, run_id, start, end) {
            aligned.entry(point.valid_time).or_default().0 = Some(point);
        }
        for point in self.query_run(actual_series_id, 0, start, end) {
            aligned.entry(point.valid_time).or_default().1 = Some(point);
        }
        aligned
            .into_iter()
            .map(|(valid_time, (planned, actual))| PlanOutcome {
                valid_time,
                difference: planned
                    .zip(actual)
                    .map(|(planned, actual)| actual.value - planned.value),
                planned,
                actual,
            })
            .collect()
    }

    /// Separates forecast issue-time and correction-time cutoffs for strict
    /// point-in-time backtests.
    #[must_use]
    pub fn query_with_cutoffs(
        &self,
        series_id: u64,
        start: i64,
        end: i64,
        maximum_knowledge_time: Option<i64>,
        maximum_change_time: Option<i64>,
    ) -> Vec<Point> {
        let mut winners = BTreeMap::<i64, Point>::new();
        for point in self.index.get(&series_id).into_iter().flatten() {
            if point.valid_time < start || point.valid_time >= end {
                continue;
            }
            if maximum_knowledge_time.is_some_and(|cutoff| point.knowledge_time > cutoff)
                || maximum_change_time.is_some_and(|cutoff| point.change_time > cutoff)
            {
                continue;
            }

            let candidate_key = (point.knowledge_time, point.change_time);
            match winners.get(&point.valid_time) {
                Some(current) if (current.knowledge_time, current.change_time) > candidate_key => {}
                _ => {
                    winners.insert(point.valid_time, *point);
                }
            }
        }
        winners.into_values().collect()
    }

    /// Materializes fixed UTC gauge buckets from the winning revisions in a
    /// range. Persistent background rollups will use the same bucket state.
    #[must_use]
    pub fn rollup_gauge(
        &self,
        series_id: u64,
        start: i64,
        end: i64,
        resolution_micros: i64,
        max_gap_micros: i64,
    ) -> FixedGaugeRollup {
        FixedGaugeRollup::build(
            &self.query_latest(series_id, start, end),
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

    pub fn stats(&self) -> Result<Stats> {
        Ok(Stats {
            points: self.points,
            commits: self.commits,
            series: self.index.len(),
            catalog_records: self.catalog_records,
            file_bytes: self.file.metadata()?.len(),
            recovered_tail_bytes: self.recovered_tail_bytes,
        })
    }
}

fn validate_config(config: Config) -> Result<()> {
    if config.max_batch_points == 0 {
        return Err(Error::InvalidConfig("max_batch_points must be positive"));
    }
    if config.max_transaction_bytes < TRANSACTION_HEADER_BYTES + RECORD_HEADER_BYTES {
        return Err(Error::InvalidConfig(
            "max_transaction_bytes is too small for one record",
        ));
    }
    if matches!(config.durability, Durability::EveryBytes(0)) {
        return Err(Error::InvalidConfig(
            "EveryBytes durability threshold must be positive",
        ));
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
    commits: u64,
    points: u64,
    catalog_records: u64,
    recovered_tail_bytes: u64,
}

fn scan_and_recover(
    file: &mut File,
    max_batch_points: usize,
    max_transaction_bytes: usize,
) -> Result<Scan> {
    let mut database_header = [0_u8; DATABASE_HEADER_BYTES];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut database_header)?;
    if &database_header[..8] != DATABASE_MAGIC {
        return Err(Error::InvalidHeader);
    }
    let version = u16::from_le_bytes(database_header[8..10].try_into().unwrap());
    if version != DATABASE_VERSION {
        return Err(Error::UnsupportedVersion(version));
    }
    let expected_checksum = u32::from_le_bytes(database_header[12..16].try_into().unwrap());
    if hash(&database_header[..12]) != expected_checksum {
        return Err(Error::InvalidHeader);
    }

    let original_len = file.metadata()?.len();
    let mut offset = DATABASE_HEADER_BYTES as u64;
    let mut index = HashMap::<u64, Vec<Point>>::new();
    let mut catalog = Catalog::default();
    let mut commits = 0_u64;
    let mut points = 0_u64;
    let mut catalog_records = 0_u64;
    let mut recovered_tail_bytes = 0_u64;

    while offset < original_len {
        let remaining = original_len - offset;
        if remaining < FRAME_HEADER_BYTES as u64 {
            recovered_tail_bytes = remaining;
            truncate_recovered_tail(file, offset)?;
            break;
        }

        let mut frame_header = [0_u8; FRAME_HEADER_BYTES];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut frame_header)?;
        if &frame_header[..4] != FRAME_MAGIC {
            return corruption(offset, "invalid frame magic");
        }
        let frame_version = u16::from_le_bytes(frame_header[4..6].try_into().unwrap());
        if frame_version != FRAME_VERSION {
            return corruption(offset, "unsupported frame version");
        }
        let header_checksum = u32::from_le_bytes(frame_header[20..24].try_into().unwrap());
        if hash(&frame_header[..20]) != header_checksum {
            return corruption(offset, "frame header checksum mismatch");
        }

        let frame_kind = u16::from_le_bytes(frame_header[6..8].try_into().unwrap());
        let item_count = u32::from_le_bytes(frame_header[8..12].try_into().unwrap()) as usize;
        let payload_len = u32::from_le_bytes(frame_header[12..16].try_into().unwrap()) as usize;
        let payload_checksum = u32::from_le_bytes(frame_header[16..20].try_into().unwrap());
        if frame_kind == FRAME_KIND_LEGACY_POINTS {
            let expected_payload_len =
                item_count
                    .checked_mul(POINT_BYTES)
                    .ok_or_else(|| Error::Corruption {
                        offset,
                        reason: "frame point count overflows".to_owned(),
                    })?;
            if item_count > max_batch_points || payload_len != expected_payload_len {
                return corruption(offset, "invalid legacy point frame size");
            }
        } else if frame_kind == FRAME_KIND_TRANSACTION {
            if payload_len > max_transaction_bytes {
                return corruption(offset, "transaction frame exceeds configured maximum");
            }
        } else {
            return corruption(offset, "unknown frame kind");
        }

        let frame_len = FRAME_HEADER_BYTES as u64 + payload_len as u64;
        if remaining < frame_len {
            recovered_tail_bytes = remaining;
            truncate_recovered_tail(file, offset)?;
            break;
        }

        let mut payload = vec![0_u8; payload_len];
        file.read_exact(&mut payload)?;
        if hash(&payload) != payload_checksum {
            if remaining == frame_len {
                recovered_tail_bytes = frame_len;
                truncate_recovered_tail(file, offset)?;
                break;
            }
            return corruption(offset, "payload checksum mismatch before valid tail");
        }

        if frame_kind == FRAME_KIND_LEGACY_POINTS {
            for raw in payload.chunks_exact(POINT_BYTES) {
                let point = decode_point(raw);
                index.entry(point.series_id).or_default().push(point);
            }
            points += item_count as u64;
        } else {
            let records = decode_transaction(&payload, item_count, offset)?;
            catalog.apply_recovered(&records, offset)?;
            let recovered_point_count: usize = records
                .iter()
                .map(|record| match record {
                    Record::Points(recovered_points) => recovered_points.len(),
                    _ => 0,
                })
                .sum();
            if recovered_point_count > max_batch_points {
                return corruption(offset, "transaction point count exceeds maximum");
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
        }
        commits += 1;
        offset += frame_len;
    }

    Ok(Scan {
        index,
        catalog,
        commits,
        points,
        catalog_records,
        recovered_tail_bytes,
    })
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
    let record_count = u32::from_le_bytes(payload[8..12].try_into().unwrap()) as usize;
    if record_count != expected_records {
        return corruption(offset, "transaction record count mismatch");
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
    use super::{Config, Database, Durability, Point};
    use crate::{
        Entity, EntityId, Error, Plan, PlanStatus, RollupPolicy, Run, RunId, RunKind, RunStatus,
        SeriesDefinition, SeriesSemantics, Transaction,
    };
    use std::collections::BTreeMap;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
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
        let latest = database.query_latest(7, 0, 1_000);
        assert_eq!(latest.len(), 2);
        assert_eq!(latest[0].value, 3.0);
        assert_eq!(database.query_as_of(7, 0, 1_000, 15)[0].value, 1.0);
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
    fn incomplete_last_frame_is_removed_as_one_atomic_batch() {
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
        assert_eq!(database.query_latest(7, 0, 10).len(), 1);
        assert_eq!(database.stats().unwrap().file_bytes, first_length);
        assert!(database.stats().unwrap().recovered_tail_bytes > 0);
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
        assert_eq!(database.query_run(7, 9, 0, 1_000), vec![planned]);
        let comparison = database.compare_plan_to_actual(7, 7, 9, 0, 1_000);
        assert_eq!(comparison.len(), 1);
        assert_eq!(comparison[0].difference, Some(-200.0));
        assert_eq!(database.stats().unwrap().catalog_records, 4);
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
        assert!(recovered.query_latest(7, 0, 1_000).is_empty());
    }
}
