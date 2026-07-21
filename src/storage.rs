use crate::FixedGaugeRollup;
use crate::error::{Error, Result};
use crc32fast::hash;
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const DATABASE_MAGIC: &[u8; 8] = b"WATTDB01";
const DATABASE_VERSION: u16 = 1;
const DATABASE_HEADER_BYTES: usize = 16;
const FRAME_MAGIC: &[u8; 4] = b"WBAT";
const FRAME_VERSION: u16 = 1;
const FRAME_HEADER_BYTES: usize = 24;
const POINT_BYTES: usize = 72;

/// A value in three-dimensional energy time plus provenance.
///
/// All timestamps are UTC microseconds since Unix epoch. `valid_time` is when
/// the value applies, `knowledge_time` is when it became known, and
/// `change_time` is when this revision was recorded. `valid_time_end` equals
/// `valid_time` for an instant and is exclusive for interval values.
#[derive(Clone, Copy, Debug, PartialEq)]
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            durability: Durability::Always,
            max_batch_points: 262_144,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Commit {
    pub frame_offset: u64,
    pub points: usize,
    pub bytes_written: u64,
    pub durable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Stats {
    pub points: u64,
    pub commits: u64,
    pub series: usize,
    pub file_bytes: u64,
    pub recovered_tail_bytes: u64,
}

/// A single-writer embedded database.
pub struct Database {
    file: File,
    config: Config,
    index: HashMap<u64, Vec<Point>>,
    commits: u64,
    points: u64,
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
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        if file.metadata()?.len() == 0 {
            write_database_header(&mut file)?;
        }

        let scan = scan_and_recover(&mut file, config.max_batch_points)?;
        Ok(Self {
            file,
            config,
            index: scan.index,
            commits: scan.commits,
            points: scan.points,
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
        let frame_header = encode_frame_header(point_count, payload_len_u32, hash(&payload));
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
            bytes_written,
            durable,
        })
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

    pub fn stats(&self) -> Result<Stats> {
        Ok(Stats {
            points: self.points,
            commits: self.commits,
            series: self.index.len(),
            file_bytes: self.file.metadata()?.len(),
            recovered_tail_bytes: self.recovered_tail_bytes,
        })
    }
}

fn validate_config(config: Config) -> Result<()> {
    if config.max_batch_points == 0 {
        return Err(Error::InvalidConfig("max_batch_points must be positive"));
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
    commits: u64,
    points: u64,
    recovered_tail_bytes: u64,
}

fn scan_and_recover(file: &mut File, max_batch_points: usize) -> Result<Scan> {
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
    let mut commits = 0_u64;
    let mut points = 0_u64;
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

        let point_count = u32::from_le_bytes(frame_header[8..12].try_into().unwrap()) as usize;
        let payload_len = u32::from_le_bytes(frame_header[12..16].try_into().unwrap()) as usize;
        let payload_checksum = u32::from_le_bytes(frame_header[16..20].try_into().unwrap());
        let expected_payload_len =
            point_count
                .checked_mul(POINT_BYTES)
                .ok_or_else(|| Error::Corruption {
                    offset,
                    reason: "frame point count overflows".to_owned(),
                })?;
        if point_count > max_batch_points || payload_len != expected_payload_len {
            return corruption(offset, "invalid frame size");
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

        for raw in payload.chunks_exact(POINT_BYTES) {
            let point = decode_point(raw);
            index.entry(point.series_id).or_default().push(point);
        }
        commits += 1;
        points += point_count as u64;
        offset += frame_len;
    }

    Ok(Scan {
        index,
        commits,
        points,
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

fn encode_frame_header(point_count: u32, payload_len: u32, payload_checksum: u32) -> [u8; 24] {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    header[..4].copy_from_slice(FRAME_MAGIC);
    header[4..6].copy_from_slice(&FRAME_VERSION.to_le_bytes());
    // Bytes 6..8 are reserved frame flags.
    header[8..12].copy_from_slice(&point_count.to_le_bytes());
    header[12..16].copy_from_slice(&payload_len.to_le_bytes());
    header[16..20].copy_from_slice(&payload_checksum.to_le_bytes());
    let header_checksum = hash(&header[..20]);
    header[20..24].copy_from_slice(&header_checksum.to_le_bytes());
    header
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
    use crate::Error;
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

    #[test]
    fn round_trips_batches_and_revisions() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("round-trip.wattdb");
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
    fn incomplete_last_frame_is_removed_as_one_atomic_batch() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("torn-tail.wattdb");
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
        let path = directory.path().join("corrupt.wattdb");
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
}
