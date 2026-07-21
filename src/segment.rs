use crate::storage::sync_parent_directory;
use crate::{Error, Point, Result};
use crc32fast::hash;
use lz4_flex::block::{compress_prepend_size, decompress};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const SEGMENT_MAGIC: &[u8; 8] = b"WSEG0001";
const SEGMENT_VERSION: u16 = 1;
const SEGMENT_HEADER_BYTES: usize = 40;
const BLOCK_MAGIC: &[u8; 4] = b"WBLK";
const BLOCK_VERSION: u16 = 1;
const BLOCK_HEADER_BYTES: usize = 56;
const BLOCK_ENCODING_COLUMN_V1: u8 = 1;
const COMPRESSION_RAW: u8 = 0;
const COMPRESSION_LZ4: u8 = 1;
const INDEX_MAGIC: &[u8; 4] = b"WIDX";
const INDEX_VERSION: u16 = 1;
const INDEX_HEADER_BYTES: usize = 16;
const INDEX_ENTRY_BYTES: usize = 40;
const COLUMN_COUNT: usize = 7;
/// Hard ceiling for one decoded block. This matches the default append batch
/// limit and keeps a valid, highly compressible block from expanding into a
/// multi-gigabyte `Vec<Point>`.
const MAX_BLOCK_POINTS: u32 = 262_144;
/// Every encoded point consumes at least one varint byte in each of the
/// timestamp, valid-end, knowledge, change, run-id, and value columns plus two
/// in the quality/flags column, so eight bytes is a hard lower bound per point.
const MIN_ENCODED_POINT_BYTES: u64 = 8;
/// A conservative per-point ceiling: four ten-byte i64 varints, a 19-byte
/// u128 run-id varint, a ten-byte value varint, and up to twenty bytes for
/// quality and flags. The writer never exceeds it for any point.
const MAX_ENCODED_POINT_BYTES: u64 = 89;
/// The LZ4 block format cannot expand one compressed byte into more than 255
/// output bytes, so a payload bounds its decompressed size by this ratio.
const MAX_LZ4_RATIO: u64 = 255;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentStats {
    pub blocks: u32,
    pub points: u64,
    pub stored_bytes: u64,
    pub logical_point_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IndexEntry {
    series_id: u64,
    min_time: i64,
    max_time: i64,
    offset: u64,
    length: u32,
    points: u32,
}

/// An immutable, indexed raw-point segment.
pub struct Segment {
    file: File,
    index: Vec<IndexEntry>,
    stats: SegmentStats,
}

impl Segment {
    /// Creates a new segment without replacing an existing target.
    pub fn create(
        path: impl AsRef<Path>,
        points: &[Point],
        block_points: usize,
    ) -> Result<SegmentStats> {
        if block_points == 0 || block_points > MAX_BLOCK_POINTS as usize {
            return Err(Error::InvalidConfig(
                "segment block_points must be in 1..=262144",
            ));
        }
        let path = path.as_ref();
        if path.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "segment target already exists",
            )));
        }
        let temporary = temporary_path(path)?;
        let result = write_temporary_segment(&temporary, points, block_points);
        let stats = match result {
            Ok(stats) => stats,
            Err(error) => {
                let _ = std::fs::remove_file(&temporary);
                return Err(error);
            }
        };

        // A hard link publishes without overwriting an existing name. If power
        // is lost before the temporary link is removed, both names reference
        // the same fully synced immutable inode and recovery can safely clean it.
        if let Err(error) = std::fs::hard_link(&temporary, path) {
            let _ = std::fs::remove_file(&temporary);
            return Err(Error::Io(error));
        }
        sync_parent_directory(path)?;
        std::fs::remove_file(&temporary)?;
        sync_parent_directory(path)?;
        Ok(stats)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < (SEGMENT_HEADER_BYTES + INDEX_HEADER_BYTES) as u64 {
            return corruption(0, "segment is too small");
        }

        let mut header = [0_u8; SEGMENT_HEADER_BYTES];
        file.read_exact(&mut header)?;
        if &header[..8] != SEGMENT_MAGIC {
            return corruption(0, "invalid segment magic");
        }
        let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
        if version != SEGMENT_VERSION {
            return corruption(0, "unsupported segment version");
        }
        let expected_header_crc = u32::from_le_bytes(header[36..40].try_into().unwrap());
        if hash(&header[..36]) != expected_header_crc {
            return corruption(0, "segment header checksum mismatch");
        }
        let block_count = u32::from_le_bytes(header[12..16].try_into().unwrap());
        let point_count = u64::from_le_bytes(header[16..24].try_into().unwrap());
        let index_offset = u64::from_le_bytes(header[24..32].try_into().unwrap());
        let index_bytes = u32::from_le_bytes(header[32..36].try_into().unwrap()) as u64;
        if index_offset < SEGMENT_HEADER_BYTES as u64
            || index_offset
                .checked_add(index_bytes)
                .is_none_or(|end| end != file_len)
        {
            return corruption(0, "invalid segment index bounds");
        }

        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_header = [0_u8; INDEX_HEADER_BYTES];
        file.read_exact(&mut index_header)?;
        if &index_header[..4] != INDEX_MAGIC {
            return corruption(index_offset, "invalid segment index magic");
        }
        let index_version = u16::from_le_bytes(index_header[4..6].try_into().unwrap());
        let entry_size = u16::from_le_bytes(index_header[6..8].try_into().unwrap()) as usize;
        let entry_count = u32::from_le_bytes(index_header[8..12].try_into().unwrap());
        let expected_index_crc = u32::from_le_bytes(index_header[12..16].try_into().unwrap());
        if index_version != INDEX_VERSION
            || entry_size != INDEX_ENTRY_BYTES
            || entry_count != block_count
        {
            return corruption(index_offset, "unsupported segment index layout");
        }
        let expected_index_bytes =
            INDEX_HEADER_BYTES as u64 + u64::from(entry_count) * INDEX_ENTRY_BYTES as u64;
        if index_bytes != expected_index_bytes {
            return corruption(index_offset, "segment index length mismatch");
        }
        let mut entries = vec![0_u8; entry_count as usize * INDEX_ENTRY_BYTES];
        file.read_exact(&mut entries)?;
        if hash(&entries) != expected_index_crc {
            return corruption(index_offset, "segment index checksum mismatch");
        }
        let index: Vec<_> = entries
            .chunks_exact(INDEX_ENTRY_BYTES)
            .map(decode_index)
            .collect();
        validate_index(&index, index_offset, point_count)?;

        Ok(Self {
            file,
            index,
            stats: SegmentStats {
                blocks: block_count,
                points: point_count,
                stored_bytes: file_len,
                logical_point_bytes: point_count.saturating_mul(72),
            },
        })
    }

    #[must_use]
    pub const fn stats(&self) -> SegmentStats {
        self.stats
    }

    /// Reads one series/time range, touching only overlapping indexed blocks.
    pub fn query(&mut self, series_id: u64, start: i64, end: i64) -> Result<Vec<Point>> {
        let entries: Vec<_> = self
            .index
            .iter()
            .filter(|entry| {
                entry.series_id == series_id && entry.max_time >= start && entry.min_time < end
            })
            .copied()
            .collect();
        let mut result = Vec::new();
        for entry in entries {
            let points = read_block(&mut self.file, entry)?;
            result.extend(
                points
                    .into_iter()
                    .filter(|point| point.valid_time >= start && point.valid_time < end),
            );
        }
        Ok(result)
    }
}

fn write_temporary_segment(
    path: &Path,
    points: &[Point],
    block_points: usize,
) -> Result<SegmentStats> {
    let mut ordered = points.to_vec();
    ordered.sort_by_key(|point| {
        (
            point.series_id,
            point.valid_time,
            point.knowledge_time,
            point.change_time,
        )
    });
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)?;
    file.write_all(&[0_u8; SEGMENT_HEADER_BYTES])?;

    let mut index = Vec::new();
    let mut series_start = 0;
    while series_start < ordered.len() {
        let series_id = ordered[series_start].series_id;
        let mut series_end = series_start + 1;
        while series_end < ordered.len() && ordered[series_end].series_id == series_id {
            series_end += 1;
        }
        for block in ordered[series_start..series_end].chunks(block_points) {
            let offset = file.stream_position()?;
            let encoded = encode_columns(block)?;
            let compressed = compress_prepend_size(&encoded);
            let (compression, payload) = if compressed.len() < encoded.len() {
                (COMPRESSION_LZ4, compressed)
            } else {
                (COMPRESSION_RAW, encoded)
            };
            let payload_len = u32::try_from(payload.len())
                .map_err(|_| Error::Serialization("segment block is too large".to_owned()))?;
            let uncompressed_len = u32::try_from(if compression == COMPRESSION_RAW {
                payload.len()
            } else {
                // The size prefix is the first four compressed bytes.
                u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize
            })
            .map_err(|_| Error::Serialization("segment block is too large".to_owned()))?;
            let point_count = u32::try_from(block.len())
                .map_err(|_| Error::Serialization("too many block points".to_owned()))?;
            let min_time = block.first().unwrap().valid_time;
            let max_time = block.last().unwrap().valid_time;
            let header = encode_block_header(
                compression,
                series_id,
                point_count,
                uncompressed_len,
                payload_len,
                hash(&payload),
                min_time,
                max_time,
            );
            file.write_all(&header)?;
            file.write_all(&payload)?;
            let length = u32::try_from(BLOCK_HEADER_BYTES + payload.len())
                .map_err(|_| Error::Serialization("segment block is too large".to_owned()))?;
            index.push(IndexEntry {
                series_id,
                min_time,
                max_time,
                offset,
                length,
                points: point_count,
            });
        }
        series_start = series_end;
    }

    let index_offset = file.stream_position()?;
    let mut index_entries = Vec::with_capacity(index.len() * INDEX_ENTRY_BYTES);
    for entry in &index {
        encode_index(*entry, &mut index_entries);
    }
    let mut index_header = [0_u8; INDEX_HEADER_BYTES];
    index_header[..4].copy_from_slice(INDEX_MAGIC);
    index_header[4..6].copy_from_slice(&INDEX_VERSION.to_le_bytes());
    index_header[6..8].copy_from_slice(&(INDEX_ENTRY_BYTES as u16).to_le_bytes());
    index_header[8..12].copy_from_slice(
        &u32::try_from(index.len())
            .map_err(|_| Error::Serialization("too many segment blocks".to_owned()))?
            .to_le_bytes(),
    );
    index_header[12..16].copy_from_slice(&hash(&index_entries).to_le_bytes());
    file.write_all(&index_header)?;
    file.write_all(&index_entries)?;

    let index_bytes = u32::try_from(INDEX_HEADER_BYTES + index_entries.len())
        .map_err(|_| Error::Serialization("segment index is too large".to_owned()))?;
    let mut segment_header = [0_u8; SEGMENT_HEADER_BYTES];
    segment_header[..8].copy_from_slice(SEGMENT_MAGIC);
    segment_header[8..10].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
    segment_header[12..16].copy_from_slice(
        &u32::try_from(index.len())
            .map_err(|_| Error::Serialization("too many segment blocks".to_owned()))?
            .to_le_bytes(),
    );
    segment_header[16..24].copy_from_slice(&(points.len() as u64).to_le_bytes());
    segment_header[24..32].copy_from_slice(&index_offset.to_le_bytes());
    segment_header[32..36].copy_from_slice(&index_bytes.to_le_bytes());
    let header_crc = hash(&segment_header[..36]);
    segment_header[36..40].copy_from_slice(&header_crc.to_le_bytes());
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&segment_header)?;
    file.sync_all()?;
    let stored_bytes = file.metadata()?.len();

    Ok(SegmentStats {
        blocks: index.len() as u32,
        points: points.len() as u64,
        stored_bytes,
        logical_point_bytes: (points.len() as u64).saturating_mul(72),
    })
}

fn encode_columns(points: &[Point]) -> Result<Vec<u8>> {
    debug_assert!(!points.is_empty());
    let mut columns: Vec<Vec<u8>> = (0..COLUMN_COUNT).map(|_| Vec::new()).collect();

    columns[0].extend_from_slice(&points[0].valid_time.to_le_bytes());
    if points.len() > 1 {
        let mut previous_delta = checked_delta(points[1].valid_time, points[0].valid_time)?;
        write_signed(previous_delta, &mut columns[0]);
        for pair in points[1..].windows(2) {
            let delta = checked_delta(pair[1].valid_time, pair[0].valid_time)?;
            let delta_of_delta = checked_delta(delta, previous_delta)?;
            write_signed(delta_of_delta, &mut columns[0]);
            previous_delta = delta;
        }
    }

    let mut previous_value_bits = 0_u64;
    for (index, point) in points.iter().enumerate() {
        write_signed(
            checked_delta(point.valid_time_end, point.valid_time)?,
            &mut columns[1],
        );
        write_signed(
            checked_delta(point.knowledge_time, point.valid_time)?,
            &mut columns[2],
        );
        write_signed(
            checked_delta(point.change_time, point.knowledge_time)?,
            &mut columns[3],
        );
        write_unsigned_u128(point.run_id, &mut columns[4]);
        let bits = point.value.to_bits();
        if index == 0 {
            columns[5].extend_from_slice(&bits.to_le_bytes());
        } else {
            write_unsigned_u64(bits ^ previous_value_bits, &mut columns[5]);
        }
        previous_value_bits = bits;
        write_unsigned_u64(u64::from(point.quality), &mut columns[6]);
        write_unsigned_u64(u64::from(point.flags), &mut columns[6]);
    }

    let mut encoded = Vec::new();
    for column in columns {
        let length = u32::try_from(column.len())
            .map_err(|_| Error::Serialization("segment column is too large".to_owned()))?;
        encoded.extend_from_slice(&length.to_le_bytes());
        encoded.extend_from_slice(&column);
    }
    Ok(encoded)
}

fn decode_columns(encoded: &[u8], series_id: u64, count: usize, offset: u64) -> Result<Vec<Point>> {
    if count == 0 {
        return corruption(offset, "zero-point segment block");
    }
    // Bound the untrusted point count by the payload that is actually present
    // before reserving any point-sized capacity.
    if minimum_encoded_bytes(count as u64) > encoded.len() as u64 {
        return corruption(offset, "block point count exceeds payload capacity");
    }
    let columns = split_columns(encoded, offset)?;
    let mut timestamp_cursor = 0;
    if columns[0].len() < 8 {
        return corruption(offset, "truncated first timestamp");
    }
    let mut valid_times = Vec::with_capacity(count);
    let first = i64::from_le_bytes(columns[0][..8].try_into().unwrap());
    valid_times.push(first);
    timestamp_cursor += 8;
    if count > 1 {
        let mut previous_delta = read_signed(columns[0], &mut timestamp_cursor, offset)?;
        let second = first
            .checked_add(previous_delta)
            .ok_or_else(|| corrupt_error(offset, "timestamp delta overflows"))?;
        valid_times.push(second);
        while valid_times.len() < count {
            let delta_of_delta = read_signed(columns[0], &mut timestamp_cursor, offset)?;
            previous_delta = previous_delta
                .checked_add(delta_of_delta)
                .ok_or_else(|| corrupt_error(offset, "timestamp delta-of-delta overflows"))?;
            let next = valid_times
                .last()
                .unwrap()
                .checked_add(previous_delta)
                .ok_or_else(|| corrupt_error(offset, "timestamp overflows"))?;
            valid_times.push(next);
        }
    }
    require_consumed(columns[0], timestamp_cursor, offset, "timestamp")?;

    let mut cursors = [0_usize; COLUMN_COUNT];
    cursors[0] = timestamp_cursor;
    let mut points = Vec::with_capacity(count);
    let mut previous_value_bits = 0_u64;
    for (index, valid_time) in valid_times.into_iter().enumerate() {
        let valid_end_delta = read_signed(columns[1], &mut cursors[1], offset)?;
        let knowledge_delta = read_signed(columns[2], &mut cursors[2], offset)?;
        let knowledge_time = valid_time
            .checked_add(knowledge_delta)
            .ok_or_else(|| corrupt_error(offset, "knowledge timestamp overflows"))?;
        let change_delta = read_signed(columns[3], &mut cursors[3], offset)?;
        let run_id = read_unsigned_u128(columns[4], &mut cursors[4], offset)?;
        let value_bits = if index == 0 {
            if columns[5].len() < 8 {
                return corruption(offset, "truncated first value");
            }
            cursors[5] = 8;
            u64::from_le_bytes(columns[5][..8].try_into().unwrap())
        } else {
            previous_value_bits ^ read_unsigned_u64(columns[5], &mut cursors[5], offset)?
        };
        previous_value_bits = value_bits;
        let quality = u32::try_from(read_unsigned_u64(columns[6], &mut cursors[6], offset)?)
            .map_err(|_| corrupt_error(offset, "quality exceeds u32"))?;
        let flags = u32::try_from(read_unsigned_u64(columns[6], &mut cursors[6], offset)?)
            .map_err(|_| corrupt_error(offset, "flags exceed u32"))?;
        points.push(Point {
            series_id,
            valid_time,
            valid_time_end: valid_time
                .checked_add(valid_end_delta)
                .ok_or_else(|| corrupt_error(offset, "valid end timestamp overflows"))?,
            knowledge_time,
            change_time: knowledge_time
                .checked_add(change_delta)
                .ok_or_else(|| corrupt_error(offset, "change timestamp overflows"))?,
            run_id,
            value: f64::from_bits(value_bits),
            quality,
            flags,
        });
    }
    for column in 1..COLUMN_COUNT {
        require_consumed(columns[column], cursors[column], offset, "point")?;
    }
    Ok(points)
}

fn split_columns(encoded: &[u8], offset: u64) -> Result<Vec<&[u8]>> {
    let mut cursor = 0_usize;
    let mut columns = Vec::with_capacity(COLUMN_COUNT);
    for _ in 0..COLUMN_COUNT {
        if cursor.checked_add(4).is_none_or(|end| end > encoded.len()) {
            return corruption(offset, "truncated segment column length");
        }
        let length = u32::from_le_bytes(encoded[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let end = cursor
            .checked_add(length)
            .ok_or_else(|| corrupt_error(offset, "segment column length overflows"))?;
        if end > encoded.len() {
            return corruption(offset, "truncated segment column");
        }
        columns.push(&encoded[cursor..end]);
        cursor = end;
    }
    if cursor != encoded.len() {
        return corruption(offset, "trailing segment column bytes");
    }
    Ok(columns)
}

fn read_block(file: &mut File, entry: IndexEntry) -> Result<Vec<Point>> {
    file.seek(SeekFrom::Start(entry.offset))?;
    let mut header = [0_u8; BLOCK_HEADER_BYTES];
    file.read_exact(&mut header)?;
    if &header[..4] != BLOCK_MAGIC {
        return corruption(entry.offset, "invalid block magic");
    }
    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != BLOCK_VERSION || header[7] != BLOCK_ENCODING_COLUMN_V1 {
        return corruption(entry.offset, "unsupported block encoding");
    }
    let expected_header_crc = u32::from_le_bytes(header[48..52].try_into().unwrap());
    if hash(&header[..48]) != expected_header_crc {
        return corruption(entry.offset, "block header checksum mismatch");
    }
    let compression = header[6];
    let series_id = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let points = u32::from_le_bytes(header[16..20].try_into().unwrap());
    let uncompressed_len = u32::from_le_bytes(header[20..24].try_into().unwrap()) as usize;
    let payload_len = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;
    let expected_payload_crc = u32::from_le_bytes(header[28..32].try_into().unwrap());
    let min_time = i64::from_le_bytes(header[32..40].try_into().unwrap());
    let max_time = i64::from_le_bytes(header[40..48].try_into().unwrap());
    if series_id != entry.series_id
        || points != entry.points
        || min_time != entry.min_time
        || max_time != entry.max_time
        || BLOCK_HEADER_BYTES + payload_len != entry.length as usize
    {
        return corruption(entry.offset, "block header disagrees with index");
    }
    // The claimed uncompressed length sizes the decompression buffer, so pin
    // it to what `points` column-encoded points can actually occupy before
    // trusting it.
    if (uncompressed_len as u64) < minimum_encoded_bytes(u64::from(points))
        || uncompressed_len as u64 > maximum_encoded_bytes(u64::from(points))
    {
        return corruption(entry.offset, "block uncompressed length is out of bounds");
    }
    let mut payload = vec![0_u8; payload_len];
    file.read_exact(&mut payload)?;
    if hash(&payload) != expected_payload_crc {
        return corruption(entry.offset, "block payload checksum mismatch");
    }
    let decoded = match compression {
        COMPRESSION_RAW => payload,
        COMPRESSION_LZ4 => {
            // The payload is `compress_prepend_size` output: a four-byte
            // little-endian size prefix followed by the raw LZ4 block. Size
            // the output from the validated header length instead of the
            // in-payload prefix, which would otherwise dictate an unbounded
            // allocation before decompression can fail.
            if payload.len() < 4 {
                return corruption(entry.offset, "LZ4 block is too short");
            }
            let prefix = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
            if prefix != uncompressed_len {
                return corruption(entry.offset, "LZ4 size prefix disagrees with block header");
            }
            // The per-point bound above still admits headers claiming roughly
            // `points * MAX_ENCODED_POINT_BYTES` output from a tiny payload,
            // so also pin the claim to the stream itself: the LZ4 block format
            // cannot expand one stored byte into more than MAX_LZ4_RATIO
            // output bytes, so a compressed stream (the payload minus its
            // four-byte size prefix) of real data is never smaller than
            // uncompressed_len / MAX_LZ4_RATIO and the writer's own output
            // always passes this check.
            let stream_len = (payload.len() - 4) as u64;
            if uncompressed_len as u64 > stream_len.saturating_mul(MAX_LZ4_RATIO) {
                return corruption(
                    entry.offset,
                    "block uncompressed length exceeds LZ4 capacity",
                );
            }
            decompress(&payload[4..], uncompressed_len).map_err(|error| Error::Corruption {
                offset: entry.offset,
                reason: format!("invalid LZ4 block: {error}"),
            })?
        }
        _ => return corruption(entry.offset, "unknown block compression"),
    };
    if decoded.len() != uncompressed_len {
        return corruption(entry.offset, "block uncompressed length mismatch");
    }
    decode_columns(&decoded, series_id, points as usize, entry.offset)
}

#[allow(clippy::too_many_arguments)]
fn encode_block_header(
    compression: u8,
    series_id: u64,
    points: u32,
    uncompressed_len: u32,
    payload_len: u32,
    payload_crc: u32,
    min_time: i64,
    max_time: i64,
) -> [u8; BLOCK_HEADER_BYTES] {
    let mut header = [0_u8; BLOCK_HEADER_BYTES];
    header[..4].copy_from_slice(BLOCK_MAGIC);
    header[4..6].copy_from_slice(&BLOCK_VERSION.to_le_bytes());
    header[6] = compression;
    header[7] = BLOCK_ENCODING_COLUMN_V1;
    header[8..16].copy_from_slice(&series_id.to_le_bytes());
    header[16..20].copy_from_slice(&points.to_le_bytes());
    header[20..24].copy_from_slice(&uncompressed_len.to_le_bytes());
    header[24..28].copy_from_slice(&payload_len.to_le_bytes());
    header[28..32].copy_from_slice(&payload_crc.to_le_bytes());
    header[32..40].copy_from_slice(&min_time.to_le_bytes());
    header[40..48].copy_from_slice(&max_time.to_le_bytes());
    let header_crc = hash(&header[..48]);
    header[48..52].copy_from_slice(&header_crc.to_le_bytes());
    header
}

fn encode_index(entry: IndexEntry, destination: &mut Vec<u8>) {
    destination.extend_from_slice(&entry.series_id.to_le_bytes());
    destination.extend_from_slice(&entry.min_time.to_le_bytes());
    destination.extend_from_slice(&entry.max_time.to_le_bytes());
    destination.extend_from_slice(&entry.offset.to_le_bytes());
    destination.extend_from_slice(&entry.length.to_le_bytes());
    destination.extend_from_slice(&entry.points.to_le_bytes());
}

fn decode_index(raw: &[u8]) -> IndexEntry {
    IndexEntry {
        series_id: u64::from_le_bytes(raw[0..8].try_into().unwrap()),
        min_time: i64::from_le_bytes(raw[8..16].try_into().unwrap()),
        max_time: i64::from_le_bytes(raw[16..24].try_into().unwrap()),
        offset: u64::from_le_bytes(raw[24..32].try_into().unwrap()),
        length: u32::from_le_bytes(raw[32..36].try_into().unwrap()),
        points: u32::from_le_bytes(raw[36..40].try_into().unwrap()),
    }
}

fn validate_index(index: &[IndexEntry], index_offset: u64, point_count: u64) -> Result<()> {
    let mut previous: Option<IndexEntry> = None;
    let mut indexed_points = 0_u64;
    let mut expected_offset = SEGMENT_HEADER_BYTES as u64;
    for entry in index {
        if entry.points == 0
            || entry.points > MAX_BLOCK_POINTS
            || entry.min_time > entry.max_time
            || entry.offset != expected_offset
            || entry
                .offset
                .checked_add(u64::from(entry.length))
                .is_none_or(|end| end > index_offset)
        {
            return corruption(index_offset, "invalid segment index entry");
        }
        // The block payload cannot decode to more points than its byte count
        // allows: even through LZ4 the payload expands at most MAX_LZ4_RATIO
        // times, and every decoded point needs MIN_ENCODED_POINT_BYTES.
        let payload_bytes = u64::from(entry.length).saturating_sub(BLOCK_HEADER_BYTES as u64);
        if minimum_encoded_bytes(u64::from(entry.points))
            > payload_bytes.saturating_mul(MAX_LZ4_RATIO)
        {
            return corruption(
                index_offset,
                "segment index point count exceeds block capacity",
            );
        }
        if let Some(previous) = previous
            && (entry.series_id, entry.min_time) < (previous.series_id, previous.min_time)
        {
            return corruption(index_offset, "segment index is not sorted");
        }
        indexed_points += u64::from(entry.points);
        expected_offset = entry.offset + u64::from(entry.length);
        previous = Some(*entry);
    }
    if indexed_points != point_count || expected_offset != index_offset {
        return corruption(
            index_offset,
            "segment index coverage or point count mismatch",
        );
    }
    Ok(())
}

/// The smallest possible encoded size of `points` column-encoded points: seven
/// u32 column lengths, the fixed eight-byte first timestamp and first value
/// (seven bytes beyond their one-byte varint minimum each), and at least
/// `MIN_ENCODED_POINT_BYTES` for every point.
fn minimum_encoded_bytes(points: u64) -> u64 {
    COLUMN_COUNT as u64 * 4 + 14 + points.saturating_mul(MIN_ENCODED_POINT_BYTES)
}

/// The largest possible encoded size of `points` column-encoded points: seven
/// u32 column lengths plus at most `MAX_ENCODED_POINT_BYTES` for every point.
fn maximum_encoded_bytes(points: u64) -> u64 {
    COLUMN_COUNT as u64 * 4 + points.saturating_mul(MAX_ENCODED_POINT_BYTES)
}

fn checked_delta(left: i64, right: i64) -> Result<i64> {
    left.checked_sub(right)
        .ok_or_else(|| Error::Serialization("timestamp delta overflows i64".to_owned()))
}

fn write_signed(value: i64, destination: &mut Vec<u8>) {
    let zigzag = ((value as u64) << 1) ^ ((value >> 63) as u64);
    write_unsigned_u64(zigzag, destination);
}

fn read_signed(source: &[u8], cursor: &mut usize, offset: u64) -> Result<i64> {
    let value = read_unsigned_u64(source, cursor, offset)?;
    Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
}

fn write_unsigned_u64(mut value: u64, destination: &mut Vec<u8>) {
    while value >= 0x80 {
        destination.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    destination.push(value as u8);
}

fn read_unsigned_u64(source: &[u8], cursor: &mut usize, offset: u64) -> Result<u64> {
    let value = read_unsigned_u128_limited(source, cursor, offset, 10)?;
    u64::try_from(value).map_err(|_| corrupt_error(offset, "u64 varint overflows"))
}

fn write_unsigned_u128(mut value: u128, destination: &mut Vec<u8>) {
    while value >= 0x80 {
        destination.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    destination.push(value as u8);
}

fn read_unsigned_u128(source: &[u8], cursor: &mut usize, offset: u64) -> Result<u128> {
    read_unsigned_u128_limited(source, cursor, offset, 19)
}

fn read_unsigned_u128_limited(
    source: &[u8],
    cursor: &mut usize,
    offset: u64,
    maximum_bytes: usize,
) -> Result<u128> {
    let mut value = 0_u128;
    for byte_index in 0..maximum_bytes {
        let Some(byte) = source.get(*cursor).copied() else {
            return corruption(offset, "truncated varint");
        };
        *cursor += 1;
        let bits = u128::from(byte & 0x7f);
        let shift = byte_index * 7;
        if shift >= 128 || (shift == 126 && bits > 3) {
            return corruption(offset, "varint overflows");
        }
        value |= bits << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    corruption(offset, "varint is too long")
}

fn require_consumed(source: &[u8], cursor: usize, offset: u64, column: &str) -> Result<()> {
    if cursor != source.len() {
        return corruption(offset, &format!("trailing {column} column bytes"));
    }
    Ok(())
}

fn temporary_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .ok_or_else(|| Error::InvalidModel("segment target needs a file name".to_owned()))?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    Ok(parent.join(format!(
        ".{}.tmp-{}-{nonce}",
        file_name.to_string_lossy(),
        std::process::id()
    )))
}

fn corruption<T>(offset: u64, reason: &str) -> Result<T> {
    Err(corrupt_error(offset, reason))
}

fn corrupt_error(offset: u64, reason: &str) -> Error {
    Error::Corruption {
        offset,
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BLOCK_HEADER_BYTES, COMPRESSION_LZ4, INDEX_ENTRY_BYTES, INDEX_HEADER_BYTES,
        MAX_BLOCK_POINTS, SEGMENT_HEADER_BYTES, Segment,
    };
    use crate::{Error, Point};
    use crc32fast::hash;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::Path;
    use tempfile::tempdir;

    fn points(count: usize) -> Vec<Point> {
        (0..count)
            .flat_map(|index| {
                [1_u64, 2_u64].map(|series_id| Point {
                    series_id,
                    valid_time: index as i64 * 1_000_000,
                    valid_time_end: index as i64 * 1_000_000,
                    knowledge_time: index as i64 * 1_000_000 + 10,
                    change_time: index as i64 * 1_000_000 + 20,
                    run_id: if index % 10 == 0 { 77 } else { 0 },
                    value: series_id as f64 * 100.0 + (index % 50) as f64,
                    quality: (index % 4) as u32,
                    flags: (index % 2) as u32,
                })
            })
            .collect()
    }

    /// Rewrites the claimed point count of the first block, keeping every
    /// checksum consistent so only the new structural bounds can reject it.
    fn claim_first_block_points(path: &Path, claimed: u32) {
        let mut bytes = std::fs::read(path).unwrap();
        let index_offset = u64::from_le_bytes(bytes[24..32].try_into().unwrap()) as usize;
        let entry_count = u32::from_le_bytes(
            bytes[index_offset + 8..index_offset + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        let entries_start = index_offset + INDEX_HEADER_BYTES;
        let entries_end = entries_start + entry_count * INDEX_ENTRY_BYTES;

        // Index entry point count.
        bytes[entries_start + 36..entries_start + 40].copy_from_slice(&claimed.to_le_bytes());
        // Matching block header point count plus its checksum.
        let block_offset = u64::from_le_bytes(
            bytes[entries_start + 24..entries_start + 32]
                .try_into()
                .unwrap(),
        ) as usize;
        bytes[block_offset + 16..block_offset + 20].copy_from_slice(&claimed.to_le_bytes());
        let block_crc = hash(&bytes[block_offset..block_offset + 48]);
        bytes[block_offset + 48..block_offset + 52].copy_from_slice(&block_crc.to_le_bytes());
        // Index checksum.
        let index_crc = hash(&bytes[entries_start..entries_end]);
        bytes[index_offset + 12..index_offset + 16].copy_from_slice(&index_crc.to_le_bytes());
        // Segment header total point count plus its checksum.
        let mut total = 0_u64;
        for entry in 0..entry_count {
            let offset = entries_start + entry * INDEX_ENTRY_BYTES;
            total += u64::from(u32::from_le_bytes(
                bytes[offset + 36..offset + 40].try_into().unwrap(),
            ));
        }
        bytes[16..24].copy_from_slice(&total.to_le_bytes());
        let header_crc = hash(&bytes[..36]);
        bytes[36..40].copy_from_slice(&header_crc.to_le_bytes());
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn compressed_segment_round_trips_and_prunes_ranges() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("raw-1.seg");
        let input = points(10_000);
        let stats = Segment::create(&path, &input, 512).unwrap();
        assert_eq!(stats.points, 20_000);
        assert!(stats.blocks > 2);
        assert!(stats.stored_bytes < stats.logical_point_bytes / 2);

        let mut segment = Segment::open(&path).unwrap();
        assert_eq!(segment.stats(), stats);
        let selected = segment.query(2, 123_000_000, 140_000_000).unwrap();
        let expected: Vec<_> = input
            .into_iter()
            .filter(|point| {
                point.series_id == 2
                    && point.valid_time >= 123_000_000
                    && point.valid_time < 140_000_000
            })
            .collect();
        assert_eq!(selected, expected);
    }

    #[test]
    fn payload_corruption_is_detected_on_read() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("corrupt.seg");
        Segment::create(&path, &points(100), 100).unwrap();
        let segment = Segment::open(&path).unwrap();
        let payload_offset = segment.index[0].offset + BLOCK_HEADER_BYTES as u64 + 1;
        drop(segment);

        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(payload_offset)).unwrap();
        file.write_all(&[0xAA]).unwrap();
        file.sync_all().unwrap();

        let mut segment = Segment::open(&path).unwrap();
        assert!(matches!(
            segment.query(1, i64::MIN, i64::MAX),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn huge_index_point_count_is_rejected_on_open() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("huge-index.seg");
        Segment::create(&path, &points(100), 1_000).unwrap();

        // Claim u32::MAX points in one small block; opening must fail with a
        // corruption error instead of reserving tens of gigabytes on query.
        claim_first_block_points(&path, u32::MAX);
        assert!(matches!(
            Segment::open(&path),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn block_point_limit_bounds_writer_and_reader_allocations() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("bounded.seg");
        assert!(matches!(
            Segment::create(&path, &points(10), MAX_BLOCK_POINTS as usize + 1),
            Err(Error::InvalidConfig(_))
        ));

        Segment::create(&path, &points(100), 1_000).unwrap();
        claim_first_block_points(&path, MAX_BLOCK_POINTS + 1);
        assert!(matches!(
            Segment::open(&path),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn inflated_block_point_count_is_rejected_on_query() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("inflated-block.seg");
        Segment::create(&path, &points(100), 1_000).unwrap();

        // Claim more points than the decoded payload can hold while staying
        // under the index's compression-ratio bound, so the block is only
        // rejected when its columns are decoded.
        claim_first_block_points(&path, 1_000);
        let mut segment = Segment::open(&path).unwrap();
        assert!(matches!(
            segment.query(1, i64::MIN, i64::MAX),
            Err(Error::Corruption { .. })
        ));
    }

    /// Rewrites the first block's claimed uncompressed length and LZ4 size
    /// prefix, keeping every checksum consistent so only the decompression
    /// bounds can reject it.
    fn tamper_first_block_lz4(path: &Path, claimed_length: Option<u32>, claimed_prefix: u32) {
        let mut bytes = std::fs::read(path).unwrap();
        let block = SEGMENT_HEADER_BYTES;
        assert_eq!(bytes[block + 6], COMPRESSION_LZ4);
        if let Some(claimed) = claimed_length {
            bytes[block + 20..block + 24].copy_from_slice(&claimed.to_le_bytes());
        }
        let payload_len =
            u32::from_le_bytes(bytes[block + 24..block + 28].try_into().unwrap()) as usize;
        let payload_start = block + BLOCK_HEADER_BYTES;
        bytes[payload_start..payload_start + 4].copy_from_slice(&claimed_prefix.to_le_bytes());
        let payload_crc = hash(&bytes[payload_start..payload_start + payload_len]);
        bytes[block + 28..block + 32].copy_from_slice(&payload_crc.to_le_bytes());
        let header_crc = hash(&bytes[block..block + 48]);
        bytes[block + 48..block + 52].copy_from_slice(&header_crc.to_le_bytes());
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn huge_block_uncompressed_length_is_rejected_on_query() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("bomb-header.seg");
        Segment::create(&path, &points(3_000), 4_096).unwrap();

        // Both the block header and the LZ4 size prefix claim ~4 GiB; the
        // structural bound on the header length must reject the block before
        // any decompression buffer is sized.
        tamper_first_block_lz4(&path, Some(u32::MAX), u32::MAX);
        let mut segment = Segment::open(&path).unwrap();
        assert!(matches!(
            segment.query(1, i64::MIN, i64::MAX),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn decompression_bomb_within_point_bounds_is_rejected_on_query() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("bomb-ratio.seg");
        Segment::create(&path, &points(3_000), 4_096).unwrap();

        // Claim a point count that passes the index's compression-ratio bound
        // and an uncompressed length inside the per-point window it implies,
        // yet far beyond what the actual payload can legally expand to: the
        // stream-ratio bound must reject the block before the decompression
        // buffer is sized.
        let bytes = std::fs::read(&path).unwrap();
        let block = SEGMENT_HEADER_BYTES;
        let payload_len = u32::from_le_bytes(bytes[block + 24..block + 28].try_into().unwrap());
        let claimed_length = payload_len * 255;
        let claimed_points = claimed_length / 64;
        tamper_first_block_lz4(&path, Some(claimed_length), claimed_length);
        claim_first_block_points(&path, claimed_points);

        let mut segment = Segment::open(&path).unwrap();
        assert!(matches!(
            segment.query(1, i64::MIN, i64::MAX),
            Err(Error::Corruption { reason, .. }) if reason.contains("exceeds LZ4 capacity")
        ));
    }

    #[test]
    fn lying_lz4_size_prefix_is_rejected_on_query() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("bomb-prefix.seg");
        Segment::create(&path, &points(3_000), 4_096).unwrap();

        // Only the in-payload size prefix claims ~4 GiB; it must be rejected
        // against the validated header length instead of sizing an allocation.
        tamper_first_block_lz4(&path, None, u32::MAX);
        let mut segment = Segment::open(&path).unwrap();
        assert!(matches!(
            segment.query(1, i64::MIN, i64::MAX),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn existing_segment_is_never_replaced() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("existing.seg");
        Segment::create(&path, &points(10), 10).unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(matches!(
            Segment::create(&path, &points(20), 10),
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}
