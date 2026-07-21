use crate::{Error, GaugeBucket, Result, Sample};
use crc32fast::hash;
use lz4_flex::block::{compress_prepend_size, decompress};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC: &[u8; 8] = b"WROL0001";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = 48;
const BUCKET_BYTES: usize = 104;
const COMPRESSION_RAW: u8 = 0;
const COMPRESSION_LZ4: u8 = 1;
/// A fixed shard cap keeps both the decompressed byte buffer and the decoded
/// bucket vector bounded even for a valid, maximally compressed payload.
const MAX_ROLLUP_BUCKETS: u32 = 262_144;
/// The LZ4 block format cannot expand one compressed byte into more than 255
/// output bytes, so a payload bounds its decompressed size by this ratio.
const MAX_LZ4_RATIO: u64 = 255;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RollupSegmentStats {
    pub buckets: u32,
    pub stored_bytes: u64,
    pub logical_bytes: u64,
    pub min_start: Option<i64>,
    pub max_end: Option<i64>,
}

/// An immutable, checksummed file of closed gauge aggregate states.
pub struct RollupSegment {
    buckets: Vec<GaugeBucket>,
    stats: RollupSegmentStats,
}

impl RollupSegment {
    /// Publishes a fully synced segment without replacing an existing target.
    pub fn create(path: impl AsRef<Path>, buckets: &[GaugeBucket]) -> Result<RollupSegmentStats> {
        if buckets.len() > MAX_ROLLUP_BUCKETS as usize {
            return Err(Error::InvalidModel(
                "rollup segment exceeds 262144 buckets".to_owned(),
            ));
        }
        validate_buckets(buckets, 0)?;
        let path = path.as_ref();
        if path.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "rollup segment target already exists",
            )));
        }
        let temporary = temporary_path(path)?;
        let stats = match write_temporary(&temporary, buckets) {
            Ok(stats) => stats,
            Err(error) => {
                let _ = std::fs::remove_file(&temporary);
                return Err(error);
            }
        };
        if let Err(error) = std::fs::hard_link(&temporary, path) {
            let _ = std::fs::remove_file(&temporary);
            return Err(Error::Io(error));
        }
        sync_parent(path)?;
        std::fs::remove_file(&temporary)?;
        sync_parent(path)?;
        Ok(stats)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < HEADER_BYTES as u64 {
            return corruption(0, "rollup segment is too small");
        }
        let mut header = [0_u8; HEADER_BYTES];
        file.read_exact(&mut header)?;
        if &header[..8] != MAGIC {
            return corruption(0, "invalid rollup segment magic");
        }
        let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
        if version != VERSION {
            return corruption(0, "unsupported rollup segment version");
        }
        if hash(&header[..44]) != u32::from_le_bytes(header[44..48].try_into().unwrap()) {
            return corruption(0, "rollup segment header checksum mismatch");
        }
        let compression = header[10];
        let bucket_count = u32::from_le_bytes(header[12..16].try_into().unwrap());
        let uncompressed_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let stored_len = u32::from_le_bytes(header[20..24].try_into().unwrap()) as usize;
        let payload_crc = u32::from_le_bytes(header[24..28].try_into().unwrap());
        let min_start = i64::from_le_bytes(header[28..36].try_into().unwrap());
        let max_end = i64::from_le_bytes(header[36..44].try_into().unwrap());
        if bucket_count > MAX_ROLLUP_BUCKETS {
            return corruption(0, "rollup bucket count exceeds implementation limit");
        }
        let expected_uncompressed = (bucket_count as usize)
            .checked_mul(BUCKET_BYTES)
            .ok_or_else(|| Error::Corruption {
                offset: 0,
                reason: "rollup bucket count overflows".to_owned(),
            })?;
        if uncompressed_len != expected_uncompressed
            || file_len != (HEADER_BYTES + stored_len) as u64
        {
            return corruption(0, "invalid rollup segment lengths");
        }
        let mut payload = vec![0_u8; stored_len];
        file.read_exact(&mut payload)?;
        if hash(&payload) != payload_crc {
            return corruption(HEADER_BYTES as u64, "rollup payload checksum mismatch");
        }
        let decoded = match compression {
            COMPRESSION_RAW => payload,
            COMPRESSION_LZ4 => {
                // The payload is `compress_prepend_size` output: a four-byte
                // little-endian size prefix followed by the raw LZ4 block.
                // Size the output from the header length — itself bounded by
                // the format's maximum expansion ratio of the bytes actually
                // stored — instead of the in-payload prefix, which would
                // otherwise dictate an unbounded allocation before
                // decompression can fail.
                if uncompressed_len as u64 > (stored_len as u64).saturating_mul(MAX_LZ4_RATIO) {
                    return corruption(0, "rollup uncompressed length exceeds LZ4 capacity");
                }
                if payload.len() < 4 {
                    return corruption(HEADER_BYTES as u64, "LZ4 rollup payload is too short");
                }
                let prefix = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
                if prefix != uncompressed_len {
                    return corruption(
                        HEADER_BYTES as u64,
                        "LZ4 size prefix disagrees with rollup header",
                    );
                }
                decompress(&payload[4..], uncompressed_len).map_err(|error| Error::Corruption {
                    offset: HEADER_BYTES as u64,
                    reason: format!("invalid LZ4 rollup payload: {error}"),
                })?
            }
            _ => return corruption(0, "unknown rollup compression"),
        };
        if decoded.len() != uncompressed_len {
            return corruption(HEADER_BYTES as u64, "rollup decoded length mismatch");
        }
        let buckets: Vec<_> = decoded
            .chunks_exact(BUCKET_BYTES)
            .map(decode_bucket)
            .collect();
        validate_buckets(&buckets, HEADER_BYTES as u64)?;
        if buckets
            .first()
            .is_some_and(|bucket| bucket.start != min_start)
            || buckets.last().is_some_and(|bucket| bucket.end != max_end)
            || (buckets.is_empty() && (min_start != 0 || max_end != 0))
        {
            return corruption(0, "rollup header bounds mismatch");
        }
        Ok(Self {
            buckets,
            stats: RollupSegmentStats {
                buckets: bucket_count,
                stored_bytes: file_len,
                logical_bytes: uncompressed_len as u64,
                min_start: (bucket_count > 0).then_some(min_start),
                max_end: (bucket_count > 0).then_some(max_end),
            },
        })
    }

    #[must_use]
    pub const fn stats(&self) -> RollupSegmentStats {
        self.stats
    }

    #[must_use]
    pub fn buckets(&self) -> &[GaugeBucket] {
        &self.buckets
    }

    #[must_use]
    pub fn query(&self, start: i64, end: i64) -> Vec<GaugeBucket> {
        self.buckets
            .iter()
            .filter(|bucket| bucket.end > start && bucket.start < end)
            .copied()
            .collect()
    }
}

fn write_temporary(path: &Path, buckets: &[GaugeBucket]) -> Result<RollupSegmentStats> {
    let mut encoded = Vec::with_capacity(buckets.len() * BUCKET_BYTES);
    for bucket in buckets {
        encode_bucket(*bucket, &mut encoded);
    }
    let compressed = compress_prepend_size(&encoded);
    let (compression, payload) = if compressed.len() < encoded.len() {
        (COMPRESSION_LZ4, compressed)
    } else {
        (COMPRESSION_RAW, encoded)
    };
    let uncompressed_len = u32::try_from(buckets.len() * BUCKET_BYTES)
        .map_err(|_| Error::Serialization("rollup segment exceeds u32 length".to_owned()))?;
    let stored_len = u32::try_from(payload.len())
        .map_err(|_| Error::Serialization("rollup segment exceeds u32 length".to_owned()))?;
    let bucket_count = u32::try_from(buckets.len())
        .map_err(|_| Error::Serialization("too many rollup buckets".to_owned()))?;
    let min_start = buckets.first().map_or(0, |bucket| bucket.start);
    let max_end = buckets.last().map_or(0, |bucket| bucket.end);
    let mut header = [0_u8; HEADER_BYTES];
    header[..8].copy_from_slice(MAGIC);
    header[8..10].copy_from_slice(&VERSION.to_le_bytes());
    header[10] = compression;
    header[12..16].copy_from_slice(&bucket_count.to_le_bytes());
    header[16..20].copy_from_slice(&uncompressed_len.to_le_bytes());
    header[20..24].copy_from_slice(&stored_len.to_le_bytes());
    header[24..28].copy_from_slice(&hash(&payload).to_le_bytes());
    header[28..36].copy_from_slice(&min_start.to_le_bytes());
    header[36..44].copy_from_slice(&max_end.to_le_bytes());
    let header_crc = hash(&header[..44]);
    header[44..48].copy_from_slice(&header_crc.to_le_bytes());

    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(&header)?;
    file.write_all(&payload)?;
    file.sync_all()?;
    let stored_bytes = file.metadata()?.len();
    Ok(RollupSegmentStats {
        buckets: bucket_count,
        stored_bytes,
        logical_bytes: u64::from(uncompressed_len),
        min_start: (bucket_count > 0).then_some(min_start),
        max_end: (bucket_count > 0).then_some(max_end),
    })
}

fn encode_bucket(bucket: GaugeBucket, output: &mut Vec<u8>) {
    output.extend_from_slice(&bucket.start.to_le_bytes());
    output.extend_from_slice(&bucket.end.to_le_bytes());
    output.extend_from_slice(&bucket.count.to_le_bytes());
    output.extend_from_slice(&bucket.sum.to_bits().to_le_bytes());
    output.extend_from_slice(&bucket.min.unwrap_or(0.0).to_bits().to_le_bytes());
    output.extend_from_slice(&bucket.max.unwrap_or(0.0).to_bits().to_le_bytes());
    output.extend_from_slice(
        &bucket
            .first
            .map_or(0, |sample| sample.timestamp)
            .to_le_bytes(),
    );
    output.extend_from_slice(
        &bucket
            .first
            .map_or(0.0, |sample| sample.value)
            .to_bits()
            .to_le_bytes(),
    );
    output.extend_from_slice(
        &bucket
            .last
            .map_or(0, |sample| sample.timestamp)
            .to_le_bytes(),
    );
    output.extend_from_slice(
        &bucket
            .last
            .map_or(0.0, |sample| sample.value)
            .to_bits()
            .to_le_bytes(),
    );
    output.extend_from_slice(&bucket.integral_value_micros.to_bits().to_le_bytes());
    output.extend_from_slice(&bucket.covered_micros.to_le_bytes());
    let mut presence = 0_u8;
    presence |= u8::from(bucket.min.is_some());
    presence |= u8::from(bucket.max.is_some()) << 1;
    presence |= u8::from(bucket.first.is_some()) << 2;
    presence |= u8::from(bucket.last.is_some()) << 3;
    output.push(presence);
    output.extend_from_slice(&[0_u8; 7]);
}

fn decode_bucket(raw: &[u8]) -> GaugeBucket {
    let presence = raw[96];
    GaugeBucket {
        start: i64::from_le_bytes(raw[0..8].try_into().unwrap()),
        end: i64::from_le_bytes(raw[8..16].try_into().unwrap()),
        count: u64::from_le_bytes(raw[16..24].try_into().unwrap()),
        sum: f64::from_bits(u64::from_le_bytes(raw[24..32].try_into().unwrap())),
        min: (presence & 1 != 0)
            .then(|| f64::from_bits(u64::from_le_bytes(raw[32..40].try_into().unwrap()))),
        max: (presence & 2 != 0)
            .then(|| f64::from_bits(u64::from_le_bytes(raw[40..48].try_into().unwrap()))),
        first: (presence & 4 != 0).then(|| {
            Sample::new(
                i64::from_le_bytes(raw[48..56].try_into().unwrap()),
                f64::from_bits(u64::from_le_bytes(raw[56..64].try_into().unwrap())),
            )
        }),
        last: (presence & 8 != 0).then(|| {
            Sample::new(
                i64::from_le_bytes(raw[64..72].try_into().unwrap()),
                f64::from_bits(u64::from_le_bytes(raw[72..80].try_into().unwrap())),
            )
        }),
        integral_value_micros: f64::from_bits(u64::from_le_bytes(raw[80..88].try_into().unwrap())),
        covered_micros: i64::from_le_bytes(raw[88..96].try_into().unwrap()),
    }
}

fn validate_buckets(buckets: &[GaugeBucket], offset: u64) -> Result<()> {
    for (index, bucket) in buckets.iter().enumerate() {
        if bucket.end <= bucket.start || bucket.covered_micros < 0 {
            return corruption(
                offset + index as u64 * BUCKET_BYTES as u64,
                "invalid bucket bounds",
            );
        }
        if index > 0 && buckets[index - 1].end > bucket.start {
            return corruption(
                offset + index as u64 * BUCKET_BYTES as u64,
                "rollup buckets overlap or are unsorted",
            );
        }
        if (bucket.count == 0) != (bucket.first.is_none() && bucket.last.is_none())
            || bucket.min.is_some() != bucket.max.is_some()
            || bucket.first.is_some() != bucket.last.is_some()
        {
            return corruption(
                offset + index as u64 * BUCKET_BYTES as u64,
                "inconsistent rollup aggregate presence",
            );
        }
        if bucket.covered_micros > bucket.end - bucket.start {
            return corruption(
                offset + index as u64 * BUCKET_BYTES as u64,
                "rollup coverage exceeds bucket duration",
            );
        }
    }
    Ok(())
}

fn temporary_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().ok_or(Error::InvalidConfig(
        "rollup segment path must have a parent directory",
    ))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(Error::InvalidConfig("rollup segment path must be UTF-8"))?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    Ok(parent.join(format!(".{file_name}.tmp-{}-{nonce}", std::process::id())))
}

/// Routes through the shared, platform-gated directory sync helper so the
/// Unix/non-Unix policy lives in exactly one place (`storage::sync_directory`).
/// A missing parent was already rejected by `temporary_path` before any
/// caller reaches this sync.
fn sync_parent(path: &Path) -> Result<()> {
    crate::storage::sync_parent_directory(path)
}

fn corruption<T>(offset: u64, reason: &str) -> Result<T> {
    Err(Error::Corruption {
        offset,
        reason: reason.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        BUCKET_BYTES, COMPRESSION_LZ4, HEADER_BYTES, MAGIC, MAX_ROLLUP_BUCKETS, RollupSegment,
        VERSION,
    };
    use crate::{Error, FixedGaugeRollup, Point};
    use crc32fast::hash;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::tempdir;

    fn buckets() -> Vec<crate::GaugeBucket> {
        let points = [
            Point::actual(1, 0, 2.0),
            Point::actual(1, 5_000_000, 4.0),
            Point::actual(1, 10_000_000, 8.0),
        ];
        FixedGaugeRollup::build(&points, 5_000_000, 5_000_000)
            .unwrap()
            .buckets()
            .copied()
            .collect()
    }

    #[test]
    fn round_trips_closed_aggregate_states() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("rollup.rseg");
        let expected = buckets();
        let stats = RollupSegment::create(&path, &expected).unwrap();
        let segment = RollupSegment::open(&path).unwrap();

        assert_eq!(stats, segment.stats());
        assert_eq!(segment.buckets(), expected);
        assert_eq!(segment.query(5_000_000, 10_000_000), expected[1..2]);
    }

    #[test]
    fn refuses_to_replace_an_existing_segment() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("rollup.rseg");
        RollupSegment::create(&path, &buckets()).unwrap();
        assert!(RollupSegment::create(&path, &buckets()).is_err());
    }

    /// Builds a rollup segment file with a consistent header around an
    /// arbitrary LZ4 payload, so only the decompression bounds can reject it.
    fn crafted_lz4_segment(bucket_count: u32, payload: &[u8]) -> Vec<u8> {
        let uncompressed_len = bucket_count * BUCKET_BYTES as u32;
        let mut header = [0_u8; HEADER_BYTES];
        header[..8].copy_from_slice(MAGIC);
        header[8..10].copy_from_slice(&VERSION.to_le_bytes());
        header[10] = COMPRESSION_LZ4;
        header[12..16].copy_from_slice(&bucket_count.to_le_bytes());
        header[16..20].copy_from_slice(&uncompressed_len.to_le_bytes());
        header[20..24].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[24..28].copy_from_slice(&hash(payload).to_le_bytes());
        let header_crc = hash(&header[..44]);
        header[44..48].copy_from_slice(&header_crc.to_le_bytes());
        let mut bytes = header.to_vec();
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn rejects_decompression_bomb_bucket_count() {
        // A few dozen bytes claiming ~4 GiB of buckets must fail on the LZ4
        // expansion bound before any decompression buffer is sized.
        let directory = tempdir().unwrap();
        let path = directory.path().join("bomb.rseg");
        let mut payload = u32::MAX.to_le_bytes().to_vec();
        payload.extend_from_slice(&[0_u8; 12]);
        std::fs::write(&path, crafted_lz4_segment(41_000_000, &payload)).unwrap();
        assert!(matches!(
            RollupSegment::open(&path),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn rejects_bucket_count_above_memory_limit() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("too-many-buckets.rseg");
        let payload = 0_u32.to_le_bytes();
        std::fs::write(&path, crafted_lz4_segment(MAX_ROLLUP_BUCKETS + 1, &payload)).unwrap();
        assert!(matches!(
            RollupSegment::open(&path),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn rejects_lying_lz4_size_prefix() {
        // The header claims one bucket while the in-payload LZ4 size prefix
        // claims ~4 GiB; the prefix must be rejected against the header
        // instead of sizing an allocation.
        let directory = tempdir().unwrap();
        let path = directory.path().join("prefix.rseg");
        let mut payload = u32::MAX.to_le_bytes().to_vec();
        payload.extend_from_slice(&[0_u8; 12]);
        std::fs::write(&path, crafted_lz4_segment(1, &payload)).unwrap();
        assert!(matches!(
            RollupSegment::open(&path),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn detects_payload_corruption() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("rollup.rseg");
        RollupSegment::create(&path, &buckets()).unwrap();
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(52)).unwrap();
        file.write_all(&[0xA5]).unwrap();
        file.sync_all().unwrap();

        assert!(RollupSegment::open(&path).is_err());
    }
}
