# Prototype file format v1

All integers are little-endian. The current format is intentionally simple and
uncompressed; it is a durability test vehicle, not the final segment format.

## Database header (16 bytes)

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | ASCII `WATTDB01` |
| 8 | 2 | format version (`1`) |
| 10 | 2 | reserved flags |
| 12 | 4 | CRC32 of bytes 0..12 |

## Batch frame header (24 bytes)

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | ASCII `WBAT` |
| 4 | 2 | frame version (`1`) |
| 6 | 2 | reserved flags |
| 8 | 4 | point count |
| 12 | 4 | payload bytes |
| 16 | 4 | CRC32 of payload |
| 20 | 4 | CRC32 of header bytes 0..20 |

## Point record (72 bytes)

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | `series_id` |
| 8 | 8 | `valid_time` (UTC microseconds) |
| 16 | 8 | exclusive `valid_time_end` |
| 24 | 8 | `knowledge_time` |
| 32 | 8 | `change_time` |
| 40 | 16 | `run_id`; zero means unspecified |
| 56 | 8 | IEEE-754 binary64 value bits |
| 64 | 4 | quality code/bitset |
| 68 | 4 | flags |

The complete batch frame is the recovery unit. An incomplete final header or
payload is truncated on open. A checksum failure in the final frame discards
that frame. A checksum failure before a later frame is reported as corruption.

The immutable segment format will be separately versioned and use per-column
encoding, block checksums, sparse indexes, and footer redundancy.

