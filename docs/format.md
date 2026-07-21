# Prototype file format v1

All integers are little-endian. The current format is intentionally simple and
uncompressed; it is a durability test vehicle, not the final segment format.

## Database header (16 bytes)

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | ASCII `FTWDB001` |
| 8 | 2 | format version (`1`) |
| 10 | 2 | reserved flags |
| 12 | 4 | CRC32 of bytes 0..12 |

## Batch frame header (24 bytes)

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | ASCII `WBAT` |
| 4 | 2 | frame version (`1`) |
| 6 | 2 | frame kind: `0` legacy points, `1` mixed transaction, `2` identified mixed transaction |
| 8 | 4 | item count: points or transaction records |
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
payload is truncated on a writable open and reported as `incomplete-header` or
`incomplete-payload`. A read-only open reports the same recovery without
changing the file. A full final frame with a bad payload checksum is corruption;
both open modes return an error and leave every byte unchanged. A checksum
failure before a later frame is also reported as corruption.

## Mixed transaction payload

A frame of kind `1` begins with `WTXN`, a transaction version, reserved flags,
and a record count. Each record has a kind, record version, byte length, and
body. Version 1 record kinds are entity, relation, series definition, run,
plan, and fixed-width point batch. Metadata bodies use version-pinned Postcard
encoding; point bodies retain the explicit 72-byte layout above.

All records in the frame are validated against the resulting catalog before
the frame is appended. Recovery applies either every record and point or none.
Unknown frame/record versions fail closed rather than being skipped.

## Identified transaction payload

A frame of kind `2` is a mixed transaction carrying a client-supplied
idempotency identifier: 16 bytes of little-endian `u128` commit identifier
followed by an unmodified kind `1` transaction payload. Format evolution uses
a new frame kind — the established mechanism, kinds `0` and `1` already
coexist — so logs written before this kind existed decode unchanged, and
transactions without an identifier still produce byte-identical kind `1`
frames. The identifier shares the frame's checksummed durable unit with the
records it protects; a separate identifier frame could tear away from its
transaction and reopen the duplicate-on-retry window this kind closes.

Recovery collects every identifier seen during the log scan, and a commit
whose identifier is already present writes nothing and reports deduplication.
A duplicate identifier encountered in the log itself is reported as
corruption, since the writer never appends one.

The immutable segment format will be separately versioned and use per-column
encoding, block checksums, sparse indexes, and footer redundancy.
