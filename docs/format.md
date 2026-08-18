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
| 6 | 2 | frame kind: `0` legacy points, `1` mixed transaction, `2` identified mixed transaction, `3` ordered ingress transaction, `4` seal checkpoint, `5` identity index |
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

## Ordered ingress transaction payload

A frame of kind `3` starts with this fixed 40-byte identity:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 16 | `source_id` as little-endian `u128` |
| 16 | 8 | `sequence` as little-endian `u64` |
| 24 | 16 | `commit_id` as little-endian `u128` |

The identity is followed by the canonical kind `1` transaction payload. The
frame checksum covers both parts.

FTWDB accepts any first sequence for a new non-zero source ID. It then accepts
only a strictly greater cursor; gaps are valid. An exact retry of a stored source and sequence reads
the original transaction bytes from the log and compares every byte. It
returns the original frame offset, record count, point count, and byte count
without writing. A matching CRC is only a fast check and never replaces the
byte comparison. Reusing a source sequence or commit ID for other data fails
without poisoning the writer.

Recovery rebuilds the source watermarks and receipt indexes from complete
kind `3` frames. A torn last frame exposes neither its identity nor its data.
Duplicate keys or a source cursor that does not increase inside a complete log
are corruption. Kinds
`0` through `2` remain byte-compatible.

## Seal checkpoint payload

A frame of kind `4` carries a fixed 16-byte payload:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | sealed manifest generation as little-endian `u64` |
| 8 | 8 | sealed point count as little-endian `u64` |

The checkpoint is appended before the live log is reclaimed. Recovery treats an
invalid payload length or generation mismatch as corruption.

## Identity index payload

A frame of kind `5` stores a compact Postcard-encoded index of ingress receipts
written during log reclamation. Recovery validates the payload checksum and
decodes the index before accepting the compact log. An invalid or truncated
index fails closed as corruption.

After reclaim, identity replay verification uses the identity-index frame and
the retained receipt bytes in the compact log. A post-reclaim duplicate or
cursor regression is reported as corruption from those durable bytes, not from
recomputing a standalone frame CRC in isolation.

The immutable segment format will be separately versioned and use per-column
encoding, block checksums, sparse indexes, and footer redundancy.
