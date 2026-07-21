# Immutable raw segment format v1

Segments are created under a unique temporary name, fully written and synced,
published with a no-replace hard link, followed by a parent-directory sync.
The temporary link is then removed and the directory synced again. Existing
segment names are never overwritten.

## Layout

```text
40-byte segment header
  N compressed point blocks
16-byte index header
  N fixed 40-byte sparse index entries
```

The segment header contains magic/version, block and point counts, index bounds,
and a header CRC32. The index has its own CRC32 and must exactly cover every
block byte in sorted `(series_id, min_valid_time)` order.

Each block belongs to one series and stores min/max valid time, point count,
encoding/compression IDs, encoded/stored sizes, a payload CRC32, and a header
CRC32. A block holds at most 262,144 points, which bounds both decompression and
the decoded point vector even when valid data reaches LZ4's maximum compression
ratio. A query scans the small index and reads only overlapping blocks.

## Column encoding

The v1 encoded payload has seven length-delimited columns:

1. first valid timestamp plus delta and delta-of-delta signed varints;
2. `valid_time_end - valid_time` signed varints;
3. `knowledge_time - valid_time` signed varints;
4. `change_time - knowledge_time` signed varints;
5. `run_id` unsigned 128-bit varints;
6. first IEEE-754 value plus XOR-against-previous unsigned varints;
7. quality and flags unsigned varints.

The series ID is stored once in the block header. The complete encoded column
payload is LZ4-compressed only when that is smaller than the encoded bytes;
otherwise it is stored raw. All decoding uses checked arithmetic and exact
column-consumption checks.

Future block encoding versions can add Gorilla/Chimp/ALP, bit packing, fixed
point, or Zstd without changing segment/index framing. Codec selection will be
data-driven by the energy corpus rather than globally fixed.

## Current constraints

Segment creation sorts a snapshot in memory. Reads are bounded per compressed
block plus returned result, but large compactions still need an external/streamed
merge path. Segments currently contain raw points only; catalog state remains
in the commit log and the M3 manifest coordinates segment publication.
