# Persistent rollups and retention

## Aggregate state

Gauge rollups persist closed bucket states, not only an average. Every bucket
contains sample count/sum/min/max/first/last plus the previous-value-hold
integral and its actually covered duration. This supports all of the following
without rereading raw points:

- sample mean;
- time-weighted mean;
- minimum and maximum;
- exact power-to-energy conversion;
- exact merging into a coarser adjacent bucket.

Gaps longer than `maximum_gap_micros` add no coverage and no invented energy.
Missing coverage remains visible to callers through `covered_micros`.

## Fixed and calendar boundaries

`FixedMicros` is aligned with Euclidean division in UTC, including timestamps
before the Unix epoch. `Calendar` resolves an IANA zone and uses local
midnights. The UTC duration of a Europe/Stockholm day can therefore be 23, 24,
or 25 hours, and month duration follows the real calendar. Bucket edges are
stored as UTC microseconds after resolution, so queries need no timezone
calculation on the hot materialized path.

## Durable publication

`Store::maintain(now)` performs this order:

1. sync the raw commit log;
2. compute every completed configured bucket;
3. write and checksum an immutable `.rseg` file;
4. sync the file and publish its no-replace hard link;
5. sync the rollup directory;
6. publish and sync a new `MANIFEST.<generation>` file.

Fixed 5-minute, 30-minute, and hourly buckets are grouped into stable completed
UTC-day segments. Calendar day/month buckets are each an independent segment.
Consequently a normal new day appends files without rewriting historical
rollups, and a late correction invalidates only the segment whose coverage can
change. Unusual fixed resolutions that do not divide a UTC day use one bucket
per file rather than a moving tail chunk.

Manifest files are append-only generations. Startup scans newest to oldest and
uses the highest fully valid generation, so a torn newest file does not require
an independently mutable `CURRENT` pointer. Old rollup files and old manifests
are retained until a future manifest garbage collector can prove they are
unreferenced.

## Late values and corrections

A point inside a materialized range, at its closing boundary, or within the
series maximum gap before its opening boundary invalidates that rollup. The raw
commit is forced durable before the invalidating manifest is published. A point
outside the range advances the rollup's source watermark without rebuilding it.

Power can still fail between those two durable publications. On startup, any
active rollup whose raw-point watermark is behind the recovered commit log is
therefore invalidated conservatively before it can answer queries. Maintenance
then rebuilds it from the winning revisions.

## Query and retention behavior

The planner uses an in-memory cache of verified immutable rollup segments. It
can cover one query with many adjacent descriptors and reads raw points only
for an uncovered current edge or invalidated time shard. The response reports
`Materialized`, `Hybrid`, or `Raw`. Raw edge construction reads only the bucket
range plus the series maximum-gap context, rather than rescanning all history.

Raw retention is currently a safety report, not a deletion operation. A series
is eligible only when every configured tier is current and covers all raw data
through the cutoff. The active log mixes catalog records and points, so actual
reclamation remains disabled until M4 compaction can rewrite retained records
without losing metadata or provenance.
