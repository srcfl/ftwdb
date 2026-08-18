# Architecture and invariants

## Product boundary

FTWDB is an embedded, single-node energy database first. It should run inside
an ARM edge service with no separate database daemon. A server protocol can be
added around the same library later. Distributed consensus, arbitrary SQL, and
PromQL are explicitly outside the first storage milestones.

The database is broader than a metrics TSDB. It must keep four connected forms
of state:

1. assets and topology;
2. series definitions, units, and physical semantics;
3. forecasts, optimization runs, scenarios, plans, and provenance;
4. measurements, planned setpoints, forecasts, revisions, and outcomes.

## First-principles requirements

1. **Power-loss behavior is specified.** A committed batch is either present in
   full or absent after recovery. Durability modes state exactly when an
   acknowledgement implies an `fsync`.
2. **Corruption is detected.** Headers and payloads are checksummed. Only an
   incomplete final header or payload is recoverable. A complete frame with a
   bad checksum is an error even when it is last; open leaves its bytes intact.
3. **Writes are predominantly sequential.** Small random page rewrites and
   write-amplifying B-trees are avoided on the ingest path.
4. **Write amplification is a product metric.** Every benchmark records logical
   input bytes, database bytes, bytes written by compaction, and sync count.
5. **Aggregates are first-class.** Query latency over years must depend on the
   requested result resolution, not on the raw sample count.
6. **Energy semantics are explicit.** A gauge, interval total, and monotonic
   meter counter do not share the same aggregation rule.
7. **Historical knowledge is reproducible.** Forecast and plan revisions are
   append-only and queryable as they were known at a previous instant.
8. **Raw deletion is gated.** Raw data is not removed until required rollups are
   durable, checksummed, and referenced by a durable manifest generation.

## Target storage layout

```text
database/
  active.wlog
  manifests/
    MANIFEST.00000000000000000001
  rollups/
    g1-s42-f300000000-*.rseg
  segments/
    g2-*.wseg
```

This is the current directory shape. Immutable raw segments are published
through the same manifest generations as rollups. After `seal_and_reclaim`,
`active.wlog` holds catalog records, identity receipts, and the unsealed tail.
Open replays only that tail; older raw points are read from sealed segment
files.

## Write path

1. Validate identifiers, time dimensions, series semantics, and batch limits.
2. Encode a transaction frame with a length, type/version, and checksum.
3. Append the frame in one sequential stream.
4. Sync according to the configured durability contract.
5. Publish the in-memory commit index only after the write path succeeds.
6. Seal large logs. Build an immutable, sorted, compressed segment and publish
   it through a new manifest generation. Source files remain until publication
   is durable.

The long-term format should use the append log itself as the L0 segment when
possible, so a WAL is not automatically a second full copy of every point.

## Read path

Readers use a stable manifest snapshot. The planner:

1. resolves assets, series, run/scenario, and time-of-knowledge predicates;
2. chooses the coarsest rollup level that exactly satisfies the requested
   bucket alignment and aggregate fields;
3. reads finer data only for incomplete edge buckets or invalidated ranges;
4. merges immutable blocks and recent committed frames;
5. applies revision winner rules `(knowledge_time, change_time, append_order)`.

Open rebuilds an in-memory per-series index from the unsealed tail only.
Sealed raw points stay on the sparse segment index and are merged at query
time. Materialized rollups use verified immutable files and a process-local
cache. A full sparse on-disk tail index remains later M4 hardening.

## Crash and corruption model

The engine assumes the filesystem implements normal local POSIX behavior but
does not assume atomic multi-sector writes. It does not assume an SD card has
SQLite's “powersafe overwrite” property. Therefore new data is appended, every
frame is checksummed, immutable outputs are fully written and synced before
publication, and manifest replacement includes a directory sync.

FTWDB v0.1 supports Unix targets only. Ubuntu Linux and macOS run the same root
and SD-emulator checks in CI. Every directory-entry durability point uses one
shared helper that opens and syncs the directory after a create, link, rename,
or removal. Non-Unix builds fail instead of replacing this step with a no-op
and claiming the same durability. The privileged NBD filesystem test remains a
separate Linux-only check.

The test matrix must include process kill, arbitrary frame truncation, payload
bit flips, full-disk errors, failed sync, stale manifests, and real power-cut
testing on representative SD hardware. Process tests alone cannot prove power
loss safety.

## Concurrency

The first production shape is one writer with snapshot readers. This matches
edge ingestion, makes commit order unambiguous, and avoids a coordination-heavy
write path. Concurrent producers feed one bounded writer queue. Independent
readers never mutate segment files.

## FTW shadow boundary

The first FTW link runs FTWDB as a local Unix sidecar while FTW keeps its
current data path and all control duties. The sidecar has its own process,
failure state, update path, and kill switch. A missing, slow, full, corrupt, or
stopped sidecar must not delay device reads, planning, dispatch, or safety
checks.

One versioned wire batch maps to one atomic ordered-ingress frame. The batch
keeps catalog changes, runs, plans, points, and its source/sequence/commit
identity in the same recovery unit. A bounded nonblocking queue feeds the only
writer. Client errors reject one request; storage errors poison the writer and
require a checked reopen.

FTWDB does not serve authoritative FTW reads during shadow collection. Read
promotion starts with a diagnostic comparison after replay, retry, overload,
soak, target-board power-cut, and rollback checks pass. See
[FTW shadow sidecar](shadow-sidecar.md).
