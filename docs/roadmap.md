# Roadmap

## M0: durability and semantics — in progress

- checksummed append frames and tail recovery;
- explicit durability modes;
- three-dimensional point time and run provenance;
- latest/as-of queries;
- mergeable gauge/counter aggregate states;
- property, corruption, and microbenchmark coverage.

Exit: recovery invariants pass under randomized truncation and bit flips.

## M1: catalog, plans, and transactional records — complete

- database directory and manifest generations;
- typed entities, topology relations, series definitions, units, runs,
  scenarios, plans, and input snapshots;
- atomic transactions containing catalog changes and point batches;
- plan-versus-outcome query API;
- schema migration/version rules.

Exit met: an optimization run, plan metadata, scheduled points, and subsequent
actuals round-trip; torn mixed frames expose neither partial catalog state nor
partial points.

## M2: immutable compressed raw segments — initial format complete

- seal append logs;
- sort by series and temporal dimensions;
- sparse series/time index and checksummed block/footer format;
- timestamp/value codec bake-off (delta, Gorilla, Chimp, ALP, fixed point,
  LZ4/Zstd);
- bounded memory and segment cache.

Initial exit met: v1 segments are indexed, checksummed, no-replace published,
property-tested, and roughly 10.3x smaller than the logical point layout on the
bootstrap energy waveform. External merge sorting and the broader codec bake-off
remain hardening work rather than format blockers.

## M3: automatic rollup pyramid — initial vertical slice complete

- durable policies and scheduler;
- 5m/30m/hour/day/month materialization;
- timezone/DST calendar buckets;
- invalidation/rebuild for late data and revisions;
- query planner using exact rollups plus raw edge buckets;
- raw deletion safety gate.

Exit: multi-year aggregate query cost scales with returned buckets, and crash
tests prove raw data is never deleted before valid durable rollups exist.

Initial exit met: fixed and DST-correct calendar aggregate states are persisted
in checksummed immutable files, generations recover from a corrupt newest
manifest, late revisions invalidate and rebuild materializations, cached query
cost scales with returned buckets, and retention remains a non-destructive gate.
Stable time shards bound rewrite amplification, and the planner combines
adjacent rollups with raw invalid/current edges. Background scheduling, bounded
cache eviction, and physical log reclamation remain hardening work.

## M4: comparative benchmark and edge hardening

- deterministic energy generator and adapters;
- TSBS compatibility;
- Docker matrix across the registry;
- Linux ARM64/SD runs, full-disk handling, soak and real power cuts;
- backup/restore, integrity check, salvage, metrics, and operational tooling.

Exit: reproducible, result-verified reports with no unexplained durability or
configuration asymmetry.

Initial progress: the deterministic mixed energy generator, portable bundle,
machine-readable FTWDB runner, and exact FTWDB/SQLite native comparison are
implemented. The pinned server registry is explicitly marked compose/smoke-only
until each adapter verifies results. VictoriaMetrics now has a result-verified
telemetry-subset adapter, but not a full-domain or equal-durability comparison.
ARM64 SD, full-disk, soak, remote backup/salvage, the remaining adapters, and physical
power-cut work remain open and are required for the M4 exit. Local snapshot
backup plus post-publication integrity verification is implemented; remote
backup policy and corruption salvage remain open.
