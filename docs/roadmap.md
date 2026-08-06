# Roadmap

## M0: durability and semantics — initial exit complete

- checksummed append frames and tail recovery;
- explicit durability modes;
- three-dimensional point time and run provenance;
- latest/as-of queries;
- mergeable gauge/counter aggregate states;
- property, corruption, and microbenchmark coverage.

Initial exit met: property tests cover every truncation point in the final
frame, complete checksum-corrupt frames stay intact and return corruption, and
earlier frame corruption never becomes tail recovery. Broader media-fault and
physical power-cut work continues in M4.

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
QuestDB now has a result-verified stored-revision subset: ILP/HTTP stores every
portable point and all three revision times, while an exact-range
`SAMPLE BY 5m ALIGN TO FIRST OBSERVATION` query verifies site-0 grid actuals.
This adapter does not yet import catalog or plan records or verify a DST
calendar query. Its acknowledged durability point is not yet equal to FTWDB's.
The TSBS IoT write adapter, fixed sanitized real-installation replay, model-query
benchmarks, process-kill test, and local full-disk evidence now cover more of the
workload and failure surface. A deterministic NBD SD-card emulator and one ARM64
Linux/ext4 smoke run add repeatable media-cache and power-loss checks, but the
recorded cut happened after the load and host sync. A repeatable 64 MiB
Linux/NBD/ext4 run reached `ENOSPC` after 770,000 durable points in 77 commits;
`check-store` and `inspect` passed on that prefix. Local snapshot backup and
strict restore now use atomic no-clobber publication, read-only checks, selected
file CRCs, and tested rollback after final sync or post-check errors.
Process-level CLI tests cover usage errors, generated and sanitized input paths,
integrity checks, inspection, backup and restore JSON, exact restored bytes,
and byte-for-byte read-only behavior. The sanitized fixture restore fixes the
expected result at 889,978 points, 89 commits, and snapshot CRC32 `9ff1a95b`.
The Linux NBD power-loss run now follows recovery with an off-card backup and a
verified restore to a new target on the emulated card. Strict salvage now reads
only the raw log, stops at the first invalid frame without resync, ignores all
derived files, and publishes a checked prefix through the same atomic
no-clobber path. The sanitized fixture fixes clean salvage at 889,978 points,
89 commits, and snapshot CRC32 `9ff1a95b`; the NBD run also checks a seven-byte
torn suffix and a verified partial target. Together with the
process-lock, sync-failure, `/dev/full`, and NBD tests, this closes the
robustness gaps tracked in issue #17. Tests on the target board and SD cards,
power cuts during commits, soak runs, remote backup policy, and the remaining
result-verified adapters stay open and are required for the M4 exit.

## M5: FTW shadow integration — foundation in progress

- portable, versioned local wire contract for catalog, runs, plans, and points;
- durable source, sequence, and commit identity with exact replay receipts;
- bounded single-writer runtime with clear overload and poison states;
- local Unix sidecar with private path modes, peer-UID checks, clean signal shutdown,
  and time, frame, client, and memory bounds;
- managed service files and live operational metrics;
- FTW async dual-write adapter, cross-language golden fixtures, and reconcile report;
- box metrics, week soak, live SD-write measurements, physical power cuts, and rollback.

Exit: selected FTW beta boxes can enable and disable shadow capture without any
control-path effect; every accepted batch has a clear durability state; Go and
Rust agree on every contract fixture; restart, retry, disk-full, corruption,
and power-cut tests preserve one exact durable prefix; operators can compare,
export, restore, rotate, and remove the shadow store.

The storage ingress frame, protocol draft, bounded writer, and sidecar work are
the first slice. FTWDB remains a shadow sink until every M4 hardware gate and
the M5 exit above pass.
