# Roadmap

## M0: durability and semantics — in progress

- checksummed append frames and tail recovery;
- explicit durability modes;
- three-dimensional point time and run provenance;
- latest/as-of queries;
- mergeable gauge/counter aggregate states;
- property, corruption, and microbenchmark coverage.

Exit: recovery invariants pass under randomized truncation and bit flips.

## M1: catalog, plans, and transactional records

- database directory and manifest generations;
- typed entities, topology relations, series definitions, units, runs,
  scenarios, plans, and input snapshots;
- atomic transactions containing catalog changes and point batches;
- plan-versus-outcome query API;
- schema migration/version rules.

Exit: an optimization run, its planned schedules, and subsequent actuals
round-trip and survive injected crashes as one consistent history.

## M2: immutable compressed raw segments

- seal append logs;
- sort by series and temporal dimensions;
- sparse series/time index and checksummed block/footer format;
- timestamp/value codec bake-off (delta, Gorilla, Chimp, ALP, fixed point,
  LZ4/Zstd);
- bounded memory and segment cache.

Exit: faster/smaller than prototype with measured write amplification.

## M3: automatic rollup pyramid

- durable policies and scheduler;
- 5m/30m/hour/day/month materialization;
- timezone/DST calendar buckets;
- invalidation/rebuild for late data and revisions;
- query planner using exact rollups plus raw edge buckets;
- raw deletion safety gate.

Exit: multi-year aggregate query cost scales with returned buckets, and crash
tests prove raw data is never deleted before valid durable rollups exist.

## M4: comparative benchmark and edge hardening

- deterministic energy generator and adapters;
- TSBS compatibility;
- Docker matrix across the registry;
- Linux ARM64/SD runs, full-disk handling, soak and real power cuts;
- backup/restore, integrity check, salvage, metrics, and operational tooling.

Exit: reproducible, result-verified reports with no unexplained durability or
configuration asymmetry.

