# FTWDB

**Forecasts, Telemetry & Watts.**

FTWDB is an experimental embedded database for energy systems. The goal is a
small Rust engine that is fast on time-window aggregates, survives abrupt power
loss, and minimizes write amplification on SD cards and other constrained edge
storage.

This repository currently contains the first executable storage slice:

- append-only, checksummed atomic batches;
- configurable durability (`Always`, byte-grouped sync, or explicit sync);
- recovery that removes a torn final batch but reports earlier corruption;
- three-dimensional time (`valid`, `knowledge`, and `change`) plus `run_id`;
- atomic mixed transactions for assets, topology, series, runs, plans, and points;
- persistent catalog recovery and exact-time plan-versus-actual queries;
- latest-revision and point-in-time queries;
- mergeable gauge, energy-integral, and reset-aware counter aggregates;
- immutable compressed raw and rollup segments with checksummed manifests;
- persistent fixed and IANA-calendar rollups, including DST-correct energy;
- automatic late-data invalidation, rebuild, cached queries, and retention gates;
- tests, property tests, Criterion benchmarks, and a competitor benchmark plan.

It is **not production-ready**. The active log still rebuilds an in-memory read
index, raw compaction/deletion is intentionally disabled, and real SD-card
power-cut evidence has not yet been collected.

## Quick start

```sh
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo bench --bench storage
cargo bench --bench energy_compare -- --quick
```

Generate one deterministic portable workload and run FTWDB against it:

```sh
cargo run --release -- generate ./bench-results/workload --sites 1 --days 7 --cadence-seconds 60 --seed 42
cargo run --release -- bench-ftwdb ./bench-results/workload ./bench-results/ftwdb-manual --durability manual
```

Inspect a database file:

```sh
cargo run --release -- inspect ./data.ftwdb
```

Verify or snapshot a directory store:

```sh
cargo run --release -- check-store ./energy.ftwdb
cargo run --release -- backup ./energy.ftwdb ./backups/energy-2026-07-21.ftwdb
```

## Design documents

- [Architecture and invariants](docs/architecture.md)
- [Energy model, plans, and rollups](docs/energy-model.md)
- [Storage format v1](docs/format.md)
- [Immutable segment format](docs/segment-format.md)
- [Persistent rollups and retention](docs/rollups.md)
- [Integrity checks and backup](docs/operations.md)
- [OSS database research](docs/research.md)
- [Benchmark protocol](docs/benchmarking.md)
- [Deterministic energy workload](docs/workload.md)
- [Bootstrap benchmark result](docs/results/2026-07-21-macos-arm64.md)
- [TSBS and robustness result](docs/results/2026-07-21-tsbs-robustness.md)
- [Roadmap](docs/roadmap.md)

## License

Apache-2.0.
