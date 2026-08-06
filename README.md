# FTWDB

**Forecasts, Telemetry & Watts.**

FTWDB is an experimental embedded database for energy systems. The goal is a
small Rust engine that is fast on time-window aggregates, survives abrupt power
loss, and minimizes write amplification on SD cards and other constrained edge
storage.

The current release is **v0.1.0-alpha.1**. It is an evaluation build for Unix
systems, not a production release. Download release archives from
[GitHub Releases](https://github.com/srcfl/ftwdb/releases), or install the CLI
from its immutable tag:

```sh
cargo install --git https://github.com/srcfl/ftwdb --tag v0.1.0-alpha.1 --locked --bin ftw
```

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
- exact per-source ingress replay receipts that survive reopen;
- a draft, bounded Go-portable shadow protocol and local Unix sidecar;
- tests, property tests, Criterion benchmarks, and a competitor benchmark plan.

It is **not production-ready**. The active log still rebuilds an in-memory read
index, raw compaction/deletion is intentionally disabled, and real SD-card
power-cut evidence has not yet been collected.

## Platform support

FTWDB v0.1 supports Unix targets only. CI tests Ubuntu Linux and macOS. A build
for a non-Unix target stops with a clear compile error; FTWDB does not replace
directory syncs with no-ops because that would weaken the stated durability
contract. The NBD block-device smoke test needs Linux and runs separately from
the portable Linux/macOS CI matrix.

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

Verify, snapshot, restore, or salvage a directory store:

```sh
cargo run --release -- check-store ./energy.ftwdb
cargo run --release -- backup ./energy.ftwdb ./backups/energy-2026-07-21.ftwdb
cargo run --release -- restore ./backups/energy-2026-07-21.ftwdb ./restored-energy.ftwdb
cargo run --release -- salvage ./damaged-energy.ftwdb ./salvaged-energy.ftwdb
```

Run the local shadow sidecar with sync-on-every-batch durability:

```sh
cargo run --release --bin ftwdb-shadow -- ./shadow.ftwdb ./run/ftwdb-shadow.sock
```

The sidecar protocol is still a draft. Do not make FTW control or production
reads depend on it. See the shadow guide and its beta gates below.

## Design documents

- [Architecture and invariants](docs/architecture.md)
- [Energy model, plans, and rollups](docs/energy-model.md)
- [Storage format v1](docs/format.md)
- [Immutable segment format](docs/segment-format.md)
- [Persistent rollups and retention](docs/rollups.md)
- [Integrity checks, backup, restore, and salvage](docs/operations.md)
- [FTW shadow sidecar and beta gates](docs/shadow-sidecar.md)
- [OSS database research](docs/research.md)
- [Benchmark protocol](docs/benchmarking.md)
- [Deterministic energy workload](docs/workload.md)
- [Bootstrap benchmark result](docs/results/2026-07-21-macos-arm64.md)
- [TSBS and robustness result](docs/results/2026-07-21-tsbs-robustness.md)
- [Release policy and process](docs/releases.md)
- [Roadmap](docs/roadmap.md)

## License

Apache-2.0.
