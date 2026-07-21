# WattDB

WattDB is an experimental embedded database for energy systems. The goal is a
small Rust engine that is fast on time-window aggregates, survives abrupt power
loss, and minimizes write amplification on SD cards and other constrained edge
storage.

This repository currently contains the first executable storage slice:

- append-only, checksummed atomic batches;
- configurable durability (`Always`, byte-grouped sync, or explicit sync);
- recovery that removes a torn final batch but reports earlier corruption;
- three-dimensional time (`valid`, `knowledge`, and `change`) plus `run_id`;
- latest-revision and point-in-time queries;
- mergeable gauge, energy-integral, and reset-aware counter aggregates;
- tests, property tests, Criterion benchmarks, and a competitor benchmark plan.

It is **not production-ready**. The current in-memory read index and uncompressed
point encoding are intentional bootstrap implementations used to validate
durability and semantics before the immutable columnar segment format lands.

## Quick start

```sh
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo bench --bench storage
```

Inspect a database file:

```sh
cargo run --release -- inspect ./data.wattdb
```

## Design documents

- [Architecture and invariants](docs/architecture.md)
- [Energy model, plans, and rollups](docs/energy-model.md)
- [Storage format v1](docs/format.md)
- [OSS database research](docs/research.md)
- [Benchmark protocol](docs/benchmarking.md)
- [Bootstrap benchmark result](docs/results/2026-07-21-macos-arm64.md)
- [Roadmap](docs/roadmap.md)

## License

Apache-2.0.
