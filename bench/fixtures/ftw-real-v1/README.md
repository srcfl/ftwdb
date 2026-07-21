# Sanitized real FTW fixture

This fixture comes from a read-only export of a running FTW installation. It
contains a 24-hour slice from all 54 active time series found during the
export. The source export stays outside Git.

The export replaced driver, metric, series, and asset identifiers with fixed
aliases. This fixture also replaces the source timestamps with offsets from
the start of the slice. It keeps values, cadence, jitter, gaps, series groups,
and chronological order. The source API did not expose receive time or insert
order, so the fixture cannot preserve late-arrival order.

`points.csv.gz` contains `driver_id,series_id,offset_ms,value`. The loader maps
offset zero to 2026-01-01 00:00:00 UTC. `energy.csv.gz` contains the system
energy view with source, quality, and provenance fields. It is reference data
for energy-ledger tests and is not loaded by `bench-real-fixture` yet.

Verify the files and run the write test:

```sh
cd bench/fixtures/ftw-real-v1
shasum -a 256 -c SHA256SUMS
cd ../../..
cargo run --release -- bench-real-fixture \
  bench/fixtures/ftw-real-v1/points.csv.gz bench-results/ftw-real \
  --durability always --batch-points 10000
```

Use a new or empty output directory for each run.
