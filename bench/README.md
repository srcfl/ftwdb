# Competitor harness

`compose.yml` pins each server image by digest. Profiles keep heavyweight
engines from accidentally running together on a developer laptop.

Validate or start one engine:

```sh
docker compose -f bench/compose.yml config --quiet
docker compose -f bench/compose.yml --profile victoriametrics up -d
docker compose -f bench/compose.yml --profile victoriametrics down
```

Named volumes are deliberately retained by `down`; benchmark cleanup must be
an explicit runner action and record the volume size first. Resource defaults
are four CPUs and 4 GiB RAM per engine and can be changed with
`BENCH_CPUS`/`BENCH_MEMORY`.

The compose file is infrastructure, not yet a complete fair benchmark. Each
adapter must implement the common generator/query contract in
`docs/benchmarking.md`, verify result checksums, capture configuration and
durability, and run one engine at a time. `tsink`, RRDtool, SQLite, DuckDB, and
ReductStore are native/embedded adapters and therefore are not defined as
server containers here.

`capabilities.csv` is the honesty gate for published charts. `compose_only`
means an image is pinned but no result-verified energy adapter exists yet;
`smoke_only` must never appear in a performance ranking. Generate the shared
input with:

```sh
cargo run --release -- generate bench-results/workload \
  --sites 1 --days 7 --cadence-seconds 60 --seed 42
```

The checked-in native WattDB/SQLite comparison uses the same mixed-domain
generator and rejects unequal 5-minute results:

```sh
cargo bench --bench energy_compare -- --quick
```

## VictoriaMetrics telemetry subset

The adapter imports only `run_id=0` telemetry, labels it with the dataset CRC,
and verifies every 5-minute count/sum/min/max bucket before emitting JSON. It
refuses a database that already contains the dataset.

```sh
docker compose -f bench/compose.yml --profile victoriametrics up -d --pull never
python3 bench/adapters/victoriametrics.py bench-results/workload
docker compose -f bench/compose.yml --profile victoriametrics down
```

Use a fresh named volume for a measured run. The result is a telemetry-subset
server/HTTP chart, not a full workload or native embedded comparison: catalog,
plans, three-dimensional revisions, and DST calendar totals are explicitly
reported as unsupported.
