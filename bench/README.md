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

