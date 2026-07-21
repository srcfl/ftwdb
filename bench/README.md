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

The checked-in native FTWDB/SQLite comparison uses the same mixed-domain
generator and rejects unequal 5-minute results:

```sh
cargo bench --bench energy_compare -- --quick
```

## TSBS IoT write adapter

The FTWDB CLI can load the official TSBS IoT Influx line-protocol stream. The
TSBS source revision is pinned in `versions.lock`. Build its
`tsbs_generate_data` program, then run:

```sh
tsbs_generate_data --format influx --use-case iot --seed 123 --scale 100 \
  --timestamp-start 2016-01-01T00:00:00Z \
  --timestamp-end 2016-01-02T00:00:00Z --log-interval 10s \
  | cargo run --release -- bench-tsbs-iot - bench-results/tsbs-ftwdb \
      --batch-rows 10000 --durability always
```

Use a new output directory for each run. The JSON result reports TSBS rows,
FTWDB points, both rates, commit counts, stored bytes, and CRC32 values for the
source bytes and normalized points. `manual` and `every-bytes:N` are separate
durability modes; the runner flushes once before it returns.

TSBS may omit tags and fields and may emit old rows late. The adapter retains
that order and maps each distinct Influx tag set, measurement, and field to a
stable FTWDB series. Influx serializes the three numeric truck attributes as
fields, so they count as points here too.

This is a write adapter, not yet a full TSBS query adapter. Do not put its load
result in a chart that claims TSBS query coverage.

TSBS commit `8323e59` needs `patches/tsbs-timescaledb-2.28.patch` when used
with the pinned TimescaleDB 2.28 image. Apply it in the TSBS checkout before
building `tsbs_load_timescaledb`:

```sh
git apply --unidiff-zero /path/to/ftwdb/bench/patches/tsbs-timescaledb-2.28.patch
```

## Sanitized real-installation fixture

The repository includes a 24-hour fixture derived from a read-only export of
a running FTW installation. It has 889,978 points from 54 active series, real
cadence and values, ten gaps, and a separate energy-ledger sample with quality
and provenance fields. Exact source times, names, identifiers, and the
installation address are absent.

```sh
cargo run --release -- bench-real-fixture \
  bench/fixtures/ftw-real-v1/points.csv.gz bench-results/ftw-real \
  --durability always --batch-points 10000
```

See `fixtures/ftw-real-v1/manifest.json` for counts and checksums. Use this
fixture for realistic replay. Keep TSBS as the standard cross-database load.

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

## QuestDB stored revision subset

The QuestDB adapter checks the bundle summary and every numeric CSV field before
it writes. It stores every `points.csv` row through ILP over HTTP. The table uses
`valid_time` as its designated timestamp and maps the portable `change_time` to
`event_time`; it also stores `knowledge_time`, `valid_time_end`, `series_id`,
`run_id`, value, quality, and flags. The dataset CRC32 tags each row.

Use a new Compose project name for each run. The adapter refuses to run if the
volume contains any table. It also checks that `summary.txt` and `points.csv`
stay byte-identical from validation through import:

```sh
docker compose -p ftwdb-questdb-run-1 -f bench/compose.yml \
  --profile questdb up -d --pull never questdb
python3 bench/adapters/questdb.py bench-results/workload
docker compose -p ftwdb-questdb-run-1 -f bench/compose.yml \
  --profile questdb down
```

`down` keeps the named volume. Record its size before any explicit `down -v`.
The adapter waits until the dataset's exact point count is query-visible. It
then filters site-0 grid actuals (`series_id=1`, `run_id=0`) to
`[first,last)`, which excludes the generator's final closing point. QuestDB
runs `SAMPLE BY 5m ALIGN TO FIRST OBSERVATION`, so an arbitrary start
microsecond stays the bucket anchor. Each count, sum, minimum, and maximum must
match the local result before the adapter emits one JSON line.

The result has `implemented_subset` scope. QuestDB stores all point revisions,
but this adapter queries only the grid actual series. The JSON lists catalog
import, plan-record import, and a DST calendar query as `not_implemented`; it
does not claim that QuestDB lacks those features. Its WAL plus ILP/HTTP
acknowledgement does not claim the same durability as FTWDB.
The first real 9.4.3 check is recorded in
[`docs/results/2026-07-21-questdb-9.4.3.md`](../docs/results/2026-07-21-questdb-9.4.3.md).
