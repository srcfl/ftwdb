# Archived engine evaluation

Historical runs from 7 September 2026. Read the [report](../../docs/results/2026-09-07-engine-evaluation.md) for methods, results and limits, and the [archive record](../../docs/test-lab.md) for the later DuckDB/Core decision.

`results/` retains the raw submission and query timing samples, validation, settings and failures. Main SQL results use three fresh runs per engine, with `binary-clickhouse-*` for ClickHouse. `parquet-*` compares the same Parquet file. `bounded-*`, `semantics-*` and `restore.json` cover separate scopes. `smoke*` and `precise-*` include setup or rejected transfer paths and must not enter the main ranking.

The archive removes workstation paths, detailed process/cgroup dumps and volume file listings. It retains recorded memory peaks and footprint totals. Timing and validation values are unchanged. `provenance.json` records original and archived SHA-256 values and each transformation. No private site export, running database, credential or backup is included. The sanitized input fixture was already public in this repository.

`harness/` preserves the original Go/Python adapters and runner, with one path change: `FTWDB_EVAL_FIXTURE` or the checked-in fixture replaces a workstation path. The old report generator is omitted because it would recreate a superseded product recommendation. The archived runners were syntax-checked during preservation; the database workloads were not rerun.

## Reading the evidence

- `results/fixture.json`: input identity and counts.
- `results/environment.json`: original platform, versions and artifact hashes. Hashes for binaries and the generated Parquet file refer to original run artifacts, which are not shipped here.
- `results/summary.json`: medians from the successful comparison paths.
- `results/main-*`, `results/binary-*`: repeated full-day imports and queries.
- `results/parquet-*`: first and warm query times over one identical file.
- `results/semantics-*`: one process kill and duplicate-identity probes per configuration.
- `results/restore.json`: offline restore to fresh volumes.

## Reusing the runners

Work in a scratch copy so the historical results stay unchanged. The runners create Docker containers, an internal network and named volumes with the `ftwdb-eval-` prefix. They expect Linux ARM64 tools, a Go-built `bin/sqlite-http`, DuckDB 1.5.5 in `bin/python-site`, the image digests in `harness/run.py`, and `results/`, `logs/`, `snapshots/` directories under the evaluation root. Set `FTWDB_EVAL_FIXTURE` to the absolute path of `bench/fixtures/ftw-real-v1/points.csv.gz` when moving the runner.

The Go adapter uses the pinned dependencies in `harness/go.mod` and `go.sum`. `run.py` accepts engine, repeat, memory, row-limit and prefix arguments; inspect `--help` before running. RowBinary uses `FTWDB_EVAL_CH_BINARY=1`. `parquet_probe.py` expects the Go-generated `snapshots/day.parquet` from a preceding SQLite run. Archive/restore probes expect the earlier stopped test volumes. They are not self-contained benchmarks without this setup.

Keep first-run and warm timings separate, verify every result, record new hardware and versions, and use fresh prefixes and volumes. Stopping a run retains its volumes; deletion is a separate operator action. A fork that revives this lab needs to validate its own dependencies and execution environment.
