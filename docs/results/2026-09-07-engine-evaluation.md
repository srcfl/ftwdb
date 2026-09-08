# Engine evaluation, 7 September 2026

This report records the lab evidence behind the later DuckDB/Core decision.
The current decision and the custom engine's lessons are in the
[archive record](../test-lab.md). These are historical measurements of pinned
versions, not a ranking of current database products.

## Method and limits

All main runs used the same sanitized FTW day: 889,978 points across 54 series,
with identical timestamps and float64 values. The compressed fixture SHA-256
is `31d65e589f07b36faac8f4be3d2a2ca38aadef9390aa96d84c744cf13783f298`.
Each main configuration used three fresh databases, batches of 10,000 points
and five query repeats. The runner rotated engine order. Queries followed the
import with warm OS caches. Three repetitions do not establish a stable p95
or long-term behaviour; this study used fewer repetitions than the general
lab protocol recommends.

The host was an Apple M5 running ARM64 Linux through OrbStack. Each engine had
one CPU, 512 MiB and no swap. A separate full-workload run at 256 MiB finished
for each engine. Reaching a small memory budget in one bounded run does not
prove enough headroom for continuous operation or large history.

SQLite and DuckDB used small HTTP adapters through the same local bridge as
the servers. The SQLite adapter used string keys, while Core interns names to
integer IDs. These measurements cover the adapters, not pure engine calls or
the entire FTW process. Core's energy ledger, live queues and full API were
outside this comparison.

| Engine/version and tested mode | Submit one day | Per-series day aggregate | Five-minute groups |
|---|---:|---:|---:|
| SQLite 3.53.3, WAL/FULL | 3.81 s | 180.7 ms | 908.4 ms |
| SQLite 3.53.3, WAL/NORMAL | 3.58 s | 179.6 ms | 919.3 ms |
| DuckDB 1.5.5 | 2.31 s | 15.7 ms | 38.9 ms |
| VictoriaMetrics 1.151.0 | 0.35 s | Different query | Different query |
| InfluxDB 3 Core 3.10.0 | 8.87 s | 269.5 ms | 270.1 ms |
| ClickHouse 26.3.32.14 LTS, RowBinary | 2.46 s | 28.8 ms | 70.6 ms |

The table uses medians. Day queries returned count, sum, min and max per
series; five-minute queries returned count and sum for 14,990 groups. The VM
probe only counted points over 24 hours, so it does not share a query ranking.
Submission times have different acknowledgement and durability contracts and
must not be presented as equivalent durable-write throughput.

Raw repeats, settings, image digests, result validation and the original
[summary](../../bench/engine-evaluation-2026-09-07/results/summary.json) are
preserved in the [evaluation archive](../../bench/engine-evaluation-2026-09-07/README.md).

## Same Parquet file, different read paths

The Go adapter wrote a roughly 1 MiB Parquet file using Core's schema. Both
readers used that exact file with one CPU and 256 MiB. The Go path read the
whole day and then selected a series; DuckDB queried Parquet directly.

| Query | Go whole-day read | DuckDB Parquet read |
|---|---:|---:|
| Count and sum, all points | 300.7 ms | 9.0 ms |
| Count and sum, one series | 240.5 ms | 10.6 ms |

Each row pools 15 warm measurements across three new processes. First-query
times are also preserved. Go read 889,978 rows to select 17,122. A separate
DuckDB readback checked all timestamps and values without differences.
The file SHA-256 is
`be9c3b795bd2d4494143ee823ff0f50877b0464c41b8fe2361e5e0310308db3b`.

This supports pushing filters and column selection into the reader. It does
not show that FTW as a whole became 20–30 times faster. The file used the Core
schema but was generated from the sanitized fixture. The study did not compare
an improved streaming Go reader or the later DuckDB primary-history path.

## Values, retries and durability

SQLite, DuckDB and InfluxDB read back the full day bit-for-bit. VM differed in
119,433 float values, with a maximum absolute error of about 7.75e-9. That is
small for many charts but is not an unchanged original record. ClickHouse's
tested CSV path differed in 1,634 values by at most 8.88e-16; enabling precise
float parsing did not fix those cases. RowBinary preserved all 889,978 values
in all three runs. This is evidence about the transfer path, not a general
claim that ClickHouse's storage format loses precision.

A separate probe sent 50 points and killed the process after its response.
The kill command returned 115–160 ms after acknowledgement. SQLite FULL,
SQLite NORMAL, DuckDB, InfluxDB and the configured ClickHouse recovered 50/50.
VM recovered 0/50 with its normal buffering. Each configuration had one such
probe. This tests process death, not host power loss or physical SD behaviour.

SQLite FULL syncs the WAL at commit; NORMAL has a weaker power-loss promise.
The InfluxDB run used `no_sync=false` and a 100 ms WAL interval. The ClickHouse
run enabled `fsync_after_insert` and `fsync_part_directory`. VM's buffered
acceptance and the probe's explicit visibility flush are not equivalent to a
durable per-batch commit. See [SQLite synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous)
and [VM storage documentation](https://docs.victoriametrics.com/victoriametrics/).

The original source review traced InfluxDB 3.10.0's local WAL through
`object_store` 0.12.5. Its local write path used write/rename or hardlink without
file and directory fsync. The review therefore did not treat that local ACK as
equivalent to SQLite FULL. This is a version-specific code inference, not a
measured power-cut result. References: [WAL](https://github.com/influxdata/influxdb/blob/v3.10.0/influxdb3_wal/src/object_store.rs),
[adapter](https://github.com/influxdata/influxdb/blob/v3.10.0/influxdb3_clap_blocks/src/object_store.rs),
[local file code](https://github.com/apache/arrow-rs-object-store/blob/v0.12.5/src/local.rs).
The evaluation also flagged Core's per-query Parquet file limit: a long-history
test must include enough files and snapshot cycles to exercise it, rather than
inferring a fixed time horizon from a one-day import.

Retry behaviour differed too. SQLite replaced a same-key value; DuckDB's COPY
rejected the duplicate primary key; InfluxDB selected the later same-identity
value; VM and plain ClickHouse MergeTree retained duplicate rows. FTW must
define retry identity and correction rules itself, regardless of engine.

Offline restore into fresh volumes returned all 889,978 points for SQLite and
DuckDB; SQLite also returned `integrity_check=ok`. The comparison did not test
online backup, long compaction cycles, full storage media or physical power cuts.
The custom FTWDB engine has separate fault tests; those results are not an
interchangeable baseline for these adapter measurements.

## Reuse and licensing

The checked versions use these base-project licenses: [DuckDB: MIT](https://github.com/duckdb/duckdb/blob/v1.5.5/LICENSE),
[InfluxDB Core: MIT or Apache-2.0](https://github.com/influxdata/influxdb/blob/v3.10.0/Cargo.toml),
[VictoriaMetrics OSS: Apache-2.0](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.151.0/LICENSE),
and [ClickHouse OSS: Apache-2.0](https://github.com/ClickHouse/ClickHouse/blob/v26.3.32.14-lts/LICENSE).
These permit commercial reuse under their terms. Retain required licenses and
notices, and check bindings, extensions and enterprise editions separately.
This record is not a complete dependency-license inventory.

The measured deployment form matters separately from the license. DuckDB and
SQLite ran as embedded libraries behind adapters. VM, InfluxDB and ClickHouse
ran as separate servers. Extracting internal server code would create a new
maintenance burden even when its license permits it.

PostgreSQL, TimescaleDB, QuestDB, RocksDB and chDB were discussed as alternatives
but were not run in this September study. Earlier QuestDB subset results in
this repository have their own scope. Server ClickHouse numbers are not chDB
measurements. No result here supports a general speed claim for those untested
alternatives, for config reads, or for the physical FTW box.
