# OSS database research

Reviewed 2026-07-21 from primary project documentation, repositories, and
papers. “All databases” is not a finite or stable set; this is the maintained
OSS comparison set plus historically important designs. The benchmark registry
tracks exact versions separately.

## Energy-specific reference

[Rebase TimeDB](https://github.com/rebase-energy/timedb) models
`valid_time`, `knowledge_time`, and `change_time`, keeps forecast revisions and
corrections append-only, supports point-in-time/relative forecast queries, and
uses a ClickHouse `MergeTree` ordered by series and all three time dimensions.
[Rebase EnergyDB](https://github.com/rebase-energy/energydb) adds asset trees,
grid edges, units, a series catalog, workflow/run provenance, and structural
diffs in PostgreSQL. Those domain ideas are directly relevant. Requiring two
server databases is not suitable for FTWDB's embedded edge target.

## Storage designs reviewed

| System | Useful pattern | FTWDB implication |
|---|---|---|
| [VictoriaMetrics](https://docs.victoriametrics.com/victoriametrics/#storage) | immutable sorted parts, per-series blocks, atomic part publication, background merges | adopt immutable publication and free-space-aware merging; add checksums and explicitly measure merge writes |
| [Prometheus TSDB](https://prometheus.io/docs/prometheus/latest/storage/) | WAL-protected head, immutable time blocks, chunk/index separation | good recovery/block model; two-hour head and metrics-only semantics are not the energy product model |
| [InfluxDB TSM](https://docs.influxdata.com/influxdb/v1/concepts/storage_engine/) | WAL + cache flushed to immutable TSM files with compaction | baseline LSM/TSM design; quantify its compaction and WAL write amplification |
| [QuestDB](https://questdb.com/docs/architecture/storage-engine/) | parallel WAL, column files, time partitions, explicit out-of-order merge path | partition late data and bound the rewritten range; keep hot write path row-oriented and cold reads columnar |
| [ClickHouse MergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree) | sorted immutable parts, partition pruning, column codecs | model for analytic scans and Rebase's 3D ordering; too heavy as an embedded edge dependency |
| [TimescaleDB](https://github.com/timescale/timescaledb) | time chunks, continuous aggregates, relational metadata | continuous-aggregate semantics and invalidation are key comparison points |
| [RRDtool](https://www.rrdtool.org/rrdtool/doc/rrdtool.en.html) | fixed-size round-robin archives and continuous consolidation | bounded-storage mode is valuable, but FTWDB must preserve revisions/plans and support raw retention policies |
| [SQLite](https://sqlite.org/atomiccommit.html) | precisely documented atomic commit and storage assumptions | copy its rigor about filesystem assumptions; do not assume powersafe overwrite on SD cards |
| [tsink](https://github.com/h2337/tsink) | Rust embedded/server modes, segmented WAL, leveled compaction, adaptive encodings | closest Rust/embedded comparison; include it in every engine benchmark stage |
| [GreptimeDB](https://github.com/GreptimeTeam/greptimedb) | Rust distributed TSDB, regions, indexes, object storage | useful Rust implementation comparison, but operational scope differs radically |
| [Apache IoTDB](https://github.com/apache/iotdb) | device-oriented schema, aligned series, IoT benchmark tooling | include device/edge workloads and aligned multi-sensor batches |
| [TDengine](https://github.com/taosdata/TDengine) | IoT-oriented supertables, retention and stream processing | compare ingestion, downsampling, and device cardinality |
| [GridDB](https://github.com/griddb/griddb) | IoT containers and time-series operations | additional IoT SQL baseline |
| [M3DB](https://github.com/m3db/m3) | distributed metrics storage and index separation | relevant scale reference, not an edge deployment peer |
| [OpenTSDB](https://github.com/OpenTSDB/opentsdb) | metric/tag model on a distributed KV store | historical tag-index baseline; not a write-constrained embedded design |
| [ReductStore](https://github.com/reductstore/reductstore) | Rust edge/industrial blob time series | compare edge footprint and sequential record workloads |

## Compression research

- The [Gorilla paper](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf)
  establishes delta-of-delta timestamp and XOR floating-point encoding as the
  classic streaming baseline.
- [Chimp](https://www.vldb.org/pvldb/vol15/p3058-liakos.pdf) reports a better
  compression/speed trade-off than earlier streaming float codecs and belongs
  in the codec bake-off.
- [ALP](https://ir.cwi.nl/pub/33334/33334.pdf) is a vectorizable adaptive
  lossless float codec suitable for columnar blocks.
- Rebase's ClickHouse schema uses Delta/DoubleDelta for IDs/timestamps, Gorilla
  for values, and Zstd as a second layer. FTWDB should benchmark codecs per
  real energy series rather than selecting one globally.

Candidate block policy: detect fixed step first; compare delta-varint and
delta-of-delta timestamps; compare bit-pack/frame-of-reference for fixed-point
energy values and Gorilla/Chimp/ALP for floats; optionally apply LZ4 or Zstd to
the complete column block. Store the chosen codec in every block header.

## Practices adopted

- append-only revisions and historical-knowledge queries from Rebase;
- immutable, atomically published parts from VictoriaMetrics/ClickHouse;
- WAL/frame recovery contracts from Prometheus, QuestDB, and SQLite;
- mergeable continuous aggregates from Timescale and RRDtool;
- explicit out-of-order rewrite accounting from QuestDB;
- adaptive per-block compression evaluated against Gorilla, Chimp, and ALP;
- an engine-specific energy benchmark in addition to
  [TSBS](https://github.com/timescale/TSBS), because TSBS does not exercise
  plans, bitemporal forecasts, calendar totals, or SD write amplification.

