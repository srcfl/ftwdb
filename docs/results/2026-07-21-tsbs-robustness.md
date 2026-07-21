# TSBS and robustness run: macOS ARM64, 2026-07-21

This is a developer benchmark, not a final engine ranking. It records the first
repeatable TSBS IoT write run, the full TimescaleDB IoT query set, FTWDB model
queries, a sanitized real-installation replay, process-kill tests, a full-disk
test, constrained-storage runs, an SD fault emulator, and a large local backup.

The compact raw values are in
[`2026-07-21-tsbs-summary.json`](2026-07-21-tsbs-summary.json). Criterion keeps
its raw files under the ignored `target/criterion` directory. The power-cut
test keeps JSONL under the ignored `bench-results/power-cut` directory.

## Test identity

- FTWDB commit before these benchmark additions:
  `f082cb2a6ca69412403c31d96e17895c7f7e193f`.
- TSBS commit: `8323e59c74027b108f4ad5ec5d3e498b0101a02e`.
- Host: Apple M5, 10 logical CPUs, 32 GiB RAM, macOS 26.5.1, APFS.
- Docker: 29.4.0, Linux ARM64 VM, four CPUs and 4 GiB per server.
- Native FTWDB results used Rust nightly 1.99.0 because Rust 1.97.1 was not
  installed on the host. The constrained Linux runs used Rust 1.97.1.
- TSBS tools used Go 1.26.5.

TSBS master still calls the removed `replication_factor => NULL` argument in
`create_hypertable`. The run applied
[`tsbs-timescaledb-2.28.patch`](../../bench/patches/tsbs-timescaledb-2.28.patch)
to work with TimescaleDB 2.28.3.

## TSBS write workload

The write workload used 500 trucks, six hours, a ten-second interval, and seed
42. Both formats contain 1,944,639 TSBS rows. The Influx line-protocol form
contains 15,376,101 numeric fields. The TimescaleDB form treats three static
truck values as tags and reports 9,723,473 metrics. Compare row rates, not the
two tools' metric rates.

| Target and mode | Five row/s results | Median row/s |
|---|---|---:|
| FTWDB, one final sync | 392,726; 411,172; 416,068; 411,042; 408,776 | 411,042 |
| FTWDB, sync every 10,000-row batch | 338,892; 341,128; 337,761; 337,115; 337,930 | 337,930 |
| TimescaleDB, four workers | 722,935; 842,012; 864,442; 879,405; 866,237 | 864,442 |
| QuestDB, four-worker client submit | 1,869,589; 3,177,357; 2,509,984; 2,957,639; 2,370,967 | 2,509,984 |

FTWDB stored 1,110,199,619 bytes, or 72.203 bytes per numeric point. Its
durable mode was about 18% slower than its one-sync mode. Each FTWDB run loaded
2,718 tag-set entities, 40,023 series, and 195 batches.

Do not rank the four rows as equal tests. FTWDB ran as a single native process.
The servers ran in a Linux VM and used four workers. TimescaleDB returned a
transaction result. The QuestDB TSBS client uses its line-protocol TCP path and
stops timing after it sends the input; this run did not prove a durable server
watermark. FTWDB expanded each numeric field into a point, while the two servers
kept multi-field rows.

## Sanitized real-installation replay

A read-only export from a running FTW installation supplied a second FTWDB
write shape. The source had 9,462,903 points across 54 active series over 14
days. The committed fixture keeps a 24-hour slice with all 54 series and
889,978 points. It removes source dates, names, identifiers, and the
installation address. It keeps values, cadence, jitter, two driver groups, and
ten gaps. A separate 546-row system energy sample keeps its quality and
provenance fields.

Five fresh runs used 10,000-point batches:

| Durability | Five point/s results | Median point/s |
|---|---|---:|
| One final sync | 6,453,561; 6,865,227; 6,796,399; 6,984,618; 6,966,503 | 6,865,227 |
| Sync each batch | 1,688,129; 1,685,050; 1,816,235; 1,693,410; 1,755,678 | 1,693,410 |

Both modes produced the same 889,978 points, 54 series, 89 commits, normalized
point CRC32 `9ebde920`, and 64,085,678 stored bytes. The result is faster than
the TSBS row adapter because each real-fixture CSV row is one FTWDB point and
the catalog is small. Do not compare these point rates to TSBS row rates.

## TSBS TimescaleDB queries

The query database used 100 trucks for 24 hours: 1,555,511 rows, 7,778,685
metrics, and 252,671,679 database bytes. Each query type had ten warm-up calls
and 90 measured calls with one worker. The run did not drop the host or server
cache, so these are warm developer results.

| Query | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|
| Last location | 0.246 | 0.587 | 0.653 |
| Low fuel | 0.221 | 0.283 | 0.301 |
| High load | 0.214 | 0.304 | 0.341 |
| Stationary trucks | 0.870 | 1.890 | 2.151 |
| Long driving sessions | 13.790 | 14.413 | 15.342 |
| Long daily sessions | 62.283 | 64.161 | 73.187 |
| Actual versus projected fuel | 31.005 | 34.323 | 34.975 |
| Daily driving duration | 58.427 | 67.979 | 115.795 |
| Daily driving session | 60.683 | 66.123 | 98.347 |
| Average load | 21.230 | 23.748 | 24.376 |
| Daily activity | 66.851 | 70.255 | 73.275 |
| Breakdown frequency | 61.499 | 67.331 | 69.383 |

Current TSBS code has the full IoT query generator for TimescaleDB and the old
InfluxDB interface. It can load IoT data into QuestDB but cannot generate its
IoT queries. The repository's InfluxDB 3 image does not support the old TSBS
write and InfluxQL interfaces. The new FTWDB adapter therefore reports
write-only scope and must not appear in a TSBS query ranking yet.

## FTWDB model queries

The new `model_queries` Criterion target verifies every answer before timing a
warm in-process query over 230,000 points.

| Query | Median |
|---|---:|
| Latest revision | 1.579 ms |
| Full history | 109.0 us |
| As known at time | 619.6 us |
| Selected run | 1.154 ms |
| Plan versus outcome | 23.67 ms |
| Materialized five-minute Gauge rollup | 5.292 us |

The plan query needs work. It uses exact timestamps and scans vectors. The
benchmark does not cover resampling or grouping by asset. Gauge is still the
only model with stored rollups. Counter has a standalone aggregate but no
stored query path. IntervalTotal, State, and Event do not have aggregate query
paths. The workload also lacks Relation and fleet topology data.

## Constrained-storage runs

Docker's block-device limits were checked with a direct 20 MiB write. A 4 MiB/s
limit completed in 4.97 seconds at 4.0 MiB/s. FTWDB then ran with one CPU,
512 MiB RAM, `Durability::Always`, 64,941 rows, and 513,478 points.

| Profile | Batch rows | Commits | Rows/s | Points/s |
|---|---:|---:|---:|---:|
| 4 MiB/s, 100 write IOPS | 10,000 | 7 | 7,289 | 57,636 |
| 4 MiB/s, 100 write IOPS | 1,000 | 65 | 7,226 | 57,137 |
| 20 MiB/s, 500 write IOPS | 10,000 | 7 | 36,848 | 291,352 |

These results show a write-bandwidth limit. They do not model flash erase
blocks, controller caches, wear levelling, a false flush response, or a torn
physical page. Only tests on the target board, filesystem, and SD cards can
provide that evidence.

## SD fault emulator

The standalone Rust crate under `bench/sd-card-emulator` exposes sparse media
through NBD on Linux. Its profiles can set bandwidth, IOPS, latency tails,
volatile cache behavior, false flush replies, power loss, torn and reordered
writes, wear, bad blocks, EIO, silent bit changes, read-only state, and device
loss. Ten protocol and model tests passed, including FTWDB acknowledged-
watermark checks.

An OrbStack container then ran Linux 7.0.11 on ARM64 with a 1 GiB emulated NBD
device and ext4. FTWDB loaded the 889,978-point real fixture with per-batch
durability at 81,668 points/s. After a host `sync`, the test cut power. The
emulator dropped one 4 KiB write. `e2fsck` replayed the journal and repaired
free-block and free-inode counts. FTWDB recovered all 889,978 acknowledged
points and all 89 commits, cut zero tail bytes, and kept active-log SHA-256
`04b9987229646214a45274e3d3245a25d50f5ead2b1c02185d55dd8a2b69dba3`.

The checked-in OrbStack script repeats this flow and rejects a changed log hash
or FTWDB watermark. This cut happened after the completed load and host sync;
it does not cover power loss during a commit. The profiles are test inputs, not
measured properties of a named SD-card model.

## Abrupt stop and full disk

The process test starts a separate writer, waits for durable acknowledgements,
sends real `SIGKILL`, opens the file again, and checks for a gap-free sequence
without partial batches. All 32 seeded rounds passed. Every round had zero
recovered tail bytes, so the run tested process loss on APFS rather than torn
media writes.

A second run killed a Linux container during a 4 MiB/s constrained write. It
exited with code 137. A new container recovered and verified 118,714 points in
15 complete commits. It also had no torn tail.

A 16 MiB filesystem forced `ENOSPC`. The writer returned an explicit operating
system error. Reopen removed a 39,535-byte partial tail, retained 229,308 points
in 29 commits, and passed `check-store`.

## Backup

The CLI backed up a checked 1,110,199,619-byte FTWDB store in 4.18 seconds. The
source and backup each contained 15,376,101 points and 195 commits. Both passed
`check-store`; both active logs had SHA-256
`29027557f99738fa03727251ac262352a1a9064226fe7e17ebec4081ae9cb0eb`.

This proves a local, self-contained snapshot. FTWDB still lacks a restore
command, scheduled restore drills, remote upload, encryption, incremental
backup, retention, and salvage from a damaged source.

## Risks found

1. A complete final frame with a bad payload CRC is treated as an interrupted
   tail and removed. That rule can hide bit rot in a batch already acknowledged
   by `Durability::Always`. Normal open should fail on a complete bad frame; a
   separate repair command may choose to remove it.
2. FTWDB has no process lock. A two-writer trial let both writers commit and
   produced both copies of the dataset. The trial did not corrupt the log, but
   it confirms that the database does not enforce its single-writer rule.
3. The new active log does not have a clear parent-directory sync after file
   creation.
4. The emulator covers returned I/O errors and several media faults. The suite
   still needs a Linux NBD matrix, failed host `fsync`, corrupt inactive
   manifests and segments, and runs on the target board and cards.
5. The active log rebuilds its full in-memory index on open. Long edge runs need
   raw compaction, retention, and bounded-memory restart tests.

## Current conclusion

FTWDB has good logical batch recovery, explicit full-disk failure, fast stored
Gauge rollups, a working local backup, a fixed real-data replay, and a
repeatable SD fault model. It is not ready for unattended SD-card use. The
last-frame CRC policy, lack of a writer lock, missing model query paths, and
lack of target-hardware power-cut evidence remain release blockers.
