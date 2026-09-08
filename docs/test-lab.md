# FTWDB test lab: archive record

Archive decision: 8 September 2026. FTWDB is a frozen energy-storage test lab.
Development of its own database engine has ended. The source, formats, tests,
sanitized fixtures and measured results remain available under this repository's
Apache-2.0 license. This is not a supported database or an FTW runtime component.

## Where FTW storage now lives

FTW embeds DuckDB in Core for time-series reads and writes, and retains SQLite
for configuration and other state. Core owns the schema, queue, energy rules,
migration, backup and history API. See [the integration](https://github.com/srcfl/ftw/pull/1129)
and [Core's storage package](https://github.com/srcfl/ftw/tree/master/go/internal/state).
Storage defects and new FTW features belong in that repository.

The first evaluation favoured SQLite/Parquet with DuckDB as a read layer. The
later decision moved both history reads and writes to DuckDB in Core. The
earlier measurements remain valid for their stated workload; the earlier
product recommendation does not describe the adopted architecture.

An established engine reduces the amount of storage code FTW must maintain.
It does not supply FTW's energy semantics, queue policy or upgrade guarantees.
Using a supported embedded API also avoids maintaining a private extraction of
another project's internal storage code. A separate FTWDB wrapper or service
would add another version and failure boundary without a demonstrated need.

## What the lab preserves

| Material | Location and scope |
|---|---|
| Experimental Rust engine | `src/`; append log, catalog, segments, rollups, snapshots and shadow protocol |
| Energy semantics | [Energy model](energy-model.md), [rollups](rollups.md), `src/aggregate.rs` and model tests |
| Replay and integrity checks | `tests/properties.rs`, `tests/real_fixture.rs`, `tests/shadow_lifecycle.rs`, `tests/shadow_reconcile_cli.rs` |
| Fault tests | `tests/power_cut.rs`, [SD-card emulator](../bench/sd-card-emulator/README.md), full-disk and mid-commit scripts |
| Repeatable input | [Sanitized 889,978-point fixture](../bench/fixtures/ftw-real-v1/README.md) and deterministic workload generator |
| Engine comparison | [September report](results/2026-09-07-engine-evaluation.md), [archived runners and result samples](../bench/engine-evaluation-2026-09-07/README.md) |
| Earlier results | [Result directory](results/), [benchmark protocol](benchmarking.md), [research references](research.md) |
| Later Core migration check | [Local import evidence](results/2026-09-08-core-import.json), with explicit scope limits |

These are research assets, not a claim that every old runner works with a new
toolchain or that every listed competitor has an equivalent adapter. The
capability registry distinguishes implemented subsets from smoke-only entries.

## Lessons from the custom engine

**Keep energy types distinct.** Power gauges, interval energy and meter
counters need different aggregates. Preserve units, sign rules, quality and
missing intervals. A gap is not zero consumption. A counter reset is not
generation. Calendar buckets must handle local time and daylight-saving changes.

**Keep revisions and provenance.** Measurement time, knowledge time and change
time answer different questions. Forecasts and plans need their run identity.
Late values and corrections must invalidate affected aggregates. Use these
rules and their fixtures in Core where the product needs them; a new file
format is not required to retain them.

**Define the acknowledgement.** Accepted into a queue, written to an OS cache
and durably committed are different states. A batch must recover wholly or
not at all. Keep source identity, sequence and exact retry content together.
The lab found that a valid JSON fragment without a final newline could be
mistaken for a complete acknowledgement; the emulator now tests that boundary.

**Publish before deleting.** Write new files, sync them, publish the manifest
and sync its directory before reclaiming source data. Refuse unsafe retention
when required aggregates are missing or invalid. Checksums must bind both file
contents and the metadata that selects them. Surface a corrupt sealed segment
as an error instead of returning an apparently complete partial answer.

**Bound recovery work as well as steady-state work.** Decoders, indexes,
queues, queries and merge operations all need limits. Reject invalid lengths,
cycles, non-finite values, duplicate identities and unsafe filesystem entries.
Recovery and salvage have different promises: preserve and report corruption;
do not silently turn a repair into a successful full restore.

**Treat storage format changes as upgrade changes.** The newer identity index
uses `WIDX0002`. An older reader cannot open a store after the newer reclaim
path has written that format. An old binary alone is not a rollback plan;
keep a verified pre-upgrade store. See [format notes](format.md).

**Measure writes to the medium.** File size and ingest throughput do not show
write amplification. Include WAL, checkpoints, compaction, filesystem writes,
free-space needs and syncs. A deterministic emulator can repeat a failure; it
cannot establish how a physical SD card behaves when power disappears.

## Lessons carried into the DuckDB integration

The local test of Core commit
`1bd31f6b3fa6d95e3bcc8167d2064c1c7c0c3842` imported 20,998,953 SQLite samples
and 139,587,008 Parquet rows from 131 files. The independent comparison covered
all 160,585,961 samples over 147 UTC days with no differences. Opening and
import took about 387.5 seconds on an Apple M5, not a Pi. The archived JSON
records the exact binary, settings, counts and comparison hash.

This was the earlier blocking import path. A separate short sample-write
probe did not exercise the complete history/sample/energy-ledger transaction.
Neither result proves a full-day live-write soak, a full portable backup and
restore of that large database, or physical power-loss safety.

Subsequent Core work exposed concerns that an engine-only benchmark missed:

- [Background import and upgrade handling](https://github.com/srcfl/ftw/pull/1178):
  keep live collection running, resume from receipts and preserve source files.
  A long migration must report its state; an image-only rollback can interrupt
  an incompatible data upgrade.
- [Memory pressure and retry](https://github.com/srcfl/ftw/pull/1181): a DuckDB
  memory setting is not a cap on total process RSS. Failed transactions and
  native connections need clear ownership. Retry the same queued batch only
  after rollback and bounded maintenance; preserve receipt identity.
- [Visible progress](https://github.com/srcfl/ftw/pull/1182): distinguish checking,
  importing, waiting for live writes and checkpointing. Count compressed source
  bytes honestly; old completed work must not inflate the current import rate.
  Omit an unknown total or ETA instead of presenting a made-up estimate.

Large history reads also competed with the background import on the target
box. That observation does not establish normal performance after import.
Config remains in SQLite, so its response time cannot be attributed to DuckDB.
Core release validation remains separate from this frozen lab record.

## Preserved local work

The archive preparation started from default-branch commit
`6486961ac05f96d1bce230364845b6bc8c2cf811`. It includes the earlier
`8e103e0f32a20ccf74b6dcbce2439dfdd9ecde88` commit and later beta fixes.

The twelve uncommitted files in a separate local checkout of `8e103e0` were
saved byte-for-byte in commit
[`3c701a89fc1c70eba1bb4e39ce2b4d87aec82dc2`](https://github.com/srcfl/ftwdb/commit/3c701a89fc1c70eba1bb4e39ce2b4d87aec82dc2),
on `archive/final-lab-snapshot-2026-09-08`. That branch preserves the original
local state; it is not a replacement for the later fixes already on `main`.
Archive preparation did not promote that snapshot to a release or merge its
storage changes over the newer default branch.

The repository keeps its prior releases, branches, commits and issue history.
The release guide and roadmap describe the former experiment. They do not
promise further releases, maintenance or production support. No site database
or backup needs to be deleted to archive this repository.
