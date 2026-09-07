# FTW shadow sidecar

## Scope

The shadow sidecar lets FTW copy data into FTWDB without changing control,
dispatch, or the current source of truth. FTW must keep running when the
sidecar is missing, slow, full, corrupt, or stopped.

The first supported link is a local Unix socket. It has no TCP listener and no
remote write path. One sidecar owns one FTWDB store and one bounded writer.

The sidecar is an evaluation path, not a production data authority. FTW keeps
its current SQLite and Parquet paths until the shadow checks below pass on real
boxes.

## Data contract

One accepted wire batch maps to one atomic FTWDB transaction. A batch can hold:

- entities and topology relations;
- series definitions with units and physical meaning;
- forecast, import, control, optimization, and reconciliation runs;
- plan records and planned setpoints;
- telemetry, prices, forecasts, decisions, and hardware outcomes as points.

Every point keeps FTWDB's full time and source record:

- `valid_time` and exclusive `valid_time_end` in UTC microseconds;
- `knowledge_time`, when the value became known;
- `change_time`, when this revision was recorded;
- `run_id`, which links the value to its source run;
- `quality` and `flags`;
- an IEEE-754 `f64` value.

FTW uses positive power into the site and negative power out of the site. The
adapter must apply that rule before it sends a batch. The catalog must state
the physical quantity, unit, and series meaning; the server must not infer
them from a series name.

## Identity and retry

Each source assigns a stable `source_id`, a strictly increasing `sequence`, and
a unique `commit_id` to every logical batch. A retry must reuse all three IDs
and the exact same transaction bytes.

FTWDB stores the ingress identity in the same checked frame as the records.
After a restart it can return the first receipt for an exact retry. It rejects
a reused source sequence or commit ID when the transaction differs. This rule
avoids both duplicate points and silent replacement of a prior decision.

The client may discard a batch only after the acknowledgement says that the
batch is durable. An accepted but non-durable batch must stay in the client's
bounded memory queue, or in an existing source store that can recreate the
same batch. The shadow path must not add a second small-write spool on the same
SD card.

## Failure boundary

The FTW adapter must use a bounded, nonblocking queue. A full queue drops or
marks shadow work; it never waits in a control or device loop. Connect, encode,
socket write, acknowledgement wait, and retry all run outside those loops.

The sidecar owns the only writable FTWDB handle. It processes commits and
flushes in queue order. A storage I/O or sync error poisons that writer, rejects
later writes, and requires a reopen. A bad client batch returns a request error
without taking down a healthy writer.

The current wire health reply reports writer status, queued operations, the
connected source's accepted and durable watermarks, overload and protocol-error
counts, database bytes, points, commits, recovered tail bytes, the live sync
policy, and whether the last acknowledgement was durable. Older v1 health
frames that omit the trailing ops fields still decode with zeroed counts and
`always` sync policy. The in-process runtime also tracks queue limits,
accepted, acknowledged, and failed counts, all known source watermarks, and the
latest fatal writer error. On clean shutdown, the service log reports the same
ops fields next to accepted clients, peer-auth failures, and client errors.

Snapshot backup is still `ftw backup`. Stop the sidecar first (it holds the
exclusive writer lock), copy the published snapshot off the card, and
restore-verify CRC as in [`operations.md`](operations.md).

## Flash-write policy

The sidecar keeps `Durability::Always` fixed during the first beta work and
sends useful batches instead of one point per transaction. This gives a clear
acknowledgement contract but can issue too many syncs if the source sends small
batches. Do not change that default until target-box write counts and physical
power cuts prove a safer policy.

`Durability::EveryBytes` exists in the storage layer, but the sidecar must not
use it for beta. A later change needs its own target-box write-count results,
physical power-cut evidence, and proof that the client retains and replays each
non-durable batch with the same IDs.

Measure these values on the target box for each policy:

- logical point and transaction bytes;
- bytes written to the FTWDB store;
- sync calls and batches per sync;
- p50, p95, p99, and maximum acknowledgement time;
- queue high-water mark and dropped shadow batches;
- boot replay time and peak resident memory;
- recovered prefix after each forced power cut.

Do not enable raw deletion until immutable raw segments, rollups, manifests,
and restore tests prove that all required data remains available.

## Bounded collection in the FTW beta

The current candidate copies live history alongside SQLite/Parquet. It is an
opt-in experiment, not complete replication: client restart, a full queue, or
a long sidecar outage can leave gaps. The client must report those gaps. It
must not claim that historical corrections, deletes, forecasts, or config are
covered merely because the sidecar accepts those record types.

The command has two positive byte-count settings:

| Setting | Default | Effect |
|---|---:|---|
| `FTWDB_SHADOW_MAX_STORE_BYTES` | 536870912 (512 MiB) | Reject a new frame if it would exceed the store limit. |
| `FTWDB_SHADOW_MIN_FREE_BYTES` | 536870912 (512 MiB) | Keep this much free space, measured with the service user's available blocks, after the frame. |

Invalid settings stop startup. A write that reaches either limit gets the
existing retryable `Overloaded` response with a fixed reason. Health becomes
`Degraded`; accepted and durable watermarks do not advance. Exact retries of
already stored data still receive their durable receipt, including after
restart. A changed retry still conflicts. After freeing disk space or raising the budget, restart the sidecar to
resume new writes.

The bounded writer requires a store with no active rollups and runs without
background maintenance, sealing, or retention. That keeps the size check on
the only append path. `FTWDB_SHADOW_MAINTAIN_SECS` is no longer accepted by the
command. Use a fresh dedicated shadow store. Keep offline maintenance work on
a copy until its peak disk use has a tested budget.

This check does not reserve filesystem blocks against other processes. Keep
SQLite's own disk alerts, monitor memory and CPU, and set service/container
limits. Do not treat a shared filesystem as full isolation. The systemd
example caps memory at 512 MiB and CPU at half a core. Those are evaluation
limits, not measured target-box requirements.

On rollback, stop the sidecar and source copy, retain the current store, and
use a verified snapshot from before the upgrade or a new empty shadow store.
Do not open the upgraded store with the old alpha binary. Do not change the
SQLite/Parquet paths during this drill.

## Local access

The service creates or checks a store root owned by its effective user with
mode `0700`. It refuses a symlink, another owner, or any group or world access.
It also creates a private socket directory and a Unix socket with mode `0600`.
For every accepted connection, Linux and macOS ask the kernel for the peer's
effective UID. The service closes the connection before reading a frame unless
that UID matches the configured service UID. Never expose this protocol through
a TCP proxy in the beta.

The command installs small SIGTERM and SIGINT handlers that only set an atomic
flag. A helper thread turns that flag into a server stop request. The server
then drains the writer, syncs the store, removes its socket, and returns a
normal exit code. The current two-second frame deadline also bounds shutdown
when a connected client stops sending bytes.

The client must send `HELLO` first. The server rejects an unknown major version,
an unknown message kind, set reserved bits, a bad checksum, a frame above the
fixed size limit, a malformed record, and a request before `HELLO`.
A clean EOF before the next frame is a normal disconnect. An EOF inside a
header or body is a client error.

## Frozen protocol fixtures

`testdata/shadow-protocol-v1` contains one hex-encoded frame for every v1
request and response kind, plus separate commit and flush acknowledgements.
The Rust integration test checks both directions against those bytes. The Go
adapter must use the same files; copied values or a second fixture generator do
not count as a shared contract test.

The v1 source sequence is an opaque, strictly increasing source cursor. It may
contain gaps. An exact retry must reuse the same source ID, sequence, commit ID,
and transaction bytes.

## Reconciliation report

`shadow_reconcile::reconcile_shadow_batches` compares a bounded source window
without writing to the store. It checks:

- each source and sequence against its stored ingress receipt;
- the exact canonical transaction bytes stored in that receipt's frame;
- commit ID, record count, point count, and current durability proof;
- the last supplied state of each entity, relation, series, run, and plan;
- exact point multiplicity and every point bit inside each supplied series'
  smallest covered timestamp span.

The report keeps full counts but caps mismatch details. Separate limits cap
input batches, metadata, expected points, observed points, and all raw series
entries visited before timestamp filtering. Catalog checks are one-way because
catalog objects do not yet keep an ingress source ID. The caller must pass
batches in the intended cross-source catalog order. A read-only open can prove
content but cannot claim that a prior writer synced a receipt.

`ftwdb-shadow-reconcile <store-directory> <commit-request.hex> ...` runs this
check offline against exact v1 commit frames and writes one stable JSON summary.
Stop the sidecar first: the read-only opener takes the store's shared lock and
will not bypass its active writer.
Exit code `3` means the command completed and found a content mismatch; a read or input error
uses exit code `2`. The JSON states that a read-only run has no durability
proof, so pair it with the sidecar's live durable watermark.
Each input must be a regular hex file no larger than one encoded protocol
frame. The command also caps frame count, decoded bytes, metadata records, and
points before it opens the store. Its current decoded-input cap is 256 MiB.

## Required tests before an FTW beta

### Contract

- frozen byte fixtures for every v1 message;
- round trips for every catalog record and every point field;
- unknown version, kind, flag, trailing byte, bad checksum, short read, and
  maximum-size cases;
- clean frame-boundary EOF versus a partial-frame EOF;
- Go and Rust encode/decode checks against the same fixtures;
- sign, unit, UTC, interval, revision, run, plan, and outcome examples.

### Retry and order

- exact retry before and after reopen returns the original receipt;
- same sequence with another commit ID fails;
- same commit ID with other data fails;
- a new cursor that is equal to or below the prior cursor fails without poisoning the writer;
- an acknowledgement lost after a durable write does not duplicate data;
- a flush covers every earlier accepted batch and no later batch.

### Failure and load

- hard queue bound under slow and stopped writers;
- stalled and malformed clients cannot grow memory without limit;
- invalid input does not stop later valid input;
- storage write and sync faults poison the writer and stop later storage calls;
- `ENOSPC`, process kill, torn frame, corrupt frame, and restart checks;
- hour, day, and week soaks at the highest real box rate;
- real target-board power cuts during writes and flushes;
- fixed memory and boot-time limits at 14 days, 90 days, and the planned
  retention limit.

### FTW detachment

- unplug or kill the sidecar while FTW controls real or simulated hardware;
- fill the queue and the filesystem while control timing stays within its
  current limit;
- corrupt the shadow store while FTW keeps its current source of truth;
- disable the source flag and sidecar service separately;
- compare source rows, shadow rows, plans, decisions, and outcomes with a
  stable reconciliation report whose receipt checks use exact stored bytes.

## Rollout gates

1. Run protocol and storage tests in CI. Keep the FTW adapter disabled.
2. Enable bounded shadow writes on test boxes. Do not serve reads from FTWDB.
3. Run a long soak and physical power-cut set. Record write volume and recovery.
4. Enable shadow reads only in a diagnostic comparison view.
5. Let selected beta users opt in after rollback, export, and alert checks pass.
6. Move one read path at a time. Keep all hardware control on the current path.

FTWDB must not become a control dependency during these gates. A later move
from shadow data to advisory or live control needs a separate safety review.
