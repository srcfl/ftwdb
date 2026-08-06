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

The current wire health reply reports writer status, queued operations, and the
connected source's accepted and durable watermarks. The in-process runtime also
tracks queue limits, accepted, acknowledged, and failed counts, all known
source watermarks, and the latest fatal writer error.

Before beta, an operations endpoint or service log must also expose overload
and protocol-error counts, database bytes, points, commits, recovered tail
bytes, sync policy, and whether the last acknowledgement was durable.

## Flash-write policy

The safe first beta uses `Durability::Always` and sends useful batches instead
of one point per transaction. This gives a clear acknowledgement contract but
can issue too many syncs if the source sends small batches.

`Durability::EveryBytes` can cut sync traffic after the client proves that it
retains every non-durable batch and can replay it with the same IDs. A flush
advances the durable watermark for all earlier accepted batches. Manual
durability is not a beta default.

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

## Local access

The service creates or checks a store root owned by its effective user with
mode `0700`. It refuses a symlink, another owner, or any group or world access.
It also creates a private socket directory and a Unix socket with mode `0600`.
A later hardening step must check the peer UID or GID on each accepted
connection. Never expose this protocol through a TCP proxy in the beta.

The client must send `HELLO` first. The server rejects an unknown major version,
an unknown message kind, set reserved bits, a bad checksum, a frame above the
fixed size limit, a malformed record, and a request before `HELLO`.

## Required tests before an FTW beta

### Contract

- frozen byte fixtures for every v1 message;
- round trips for every catalog record and every point field;
- unknown version, kind, flag, trailing byte, bad checksum, short read, and
  maximum-size cases;
- Go and Rust encode/decode checks against the same fixtures;
- sign, unit, UTC, interval, revision, run, plan, and outcome examples.

### Retry and order

- exact retry before and after reopen returns the original receipt;
- same sequence with another commit ID fails;
- same commit ID with other data fails;
- a sequence gap fails without poisoning the writer;
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
  stable reconciliation report.

## Rollout gates

1. Run protocol and storage tests in CI. Keep the FTW adapter disabled.
2. Enable bounded shadow writes on test boxes. Do not serve reads from FTWDB.
3. Run a long soak and physical power-cut set. Record write volume and recovery.
4. Enable shadow reads only in a diagnostic comparison view.
5. Let selected beta users opt in after rollback, export, and alert checks pass.
6. Move one read path at a time. Keep all hardware control on the current path.

FTWDB must not become a control dependency during these gates. A later move
from shadow data to advisory or live control needs a separate safety review.
