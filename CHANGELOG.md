# Changelog

This file records user-visible FTWDB changes. The project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) and keeps release
steps in [docs/releases.md](docs/releases.md).

## [Unreleased]

### Added

- Sealed raw segments are published through the store manifest and used on the
  query path. `Store::seal_and_reclaim` rewrites `active.wlog` to catalog,
  identity receipts, and the unsealed tail so reopen does not reload every
  historical point.
- Salvage recovers sealed `.wseg` coverage and the live `active.wlog` tail
  instead of refusing stores that have already sealed raw history. A missing
  or unreadable sealed segment fails closed.
- Ordered ingress frames now store source, sequence, commit ID, and transaction
  as one checked unit. Exact retries compare the original bytes and return the
  original receipt across reopen.
- A bounded single-writer runtime separates request errors from storage faults
  and tracks accepted and durable progress per source.
- A draft, hand-written metadata wire codec and local Unix shadow sidecar
  carry catalog, run, plan, point, and outcome data without joining FTW's
  control path.
- The sidecar checks each Unix peer's effective UID and drains the writer on
  SIGTERM or SIGINT before it removes the socket and exits.
- Checked systemd and launchd examples keep the store and socket private and
  give the sidecar time to finish a clean stop.
- An offline reconcile command compares exact source commit frames with stored
  receipts, catalog state, and point bits and emits a bounded JSON summary.
- Reconciliation binds every receipt to its exact canonical payload, caps raw
  points scanned before time filtering, and bounds regular-file inputs.
- A clean client EOF at a frame boundary no longer raises the sidecar's client
  error count; partial frames still do.

### Fixed

- Release publication now reads notes from the annotated tag through a file,
  which works with an explicit GitHub repository target.
- A live or later-sealed correction now wins when all three time keys match an
  older sealed point.
- Log reclaim sorts ingress receipts and retains their exact bytes. CRC32
  equality alone can no longer accept a changed retry.
- This version still reads the old exact-receipt index, but older binaries
  cannot read a store after the new writer reclaims it.
- Catalog compaction rejects run and plan cycles instead of writing a partial
  catalog.
- Store paths, segment links, manifest order, reserved fields, rollup values,
  and SD-card ACK evidence now fail on invalid input.

## [0.1.0-alpha.1] - 2026-07-21

First public evaluation release.

### Added

- Checksummed atomic point and catalog transactions with explicit durability.
- Valid, knowledge, and change time plus run and plan records.
- Immutable raw segments, persistent fixed and calendar rollups, late-data
  rebuilds, and retention safety checks.
- Read-only integrity checks, verified snapshot backup, strict no-clobber
  restore, and conservative raw-log salvage.
- File locking, commit identifiers for safe retry, bounded decoders, and
  structured command errors.
- Deterministic energy and TSBS workloads, a sanitized real-data fixture, an
  SD-card emulator, and result-verified VictoriaMetrics and QuestDB subsets.

### Verified

- CI passes on Ubuntu Linux and macOS with Rust 1.97.1.
- CI and release actions use verified full commit SHA pins.
- Process-kill, full-disk, sync-failure, corruption, recovery, CLI, backup,
  restore, and salvage checks pass.
- The crate packages and verifies from its release source.

### Known limits

- FTWDB supports Unix targets only.
- The active log still rebuilds an in-memory read index, and physical raw-log
  reclamation remains disabled.
- Physical target-board and SD-card power cuts during commits, long soak runs,
  and remote backup policy remain open M4 work.
- Competitor adapters cover stated subsets and do not claim equal durability
  or full model support.
- This release does not carry a production support or file-format stability
  promise.

[Unreleased]: https://github.com/srcfl/ftwdb/compare/v0.1.0-alpha.1...HEAD
[0.1.0-alpha.1]: https://github.com/srcfl/ftwdb/releases/tag/v0.1.0-alpha.1
