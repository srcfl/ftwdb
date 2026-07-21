# Changelog

This file records user-visible FTWDB changes. The project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) and keeps release
steps in [docs/releases.md](docs/releases.md).

## [Unreleased]

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
