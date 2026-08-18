# Integrity checks, backup, restore, and salvage

## Integrity check

`ftw check-store <directory>` opens the store read-only: the active commit
log is opened without write access under a shared lock, torn-tail recovery is
simulated in memory, and no manifest generation is published, pruned, or swept.
It loads the highest valid manifest generation and re-opens every active rollup
segment, validating checksums, encoded lengths, aggregate invariants,
descriptor coverage, and raw-source watermarks. The command emits a single JSON
record with raw commit/point counts, active rollup file/bucket/byte counts, and
`raw_recovered_tail_bytes` plus `raw_recovered_tail` fields that distinguish an
`incomplete-header`, an `incomplete-payload`, and no recovery. It also reports a
`stale_rollup_files` count of rollups whose provenance trails the raw log — state
a writable open would reconcile but a check only reports. These are additive
fields in the `ftwdb-integrity-v1` JSON object.

Inactive files are not required for the current database state and are not
included in this check. Rollup files that no retained manifest generation
references are removed automatically after publication and at writable
startup; a read-only open never deletes them.

## Snapshot backup

`ftw backup <source> <absent-destination>` opens the source read-only, so a
backup can never alter (or accidentally create) the store it is copying, and
uses this publication order:

1. integrity-check the source;
2. create a hidden sibling directory;
3. copy and sync `active.wlog`;
4. hard-link active immutable rollups and the selected manifest when source and
   destination share a filesystem, otherwise copy and sync them;
5. sync both subdirectories and the temporary root;
6. publish the temporary root with an atomic no-clobber rename;
7. sync the parent directory;
8. open the published backup read-only and integrity-check it.

Backup and restore use Linux `renameat2(RENAME_NOREPLACE)` or the matching
Apple exclusive rename through `rustix`. They do not use an `exists()` check
followed by a replacing rename. On an unsupported Unix target, publication
returns an unsupported-operation error instead of weakening this rule. If the
final parent sync or post-check fails, the code verifies the directory identity
and tries to move its own publication back to its hidden name, remove it, and
sync the parent. If that rollback also fails, the error says that rollback
failed; the caller must inspect the named destination before retrying.

The active log is always copied, never hard-linked, because later appends to a
shared inode would silently mutate the backup. A unit test writes to the source
after publication and proves the backup point count remains unchanged.

The `ftwdb-backup-v1` JSON record includes `linked_files`, `copied_files`,
`hard_link_fallbacks`, and `hard_link_fallback_error_kinds`. Error kinds use
fixed names such as `crosses-devices`, `permission-denied`, and `storage-full`;
the command does not put raw operating-system error text in JSON. The active
log counts as copied. If a hard link fails but the copy works, the report keeps
the link error kind. If both fail, the returned I/O error keeps the copy error
kind and its text and source also include the hard-link failure.

This is a local consistent snapshot, not yet a remote backup policy. Encryption,
incremental upload, retention, and salvage of a corrupted source remain open.

## Strict restore

`ftw restore <backup> <absent-target>` restores only a fully valid snapshot.
The command exits with code 2 for missing or extra arguments and code 1 for a
store, file, or publication error. It has no replace option. An existing target,
including an empty directory or a dangling symlink, causes `AlreadyExists` and
stays unchanged.

Restore opens the backup read-only under a shared lock, runs the full store
check, and refuses any raw-log recovery. Both recovered tail bytes and a
recovery reason must be zero. A short final header or payload, a full bad frame,
a bad selected manifest, a bad active rollup, or an active rollup whose source
watermark trails the raw log causes an error. Orphan rollups, older manifest
generations, and other files outside the selected snapshot do not affect the
restore and do not appear in the target.

The selected snapshot contains:

- `active.wlog`, always copied;
- the selected manifest generation, when one exists;
- each active immutable rollup named by that manifest;
- each sealed raw segment named by that manifest.

Every selected path must be a regular file according to `symlink_metadata` and
the opened file identity. Read-only file opens use no-follow and nonblocking
flags, and snapshot checksum traversal uses directory file descriptors, so
restore rejects symlinks and special files without blocking or leaving the
snapshot root. It copies the selected files to a unique hidden sibling
directory, syncs each file and directory, and opens that stage read-only for a
full check. The stage lock stays held through no-clobber publication and the
target's read-only post-check, so a writer cannot append between publication
and verification.

The snapshot checksum uses the CRC32 domain `FTWDB snapshot CRC32 v1\0` and the
selected relative paths sorted by UTF-8 bytes. Each file adds, in order, the
path byte length as little-endian `u64`, the path bytes, the file length as
little-endian `u64`, and the exact file bytes. The byte count is the sum of
those file lengths. Permissions, times, directories, and unselected files do
not enter the checksum. Restore compares source and stage before publication,
then source and target after publication.

On success, `ftwdb-restore-v1` reports `files`, `bytes`,
`manifest_generation`, `raw_commits`, `raw_points`,
`source_snapshot_crc32`, and `destination_snapshot_crc32`. The CRC values are
eight lower-case hex digits. Equal CRCs are a deterministic corruption check,
not proof that the selected bytes match, and they do not turn CRC32 into a
cryptographic authenticity check.

Restore does not repair a damaged backup and does not infer data past a corrupt
frame. Use a separate target, keep the source backup, and run `ftw check-store`
on the result.

## Strict salvage

`ftw salvage <damaged-store> <absent-target>` copies the longest valid raw-log
prefix into a new store, together with sealed raw segments the recovered
manifest still names. Pass `--drop-orphan-segments` to ignore `.wseg` files on
disk that the recovered manifest does not reference instead of failing closed.
The command exits with code 2 for missing or extra
arguments. A valid database header followed by a damaged frame produces a
verified `partial` result and exit code 0. Header, source-open, lock,
source-race, sealed-segment, stage-check, or publication errors use exit code
1. The command never changes the source. It has no replace option, and it
leaves any existing target, including an empty directory or dangling symlink,
unchanged.

Salvage opens the source directory without following a symlink, then opens
`active.wlog` relative to that directory with no-follow and nonblocking flags.
Both the directory and that file must keep the same file identity during the
run, and `active.wlog` must be a regular file. The command takes a shared lock
on that file. Damaged, stale, and orphan rollups do not affect the recovered
raw prefix. Sealed `.wseg` files named by a readable manifest are copied and
re-attached so historical raw stays queryable. A missing or unreadable sealed
segment fails closed; salvage never publishes a store that silently dropped
that coverage.

The scanner first requires a valid FTWDB-v1 database header. It then validates
each frame in order: bounds, kind and version, header and payload CRC, payload
duplicate commit IDs, decode, catalog and transaction rules, and point limits.
It stops before the first failed frame. It never searches for a later `WBAT`
marker, and it never copies frames after a failure. This means that salvage
cannot recover sound data that happens to follow damaged data. A complete last
frame with a bad CRC counts as damage; salvage cannot prove that an interrupted
write caused it.

The fixed `stop_reason` values are:

- `clean-eof`;
- `incomplete-frame-header` and `incomplete-frame-payload`;
- `invalid-frame-magic`, `unsupported-frame-version`, and
  `frame-header-checksum-mismatch`;
- `invalid-legacy-frame-size`, `transaction-frame-too-large`,
  `identified-transaction-too-short`, `ingress-transaction-too-short`, and
  `unknown-frame-kind`;
- `payload-checksum-mismatch`, `duplicate-commit-id`,
  `duplicate-ingress-sequence`, `invalid-ingress-sequence`,
  `invalid-transaction`, `transaction-point-count-too-large`, and
  `invalid-catalog-transaction`;
- `seal-checkpoint-invalid` and `identity-index-invalid`.

After a valid database header, each listed frame or transaction fault yields a
`partial` result, including a fault in the first frame. A header-only source is
`clean`; a bad first frame can produce a checked header-only target with zero
commits and points. An invalid or unsupported database header is fatal and
publishes no target.

The new store contains the header and validated frames as `active.wlog`, empty
`rollups`, and — when sealed coverage exists — the recovered segment files plus
a manifest that names only those segments. Salvage uses the same hidden-stage,
file sync, directory sync, identity rollback, and atomic no-clobber publication
as restore. It opens both the stage and target read-only and runs the full store
check with no recovery. The shared stage lock stays held through publication,
the target check, and any rollback.

The source-prefix CRC32 uses the restore snapshot domain. It always covers
`active.wlog` at the recovered prefix length and bytes. When sealed segments
are recovered, their relative paths and exact bytes are included too. The
destination snapshot covers the same paths and bytes. Salvage compares the
values before and after publication. CRC32 detects many accidental changes but
does not prove authenticity.

On success, `ftwdb-salvage-v1` reports `status`, `source_bytes`,
`recovered_prefix_bytes`, `discarded_bytes`, `stop_offset`, `stop_reason`,
`recovered_commits`, `recovered_points`, `source_prefix_crc32`, and
`destination_snapshot_crc32`. `status` is `clean` only at a checked frame
boundary at end of file; otherwise it is `partial`.

## Sync and full-disk checks

The unit suite injects one sync failure into each `Durability::Always` writer
path. Both `append` and `commit` must return the injected I/O error kind once,
then reject every writer call as poisoned. The existing pipe-based `flush`
test still exercises a real kernel sync failure. On Linux, a separate
`/dev/full` test requires `StorageFull` and the same poisoned state; it skips
only when `/dev/full` does not exist.

The privileged
[`linux-full-disk.sh`](../bench/sd-card-emulator/linux-full-disk.sh) test puts a
real ext4 filesystem on the 64 MiB NBD profile, writes durable fixture batches
until `ENOSPC`, then checks the readable durable prefix. See the emulator
README for its command, output files, and pass result. This does not replace
the M4 physical SD-card power-cut release gate.

## Command-line checks

`tests/cli.rs` runs the built `ftw` binary as a subprocess. It fixes the usage
contract at exit code 2 with usage text on standard error, while data, file,
and store errors use exit code 1. The test covers the generated workload,
sanitized real fixture, TSBS IoT, integrity check, log inspection, backup,
strict restore, and salvage paths. It parses each promised JSON record and
checks its main counts and status fields.

The test also snapshots every path and every file byte in a store before and
after both `check-store` and `inspect`. Separate missing-path checks prove that
neither command creates its input. The backup check covers linked and copied
file counts plus the hard-link fallback count on a normal local filesystem.
The restore check covers JSON, exact bytes, counts, checksum equality,
corruption refusal, and no-clobber behavior. The salvage checks cover clean and
partial results, every frame and transaction stop class, no resync, source
identity, special files, concurrent no-clobber publication, rollback, and the
sanitized fixture. Physical SD-card tests remain an M4 release gate.
