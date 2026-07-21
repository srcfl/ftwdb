# Integrity checks and backup

## Integrity check

`ftwdb check-store <directory>` opens the store read-only: the active commit
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

`ftwdb backup <source> <absent-destination>` opens the source read-only, so a
backup can never alter (or accidentally create) the store it is copying, and
uses this publication order:

1. integrity-check the source;
2. create a hidden sibling directory;
3. copy and sync `active.wlog`;
4. hard-link active immutable rollups and the selected manifest when source and
   destination share a filesystem, otherwise copy and sync them;
5. sync both subdirectories and the temporary root;
6. rename the temporary root to the requested absent destination;
7. sync the parent directory;
8. open and integrity-check the published backup.

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
incremental upload, retention, restore drills, and salvage of a corrupted source
remain operational work.
