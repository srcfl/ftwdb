# Integrity checks and backup

## Integrity check

`ftwdb check-store <directory>` opens and recovers the active commit log,
loads the highest valid manifest generation, and re-opens every active rollup
segment. It validates checksums, encoded lengths, aggregate invariants,
descriptor coverage, and raw-source watermarks. The command emits a single JSON
record with raw commit/point counts and active rollup file/bucket/byte counts.

Inactive files are not required for the current database state and are not
included in this check. Rollup files that no retained manifest generation
references are removed automatically after publication and at startup.

## Snapshot backup

`ftwdb backup <source> <absent-destination>` uses this publication order:

1. flush and integrity-check the source;
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

This is a local consistent snapshot, not yet a remote backup policy. Encryption,
incremental upload, retention, restore drills, and salvage of a corrupted source
remain operational work.
