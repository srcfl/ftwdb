#!/usr/bin/env bash
set -euo pipefail

source bench/sd-card-emulator/linux-nbd-common.sh

out=${FTW_NBD_OUTPUT:-/work/bench-results/linux-nbd-smoke}
if [[ -e "$out" ]] && find "$out" -mindepth 1 -print -quit | grep -q .; then
  printf 'output directory must be absent or empty: %s\n' "$out" >&2
  exit 1
fi
linux_nbd_prepare
trap linux_nbd_cleanup EXIT
linux_nbd_start bench/sd-card-emulator/profiles/healthy.json 42
linux_nbd_connect
mkfs.ext4 -q -F /dev/nbd0
linux_nbd_mount

"$ftw" bench-real-fixture \
  bench/fixtures/ftw-real-v1/points.csv.gz \
  "$mount_dir/database" \
  --durability always \
  --batch-points 10000 \
  >"$out/load.json"
sync
"$ftw" check-store "$mount_dir/database" >"$out/check-before.json"
"$ftw" inspect "$mount_dir/database/active.wlog" >"$out/inspect-before.txt"
sha256sum "$mount_dir/database/active.wlog" >"$out/sha-before.txt"
"$emulator" ctl status >"$out/status-before-power-loss.json"

"$emulator" ctl power-loss >"$out/power-loss.json"
linux_nbd_unmount_lazy || true
linux_nbd_disconnect_best_effort || true
"$emulator" ctl reset >"$out/reset.json"
linux_nbd_connect

set +e
e2fsck -fy /dev/nbd0 >"$out/fsck.txt" 2>&1
fsck_exit=$?
set -e
if [[ "$fsck_exit" -gt 1 ]]; then
  printf 'fsck_exit=%d\n' "$fsck_exit" >&2
  exit "$fsck_exit"
fi
printf '%d\n' "$fsck_exit" >"$out/fsck-exit.txt"

linux_nbd_mount
"$ftw" check-store "$mount_dir/database" >"$out/check-after.json"
"$ftw" inspect "$mount_dir/database/active.wlog" >"$out/inspect-after.txt"
sha256sum "$mount_dir/database/active.wlog" >"$out/sha-after.txt"
before_hash=$(cut -d' ' -f1 "$out/sha-before.txt")
after_hash=$(cut -d' ' -f1 "$out/sha-after.txt")
test "$before_hash" = "$after_hash"
"$emulator" ctl status >"$out/status-after-recovery.json"

"$emulator" verify \
  --emulator "$out/status-after-recovery.json" \
  --check "$out/check-after.json" \
  --inspect "$out/inspect-after.txt" \
  --expected-points 889978 \
  --expected-commits 89 \
  --checksum-ok true \
  --output "$out/verification.jsonl" \
  >"$out/verification.json"

# Keep the backup off the emulated card, then prove that a new store restored
# onto the recovered card has the same checked counts and selected bytes.
"$ftw" backup "$mount_dir/database" "$out/restore-backup" \
  >"$out/restore-backup.json"
"$ftw" restore "$out/restore-backup" "$mount_dir/restored-database" \
  >"$out/restore.json"
"$ftw" check-store "$mount_dir/restored-database" \
  >"$out/restore-check.json"
sha256sum "$out/restore-backup/active.wlog" >"$out/restore-backup-sha.txt"
sha256sum "$mount_dir/restored-database/active.wlog" >"$out/restore-target-sha.txt"

json_u64() {
  local file=$1
  local field=$2
  local value
  value=$(sed -n "s/.*\"$field\":\([0-9][0-9]*\).*/\1/p" "$file")
  test -n "$value"
  printf '%s\n' "$value"
}

json_string() {
  local file=$1
  local field=$2
  local value
  value=$(sed -n "s/.*\"$field\":\"\([^\"]*\)\".*/\1/p" "$file")
  test -n "$value"
  printf '%s\n' "$value"
}

test "$(json_u64 "$out/check-after.json" raw_points)" = \
  "$(json_u64 "$out/restore-check.json" raw_points)"
test "$(json_u64 "$out/check-after.json" raw_commits)" = \
  "$(json_u64 "$out/restore-check.json" raw_commits)"
test "$(json_u64 "$out/check-after.json" raw_points)" = \
  "$(json_u64 "$out/restore.json" raw_points)"
test "$(json_u64 "$out/check-after.json" raw_commits)" = \
  "$(json_u64 "$out/restore.json" raw_commits)"
test "$(json_string "$out/restore.json" source_snapshot_crc32)" = \
  "$(json_string "$out/restore.json" destination_snapshot_crc32)"
test "$(cut -d' ' -f1 "$out/restore-backup-sha.txt")" = \
  "$(cut -d' ' -f1 "$out/restore-target-sha.txt")"

linux_nbd_finish
trap - EXIT

printf 'linux_nbd_smoke=passed\n'
printf 'restore_drill=passed\n'
printf 'fsck_exit=%d\n' "$fsck_exit"
printf 'active_log_sha256=%s\n' "$after_hash"
cat "$out/load.json"
cat "$out/check-after.json"
cat "$out/verification.json"
cat "$out/restore.json"
cat "$out/restore-check.json"
