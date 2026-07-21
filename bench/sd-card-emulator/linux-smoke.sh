#!/usr/bin/env bash
set -euo pipefail

source bench/sd-card-emulator/linux-nbd-common.sh

out=${FTW_NBD_OUTPUT:-/work/bench-results/linux-nbd-smoke}
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

linux_nbd_finish
trap - EXIT

printf 'linux_nbd_smoke=passed\n'
printf 'fsck_exit=%d\n' "$fsck_exit"
printf 'active_log_sha256=%s\n' "$after_hash"
cat "$out/load.json"
cat "$out/check-after.json"
cat "$out/verification.json"
