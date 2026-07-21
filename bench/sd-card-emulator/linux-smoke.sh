#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq e2fsprogs kmod nbd-client util-linux

out=${FTW_NBD_OUTPUT:-/work/bench-results/linux-nbd-smoke}
mount_dir=/mnt/ftw-sd
emulator_target=/tmp/ftw-sd-emulator-target
ftw_target=/tmp/ftw-target
emulator="$emulator_target/release/ftw-sd-emulator"
ftw="$ftw_target/release/ftw"

mkdir -p "$out" "$mount_dir"
CARGO_TARGET_DIR="$ftw_target" cargo build --release --locked
FTW_SD_EMULATOR_COMMIT=${FTW_SD_EMULATOR_COMMIT:-working-tree} \
  CARGO_TARGET_DIR="$emulator_target" cargo build --release --locked \
  --manifest-path bench/sd-card-emulator/Cargo.toml

emulator_pid=
cleanup() {
  set +e
  mountpoint -q "$mount_dir" && umount -l "$mount_dir"
  nbd-client -d /dev/nbd0 >/dev/null 2>&1
  "$emulator" ctl shutdown >/dev/null 2>&1
  if [[ -n "$emulator_pid" ]]; then
    wait "$emulator_pid" >/dev/null 2>&1
  fi
}
trap cleanup EXIT

"$emulator" serve \
  --config bench/sd-card-emulator/profiles/healthy.json \
  --backing "$out/card.img" \
  --seed 42 \
  --metrics "$out/emulator.jsonl" \
  >"$out/server.json" 2>"$out/server.log" &
emulator_pid=$!

for _ in $(seq 1 100); do
  if "$emulator" ctl status >"$out/initial-status.json" 2>/dev/null; then
    break
  fi
  sleep 0.05
done
test -s "$out/initial-status.json"

nbd-client 127.0.0.1 10809 /dev/nbd0 -N ftw-sd
mkfs.ext4 -q -F /dev/nbd0
mount /dev/nbd0 "$mount_dir"

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
umount -l "$mount_dir" || true
nbd-client -d /dev/nbd0 >/dev/null 2>&1 || true
"$emulator" ctl reset >"$out/reset.json"
nbd-client 127.0.0.1 10809 /dev/nbd0 -N ftw-sd

set +e
e2fsck -fy /dev/nbd0 >"$out/fsck.txt" 2>&1
fsck_exit=$?
set -e
if [[ "$fsck_exit" -gt 1 ]]; then
  printf 'fsck_exit=%d\n' "$fsck_exit" >&2
  exit "$fsck_exit"
fi
printf '%d\n' "$fsck_exit" >"$out/fsck-exit.txt"

mount /dev/nbd0 "$mount_dir"
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

umount "$mount_dir"
nbd-client -d /dev/nbd0
"$emulator" ctl shutdown >"$out/shutdown.json"
wait "$emulator_pid"
emulator_pid=
trap - EXIT

printf 'linux_nbd_smoke=passed\n'
printf 'fsck_exit=%d\n' "$fsck_exit"
printf 'active_log_sha256=%s\n' "$after_hash"
cat "$out/load.json"
cat "$out/check-after.json"
cat "$out/verification.json"
