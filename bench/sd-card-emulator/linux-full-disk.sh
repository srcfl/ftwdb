#!/usr/bin/env bash
set -euo pipefail

source bench/sd-card-emulator/linux-nbd-common.sh

out=${FTW_NBD_FULL_OUTPUT:-/work/bench-results/linux-nbd-full-disk}
if [[ -e "$out" ]] && find "$out" -mindepth 1 -print -quit | grep -q .; then
  printf 'output directory must be absent or empty: %s\n' "$out" >&2
  exit 1
fi
linux_nbd_prepare
trap linux_nbd_cleanup EXIT
linux_nbd_start bench/sd-card-emulator/profiles/full-disk-64m.json 17
linux_nbd_connect
mkfs.ext4 -q -F -m 0 /dev/nbd0
linux_nbd_mount

set +e
"$ftw" bench-real-fixture \
  bench/fixtures/ftw-real-v1/points.csv.gz \
  "$mount_dir/database" \
  --durability always \
  --batch-points 10000 \
  >"$out/writer.stdout" 2>"$out/writer.stderr"
writer_exit=$?
set -e
printf '%d\n' "$writer_exit" >"$out/writer-exit.txt"

if [[ "$writer_exit" -ne 1 ]]; then
  printf 'writer exit was %d, expected 1\n' "$writer_exit" >&2
  exit 1
fi
if ! grep -q 'No space left on device' "$out/writer.stderr"; then
  printf 'writer failed without ENOSPC\n' >&2
  cat "$out/writer.stderr" >&2
  exit 1
fi

"$ftw" check-store "$mount_dir/database" >"$out/check.json"
"$ftw" inspect "$mount_dir/database/active.wlog" >"$out/inspect.txt"
if ! grep -Eq '"raw_points":[1-9][0-9]*' "$out/check.json"; then
  printf 'store check found no durable points\n' >&2
  exit 1
fi
if ! grep -Eq '"raw_commits":[1-9][0-9]*' "$out/check.json"; then
  printf 'store check found no durable commits\n' >&2
  exit 1
fi

"$emulator" ctl status >"$out/status-after-enospc.json"
linux_nbd_finish
trap - EXIT

printf 'linux_nbd_full_disk=passed\n'
printf 'writer_exit=%d\n' "$writer_exit"
printf 'writer_error=ENOSPC\n'
cat "$out/check.json"
