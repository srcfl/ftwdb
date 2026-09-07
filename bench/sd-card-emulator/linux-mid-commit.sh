#!/usr/bin/env bash
set -euo pipefail

# Cut NBD power during a live Durability::Always ingest. Verify recovered
# counts against the last fsynced ACK, not the full-workload totals.
#
# This needs Linux, root, and the nbd module. On macOS run it in the same
# privileged container used by linux-smoke.sh.

source bench/sd-card-emulator/linux-nbd-common.sh

out=${FTW_NBD_MID_OUTPUT:-/work/bench-results/linux-nbd-mid-commit}
profile=${FTW_NBD_PROFILE:-bench/sd-card-emulator/profiles/healthy.json}
cut_after_acks=${FTW_NBD_CUT_AFTER_ACKS:-3}
batch_points=${FTW_NBD_BATCH_POINTS:-10000}
seed=${FTW_NBD_SEED:-42}

if [[ -e "$out" ]] && find "$out" -mindepth 1 -print -quit | grep -q .; then
  printf 'output directory must be absent or empty: %s\n' "$out" >&2
  exit 1
fi
linux_nbd_prepare
cleanup() {
  if [[ -n ${writer_pid:-} ]]; then
    kill "$writer_pid" >/dev/null 2>&1 || true
    wait "$writer_pid" >/dev/null 2>&1 || true
  fi
  linux_nbd_cleanup
}
trap cleanup EXIT

serve_extra=()
if [[ -n ${FTW_NBD_POWER_LOSS_AFTER_OPS:-} ]]; then
  serve_extra+=(--power-loss-after-ops "$FTW_NBD_POWER_LOSS_AFTER_OPS")
fi
linux_nbd_start "$profile" "$seed" "${serve_extra[@]}"
linux_nbd_connect
mkfs.ext4 -q -F /dev/nbd0
linux_nbd_mount

ack_log=$out/ack.jsonl
: >"$ack_log"

set +e
"$ftw" bench-real-fixture \
  bench/fixtures/ftw-real-v1/points.csv.gz \
  "$mount_dir/database" \
  --durability always \
  --batch-points "$batch_points" \
  --ack-log "$ack_log" \
  >"$out/writer.stdout" 2>"$out/writer.stderr" &
writer_pid=$!
set -e

wait_for_acks() {
  local needed=$1
  local file=$2
  local n
  for _ in $(seq 1 600); do
    if [[ -f "$file" ]]; then
      n=$(grep -c '"format":"ftwdb-ack-watermark-v1"' "$file" || true)
      if [[ "$n" -ge "$needed" ]]; then
        return 0
      fi
    fi
    if ! kill -0 "$writer_pid" 2>/dev/null; then
      return 1
    fi
    sleep 0.1
  done
  return 1
}

if [[ -z ${FTW_NBD_POWER_LOSS_AFTER_OPS:-} ]]; then
  if ! wait_for_acks "$cut_after_acks" "$ack_log"; then
    set +e
    wait "$writer_pid"
    writer_exit=$?
    set -e
    printf 'writer exited before %s durable acks (exit=%d)\n' "$cut_after_acks" "$writer_exit" >&2
    cat "$out/writer.stderr" >&2
    exit 1
  fi
  "$emulator" ctl power-loss >"$out/power-loss.json"
fi

set +e
wait "$writer_pid"
writer_exit=$?
set -e
printf '%d\n' "$writer_exit" >"$out/writer-exit.txt"
if [[ "$writer_exit" -eq 0 ]]; then
  printf 'writer finished the workload before the mid-commit cut\n' >&2
  exit 1
fi

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
"$emulator" ctl status >"$out/status-after-recovery.json"

"$emulator" verify \
  --emulator "$out/status-after-recovery.json" \
  --check "$out/check-after.json" \
  --inspect "$out/inspect-after.txt" \
  --ack-log "$ack_log" \
  --max-in-flight-commits 1 \
  --max-in-flight-points "$batch_points" \
  --checksum-ok true \
  --writer-exit "$writer_exit" \
  --output "$out/verification.jsonl" \
  >"$out/verification.json"

linux_nbd_finish
trap - EXIT

printf 'linux_nbd_mid_commit=passed\n'
printf 'profile=%s\n' "$profile"
printf 'writer_exit=%d\n' "$writer_exit"
printf 'fsck_exit=%d\n' "$fsck_exit"
cat "$out/verification.json"
