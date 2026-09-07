#!/usr/bin/env bash

# Shared setup for the privileged Linux NBD evidence scripts. Callers source
# this file, set `out`, then call these functions from the repository root.

linux_nbd_prepare() {
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq e2fsprogs kmod nbd-client util-linux

  mount_dir=${FTW_NBD_MOUNT_DIR:-/mnt/ftw-sd}
  emulator_target=${FTW_NBD_EMULATOR_TARGET:-/tmp/ftw-sd-emulator-target}
  ftw_target=${FTW_NBD_FTW_TARGET:-/tmp/ftw-target}
  emulator="$emulator_target/release/ftw-sd-emulator"
  ftw="$ftw_target/release/ftw"

  mkdir -p "$out" "$mount_dir"
  if mountpoint -q "$mount_dir"; then
    printf 'mount directory is already in use: %s\n' "$mount_dir" >&2
    return 1
  fi
  CARGO_TARGET_DIR="$ftw_target" cargo build --release --locked
  FTW_SD_EMULATOR_COMMIT=${FTW_SD_EMULATOR_COMMIT:-working-tree} \
    CARGO_TARGET_DIR="$emulator_target" cargo build --release --locked \
    --manifest-path bench/sd-card-emulator/Cargo.toml
  emulator_pid=
  nbd_connected=0
  mount_active=0
}

linux_nbd_cleanup() {
  set +e
  if [[ ${mount_active:-0} -eq 1 ]]; then
    umount -l "$mount_dir"
  fi
  if [[ ${nbd_connected:-0} -eq 1 ]]; then
    nbd-client -d /dev/nbd0 >/dev/null 2>&1
  fi
  if [[ -n ${emulator_pid:-} ]]; then
    kill "$emulator_pid" >/dev/null 2>&1
    wait "$emulator_pid" >/dev/null 2>&1
  fi
}

linux_nbd_start() {
  local profile=$1
  local seed=$2
  shift 2

  "$emulator" serve \
    --config "$profile" \
    --backing "$out/card.img" \
    --seed "$seed" \
    --metrics "$out/emulator.jsonl" \
    "$@" \
    >"$out/server.json" 2>"$out/server.log" &
  emulator_pid=$!

  for _ in $(seq 1 100); do
    if "$emulator" ctl status >"$out/initial-status.json" 2>/dev/null; then
      break
    fi
    sleep 0.05
  done
  test -s "$out/initial-status.json"
}

linux_nbd_connect() {
  nbd-client 127.0.0.1 10809 /dev/nbd0 -N ftw-sd
  nbd_connected=1
}

linux_nbd_mount() {
  mount /dev/nbd0 "$mount_dir"
  mount_active=1
}

linux_nbd_unmount_lazy() {
  if umount -l "$mount_dir"; then
    mount_active=0
  fi
}

linux_nbd_disconnect_best_effort() {
  if nbd-client -d /dev/nbd0 >/dev/null 2>&1; then
    nbd_connected=0
  fi
}

linux_nbd_finish() {
  umount "$mount_dir"
  mount_active=0
  nbd-client -d /dev/nbd0
  nbd_connected=0
  "$emulator" ctl shutdown >"$out/shutdown.json"
  wait "$emulator_pid"
  emulator_pid=
}
