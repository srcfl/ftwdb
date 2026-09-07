# FTW SD-card emulator

This standalone Rust crate exposes a sparse file as an NBD block device. Put a
real Linux filesystem on `/dev/nbd0`, then run FTWDB on that filesystem. This
keeps the filesystem, page cache, block layer, and database in the test path.

CI uses the full workload and the healthy card's cache and power-loss settings,
with fast read/write timing from the full-disk profile. It checks filesystem
faults and recovery, not SD latency. The generated profile travels with the CI
evidence. Local scripts keep the slow healthy profile unless `FTW_NBD_PROFILE`
selects another profile; use those runs for the separate timing experiment.

The model can inject:

- read and write bandwidth and IOPS limits;
- base latency, jitter, and long latency spikes;
- a volatile write cache with correct or false `FLUSH` and FUA replies;
- dropped, reordered, and torn cached writes on power loss;
- returned `EIO`, silent bit changes, and torn writes;
- erase-block wear, permanent bad blocks, read-only mode, and disappearance;
- fixed operation-triggered faults that replay with the same seed.

The server implements fixed-newstyle NBD with named export `ftw-sd`, simple
replies, reads, writes, flush, FUA, and disconnect. It follows the
[NBD protocol](https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md).
It binds to localhost by default and has no access control or TLS. Do not expose
its data or control ports to another host.

## Build and test

Run these commands from the repository root:

```sh
cargo test --manifest-path bench/sd-card-emulator/Cargo.toml
cargo build --release --manifest-path bench/sd-card-emulator/Cargo.toml
```

The crate has its own manifest and lock file. It does not add emulator code or
dependencies to FTWDB.

Validate one of the checked-in profiles:

```sh
bench/sd-card-emulator/target/release/ftw-sd-emulator validate \
  bench/sd-card-emulator/profiles/healthy.json
```

The profiles are starting points, not measured claims about a named SD card:

- `healthy.json` has no returned or silent media faults.
- `cheap-consumer.json` has slow random writes, long stalls, rare false flushes,
  and accelerated wear.
- `nearly-worn.json` raises fault rates and reaches bad blocks sooner.
- `sudden-power-loss.json` cuts the device at operation 1,000. Override that
  point with `--power-loss-after-ops N` for a seeded test matrix.
- `full-disk-64m.json` is a fast 64 MiB device with no injected media faults.
  The full-disk script uses it to reach a real ext4 `ENOSPC` result quickly.

## Linux NBD smoke test

NBD needs a Linux host or VM, the kernel `nbd` module, `nbd-client`, and root
rights for device and mount work. The NBD server itself runs without root.

On macOS, OrbStack can run the full Linux, NBD, ext4, power-loss, filesystem
repair, and FTWDB check in one isolated container:

```sh
docker run --rm --privileged \
  -e FTW_SD_EMULATOR_COMMIT="$(git rev-parse --short=12 HEAD)" \
  -v "$PWD":/work -w /work rust:1.97-slim-bookworm \
  bash bench/sd-card-emulator/linux-smoke.sh
```

The script installs `nbd-client` and `e2fsprogs` in the disposable container.
It writes raw results under the ignored `bench-results/linux-nbd-smoke`
directory, which must be absent or empty when the run starts. It uses the
checked-in real-installation fixture and rejects a
changed active-log checksum or recovered FTWDB watermark. After recovery, it
backs up the checked store outside the emulated card, restores that backup to a
new store on the card, and compares checked counts, snapshot CRCs, and active
log SHA-256 values. It then adds a seven-byte short tail to an off-card copy,
salvages it to another new store on the card, and requires a `partial` result,
seven discarded bytes, 889,978 points, 89 commits, equal snapshot CRCs, and the
clean raw-log SHA-256 value.

```sh
mkdir -p bench-results/sd-emulator

bench/sd-card-emulator/target/release/ftw-sd-emulator serve \
  --config bench/sd-card-emulator/profiles/healthy.json \
  --backing bench-results/sd-emulator/card.img \
  --seed 42 \
  --metrics bench-results/sd-emulator/emulator.jsonl
```

In another shell:

```sh
sudo modprobe nbd max_part=8
sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N ftw-sd
sudo mkfs.ext4 -F /dev/nbd0
sudo mkdir -p /mnt/ftw-sd
sudo mount /dev/nbd0 /mnt/ftw-sd
sudo chown "$(id -u):$(id -g)" /mnt/ftw-sd

target/release/ftw bench-tsbs-iot bench-results/tsbs-iot-smoke.influx \
  /mnt/ftw-sd/database --batch-rows 1000 --durability always

bench/sd-card-emulator/target/release/ftw-sd-emulator ctl status
```

Use the fixed TSBS smoke data from the benchmark task for a shared baseline:
seed 42, IoT, 100 trucks, one hour, ten-second cadence. It has 64,941 rows,
513,478 FTWDB points, input CRC32 `0fc89c22`, and point CRC32 `965c6ea9`.

## Linux full-disk gate

The full-disk gate uses the same Linux/NBD setup and cleanup as the power-loss
smoke test. It writes the real fixture with `Durability::Always` to a 64 MiB
ext4 filesystem until the kernel returns `ENOSPC`, then opens the store
read-only and runs both `check-store` and `inspect` on the durable prefix.

Run it from the repository root on Linux, or in the same privileged container
used by the smoke test:

```sh
docker run --rm --privileged \
  -e FTW_SD_EMULATOR_COMMIT="$(git rev-parse --short=12 HEAD)" \
  -e FTW_NBD_FULL_OUTPUT=/work/bench-results/linux-nbd-full-disk \
  -v "$PWD":/work -w /work rust:1.97-slim-bookworm \
  bash bench/sd-card-emulator/linux-full-disk.sh
```

`FTW_NBD_FULL_OUTPUT` must name an absent or empty directory. A passing run
exits zero and saves the writer output, store check, inspect output, emulator
status, and shutdown result there. Stdout ends with these three lines followed
by one JSON line:

```text
linux_nbd_full_disk=passed
writer_exit=1
writer_error=ENOSPC
```

The final JSON line must report positive `raw_points` and `raw_commits`.
The script exits nonzero if the writer does not return `ENOSPC`, returns a
different exit code, or either store check fails. This privileged script stays
outside normal CI. The quick emulator result covers the #17 disk-full gate;
physical SD-card power cuts remain a separate M4 release gate.

## Linux mid-commit power-cut

The smoke test cuts after `sync`. `linux-mid-commit.sh` cuts **during** a live
`Durability::Always` ingest. The writer fsyncs one JSONL watermark line after
each durable commit (`--ack-log`). After `ctl power-loss` (or a seeded
`--power-loss-after-ops` cut), the script runs e2fsck, reopens the store, and
verifies recovered counts against the last complete ACK: every acked batch is
present, at most one in-flight batch is missing, and a torn tail is only an
incomplete header or payload.

Run it from the repository root on Linux, or in the same privileged container:

```sh
docker run --rm --privileged \
  -e FTW_SD_EMULATOR_COMMIT="$(git rev-parse --short=12 HEAD)" \
  -e FTW_NBD_MID_OUTPUT=/work/bench-results/linux-nbd-mid-commit \
  -v "$PWD":/work -w /work rust:1.97-slim-bookworm \
  bash bench/sd-card-emulator/linux-mid-commit.sh
```

`FTW_NBD_PROFILE` selects the emulator profile (`healthy.json` by default).
Set it to `bench/sd-card-emulator/profiles/cheap-consumer.json` to include
false flushes. `FTW_NBD_CUT_AFTER_ACKS` (default 3) is how many durable ACK
lines must land before `ctl power-loss`. For a seeded cut, set
`FTW_NBD_POWER_LOSS_AFTER_OPS` and use `profiles/sudden-power-loss.json`.
`FTW_NBD_MID_OUTPUT` must be absent or empty.

Host software tests cover the ACK parser and prefix verifier without NBD.
This privileged script stays outside normal CI.

## Write amplification

`ctl status` and `--metrics FILE.jsonl` report `write_bytes`, `persisted_bytes`,
and `write_amplification` (`persisted_bytes / write_bytes` when any writes
landed). Those are emulator-model ratios for that run, not a claim about a
named SD card. Read them from the JSONL after a Linux NBD job. Do not invent
or copy numbers from another profile.

## Nearly-worn EIO

`profiles/nearly-worn.json` raises fault rates and can return `EIO` during
reads and writes. Format the filesystem on `healthy.json` first: the same
profile's EIO probability can fail `mkfs`. Then reopen the backing image with
`nearly-worn.json` and ingest until the writer sees `EIO`. Keep the durable
prefix with `check-store`. This path is probabilistic; pin a seed and keep the
JSONL. Physical wear-out remains an M4 hardware gate.

## Power-loss run

Start the writer, then cut the virtual card from another shell:

```sh
bench/sd-card-emulator/target/release/ftw-sd-emulator ctl power-loss
```

The control command applies the profile's deterministic cached-write outcomes,
syncs that damaged media state, marks the device offline, and drops active NBD
connections. Restore and inspect it with:

```sh
sudo umount -l /mnt/ftw-sd
sudo nbd-client -d /dev/nbd0
bench/sd-card-emulator/target/release/ftw-sd-emulator ctl reset
sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N ftw-sd
sudo fsck.ext4 -fy /dev/nbd0
sudo mount /dev/nbd0 /mnt/ftw-sd

target/release/ftw check-store /mnt/ftw-sd/database \
  > bench-results/sd-emulator/check.json
target/release/ftw inspect /mnt/ftw-sd/database/active.wlog \
  > bench-results/sd-emulator/inspect.txt
bench/sd-card-emulator/target/release/ftw-sd-emulator ctl status \
  > bench-results/sd-emulator/status.json
```

The benchmark controller must record the FTWDB watermark that the writer had
already acknowledged. Verify recovered counts and append one normalized JSONL
row with:

```sh
bench/sd-card-emulator/target/release/ftw-sd-emulator verify \
  --emulator bench-results/sd-emulator/status.json \
  --check bench-results/sd-emulator/check.json \
  --inspect bench-results/sd-emulator/inspect.txt \
  --expected-points 513478 \
  --expected-commits 65 \
  --checksum-ok true \
  --output bench-results/sd-emulator/fault-runs.jsonl
```

`verify` exits with a nonzero status if recovered point or commit counts differ
from the acknowledged watermark, or if the caller reports a checksum failure.
Its JSON includes profile, seed, fault kind and offset, writer exit data,
recovered counts, tail bytes, injected faults, dropped/reordered/torn writes,
wear counts, and emulator version and commit.

For a cut during ingestion, do not use the full-workload counts shown above.
Pass the last counts that the writer confirmed before the cut. A test cannot
prove the durable-write rule without that watermark.

## Control commands

The control port accepts one line per connection:

```text
status
power-loss
reset
detach
read-only
read-write
flush
shutdown
```

Each response is one JSON object. `--metrics FILE` also appends each response to
an fsynced JSONL file outside the emulated card.

## Limits

This is a fault model, not a copy of a vendor's NAND controller or flash
translation layer. It does not yet persist wear counters across emulator
process restarts. Calibrate latency, failure rates, and wear against the target
cards, boards, filesystems, free-space levels, and workloads. Keep physical
power-cut tests on the release gate; the emulator makes failures repeatable but
cannot replace target hardware evidence.
