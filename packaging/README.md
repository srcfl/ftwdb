# Managed shadow sidecar examples

These files are install examples, not ready-made production units. Change the
binary path, service user, group, store path, and socket path for each target.
The FTW client and `ftwdb-shadow` must run with the same effective user ID. The
server rejects a client with another user ID.

Keep the store and socket parent at mode `0700`. Do not put either path in
`/tmp`, a home directory, or a directory shared with another service. The
examples expose only a Unix socket. They do not open a TCP or UDP port.

## systemd on Linux

Copy `systemd/ftwdb-shadow.service` to the system unit directory after changing
all target values. `StateDirectory` and `RuntimeDirectory` create the two
writable paths while `ProtectSystem=strict` keeps the rest of the file system
read-only for the process.

The sample user is `ftw`. Use the account that also runs the FTW client. Do not
set `DynamicUser=yes`: the sidecar and client need one stable, shared user ID.

Run these checks on the target box before enabling the unit:

```sh
systemd-analyze verify /etc/systemd/system/ftwdb-shadow.service
systemd-analyze security ftwdb-shadow.service
systemctl start ftwdb-shadow.service
systemctl stop ftwdb-shadow.service
```

Confirm that start creates a `0700` store directory, a `0700` runtime
directory, and a `0600` socket. Confirm that stop lets the process exit before
the 30-second limit. The Linux unit still needs a test on each target box. Old
systemd or kernel versions may not support every hardening setting.

## launchd on macOS

Change the values in `launchd/com.sourceful.ftwdb-shadow.plist`. Create and own
the paths before loading the job because launchd does not create them:

```sh
sudo install -d -o ftw -g ftw -m 0700 /var/db/ftwdb-shadow
sudo install -d -o ftw -g ftw -m 0700 /var/run/ftwdb-shadow
plutil -lint packaging/launchd/com.sourceful.ftwdb-shadow.plist
```

Install the plist under `/Library/LaunchDaemons` as a root-owned file with mode
`0644`, then use `launchctl bootstrap system` to load it. launchd sends
`SIGTERM` on stop and gives the process 30 seconds to finish. The job restarts
after a failed exit but stays down after a clean stop.

Run the local regression check with:

```sh
cargo test --test service_examples
```
