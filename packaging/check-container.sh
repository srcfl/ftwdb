#!/usr/bin/env bash
set -euo pipefail
image=${1:?usage: check-container.sh IMAGE}
container=$(docker create --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges --memory 512m --cpus 0.5 --pids-limit 32 \
  --tmpfs /var/lib/ftwdb-shadow:rw,noexec,nosuid,size=16m,uid=100,gid=101,mode=0700 \
  --tmpfs /run/ftwdb-shadow:rw,noexec,nosuid,size=1m,uid=100,gid=101,mode=0700 \
  --env FTWDB_SHADOW_MIN_FREE_BYTES=1048576 "$image")
trap 'docker rm -f "$container" >/dev/null' EXIT
docker start "$container" >/dev/null
for _ in {1..100}; do
  if docker exec "$container" test -S /run/ftwdb-shadow/shadow.sock; then
    break
  fi
  sleep 0.1
done
docker exec "$container" sh -ec '
  test "$(id -u)" = 100
  test "$(id -g)" = 101
  test "$(stat -c %a /var/lib/ftwdb-shadow)" = 700
  test "$(stat -c %a /run/ftwdb-shadow)" = 700
  test "$(stat -c %a /run/ftwdb-shadow/shadow.sock)" = 600
  ftw --version
  ftwdb-shadow --version
  ftwdb-shadow-reconcile --version
'
docker stop --time 10 "$container" >/dev/null
test "$(docker inspect --format '{{.State.ExitCode}}' "$container")" = 0
docker logs "$container" 2>&1 | grep -F 'ftwdb-shadow: stopped'
