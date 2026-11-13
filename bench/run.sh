#!/usr/bin/env bash
# Runs memtier_benchmark against each target, one target at a time, with the same CPU and memory limits for all.
#
# Usage: bench/run.sh TARGET...
#   TARGET is redis, valkey, or gratis@REF (any git ref; the image is built with build-image.sh).
#   Example: bench/run.sh gratis@v1-baseline gratis@v2 redis valkey
#
# Results go to $OUT (default bench/results/<UTC timestamp>), which git ignores: one memtier JSON file and HDR
# histograms per run, plus meta.txt with every version and setting. bench/report.py turns them into tables and charts.
#
# Settings (environment variables, defaults in brackets):
#   CONNECTIONS ["1 4 16 64 256"]  PIPELINES ["1 16"]  RUNS [5]  TEST_TIME [20 s]  WARMUP_TIME [10 s]
#   KEYS [100000]  DATA_SIZE [100 bytes]  RATIO [1:10, SET:GET]
#   SERVER_CPUS ["2,3,6,7", two physical cores]  CLIENT_CPUS ["0,1,4,5", the other two]  SERVER_MEMORY [4g]
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$here" rev-parse --show-toplevel)

CONNECTIONS=${CONNECTIONS:-"1 4 16 64 256"}
PIPELINES=${PIPELINES:-"1 16"}
RUNS=${RUNS:-5}
TEST_TIME=${TEST_TIME:-20}
WARMUP_TIME=${WARMUP_TIME:-10}
KEYS=${KEYS:-100000}
DATA_SIZE=${DATA_SIZE:-100}
RATIO=${RATIO:-1:10}
SERVER_CPUS=${SERVER_CPUS:-2,3,6,7}
CLIENT_CPUS=${CLIENT_CPUS:-0,1,4,5}
SERVER_MEMORY=${SERVER_MEMORY:-4g}
OUT=${OUT:-$here/results/$(date -u +%Y%m%dT%H%M%SZ)}

# Pinned by digest: the versions recorded on Day 2 (2026-11-13).
REDIS_IMAGE=${REDIS_IMAGE:-redis:8-alpine@sha256:7958cfbb9d152f87dadfbc89f1b0b15938f0b81ff8982feb327476f4911935c3}
VALKEY_IMAGE=${VALKEY_IMAGE:-valkey/valkey:alpine@sha256:bf09d58e96d6b4524a15396b65b242bbe18beebbcc15a04d8ee75ddac62fae0b}
MEMTIER_IMAGE=${MEMTIER_IMAGE:-redislabs/memtier_benchmark@sha256:41849040f6ea0b335c31ae532b1488ce47ea6e8c48a086d661f3d6953d3b599d}

NETWORK=gratis-bench
SERVER=gratis-bench-target

[ $# -gt 0 ] || { sed -n '2,15p' "$0"; exit 1; }
mkdir -p "$OUT"
docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK" >/dev/null
trap 'docker rm -f "$SERVER" >/dev/null 2>&1 || true' EXIT

memtier() {
  docker run --rm --network "$NETWORK" --cpuset-cpus "$CLIENT_CPUS" --user "$(id -u):$(id -g)" \
    -v "$OUT:/out" -w /out "$MEMTIER_IMAGE" -s "$SERVER" -p 6379 --protocol=redis \
    --key-minimum=1 --key-maximum="$KEYS" --data-size="$DATA_SIZE" --hide-histogram "$@"
}

start_server() { # Starts one target alone, with the shared limits, and waits until it answers PING.
  local target=$1
  local -a cmd
  case $target in
    redis) cmd=("$REDIS_IMAGE" redis-server --save "" --appendonly no --protected-mode no) ;;
    valkey) cmd=("$VALKEY_IMAGE" valkey-server --save "" --appendonly no --protected-mode no) ;;
    gratis@*) cmd=("$("$here/build-image.sh" "${target#gratis@}")") ;;
    *) echo "unknown target: $target" >&2; exit 1 ;;
  esac
  docker rm -f "$SERVER" >/dev/null 2>&1 || true
  docker run -d --name "$SERVER" --network "$NETWORK" --cpuset-cpus "$SERVER_CPUS" --memory "$SERVER_MEMORY" \
    "${cmd[@]}" >/dev/null
  echo "$target image: ${cmd[0]} $(docker image inspect "${cmd[0]}" --format '{{.Id}}')" >> "$OUT/meta.txt"
  for _ in $(seq 100); do
    if docker run --rm --network "$NETWORK" "$REDIS_IMAGE" redis-cli -h "$SERVER" ping 2>/dev/null | grep -q PONG; then
      return
    fi
    sleep 0.2
  done
  echo "$target did not answer PING" >&2
  docker logs "$SERVER" >&2
  exit 1
}

{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "host: $(lscpu | sed -n 's/^Model name: *//p'), $(nproc) threads, $(uname -r), governor $(cat /sys/devices/system/cpu/cpufreq/policy0/scaling_governor 2>/dev/null || echo unknown)"
  echo "docker: $(docker version --format '{{.Server.Version}}')"
  echo "memtier: $(docker run --rm "$MEMTIER_IMAGE" --version 2>&1 | head -1)"
  echo "gratis repo: $(git -C "$repo" rev-parse --short HEAD) ($(git -C "$repo" status --porcelain | wc -l) uncommitted files)"
  echo "settings: CONNECTIONS='$CONNECTIONS' PIPELINES='$PIPELINES' RUNS=$RUNS TEST_TIME=$TEST_TIME WARMUP_TIME=$WARMUP_TIME"
  echo "          KEYS=$KEYS DATA_SIZE=$DATA_SIZE RATIO=$RATIO key-pattern=R:R"
  echo "          SERVER_CPUS=$SERVER_CPUS CLIENT_CPUS=$CLIENT_CPUS SERVER_MEMORY=$SERVER_MEMORY"
} > "$OUT/meta.txt"

alive() {
  docker run --rm --network "$NETWORK" "$REDIS_IMAGE" redis-cli -h "$SERVER" ping 2>/dev/null | grep -q PONG
}

# Runs one memtier step. If memtier fails or the server stops answering, records why and returns 1, so the script
# moves on to the next target instead of measuring a dead server.
step() { # log file (under $OUT), then memtier options
  local log=$1
  shift
  if memtier "$@" > "$OUT/$log" 2>&1 && alive; then
    return 0
  fi
  {
    echo "  FAILED at $log: memtier exit or server gone. Last server log lines:"
    docker logs "$SERVER" 2>&1 | tail -5 | sed 's/^/    /'
  } | tee -a "$OUT/meta.txt"
  return 1
}

bench_target() {
  local target=$1 name=${1//@/-}
  mkdir -p "$OUT/$name"
  echo "== $target"
  start_server "$target"
  # Fill every key once, so GETs hit, then run the JIT (and every server's caches) warm before measuring.
  step "$name/fill.log" --ratio=1:0 --key-pattern=P:P --requests=allkeys --threads=1 --clients=1 --pipeline=64 \
    || return 1
  step "$name/warmup.log" --ratio="$RATIO" --key-pattern=R:R --threads=4 --clients=16 --pipeline=1 \
    --test-time="$WARMUP_TIME" || return 1
  for pipeline in $PIPELINES; do
    for connections in $CONNECTIONS; do
      local threads=$(( connections < 4 ? connections : 4 ))
      local clients=$(( connections / threads ))
      for run in $(seq "$RUNS"); do
        local file="$name/p$pipeline-c$connections-r$run"
        step "$file.log" --ratio="$RATIO" --key-pattern=R:R --threads="$threads" --clients="$clients" \
          --pipeline="$pipeline" --test-time="$TEST_TIME" --distinct-client-seed \
          --json-out-file="$file.json" --hdr-file-prefix="$file" || return 1
        printf '  pipeline %-2s connections %-3s run %s: %s ops/s\n' "$pipeline" "$connections" "$run" \
          "$(python3 -c 'import json,sys; print(round(json.load(open(sys.argv[1]))["ALL STATS"]["Totals"]["Ops/sec"]))' "$OUT/$file.json")"
      done
    done
  done
}

for target in "$@"; do
  bench_target "$target" || true
  docker logs "$SERVER" > "$OUT/${target//@/-}/server.log" 2>&1 || true
  docker rm -f "$SERVER" >/dev/null 2>&1 || true
done
echo "results: $OUT"
