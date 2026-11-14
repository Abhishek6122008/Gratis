#!/usr/bin/env bash
# Runs memtier_benchmark against each target with the same CPU and memory limits for all. All targets start first
# (fill + warm-up each), then the runs are interleaved: for every cell (pipeline, connections) and every run number,
# each target gets one run in turn. Only one target is under load at any moment; the others sit idle on the same CPUs.
# Interleaving means a slow stretch on the machine (another process, the CPU clocking down) hits every target alike,
# instead of whichever target happened to be measured during it (Day 4 found exactly that).
#
# Usage: bench/run.sh TARGET...
#   TARGET is redis, valkey, or gratis@REF[,OPTION...] (any git ref; the image is built with build-image.sh).
#   OPTIONs: shards=N (passes --shards N), zgc (generational ZGC instead of the default G1).
#   Example: bench/run.sh gratis@v1-baseline gratis@v2,shards=1 gratis@v2,shards=4,zgc redis valkey
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
PREFIX=gratis-bench-

[ $# -gt 0 ] || { sed -n '2,15p' "$0"; exit 1; }
mkdir -p "$OUT"
docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK" >/dev/null
cleanup() { docker ps -aq --filter "name=^$PREFIX" | xargs -r docker rm -f >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

memtier() { # server, then memtier options
  local server=$1
  shift
  docker run --rm --network "$NETWORK" --cpuset-cpus "$CLIENT_CPUS" --user "$(id -u):$(id -g)" \
    -v "$OUT:/out" -w /out "$MEMTIER_IMAGE" -s "$server" -p 6379 --protocol=redis \
    --key-minimum=1 --key-maximum="$KEYS" --data-size="$DATA_SIZE" --hide-histogram "$@"
}

# A target's container and directory name: gratis@v2,shards=4 -> gratis-v2-shards-4.
name_of() {
  local name=${1//@/-}
  name=${name//,/-}
  echo "${name//=/-}"
}

alive() { # server
  docker run --rm --network "$NETWORK" "$REDIS_IMAGE" redis-cli -h "$1" ping 2>/dev/null | grep -q PONG
}

start_server() { # target: starts it with the shared limits and waits until it answers PING
  local target=$1 server=$PREFIX$(name_of "$1")
  local -a cmd env=()
  case $target in
    redis) cmd=("$REDIS_IMAGE" redis-server --save "" --appendonly no --protected-mode no) ;;
    valkey) cmd=("$VALKEY_IMAGE" valkey-server --save "" --appendonly no --protected-mode no) ;;
    gratis@*)
      local spec=${target#gratis@} java_opts="-Xms1g -Xmx1g" args="" option
      local -a options
      IFS=, read -ra options <<< "$spec"
      for option in "${options[@]:1}"; do
        case $option in
          shards=*) args="$args --shards ${option#shards=}" ;;
          zgc) java_opts="$java_opts -XX:+UseZGC" ;;
          *) echo "unknown option in $target: $option" >&2; exit 1 ;;
        esac
      done
      cmd=("$("$here/build-image.sh" "${options[0]}")")
      env=(-e "JAVA_OPTS=$java_opts" -e "GRATIS_ARGS=$args")
      ;;
    *) echo "unknown target: $target" >&2; exit 1 ;;
  esac
  docker run -d --name "$server" --network "$NETWORK" --cpuset-cpus "$SERVER_CPUS" --memory "$SERVER_MEMORY" \
    "${env[@]}" "${cmd[@]}" >/dev/null
  echo "$target image: ${cmd[0]} $(docker image inspect "${cmd[0]}" --format '{{.Id}}') ${env[*]}" >> "$OUT/meta.txt"
  for _ in $(seq 100); do
    alive "$server" && return
    sleep 0.2
  done
  echo "$target did not answer PING" >&2
  docker logs "$server" >&2
  exit 1
}

# Runs one memtier step against a target. If memtier fails or the server stops answering, records why and returns 1,
# so that target is dropped instead of measured dead.
step() { # target, log file (under $OUT), then memtier options
  local target=$1 log=$2 server=$PREFIX$(name_of "$1")
  shift 2
  if memtier "$server" "$@" > "$OUT/$log" 2>&1 && alive "$server"; then
    return 0
  fi
  {
    echo "  $target FAILED at $log: memtier exit or server gone. Last server log lines:"
    docker logs "$server" 2>&1 | tail -5 | sed 's/^/    /'
  } | tee -a "$OUT/meta.txt"
  return 1
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

live=()
for target in "$@"; do
  name=$(name_of "$target")
  mkdir -p "$OUT/$name"
  echo "== start $target"
  start_server "$target"
  # Fill every key once, so GETs hit, then run the JIT (and every server's caches) warm before measuring.
  if step "$target" "$name/fill.log" --ratio=1:0 --key-pattern=P:P --requests=allkeys --threads=1 --clients=1 \
      --pipeline=64 \
    && step "$target" "$name/warmup.log" --ratio="$RATIO" --key-pattern=R:R --threads=4 --clients=16 --pipeline=1 \
      --test-time="$WARMUP_TIME"; then
    live+=("$target")
  fi
done

for pipeline in $PIPELINES; do
  for connections in $CONNECTIONS; do
    threads=$(( connections < 4 ? connections : 4 ))
    clients=$(( connections / threads ))
    for run in $(seq "$RUNS"); do
      still=()
      for target in "${live[@]}"; do
        name=$(name_of "$target")
        file="$name/p$pipeline-c$connections-r$run"
        if step "$target" "$file.log" --ratio="$RATIO" --key-pattern=R:R --threads="$threads" --clients="$clients" \
            --pipeline="$pipeline" --test-time="$TEST_TIME" --distinct-client-seed \
            --json-out-file="$file.json" --hdr-file-prefix="$file"; then
          still+=("$target")
          printf '  %-28s pipeline %-2s connections %-3s run %s: %s ops/s\n' "$target" "$pipeline" "$connections" \
            "$run" "$(python3 -c 'import json,sys; print(round(json.load(open(sys.argv[1]))["ALL STATS"]["Totals"]["Ops/sec"]))' "$OUT/$file.json")"
        fi
      done
      live=("${still[@]}")
    done
  done
done

for target in "$@"; do
  docker logs "$PREFIX$(name_of "$target")" > "$OUT/$(name_of "$target")/server.log" 2>&1 || true
done
echo "results: $OUT"
