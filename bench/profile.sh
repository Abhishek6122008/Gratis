#!/usr/bin/env bash
# Profiles Gratis at a git ref under one memtier load with JDK Flight Recorder: CPU-time samples (JDK 25's
# jdk.CPUTimeSample), execution samples, allocation samples and GC pauses, in one recording.
#
# Usage: bench/profile.sh REF [PIPELINE] [CONNECTIONS]     (defaults: pipeline 16, 64 connections)
#
# The server runs from the eclipse-temurin:25-jdk image (the same Temurin 25.0.5+8 build as the JRE image run.sh
# uses) so that jcmd can start the recording after the fill and warm-up. Same CPUs, memory and heap as run.sh.
# Output, in $OUT (default bench/results/profile-<sha>-p<P>-c<C>): profile.jfr, memtier.json and gc.log, then
# flame graphs and text summaries made by bench/flamegraph.py and the JDK's jfr tool.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
ref=${1:?usage: profile.sh REF [PIPELINE] [CONNECTIONS]}
pipeline=${2:-16}
connections=${3:-64}
PROFILE_TIME=${PROFILE_TIME:-30}
WARMUP_TIME=${WARMUP_TIME:-10}
KEYS=${KEYS:-100000}
DATA_SIZE=${DATA_SIZE:-100}
RATIO=${RATIO:-1:10}
SERVER_CPUS=${SERVER_CPUS:-2,3,6,7}
CLIENT_CPUS=${CLIENT_CPUS:-0,1,4,5}
SERVER_MEMORY=${SERVER_MEMORY:-4g}
JDK_IMAGE=${JDK_IMAGE:-eclipse-temurin:25-jdk@sha256:83f4a7fc27df94dcc7b8de04d14cbbba8da14406f921e966969c3b09000aa507}
REDIS_IMAGE=${REDIS_IMAGE:-redis:8-alpine@sha256:7958cfbb9d152f87dadfbc89f1b0b15938f0b81ff8982feb327476f4911935c3}
MEMTIER_IMAGE=${MEMTIER_IMAGE:-redislabs/memtier_benchmark@sha256:41849040f6ea0b335c31ae532b1488ce47ea6e8c48a086d661f3d6953d3b599d}
JFR=${JFR:-$HOME/.jdks/zulu-25/bin/jfr}
PYTHON=${PYTHON:-python3}
NETWORK=gratis-bench
SERVER=gratis-bench-target

image=$("$here/build-image.sh" "$ref")
sha=${image#gratis:}
OUT=${OUT:-$here/results/profile-$sha-p$pipeline-c$connections}
mkdir -p "$OUT/classes"
chmod 777 "$OUT"
docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK" >/dev/null
trap 'docker rm -f "$SERVER" >/dev/null 2>&1 || true' EXIT

# The classes from the image run.sh measures, so both run the same bytes.
container=$(docker create "$image")
docker cp "$container:/app/." "$OUT/classes" >/dev/null
docker rm "$container" >/dev/null

memtier() {
  docker run --rm --network "$NETWORK" --cpuset-cpus "$CLIENT_CPUS" --user "$(id -u):$(id -g)" \
    -v "$OUT:/out" -w /out "$MEMTIER_IMAGE" -s "$SERVER" -p 6379 --protocol=redis \
    --key-minimum=1 --key-maximum="$KEYS" --data-size="$DATA_SIZE" --hide-histogram "$@"
}

docker rm -f "$SERVER" >/dev/null 2>&1 || true
docker run -d --name "$SERVER" --network "$NETWORK" --cpuset-cpus "$SERVER_CPUS" --memory "$SERVER_MEMORY" \
  --user "$(id -u):$(id -g)" -v "$OUT/classes:/app:ro" -v "$OUT:/out" "$JDK_IMAGE" \
  java -Xms1g -Xmx1g -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -Xlog:gc:file=/out/gc.log \
  -cp /app Main --port 6379 >/dev/null
for _ in $(seq 100); do
  docker run --rm --network "$NETWORK" "$REDIS_IMAGE" redis-cli -h "$SERVER" ping 2>/dev/null | grep -q PONG && break
  sleep 0.2
done

threads=$(( connections < 4 ? connections : 4 ))
clients=$(( connections / threads ))
memtier --ratio=1:0 --key-pattern=P:P --requests=allkeys --threads=1 --clients=1 --pipeline=64 > "$OUT/fill.log" 2>&1
memtier --ratio="$RATIO" --key-pattern=R:R --threads=4 --clients=16 --pipeline=1 --test-time="$WARMUP_TIME" \
  > "$OUT/warmup.log" 2>&1

# Record only the measured load: start JFR, run memtier for PROFILE_TIME, stop and dump.
docker exec --user "$(id -u):$(id -g)" "$SERVER" jcmd 1 JFR.start name=bench settings=profile filename=/out/profile.jfr \
  jdk.CPUTimeSample#enabled=true jdk.CPUTimeSample#throttle=1ms jdk.ExecutionSample#period=1ms \
  > "$OUT/jfr-start.log" 2>&1
memtier --ratio="$RATIO" --key-pattern=R:R --threads="$threads" --clients="$clients" --pipeline="$pipeline" \
  --test-time="$PROFILE_TIME" --distinct-client-seed --json-out-file=memtier.json > "$OUT/memtier.log" 2>&1
docker exec --user "$(id -u):$(id -g)" "$SERVER" jcmd 1 JFR.stop name=bench > "$OUT/jfr-stop.log" 2>&1
docker logs "$SERVER" > "$OUT/server.log" 2>&1
docker rm -f "$SERVER" >/dev/null

# Text views from the JDK's own tool, and flame graphs.
for view in cpu-time-hot-methods hot-methods allocation-by-site gc-pauses gc; do
  "$JFR" view --width 160 "$view" "$OUT/profile.jfr" > "$OUT/view-$view.txt" 2>&1 || true
done
for event in jdk.CPUTimeSample jdk.ObjectAllocationSample; do
  "$JFR" print --json --stack-depth 64 --events "$event" "$OUT/profile.jfr" > "$OUT/$event.json"
  "$PYTHON" -I "$here/flamegraph.py" "$OUT/$event.json" "$OUT/flame-${event#jdk.}.svg" \
    "Gratis $sha, pipeline $pipeline, $connections connections: $event"
  rm "$OUT/$event.json"
done
echo "$(python3 -c 'import json,sys; print(round(json.load(open(sys.argv[1]))["ALL STATS"]["Totals"]["Ops/sec"]))' "$OUT/memtier.json") ops/s while profiling"
echo "results: $OUT"
