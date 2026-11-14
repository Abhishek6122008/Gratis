# Benchmarks

Gratis at any git ref against Redis and Valkey, with memtier_benchmark, one server at a time.

```sh
bench/run.sh gratis@39f0113 gratis@v2 redis valkey      # about 20 minutes per target with the defaults
.venv/bin/python bench/report.py bench/results/<run>    # report.md + charts in that directory (needs matplotlib)
```

- **Targets.** `gratis@REF` builds an image from that ref's sources with `build-image.sh` (a git worktree, JDK 25,
  `-Xms1g -Xmx1g`). `redis` and `valkey` are pinned by digest in `run.sh`, with persistence off (`--save ""
  --appendonly no`), because Gratis has none.
- **Same limits for everyone.** The server gets CPUs `2,3,6,7` (two physical cores on the i5-10300H) and 4 GB.
  memtier gets `0,1,4,5`. Both are containers on one Docker bridge network, so the network path is the same too.
- **Workload.** 100,000 keys, 100-byte values, SET:GET 1:10, uniform random keys. Before measuring, every key is
  written once (so GETs hit) and a 10-second warm-up runs (so the JIT has compiled the hot paths).
- **Matrix.** 1, 4, 16, 64 and 256 connections (up to 4 memtier threads), pipeline 1 and 16, 5 runs of 20 seconds
  each, interleaved: every target is started first, then each run number goes round all targets in turn, so a
  slow stretch on the laptop hits all of them alike. The report gives the median and the min–max spread of the 5 runs, plus p50, p99 and p99.9 latency.
- **Output.** `bench/results/<UTC time>/`, which git ignores: memtier JSON and HDR histograms per run, logs,
  `meta.txt` (every version, digest and setting), and after `report.py`, `report.md` and PNG charts.
- memtier only sends `SET` and `GET`, and the readiness check sends `PING`, so Gratis needed no new commands.

## Why there are no numbers for `v1-baseline` itself

memtier closes its connections with a TCP reset. Gratis at `v1-baseline` dies on the first reset
(`IOException: Connection reset by peer` escapes the selector loop), so it stops at the end of the fill step, before
the first measured run. `run.sh` reports that and moves on.

The v1 numbers therefore come from `39f0113`: `v1-baseline` plus the Maven wrapper, tests, docs and two crash fixes
(`cef3940` bad commands, `ff157df` client resets). Neither fix touches the per-command hot path: one adds a `try`
around each command, the other moves the read into its own method. Every later speed-up is measured against
`39f0113`.

## Caveats

- This is a laptop: the CPU governor is `powersave` with turbo on, and nothing else is stopped during a run. The
  5-run spread is there to show how much that matters.
- Client and server share one machine, so the client's CPU use can limit the result at high connection counts.

## Profiling

```sh
PYTHON=.venv/bin/python bench/profile.sh 39f0113 16 64    # ref, pipeline depth, connections
```

Runs one target under one memtier load (fill and warm-up first, then 30 s recorded) and records it with JDK Flight
Recorder. Recording uses JDK 25's CPU-time sampler (`jdk.CPUTimeSample`, 1 ms) plus allocation samples and GC
pauses. Output in `bench/results/profile-<sha>-p<P>-c<C>/`: `profile.jfr`, CPU and allocation flame graphs
(`flame-*.svg`, drawn by `flamegraph.py`, with their top frames in `flame-*.txt`), and the JDK's own `jfr view`
summaries (`view-*.txt`).

- The recording costs a lot of throughput (218k against 513k ops/s at pipeline 16 and 64 connections, Day 3), so a
  profile shows *where* the time goes, not how fast the code is. The numbers come from `run.sh`.
- Allocation samples are weighted by the bytes allocated since the previous sample, so a few large allocations right
  after the recording starts (each connection's 64 KB buffer) come out far heavier than they are.
- async-profiler would also show native and kernel frames. JFR stops at the Java frame that makes the system call
  (`SocketDispatcher.write0`), which is enough to see the cost of each call.
