#!/usr/bin/env python3
"""Turns one bench/run.sh results directory into report.md and charts.

Usage: bench/report.py RESULTS_DIR   (needs matplotlib)

For each target, pipeline depth and connection count it takes the RUNS memtier JSON files and reports the median and
the spread (min to max) of ops/s, and the medians of p50, p99 and p99.9 latency. Charts: ops/s and p99 against
connections, one line per target, one chart per pipeline depth.
"""
import json
import re
import statistics
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RUN_FILE = re.compile(r"p(\d+)-c(\d+)-r(\d+)\.json$")


def load(results):
    """{target: {(pipeline, connections): [run dicts]}}, targets in the order they ran."""
    data = {}
    for target_dir in sorted((d for d in results.iterdir() if d.is_dir()), key=lambda d: d.stat().st_mtime):
        runs = {}
        for f in target_dir.glob("*.json"):
            m = RUN_FILE.search(f.name)
            if not m:
                continue
            totals = json.loads(f.read_text())["ALL STATS"]["Totals"]
            p = totals["Percentile Latencies"]
            runs.setdefault((int(m[1]), int(m[2])), []).append(
                {"ops": totals["Ops/sec"], "p50": p["p50.00"], "p99": p["p99.00"], "p999": p["p99.90"],
                 "misses": totals["Misses/sec"]})
        if runs:
            data[target_dir.name] = runs
    return data


def summary(runs):
    ops = [r["ops"] for r in runs]
    return {"n": len(runs), "ops": statistics.median(ops), "min": min(ops), "max": max(ops),
            "p50": statistics.median(r["p50"] for r in runs), "p99": statistics.median(r["p99"] for r in runs),
            "p999": statistics.median(r["p999"] for r in runs), "misses": max(r["misses"] for r in runs)}


def fmt_ops(s):
    return f"{s['ops']:,.0f} ({s['min']:,.0f}–{s['max']:,.0f})"


def chart(data, pipeline, key, ylabel, path):
    fig, ax = plt.subplots(figsize=(7, 4.2))
    for target, runs in data.items():
        points = sorted((c, summary(r)) for (p, c), r in runs.items() if p == pipeline)
        if not points:
            continue
        xs = [c for c, _ in points]
        ys = [s[key] for _, s in points]
        if key == "ops":
            err = [[s["ops"] - s["min"] for _, s in points], [s["max"] - s["ops"] for _, s in points]]
            ax.errorbar(xs, ys, yerr=err, marker="o", capsize=3, label=target)
        else:
            ax.plot(xs, ys, marker="o", label=target)
    ax.set_xscale("log", base=2)
    ticks = sorted({c for runs in data.values() for (p, c) in runs if p == pipeline})
    ax.set_xticks(ticks, [str(c) for c in ticks])
    ax.set_xlabel("connections")
    ax.set_ylabel(ylabel)
    ax.set_title(f"{ylabel}, pipeline {pipeline}")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def main():
    results = Path(sys.argv[1])
    data = load(results)
    meta = (results / "meta.txt").read_text() if (results / "meta.txt").exists() else ""
    pipelines = sorted({p for runs in data.values() for p, _ in runs})
    targets = list(data)
    out = ["# Benchmark report", "", "```", meta.rstrip(), "```", ""]
    out += ["Each cell: median of the runs, with min–max in brackets. Latency is memtier's, in ms, median over runs.",
            ""]
    for pipeline in pipelines:
        conns = sorted({c for runs in data.values() for p, c in runs if p == pipeline})
        out += [f"## Pipeline {pipeline}", "", "### ops/s", ""]
        out += ["| connections | " + " | ".join(targets) + " |", "|---" * (len(targets) + 1) + "|"]
        for c in conns:
            cells = [fmt_ops(summary(data[t][(pipeline, c)])) if (pipeline, c) in data[t] else "—" for t in targets]
            out.append(f"| {c} | " + " | ".join(cells) + " |")
        out += ["", "### Latency p50 / p99 / p99.9 (ms)", ""]
        out += ["| connections | " + " | ".join(targets) + " |", "|---" * (len(targets) + 1) + "|"]
        for c in conns:
            cells = []
            for t in targets:
                if (pipeline, c) in data[t]:
                    s = summary(data[t][(pipeline, c)])
                    cells.append(f"{s['p50']:.3f} / {s['p99']:.3f} / {s['p999']:.3f}")
                else:
                    cells.append("—")
            out.append(f"| {c} | " + " | ".join(cells) + " |")
        runs_per_cell = {summary(r)["n"] for runs in data.values() for (p, _), r in runs.items() if p == pipeline}
        misses = max(summary(r)["misses"] for runs in data.values() for (p, _), r in runs.items() if p == pipeline)
        out += ["", f"Runs per cell: {sorted(runs_per_cell)}. Highest GET misses/s in any run: {misses:,.0f}.", ""]
        for key, label in (("ops", "ops/s"), ("p99", "p99 latency (ms)")):
            name = f"{'ops' if key == 'ops' else 'p99'}-pipeline{pipeline}.png"
            chart(data, pipeline, key, label, results / name)
            out.append(f"![{label}, pipeline {pipeline}]({name})")
        out.append("")
    (results / "report.md").write_text("\n".join(out))
    print(results / "report.md")


if __name__ == "__main__":
    main()
