#!/usr/bin/env python3
"""Draws a flame graph SVG from `jfr print --json` output, and prints the hottest frames.

Usage: flamegraph.py EVENTS.json OUT.svg TITLE

Works for any JFR event with a stack trace. Allocation samples are weighted by their `weight` (bytes), everything
else counts one per sample. Hover a box for its frame, sample share and count. The text summary (top 15 frames by
self time, then by total time) goes to stdout and next to the SVG as OUT.txt.
"""
import html
import json
import sys
from collections import defaultdict

WIDTH = 1400
ROW = 17
MIN_WIDTH = 0.6  # pixels; narrower boxes are skipped


def frame_name(frame):
    method = frame["method"]
    cls = method["type"]["name"].replace("/", ".").split(".")[-1]
    return f"{cls}.{method['name']}"


def load(path):
    events = json.load(open(path))["recording"]["events"]
    stacks = []
    for event in events:
        trace = event["values"].get("stackTrace")
        if not trace:
            continue
        frames = [frame_name(f) for f in reversed(trace["frames"])]  # root first
        weight = event["values"].get("weight", 1) or 1
        stacks.append((frames, weight))
    return stacks


def tree(stacks):
    root = {"name": "all", "value": 0, "children": {}}
    for frames, weight in stacks:
        node = root
        node["value"] += weight
        for name in frames:
            node = node["children"].setdefault(name, {"name": name, "value": 0, "children": {}})
            node["value"] += weight
    return root


def color(name):
    h = sum(ord(c) for c in name) % 40
    if name.startswith(("Main.", "Main$")):
        return f"rgb(230,{120 + h},40)"
    if any(name.startswith(p) for p in ("SocketChannelImpl", "IOUtil", "SocketDispatcher", "EPoll", "SelectorImpl")):
        return f"rgb(80,{150 + h},200)"
    return f"rgb(240,{180 + h},{60 + h})"


def svg(root, title):
    total = root["value"]
    rows = []

    def walk(node, x, depth):
        w = node["value"] / total * WIDTH
        if w < MIN_WIDTH:
            return
        rows.append((x, depth, w, node))
        cx = x
        for child in sorted(node["children"].values(), key=lambda n: n["name"]):
            walk(child, cx, depth + 1)
            cx += child["value"] / total * WIDTH

    walk(root, 0, 0)
    depth = max(d for _, d, _, _ in rows) + 1
    height = (depth + 2) * ROW + 10
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{height}" font-family="monospace" '
           f'font-size="11"><rect width="100%" height="100%" fill="#fdfdfd"/>',
           f'<text x="6" y="14" font-size="13">{html.escape(title)} ({total:,} samples/weight)</text>']
    for x, d, w, node in rows:
        y = height - (d + 1) * ROW
        share = node["value"] / total * 100
        label = node["name"]
        tip = f"{label}: {share:.1f}% ({node['value']:,})"
        out.append(f'<g><title>{html.escape(tip)}</title><rect x="{x:.1f}" y="{y}" width="{w:.1f}" '
                   f'height="{ROW - 1}" fill="{color(label)}" rx="1"/>')
        chars = int(w / 6.6)
        if chars >= 3:
            text = label if len(label) <= chars else label[:chars - 2] + ".."
            out.append(f'<text x="{x + 3:.1f}" y="{y + ROW - 5}">{html.escape(text)}</text>')
        out.append("</g>")
    out.append("</svg>")
    return "\n".join(out)


def summary(stacks):
    self_w, total_w = defaultdict(int), defaultdict(int)
    grand = sum(w for _, w in stacks) or 1
    for frames, weight in stacks:
        if frames:
            self_w[frames[-1]] += weight
        for name in set(frames):
            total_w[name] += weight
    lines = [f"{len(stacks):,} samples, total weight {grand:,}", "", "Top 15 by self:"]
    for name, w in sorted(self_w.items(), key=lambda kv: -kv[1])[:15]:
        lines.append(f"  {w / grand * 100:5.1f}%  {name}")
    lines += ["", "Top 15 by total (inclusive):"]
    for name, w in sorted(total_w.items(), key=lambda kv: -kv[1])[:15]:
        lines.append(f"  {w / grand * 100:5.1f}%  {name}")
    return "\n".join(lines)


def main():
    src, dst, title = sys.argv[1], sys.argv[2], sys.argv[3]
    stacks = load(src)
    if not stacks:
        print(f"{src}: no samples")
        return
    with open(dst, "w") as f:
        f.write(svg(tree(stacks), title))
    text = summary(stacks)
    with open(dst.rsplit(".", 1)[0] + ".txt", "w") as f:
        f.write(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
