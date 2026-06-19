#!/usr/bin/env python3
"""Render a side-by-side markdown comparison from results/*.json.

report.py                         # all results
report.py --scenario S1 --tier lite --arch aarch64
"""

from __future__ import annotations

import argparse
import json

from benchlib import RESULTS


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--scenario", default=None)
    ap.add_argument("--tier", default=None)
    ap.add_argument("--arch", default=None)
    args = ap.parse_args()

    files = sorted(RESULTS.glob("*.json"))
    runs = []
    for f in files:
        d = json.loads(f.read_text())
        m = d.get("meta", {})
        if args.scenario and m.get("scenario") != args.scenario:
            continue
        if args.tier and d.get("tier") != args.tier:
            continue
        if args.arch and d.get("arch") != args.arch:
            continue
        runs.append(d)
    if not runs:
        raise SystemExit("no matching results in " + str(RESULTS))

    sample = runs[0]
    print(
        f"### Install benchmark — {sample.get('tier')} / {sample.get('arch')} / "
        f"uv {sample.get('uv_version', '?').replace('uv ', '')} / "
        f"sampling={sample.get('sampling')} / {sample.get('projects')} projects\n"
    )
    print("| Server | Track | C | installs/min | p50 ms | p95 ms | p99 ms | err | resolve p50 ms |")
    print("|---|---|---|---|---|---|---|---|---|")
    for d in sorted(runs, key=lambda d: (d.get("label", ""), d.get("meta", {}).get("track", 0))):
        track = d.get("meta", {}).get("track", "?")
        resolve = d.get("resolve_only", {}).get("p50_ms", "—")
        first = True
        for c, s in sorted(d.get("sweeps", {}).items(), key=lambda kv: int(kv[0])):
            label = d.get("label", "?") if first else ""
            tr = track if first else ""
            rs = resolve if first else ""
            print(
                f"| {label} | {tr} | {s['concurrency']} | {s.get('installs_per_min', '—')} "
                f"| {s.get('p50_ms', '—')} | {s.get('p95_ms', '—')} | {s.get('p99_ms', '—')} "
                f"| {s.get('errors', 0)} | {rs} |"
            )
            first = False


if __name__ == "__main__":
    main()
