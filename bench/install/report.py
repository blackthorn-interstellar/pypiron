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

    runs, caps = [], []
    for f in sorted(RESULTS.glob("*.json")):
        d = json.loads(f.read_text())
        if "index" in d and "ramp" in d.get("index", {}):
            caps.append(d)  # capacity (cap-*.json)
        elif "sweeps" in d:
            if args.scenario and d.get("meta", {}).get("scenario") != args.scenario:
                continue
            if args.tier and d.get("tier") != args.tier:
                continue
            if args.arch and d.get("arch") != args.arch:
                continue
            runs.append(d)

    if runs:
        sample = runs[0]
        print(
            f"### Install benchmark — {sample.get('tier')} / {sample.get('arch')} / "
            f"uv {sample.get('uv_version', '?').replace('uv ', '')} / "
            f"sampling={sample.get('sampling')} / {sample.get('projects')} projects\n"
        )
        print(
            "| Server | Track | C | installs/min | p50 ms | p95 ms | p99 ms | err | resolve p50 ms |"
        )
        print("|---|---|---|---|---|---|---|---|---|")
        for d in sorted(
            runs, key=lambda d: (d.get("label", ""), d.get("meta", {}).get("track", 0))
        ):
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

    if caps:
        print("\n### Breaking point — index-read MST (oha ramp)\n")
        print(
            "| Server | MST rps | c_knee | p99@knee ms | breach | bytes/req | R_ceiling | headroom | bound |"
        )
        print("|---|---|---|---|---|---|---|---|---|")
        for d in sorted(caps, key=lambda d: -d["index"].get("mst_rps", 0)):
            i = d["index"]
            ctl = d.get("control", {})
            print(
                f"| {d.get('label', '?')} | {i.get('mst_rps', '—')} | {i.get('c_knee', '—')} "
                f"| {i.get('p99_at_knee_ms', '—')} | {i.get('breach_mode', '—')} "
                f"| {i.get('bytes_per_request', '—')} | {ctl.get('r_ceiling_rps', '—')} "
                f"| {ctl.get('headroom', '—')} | {d.get('bound_class', '—')} |"
            )

    if not runs and not caps:
        raise SystemExit("no matching results in " + str(RESULTS))


if __name__ == "__main__":
    main()
