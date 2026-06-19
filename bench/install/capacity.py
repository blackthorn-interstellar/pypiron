#!/usr/bin/env python3
"""Breaking-point / capacity metric for the install benchmark.

The install ramp (drive.py) spawns one uv PROCESS per concurrent install, so on a
finite loadgen it saturates before a fast server does — it can't locate a fast
server's breaking point. This module drives the lightweight `oha` HTTP load
tester (its own runner — the frozen meter.run_oha uses an older oha CLI) to ramp
connections against each server's hot serving path until it breaks, and reports:

  MST (Max Sustained Throughput) — the highest rps at which a steady window still
  meets success >= 99.5% AND p99 <= max(floor, mult x unloaded_p99). Plus c_knee,
  p99 at the knee, the breach mode (latency | errors | collapse), bytes/req, and —
  when a static control target is given — loadgen_headroom = R_ceiling / MST and a
  bound_class (server-bound | caution | rig-bound) so a server too fast for this
  rig to break is flagged honestly rather than reported as a flat ceiling.

Runs inside the loadgen container (needs the oha binary; bench.py provisions it).

  capacity.py --index-url http://pypiron:8080/simple/ --host pypiron:8080 \
              --rep-pkg flask --oha /repo/bench/install/.bin/oha \
              --control-url http://control/control-index.json --output results/cap.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional

LADDER = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]


def oha_run(
    oha: str,
    url: str,
    duration: str,
    connections: int,
    headers: Optional[List[str]] = None,
    expect_status: int = 200,
) -> Dict:
    """Run oha and return rps + latency percentiles + success%. Uses oha's current
    `--json` CLI (the frozen meter.run_oha passes the old `--output-format json`),
    but the JSON shape is identical. `-r 0`: never follow redirects (a 302 measures
    the 302)."""
    cmd = [oha, "--no-tui", "--json", "-r", "0", "-z", duration, "-c", str(connections)]
    for h in headers or []:
        cmd += ["-H", h]
    cmd.append(url)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(f"oha failed: {proc.stderr[:300]}")
    data = json.loads(proc.stdout)
    summary = data.get("summary", {})
    pct = data.get("latencyPercentiles", {})
    statuses = data.get("statusCodeDistribution", {})
    total = sum(statuses.values()) or 1
    ok = statuses.get(str(expect_status), 0)

    def ms(key: str) -> float:
        v = pct.get(key)
        return round(v * 1000, 2) if isinstance(v, (int, float)) else 0.0

    time.sleep(1.0)  # settle between steps (mirrors meter.run_oha)
    return {
        "rps": round(summary.get("requestsPerSec") or 0.0, 1),
        "p50_ms": ms("p50"),
        "p95_ms": ms("p95"),
        "p99_ms": ms("p99"),
        "status_ok_pct": round(100.0 * ok / total, 2),
        "size_per_request": summary.get("sizePerRequest"),
        "total_data_bytes": summary.get("totalData"),
    }


def passes(step: Dict, ceiling_ms: float) -> bool:
    return step["status_ok_pct"] >= 99.5 and step["p99_ms"] <= ceiling_ms


def analyze_ramp(steps: List[Dict], ceiling_ms: float) -> Dict:
    """Pure: reduce a list of ramp steps to the breaking-point summary.

    Each step: {connections, rps, p50_ms, p95_ms, p99_ms, status_ok_pct}.

    MST = peak rps among SLO-passing steps (NOT merely the last — throughput can
    go retrograde while latency still passes). A break is the first of, scanning
    up by connections: an SLO failure (latency/errors) or a throughput collapse
    (rps < 85% of peak beyond the knee).
    """
    peak = max((s["rps"] for s in steps), default=0.0)
    good = [s for s in steps if passes(s, ceiling_ms)]
    best = max(good, key=lambda s: s["rps"]) if good else None
    mst = best["rps"] if best else 0.0
    c_knee = best["connections"] if best else 0
    p99_at_knee = best["p99_ms"] if best else None

    breach: Optional[str] = None
    for s in steps:
        if not passes(s, ceiling_ms):
            breach = "errors" if s["status_ok_pct"] < 99.5 else "latency"
            break
        if peak and s["rps"] < 0.85 * peak and s["connections"] > c_knee:
            breach = "collapse"
            break
    broke = breach is not None
    if not broke and steps and steps[-1]["connections"] == max(s["connections"] for s in steps):
        breach = "none(ladder-cap)"  # never broke within the ladder

    return {
        "mst_rps": round(mst, 1),
        "c_knee": c_knee,
        "p99_at_knee_ms": p99_at_knee,
        "peak_rps": round(peak, 1),
        "breach_mode": breach,
        "ceiling_ms": round(ceiling_ms, 1),
        "broke": broke,
    }


def ramp(
    oha: str,
    url: str,
    headers: List[str],
    expect: int,
    duration: str,
    ceiling_ms: float,
    ladder: List[int],
) -> List[Dict]:
    """Run the connection ladder, stopping once the server breaks (or collapses)."""
    steps: List[Dict] = []
    peak = 0.0
    collapse_streak = 0
    for c in ladder:
        r = oha_run(oha, url, duration, c, headers=headers, expect_status=expect)
        step = {
            "connections": c,
            "rps": r["rps"],
            "p50_ms": r["p50_ms"],
            "p95_ms": r["p95_ms"],
            "p99_ms": r["p99_ms"],
            "status_ok_pct": r["status_ok_pct"],
            "bytes_per_request": r.get("size_per_request"),
        }
        steps.append(step)
        peak = max(peak, r["rps"])
        if r["rps"] < 0.85 * peak:
            collapse_streak += 1
        else:
            collapse_streak = 0
        # Stop climbing once it has clearly broken: SLO violation, hard errors,
        # or two consecutive retrograde (thrashing) steps.
        if r["status_ok_pct"] < 95.0 or collapse_streak >= 2:
            break
        if not passes(step, ceiling_ms):
            break
    return steps


def measure(
    oha: str,
    url: str,
    headers: List[str],
    expect: int,
    duration: str,
    slo_floor_ms: float,
    slo_mult: float,
    ladder: List[int],
) -> Dict:
    base = oha_run(oha, url, "5s", 1, headers=headers, expect_status=expect)
    ceiling = max(slo_floor_ms, slo_mult * base["p99_ms"])
    steps = ramp(oha, url, headers, expect, duration, ceiling, ladder)
    out = analyze_ramp(steps, ceiling)
    out.update(
        {
            "url": url,
            "expect_status": expect,
            "unloaded_p50_ms": base["p50_ms"],
            "unloaded_p99_ms": base["p99_ms"],
            "bytes_per_request": steps[-1].get("bytes_per_request") if steps else None,
            "ramp": steps,
        }
    )
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--index-url", required=True, help="PEP 503 index root")
    ap.add_argument("--host", required=True)
    ap.add_argument(
        "--rep-pkg", default="flask", help="a package present on every server (small clean index)"
    )
    ap.add_argument("--oha", default="oha")
    ap.add_argument("--duration", default="10s")
    ap.add_argument("--slo-floor-ms", type=float, default=50.0)
    ap.add_argument("--slo-mult", type=float, default=10.0)
    ap.add_argument("--control-url", default=None, help="static control target for R_ceiling")
    ap.add_argument("--label", default="")
    ap.add_argument("--output", default=None)
    args = ap.parse_args()

    ladder = LADDER
    index_url = args.index_url.rstrip("/") + "/" + args.rep_pkg + "/"
    headers = ["Accept: application/vnd.pypi.simple.v1+json"]

    print(f"capacity {args.label}: index ramp {index_url}")
    index = measure(
        args.oha, index_url, headers, 200, args.duration, args.slo_floor_ms, args.slo_mult, ladder
    )

    result = {"label": args.label, "host": args.host, "rep_pkg": args.rep_pkg, "index": index}

    if args.control_url:
        print(f"capacity {args.label}: control ramp {args.control_url}")
        ctl = measure(
            args.oha,
            args.control_url,
            [],
            200,
            args.duration,
            args.slo_floor_ms,
            args.slo_mult,
            ladder,
        )
        r_ceiling = ctl["peak_rps"]
        mst = index["mst_rps"]
        headroom = round(r_ceiling / mst, 2) if mst else 0.0
        if not index["broke"]:
            bound = "rig-bound"  # never broke within the ladder
        elif headroom >= 2.0:
            bound = "server-bound"
        elif headroom >= 1.3:
            bound = "caution"
        else:
            bound = "rig-bound"
        result["control"] = {"r_ceiling_rps": r_ceiling, "headroom": headroom}
        result["bound_class"] = bound
        index["headroom"] = headroom
        index["bound_class"] = bound

    idx = result["index"]
    print(
        f"  MST={idx['mst_rps']} rps @ c={idx['c_knee']}  p99@knee={idx['p99_at_knee_ms']}ms  "
        f"breach={idx['breach_mode']}  bound={result.get('bound_class', 'n/a')}"
    )

    blob = json.dumps(result, indent=2)
    if args.output:
        Path(args.output).write_text(blob + "\n")
        print(f"wrote {args.output}")
    else:
        print(blob)


if __name__ == "__main__":
    main()
