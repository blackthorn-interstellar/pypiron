#!/usr/bin/env python3
"""Multi-node oha install-mix ramp (Track 2 ceiling finder).

Drives N loadgen instances in lockstep against one server, each replaying the
install-mix and FOLLOWING the 302 to download wheel bytes from S3, summing req/s
and MB/s per step. One loadgen's 12.5 Gbps NIC caps a single box; aggregating N
boxes pushes past that to find where the SERVER's index+redirect node (or S3)
actually breaks. Runs on the coordinator (this host); orchestrates loadgens over
ssh and samples the server's CPU each step. Reads .rig2.env.

  python3 mn_ramp.py --tier lite --ladder 512,1024,2048,4096
"""

from __future__ import annotations

import argparse
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load_env() -> dict:
    """Parse .rig2.env if present. Returns {} when absent so the module imports
    cleanly off-rig (the pure ceiling-finder is unit-tested without a rig)."""
    f = HERE / ".rig2.env"
    if not f.exists():
        return {}
    env = {}
    for line in f.read_text().splitlines():
        if line.startswith("export "):
            k, v = line[len("export ") :].split("=", 1)
            env[k] = v.strip().strip('"')
    return env


ENV = load_env()
KEY = ENV.get("RIG_KEY", "")
PRIV = ENV.get("RIG2_SERVER_PRIV", "")
SERVER_IP = ENV.get("RIG2_SERVER_IP", "")
N = int(ENV.get("RIG2_LOADGEN_N", 1))
LGS = [ENV[k] for i in range(1, N + 1) if (k := f"RIG2_LOADGEN_IP_{i}") in ENV]
SSH = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-i", KEY]
INDEX_URL = f"http://{PRIV}:8080/simple/"  # overridden by --index-url in main
CONTAINER = "pypiron"  # overridden by --container in main


def ssh_run(ip: str, cmd: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        SSH + [f"ec2-user@{ip}", cmd], capture_output=True, text=True, timeout=timeout
    )


def scp(local: str, ip: str, remote: str) -> None:
    subprocess.run(
        [
            "scp",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-i",
            KEY,
            local,
            f"ec2-user@{ip}:{remote}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def build_mix(tier: str) -> tuple[str, float]:
    """Build the install-mix regex + reqs/install on a loadgen (it reaches the server)."""
    code = (
        "import capacity,json;"
        f"r=capacity.build_install_mix('{INDEX_URL}','x86_64','{tier}');"
        "print(json.dumps({'regex':r[0],'rpi':r[1],'nidx':r[2],'nwhl':r[3],'drop':r[4]}))"
    )
    out = ssh_run(LGS[0], f'cd pypiron/bench/install && python3.11 -c "{code}"', timeout=240)
    if out.returncode != 0:
        raise SystemExit(f"build_mix failed: {out.stderr[-400:]}")
    d = json.loads(out.stdout.strip().splitlines()[-1])
    print(
        f"mix: {d['nidx']} index + {d['nwhl']} wheel URLs, {d['drop']} dropped, ~{d['rpi']} reqs/install"
    )
    return d["regex"], d["rpi"]


def push_runner(regex: str) -> None:
    """Generate a runner that embeds the regex single-quoted (safe — no quotes in
    it) and ship to every loadgen, so we avoid ssh/shell quoting the metachars."""
    runner = (
        "#!/bin/bash\n"
        '/home/ec2-user/oha --no-tui --json --redirect 5 -z "$1" -c "$2" '
        "-H 'Accept: application/vnd.pypi.simple.v1+json' "
        f"--rand-regex-url '{regex}'\n"
    )
    p = HERE / "_oha_runner.sh"
    p.write_text(runner)
    for ip in LGS:
        scp(str(p), ip, "/home/ec2-user/oha_runner.sh")
    p.unlink()


def server_cpu() -> float:
    out = ssh_run(
        SERVER_IP,
        f"sudo docker stats --no-stream --format '{{{{.CPUPerc}}}}' {CONTAINER}",
        timeout=30,
    )
    try:
        return float(out.stdout.strip().rstrip("%"))
    except ValueError:
        return -1.0


def run_node(ip: str, duration: str, c: int) -> dict:
    out = ssh_run(
        ip, f"bash /home/ec2-user/oha_runner.sh {duration} {c}", timeout=int(duration[:-1]) + 120
    )
    if out.returncode != 0:
        return {"rps": 0.0, "p99_ms": 0.0, "ok": 0, "total": 1, "bytes": 0.0}
    d = json.loads(out.stdout)
    s, pct = d.get("summary", {}), d.get("latencyPercentiles", {})
    st = d.get("statusCodeDistribution", {})
    return {
        "rps": s.get("requestsPerSec") or 0.0,
        "p99_ms": (pct.get("p99") or 0.0) * 1000,
        "ok": st.get("200", 0),
        "total": sum(st.values()) or 1,
        "bytes": s.get("totalData") or 0.0,
        "dur": s.get("total") or float(duration[:-1]),
    }


def measure_step(c: int, duration: str, rpi: float) -> dict:
    """Drive all N loadgens at per-node concurrency `c` for `duration` in lockstep,
    sample the server's CPU once, and aggregate into one ramp step."""
    with ThreadPoolExecutor(max_workers=N + 1) as pool:
        cpu_fut = pool.submit(server_cpu)
        res = [f.result() for f in [pool.submit(run_node, ip, duration, c) for ip in LGS]]
        scpu = cpu_fut.result()
    agg_rps = round(sum(r["rps"] for r in res), 1)
    ok = sum(r["ok"] for r in res)
    tot = sum(r["total"] for r in res)
    return {
        "per_node_c": c,
        "agg_concurrency": c * N,
        "agg_rps": agg_rps,
        "installs_per_sec": round(agg_rps / rpi, 1) if rpi else 0.0,
        "agg_mb_per_sec": round(sum(r["bytes"] / r.get("dur", 15) for r in res) / 1e6, 1),
        "p99_ms": round(max(r["p99_ms"] for r in res), 1),
        "ok_pct": round(100.0 * ok / tot, 2),
        "server_cpu_pct": scpu,
    }


def is_collapse(step: dict, best_installs: float, best_mbs: float) -> str | None:
    """Why this step is NOT a sustainable point, or None if healthy. Over-concurrency
    surfaces as wheel bytes stalling — MB/s craters while cheap index-only requests
    keep rps high — so check MB/s, not just rps. Server-CPU saturation is NOT a
    collapse: it's the wall we're hunting, handled by the caller."""
    if step["ok_pct"] < 99.0:
        return "errors"
    if best_mbs and step["agg_mb_per_sec"] < 0.5 * best_mbs:
        return "collapse"
    if best_installs and step["installs_per_sec"] < 0.85 * best_installs:
        return "collapse"
    return None


def summarize(ramp: list[dict], cpu_break: float) -> tuple[dict, str, float]:
    """Peak SUSTAINED step, server/rig bound verdict, peak healthy MB/s. Excludes
    collapsed/errored steps: their rps is inflated by index-only completions while
    real installs (wheel bytes) time out — not throughput the server sustains."""
    healthy = [s for s in ramp if s.get("breach") not in ("collapse", "errors")]
    peak = max(healthy or ramp, key=lambda s: s["installs_per_sec"])
    max_cpu = max((s["server_cpu_pct"] for s in ramp if s["server_cpu_pct"] >= 0), default=0.0)
    bound = "server-bound" if max_cpu >= 0.85 * cpu_break else "rig-limited"
    peak_mbs = max((s["agg_mb_per_sec"] for s in healthy), default=peak["agg_mb_per_sec"])
    return peak, bound, peak_mbs


def run_ladder(measure, ladder: list[int], cpu_break: float) -> tuple[list[dict], str]:
    """Fixed-ladder ramp (reproducible / debugging): step through `ladder`, stop at
    the first breach. `measure(c) -> step`."""
    ramp: list[dict] = []
    best_installs = best_mbs = 0.0
    breach = "none(ladder-cap)"
    for c in ladder:
        s = measure(c)
        ramp.append(s)
        best_mbs = max(best_mbs, s["agg_mb_per_sec"])
        why = is_collapse(s, best_installs, best_mbs)
        best_installs = max(best_installs, s["installs_per_sec"])
        if why:
            s["breach"] = breach = why
            break
        if s["server_cpu_pct"] >= cpu_break:
            s["breach"] = breach = "server-cpu"
            break
    return ramp, breach


def find_ceiling(
    measure,
    *,
    c_start: int = 64,
    c_max: int = 32768,
    cpu_break: float = 92.0,
    refine_ratio: float = 1.2,
    plateau_eps: float = 0.04,
    max_samples: int = 24,
) -> tuple[list[dict], str]:
    """Auto-find max sustained installs/s, no hand-tuned ladder. Phase 1 doubles
    per-node concurrency to BRACKET the knee — stopping on collapse, on CPU
    saturation once throughput flattens (gain < plateau_eps), or at c_max. Phase 2
    geometric-bisects the bracket to PIN the knee within `refine_ratio`. This is
    scale-free: a Python server brackets in a few low steps; pypiron in ~7 high
    ones — and it never steps over the knee the way a fixed ladder can.
    `measure(c) -> step`; returns (samples sorted by c, top-level breach)."""
    samples: dict[int, dict] = {}

    def at(c: int) -> dict:
        c = max(1, int(c))
        if c not in samples:
            samples[c] = measure(c)
        return samples[c]

    best_installs = best_mbs = 0.0
    c_lo, c_hi, breach = c_start, 0, "rig-cap"

    c = c_start
    while c <= c_max and len(samples) < max_samples:
        s = at(c)
        best_mbs = max(best_mbs, s["agg_mb_per_sec"])
        if why := is_collapse(s, best_installs, best_mbs):
            s["breach"], breach, c_hi = why, why, c
            break
        gain = (s["installs_per_sec"] - best_installs) / best_installs if best_installs else 1.0
        best_installs = max(best_installs, s["installs_per_sec"])
        if s["server_cpu_pct"] >= cpu_break and gain < plateau_eps:
            s["breach"], breach, c_hi, c_lo = "server-cpu", "server-cpu", c, c
            break
        c_lo = c
        c *= 2

    # Bisect the [last-healthy, first-breach] bracket toward the higher sustained
    # throughput. Skipped when the bracket is a point (plateau/rig-cap: c_hi == 0).
    while c_hi > int(c_lo * refine_ratio) and len(samples) < max_samples:
        mid = int(round((c_lo * c_hi) ** 0.5))
        if mid <= c_lo or mid >= c_hi:
            break
        s = at(mid)
        best_mbs = max(best_mbs, s["agg_mb_per_sec"])
        if why := is_collapse(s, best_installs, best_mbs):
            s["breach"], c_hi = why, mid
        else:
            best_installs = max(best_installs, s["installs_per_sec"])
            c_lo = mid

    return [samples[c] for c in sorted(samples)], breach


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", default="lite")
    ap.add_argument(
        "--ladder", default=None, help="per-node concurrency CSV; OMIT to auto-search the ceiling"
    )
    ap.add_argument("--c-start", type=int, default=64, help="search: starting per-node concurrency")
    ap.add_argument("--c-max", type=int, default=32768, help="search: per-node safety cap")
    ap.add_argument(
        "--refine-ratio", type=float, default=1.2, help="search: stop bisecting within this ratio"
    )
    ap.add_argument(
        "--plateau-eps",
        type=float,
        default=0.04,
        help="search: throughput gain below this at CPU saturation = the knee",
    )
    ap.add_argument("--duration", default="15s")
    ap.add_argument("--cpu-break", type=float, default=92.0, help="server CPU%% = saturated")
    ap.add_argument("--index-url", default=f"http://{PRIV}:8080/simple/", help="PEP503 root")
    ap.add_argument("--container", default="pypiron", help="server container name for CPU sampling")
    ap.add_argument("--output", default="results/mnramp-pypiron-t2.json")
    args = ap.parse_args()

    global INDEX_URL, CONTAINER
    INDEX_URL = args.index_url
    CONTAINER = args.container
    regex, rpi = build_mix(args.tier)
    push_runner(regex)
    mode = "fixed ladder" if args.ladder else "AUTO-SEARCH (bracket -> bisect)"
    print(f"driving {N} loadgens in lockstep vs {PRIV}:8080 (Track 2, bytes from S3) — {mode}\n")

    def measure(c: int) -> dict:
        s = measure_step(c, args.duration, rpi)
        print(
            f"  c={c}x{N}={c * N:<7} {s['agg_rps']:>8} rps  {s['installs_per_sec']:>7} inst/s  "
            f"{s['agg_mb_per_sec']:>6} MB/s  p99={s['p99_ms']:>7}ms  ok={s['ok_pct']}%  "
            f"serverCPU={s['server_cpu_pct']}%"
        )
        return s

    if args.ladder:
        ramp, breach = run_ladder(measure, [int(x) for x in args.ladder.split(",")], args.cpu_break)
    else:
        ramp, breach = find_ceiling(
            measure,
            c_start=args.c_start,
            c_max=args.c_max,
            cpu_break=args.cpu_break,
            refine_ratio=args.refine_ratio,
            plateau_eps=args.plateau_eps,
        )

    peak, bound, peak_mbs = summarize(ramp, args.cpu_break)
    out = {
        "label": f"{args.container}-t2-mn",
        "loadgens": N,
        "reqs_per_install": rpi,
        "peak_agg_rps": peak["agg_rps"],
        "peak_installs_per_sec": peak["installs_per_sec"],
        "peak_mb_per_sec": peak_mbs,
        "breach": breach,
        "bound": bound,
        "peak_per_node_c": peak["per_node_c"],
        "samples": len(ramp),
        "ramp": ramp,
    }
    Path(HERE / args.output).parent.mkdir(parents=True, exist_ok=True)
    (HERE / args.output).write_text(json.dumps(out, indent=2))
    print(
        f"\n  => peak {peak['agg_rps']} rps ({peak['installs_per_sec']} inst/s @ "
        f"c={peak['per_node_c']}x{N}, {peak_mbs} MB/s) {bound}, breach={breach}; "
        f"{len(ramp)} samples; wrote {args.output}"
    )


if __name__ == "__main__":
    main()
