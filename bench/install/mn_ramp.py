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
    env = {}
    for line in (HERE / ".rig2.env").read_text().splitlines():
        if line.startswith("export "):
            k, v = line[len("export ") :].split("=", 1)
            env[k] = v.strip().strip('"')
    return env


ENV = load_env()
KEY = ENV["RIG_KEY"]
PRIV = ENV["RIG2_SERVER_PRIV"]
SERVER_IP = ENV["RIG2_SERVER_IP"]
N = int(ENV["RIG2_LOADGEN_N"])
LGS = [ENV[f"RIG2_LOADGEN_IP_{i}"] for i in range(1, N + 1)]
SSH = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-i", KEY]


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
        f"r=capacity.build_install_mix('http://{PRIV}:8080/simple/','x86_64','{tier}');"
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
        SERVER_IP, "sudo docker stats --no-stream --format '{{.CPUPerc}}' pypiron", timeout=30
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", default="lite")
    ap.add_argument("--ladder", default="256,512,1024,2048,4096", help="per-node concurrency")
    ap.add_argument("--duration", default="15s")
    ap.add_argument("--cpu-break", type=float, default=92.0, help="server CPU%% = saturated")
    ap.add_argument("--output", default="results/mnramp-pypiron-t2.json")
    args = ap.parse_args()

    regex, rpi = build_mix(args.tier)
    push_runner(regex)
    print(f"driving {N} loadgens in lockstep vs {PRIV}:8080 (Track 2, bytes from S3)\n")

    steps, peak = [], 0.0
    for c in [int(x) for x in args.ladder.split(",")]:
        cpu_box: list[float] = []
        with ThreadPoolExecutor(max_workers=N + 1) as pool:
            cpu_fut = pool.submit(server_cpu)
            futs = [pool.submit(run_node, ip, args.duration, c) for ip in LGS]
            res = [f.result() for f in futs]
            cpu_box.append(cpu_fut.result())
        agg_rps = round(sum(r["rps"] for r in res), 1)
        agg_mbs = round(sum(r["bytes"] / r.get("dur", 15) for r in res) / 1e6, 1)
        p99 = round(max(r["p99_ms"] for r in res), 1)
        ok = sum(r["ok"] for r in res)
        tot = sum(r["total"] for r in res)
        ok_pct = round(100.0 * ok / tot, 2)
        scpu = cpu_box[0]
        installs_s = round(agg_rps / rpi, 1) if rpi else 0.0
        step = {
            "per_node_c": c,
            "agg_concurrency": c * N,
            "agg_rps": agg_rps,
            "installs_per_sec": installs_s,
            "agg_mb_per_sec": agg_mbs,
            "p99_ms": p99,
            "ok_pct": ok_pct,
            "server_cpu_pct": scpu,
        }
        steps.append(step)
        peak = max(peak, agg_rps)
        print(
            f"  c={c}x{N}={c * N:<6} {agg_rps:>8} rps  {installs_s:>6} inst/s  {agg_mbs:>6} MB/s  "
            f"p99={p99:>7}ms  ok={ok_pct}%  serverCPU={scpu}%"
        )
        if ok_pct < 99.0:
            step["breach"] = "errors"
            break
        if scpu >= args.cpu_break:
            step["breach"] = "server-cpu"
            break
        if peak and agg_rps < 0.85 * peak:
            step["breach"] = "collapse"
            break
    else:
        steps[-1]["breach"] = "none(ladder-cap)"

    best = max((s for s in steps), key=lambda s: s["agg_rps"])
    out = {
        "label": "pypiron-t2-mn",
        "loadgens": N,
        "reqs_per_install": rpi,
        "peak_agg_rps": best["agg_rps"],
        "peak_installs_per_sec": best["installs_per_sec"],
        "peak_mb_per_sec": best["agg_mb_per_sec"],
        "breach": steps[-1].get("breach"),
        "ramp": steps,
    }
    Path(HERE / args.output).parent.mkdir(parents=True, exist_ok=True)
    (HERE / args.output).write_text(json.dumps(out, indent=2))
    print(
        f"\n  => peak {best['agg_rps']} rps ({best['installs_per_sec']} inst/s, {best['agg_mb_per_sec']} MB/s) "
        f"breach={steps[-1].get('breach')}; wrote {args.output}"
    )


if __name__ == "__main__":
    main()
