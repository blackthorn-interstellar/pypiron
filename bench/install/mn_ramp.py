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


def build_mix(tier: str) -> dict:
    """Build the install-mix consts + per-class regexes on a loadgen (it reaches the
    server). Returns capacity.build_install_mix's dict verbatim."""
    code = (
        "import capacity,json;"
        f"print(json.dumps(capacity.build_install_mix('{INDEX_URL}','x86_64','{tier}')))"
    )
    out = ssh_run(LGS[0], f'cd pypiron/bench/install && python3.11 -c "{code}"', timeout=240)
    if out.returncode != 0:
        raise SystemExit(f"build_mix failed: {out.stderr[-400:]}")
    m = json.loads(out.stdout.strip().splitlines()[-1])
    print(
        f"mix: {m['n_index']} index + {m['n_wheel']} wheel URLs, {m['dropped']} dropped, "
        f"~{m['reqs_per_install']} reqs/install, mean wheel {m['mean_wheel_bytes'] / 1e6:.2f} MB"
    )
    return m


def push_runner(index_regex: str, wheel_regex: str) -> None:
    """Ship a runner that fires TWO concurrent ohas — one index-only, one wheel-only —
    so wheel completions are counted independently of cheap index hits (oha reports
    only ONE aggregate status/byte total per process). Regexes are embedded
    single-quoted (safe — no quotes in them) and scp'd, avoiding ssh/shell quoting
    the metachars. stderr is discarded so it can't corrupt the JSON stream."""
    runner = (
        "#!/bin/bash\n"
        'DUR="$1"; CIDX="$2"; CWHL="$3"\n'
        "H1='Accept: application/vnd.pypi.simple.v1+json'; H2='User-Agent: uv/0.9.30'\n"
        '/home/ec2-user/oha --no-tui --json --redirect 5 -z "$DUR" -c "$CIDX" -H "$H1" -H "$H2" \\\n'
        f"  --rand-regex-url '{index_regex}' >/tmp/oha_idx.json 2>/dev/null &\n"
        "PI=$!\n"
        '/home/ec2-user/oha --no-tui --json --redirect 5 -z "$DUR" -c "$CWHL" -H "$H1" -H "$H2" \\\n'
        f"  --rand-regex-url '{wheel_regex}' >/tmp/oha_whl.json 2>/dev/null &\n"
        "PW=$!\n"
        "wait $PI; wait $PW\n"
        "echo '===IDX==='; cat /tmp/oha_idx.json\n"
        "echo '===WHL==='; cat /tmp/oha_whl.json\n"
    )
    p = HERE / "_oha_runner.sh"
    p.write_text(runner)
    for ip in LGS:
        scp(str(p), ip, "/home/ec2-user/oha_runner.sh")
    p.unlink()


def parse_oha(section: str, duration: str) -> dict:
    """Parse ONE oha JSON section into completed-200 counts + body bytes. `rps` is the
    completed-200 rate (ok/dur), NOT summary.requestsPerSec — that is the monotone-
    truthful signal: a stalled body never lands in status["200"] or totalData. A -z
    deadline aborts in-flight requests into errorDistribution and EXCLUDES them from
    both status and bytes, so benign deadline aborts must NOT count as failures; only
    real errors (reset/refused/body-read) do."""
    default = float(duration[:-1])
    try:
        d = json.loads(section)
    except (json.JSONDecodeError, ValueError):
        return {"ok": 0, "total": 1, "bytes": 0.0, "dur": default, "p99_ms": 0.0, "rps": 0.0}
    s = d.get("summary", {}) or {}
    st = d.get("statusCodeDistribution", {}) or {}
    err = d.get("errorDistribution", {}) or {}
    pct = d.get("latencyPercentiles", {}) or {}
    completed = sum(st.values())
    deadline = sum(v for k, v in err.items() if "deadline" in k.lower())
    real_err = sum(err.values()) - deadline
    ok = st.get("200", 0)
    dur = s.get("total") or default
    return {
        "ok": ok,
        "total": (completed + real_err) or 1,
        "bytes": s.get("totalData") or 0.0,
        "dur": dur,
        "p99_ms": (pct.get("p99") or 0.0) * 1000,
        "rps": (ok / dur) if dur else 0.0,
    }


def run_node(ip: str, duration: str, c: int, wheel_frac: float) -> dict:
    """Drive one loadgen's two-oha runner at per-node concurrency `c`, split index vs
    wheel by the mix's wheel fraction, and parse both sections."""
    c_whl = max(1, round(c * wheel_frac))
    c_idx = max(1, c - c_whl)
    out = ssh_run(
        ip,
        f"bash /home/ec2-user/oha_runner.sh {duration} {c_idx} {c_whl}",
        timeout=int(duration[:-1]) + 120,
    )
    if out.returncode != 0:
        z = {
            "ok": 0,
            "total": 1,
            "bytes": 0.0,
            "dur": float(duration[:-1]),
            "p99_ms": 0.0,
            "rps": 0.0,
        }
        return {"c_idx": c_idx, "c_whl": c_whl, "idx": z, "whl": dict(z)}
    txt = out.stdout
    idx_sec = txt.partition("===IDX===")[2].partition("===WHL===")[0]
    whl_sec = txt.partition("===WHL===")[2]
    return {
        "c_idx": c_idx,
        "c_whl": c_whl,
        "idx": parse_oha(idx_sec, duration),
        "whl": parse_oha(whl_sec, duration),
    }


def parse_cpu_line(text: str) -> tuple[list[int], int]:
    """Extract the aggregate `cpu` jiffie fields and count `cpuN` cores from a
    /proc/stat snapshot."""
    fields, cores = [], 0
    for line in text.splitlines():
        p = line.split()
        if not p:
            continue
        if p[0] == "cpu":
            fields = [int(x) for x in p[1:]]
        elif p[0][:3] == "cpu" and p[0][3:].isdigit():
            cores += 1
    return fields, cores


def cpu_pct_from_stat(stat0: str, stat1: str) -> float:
    """Whole-machine busy% from two /proc/stat snapshots, scaled to 0..cores×100 (the
    docker convention, so --cpu-break = cores*95 stays meaningful). busy =
    user+sys+irq+softirq; iowait/idle/steal excluded. -1.0 sentinel on bad input."""
    f0, cores = parse_cpu_line(stat0)
    f1, _ = parse_cpu_line(stat1)
    if not f0 or not f1 or cores == 0 or len(f0) < 7 or len(f1) < 7:
        return -1.0
    d = [b - a for a, b in zip(f0, f1)]  # user nice sys idle iowait irq softirq steal...
    total = sum(d)
    if total <= 0:
        return -1.0
    busy = d[0] + d[2] + d[5] + d[6]  # user+sys+irq+softirq (R4, verbatim)
    return round(busy / total * cores * 100, 1)


def server_cpu_window(dur_s: int) -> float:
    """Average server CPU over the whole measurement window via a /proc/stat delta."""
    out = ssh_run(
        SERVER_IP,
        f"cat /proc/stat; sleep {dur_s}; echo ---; cat /proc/stat",
        timeout=dur_s + 60,
    )
    if out.returncode != 0:
        return -1.0
    a, _, b = out.stdout.partition("---")
    return cpu_pct_from_stat(a, b)


ANCHOR_HARD = 0.35


def mix_ratio(wheel_bytes: float, wheel_ok: int, mean_wheel_bytes: float) -> float:
    """Served mean wheel size / mix mean wheel size. 1.0 = the wheel traffic matches
    the corpus; <1 means truncated/short bodies survived as completions."""
    if wheel_ok <= 0 or mean_wheel_bytes <= 0:
        return 0.0
    return (wheel_bytes / wheel_ok) / mean_wheel_bytes


def aggregate_step(c: int, node_rows: list[dict], scpu: float, consts: dict) -> dict:
    """Aggregate all N nodes' two-oha rows into one byte-truthful ramp step.
    installs/s is derived from COMPLETED wheel fetches only, so it cannot inflate
    when wheel delivery stalls while cheap index hits keep completing."""
    wpi = consts["wheels_per_install"]
    mwb = consts["mean_wheel_bytes"]
    tol = consts["mix_tol"]
    W = [r["whl"] for r in node_rows]
    I = [r["idx"] for r in node_rows]
    wheel_ok = sum(w["ok"] for w in W)
    wheel_total = sum(w["total"] for w in W)
    wheel_bytes = sum(w["bytes"] for w in W)
    wheel_rps = round(sum(w["ok"] / w["dur"] for w in W), 1)
    wheel_mbps = round(sum(w["bytes"] / w["dur"] for w in W) / 1e6, 1)
    wheel_p99 = round(max((w["p99_ms"] for w in W), default=0.0), 1)
    index_ok = sum(i["ok"] for i in I)
    index_total = sum(i["total"] for i in I)
    index_rps = round(sum(i["ok"] / i["dur"] for i in I), 1)
    index_p99 = round(max((i["p99_ms"] for i in I), default=0.0), 1)

    installs = round(wheel_rps / wpi, 1) if wpi else 0.0
    mb_per_install = mwb * wpi / 1e6
    r = mix_ratio(wheel_bytes, wheel_ok, mwb)
    if r > 1 + ANCHOR_HARD and wheel_ok >= 30:  # R6 per-step hard stop (upward = impossible)
        raise RuntimeError(
            f"byte-anchor broken at c={c}: served mean {r:.2f}x manifest "
            f"(wheel_ok={wheel_ok}, bytes={wheel_bytes}); harness miscount, refusing to emit a number"
        )
    return {
        "per_node_c": c,
        "agg_concurrency": c * N,
        "c_idx": node_rows[0]["c_idx"] if node_rows else 0,
        "c_whl": node_rows[0]["c_whl"] if node_rows else 0,
        "agg_rps": round(index_rps + wheel_rps, 1),
        "installs_per_sec": installs,  # R1: wheel-count truth
        "agg_mb_per_sec": wheel_mbps,  # wheel payload = byte anchor
        "p99_ms": wheel_p99,
        "ok_pct": round(100.0 * wheel_ok / max(wheel_total, 1), 2),
        "server_cpu_pct": scpu,
        "wheel_ok": wheel_ok,
        "wheel_total": wheel_total,
        "wheel_ok_pct": round(100.0 * wheel_ok / max(wheel_total, 1), 2),
        "wheel_bytes": wheel_bytes,
        "wheel_mb_per_sec": wheel_mbps,
        "wheel_rps": wheel_rps,
        "wheel_p99_ms": wheel_p99,
        "index_ok": index_ok,
        "index_total": index_total,
        "index_ok_pct": round(100.0 * index_ok / max(index_total, 1), 2),
        "index_rps": index_rps,
        "index_p99_ms": index_p99,
        "mb_per_install": round((wheel_bytes / wheel_ok / 1e6) * wpi, 3) if wheel_ok else 0.0,
        "installs_from_bytes": round(wheel_mbps / mb_per_install, 1) if mb_per_install else 0.0,
        "mix_r": round(r, 3),
        "mix_ok": bool(wheel_ok > 0 and r >= 1 - tol),  # R2
    }


def measure_step(c: int, duration: str, consts: dict) -> dict:
    """Drive all N loadgens (two ohas each) at per-node concurrency `c` for `duration`
    in lockstep, average the server's CPU over the same window, and aggregate."""
    with ThreadPoolExecutor(max_workers=N + 1) as pool:
        cpu_fut = pool.submit(server_cpu_window, int(duration[:-1]))
        rows = [
            f.result()
            for f in [pool.submit(run_node, ip, duration, c, consts["wheel_frac"]) for ip in LGS]
        ]
        scpu = cpu_fut.result()
    return aggregate_step(c, rows, scpu, consts)


def is_collapse(step: dict, best_installs: float) -> str | None:
    """Why this step is NOT a sustainable point, or None if healthy. Order matters:
    real wheel failures first, then the mix-integrity breach (count-high / bytes-low
    survivorship — the R2 target), then a genuine retrograde in wheel-truthful
    installs. Server-CPU saturation is NOT a collapse: it's the wall we're hunting,
    handled by the caller."""
    if step["wheel_ok_pct"] < 99.0:
        return "errors"  # real wheel failures
    if not step["mix_ok"]:
        return "mix"  # R2: bytes-per-install off constant
    if best_installs and step["installs_per_sec"] < 0.85 * best_installs:
        return "collapse"
    return None


def summarize(ramp: list[dict], cpu_break: float) -> tuple[dict, str, float]:
    """Peak SUSTAINED step, server/rig bound verdict, peak healthy MB/s. Peak comes
    ONLY from a fully healthy step (never a collapse/errors/mix breach).

    server-bound requires evidence AT/ADJACENT to the peak: CPU saturated there, OR a
    genuine post-knee decline in WHEEL throughput while the server is still busy AND
    index rps did NOT climb (a climbing index while wheels crater is fleet thrash =
    rig-limited, not the server breaking)."""
    healthy = [
        s
        for s in ramp
        if s.get("breach") not in ("collapse", "errors", "mix") and s.get("mix_ok", True)
    ]
    peak = max(healthy or ramp, key=lambda s: s["installs_per_sec"])
    order = sorted(ramp, key=lambda s: s["per_node_c"])
    i = next(j for j, s in enumerate(order) if s is peak)  # identity, not ==
    neigh = order[max(0, i - 1) : i + 2]
    cpu = lambda s: s["server_cpu_pct"]  # noqa: E731
    saturated_at_peak = any(cpu(s) >= 0.85 * cpu_break for s in neigh if cpu(s) >= 0)
    post = order[i + 1 :]
    genuine_knee = any(
        s["installs_per_sec"] < 0.85 * peak["installs_per_sec"]
        and cpu(s) >= 0.5 * cpu_break
        and s["index_rps"]
        <= peak["index_rps"] * 1.05  # index did NOT climb → real knee, not thrash
        for s in post
    )
    bound = "server-bound" if (saturated_at_peak or genuine_knee) else "rig-limited"
    peak_mbs = max((s["agg_mb_per_sec"] for s in healthy), default=peak["agg_mb_per_sec"])
    return peak, bound, peak_mbs


def run_ladder(measure, ladder: list[int], cpu_break: float) -> tuple[list[dict], str]:
    """Fixed-ladder ramp (reproducible / debugging): step through `ladder`, stop at
    the first breach. `measure(c) -> step`."""
    ramp: list[dict] = []
    best_installs = 0.0
    breach = "none(ladder-cap)"
    for c in ladder:
        s = measure(c)
        ramp.append(s)
        why = is_collapse(s, best_installs)
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

    best_installs = 0.0
    c_lo, c_hi, breach = c_start, 0, "rig-cap"

    c = c_start
    while c <= c_max and len(samples) < max_samples:
        s = at(c)
        if why := is_collapse(s, best_installs):
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
        if why := is_collapse(s, best_installs):
            s["breach"], c_hi = why, mid
        else:
            best_installs = max(best_installs, s["installs_per_sec"])
            c_lo = mid

    # Walk DOWN if the best sample is the lowest concurrency tried: a server that
    # saturates below c_start (e.g. a single GIL-bound worker) peaks at tiny
    # concurrency, so the upward bracket started past its knee. Halve while
    # lowering load still raises throughput, until it doesn't or we hit c=1.
    while len(samples) < max_samples:
        lo = min(samples)
        if lo <= 1 or max(samples, key=lambda k: samples[k]["installs_per_sec"]) != lo:
            break
        if at(lo // 2)["installs_per_sec"] <= samples[lo]["installs_per_sec"]:
            break

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
    ap.add_argument(
        "--mix-tol",
        type=float,
        default=0.20,
        help="mix-integrity band: served mean wheel size vs mix mean",
    )
    ap.add_argument("--output", default="results/mnramp-pypiron-t2.json")
    args = ap.parse_args()

    global INDEX_URL, CONTAINER
    INDEX_URL = args.index_url
    CONTAINER = args.container
    m = build_mix(args.tier)
    consts = {
        "wheels_per_install": m["wheels_per_install"],
        "mean_wheel_bytes": m["mean_wheel_bytes"],
        "wheel_frac": m["wheel_frac"],
        "mix_tol": args.mix_tol,
    }
    push_runner(m["index_regex"], m["wheel_regex"])
    mode = "fixed ladder" if args.ladder else "AUTO-SEARCH (bracket -> bisect)"
    print(f"driving {N} loadgens in lockstep vs {PRIV}:8080 (Track 2, bytes from S3) — {mode}\n")

    def measure(c: int) -> dict:
        s = measure_step(c, args.duration, consts)
        print(
            f"  c={c}x{N}={c * N:<7} {s['agg_rps']:>8} rps  {s['installs_per_sec']:>7} inst/s  "
            f"wheel={s['wheel_rps']:>8} idx={s['index_rps']:>8}  {s['agg_mb_per_sec']:>6} MB/s  "
            f"mix_r={s['mix_r']}  p99={s['p99_ms']:>7}ms  ok={s['ok_pct']}%  "
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

    # R6 fail-loud: never emit a number the byte anchor can't back.
    if consts["wheels_per_install"] <= 0 or consts["mean_wheel_bytes"] <= 0:
        raise SystemExit("degenerate mix constants — refusing to emit a number")
    if any(s["wheel_ok"] > 0 for s in ramp) and not any(s["mix_ok"] for s in ramp):
        raise SystemExit(
            "byte anchor never held: served wheel bytes match the mix on NO step — "
            "harness miscalibrated (mix/sizes/parse), refusing to emit a number"
        )

    peak, bound, peak_mbs = summarize(ramp, args.cpu_break)
    out = {
        "label": f"{args.container}-t2-mn",
        "loadgens": N,
        "reqs_per_install": m["reqs_per_install"],
        "peak_agg_rps": peak["agg_rps"],
        "peak_installs_per_sec": peak["installs_per_sec"],
        "peak_mb_per_sec": peak_mbs,
        "breach": breach,
        "bound": bound,
        "peak_per_node_c": peak["per_node_c"],
        "samples": len(ramp),
        "ramp": ramp,
        "wheels_per_install": m["wheels_per_install"],
        "mean_wheel_mb": round(m["mean_wheel_bytes"] / 1e6, 4),
        "mb_per_install": round(m["mean_wheel_bytes"] * m["wheels_per_install"] / 1e6, 3),
        "n_index": m["n_index"],
        "n_wheel": m["n_wheel"],
        "dropped": m["dropped"],
        "peak_server_cpu_pct": round(
            max((s["server_cpu_pct"] for s in ramp if s["server_cpu_pct"] >= 0), default=0.0), 1
        ),
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
