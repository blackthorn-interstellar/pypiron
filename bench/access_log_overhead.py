#!/usr/bin/env python3
"""Quantify the per-request cost of logging a request.

A/B throughput + latency across the four logging modes against the same release
binary and endpoint, driven by `oha`. Server logs go to /dev/null, so this
isolates the formatting + `write()` cost from disk variance.

  off              reads not logged (the default) — baseline
  structured       --access-log                              (tracing event, text)
  structured-json  --access-log --log-format json
  clf              --access-log --access-log-format clf      (Combined Log Format)

The endpoint is a read (`/simple/index.json`): in the default mode reads are NOT
logged (clean baseline), and `--access-log` logs every request — so the delta is
exactly the logging cost. (Mutations log by default; `/health` and `/metrics` log
only at debug, so they would show no overhead here.) The access target runs at
info (`RUST_LOG=warn,pypiron::access=info`) with the diagnostic log quiet so it
doesn't skew the baseline; `clf` is flag-gated and emits regardless.

Usage:
  python bench/access_log_overhead.py [--duration 3s] [--connections 50]
                                      [--rounds 8] [--endpoint /simple/index.json]
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent

MODES: List[Tuple[str, List[str]]] = [
    ("off", []),
    ("structured", ["--access-log"]),
    ("structured-json", ["--access-log", "--log-format", "json"]),
    ("clf", ["--access-log", "--access-log-format", "clf"]),
]


def find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def ensure_built(bin_path: Path) -> Path:
    # Always build: cargo is incremental, so this is a no-op when up to date and
    # avoids benchmarking a stale binary.
    subprocess.run(["cargo", "build", "--release"], cwd=REPO_ROOT, check=True)
    return bin_path


def wait_healthy(base: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base}/health", timeout=1.0) as r:
                if r.status == 200:
                    return
        except (urllib.error.URLError, OSError):
            time.sleep(0.1)
    raise RuntimeError("server did not become healthy")


def start_server(bin_path: Path, data_dir: str, port: int, extra: List[str]):
    env = os.environ.copy()
    env["RUST_LOG"] = "warn,pypiron::access=info"
    args = [
        str(bin_path),
        "serve",
        "--bind-addr",
        f"127.0.0.1:{port}",
        "--data-dir",
        data_dir,
        "--worker-interval-secs",
        "60",  # quiet the worker for the measurement window
        *extra,
    ]
    devnull = open(os.devnull, "w")
    proc = subprocess.Popen(args, env=env, stdout=devnull, stderr=subprocess.STDOUT)
    return proc, devnull


def warm(base: str, endpoint: str, n: int = 200) -> None:
    for _ in range(n):
        try:
            urllib.request.urlopen(f"{base}{endpoint}", timeout=1.0).read()
        except OSError:
            pass


def run_oha(oha: str, url: str, duration: str, connections: int) -> Dict:
    cmd = [
        oha,
        "--no-tui",
        "--output-format",
        "json",
        "-r",
        "0",
        "-z",
        duration,
        "-c",
        str(connections),
        url,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(f"oha failed: {proc.stderr[:500]}")
    data = json.loads(proc.stdout)
    summary = data.get("summary", {})
    pct = data.get("latencyPercentiles", {})

    def ms(key: str) -> float:
        v = pct.get(key)
        return round(v * 1000, 3) if isinstance(v, (int, float)) else 0.0

    return {
        "rps": round(summary.get("requestsPerSec") or 0.0, 1),
        "p50_ms": ms("p50"),
        "p99_ms": ms("p99"),
        "statuses": data.get("statusCodeDistribution", {}),
    }


def mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def stdev(xs: List[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--duration", default="3s", help="oha duration per sample")
    ap.add_argument("--connections", type=int, default=50)
    ap.add_argument("--rounds", type=int, default=8, help="interleaved sampling rounds")
    ap.add_argument("--oha", default="oha")
    ap.add_argument("--bin", default=str(REPO_ROOT / "target/release/pypiron"))
    ap.add_argument("--endpoint", default="/simple/index.json")
    args = ap.parse_args()

    bin_path = ensure_built(Path(args.bin))

    # All servers stay up at once; only one is hammered at a time, so the others
    # sit idle. Sampling every mode back-to-back per round means each sees the
    # same instantaneous machine load — drift cancels out of the relative deltas,
    # which matters on a busy box.
    servers: Dict[str, Dict] = {}
    tmpdirs: List[tempfile.TemporaryDirectory] = []
    try:
        for name, extra in MODES:
            port = find_free_port()
            td = tempfile.TemporaryDirectory()
            tmpdirs.append(td)
            proc, devnull = start_server(bin_path, td.name, port, extra)
            servers[name] = {"proc": proc, "devnull": devnull, "port": port}
        for name in servers:
            base = f"http://127.0.0.1:{servers[name]['port']}"
            wait_healthy(base)
            warm(base, args.endpoint)

        samples: Dict[str, List[Dict]] = {name: [] for name, _ in MODES}
        for rnd in range(args.rounds):
            for name, _ in MODES:
                base = f"http://127.0.0.1:{servers[name]['port']}"
                samples[name].append(
                    run_oha(args.oha, f"{base}{args.endpoint}", args.duration, args.connections)
                )
            print(f"round {rnd + 1}/{args.rounds} done", flush=True)
    finally:
        for s in servers.values():
            s["proc"].terminate()
            try:
                s["proc"].wait(timeout=10)
            except subprocess.TimeoutExpired:
                s["proc"].kill()
            s["devnull"].close()
        for td in tmpdirs:
            td.cleanup()

    agg = {
        name: {
            "rps_mean": round(mean([s["rps"] for s in samples[name]]), 1),
            "rps_stdev": round(stdev([s["rps"] for s in samples[name]]), 1),
            "p50_ms": round(mean([s["p50_ms"] for s in samples[name]]), 3),
            "p99_ms": round(mean([s["p99_ms"] for s in samples[name]]), 3),
        }
        for name, _ in MODES
    }
    base_rps = agg["off"]["rps_mean"] or 1.0
    print(
        f"\naccess-log overhead — endpoint {args.endpoint}, oha -c {args.connections} "
        f"-z {args.duration} × {args.rounds} interleaved rounds, logs→/dev/null\n"
    )
    header = f"{'mode':<16}{'rps (mean±sd)':>20}{'p50_ms':>9}{'p99_ms':>9}{'Δ rps':>9}"
    print(header)
    print("-" * len(header))
    for name, _ in MODES:
        a = agg[name]
        delta = (a["rps_mean"] - base_rps) / base_rps * 100.0
        d = "—" if name == "off" else f"{delta:+.1f}%"
        rps = f"{a['rps_mean']:.0f}±{a['rps_stdev']:.0f}"
        print(f"{name:<16}{rps:>20}{a['p50_ms']:>9.3f}{a['p99_ms']:>9.3f}{d:>9}")
    print()
    print(json.dumps(agg, indent=2))


if __name__ == "__main__":
    main()
