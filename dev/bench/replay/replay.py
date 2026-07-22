#!/usr/bin/env python3
"""Replay a PyPI traffic trace against a pypiron server.

Open-loop: arrivals are exogenous (a scheduler fires each request at its trace
time, scaled by 1/speed) and a fixed pool of keep-alive connections services
them. If the server can't keep up, the ready queue backs up — that backlog is
the saturation signal, reported alongside per-tier latency. This mirrors real
traffic, where clients arrive whether or not the server is ready, unlike a
closed-loop tool (oha) that offers exactly N in flight.

Reports, per tier (index / artifact / metadata) and overall: throughput (rps),
p50/p95/p99 service latency, error and non-2xx/3xx counts, bytes moved, and the
queue backlog. Stdlib asyncio only — no oha here, because oha hammers one URL;
a trace is a stream of different URLs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
from urllib.parse import urlparse

UA = {"uv": "uv/0.7.0", "pip": "pip/24.0", "poetry": "poetry/1.8.0"}


def load_trace(path: Path) -> list[dict]:
    with path.open() as fh:
        return [json.loads(line) for line in fh if line.strip()]


class Conn:
    """One keep-alive HTTP/1.1 connection; reconnects on a dropped socket."""

    def __init__(self, host: str, port: int):
        self.host, self.port = host, port
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def _connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def get(self, path: str, ua: str) -> tuple[int, int]:
        """Return (status, body_bytes). Drains the body so the socket is reusable."""
        if self.writer is None:
            await self._connect()
        req = (
            f"GET {path} HTTP/1.1\r\nHost: {self.host}\r\n"
            f"User-Agent: {ua}\r\nAccept-Encoding: identity\r\n\r\n"
        )
        assert self.writer is not None and self.reader is not None
        self.writer.write(req.encode())
        await self.writer.drain()

        status_line = await self.reader.readline()
        if not status_line:
            raise ConnectionError("empty status line")
        status = int(status_line.split(b" ")[1])

        headers: dict[str, str] = {}
        while True:
            line = await self.reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            k, _, v = line.decode("latin1").partition(":")
            headers[k.strip().lower()] = v.strip()

        nbytes = await self._drain_body(status, headers)
        if headers.get("connection", "").lower() == "close":
            await self.close()
        return status, nbytes

    async def _drain_body(self, status: int, headers: dict[str, str]) -> int:
        assert self.reader is not None
        if (
            status in (204, 304)
            or headers.get("transfer-encoding") is None
            and "content-length" not in headers
        ):
            return 0
        if headers.get("transfer-encoding", "").lower() == "chunked":
            total = 0
            while True:
                size_line = await self.reader.readline()
                n = int(size_line.strip() or b"0", 16)
                if n == 0:
                    await self.reader.readline()  # trailing CRLF
                    return total
                total += len(await self.reader.readexactly(n + 2))  # data + CRLF
        n = int(headers["content-length"])
        if n:
            await self.reader.readexactly(n)
        return n

    async def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except (ConnectionError, OSError):
                pass
        self.reader = self.writer = None


async def run_speed(trace: list[dict], base: str, connections: int, speed: float) -> dict:
    parsed = urlparse(base)
    host, port = parsed.hostname, parsed.port or 80
    queue: asyncio.Queue = asyncio.Queue()
    records: list[tuple[str, int, float, int]] = []  # kind, status, latency, bytes
    errors = {"index": 0, "artifact": 0, "metadata": 0}
    max_backlog = 0

    async def scheduler() -> None:
        nonlocal max_backlog
        t0 = asyncio.get_event_loop().time()
        for req in trace:
            target = t0 + req["t"] / speed
            now = asyncio.get_event_loop().time()
            if target > now:
                await asyncio.sleep(target - now)
            queue.put_nowait(req)
            max_backlog = max(max_backlog, queue.qsize())
        for _ in range(connections):
            queue.put_nowait(None)

    async def worker() -> None:
        conn = Conn(host, port)
        while True:
            req = await queue.get()
            if req is None:
                break
            ua = UA.get(req["installer"], req["installer"])
            t = time.perf_counter()
            try:
                status, nbytes = await conn.get(req["path"], ua)
                records.append((req["kind"], status, time.perf_counter() - t, nbytes))
            except (ConnectionError, OSError, asyncio.IncompleteReadError, ValueError):
                errors[req["kind"]] += 1
                await conn.close()
        await conn.close()

    wall_start = time.perf_counter()
    await asyncio.gather(scheduler(), *(worker() for _ in range(connections)))
    wall = time.perf_counter() - wall_start

    return summarize(records, errors, wall, max_backlog, speed, connections)


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, int(len(s) * p))]


def summarize(records, errors, wall, max_backlog, speed, connections) -> dict:
    tiers = ("index", "artifact", "metadata")
    per_tier = {}
    for tier in tiers:
        lat = [r[2] * 1000 for r in records if r[0] == tier]
        ok = sum(1 for r in records if r[0] == tier and 200 <= r[1] < 400)
        bad = sum(1 for r in records if r[0] == tier and not (200 <= r[1] < 400))
        nbytes = sum(r[3] for r in records if r[0] == tier)
        n = len(lat)
        per_tier[tier] = {
            "requests": n,
            "rps": round(n / wall, 1) if wall else 0.0,
            "p50_ms": round(pct(lat, 0.5), 2),
            "p95_ms": round(pct(lat, 0.95), 2),
            "p99_ms": round(pct(lat, 0.99), 2),
            "ok": ok,
            "non_2xx_3xx": bad,
            "errors": errors[tier],
            "mb": round(nbytes / 1e6, 1),
        }
    total = len(records)
    return {
        "speed": speed,
        "connections": connections,
        "wall_s": round(wall, 2),
        "total_rps": round(total / wall, 1) if wall else 0.0,
        "max_backlog": max_backlog,
        "tiers": per_tier,
    }


def print_report(results: list[dict]) -> None:
    for r in results:
        print(
            f"\n=== speed x{r['speed']:g}  conns {r['connections']}  "
            f"wall {r['wall_s']}s  total {r['total_rps']} rps  max backlog {r['max_backlog']} ==="
        )
        print("| tier | reqs | rps | p50 ms | p95 ms | p99 ms | ok | non-2xx/3xx | err | MB |")
        print("|---|---|---|---|---|---|---|---|---|---|")
        for tier, t in r["tiers"].items():
            print(
                f"| {tier} | {t['requests']} | {t['rps']} | {t['p50_ms']} | {t['p95_ms']} "
                f"| {t['p99_ms']} | {t['ok']} | {t['non_2xx_3xx']} | {t['errors']} | {t['mb']} |"
            )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--trace", default=str(Path(__file__).resolve().parent / "trace" / "trace.jsonl")
    )
    ap.add_argument("--base-url", default="http://127.0.0.1:8080")
    ap.add_argument("--connections", type=int, default=32)
    ap.add_argument("--speed", default="1", help="comma-separated rate multipliers, e.g. 1,2,5")
    ap.add_argument("--output", default=None, help="write results JSON here")
    args = ap.parse_args()

    trace = load_trace(Path(args.trace))
    speeds = [float(s) for s in args.speed.split(",")]
    print(f"replaying {len(trace):,} requests over {trace[-1]['t']:.0f}s trace at speeds {speeds}")

    results = []
    for speed in speeds:
        results.append(asyncio.run(run_speed(trace, args.base_url, args.connections, speed)))
    print_report(results)

    if args.output:
        Path(args.output).write_text(json.dumps(results, indent=2))
        print(f"\nwrote {args.output}")


if __name__ == "__main__":
    main()
