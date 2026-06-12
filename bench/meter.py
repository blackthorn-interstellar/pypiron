#!/usr/bin/env python3
"""The meter: the fixed benchmark suite for the reference rig.

Stdlib only (runs on a bare AL2023 box with python3 + an `oha` binary).
Scenarios and targets are defined in docs/BENCHMARKS.md; this file must keep
its shape forever so any two runs are comparable.

Subcommands:
  seed   - create the meter corpus through the server's /legacy/ endpoint
  run    - run the meter scenarios, write results JSON + markdown rows

Server-mode switches (sync uploads, proxy downloads) are delegated to
--restart-cmd: a command invoked as `<cmd> <default|sync|proxy>` that
restarts the server in that mode and returns once it's healthy. Without it,
mode-specific scenarios are skipped (noted in results).
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import shlex
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

# Corpus shape: frozen. Changing these breaks run-to-run comparability.
SMALL_PKG = "bench-small"
SMALL_FILES = 10
TORCH_PKG = "torchsim"
TORCH_FILES = 2000
W1_PKG = "bench-w1meter"
W1_MB = 100
TORCH_UPLOAD_PKG = "bench-w1torch"
TORCH_UPLOAD_MB = 900
W3_PKG = "bench-w3"
W3_ITERATIONS = 20
W4_PKG = "bench-w4"
W4_ITERATIONS = 10


# ------------------------------- HTTP helpers --------------------------------


def _auth_header(user: str, password: str) -> str:
    token = f"{user}:{password}".encode("utf-8")
    return "Basic " + base64.b64encode(token).decode("ascii")


def http_get(url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 30.0) -> Tuple[int, bytes, Dict[str, str]]:
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read(), dict(e.headers)


def wait_healthy(base_url: str, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            status, _, _ = http_get(f"{base_url}/simple/", timeout=5.0)
            if status == 200:
                return
            last = f"status {status}"
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            last = repr(e)
        time.sleep(0.5)
    raise RuntimeError(f"server at {base_url} not healthy after {timeout}s: {last}")


# ------------------------------ Wheel generation -----------------------------


def make_wheel_bytes(name: str, version: str, payload_size: int) -> bytes:
    """Minimal valid wheel: dist-info with METADATA/WHEEL/RECORD + stored payload.

    ZIP_STORED so payload_size is honest (no compression lies in transfer
    numbers) and generation is fast.
    """
    metadata = (
        f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
        f"Requires-Python: >=3.8\nSummary: pypiron bench corpus\n"
    )
    wheel_meta = "Wheel-Version: 1.0\nGenerator: pypiron-bench\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
    di = f"{name}-{version}.dist-info"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        if payload_size > 0:
            # One random MB repeated: incompressible enough for transfer
            # honesty, generated in milliseconds even at 900 MB.
            chunk = os.urandom(min(payload_size, 1 << 20))
            with zf.open(zipfile.ZipInfo(f"{name}/payload.bin"), "w", force_zip64=True) as f:
                remaining = payload_size
                while remaining > 0:
                    f.write(chunk[: min(remaining, len(chunk))])
                    remaining -= len(chunk)
        zf.writestr(f"{di}/METADATA", metadata)
        zf.writestr(f"{di}/WHEEL", wheel_meta)
        zf.writestr(f"{di}/RECORD", "")
    return buf.getvalue()


def wheel_filename(name: str, version: str, tag: str = "py3-none-any") -> str:
    return f"{name.replace('-', '_')}-{version}-{tag}.whl"


# --------------------------------- Uploading ----------------------------------


def upload_wheel(
    legacy_url: str,
    filename: str,
    file_bytes: bytes,
    name: str,
    version: str,
    user: str,
    password: str,
    timeout: float = 600.0,
) -> Tuple[int, bytes]:
    """POST multipart/form-data the way twine does (mirrors tests/helpers.py)."""
    form = {
        ":action": "file_upload",
        "protocol_version": "1",
        "name": name,
        "version": version,
        "sha256_digest": hashlib.sha256(file_bytes).hexdigest(),
    }
    boundary = f"------------------------{uuid.uuid4().hex}"
    crlf = "\r\n"
    parts: List[bytes] = []
    for key, value in form.items():
        parts.append(
            (
                f"--{boundary}{crlf}"
                f'Content-Disposition: form-data; name="{key}"{crlf}{crlf}'
                f"{value}{crlf}"
            ).encode()
        )
    parts.append(
        (
            f"--{boundary}{crlf}"
            f'Content-Disposition: form-data; name="content"; filename="{filename}"{crlf}'
            f"Content-Type: application/octet-stream{crlf}{crlf}"
        ).encode()
    )
    parts.append(file_bytes)
    parts.append(crlf.encode())
    parts.append(f"--{boundary}--{crlf}".encode())
    body = b"".join(parts)

    req = urllib.request.Request(
        legacy_url,
        data=body,
        method="POST",
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Authorization": _auth_header(user, password),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def wait_visible(base_url: str, pkg: str, filename: str, timeout: float = 120.0, poll: float = 0.05) -> float:
    """Seconds until filename appears in the package JSON index."""
    t0 = time.perf_counter()
    deadline = time.time() + timeout
    url = f"{base_url}/simple/{pkg}/index.json"
    while time.time() < deadline:
        status, body, _ = http_get(url, timeout=10.0)
        if status == 200 and filename.encode() in body:
            return time.perf_counter() - t0
        time.sleep(poll)
    raise RuntimeError(f"{pkg}/{filename} not visible after {timeout}s")


# --------------------------------- RSS sampling --------------------------------


class RssSampler:
    """Polls `rss_cmd` (prints server RSS in KB) in a thread; tracks the peak."""

    def __init__(self, rss_cmd: Optional[str], interval: float = 0.5):
        self.rss_cmd = rss_cmd
        self.interval = interval
        self.peak_kb = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                out = subprocess.run(
                    shlex.split(self.rss_cmd), capture_output=True, text=True, timeout=10
                ).stdout.strip()
                if out:
                    self.peak_kb = max(self.peak_kb, int(out.split()[0]))
            except (subprocess.SubprocessError, ValueError, OSError):
                pass
            self._stop.wait(self.interval)

    def __enter__(self) -> "RssSampler":
        if self.rss_cmd:
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    @property
    def peak_mb(self) -> Optional[float]:
        return round(self.peak_kb / 1024.0, 1) if self.rss_cmd else None


# ----------------------------------- oha --------------------------------------


def run_oha(
    oha: str,
    url: str,
    duration: str,
    connections: int,
    headers: Optional[List[str]] = None,
    expect_status: int = 200,
) -> Dict:
    # -r 0: never follow redirects — a 302 scenario measures the 302, and
    # following presigned URLs at full blast exhausts loopback ephemeral ports.
    cmd = [oha, "--no-tui", "--output-format", "json", "-r", "0", "-z", duration, "-c", str(connections)]
    for h in headers or []:
        cmd += ["-H", h]
    cmd.append(url)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(f"oha failed: {proc.stderr[:500]}")
    data = json.loads(proc.stdout)
    summary = data.get("summary", {})
    pct = data.get("latencyPercentiles", {})
    statuses = data.get("statusCodeDistribution", {})
    total = sum(statuses.values()) or 1
    ok = statuses.get(str(expect_status), 0)

    def ms(key: str) -> float:
        v = pct.get(key)  # oha emits null percentiles when nothing completed
        return round(v * 1000, 2) if isinstance(v, (int, float)) else 0.0

    time.sleep(1.0)  # let the server settle between scenarios
    return {
        "rps": round(summary.get("requestsPerSec") or 0.0, 1),
        "p50_ms": ms("p50"),
        "p95_ms": ms("p95"),
        "p99_ms": ms("p99"),
        "status_ok_pct": round(100.0 * ok / total, 2),
        "statuses": statuses,
        "size_per_request": summary.get("sizePerRequest"),
        "total_data_bytes": summary.get("totalData"),
        "duration": duration,
        "connections": connections,
    }


# --------------------------------- Scenarios -----------------------------------


def percentile(values: List[float], p: float) -> float:
    vals = sorted(values)
    return vals[min(len(vals) - 1, int(len(vals) * p))]


def seed(args: argparse.Namespace) -> None:
    base, legacy = args.base_url.rstrip("/"), args.base_url.rstrip("/") + "/legacy/"
    wait_healthy(base)
    t0 = time.time()

    def upload_one(pkg: str, version: str, tag: str = "py3-none-any", size: int = 1024) -> str:
        fname = wheel_filename(pkg, version, tag)
        status, body = upload_wheel(
            legacy, fname, make_wheel_bytes(pkg, version, size), pkg, version, args.user, args.password
        )
        # 409 = already seeded (filename immutability); re-seeding is a no-op.
        if status not in (200, 409):
            raise RuntimeError(f"seed upload {fname}: {status} {body[:200]!r}")
        return fname

    print(f"seeding {SMALL_PKG} ({SMALL_FILES} files)...")
    last_small = ""
    for i in range(SMALL_FILES):
        last_small = upload_one(SMALL_PKG, f"1.{i}.0")

    n = args.torch_files
    print(f"seeding {TORCH_PKG} ({n} files)...")
    # torch-shaped: ~n/20 versions x 20 (python, platform) tag combos
    combos = [
        (f"cp3{minor}-cp3{minor}", plat)
        for minor in (9, 10, 11, 12)
        for plat in (
            "manylinux2014_x86_64",
            "manylinux2014_aarch64",
            "macosx_11_0_arm64",
            "win_amd64",
            "musllinux_1_2_x86_64",
        )
    ]
    jobs = []
    for i in range(n):
        version = f"2.{i // len(combos)}.0"
        py, plat = combos[i % len(combos)]
        jobs.append((version, f"{py.split('-')[0]}-{py.split('-')[1]}-{plat}"))
    last_torch = ""
    with ThreadPoolExecutor(max_workers=args.seed_concurrency) as pool:
        names = list(
            pool.map(lambda j: upload_one(TORCH_PKG, j[0], j[1]), jobs)
        )
        last_torch = names[-1]

    print("waiting for index visibility (worker catches up)...")
    wait_visible(base, SMALL_PKG, last_small, timeout=300)
    wait_visible(base, TORCH_PKG, last_torch, timeout=600, poll=1.0)
    print(f"seeded in {time.time() - t0:.0f}s")


def run(args: argparse.Namespace) -> None:
    base = args.base_url.rstrip("/")
    wait_healthy(base)
    results: Dict[str, Dict] = {}
    oha, dur, conns = args.oha, args.duration, args.connections

    def restart(mode: str) -> bool:
        if not args.restart_cmd:
            return False
        subprocess.run(shlex.split(args.restart_cmd) + [mode], check=True, timeout=180)
        wait_healthy(base)
        return True

    # Pick a torchsim artifact + capture the small-index ETag up front.
    status, body, _ = http_get(f"{base}/simple/{TORCH_PKG}/index.json")
    if status != 200:
        raise RuntimeError(f"corpus missing: /simple/{TORCH_PKG}/ -> {status}; run `meter.py seed` first")
    torch_file = json.loads(body)["files"][0]["filename"]
    status, _, hdrs = http_get(f"{base}/simple/{SMALL_PKG}/index.json")
    etag = next((v for k, v in hdrs.items() if k.lower() == "etag"), "")
    if not etag:
        raise RuntimeError("no ETag header on package index; R3 cannot run")

    print("R1: package index reads (small)")
    results["R1_json"] = run_oha(oha, f"{base}/simple/{SMALL_PKG}/index.json", dur, conns)
    results["R1_html"] = run_oha(oha, f"{base}/simple/{SMALL_PKG}/", dur, conns)

    print("R3: 304 revalidation")
    results["R3_304"] = run_oha(
        oha, f"{base}/simple/{SMALL_PKG}/index.json", dur, conns,
        headers=[f"If-None-Match: {etag}"], expect_status=304,
    )

    print("R2-lite: torch-shaped index read")
    results["R2_torch_idx"] = run_oha(oha, f"{base}/simple/{TORCH_PKG}/index.json", dur, conns)

    # In `auto` delivery (the customer default), only redirect-safe clients
    # (uv) get 302s — so R6 presents a uv User-Agent. Metadata always streams.
    print("R6: artifact 302 redirects (uv client)")
    results["R6_302"] = run_oha(
        oha, f"{base}/files/{TORCH_PKG}/{torch_file}", dur, conns,
        headers=["User-Agent: uv/0.7.0"], expect_status=302,
    )

    print("R7: PEP 658 metadata fetches")
    results["R7_metadata"] = run_oha(
        oha, f"{base}/files/{TORCH_PKG}/{torch_file}.metadata", dur, conns,
        headers=["User-Agent: uv/0.7.0"], expect_status=200,
    )

    print(f"W3: upload->visible latency x{W3_ITERATIONS}")
    legacy = f"{base}/legacy/"
    lat: List[float] = []
    for i in range(W3_ITERATIONS):
        v = f"0.{int(time.time())}.{i}"
        fname = wheel_filename(W3_PKG, v)
        s, b = upload_wheel(legacy, fname, make_wheel_bytes(W3_PKG, v, 1024), W3_PKG, v, args.user, args.password)
        if s != 200:
            raise RuntimeError(f"W3 upload failed: {s} {b[:200]!r}")
        lat.append(wait_visible(base, W3_PKG, fname))
    results["W3_visibility"] = {
        "iterations": W3_ITERATIONS,
        "p50_s": round(percentile(lat, 0.5), 3),
        "p99_s": round(percentile(lat, 0.99), 3),
        "max_s": round(max(lat), 3),
    }

    print(f"W1-meter: {W1_MB} MB upload")
    v = f"0.{int(time.time())}.0"
    fname = wheel_filename(W1_PKG, v)
    blob = make_wheel_bytes(W1_PKG, v, W1_MB << 20)
    with RssSampler(args.rss_cmd) as rss:
        t0 = time.perf_counter()
        s, b = upload_wheel(legacy, fname, blob, W1_PKG, v, args.user, args.password)
        wall = time.perf_counter() - t0
    if s != 200:
        raise RuntimeError(f"W1-meter upload failed: {s} {b[:200]!r}")
    results["W1_100mb"] = {"wall_s": round(wall, 2), "peak_rss_mb": rss.peak_mb, "size_mb": W1_MB}
    w1_file = fname

    if restart("sync"):
        print(f"W4: sync-upload round trip x{W4_ITERATIONS}")
        lat = []
        ryw_failures = 0
        for i in range(W4_ITERATIONS):
            v = f"0.{int(time.time())}.{i}"
            fname = wheel_filename(W4_PKG, v)
            t0 = time.perf_counter()
            s, b = upload_wheel(legacy, fname, make_wheel_bytes(W4_PKG, v, 1024), W4_PKG, v, args.user, args.password)
            lat.append(time.perf_counter() - t0)
            if s != 200:
                raise RuntimeError(f"W4 upload failed: {s} {b[:200]!r}")
            status, body, _ = http_get(f"{base}/simple/{W4_PKG}/index.json")
            if fname.encode() not in body:
                ryw_failures += 1
        results["W4_sync_upload"] = {
            "iterations": W4_ITERATIONS,
            "p50_s": round(percentile(lat, 0.5), 3),
            "p99_s": round(percentile(lat, 0.99), 3),
            "read_your_write_failures": ryw_failures,
        }
    else:
        results["W4_sync_upload"] = {"skipped": "no --restart-cmd"}

    if restart("proxy"):
        print("R5-lite: proxy-mode artifact download throughput")
        with RssSampler(args.rss_cmd) as rss:
            r = run_oha(oha, f"{base}/files/{W1_PKG}/{w1_file}", dur, min(conns, 8))
        gbps = 0.0
        if r.get("total_data_bytes"):
            secs = float(dur.rstrip("s"))
            gbps = round(r["total_data_bytes"] * 8 / secs / 1e9, 2)
        r["gbps"] = gbps
        r["peak_rss_mb"] = rss.peak_mb
        results["R5_proxy_download"] = r
    else:
        results["R5_proxy_download"] = {"skipped": "no --restart-cmd"}

    restart("default")

    if args.skip_torch_upload:
        results["W1_torch_900mb"] = {"skipped": "--skip-torch-upload"}
    else:
        print(f"W1-torch: {TORCH_UPLOAD_MB} MB upload (expected to fail on 2 GiB boxes until streaming)")
        v = f"0.{int(time.time())}.0"
        fname = wheel_filename(TORCH_UPLOAD_PKG, v)
        blob = make_wheel_bytes(TORCH_UPLOAD_PKG, v, TORCH_UPLOAD_MB << 20)
        with RssSampler(args.rss_cmd) as rss:
            t0 = time.perf_counter()
            try:
                s, b = upload_wheel(legacy, fname, blob, TORCH_UPLOAD_PKG, v, args.user, args.password, timeout=900)
                wall = time.perf_counter() - t0
                outcome = {"result": "PASS" if s == 200 else f"FAIL(status {s})", "wall_s": round(wall, 2)}
                if s != 200:
                    outcome["body"] = b[:200].decode("utf-8", "replace")
            except (urllib.error.URLError, ConnectionError, OSError, TimeoutError) as e:
                outcome = {"result": f"FAIL({type(e).__name__})", "wall_s": round(time.perf_counter() - t0, 2)}
        outcome["peak_rss_mb"] = rss.peak_mb
        outcome["size_mb"] = TORCH_UPLOAD_MB
        results["W1_torch_900mb"] = outcome
        del blob

    meta = {
        "date": time.strftime("%Y-%m-%d"),
        "commit": args.commit or git_commit(),
        "base_url": base,
        "rig": args.rig,
        "duration": dur,
        "connections": conns,
    }
    out = {"meta": meta, "results": results}
    if args.output:
        with open(args.output, "w") as f:
            json.dump(out, f, indent=2)
        print(f"wrote {args.output}")
    print_markdown(out)


def git_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, timeout=10
        ).stdout.strip() or "unknown"
    except (subprocess.SubprocessError, OSError):
        return "unknown"


def print_markdown(out: Dict) -> None:
    m, r = out["meta"], out["results"]

    def rps(key: str) -> str:
        v = r.get(key, {})
        return str(v.get("rps", "—")) if "rps" in v else "—"

    def torch_cell() -> str:
        t = r.get("W1_torch_900mb", {})
        if "skipped" in t:
            return "skipped"
        if t.get("result") == "PASS":
            return f"PASS {t['wall_s']}s"
        return f"{t.get('result', '—')}"

    w1 = r.get("W1_100mb", {})
    w1_cell = f"{w1.get('wall_s', '—')}s / {w1.get('peak_rss_mb', '—')}MB" if w1 else "—"
    w3 = r.get("W3_visibility", {})
    w4 = r.get("W4_sync_upload", {})
    w4_cell = f"{w4['p99_s']}s" if "p99_s" in w4 else "skipped"

    print("\n--- meter series row (append to docs/BENCHMARK_RESULTS.md) ---")
    print(
        f"| # | {m['date']} | `{m['commit']}` | {rps('R1_json')} | {rps('R3_304')} "
        f"| {rps('R2_torch_idx')} | {rps('R6_302')} | {rps('R7_metadata')} "
        f"| {w3.get('p99_s', '—')}s | {w4_cell} | {w1_cell} | {torch_cell()} |"
    )
    print("\n--- full detail ---")
    print(f"rig: {m['rig']}  base: {m['base_url']}  oha: -z {m['duration']} -c {m['connections']}")
    print("| Scenario | rps | p50 ms | p95 ms | p99 ms | ok% | notes |")
    print("|---|---|---|---|---|---|---|")
    for key, v in r.items():
        if "rps" in v:
            extra = []
            if v.get("gbps"):
                extra.append(f"{v['gbps']} Gbps")
            if v.get("peak_rss_mb") is not None:
                extra.append(f"RSS {v['peak_rss_mb']}MB")
            if v.get("note"):
                extra.append(v["note"])
            print(
                f"| {key} | {v['rps']} | {v['p50_ms']} | {v['p95_ms']} | {v['p99_ms']} "
                f"| {v['status_ok_pct']} | {'; '.join(extra)} |"
            )
        else:
            print(f"| {key} | — | — | — | — | — | {json.dumps(v)} |")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--base-url", default="http://127.0.0.1:8080")
    common.add_argument("--user", default=os.environ.get("PYPIRON_BASIC_AUTH_USER", "admin"))
    common.add_argument("--password", default=os.environ.get("PYPIRON_BASIC_AUTH_PASS", "secret"))

    s = sub.add_parser("seed", parents=[common])
    s.add_argument("--torch-files", type=int, default=TORCH_FILES)
    s.add_argument("--seed-concurrency", type=int, default=16)
    s.set_defaults(fn=seed)

    rn = sub.add_parser("run", parents=[common])
    rn.add_argument("--oha", default="oha")
    rn.add_argument("--duration", default="30s")
    rn.add_argument("--connections", type=int, default=64)
    rn.add_argument("--rig", default="local")
    rn.add_argument("--rss-cmd", default=None, help="command printing server RSS in KB")
    rn.add_argument("--restart-cmd", default=None, help="`<cmd> <default|sync|proxy>` restarts server in mode")
    rn.add_argument("--skip-torch-upload", action="store_true")
    rn.add_argument("--commit", default=None, help="commit hash override (loadgen copies have no .git)")
    rn.add_argument("--output", default=None, help="write results JSON here")
    rn.set_defaults(fn=run)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
