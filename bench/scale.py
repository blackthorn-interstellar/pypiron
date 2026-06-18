#!/usr/bin/env python3
"""Full-PyPI-scale risk probe: how does pypiron behave at 780k projects / 17M files?

Strategy: don't mirror PyPI — fabricate its *shape*. Truth in pypiron is just
artifacts plus sidecars, and nothing in the read/sweep path opens artifact
bytes when a sidecar exists. So a storage tree with 0-byte artifacts and real
sidecars exercises exactly the same code as a real mirror, at ~1/40,000th of
the bytes.

Realism comes from the corpus downloads in bench/corpus/ (see
src/corpus_check.rs for provenance):
  - real project names (all 779,934 of them),
  - real files-per-project distribution (median 4, p99 262, max 43,145),
sampled deterministically so tiers are comparable run to run.

Subcommands:
  seed  - fabricate a tree:    scale.py seed --packages 50000 --dest DIR
  run   - measure a tree:      scale.py run --data-dir DIR --bin PATH
Measurements per tier: cold reconcile sweep (restore-from-backup case),
steady-state sweep, global index size + latency, package index latency,
upload->visible while sweeping, server RSS. Results land in a JSON file for
cross-tier extrapolation.

Stdlib only, like meter.py.
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import io
import json
import os
import random
import re
import subprocess
import time
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
FILECOUNTS = REPO / "bench" / "corpus" / "pypi-project-filecounts.tsv.gz"


def normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower().strip("-")


# ---------------------------------- seed -----------------------------------


def load_projects() -> list[tuple[str, int]]:
    """(normalized name, file count) for every real PyPI project, deduped."""
    counts: dict[str, int] = {}
    with gzip.open(FILECOUNTS, "rt", encoding="utf-8") as fh:
        for line in fh:
            name, _, cnt = line.rstrip("\n").partition("\t")
            norm = normalize(name)
            if norm:
                counts[norm] = counts.get(norm, 0) + int(cnt)
    return sorted(counts.items())


def seed_package(pkg_dir: Path, name: str, n_files: int) -> int:
    pkg_dir.mkdir(parents=True, exist_ok=True)
    for i in range(n_files):
        version = f"0.{i}.0"
        filename = f"{name}-{version}.tar.gz"
        artifact = pkg_dir / filename
        artifact.touch()
        sidecar = {
            "sha256": hashlib.sha256(filename.encode()).hexdigest(),
            "size": 0,
            "version": version,
            "upload-time": f"2025-01-{(i % 28) + 1:02d}T00:00:00Z",
            "yanked": False,
        }
        (pkg_dir / f"{filename}.meta.json").write_text(json.dumps(sidecar))
    return n_files


def cmd_seed(args: argparse.Namespace) -> None:
    projects = load_projects()
    rng = random.Random(args.seed)
    sample = projects if args.packages >= len(projects) else rng.sample(projects, args.packages)
    total_files = sum(c for _, c in sample)
    print(f"seeding {len(sample):,} packages / {total_files:,} files -> {args.dest}")

    packages_root = Path(args.dest) / "packages"
    start = time.monotonic()
    done_files = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(seed_package, packages_root / name, name, count) for name, count in sample
        ]
        for i, fut in enumerate(futures, 1):
            done_files += fut.result()
            if i % 5000 == 0:
                rate = done_files / (time.monotonic() - start)
                print(f"  {i:,} pkgs, {done_files:,} files ({rate:,.0f} files/s)")
    elapsed = time.monotonic() - start
    print(f"seeded in {elapsed:,.1f}s ({total_files / elapsed:,.0f} files/s)")
    manifest = {
        "packages": len(sample),
        "files": total_files,
        "seed": args.seed,
        "sample_names": [n for n, _ in sample[:50]],
    }
    (Path(args.dest) / "scale-manifest.json").write_text(json.dumps(manifest, indent=2))


# ----------------------------------- run -----------------------------------


def http_get(url: str, timeout: float = 60.0) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"Accept-Encoding": "identity"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def time_get(url: str, n: int = 20) -> dict:
    """Latency stats (ms) over n GETs; body size from the first."""
    status, body = http_get(url)
    if status != 200:
        return {"status": status}
    times = []
    for _ in range(n):
        t0 = time.monotonic()
        http_get(url)
        times.append((time.monotonic() - t0) * 1000)
    times.sort()
    return {
        "status": status,
        "bytes": len(body),
        "p50_ms": round(times[len(times) // 2], 2),
        "max_ms": round(times[-1], 2),
    }


def metric_value(base: str, name: str) -> int:
    _, body = http_get(f"{base}/metrics")
    for line in body.decode().splitlines():
        if line.startswith(name + " "):
            return int(line.split()[1])
    return 0


def wait_metric_at_least(base: str, name: str, target: int, timeout: float) -> float:
    """Seconds until counter reaches target."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if metric_value(base, name) >= target:
            return time.monotonic() - t0
        time.sleep(0.2)
    raise TimeoutError(f"{name} never reached {target}")


def sweep_durations(log_path: Path) -> list[float]:
    """duration_secs of each completed reconcile sweep, from the server log."""
    out = []
    for line in log_path.read_text().splitlines():
        plain = re.sub(r"\x1b\[[0-9;]*m", "", line)
        if "reconcile: sweep complete" in plain:
            m = re.search(r"duration_secs=([0-9.]+)", plain)
            if m:
                out.append(float(m.group(1)))
    return out


def rss_mb(pid: int) -> float:
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)])
    return int(out.split()[0]) / 1024


def make_wheel(name: str, version: str) -> bytes:
    """Minimal valid wheel for the upload-visibility probe."""
    dist = name.replace("-", "_")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            f"{dist}-{version}.dist-info/METADATA",
            f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n",
        )
        zf.writestr(f"{dist}-{version}.dist-info/WHEEL", "Wheel-Version: 1.0\n")
    return buf.getvalue()


def upload_visibility_secs(base: str, user: str, password: str) -> float:
    """Upload one wheel, poll its package index until the file appears."""
    name = "scale-probe"
    version = f"0.0.{int(time.time())}"
    wheel = make_wheel(name, version)
    filename = f"{name.replace('-', '_')}-{version}-py3-none-any.whl"
    boundary = "scalebench"
    parts = []
    for field, value in [
        (":action", "file_upload"),
        ("protocol_version", "1"),
        ("name", name),
        ("version", version),
        ("filetype", "bdist_wheel"),
        ("sha256_digest", hashlib.sha256(wheel).hexdigest()),
    ]:
        parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{field}"\r\n\r\n{value}\r\n'.encode()
        )
    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="content"; filename="{filename}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n".encode()
        + wheel
        + b"\r\n"
    )
    parts.append(f"--{boundary}--\r\n".encode())
    body = b"".join(parts)
    req = urllib.request.Request(
        f"{base}/legacy/",
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Authorization": "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode(),
        },
    )
    t0 = time.monotonic()
    with urllib.request.urlopen(req, timeout=60) as resp:
        assert resp.status in (200, 201), resp.status
    while time.monotonic() - t0 < 120:
        status, idx = http_get(f"{base}/simple/scale-probe/index.json")
        if status == 200 and filename in idx.decode():
            return time.monotonic() - t0
        time.sleep(0.05)
    raise TimeoutError("upload never became visible")


def cmd_run(args: argparse.Namespace) -> None:
    data_dir = Path(args.data_dir)
    manifest = json.loads((data_dir / "scale-manifest.json").read_text())
    port = args.port
    base = f"http://127.0.0.1:{port}"
    server_args = [
        args.bin,
        "--bind-addr",
        f"127.0.0.1:{port}",
        "--data-dir",
        str(data_dir),
        "--admin-user",
        "admin",
        "--admin-pass",
        "secret",
        "--worker-interval-secs",
        "1",
        "--reconcile-interval-secs",
        str(args.reconcile_interval),
    ]
    env = dict(os.environ, RUST_LOG="info,pypiron=debug")
    results: dict = {"manifest": manifest}

    def boot_and_wait_sweep(log_path: Path) -> subprocess.Popen:
        log_file = open(log_path, "w")
        proc = subprocess.Popen(server_args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 30
        while True:
            try:
                http_get(f"{base}/health", timeout=2)
                break
            except (urllib.error.URLError, OSError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.1)
        wait_metric_at_least(base, "pypiron_reconcile_sweeps_total", 1, args.sweep_timeout)
        return proc

    def stop(proc: subprocess.Popen) -> None:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()

    # Boot 1: cold — fresh tree, no fingerprints, every view + fingerprint
    # written (the restore-from-backup case).
    cold_log = data_dir / "scale-cold.log"
    proc = boot_and_wait_sweep(cold_log)
    try:
        results["rss_after_cold_mb"] = round(rss_mb(proc.pid), 1)
        results["cold_sweep_secs"] = round(sweep_durations(cold_log)[0], 1)
    finally:
        stop(proc)

    # Boot 2: steady — fingerprints exist, nothing changed; the boot audit is
    # the crash-restart cost: listing-bound, zero sidecar reads, zero writes.
    steady_log = data_dir / "scale-steady.log"
    proc = boot_and_wait_sweep(steady_log)
    try:
        results["steady_sweep_secs"] = round(sweep_durations(steady_log)[0], 1)

        # Read-side latencies on materialized views.
        results["global_index_json"] = time_get(f"{base}/simple/index.json")
        results["global_index_html"] = time_get(f"{base}/simple/")
        pkg = manifest["sample_names"][0]
        results["package_index_json"] = time_get(f"{base}/simple/{pkg}/index.json")

        # Write visibility while the corpus is at full size.
        results["upload_visible_secs"] = round(upload_visibility_secs(base, "admin", "secret"), 2)
        results["rss_final_mb"] = round(rss_mb(proc.pid), 1)
    finally:
        stop(proc)

    out = REPO / "bench" / "corpus" / f"scale-{manifest['packages']}.json"
    out.write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))
    print(f"\nwrote {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("seed", help="fabricate a storage tree at scale")
    s.add_argument("--packages", type=int, required=True)
    s.add_argument("--dest", required=True)
    s.add_argument("--seed", type=int, default=42)
    s.add_argument("--workers", type=int, default=8)
    s.set_defaults(func=cmd_seed)

    r = sub.add_parser("run", help="measure a seeded tree")
    r.add_argument("--data-dir", required=True)
    r.add_argument("--bin", default=str(REPO / "target" / "release" / "pypiron"))
    r.add_argument("--port", type=int, default=18080)
    r.add_argument("--reconcile-interval", type=int, default=5)
    r.add_argument("--sweep-timeout", type=float, default=3600)
    r.set_defaults(func=cmd_run)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
