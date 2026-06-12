#!/usr/bin/env python3
"""Measure the event-driven audit against a REAL S3 corpus, in-region.

Seeds a fabricated full-PyPI-shaped corpus (0-byte artifacts + real sidecars,
realistic files-per-project distribution; same RNG as the disk tiers) into a
real bucket, then measures:

  - cold audit  : boot against fresh truth, no fingerprints -> rebuild every view
                  (the restore-from-backup case). rebuilt == packages.
  - steady audit: reboot; fingerprints match -> rebuilt == 0, listing-bound.
  - LIST count / cost: derived from the live object count (1,000 keys per page).
  - upload -> visible WHILE the (cold) audit runs: the event path is not starved.

Self-contained (stdlib + scale.py seed + aws CLI). Credentials come from the
loadgen instance profile. Usage:

  s3_scale_measure.py <pypiron-bin> <bucket> <region> [packages]
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

HERE = Path(__file__).resolve().parent
BIN, BUCKET, REGION = sys.argv[1], sys.argv[2], sys.argv[3]
PACKAGES = int(sys.argv[4]) if len(sys.argv) > 4 else 5000
TREE = Path("/tmp/scale-tree")


def sh(cmd: str, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, text=True, capture_output=True, **kw)


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def basic_auth() -> str:
    return "Basic " + base64.b64encode(b"admin:secret").decode()


def get(url: str, timeout: float = 10.0) -> bytes:
    try:
        return urlopen(url, timeout=timeout).read()
    except (URLError, HTTPError, OSError):
        return b""


def metric(base: str, name: str) -> float:
    m = re.search(rf"^{re.escape(name)} ([\d.eE+-]+)$", get(f"{base}/metrics").decode(), re.M)
    return float(m.group(1)) if m else 0.0


def make_wheel(name: str, version: str) -> Path:
    path = TREE.parent / f"{name}-{version}-py3-none-any.whl"
    di = f"{name}-{version}.dist-info"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{name}.py", f'__version__ = "{version}"\n')
        zf.writestr(f"{di}/METADATA", f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
        zf.writestr(f"{di}/WHEEL", "Wheel-Version: 1.0\nGenerator: m\nRoot-Is-Purelib: true\nTag: py3-none-any\n")
        zf.writestr(f"{di}/RECORD", "")
    return path


def upload(base: str, wheel: Path) -> None:
    data = wheel.read_bytes()
    name, version = wheel.name.split("-")[0], wheel.name.split("-")[1]
    form = {
        ":action": "file_upload",
        "protocol_version": "1",
        "name": name,
        "version": version,
        "sha256_digest": hashlib.sha256(data).hexdigest(),
    }
    boundary = f"----{uuid.uuid4().hex}"
    parts = []
    for k, v in form.items():
        parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode())
    parts.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"{wheel.name}\"\r\n"
        "Content-Type: application/octet-stream\r\n\r\n".encode()
    )
    parts.append(data)
    parts.append(f"\r\n--{boundary}--\r\n".encode())
    req = Request(
        f"{base}/legacy/",
        data=b"".join(parts),
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}", "Authorization": basic_auth()},
    )
    urlopen(req, timeout=30).read()


def server_env(port: int, audit_on_boot: bool, reconcile: int) -> dict:
    return dict(
        os.environ,
        PYPIRON_STORAGE="s3",
        PYPIRON_S3_BUCKET=BUCKET,
        AWS_REGION=REGION,
        PYPIRON_BIND_ADDR=f"127.0.0.1:{port}",
        PYPIRON_WORKER_INTERVAL_SECS="1",
        PYPIRON_ADMIN_USER="admin",
        PYPIRON_ADMIN_PASS="secret",
        PYPIRON_AUDIT_ON_BOOT="true" if audit_on_boot else "false",
        PYPIRON_RECONCILE_INTERVAL_SECS=str(reconcile),
        PYPIRON_SPOOL_DIR="/home/ec2-user/spool",
        RUST_LOG="info,pypiron=debug",
    )


def boot(label: str, audit_on_boot: bool, reconcile: int) -> tuple:
    port = free_port()
    logp = TREE.parent / f"{label}.log"
    log = open(logp, "w")
    proc = subprocess.Popen([BIN, "serve"], env=server_env(port, audit_on_boot, reconcile), stdout=log, stderr=subprocess.STDOUT)
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 30
    while time.time() < deadline:
        if get(f"{base}/health", timeout=2):
            break
        time.sleep(0.2)
    return proc, base, logp


def wait_sweep(base: str, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if metric(base, "pypiron_reconcile_sweeps_total") >= 1:
            return True
        time.sleep(0.5)
    return False


def sweep_line(logp: Path) -> dict:
    for line in reversed(logp.read_text(errors="replace").splitlines()):
        if "reconcile: sweep complete" in line:
            return {
                k: float(m.group(1))
                for k, m in {
                    "duration_secs": re.search(r"duration_secs=([\d.]+)", line),
                    "rebuilt": re.search(r"rebuilt=(\d+)", line),
                    "skipped": re.search(r"skipped=(\d+)", line),
                    "packages": re.search(r"packages=(\d+)", line),
                }.items()
                if m
            }
    return {}


def stop(proc) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()


def count_objects(prefix: str) -> int:
    out = sh(f"aws s3 ls s3://{BUCKET}/{prefix} --recursive --summarize | tail -3")
    m = re.search(r"Total Objects:\s*(\d+)", out.stdout)
    return int(m.group(1)) if m else -1


def main() -> int:
    results: dict = {"packages_requested": PACKAGES}

    # 1. Seed truth (idempotent: skip if the tree already exists).
    if not (TREE / "scale-manifest.json").exists():
        print(f"== seeding {PACKAGES} packages -> {TREE}", flush=True)
        r = sh(f"python3 {HERE}/scale.py seed --packages {PACKAGES} --dest {TREE} --workers 16")
        print(r.stdout[-500:], r.stderr[-500:])
    manifest = json.loads((TREE / "scale-manifest.json").read_text())
    results["manifest"] = {"packages": manifest["packages"], "files": manifest["files"]}

    # 2. Push to S3 (empty bucket first so the audit sees only this corpus).
    print("== emptying bucket + syncing tree to S3", flush=True)
    sh(f"aws s3 rm s3://{BUCKET}/ --recursive --only-show-errors")
    t0 = time.time()
    sync = sh(f"aws s3 sync {TREE}/ s3://{BUCKET}/ --only-show-errors")
    results["seed_sync_secs"] = round(time.time() - t0, 1)
    if sync.returncode != 0:
        print("sync FAILED:", sync.stderr[-1000:])
        return 1

    pkg_objs = count_objects("packages/")
    results["packages_objects_seeded"] = pkg_objs
    print(f"== seeded {pkg_objs} objects under packages/ in {results['seed_sync_secs']}s", flush=True)

    # 3. Cold audit (rebuild everything) + upload->visible while it runs.
    print("== cold audit (rebuild-everything) + concurrent upload", flush=True)
    proc, base, logp = boot("cold", audit_on_boot=True, reconcile=100000)
    probe_visible = {"secs": None}

    def probe() -> None:
        time.sleep(1.0)  # let the cold audit get underway first
        wheel = make_wheel("auditoverlapprobe", "1.0")
        t = time.time()
        try:
            upload(base, wheel)
        except (URLError, HTTPError, OSError) as e:
            probe_visible["err"] = str(e)
            return
        deadline = time.time() + 60
        while time.time() < deadline:
            doc = get(f"{base}/simple/auditoverlapprobe/index.json")
            if b"auditoverlapprobe-1.0" in doc:
                probe_visible["secs"] = round(time.time() - t, 2)
                return
            time.sleep(0.2)

    th = threading.Thread(target=probe)
    th.start()
    try:
        assert wait_sweep(base, 600), "cold audit never completed"
        results["cold_audit"] = sweep_line(logp)
        results["cold_audit"]["metric_rebuilt"] = metric(base, "pypiron_audit_packages_rebuilt_total")
        results["cold_audit"]["metric_skipped"] = metric(base, "pypiron_audit_packages_skipped_total")
        results["cold_audit"]["metric_last_duration_s"] = metric(base, "pypiron_audit_last_duration_seconds")
        th.join(timeout=70)
        results["upload_visible_during_audit_secs"] = probe_visible["secs"]
        sim_objs = count_objects("simple/")
        results["simple_objects_after_cold"] = sim_objs
    finally:
        stop(proc)

    # 4. Steady audit (fingerprints match -> rebuilt == 0).
    print("== steady audit (fingerprint hits, zero reads)", flush=True)
    proc, base, logp = boot("steady", audit_on_boot=True, reconcile=100000)
    try:
        assert wait_sweep(base, 600), "steady audit never completed"
        results["steady_audit"] = sweep_line(logp)
        results["steady_audit"]["metric_rebuilt"] = metric(base, "pypiron_audit_packages_rebuilt_total")
        results["steady_audit"]["metric_skipped"] = metric(base, "pypiron_audit_packages_skipped_total")
        results["steady_audit"]["metric_last_duration_s"] = metric(base, "pypiron_audit_last_duration_seconds")
    finally:
        stop(proc)

    # 5. LIST request count + cost, derived from live object counts. The audit
    #    flat-lists packages/<shard> and simple/<shard> across 36 shards, 1,000
    #    keys per page; the per-shard ceiling adds up to ~36 partial pages each.
    pkg_objs = count_objects("packages/")
    sim_objs = count_objects("simple/")
    list_pages = (pkg_objs + 999) // 1000 + (sim_objs + 999) // 1000 + 2 * len(
        "0123456789abcdefghijklmnopqrstuvwxyz"
    )
    results["live_objects"] = {"packages": pkg_objs, "simple": sim_objs, "total": pkg_objs + sim_objs}
    results["audit_list_requests_est"] = list_pages
    results["audit_list_cost_usd"] = round(list_pages / 1000 * 0.005, 4)

    out = HERE / "corpus" / f"s3-scale-{manifest['packages']}.json"
    out.write_text(json.dumps(results, indent=2))
    print("\n" + json.dumps(results, indent=2))
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
