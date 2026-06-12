#!/usr/bin/env python3
"""Measure upload->visible WHILE a steady (daily-kind) audit runs.

The corpus + fingerprints are already in the bucket, so the boot audit is the
cheap steady kind (LISTs only, rebuilt=0). Upload a fresh package as that audit
runs and time how long until its index is visible. Stdlib only.

Usage: s3_upload_during_steady.py <bin> <bucket> <region> <probe-name>
"""
from __future__ import annotations

import base64
import hashlib
import os
import re
import socket
import subprocess
import sys
import time
import uuid
import zipfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BIN, BUCKET, REGION, PROBE = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
WORK = Path("/tmp/steady-probe")
WORK.mkdir(exist_ok=True)


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


def get(url, timeout=10):
    try:
        return urlopen(url, timeout=timeout).read()
    except (URLError, HTTPError, OSError):
        return b""


def metric(base, name):
    m = re.search(rf"^{re.escape(name)} ([\d.eE+-]+)$", get(f"{base}/metrics").decode(), re.M)
    return float(m.group(1)) if m else 0.0


def make_wheel(name, version):
    path = WORK / f"{name}-{version}-py3-none-any.whl"
    di = f"{name}-{version}.dist-info"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{name}.py", "x=1\n")
        zf.writestr(f"{di}/METADATA", f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
        zf.writestr(f"{di}/WHEEL", "Wheel-Version: 1.0\nGenerator: m\nRoot-Is-Purelib: true\nTag: py3-none-any\n")
        zf.writestr(f"{di}/RECORD", "")
    return path


def upload(base, wheel):
    data = wheel.read_bytes()
    name, version = wheel.name.split("-")[0], wheel.name.split("-")[1]
    form = {":action": "file_upload", "protocol_version": "1", "name": name, "version": version,
            "sha256_digest": hashlib.sha256(data).hexdigest()}
    b = f"----{uuid.uuid4().hex}"
    parts = [f"--{b}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode() for k, v in form.items()]
    parts.append(f"--{b}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"{wheel.name}\"\r\n"
                 "Content-Type: application/octet-stream\r\n\r\n".encode())
    parts.append(data); parts.append(f"\r\n--{b}--\r\n".encode())
    req = Request(f"{base}/legacy/", data=b"".join(parts), method="POST",
                  headers={"Content-Type": f"multipart/form-data; boundary={b}",
                           "Authorization": "Basic " + base64.b64encode(b"admin:secret").decode()})
    urlopen(req, timeout=30).read()


def main():
    port = free_port()
    env = dict(os.environ, PYPIRON_STORAGE="s3", PYPIRON_S3_BUCKET=BUCKET, AWS_REGION=REGION,
               PYPIRON_BIND_ADDR=f"127.0.0.1:{port}", PYPIRON_WORKER_INTERVAL_SECS="1",
               PYPIRON_ADMIN_USER="admin", PYPIRON_ADMIN_PASS="secret",
               PYPIRON_AUDIT_ON_BOOT="true", PYPIRON_RECONCILE_INTERVAL_SECS="100000",
               PYPIRON_SPOOL_DIR="/home/ec2-user/spool", RUST_LOG="info,pypiron=warn")
    log = open(WORK / "steady.log", "w")
    proc = subprocess.Popen([BIN, "serve"], env=env, stdout=log, stderr=subprocess.STDOUT)
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 30
        while time.time() < deadline and not get(f"{base}/health", 2):
            time.sleep(0.1)
        # Upload immediately so the probe overlaps the boot (steady) audit.
        wheel = make_wheel(PROBE, "1.0")
        t = time.time()
        upload(base, wheel)
        audits_at_upload = metric(base, "pypiron_reconcile_sweeps_total")
        visible = None
        dl = time.time() + 60
        while time.time() < dl:
            if f"{PROBE}-1.0".encode() in get(f"{base}/simple/{PROBE}/index.json"):
                visible = round(time.time() - t, 2)
                break
            time.sleep(0.1)
        audits_after = metric(base, "pypiron_reconcile_sweeps_total")
        print(f"upload_visible_during_steady_audit_secs={visible}")
        print(f"reconcile_sweeps at_upload={audits_at_upload} after={audits_after} "
              f"(>=1 sweep overlapped the visibility window)")
        print(f"audit_last_duration_seconds={metric(base, 'pypiron_audit_last_duration_seconds')}")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    main()
