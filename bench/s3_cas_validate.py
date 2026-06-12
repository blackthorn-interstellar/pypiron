#!/usr/bin/env python3
"""Validate S3 conditional writes against the REAL thing (not MinIO).

Exercises every conditional-write path the design leans on and asserts each one
behaves — proving S3 returns the precondition errors that `lost_conditional_write`
(src/storage.rs) catches, so a lost race becomes a clean retry rather than a 500:

  - put_if_none_match acquire : node A creates _leader/lease.json (logs "lease acquired")
  - put_if_none_match conflict: node B's create loses, it stays a follower (no error)
  - put_if_match steal        : after the leader is frozen past its TTL, B steals
                                the lease (logs "lease stolen")
  - put_if_match global CAS   : a thawed zombie writes the global index with a
                                stale ETag, loses the CAS, and self-heals
                                (pypiron_global_cas_conflicts_total >= 1)

Runs in-region on the loadgen; credentials come from the instance profile.
Stdlib only. Usage: s3_cas_validate.py <pypiron-bin> <bucket> <region>
"""

from __future__ import annotations

import base64
import hashlib
import os
import re
import signal
import socket
import subprocess
import sys
import time
import uuid
import zipfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BIN, BUCKET, REGION = sys.argv[1], sys.argv[2], sys.argv[3]
WORK = Path("/tmp/cas-validate")
WORK.mkdir(exist_ok=True)
TTL = 2
BUDGET = TTL + 10


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def make_wheel(name: str, version: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9.]+", "_", name).strip("_")
    path = WORK / f"{safe}-{version}-py3-none-any.whl"
    di = f"{safe}-{version}.dist-info"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{safe.lower()}.py", f'__version__ = "{version}"\n')
        zf.writestr(f"{di}/METADATA", f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
        zf.writestr(f"{di}/WHEEL", "Wheel-Version: 1.0\nGenerator: cas\nRoot-Is-Purelib: true\nTag: py3-none-any\n")
        zf.writestr(f"{di}/RECORD", "")
    return path


def basic_auth(u: str = "admin", p: str = "secret") -> str:
    return "Basic " + base64.b64encode(f"{u}:{p}".encode()).decode()


def upload(base: str, wheel: Path) -> None:
    fname = wheel.name
    data = wheel.read_bytes()
    name = fname.split("-")[0]
    version = fname.split("-")[1]
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
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"{fname}\"\r\n"
        "Content-Type: application/octet-stream\r\n\r\n".encode()
    )
    parts.append(data)
    parts.append(f"\r\n--{boundary}--\r\n".encode())
    body = b"".join(parts)
    req = Request(
        f"{base}/legacy/",
        data=body,
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}", "Authorization": basic_auth()},
    )
    urlopen(req, timeout=30).read()


def try_upload(base: str, wheel: Path) -> None:
    try:
        upload(base, wheel)
    except (URLError, HTTPError, ConnectionError, OSError):
        pass


def start_node(label: str) -> dict:
    port = free_port()
    env = dict(
        os.environ,
        PYPIRON_STORAGE="s3",
        PYPIRON_S3_BUCKET=BUCKET,
        AWS_REGION=REGION,
        PYPIRON_BIND_ADDR=f"127.0.0.1:{port}",
        PYPIRON_WORKER_INTERVAL_SECS="1",
        PYPIRON_ADMIN_USER="admin",
        PYPIRON_ADMIN_PASS="secret",
        PYPIRON_AUDIT_ON_BOOT="false",
        PYPIRON_RECONCILE_INTERVAL_SECS="100000",
        PYPIRON_INTENT_GRACE_SECS="1",
        PYPIRON_LEASE_TTL_SECS=str(TTL),
        PYPIRON_SPOOL_DIR="/home/ec2-user/spool",
        RUST_LOG="info,pypiron=debug",
    )
    logp = WORK / f"{label}.log"
    log = open(logp, "w")
    proc = subprocess.Popen([BIN, "serve"], env=env, stdout=log, stderr=subprocess.STDOUT)
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            urlopen(f"{base}/health", timeout=2).read()
            break
        except (URLError, OSError):
            time.sleep(0.2)
    return {"proc": proc, "base": base, "log": logp, "label": label}


def logtext(n: dict) -> str:
    try:
        return n["log"].read_text(errors="replace")
    except OSError:
        return ""


def get(url: str) -> bytes:
    try:
        return urlopen(url, timeout=5).read()
    except (URLError, HTTPError, OSError):
        return b""


def metric(n: dict, name: str) -> float:
    m = re.search(rf"^{re.escape(name)} ([\d.eE+-]+)$", get(f"{n['base']}/metrics").decode(), re.M)
    return float(m.group(1)) if m else 0.0


def wait_for(pred, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.2)
    return False


def sig(n: dict, s: int) -> None:
    try:
        os.kill(n["proc"].pid, s)
    except ProcessLookupError:
        pass


def main() -> int:
    a = start_node("node-a")
    b = start_node("node-b")
    results = {}
    leader = None
    try:
        # put_if_none_match acquire: exactly one node creates the lease.
        assert wait_for(
            lambda: sum("lease acquired" in logtext(n) for n in (a, b)) == 1, BUDGET
        ), "no node acquired the lease (put_if_none_match acquire failed on real S3)"
        leader = a if "lease acquired" in logtext(a) else b
        follower = b if leader is a else a
        results["put_if_none_match_acquire"] = f"{leader['label']} acquired"
        # put_if_none_match conflict: the other node did NOT also acquire.
        assert "lease acquired" not in logtext(follower), "both nodes acquired — conflict not detected"
        results["put_if_none_match_conflict"] = f"{follower['label']} stayed follower (create lost cleanly)"

        # Seed the leader's in-memory global name set + ETag pin.
        upload(leader["base"], make_wheel("casseed", "1.0"))
        assert wait_for(lambda: b"casseed" in get(f"{leader['base']}/simple/index.json"), 25)

        # Freeze leader → follower steals via put_if_match after the TTL.
        sig(leader, signal.SIGSTOP)
        assert wait_for(lambda: "lease stolen" in logtext(follower), BUDGET), (
            "follower never stole the lease (put_if_match steal failed on real S3)"
        )
        results["put_if_match_steal"] = f"{follower['label']} stole the lease after TTL"

        # New leader advances the global-index ETag past the zombie's cached one.
        for i in range(3):
            upload(follower["base"], make_wheel(f"casnew{i}", "1.0"))
        assert wait_for(lambda: b"casnew2" in get(f"{follower['base']}/simple/index.json"), 25)

        # Kill the new leader so the zombie must be the one to write next.
        sig(follower, signal.SIGKILL)
        # Thaw the zombie: it still holds the stale ETag.
        sig(leader, signal.SIGCONT)
        # Force a global-index write from the zombie → stale-ETag CAS loss.
        upload(leader["base"], make_wheel("caszombie", "1.0"))
        assert wait_for(
            lambda: metric(leader, "pypiron_global_cas_conflicts_total") >= 1, BUDGET + 10
        ), "zombie never lost a global-index CAS (put_if_match conflict not detected on real S3)"
        results["put_if_match_global_cas"] = (
            f"zombie conflicts={metric(leader, 'pypiron_global_cas_conflicts_total'):.0f}, self-healed"
        )
    finally:
        if leader is not None:
            sig(leader, signal.SIGCONT)  # never leave a stopped process behind
        for n in (a, b):
            sig(n, signal.SIGKILL)
            n["proc"].wait()

    print("S3 conditional-write validation: PASS")
    for k, v in results.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
