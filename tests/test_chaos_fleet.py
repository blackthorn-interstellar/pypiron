"""Multi-node convergence under an ungraceful kill.

The README claims you can "point any number of nodes at one bucket. No
coordination." This proves it survives a crash: three pypiron nodes share one
MinIO bucket, uploads and downloads run concurrently across all of them, one node
is SIGKILL'd mid-write, restarted, and the fleet must converge — every
successfully-acked upload installable from *every* node, the global index
identical across nodes, and the read-only oracle (`verify-index`) reporting no
divergence or orphaned state.

Disk is documented single-node (dev/DESIGN.md); multi-node is the cloud
backend's job, so this runs against MinIO like the other S3 chaos tests. Audit is
disabled so markers + lease election + CAS global writes do all the healing — the
same event-path-only rigor as test_crash_consistency.py.
"""

from __future__ import annotations

import hashlib
import os
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, Set, Tuple

import pytest

from .conftest import _s3_env
from .helpers import (
    ACCEPT_PEP691,
    find_free_port,
    http_get,
    http_get_json,
    kill_process_tree,
    make_wheel,
    sha256_file,
    upload_legacy,
    wait_http_responding,
)

pytestmark = [pytest.mark.integration, pytest.mark.s3, pytest.mark.chaos]

AUTH = {"username": "admin", "password": "secret"}

# Event-path-only: no audit, a short lease TTL so a killed leader's lock expires
# and a survivor takes over within a couple of worker ticks.
EVENT_ONLY_ENV = {
    "PYPIRON_AUDIT_ON_BOOT": "false",
    "PYPIRON_RECONCILE_INTERVAL_SECS": "100000",
    "PYPIRON_INTENT_GRACE_SECS": "1",
    "PYPIRON_LEASE_TTL_SECS": "2",
}

CONVERGE_BUDGET_SECS = 90.0
# An acked upload record: (name, wheel filename, sha256 of the bytes).
Ack = Tuple[str, str, str]


def _start_node(pypiron_bin: Path, minio: Dict, tmp_path: Path, label: str) -> Dict:
    port = find_free_port()
    env = _s3_env(minio, f"127.0.0.1:{port}")
    env.update(EVENT_ONLY_ENV)
    log_path = tmp_path / f"{label}.log"
    log = open(log_path, "w")
    proc = subprocess.Popen(
        [str(pypiron_bin), "serve"], env=env, stdout=log, stderr=subprocess.STDOUT
    )
    base = f"http://127.0.0.1:{port}"
    try:
        wait_http_responding(f"{base}/health", timeout=30)
    except TimeoutError:
        # A node may still be racing for the boot lease; callers proceed anyway.
        pass
    return {
        "proc": proc,
        "base": base,
        "simple": f"{base}/simple/",
        "legacy": f"{base}/legacy/",
        "label": label,
        "log_path": log_path,
    }


def _verify_converged(
    pypiron_bin: Path, minio: Dict, timeout: float
) -> subprocess.CompletedProcess:
    """Poll the read-only oracle until views == truth (or the deadline)."""
    env = _s3_env(minio, "unused")
    env.update(EVENT_ONLY_ENV)
    deadline = time.time() + timeout
    result = None
    while time.time() < deadline:
        result = subprocess.run(
            [str(pypiron_bin), "verify-index"],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )
        if result.returncode == 0:
            return result
        time.sleep(2)
    return result  # type: ignore[return-value]


def _global_names(node: Dict) -> Set[str]:
    data = http_get_json(
        f"{node['simple']}index.json", headers={"Accept": ACCEPT_PEP691}, timeout=15
    )
    return {p.get("name") for p in data.get("projects", [])}


def _wait_global_superset(node: Dict, names: Set[str], timeout: float) -> Set[str]:
    """Poll a node's global index until it lists at least `names`; return its set."""
    deadline = time.time() + timeout
    latest: Set[str] = set()
    while time.time() < deadline:
        try:
            latest = _global_names(node)
            if names <= latest:
                return latest
        except (RuntimeError, ConnectionError, OSError):
            pass
        time.sleep(0.5)
    missing = names - latest
    raise AssertionError(f"{node['label']} global index never listed: {sorted(missing)}")


def _wait_file_installable(node: Dict, name: str, filename: str, sha: str, timeout: float) -> None:
    """Poll until the node lists the file, then download it from that node and
    check the bytes — proof the artifact is serveable from every node."""
    deadline = time.time() + timeout
    listed = False
    while time.time() < deadline:
        try:
            data = http_get_json(
                f"{node['simple']}{name}/index.json", headers={"Accept": ACCEPT_PEP691}, timeout=15
            )
            if filename in [f.get("filename") for f in data.get("files", [])]:
                listed = True
                break
        except (RuntimeError, ConnectionError, OSError):
            pass
        time.sleep(0.5)
    assert listed, f"{node['label']} never listed {filename} for {name}"

    code, body, _ = http_get(f"{node['base']}/files/{name}/{filename}", timeout=30)
    assert code == 200, f"{node['label']} served {filename} as {code}"
    assert hashlib.sha256(body).hexdigest() == sha, f"{node['label']} served corrupt {filename}"


def test_fleet_converges_after_node_killed_mid_write(pypiron_bin, minio, tmp_path):
    nodes = [_start_node(pypiron_bin, minio, tmp_path, f"node-{i}") for i in range(3)]

    acked: Set[Ack] = set()
    acked_lock = threading.Lock()
    stop = threading.Event()
    wheels_dir = tmp_path / "wheels"
    wheels_dir.mkdir()

    def churn(node: Dict, prefix: str) -> None:
        i = 0
        while not stop.is_set():
            name = f"{prefix}-{i}"
            i += 1
            try:
                wheel = make_wheel(name, "1.0", wheels_dir)
            except OSError:
                continue
            try:
                upload_legacy(node["legacy"], wheel, **AUTH)  # raises unless 200
            except (RuntimeError, ConnectionError, OSError, TimeoutError):
                continue
            # Only a 200 counts: an acked upload's artifact is durable, so it must
            # be installable from every node after convergence.
            with acked_lock:
                acked.add((name, wheel.name, sha256_file(wheel)))
            time.sleep(0.15)

    threads = [
        threading.Thread(target=churn, args=(node, f"c{i}"), daemon=True)
        for i, node in enumerate(nodes)
    ]
    try:
        for t in threads:
            t.start()
        time.sleep(2.5)  # let uploads flow and indexes build across the fleet

        # Ungraceful death mid-write: no lease release, no flushed markers.
        victim = nodes[1]
        os.kill(victim["proc"].pid, signal.SIGKILL)
        time.sleep(1.5)  # survivors keep taking uploads while one node is gone
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=15)

    # Bring the killed node back so every node is available for the per-node
    # installability check below.
    kill_process_tree(victim["proc"])
    nodes[1] = _start_node(pypiron_bin, minio, tmp_path, "node-1-restart")

    try:
        with acked_lock:
            acks = sorted(acked)
        assert acks, "no uploads were acked — the test drove nothing"

        # 1. The oracle: storage views converge to recomputed-from-truth. This is
        #    also the orphaned-state detector (verify-index fails on any file
        #    listed-but-missing or index/truth divergence).
        result = _verify_converged(pypiron_bin, minio, CONVERGE_BUDGET_SECS)
        assert result is not None and result.returncode == 0, (
            "fleet never converged after the killed node restarted:\n"
            f"{result.stdout if result else ''}{result.stderr if result else ''}"
        )

        acked_names = {name for name, _, _ in acks}

        # 2. Every node lists every acked upload, and the global indexes agree.
        global_sets = [_wait_global_superset(node, acked_names, timeout=60) for node in nodes]
        assert global_sets[0] == global_sets[1] == global_sets[2], (
            "nodes disagree on the global index after convergence: "
            f"{[sorted(s ^ global_sets[0]) for s in global_sets]}"
        )

        # 3. A sample of acked uploads is installable (download + hash) from every
        #    node — cross-node artifact serving over the one shared bucket.
        sample = acks[:: max(1, len(acks) // 5)][:5]
        for node in nodes:
            for name, filename, sha in sample:
                _wait_file_installable(node, name, filename, sha, timeout=60)
    finally:
        for node in nodes:
            kill_process_tree(node["proc"])
