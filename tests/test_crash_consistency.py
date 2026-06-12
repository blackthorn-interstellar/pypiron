"""Crash-point chaos: prove the event protocol alone converges.

Every scenario runs with the audit disabled entirely (--audit-on-boot false,
interval effectively never): markers + idempotent rebuilds must do all the
healing. The fault server aborts the whole process immediately before its
Nth mutating storage operation (PYPIRON_FAULT_ABORT_AFTER_WRITES, see
storage.rs); sweeping N over a scenario's write count crashes in every gap
of the write protocol. After a clean restart and marker drain, `pypiron
verify` must find zero divergence.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Optional, Tuple
from urllib.error import HTTPError, URLError

import pytest

from .helpers import (
    find_free_port,
    kill_process_tree,
    make_wheel,
    upload_legacy,
    wait_http_responding,
)

# No audit, fast ticks, 1s intent grace so crashed writers heal quickly.
EVENT_ONLY_ARGS = [
    "--audit-on-boot",
    "false",
    "--reconcile-interval-secs",
    "100000",
    "--worker-interval-secs",
    "1",
    "--intent-grace-secs",
    "1",
    "--admin-user",
    "admin",
    "--admin-pass",
    "secret",
]
AUTH = {"username": "admin", "password": "secret"}


def start_server(
    bin_path: Path,
    data_dir: Path,
    *,
    fault_after: Optional[int] = None,
    boot_timeout: float = 3.0,
) -> Tuple[subprocess.Popen, str]:
    port = find_free_port()
    env = os.environ.copy()
    env.pop("PYPIRON_FAULT_ABORT_AFTER_WRITES", None)
    if fault_after is not None:
        env["PYPIRON_FAULT_ABORT_AFTER_WRITES"] = str(fault_after)
    env.setdefault("RUST_LOG", "info,pypiron=debug")
    args = [
        str(bin_path),
        "--bind-addr",
        f"127.0.0.1:{port}",
        "--data-dir",
        str(data_dir),
        *EVENT_ONLY_ARGS,
    ]
    log = open(data_dir.parent / f"{data_dir.name}-server.log", "a")
    proc = subprocess.Popen(args, env=env, stdout=log, stderr=subprocess.STDOUT)
    base = f"http://127.0.0.1:{port}"
    try:
        wait_http_responding(f"{base}/health", timeout=boot_timeout)
    except Exception:
        # A fault server may abort during boot writes; the caller proceeds to
        # recovery either way.
        pass
    return proc, base


def drain_markers(data_dir: Path, *, timeout: float = 25.0) -> None:
    """Wait until _dirty/ stays empty (grace-stale intents included)."""
    dirty = data_dir / "_dirty"
    deadline = time.time() + timeout
    clear_since = None
    while time.time() < deadline:
        empty = not dirty.exists() or not any(dirty.iterdir())
        if empty:
            if clear_since is None:
                clear_since = time.time()
            elif time.time() - clear_since >= 2.0:
                return
        else:
            clear_since = None
        time.sleep(0.2)
    leftovers = list(dirty.iterdir()) if dirty.exists() else []
    raise AssertionError(f"dirty markers never drained: {leftovers}")


def run_verify(bin_path: Path, data_dir: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(bin_path), "verify", "--data-dir", str(data_dir)],
        capture_output=True,
        text=True,
        timeout=60,
    )


def assert_converged(bin_path: Path, data_dir: Path, context: str) -> None:
    """Start a clean server, drain every pending event, verify storage."""
    proc, _ = start_server(bin_path, data_dir, boot_timeout=15.0)
    try:
        drain_markers(data_dir)
    finally:
        kill_process_tree(proc)
    result = run_verify(bin_path, data_dir)
    log = (data_dir.parent / f"{data_dir.name}-server.log").read_text()
    assert result.returncode == 0, (
        f"divergence after {context}:\n{result.stdout}{result.stderr}\n"
        f"--- server log ---\n{log[-4000:]}"
    )


def attempt(callable_, *args, **kwargs):
    """Run a client call against a server that may abort mid-request."""
    try:
        callable_(*args, **kwargs)
    except (URLError, HTTPError, ConnectionError, RuntimeError, TimeoutError, OSError):
        pass


@pytest.mark.chaos
def test_event_path_converges_without_audit(pypiron_bin: Path, tmp_path: Path) -> None:
    """No-fault sanity: upload/yank/delete drain to a verified-clean tree
    with the audit disabled — the event path is doing all the work."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    wheel_a = make_wheel("chaos-alpha", "1.0", tmp_path)
    wheel_b = make_wheel("chaos-beta", "1.0", tmp_path)

    proc, base = start_server(pypiron_bin, data_dir, boot_timeout=15.0)
    try:
        upload_legacy(f"{base}/legacy/", wheel_a, **AUTH)
        upload_legacy(f"{base}/legacy/", wheel_b, **AUTH)
        # Yank one, delete the other — every mutation kind in one pass.
        import urllib.request

        req = urllib.request.Request(
            f"{base}/files/chaos-alpha/{wheel_a.name}/yank",
            data=b"security",
            method="POST",
            headers={"Authorization": _basic_auth()},
        )
        urllib.request.urlopen(req, timeout=10)
        req = urllib.request.Request(
            f"{base}/files/chaos-beta/{wheel_b.name}",
            method="DELETE",
            headers={"Authorization": _basic_auth()},
        )
        urllib.request.urlopen(req, timeout=10)
        drain_markers(data_dir)
    finally:
        kill_process_tree(proc)

    result = run_verify(pypiron_bin, data_dir)
    assert result.returncode == 0, result.stdout + result.stderr


def _basic_auth() -> str:
    import base64

    return "Basic " + base64.b64encode(b"admin:secret").decode()


@pytest.mark.chaos
@pytest.mark.parametrize("kill_point", range(1, 16))
def test_crash_during_upload_converges(
    pypiron_bin: Path, tmp_path: Path, kill_point: int
) -> None:
    """Kill the process before the Nth write during a fresh-package upload
    (covers boot init, origin claim, intent, artifact, companion, sidecar,
    commit, and the worker's view/global writes)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    wheel = make_wheel("chaos-upload", "1.0", tmp_path)

    proc, base = start_server(pypiron_bin, data_dir, fault_after=kill_point)
    attempt(upload_legacy, f"{base}/legacy/", wheel, **AUTH)
    # Give the nudged worker a moment to walk into the kill point too.
    time.sleep(1.5)
    kill_process_tree(proc)

    assert_converged(pypiron_bin, data_dir, f"upload kill point {kill_point}")


@pytest.mark.chaos
@pytest.mark.parametrize("kill_point", range(1, 9))
def test_crash_during_yank_converges(
    pypiron_bin: Path, tmp_path: Path, kill_point: int
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    wheel = make_wheel("chaos-yank", "1.0", tmp_path)

    # Clean setup: package uploaded and fully indexed.
    proc, base = start_server(pypiron_bin, data_dir, boot_timeout=15.0)
    upload_legacy(f"{base}/legacy/", wheel, **AUTH)
    drain_markers(data_dir)
    kill_process_tree(proc)

    proc, base = start_server(pypiron_bin, data_dir, fault_after=kill_point)
    import urllib.request

    def yank():
        req = urllib.request.Request(
            f"{base}/files/chaos-yank/{wheel.name}/yank",
            data=b"oops",
            method="POST",
            headers={"Authorization": _basic_auth()},
        )
        urllib.request.urlopen(req, timeout=10)

    attempt(yank)
    time.sleep(1.5)
    kill_process_tree(proc)

    assert_converged(pypiron_bin, data_dir, f"yank kill point {kill_point}")


@pytest.mark.chaos
@pytest.mark.parametrize("kill_point", range(1, 13))
def test_crash_during_delete_converges(
    pypiron_bin: Path, tmp_path: Path, kill_point: int
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    wheel = make_wheel("chaos-delete", "1.0", tmp_path)

    proc, base = start_server(pypiron_bin, data_dir, boot_timeout=15.0)
    upload_legacy(f"{base}/legacy/", wheel, **AUTH)
    drain_markers(data_dir)
    kill_process_tree(proc)

    proc, base = start_server(pypiron_bin, data_dir, fault_after=kill_point)
    import urllib.request

    def delete():
        req = urllib.request.Request(
            f"{base}/files/chaos-delete/{wheel.name}",
            method="DELETE",
            headers={"Authorization": _basic_auth()},
        )
        urllib.request.urlopen(req, timeout=10)

    attempt(delete)
    time.sleep(1.5)
    kill_process_tree(proc)

    assert_converged(pypiron_bin, data_dir, f"delete kill point {kill_point}")


@pytest.mark.chaos
@pytest.mark.s3
def test_multi_node_s3_uploads_converge(pypiron_bin: Path, minio, tmp_path: Path) -> None:
    """Two nodes, one bucket, audit disabled: concurrent uploads land on both
    nodes and must converge through markers + lease leadership + CAS global
    updates alone. (Disk is documented single-node; multi-node is S3's job.)"""
    from .conftest import _s3_env

    event_only_env = {
        "PYPIRON_AUDIT_ON_BOOT": "false",
        "PYPIRON_RECONCILE_INTERVAL_SECS": "100000",
        "PYPIRON_INTENT_GRACE_SECS": "1",
    }

    def start_node() -> Tuple[subprocess.Popen, str]:
        port = find_free_port()
        env = _s3_env(minio, f"127.0.0.1:{port}")
        env.update(event_only_env)
        log = open(tmp_path / f"node-{port}.log", "w")
        proc = subprocess.Popen(
            [str(pypiron_bin)], env=env, stdout=log, stderr=subprocess.STDOUT
        )
        wait_http_responding(f"http://127.0.0.1:{port}/health", timeout=30)
        return proc, f"http://127.0.0.1:{port}"

    proc_a, base_a = start_node()
    proc_b, base_b = start_node()
    try:
        import threading

        def uploads(base: str, names: list[str]) -> None:
            for name in names:
                wheel = make_wheel(name, "1.0", tmp_path)
                attempt(upload_legacy, f"{base}/legacy/", wheel, **AUTH)

        t_a = threading.Thread(
            target=uploads, args=(base_a, [f"dual-{i}" for i in range(6)])
        )
        t_b = threading.Thread(
            target=uploads, args=(base_b, [f"dual-{i}" for i in range(3, 9)])
        )
        t_a.start()
        t_b.start()
        t_a.join()
        t_b.join()

        # Eventual convergence is the property: poll the oracle.
        verify_env = _s3_env(minio, "unused")
        verify_env.update(event_only_env)
        deadline = time.time() + 60
        result = None
        while time.time() < deadline:
            result = subprocess.run(
                [str(pypiron_bin), "verify"],
                env=verify_env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode == 0:
                break
            time.sleep(2)
        assert result is not None and result.returncode == 0, (
            result.stdout + result.stderr if result else "verify never ran"
        )
    finally:
        kill_process_tree(proc_a)
        kill_process_tree(proc_b)
