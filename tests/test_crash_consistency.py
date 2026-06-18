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
import re
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError

import pytest

from .helpers import (
    find_free_port,
    http_get,
    kill_process_tree,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
    wait_for_project_in_global,
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
def test_crash_during_upload_converges(pypiron_bin: Path, tmp_path: Path, kill_point: int) -> None:
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
def test_crash_during_yank_converges(pypiron_bin: Path, tmp_path: Path, kill_point: int) -> None:
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
def test_crash_during_delete_converges(pypiron_bin: Path, tmp_path: Path, kill_point: int) -> None:
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
        proc = subprocess.Popen([str(pypiron_bin)], env=env, stdout=log, stderr=subprocess.STDOUT)
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

        t_a = threading.Thread(target=uploads, args=(base_a, [f"dual-{i}" for i in range(6)]))
        t_b = threading.Thread(target=uploads, args=(base_b, [f"dual-{i}" for i in range(3, 9)]))
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


# ============================ Leader-election chaos ============================
#
# The design claims the lease is "a cost optimization, never a correctness
# requirement". These tests convert that claim into measurements: kill, freeze,
# and overlap leaders against MinIO with a short lease TTL and the audit
# disabled, so the event path + election do all the healing. A 2s TTL means
# failover lands within a few worker ticks; convergence is asserted by polling
# the read-only oracle (`pypiron verify`) to a bounded deadline.

LEASE_TTL_SECS = 2
# Boot writes (lease acquire) + an upload's marker/artifact protocol all happen
# concurrently across nodes, so failover lands within TTL + a tick or two.
FAILOVER_BUDGET_SECS = LEASE_TTL_SECS + 8


def _chaos_s3_env(minio: Dict, bind: str) -> Dict[str, str]:
    """S3 node env: short lease TTL, audit off, fast ticks — election + markers
    are the only healing mechanisms in play."""
    from .conftest import _s3_env

    env = _s3_env(minio, bind)
    env.update(
        {
            "PYPIRON_AUDIT_ON_BOOT": "false",
            "PYPIRON_RECONCILE_INTERVAL_SECS": "100000",
            "PYPIRON_INTENT_GRACE_SECS": "1",
            "PYPIRON_LEASE_TTL_SECS": str(LEASE_TTL_SECS),
        }
    )
    return env


def _start_s3_node(
    pypiron_bin: Path,
    minio: Dict,
    tmp_path: Path,
    label: str,
    *,
    fault_after: Optional[int] = None,
) -> Dict:
    port = find_free_port()
    env = _chaos_s3_env(minio, f"127.0.0.1:{port}")
    if fault_after is not None:
        env["PYPIRON_FAULT_ABORT_AFTER_WRITES"] = str(fault_after)
    log_path = tmp_path / f"{label}.log"
    log = open(log_path, "w")
    proc = subprocess.Popen([str(pypiron_bin)], env=env, stdout=log, stderr=subprocess.STDOUT)
    node = {"proc": proc, "base": f"http://127.0.0.1:{port}", "log": log_path, "label": label}
    try:
        # A faulted node may abort during boot writes; recovery proceeds either way.
        wait_http_responding(f"{node['base']}/health", timeout=30)
    except Exception:
        pass
    return node


def _log_text(node: Dict) -> str:
    try:
        return node["log"].read_text(errors="replace")
    except OSError:
        return ""


def _sigkill(node: Dict) -> None:
    """Ungraceful death: SIGKILL, no lease release, no graceful handover."""
    try:
        os.kill(node["proc"].pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _signal(node: Dict, sig: int) -> None:
    try:
        os.kill(node["proc"].pid, sig)
    except ProcessLookupError:
        pass


def _wait_for_leader(nodes: List[Dict], timeout: float = 20.0) -> Dict:
    """The boot leader is the single node whose log shows it took the lease
    (create-if-absent is atomic, so exactly one wins at startup)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        owners = [n for n in nodes if "lease acquired" in _log_text(n)]
        if len(owners) == 1:
            return owners[0]
        time.sleep(0.2)
    logs = "\n".join(f"[{n['label']}]\n{_log_text(n)[-1500:]}" for n in nodes)
    raise AssertionError(f"no unique boot leader emerged:\n{logs}")


def _wait_for_steal(node: Dict, timeout: float) -> bool:
    """True once `node` logs a lease steal (it became leader after the holder
    died or its lease expired)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if "lease stolen" in _log_text(node):
            return True
        time.sleep(0.2)
    return False


def _metric(node: Dict, name: str) -> float:
    code, body, _ = http_get(f"{node['base']}/metrics", timeout=5)
    if code != 200:
        return 0.0
    m = re.search(rf"^{re.escape(name)} ([\d.eE+-]+)$", body.decode(), re.MULTILINE)
    return float(m.group(1)) if m else 0.0


def _wait_for_metric(node: Dict, name: str, threshold: float, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if _metric(node, name) >= threshold:
                return True
        except (ConnectionError, OSError):
            pass
        time.sleep(0.3)
    return False


def _s3_verify(pypiron_bin: Path, minio: Dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(pypiron_bin), "verify"],
        env=_chaos_s3_env(minio, "unused"),
        capture_output=True,
        text=True,
        timeout=90,
    )


def _assert_s3_converges(
    pypiron_bin: Path, minio: Dict, tmp_path: Path, context: str, *, timeout: float = 60.0
) -> None:
    """Bring up a clean node to drain any pending markers, then poll the oracle
    until storage converges (views == recomputed-from-truth)."""
    node = _start_s3_node(
        pypiron_bin, minio, tmp_path, f"recover-{int(time.time() * 1000) % 100000}"
    )
    try:
        deadline = time.time() + timeout
        result = None
        while time.time() < deadline:
            result = _s3_verify(pypiron_bin, minio)
            if result.returncode == 0:
                return
            time.sleep(2)
        detail = (result.stdout + result.stderr) if result else "verify never ran"
        raise AssertionError(
            f"divergence after {context}:\n{detail}\n--- recovery log ---\n{_log_text(node)[-3000:]}"
        )
    finally:
        kill_process_tree(node["proc"])


@pytest.mark.chaos
@pytest.mark.s3
def test_ungraceful_failover_under_churn(pypiron_bin: Path, minio, tmp_path: Path) -> None:
    """Two nodes, continuous uploads to both; SIGKILL the leader mid-stream. A
    follower must take over within TTL + a tick, every marker must drain, and an
    upload made AFTER the kill must become visible in bounded time (no unbounded
    write outage)."""
    nodes = [
        _start_s3_node(pypiron_bin, minio, tmp_path, "node-a"),
        _start_s3_node(pypiron_bin, minio, tmp_path, "node-b"),
    ]
    stop = threading.Event()

    def churn(node: Dict, prefix: str) -> None:
        i = 0
        while not stop.is_set():
            wheel = make_wheel(f"{prefix}-{i}", "1.0", tmp_path / "churn")
            attempt(upload_legacy, f"{node['base']}/legacy/", wheel, **AUTH)
            i += 1
            time.sleep(0.1)

    threads = [
        threading.Thread(target=churn, args=(nodes[0], "churn-a"), daemon=True),
        threading.Thread(target=churn, args=(nodes[1], "churn-b"), daemon=True),
    ]
    try:
        leader = _wait_for_leader(nodes)
        follower = next(n for n in nodes if n is not leader)
        for t in threads:
            t.start()
        time.sleep(2.0)  # let churn flow and the leader build indexes

        # Ungraceful: no lease release, so the follower must wait out the TTL.
        _sigkill(leader)
        assert _wait_for_steal(follower, FAILOVER_BUDGET_SECS), (
            "follower never took the lease after the leader was killed:\n"
            f"{_log_text(follower)[-1500:]}"
        )

        # upload -> visible latency for a NEW upload after the kill, on the
        # survivor. The follower is already leader here, so this is bounded by a
        # tick + rebuild — proof the outage was bounded, not open-ended.
        probe = make_wheel("failover-probe", "1.0", tmp_path)
        t0 = time.time()
        upload_legacy(f"{follower['base']}/legacy/", probe, **AUTH)
        wait_for_file_in_index(
            f"{follower['base']}/simple/",
            "failover-probe",
            probe.name,
            timeout=FAILOVER_BUDGET_SECS,
        )
        latency = time.time() - t0
        assert latency < FAILOVER_BUDGET_SECS, (
            f"post-kill upload took {latency:.1f}s (unbounded outage)"
        )
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
        for n in nodes:
            kill_process_tree(n["proc"])

    _assert_s3_converges(pypiron_bin, minio, tmp_path, "ungraceful failover under churn")


@pytest.mark.chaos
@pytest.mark.s3
def test_dual_leadership_overlap_triggers_cas_conflict(
    pypiron_bin: Path, minio, tmp_path: Path
) -> None:
    """SIGSTOP the leader past its TTL so a follower steals the lease — true
    dual leadership. The zombie keeps a stale in-memory global-index ETag; when
    it resumes and writes the global index, its CAS must lose to the new
    leader's, fire the reload-and-retry path, and still converge. A test that
    can't prove the race occurred is a test of nothing, so we assert the
    CAS-conflict counter actually incremented on the zombie."""
    nodes = [
        _start_s3_node(pypiron_bin, minio, tmp_path, "zombie"),
        _start_s3_node(pypiron_bin, minio, tmp_path, "usurper"),
    ]
    leader = _wait_for_leader(nodes)
    follower = next(n for n in nodes if n is not leader)
    try:
        # 1. Populate the leader's in-memory global name set + ETag pin.
        seed = make_wheel("overlap-seed", "1.0", tmp_path)
        upload_legacy(f"{leader['base']}/legacy/", seed, **AUTH)
        wait_for_project_in_global(f"{leader['base']}/simple/", "overlap-seed", timeout=25)

        # 2. Freeze the leader: a zombie that still believes it holds the lease.
        _signal(leader, signal.SIGSTOP)

        # 3. The follower waits out the (now un-renewed) lease and steals it.
        assert _wait_for_steal(follower, FAILOVER_BUDGET_SECS), (
            f"follower never stole the lease from the frozen leader:\n{_log_text(follower)[-1500:]}"
        )

        # 4. New leader changes the global name set → global index ETag advances
        #    past the value the zombie still has cached.
        for i in range(3):
            wheel = make_wheel(f"overlap-new-{i}", "1.0", tmp_path)
            upload_legacy(f"{follower['base']}/legacy/", wheel, **AUTH)
        wait_for_project_in_global(f"{follower['base']}/simple/", "overlap-new-2", timeout=25)

        # 5. Kill the new leader so it can't drain the zombie's next marker —
        #    this forces the zombie to be the one that writes the global index.
        _sigkill(follower)

        # 6. Thaw the zombie. It still holds the stale ETag in memory.
        _signal(leader, signal.SIGCONT)

        # 7. Make the zombie change the global name set. Once it re-acquires the
        #    (expired) lease and processes this marker, its If-Match write uses
        #    the stale ETag and must lose the CAS.
        zombie_pkg = make_wheel("overlap-zombie", "1.0", tmp_path)
        upload_legacy(f"{leader['base']}/legacy/", zombie_pkg, **AUTH)

        assert _wait_for_metric(
            leader, "pypiron_global_cas_conflicts_total", 1, FAILOVER_BUDGET_SECS + 10
        ), (
            "zombie never hit the global-index CAS conflict — the dual-leadership "
            f"race did not occur:\n{_log_text(leader)[-2000:]}"
        )
    finally:
        _signal(leader, signal.SIGCONT)  # never leave a stopped process for teardown
        for n in nodes:
            kill_process_tree(n["proc"])

    _assert_s3_converges(pypiron_bin, minio, tmp_path, "dual-leadership overlap")


@pytest.mark.chaos
@pytest.mark.s3
def test_s3_kill_point_sweep_during_upload(pypiron_bin: Path, minio, tmp_path: Path) -> None:
    """The disk kill-point sweep, re-run on real conditional-write storage:
    sweep PYPIRON_FAULT_ABORT_AFTER_WRITES over a fresh-package upload so crashes
    land on lease writes (put_if_none_match / put_if_match), multipart artifact
    uploads, and marker ops. Each kill point recovers and verifies before the
    next, so convergence is asserted independently for every N."""
    kill_points = range(1, 17)  # 16 points: boot/lease through the full upload protocol
    for kill_point in kill_points:
        node = _start_s3_node(
            pypiron_bin, minio, tmp_path, f"fault-{kill_point}", fault_after=kill_point
        )
        wheel = make_wheel(f"s3-chaos-{kill_point}", "1.0", tmp_path)
        attempt(upload_legacy, f"{node['base']}/legacy/", wheel, **AUTH)
        # Give the nudged worker a moment to walk into the kill point too.
        time.sleep(1.2)
        _sigkill(node)
        _assert_s3_converges(
            pypiron_bin, minio, tmp_path, f"s3 upload kill point {kill_point}", timeout=45
        )
