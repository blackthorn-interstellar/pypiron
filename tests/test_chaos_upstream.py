"""Upstream fault injection for the on-demand proxy.

The proxy's whole promise is that an unreliable upstream never poisons the local
cache: a truncated, corrupt, 500ing, or hanging upstream response must leave the
storage tree clean (no partial artifact, no orphaned temp file), never hand a
client a corrupt or partial artifact, and never block a later healthy fetch.

These tests run a real pypiron in proxy mode against a fake upstream we can make
misbehave on demand (`_FaultServer`). The upstream speaks just enough PEP 691 to
be consumed: a JSON simple index plus artifact bytes, with a per-package fault
mode injected on the artifact endpoint.
"""

from __future__ import annotations

import hashlib
import http.server
import json
import threading
import time
from pathlib import Path
from typing import Dict, Iterator, Tuple

import pytest

from .conftest import _start_disk_server
from .helpers import (
    find_free_port,
    http_get,
    make_wheel,
    origin_owner,
    run_checked,
    sha256_file,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

# Past upload-time so the proxy's freshness window is never the reason a file is
# withheld — we disable the cooldown too (`--exclude-newer ""`), belt and braces.
PAST_UPLOAD_TIME = "2020-01-01T00:00:00Z"
# The fake upstream stalls this long on a "hang" before dropping the connection.
# Small so the test stays fast; the proxy's own 30s read timeout is the real
# ceiling, this just proves a slow-then-dead upstream is handled, not waited on.
HANG_SECS = 1.5
# The proxy retries a failed download 3x with 2s+4s backoff, so a persistently
# faulty artifact GET takes ~6-10s to fail. Generous client timeout covers it;
# the boundedness assertion uses a much larger ceiling than the real ~10s.
FAULT_GET_TIMEOUT = 60.0
FAULT_BOUND_SECS = 45.0


class _FaultHandler(http.server.BaseHTTPRequestHandler):
    """Serves a PEP 691 index and artifact bytes, injecting the per-package
    fault mode set on the owning server."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # noqa: D102 - silence the default stderr spam
        pass

    def do_GET(self):  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        path = self.path.split("?", 1)[0]
        registry = self.server.registry  # type: ignore[attr-defined]

        if path.startswith("/simple/") and path.endswith("/"):
            pkg = path[len("/simple/") : -1]
            info = registry.get(pkg)
            if info is None:
                self.send_error(404)
                return
            self._serve_index(pkg, info)
            return

        if path.startswith("/files/"):
            pkg, _, filename = path[len("/files/") :].partition("/")
            info = registry.get(pkg)
            if info is None or filename != info["filename"]:
                self.send_error(404)
                return
            self._serve_artifact(pkg, info)
            return

        self.send_error(404)

    def _serve_index(self, pkg: str, info: Dict) -> None:
        body = json.dumps(
            {
                "meta": {"api-version": "1.0"},
                "name": pkg,
                "files": [
                    {
                        "filename": info["filename"],
                        "url": info["url"],
                        "hashes": {"sha256": info["sha256"]},
                        "size": info["size"],
                        "upload-time": info["upload_time"],
                    }
                ],
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.pypi.simple.v1+json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_artifact(self, pkg: str, info: Dict) -> None:
        server = self.server  # type: ignore[attr-defined]
        mode = server.faults.get(pkg, "healthy")
        server.hits[pkg] = server.hits.get(pkg, 0) + 1
        full: bytes = info["bytes"]

        if mode == "recover":
            # 500 for the first N attempts, then heal — exercises the proxy's
            # in-request retry recovering without a poisoned cache.
            if server.hits[pkg] <= server.recover_after.get(pkg, 1):
                self.send_error(500)
                return
            mode = "healthy"

        if mode == "error500":
            self.send_error(500)
            return

        if mode == "healthy":
            self._send_bytes(full)
            return

        if mode == "truncate":
            # Advertise the full length but send only half, then drop: the client
            # sees a premature EOF against Content-Length.
            self._send_partial(len(full), full[: max(1, len(full) // 2)])
            return

        if mode == "corrupt":
            # Full length, wrong bytes: passes the size check, fails the sha256.
            bad = bytearray(full)
            for i in range(min(len(bad), 64)):
                bad[i] ^= 0xFF
            self._send_bytes(bytes(bad))
            return

        if mode == "hang":
            # Send a sliver, stall, then drop — a slow/hanging upstream that
            # never completes the body.
            self._send_partial(len(full), full[: max(1, len(full) // 4)], stall=HANG_SECS)
            return

        raise AssertionError(f"unknown fault mode {mode!r}")

    def _send_bytes(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_partial(self, declared_len: int, partial: bytes, *, stall: float = 0.0) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(declared_len))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(partial)
        self.wfile.flush()
        if stall:
            time.sleep(stall)
        self.close_connection = True


class _FaultServer:
    """A threaded fake upstream. Register a wheel, then flip its artifact fault
    mode at will; the proxy under test fetches from `base`."""

    def __init__(self) -> None:
        port = find_free_port()
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", port), _FaultHandler)
        self.httpd.registry = {}  # type: ignore[attr-defined]
        self.httpd.faults = {}  # type: ignore[attr-defined]
        self.httpd.hits = {}  # type: ignore[attr-defined]
        self.httpd.recover_after = {}  # type: ignore[attr-defined]
        self.base = f"http://127.0.0.1:{port}"
        self._thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self._thread.start()

    def register(self, pkg: str, wheel_path: Path) -> str:
        data = wheel_path.read_bytes()
        self.httpd.registry[pkg] = {  # type: ignore[attr-defined]
            "filename": wheel_path.name,
            "url": f"{self.base}/files/{pkg}/{wheel_path.name}",
            "sha256": hashlib.sha256(data).hexdigest(),
            "size": len(data),
            "upload_time": PAST_UPLOAD_TIME,
            "bytes": data,
        }
        return wheel_path.name

    def set_fault(self, pkg: str, mode: str, *, recover_after: int = 1) -> None:
        self.httpd.faults[pkg] = mode  # type: ignore[attr-defined]
        self.httpd.recover_after[pkg] = recover_after  # type: ignore[attr-defined]

    def heal(self, pkg: str) -> None:
        self.httpd.faults[pkg] = "healthy"  # type: ignore[attr-defined]

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def _proxy_over_fault(
    tmp_path_factory,
    pypiron_bin: Path,
    tmp_path: Path,
    *,
    worker_args: tuple[str, ...] = (),
) -> Iterator[Tuple[Dict, _FaultServer]]:
    """A real pypiron proxy pointed at a controllable fake upstream. The spool
    dir is isolated so a test can prove it drains after a failed download."""
    upstream = _FaultServer()
    spool = tmp_path / "spool"
    spool.mkdir()
    gen = _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=[
            "--proxy-upstream",
            upstream.base,
            # Loopback fault-injecting upstream on plain http; see conftest.
            "--allow-insecure-upstream",
            "--exclude-newer",
            "",
            "--spool-dir",
            str(spool),
            *worker_args,
        ],
    )
    proxy = next(gen)
    proxy["spool_dir"] = spool
    try:
        yield proxy, upstream
    finally:
        gen.close()
        upstream.stop()


@pytest.fixture()
def proxy_over_fault(
    tmp_path_factory, pypiron_bin: Path, tmp_path: Path
) -> Iterator[Tuple[Dict, _FaultServer]]:
    yield from _proxy_over_fault(tmp_path_factory, pypiron_bin, tmp_path)


@pytest.fixture()
def proxy_over_fault_reclaim(
    tmp_path_factory, pypiron_bin: Path, tmp_path: Path
) -> Iterator[Tuple[Dict, _FaultServer]]:
    yield from _proxy_over_fault(
        tmp_path_factory,
        pypiron_bin,
        tmp_path,
        worker_args=(
            "--intent-grace-secs",
            "10",
            "--reconcile-interval-secs",
            "1",
        ),
    )


# ------------------------------- assertions -----------------------------------


def _pkg_dir(proxy: Dict, pkg: str) -> Path:
    return proxy["data_dir"] / "packages" / pkg


def _assert_storage_clean(proxy: Dict, pkg: str, filename: str) -> None:
    """No artifact committed, no orphaned temp sibling, spool drained — the
    write-to-tmp-then-rename contract holds even when the download failed."""
    pkg_dir = _pkg_dir(proxy, pkg)
    if pkg_dir.exists():
        assert not (pkg_dir / filename).exists(), (
            f"a failed upstream fetch left an artifact in storage: {filename}"
        )
        stray = [p.name for p in pkg_dir.iterdir() if p.name.startswith(".tmp")]
        assert not stray, f"orphaned temp files in {pkg_dir}: {stray}"
        # Failure-path claim release races live writers. Reclamation belongs to
        # the leader audit; until then the exact mirror claim must remain present.
        origin_file = pkg_dir / ".origin"
        assert origin_file.exists(), "failed first fetch lost its origin claim"
        assert origin_owner(origin_file.read_text()) == "mirror"
    # The spool file self-cleans on every early-return path; give the drop a
    # moment to run, then require it empty.
    deadline = time.time() + 5.0
    while time.time() < deadline and list(proxy["spool_dir"].iterdir()):
        time.sleep(0.1)
    assert not list(proxy["spool_dir"].iterdir()), (
        f"spool not drained after failed download: {list(proxy['spool_dir'].iterdir())}"
    )


def _healthy_fetch_matches(proxy: Dict, pkg: str, filename: str, expected_sha: str) -> None:
    code, body, _ = http_get(
        f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
    )
    assert code == 200, f"healthy fetch after fault returned {code}"
    assert hashlib.sha256(body).hexdigest() == expected_sha, (
        "served bytes do not match upstream sha256"
    )
    # And it committed cleanly this time.
    assert (_pkg_dir(proxy, pkg) / filename).exists()
    assert origin_owner((_pkg_dir(proxy, pkg) / ".origin").read_text()) == "mirror"


# --------------------------------- tests --------------------------------------


def test_truncated_body_never_poisons_cache(proxy_over_fault, tmp_path, uv_path, uv_venv):
    """A truncated artifact stream fails verification, is never committed, and a
    later healthy fetch caches + installs cleanly."""
    proxy, upstream = proxy_over_fault
    pkg = "truncpkg"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    filename = upstream.register(pkg, wheel)
    upstream.set_fault(pkg, "truncate")

    code, body, _ = http_get(
        f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
    )
    assert code != 200, "a truncated upstream body must never be served as a 200"
    assert body != wheel.read_bytes(), (
        "client received the (impossible) full body from a truncated stream"
    )
    _assert_storage_clean(proxy, pkg, filename)

    upstream.heal(pkg)
    _healthy_fetch_matches(proxy, pkg, filename, sha256_file(wheel))

    # End-to-end: the recovered cache installs into a fresh venv with a real client.
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(uv_venv),
            "--index-url",
            proxy["simple"],
            "--no-cache-dir",
            f"{pkg}==1.0",
        ],
        timeout=180,
    )
    run_checked([str(uv_venv), "-c", "import truncpkg"])


def test_upstream_500_then_recovers_within_retries(proxy_over_fault, tmp_path):
    """Upstream 500s on the first attempt then heals; the proxy's in-request
    retry succeeds, so a single client GET returns the verified artifact."""
    proxy, upstream = proxy_over_fault
    pkg = "recoverpkg"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    filename = upstream.register(pkg, wheel)
    upstream.set_fault(pkg, "recover", recover_after=1)

    code, body, _ = http_get(
        f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
    )
    assert code == 200, "the proxy should have retried past a transient 500"
    assert hashlib.sha256(body).hexdigest() == sha256_file(wheel)
    assert (_pkg_dir(proxy, pkg) / filename).exists(), "recovered download was not committed"
    assert origin_owner((_pkg_dir(proxy, pkg) / ".origin").read_text()) == "mirror"


def test_hash_mismatch_is_never_committed(proxy_over_fault, tmp_path):
    """Full-length but corrupt bytes fail the sha256 check: nothing is committed,
    the client is never handed the corrupt body, and a healed fetch caches the
    real artifact."""
    proxy, upstream = proxy_over_fault
    pkg = "corruptpkg"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    filename = upstream.register(pkg, wheel)
    upstream.set_fault(pkg, "corrupt")

    code, body, _ = http_get(
        f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
    )
    assert code != 200, "artifact whose bytes mismatch the advertised hash must not be served"
    assert hashlib.sha256(body).hexdigest() != sha256_file(wheel)
    _assert_storage_clean(proxy, pkg, filename)

    upstream.heal(pkg)
    _healthy_fetch_matches(proxy, pkg, filename, sha256_file(wheel))


def test_worker_reclaims_failed_proxy_claim_for_private_upload(proxy_over_fault_reclaim, tmp_path):
    """The real single-bucket audit releases a stable abandoned mirror claim."""
    proxy, upstream = proxy_over_fault_reclaim
    pkg = "reclaimfailedproxy"
    mirror_wheel = make_wheel(pkg, "1.0", tmp_path / "mirror")
    filename = upstream.register(pkg, mirror_wheel)
    upstream.set_fault(pkg, "corrupt")

    code, _, _ = http_get(f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT)
    assert code != 200
    origin_file = _pkg_dir(proxy, pkg) / ".origin"
    assert origin_file.exists()
    assert origin_owner(origin_file.read_text()) == "mirror"

    deadline = time.monotonic() + 20
    owner = "mirror"
    while time.monotonic() < deadline:
        owner = origin_owner(origin_file.read_text())
        if owner == "unclaimed":
            break
        time.sleep(0.2)
    assert owner == "unclaimed", "worker audits did not reclaim the abandoned mirror claim"

    private_wheel = make_wheel(pkg, "2.0", tmp_path / "private")
    upload_legacy(
        proxy["legacy"],
        private_wheel,
        username=proxy["user"],
        password=proxy["password"],
    )
    wait_for_file_in_index(proxy["simple"], pkg, private_wheel.name)
    assert origin_owner(origin_file.read_text()) == "private"


def test_hanging_upstream_is_bounded_and_clean(proxy_over_fault, tmp_path):
    """A stalling/hanging upstream fails in bounded time (no open-ended client
    outage), leaves storage clean, and does not block a later healthy fetch."""
    proxy, upstream = proxy_over_fault
    pkg = "hangpkg"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    filename = upstream.register(pkg, wheel)
    upstream.set_fault(pkg, "hang")

    t0 = time.time()
    code, _, _ = http_get(f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT)
    elapsed = time.time() - t0
    assert code != 200, "a hung/incomplete upstream body must never be served as a 200"
    assert elapsed < FAULT_BOUND_SECS, (
        f"hanging upstream produced an unbounded outage ({elapsed:.1f}s)"
    )
    _assert_storage_clean(proxy, pkg, filename)

    upstream.heal(pkg)
    _healthy_fetch_matches(proxy, pkg, filename, sha256_file(wheel))
