"""A tombstoned artifact must stop being downloadable by its direct URL.

Deleting a private file writes `<filename>.tombstone` beside it and then drops
the body. Two supported paths leave the tombstone standing over live bytes:
a delete that fails after writing it, and an upload that lands on a fenced
filename and deliberately declines to free an immutable name on a guess
(src/publish.rs). `tombstone::complete_interrupted_deletes` reaps that residue,
but only when the audit next sweeps — a reconcile interval later, 24 h by
default. Until then the file is suppressed from every index yet still fetchable
by anyone who kept the URL.

Multi-bucket already refuses it, on markers it HEADs per request. Single-bucket
now refuses it too, off a verdict cached per artifact key for
`cache::FENCE_CACHE_TTL`. These tests drive that residue state directly — a
tombstone object written beside a live body, which is exactly what both paths
above leave behind — and assert a node that did not create it refuses the
download.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator

import pytest

from .helpers import (
    find_free_port,
    http_get,
    http_request_auth,
    kill_process_tree,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
    wait_http_responding,
)

pytestmark = pytest.mark.integration

PACKAGE = "fencedpkg"
VERSION = "1.0.0"

# No boot audit and a day-long reconcile: the audit is the thing that reaps a
# tombstoned body, so leaving it armed would mask exactly what is under test.
QUIET_ARGS = (
    "--audit-on-boot",
    "false",
    "--reconcile-interval-secs",
    "86400",
)
CREDS = {"username": "admin", "password": "secret"}


def fence_cache_ttl(repo_root: Path) -> float:
    """The Rust-side TTL, read from source so the two can't drift apart."""
    text = (repo_root / "src" / "cache.rs").read_text()
    match = re.search(r"FENCE_CACHE_TTL: Duration = Duration::from_secs\((\d+)\)", text)
    assert match, "FENCE_CACHE_TTL not found in src/cache.rs"
    return float(match.group(1))


@contextmanager
def disk_node(bin_path: Path, data_dir: Path, log_path: Path) -> Iterator[Dict]:
    """One pypiron on `data_dir`. Several nodes may share one directory."""
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    args = [
        str(bin_path),
        "serve",
        "--bind-addr",
        bind,
        "--data-dir",
        str(data_dir),
        "--admin-user",
        "admin",
        "--admin-pass",
        "secret",
        "--worker-interval-secs",
        "1",
        *QUIET_ARGS,
    ]
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info,pypiron=debug")
    env.setdefault("PYPIRON_ADVISORY_FEED", "")
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        try:
            wait_http_responding(f"http://{bind}/simple/index.json", timeout=20.0)
            yield _node(bind, proc)
        finally:
            kill_process_tree(proc)


@contextmanager
def s3_node(bin_path: Path, minio: Dict, log_path: Path) -> Iterator[Dict]:
    """One pypiron on a MinIO bucket. Several nodes may share one bucket."""
    from .conftest import _s3_env

    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _s3_env(minio, bind)
    env["PYPIRON_ADVISORY_FEED"] = ""
    env["PYPIRON_AUDIT_ON_BOOT"] = "false"
    env["PYPIRON_RECONCILE_INTERVAL_SECS"] = "86400"
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            [str(bin_path), "serve"], env=env, stdout=log_file, stderr=subprocess.STDOUT
        )
        try:
            wait_http_responding(f"http://{bind}/simple/index.json", timeout=30.0)
            yield _node(bind, proc)
        finally:
            kill_process_tree(proc)


def _node(bind: str, proc: subprocess.Popen) -> Dict:
    return {
        "base_url": f"http://{bind}",
        "legacy": f"http://{bind}/legacy/",
        "simple": f"http://{bind}/simple/",
        "proc": proc,
    }


def artifact_url(node: Dict, filename: str) -> str:
    return f"{node['base_url']}/files/{PACKAGE}/{filename}"


def tombstone_body(filename: str) -> str:
    """The `Tombstone` struct src/tombstone.rs writes (src/sidecar.rs keys it
    `<artifact key>.tombstone`)."""
    return json.dumps({"filename": filename})


def publish(node: Dict, wheel: Path) -> None:
    upload_legacy(node["legacy"], wheel, **CREDS)
    wait_for_file_in_index(node["simple"], PACKAGE, wheel.name)


def wait_for_status(url: str, want: int, *, timeout: float) -> float:
    """Seconds until `url` answers `want`; fails the test if it never does."""
    start = time.monotonic()
    deadline = start + timeout
    last = None
    while time.monotonic() < deadline:
        last, _, _ = http_get(url)
        if last == want:
            return time.monotonic() - start
        time.sleep(0.25)
    pytest.fail(f"{url} still answering {last} after {timeout}s; wanted {want}")


def test_delete_makes_the_direct_url_404(tmp_path, pypiron_bin):
    """The ordinary delete: the direct URL must 404 on the deleting node at
    once. Same status the multi-bucket fence returns for a tombstoned artifact
    (`not_found("artifact is fenced")`, src/serve.rs)."""
    data = tmp_path / "data"
    data.mkdir()
    wheel = make_wheel(PACKAGE, VERSION, tmp_path)
    with disk_node(pypiron_bin, data, tmp_path / "a.log") as node:
        publish(node, wheel)
        code, body, _ = http_get(artifact_url(node, wheel.name))
        assert code == 200 and body

        code, _, _ = http_request_auth("DELETE", artifact_url(node, wheel.name), **CREDS)
        assert code == 204

        code, _, _ = http_get(artifact_url(node, wheel.name))
        assert code == 404, "a deleted artifact must not be downloadable by direct URL"


def test_tombstoned_body_is_fenced_on_disk(tmp_path, pypiron_bin, repo_root):
    """A body left standing under its tombstone: a node that did not write the
    tombstone must refuse it, and one that already served the file must stop
    within the cache TTL."""
    ttl = fence_cache_ttl(repo_root)
    data = tmp_path / "data"
    data.mkdir()
    wheel = make_wheel(PACKAGE, VERSION, tmp_path)
    with disk_node(pypiron_bin, data, tmp_path / "a.log") as node_a:
        publish(node_a, wheel)
        # Warms node A's verdict for this key: it has now seen the file unfenced.
        code, body, _ = http_get(artifact_url(node_a, wheel.name))
        assert code == 200 and body

        with disk_node(pypiron_bin, data, tmp_path / "b.log") as node_b:
            live = data / "packages" / PACKAGE / wheel.name
            assert live.exists()
            (live.parent / f"{wheel.name}.tombstone").write_text(tombstone_body(wheel.name))

            # Node B never served this key, so it reads the markers: fenced.
            code, _, _ = http_get(artifact_url(node_b, wheel.name))
            assert code == 404, "a tombstoned body must not be downloadable by direct URL"
            assert live.exists(), "the fence must be the marker, not a body the read path ate"

            # Node A is still inside its TTL, so it is still serving — that is
            # the bound this cache buys, and the reason it costs two HEADs per
            # artifact rather than two per request.
            code, _, _ = http_get(artifact_url(node_a, wheel.name))
            assert code == 200

            elapsed = wait_for_status(artifact_url(node_a, wheel.name), 404, timeout=ttl + 20.0)
            assert elapsed > 0.0, "node A converged; it must do so off an expiring verdict"


@pytest.mark.s3
def test_tombstoned_body_is_fenced_on_s3(tmp_path, pypiron_bin, repo_root, minio):
    """The same, on an object store: two nodes, one bucket."""
    from .conftest import minio_list_keys, minio_put_key

    ttl = fence_cache_ttl(repo_root)
    wheel = make_wheel(PACKAGE, VERSION, tmp_path)
    with s3_node(pypiron_bin, minio, tmp_path / "a.log") as node_a:
        publish(node_a, wheel)
        code, body, _ = http_get(artifact_url(node_a, wheel.name))
        assert code == 200 and body

        with s3_node(pypiron_bin, minio, tmp_path / "b.log") as node_b:
            key = f"packages/{PACKAGE}/{wheel.name}"
            assert key in minio_list_keys(minio)
            minio_put_key(minio, f"{key}.tombstone", tombstone_body(wheel.name))

            code, _, _ = http_get(artifact_url(node_b, wheel.name))
            assert code == 404, "a tombstoned body must not be downloadable by direct URL"
            assert key in minio_list_keys(minio), "the fence must be the marker, not the body"

            code, _, _ = http_get(artifact_url(node_a, wheel.name))
            assert code == 200

            wait_for_status(artifact_url(node_a, wheel.name), 404, timeout=ttl + 20.0)
