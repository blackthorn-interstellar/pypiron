"""Server-side-copy replication transport (real binary, real MinIO).

The replication ladder asks the provider to copy an artifact between two buckets
of the same cloud (S3 CopyObject) instead of streaming GET+PUT through the node,
and falls back to streaming on any ineligibility or failure. These tests drive
the REAL path against MinIO:

- two buckets on ONE MinIO endpoint (same credentials) are copy-eligible, so an
  upload fans out via a genuine CopyObject — proven by
  `pypiron_replication_server_side_copies_total` and the boot transport matrix;
- two buckets on DISTINCT MinIO endpoints are not copyable, so the same upload
  fans out by streaming — the copy counter stays zero and the matrix logs it.

The GCS/Azure request construction and signing are covered by Rust unit tests
(`src/reqsign.rs`, `src/storage.rs`), and the copy->stream fallback under injected
faults (denied / timeout / phantom-200) by the deterministic simulator
(`tests/model_replication.rs::convergence_is_transport_invariant`).
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import time

import pytest

from .conftest import (
    minio_key_exists_in,
    minio_object_sha256,
)
from .helpers import (
    find_free_port,
    http_get,
    kill_process_tree,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
    wait_http_ok,
)

pytestmark = [pytest.mark.integration, pytest.mark.s3]


def _server_side_copies(base_url: str) -> int:
    """Read `pypiron_replication_server_side_copies_total` from /metrics."""
    code, body, _ = http_get(f"{base_url}/metrics", timeout=3)
    assert code == 200, f"metrics returned {code}"
    for line in body.decode().splitlines():
        if line.startswith("pypiron_replication_server_side_copies_total "):
            return int(line.rsplit(" ", 1)[1])
    return 0


def _matrix_lines(log_path) -> list[str]:
    """Boot transport-matrix log lines (one per ordered bucket pair)."""
    text = log_path.read_text(errors="replace")
    return [ln for ln in text.splitlines() if "replication copy matrix" in ln]


def _has_transport(lines: list[str], transport: str) -> bool:
    # tolerate both the plain (`transport=copy`) and JSON (`transport="copy"`)
    # tracing renderings.
    return any(f"transport={transport}" in ln or f'transport="{transport}"' in ln for ln in lines)


def _eventually(check, *, timeout: float = 20.0, what: str = "condition") -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def test_same_endpoint_two_buckets_replicate_via_server_side_copy(
    minio_two, s3_server_multi, tmp_path
):
    """Two buckets on one MinIO endpoint are copy-eligible: an upload's fan-out
    is a real S3 CopyObject (zero artifact bytes through the node), and both
    buckets end byte-identical."""
    server = s3_server_multi
    a, b = minio_two["buckets"]

    # The boot matrix verified the pair by copying the topology stamp — the
    # eligible cell logs `transport=copy`.
    matrix = _matrix_lines(server["log_path"])
    assert matrix, "boot did not log a replication copy matrix"
    assert _has_transport(matrix, "copy"), f"expected a copy cell, got:\n{matrix}"

    wheel = make_wheel("copytransport", "1.0", tmp_path)
    wheel_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
        timeout=20,
    )
    wait_for_file_in_index(server["simple"], "copytransport", wheel.name)

    key = f"packages/copytransport/{wheel.name}"
    # Landed on the write bucket, and the fan-out placed it on the peer too.
    assert minio_key_exists_in(minio_two, a, key)
    _eventually(
        lambda: minio_key_exists_in(minio_two, b, key),
        what="artifact fanned out to the peer bucket",
    )
    # Byte-identical on the peer — the server-side copy landed correct bytes.
    assert minio_object_sha256(minio_two, b, key) == wheel_sha

    # And it moved provider-side: the copy counter incremented.
    _eventually(
        lambda: _server_side_copies(server["base_url"]) >= 1,
        what="a server-side copy was recorded",
    )


def test_distinct_endpoints_fall_back_to_streaming(minio, minio_alt, pypiron_bin, tmp_path):
    """Two buckets on separate MinIO endpoints are NOT copyable (different
    endpoints, the two-cluster case): the boot matrix logs `transport=stream`,
    fan-out streams through the node, and the copy counter stays zero — while the
    artifact still lands byte-for-byte on both."""
    a = minio["bucket"]
    b = minio_alt["bucket"]
    config = tmp_path / "pypiron.toml"
    config.write_text(
        f"""
[serve.bucket."s3://{b}"]
endpoint-url = "{minio_alt["endpoint"]}"
force-path-style = true
env-prefix = "BUCKETB_"
"""
    )

    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("BUCKETB_"):
            del env[key]
    env.update(
        {
            "PYPIRON_BIND_ADDR": bind,
            "PYPIRON_BUCKETS": f"s3://{a},s3://{b}",
            "PYPIRON_WORKER_INTERVAL_SECS": "1",
            "PYPIRON_AUDIT_ON_BOOT": "false",
            "PYPIRON_ADMIN_USER": "admin",
            "PYPIRON_ADMIN_PASS": "secret",
            "PYPIRON_UPLOADER_USER": "uploader",
            "PYPIRON_UPLOADER_PASS": "uploadersecret",
            "AWS_REGION": "us-east-1",
            "PYPIRON_S3_ENDPOINT_URL": minio["endpoint"],
            "PYPIRON_S3_FORCE_PATH_STYLE": "true",
            "AWS_ACCESS_KEY_ID": minio["access_key"],
            "AWS_SECRET_ACCESS_KEY": minio["secret_key"],
            "BUCKETB_AWS_ACCESS_KEY_ID": minio_alt["access_key"],
            "BUCKETB_AWS_SECRET_ACCESS_KEY": minio_alt["secret_key"],
            "RUST_LOG": "info,pypiron=debug",
        }
    )

    log_path = tmp_path / "server.log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [str(pypiron_bin), "serve", "--config", str(config)],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    try:
        wait_http_ok(f"http://{bind}/simple/index.json", timeout=30.0)
        base_url = f"http://{bind}"

        matrix = _matrix_lines(log_path)
        assert matrix, "boot did not log a replication copy matrix"
        assert _has_transport(matrix, "stream"), f"expected a stream cell, got:\n{matrix}"
        assert not _has_transport(matrix, "copy"), (
            f"distinct endpoints must not be copyable, got:\n{matrix}"
        )

        wheel = make_wheel("streamfallback", "1.0", tmp_path)
        wheel_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
        upload_legacy(
            f"http://{bind}/legacy/",
            wheel,
            username="admin",
            password="secret",
            timeout=20,
        )
        wait_for_file_in_index(f"http://{bind}/simple/", "streamfallback", wheel.name)

        key = f"packages/streamfallback/{wheel.name}"
        assert minio_key_exists_in(minio, a, key)
        _eventually(
            lambda: minio_key_exists_in(minio_alt, b, key),
            what="artifact fanned out to the second-endpoint bucket by streaming",
        )
        assert minio_object_sha256(minio, a, key) == wheel_sha
        assert minio_object_sha256(minio_alt, b, key) == wheel_sha
        # No server-side copy was possible across two endpoints.
        assert _server_side_copies(base_url) == 0
    finally:
        kill_process_tree(proc)
