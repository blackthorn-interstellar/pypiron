"""Per-bucket overrides (dev/PLAN_BUCKET_CONFIG.md, phase 2).

Real pypiron against two INDEPENDENT MinIO containers with different root
credentials, so a bucket is reachable only with its own scoped keys. The
`[serve.bucket."scheme://name"]` TOML table steers one bucket's endpoint and
credentials without touching the others. Assertions inspect each container
directly with its own keys, independent of what the server reports.

Covered: two-bucket fan-out where each side needs its own credentials, startup
refusal on a half-configured `env-prefix`, startup refusal on an override keyed
to an unknown bucket, and per-bucket `endpoint-url` steering that wins over the
global endpoint for one bucket only.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import time
from pathlib import Path

import pytest

from .conftest import minio_key_exists_in, minio_object_sha256
from .helpers import (
    find_free_port,
    kill_process_tree,
    make_wheel,
    run_returncode,
    upload_legacy,
    wait_for_file_in_index,
    wait_http_ok,
)

pytestmark = [pytest.mark.integration, pytest.mark.s3]


def _base_env(bind: str, buckets: str, primary: dict) -> dict:
    """Server env: ambient AWS_* keys belong to the PRIMARY container, and the
    global S3 endpoint points at it. A secondary bucket reaches its own
    container only through a per-bucket override in the config file."""
    env = os.environ.copy()
    env.update(
        {
            "PYPIRON_BIND_ADDR": bind,
            "PYPIRON_BUCKETS": buckets,
            "PYPIRON_WORKER_INTERVAL_SECS": "1",
            "PYPIRON_AUDIT_ON_BOOT": "false",
            "PYPIRON_ADMIN_USER": "admin",
            "PYPIRON_ADMIN_PASS": "secret",
            "PYPIRON_UPLOADER_USER": "uploader",
            "PYPIRON_UPLOADER_PASS": "uploadersecret",
            "AWS_REGION": "us-east-1",
            "PYPIRON_S3_ENDPOINT_URL": primary["endpoint"],
            "PYPIRON_S3_FORCE_PATH_STYLE": "true",
            "AWS_ACCESS_KEY_ID": primary["access_key"],
            "AWS_SECRET_ACCESS_KEY": primary["secret_key"],
            "RUST_LOG": "info,pypiron=debug",
        }
    )
    # A stray override credential from the ambient environment would mask the
    # exact miswiring these tests probe; scrub the prefixes we set below.
    for key in list(env):
        if key.startswith("BUCKETB_"):
            del env[key]
    return env


def _write_config(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "pypiron.toml"
    path.write_text(body)
    return path


def _serve_refuses(pypiron_bin: Path, env: dict, config: Path) -> tuple[int, str]:
    """Run `serve`, expecting it to fail fast at startup. Returns (rc, output).
    A server that instead starts serving would block until the timeout — which
    fails the test, since a refusal is the whole point."""
    proc = subprocess.run(
        [str(pypiron_bin), "serve", "--config", str(config)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return proc.returncode, proc.stdout + proc.stderr


def _start_server(pypiron_bin: Path, env: dict, config: Path, log_path: Path, bind: str) -> dict:
    log = open(log_path, "w")
    proc = subprocess.Popen(
        [str(pypiron_bin), "serve", "--config", str(config)],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    wait_http_ok(f"http://{bind}/simple/index.json", timeout=30.0)
    return {
        "proc": proc,
        "base_url": f"http://{bind}",
        "legacy": f"http://{bind}/legacy/",
        "simple": f"http://{bind}/simple/",
        "user": "admin",
        "password": "secret",
        "log_path": log_path,
    }


def _eventually(check, *, timeout: float = 15.0, what: str = "condition") -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def _uv_install(uv_path: str, venv: Path, simple_url: str, spec: str) -> tuple[int, str, str]:
    return run_returncode(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(venv),
            "--index-url",
            simple_url,
            "--no-cache",
            spec,
        ],
        timeout=180,
    )


def test_two_bucket_fanout_each_bucket_reachable_only_with_its_own_creds(
    minio, minio_alt, pypiron_bin, tmp_path, uv_path, uv_venv
):
    """Bucket A lives on the primary MinIO (ambient AWS_* creds); bucket B lives
    on a second MinIO with different root keys, reachable only through a
    per-bucket override that supplies both its endpoint and its scoped
    credentials. One upload must land byte-for-byte in both, and an install must
    resolve it — proof the override's credentials really are what reach B."""
    a = minio["bucket"]
    b = minio_alt["bucket"]
    buckets = f"s3://{a},s3://{b}"

    config = _write_config(
        tmp_path,
        f"""
[serve.bucket."s3://{b}"]
endpoint-url = "{minio_alt["endpoint"]}"
force-path-style = true
env-prefix = "BUCKETB_"
""",
    )

    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _base_env(bind, buckets, minio)
    # B's scoped credentials — the primary's ambient keys cannot sign for it.
    env["BUCKETB_AWS_ACCESS_KEY_ID"] = minio_alt["access_key"]
    env["BUCKETB_AWS_SECRET_ACCESS_KEY"] = minio_alt["secret_key"]

    server = _start_server(pypiron_bin, env, config, tmp_path / "server.log", bind)
    try:
        wheel = make_wheel("twocreds", "1.0", tmp_path)
        wheel_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
        upload_legacy(
            server["legacy"],
            wheel,
            username=server["user"],
            password=server["password"],
            timeout=20,
        )
        wait_for_file_in_index(server["simple"], "twocreds", wheel.name)

        key = f"packages/twocreds/{wheel.name}"
        # Landed on the primary immediately; fans out to the second container.
        assert minio_key_exists_in(minio, a, key)
        _eventually(
            lambda: minio_key_exists_in(minio_alt, b, key),
            what="artifact fanned out to the second-credentials bucket",
        )
        # Byte-equal on both, each read with that container's own keys.
        assert minio_object_sha256(minio, a, key) == wheel_sha
        assert minio_object_sha256(minio_alt, b, key) == wheel_sha

        rc, out, err = _uv_install(uv_path, uv_venv, server["simple"], "twocreds==1.0")
        assert rc == 0, f"install failed:\n{out}\n{err}"
    finally:
        kill_process_tree(server["proc"])


def test_half_configured_env_prefix_refuses_to_start(minio, minio_alt, pypiron_bin, tmp_path):
    """An `env-prefix` with its access-key half present but the secret half
    missing can never authenticate — startup must refuse and name the bucket."""
    a = minio["bucket"]
    b = minio_alt["bucket"]
    config = _write_config(
        tmp_path,
        f"""
[serve.bucket."s3://{b}"]
endpoint-url = "{minio_alt["endpoint"]}"
force-path-style = true
env-prefix = "BUCKETB_"
""",
    )
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _base_env(bind, f"s3://{a},s3://{b}", minio)
    # Only the id half — the secret half is deliberately absent.
    env["BUCKETB_AWS_ACCESS_KEY_ID"] = minio_alt["access_key"]

    rc, out = _serve_refuses(pypiron_bin, env, config)
    assert rc != 0, f"half-configured env-prefix should refuse to start:\n{out}"
    assert f"s3://{b}" in out, out
    assert "BUCKETB_AWS_SECRET_ACCESS_KEY" in out, out


def test_override_keyed_to_unknown_bucket_refuses_to_start(minio, pypiron_bin, tmp_path):
    """An override table keyed to a bucket not in `--buckets` is almost always a
    typo; startup must refuse and list the valid bucket identities."""
    a = minio["bucket"]
    config = _write_config(
        tmp_path,
        """
[serve.bucket."s3://ghost-typo"]
endpoint-url = "http://example.invalid:9000"
""",
    )
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _base_env(bind, f"s3://{a}", minio)

    rc, out = _serve_refuses(pypiron_bin, env, config)
    assert rc != 0, f"override for an unknown bucket should refuse to start:\n{out}"
    assert "s3://ghost-typo" in out, out
    # The error lists the real bucket so the operator sees the intended name.
    assert f"s3://{a}" in out, out


def test_per_bucket_endpoint_url_wins_over_global_for_that_bucket_only(
    minio, minio_alt, pypiron_bin, tmp_path
):
    """The global `--s3-endpoint-url` points every bucket at the primary MinIO.
    A per-bucket `endpoint-url` override redirects bucket B to the second MinIO
    alone: B's artifacts must land there (not on the primary), while A stays on
    the primary — proof the override steers one bucket without moving the rest."""
    a = minio["bucket"]
    b = minio_alt["bucket"]
    config = _write_config(
        tmp_path,
        f"""
[serve.bucket."s3://{b}"]
endpoint-url = "{minio_alt["endpoint"]}"
force-path-style = true
env-prefix = "BUCKETB_"
""",
    )
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _base_env(bind, f"s3://{a},s3://{b}", minio)
    env["BUCKETB_AWS_ACCESS_KEY_ID"] = minio_alt["access_key"]
    env["BUCKETB_AWS_SECRET_ACCESS_KEY"] = minio_alt["secret_key"]

    server = _start_server(pypiron_bin, env, config, tmp_path / "server.log", bind)
    try:
        wheel = make_wheel("steered", "2.0", tmp_path)
        upload_legacy(
            server["legacy"],
            wheel,
            username=server["user"],
            password=server["password"],
            timeout=20,
        )
        wait_for_file_in_index(server["simple"], "steered", wheel.name)
        key = f"packages/steered/{wheel.name}"

        # B's copy lands on the SECOND container (its override endpoint), reached
        # with the second container's own keys. Bucket B exists only on that
        # container, so a copy appearing there is itself proof the override
        # steered the write off the global endpoint — had B been reached at the
        # global (primary) endpoint, its bucket would not exist and the write
        # would never land here.
        _eventually(
            lambda: minio_key_exists_in(minio_alt, b, key),
            what="bucket B artifact on the override endpoint's container",
        )
        # A stayed on the primary — the override moved one bucket, not the rest.
        assert minio_key_exists_in(minio, a, key)
    finally:
        kill_process_tree(server["proc"])
