from __future__ import annotations

import os
import sys
import time
import platform
import subprocess
from pathlib import Path
from typing import Dict, Iterator, Optional

import pytest

from .helpers import (
    ACCEPT_PEP691,
    cmd_exists,
    ensure_built,
    find_free_port,
    kill_process_tree,
    pypiron_binary_path,
    run_checked,
    run_returncode,
    uv_python_path,
    wait_http_ok,
)


# ----------------------------- Basic path fixtures ----------------------------


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def uv_path() -> str:
    uv = os.environ.get("UV", "")
    if uv and Path(uv).exists():
        return uv
    if not cmd_exists("uv"):
        pytest.skip("uv is required for these integration tests; not found on PATH")
    return "uv"


@pytest.fixture(scope="session")
def cargo_path() -> str:
    if not cmd_exists("cargo"):
        pytest.skip("cargo is required to build the pypiron server; not found on PATH")
    return "cargo"


@pytest.fixture(scope="session")
def pypiron_bin(repo_root: Path, cargo_path: str) -> Path:
    return ensure_built(repo_root)


@pytest.fixture(scope="session")
def pypiron_release_bin(repo_root: Path, cargo_path: str) -> Path:
    """Release binary, for perf tests — debug-build numbers are meaningless."""
    return ensure_built(repo_root, release=True)


# ----------------------------- uv venv fixture --------------------------------


@pytest.fixture(scope="function")
def uv_venv(tmp_path_factory, uv_path: str) -> Path:
    venv_dir = tmp_path_factory.mktemp("uv-venv")
    # Create the environment
    run_checked([uv_path, "venv", str(venv_dir)])
    py = uv_python_path(venv_dir)
    assert py.exists(), f"uv venv python not found at {py}"
    return py


# ---------------------------- Disk server fixture -----------------------------


def _start_disk_server(tmp_path_factory, bin_path: Path) -> Iterator[Dict[str, str]]:
    data_dir = tmp_path_factory.mktemp("pypiron-data")
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    user = "admin"
    pw = "secret"

    args = [
        str(bin_path),
        "--bind-addr",
        bind,
        "--data-dir",
        str(data_dir),
        "--basic-auth-user",
        user,
        "--basic-auth-pass",
        pw,
        "--worker-interval-secs",
        "1",
        "--job-batch-size",
        "20",
    ]

    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info")

    proc = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    # Wait for readiness
    wait_http_ok(f"http://{bind}/simple/index.json", timeout=20.0)

    yield {
        "bind": bind,
        "base_url": f"http://{bind}",
        "legacy": f"http://{bind}/legacy/",
        "simple": f"http://{bind}/simple/",
        "user": user,
        "password": pw,
        "proc": proc,  # keep for teardown
    }

    kill_process_tree(proc)


@pytest.fixture(scope="function")
def disk_server(tmp_path_factory, pypiron_bin: Path) -> Iterator[Dict[str, str]]:
    """Start pypiron in disk mode with basic auth for uploads."""
    yield from _start_disk_server(tmp_path_factory, pypiron_bin)


@pytest.fixture(scope="function")
def disk_server_release(tmp_path_factory, pypiron_release_bin: Path) -> Iterator[Dict[str, str]]:
    """Disk-mode server running the release binary (perf tests)."""
    yield from _start_disk_server(tmp_path_factory, pypiron_release_bin)


# ------------------------------ MinIO (S3) fixtures ---------------------------


@pytest.fixture(scope="function")
def minio_container() -> Optional[str]:
    """Start MinIO via Docker, return container name (or skip if docker missing)."""
    if not cmd_exists("docker"):
        pytest.skip("docker is required for S3/MinIO integration test; not found on PATH")

    name = f"pypiron-minio-{int(time.time())}"
    run_checked(
        [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "-p",
            "9000:9000",
            "-p",
            "9001:9001",
            "-e",
            "MINIO_ROOT_USER=minioadmin",
            "-e",
            "MINIO_ROOT_PASSWORD=minioadmin",
            "minio/minio",
            "server",
            "/data",
            "--console-address",
            ":9001",
        ]
    )

    # Health
    wait_http_ok("http://127.0.0.1:9000/minio/health/ready", timeout=60.0)

    # Create bucket using minio/mc; prefer host.docker.internal for cross-OS
    bucket = "s3pypi"
    created = False

    # Attempt using host.docker.internal
    rc, out, err = run_returncode(
        [
            "docker",
            "run",
            "--rm",
            "-e",
            "MC_HOST_local=http://minioadmin:minioadmin@host.docker.internal:9000",
            "minio/mc",
            "mb",
            "--ignore-existing",
            f"local/{bucket}",
        ]
    )
    if rc == 0:
        created = True
    else:
        # Fallback to --network host (Linux)
        rc2, out2, err2 = run_returncode(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "host",
                "-e",
                "MC_HOST_local=http://minioadmin:minioadmin@127.0.0.1:9000",
                "minio/mc",
                "mb",
                "--ignore-existing",
                f"local/{bucket}",
            ]
        )
        created = rc2 == 0

    if not created:
        # Cleanup and skip
        run_returncode(["docker", "rm", "-f", name])
        pytest.skip("Unable to create MinIO bucket using minio/mc (check Docker networking)")

    try:
        yield name
    finally:
        run_returncode(["docker", "rm", "-f", name])


@pytest.fixture(scope="function")
def s3_server(pypiron_bin: Path, minio_container: Optional[str]):
    """Run pypiron configured to use the MinIO S3 backend."""
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = os.environ.copy()
    env.update(
        {
            "PYPIRON_STORAGE": "s3",
            "PYPIRON_S3_BUCKET": "s3pypi",
            "AWS_REGION": "us-east-1",
            "PYPIRON_S3_ENDPOINT_URL": "http://127.0.0.1:9000",
            "PYPIRON_S3_FORCE_PATH_STYLE": "true",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "PYPIRON_BIND_ADDR": bind,
            "PYPIRON_WORKER_INTERVAL_SECS": "2",
            "PYPIRON_JOB_BATCH_SIZE": "20",
            "PYPIRON_BASIC_AUTH_USER": "twine",
            "PYPIRON_BASIC_AUTH_PASS": "secret",
            "RUST_LOG": "info",
        }
    )

    proc = subprocess.Popen([str(pypiron_bin)], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    wait_http_ok(f"http://{bind}/simple/index.json", timeout=30.0)

    yield {
        "bind": bind,
        "base_url": f"http://{bind}",
        "legacy": f"http://{bind}/legacy/",
        "simple": f"http://{bind}/simple/",
        "user": "twine",
        "password": "secret",
        "proc": proc,
    }

    kill_process_tree(proc)