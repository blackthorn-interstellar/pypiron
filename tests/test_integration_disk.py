from __future__ import annotations

import json
from pathlib import Path
import time

import pytest

from .helpers import (
    ACCEPT_PEP691,
    http_get_bytes,
    http_get_json,
    run_checked,
    run_returncode,
    sha256_file,
    upload_legacy,
)


PACKAGE = "six"
VERSION = "1.16.0"
ARTIFACT_NAME = f"{PACKAGE}-{VERSION}-py2.py3-none-any.whl"
PYPI_URL = (
    "https://files.pythonhosted.org/packages/d9/5a/"
    "e7c31adbe875f2abbb91bd84cf2dc52d792b5a01506781dbcf25c91daf11/"
    f"{ARTIFACT_NAME}"
)


@pytest.mark.integration
def test_disk_upload_index_download_install(disk_server, tmp_path, uv_path, uv_venv):
    base_url = disk_server["base_url"]
    simple_url = disk_server["simple"]
    legacy_url = disk_server["legacy"]
    user = disk_server["user"]
    password = disk_server["password"]

    wheel_path = tmp_path / ARTIFACT_NAME
    # Download a real wheel from public PyPI (small pure-Python file)
    wheel_bytes = http_get_bytes(PYPI_URL)
    wheel_path.write_bytes(wheel_bytes)
    orig_sha = sha256_file(wheel_path)

    # Try upload via uv publish; if it fails, fallback to direct legacy HTTP
    rc, out, err = run_returncode(
        [
            uv_path,
            "publish",
            "--publish-url",
            legacy_url,
            "--username",
            user,
            "--password",
            password,
            str(wheel_path),
        ]
    )
    if rc != 0:
        # Fallback to manual legacy upload
        upload_legacy(legacy_url, wheel_path, username=user, password=password)

    # Wait for background indexing to include our artifact
    pkg_index_url = f"{simple_url}{PACKAGE}/index.json"
    deadline = time.time() + 30.0
    found = False
    while time.time() < deadline and not found:
        try:
            data = http_get_json(pkg_index_url, headers={"Accept": ACCEPT_PEP691})
            files = [f.get("filename") for f in data.get("files", [])]
            if ARTIFACT_NAME in files:
                found = True
                break
        except RuntimeError:
            # Index not ready yet (404), keep polling
            pass
        time.sleep(0.2)
    assert found, "Timed out waiting for package index to include our wheel"

    # Global index lists the project
    global_idx = http_get_json(f"{simple_url}index.json", headers={"Accept": ACCEPT_PEP691})
    projects = [p.get("name") for p in global_idx.get("projects", [])]
    assert PACKAGE in projects, "Global index did not include the project"

    # Download back via /files and verify integrity
    downloaded = tmp_path / "downloaded.whl"
    file_bytes = http_get_bytes(f"{base_url}/files/{PACKAGE}/{ARTIFACT_NAME}")
    downloaded.write_bytes(file_bytes)
    down_sha = sha256_file(downloaded)
    assert down_sha == orig_sha, f"Downloaded file hash mismatch: {down_sha} != {orig_sha}"

    # Install from our server into a uv-managed venv
    py = uv_venv
    index_url = f"http://{user}:{password}@{disk_server['bind']}/simple/"
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(py),
            "--index-url",
            index_url,
            "--no-cache-dir",
            f"{PACKAGE}=={VERSION}",
        ],
        timeout=180,
    )

    # Verify import
    run_checked([str(py), "-c", f"import {PACKAGE}; print('{PACKAGE} imported successfully')"])