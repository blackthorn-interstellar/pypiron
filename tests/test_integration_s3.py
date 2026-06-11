from __future__ import annotations

import time
from pathlib import Path

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
# Let the server index any version we upload; for simplicity use same wheel as disk test
ARTIFACT_NAME = "six-1.16.0-py2.py3-none-any.whl"
PYPI_URL = (
    "https://files.pythonhosted.org/packages/d9/5a/"
    "e7c31adbe875f2abbb91bd84cf2dc52d792b5a01506781dbcf25c91daf11/"
    f"{ARTIFACT_NAME}"
)


@pytest.mark.integration
@pytest.mark.s3
def test_s3_upload_index_install(s3_server, tmp_path, uv_path, uv_venv):
    base_url = s3_server["base_url"]
    simple_url = s3_server["simple"]
    legacy_url = s3_server["legacy"]
    user = s3_server["user"]
    password = s3_server["password"]

    # Download a real wheel
    wheel_path = tmp_path / ARTIFACT_NAME
    wheel_bytes = http_get_bytes(PYPI_URL)
    wheel_path.write_bytes(wheel_bytes)
    # Upload via uv publish if possible else fallback
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
        upload_legacy(legacy_url, wheel_path, username=user, password=password)

    # Wait for indexing
    pkg_index_url = f"{simple_url}{PACKAGE}/index.json"
    deadline = time.time() + 60.0
    while time.time() < deadline:
        try:
            data = http_get_json(pkg_index_url, headers={"Accept": ACCEPT_PEP691})
            filenames = [f.get("filename") for f in data.get("files", [])]
            if ARTIFACT_NAME in filenames:
                break
        except Exception:
            pass
        time.sleep(0.3)
    else:
        pytest.fail("Timed out waiting for S3-backed package index")

    # Install from our S3-backed server into a uv-managed venv (no auth required for reads)
    py = uv_venv
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(py),
            "--index-url",
            f"{simple_url}",
            "--no-cache-dir",
            PACKAGE,
        ],
        timeout=240,
    )

    # Verify we can import the package
    run_checked([str(py), "-c", f"import {PACKAGE}; print('{PACKAGE} imported successfully')"])