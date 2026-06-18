"""Round trip: upload a real wheel -> indexed -> bytes intact -> installable.

Runs against both backends: disk (tmpdir) and S3 (MinIO in Docker).
"""

from __future__ import annotations

import pytest

from .helpers import (
    download_pypi_wheel,
    get_index_json,
    http_get_bytes,
    run_checked,
    sha256_file,
    wait_for_file_in_index,
    wait_for_project_in_global,
)

PACKAGE = "six"
VERSION = "1.17.0"

pytestmark = pytest.mark.integration


@pytest.fixture(
    params=[
        pytest.param("disk_server", id="disk"),
        pytest.param("s3_server", id="s3", marks=pytest.mark.s3),
    ]
)
def server(request):
    return request.getfixturevalue(request.param)


@pytest.mark.compat("uv", "upload")
@pytest.mark.compat("uv", "install")
def test_upload_index_download_install(server, tmp_path, uv_path, uv_venv):
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    orig_sha = sha256_file(wheel_path)

    # Upload with a real client (uv publish speaks the legacy API like twine).
    run_checked(
        [
            uv_path,
            "publish",
            "--publish-url",
            server["legacy"],
            "--username",
            server["user"],
            "--password",
            server["password"],
            str(wheel_path),
        ],
        timeout=120,
    )

    # Appears in the package index, then the global index (in that order).
    wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name)
    wait_for_project_in_global(server["simple"], PACKAGE)
    global_idx = get_index_json(server["simple"])
    assert PACKAGE in [p.get("name") for p in global_idx.get("projects", [])]

    # Downloaded bytes match the original sha256.
    downloaded = tmp_path / "downloaded.whl"
    downloaded.write_bytes(
        http_get_bytes(f"{server['base_url']}/files/{PACKAGE}/{wheel_path.name}")
    )
    assert sha256_file(downloaded) == orig_sha

    # Installs into a fresh venv and imports.
    py = uv_venv
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(py),
            "--index-url",
            server["simple"],
            "--no-cache-dir",
            f"{PACKAGE}=={VERSION}",
        ],
        timeout=180,
    )
    run_checked([str(py), "-c", f"import {PACKAGE}"])
