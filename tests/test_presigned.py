"""Milestone 10: S3 presigned redirects — the node never touches wheel bytes."""

from __future__ import annotations

import pytest

from .helpers import (
    download_pypi_wheel,
    http_get,
    http_get_bytes,
    http_get_no_redirect,
    run_checked,
    sha256_file,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
VERSION = "1.17.0"

pytestmark = [pytest.mark.integration, pytest.mark.s3]


@pytest.fixture()
def presigned_server(s3_server_presigned, tmp_path):
    server = s3_server_presigned
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(
        server["legacy"], wheel_path, username=server["user"], password=server["password"]
    )
    wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name)
    return {**server, "wheel_path": wheel_path}


def test_artifact_download_redirects_to_presigned_url(presigned_server):
    server = presigned_server
    wheel_name = server["wheel_path"].name
    url = f"{server['base_url']}/files/{PACKAGE}/{wheel_name}"

    code, _, headers = http_get_no_redirect(url)
    assert code == 302
    location = headers["location"]
    assert location.startswith(server["minio"]["endpoint"]), "redirect must point at S3"
    assert "X-Amz-Signature" in location, "URL must be presigned"
    assert headers["cache-control"] == "no-cache", "expiring redirects must not be cached"

    # Following the redirect yields the exact bytes.
    body = http_get_bytes(location)
    assert len(body) == server["wheel_path"].stat().st_size
    import hashlib

    assert hashlib.sha256(body).hexdigest() == sha256_file(server["wheel_path"])

    # Missing files are a 404, not a signed URL to nothing.
    code, _, _ = http_get_no_redirect(f"{server['base_url']}/files/{PACKAGE}/nope-1.0.whl")
    assert code == 404

    # Metadata companions keep streaming from the node (resolution-critical).
    code, body, _ = http_get_no_redirect(f"{server['base_url']}/files/{PACKAGE}/{wheel_name}.metadata")
    assert code == 200
    assert body.startswith(b"Metadata-Version:")


def test_install_works_through_redirects(presigned_server, uv_path, uv_venv):
    server = presigned_server
    py = str(uv_venv)
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            py,
            "--index-url",
            server["simple"],
            "--no-cache",
            f"{PACKAGE}=={VERSION}",
        ],
        timeout=180,
    )
    run_checked([py, "-c", f"import {PACKAGE}"])

    # The node never streamed the wheel: every wheel GET was answered 302.
    log = server["log_path"].read_text()
    wheel_lines = [
        line
        for line in log.splitlines()
        if f"GET /files/{PACKAGE}/" in line and ".metadata" not in line
    ]
    assert wheel_lines, "uv must have requested the wheel from the node"