"""Milestone 10: artifact delivery — presigned redirects vs streaming, per client.

`redirect` mode: every artifact GET is a 302 to a presigned URL; the node
never touches wheel bytes. `auto` mode (the default): only clients whose
caches survive presigned-URL churn (uv) are redirected; everyone else — pip,
whose HTTP cache is keyed by URL, and unknown clients — gets streamed bytes
under the stable /files/ URL with immutable cache headers.
"""

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


def _seed_wheel(server, tmp_path):
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(
        server["legacy"], wheel_path, username=server["user"], password=server["password"]
    )
    wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name)
    return {**server, "wheel_path": wheel_path}


@pytest.fixture()
def presigned_server(s3_server_presigned, tmp_path):
    return _seed_wheel(s3_server_presigned, tmp_path)


@pytest.fixture()
def auto_server(s3_server, tmp_path):
    """S3-backed server in the default `auto` delivery mode."""
    return _seed_wheel(s3_server, tmp_path)


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

    # Missing files redirect too — existence is the index's job, and the
    # redirect path costs zero network round trips. S3 answers the follow-up
    # GET with its own 404 (the server's creds have ListBucket, so it's not
    # a 403).
    missing = f"{server['base_url']}/files/{PACKAGE}/nope-1.0.whl"
    code, _, headers = http_get_no_redirect(missing)
    assert code == 302
    assert "X-Amz-Signature" in headers["location"]
    code, _, _ = http_get(headers["location"])
    assert code == 404

    # Metadata companions keep streaming from the node (resolution-critical).
    code, body, _ = http_get_no_redirect(
        f"{server['base_url']}/files/{PACKAGE}/{wheel_name}.metadata"
    )
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
        if f"path=/files/{PACKAGE}/" in line and ".metadata" not in line
    ]
    assert wheel_lines, "uv must have requested the wheel from the node"


IMMUTABLE = "public, max-age=31536000, immutable"


def test_auto_mode_dispatches_on_client(auto_server):
    """auto: uv is redirected; pip and unknown clients get streamed bytes."""
    server = auto_server
    wheel_name = server["wheel_path"].name
    url = f"{server['base_url']}/files/{PACKAGE}/{wheel_name}"

    # uv caches wheels by index + filename — presigned-URL churn is free.
    code, _, headers = http_get_no_redirect(url, headers={"User-Agent": "uv/0.7.0"})
    assert code == 302
    assert "X-Amz-Signature" in headers["location"]
    assert headers["cache-control"] == "no-cache"

    # pip's HTTP cache is keyed by URL — a fresh presigned URL per request
    # would force a full re-download on every install. Stream instead, under
    # the stable URL pip can cache forever.
    code, body, headers = http_get_no_redirect(
        url, headers={"User-Agent": 'pip/25.0 {"installer":{"name":"pip"}}'}
    )
    assert code == 200
    assert headers["cache-control"] == IMMUTABLE
    assert len(body) == server["wheel_path"].stat().st_size

    # Unknown clients are assumed URL-keyed: streaming is always correct,
    # a defeated cache is not.
    code, _, headers = http_get_no_redirect(url, headers={"User-Agent": "curl/8.0"})
    assert code == 200
    assert headers["cache-control"] == IMMUTABLE

    # Metadata companions stream even for redirect-safe clients.
    code, body, _ = http_get_no_redirect(f"{url}.metadata", headers={"User-Agent": "uv/0.7.0"})
    assert code == 200
    assert body.startswith(b"Metadata-Version:")
