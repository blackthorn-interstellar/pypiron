"""Milestone 3: cache-correctness — ETag revalidation, immutable artifacts, Range."""

from __future__ import annotations

import pytest

from .helpers import (
    ACCEPT_PEP691,
    download_pypi_wheel,
    http_get,
    upload_legacy,
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
def server(request, tmp_path):
    server = request.getfixturevalue(request.param)
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(
        server["legacy"], wheel_path, username=server["user"], password=server["password"]
    )
    wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name)
    # The global index is written after the package index; wait for it too,
    # or the ETag assertions race the worker.
    wait_for_project_in_global(server["simple"], PACKAGE)
    return {**server, "wheel_path": wheel_path}


def test_index_etag_roundtrips_304(server):
    for url, accept in [
        (f"{server['simple']}", None),
        (f"{server['simple']}", ACCEPT_PEP691),
        (f"{server['simple']}{PACKAGE}/", None),
        (f"{server['simple']}{PACKAGE}/", ACCEPT_PEP691),
    ]:
        req_headers = {"Accept": accept} if accept else {}
        code, body, headers = http_get(url, headers=req_headers)
        assert code == 200
        assert headers["cache-control"] == "no-cache"
        etag = headers["etag"]
        assert etag.startswith('"') and etag.endswith('"')

        code, body, headers = http_get(url, headers={**req_headers, "If-None-Match": etag})
        assert code == 304, f"conditional GET must revalidate to 304 for {url}"
        assert body == b""
        assert headers["etag"] == etag

        # A non-matching validator gets fresh content.
        code, body, _ = http_get(url, headers={**req_headers, "If-None-Match": '"stale"'})
        assert code == 200
        assert body


def test_artifact_cached_immutably(server):
    url = f"{server['base_url']}/files/{PACKAGE}/{server['wheel_path'].name}"
    code, body, headers = http_get(url)
    assert code == 200
    assert headers["cache-control"] == "public, max-age=31536000, immutable"
    assert body == server["wheel_path"].read_bytes()


def test_artifact_range_requests(server):
    wheel_bytes = server["wheel_path"].read_bytes()
    size = len(wheel_bytes)
    url = f"{server['base_url']}/files/{PACKAGE}/{server['wheel_path'].name}"

    # First 100 bytes
    code, body, headers = http_get(url, headers={"Range": "bytes=0-99"})
    assert code == 206
    assert body == wheel_bytes[:100]
    assert headers["content-range"] == f"bytes 0-99/{size}"

    # Open-ended tail
    code, body, headers = http_get(url, headers={"Range": f"bytes={size - 50}-"})
    assert code == 206
    assert body == wheel_bytes[-50:]

    # Suffix range
    code, body, headers = http_get(url, headers={"Range": "bytes=-100"})
    assert code == 206
    assert body == wheel_bytes[-100:]

    # Unsatisfiable
    code, _, _ = http_get(url, headers={"Range": f"bytes={size + 1000}-"})
    assert code == 416
