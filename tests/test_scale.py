"""Scale edge cases the listing layer must survive.

Regression for the 1000-package truncation: S3 ListObjectsV2 returns at most
1000 common prefixes per page, and an unpaginated `list_dirs` silently capped
the global index (and the reconciler's sweep) at the first thousand packages.
Found at 10k-package scale on real S3; pinned here at 1,100 via MinIO.
"""

from __future__ import annotations

import io
import json
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pytest

from .helpers import http_get, upload_legacy

pytestmark = [pytest.mark.integration, pytest.mark.s3]

# Just past the ListObjectsV2 page size, plus headroom for pre-existing
# packages other fixtures may have left in the bucket.
PACKAGE_COUNT = 1100


def tiny_wheel(tmp_path, name: str):
    """Minimal valid wheel on disk; enough for upload + METADATA extraction."""
    version = "1.0.0"
    di = f"{name.replace('-', '_')}-{version}.dist-info"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr(f"{di}/METADATA", f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
        zf.writestr(f"{di}/WHEEL", "Wheel-Version: 1.0\nTag: py3-none-any\n")
        zf.writestr(f"{di}/RECORD", "")
    path = tmp_path / f"{name.replace('-', '_')}-{version}-py3-none-any.whl"
    path.write_bytes(buf.getvalue())
    return path


def test_global_index_spans_more_than_one_listing_page(s3_server, tmp_path):
    server = s3_server
    names = [f"scaletest-{i:04d}" for i in range(PACKAGE_COUNT)]

    def upload(name: str) -> None:
        upload_legacy(
            server["legacy"],
            tiny_wheel(tmp_path, name),
            username=server["user"],
            password=server["password"],
        )

    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(upload, names))

    # The global index changes when the package set changes; the worker must
    # eventually list EVERY package directory — page two included.
    deadline = time.time() + 300
    missing: set = set(names)
    while time.time() < deadline and missing:
        status, body, _ = http_get(f"{server['base_url']}/simple/index.json")
        if status == 200:
            projects = {p["name"] for p in json.loads(body)["projects"]}
            missing = set(names) - projects
        time.sleep(2)

    assert not missing, (
        f"{len(missing)} of {PACKAGE_COUNT} packages absent from the global index "
        f"(unpaginated list_dirs caps at 1000): sample {sorted(missing)[:5]}"
    )
