"""Index rebuilds reuse unchanged sidecars, and never a changed one.

An upload to an N-file package used to re-read all N sidecars to render one new
line. The worker now remembers each sidecar it parsed under the listing's change
detector (ETag on S3, nanosecond mtime + size on disk), so the next rebuild reads
only what moved. The two things that must hold, on every backend: a yank (which
rewrites the sidecar) still reaches the index, and an upload's reads stop
scaling with the package's size.
"""

from __future__ import annotations

import re
import time

import pytest

from .helpers import (
    get_index_json,
    http_get,
    http_request_auth,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration

PKG = "reusepkg"
FILES = 60


def _reads(server) -> int:
    _, body, _ = http_get(f"{server['base_url']}/metrics")
    m = re.search(r'^pypiron_storage_ops_total\{op="read"\} (\d+)$', body.decode(), re.M)
    assert m, "storage read counter missing from /metrics"
    return int(m.group(1))


def _yanked(server, filename):
    files = get_index_json(server["simple"], PKG)["files"]
    return next(f["yanked"] for f in files if f["filename"] == filename)


def _await_yanked(server, filename, want, timeout=30.0):
    deadline = time.monotonic() + timeout
    while (got := _yanked(server, filename)) != want:
        assert time.monotonic() < deadline, f"yanked stayed {got!r}, wanted {want!r}"
        time.sleep(0.1)


def _exercise(server, tmp_path):
    creds = {"username": server["user"], "password": server["password"]}
    wheels = [make_wheel(PKG, f"1.0.{i}", tmp_path) for i in range(FILES)]
    for wheel in wheels:
        upload_legacy(server["legacy"], wheel, **creds)
    wait_for_file_in_index(server["simple"], PKG, wheels[-1].name)

    # One more upload: its rebuild lists all FILES + 1 sidecars but reads only
    # the new one (plus the package's fixed-cost objects).
    before = _reads(server)
    extra = make_wheel(PKG, "2.0", tmp_path)
    upload_legacy(server["legacy"], extra, **creds)
    wait_for_file_in_index(server["simple"], PKG, extra.name)
    spent = _reads(server) - before
    assert spent < FILES // 2, f"an upload to a {FILES}-file package cost {spent} reads"

    # A yank rewrites the sidecar: the memo must not serve the old parse.
    target = wheels[0]
    yank_url = f"{server['base_url']}/files/{PKG}/{target.name}/yank"
    code, _, _ = http_request_auth("POST", yank_url, data=b"bad build", **creds)
    assert code == 200
    _await_yanked(server, target.name, "bad build")

    code, _, _ = http_request_auth("DELETE", yank_url, **creds)
    assert code == 200
    _await_yanked(server, target.name, False)

    # And a later rebuild (another upload) keeps the un-yank.
    later = make_wheel(PKG, "3.0", tmp_path)
    upload_legacy(server["legacy"], later, **creds)
    wait_for_file_in_index(server["simple"], PKG, later.name)
    assert _yanked(server, target.name) is False
    assert len(get_index_json(server["simple"], PKG)["files"]) == FILES + 2


def test_disk_rebuild_reuses_unchanged_sidecars_only(disk_server, tmp_path):
    _exercise(disk_server, tmp_path)


@pytest.mark.s3
def test_s3_rebuild_reuses_unchanged_sidecars_only(s3_server, tmp_path):
    _exercise(s3_server, tmp_path)
