"""Milestone 1: write-time metadata sidecars (<filename>.meta.json).

The storage layout is the contract (DESIGN.md), so asserting sidecar files on
the disk backend is blackbox testing of that contract.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from .helpers import (
    download_pypi_wheel,
    sha256_file,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
OLD_VERSION = "1.16.0"
NEW_VERSION = "1.17.0"

pytestmark = pytest.mark.integration


def _upload(server, wheel_path, **kwargs):
    return upload_legacy(
        server["legacy"],
        wheel_path,
        username=server["user"],
        password=server["password"],
        **kwargs,
    )


def test_sidecar_written_at_upload(disk_server, tmp_path):
    wheel_path = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    _upload(disk_server, wheel_path)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)

    sidecar_path = disk_server["data_dir"] / "packages" / PACKAGE / f"{wheel_path.name}.meta.json"
    assert sidecar_path.exists(), "sidecar must be written at upload time"
    sc = json.loads(sidecar_path.read_text())
    assert sc["sha256"] == sha256_file(wheel_path)
    assert sc["size"] == wheel_path.stat().st_size
    assert sc["version"] == NEW_VERSION
    assert sc["yanked"] is False
    uploaded = datetime.fromisoformat(sc["upload-time"].replace("Z", "+00:00"))
    assert abs(datetime.now(timezone.utc) - uploaded) < timedelta(hours=1)


def test_upload_with_bad_digest_rejected(disk_server, tmp_path):
    wheel_path = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    _upload(disk_server, wheel_path, fields={"sha256_digest": "0" * 64}, expect_status=400)

    # The artifact must not have been stored.
    artifact = disk_server["data_dir"] / "packages" / PACKAGE / wheel_path.name
    assert not artifact.exists(), "artifact must not be written when the digest check fails"


def test_sidecar_backfilled_for_legacy_files(disk_server, tmp_path):
    # A file that predates sidecars: dropped straight into the packages tree.
    legacy_wheel = download_pypi_wheel(PACKAGE, OLD_VERSION, tmp_path)
    pkg_dir = disk_server["data_dir"] / "packages" / PACKAGE
    pkg_dir.mkdir(parents=True)
    (pkg_dir / legacy_wheel.name).write_bytes(legacy_wheel.read_bytes())

    # Any rebuild of the package backfills it; an upload triggers one.
    new_wheel = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    _upload(disk_server, new_wheel)
    index = wait_for_file_in_index(disk_server["simple"], PACKAGE, legacy_wheel.name)

    sidecar_path = pkg_dir / f"{legacy_wheel.name}.meta.json"
    assert sidecar_path.exists(), "rebuild must backfill missing sidecars"
    sc = json.loads(sidecar_path.read_text())
    assert sc["sha256"] == sha256_file(legacy_wheel)

    (entry,) = [f for f in index["files"] if f["filename"] == legacy_wheel.name]
    assert entry["hashes"]["sha256"] == sha256_file(legacy_wheel)


def test_index_upload_time_comes_from_sidecar(disk_server, tmp_path):
    old_wheel = download_pypi_wheel(PACKAGE, OLD_VERSION, tmp_path)
    _upload(disk_server, old_wheel)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, old_wheel.name)

    # Rewrite the sidecar's upload-time to a historical date (storage-credential
    # backdating, exactly what mirroring does), then trigger a rebuild.
    sidecar_path = disk_server["data_dir"] / "packages" / PACKAGE / f"{old_wheel.name}.meta.json"
    sc = json.loads(sidecar_path.read_text())
    sc["upload-time"] = "2020-01-01T00:00:00Z"
    sidecar_path.write_text(json.dumps(sc))

    new_wheel = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    _upload(disk_server, new_wheel)
    index = wait_for_file_in_index(disk_server["simple"], PACKAGE, new_wheel.name)

    (entry,) = [f for f in index["files"] if f["filename"] == old_wheel.name]
    assert entry["upload-time"] == "2020-01-01T00:00:00Z", (
        "index upload-time must come from the sidecar, not storage mtime"
    )


def test_corrupt_sidecar_omits_file_never_fabricates(disk_server, tmp_path):
    """A present-but-unreadable sidecar must not be silently rebuilt — that
    would reset a security yank to false. The file drops out of the index."""
    old_wheel = download_pypi_wheel(PACKAGE, OLD_VERSION, tmp_path)
    new_wheel = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    _upload(disk_server, old_wheel)
    _upload(disk_server, new_wheel)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, old_wheel.name)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, new_wheel.name)

    sidecar_path = disk_server["data_dir"] / "packages" / PACKAGE / f"{old_wheel.name}.meta.json"
    sidecar_path.write_text("{corrupt json!!")

    # Trigger a rebuild via the other file's yank flip.
    import time as _time

    from .helpers import http_request_auth

    creds = {"username": disk_server["user"], "password": disk_server["password"]}
    yank_url = f"{disk_server['base_url']}/files/{PACKAGE}/{new_wheel.name}/yank"
    code, _, _ = http_request_auth("POST", yank_url, **creds)
    assert code == 200

    from .helpers import get_index_json

    deadline = _time.time() + 15.0
    while _time.time() < deadline:
        doc = get_index_json(disk_server["simple"], PACKAGE)
        names = [f["filename"] for f in doc["files"]]
        if old_wheel.name not in names and new_wheel.name in names:
            break
        _time.sleep(0.2)
    else:
        raise AssertionError("corrupt-sidecar file should be omitted from the index")

    # The corrupt sidecar was not overwritten with fabricated metadata.
    assert sidecar_path.read_text() == "{corrupt json!!"


def test_invalid_package_name_rejected(disk_server, tmp_path):
    wheel_path = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    _upload(
        disk_server,
        wheel_path,
        fields={"name": "../escape"},
        expect_status=400,
    )
    assert not (disk_server["data_dir"] / "escape").exists()
