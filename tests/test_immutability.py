"""Milestone 2: filenames are immutable — re-uploads are rejected with 409."""

from __future__ import annotations

import json
import shutil

import pytest

from .helpers import download_pypi_wheel, make_wheel, upload_legacy, wait_for_file_in_index

PACKAGE = "six"
OLD_VERSION = "1.16.0"
NEW_VERSION = "1.17.0"

pytestmark = pytest.mark.integration


def test_reupload_same_filename_rejected(disk_server, tmp_path):
    wheel_path = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    creds = {"username": disk_server["user"], "password": disk_server["password"]}

    upload_legacy(disk_server["legacy"], wheel_path, **creds)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)

    # Identical bytes, same filename: still rejected. Sync clients match on this
    # text, so it is a wire contract.
    _, body = upload_legacy(disk_server["legacy"], wheel_path, expect_status=409, **creds)
    assert body.startswith(b"File already exists"), body

    # Different bytes under an existing filename: rejected — nobody can swap
    # bytes under a published version.
    impostor = tmp_path / "impostor" / wheel_path.name
    impostor.parent.mkdir()
    impostor.write_bytes(download_pypi_wheel(PACKAGE, OLD_VERSION, tmp_path).read_bytes())
    upload_legacy(disk_server["legacy"], impostor, expect_status=409, **creds)


def test_a_retry_finishes_an_upload_interrupted_after_its_bytes_landed(disk_server, tmp_path):
    """An upload that failed after its bytes landed (sidecar PUT, fence HEAD, a
    client timeout) leaves the body with no sidecar naming it. Retrying the same
    bytes finishes the write — 200 and the uploader's own full sidecar — instead
    of a 409 for its own bytes. Different bytes on such a body still 409."""
    creds = {"username": disk_server["user"], "password": disk_server["password"]}
    data_dir = disk_server["data_dir"]

    wheel = make_wheel("retryresume", "1.0.0", tmp_path)
    pkg_dir = data_dir / "packages" / "retryresume"
    pkg_dir.mkdir(parents=True)
    shutil.copyfile(wheel, pkg_dir / wheel.name)  # the bytes landed; nothing else did

    upload_legacy(disk_server["legacy"], wheel, fields={"requires_python": ">=3.9"}, **creds)
    wait_for_file_in_index(disk_server["simple"], "retryresume", wheel.name)
    sidecar = json.loads((pkg_dir / f"{wheel.name}.meta.json").read_text())
    assert sidecar["requires-python"] == ">=3.9", sidecar  # the upload's, not a backfill's
    # Finished means finished: the next identical upload is an ordinary duplicate.
    upload_legacy(disk_server["legacy"], wheel, expect_status=409, **creds)

    other = make_wheel("retryresumeb", "1.0.0", tmp_path)
    other_dir = data_dir / "packages" / "retryresumeb"
    other_dir.mkdir(parents=True)
    (other_dir / other.name).write_bytes(b"someone else's bytes")
    upload_legacy(disk_server["legacy"], other, expect_status=409, **creds)
