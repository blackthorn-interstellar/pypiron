"""Milestone 2: filenames are immutable — re-uploads are rejected with 409."""

from __future__ import annotations

import pytest

from .helpers import download_pypi_wheel, upload_legacy, wait_for_file_in_index

PACKAGE = "six"
OLD_VERSION = "1.16.0"
NEW_VERSION = "1.17.0"

pytestmark = pytest.mark.integration


def test_reupload_same_filename_rejected(disk_server, tmp_path):
    wheel_path = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    creds = {"username": disk_server["user"], "password": disk_server["password"]}

    upload_legacy(disk_server["legacy"], wheel_path, **creds)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)

    # Identical bytes, same filename: still rejected.
    upload_legacy(disk_server["legacy"], wheel_path, expect_status=409, **creds)

    # Different bytes under an existing filename: rejected — nobody can swap
    # bytes under a published version.
    impostor = tmp_path / "impostor" / wheel_path.name
    impostor.parent.mkdir()
    impostor.write_bytes(download_pypi_wheel(PACKAGE, OLD_VERSION, tmp_path).read_bytes())
    upload_legacy(disk_server["legacy"], impostor, expect_status=409, **creds)
