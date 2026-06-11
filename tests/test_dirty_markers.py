"""Milestone 4: dirty markers drive index rebuilds; the queue is gone."""

from __future__ import annotations

import time

import pytest

from .helpers import download_pypi_wheel, upload_legacy, wait_for_file_in_index

PACKAGE = "six"
OLD_VERSION = "1.16.0"
NEW_VERSION = "1.17.0"

pytestmark = pytest.mark.integration


def _wait_dirty_empty(data_dir, timeout=15.0):
    dirty = data_dir / "_dirty"
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not dirty.exists() or not any(dirty.iterdir()):
            return
        time.sleep(0.1)
    raise TimeoutError(f"_dirty/ still has markers: {list(dirty.iterdir())}")


def test_markers_processed_and_no_queue(disk_server, tmp_path):
    wheel_path = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel_path,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)
    _wait_dirty_empty(disk_server["data_dir"])

    # The copy-then-delete queue is gone for good.
    assert not (disk_server["data_dir"] / "_internal").exists()


def test_global_index_rebuilt_only_on_name_set_change(disk_server, tmp_path):
    creds = {"username": disk_server["user"], "password": disk_server["password"]}
    global_html = disk_server["data_dir"] / "simple" / "index.html"

    # First upload introduces a new name: global index must be rebuilt.
    old_wheel = download_pypi_wheel(PACKAGE, OLD_VERSION, tmp_path)
    upload_legacy(disk_server["legacy"], old_wheel, **creds)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, old_wheel.name)
    _wait_dirty_empty(disk_server["data_dir"])
    assert PACKAGE in global_html.read_text()
    baseline_mtime = global_html.stat().st_mtime_ns

    # Second file for the same package: name set unchanged, global untouched.
    new_wheel = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    upload_legacy(disk_server["legacy"], new_wheel, **creds)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, new_wheel.name)
    _wait_dirty_empty(disk_server["data_dir"])
    assert global_html.stat().st_mtime_ns == baseline_mtime, (
        "global index must not be rewritten when the package-name set is unchanged"
    )
