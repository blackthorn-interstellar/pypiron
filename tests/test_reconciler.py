"""Milestone 5: the reconciler is the self-heal backbone.

A lost dirty marker must be harmless: files written straight to storage with
no marker (the end state of "marker deleted mid-flight") get indexed by the
periodic sweep, and stale index entries pointing at deleted files get pruned.
"""

from __future__ import annotations

import shutil
import time

import pytest

from .helpers import (
    ACCEPT_PEP691,
    download_pypi_wheel,
    get_index_json,
    http_get,
    sha256_file,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
OLD_VERSION = "1.16.0"
NEW_VERSION = "1.17.0"

pytestmark = pytest.mark.integration


def test_lost_marker_is_harmless(disk_server_fast_reconcile, tmp_path):
    server = disk_server_fast_reconcile

    # Artifact dropped straight into the truth tree: no upload, no sidecar,
    # no dirty marker. Exactly what remains if a marker is lost mid-flight.
    wheel_path = download_pypi_wheel(PACKAGE, OLD_VERSION, tmp_path)
    pkg_dir = server["data_dir"] / "packages" / PACKAGE
    pkg_dir.mkdir(parents=True)
    (pkg_dir / wheel_path.name).write_bytes(wheel_path.read_bytes())

    # The sweep indexes it and backfills the sidecar without any event.
    index = wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name, timeout=15.0)
    (entry,) = [f for f in index["files"] if f["filename"] == wheel_path.name]
    assert entry["hashes"]["sha256"] == sha256_file(wheel_path)
    assert (pkg_dir / f"{wheel_path.name}.meta.json").exists()

    global_idx = get_index_json(server["simple"])
    assert PACKAGE in [p["name"] for p in global_idx["projects"]]


def test_reconcile_prunes_stale_views(disk_server_fast_reconcile, tmp_path):
    server = disk_server_fast_reconcile
    wheel_path = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    upload_legacy(
        server["legacy"], wheel_path, username=server["user"], password=server["password"]
    )
    wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name)

    # Nuke the package's truth directory outright, leaving stale views behind.
    shutil.rmtree(server["data_dir"] / "packages" / PACKAGE)

    deadline = time.time() + 15.0
    while time.time() < deadline:
        code, _, _ = http_get(f"{server['simple']}{PACKAGE}/", headers={"Accept": ACCEPT_PEP691})
        if code == 404:
            break
        time.sleep(0.2)
    else:
        pytest.fail("reconcile did not prune the stale package index")

    global_idx = get_index_json(server["simple"])
    assert PACKAGE not in [p["name"] for p in global_idx["projects"]], (
        "reconcile must remove vanished packages from the global index"
    )
