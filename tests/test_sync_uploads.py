"""Milestone 12: synchronous uploads — publish-then-install never races."""

from __future__ import annotations

import pytest

from .helpers import (
    download_pypi_wheel,
    get_index_json,
    run_checked,
    upload_legacy,
)

PACKAGE = "six"
OLD_VERSION = "1.16.0"
NEW_VERSION = "1.17.0"

pytestmark = pytest.mark.integration


def test_upload_returns_only_after_index_visibility(
    disk_server_sync_uploads, tmp_path, uv_path, uv_venv
):
    server = disk_server_sync_uploads
    creds = {"username": server["user"], "password": server["password"]}

    # No polling anywhere: the moment the upload returns, the index has it.
    for version in (OLD_VERSION, NEW_VERSION):
        wheel = download_pypi_wheel(PACKAGE, version, tmp_path)
        upload_legacy(server["legacy"], wheel, **creds)
        index = get_index_json(server["simple"], PACKAGE)
        assert wheel.name in [f["filename"] for f in index["files"]], (
            "a synchronous upload must be index-visible the instant it returns"
        )

    # The CI pattern end-to-end: publish, then immediately install.
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
            f"{PACKAGE}=={NEW_VERSION}",
        ],
        timeout=180,
    )
    run_checked([py, "-c", f"import {PACKAGE}"])
