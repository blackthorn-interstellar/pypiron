"""Tool matrix: twine for upload, pip for install (uv covered in test_roundtrip)."""

from __future__ import annotations

import sys

import pytest

from .helpers import download_pypi_wheel, run_checked, wait_for_file_in_index

PACKAGE = "six"
VERSION = "1.17.0"

pytestmark = pytest.mark.integration


def test_twine_upload_pip_install(disk_server, tmp_path, pip_venv):
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)

    # twine runs from the test venv (a dev dependency of this repo).
    run_checked(
        [
            sys.executable,
            "-m",
            "twine",
            "upload",
            "--non-interactive",
            "--disable-progress-bar",
            "--repository-url",
            disk_server["legacy"],
            "-u",
            disk_server["user"],
            "-p",
            disk_server["password"],
            str(wheel_path),
        ],
        timeout=120,
    )

    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)

    py = pip_venv
    run_checked(
        [
            str(py),
            "-m",
            "pip",
            "install",
            "--index-url",
            disk_server["simple"],
            "--no-cache-dir",
            f"{PACKAGE}=={VERSION}",
        ],
        timeout=180,
    )
    run_checked([str(py), "-c", f"import {PACKAGE}"])
