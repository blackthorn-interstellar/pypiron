"""End-to-end proof of PEP 700: uv's --exclude-newer resolves by upload time."""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from .helpers import download_pypi_wheel, run_checked, wait_for_file_in_index

PACKAGE = "six"
OLD_VERSION = "1.16.0"
NEW_VERSION = "1.17.0"

pytestmark = pytest.mark.integration


def _installed_version(py: str) -> str:
    cp = run_checked([py, "-c", "import six; print(six.__version__)"])
    return cp.stdout.strip()


@pytest.mark.compat("uv", "exclude-newer")
def test_exclude_newer_resolves_old_version(disk_server, tmp_path, uv_path, uv_venv):
    old_wheel = download_pypi_wheel(PACKAGE, OLD_VERSION, tmp_path)
    new_wheel = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)

    def publish(wheel):
        run_checked(
            [
                uv_path,
                "publish",
                "--publish-url",
                disk_server["legacy"],
                "--username",
                disk_server["user"],
                "--password",
                disk_server["password"],
                str(wheel),
            ],
            timeout=120,
        )

    publish(old_wheel)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, old_wheel.name)

    # The cutoff must strictly separate the two upload times.
    time.sleep(1.5)
    cutoff = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    time.sleep(1.5)

    publish(new_wheel)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, new_wheel.name)

    py = str(uv_venv)

    # As-of-cutoff resolution must pick the old version...
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            py,
            "--index-url",
            disk_server["simple"],
            "--no-cache",
            "--exclude-newer",
            cutoff,
            PACKAGE,
        ],
        timeout=180,
    )
    assert _installed_version(py) == OLD_VERSION

    # ... while an unconstrained resolve picks the new one.
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            py,
            "--index-url",
            disk_server["simple"],
            "--no-cache",
            "--upgrade",
            PACKAGE,
        ],
        timeout=180,
    )
    assert _installed_version(py) == NEW_VERSION
