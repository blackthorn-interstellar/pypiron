"""Milestone 9: direct-storage sync carries PyPI's true upload times.

The end-to-end proof of mirrored timestamps: after mirroring a package,
`uv pip install --exclude-newer <historical date>` resolves exactly the
version that existed on PyPI at that date.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from packaging.version import Version

from .helpers import (
    download_pypi_wheel,
    pypi_project_json,
    run_checked,
    run_returncode,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
CUTOFF = "2016-01-01T00:00:00Z"

pytestmark = pytest.mark.integration


def _expected_version_at(cutoff: str) -> str:
    """Highest six version with a wheel uploaded before the cutoff, per pypi.org."""
    cutoff_dt = datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
    data = pypi_project_json(PACKAGE)
    candidates = []
    for version, files in data["releases"].items():
        for f in files:
            uploaded = datetime.fromisoformat(f["upload_time_iso_8601"].replace("Z", "+00:00"))
            if f["filename"].endswith(".whl") and not f.get("yanked") and uploaded < cutoff_dt:
                candidates.append(Version(version))
    assert candidates, "PyPI should have pre-cutoff six wheels"
    return str(max(candidates))


def test_mirror_preserves_historical_timestamps(disk_server, pypiron_bin, tmp_path, uv_path, uv_venv):
    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}\n")

    run_checked(
        [
            str(pypiron_bin),
            "sync",
            "--packages-list",
            str(pkg_list),
            "--data-dir",
            str(disk_server["data_dir"]),
            "--only-wheels",
        ],
        timeout=600,
    )

    # The server's worker picks up the dirty marker and indexes the mirror.
    data = pypi_project_json(PACKAGE)
    latest = data["info"]["version"]
    pypi_entry = next(
        f for f in data["releases"][latest] if f["filename"].endswith(".whl")
    )
    latest_wheel = pypi_entry["filename"]
    wait_for_file_in_index(disk_server["simple"], PACKAGE, latest_wheel)

    # Truth tree: mirror-owned, sidecars carry PyPI's timestamps verbatim.
    pkg_dir = disk_server["data_dir"] / "packages" / PACKAGE
    assert (pkg_dir / ".origin").read_text() == "mirror"
    sidecar = json.loads((pkg_dir / f"{latest_wheel}.meta.json").read_text())
    assert sidecar["upload-time"] == pypi_entry["upload_time_iso_8601"]
    assert sidecar["sha256"] == pypi_entry["digests"]["sha256"]

    # The point of it all: historical resolution against the mirror.
    expected = _expected_version_at(CUTOFF)
    py = str(uv_venv)
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
            CUTOFF,
            PACKAGE,
        ],
        timeout=180,
    )
    installed = run_checked([py, "-c", "import six; print(six.__version__)"]).stdout.strip()
    assert installed == expected, (
        f"--exclude-newer {CUTOFF} must resolve {expected} (PyPI history), got {installed}"
    )


def test_sync_refuses_private_owned_names(disk_server, pypiron_bin, tmp_path):
    # Claim the name privately first, via the upload API.
    wheel_path = download_pypi_wheel(PACKAGE, "1.17.0", tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel_path,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)

    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}\n")
    rc, out, err = run_returncode(
        [
            str(pypiron_bin),
            "sync",
            "--packages-list",
            str(pkg_list),
            "--data-dir",
            str(disk_server["data_dir"]),
            "--only-wheels",
        ],
        timeout=120,
    )
    assert rc != 0, "sync must hard-fail on a private-owned name"
    assert "private" in (out + err)

    pkg_dir = disk_server["data_dir"] / "packages" / PACKAGE
    assert (pkg_dir / ".origin").read_text() == "private", "the claim must be untouched"
    artifacts = [p.name for p in pkg_dir.iterdir() if p.name.endswith(".whl")]
    assert artifacts == [wheel_path.name], "no mirrored files may appear"


def test_sync_refuses_private_namespace(pypiron_bin, tmp_path):
    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}\n")
    data_dir = tmp_path / "data"
    rc, out, err = run_returncode(
        [
            str(pypiron_bin),
            "sync",
            "--packages-list",
            str(pkg_list),
            "--data-dir",
            str(data_dir),
            "--private-prefix",
            PACKAGE,
        ],
        timeout=120,
    )
    assert rc != 0
    assert "namespace" in (out + err)
    assert not (data_dir / "packages" / PACKAGE).exists()
