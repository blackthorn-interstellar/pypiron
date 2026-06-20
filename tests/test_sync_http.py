"""Mirror-over-HTTP: sync --to pushes PyPI's history through /legacy/.

The server owns all storage writes; sync needs only a URL and the admin
credential. Mirroring (backdating) is an admin operation — ordinary uploader
credentials cannot do it.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest
from packaging.version import Version

from .helpers import (
    download_pypi_wheel,
    pypi_project_json,
    run_checked,
    run_returncode,
    sync_to,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
CUTOFF = "2016-01-01T00:00:00Z"

pytestmark = pytest.mark.integration


def _expected_version_at(cutoff: str) -> str:
    """Highest non-yanked six version with a wheel uploaded before the cutoff,
    computed independently from pypi.org — the ground truth uv must resolve to."""
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


def _sync_to(server, pypiron_bin, pkg_list, *extra, user=None, password=None):
    # Mirroring is an admin operation — authenticate with the admin credential.
    user = user if user is not None else server.get("admin_user", server["user"])
    password = (
        password if password is not None else server.get("admin_password", server["password"])
    )
    return run_returncode(
        [
            str(pypiron_bin),
            "sync",
            "--packages-list",
            str(pkg_list),
            "--to",
            server["base_url"],
            "--username",
            user,
            "--password",
            password,
            "--only-wheels",
            *extra,
        ],
        timeout=600,
    )


@pytest.mark.compat("uv", "exclude-newer")
def test_http_mirror_preserves_historical_timestamps(
    disk_server, pypiron_bin, tmp_path, uv_path, uv_venv
):
    server = disk_server
    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}\n")

    rc, out, err = _sync_to(server, pypiron_bin, pkg_list)
    assert rc == 0, f"sync --to failed:\n{out}\n{err}"

    data = pypi_project_json(PACKAGE)
    latest = data["info"]["version"]
    pypi_entry = next(f for f in data["releases"][latest] if f["filename"].endswith(".whl"))
    wait_for_file_in_index(server["simple"], PACKAGE, pypi_entry["filename"])

    # The server wrote truth: mirror-owned, PyPI's timestamp in the sidecar,
    # and PEP 658 metadata extracted server-side from the wheel.
    pkg_dir = server["data_dir"] / "packages" / PACKAGE
    assert (pkg_dir / ".origin").read_text() == "mirror"
    sidecar = json.loads((pkg_dir / f"{pypi_entry['filename']}.meta.json").read_text())
    assert sidecar["upload-time"] == pypi_entry["upload_time_iso_8601"]
    assert sidecar["sha256"] == pypi_entry["digests"]["sha256"]
    assert (pkg_dir / f"{pypi_entry['filename']}.metadata").exists()

    # Historical resolution works through the HTTP-mirrored history.
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
            "--exclude-newer",
            CUTOFF,
            PACKAGE,
        ],
        timeout=180,
    )
    installed = run_checked([py, "-c", "import six; print(six.__version__)"]).stdout.strip()
    # Cross-check against PyPI ground truth: the backdated mirror must resolve the
    # EXACT maximal version that existed pre-cutoff, not merely *some* older one.
    expected = _expected_version_at(CUTOFF)
    assert installed == expected, (
        f"--exclude-newer {CUTOFF} must resolve {expected} (PyPI history), got {installed}"
    )
    sidecar_installed = json.loads(
        next(pkg_dir.glob(f"six-{installed}-*.whl.meta.json")).read_text()
    )
    assert sidecar_installed["upload-time"] < CUTOFF, (
        f"resolved {installed} must predate the cutoff"
    )


def test_sync_refuses_private_namespace(disk_server, pypiron_bin):
    """The client-side private-namespace gate refuses before any upload — no
    network traffic, nothing written."""
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--pkg",
        PACKAGE,
        "--private-prefix",
        PACKAGE,
        timeout=120,
    )
    assert rc != 0
    assert "namespace" in (out + err)
    assert not (disk_server["data_dir"] / "packages" / PACKAGE).exists()


def test_mirror_disabled_without_admin_credential(disk_server_uploader_only, pypiron_bin, tmp_path):
    """A server with no admin credential refuses mirror pushes outright."""
    server = disk_server_uploader_only
    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}\n")
    rc, out, err = _sync_to(server, pypiron_bin, pkg_list)
    assert rc != 0
    assert not (server["data_dir"] / "packages" / PACKAGE).exists(), (
        "nothing may be written when mirror uploads are disabled"
    )


def test_http_mirror_refuses_private_owned_names(disk_server, pypiron_bin, tmp_path):
    server = disk_server
    wheel_path = download_pypi_wheel(PACKAGE, "1.17.0", tmp_path)
    upload_legacy(
        server["legacy"], wheel_path, username=server["user"], password=server["password"]
    )
    wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name)

    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}\n")
    rc, out, err = _sync_to(server, pypiron_bin, pkg_list)
    assert rc != 0, "mirroring over a private-owned name must hard-fail"
    # The failure is the private-name guard, not some unrelated error.
    assert "private" in (out + err), f"expected a private-name diagnostic:\n{out}\n{err}"
    pkg_dir = server["data_dir"] / "packages" / PACKAGE
    assert (pkg_dir / ".origin").read_text() == "private", "the claim must be untouched"
    # No mirrored wheel may leak in alongside the private claim.
    wheels = sorted(p.name for p in pkg_dir.iterdir() if p.name.endswith(".whl"))
    assert wheels == [wheel_path.name], f"no mirrored files may appear, found {wheels}"


def test_normal_uploads_cannot_backdate(disk_server, tmp_path):
    """A non-mirror upload carrying a timestamp is rejected even when sent with
    the admin credential — backdating only happens through mirror=true."""
    wheel_path = download_pypi_wheel(PACKAGE, "1.17.0", tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel_path,
        username=disk_server["user"],
        password=disk_server["password"],
        fields={"upload_time": "2015-01-01T00:00:00Z"},
        expect_status=400,
    )


def test_mirror_prefix_block_holds_even_when_already_mirror_claimed(disk_server_prefixed, tmp_path):
    """The private namespace is off-limits to mirrors on every write, not just
    the first claim — adopting a prefix after a name was mirror-claimed still
    shuts the door (the guardrail can't silently no-op)."""
    server = disk_server_prefixed

    wheel = download_pypi_wheel(PACKAGE, "1.17.0", tmp_path)
    acme = tmp_path / "acme_tool-1.0-py3-none-any.whl"
    acme.write_bytes(wheel.read_bytes())

    # Pre-claim 'acme-tool' as mirror directly in the tree (as if it were
    # mirror-claimed before the prefix was configured).
    pkg_dir = server["data_dir"] / "packages" / "acme-tool"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / ".origin").write_text("mirror")

    # A mirror upload (admin) to this already-mirror-claimed, in-prefix name is
    # still 403.
    upload_legacy(
        server["legacy"],
        acme,
        username=server["admin_user"],
        password=server["admin_password"],
        fields={"mirror": "true", "name": "acme-tool", "upload_time": "2020-01-01T00:00:00Z"},
        expect_status=403,
    )


def test_mirror_requires_admin_credential(disk_server, tmp_path):
    """The uploader credential cannot perform a mirror upload; only admin can.
    This is the whole point of the two roles — ordinary uploaders must not be
    able to backdate."""
    wheel = download_pypi_wheel(PACKAGE, "1.17.0", tmp_path)
    server = disk_server
    mirror_fields = {"mirror": "true", "upload_time": "2014-01-01T00:00:00Z"}

    # The uploader credential is rejected for a mirror upload.
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["uploader_user"],
        password=server["uploader_password"],
        fields=mirror_fields,
        expect_status=401,
    )
    # No artifact, no claim leaked through.
    assert not (server["data_dir"] / "packages" / PACKAGE).exists()

    # The admin credential succeeds and the backdated time is honored.
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["admin_user"],
        password=server["admin_password"],
        fields=mirror_fields,
    )
    index = wait_for_file_in_index(server["simple"], PACKAGE, wheel.name)
    (entry,) = [f for f in index["files"] if f["filename"] == wheel.name]
    assert entry["upload-time"] == "2014-01-01T00:00:00Z"
    assert (server["data_dir"] / "packages" / PACKAGE / ".origin").read_text() == "mirror"
