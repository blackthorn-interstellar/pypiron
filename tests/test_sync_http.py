"""Mirror-over-HTTP: sync --to pushes PyPI's history through /legacy/.

The server owns all storage writes; sync needs only a URL and the upload
credential. Backdating is gated on a dedicated mirror credential.
"""

from __future__ import annotations

import json

import pytest

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


def _sync_to(server, pypiron_bin, pkg_list, *extra, user=None, password=None):
    # Mirroring authenticates against the dedicated mirror credential, not the
    # ordinary upload credential.
    user = user if user is not None else server.get("mirror_user", server["user"])
    password = password if password is not None else server.get("mirror_password", server["password"])
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


def test_http_mirror_preserves_historical_timestamps(
    disk_server_mirror, pypiron_bin, tmp_path, uv_path, uv_venv
):
    server = disk_server_mirror
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
    sidecar_installed = json.loads(
        next(pkg_dir.glob(f"six-{installed}-*.whl.meta.json")).read_text()
    )
    assert sidecar_installed["upload-time"] < CUTOFF, (
        f"resolved {installed} must predate the cutoff"
    )


def test_mirror_uploads_require_mirror_credential(disk_server, pypiron_bin, tmp_path):
    """A stock server (no mirror credential) refuses mirror pushes outright."""
    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}\n")
    rc, out, err = _sync_to(disk_server, pypiron_bin, pkg_list)
    assert rc != 0
    assert not (disk_server["data_dir"] / "packages" / PACKAGE).exists(), (
        "nothing may be written when mirror uploads are disabled"
    )


def test_http_mirror_refuses_private_owned_names(disk_server_mirror, pypiron_bin, tmp_path):
    server = disk_server_mirror
    wheel_path = download_pypi_wheel(PACKAGE, "1.17.0", tmp_path)
    upload_legacy(
        server["legacy"], wheel_path, username=server["user"], password=server["password"]
    )
    wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name)

    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}\n")
    rc, _, _ = _sync_to(server, pypiron_bin, pkg_list)
    assert rc != 0, "mirroring over a private-owned name must hard-fail"
    assert (server["data_dir"] / "packages" / PACKAGE / ".origin").read_text() == "private"


def test_normal_uploads_cannot_backdate(disk_server_mirror, tmp_path):
    """Even on a mirror-enabled server, a non-mirror upload with a timestamp
    is rejected — backdating never rides along on ordinary credentials."""
    wheel_path = download_pypi_wheel(PACKAGE, "1.17.0", tmp_path)
    upload_legacy(
        disk_server_mirror["legacy"],
        wheel_path,
        username=disk_server_mirror["user"],
        password=disk_server_mirror["password"],
        fields={"upload_time": "2015-01-01T00:00:00Z"},
        expect_status=400,
    )


def test_mirror_prefix_block_holds_even_when_already_mirror_claimed(
    disk_server_mirror_prefixed, tmp_path
):
    """The private namespace is off-limits to mirrors on every write, not just
    the first claim — adopting a prefix after a name was mirror-claimed still
    shuts the door (the guardrail can't silently no-op)."""
    server = disk_server_mirror_prefixed

    wheel = download_pypi_wheel(PACKAGE, "1.17.0", tmp_path)
    acme = tmp_path / "acme_tool-1.0-py3-none-any.whl"
    acme.write_bytes(wheel.read_bytes())

    # Pre-claim 'acme-tool' as mirror directly in the tree (as if it were
    # mirror-claimed before the prefix was configured).
    pkg_dir = server["data_dir"] / "packages" / "acme-tool"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / ".origin").write_text("mirror")

    # A mirror upload to this already-mirror-claimed, in-prefix name is still 403.
    upload_legacy(
        server["legacy"],
        acme,
        username=server["mirror_user"],
        password=server["mirror_password"],
        fields={"mirror": "true", "name": "acme-tool", "upload_time": "2020-01-01T00:00:00Z"},
        expect_status=403,
    )


def test_mirror_requires_the_mirror_credential(disk_server_mirror, tmp_path):
    """Ordinary upload credentials cannot perform a mirror upload; only the
    dedicated mirror credential can. This is the whole point of the separate
    credential — normal uploaders must not be able to backdate."""
    wheel = download_pypi_wheel(PACKAGE, "1.17.0", tmp_path)
    server = disk_server_mirror
    mirror_fields = {"mirror": "true", "upload_time": "2014-01-01T00:00:00Z"}

    # The upload credential (admin/secret) is rejected for a mirror upload.
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
        fields=mirror_fields,
        expect_status=401,
    )
    # No artifact, no claim leaked through.
    assert not (server["data_dir"] / "packages" / PACKAGE).exists()

    # The mirror credential succeeds and the backdated time is honored.
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["mirror_user"],
        password=server["mirror_password"],
        fields=mirror_fields,
    )
    index = wait_for_file_in_index(server["simple"], PACKAGE, wheel.name)
    (entry,) = [f for f in index["files"] if f["filename"] == wheel.name]
    assert entry["upload-time"] == "2014-01-01T00:00:00Z"
    assert (server["data_dir"] / "packages" / PACKAGE / ".origin").read_text() == "mirror"
