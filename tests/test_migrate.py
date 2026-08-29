"""Migration: drain a private incumbent index into pypiron's private namespace.

The incumbent here is a real devpi-server (fetched via `uv tool run`): a private
package is uploaded to it with credentials, then

    pypiron sync --from <devpi +simple> --source-user U --source-pass P --as-private

authenticates against the source, downloads the artifact, and re-uploads it as a
PRIVATE package (origin=private — your own package, not a PyPI mirror). uv then
installs it from pypiron by its hash.

devpi is optional: the fixture skips cleanly when it can't be fetched or started.
"""

from __future__ import annotations

import json
import sys

import pytest

from .helpers import (
    make_wheel,
    origin_owner,
    run_checked,
    sha256_file,
    sync_to,
    wait_for_file_in_index,
)

pytestmark = [pytest.mark.integration, pytest.mark.devpi]

PACKAGE = "migratekit"
VERSION = "0.4.2"


def _twine_upload(wheel_path, devpi_server) -> None:
    """Upload a prebuilt wheel to the devpi index — auth-gated, so this is what
    the migration's --source-user/--source-pass must satisfy to read it back."""
    run_checked(
        [
            sys.executable,
            "-m",
            "twine",
            "upload",
            "--non-interactive",
            "--disable-progress-bar",
            "--repository-url",
            devpi_server["upload_url"],
            "-u",
            devpi_server["user"],
            "-p",
            devpi_server["password"],
            str(wheel_path),
        ],
        timeout=120,
    )


def test_migrate_devpi_as_private(
    devpi_server, disk_server, pypiron_bin, tmp_path, uv_venv, uv_path
):
    # A private package living on the incumbent devpi.
    wheel = make_wheel(PACKAGE, VERSION, tmp_path)
    local_sha = sha256_file(wheel)
    _twine_upload(wheel, devpi_server)

    # Migrate it into pypiron's private namespace. Source creds authenticate the
    # devpi read; --as-private omits the mirror form field so the server takes the
    # private path. The fixture's devpi is plaintext http:// on loopback, so
    # carrying a credential there needs --allow-insecure-source — the trusted-
    # network escape hatch; test_sec_sync.py covers the refusal it overrides.
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--include-package",
        PACKAGE,
        "--include-format",
        "wheel",
        "--source-user",
        devpi_server["user"],
        "--source-pass",
        devpi_server["password"],
        "--allow-insecure-source",
        "--as-private",
        source=devpi_server["source"],
    )
    assert rc == 0, f"migration sync failed:\n{out}\n{err}"

    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel.name)

    # Origin is the whole point: private, not mirror. origin=private is only
    # reachable when the upload carried NO mirror field — so this also proves
    # --as-private omitted it.
    pkg_dir = disk_server["data_dir"] / "packages" / PACKAGE
    assert origin_owner((pkg_dir / ".origin").read_text()) == "private", (
        f"migrated package must land private, not mirror:\n{out}\n{err}"
    )

    # The exact bytes round-tripped devpi -> pypiron: the stored sidecar digest
    # equals the wheel we uploaded.
    sidecar = json.loads((pkg_dir / f"{wheel.name}.meta.json").read_text())
    assert sidecar["sha256"] == local_sha

    # uv installs it from pypiron, verifying the index-declared hash as it goes.
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(uv_venv),
            "--index-url",
            disk_server["simple"],
            "--no-cache",
            f"{PACKAGE}=={VERSION}",
        ],
        timeout=180,
    )
    installed = run_checked(
        [str(uv_venv), "-c", f"import {PACKAGE}; print({PACKAGE}.__version__)"]
    ).stdout.strip()
    assert installed == VERSION


def test_migrate_as_private_refuses_a_mirror_owned_name(
    devpi_server, disk_server, pypiron_bin, tmp_path
):
    """A destination that already mirrors the name must stop the migration cold.

    The skip that keeps a re-run cheap is keyed on the filename alone, so without
    an ownership check every selected file matches a mirror-owned one, nothing is
    uploaded, no POST ever reaches the server's private-vs-mirror rejection, and
    the run exits 0 while the mirror's bytes keep serving under the name.
    """
    wheel = make_wheel(PACKAGE, VERSION, tmp_path)
    local_sha = sha256_file(wheel)
    _twine_upload(wheel, devpi_server)
    common = (
        "--include-package",
        PACKAGE,
        "--include-format",
        "wheel",
        "--source-user",
        devpi_server["user"],
        "--source-pass",
        devpi_server["password"],
        # Plaintext loopback devpi: see test_migrate_devpi_as_private.
        "--allow-insecure-source",
    )

    # Someone claimed the name as a mirror first.
    rc, out, err = sync_to(pypiron_bin, disk_server, *common, source=devpi_server["source"])
    assert rc == 0, f"mirror seed failed:\n{out}\n{err}"
    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel.name)
    pkg_dir = disk_server["data_dir"] / "packages" / PACKAGE
    assert origin_owner((pkg_dir / ".origin").read_text()) == "mirror"

    # Migrating the same name private now has to fail, loudly. Reaching the
    # ownership error at all also proves the run didn't replay the mirror-mode
    # cursor the seed left behind and 304-skip before ever looking.
    rc, out, err = sync_to(
        pypiron_bin, disk_server, *common, "--as-private", source=devpi_server["source"]
    )
    assert rc != 0, (
        f"--as-private onto a mirror-owned name must fail, not report success:\n{out}\n{err}"
    )
    assert "mirror package" in out + err, f"error must name the conflict:\n{out}\n{err}"

    # And it changed nothing: the mirror still owns the name and its bytes.
    assert origin_owner((pkg_dir / ".origin").read_text()) == "mirror"
    sidecar = json.loads((pkg_dir / f"{wheel.name}.meta.json").read_text())
    assert sidecar["sha256"] == local_sha
    assert sha256_file(pkg_dir / wheel.name) == local_sha
