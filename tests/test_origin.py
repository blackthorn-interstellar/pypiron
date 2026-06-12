"""Milestone 8: origin exclusivity and the private-namespace prefix policy."""

from __future__ import annotations

import shutil

import pytest

from .helpers import download_pypi_wheel, upload_legacy, wait_for_file_in_index

PACKAGE = "six"
VERSION = "1.17.0"

pytestmark = pytest.mark.integration


def test_first_upload_claims_private_origin(disk_server, tmp_path):
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel_path,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)

    origin = disk_server["data_dir"] / "packages" / PACKAGE / ".origin"
    assert origin.read_text() == "private"


def test_upload_to_mirror_owned_name_rejected(disk_server, tmp_path):
    # A mirror-owned package, claimed via storage (what sync does).
    pkg_dir = disk_server["data_dir"] / "packages" / PACKAGE
    pkg_dir.mkdir(parents=True)
    (pkg_dir / ".origin").write_text("mirror")

    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel_path,
        username=disk_server["user"],
        password=disk_server["password"],
        expect_status=403,
    )
    assert not (pkg_dir / wheel_path.name).exists()


def test_private_prefix_policy(disk_server_prefixed, tmp_path):
    server = disk_server_prefixed
    creds = {"username": server["user"], "password": server["password"]}
    six_wheel = download_pypi_wheel(PACKAGE, VERSION, tmp_path)

    # Outside the reserved namespace: rejected.
    upload_legacy(server["legacy"], six_wheel, expect_status=403, **creds)
    # Prefix is namespace-shaped, not a string prefix: acmefoo is outside.
    impostor = tmp_path / "acmefoo-1.0-py3-none-any.whl"
    shutil.copyfile(six_wheel, impostor)
    upload_legacy(server["legacy"], impostor, expect_status=403, **creds)

    # Inside the namespace: accepted and claimed private.
    private = tmp_path / "acme_foo-1.0-py3-none-any.whl"
    shutil.copyfile(six_wheel, private)
    upload_legacy(server["legacy"], private, **creds)
    wait_for_file_in_index(server["simple"], "acme-foo", private.name)
    assert (server["data_dir"] / "packages" / "acme-foo" / ".origin").read_text() == "private"


def test_origin_claim_survives_deletion(disk_server, tmp_path):
    """A mirror-owned name emptied by deletion must not become re-claimable as
    private — that would be the dependency-confusion direction. The .origin
    claim is durable; re-purposing a name needs storage access."""
    from .helpers import http_request_auth

    # Claim the name as mirror via storage (what sync does).
    pkg_dir = disk_server["data_dir"] / "packages" / PACKAGE
    pkg_dir.mkdir(parents=True)
    (pkg_dir / ".origin").write_text("mirror")

    creds = {"username": disk_server["user"], "password": disk_server["password"]}
    # Mirror an artifact in via the file tree, then index it.
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    (pkg_dir / wheel_path.name).write_bytes(wheel_path.read_bytes())
    import json as _json
    import hashlib

    (pkg_dir / f"{wheel_path.name}.meta.json").write_text(
        _json.dumps(
            {
                "sha256": hashlib.sha256(wheel_path.read_bytes()).hexdigest(),
                "size": wheel_path.stat().st_size,
                "version": VERSION,
                "upload-time": "2020-01-01T00:00:00Z",
                "yanked": False,
            }
        )
    )
    # A private upload to this mirror-owned name is forbidden (origin check
    # precedes immutability).
    upload_legacy(disk_server["legacy"], wheel_path, expect_status=403, **creds)

    # Delete the only artifact.
    code, _, _ = http_request_auth(
        "DELETE", f"{disk_server['base_url']}/files/{PACKAGE}/{wheel_path.name}", **creds
    )
    assert code == 204

    # The claim is still mirror — a private re-upload remains forbidden.
    assert (pkg_dir / ".origin").read_text() == "mirror"
    upload_legacy(disk_server["legacy"], wheel_path, expect_status=403, **creds)


def test_origin_marker_not_deletable_via_api(disk_server, tmp_path):
    """DELETE /files/<pkg>/.origin must not work — the claim is server-managed."""
    from .helpers import http_request_auth

    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    creds = {"username": disk_server["user"], "password": disk_server["password"]}
    upload_legacy(disk_server["legacy"], wheel_path, **creds)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)

    for target in (".origin", f"{wheel_path.name}.meta.json", f"{wheel_path.name}.metadata"):
        code, _, _ = http_request_auth(
            "DELETE", f"{disk_server['base_url']}/files/{PACKAGE}/{target}", **creds
        )
        assert code == 404, f"{target} must not be deletable via the API"

    assert (disk_server["data_dir"] / "packages" / PACKAGE / ".origin").exists()
