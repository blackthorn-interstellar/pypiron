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


def test_deleting_last_file_releases_claim(disk_server, tmp_path):
    from .helpers import http_request_auth

    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    creds = {"username": disk_server["user"], "password": disk_server["password"]}
    upload_legacy(disk_server["legacy"], wheel_path, **creds)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)

    code, _, _ = http_request_auth(
        "DELETE", f"{disk_server['base_url']}/files/{PACKAGE}/{wheel_path.name}", **creds
    )
    assert code == 204
    assert not (disk_server["data_dir"] / "packages" / PACKAGE / ".origin").exists(), (
        "the origin claim dies with the package"
    )
