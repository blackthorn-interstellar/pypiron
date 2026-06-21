"""Two-role auth: uploader publishes; admin does everything (mirror, delete, yank)."""

from __future__ import annotations

import pytest

from .helpers import (
    download_pypi_wheel,
    http_get,
    http_request_auth,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
VERSION = "1.17.0"

pytestmark = pytest.mark.integration


def test_uploader_can_publish(disk_server, tmp_path):
    wheel = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["uploader_user"],
        password=disk_server["uploader_password"],
    )
    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel.name)


def test_bad_credential_rejected(disk_server, tmp_path):
    wheel = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel,
        username="nope",
        password="wrong",
        expect_status=401,
    )


def test_uploader_cannot_delete_or_yank(disk_server, tmp_path):
    wheel = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    up = {"username": disk_server["uploader_user"], "password": disk_server["uploader_password"]}
    upload_legacy(disk_server["legacy"], wheel, **up)
    wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel.name)

    base = disk_server["base_url"]
    # Delete and yank are admin-only — the uploader credential gets 401.
    code, _, _ = http_request_auth("DELETE", f"{base}/files/{PACKAGE}/{wheel.name}", **up)
    assert code == 401
    code, _, _ = http_request_auth(
        "POST", f"{base}/files/{PACKAGE}/{wheel.name}/yank", data=b"oops", **up
    )
    assert code == 401

    # The artifact and index are untouched.
    assert (disk_server["data_dir"] / "packages" / PACKAGE / wheel.name).exists()

    # The admin credential can do both.
    admin = {"username": disk_server["admin_user"], "password": disk_server["admin_password"]}
    code, _, _ = http_request_auth(
        "POST", f"{base}/files/{PACKAGE}/{wheel.name}/yank", data=b"bad", **admin
    )
    assert code == 200
    code, _, _ = http_request_auth("DELETE", f"{base}/files/{PACKAGE}/{wheel.name}", **admin)
    assert code == 204


def test_401_carries_www_authenticate(disk_server):
    """RFC 7235: pip's keyring prompt and browsers need the challenge header."""
    from .helpers import _encode_basic_auth, _http_request

    code, _, headers = _http_request(
        disk_server["legacy"],
        method="POST",
        headers={
            "Authorization": _encode_basic_auth("nope", "wrong"),
            "Content-Type": "multipart/form-data; boundary=x",
        },
        data=b"",
    )
    assert code == 401
    assert headers.get("www-authenticate") == 'Basic realm="PypIron"'


def test_no_credentials_means_read_only(disk_server_no_creds, tmp_path):
    """With no credentials configured every write is disabled — not open."""
    server = disk_server_no_creds
    wheel = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(server["legacy"], wheel, expect_status=403)

    code, _, _ = http_request_auth(
        "DELETE",
        f"{server['base_url']}/files/{PACKAGE}/{wheel.name}",
        username="any",
        password="thing",
    )
    assert code == 403

    # Reads stay public.
    code, _, _ = http_get(f"{server['simple']}index.json")
    assert code == 200


def test_admin_pass_only_defaults_username_to_admin(disk_server_admin_pass_only, tmp_path):
    """`serve --admin-pass secret` alone is a complete admin credential under the
    default `admin` username — publish plus the admin-only delete/yank."""
    server = disk_server_admin_pass_only
    admin = {"username": "admin", "password": "secret"}
    wheel = download_pypi_wheel(PACKAGE, VERSION, tmp_path)

    # Publishing authenticates as the defaulted admin username.
    upload_legacy(server["legacy"], wheel, **admin)
    wait_for_file_in_index(server["simple"], PACKAGE, wheel.name)

    # A wrong username is still rejected — the default isn't a bypass.
    code, _, _ = http_request_auth(
        "DELETE",
        f"{server['base_url']}/files/{PACKAGE}/{wheel.name}",
        username="root",
        password="secret",
    )
    assert code == 401

    # The admin-only operations work under the default username.
    code, _, _ = http_request_auth(
        "DELETE", f"{server['base_url']}/files/{PACKAGE}/{wheel.name}", **admin
    )
    assert code == 204


def test_admin_disabled_when_unconfigured(disk_server_uploader_only, tmp_path):
    """With no admin credential, delete/yank are disabled for everyone."""
    server = disk_server_uploader_only
    wheel = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    up = {"username": server["user"], "password": server["password"]}
    upload_legacy(server["legacy"], wheel, **up)
    wait_for_file_in_index(server["simple"], PACKAGE, wheel.name)

    # No admin credential exists → the operation is disabled (403), not a 401.
    code, _, _ = http_request_auth(
        "DELETE", f"{server['base_url']}/files/{PACKAGE}/{wheel.name}", **up
    )
    assert code == 403
