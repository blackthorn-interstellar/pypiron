"""--read-user/--read-pass: when set, index and artifact reads require basic
auth; any stronger credential (uploader, admin) also reads; /health and
/metrics stay open for probes and scrapers."""

from __future__ import annotations

import time

import pytest

from .helpers import (
    _encode_basic_auth,
    http_get,
    http_get_json,
    make_wheel,
    upload_legacy,
)

pytestmark = pytest.mark.integration


def _auth_header(user: str, password: str) -> dict:
    return {"Authorization": _encode_basic_auth(user, password)}


def test_reads_require_credential(disk_server_read_auth):
    server = disk_server_read_auth
    for url in (
        f"{server['simple']}",
        f"{server['simple']}index.json",
        f"{server['simple']}somepkg/",
        f"{server['base_url']}/files/somepkg/some-1.0-py3-none-any.whl",
        f"{server['base_url']}/downloads/",
    ):
        code, _, headers = http_get(url)
        assert code == 401, url
        assert headers.get("www-authenticate") == 'Basic realm="PypIron"', url


def test_any_configured_credential_reads(disk_server_read_auth):
    server = disk_server_read_auth
    url = f"{server['simple']}index.json"
    for user, password in (
        (server["read_user"], server["read_password"]),
        (server["uploader_user"], server["uploader_password"]),
        (server["admin_user"], server["admin_password"]),
    ):
        code, _, _ = http_get(url, headers=_auth_header(user, password))
        assert code == 200, user

    code, _, _ = http_get(url, headers=_auth_header("reader", "wrong"))
    assert code == 401


def test_health_and_metrics_bypass_read_auth(disk_server_read_auth):
    server = disk_server_read_auth
    code, _, _ = http_get(f"{server['base_url']}/health")
    assert code == 200
    code, _, _ = http_get(f"{server['base_url']}/metrics")
    assert code == 200


def test_subaddressed_username_reads_and_attributes(disk_server_read_auth):
    """Gmail-style subaddressing: `reader+proj` authenticates as `reader`
    (password still validated) and `proj` shows up as a project tag in
    /metrics. A wrong password is a 401 and is never attributed."""
    server = disk_server_read_auth
    url = f"{server['simple']}index.json"

    code, _, _ = http_get(
        url, headers=_auth_header(f"{server['read_user']}+billing-api", server["read_password"])
    )
    assert code == 200

    code, _, _ = http_get(
        url, headers=_auth_header(f"{server['read_user']}+stolen-tag", "wrongpassword")
    )
    assert code == 401

    _, body, _ = http_get(f"{server['base_url']}/metrics")
    text = body.decode()
    assert 'pypiron_project_requests_total{project="billing-api",route="simple"} 1' in text
    assert "stolen-tag" not in text


def test_publish_then_install_flow_with_read_auth(disk_server_read_auth, tmp_path):
    server = disk_server_read_auth
    wheel = make_wheel("authpkg", "1.0", tmp_path)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["admin_user"],
        password=server["admin_password"],
    )

    # wait_for_file_in_index has no auth support; poll with the read credential.
    auth = _auth_header(server["read_user"], server["read_password"])
    index_url = f"{server['simple']}authpkg/index.json"
    deadline = time.time() + 30.0
    while time.time() < deadline:
        code, _, _ = http_get(index_url, headers=auth)
        if code == 200:
            data = http_get_json(index_url, headers=auth)
            if wheel.name in [f["filename"] for f in data.get("files", [])]:
                break
        time.sleep(0.2)
    else:
        raise TimeoutError("uploaded file never appeared in the authed index")

    code, body, _ = http_get(f"{server['base_url']}/files/authpkg/{wheel.name}", headers=auth)
    assert code == 200
    assert body == wheel.read_bytes()
