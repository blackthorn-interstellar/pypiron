"""Stateless install tokens.

`POST /tokens` mints a short-lived bearer token presented as basic-auth
username `__token__`; its role can never exceed the credential that minted it;
token auth is disabled unless `--token-signing-key` is configured. The
`create-token` CLI mints one against a running server, auto-detecting
repo/commit/user.
"""

from __future__ import annotations

import json
import subprocess

import pytest

from .helpers import _encode_basic_auth, _http_request, http_get, make_wheel, upload_legacy

pytestmark = pytest.mark.integration


def _auth(user: str, password: str) -> dict:
    return {"Authorization": _encode_basic_auth(user, password)}


def _token_header(token: str) -> dict:
    return {"Authorization": _encode_basic_auth("__token__", token)}


def _mint(server, *, role=None, auth=None):
    body = json.dumps({"role": role} if role else {}).encode()
    headers = {"Content-Type": "application/json"}
    if auth:
        headers.update(_auth(*auth))
    return _http_request(f"{server['base_url']}/tokens", method="POST", headers=headers, data=body)


def test_token_grants_read_on_a_gated_server(disk_server_token_auth):
    server = disk_server_token_auth
    index = f"{server['simple']}index.json"

    # Baseline: reads require a credential.
    code, _, _ = http_get(index)
    assert code == 401

    # Mint a reader token with the read credential, then read with it.
    code, body, _ = _mint(server, auth=(server["read_user"], server["read_password"]))
    assert code == 200, body
    payload = json.loads(body)
    assert payload["username"] == "__token__"
    assert payload["role"] == "reader"
    assert payload["expires_in"] == 300
    assert payload["token"].startswith("pypiron-")

    code, _, _ = http_get(index, headers=_token_header(payload["token"]))
    assert code == 200


def test_bad_tokens_are_rejected(disk_server_token_auth):
    server = disk_server_token_auth
    index = f"{server['simple']}index.json"
    _, body, _ = _mint(server, auth=(server["read_user"], server["read_password"]))
    token = json.loads(body)["token"]

    # A one-character edit to the signature, a garbage token, and a non-token
    # all fail closed.
    tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
    for bad in (tampered, "pypiron-garbage.sig", "not-a-token"):
        code, _, _ = http_get(index, headers=_token_header(bad))
        assert code == 401, bad


def test_a_token_cannot_exceed_its_minting_credential(disk_server_token_auth):
    server = disk_server_token_auth
    reader = (server["read_user"], server["read_password"])
    uploader = (server["uploader_user"], server["uploader_password"])
    admin = (server["admin_user"], server["admin_password"])

    # reader credential: reader only.
    assert _mint(server, role="reader", auth=reader)[0] == 200
    assert _mint(server, role="uploader", auth=reader)[0] == 401
    assert _mint(server, role="admin", auth=reader)[0] == 401

    # uploader credential: uploader (and below), not admin.
    assert _mint(server, role="uploader", auth=uploader)[0] == 200
    assert _mint(server, role="admin", auth=uploader)[0] == 401

    # admin credential: anything.
    assert _mint(server, role="admin", auth=admin)[0] == 200

    # An unknown role is a 400.
    assert _mint(server, role="superuser", auth=admin)[0] == 400


def test_an_uploader_token_can_publish(disk_server_token_auth, tmp_path):
    server = disk_server_token_auth
    # Mint an uploader token (admin credential grants uploader), then publish a
    # wheel authenticating purely with the token.
    _, body, _ = _mint(
        server, role="uploader", auth=(server["admin_user"], server["admin_password"])
    )
    token = json.loads(body)["token"]

    wheel = make_wheel("tokenpkg", "1.0.0", tmp_path)
    code, resp = upload_legacy(
        server["legacy"], wheel, username="__token__", password=token, expect_status=200
    )
    assert code == 200, resp


def test_minting_is_disabled_without_a_signing_key(disk_server_read_auth):
    # Read-gated, but no --token-signing-key: minting is refused.
    server = disk_server_read_auth
    code, body, _ = _http_request(
        f"{server['base_url']}/tokens",
        method="POST",
        headers={
            "Content-Type": "application/json",
            **_auth(server["read_user"], server["read_password"]),
        },
        data=b"{}",
    )
    assert code == 403, body


def test_create_token_cli_mints_and_reads(disk_server_token_auth, pypiron_bin):
    server = disk_server_token_auth
    proc = subprocess.run(
        [
            str(pypiron_bin),
            "create-token",
            "--url",
            server["base_url"],
            "--auth",
            f"{server['read_user']}:{server['read_password']}",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    token = proc.stdout.strip()
    assert token.startswith("pypiron-")

    code, _, _ = http_get(f"{server['simple']}index.json", headers=_token_header(token))
    assert code == 200
