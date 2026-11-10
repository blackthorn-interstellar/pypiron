"""Stateless install tokens.

`POST /tokens` mints a short-lived bearer token presented as basic-auth
username `__token__`; its role can never exceed the credential that minted it;
token auth is disabled unless `--token-signing-key` is configured. The
`create-token` CLI mints one against a running server, auto-detecting
repo/commit/user.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import http.client
import json
import os
import subprocess
import time
from urllib.parse import urlsplit

import pytest

from .conftest import _start_disk_server
from .helpers import (
    _encode_basic_auth,
    _http_request,
    find_free_port,
    http_get,
    make_wheel,
    upload_legacy,
)

pytestmark = pytest.mark.integration


def _auth(user: str, password: str) -> dict:
    return {"Authorization": _encode_basic_auth(user, password)}


def _token_header(token: str) -> dict:
    return {"Authorization": _encode_basic_auth("__token__", token)}


SIGNING_KEY = b"test-signing-key-0123456789abcdef"  # disk_server_token_auth's key


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _sign(role: str, iat: int) -> str:
    """A token as a peer sharing the signing key would mint it, with its own clock."""
    payload = _b64(json.dumps({"role": role, "iat": iat, "exp": iat + 300}).encode())
    mac = _b64(hmac.new(SIGNING_KEY, payload.encode(), hashlib.sha256).digest())
    return f"pypiron-{payload}.{mac}"


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


def test_a_token_cannot_mint_another_token(disk_server_token_auth):
    # A leaked token must not refresh itself indefinitely: minting requires a base
    # credential, so presenting a token as the minting credential is refused. This
    # keeps the short TTL meaningful — the basis for carrying no revocation list.
    server = disk_server_token_auth
    _, body, _ = _mint(
        server, role="uploader", auth=(server["admin_user"], server["admin_password"])
    )
    token = json.loads(body)["token"]
    # The token can read/publish, but it cannot mint — for its own role or a lesser.
    assert _mint(server, role="uploader", auth=("__token__", token))[0] == 403
    assert _mint(server, role="reader", auth=("__token__", token))[0] == 403


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


def test_a_read_only_node_refuses_write_tokens(
    disk_server_token_auth, tmp_path_factory, pypiron_bin, tmp_path
):
    """A node with no write credential is read-only even for admin and uploader
    tokens a write-enabled node sharing its signing key minted."""
    writer = disk_server_token_auth
    tokens = {}
    for role in ("admin", "uploader"):
        _, body, _ = _mint(writer, role=role, auth=(writer["admin_user"], writer["admin_password"]))
        tokens[role] = json.loads(body)["token"]

    replica_gen = _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=["--token-signing-key", "test-signing-key-0123456789abcdef"],
        creds="none",
    )
    replica = next(replica_gen)
    try:
        wheel = make_wheel("replicapkg", "1.0.0", tmp_path)
        for token in tokens.values():
            upload_legacy(
                replica["legacy"], wheel, username="__token__", password=token, expect_status=403
            )
        code, _, _ = _http_request(
            f"{replica['base_url']}/files/replicapkg/{wheel.name}/yank",
            method="POST",
            headers=_token_header(tokens["admin"]),
            data=b"x",
        )
        assert code == 403
    finally:
        replica_gen.close()


def test_a_token_issued_in_the_future_is_refused(disk_server_token_auth):
    """A peer whose clock runs fast mints tokens that would otherwise live for the
    skew plus the TTL; they are refused instead, so the skew gets noticed."""
    index = f"{disk_server_token_auth['simple']}index.json"
    now = int(time.time())
    # The hand-signed format is right: a token issued now reads.
    assert http_get(index, headers=_token_header(_sign("reader", now)))[0] == 200
    assert http_get(index, headers=_token_header(_sign("reader", now + 3600)))[0] == 401


def test_a_token_that_turns_valid_mid_request_cannot_mint(disk_server_token_auth):
    """The minting endpoint refuses token-mode callers before reading the body.
    That refusal must not depend on the token being valid right now: a token
    issued just past the clock-skew allowance is invalid when the headers land
    and valid by the time a slow body arrives and the role check runs."""
    server = disk_server_token_auth
    token = _sign("uploader", int(time.time()) + 63)
    body = json.dumps({"role": "uploader"}).encode()
    url = urlsplit(server["base_url"])
    conn = http.client.HTTPConnection(url.hostname, url.port, timeout=30)
    try:
        conn.putrequest("POST", "/tokens")
        conn.putheader("Authorization", _token_header(token)["Authorization"])
        conn.putheader("Content-Type", "application/json")
        conn.putheader("Content-Length", str(len(body)))
        conn.endheaders()
        time.sleep(5)
        try:
            conn.send(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # refused before the body: the fixed behavior
        resp = conn.getresponse()
        assert resp.status == 403, resp.read()
    finally:
        conn.close()


def test_a_short_signing_key_refuses_startup(pypiron_bin, tmp_path):
    """Anyone can collect a token, so a short key falls to an offline guess and
    then signs admin tokens. Startup refuses it and says how to make a good one,
    without echoing the key."""
    weak = "k" * 31
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYPIRON_")}
    env["PYPIRON_TOKEN_SIGNING_KEY"] = weak
    cp = subprocess.run(
        [
            str(pypiron_bin),
            "serve",
            "--bind-addr",
            f"127.0.0.1:{find_free_port()}",
            "--data-dir",
            str(tmp_path / "data"),
        ],
        capture_output=True,
        text=True,
        timeout=20,
        env=env,
    )
    out = cp.stdout + cp.stderr
    assert cp.returncode != 0, out
    assert "openssl rand -hex 32" in out, out
    assert weak not in out, out


@pytest.fixture()
def public_read_token_server(tmp_path_factory, pypiron_bin):
    """Reads are public, uploads need a credential, and tokens are minted."""
    gen = _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=["--token-signing-key", SIGNING_KEY.decode()],
    )
    try:
        yield next(gen)
    finally:
        gen.close()


def test_a_wrong_password_never_mints_on_a_public_read_server(public_read_token_server):
    """Reads are open, so a reader token needs no credential. But a wrong one
    is a failed login, not an anonymous request: it gets 401, not a token."""
    server = public_read_token_server
    assert _mint(server)[0] == 200
    assert _mint(server, auth=(server["uploader_user"], server["uploader_password"]))[0] == 200
    code, body, _ = _mint(server, auth=(server["uploader_user"], "wrong"))
    assert code == 401, body


def test_an_anonymous_token_does_not_pass_a_read_gated_peer(
    public_read_token_server, disk_server_token_auth
):
    """Two servers share a signing key; one serves reads publicly, the other
    requires a read credential. A token anyone could mint on the public one
    must not get past the other's credential. One minted with a real credential
    still works on both."""
    public, gated = public_read_token_server, disk_server_token_auth
    index = f"{gated['simple']}index.json"

    code, body, _ = _mint(public)
    assert code == 200, body
    anonymous = json.loads(body)["token"]
    assert http_get(f"{public['simple']}index.json", headers=_token_header(anonymous))[0] == 200
    assert http_get(index, headers=_token_header(anonymous))[0] == 401

    code, body, _ = _mint(public, auth=(public["uploader_user"], public["uploader_password"]))
    assert code == 200, body
    credentialed = json.loads(body)["token"]
    assert http_get(index, headers=_token_header(credentialed))[0] == 200
