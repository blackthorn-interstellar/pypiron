"""`--require-repo`: installs must say which repo they are for.

The repo rides in the index username (`ci+repo=org/repo`) or in an install
token. `report` counts index requests that name none; `enforce` refuses index
and artifact reads that name none with 428 and a reason uv prints.
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

import pytest

from .conftest import _start_disk_server
from .helpers import (
    _encode_basic_auth,
    http_get,
    http_get_json,
    make_wheel,
    run_checked,
    upload_legacy,
)

pytestmark = pytest.mark.integration

SIGNING_KEY = "test-signing-key-0123456789abcdef"
REASON = "Repo required: add repo= to the index username"
WITH_REPO = {"Authorization": _encode_basic_auth("ci+repo=acme/api", "")}


@pytest.fixture()
def enforcing(tmp_path_factory, pypiron_bin):
    yield from _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=["--require-repo", "enforce", "--token-signing-key", SIGNING_KEY],
    )


@pytest.fixture()
def reporting(tmp_path_factory, pypiron_bin):
    yield from _start_disk_server(
        tmp_path_factory, pypiron_bin, extra_args=["--require-repo", "report"]
    )


@pytest.fixture()
def enforcing_gated(tmp_path_factory, pypiron_bin):
    yield from _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=["--require-repo", "enforce", "--read-user", "reader", "--read-pass", "pw"],
    )


def _get(server, path, headers=None):
    """GET returning (status, reason phrase, body) — urllib hides the phrase."""
    host, port = server["bind"].split(":")
    conn = http.client.HTTPConnection(host, int(port), timeout=10)
    try:
        conn.request("GET", path, headers=headers or {})
        resp = conn.getresponse()
        return resp.status, resp.reason, resp.read().decode(errors="replace")
    finally:
        conn.close()


def _missing(server) -> int:
    _, body, _ = http_get(f"{server['base_url']}/metrics")
    line = next(
        ln for ln in body.decode().splitlines() if ln.startswith("pypiron_repo_missing_total ")
    )
    return int(line.split()[1])


def _token(**claims) -> dict:
    now = int(time.time())
    payload = json.dumps({"role": "reader", "iat": now, "exp": now + 300, **claims}).encode()
    b64 = lambda raw: base64.urlsafe_b64encode(raw).rstrip(b"=").decode()  # noqa: E731
    mac = hmac.new(SIGNING_KEY.encode(), b64(payload).encode(), hashlib.sha256).digest()
    return {"Authorization": _encode_basic_auth("__token__", f"pypiron-{b64(payload)}.{b64(mac)}")}


def _publish(server, tmp_path, name="repopkg"):
    wheel = make_wheel(name, "1.0", tmp_path)
    upload_legacy(server["legacy"], wheel, username=server["user"], password=server["password"])
    index = f"{server['simple']}{name}/index.json"
    deadline = time.time() + 30
    while time.time() < deadline:
        code, _, _ = http_get(index, headers=WITH_REPO)
        if code == 200 and wheel.name in json.dumps(http_get_json(index, headers=WITH_REPO)):
            return wheel
        time.sleep(0.2)
    raise TimeoutError("uploaded wheel never appeared in the index")


def test_enforce_refuses_installs_that_name_no_repo(enforcing, tmp_path):
    server = enforcing
    wheel = _publish(server, tmp_path)  # an upload is not an install: no repo needed

    for path in ("/simple/", "/simple/repopkg/", f"/files/repopkg/{wheel.name}"):
        status, reason, body = _get(server, path)
        assert (status, reason) == (428, REASON), path
        assert "ci+repo=org/repo" in body
        # Tags that name no repo are still no repo.
        other = {"Authorization": _encode_basic_auth("ci+delivery=stream", "")}
        assert _get(server, path, other)[0] == 428, path
        assert _get(server, path, WITH_REPO)[0] == 200, path

    # Pages and probes are not installs.
    for path in ("/", "/health", "/metrics", "/project/repopkg/"):
        assert _get(server, path)[0] == 200, path


def test_an_install_token_minted_with_a_repo_satisfies_it(enforcing):
    assert _get(enforcing, "/simple/", _token(repo="acme/api"))[0] == 200
    assert _get(enforcing, "/simple/", _token())[0] == 428


def test_report_serves_everyone_and_counts_index_requests_without_a_repo(reporting, tmp_path):
    server = reporting
    wheel = _publish(server, tmp_path)
    before = _missing(server)

    assert _get(server, "/simple/repopkg/")[0] == 200
    assert _missing(server) == before + 1

    # Naming a repo is not counted; neither is an artifact request, which uv
    # sends without the index URL's username until one is refused.
    assert _get(server, "/simple/repopkg/", WITH_REPO)[0] == 200
    assert _get(server, f"/files/repopkg/{wheel.name}")[0] == 200
    assert _missing(server) == before + 1


def test_a_missing_credential_is_challenged_before_a_missing_repo(enforcing_gated):
    """A client that only sends its credential once challenged must still get
    the 401; the repo is asked of requests that already authenticate."""
    server = enforcing_gated
    assert _get(server, "/simple/")[0] == 401
    assert _get(server, "/simple/", WITH_REPO)[0] == 401
    reader = {"Authorization": _encode_basic_auth("reader", "pw")}
    assert _get(server, "/simple/", reader)[:2] == (428, REASON)
    tagged = {"Authorization": _encode_basic_auth("reader+repo=acme/api", "pw")}
    assert _get(server, "/simple/", tagged)[0] == 200


def test_uv_installs_with_the_repo_committed_in_the_index_url(enforcing, uv_path, tmp_path):
    """The rollout path: one line in pyproject.toml covers every developer and
    CI job. uv sends that username on the wheel download once the bare request
    is refused, and keeps it out of uv.lock."""
    server = enforcing
    _publish(server, tmp_path)
    project = tmp_path / "proj"
    project.mkdir()
    pyproject = """[project]
name = "proj"
version = "0"
requires-python = ">=3.9"
dependencies = ["repopkg==1.0"]

[[tool.uv.index]]
name = "corp"
url = "http://{userinfo}{bind}/simple"
default = true
"""
    (project / "pyproject.toml").write_text(pyproject.format(userinfo="", bind=server["bind"]))
    env = {**os.environ, "UV_HTTP_RETRIES": "0"}
    cp = subprocess.run(
        [uv_path, "lock", "--no-cache"], cwd=project, env=env, capture_output=True, text=True
    )
    assert cp.returncode != 0
    assert REASON in cp.stderr, cp.stderr

    (project / "pyproject.toml").write_text(
        pyproject.format(userinfo="ci+repo=acme%2Fapi@", bind=server["bind"])
    )
    run_checked([uv_path, "sync", "--no-cache"], cwd=project, env=env, timeout=180)
    assert "repopkg" in (project / "uv.lock").read_text()
    assert "repo=" not in (project / "uv.lock").read_text()
