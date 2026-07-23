"""--login-cooldown-secs: five failed logins from one address, each within the
cooldown of the last, and that address is refused further credential-bearing
requests with 429 + Retry-After until the cooldown passes. Successes are never
counted, role denials (403) are not failed logins, anonymous requests and
probes are never throttled, and 0 disables the throttle."""

from __future__ import annotations

import time

import pytest

from .helpers import _encode_basic_auth, http_get, http_request_auth

pytestmark = pytest.mark.integration


def _auth_header(user: str, password: str) -> dict:
    return {"Authorization": _encode_basic_auth(user, password)}


def test_five_failures_lock_the_address_out(disk_server_login_cooldown):
    server = disk_server_login_cooldown
    url = f"{server['simple']}index.json"

    # Anonymous requests present no credential: challenged, never counted.
    code, _, _ = http_get(url)
    assert code == 401

    bad = _auth_header("reader", "wrong")
    for attempt in range(5):
        code, _, _ = http_get(url, headers=bad)
        assert code == 401, f"attempt {attempt}"

    # Sixth attempt: refused without evaluation, with a Retry-After.
    code, _, headers = http_get(url, headers=bad)
    assert code == 429
    assert 0 < int(headers["retry-after"]) <= 300

    # Even the correct password is refused during the cooldown — a guess that
    # happens to be right must not be confirmable.
    code, _, _ = http_get(url, headers=_auth_header("reader", "readersecret"))
    assert code == 429

    # Anonymous traffic and probes are untouched by the block.
    code, _, _ = http_get(url)
    assert code == 401
    code, _, _ = http_get(f"{server['base_url']}/health")
    assert code == 200


def test_role_denials_are_not_failed_logins(disk_server_login_cooldown):
    """A valid credential lacking the role (403) never accumulates: only a
    credential that authenticates as nobody (401) is a guess."""
    server = disk_server_login_cooldown
    yank = f"{server['base_url']}/files/somepkg/some-1.0-py3-none-any.whl/yank"
    for attempt in range(6):
        code, _, _ = http_request_auth(
            "POST",
            yank,
            username=server["uploader_user"],
            password=server["uploader_password"],
        )
        assert code == 403, f"attempt {attempt}"
    code, _, _ = http_get(
        f"{server['simple']}index.json",
        headers=_auth_header(server["uploader_user"], server["uploader_password"]),
    )
    assert code == 200


def test_cooldown_expires(disk_server_login_cooldown_short):
    server = disk_server_login_cooldown_short
    url = f"{server['simple']}index.json"
    bad = _auth_header("reader", "wrong")
    for _ in range(6):
        http_get(url, headers=bad)

    # 2s cooldown: the correct credential works again once it passes. Poll
    # rather than sleep-and-assert — under xdist load the expiry moment shifts.
    # Polls during the block are 429s, which never extend the cooldown.
    good = _auth_header("reader", "readersecret")
    deadline = time.monotonic() + 15
    code = None
    while time.monotonic() < deadline:
        code, _, _ = http_get(url, headers=good)
        if code == 200:
            break
        time.sleep(0.25)
    assert code == 200


def test_zero_disables(disk_server_login_cooldown_off):
    server = disk_server_login_cooldown_off
    url = f"{server['simple']}index.json"
    bad = _auth_header("reader", "wrong")
    for attempt in range(8):
        code, _, _ = http_get(url, headers=bad)
        assert code == 401, f"attempt {attempt}"
    code, _, _ = http_get(url, headers=_auth_header("reader", "readersecret"))
    assert code == 200
