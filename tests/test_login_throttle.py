"""--login-cooldown-secs: five failed logins from one address, each within the
cooldown of the last, and that address is refused further credential-bearing
requests with 429 + Retry-After until the cooldown passes. Successes are never
counted, role denials (403) are not failed logins, anonymous requests and
probes are never throttled, and 0 disables the throttle. Guesses sent at the
same time share the five tries."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import select
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

import pytest

from .conftest import _start_disk_server
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


def test_wrong_passwords_on_the_landing_page_count_as_failed_logins(
    disk_server_login_cooldown,
):
    """The landing page folds in an activity panel for a valid reader, so a
    wrong credential there must be a counted 401, not a 200 whose size tells
    a right guess from a wrong one."""
    server = disk_server_login_cooldown
    root = f"{server['base_url']}/"
    code, _, _ = http_get(root)
    assert code == 200  # anonymous visitors still get the front door

    bad = _auth_header("reader", "wrong")
    for attempt in range(5):
        code, _, _ = http_get(root, headers=bad)
        assert code == 401, f"attempt {attempt}"
    code, _, _ = http_get(root, headers=_auth_header("reader", "readersecret"))
    assert code == 429


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


def _held_mint(server: dict, user: str, password: str) -> socket.socket:
    """Send `POST /tokens` headers but hold back the 2-byte body."""
    url = urlsplit(server["base_url"])
    sock = socket.create_connection((url.hostname, url.port), timeout=10)
    sock.sendall(
        (
            f"POST /tokens HTTP/1.1\r\nHost: {url.netloc}\r\n"
            f"Authorization: {_encode_basic_auth(user, password)}\r\n"
            "Content-Type: application/json\r\nContent-Length: 2\r\n"
            "Connection: close\r\n\r\n"
        ).encode()
    )
    return sock


def _status(sock: socket.socket) -> int:
    data = b""
    while b"\r\n" not in data:
        chunk = sock.recv(1024)
        if not chunk:
            break
        data += chunk
    sock.close()
    return int(data.split(b" ")[1])


def _answered_early(sock: socket.socket) -> bool:
    """Did the server respond before the body was sent?"""
    readable, _, _ = select.select([sock], [], [], 3)
    return bool(readable)


def test_a_wrong_credential_minting_a_token_is_refused_before_its_body(
    disk_server_login_cooldown,
):
    """A slow body must not hold a login slot: the wrong credential is answered
    401 before the body arrives, and counts. When the body was read first, five
    trickling bodies held all five slots, every login from the address got
    429, and none of them counted as a failure."""
    server = disk_server_login_cooldown
    for attempt in range(5):
        sock = _held_mint(server, "reader", f"wrong{attempt}")
        assert _answered_early(sock), f"attempt {attempt} waited for its body"
        assert _status(sock) == 401, f"attempt {attempt}"
    code, _, _ = http_get(
        f"{server['simple']}index.json", headers=_auth_header("reader", "readersecret")
    )
    assert code == 429


def test_concurrent_guesses_share_one_budget(disk_server_login_cooldown):
    """Thirty different wrong passwords sent at once get at most five
    evaluations: the budget is reserved on admission, not tallied after a 401,
    so however the requests interleave the rest are refused with 429."""
    server = disk_server_login_cooldown
    url = f"{server['simple']}index.json"
    with ThreadPoolExecutor(max_workers=30) as pool:
        codes = list(
            pool.map(
                lambda i: http_get(url, headers=_auth_header("reader", f"wrong{i}"))[0],
                range(30),
            )
        )
    assert set(codes) <= {401, 429}, codes
    assert 1 <= codes.count(401) <= 5, codes

    # Locked out now, the right password included: a 200 here would answer
    # the guesses the 429s were meant to withhold.
    code, _, _ = http_get(url, headers=_auth_header("reader", "readersecret"))
    assert code == 429


def test_stale_credential_on_public_reads_is_never_refused(disk_server):
    """A public-read server ignores the credential on reads; a client with a
    stale one fetching in parallel is never throttled."""
    url = f"{disk_server['simple']}index.json"
    stale = _auth_header("ci+billing", "stale")
    with ThreadPoolExecutor(max_workers=20) as pool:
        codes = list(pool.map(lambda _: http_get(url, headers=stale)[0], range(20)))
    assert codes == [200] * 20


@pytest.fixture()
def public_read_server_over_a_hung_upstream(tmp_path_factory, pypiron_bin):
    """A public-read server with an uploader credential, proxying an upstream
    that accepts connections and never answers: a read of an unknown project
    stays in flight for as long as the test needs."""
    hung = socket.socket()
    hung.bind(("127.0.0.1", 0))
    hung.listen(64)
    upstream = f"http://127.0.0.1:{hung.getsockname()[1]}/simple/"
    server = _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=["--proxy-upstream", upstream, "--allow-insecure-upstream"],
    )
    try:
        yield next(server)
    finally:
        server.close()
        hung.close()


def _held_read(server: dict, path: str, user: str, password: str) -> socket.socket:
    url = urlsplit(server["base_url"])
    sock = socket.create_connection((url.hostname, url.port), timeout=10)
    sock.sendall(
        (
            f"GET {path} HTTP/1.1\r\nHost: {url.netloc}\r\n"
            f"Authorization: {_encode_basic_auth(user, password)}\r\n"
            "Connection: close\r\n\r\n"
        ).encode()
    )
    return sock


def test_distinct_stale_credentials_on_public_reads_are_never_refused(
    public_read_server_over_a_hung_upstream,
):
    """Eight CI jobs behind one address, each with its own stale password, all
    installing at once from a public-read server: none is refused. The reads
    never evaluate the credential, so they used to take one of the address's
    five login slots each, and the sixth was answered 429."""
    server = public_read_server_over_a_hung_upstream
    held = [_held_read(server, "/simple/hungpkg/", f"job{i}", "stale") for i in range(8)]
    try:
        readable, _, _ = select.select(held, [], [], 2)
        refused = [_status(sock) for sock in readable]
        assert refused == [], f"public reads refused: {refused}"
    finally:
        for sock in held:
            sock.close()


def _signed_token(iat: int, exp: int) -> str:
    """A reader token signed with the cooldown server's key."""

    def b64(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    payload = b64(json.dumps({"role": "reader", "iat": iat, "exp": exp}).encode())
    key = b"test-signing-key-0123456789abcdef"
    mac = b64(hmac.new(key, payload.encode(), hashlib.sha256).digest())
    return f"pypiron-{payload}.{mac}"


def test_an_expired_token_is_not_a_failed_login(disk_server_login_cooldown):
    """A job that keeps using its token past the five minutes gets 401s, but the
    token is genuine, not a password guess: it must not lock its address out."""
    server = disk_server_login_cooldown
    url = f"{server['simple']}index.json"
    now = int(time.time())
    expired = _auth_header("__token__", _signed_token(now - 600, now - 300))
    for attempt in range(6):
        code, _, _ = http_get(url, headers=expired)
        assert code == 401, f"attempt {attempt}"
    code, _, _ = http_get(url, headers=_auth_header("reader", "readersecret"))
    assert code == 200

    # A forged token is still a guess.
    forged = _auth_header("__token__", _signed_token(now, now + 300)[:-2] + "AA")
    for attempt in range(5):
        assert http_get(url, headers=forged)[0] == 401, f"forged attempt {attempt}"
    assert http_get(url, headers=forged)[0] == 429
