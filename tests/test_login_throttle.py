"""--login-cooldown-secs: five failed logins from one address, each within the
cooldown of the last, and that address is refused further credential-bearing
requests with 429 + Retry-After until the cooldown passes. Successes are never
counted, role denials (403) are not failed logins, anonymous requests and
probes are never throttled, and 0 disables the throttle. Guesses sent at the
same time share the five tries."""

from __future__ import annotations

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
    """Send `POST /tokens` headers but hold back the 2-byte body. The handler
    reads the body before checking the credential, so an admitted guess sits
    in flight until `_release` sends it."""
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


def _release(sock: socket.socket) -> int:
    sock.sendall(b"{}")
    return _status(sock)


def test_concurrent_guesses_share_one_budget(disk_server_login_cooldown):
    """Ten different wrong passwords in flight at once get five evaluations,
    not ten: the budget is reserved on admission, not tallied after a 401.
    Which five is up to the order the server reads the request heads in."""
    server = disk_server_login_cooldown
    url = f"{server['simple']}index.json"
    held = [_held_mint(server, "reader", f"wrong{i}") for i in range(10)]

    # Five are refused without waiting for their bodies; five wait.
    refused: list[socket.socket] = []
    deadline = time.time() + 10
    while len(refused) < 5 and time.time() < deadline:
        waiting = [sock for sock in held if sock not in refused]
        readable, _, _ = select.select(waiting, [], [], 0.2)
        refused += readable
    assert len(refused) == 5, f"{len(refused)} guesses refused early, not 5"
    admitted = [sock for sock in held if sock not in refused]
    assert not any(_answered_early(sock) for sock in admitted[:1]), "a sixth was refused"
    assert [_status(sock) for sock in refused] == [429] * 5

    # The right password is refused too while the budget is spoken for —
    # a 200 here would answer the guess the 429s were meant to withhold.
    code, _, _ = http_get(url, headers=_auth_header("reader", "readersecret"))
    assert code == 429

    assert [_release(sock) for sock in admitted] == [401] * 5
    code, _, _ = http_get(url, headers=_auth_header("reader", "readersecret"))
    assert code == 429


def test_one_wrong_credential_in_parallel_is_one_guess(disk_server_login_cooldown):
    """Retrying the same stale credential on parallel requests is one guess:
    every copy is answered 401 and together they count one failure."""
    server = disk_server_login_cooldown
    url = f"{server['simple']}index.json"
    held = [_held_mint(server, "reader", "stale") for _ in range(10)]
    time.sleep(0.5)
    assert not any(_answered_early(sock) for sock in held[-1:])
    assert [_release(sock) for sock in held] == [401] * 10

    # Four more failures make five: the address locks out on the next try.
    bad = _auth_header("reader", "stale")
    for attempt in range(4):
        code, _, _ = http_get(url, headers=bad)
        assert code == 401, f"attempt {attempt}"
    code, _, _ = http_get(url, headers=bad)
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
