"""Enterprise egress and custom-CA (MITM) TLS interception.

T1  A full disk->disk sync bypasses an ambient forward proxy so its SSRF resolver
    always resolves listing-derived hostnames itself. The advisory feed, whose
    URL is operator-configured, continues to honor the proxy variables.

T2  A private "corporate" CA signs the advisory feed's TLS leaf. Without
    --upstream-ca-cert the feed is untrusted and the explicit-feed server fails
    closed; with it, the snapshot loads and the server serves. Skips cleanly when
    `openssl` is unavailable.

The IMDS client (node_region) keeps `.no_proxy()` and is never routed here — a
separate concern verified by the existing suite.
"""

from __future__ import annotations

import http.server
import os
import socket
import ssl
import subprocess
import threading
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from .conftest import _start_disk_server
from .helpers import (
    cmd_exists,
    find_free_port,
    http_get,
    kill_process_tree,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
    wait_http_responding,
)
from .test_advisories import _DIRECT_EGRESS, canonical_records, make_osv_zip

pytestmark = pytest.mark.integration


# --------------------------------------------------------------------------- #
# T1: a stdlib logging forward proxy                                          #
# --------------------------------------------------------------------------- #


class _ForwardProxy(http.server.BaseHTTPRequestHandler):
    """A minimal forward proxy that logs every target it is asked to reach and
    relays the exchange. For an `http://` target reqwest sends an absolute-form
    request line (`GET http://host:port/p ...`); an `https://` target would send
    `CONNECT host:port`. Both append `host:port` to the shared `targets` log."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # noqa: D102 - silence default stderr spam
        pass

    def _forward(self):  # absolute-form: self.path is a full URL
        url = urlsplit(self.path)
        host, port = url.hostname, url.port or 80
        self.server.targets.append(f"{host}:{port}")  # type: ignore[attr-defined]
        path = url.path or "/"
        if url.query:
            path += "?" + url.query
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        lines = [f"{self.command} {path} HTTP/1.1"]
        for key, value in self.headers.items():
            if key.lower() in ("proxy-connection", "connection"):
                continue
            lines.append(f"{key}: {value}")
        # Force the target to close after responding so a read-until-EOF relay
        # terminates without parsing chunked/keep-alive framing.
        lines.append("Connection: close")
        request = ("\r\n".join(lines) + "\r\n\r\n").encode() + body
        upstream = socket.create_connection((host, port), timeout=30)
        upstream.settimeout(30)
        try:
            upstream.sendall(request)
            chunks = []
            while True:
                try:
                    chunk = upstream.recv(65536)
                except socket.timeout:
                    break
                if not chunk:
                    break
                chunks.append(chunk)
        finally:
            upstream.close()
        self.close_connection = True
        self.wfile.write(b"".join(chunks))

    def do_CONNECT(self):  # noqa: N802 - fixed by BaseHTTPRequestHandler
        host, _, port = self.path.partition(":")
        self.server.targets.append(f"{host}:{port}")  # type: ignore[attr-defined]
        upstream = socket.create_connection((host, int(port)), timeout=30)
        self.send_response(200)
        self.end_headers()
        _tunnel(self.connection, upstream)

    do_GET = _forward
    do_POST = _forward
    do_PUT = _forward
    do_HEAD = _forward
    do_DELETE = _forward


def _tunnel(a: socket.socket, b: socket.socket) -> None:
    """Pump bytes both directions until either side closes."""

    def pump(src, dst):
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    t = threading.Thread(target=pump, args=(b, a), daemon=True)
    t.start()
    pump(a, b)
    t.join(timeout=5)


@pytest.fixture()
def forward_proxy():
    """A logging forward proxy on loopback. Yields an object exposing `.url` (to
    set as HTTP(S)_PROXY) and `.targets` (the `host:port`s it was asked to reach)."""
    port = find_free_port()
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", port), _ForwardProxy)
    httpd.targets = []  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    class _Handle:
        url = f"http://127.0.0.1:{port}"
        targets = httpd.targets  # type: ignore[attr-defined]

    try:
        yield _Handle()
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def _proxy_env(proxy_url: str) -> dict:
    # NO_PROXY empty (not "*") so loopback targets are NOT bypassed — they must
    # route through the proxy for the assertion to mean anything.
    return {
        "HTTP_PROXY": proxy_url,
        "HTTPS_PROXY": proxy_url,
        "ALL_PROXY": proxy_url,
        "NO_PROXY": "",
    }


def test_sync_bypasses_forward_proxy(forward_proxy, tmp_path_factory, pypiron_bin, tmp_path):
    """A full sync ignores ambient proxies so hostname SSRF checks stay local."""
    with ExitStack() as stack:
        src_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
        source = next(src_gen)
        stack.callback(src_gen.close)
        dst_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
        dest = next(dst_gen)
        stack.callback(dst_gen.close)

        wheel = make_wheel("egresspkg", "1.0", tmp_path)
        upload_legacy(
            source["legacy"],
            wheel,
            username=source["admin_user"],
            password=source["admin_password"],
        )
        wait_for_file_in_index(source["simple"], "egresspkg", wheel.name)

        env = os.environ.copy()
        env.update(_proxy_env(forward_proxy.url))
        env["PYPIRON_ADVISORY_FEED"] = ""  # keep the sync run hermetic
        result = subprocess.run(
            [
                str(pypiron_bin),
                "sync",
                "--from",
                source["base_url"],
                "--to",
                dest["base_url"],
                "--admin-user",
                dest["admin_user"],
                "--admin-pass",
                dest["admin_password"],
                "--include-package",
                "egresspkg",
                "--exclude-newer",
                "",  # opt out of the 7-day cooldown so a fresh upload transfers
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, f"sync failed:\n{result.stdout}\n{result.stderr}"
        assert (dest["data_dir"] / "packages" / "egresspkg").exists(), (
            "package never landed in the destination"
        )
        assert forward_proxy.targets == []


def test_advisory_poll_routes_through_forward_proxy(
    forward_proxy, tmp_path_factory, pypiron_bin, tmp_path
):
    """A server started with an explicit --advisory-feed and proxy env set pulls
    the feed through the forward proxy and arms blocking. Proves the advisory feed
    client's proxy-honoring egress reaches the feed via the proxy and still works."""
    feed_bytes = make_osv_zip(tmp_path / "osv.zip", canonical_records()).read_bytes()
    with _http_feed_server(feed_bytes) as (feed_url, feed_bind):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        port = find_free_port()
        env = os.environ.copy()
        env.update(_proxy_env(forward_proxy.url))
        env["RUST_LOG"] = "info,pypiron=debug"
        proc = subprocess.Popen(
            [
                str(pypiron_bin),
                "serve",
                "--bind-addr",
                f"127.0.0.1:{port}",
                "--data-dir",
                str(data_dir),
                "--admin-user",
                "admin",
                "--admin-pass",
                "secret",
                "--advisory-feed",
                feed_url,
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        try:
            # It only binds once the explicit feed is obtained (fail-closed), so a
            # successful bind already proves the pull went through the proxy.
            wait_http_responding(f"http://127.0.0.1:{port}/simple/index.json", timeout=25.0)
            assert _wait_metric(port, "pypiron_advisory_snapshot_age_seconds", timeout=15.0)
            assert feed_bind in forward_proxy.targets, forward_proxy.targets
        finally:
            kill_process_tree(proc)


@contextmanager
def _http_feed_server(payload: bytes):
    """Serve `payload` as the advisory zip over plain-HTTP loopback. Yields
    (feed_url, "host:port")."""
    port = find_free_port()
    httpd = http.server.HTTPServer(("127.0.0.1", port), _make_zip_handler(payload))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/osv.zip", f"127.0.0.1:{port}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


# --------------------------------------------------------------------------- #
# T2: custom-CA (MITM) TLS upstream                                           #
# --------------------------------------------------------------------------- #


def _make_zip_handler(payload: bytes):
    class _Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # noqa: D102
            pass

        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("ETag", '"egress-fixture"')
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return _Handler


def _gen_ca_and_leaf(work: Path):
    """A private CA that signs a TLS leaf for 127.0.0.1 — the corporate-MITM
    model: trust the CA, the interceptor presents a CA-signed leaf. Returns
    (ca_pem, chain_pem, leaf_key). Skips the test if openssl can't build it."""
    ca_key, ca = work / "ca.key", work / "ca.pem"
    leaf_key, leaf_csr, leaf = work / "leaf.key", work / "leaf.csr", work / "leaf.pem"
    chain, ext = work / "chain.pem", work / "leaf.ext"
    ext.write_text(
        "subjectAltName=IP:127.0.0.1\n"
        "basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n"
    )
    try:
        _run_openssl(
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            ca_key,
            "-out",
            ca,
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=pypiron-egress-test-ca",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        )
        _run_openssl(
            "req",
            "-newkey",
            "rsa:2048",
            "-keyout",
            leaf_key,
            "-out",
            leaf_csr,
            "-nodes",
            "-subj",
            "/CN=127.0.0.1",
        )
        _run_openssl(
            "x509",
            "-req",
            "-in",
            leaf_csr,
            "-CA",
            ca,
            "-CAkey",
            ca_key,
            "-CAcreateserial",
            "-out",
            leaf,
            "-days",
            "1",
            "-extfile",
            ext,
        )
    except subprocess.CalledProcessError as exc:  # old openssl without -addext
        pytest.skip(f"openssl could not build the test cert: {exc}")
    chain.write_bytes(leaf.read_bytes() + ca.read_bytes())
    return ca, chain, leaf_key


def _run_openssl(*args):
    subprocess.run(["openssl", *[str(a) for a in args]], check=True, capture_output=True)


@contextmanager
def _tls_feed_server(chain: Path, key: Path, payload: bytes):
    """Serve `payload` as the advisory zip over TLS with `chain`/`key`. Yields the
    https feed URL."""
    port = find_free_port()
    httpd = http.server.HTTPServer(("127.0.0.1", port), _make_zip_handler(payload))
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(chain), str(key))
    httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"https://127.0.0.1:{port}/osv.zip"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


@pytest.mark.skipif(not cmd_exists("openssl"), reason="openssl not available")
def test_advisory_feed_fails_closed_without_upstream_ca(pypiron_bin, tmp_path):
    """A self-signed-CA TLS feed is untrusted by the built-in roots: an explicit
    --advisory-feed can't be obtained, so serve fails closed (exits) instead of
    binding with malware blocking silently disabled."""
    _ca, chain, key = _gen_ca_and_leaf(tmp_path)
    feed_bytes = make_osv_zip(tmp_path / "osv.zip", canonical_records()).read_bytes()
    with _tls_feed_server(chain, key, feed_bytes) as feed_url:
        result = _serve_exits(pypiron_bin, tmp_path / "d_noca", ["--advisory-feed", feed_url])
    assert result.returncode != 0, result.stdout
    assert "advisory" in result.stdout.lower(), result.stdout


@pytest.mark.skipif(not cmd_exists("openssl"), reason="openssl not available")
def test_advisory_feed_loads_with_upstream_ca(pypiron_bin, tmp_path):
    """With the corporate CA passed as --upstream-ca-cert, the same self-signed
    TLS feed validates: the server boots and arms blocking from the snapshot."""
    ca, chain, key = _gen_ca_and_leaf(tmp_path)
    feed_bytes = make_osv_zip(tmp_path / "osv.zip", canonical_records()).read_bytes()
    with _tls_feed_server(chain, key, feed_bytes) as feed_url:
        data_dir = tmp_path / "d_ca"
        data_dir.mkdir()
        port = find_free_port()
        env = os.environ.copy()
        env.update(_DIRECT_EGRESS)  # direct TLS to the feed; the CA is the gate
        env["RUST_LOG"] = "info,pypiron=debug"
        proc = subprocess.Popen(
            [
                str(pypiron_bin),
                "serve",
                "--bind-addr",
                f"127.0.0.1:{port}",
                "--data-dir",
                str(data_dir),
                "--admin-user",
                "admin",
                "--admin-pass",
                "secret",
                "--advisory-feed",
                feed_url,
                "--upstream-ca-cert",
                str(ca),
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_http_responding(f"http://127.0.0.1:{port}/simple/index.json", timeout=25.0)
            assert _wait_metric(port, "pypiron_advisory_snapshot_age_seconds", timeout=15.0)
        finally:
            kill_process_tree(proc)


def _serve_exits(pypiron_bin, data_dir: Path, extra_args, *, timeout: float = 30.0):
    """Run `serve` under direct egress, expecting it to exit (the advisory obtain
    runs before the listener binds, so a fail-closed refusal returns)."""
    data_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(_DIRECT_EGRESS)
    args = [
        str(pypiron_bin),
        "serve",
        "--bind-addr",
        f"127.0.0.1:{find_free_port()}",
        "--data-dir",
        str(data_dir),
        *extra_args,
    ]
    return subprocess.run(
        args, env=env, capture_output=True, text=True, timeout=timeout, cwd=str(data_dir)
    )


def _wait_metric(port: int, name: str, *, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        code, body, _ = http_get(f"http://127.0.0.1:{port}/metrics")
        if code == 200 and name in body.decode():
            return True
        time.sleep(0.3)
    return False
