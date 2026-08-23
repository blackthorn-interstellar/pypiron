"""A destination that doesn't report an origin claim must not silence
`--as-private`.

`pypiron sync --as-private` refuses to migrate onto a name the destination
already holds as a mirror, and it learns the claim from the `x-pypiron-origin`
header on `/sync/local-index`. Anything between client and server can take that
header away — a destination older than the header, or a reverse proxy / CDN that
strips unknown `x-` headers. Silence must not read as "unclaimed": with the
filename-keyed skip in play, every selected file would match a mirror-owned one,
nothing would upload, no POST would reach the server's private-vs-mirror
adjudication, and the run would exit 0 while the mirror kept serving the name.

This drives the REAL binary against a real destination through a forwarder that
drops the header, and asserts the migration still fails — via the server's own
403, which is the fail-closed backstop.
"""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterator, Tuple

import pytest

from .helpers import (
    find_free_port,
    make_wheel,
    origin_owner,
    sha256_file,
    sync_to,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration

PACKAGE = "headerlesspkg"
VERSION = "2.1.0"
STRIPPED_HEADER = "x-pypiron-origin"

# Per-hop headers a forwarder owns rather than copies.
_HOP_BY_HOP = {
    "connection",
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


# ----------------------------- the fake source --------------------------------


class _SourceHandler(BaseHTTPRequestHandler):
    """Serve a fixed route table: path -> (content_type, body). 404 otherwise."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *_args) -> None:
        pass

    def do_GET(self) -> None:
        route = self.server.routes.get(self.path)
        if route is None:
            self.send_error(404, "not found")
            return
        content_type, body = route
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True


class _SourceServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, routes: Dict[str, Tuple[str, bytes]]):
        super().__init__(address, _SourceHandler)
        self.routes = routes


# --------------------- the header-stripping forwarder -------------------------


class _StrippingHandler(BaseHTTPRequestHandler):
    """Forward every request to the real destination, minus one response header.

    A stand-in for the reverse proxy in front of a real deployment (or a dest
    too old to send the header at all): the sync client sees a fully working
    pypiron that simply never states who owns the name.
    """

    protocol_version = "HTTP/1.1"

    def log_message(self, *_args) -> None:
        pass

    def _read_body(self) -> bytes | None:
        """The request body, or None if it arrived chunked (unsupported here)."""
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            return None
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def _forward(self) -> None:
        body = self._read_body()
        if body is None:
            self.send_error(501, "forwarder cannot read a chunked request body")
            self.close_connection = True
            return
        headers = {
            k: v
            for k, v in self.headers.items()
            if k.lower() not in _HOP_BY_HOP and k.lower() != "host"
        }
        req = urllib.request.Request(
            f"{self.server.target}{self.path}",
            data=body if self.command != "GET" else None,
            headers=headers,
            method=self.command,
        )
        try:
            resp = urllib.request.urlopen(req, timeout=120)
        except urllib.error.HTTPError as e:  # a 4xx/5xx is a real answer, not a failure
            resp = e
        with resp:
            payload = resp.read()
            status = resp.status
            out = [
                (k, v)
                for k, v in resp.headers.items()
                if k.lower() not in _HOP_BY_HOP and k.lower() != STRIPPED_HEADER
            ]
        self.server.stripped += sum(1 for k in resp.headers if k.lower() == STRIPPED_HEADER)
        self.send_response(status)
        for k, v in out:
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(payload)
        self.close_connection = True

    do_GET = do_POST = do_PUT = do_DELETE = _forward


class _StrippingForwarder(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, target: str):
        super().__init__(address, _StrippingHandler)
        self.target = target.rstrip("/")
        self.stripped = 0


def _serve(server: ThreadingHTTPServer, name: str) -> Iterator[str]:
    thread = threading.Thread(target=server.serve_forever, name=name, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


# ---------------------------------- the test ----------------------------------


def test_as_private_onto_mirror_fails_when_dest_reports_no_origin(
    disk_server, pypiron_bin, tmp_path
):
    wheel = make_wheel(PACKAGE, VERSION, tmp_path)
    wheel_bytes = wheel.read_bytes()
    local_sha = sha256_file(wheel)

    base_path = "/corp/dev/+simple"
    index_path = f"{base_path}/{PACKAGE}/"
    file_path = f"/files/{wheel.name}"
    index_doc = json.dumps(
        {
            "meta": {"api-version": "1.0"},
            "name": PACKAGE,
            "files": [
                {
                    "filename": wheel.name,
                    "url": file_path,
                    "hashes": {"sha256": local_sha},
                    "size": len(wheel_bytes),
                }
            ],
        }
    ).encode()

    source = _SourceServer(
        ("127.0.0.1", find_free_port()),
        {
            index_path: ("application/vnd.pypi.simple.v1+json", index_doc),
            file_path: ("application/octet-stream", wheel_bytes),
        },
    )
    source_gen = _serve(source, "fake-source")
    source_url = next(source_gen)
    forwarder = _StrippingForwarder(("127.0.0.1", find_free_port()), disk_server["base_url"])
    forwarder_gen = _serve(forwarder, "origin-header-stripper")
    forwarder_url = next(forwarder_gen)
    try:
        common = ("--include-package", PACKAGE, "--include-format", "wheel")

        # Someone mirrored the name first — straight at the destination.
        rc, out, err = sync_to(pypiron_bin, disk_server, *common, source=f"{source_url}{base_path}")
        assert rc == 0, f"mirror seed failed:\n{out}\n{err}"
        wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel.name)
        pkg_dir = disk_server["data_dir"] / "packages" / PACKAGE
        assert origin_owner((pkg_dir / ".origin").read_text()) == "mirror"

        # Now migrate the same name private, through a destination that reports
        # no claim. The client can't refuse on the header, so it must re-offer
        # every file and let the server refuse.
        rc, out, err = sync_to(
            pypiron_bin,
            {**disk_server, "base_url": forwarder_url},
            *common,
            "--as-private",
            source=f"{source_url}{base_path}",
        )
        combined = out + err
        assert rc != 0, (
            "--as-private onto a mirror-owned name must fail even when the destination "
            f"reports no origin, not report success:\n{combined}"
        )
        # Proof it failed for the right reason: the upload happened and the
        # server's own private-vs-mirror adjudication rejected it.
        assert "mirror-owned" in combined, (
            f"failure must be the server's origin rejection:\n{combined}"
        )
        assert forwarder.stripped > 0, "the forwarder never saw an origin header to strip"

        # And it changed nothing: the mirror still owns the name and its bytes.
        assert origin_owner((pkg_dir / ".origin").read_text()) == "mirror"
        sidecar = json.loads((pkg_dir / f"{wheel.name}.meta.json").read_text())
        assert sidecar["sha256"] == local_sha
        assert sha256_file(pkg_dir / wheel.name) == local_sha
    finally:
        forwarder_gen.close()
        source_gen.close()
