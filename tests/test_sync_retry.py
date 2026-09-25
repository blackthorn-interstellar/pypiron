"""Regression: one transient CDN error must not fail a sync run.

A single 503 ("first byte timeout") on one of 7,714 files marked the whole
package — and therefore the whole run — as failed, with no retry. At mirror
scale transient errors are a statistical certainty; sync retries each file
download with backoff before giving up.
"""

from __future__ import annotations

import hashlib
import io
import json
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from .helpers import find_free_port, sync_to

pytestmark = pytest.mark.integration

WHEEL_NAME = "flaky_pkg-1.0.0-py3-none-any.whl"


def make_wheel() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr(
            "flaky_pkg-1.0.0.dist-info/METADATA",
            "Metadata-Version: 2.1\nName: flaky-pkg\nVersion: 1.0.0\n",
        )
        zf.writestr("flaky_pkg-1.0.0.dist-info/WHEEL", "Wheel-Version: 1.0\n")
        zf.writestr("flaky_pkg-1.0.0.dist-info/RECORD", "")
    return buf.getvalue()


class FlakyPyPI(BaseHTTPRequestHandler):
    """Serves the PEP 691 Simple API normally; 503s the FIRST artifact only."""

    wheel = make_wheel()
    failures_remaining = 1
    lock = threading.Lock()

    def do_GET(self):  # noqa: N802 - stdlib naming
        if self.path == "/simple/flaky-pkg/":
            body = json.dumps(
                {
                    "meta": {"api-version": "1.1"},
                    "name": "flaky-pkg",
                    "files": [
                        {
                            "filename": WHEEL_NAME,
                            "url": f"http://127.0.0.1:{self.server.server_port}/files/{WHEEL_NAME}",
                            "hashes": {"sha256": hashlib.sha256(self.wheel).hexdigest()},
                            "size": len(self.wheel),
                            "upload-time": "2026-01-01T00:00:00.000000Z",
                        }
                    ],
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.pypi.simple.v1+json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == f"/files/{WHEEL_NAME}":
            with FlakyPyPI.lock:
                if FlakyPyPI.failures_remaining > 0:
                    FlakyPyPI.failures_remaining -= 1
                    self.send_response(503)
                    self.end_headers()
                    return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(self.wheel)))
            self.end_headers()
            self.wfile.write(self.wheel)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):  # quiet
        pass


def test_sync_retries_transient_download_failures(disk_server, pypiron_bin):
    port = find_free_port()
    httpd = ThreadingHTTPServer(("127.0.0.1", port), FlakyPyPI)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        # Mirror-over-HTTP against the flaky source: the 503 on the first
        # download attempt must be retried, and the run must still succeed.
        rc, out, err = sync_to(
            pypiron_bin,
            disk_server,
            "--include-package",
            "flaky-pkg",
            source=f"http://127.0.0.1:{port}",
            timeout=120,
        )
        assert rc == 0, f"sync failed:\n{out}\n{err}"
        stored = disk_server["data_dir"] / "packages" / "flaky-pkg" / WHEEL_NAME
        assert stored.exists(), "artifact missing after sync with one transient 503"
        assert stored.read_bytes() == FlakyPyPI.wheel
    finally:
        httpd.shutdown()


HUGE_NAME = "flaky_pkg-1.0.0-cp312-cp312-manylinux_2_28_x86_64.whl"


class OversizePyPI(BaseHTTPRequestHandler):
    """Lists a normal wheel plus one declared at 6 GiB — over the destination's
    5 GiB upload limit — and records which artifacts were fetched."""

    wheel = make_wheel()
    fetched: list[str] = []

    def do_GET(self):  # noqa: N802 - stdlib naming
        base = f"http://127.0.0.1:{self.server.server_port}/files"
        if self.path == "/simple/flaky-pkg/":
            files = [
                {
                    "filename": WHEEL_NAME,
                    "url": f"{base}/{WHEEL_NAME}",
                    "hashes": {"sha256": hashlib.sha256(self.wheel).hexdigest()},
                    "size": len(self.wheel),
                },
                {
                    "filename": HUGE_NAME,
                    "url": f"{base}/{HUGE_NAME}",
                    "hashes": {"sha256": "0" * 64},
                    "size": 6 * 1024**3,
                },
            ]
            body = json.dumps(
                {"meta": {"api-version": "1.1"}, "name": "flaky-pkg", "files": files}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.pypi.simple.v1+json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path.startswith("/files/"):
            OversizePyPI.fetched.append(self.path.rsplit("/", 1)[-1])
            if not self.path.endswith(WHEEL_NAME):
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(self.wheel)))
            self.end_headers()
            self.wfile.write(self.wheel)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):  # quiet
        pass


def test_sync_fails_oversize_file_without_downloading_it(disk_server, pypiron_bin):
    """A file over the destination's 5 GiB upload limit fails loudly before any
    download (it could never be uploaded); the rest of the package still lands."""
    port = find_free_port()
    httpd = ThreadingHTTPServer(("127.0.0.1", port), OversizePyPI)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        rc, out, err = sync_to(
            pypiron_bin,
            disk_server,
            "--include-package",
            "flaky-pkg",
            source=f"http://127.0.0.1:{port}",
            timeout=120,
        )
        assert rc != 0, f"an unuploadable file must fail the run:\n{out}\n{err}"
        assert "--exclude-larger" in out + err
        assert HUGE_NAME not in OversizePyPI.fetched, "oversize file was downloaded"
        stored = disk_server["data_dir"] / "packages" / "flaky-pkg" / WHEEL_NAME
        assert stored.read_bytes() == OversizePyPI.wheel
    finally:
        httpd.shutdown()
