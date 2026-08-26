"""Legacy (non-PEP-440) version handling: reject on upload, skip on mirror.

pypiron keeps malformed versions out of the store by default. A direct upload of
a non-PEP-440 version is refused with a clear ``400``; a ``sync`` run skips such
an upstream file instead of failing the whole run. ``--allow-legacy-versions``
restores the old accept-everything behavior on each side. These drive the real
binary over HTTP with the same clients an operator uses.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterator, Tuple

import pytest

from .conftest import _start_disk_server
from .helpers import (
    find_free_port,
    make_sdist,
    make_wheel,
    sha256_file,
    sync_to,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration

# Confirmed invalid PEP 440 (packaging.version.Version raises); a wheel/sdist
# whose version slot is this is exactly the "legacy" case the gate drops.
LEGACY_VERSION = "1.0.0.junk"
GOOD_VERSION = "1.2.3"


@pytest.fixture()
def legacy_ok_server(tmp_path_factory, pypiron_bin) -> Iterator[Dict]:
    """A server started with --allow-legacy-versions: accepts legacy versions."""
    yield from _start_disk_server(
        tmp_path_factory, pypiron_bin, extra_args=["--allow-legacy-versions"]
    )


def _make_dist(fmt: str, name: str, version: str, dest_dir):
    return (
        make_wheel(name, version, dest_dir)
        if fmt == "wheel"
        else make_sdist(name, version, dest_dir)
    )


# ------------------------------- Direct upload -------------------------------


@pytest.mark.parametrize("fmt", ["wheel", "sdist"])
def test_upload_legacy_version_rejected_by_default(disk_server, tmp_path, fmt):
    """A direct upload of a non-PEP-440 version is refused with an actionable
    400 — the store never sees the malformed version."""
    dist = _make_dist(fmt, "legacypkg", LEGACY_VERSION, tmp_path)
    creds = {"username": disk_server["admin_user"], "password": disk_server["admin_password"]}

    code, body = upload_legacy(disk_server["legacy"], dist, expect_status=400, **creds)
    text = body.decode("utf-8", "replace")
    assert LEGACY_VERSION in text, text
    assert "PEP 440" in text, text
    assert "--allow-legacy-versions" in text, text

    # Nothing landed under the name.
    assert not (disk_server["data_dir"] / "packages" / "legacypkg").exists()


def test_upload_legacy_version_accepted_with_flag(legacy_ok_server, tmp_path):
    """--allow-legacy-versions restores the old behavior: the same upload that
    is refused by default is stored, and installers can resolve it."""
    server = legacy_ok_server
    wheel = make_wheel("legacypkg", LEGACY_VERSION, tmp_path)
    creds = {"username": server["admin_user"], "password": server["admin_password"]}

    code, _ = upload_legacy(server["legacy"], wheel, expect_status=200, **creds)
    assert code == 200
    wait_for_file_in_index(server["simple"], "legacypkg", wheel.name)
    assert (server["data_dir"] / "packages" / "legacypkg" / wheel.name).exists()


def test_upload_pep440_version_unaffected(disk_server, tmp_path):
    """Regression guard: an ordinary PEP 440 upload is untouched by the gate."""
    wheel = make_wheel("normalpkg", GOOD_VERSION, tmp_path)
    creds = {"username": disk_server["admin_user"], "password": disk_server["admin_password"]}

    code, _ = upload_legacy(disk_server["legacy"], wheel, expect_status=200, **creds)
    assert code == 200
    wait_for_file_in_index(disk_server["simple"], "normalpkg", wheel.name)


# --------------------------------- Mirror ------------------------------------


class _FakeSourceHandler(BaseHTTPRequestHandler):
    """Serve a fixed route table: path -> (status, content_type, body)."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *_args) -> None:  # noqa: D401 - silence test spam
        pass

    def do_GET(self) -> None:
        route = self.server.routes.get(self.path)
        if route is None:
            self.send_error(404, "not found")
            return
        status, content_type, body = route
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True


class _FakeSourceServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, routes):
        super().__init__(address, _FakeSourceHandler)
        self.routes = routes


def _start_source(routes) -> Iterator[Tuple[str, int]]:
    port = find_free_port()
    server = _FakeSourceServer(("127.0.0.1", port), routes)
    thread = threading.Thread(target=server.serve_forever, name="fake-source", daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _file_row(wheel):
    return {
        "filename": wheel.name,
        "url": f"/files/{wheel.name}",
        "hashes": {"sha256": sha256_file(wheel)},
        "size": wheel.stat().st_size,
    }


def test_sync_skips_legacy_version_and_continues(disk_server, pypiron_bin, tmp_path):
    """A mirror run that meets a legacy-versioned upstream file skips just that
    file (logged) and mirrors the rest — one ancient release can't break it."""
    package = "mixedpkg"
    good = make_wheel(package, GOOD_VERSION, tmp_path)
    legacy = make_wheel(package, LEGACY_VERSION, tmp_path)

    index_doc = json.dumps(
        {
            "meta": {"api-version": "1.0"},
            "name": package,
            "files": [_file_row(good), _file_row(legacy)],
        }
    ).encode()
    routes = {
        f"/simple/{package}/": (200, "application/vnd.pypi.simple.v1+json", index_doc),
        f"/files/{good.name}": (200, "application/octet-stream", good.read_bytes()),
        f"/files/{legacy.name}": (200, "application/octet-stream", legacy.read_bytes()),
    }
    source_gen = _start_source(routes)
    source_url, _ = next(source_gen)
    try:
        rc, out, err = sync_to(
            pypiron_bin,
            disk_server,
            "--include-package",
            package,
            "--include-format",
            "wheel",
            "--exclude-newer",
            "",
            "--advisory-feed",
            "",
            source=f"{source_url}/simple",
        )
        assert rc == 0, f"one legacy file must not fail the run:\n{out}\n{err}"

        wait_for_file_in_index(disk_server["simple"], package, good.name)
        pkg_dir = disk_server["data_dir"] / "packages" / package
        assert (pkg_dir / good.name).exists(), "the PEP 440 file must still mirror"
        assert not (pkg_dir / legacy.name).exists(), "the legacy file must be skipped"

        combined = out + err
        assert "skipping non-PEP-440" in combined, f"the skip must be logged:\n{combined}"
        assert legacy.name in combined, combined
    finally:
        source_gen.close()


def test_sync_mirrors_legacy_version_with_flag(disk_server, pypiron_bin, tmp_path):
    """--allow-legacy-versions on the sync side mirrors the legacy file too (the
    destination must also allow it, so point at a legacy-accepting server)."""
    # The default disk_server rejects a legacy *private* upload, but a mirror
    # upload bypasses that gate (mirror trust), so the same server accepts the
    # relayed legacy file once sync stops filtering it.
    package = "mixedflagpkg"
    good = make_wheel(package, GOOD_VERSION, tmp_path)
    legacy = make_wheel(package, LEGACY_VERSION, tmp_path)

    index_doc = json.dumps(
        {
            "meta": {"api-version": "1.0"},
            "name": package,
            "files": [_file_row(good), _file_row(legacy)],
        }
    ).encode()
    routes = {
        f"/simple/{package}/": (200, "application/vnd.pypi.simple.v1+json", index_doc),
        f"/files/{good.name}": (200, "application/octet-stream", good.read_bytes()),
        f"/files/{legacy.name}": (200, "application/octet-stream", legacy.read_bytes()),
    }
    source_gen = _start_source(routes)
    source_url, _ = next(source_gen)
    try:
        rc, out, err = sync_to(
            pypiron_bin,
            disk_server,
            "--include-package",
            package,
            "--include-format",
            "wheel",
            "--exclude-newer",
            "",
            "--advisory-feed",
            "",
            "--allow-legacy-versions",
            source=f"{source_url}/simple",
        )
        assert rc == 0, f"sync failed:\n{out}\n{err}"

        pkg_dir = disk_server["data_dir"] / "packages" / package
        wait_for_file_in_index(disk_server["simple"], package, good.name)
        assert (pkg_dir / good.name).exists()
        assert (pkg_dir / legacy.name).exists(), "the flag must let the legacy file mirror"
    finally:
        source_gen.close()
