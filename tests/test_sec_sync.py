"""What a hostile (or MITM'd) sync source must not be able to do.

`sync` reads a listing from a source the operator names and pushes to a
destination it holds admin credentials for. Everything in the listing — file
URLs, redirect targets, the project-status object — is attacker-influenceable
input. These drive the real binary against a fake source that abuses it.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterator, Optional, Tuple

import pytest

from .helpers import (
    ACCEPT_PEP691,
    find_free_port,
    http_request_auth,
    make_wheel,
    sha256_file,
    sync_to,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration


class _FakeSourceHandler(BaseHTTPRequestHandler):
    """Serve a fixed route table (path -> (status, content_type, body)) and
    record the Authorization header each path was asked with."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *_args) -> None:
        pass

    def do_GET(self) -> None:
        self.server.seen_auth[self.path] = self.headers.get("Authorization")
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
        self.seen_auth: Dict[str, Optional[str]] = {}


def _start_source(routes, port: Optional[int] = None) -> Iterator[Tuple[str, _FakeSourceServer]]:
    port = port if port is not None else find_free_port()
    server = _FakeSourceServer(("127.0.0.1", port), routes)
    thread = threading.Thread(target=server.serve_forever, name="fake-source", daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _listing(package: str, files, status=None) -> bytes:
    doc = {"meta": {"api-version": "1.0"}, "name": package, "files": list(files)}
    if status is not None:
        doc["project-status"] = status
    return json.dumps(doc).encode()


def _file_row(wheel, url: Optional[str] = None) -> Dict:
    return {
        "filename": wheel.name,
        "url": url if url is not None else f"/files/{wheel.name}",
        "hashes": {"sha256": sha256_file(wheel)},
        "size": wheel.stat().st_size,
    }


def _mirror_sync(pypiron_bin, server, package, source, *extra):
    return sync_to(
        pypiron_bin,
        server,
        "--include-package",
        package,
        "--exclude-newer",
        "",
        "--advisory-feed",
        "",
        *extra,
        source=f"{source}/simple",
    )


# --------------------- The source credential's blast radius -------------------


def test_plaintext_source_refuses_credentials_by_default(disk_server, pypiron_bin):
    """A credential aimed at an http:// source goes out in the clear, so the run
    refuses to start and names the override."""
    rc, out, err = _mirror_sync(
        pypiron_bin,
        disk_server,
        "anypkg",
        "http://127.0.0.1:9",
        "--source-user",
        "reader",
        "--source-pass",
        "secret",
    )
    combined = out + err
    assert rc != 0, combined
    assert "plaintext http://" in combined, combined
    assert "--allow-insecure-source" in combined, combined


def test_allow_insecure_source_permits_the_plaintext_credential(disk_server, pypiron_bin, tmp_path):
    """The override is the only difference: the same run mirrors, and the source
    sees the credential it was configured with."""
    package = "insecuresrcpkg"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    routes = {
        f"/simple/{package}/": (
            200,
            ACCEPT_PEP691,
            _listing(package, [_file_row(wheel)]),
        ),
        f"/files/{wheel.name}": (200, "application/octet-stream", wheel.read_bytes()),
    }
    source_gen = _start_source(routes)
    source_url, source = next(source_gen)
    try:
        rc, out, err = _mirror_sync(
            pypiron_bin,
            disk_server,
            package,
            source_url,
            "--source-user",
            "reader",
            "--source-pass",
            "secret",
            "--allow-insecure-source",
        )
        assert rc == 0, f"{out}\n{err}"
        wait_for_file_in_index(disk_server["simple"], package, wheel.name)
        assert source.seen_auth[f"/simple/{package}/"] is not None
    finally:
        source_gen.close()


def test_source_credential_stays_on_the_source_origin(disk_server, pypiron_bin, tmp_path):
    """The listing points the artifact at the same host on another port — a
    different service. The credential is scoped to the source's whole origin
    (scheme, host, port), so that fetch goes out anonymous."""
    package = "originscopedpkg"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    other_port = find_free_port()
    other_gen = _start_source(
        {f"/files/{wheel.name}": (200, "application/octet-stream", wheel.read_bytes())},
        port=other_port,
    )
    other_url, other = next(other_gen)
    try:
        listing = _listing(package, [_file_row(wheel, url=f"{other_url}/files/{wheel.name}")])
        source_gen = _start_source({f"/simple/{package}/": (200, ACCEPT_PEP691, listing)})
        source_url, source = next(source_gen)
        try:
            rc, out, err = _mirror_sync(
                pypiron_bin,
                disk_server,
                package,
                source_url,
                "--source-user",
                "reader",
                "--source-pass",
                "secret",
                "--allow-insecure-source",
            )
            assert rc == 0, f"{out}\n{err}"
            wait_for_file_in_index(disk_server["simple"], package, wheel.name)
            # The source itself was authenticated...
            assert source.seen_auth[f"/simple/{package}/"] is not None
            # ...and the off-origin hop it pointed us at was not.
            assert other.seen_auth[f"/files/{wheel.name}"] is None, (
                "the source credential leaked to another port on the same host"
            )
        finally:
            source_gen.close()
    finally:
        other_gen.close()


# ------------------------ The upstream project status -------------------------


def _quarantine(server, package: str) -> None:
    code, body, _ = http_request_auth(
        "POST",
        f"{server['base_url']}/project/{package}/status",
        data=b'{"status":"quarantined"}',
        username=server["admin_user"],
        password=server["admin_password"],
    )
    assert code in (200, 204), (code, body)


def _mirror_one(server, wheel) -> None:
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["admin_user"],
        password=server["admin_password"],
        fields={"mirror": "true"},
    )


def _status_of(server, package: str):
    """The destination's own status marker, read from storage — truth on disk,
    and unlike the rendered index it carries no rebuild lag to race."""
    marker = server["data_dir"] / "packages" / package / ".project-status.json"
    if not marker.exists():
        return None
    doc = json.loads(marker.read_text())
    # Active is the default and renders identically to no marker at all.
    return None if doc.get("status") == "active" else doc


def _sync_against_status(pypiron_bin, server, package, wheel, status):
    """Mirror `wheel`, quarantine it on the destination, then sync from a source
    whose listing carries `status` and offers no files (a lifted quarantine looks
    exactly like this). Returns the destination's status afterwards, plus logs."""
    _mirror_one(server, wheel)
    wait_for_file_in_index(server["simple"], package, wheel.name)
    _quarantine(server, package)
    assert _status_of(server, package)["status"] == "quarantined"

    routes = {f"/simple/{package}/": (200, ACCEPT_PEP691, _listing(package, [], status))}
    source_gen = _start_source(routes)
    source_url, _ = next(source_gen)
    try:
        rc, out, err = _mirror_sync(pypiron_bin, server, package, source_url, "--full")
        assert rc == 0, f"{out}\n{err}"
        return _status_of(server, package), out + err
    finally:
        source_gen.close()


def test_upstream_clearing_a_status_relays(disk_server, pypiron_bin, tmp_path):
    """The control for the test below: upstream is authoritative, so a listing
    with no status marker does clear the destination's quarantine."""
    package = "statusclearedpkg"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    status, _ = _sync_against_status(pypiron_bin, disk_server, package, wheel, None)
    assert status is None, f"upstream said active; the quarantine should be gone: {status}"


def test_unreadable_upstream_status_holds_the_quarantine(disk_server, pypiron_bin, tmp_path):
    """A project-status we can't parse is not a verdict. Relaying it as "active"
    would clear a destination quarantine on the strength of upstream garbage, so
    the run holds the destination's own status and says so."""
    package = "statusheldpkg"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    status, logs = _sync_against_status(
        pypiron_bin, disk_server, package, wheel, {"status": "hexed"}
    )
    assert status is not None and status["status"] == "quarantined", (
        f"an unparseable upstream status cleared the destination's quarantine: {status}"
    )
    assert "can't parse" in logs, logs
