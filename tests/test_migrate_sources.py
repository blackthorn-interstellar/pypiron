"""Migration source-layout matrix: `pypiron sync --from <incumbent> --as-private`.

The "migrate off devpi/Artifactory/Nexus in one command" promise only holds if
pypiron reacts sanely to what each incumbent actually *serves* at its simple
endpoint. devpi and modern Artifactory/Nexus can emit PEP 691 JSON; stock
Artifactory 7.x / Nexus 3.x default to the HTML PEP 503 simple API, and a
fat-fingered credential yields a 200 HTML login page. This matrix stands up each
of those five sources as a fake index (no Docker, no devpi) and drives the REAL
pypiron binary over HTTP:

  a. Artifactory HTML-only PEP 503        -> fail-closed, cause named
  b. Nexus HTML-only PEP 503              -> fail-closed, cause named
  c. PEP 691 JSON at an Artifactory base  -> migrates byte-exact
  d. PEP 691 JSON at a devpi +simple base -> migrates byte-exact
  e. 200 HTML login page (auth failure)   -> fail-closed, cause named

The JSON rows assert a byte-exact round-trip (stored sidecar sha256 == source
wheel sha256, like tests/test_migrate.py). The HTML/login rows assert pypiron
refuses with a clear, actionable line — never an opaque serde parse error and
never a silent "0 files, success".
"""

from __future__ import annotations

import json
import threading
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


class _FakeSourceHandler(BaseHTTPRequestHandler):
    """Serve a fixed route table: path -> (status, content_type, body bytes).

    A missing route is a 404. This deliberately ignores the client's Accept
    header — exactly what a stock Artifactory/Nexus does when it only knows how
    to render HTML, and the crux of the bug under test.
    """

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

    def __init__(self, address, routes: Dict[str, Tuple[int, str, bytes]]):
        super().__init__(address, _FakeSourceHandler)
        self.routes = routes


def _start_source(routes: Dict[str, Tuple[int, str, bytes]]) -> Iterator[Tuple[str, int]]:
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


# --------------------------- JSON source rows (c, d) ---------------------------

# (label, simple base path segment). The base is what the migrate guide tells an
# operator to pass to `--from`; simple_root honors a base already ending in its
# simple segment verbatim, so both an Artifactory-style and a devpi-style layout
# work by naming the endpoint in full.
JSON_LAYOUTS = [
    ("artifactory", "/artifactory/api/pypi/corp/simple"),
    ("devpi", "/corp/dev/+simple"),
]


@pytest.mark.parametrize("label,base_path", JSON_LAYOUTS, ids=[r[0] for r in JSON_LAYOUTS])
def test_migrate_json_source_round_trips_byte_exact(
    label, base_path, disk_server, pypiron_bin, tmp_path
):
    package = f"jsonpkg{label}"
    version = "1.2.3"
    wheel = make_wheel(package, version, tmp_path)
    wheel_bytes = wheel.read_bytes()
    local_sha = sha256_file(wheel)

    index_path = f"{base_path}/{package}/"
    file_path = f"/files/{wheel.name}"
    index_doc = json.dumps(
        {
            "meta": {"api-version": "1.0"},
            "name": package,
            "files": [
                {
                    "filename": wheel.name,
                    # Absolute URL on the same source host, resolved against the
                    # listing page the same way a real PEP 691 client would.
                    "url": file_path,
                    "hashes": {"sha256": local_sha},
                    "size": len(wheel_bytes),
                }
            ],
        }
    ).encode()

    routes = {
        index_path: (200, "application/vnd.pypi.simple.v1+json", index_doc),
        file_path: (200, "application/octet-stream", wheel_bytes),
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
            "--as-private",
            source=f"{source_url}{base_path}",
        )
        assert rc == 0, f"JSON migration should succeed:\n{out}\n{err}"

        wait_for_file_in_index(disk_server["simple"], package, wheel.name)

        pkg_dir = disk_server["data_dir"] / "packages" / package
        assert origin_owner((pkg_dir / ".origin").read_text()) == "private", (
            f"migrated package must land private, not mirror:\n{out}\n{err}"
        )
        # Byte-exact: the stored sidecar digest equals the source wheel's.
        sidecar = json.loads((pkg_dir / f"{wheel.name}.meta.json").read_text())
        assert sidecar["sha256"] == local_sha
    finally:
        source_gen.close()


# ----------------------- Non-JSON source rows (a, b, e) ------------------------

_ARTIFACTORY_HTML = (
    "<!DOCTYPE html>\n<html><head><title>Links for pkg</title></head>\n"
    '<body><h1>Links for pkg</h1>\n<a href="pkg-1.0-py3-none-any.whl#sha256=deadbeef">'
    "pkg-1.0-py3-none-any.whl</a><br/>\n</body></html>\n"
).encode()

_NEXUS_HTML = (
    "<html>\n<head><title>Nexus Repository Manager</title></head>\n<body>\n"
    '<a href="pkg/pkg-1.0-py3-none-any.whl">pkg-1.0-py3-none-any.whl</a>\n'
    "</body>\n</html>\n"
).encode()

_LOGIN_HTML = (
    "<!DOCTYPE html>\n<html><head><title>Sign in</title></head>\n<body>\n"
    '<form method="post" action="/login">\n'
    '<input name="username"><input name="password" type="password">\n'
    "</form>\n</body></html>\n"
).encode()

# (label, base path, index content-type, index body). All three answer 200 with
# HTML at the package listing — the failure modes an SRE actually hits.
NON_JSON_ROWS = [
    (
        "artifactory_html",
        "/artifactory/api/pypi/corp/simple",
        "text/html; charset=utf-8",
        _ARTIFACTORY_HTML,
    ),
    ("nexus_html", "/repository/corp/simple", "text/html;charset=UTF-8", _NEXUS_HTML),
    ("login_page", "/corp/dev/+simple", "text/html; charset=utf-8", _LOGIN_HTML),
]


@pytest.mark.parametrize(
    "label,base_path,content_type,body",
    NON_JSON_ROWS,
    ids=[r[0] for r in NON_JSON_ROWS],
)
def test_migrate_non_json_source_fails_closed(
    label, base_path, content_type, body, disk_server, pypiron_bin
):
    package = f"htmlpkg{label.replace('_', '')}"
    index_path = f"{base_path}/{package}/"
    routes = {index_path: (200, content_type, body)}
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
            "--as-private",
            source=f"{source_url}{base_path}",
        )
        combined = out + err
        # Fail-closed: the adversary's stock Artifactory/Nexus or login page must
        # NOT read as "0 files, success".
        assert rc != 0, f"non-JSON source must fail the sync, not silently pass:\n{combined}"
        # The error names the cause and the fix — not an opaque serde error.
        assert "expected value" not in combined, (
            f"leaked a raw serde parse error instead of a clear cause:\n{combined}"
        )
        assert "HTML" in combined, f"error must name the HTML cause:\n{combined}"
        assert "PEP 691 JSON" in combined, f"error must name what's required:\n{combined}"
        assert "--from" in combined, f"error must point at the fix:\n{combined}"

        # And nothing landed in the destination's private namespace.
        pkg_dir = disk_server["data_dir"] / "packages" / package
        assert not pkg_dir.exists(), (
            f"no artifact should be stored on a failed migrate:\n{combined}"
        )
    finally:
        source_gen.close()
