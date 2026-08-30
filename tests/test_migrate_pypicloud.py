"""Pypicloud migration: operator patterns select stored private projects."""

from __future__ import annotations

import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterator, Optional, Tuple

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


class _PypicloudHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args) -> None:
        pass

    def do_GET(self) -> None:
        self.server.seen[self.path] = self.headers.get("Authorization")
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


class _PypicloudServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, routes):
        super().__init__(address, _PypicloudHandler)
        self.routes = routes
        self.seen: Dict[str, Optional[str]] = {}


def _start_source(routes) -> Iterator[Tuple[str, _PypicloudServer]]:
    server = _PypicloudServer(("127.0.0.1", find_free_port()), routes)
    thread = threading.Thread(target=server.serve_forever, name="fake-pypicloud", daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _json(value) -> Tuple[int, str, bytes]:
    return 200, "application/json", json.dumps(value).encode()


def _record(package: str, wheel, *, uploader: Optional[str], with_hash: bool = True):
    metadata = {}
    if uploader is not None:
        metadata["uploader"] = uploader
    if with_hash:
        metadata["hash_sha256"] = sha256_file(wheel)
    return {
        "name": package,
        "filename": wheel.name,
        "version": "1.0.0",
        "url": f"/ignored/{wheel.name}",
        "metadata": metadata,
    }


def _pypicloud_sync(pypiron_bin, disk_server, source, *extra):
    return sync_to(
        pypiron_bin,
        disk_server,
        "--source-kind",
        "pypicloud",
        "--as-private",
        "--exclude-newer",
        "",
        "--advisory-feed",
        "",
        *extra,
        source=source,
    )


def test_patterns_migrate_only_matching_private_projects(disk_server, pypiron_bin, tmp_path):
    private_name = "acme-private"
    public_name = "requests"
    private_wheel = make_wheel(private_name, "1.0.0", tmp_path / "private")
    routes = {
        "/api/package/": _json({"packages": [public_name, "Acme_Private"]}),
        f"/api/package/{private_name}/": _json(
            {"packages": [_record(private_name, private_wheel, uploader="builder")]}
        ),
        f"/api/package/{private_name}/{private_wheel.name}": (
            200,
            "application/octet-stream",
            private_wheel.read_bytes(),
        ),
        # A public record exists, but selecting it would make this test fail: no
        # detail or artifact route is exposed for it.
    }
    source_gen = _start_source(routes)
    source_url, source = next(source_gen)
    try:
        rc, out, err = _pypicloud_sync(
            pypiron_bin,
            disk_server,
            source_url,
            "--private-pattern",
            "Acme_*",
            "--source-user",
            "reader",
            "--source-pass",
            "secret",
            "--allow-insecure-source",
        )
        assert rc == 0, f"{out}\n{err}"
        wait_for_file_in_index(disk_server["simple"], private_name, private_wheel.name)
        package_dir = disk_server["data_dir"] / "packages" / private_name
        assert origin_owner((package_dir / ".origin").read_text()) == "private"
        assert sha256_file(package_dir / private_wheel.name) == sha256_file(private_wheel)
        assert not (disk_server["data_dir"] / "packages" / public_name).exists()

        expected_auth = "Basic " + base64.b64encode(b"reader:secret").decode()
        assert source.seen["/api/package/"] == expected_auth
        assert source.seen[f"/api/package/{private_name}/"] == expected_auth
        assert source.seen[f"/api/package/{private_name}/{private_wheel.name}"] == expected_auth
        assert f"/api/package/{public_name}/" not in source.seen
    finally:
        source_gen.close()


def test_explicit_legacy_package_computes_a_missing_hash(disk_server, pypiron_bin, tmp_path):
    package = "legacy-private"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    routes = {
        f"/api/package/{package}/": _json(
            {"packages": [_record(package, wheel, uploader=None, with_hash=False)]}
        ),
        f"/api/package/{package}/{wheel.name}": (
            200,
            "application/octet-stream",
            wheel.read_bytes(),
        ),
    }
    source_gen = _start_source(routes)
    source_url, _ = next(source_gen)
    try:
        rc, out, err = _pypicloud_sync(
            pypiron_bin,
            disk_server,
            source_url,
            "--include-package",
            package,
        )
        assert rc == 0, f"{out}\n{err}"
        assert "without uploader metadata" in out + err
        wait_for_file_in_index(disk_server["simple"], package, wheel.name)
        package_dir = disk_server["data_dir"] / "packages" / package
        sidecar = json.loads((package_dir / f"{wheel.name}.meta.json").read_text())
        assert sidecar["sha256"] == sha256_file(wheel)
    finally:
        source_gen.close()


def test_pattern_file_dry_run_selects_without_writing(disk_server, pypiron_bin, tmp_path):
    package = "partner-private"
    wheel = make_wheel(package, "1.0.0", tmp_path / "wheel")
    patterns = tmp_path / "private-packages.txt"
    patterns.write_text("# owned namespaces\n\nPartner_*\n")
    routes = {
        "/api/package/": _json({"packages": [package, "requests"]}),
        f"/api/package/{package}/": _json(
            {"packages": [_record(package, wheel, uploader="builder")]}
        ),
    }
    source_gen = _start_source(routes)
    source_url, _ = next(source_gen)
    try:
        rc, out, err = _pypicloud_sync(
            pypiron_bin,
            disk_server,
            source_url,
            "--private-patterns-from",
            str(patterns),
            "--dry-run",
        )
        assert rc == 0, f"{out}\n{err}"
        assert f"would copy {wheel.name}" in out + err
        assert not (disk_server["data_dir"] / "packages" / package).exists()
    finally:
        source_gen.close()


def test_pypicloud_mode_requires_private_ownership_rules(disk_server, pypiron_bin):
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--source-kind",
        "pypicloud",
        "--as-private",
        source="https://old.example.com",
    )
    assert rc != 0
    assert "explicit private work list" in out + err


def test_pypicloud_mode_requires_as_private(disk_server, pypiron_bin):
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--source-kind",
        "pypicloud",
        "--private-pattern",
        "acme-*",
        source="https://old.example.com",
    )
    assert rc != 0
    assert "requires --as-private" in out + err


def test_pypicloud_mode_requires_an_explicit_source(disk_server, pypiron_bin):
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--source-kind",
        "pypicloud",
        "--as-private",
        "--private-pattern",
        "acme-*",
    )
    assert rc != 0
    assert "requires --from" in out + err
