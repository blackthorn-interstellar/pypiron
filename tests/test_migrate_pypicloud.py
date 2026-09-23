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
        self.server.conditionals.append(self.headers.get("If-None-Match"))
        if self.server.etag and self.headers.get("If-None-Match") == self.server.etag:
            self.send_response(304)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        route = self.server.routes.get(self.path)
        if route is None:
            self.send_error(404, "not found")
            return
        status, content_type, body = route
        if body is None:
            self._stream_forever(status, content_type)
            return
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        if self.server.etag:
            self.send_header("ETag", self.server.etag)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def _stream_forever(self, status, content_type) -> None:
        """An error page that never ends, for as long as the client reads."""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Connection", "close")
        self.end_headers()
        chunk = b"x" * 65536
        try:
            while True:
                self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass
        self.close_connection = True


class _PypicloudServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, routes):
        super().__init__(address, _PypicloudHandler)
        self.routes = routes
        self.seen: Dict[str, Optional[str]] = {}
        self.etag = None
        self.conditionals = []


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


def _pypicloud_sync(pypiron_bin, disk_server, source, *extra, **kwargs):
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
        **kwargs,
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


def test_endless_error_page_fails_fast_with_a_short_message(disk_server, pypiron_bin):
    """A source that streams an unbounded error body must not be buffered whole:
    the run fails promptly, quoting only a snippet."""
    source_gen = _start_source({"/api/package/": (500, "text/plain", None)})
    source_url, _source = next(source_gen)
    try:
        rc, out, err = _pypicloud_sync(
            pypiron_bin, disk_server, source_url, "--private-pattern", "acme-*", timeout=60
        )
    finally:
        next(source_gen, None)
    assert rc != 0
    assert "[500" in out + err
    assert len(out + err) < 64 * 1024


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


# Pypicloud's package API returns Unix seconds, not an RFC 3339 string.
SOURCE_DATE = "2020-01-02T03:04:05Z"
SOURCE_SECONDS = 1577934245


def _dated_routes(package, wheel, *, timestamp=SOURCE_SECONDS, with_hash=True):
    record = _record(package, wheel, uploader="builder", with_hash=with_hash)
    record["last_modified"] = timestamp
    return {
        f"/api/package/{package}/": _json({"packages": [record]}),
        f"/api/package/{package}/{wheel.name}": (
            200,
            "application/octet-stream",
            wheel.read_bytes(),
        ),
    }


def _sidecar(server, package, wheel):
    return server["data_dir"] / "packages" / package / f"{wheel.name}.meta.json"


def _wait_date(server, package, wheel, date):
    import time

    from .helpers import get_index_json

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        index = get_index_json(server["simple"], package)
        if any(
            f["filename"] == wheel.name and f.get("upload-time") == date for f in index["files"]
        ):
            return
        time.sleep(0.1)
    raise AssertionError(f"upload date never became {date}: {index}")


def test_pypicloud_preserves_date_for_uv_exclude_newer(
    disk_server, pypiron_bin, tmp_path, uv_path, uv_venv
):
    from .helpers import run_checked

    package = "historical-private"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    source_gen = _start_source(_dated_routes(package, wheel))
    source_url, _ = next(source_gen)
    try:
        rc, out, err = _pypicloud_sync(
            pypiron_bin, disk_server, source_url, "--include-package", package
        )
        assert rc == 0, f"{out}\n{err}"
    finally:
        source_gen.close()
    index = wait_for_file_in_index(disk_server["simple"], package, wheel.name)
    assert index["files"][0]["upload-time"] == SOURCE_DATE
    sc = json.loads(_sidecar(disk_server, package, wheel).read_text())
    assert sc["origin"] == "private"
    assert sc["upload-epoch-ms"] > SOURCE_SECONDS * 1000
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(uv_venv),
            "--index-url",
            disk_server["simple"],
            "--no-cache",
            "--exclude-newer",
            "2021-01-01T00:00:00Z",
            package,
        ]
    )


@pytest.mark.parametrize("with_hash", [True, False])
def test_repair_dates_is_hash_checked_previewable_and_repeatable(
    disk_server, pypiron_bin, tmp_path, with_hash
):
    from .helpers import upload_legacy

    package = "repair-private"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    upload_legacy(
        disk_server["legacy"], wheel, username=disk_server["user"], password=disk_server["password"]
    )
    wait_for_file_in_index(disk_server["simple"], package, wheel.name)
    sc_path = _sidecar(disk_server, package, wheel)
    before_bytes = sc_path.read_bytes()
    before = json.loads(before_bytes)
    artifact = sc_path.with_name(wheel.name)
    before_stat = artifact.stat()
    routes = _dated_routes(package, wheel, with_hash=with_hash)
    missing = make_wheel(package, "2.0.0", tmp_path / "missing")
    records = json.loads(routes[f"/api/package/{package}/"][2])
    records["packages"].append(
        {**_record(package, missing, uploader="builder"), "last_modified": SOURCE_SECONDS}
    )
    routes[f"/api/package/{package}/"] = _json(records)
    source_gen = _start_source(routes)
    source_url, source = next(source_gen)
    source.etag = '"historical-dates"'
    args = ("--include-package", f"{package}==1.0.0", "--repair-upload-times")
    try:
        # Ordinary re-sync continues to skip existing files. Repair is explicit.
        rc, out, err = _pypicloud_sync(
            pypiron_bin, disk_server, source_url, "--include-package", f"{package}==1.0.0"
        )
        assert rc == 0, f"{out}\n{err}"
        assert sc_path.read_bytes() == before_bytes
        source.conditionals.clear()
        cursor_path = disk_server["data_dir"] / "_sync" / "cursors.json"
        cursor_bytes = cursor_path.read_bytes()
        rc, out, err = _pypicloud_sync(pypiron_bin, disk_server, source_url, *args, "--dry-run")
        assert rc == 0, f"{out}\n{err}"
        assert f"would repair upload time {package}/{wheel.name} -> {SOURCE_DATE}" in out
        assert sc_path.read_bytes() == before_bytes
        rc, out, err = _pypicloud_sync(pypiron_bin, disk_server, source_url, *args)
        assert rc == 0, f"{out}\n{err}"
        _wait_date(disk_server, package, wheel, SOURCE_DATE)
        after = json.loads(sc_path.read_text())
        assert after == {**before, "upload-time": SOURCE_DATE, "yank-epoch": 1}
        assert artifact.stat().st_mtime_ns == before_stat.st_mtime_ns
        assert artifact.read_bytes() == wheel.read_bytes()
        assert not artifact.with_name(missing.name).exists()
        rc, out, err = _pypicloud_sync(
            pypiron_bin,
            disk_server,
            source_url,
            "--include-package",
            package,
            "--repair-upload-times",
            "--dry-run",
        )
        assert rc == 0, f"{out}\n{err}"
        assert "absent on destination; skipping" in out + err

        rc, out, err = _pypicloud_sync(pypiron_bin, disk_server, source_url, *args)
        assert rc == 0, f"{out}\n{err}"
        assert "upload date already matches" in out + err
        assert json.loads(sc_path.read_text()) == after
        assert all(value is None for value in source.conditionals)
        assert cursor_path.read_bytes() == cursor_bytes
        assert (f"/api/package/{package}/{wheel.name}" in source.seen) is not with_hash
    finally:
        source_gen.close()


@pytest.mark.parametrize("dry_run", [False, True])
def test_repair_refuses_source_hash_mismatch(disk_server, pypiron_bin, tmp_path, dry_run):
    from .helpers import upload_legacy

    package = "repair-mismatch"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    upload_legacy(
        disk_server["legacy"], wheel, username=disk_server["user"], password=disk_server["password"]
    )
    wait_for_file_in_index(disk_server["simple"], package, wheel.name)
    before = _sidecar(disk_server, package, wheel).read_bytes()
    routes = _dated_routes(package, wheel)
    record = json.loads(routes[f"/api/package/{package}/"][2])
    record["packages"][0]["metadata"]["hash_sha256"] = "0" * 64
    routes[f"/api/package/{package}/"] = _json(record)
    source_gen = _start_source(routes)
    source_url, _ = next(source_gen)
    try:
        rc, out, err = _pypicloud_sync(
            pypiron_bin,
            disk_server,
            source_url,
            "--include-package",
            package,
            "--repair-upload-times",
            *(["--dry-run"] if dry_run else []),
        )
        assert rc != 0
        assert "SHA-256 mismatch" in out + err
        assert _sidecar(disk_server, package, wheel).read_bytes() == before
    finally:
        source_gen.close()


@pytest.mark.parametrize("timestamp", [None, "not-a-date", 9223372036854775807])
def test_missing_and_invalid_source_dates(disk_server, pypiron_bin, tmp_path, timestamp):
    package = "missing-date"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    source_gen = _start_source(_dated_routes(package, wheel, timestamp=timestamp))
    source_url, _ = next(source_gen)
    try:
        rc, out, err = _pypicloud_sync(
            pypiron_bin, disk_server, source_url, "--include-package", package
        )
        if timestamp is not None:
            assert rc != 0
            assert not _sidecar(disk_server, package, wheel).exists()
            return
        assert rc == 0, f"{out}\n{err}"
        assert "source has no upload date" in out + err
        wait_for_file_in_index(disk_server["simple"], package, wheel.name)
        before = _sidecar(disk_server, package, wheel).read_bytes()
        rc, out, err = _pypicloud_sync(
            pypiron_bin,
            disk_server,
            source_url,
            "--include-package",
            package,
            "--repair-upload-times",
        )
        assert rc == 0, f"{out}\n{err}"
        assert "leaving destination unchanged" in out + err
        assert _sidecar(disk_server, package, wheel).read_bytes() == before
    finally:
        source_gen.close()


def test_private_migration_dates_require_admin_and_explicit_mode(disk_server, tmp_path):
    from .helpers import http_request_auth, upload_legacy

    server = disk_server
    package = "date-permissions"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    fields = {"migration": "true", "upload_time": SOURCE_DATE}
    upload_legacy(
        server["legacy"],
        wheel,
        fields=fields,
        username=server["uploader_user"],
        password=server["uploader_password"],
        expect_status=403,
    )
    admin = {"username": server["user"], "password": server["password"]}
    for invalid in (
        {"upload_time": SOURCE_DATE},
        {**fields, "mirror": "true"},
        {**fields, "yanked": "true"},
        {**fields, "upload_time": "bad"},
    ):
        upload_legacy(server["legacy"], wheel, fields=invalid, **admin, expect_status=400)
    upload_legacy(server["legacy"], wheel, **admin)
    wait_for_file_in_index(server["simple"], package, wheel.name)
    before = _sidecar(server, package, wheel).read_bytes()
    url = f"{server['base_url']}/files/{package}/{wheel.name}/upload-time"
    body = {"sha256": sha256_file(wheel), "upload-time": SOURCE_DATE}
    code, _, _ = http_request_auth(
        "POST",
        url,
        data=json.dumps(body).encode(),
        username=server["uploader_user"],
        password=server["uploader_password"],
    )
    assert code == 403
    for invalid, expected in (
        ({**body, "sha256": "0" * 64}, 409),
        ({**body, "upload-time": "bad"}, 400),
    ):
        code, response, _ = http_request_auth(
            "POST", url, data=json.dumps(invalid).encode(), **admin
        )
        assert code == expected, response
    assert _sidecar(server, package, wheel).read_bytes() == before
    # A date repair and a later yank preserve each other's metadata.
    code, response, _ = http_request_auth("POST", url, data=json.dumps(body).encode(), **admin)
    assert code == 200, response
    code, response, _ = http_request_auth(
        "POST", url.replace("upload-time", "yank"), data=b"withdrawn", **admin
    )
    assert code == 200, response
    sc = json.loads(_sidecar(server, package, wheel).read_text())
    assert sc["upload-time"] == SOURCE_DATE
    assert sc["yanked"] == "withdrawn"
    assert sc["yank-epoch"] == 2
    body["upload-time"] = "2019-01-01T00:00:00Z"
    code, response, _ = http_request_auth("POST", url, data=json.dumps(body).encode(), **admin)
    assert code == 200, response
    sc = json.loads(_sidecar(server, package, wheel).read_text())
    assert sc["yanked"] == "withdrawn"
    assert sc["yank-epoch"] == 3
    code, response, _ = http_request_auth("POST", url, data=json.dumps(body).encode(), **admin)
    assert code == 200, response
    assert json.loads(_sidecar(server, package, wheel).read_text()) == sc


def test_repair_requires_private_mode(disk_server, pypiron_bin):
    rc, out, err = sync_to(
        pypiron_bin, disk_server, "--include-package", "six", "--repair-upload-times"
    )
    assert rc != 0
    assert "--repair-upload-times requires --as-private" in out + err


def test_simple_source_private_migration_preserves_dates(
    disk_server, disk_server_wait_on_upload, pypiron_bin, tmp_path
):
    from .helpers import upload_legacy

    source = disk_server_wait_on_upload
    package = "simple-history"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    upload_legacy(
        source["legacy"],
        wheel,
        username=source["user"],
        password=source["password"],
        fields={"migration": "true", "upload_time": SOURCE_DATE},
    )
    wait_for_file_in_index(source["simple"], package, wheel.name)
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--include-package",
        package,
        "--as-private",
        "--advisory-feed",
        "",
        source=source["base_url"],
    )
    assert rc == 0, f"{out}\n{err}"
    index = wait_for_file_in_index(disk_server["simple"], package, wheel.name)
    assert index["files"][0]["upload-time"] == SOURCE_DATE
    assert json.loads(_sidecar(disk_server, package, wheel).read_text())["origin"] == "private"


def test_repair_endpoint_refuses_mirror_and_deleted_files(disk_server, tmp_path):
    from .helpers import http_request_auth, upload_legacy

    package = "repair-mirror"
    wheel = make_wheel(package, "1.0.0", tmp_path)
    server = disk_server
    admin = {"username": server["user"], "password": server["password"]}
    upload_legacy(server["legacy"], wheel, fields={"mirror": "true"}, **admin)
    wait_for_file_in_index(server["simple"], package, wheel.name)
    before = _sidecar(server, package, wheel).read_bytes()
    body = json.dumps({"sha256": sha256_file(wheel), "upload-time": SOURCE_DATE}).encode()
    url = f"{server['base_url']}/files/{package}/{wheel.name}"
    code, response, _ = http_request_auth("POST", f"{url}/upload-time", data=body, **admin)
    assert code == 409, response
    assert _sidecar(server, package, wheel).read_bytes() == before
    code, response, _ = http_request_auth("DELETE", url, **admin)
    assert code == 204, response
    code, response, _ = http_request_auth("POST", f"{url}/upload-time", data=body, **admin)
    assert code == 404, response
    assert not _sidecar(server, package, wheel).exists()
