"""SSRF hardening for the on-demand proxy.

The proxy fetches artifacts and their companions (`.metadata`, `.provenance`)
from URLs that come out of the *upstream listing* — attacker-influenceable if the
upstream is malicious or MITM'd. Those URLs can be absolute to any host, and the
companion bodies are reflected to the client (and are not hash-gated by default).
A URL pointed at an internal address (a cloud metadata endpoint, a loopback
admin port) must therefore be refused before the proxy ever connects.

The gap a plain DNS-resolver guard misses is the IP *literal*: hyper-util skips
DNS resolution when the host is already an address, so `http://127.0.0.1:.../x`
never reaches the resolver. This test stands up a real sink on 127.0.0.1, serves
a crafted listing whose companion URLs point straight at it, and asserts the
proxy refuses — the sink records no hit and its body is never relayed. The proxy
upstream is referenced as `localhost` so the sink's `127.0.0.1` literal is a
different (non-exempt) host than the trusted upstream.
"""

from __future__ import annotations

import hashlib
import http.server
import json
import threading
from typing import Dict, Iterator, Tuple

import pytest

from .conftest import _start_disk_server
from .helpers import find_free_port, http_get, make_wheel

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

PAST_UPLOAD_TIME = "2020-01-01T00:00:00Z"
SINK_SECRET = b"SSRF-SINK-SECRET-BODY-should-never-be-relayed"


class _SinkHandler(http.server.BaseHTTPRequestHandler):
    """An internal service the proxy must never be tricked into fetching. Every
    request is recorded; if the guard fails, the recorded hit (and the relayed
    body) makes the regression loud."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # noqa: D102 - silence default stderr spam
        pass

    def do_GET(self):  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        self.server.hits.append(self.path)  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(SINK_SECRET)))
        self.end_headers()
        self.wfile.write(SINK_SECRET)


class _IndexHandler(http.server.BaseHTTPRequestHandler):
    """A malicious/compromised upstream index. It serves crafted PEP 691 listings
    whose companion URLs point at the sink, plus a benign package whose companion
    lives on the (trusted) upstream host to prove the guard doesn't over-block."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # noqa: D102
        pass

    def do_GET(self):  # noqa: N802
        path = self.path.split("?", 1)[0]
        registry = self.server.registry  # type: ignore[attr-defined]

        if path.startswith("/simple/") and path.endswith("/"):
            pkg = path[len("/simple/") : -1]
            info = registry.get(pkg)
            if info is None:
                self.send_error(404)
                return
            self._send_json(_listing(pkg, info))
            return

        # Companion bytes for the benign package are served from here (the
        # trusted upstream host), so the proxy is allowed to fetch them.
        if path.endswith(".metadata"):
            for info in registry.values():
                if info.get("metadata") and path == info["metadata_path"]:
                    self._send_bytes(info["metadata"], "text/plain; charset=utf-8")
                    return

        self.send_error(404)

    def _send_json(self, obj: Dict) -> None:
        self._send_bytes(json.dumps(obj).encode(), "application/vnd.pypi.simple.v1+json")

    def _send_bytes(self, body: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _listing(pkg: str, info: Dict) -> Dict:
    file_entry: Dict = {
        "filename": info["filename"],
        "url": info["url"],
        "hashes": {"sha256": info["sha256"]},
        "size": info["size"],
        "upload-time": PAST_UPLOAD_TIME,
    }
    if "core_metadata" in info:
        file_entry["core-metadata"] = info["core_metadata"]
    if "provenance" in info:
        file_entry["provenance"] = info["provenance"]
    return {"meta": {"api-version": "1.0"}, "name": pkg, "files": [file_entry]}


class _Servers:
    """A malicious upstream index plus an internal sink, both on 127.0.0.1."""

    def __init__(self) -> None:
        sink_port = find_free_port()
        self.sink = http.server.ThreadingHTTPServer(("127.0.0.1", sink_port), _SinkHandler)
        self.sink.hits = []  # type: ignore[attr-defined]
        self.sink_base = f"http://127.0.0.1:{sink_port}"

        index_port = find_free_port()
        self.index = http.server.ThreadingHTTPServer(("127.0.0.1", index_port), _IndexHandler)
        self.index.registry = {}  # type: ignore[attr-defined]
        # Referenced as `localhost` so the trusted-upstream exemption keys on the
        # name "localhost", leaving the sink's "127.0.0.1" literal non-exempt.
        self.index_base = f"http://localhost:{index_port}"
        self.index_host_base = f"http://127.0.0.1:{index_port}"

        for srv, name in ((self.sink, "ssrf-sink"), (self.index, "ssrf-index")):
            threading.Thread(target=srv.serve_forever, name=name, daemon=True).start()

    @property
    def hits(self):
        return self.sink.hits  # type: ignore[attr-defined]

    def register_evil(self, pkg: str, wheel) -> str:
        """A listing whose artifact, .metadata, and .provenance all point at the
        internal sink via an IP literal."""
        data = wheel.read_bytes()
        sink_url = f"{self.sink_base}/{wheel.name}"
        self.index.registry[pkg] = {  # type: ignore[attr-defined]
            "filename": wheel.name,
            "url": sink_url,
            "sha256": hashlib.sha256(data).hexdigest(),
            "size": len(data),
            "core_metadata": True,
            "provenance": f"{self.sink_base}/{wheel.name}.provenance",
        }
        return wheel.name

    def register_good(self, pkg: str, wheel) -> str:
        """A benign listing whose .metadata lives on the trusted upstream host,
        with a correct core-metadata digest — the guard and the hash-gate must
        both let it through."""
        data = wheel.read_bytes()
        metadata = b"Metadata-Version: 2.1\nName: %b\nVersion: 1.0\n" % pkg.encode()
        meta_path = f"/simple/{pkg}/{wheel.name}.metadata"
        self.index.registry[pkg] = {  # type: ignore[attr-defined]
            "filename": wheel.name,
            # Resolves against /simple/<pkg>/, so a bare filename lands on the
            # trusted upstream host; the ".metadata" companion is derived from it.
            "url": wheel.name,
            "sha256": hashlib.sha256(data).hexdigest(),
            "size": len(data),
            "core_metadata": {"sha256": hashlib.sha256(metadata).hexdigest()},
            "metadata": metadata,
            "metadata_path": meta_path,
        }
        return wheel.name

    def register_hash_mismatch(self, pkg: str, wheel) -> str:
        """Benign host, but the listing's core-metadata digest does not match the
        bytes served — the hash-gate must refuse it."""
        name = self.register_good(pkg, wheel)
        self.index.registry[pkg]["core_metadata"] = {"sha256": "00" * 32}  # type: ignore[attr-defined]
        return name

    def stop(self) -> None:
        for srv in (self.sink, self.index):
            srv.shutdown()
            srv.server_close()


@pytest.fixture()
def ssrf_proxy(tmp_path_factory, pypiron_bin, tmp_path) -> Iterator[Tuple[Dict, _Servers]]:
    servers = _Servers()
    gen = _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=[
            "--proxy-upstream",
            servers.index_base,
            # Loopback upstream on plain http; see conftest.
            "--allow-insecure-upstream",
            "--exclude-newer",
            "",
        ],
    )
    proxy = next(gen)
    try:
        yield proxy, servers
    finally:
        gen.close()
        servers.stop()


def test_ip_literal_companion_urls_are_refused(ssrf_proxy, tmp_path):
    """The critical fix: an IP-literal companion URL is caught by the pre-flight
    guard the DNS resolver never sees. The proxy refuses; the sink is untouched;
    its body is never relayed to the client."""
    proxy, servers = ssrf_proxy
    wheel = make_wheel("evilpkg", "1.0", tmp_path)
    name = servers.register_evil("evilpkg", wheel)

    # The reflected companions must not be fetched or relayed.
    for suffix in (".metadata", ".provenance"):
        code, body, _ = http_get(f"{proxy['base_url']}/files/evilpkg/{name}{suffix}")
        assert code == 404, f"{suffix} pointed at the sink must not be relayed (got {code})"
        assert SINK_SECRET not in body

    # The artifact GET points at the sink too; it must fail rather than fetch it.
    code, body, _ = http_get(f"{proxy['base_url']}/files/evilpkg/{name}")
    assert code != 200
    assert SINK_SECRET not in body

    assert servers.hits == [], f"proxy connected to the internal sink: {servers.hits}"


def test_trusted_upstream_companion_still_served(ssrf_proxy, tmp_path):
    """The guard must not over-block: a companion on the trusted upstream host,
    with a matching core-metadata digest, is served normally."""
    proxy, servers = ssrf_proxy
    wheel = make_wheel("goodpkg", "1.0", tmp_path)
    name = servers.register_good("goodpkg", wheel)

    code, body, _ = http_get(f"{proxy['base_url']}/files/goodpkg/{name}.metadata")
    assert code == 200, f"trusted-host companion must be served (got {code})"
    assert b"Metadata-Version" in body


def test_metadata_hash_mismatch_is_refused(ssrf_proxy, tmp_path):
    """Defense-in-depth: when the listing carries a core-metadata sha256, the
    fetched bytes must match it. A mismatch fails closed even from the trusted
    host — a MITM can otherwise reflect arbitrary bytes as `.metadata`."""
    proxy, servers = ssrf_proxy
    wheel = make_wheel("mismatchpkg", "1.0", tmp_path)
    name = servers.register_hash_mismatch("mismatchpkg", wheel)

    code, body, _ = http_get(f"{proxy['base_url']}/files/mismatchpkg/{name}.metadata")
    assert code == 404, f"a mismatched core-metadata digest must be refused (got {code})"
    assert b"Metadata-Version" not in body
