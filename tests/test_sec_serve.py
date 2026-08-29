"""Security regressions on the artifact read path.

Three holes, each observable from a client. The malware/quarantine byte gate
skipped the `.metadata`/`.provenance` companion routes, so the annotations of a
blocked wheel served while the wheel itself was refused. Credential-gated bytes
were stamped `public, immutable`, which lets a shared cache hand one reader's
artifact to the next anonymous client. And an arbitrarily long filename was
keyed straight into the download-counter and presign caches, which bound entry
count, not key length.
"""

from __future__ import annotations

import json
import re
import time
import zipfile
from contextlib import contextmanager
from pathlib import Path

import pytest

from .conftest import _start_disk_server
from .helpers import (
    _encode_basic_auth,
    http_get,
    http_get_no_redirect,
    make_wheel,
    upload_legacy,
)

pytestmark = pytest.mark.integration

MAL_ID = "MAL-2024-91002"
MAL_PKG = "totally-malware"
IMMUTABLE_PUBLIC = "public, max-age=31536000, immutable"
IMMUTABLE_PRIVATE = "private, max-age=31536000, immutable"
# Comfortably over the 256-byte cap, comfortably under hyper's request-head limit.
OVERLONG_FILENAME = "a" * 300 + "-1.0-py3-none-any.whl"


def _osv_zip(path: Path) -> Path:
    """One all-versions MAL record in the flat `<id>.json` shape of the OSV PyPI
    export — enough to arm the byte gate, hermetically."""
    record = {
        "id": MAL_ID,
        "modified": "2024-01-01T00:00:00Z",
        "summary": "malware masquerading as a helper library",
        "affected": [
            {
                "package": {"ecosystem": "PyPI", "name": MAL_PKG},
                "ranges": [{"type": "ECOSYSTEM", "events": [{"introduced": "0"}]}],
            }
        ],
    }
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr(f"{MAL_ID}.json", json.dumps(record))
    return path


@contextmanager
def _advisory_server(tmp_path_factory, pypiron_bin, feed):
    gen = _start_disk_server(
        tmp_path_factory, pypiron_bin, extra_args=["--advisory-feed", str(feed)]
    )
    server = next(gen)
    try:
        yield server
    finally:
        gen.close()  # runs _start_disk_server's finally → kill_process_tree


def _wait_snapshot(base_url: str, *, timeout: float = 20.0) -> bool:
    """The feed loads on the worker, not at bind time: poll the staleness gauge
    until the snapshot the gate reads is actually live."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        code, body, _ = http_get(f"{base_url}/metrics")
        if code == 200 and re.search(
            r"^pypiron_advisory_snapshot_age_seconds ", body.decode(), re.MULTILINE
        ):
            return True
        time.sleep(0.1)
    return False


def _poll_ok(url: str, *, headers=None, timeout: float = 20.0):
    """GET until 200. The wheel lands with its upload, but the metadata companion
    is extracted alongside it — poll rather than race the write."""
    deadline = time.time() + timeout
    while True:
        code, body, resp_headers = http_get(url, headers=headers)
        if code == 200 or time.time() > deadline:
            assert code == 200, f"{url} never returned 200 (last {code})"
            return body, resp_headers
        time.sleep(0.1)


def test_byte_gate_covers_companion_routes(tmp_path_factory, pypiron_bin, tmp_path):
    """A blocked wheel's `.metadata` and `.provenance` are refused with it. They
    describe the malicious file and every resolver fetches them before the wheel,
    so serving them was a hole in the one enforcement chokepoint — a `Range`
    request on the same URL fell past the companion branches and was always
    gated, which is how the hole showed."""
    feed = _osv_zip(tmp_path / "osv.zip")
    with _advisory_server(tmp_path_factory, pypiron_bin, feed) as server:
        base = server["base_url"]
        assert _wait_snapshot(base), "advisory snapshot never loaded"

        # Mirror origin (admin cred + mirror field), the way a sync publishes: a
        # private-origin name is exempt from the gate by design. The upload is
        # synchronous, and a blocked file is scrubbed from the listing, so there
        # is no index to wait for.
        wheel = make_wheel(MAL_PKG, "1.0.0", tmp_path)
        upload_legacy(
            server["legacy"],
            wheel,
            username=server["admin_user"],
            password=server["admin_password"],
            fields={"mirror": "true"},
        )

        for suffix in ("", ".metadata", ".provenance"):
            code, body, _ = http_get(f"{base}/files/{MAL_PKG}/{wheel.name}{suffix}")
            assert code == 403, (suffix, code, body)
            doc = json.loads(body)
            assert doc["error"] == "blocked by malware advisory", (suffix, doc)
            assert doc["advisories"] == [MAL_ID], (suffix, doc)

        code, body, _ = http_get(
            f"{base}/files/{MAL_PKG}/{wheel.name}.metadata",
            headers={"Range": "bytes=0-0"},
        )
        assert code == 403, (code, body)


def test_read_gated_bytes_are_never_publicly_cacheable(disk_server_read_auth, tmp_path):
    """With `--read-user`/`--read-pass` armed, artifact and companion responses
    carry credentials: a shared cache must not store them for anonymous clients
    (`private`) or collapse the two audiences (`Vary: Authorization`)."""
    server = disk_server_read_auth
    wheel = make_wheel("cachedpkg", "1.0", tmp_path)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["admin_user"],
        password=server["admin_password"],
    )
    auth = {"Authorization": _encode_basic_auth(server["read_user"], server["read_password"])}

    for suffix in ("", ".metadata"):
        url = f"{server['base_url']}/files/cachedpkg/{wheel.name}{suffix}"
        _, headers = _poll_ok(url, headers=auth)
        assert headers["cache-control"] == IMMUTABLE_PRIVATE, suffix
        assert "authorization" in headers.get("vary", "").lower(), (suffix, headers.get("vary"))

    # The companion's own negotiation vary survives the merge.
    _, headers = _poll_ok(
        f"{server['base_url']}/files/cachedpkg/{wheel.name}.metadata", headers=auth
    )
    assert "accept-encoding" in headers["vary"].lower(), headers["vary"]


def test_public_reads_keep_public_immutable_caching(disk_server, tmp_path):
    """The default — no read credential — is byte-identical to before the fix:
    an open mirror stays cacheable by every proxy in front of it."""
    server = disk_server
    wheel = make_wheel("publicpkg", "1.0", tmp_path)
    upload_legacy(server["legacy"], wheel, username=server["user"], password=server["password"])
    for suffix in ("", ".metadata"):
        url = f"{server['base_url']}/files/publicpkg/{wheel.name}{suffix}"
        _, headers = _poll_ok(url)
        assert headers["cache-control"] == IMMUTABLE_PUBLIC, suffix
        assert "authorization" not in headers.get("vary", "").lower(), suffix


def test_overlong_filename_is_not_an_artifact(disk_server):
    """No wheel or sdist name comes near 256 bytes; a longer one is a probe, and
    it is refused exactly like any other name that isn't an artifact."""
    base = disk_server["base_url"]
    for suffix in ("", ".metadata", ".provenance"):
        code, _, _ = http_get(f"{base}/files/somepkg/{OVERLONG_FILENAME}{suffix}")
        assert code == 404, (suffix, code)


@pytest.mark.s3
def test_overlong_filename_never_reaches_the_presign_cache(s3_server_presigned):
    """Redirect delivery presigns with no existence check — local HMAC math — so a
    missing artifact still earns a 302 and a presign-cache entry. That is the path
    an unbounded filename rode into a cache bounded by entry count, not key
    length; the cap turns it back before either key is built."""
    base = s3_server_presigned["base_url"]

    code, _, _ = http_get_no_redirect(f"{base}/files/somepkg/realish-1.0-py3-none-any.whl")
    assert code == 302, f"a normal missing artifact should still presign, got {code}"

    code, _, _ = http_get_no_redirect(f"{base}/files/somepkg/{OVERLONG_FILENAME}")
    assert code == 404, f"an overlong filename must never be keyed into a cache, got {code}"
