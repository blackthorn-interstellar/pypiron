"""Server-side replication copy of artifacts whose names the store escapes.

object_store percent-escapes ``~ % # [ ] { } ^ | < > " * ? \\`` and every
non-ASCII byte when it writes a key, so the wheel ``a~b-1.0-py3-none-any.whl``
is really stored under ``.../a%7Eb-1.0-py3-none-any.whl``. Uploads accept every
one of those bytes in a filename (the *package* name is always normalized, so
this is the only place they can appear).

The hand-rolled copy verbs build and sign their own key. While that key was the
raw one, a server-side CopyObject of such a file signed an object that is not
there: the copy missed, and the peer bucket was left without the artifact its
index promised. These tests drive the REAL CopyObject — two MinIO buckets under
one credential are copy-eligible — and check the wire name end to end: what the
source bucket actually holds, that the peer holds the same key with the same
bytes, and that a presigned direct download of it resolves.

The GCS rewrite and Azure Copy Blob request lines are covered by the Rust unit
test ``storage::tests::copy_request_lines_address_the_object_that_object_store_wrote``:
neither has a local two-buckets-one-account fixture, so there is no blackbox
copy to drive (the same split ``test_replication_copy.py`` already makes).
"""

from __future__ import annotations

import hashlib
import time
from urllib.parse import quote

import pytest

from .conftest import minio_list_keys_in, minio_object_sha256
from .helpers import (
    http_get,
    http_get_bytes,
    http_get_no_redirect,
    make_sdist,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = [pytest.mark.integration, pytest.mark.s3]

#: (package, kind, published filename). One package per case so a failure names
#: the character class that broke. Every filename carries a byte object_store
#: escapes: `~` (unreserved in RFC 3986, so a raw-key signer leaves it alone and
#: silently addresses the wrong object), `#` (also a URL fragment delimiter), and
#: a non-ASCII run (escaped as multiple bytes).
CASES = [
    ("escapedtilde", "wheel", "a~b-1.0-py3-none-any.whl"),
    ("escapedhash", "sdist", "weird#name-1.0.tar.gz"),
    ("escapedutf8", "wheel", "café-1.0-py3-none-any.whl"),
]

#: Companion keys pypiron writes beside an artifact (`sidecar::is_artifact`).
SIDECAR_SUFFIXES = (
    ".meta.json",
    ".metadata",
    ".provenance",
    ".tombstone",
    ".frozen",
)


def _server_side_copies(base_url: str) -> int:
    """Read `pypiron_replication_server_side_copies_total` from /metrics."""
    code, body, _ = http_get(f"{base_url}/metrics", timeout=3)
    assert code == 200, f"metrics returned {code}"
    for line in body.decode().splitlines():
        if line.startswith("pypiron_replication_server_side_copies_total "):
            return int(line.rsplit(" ", 1)[1])
    return 0


def _eventually(check, *, timeout: float = 30.0, what: str = "condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = check()
        if last:
            return last
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def _publish(server, tmp_path, pkg: str, kind: str, filename: str) -> str:
    """Publish one artifact under `filename` and return its sha256.

    The bytes are an ordinary wheel/sdist built for `pkg`; only the *published*
    name carries the escape, which is all the storage key depends on.
    """
    src = make_wheel(pkg, "1.0", tmp_path) if kind == "wheel" else make_sdist(pkg, "1.0", tmp_path)
    upload_legacy(
        server["legacy"],
        src,
        username=server["user"],
        password=server["password"],
        fields={"name": pkg, "version": "1.0"},
        filename=filename,
        timeout=20,
    )
    wait_for_file_in_index(server["simple"], pkg, filename)
    return hashlib.sha256(src.read_bytes()).hexdigest()


def _stored_key(minio, bucket: str, pkg: str) -> str:
    """The single artifact key under `packages/<pkg>/`, as the bucket reports it.

    Read back from the store rather than computed here: the encoding under test
    is object_store's, and a second copy of it in the test would agree with a
    buggy server for exactly the reason the server is buggy.
    """
    prefix = f"packages/{pkg}/"
    keys = [
        k
        for k in minio_list_keys_in(minio, bucket)
        if k.startswith(prefix)
        and not k[len(prefix) :].startswith(".")
        and not k.endswith(SIDECAR_SUFFIXES)
    ]
    assert len(keys) == 1, f"expected one artifact under {prefix}, got {keys}"
    return keys[0]


def test_escaped_artifact_names_replicate_via_server_side_copy(
    minio_two, s3_server_multi, tmp_path
):
    """Names the store escapes survive a real CopyObject to the peer bucket."""
    server = s3_server_multi
    write_bucket, peer = minio_two["buckets"]
    before = _server_side_copies(server["base_url"])

    for pkg, kind, filename in CASES:
        sha = _publish(server, tmp_path, pkg, kind, filename)

        # What the store actually wrote is not the name we published.
        key = _stored_key(minio_two, write_bucket, pkg)
        assert "%" in key, f"{filename} was expected to store escaped, got {key}"
        assert key != f"packages/{pkg}/{filename}"

        # The fan-out placed that same key on the peer — this is the assertion
        # the raw-key signer failed: the copy addressed a key that is not there,
        # so the peer bucket stayed empty.
        _eventually(
            lambda k=key: k in minio_list_keys_in(minio_two, peer),
            what=f"{filename} replicated to the peer bucket",
        )
        # Byte-identical, so the copy moved the right object's bytes and not
        # some neighbour whose escape happens to collide.
        assert minio_object_sha256(minio_two, peer, key) == sha, filename

        # And it moved provider-side. Convergence alone does not prove the copy
        # worked: a copy that misses falls back to streaming and the peer ends
        # correct anyway, which is exactly how the raw-key bug hid. The counter
        # is what separates the two.
        before += 1
        _eventually(
            lambda n=before: _server_side_copies(server["base_url"]) >= n,
            what=f"a server-side copy for {filename}",
        )


def test_escaped_artifact_names_download_and_presign(s3_server_multi, tmp_path):
    """The same names serve from the node and through a presigned redirect.

    A presigned URL is signed over the store's own wire name too, so a name that
    only round-trips through the node's streaming path would still 403/404 here.
    """
    server = s3_server_multi

    for pkg, kind, filename in CASES:
        sha = _publish(server, tmp_path, pkg, kind, filename)
        # `#` would truncate the request line as a fragment; encode the segment.
        url = f"{server['base_url']}/files/{pkg}/{quote(filename, safe='')}"

        # Streamed from the node (the default for URL-keyed clients).
        code, body, _ = http_get_no_redirect(url, headers={"User-Agent": "curl/8.0"})
        assert code == 200, f"{filename}: streamed GET returned {code}"
        assert hashlib.sha256(body).hexdigest() == sha, filename

        # Redirected to a presigned URL for uv, and that URL resolves.
        code, _, headers = http_get_no_redirect(url, headers={"User-Agent": "uv/0.7.0"})
        assert code == 302, f"{filename}: expected a presigned redirect, got {code}"
        location = headers["location"]
        assert "X-Amz-Signature" in location, f"{filename}: {location} is not presigned"
        assert hashlib.sha256(http_get_bytes(location)).hexdigest() == sha, filename


def test_escaped_artifact_names_round_trip_on_real_gcs(gcs_server, tmp_path):
    """The same wire-name round trip against a real GCS bucket.

    GCS has no faithful emulator and the fixture owns one bucket, so there is no
    server-side rewrite to drive here — this pins the half that is provider
    behavior: that object_store's GCS client writes the escaped name, and that
    the index and download path find it again.
    """
    server = gcs_server

    for pkg, kind, filename in CASES:
        sha = _publish(server, tmp_path, pkg, kind, filename)
        url = f"{server['base_url']}/files/{pkg}/{quote(filename, safe='')}"
        code, body, _ = http_get_no_redirect(url, headers={"User-Agent": "curl/8.0"})
        assert code == 200, f"{filename}: GET returned {code}"
        assert hashlib.sha256(body).hexdigest() == sha, filename
