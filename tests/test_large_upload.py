"""Large-artifact upload exercises the cloud multipart + copy_if_not_exists
publish path (>64 MiB) and its immutability guarantee.

Small-wheel round trips take the single conditional-PUT path; only an artifact
past the multipart threshold streams to a staging key and is then published with
copy_if_not_exists. This validates that path end to end on the cloud backends
that have a local emulator (S3/MinIO, Azure/Azurite).
"""

from __future__ import annotations

import hashlib
import http.client
import io
import os
import re
import tarfile
import time
from urllib.parse import urlparse

import pytest

from .conftest import (
    _minio_buckets,
    _minio_fault_proxy,
    _s3_list,
    _s3_signed,
    _start_s3_server,
)
from .helpers import http_get_bytes, legacy_upload_request, upload_legacy, wait_for_file_in_index

pytestmark = pytest.mark.integration

# Comfortably past the 64 MiB multipart threshold. The payload is random so the
# gzipped tarball does not compress back under the threshold.
LARGE_PAYLOAD = 72 * 1024 * 1024
THRESHOLD = 64 * 1024 * 1024


@pytest.fixture(
    params=[
        pytest.param("s3_server", id="s3", marks=pytest.mark.s3),
        pytest.param("azure_server", id="azure", marks=pytest.mark.azure),
    ]
)
def server(request):
    return request.getfixturevalue(request.param)


def _make_large_sdist(name, version, dest_dir, payload):
    base = f"{name}-{version}"
    path = dest_dir / f"{base}.tar.gz"
    pkg_info = f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n".encode()
    with tarfile.open(path, "w:gz") as tf:
        for fname, data in ((f"{base}/PKG-INFO", pkg_info), (f"{base}/big.bin", payload)):
            info = tarfile.TarInfo(fname)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return path


def test_large_upload_multipart_publish_and_immutability(server, tmp_path):
    sdist = _make_large_sdist("bigpkg", "1.0", tmp_path, os.urandom(LARGE_PAYLOAD))
    assert sdist.stat().st_size > THRESHOLD, "tarball must exceed the multipart threshold"
    orig_sha = hashlib.sha256(sdist.read_bytes()).hexdigest()

    upload_legacy(
        server["legacy"], sdist, username=server["user"], password=server["password"], timeout=120
    )
    wait_for_file_in_index(server["simple"], "bigpkg", sdist.name)

    # Bytes survive the multipart round trip intact.
    got = http_get_bytes(f"{server['base_url']}/files/bigpkg/{sdist.name}", timeout=120)
    assert hashlib.sha256(got).hexdigest() == orig_sha

    # Re-uploading the same filename is rejected — immutability holds on the
    # large path (copy_if_not_exists -> AlreadyExists -> 409), not just the
    # single-PUT path.
    code, _ = upload_legacy(
        server["legacy"],
        sdist,
        username=server["user"],
        password=server["password"],
        timeout=120,
        expect_status=409,
    )
    assert code == 409


@pytest.fixture()
def s3_server_faulty(tmp_path_factory, pypiron_bin, request):
    """An S3 server whose bucket is reached through the fault-injecting proxy."""
    buckets = _minio_buckets(request.getfixturevalue("_minio_container"), 1, "mpu")
    minio = next(buckets)
    try:
        proxied = _minio_fault_proxy(minio)
        try:
            yield from _start_s3_server(tmp_path_factory, pypiron_bin, next(proxied))
        finally:
            proxied.close()
    finally:
        buckets.close()


def _upload_then_hang_up(server, sdist, held) -> None:
    """Upload `sdist`, then hang up once `held()` says the server reached the
    write the fault proxy is holding. The hang-up point is the held write, never
    a client timeout racing a slow (debug, CI) server through the body."""
    url = urlparse(server["legacy"])
    body, headers = legacy_upload_request(
        sdist, username=server["user"], password=server["password"]
    )
    conn = http.client.HTTPConnection(url.hostname, url.port, timeout=120)
    try:
        conn.request("POST", url.path, body=body, headers=headers)
        deadline = time.monotonic() + 60
        while not held():
            assert time.monotonic() < deadline, "the upload never reached the held write"
            time.sleep(0.1)
    finally:
        conn.close()


def _open_multipart_uploads(minio, bucket):
    code, body = _s3_signed(minio, "GET", f"/{bucket}", query={"uploads": ""})
    assert code == 200, body[:200]
    return re.findall(r"<UploadId>(.*?)</UploadId>", body.decode())


@pytest.mark.s3
def test_dropped_large_upload_aborts_its_multipart(s3_server_faulty, tmp_path):
    """An upload whose request goes away mid-write (the client hung up, the
    write timed out) must not leave its multipart upload open: the provider
    bills its parts and nothing lists them."""
    server = s3_server_faulty
    faults, minio = server["faults"], server["minio"]
    bucket = minio["bucket"]
    sdist = _make_large_sdist("abandoned", "1.0", tmp_path, os.urandom(LARGE_PAYLOAD))

    # The upload starts, but its part writes hang; the client then gives up.
    faults.hold_puts(bucket, "_staging/")
    _upload_then_hang_up(
        server, sdist, lambda: faults.count(bucket=bucket, method="PUT", needle="_staging/") >= 1
    )
    assert faults.count(bucket=bucket, method="POST", needle="uploads") >= 1

    deadline = time.monotonic() + 10
    while _open_multipart_uploads(minio, bucket) and time.monotonic() < deadline:
        time.sleep(0.2)
    assert _open_multipart_uploads(minio, bucket) == [], "the dropped upload must be aborted"

    # The same file uploads cleanly once the bucket behaves.
    faults.recover(bucket)
    upload_legacy(
        server["legacy"], sdist, username=server["user"], password=server["password"], timeout=120
    )
    wait_for_file_in_index(server["simple"], "abandoned", sdist.name)
    assert _open_multipart_uploads(minio, bucket) == []


@pytest.mark.s3
def test_dropped_large_upload_cleans_up_its_staging_copy(s3_server_faulty, tmp_path):
    """Hung up on while the finished staging copy is being published to its
    final name: the staging object must not be left behind."""
    server = s3_server_faulty
    faults, minio = server["faults"], server["minio"]
    bucket = minio["bucket"]
    sdist = _make_large_sdist("halfway", "1.0", tmp_path, os.urandom(LARGE_PAYLOAD))

    # The staging upload completes; the copy to the final key hangs.
    final_key = f"packages/halfway/{sdist.name}"
    faults.hold_puts(bucket, final_key)
    _upload_then_hang_up(
        server,
        sdist,
        lambda: faults.count(bucket=bucket, method="PUT", needle=f"{final_key}?") >= 1,
    )

    deadline = time.monotonic() + 10
    while _s3_list(minio, bucket, "_staging/") and time.monotonic() < deadline:
        time.sleep(0.2)
    assert _s3_list(minio, bucket, "_staging/") == [], "the staging copy must be deleted"
