"""Large-artifact upload exercises the cloud multipart + copy_if_not_exists
publish path (>64 MiB) and its immutability guarantee.

Small-wheel round trips take the single conditional-PUT path; only an artifact
past the multipart threshold streams to a staging key and is then published with
copy_if_not_exists. This validates that path end to end on the cloud backends
that have a local emulator (S3/MinIO, Azure/Azurite).
"""

from __future__ import annotations

import hashlib
import io
import os
import tarfile

import pytest

from .helpers import http_get_bytes, upload_legacy, wait_for_file_in_index

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
