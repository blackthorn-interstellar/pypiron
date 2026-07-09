"""Storage prefix: `--storage-prefix` roots every key under one subtree, so
pypiron can share a bucket with unrelated data.

Runs against MinIO, where the bucket contents are observable independently of
pypiron (via `mc`), so these assert on the real object layout rather than on
what the server reports about itself.
"""

from __future__ import annotations

import pytest

from .conftest import STORAGE_PREFIX, minio_get_key, minio_list_keys, minio_put_key
from .helpers import (
    download_pypi_wheel,
    http_get_bytes,
    http_get_no_redirect,
    run_checked,
    sha256_file,
    wait_for_file_in_index,
    wait_for_project_in_global,
)

PACKAGE = "six"
VERSION = "1.17.0"

#: An object owned by something else entirely, living at the bucket root.
FOREIGN_KEY = "other-app/state.json"
FOREIGN_BODY = "not pypiron's"

pytestmark = [pytest.mark.integration, pytest.mark.s3]


@pytest.mark.compat("uv", "upload")
def test_prefixed_server_confines_keys_and_leaves_the_bucket_alone(
    minio, s3_server_prefixed, tmp_path, uv_path
):
    server = s3_server_prefixed
    minio_put_key(minio, FOREIGN_KEY, FOREIGN_BODY)

    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    orig_sha = sha256_file(wheel_path)
    run_checked(
        [
            uv_path,
            "publish",
            "--publish-url",
            server["legacy"],
            "--username",
            server["user"],
            "--password",
            server["password"],
            str(wheel_path),
        ],
        timeout=120,
    )

    # The server behaves exactly as it does unprefixed: indexed, bytes intact.
    wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name)
    wait_for_project_in_global(server["simple"], PACKAGE)
    downloaded = tmp_path / "downloaded.whl"
    downloaded.write_bytes(
        http_get_bytes(f"{server['base_url']}/files/{PACKAGE}/{wheel_path.name}")
    )
    assert sha256_file(downloaded) == orig_sha

    keys = minio_list_keys(minio)
    ours = [k for k in keys if k != FOREIGN_KEY]

    # Everything pypiron wrote lives under the prefix — including the artifact
    # and the indexes it regenerates.
    assert ours, f"pypiron wrote nothing: {keys}"
    stray = [k for k in ours if not k.startswith(f"{STORAGE_PREFIX}/")]
    assert not stray, f"keys escaped the prefix: {stray}"
    assert f"{STORAGE_PREFIX}/packages/{PACKAGE}/{wheel_path.name}" in ours

    # The foreign object is still there, byte for byte. A prefix that leaked
    # into list_all() would let the audit sweep see — and reap — this key.
    assert FOREIGN_KEY in keys
    assert minio_get_key(minio, FOREIGN_KEY) == FOREIGN_BODY


@pytest.mark.compat("uv", "upload")
def test_presigned_redirect_is_signed_for_the_prefixed_key(
    minio, s3_server_prefixed_presigned, tmp_path, uv_path
):
    """A presigned URL is signed over the full store key. Sign the unprefixed
    key and the signature still verifies — against an object that isn't there.
    """
    server = s3_server_prefixed_presigned
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    orig_sha = sha256_file(wheel_path)
    run_checked(
        [
            uv_path,
            "publish",
            "--publish-url",
            server["legacy"],
            "--username",
            server["user"],
            "--password",
            server["password"],
            str(wheel_path),
        ],
        timeout=120,
    )
    wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name)

    code, _, headers = http_get_no_redirect(
        f"{server['base_url']}/files/{PACKAGE}/{wheel_path.name}"
    )
    assert code == 302
    location = headers["location"]
    assert location.startswith(minio["endpoint"]), "redirect must point at S3"
    assert f"/{STORAGE_PREFIX}/packages/{PACKAGE}/" in location, location

    # Follow it: the signature must verify *and* resolve to the real object.
    downloaded = tmp_path / "presigned.whl"
    downloaded.write_bytes(http_get_bytes(location))
    assert sha256_file(downloaded) == orig_sha
