"""Upload-path resource limits.

The legacy multipart handler streams the artifact to a disk spool, but the
non-file metadata parts are read into RAM. A per-field cap alone doesn't bound
the total — thousands of uniquely-named 64 KiB fields fit under the body limit
and sit resident at once. These tests pin the aggregate field-count/byte cap
(security audit M2).
"""

from __future__ import annotations

import pytest

from .helpers import http_get, make_wheel, upload_legacy, wait_for_file_in_index

pytestmark = pytest.mark.integration


def test_metadata_field_flood_is_rejected(disk_server, tmp_path):
    """Hundreds of uniquely-named metadata fields are refused with 400 instead
    of accumulating in the server's metadata map."""
    wheel = make_wheel("floodpkg", "1.0", tmp_path)
    junk = {f"junk{i}": "x" for i in range(400)}
    code, _ = upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["user"],
        password=disk_server["password"],
        fields=junk,
        expect_status=400,
    )
    assert code == 400


def test_filename_with_control_byte_is_rejected(disk_server, tmp_path):
    """A filename carrying a control byte is refused with 400. A U+0001 would
    otherwise smuggle the project page's base-URL sentinel into the cached page,
    where it is re-expanded to the request host on every serve (an amplification
    the page-cache size cap does not bound)."""
    wheel = make_wheel("sentinelpkg", "1.0", tmp_path)
    bad_name = "sentinelpkg-1.0-\x01pypiron-base-url\x01-py3-none-any.whl"
    code, _ = upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["user"],
        password=disk_server["password"],
        fields={"name": "sentinelpkg", "version": "1.0"},
        filename=bad_name,
        expect_status=400,
    )
    assert code == 400


def test_normal_upload_with_modest_metadata_succeeds(disk_server, tmp_path):
    """A realistic number of extra fields stays well under the cap and still
    publishes — the limit is headroom, not a functional constraint."""
    wheel = make_wheel("modestpkg", "1.0", tmp_path)
    fields = {f"extra_{i}": "Topic :: Utilities" for i in range(40)}
    upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["user"],
        password=disk_server["password"],
        fields=fields,
    )
    wait_for_file_in_index(disk_server["simple"], "modestpkg", wheel.name)


# The download path refuses any filename over 256 bytes. Upload has to agree, or
# a longer name is written, listed in /simple/, and then 404s on every GET.
MAX_ARTIFACT_FILENAME_BYTES = 256
_SUFFIX = "-1.0-py3-none-any.whl"


def test_a_very_long_filename_uploads_and_downloads(disk_server, tmp_path):
    """A long filename round-trips: upload, index, artifact GET, and the PEP 658
    companion — which is the name plus a suffix, so the servable cap has to be
    measured against the artifact name, not the whole URL segment.

    Long, not maximal: a disk backend bounds every path component at NAME_MAX
    (255 bytes on macOS and Linux alike) and writes each companion through a
    `.tmp-<nanos>-<pid>-<seq>-<name>` sibling, which spends ~34 of those bytes
    before the sidecar suffix. The 256-byte cap itself is pinned by the
    `valid_artifact_filename` unit test, which no filesystem constrains.
    """
    pkg = "longname"
    version = "1" + ".0" * 66  # 133 bytes of PEP 440
    wheel = make_wheel(pkg, version, tmp_path)
    assert 150 < len(wheel.name) < 200

    upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_file_in_index(disk_server["simple"], pkg, wheel.name)

    url = f"{disk_server['base_url']}/files/{pkg}/{wheel.name}"
    code, body, _ = http_get(url)
    assert code == 200
    assert body == wheel.read_bytes()

    code, meta, _ = http_get(f"{url}.metadata")
    assert code == 200
    assert b"Metadata-Version" in meta


def test_filename_past_the_servable_cap_is_rejected(disk_server, tmp_path):
    """One byte over what the download path will ever serve, and the upload is
    refused outright rather than stored as bytes no GET can hand back."""
    pkg = "capped"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    too_long = "y" * (MAX_ARTIFACT_FILENAME_BYTES + 1 - len(_SUFFIX)) + _SUFFIX
    assert len(too_long) == MAX_ARTIFACT_FILENAME_BYTES + 1

    code, body = upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["user"],
        password=disk_server["password"],
        fields={"name": pkg, "version": "1.0"},
        filename=too_long,
        expect_status=400,
    )
    assert code == 400
    # Named reason, so the test can't pass on some unrelated 400.
    assert b"Invalid filename" in body
