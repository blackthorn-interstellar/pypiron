"""Upload-path resource limits.

The legacy multipart handler streams the artifact to a disk spool, but the
non-file metadata parts are read into RAM. A per-field cap alone doesn't bound
the total — thousands of uniquely-named 64 KiB fields fit under the body limit
and sit resident at once. These tests pin the aggregate field-count/byte cap
(security audit M2).
"""

from __future__ import annotations

import pytest

from .helpers import make_wheel, upload_legacy, wait_for_file_in_index

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
