"""A wheel's central directory is bounded before we walk it.

`ZipArchive::new` materializes every central-directory record — name string
included — before any per-entry cap applies, so a wheel of minimum-size entries
costs gigabytes of resident memory for a body well under the upload limit. The
entry-count ceiling in `src/wheel.rs` refuses that wheel's METADATA extraction;
this is the blackbox proof over real HTTP.
"""

from __future__ import annotations

import zipfile
from concurrent.futures import ThreadPoolExecutor

import pytest

from .helpers import (
    http_get,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration

# Mirrors `MAX_WHEEL_ENTRIES` in src/wheel.rs. Kept as a literal on purpose: if
# the cap moves, this test must be re-derived, not silently follow it.
MAX_WHEEL_ENTRIES = 262_144


def _pad_entries(wheel_path, count):
    """Append `count` empty members to an existing wheel, in place."""
    with zipfile.ZipFile(wheel_path, "a", compression=zipfile.ZIP_STORED) as zf:
        for i in range(count):
            zf.writestr(f"pad/f{i}", b"")
    return wheel_path


def _publish(server, wheel_path, package):
    upload_legacy(
        server["legacy"],
        wheel_path,
        username=server["user"],
        password=server["password"],
    )
    return wait_for_file_in_index(server["simple"], package, wheel_path.name)


def test_over_cap_wheel_yields_no_pep658_metadata(disk_server, tmp_path):
    """A wheel past the entry ceiling is served, but its METADATA is not read.

    The wheel itself is a legitimate artifact — clients still get the bytes — so
    the only casualty is the PEP 658 fast path, which is exactly the degradation
    we want in exchange for never walking a hostile central directory.
    """
    wheel = make_wheel("capbomb", "1.0", tmp_path)
    # One past the ceiling: the wheel's own 4 members put us comfortably over.
    _pad_entries(wheel, MAX_WHEEL_ENTRIES + 1)
    with zipfile.ZipFile(wheel) as zf:
        assert len(zf.infolist()) > MAX_WHEEL_ENTRIES

    index = _publish(disk_server, wheel, "capbomb")

    # PEP 691 omits the key entirely when there is no companion metadata.
    (entry,) = index["files"]
    assert "core-metadata" not in entry
    code, _, _ = http_get(f"{disk_server['base_url']}/files/capbomb/{wheel.name}.metadata")
    assert code == 404


def test_wheel_at_the_cap_still_yields_metadata(disk_server, tmp_path):
    """The control: the same wheel one member *under* the ceiling is read fine.

    Pairing the two pins the cap to the boundary rather than to "big wheels are
    broken" — a regression that stopped extracting metadata for every large
    wheel would fail here while the test above kept passing.
    """
    wheel = make_wheel("capfine", "1.0", tmp_path)
    with zipfile.ZipFile(wheel) as zf:
        existing = len(zf.infolist())
    _pad_entries(wheel, MAX_WHEEL_ENTRIES - existing)
    with zipfile.ZipFile(wheel) as zf:
        assert len(zf.infolist()) == MAX_WHEEL_ENTRIES

    index = _publish(disk_server, wheel, "capfine")

    (entry,) = index["files"]
    assert entry["core-metadata"] is True
    code, body, _ = http_get(f"{disk_server['base_url']}/files/capfine/{wheel.name}.metadata")
    assert code == 200
    assert b"Name: capfine" in body


def test_ordinary_wheel_is_untouched(disk_server, tmp_path):
    """A normal wheel — the overwhelmingly common case — is unaffected."""
    wheel = make_wheel("capplain", "1.0", tmp_path)
    index = _publish(disk_server, wheel, "capplain")

    (entry,) = index["files"]
    assert entry["core-metadata"] is True
    code, body, _ = http_get(f"{disk_server['base_url']}/files/capplain/{wheel.name}.metadata")
    assert code == 200
    assert b"Name: capplain" in body


def test_concurrent_uploads_all_get_metadata(disk_server, tmp_path):
    """More simultaneous wheels than there are parse permits, all served.

    The permit bounding concurrent central-directory parses is held across an
    await; if it leaked, the fifth upload would hang forever and every later one
    behind it. Sequential tests can never see that, so publish twice the permit
    count at once and require every one to come back with its METADATA.
    """
    names = [f"capconc{i}" for i in range(8)]
    wheels = [make_wheel(n, "1.0", tmp_path) for n in names]

    with ThreadPoolExecutor(max_workers=len(wheels)) as pool:
        list(pool.map(lambda pair: _publish(disk_server, *pair), zip(wheels, names)))

    for name, wheel in zip(names, wheels):
        code, body, _ = http_get(f"{disk_server['base_url']}/files/{name}/{wheel.name}.metadata")
        assert code == 200, f"{name} lost its metadata under concurrency"
        assert f"Name: {name}".encode() in body
