"""`verify-index` and the bytes it is willing to read.

The default pass is O(objects), not O(bytes): it never opens an artifact, so
the only claim it can make about a body is the one the listing already paid
for — the object's length against the `size` its own sidecar publishes. That
is free and always on.

Everything else about the body costs a full read of the corpus and lives
behind `--deep`, which re-hashes each artifact and compares it to the sha256
its sidecar publishes — the hash clients check their downloads against. It is
the only check anywhere in pypiron that can catch a body swapped for one of
the *same length*, which is precisely the shape two builds of one wheel
filename take.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from .helpers import make_wheel, upload_legacy, wait_for_file_in_index

pytestmark = pytest.mark.integration

PACKAGE = "verifydeep"


def _verify(bin_path: Path, data_dir: Path, *flags: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(bin_path), "verify-index", "--data-dir", str(data_dir), *flags],
        capture_output=True,
        text=True,
        timeout=120,
    )


def _upload_one(server, tmp_path: Path, version: str) -> Path:
    """Publish a wheel and return the artifact's path in the store."""
    wheel = make_wheel(PACKAGE, version, tmp_path)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
    )
    wait_for_file_in_index(server["simple"], PACKAGE, wheel.name)
    stored = Path(server["data_dir"]) / "packages" / PACKAGE / wheel.name
    assert stored.exists(), f"{stored} was not written"
    return stored


def test_a_same_length_body_swap_is_only_caught_by_deep(
    disk_server, pypiron_bin: Path, tmp_path: Path
) -> None:
    """The blind spot, stated as a test: swap the bytes without changing the
    length and the default pass reports a healthy store, because nothing in it
    re-derives a sha256. `--deep` is what closes it."""
    stored = _upload_one(disk_server, tmp_path, "1.0")
    original = stored.read_bytes()
    swapped = bytes(b ^ 0xFF for b in original)
    assert len(swapped) == len(original) and swapped != original
    stored.write_bytes(swapped)

    shallow = _verify(pypiron_bin, disk_server["data_dir"])
    assert shallow.returncode == 0, (
        "the default pass reads no bodies, so a same-length swap must not "
        f"change its verdict:\n{shallow.stdout}{shallow.stderr}"
    )

    deep = _verify(pypiron_bin, disk_server["data_dir"], "--deep")
    assert deep.returncode == 1, f"--deep missed it:\n{deep.stdout}{deep.stderr}"
    assert "body-mismatch" in deep.stdout, deep.stdout
    assert stored.name in deep.stdout, deep.stdout


def test_a_length_changing_swap_is_caught_for_free(
    disk_server, pypiron_bin: Path, tmp_path: Path
) -> None:
    """The listing already returned every object's size and the sidecar already
    publishes what it should be, so this one costs no I/O at all — and it is
    the half of the check that stays affordable on a full mirror."""
    stored = _upload_one(disk_server, tmp_path, "2.0")
    stored.write_bytes(stored.read_bytes()[:-1])

    shallow = _verify(pypiron_bin, disk_server["data_dir"])
    assert shallow.returncode == 1, (
        f"a truncated body must diverge without --deep:\n{shallow.stdout}{shallow.stderr}"
    )
    assert "size-mismatch" in shallow.stdout, shallow.stdout
    assert stored.name in shallow.stdout, shallow.stdout


def test_an_untouched_store_verifies_deep_clean(
    disk_server, pypiron_bin: Path, tmp_path: Path
) -> None:
    """--deep must not invent divergence: the same store that passes the default
    pass passes the re-hash."""
    _upload_one(disk_server, tmp_path, "3.0")
    deep = _verify(pypiron_bin, disk_server["data_dir"], "--deep")
    assert deep.returncode == 0, f"{deep.stdout}{deep.stderr}"
