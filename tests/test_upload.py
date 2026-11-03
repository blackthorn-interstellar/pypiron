"""Upload spools a crash left behind are removed when the server starts.

A spool is the local temp file an upload streams into before it is stored.
Every normal exit, error and cancel removes its own; a crash (SIGKILL, OOM)
leaves it. The server removes pypiron's own spool files that are more than a
day old, and nothing else in the directory.
"""

from __future__ import annotations

import os
import time

import pytest

from .conftest import _start_disk_server

pytestmark = pytest.mark.integration

TWO_DAYS = 2 * 24 * 3600


def _plant(path, *, age: float) -> None:
    path.write_bytes(b"partial upload")
    then = time.time() - age
    os.utime(path, (then, then))


def test_crash_leftover_spools_are_removed_at_startup(tmp_path_factory, pypiron_bin):
    spool = tmp_path_factory.mktemp("spool")
    leftover = spool / "pypiron-upload-1-1-0.spool"
    fresh = spool / "pypiron-upload-2-1-0.spool"
    unrelated = spool / "unrelated-1.spool"
    _plant(leftover, age=TWO_DAYS)
    _plant(fresh, age=0)
    _plant(unrelated, age=TWO_DAYS)

    server = _start_disk_server(tmp_path_factory, pypiron_bin, ["--spool-dir", str(spool)])
    next(server)
    try:
        deadline = time.monotonic() + 10
        while leftover.exists() and time.monotonic() < deadline:
            time.sleep(0.1)
        assert not leftover.exists(), "a crash's stale spool must be removed"
        assert fresh.exists(), "a recent spool may still be in use"
        assert unrelated.exists(), "files that are not pypiron spools are not ours"
    finally:
        server.close()
