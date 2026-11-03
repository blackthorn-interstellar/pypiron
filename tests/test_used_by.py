"""The project page's "Used by": hosted packages whose newest release requires
the project, narrowed to those whose requirement admits the version shown,
kept current by the worker and rebuilt from scratch by the audit."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from .helpers import http_get, make_wheel, upload_legacy, wait_for_file_in_index

pytestmark = pytest.mark.integration


def _upload(server, dists: Path, name: str, version: str, requires: list[str]) -> None:
    extra = "".join(f"Requires-Dist: {r}\n" for r in requires)
    wheel = make_wheel(name, version, dists, metadata_extra=extra)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["admin_user"],
        password=server["admin_password"],
    )
    wait_for_file_in_index(server["simple"], name, wheel.name)


def _used_by(server, path: str) -> set[str]:
    status, body, _ = http_get(f"{server['base_url']}/project/{path}")
    assert status == 200, (path, status)
    html = body.decode()
    if "<h2>Used by</h2>" not in html:
        return set()
    section = html.split("<h2>Used by</h2>", 1)[1].split("</section>", 1)[0]
    return {part.split('/"', 1)[0] for part in section.split('href="/project/')[1:]}


def _wait_used_by(server, path: str, expected: set[str], timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while (got := _used_by(server, path)) != expected:
        assert time.monotonic() < deadline, f"/project/{path}: used by {got}, want {expected}"
        time.sleep(0.2)


def _seed(server, dists: Path) -> None:
    _upload(server, dists, "lib", "1.0", [])
    _upload(server, dists, "lib", "2.0", [])
    _upload(server, dists, "app", "1.0", ["lib>=2"])
    _upload(server, dists, "old", "1.0", ["lib<2"])
    _upload(server, dists, "any", "1.0", ['lib; extra == "x"'])


def test_used_by_follows_versions_and_new_releases(disk_server, tmp_path):
    _seed(disk_server, tmp_path)
    _wait_used_by(disk_server, "lib/", {"any", "app"})
    _wait_used_by(disk_server, "lib/1.0/", {"any", "old"})
    assert _used_by(disk_server, "app/") == set()

    # Only the newest release counts: app 2.0 dropped lib.
    _upload(disk_server, tmp_path, "app", "2.0", ["other"])
    _wait_used_by(disk_server, "lib/", {"any"})


def test_audit_rebuilds_lost_used_by_files(disk_server_fast_reconcile, tmp_path):
    server = disk_server_fast_reconcile
    _seed(server, tmp_path)
    state = server["data_dir"] / "_state"
    used_by = state / "used-by" / "lib.json"
    deadline = time.monotonic() + 15
    while not used_by.exists():
        assert time.monotonic() < deadline, "lib.json never written"
        time.sleep(0.2)

    # A server upgraded from before this feature has neither file; the audit
    # must build both from the stored metadata alone.
    for path in [*state.glob("requires-*.json"), *(state / "used-by").glob("*.json")]:
        path.unlink()
    # A tick can rewrite lib.json with only the packages it just rebuilt
    # (the last seed upload is `any`) before the audit fills in the rest.
    expected = {"any", "app", "old"}
    deadline = time.monotonic() + 30
    while (got := set(json.loads(used_by.read_text())) if used_by.exists() else set()) != expected:
        assert time.monotonic() < deadline, f"the audit never rebuilt lib.json: {got}"
        time.sleep(0.2)
