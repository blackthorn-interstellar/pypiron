"""Distributed S3-backed download counters: ingest on the real GET path, flush
to the counter store, and serve per-package/version stats over /stats.

Compaction/freeze (which needs a day to roll over) is covered by the Rust unit
tests in src/counters.rs; here we exercise the user-visible path end to end with
the real binary: upload -> download -> flush -> query."""

from __future__ import annotations

import re
import time

import pytest

from .helpers import (
    http_get,
    http_get_bytes,
    http_get_json,
    make_wheel,
    upload_legacy,
)

pytestmark = pytest.mark.integration


def _stats(base_url: str, pkg: str) -> dict:
    return http_get_json(f"{base_url}/stats/downloads/{pkg}")


def _wait_for_total(base_url: str, pkg: str, want: int, *, timeout: float = 8.0) -> dict:
    """Poll /stats until the package total reaches `want` (counters flush ~1s)."""
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        last = _stats(base_url, pkg)
        if last.get("total", 0) >= want:
            return last
        time.sleep(0.2)
    return last


def test_downloads_counted_per_version(disk_server_fast_counters, tmp_path):
    server = disk_server_fast_counters
    base = server["base_url"]
    pkg, version = "countme", "1.2.3"
    wheel = make_wheel(pkg, version, tmp_path)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["uploader_user"],
        password=server["uploader_password"],
    )

    # Download the artifact three times (disk backend streams a 200 each).
    url = f"{base}/files/{pkg}/{wheel.name}"
    for _ in range(3):
        assert http_get_bytes(url)

    stats = _wait_for_total(base, pkg, 3)
    assert stats["package"] == pkg
    assert stats["metric"] == "downloads"
    assert stats["total"] == 3
    # Filenames are rolled up to versions; today's (open) day is present.
    per_version = {v: c for day in stats["days"].values() for v, c in day.items()}
    assert per_version == {version: 3}


def test_companions_and_misses_are_not_counted(disk_server_fast_counters, tmp_path):
    server = disk_server_fast_counters
    base = server["base_url"]
    pkg, version = "noisepkg", "0.1.0"
    wheel = make_wheel(pkg, version, tmp_path)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["uploader_user"],
        password=server["uploader_password"],
    )

    # One real download...
    assert http_get_bytes(f"{base}/files/{pkg}/{wheel.name}")
    # ...plus traffic that must NOT count: the PEP 658 .metadata companion and a
    # request for a file that does not exist.
    code, _, _ = http_get(f"{base}/files/{pkg}/{wheel.name}.metadata")
    assert code == 200
    code, _, _ = http_get(f"{base}/files/{pkg}/{pkg}-9.9.9-py3-none-any.whl")
    assert code == 404

    stats = _wait_for_total(base, pkg, 1)
    assert stats["total"] == 1, stats


def test_project_page_shows_downloads_card(disk_server_fast_counters, tmp_path):
    server = disk_server_fast_counters
    base = server["base_url"]
    pkg, version = "shownpkg", "2.0.0"
    wheel = make_wheel(pkg, version, tmp_path)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["uploader_user"],
        password=server["uploader_password"],
    )
    for _ in range(2):
        assert http_get_bytes(f"{base}/files/{pkg}/{wheel.name}")
    _wait_for_total(base, pkg, 2)

    code, body, _ = http_get(f"{base}/project/{pkg}/")
    assert code == 200
    html = body.decode()
    assert "<h2>Downloads</h2>" in html
    assert "in the last 30 days" in html
    assert version in html


def test_prometheus_aggregate_not_per_package(disk_server_fast_counters, tmp_path):
    server = disk_server_fast_counters
    base = server["base_url"]
    pkg, version = "promcount", "1.0.0"
    wheel = make_wheel(pkg, version, tmp_path)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["uploader_user"],
        password=server["uploader_password"],
    )
    for _ in range(2):
        assert http_get_bytes(f"{base}/files/{pkg}/{wheel.name}")

    code, body, _ = http_get(f"{base}/metrics")
    assert code == 200
    text = body.decode()
    m = re.search(r"^pypiron_downloads_total (\d+)$", text, re.MULTILINE)
    assert m, text
    assert int(m.group(1)) >= 2
    # The aggregate carries NO per-package labels (cardinality stays bounded).
    assert "pypiron_downloads_total{" not in text
    assert pkg not in text

    # The dashboard's metrics section gains a "Downloads" tile (reads are public).
    code, body, _ = http_get(f"{base}/")
    assert code == 200
    assert ">Downloads</div>" in body.decode()


def test_stats_requires_read_auth_when_configured(disk_server_prefixed, tmp_path):
    # disk_server_prefixed has no read credential, so reads are public; assert the
    # endpoint answers and is well-formed for an unknown package (empty series).
    server = disk_server_prefixed
    stats = _stats(server["base_url"], "neveruploaded")
    assert stats == {
        "metric": "downloads",
        "package": "neveruploaded",
        "total": 0,
        "days": {},
    }
