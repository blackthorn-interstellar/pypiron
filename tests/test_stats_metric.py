"""`/stats/:metric` answers only for metrics the server actually records.

A stats query walks a 30-day window of day summaries and open-day segments
across every shard. Before the allowlist, any name in the path bought a caller
that whole scan for a metric that can never hold a count, and each distinct
name it cycled took a slot in the summary cache. Both surfaces now 404 an
unrecognized metric before issuing a single storage read."""

from __future__ import annotations

import pytest

from .helpers import http_get, http_get_json

pytestmark = pytest.mark.integration

# Names an unauthenticated caller might cycle through the path param: a near
# miss, a case variant, a traversal attempt, and something merely invented.
UNKNOWN_METRICS = [
    "download",
    "Downloads",
    "..%2Fdownloads",
    "uploads",
    "a" * 200,
]


def test_downloads_metric_still_answers(disk_server):
    base = disk_server["base_url"]

    summary = http_get_json(f"{base}/stats/downloads")
    assert summary["metric"] == "downloads"
    assert "days" in summary and "top" in summary

    series = http_get_json(f"{base}/stats/downloads/requests")
    assert series["metric"] == "downloads"
    assert series["package"] == "requests"
    assert "days" in series


@pytest.mark.parametrize("metric", UNKNOWN_METRICS)
def test_unknown_metric_is_not_found(disk_server, metric):
    base = disk_server["base_url"]

    status, _, _ = http_get(f"{base}/stats/{metric}")
    assert status == 404, f"global /stats/{metric} answered {status}"

    status, _, _ = http_get(f"{base}/stats/{metric}/requests")
    assert status == 404, f"per-package /stats/{metric}/requests answered {status}"


def test_read_auth_is_checked_before_the_metric_name(disk_server_read_auth):
    """An unauthenticated caller learns nothing about which metrics exist: both
    a real and an invented name answer 401, not 404 vs 200."""
    base = disk_server_read_auth["base_url"]

    for path in ("/stats/downloads", "/stats/nosuchmetric"):
        status, _, _ = http_get(f"{base}{path}")
        assert status == 401, f"{path} answered {status} without credentials"
