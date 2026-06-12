"""/health, /metrics, and --log-format json."""

from __future__ import annotations

import json
import re

import pytest

from .helpers import _encode_basic_auth, http_get

pytestmark = pytest.mark.integration


def test_health_reports_ok(disk_server):
    code, body, headers = http_get(f"{disk_server['base_url']}/health")
    assert code == 200
    assert json.loads(body) == {"status": "ok"}
    assert headers.get("content-type") == "application/json"


def test_metrics_count_requests(disk_server):
    # Generate one known-good simple request, then scrape.
    code, _, _ = http_get(f"{disk_server['simple']}index.json")
    assert code == 200

    code, body, headers = http_get(f"{disk_server['base_url']}/metrics")
    assert code == 200
    assert headers.get("content-type", "").startswith("text/plain")
    text = body.decode()
    assert "# TYPE pypiron_http_requests_total counter" in text

    m = re.search(
        r'pypiron_http_requests_total\{route="simple",status="2xx"\} (\d+)', text
    )
    assert m, text
    assert int(m.group(1)) >= 1

    # The worker counters exist (values depend on timing, presence is the contract).
    assert "pypiron_index_rebuilds_total" in text
    assert "pypiron_reconcile_sweeps_total" in text
    assert "pypiron_proxy_artifacts_cached_total" in text


def test_metrics_attribute_project_without_read_auth(disk_server):
    """Open server: any volunteered basic-auth username is parsed for
    attribution — the password is never validated in this mode."""
    headers = {"Authorization": _encode_basic_auth("ci+attrib-open", "ignored")}
    code, _, _ = http_get(f"{disk_server['simple']}index.json", headers=headers)
    assert code == 200

    # An untagged username attributes as itself.
    headers = {"Authorization": _encode_basic_auth("plain-attrib", "ignored")}
    code, _, _ = http_get(f"{disk_server['simple']}index.json", headers=headers)
    assert code == 200

    _, body, _ = http_get(f"{disk_server['base_url']}/metrics")
    text = body.decode()
    assert re.search(
        r'pypiron_project_requests_total\{project="attrib-open",route="simple"\} \d+',
        text,
    ), text
    assert 'project="plain-attrib"' in text


def test_json_log_format_emits_json_lines(disk_server_json_logs):
    server = disk_server_json_logs
    code, _, _ = http_get(f"{server['base_url']}/health")
    assert code == 200

    lines = [
        line
        for line in server["log_path"].read_text().splitlines()
        if line.strip()
    ]
    assert lines, "server wrote no log lines"
    parsed = [json.loads(line) for line in lines]
    assert any(
        "listening on" in p.get("fields", {}).get("message", "") for p in parsed
    ), parsed
