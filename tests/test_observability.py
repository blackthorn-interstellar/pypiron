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

    m = re.search(r'pypiron_http_requests_total\{route="simple",status="2xx"\} (\d+)', text)
    assert m, text
    assert int(m.group(1)) >= 1

    # The worker counters exist (values depend on timing, presence is the contract).
    assert "pypiron_index_rebuilds_total" in text
    assert "pypiron_reconcile_sweeps_total" in text
    assert "pypiron_proxy_artifacts_cached_total" in text
    # Audit + election observability (event-driven indexer machinery).
    assert "pypiron_audit_packages_rebuilt_total" in text
    assert "pypiron_audit_packages_skipped_total" in text
    assert "pypiron_global_cas_conflicts_total" in text
    assert "pypiron_stale_intents_healed_total" in text
    assert "# TYPE pypiron_audit_last_duration_seconds gauge" in text


def _metric_value(text: str, name: str) -> float | None:
    m = re.search(rf"^{re.escape(name)} ([\d.eE+-]+)$", text, re.MULTILINE)
    return float(m.group(1)) if m else None


def test_audit_counters_populate_across_passes(disk_server_fast_reconcile):
    """An audit rebuilds a freshly-uploaded package once (no stored
    fingerprint), then skips it on every steady pass thereafter — the
    daily-audit default's "cost scales with churn" claim, made observable."""
    import time

    from .helpers import make_wheel, upload_legacy

    server = disk_server_fast_reconcile
    wheel = make_wheel("obs-audit", "1.0", server["data_dir"].parent / "wheels")
    upload_legacy(
        f"{server['base_url']}/legacy/",
        wheel,
        username=server["user"],
        password=server["password"],
    )

    # reconcile-interval is 2s; poll until a steady audit has skipped the
    # package (proves at least one rebuild pass and one fingerprint-hit pass).
    deadline = time.time() + 30
    skipped = rebuilt = 0.0
    while time.time() < deadline:
        _, body, _ = http_get(f"{server['base_url']}/metrics")
        text = body.decode()
        skipped = _metric_value(text, "pypiron_audit_packages_skipped_total") or 0.0
        rebuilt = _metric_value(text, "pypiron_audit_packages_rebuilt_total") or 0.0
        if skipped >= 1:
            break
        time.sleep(0.5)

    assert rebuilt >= 1, f"audit never rebuilt the new package: {rebuilt=}"
    assert skipped >= 1, f"steady audit never skipped on a fingerprint hit: {skipped=}"
    # The duration gauge reflects a completed pass (>= 0, present and numeric).
    _, body, _ = http_get(f"{server['base_url']}/metrics")
    assert _metric_value(body.decode(), "pypiron_audit_last_duration_seconds") is not None


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

    lines = [line for line in server["log_path"].read_text().splitlines() if line.strip()]
    assert lines, "server wrote no log lines"
    parsed = [json.loads(line) for line in lines]
    assert any("listening on" in p.get("fields", {}).get("message", "") for p in parsed), parsed


def _wait_for_line(log_path, predicate, timeout: float = 5.0):
    """Poll the server log until a line satisfies `predicate`; return it."""
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        for line in log_path.read_text().splitlines():
            if line.strip() and predicate(line):
                return line
        time.sleep(0.05)
    return None


def test_reads_not_logged_by_default(disk_server):
    """No --access-log: reads (GET) produce no access line, even at pypiron=debug."""
    code, _, _ = http_get(f"{disk_server['simple']}index.json")
    assert code == 200
    # The access events live on a dedicated target; its absence is the contract.
    assert "pypiron::access" not in disk_server["log_path"].read_text()


def test_mutations_logged_by_default(disk_server):
    """No --access-log: a mutation (upload) is still logged — the default audit."""
    from .helpers import make_wheel, upload_legacy

    wheel = make_wheel("obs-mut", "1.0", disk_server["data_dir"].parent / "wheels")
    upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    line = _wait_for_line(
        disk_server["log_path"],
        lambda ln: "pypiron::access" in ln and "method=POST" in ln and "/legacy" in ln,
    )
    assert line, "the upload (a mutation) should be logged by default"


def test_health_and_metrics_excluded_from_access_log(disk_server_access_log_info):
    """Full access log on, but at info: reads are logged, /health and /metrics
    are not (they log only at debug — LBs and Prometheus poll them constantly)."""
    server = disk_server_access_log_info
    http_get(f"{server['simple']}index.json", headers={"User-Agent": "probe-read/1"})
    http_get(f"{server['base_url']}/health")
    http_get(f"{server['base_url']}/metrics")

    # The read IS logged (firehose at info), proving the log is live...
    read = _wait_for_line(
        server["log_path"],
        lambda ln: "pypiron::access" in ln and "probe-read/1" in ln,
    )
    assert read, "a normal read should be logged with --access-log"
    # ...but no access line names /health or /metrics.
    access_lines = [
        ln for ln in server["log_path"].read_text().splitlines() if "pypiron::access" in ln
    ]
    assert not [ln for ln in access_lines if "/health" in ln or "/metrics" in ln], access_lines


def test_access_log_structured_json_fields(disk_server_access_log_json):
    server = disk_server_access_log_json
    headers = {
        "Authorization": _encode_basic_auth("ci+obs-access", "ignored"),
        "User-Agent": "pytest-acceslog/1.0",
    }
    code, _, _ = http_get(f"{server['simple']}index.json", headers=headers)
    assert code == 200

    # The fixture's startup probe also hits /simple/index.json (unauthenticated),
    # so key on our unique User-Agent to find this exact request.
    line = _wait_for_line(
        server["log_path"],
        lambda ln: '"target":"pypiron::access"' in ln and "pytest-acceslog/1.0" in ln,
    )
    assert line, "no structured access event was logged"
    event = json.loads(line)
    fields = event["fields"]
    assert event["level"] == "INFO"
    assert fields["message"] == "request"
    assert fields["method"] == "GET"
    assert fields["path"] == "/simple/index.json"
    assert fields["status"] == 200
    assert fields["project"] == "obs-access"
    assert fields["ua"] == "pytest-acceslog/1.0"
    assert fields["client"].startswith("127.0.0.1")  # ConnectInfo peer
    assert isinstance(fields["latency_ms"], (int, float))
    # bytes is the response Content-Length, or "-" when unknown.
    assert re.fullmatch(r"\d+|-", str(fields["bytes"])), fields["bytes"]


def test_access_log_clf_format(disk_server_access_log_clf):
    server = disk_server_access_log_clf
    headers = {"User-Agent": "pytest-clf/2.0"}
    code, _, _ = http_get(f"{server['simple']}index.json", headers=headers)
    assert code == 200

    # host - user [time] "METHOD target proto" status bytes "referer" "ua"
    clf = re.compile(
        r"^(?P<host>\S+) - (?P<user>\S+) \[(?P<time>[^\]]+)\] "
        r'"(?P<method>\S+) (?P<target>\S+) (?P<proto>HTTP/\S+)" '
        r'(?P<status>\d{3}) (?P<bytes>\S+) "(?P<referer>[^"]*)" "(?P<ua>[^"]*)"$'
    )
    line = _wait_for_line(
        server["log_path"],
        lambda ln: "pytest-clf/2.0" in ln and clf.match(ln) is not None,
    )
    assert line, "no Combined Log Format line was logged"
    m = clf.match(line)
    assert m.group("method") == "GET"
    assert m.group("target") == "/simple/index.json"
    assert m.group("status") == "200"
    assert m.group("ua") == "pytest-clf/2.0"
    assert m.group("host").startswith("127.0.0.1")
