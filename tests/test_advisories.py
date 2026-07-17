"""Advisory snapshot pipeline (rung 3): the feed loads into a live snapshot,
the reader-GET / admin-PUT `/advisories/feed` push path works, and startup is
fail-closed by intent (AC7). Everything through the real binary over HTTP.

Enforcement (the byte gate) and the audit report land in later rungs; this file
covers the snapshot plumbing and the module-level fixture helpers those rungs
reuse.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import zipfile
from contextlib import contextmanager
from pathlib import Path

import pytest

from .conftest import _start_disk_server
from .helpers import (
    _encode_basic_auth,
    find_free_port,
    http_get,
    http_request_auth,
    kill_process_tree,
    wait_http_responding,
)

pytestmark = pytest.mark.integration

# Canonical fixture advisories, per the ADVISORIES.md acceptance preamble: one
# MAL exact-version, one MAL all-versions (introduced "0"), one PYSEC with a
# fixed-in. IDs are asserted byte-equal by later rungs (AC9), so keep them stable.
MAL_EXACT_ID = "MAL-2024-91001"
MAL_ALL_ID = "MAL-2024-91002"
PYSEC_ID = "PYSEC-2024-9100"

MAL_EXACT_PKG = "evilpkg"
MAL_EXACT_VERSION = "1.3.7"
MAL_ALL_PKG = "totally-malware"
PYSEC_PKG = "vulnlib"
PYSEC_FIXED_IN = "2.4.0"


def canonical_records() -> dict:
    """The three-advisory fixture set the acceptance criteria are written against."""
    return {
        MAL_EXACT_ID: {
            "id": MAL_EXACT_ID,
            "summary": f"malicious code in {MAL_EXACT_PKG} {MAL_EXACT_VERSION}",
            "affected": [
                {
                    "package": {"ecosystem": "PyPI", "name": MAL_EXACT_PKG},
                    "versions": [MAL_EXACT_VERSION],
                }
            ],
        },
        MAL_ALL_ID: {
            "id": MAL_ALL_ID,
            "summary": "malware masquerading as a helper library",
            "affected": [
                {
                    "package": {"ecosystem": "PyPI", "name": MAL_ALL_PKG},
                    "ranges": [{"type": "ECOSYSTEM", "events": [{"introduced": "0"}]}],
                }
            ],
        },
        PYSEC_ID: {
            "id": PYSEC_ID,
            "summary": f"SSRF in {PYSEC_PKG} before {PYSEC_FIXED_IN}",
            "severity": [{"type": "CVSS_V3", "score": "7.5"}],
            "affected": [
                {
                    "package": {"ecosystem": "PyPI", "name": PYSEC_PKG},
                    "ranges": [
                        {
                            "type": "ECOSYSTEM",
                            "events": [{"introduced": "0"}, {"fixed": PYSEC_FIXED_IN}],
                        }
                    ],
                }
            ],
        },
    }


def make_osv_zip(path: Path, records: dict) -> Path:
    """Write `{id: advisory-dict}` as flat `<id>.json` members — the shape of the
    OSV PyPI export, small enough to seed a hermetic test (no network)."""
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as zf:
        for adv_id, record in records.items():
            zf.writestr(f"{adv_id}.json", json.dumps(record))
    return path


def start_advisory_server(tmp_path_factory, pypiron_bin, feed, extra_args=(), extra_env=None):
    """A disk server with the advisory feed configured (generator; drive with
    next()/close() or the `advisory_server` context manager). `feed` is a URL or
    local path, or None to omit --advisory-feed and take the always-on defaults.
    Reused by later-rung advisory tests."""
    args = list(extra_args)
    if feed is not None:
        args = ["--advisory-feed", str(feed), *args]
    return _start_disk_server(tmp_path_factory, pypiron_bin, extra_args=args, extra_env=extra_env)


@contextmanager
def advisory_server(tmp_path_factory, pypiron_bin, feed, extra_args=(), extra_env=None):
    gen = start_advisory_server(tmp_path_factory, pypiron_bin, feed, extra_args, extra_env)
    server = next(gen)
    try:
        yield server
    finally:
        gen.close()  # runs _start_disk_server's finally → kill_process_tree


def _metric_value(text: str, name: str):
    m = re.search(rf"^{re.escape(name)} ([\d.eE+-]+)$", text, re.MULTILINE)
    return float(m.group(1)) if m else None


def _poll_metric(base_url: str, name: str, *, timeout: float = 20.0):
    """Poll /metrics until `name` appears; returns its value or None on timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        code, body, _ = http_get(f"{base_url}/metrics")
        if code == 200:
            value = _metric_value(body.decode(), name)
            if value is not None:
                return value
        time.sleep(0.1)
    return None


def _wait_log_contains(log_path: Path, needle: str, *, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if log_path.exists() and needle in log_path.read_text(errors="replace"):
            return
        time.sleep(0.1)
    dump = log_path.read_text(errors="replace") if log_path.exists() else "(no log)"
    raise AssertionError(f"log never contained {needle!r} within {timeout}s:\n{dump}")


def _wait_file_bytes(path: Path, expected: bytes, *, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists() and path.read_bytes() == expected:
            return
        time.sleep(0.05)
    raise AssertionError(f"{path} never matched the expected bytes within {timeout}s")


def _stored_feed_path(server) -> Path:
    return Path(server["data_dir"]) / "_advisories" / "osv-pypi.zip"


def test_feed_load_populates_snapshot_age_gauge(tmp_path_factory, pypiron_bin, tmp_path):
    """A local-path feed loads at startup and arms the per-node staleness gauge."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    with advisory_server(tmp_path_factory, pypiron_bin, feed) as server:
        age = _poll_metric(server["base_url"], "pypiron_advisory_snapshot_age_seconds")
        assert age is not None, "snapshot-age gauge never appeared after loading the feed"
        assert age >= 0
        # The verbatim snapshot lands under the reserved prefix.
        assert _stored_feed_path(server).exists()


def test_feed_get_serves_zip_with_conditional_and_auth(tmp_path_factory, pypiron_bin, tmp_path):
    """GET /advisories/feed serves the stored zip to a reader, honors If-None-Match,
    and 401s an unauthenticated request on a read-gated server."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    seeded = feed.read_bytes()
    read_auth = ["--read-user", "reader", "--read-pass", "readersecret"]
    with advisory_server(tmp_path_factory, pypiron_bin, feed, extra_args=read_auth) as server:
        url = f"{server['base_url']}/advisories/feed"
        # Reader credential → 200 with the exact seeded bytes and an ETag.
        code, body, headers = http_request_auth(
            "GET", url, username="reader", password="readersecret"
        )
        assert code == 200, (code, body)
        assert body == seeded, "served feed bytes differ from the seeded snapshot"
        assert headers.get("content-type") == "application/zip"
        etag = headers.get("etag")
        assert etag, "advisory feed response carried no ETag"

        # If-None-Match with that ETag → 304, empty body.
        code, body, _ = http_get(
            url,
            headers={
                "Authorization": _encode_basic_auth("reader", "readersecret"),
                "If-None-Match": etag,
            },
        )
        assert code == 304
        assert body == b""

        # Unauthenticated on a read-gated server → 401.
        code, _, _ = http_get(url)
        assert code == 401


def test_feed_put_persists_and_reloads_without_restart(tmp_path_factory, pypiron_bin, tmp_path):
    """PUT /advisories/feed: admin persists a new snapshot byte-for-byte and the
    worker reloads it live; non-admin is rejected; a garbage body 400s and never
    overwrites the stored snapshot."""
    feed_a = make_osv_zip(tmp_path / "a.zip", {MAL_EXACT_ID: canonical_records()[MAL_EXACT_ID]})
    with advisory_server(tmp_path_factory, pypiron_bin, feed_a) as server:
        url = f"{server['base_url']}/advisories/feed"
        stored = _stored_feed_path(server)
        _wait_file_bytes(stored, feed_a.read_bytes())  # startup persisted feed A
        before = stored.read_bytes()

        # Non-admin (uploader) cannot push.
        code, _, _ = http_request_auth(
            "PUT", url, username="uploader", password="uploadersecret", data=b"whatever"
        )
        assert code in (401, 403)
        assert stored.read_bytes() == before

        # A garbage body is rejected before it can poison the snapshot.
        code, _, _ = http_request_auth(
            "PUT", url, username="admin", password="secret", data=b"not a zip at all"
        )
        assert code == 400
        assert stored.read_bytes() == before, "a rejected PUT must not overwrite the snapshot"

        # Admin pushes the full fixture → 204, persisted verbatim, loaded live.
        feed_b = make_osv_zip(tmp_path / "b.zip", canonical_records()).read_bytes()
        code, _, _ = http_request_auth("PUT", url, username="admin", password="secret", data=feed_b)
        assert code == 204
        _wait_file_bytes(stored, feed_b)
        # The worker's reload logs the swap — proof of load-without-restart.
        _wait_log_contains(Path(server["log_path"]), "advisory snapshot loaded")


def test_ac7_explicit_unreachable_source_refuses_start(pypiron_bin, tmp_path):
    """An explicit advisory setting that cannot produce a snapshot fails closed at
    startup — the credential-refusal rule, applied to the feed."""
    dead = find_free_port()
    feed = f"http://127.0.0.1:{dead}"  # nothing listening → connection refused

    # Explicit --malware-block with an unreachable feed and empty _advisories.
    cp = _serve_expect_exit(
        pypiron_bin,
        tmp_path / "explicit-block",
        ["--malware-block", "true", "--advisory-feed", feed],
    )
    assert cp.returncode != 0, cp.stdout + cp.stderr
    out = (cp.stdout + cp.stderr).lower()
    assert "advisory" in out and "snapshot" in out, out

    # An explicit feed alone (default malware-block) refuses just the same.
    cp = _serve_expect_exit(pypiron_bin, tmp_path / "explicit-feed", ["--advisory-feed", feed])
    assert cp.returncode != 0, cp.stdout + cp.stderr
    assert "advisory" in (cp.stdout + cp.stderr).lower()


def test_ac7_implicit_default_starts_unfed_then_self_arms(pypiron_bin, tmp_path):
    """The always-on default must never brick a box that never asked: with the OSV
    URL unreachable (a dead forward proxy standing in), the server starts, serves,
    warns that blocking is armed but unfed, and self-arms when a snapshot arrives —
    no restart. Hand-rolled (clean env) so it exercises the true implicit default,
    independent of the conftest advisory-disable other tests rely on."""
    env = _dead_proxy_env()
    proc, base, log_path = _hand_start(pypiron_bin, tmp_path / "data", [], extra_env=env)
    try:
        # It binds immediately (the implicit obtain is non-blocking) and serves.
        code, _, _ = http_get(f"{base}/simple/index.json")
        assert code == 200
        # The worker's first (background) obtain fails → armed-but-unfed warning.
        _wait_log_contains(log_path, "armed but unfed")
        # No snapshot yet → the gauge is absent (not a misleading zero).
        code, body, _ = http_get(f"{base}/metrics")
        assert code == 200
        assert _metric_value(body.decode(), "pypiron_advisory_snapshot_age_seconds") is None

        # Push a snapshot → blocking self-arms without a restart.
        feed_bytes = make_osv_zip(tmp_path / "osv.zip", canonical_records()).read_bytes()
        code, _, _ = http_request_auth(
            "PUT", f"{base}/advisories/feed", username="admin", password="secret", data=feed_bytes
        )
        assert code == 204
        age = _poll_metric(base, "pypiron_advisory_snapshot_age_seconds")
        assert age is not None, "gauge never appeared after the snapshot was pushed"
    finally:
        kill_process_tree(proc)


def test_restart_from_stored_snapshot_satisfies_fail_closed(pypiron_bin, tmp_path):
    """After a snapshot has been delivered, an explicit --malware-block restart with
    the source still unreachable starts cleanly from the stored snapshot alone."""
    env = _dead_proxy_env()
    feed_bytes = make_osv_zip(tmp_path / "osv.zip", canonical_records()).read_bytes()
    data_dir = tmp_path / "data"

    # First boot (implicit default, unfed), seed the snapshot via PUT, then stop.
    proc, base, _log = _hand_start(pypiron_bin, data_dir, [], extra_env=env)
    try:
        code, _, _ = http_request_auth(
            "PUT", f"{base}/advisories/feed", username="admin", password="secret", data=feed_bytes
        )
        assert code == 204
        assert _poll_metric(base, "pypiron_advisory_snapshot_age_seconds") is not None
    finally:
        kill_process_tree(proc)

    # Respawn on the SAME data dir with an explicit demand to block and the source
    # still unreachable: the stored snapshot must satisfy the fail-closed check.
    proc, base, _log = _hand_start(
        pypiron_bin, data_dir, ["--malware-block", "true"], extra_env=env
    )
    try:
        age = _poll_metric(base, "pypiron_advisory_snapshot_age_seconds")
        assert age is not None, "restart did not arm blocking from the stored snapshot"
    finally:
        kill_process_tree(proc)


def test_default_disk_server_stays_advisory_hermetic(disk_server):
    """Regression pin for hermeticity: a plain default disk server (advisory
    disabled in the shared test env) makes no OSV fetch and writes no
    `_advisories/` snapshot, so the suite never depends on the network."""
    code, body, _ = http_get(f"{disk_server['base_url']}/metrics")
    assert code == 200
    assert _metric_value(body.decode(), "pypiron_advisory_snapshot_age_seconds") is None
    assert not (Path(disk_server["data_dir"]) / "_advisories").exists()


# ------------------------------- test helpers --------------------------------


def _dead_proxy_env() -> dict:
    """Point every proxy var at a closed local port so the OSV pull fails fast and
    hermetically, while still exercising the proxy-honoring egress path."""
    dead = f"http://127.0.0.1:{find_free_port()}"
    return {
        "HTTP_PROXY": dead,
        "HTTPS_PROXY": dead,
        "ALL_PROXY": dead,
        "NO_PROXY": "",
    }


def _serve_expect_exit(pypiron_bin, data_dir: Path, extra_args, *, timeout: float = 30.0):
    """Run `serve` expecting it to exit (the advisory obtain happens before the
    listener binds, so a fail-closed refusal returns instead of serving)."""
    data_dir.mkdir(parents=True, exist_ok=True)
    args = [
        str(pypiron_bin),
        "serve",
        "--bind-addr",
        f"127.0.0.1:{find_free_port()}",
        "--data-dir",
        str(data_dir),
        *extra_args,
    ]
    # Isolate from any ambient ./pypiron.toml.
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout, cwd=str(data_dir))


def _hand_start(
    pypiron_bin, data_dir: Path, extra_args, *, extra_env=None, boot_timeout: float = 25.0
):
    """Start a server on `data_dir` with a clean env (the implicit advisory
    default, no conftest disable), returning (proc, base_url, log_path)."""
    data_dir.mkdir(parents=True, exist_ok=True)
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info,pypiron=debug")
    if extra_env:
        env.update(extra_env)
    log_path = data_dir.parent / f"{data_dir.name}-restart.log"
    args = [
        str(pypiron_bin),
        "serve",
        "--bind-addr",
        bind,
        "--data-dir",
        str(data_dir),
        "--admin-user",
        "admin",
        "--admin-pass",
        "secret",
        "--worker-interval-secs",
        "1",
        *extra_args,
    ]
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    base = f"http://{bind}"
    try:
        wait_http_responding(f"{base}/simple/index.json", timeout=boot_timeout)
    except Exception:
        kill_process_tree(proc)
        raise
    return proc, base, log_path
