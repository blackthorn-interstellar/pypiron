"""Advisory snapshot pipeline + malware enforcement.

Rung 3 (snapshot plumbing): the feed loads into a live snapshot, the reader-GET /
admin-PUT `/advisories/feed` push path works, and startup is fail-closed by
intent (AC7). Rung 4 (enforcement): the malware byte gate refuses blocked bytes
where they're served (AC1), private origin exempts a same-named package (AC2),
the on-demand proxy refuses to fill a blocked version (AC3), and a dead feed
degrades freshness, never availability (AC6). Rung 5 (listings + quarantine): the
materialized and proxy-rendered listings scrub blocked files, origin-aware (AC4),
and a relayed PEP 792 `quarantined` status blocks the byte gate after the worker's
next sweep (AC5). Rung 6 (sync relay): `pypiron sync` ferries the snapshot across
the air gap — a local zip, a URL, or the source server's own feed — pushing it to
the destination's admin PUT etag-conditioned, on by default, tolerant of a
feed-less source/dest (AC10's blocking half). Rungs 7-8 (audit report + panel):
the org audit materializes `_advisories/report.json` and serves it admin-gated at
`/audit` + `/audit.json` — exactly the seeded vulnerable/malicious rows with
download counts (AC8), advisory ids byte-equal to the feed (AC9), rows on an
air-gapped relayed dest (AC10 audit half) — plus a per-project advisory panel.
Everything through the real binary over HTTP, driven by real `uv`.
"""

from __future__ import annotations

import http.server
import json
import os
import re
import subprocess
import threading
import time
import zipfile
from contextlib import contextmanager
from pathlib import Path

import pytest

from .conftest import _start_disk_server, _start_proxy_pair
from .helpers import (
    _encode_basic_auth,
    find_free_port,
    get_index_json,
    http_get,
    http_head,
    http_request_auth,
    kill_process_tree,
    make_wheel,
    run_checked,
    run_returncode,
    sync_to,
    upload_legacy,
    wait_for_file_in_index,
    wait_http_responding,
)

pytestmark = pytest.mark.integration

# Canonical fixture advisories: one
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


# Every real OSV record carries a `modified` timestamp; the snapshot-staleness
# gauge reads the newest one as the feed's content watermark. Stamp any fixture
# record that omits it so the gauge arms (a fixed past instant keeps the content
# age large and monotonically rising with wall time).
_DEFAULT_MODIFIED = "2024-01-01T00:00:00Z"


def make_osv_zip(path: Path, records: dict) -> Path:
    """Write `{id: advisory-dict}` as flat `<id>.json` members — the shape of the
    OSV PyPI export, small enough to seed a hermetic test (no network)."""
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as zf:
        for adv_id, record in records.items():
            record = {"modified": _DEFAULT_MODIFIED, **record}
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
    blocks from the embedded floor snapshot, warns that no live snapshot is loaded,
    and self-arms when one arrives — no restart. Hand-rolled (clean env) so it
    exercises the true implicit default, independent of the conftest
    advisory-disable other tests rely on."""
    env = _dead_proxy_env()
    proc, base, log_path = _hand_start(pypiron_bin, tmp_path / "data", [], extra_env=env)
    try:
        # It binds immediately (the implicit obtain is non-blocking) and serves.
        code, _, _ = http_get(f"{base}/simple/index.json")
        assert code == 200
        # The embedded floor arms blocking at boot, before any feed exists.
        _wait_log_contains(log_path, "armed from the embedded floor snapshot")
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


def test_leader_reseeds_snapshot_deleted_from_under_it(tmp_path_factory, pypiron_bin, tmp_path):
    """The advisory snapshot is a leader-authored control singleton: multi-bucket,
    it write-throughs to every bucket, but a bucket can still lose it. The leader
    re-seeds the bytes it retains in memory when FEED_KEY is absent on its selected
    bucket. Single-bucket disk exercises the same path: delete the zip out from
    under an armed server and it reappears byte-identical."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    seeded = feed.read_bytes()
    with advisory_server(
        tmp_path_factory, pypiron_bin, feed, extra_args=["--reconcile-interval-secs", "2"]
    ) as server:
        assert _poll_metric(server["base_url"], "pypiron_advisory_snapshot_age_seconds") is not None
        stored = _stored_feed_path(server)
        assert stored.read_bytes() == seeded, "server did not arm from the local zip"
        stored.unlink()  # simulate a failover onto a bucket that never had it
        assert not stored.exists()
        # The leader's next tick detects the absence and re-persists the retained bytes.
        _wait_file_bytes(stored, seeded, timeout=15.0)


# ------------------------------ enforcement (rung 4) -------------------------


def test_ac1_byte_gate_blocks_mal_version_by_default(
    tmp_path_factory, pypiron_bin, tmp_path, uv_path, uv_venv
):
    """AC1: with only --advisory-feed (no --malware-block — default-on is the
    point), a mirror-origin file matching a MAL advisory 403s at the byte gate
    with the advisory id in the body, the pinned install fails, and the tripwire
    metric increments. A clean version of the same project still installs, proving
    the block is version-scoped, not name-scoped."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    with advisory_server(tmp_path_factory, pypiron_bin, feed) as server:
        base = server["base_url"]
        # The synchronous startup obtain means the snapshot is loaded before the
        # listener binds; confirm the gate is armed.
        assert _poll_metric(base, "pypiron_advisory_snapshot_age_seconds") is not None

        bad = make_wheel(MAL_EXACT_PKG, MAL_EXACT_VERSION, tmp_path)
        clean = make_wheel(MAL_EXACT_PKG, "1.4.0", tmp_path)
        _mirror_upload(server, bad)
        _mirror_upload(server, clean)
        # The blocked wheel is cached in storage (the upload is synchronous) but no
        # longer listed — rung 5 scrubs it from the index (AC4). The byte gate is
        # the guarantee for the direct URL below; wait on the clean file as the
        # rebuild sync point instead.
        wait_for_file_in_index(server["simple"], MAL_EXACT_PKG, clean.name)

        before = _metric_value(_metrics_text(base), "pypiron_blocked_downloads_total")
        assert before is not None, "the blocked-downloads counter should always render"

        # Direct GET of the malicious wheel → 403 naming the advisory id byte-exactly.
        code, body, _ = http_get(f"{base}/files/{MAL_EXACT_PKG}/{bad.name}")
        assert code == 403, (code, body)
        assert MAL_EXACT_ID.encode() in body, body
        assert _poll_blocked_at_least(base, before + 1), "tripwire metric never incremented"

        # A pinned install of the bad version fails (uv can't fetch the wheel).
        rc, out, err = _uv_install(
            uv_path, uv_venv, server, f"{MAL_EXACT_PKG}=={MAL_EXACT_VERSION}"
        )
        assert rc != 0, f"install of malware succeeded:\n{out}\n{err}"

        # The clean version of the SAME project installs normally.
        rc, out, err = _uv_install(uv_path, uv_venv, server, f"{MAL_EXACT_PKG}==1.4.0")
        assert rc == 0, f"clean version failed to install:\n{out}\n{err}"
        run_checked([str(uv_venv), "-c", f"import {_import_name(MAL_EXACT_PKG)}"])


def test_ac2_private_package_sharing_mal_name_installs(
    tmp_path_factory, pypiron_bin, tmp_path, uv_path, uv_venv
):
    """AC2: a private-origin package sharing a malicious (all-versions) name
    installs normally — origin exclusivity is the proof that a same-named private
    package is not the one OSV named."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    with advisory_server(tmp_path_factory, pypiron_bin, feed) as server:
        base = server["base_url"]
        assert _poll_metric(base, "pypiron_advisory_snapshot_age_seconds") is not None

        # A plain uploader-cred upload (no mirror field) claims PRIVATE origin.
        wheel = make_wheel(MAL_ALL_PKG, "1.0.0", tmp_path)
        upload_legacy(
            server["legacy"],
            wheel,
            username=server["uploader_user"],
            password=server["uploader_password"],
        )
        wait_for_file_in_index(server["simple"], MAL_ALL_PKG, wheel.name)

        # Sanity: the byte gate does allow this private file directly.
        code, _, _ = http_get(f"{base}/files/{MAL_ALL_PKG}/{wheel.name}")
        assert code in (200, 302), code

        rc, out, err = _uv_install(uv_path, uv_venv, server, f"{MAL_ALL_PKG}==1.0.0")
        assert rc == 0, f"private package sharing a MAL name failed to install:\n{out}\n{err}"
        run_checked([str(uv_venv), "-c", f"import {_import_name(MAL_ALL_PKG)}"])


def test_ac3_proxy_refuses_to_fill_mal_version(
    tmp_path_factory, pypiron_bin, tmp_path, uv_path, uv_venv
):
    """AC3: a fresh proxy asked for a MAL version refuses the fill — the install
    fails, the tripwire metric increments, and no artifact or sidecar is written
    to the proxy's storage. A clean package still proxies and caches."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    with _advisory_proxy_pair(tmp_path_factory, pypiron_bin, feed) as pair:
        upstream, proxy = pair["upstream"], pair["proxy"]
        pbase = proxy["base_url"]
        assert _poll_metric(pbase, "pypiron_advisory_snapshot_age_seconds") is not None

        # Upstream hosts the malicious wheel (the upstream has no gate of its own).
        bad = make_wheel(MAL_EXACT_PKG, MAL_EXACT_VERSION, tmp_path)
        upload_legacy(
            upstream["legacy"],
            bad,
            username=upstream["uploader_user"],
            password=upstream["uploader_password"],
        )
        wait_for_file_in_index(upstream["simple"], MAL_EXACT_PKG, bad.name)

        before = _metric_value(_metrics_text(pbase), "pypiron_blocked_downloads_total")
        assert before is not None

        # Install through the proxy fails: the fill is refused, then the byte gate
        # 403s the unclaimed name.
        rc, out, err = _uv_install(uv_path, uv_venv, proxy, f"{MAL_EXACT_PKG}=={MAL_EXACT_VERSION}")
        assert rc != 0, f"proxy install of malware succeeded:\n{out}\n{err}"

        # Direct GET through the proxy → 403 naming the id (deterministic bump).
        code, body, _ = http_get(f"{pbase}/files/{MAL_EXACT_PKG}/{bad.name}")
        assert code == 403, (code, body)
        assert MAL_EXACT_ID.encode() in body, body
        assert _poll_blocked_at_least(pbase, before + 1)

        # The refusal wrote nothing: no artifact, no sidecar, not even a claim.
        pkg_dir = Path(proxy["data_dir"]) / "packages" / MAL_EXACT_PKG
        assert not (pkg_dir / bad.name).exists(), "malware artifact was cached despite refusal"
        assert not (pkg_dir / f"{bad.name}.meta.json").exists(), "malware sidecar was written"
        assert not (pkg_dir / ".origin").exists(), "a mirror claim was made for refused malware"

        # Positive control: a clean package still proxies and caches.
        good = make_wheel("cleanlib", "2.0.0", tmp_path)
        upload_legacy(
            upstream["legacy"],
            good,
            username=upstream["uploader_user"],
            password=upstream["uploader_password"],
        )
        wait_for_file_in_index(upstream["simple"], "cleanlib", good.name)
        rc, out, err = _uv_install(uv_path, uv_venv, proxy, "cleanlib==2.0.0")
        assert rc == 0, f"clean package failed to proxy:\n{out}\n{err}"
        run_checked([str(uv_venv), "-c", "import cleanlib"])
        assert (Path(proxy["data_dir"]) / "packages" / "cleanlib" / good.name).exists()


def test_ac6_dead_feed_keeps_serving_and_ages_gauge(
    tmp_path_factory, pypiron_bin, tmp_path, uv_path, uv_venv
):
    """AC6: killing the feed source after a snapshot exists degrades freshness,
    never availability — clean installs still proceed and the staleness gauge
    rises (no network on the serve path). No timing/latency assertions."""
    feed_bytes = make_osv_zip(tmp_path / "osv.zip", canonical_records()).read_bytes()
    # Clear proxy env so the SSRF-guarded feed client reaches loopback directly.
    direct = {"HTTP_PROXY": "", "HTTPS_PROXY": "", "ALL_PROXY": "", "NO_PROXY": "*"}
    with _local_feed_server(feed_bytes) as (feed_url, stop_feed):
        with advisory_server(
            tmp_path_factory,
            pypiron_bin,
            feed_url,
            extra_args=["--reconcile-interval-secs", "2"],
            extra_env=direct,
        ) as server:
            base = server["base_url"]
            # The snapshot loaded from the live feed → gauge armed.
            assert _poll_metric(base, "pypiron_advisory_snapshot_age_seconds") is not None

            # Kill the feed; pypiron must keep serving from the last snapshot.
            stop_feed()

            # A clean package still installs — the serve path never touches the feed.
            good = make_wheel("stillfine", "1.0.0", tmp_path)
            _mirror_upload(server, good)
            wait_for_file_in_index(server["simple"], "stillfine", good.name)
            rc, out, err = _uv_install(uv_path, uv_venv, server, "stillfine==1.0.0")
            assert rc == 0, f"clean install failed after feed death:\n{out}\n{err}"
            run_checked([str(uv_venv), "-c", "import stillfine"])

            # Staleness climbs across two reads ≥3s apart — no refresh can land.
            age1 = _metric_value(_metrics_text(base), "pypiron_advisory_snapshot_age_seconds")
            assert age1 is not None
            time.sleep(3.5)
            age2 = _metric_value(_metrics_text(base), "pypiron_advisory_snapshot_age_seconds")
            assert age2 is not None
            assert age2 > age1, f"staleness gauge did not rise after feed death: {age1} -> {age2}"


# --------------------------- listings + quarantine (rung 5) ------------------


def test_ac4_materialized_listing_scrubs_blocked_file(
    tmp_path_factory, pypiron_bin, tmp_path, uv_path, uv_venv
):
    """AC4 (materialized): after the worker rebuilds the index, a MAL-blocked file
    is gone from both the PEP 503 HTML and PEP 691 JSON listings, while a clean
    version of the same project and an unrelated clean package still list and
    install. A private package sharing the all-versions MAL name keeps every file
    listed — the scrub is origin-aware (mirror only)."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    with advisory_server(tmp_path_factory, pypiron_bin, feed) as server:
        base = server["base_url"]
        # Explicit feed → synchronous startup obtain, so the scrub is armed before
        # the first rebuild ever runs.
        assert _poll_metric(base, "pypiron_advisory_snapshot_age_seconds") is not None

        # The malware landed in storage before OSV named it (mirror-cached), plus a
        # clean version of the same project and an unrelated clean package.
        bad = make_wheel(MAL_EXACT_PKG, MAL_EXACT_VERSION, tmp_path)
        clean = make_wheel(MAL_EXACT_PKG, "1.4.0", tmp_path)
        other = make_wheel("cleanmate", "1.0.0", tmp_path)
        for dist in (bad, clean, other):
            _mirror_upload(server, dist)
        wait_for_file_in_index(server["simple"], MAL_EXACT_PKG, clean.name)
        wait_for_file_in_index(server["simple"], "cleanmate", other.name)

        # PEP 691 JSON: the blocked file is scrubbed; the clean one remains.
        names = {f["filename"] for f in get_index_json(server["simple"], MAL_EXACT_PKG)["files"]}
        assert bad.name not in names, f"blocked file still listed in JSON: {names}"
        assert clean.name in names, f"clean file missing from JSON: {names}"

        # PEP 503 HTML: same scrub, same render path (render.rs takes pre-filtered files).
        code, body, _ = http_get(f"{server['simple']}{MAL_EXACT_PKG}/")
        assert code == 200, code
        html = body.decode()
        assert bad.name not in html, "blocked file still linked in HTML"
        assert clean.name in html, "clean file missing from HTML"

        # A name-scoped block would break this; the version-scoped scrub doesn't.
        rc, out, err = _uv_install(uv_path, uv_venv, server, f"{MAL_EXACT_PKG}==1.4.0")
        assert rc == 0, f"clean version failed to install:\n{out}\n{err}"
        run_checked([str(uv_venv), "-c", f"import {_import_name(MAL_EXACT_PKG)}"])

        # Origin-aware guard: a PRIVATE package sharing the all-versions MAL name
        # keeps listing (origin exclusivity — it is not the package OSV named).
        priv = make_wheel(MAL_ALL_PKG, "1.0.0", tmp_path)
        upload_legacy(
            server["legacy"],
            priv,
            username=server["uploader_user"],
            password=server["uploader_password"],
        )
        priv_doc = wait_for_file_in_index(server["simple"], MAL_ALL_PKG, priv.name)
        assert any(f["filename"] == priv.name for f in priv_doc["files"]), (
            "a private package sharing a MAL name was wrongly scrubbed"
        )


def test_ac4_proxy_listing_scrubs_blocked_file(
    tmp_path_factory, pypiron_bin, tmp_path, uv_path, uv_venv
):
    """AC4 (proxy): the proxy's rendered PEP 503/691 listing excludes an upstream
    MAL-blocked file but keeps a clean version, which then installs through the
    proxy. Proxy listings are mirror-origin by definition, so the scrub is
    unconditional (no origin read)."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    with _advisory_proxy_pair(tmp_path_factory, pypiron_bin, feed) as pair:
        upstream, proxy = pair["upstream"], pair["proxy"]
        pbase = proxy["base_url"]
        assert _poll_metric(pbase, "pypiron_advisory_snapshot_age_seconds") is not None

        # Upstream hosts a blocked and a clean version (it has no gate of its own).
        bad = make_wheel(MAL_EXACT_PKG, MAL_EXACT_VERSION, tmp_path)
        clean = make_wheel(MAL_EXACT_PKG, "1.4.0", tmp_path)
        for dist in (bad, clean):
            upload_legacy(
                upstream["legacy"],
                dist,
                username=upstream["uploader_user"],
                password=upstream["uploader_password"],
            )
        wait_for_file_in_index(upstream["simple"], MAL_EXACT_PKG, clean.name)

        # The proxy's rendered listing scrubs the blocked file, keeps the clean one.
        names = {f["filename"] for f in get_index_json(proxy["simple"], MAL_EXACT_PKG)["files"]}
        assert bad.name not in names, f"proxy listed a blocked file: {names}"
        assert clean.name in names, f"proxy dropped the clean file: {names}"

        code, body, _ = http_get(f"{proxy['simple']}{MAL_EXACT_PKG}/")
        assert code == 200, code
        html = body.decode()
        assert bad.name not in html, "proxy HTML linked a blocked file"
        assert clean.name in html, "proxy HTML dropped the clean file"

        # The clean version proxies and installs.
        rc, out, err = _uv_install(uv_path, uv_venv, proxy, f"{MAL_EXACT_PKG}==1.4.0")
        assert rc == 0, f"clean version failed to proxy-install:\n{out}\n{err}"
        run_checked([str(uv_venv), "-c", f"import {_import_name(MAL_EXACT_PKG)}"])


def test_ac5_quarantine_reaches_the_byte_gate_after_sweep(tmp_path_factory, pypiron_bin, tmp_path):
    """AC5: a mirror-origin file of a project with relayed PEP 792 `quarantined`
    status 403s at the byte gate after the worker's next sweep derives the
    quarantined set (bounded by one reconcile interval); the listing empties
    immediately at render time as before. Clearing the status dequarantines — the
    gate serves the file again once the derived set updates, so dequarantine
    propagates too. A non-MAL name, so the block is purely the relayed quarantine."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    # A fast reconcile sweep so the worker-derived quarantined set turns over quickly.
    with advisory_server(
        tmp_path_factory, pypiron_bin, feed, extra_args=["--reconcile-interval-secs", "2"]
    ) as server:
        base = server["base_url"]
        assert _poll_metric(base, "pypiron_advisory_snapshot_age_seconds") is not None

        pkg = "quarantinee"
        wheel = make_wheel(pkg, "1.0.0", tmp_path)
        _mirror_upload(server, wheel)
        wait_for_file_in_index(server["simple"], pkg, wheel.name)

        file_url = f"{base}/files/{pkg}/{wheel.name}"
        # Before quarantine the gate serves the file (no advisory, no quarantine).
        assert _poll_http_status(file_url, {200, 302}), "clean mirror file was not served"

        before = _metric_value(_metrics_text(base), "pypiron_blocked_downloads_total")
        assert before is not None

        # Relay a PEP 792 quarantine via the admin endpoint (as a sync would).
        status_url = f"{base}/project/{pkg}/status"
        admin = {"username": server["admin_user"], "password": server["admin_password"]}
        code, _, _ = http_request_auth(
            "POST", status_url, data=b'{"status":"quarantined"}', **admin
        )
        assert code == 200, code

        # The listing empties immediately at render time (existing PEP 792 behavior).
        doc = _wait_index_empty(server["simple"], pkg)
        assert doc["project-status"]["status"] == "quarantined"
        assert doc["files"] == []

        # The byte gate blocks after the next sweep derives the quarantined set.
        blocked = _poll_http_status(file_url, {403})
        assert blocked is not None, "quarantine never reached the byte gate within the bound"
        assert b"quarantined" in blocked[1], blocked
        assert _poll_blocked_at_least(base, before + 1), "tripwire metric never moved"

        # Clearing the status dequarantines: the gate serves the file again once the
        # derived set drops it (dequarantine must propagate, not just quarantine).
        code, _, _ = http_request_auth("DELETE", status_url, **admin)
        assert code == 200, code
        assert _poll_http_status(file_url, {200, 302}), (
            "dequarantine never reached the byte gate within the bound"
        )


def test_quarantine_enforced_when_malware_block_is_off(tmp_path_factory, pypiron_bin, tmp_path):
    """`--malware-block=false` disables OSV MAL-* byte blocking but NOT PEP 792
    quarantine refusal — the two are independent guarantees. A MAL-flagged mirror
    file serves (malware blocking is off), yet a quarantined project's file is
    still 403'd once the sweep derives the set. Regression for gap 5: one knob no
    longer silently disables the other."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    with advisory_server(
        tmp_path_factory,
        pypiron_bin,
        feed,
        extra_args=["--malware-block", "false", "--reconcile-interval-secs", "2"],
    ) as server:
        base = server["base_url"]
        admin = {"username": server["admin_user"], "password": server["admin_password"]}

        # A MAL-flagged mirror file: with malware blocking off it must serve, and
        # its listing is not scrubbed either (scrub is OSV blocking, also off).
        mal = make_wheel(MAL_EXACT_PKG, MAL_EXACT_VERSION, tmp_path)
        _mirror_upload(server, mal)
        wait_for_file_in_index(server["simple"], MAL_EXACT_PKG, mal.name)
        mal_url = f"{base}/files/{MAL_EXACT_PKG}/{mal.name}"
        assert _poll_http_status(mal_url, {200, 302}), (
            "a MAL file must serve when --malware-block=false"
        )

        # A separate non-MAL project, refused purely by relayed PEP 792 quarantine.
        pkg = "frozenmate"
        wheel = make_wheel(pkg, "1.0.0", tmp_path)
        _mirror_upload(server, wheel)
        wait_for_file_in_index(server["simple"], pkg, wheel.name)
        file_url = f"{base}/files/{pkg}/{wheel.name}"
        assert _poll_http_status(file_url, {200, 302}), "clean mirror file was not served"

        code, _, _ = http_request_auth(
            "POST", f"{base}/project/{pkg}/status", data=b'{"status":"quarantined"}', **admin
        )
        assert code == 200, code

        blocked = _poll_http_status(file_url, {403})
        assert blocked is not None, "quarantine not enforced with --malware-block=false"
        assert b"quarantined" in blocked[1], blocked

        # The MAL file still serves throughout — malware blocking stayed off.
        assert _poll_http_status(mal_url, {200, 302}), "the MAL file must still serve"


# ------------------------------ sync relay (rung 6) --------------------------
#
# `pypiron sync` ferries the advisory snapshot across the air gap: fetch the feed
# (a local zip, a URL, or — the default — the source server's own snapshot) and
# push it to the destination's admin PUT /advisories/feed, etag-conditioned. On by
# default; `""` opts out; a feed-less source/dest warns and the package sync
# proceeds. AC10's blocking half is proven here (audit rows are rung 7).


def _dest_dict(base: str, data_dir: Path) -> dict:
    """A `sync_to`-shaped server dict for a `_hand_start` destination (which
    returns a bare (proc, base, log) tuple, not a fixture dict)."""
    return {
        "base_url": base,
        "simple": f"{base}/simple/",
        "legacy": f"{base}/legacy/",
        # Both credential shapes: sync_to reads admin_user, but eagerly evaluates
        # server["user"] as its .get default, so the plain keys must exist too.
        "user": "admin",
        "password": "secret",
        "admin_user": "admin",
        "admin_password": "secret",
        "data_dir": data_dir,
    }


def _advisory_sync(pypiron_bin, source, dest, pkg, *extra, advisory_feed=None):
    """Run `pypiron sync` source→dest for one package. The wheels are seeded moments
    before, so the default 7-day cooldown is disabled (as the reconcile tests do),
    and info logs are pinned so the skip/warn assertions don't depend on ambient
    RUST_LOG. `advisory_feed=None` passes no flag (the default source-relay); any
    other value (including `""`, the opt-out) is passed as `--advisory-feed`."""
    args = ["--include-package", pkg, "--exclude-newer", ""]
    if advisory_feed is not None:
        args += ["--advisory-feed", str(advisory_feed)]
    return sync_to(
        pypiron_bin,
        dest,
        *args,
        *extra,
        source=source["base_url"],
        env={**os.environ, "RUST_LOG": "info,pypiron=info"},
    )


def test_ac10_sync_relays_snapshot_to_air_gapped_dest(
    tmp_path_factory, pypiron_bin, tmp_path, uv_path, uv_venv
):
    """AC10 core: a dest with no --advisory-feed and no outbound network (dead
    proxy) receives the snapshot from `sync --advisory-feed <local zip>`, then blocks
    the synced MAL artifact — direct GET 403 with the id, pinned install fails, and
    the tripwire moves. The snapshot lands verbatim under the reserved prefix."""
    fixture = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    src_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    source = next(src_gen)
    dproc, dbase, _dlog = _hand_start(
        pypiron_bin, tmp_path / "dest", [], extra_env=_dead_proxy_env()
    )
    dest = _dest_dict(dbase, tmp_path / "dest")
    try:
        bad = make_wheel(MAL_EXACT_PKG, MAL_EXACT_VERSION, tmp_path)
        _mirror_upload(source, bad)
        wait_for_file_in_index(source["simple"], MAL_EXACT_PKG, bad.name)

        rc, out, err = _advisory_sync(
            pypiron_bin, source, dest, MAL_EXACT_PKG, advisory_feed=fixture
        )
        assert rc == 0, f"sync failed:\n{out}\n{err}"

        # The snapshot landed verbatim under the dest's reserved prefix.
        stored = _stored_feed_path(dest)
        _wait_file_bytes(stored, fixture.read_bytes())

        # The synced MAL artifact (mirror-origin on the dest) 403s at the byte gate,
        # naming the advisory id byte-exactly.
        file_url = f"{dbase}/files/{MAL_EXACT_PKG}/{bad.name}"
        blocked = _poll_http_status(file_url, {403})
        assert blocked is not None, "MAL artifact never blocked after the relayed snapshot"
        assert MAL_EXACT_ID.encode() in blocked[1], blocked
        assert _poll_blocked_at_least(dbase, 1), "tripwire metric never moved on the dest"

        # The pinned install of the blocked version fails (uv can't fetch the wheel).
        rc, out, err = _uv_install(uv_path, uv_venv, dest, f"{MAL_EXACT_PKG}=={MAL_EXACT_VERSION}")
        assert rc != 0, f"install of relayed-blocked malware succeeded:\n{out}\n{err}"
    finally:
        kill_process_tree(dproc)
        src_gen.close()


def test_ac10_unchanged_second_run_transfers_no_body(tmp_path_factory, pypiron_bin, tmp_path):
    """AC10 (etag-conditioned): a second identical sync with an unchanged feed logs
    the skip line and does not rewrite the dest's stored snapshot (the HEAD ETag
    matches, so no body moves)."""
    fixture = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    src_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    source = next(src_gen)
    dproc, dbase, _dlog = _hand_start(
        pypiron_bin, tmp_path / "dest", [], extra_env=_dead_proxy_env()
    )
    dest = _dest_dict(dbase, tmp_path / "dest")
    try:
        clean = make_wheel("relayok", "1.0.0", tmp_path)
        _mirror_upload(source, clean)
        wait_for_file_in_index(source["simple"], "relayok", clean.name)

        rc, out, err = _advisory_sync(pypiron_bin, source, dest, "relayok", advisory_feed=fixture)
        assert rc == 0, f"first sync failed:\n{out}\n{err}"
        stored = _stored_feed_path(dest)
        _wait_file_bytes(stored, fixture.read_bytes())
        mtime_before = stored.stat().st_mtime_ns

        # The etag-conditioned skip relies on a stable, quoted HEAD ETag — the
        # served-from-memory fast path must not read+re-hash the 32 MB zip nor drift
        # the ETag across polls. HEAD sends no body; GET agrees on the same ETag.
        feed_url = f"{dbase}/advisories/feed"
        code, hbody, hhdr = http_head(feed_url)
        assert code == 200, (code, hbody)
        etag = hhdr.get("etag")
        assert etag and etag.startswith('"') and etag.endswith('"'), f"unquoted HEAD ETag: {etag!r}"
        assert hbody == b"", "HEAD returned a body"
        assert http_head(feed_url)[2].get("etag") == etag, "HEAD ETag drifted across polls"
        assert http_get(feed_url)[2].get("etag") == etag, "GET and HEAD ETags disagree"

        rc, out, err = _advisory_sync(pypiron_bin, source, dest, "relayok", advisory_feed=fixture)
        assert rc == 0, f"second sync failed:\n{out}\n{err}"
        assert "advisory feed unchanged; skipping push" in (out + err), out + err
        assert stored.stat().st_mtime_ns == mtime_before, "an unchanged feed rewrote the snapshot"
    finally:
        kill_process_tree(dproc)
        src_gen.close()


def test_ac10_restart_blocks_from_relayed_snapshot(tmp_path_factory, pypiron_bin, tmp_path):
    """AC10 (restart): after a relayed snapshot exists, respawning the dest with an
    explicit --malware-block and the source still unreachable starts cleanly from the
    stored snapshot alone and still 403s the MAL artifact."""
    fixture = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    env = _dead_proxy_env()
    src_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    source = next(src_gen)
    ddata = tmp_path / "dest"
    dproc, dbase, _dlog = _hand_start(pypiron_bin, ddata, [], extra_env=env)
    dest = _dest_dict(dbase, ddata)
    bad = make_wheel(MAL_EXACT_PKG, MAL_EXACT_VERSION, tmp_path)
    try:
        _mirror_upload(source, bad)
        wait_for_file_in_index(source["simple"], MAL_EXACT_PKG, bad.name)
        rc, out, err = _advisory_sync(
            pypiron_bin, source, dest, MAL_EXACT_PKG, advisory_feed=fixture
        )
        assert rc == 0, f"sync failed:\n{out}\n{err}"
        _wait_file_bytes(_stored_feed_path(dest), fixture.read_bytes())
        assert _poll_http_status(f"{dbase}/files/{MAL_EXACT_PKG}/{bad.name}", {403}) is not None
    finally:
        kill_process_tree(dproc)

    # Respawn on the SAME data dir with an explicit demand to block and the source
    # still unreachable: the stored (relayed) snapshot satisfies fail-closed.
    dproc, dbase, _dlog = _hand_start(
        pypiron_bin, ddata, ["--malware-block", "true"], extra_env=env
    )
    try:
        blocked = _poll_http_status(f"{dbase}/files/{MAL_EXACT_PKG}/{bad.name}", {403})
        assert blocked is not None, "restart did not block from the stored relayed snapshot"
        assert MAL_EXACT_ID.encode() in blocked[1], blocked
    finally:
        kill_process_tree(dproc)
        src_gen.close()


def test_ac10_default_on_relay_from_source(tmp_path_factory, pypiron_bin, tmp_path):
    """On by default: a source armed with the fixture via admin PUT, a plain sync
    with NO advisory flag, and the unfed dest receives the snapshot (relayed from the
    source) and blocks the synced MAL artifact — the "on by default" proof."""
    fixture_bytes = make_osv_zip(tmp_path / "osv.zip", canonical_records()).read_bytes()
    src_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    source = next(src_gen)
    dproc, dbase, _dlog = _hand_start(
        pypiron_bin, tmp_path / "dest", [], extra_env=_dead_proxy_env()
    )
    dest = _dest_dict(dbase, tmp_path / "dest")
    try:
        # Arm the SOURCE with the fixture via admin PUT. Its own advisory feature is
        # off (the shared test env sets PYPIRON_ADVISORY_FEED=""), but the push/pull
        # endpoints work regardless — GET serves whatever PUT stored.
        code, _, _ = http_request_auth(
            "PUT",
            f"{source['base_url']}/advisories/feed",
            username=source["admin_user"],
            password=source["admin_password"],
            data=fixture_bytes,
        )
        assert code == 204, code
        gcode, gbody, _ = http_get(f"{source['base_url']}/advisories/feed")
        assert gcode == 200 and gbody == fixture_bytes, (gcode, len(gbody))

        bad = make_wheel(MAL_EXACT_PKG, MAL_EXACT_VERSION, tmp_path)
        _mirror_upload(source, bad)
        wait_for_file_in_index(source["simple"], MAL_EXACT_PKG, bad.name)

        # Plain sync, NO --advisory-feed flag → the default source-relay fires.
        rc, out, err = _advisory_sync(pypiron_bin, source, dest, MAL_EXACT_PKG)
        assert rc == 0, f"sync failed:\n{out}\n{err}"

        _wait_file_bytes(_stored_feed_path(dest), fixture_bytes)
        blocked = _poll_http_status(f"{dbase}/files/{MAL_EXACT_PKG}/{bad.name}", {403})
        assert blocked is not None, "the default relay did not deliver a blocking snapshot"
        assert MAL_EXACT_ID.encode() in blocked[1], blocked
    finally:
        kill_process_tree(dproc)
        src_gen.close()


def test_ac10_opt_out_and_feedless_source_are_tolerant(tmp_path_factory, pypiron_bin, tmp_path):
    """`--advisory-feed ""` issues no advisory requests (no `_advisories/` appears on
    the dest), and the default relay against a feed-less source warns but still
    completes the package sync (the package lands on the dest)."""
    src_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    source = next(src_gen)  # feature off, no snapshot → a feed-less source
    dst_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    dest = next(dst_gen)  # feature off → never writes `_advisories/` on its own
    try:
        clean = make_wheel("okpkg", "1.0.0", tmp_path)
        _mirror_upload(source, clean)
        wait_for_file_in_index(source["simple"], "okpkg", clean.name)

        # Opt-out: `--advisory-feed ""` → no advisory requests at all.
        rc, out, err = _advisory_sync(pypiron_bin, source, dest, "okpkg", advisory_feed="")
        assert rc == 0, f"opt-out sync failed:\n{out}\n{err}"
        wait_for_file_in_index(dest["simple"], "okpkg", clean.name)
        assert not (Path(dest["data_dir"]) / "_advisories").exists(), "opt-out delivered a feed"

        # Default relay against a feed-less source: it warns, but the package sync
        # completes. --full re-processes the (unchanged) source rather than 304'ing.
        rc, out, err = _advisory_sync(pypiron_bin, source, dest, "okpkg", "--full")
        assert rc == 0, f"default-relay sync failed:\n{out}\n{err}"
        assert "source has no advisory feed" in (out + err), out + err
        assert not (Path(dest["data_dir"]) / "_advisories").exists(), (
            "feed-less source delivered a feed"
        )
        names = {f["filename"] for f in get_index_json(dest["simple"], "okpkg")["files"]}
        assert clean.name in names, f"package missing after tolerant relay: {names}"
    finally:
        dst_gen.close()
        src_gen.close()


# ------------------------------ audit report + panel (rungs 7-8) -------------
#
# The org audit materializes `_advisories/report.json` (walked inventory ×
# advisory index × 30-day counters), served admin-gated at `/audit` (HTML) and
# `/audit.json`. A per-project advisory panel on `/project/<pkg>/` reads the same
# index. Vulnerabilities are informational (`blocked=false`); malware is blocked.


def _row_for(rows: list, package: str, version: str):
    """The report row for (package, version), or None."""
    for row in rows:
        if row["package"] == package and row["version"] == version:
            return row
    return None


def _poll_audit_rows(server, predicate, *, timeout: float = 40.0) -> dict:
    """Poll `/audit.json` (admin creds) until `predicate(rows)` holds; return the
    parsed report. Bounded — the report turns over on the leader sweep, not the
    request, and a download row additionally waits on the counter flush."""
    url = f"{server['base_url']}/audit.json"
    admin = {"username": server["admin_user"], "password": server["admin_password"]}
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        code, body, _ = http_request_auth("GET", url, **admin)
        if code == 200:
            last = json.loads(body)
            if predicate(last.get("rows", [])):
                return last
        time.sleep(0.2)
    raise AssertionError(
        f"/audit.json never satisfied the predicate within {timeout}s; last={last}"
    )


def test_ac8_audit_report_lists_seeded_rows_with_downloads(
    tmp_path_factory, pypiron_bin, tmp_path, uv_path, uv_venv
):
    """AC8: `/audit.json` (admin) lists exactly the seeded vulnerable/malicious rows
    — ids, fixed-in, download counts after a real install, a `blocked` flag true
    only for malware — while a private same-named package is excluded. A valid
    lower-role credential is insufficient (403); no credential is a 401. `/audit`
    HTML shows the same ids. Folds in the rung-8 project panel (shared seeds): the
    vulnerable package's page shows its advisory id, the private same-named
    package's page does not (origin exclusivity)."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    # Fast reconcile so the leader sweep rebuilds the report quickly; fast counters
    # so a download flushes within a second (see disk_server_fast_counters).
    fast = ["--reconcile-interval-secs", "2", "--counters-flush-interval-secs", "1"]
    with advisory_server(tmp_path_factory, pypiron_bin, feed, extra_args=fast) as server:
        base = server["base_url"]
        assert _poll_metric(base, "pypiron_advisory_snapshot_age_seconds") is not None

        # Seed: a mirror-origin vulnerable (non-MAL, installable) release, a
        # mirror-origin MAL release, and a PRIVATE package sharing a MAL name.
        vuln = make_wheel(PYSEC_PKG, "2.0.0", tmp_path)  # < 2.4.0 → vulnerable
        malware = make_wheel(MAL_EXACT_PKG, MAL_EXACT_VERSION, tmp_path)
        private = make_wheel(MAL_ALL_PKG, "1.0.0", tmp_path)
        _mirror_upload(server, vuln)
        _mirror_upload(server, malware)
        upload_legacy(
            server["legacy"],
            private,
            username=server["uploader_user"],
            password=server["uploader_password"],
        )
        # The vulnerable and private files list (only MAL-blocked files are
        # scrubbed); the MAL file is cached but scrubbed — the audit walk still
        # sees its bytes in packages/.
        wait_for_file_in_index(server["simple"], PYSEC_PKG, vuln.name)
        wait_for_file_in_index(server["simple"], MAL_ALL_PKG, private.name)

        # A real install of the vulnerable release drives its download counter.
        rc, out, err = _uv_install(uv_path, uv_venv, server, f"{PYSEC_PKG}==2.0.0")
        assert rc == 0, f"vulnerable (non-MAL) release failed to install:\n{out}\n{err}"
        run_checked([str(uv_venv), "-c", f"import {_import_name(PYSEC_PKG)}"])

        # Poll until the counter flush + a sweep both land: both seeded rows present
        # and the PYSEC row carries the recorded download.
        def seeded(rows):
            pysec = _row_for(rows, PYSEC_PKG, "2.0.0")
            mal = _row_for(rows, MAL_EXACT_PKG, MAL_EXACT_VERSION)
            return pysec is not None and pysec["downloads_30d"] >= 1 and mal is not None

        rows = _poll_audit_rows(server, seeded)["rows"]

        # Exactly the seeded expectations: the PYSEC row and the MAL row, no more.
        assert len(rows) == 2, f"unexpected audit row set: {rows}"
        pysec = _row_for(rows, PYSEC_PKG, "2.0.0")
        assert pysec["advisories"] == [PYSEC_ID], pysec
        assert pysec["fixed_in"] == [PYSEC_FIXED_IN], pysec
        assert pysec["blocked"] is False, "a vulnerability is informational, never blocked"
        assert pysec["origin"] == "mirror", pysec
        mal = _row_for(rows, MAL_EXACT_PKG, MAL_EXACT_VERSION)
        assert mal["advisories"] == [MAL_EXACT_ID], mal
        assert mal["blocked"] is True, "a MAL match must be flagged blocked"
        # The private same-named package never appears (origin exclusivity).
        assert all(row["package"] != MAL_ALL_PKG for row in rows), f"private row leaked: {rows}"

        # Auth: a valid uploader is understood but insufficient (403); none is 401.
        code, _, _ = http_request_auth(
            "GET",
            f"{base}/audit.json",
            username=server["uploader_user"],
            password=server["uploader_password"],
        )
        assert code == 403, "a valid lower-role credential must not read the audit"
        code, _, _ = http_get(f"{base}/audit.json")
        assert code == 401

        # `/audit` HTML (admin) renders the same advisory ids.
        code, html, _ = http_request_auth(
            "GET", f"{base}/audit", username=server["admin_user"], password=server["admin_password"]
        )
        assert code == 200, code
        text = html.decode()
        assert PYSEC_ID in text and MAL_EXACT_ID in text, "audit HTML missing advisory ids"

        # Rung-8 panel: the vulnerable package's project page shows its advisory id;
        # the private same-named package's page does not.
        code, body, _ = http_get(f"{base}/project/{PYSEC_PKG}/")
        assert code == 200, code
        assert PYSEC_ID in body.decode(), "project panel missing the advisory id"
        code, body, _ = http_get(f"{base}/project/{MAL_ALL_PKG}/")
        assert code == 200, code
        assert MAL_ALL_ID not in body.decode(), "private package's page showed a MAL advisory"


def test_quarantine_badges_the_project_panel(tmp_path_factory, pypiron_bin, tmp_path):
    """The project-panel `blocked` badge rolls in quarantine too, mirroring the
    /audit report — a badge is what the byte gate 403s. An informational
    vulnerability starts unbadged; once the project is quarantined and the worker
    derives the set, the same row is badged."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    with advisory_server(
        tmp_path_factory, pypiron_bin, feed, extra_args=["--reconcile-interval-secs", "2"]
    ) as server:
        base = server["base_url"]
        assert _poll_metric(base, "pypiron_advisory_snapshot_age_seconds") is not None
        # A mirror-origin release the PYSEC advisory matches (2.0.0 < 2.4.0).
        wheel = make_wheel(PYSEC_PKG, "2.0.0", tmp_path)
        _mirror_upload(server, wheel)
        wait_for_file_in_index(server["simple"], PYSEC_PKG, wheel.name)

        page = f"{base}/project/{PYSEC_PKG}/"
        # The rendered badge, not the always-present CSS class `.aud-blocked{...}`.
        badge = '<span class="aud-blocked">blocked</span>'
        code, body, _ = http_get(page)
        assert code == 200 and PYSEC_ID in body.decode(), (code, body)
        assert badge not in body.decode(), "an unquarantined vulnerability was badged blocked"

        # Relay a PEP 792 quarantine; after the sweep derives the set the row is badged.
        admin = {"username": server["admin_user"], "password": server["admin_password"]}
        code, _, _ = http_request_auth(
            "POST", f"{base}/project/{PYSEC_PKG}/status", data=b'{"status":"quarantined"}', **admin
        )
        assert code == 200, code
        assert _poll_page_contains(page, badge), "quarantine never badged the project panel"


def test_ac9_audit_id_byte_equals_osv_record(tmp_path_factory, pypiron_bin, tmp_path):
    """AC9: the advisory id in the `/audit.json` PYSEC row is byte-identical to the
    id in the seeded OSV record — the parser never rewrites identity."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    with advisory_server(
        tmp_path_factory, pypiron_bin, feed, extra_args=["--reconcile-interval-secs", "2"]
    ) as server:
        assert _poll_metric(server["base_url"], "pypiron_advisory_snapshot_age_seconds") is not None
        vuln = make_wheel(PYSEC_PKG, "2.0.0", tmp_path)
        _mirror_upload(server, vuln)
        wait_for_file_in_index(server["simple"], PYSEC_PKG, vuln.name)

        report = _poll_audit_rows(
            server, lambda rows: _row_for(rows, PYSEC_PKG, "2.0.0") is not None
        )
        row = _row_for(report["rows"], PYSEC_PKG, "2.0.0")
        expected_id = canonical_records()[PYSEC_ID]["id"]
        assert row["advisories"] == [expected_id], (row, expected_id)


def test_audit_endpoints_are_admin_gated(tmp_path_factory, pypiron_bin, tmp_path):
    """The audit surfaces ride the strongest credential (an org's ranked vuln list
    is attacker recon): admin reads (200), a valid lower-role credential (reader or
    uploader) is understood but insufficient (403), and no credential is a 401 with
    the auth challenge. A read-gated server so a literal reader credential exists."""
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    read_auth = ["--read-user", "reader", "--read-pass", "readersecret"]
    with advisory_server(tmp_path_factory, pypiron_bin, feed, extra_args=read_auth) as server:
        base = server["base_url"]
        assert _poll_metric(base, "pypiron_advisory_snapshot_age_seconds") is not None
        for path in ("/audit.json", "/audit"):
            url = f"{base}{path}"
            code, _, _ = http_request_auth("GET", url, username="admin", password="secret")
            assert code == 200, (path, code)
            # A valid reader credential exercises the read-credential branch → 403.
            code, _, _ = http_request_auth("GET", url, username="reader", password="readersecret")
            assert code == 403, (path, code)
            # A valid uploader credential likewise → 403.
            code, _, _ = http_request_auth(
                "GET", url, username="uploader", password="uploadersecret"
            )
            assert code == 403, (path, code)
            # No credential → 401 with the WWW-Authenticate challenge.
            code, _, headers = http_get(url)
            assert code == 401, (path, code)
            assert headers.get("www-authenticate") == 'Basic realm="PypIron"', path


def test_absent_report_is_empty_with_note_not_404(tmp_path_factory, pypiron_bin, tmp_path):
    """Before any report is materialized, `/audit.json` (admin) is an empty-rows body
    carrying a clear note — never a 404 (the endpoint always exists)."""
    # A dead-proxy default server: armed but unfed, so no snapshot and no report.
    proc, base, log = _hand_start(pypiron_bin, tmp_path / "data", [], extra_env=_dead_proxy_env())
    try:
        _wait_log_contains(log, "armed but unfed")
        code, body, _ = http_request_auth(
            "GET", f"{base}/audit.json", username="admin", password="secret"
        )
        assert code == 200, code
        doc = json.loads(body)
        assert doc["rows"] == []
        assert doc["note"] == "no advisory snapshot loaded yet", doc
        # The HTML surface renders the note as a banner, not a 404.
        code, html, _ = http_request_auth(
            "GET", f"{base}/audit", username="admin", password="secret"
        )
        assert code == 200, code
        assert "no advisory snapshot loaded yet" in html.decode()
    finally:
        kill_process_tree(proc)


def test_ac10_audit_rows_on_relayed_dest(tmp_path_factory, pypiron_bin, tmp_path):
    """AC10 (audit half): after `sync --advisory-feed <zip>` relays the snapshot and
    the MAL artifact to an air-gapped dest, the dest's own leader sweep materializes
    the report — poll dest `/audit.json` (admin) until the synced MAL row appears
    with `blocked=true`."""
    fixture = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    src_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    source = next(src_gen)
    dproc, dbase, _dlog = _hand_start(
        pypiron_bin,
        tmp_path / "dest",
        ["--reconcile-interval-secs", "2"],
        extra_env=_dead_proxy_env(),
    )
    dest = _dest_dict(dbase, tmp_path / "dest")
    try:
        bad = make_wheel(MAL_EXACT_PKG, MAL_EXACT_VERSION, tmp_path)
        _mirror_upload(source, bad)
        wait_for_file_in_index(source["simple"], MAL_EXACT_PKG, bad.name)

        rc, out, err = _advisory_sync(
            pypiron_bin, source, dest, MAL_EXACT_PKG, advisory_feed=fixture
        )
        assert rc == 0, f"sync failed:\n{out}\n{err}"
        _wait_file_bytes(_stored_feed_path(dest), fixture.read_bytes())

        report = _poll_audit_rows(
            dest, lambda rows: _row_for(rows, MAL_EXACT_PKG, MAL_EXACT_VERSION) is not None
        )
        row = _row_for(report["rows"], MAL_EXACT_PKG, MAL_EXACT_VERSION)
        assert row["advisories"] == [MAL_EXACT_ID], row
        assert row["blocked"] is True, row
        assert row["origin"] == "mirror", row
    finally:
        kill_process_tree(dproc)
        src_gen.close()


# ------------------------------- test helpers --------------------------------


def _poll_http_status(url: str, accept: set, *, timeout: float = 30.0):
    """Poll GET(url) until its status is in `accept`; return (code, body) or None.
    The quarantined set is worker-derived, so the gate turns over on the sweep, not
    the request — a bounded poll, never a blind sleep."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        code, body, _ = http_get(url)
        if code in accept:
            return code, body
        time.sleep(0.2)
    return None


def _poll_page_contains(url: str, needle: str, *, timeout: float = 30.0) -> bool:
    """Poll GET(url) until `needle` is in the rendered body (worker-derived state
    plus the 1s project-page cache both settle within the bound)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        code, body, _ = http_get(url)
        if code == 200 and needle in body.decode(errors="replace"):
            return True
        time.sleep(0.2)
    return False


def _wait_index_empty(simple_url: str, pkg: str, *, timeout: float = 30.0) -> dict:
    """Poll `pkg`'s PEP 691 index until quarantine has emptied its file list."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        doc = get_index_json(simple_url, pkg)
        if doc.get("files") == [] and doc.get("project-status", {}).get("status") == "quarantined":
            return doc
        time.sleep(0.2)
    raise AssertionError(f"index for {pkg} never emptied under quarantine within {timeout}s")


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


# --------------------------- enforcement helpers -----------------------------


def _mirror_upload(server, dist: Path) -> None:
    """Publish `dist` as a mirror-origin file (admin cred + mirror field), the way
    a sync would — the origin a byte-gate block requires. Mirrors _mirror_upload
    in test_project_status.py."""
    upload_legacy(
        server["legacy"],
        dist,
        username=server["admin_user"],
        password=server["admin_password"],
        fields={"mirror": "true"},
    )


def _import_name(name: str) -> str:
    """The importable module `make_wheel` bakes into a wheel for `name`."""
    return re.sub(r"\W+", "_", name).strip("_").lower()


def _uv_install(uv_path, venv: Path, server, spec: str):
    """`uv pip install <spec>` against `server`'s index, no cache. Returns
    (rc, stdout, stderr); a nonzero rc is a failed install (what a block causes)."""
    return run_returncode(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(venv),
            "--index-url",
            server["simple"],
            "--no-cache",
            spec,
        ],
        timeout=180,
    )


def _metrics_text(base_url: str) -> str:
    code, body, _ = http_get(f"{base_url}/metrics")
    assert code == 200, code
    return body.decode()


def _poll_blocked_at_least(base_url: str, threshold: float, *, timeout: float = 10.0) -> bool:
    """The blocked-downloads counter updates in-handler before the 403 returns, but
    /metrics is a separate request — poll briefly to avoid any read ordering flake."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = _metric_value(_metrics_text(base_url), "pypiron_blocked_downloads_total")
        if value is not None and value >= threshold:
            return True
        time.sleep(0.1)
    return False


@contextmanager
def _advisory_proxy_pair(tmp_path_factory, pypiron_bin, feed):
    """A proxy pair whose PROXY side is fed the advisory zip (the --advisory-feed
    CLI flag overrides the conftest hermetic disable). Reuses _start_proxy_pair so
    the upstream/cooldown wiring stays identical to the shared fixtures."""
    gen = _start_proxy_pair(
        tmp_path_factory, pypiron_bin, proxy_extra_args=["--advisory-feed", str(feed)]
    )
    pair = next(gen)
    try:
        yield pair
    finally:
        gen.close()


class _FeedHandler(http.server.BaseHTTPRequestHandler):
    """Serves a fixed advisory zip with an ETag, honoring If-None-Match — the OSV
    export stand-in for AC6, so the source can be killed mid-flight."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # noqa: D102 - silence default stderr spam
        pass

    def do_GET(self):  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        payload = self.server.payload  # type: ignore[attr-defined]
        etag = self.server.etag  # type: ignore[attr-defined]
        if self.headers.get("If-None-Match") == etag:
            self.send_response(304)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/zip")
        self.send_header("ETag", etag)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


@contextmanager
def _local_feed_server(payload: bytes):
    """Serve `payload` as the advisory zip over loopback HTTP. Yields
    (feed_url, stop) where `stop()` shuts the source down so a test can observe
    pypiron degrading gracefully once the feed dies."""
    port = find_free_port()
    httpd = http.server.HTTPServer(("127.0.0.1", port), _FeedHandler)
    httpd.payload = payload  # type: ignore[attr-defined]
    httpd.etag = '"osv-fixture"'  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    stopped = {"done": False}

    def stop():
        if not stopped["done"]:
            httpd.shutdown()
            httpd.server_close()
            stopped["done"] = True

    try:
        yield f"http://127.0.0.1:{port}/osv.zip", stop
    finally:
        stop()
        thread.join(timeout=5)


# ------------------------------ malware probe --------------------------------
#
# Each node polls OSV's per-advisory feed (modified_id.csv + <ID>.json, siblings
# of all.zip) to block a newly-published MAL within minutes, ahead of the daily
# snapshot. These serve a MUTABLE fake OSV export from loopback so a test can
# publish/withdraw an advisory after startup and watch the byte gate follow —
# asserting on the fixture's request log that the daily zip was never re-fetched.


class _MutableFeedState:
    """The fake OSV export's shared state: a fixed baseline zip, a mutable set of
    per-advisory records (drives modified_id.csv + <ID>.json), and a request log."""

    def __init__(self, zip_bytes: bytes, baseline_records: dict):
        self.lock = threading.Lock()
        self.zip_bytes = zip_bytes
        self.records = dict(baseline_records)
        self.csv_version = 0
        self.log: list[tuple[str, str]] = []

    def csv_body(self) -> bytes:
        rows = sorted(self.records.values(), key=lambda r: r["modified"], reverse=True)
        return "".join(f"{r['modified']},{r['id']}\n" for r in rows).encode()

    def csv_etag(self) -> str:
        return f'"csv-{self.csv_version}"'


class _ProbeFeedHandler(http.server.BaseHTTPRequestHandler):
    """Serves all.zip, modified_id.csv (ETag-conditional), and <ID>.json, logging
    every request so a test can prove the zip wasn't re-fetched."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # noqa: D102 - silence default stderr spam
        pass

    def do_HEAD(self):  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        self._serve(head=True)

    def do_GET(self):  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        self._serve(head=False)

    def _serve(self, head: bool) -> None:
        st: _MutableFeedState = self.server.state  # type: ignore[attr-defined]
        inm = self.headers.get("If-None-Match")
        path = self.path
        with st.lock:
            st.log.append(("HEAD" if head else "GET", path))
            if path.endswith("/all.zip"):
                resp = (200, st.zip_bytes, "application/zip", '"osv-zip"')
            elif path.endswith("/modified_id.csv"):
                etag = st.csv_etag()
                resp = (200, st.csv_body(), "text/csv", etag)
            elif path.endswith(".json"):
                adv_id = path.rsplit("/", 1)[-1][: -len(".json")]
                rec = st.records.get(adv_id)
                resp = (
                    (200, json.dumps(rec).encode(), "application/json", f'"{adv_id}"')
                    if rec is not None
                    else (404, b"", "text/plain", None)
                )
            else:
                resp = (404, b"", "text/plain", None)
        status, body, ctype, etag = resp
        # A matching ETag short-circuits to 304 with no body (the common poll).
        if status == 200 and etag is not None and inm == etag:
            self.send_response(304)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        if etag is not None:
            self.send_header("ETag", etag)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head and body:
            self.wfile.write(body)


class _FeedControl:
    """Test-side handle to a running `_probe_feed_server`."""

    def __init__(self, state: _MutableFeedState):
        self._st = state

    def publish(self, record: dict) -> None:
        with self._st.lock:
            self._st.records[record["id"]] = record
            self._st.csv_version += 1

    def withdraw(self, adv_id: str, modified: str) -> None:
        with self._st.lock:
            rec = dict(self._st.records[adv_id])
            rec["withdrawn"] = modified
            rec["modified"] = modified  # a withdrawal bumps modified (reappears atop)
            self._st.records[adv_id] = rec
            self._st.csv_version += 1

    def count(self, method: str, needle: str) -> int:
        with self._st.lock:
            return sum(1 for (m, p) in self._st.log if m == method and needle in p)


@contextmanager
def _probe_feed_server(baseline_records: dict, tmp_path: Path):
    """Serve a mutable fake OSV export over loopback at `.../PyPI/all.zip`. Yields
    (feed_url, control); the baseline zip sets the probe watermark, and `control`
    publishes/withdraws advisories that surface only via the CSV + per-advisory
    JSON (never the zip)."""
    zip_bytes = make_osv_zip(tmp_path / "baseline.zip", baseline_records).read_bytes()
    st = _MutableFeedState(zip_bytes, baseline_records)
    port = find_free_port()
    httpd = http.server.HTTPServer(("127.0.0.1", port), _ProbeFeedHandler)
    httpd.state = st  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/PyPI/all.zip", _FeedControl(st)
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def _baseline_records() -> dict:
    """One dated, unrelated advisory — sets the snapshot watermark the probe
    backfills from without naming any test package."""
    return {
        "OSV-BASE-1": {
            "id": "OSV-BASE-1",
            "modified": "2024-02-01T00:00:00Z",
            "summary": "unrelated baseline advisory",
            "affected": [
                {"package": {"ecosystem": "PyPI", "name": "baseline-pkg"}, "versions": ["9.9.9"]}
            ],
        }
    }


def _mal_record(mal_id: str, pkg: str, version: str, modified: str) -> dict:
    return {
        "id": mal_id,
        "modified": modified,
        "summary": f"malware in {pkg}",
        "affected": [{"package": {"ecosystem": "PyPI", "name": pkg}, "versions": [version]}],
    }


# The forward-proxy vars are cleared so the SSRF-guarded feed client reaches
# loopback directly (as the AC6 live-feed test does).
_DIRECT_EGRESS = {"HTTP_PROXY": "", "HTTPS_PROXY": "", "ALL_PROXY": "", "NO_PROXY": "*"}


def test_probe_blocks_new_mal_ahead_of_daily_feed(tmp_path_factory, pypiron_bin, tmp_path):
    """A MAL published after startup blocks the file within a probe interval,
    without the daily zip being re-fetched (asserted on the fixture request log),
    and withdrawing it un-blocks on the next probe. ETag/304 behavior shows up as
    repeated CSV polls that transfer no advisory JSON until the CSV changes."""
    pkg, ver = "probemal", "1.0.0"
    mal_id = "MAL-2025-70001"
    with _probe_feed_server(_baseline_records(), tmp_path) as (feed_url, feed):
        with advisory_server(
            tmp_path_factory,
            pypiron_bin,
            feed_url,
            extra_args=["--malware-probe-secs", "2"],
            extra_env=_DIRECT_EGRESS,
        ) as server:
            base = server["base_url"]
            # Baseline snapshot armed and the probe has completed at least one cycle.
            assert _poll_metric(base, "pypiron_advisory_snapshot_age_seconds") is not None
            assert _poll_metric(base, "pypiron_malware_probe_age_seconds") is not None

            # A mirror-origin file no advisory yet names — served normally.
            wheel = make_wheel(pkg, ver, tmp_path)
            _mirror_upload(server, wheel)
            wait_for_file_in_index(server["simple"], pkg, wheel.name)
            file_url = f"{base}/files/{pkg}/{wheel.name}"
            assert _poll_http_status(file_url, {200, 302}), (
                "clean file not served before the advisory"
            )

            # Let the startup zip fetches settle, then baseline the request counts.
            time.sleep(1.5)
            zip_before = feed.count("GET", "/all.zip")
            csv_before = feed.count("GET", "/modified_id.csv")

            # Publish a brand-new MAL for the file, newer than the snapshot watermark.
            feed.publish(_mal_record(mal_id, pkg, ver, "2025-01-01T00:00:00Z"))

            # Within a couple of probe intervals the byte gate 403s, naming the id.
            blocked = _poll_http_status(file_url, {403}, timeout=30)
            assert blocked is not None, "the probe never blocked the newly-published MAL"
            assert mal_id.encode() in blocked[1], blocked
            assert _poll_blocked_at_least(base, 1), "tripwire metric never moved"

            # The block came from the probe, not a fresh daily snapshot: the zip was
            # not re-fetched, while the CSV was polled and the advisory JSON pulled.
            assert feed.count("GET", "/all.zip") == zip_before, "the probe re-fetched the daily zip"
            assert feed.count("GET", "/modified_id.csv") > csv_before, (
                "the probe never polled the CSV"
            )
            assert feed.count("GET", f"/{mal_id}.json") >= 1, (
                "the probe never fetched the advisory JSON"
            )

            # Withdrawing the advisory un-blocks on the next probe.
            feed.withdraw(mal_id, "2025-06-01T00:00:00Z")
            assert _poll_http_status(file_url, {200, 302}, timeout=30), (
                "withdrawal never un-blocked the file"
            )


def test_probe_backfills_new_mal_after_restart(tmp_path_factory, pypiron_bin, tmp_path):
    """The overlay is in-memory only: a MAL published while a node was down (present
    in the CSV/JSON but never the stored zip) is re-discovered and blocked by the
    probe after a restart — proving the backfill from the stored snapshot's
    watermark, with no persisted probe state."""
    pkg, ver = "backfillmal", "2.2.2"
    mal_id = "MAL-2025-70002"
    data_dir = tmp_path / "data"
    args = ["--advisory-feed", "PLACEHOLDER", "--malware-probe-secs", "2"]
    with _probe_feed_server(_baseline_records(), tmp_path) as (feed_url, feed):
        args[1] = feed_url
        # Boot 1: arm from the feed, persist the snapshot, seed a mirror-origin file.
        proc, base, _log = _hand_start(pypiron_bin, data_dir, args, extra_env=_DIRECT_EGRESS)
        dest = _dest_dict(base, data_dir)
        try:
            assert _poll_metric(base, "pypiron_advisory_snapshot_age_seconds") is not None
            assert _stored_feed_path(dest).exists(), "boot 1 never persisted the snapshot"
            wheel = make_wheel(pkg, ver, tmp_path)
            _mirror_upload(dest, wheel)
            wait_for_file_in_index(dest["simple"], pkg, wheel.name)
        finally:
            kill_process_tree(proc)

        # Publish a MAL newer than the stored snapshot — only in the CSV/JSON.
        feed.publish(_mal_record(mal_id, pkg, ver, "2025-01-01T00:00:00Z"))

        # Boot 2 on the SAME data dir: empty overlay, stored snapshot's watermark;
        # the probe backfills the MAL and the byte gate blocks.
        proc, base, _log = _hand_start(pypiron_bin, data_dir, args, extra_env=_DIRECT_EGRESS)
        try:
            file_url = f"{base}/files/{pkg}/{wheel.name}"
            blocked = _poll_http_status(file_url, {403}, timeout=30)
            assert blocked is not None, "restart did not backfill the probe overlay"
            assert mal_id.encode() in blocked[1], blocked
        finally:
            kill_process_tree(proc)
