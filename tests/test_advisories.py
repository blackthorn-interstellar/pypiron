"""Advisory snapshot pipeline + malware enforcement.

Rung 3 (snapshot plumbing): the feed loads into a live snapshot, the reader-GET /
admin-PUT `/advisories/feed` push path works, and startup is fail-closed by
intent (AC7). Rung 4 (enforcement): the malware byte gate refuses blocked bytes
where they're served (AC1), private origin exempts a same-named package (AC2),
the on-demand proxy refuses to fill a blocked version (AC3), and a dead feed
degrades freshness, never availability (AC6). Everything through the real binary
over HTTP, driven by real `uv`.
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
    http_get,
    http_request_auth,
    kill_process_tree,
    make_wheel,
    run_checked,
    run_returncode,
    upload_legacy,
    wait_for_file_in_index,
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
        wait_for_file_in_index(server["simple"], MAL_EXACT_PKG, bad.name)
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
