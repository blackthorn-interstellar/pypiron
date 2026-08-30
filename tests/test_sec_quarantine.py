"""Startup arming of the byte gate, and the global index's refusal to truncate.

Two regressions from the same audit, both about a view that read as "nothing
here" when the truth was "we could not tell":

1. PEP 792 quarantine is enforced independently of `--malware-block`, but the
   quarantined set only ever reached memory through the advisory feed's refresh
   tick — which the feed toggle gated. With the feed off, a restart came up with
   an empty set and served quarantined artifacts until the next audit sweep
   (a day, by default). The set now loads before the listener binds, on every
   startup path, and reloads on every tick regardless of the feed.

2. A canonical `simple/index.json` that did not parse read as an empty package
   list while keeping its live ETag, so the next upload's conditional write won
   and published a global index holding only that upload.

Everything through the real binary over HTTP.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from .helpers import (
    http_get,
    http_request_auth,
    kill_process_tree,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
)
from .test_advisories import _dead_proxy_env, _hand_start

pytestmark = pytest.mark.integration

_QUARANTINED_KEY = Path("_advisories") / "quarantined.json"


def _poll_status(url: str, accept: set, *, timeout: float = 30.0):
    """Poll GET(url) until its status is in `accept`; return (code, body) or None."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        code, body, _ = http_get(url)
        if code in accept:
            return code, body
        time.sleep(0.2)
    return None


def _mirror_upload(base: str, dist: Path) -> None:
    """Publish `dist` as a mirror-origin file the way a sync would — the origin the
    byte gate needs to see before it will refuse the bytes."""
    upload_legacy(
        f"{base}/legacy/",
        dist,
        username="admin",
        password="secret",
        fields={"mirror": "true"},
    )


def _seed_quarantined_package(pypiron_bin, data_dir: Path, tmp_path: Path, extra_args, env):
    """First boot: publish a mirror-origin wheel, quarantine its project, and wait
    for the leader's sweep to derive and persist the quarantined set. Returns the
    artifact's URL path so a later boot can ask for the same bytes."""
    pkg = "frozenboot"
    wheel = make_wheel(pkg, "1.0.0", tmp_path)
    # A fast sweep so the derived set turns over inside the test's bound.
    proc, base, _log = _hand_start(
        pypiron_bin, data_dir, [*extra_args, "--reconcile-interval-secs", "2"], extra_env=env
    )
    try:
        _mirror_upload(base, wheel)
        wait_for_file_in_index(f"{base}/simple/", pkg, wheel.name)
        file_path = f"/files/{pkg}/{wheel.name}"
        assert _poll_status(f"{base}{file_path}", {200, 302}), "clean mirror file was not served"

        code, _, _ = http_request_auth(
            "POST",
            f"{base}/project/{pkg}/status",
            username="admin",
            password="secret",
            data=b'{"status":"quarantined"}',
        )
        assert code == 200, code

        blocked = _poll_status(f"{base}{file_path}", {403})
        assert blocked is not None, "quarantine never reached the byte gate on the first boot"
        assert b"quarantined" in blocked[1], blocked
    finally:
        kill_process_tree(proc)

    # The derived set is persisted, so a restart has something to load. It is
    # stored in an epoch-bearing envelope: the epoch is what lets every writer CAS
    # it forward and every reader refuse an older copy on failover.
    stored = json.loads((data_dir / _QUARANTINED_KEY).read_bytes())
    assert pkg in stored["quarantined"], stored
    assert stored["pypiron-epoch"] >= 1, stored
    return file_path


def _assert_blocked_on_first_request(pypiron_bin, data_dir: Path, file_path: str, extra_args, env):
    """Restart with no sweep to rescue it (`--audit-on-boot false`, default day-long
    reconcile interval) and demand a 403 on the very first request: the quarantined
    set has to be armed before the listener binds, not one worker tick later."""
    proc, base, _log = _hand_start(
        pypiron_bin, data_dir, [*extra_args, "--audit-on-boot", "false"], extra_env=env
    )
    try:
        code, body, _ = http_get(f"{base}{file_path}")
        assert code == 403, (
            f"a quarantined artifact was served after restart (status {code}); the quarantined "
            f"set was not armed before the listener bound"
        )
        assert b"quarantined" in body, body
    finally:
        kill_process_tree(proc)


def test_quarantine_survives_restart_with_the_advisory_feed_off(pypiron_bin, tmp_path):
    """`--advisory-feed ""` disables OSV blocking, not PEP 792 quarantine. A restart
    with the feed off used to come up with an empty quarantined set — no startup
    load on that path, and the reload the worker would have done was gated behind
    the feed — so a frozen project's artifact was served until the next audit."""
    data_dir = tmp_path / "data"
    off = ["--advisory-feed", ""]
    file_path = _seed_quarantined_package(pypiron_bin, data_dir, tmp_path, off, None)
    _assert_blocked_on_first_request(pypiron_bin, data_dir, file_path, off, None)


def test_quarantine_survives_restart_on_the_implicit_default(pypiron_bin, tmp_path):
    """The always-on default binds immediately and obtains the feed in the
    background, so it never loaded the quarantined set at startup either. Proxies
    point at a closed port, so the background obtain fails and the test stays
    hermetic — the quarantine block must not depend on it succeeding."""
    env = _dead_proxy_env()
    data_dir = tmp_path / "data"
    file_path = _seed_quarantined_package(pypiron_bin, data_dir, tmp_path, [], env)
    _assert_blocked_on_first_request(pypiron_bin, data_dir, file_path, [], env)


def _global_names(base: str) -> set:
    code, body, _ = http_get(f"{base}/simple/index.json")
    if code != 200:
        return set()
    try:
        doc = json.loads(body)
    except json.JSONDecodeError:
        return set()
    return {p["name"] for p in doc.get("projects", [])}


def test_unparseable_global_index_does_not_truncate_the_package_list(pypiron_bin, tmp_path):
    """A corrupt canonical `simple/index.json` must not read as "no packages". It
    used to: the parse failure returned an empty name set while keeping the live
    ETag, so the next upload's delta dedupped against nothing and published a
    global index holding only that upload. The per-package views are the repair,
    the same ones an absent body already derives from."""
    data_dir = tmp_path / "data"
    off = ["--advisory-feed", ""]
    seeded = ["alphapin", "betapin"]

    proc, base, _log = _hand_start(pypiron_bin, data_dir, off)
    try:
        for name in seeded:
            wheel = make_wheel(name, "1.0.0", tmp_path)
            _mirror_upload(base, wheel)
            wait_for_file_in_index(f"{base}/simple/", name, wheel.name)
        deadline = time.time() + 30.0
        while time.time() < deadline and not set(seeded) <= _global_names(base):
            time.sleep(0.2)
        assert set(seeded) <= _global_names(base), "the seeded packages never reached the index"
    finally:
        kill_process_tree(proc)

    # Corrupt the authority, then restart with no audit sweep to repair it — only
    # the load path can save the package list here.
    authority = data_dir / "simple" / "index.json"
    assert authority.exists(), "the canonical global index was never materialized"
    authority.write_bytes(b"{ this is not the index }")

    proc, base, _log = _hand_start(pypiron_bin, data_dir, [*off, "--audit-on-boot", "false"])
    try:
        wheel = make_wheel("gammapin", "1.0.0", tmp_path)
        _mirror_upload(base, wheel)
        wait_for_file_in_index(f"{base}/simple/", "gammapin", wheel.name)
        expected = {*seeded, "gammapin"}
        deadline = time.time() + 30.0
        while time.time() < deadline and not expected <= _global_names(base):
            time.sleep(0.2)
        assert expected <= _global_names(base), (
            f"the global index lost packages to an unparseable body: {_global_names(base)}"
        )
    finally:
        kill_process_tree(proc)
