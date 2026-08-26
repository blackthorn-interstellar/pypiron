"""Streaming a large cold-miss proxy download while it is still arriving.

A file at or above `--proxy-stream-threshold` (16 MiB by default) starts reaching
the client at upstream speed instead of after the whole download, so a 300 MB
wheel is not minutes of silence. The safety property survives that: the last
64 KiB are withheld until pypiron's sha256 check passes, so a *complete* body is
always a verified one, and a corrupt, truncated, or hung upstream shows up as a
transfer cut short mid-body — never a whole artifact nobody checked.

These tests reuse the chaos suite's fake upstream (`_FaultServer`), which can be
made to throttle, corrupt, or truncate an artifact on demand.
"""

from __future__ import annotations

import hashlib
import http.client
import math
import re
import threading
import time
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlparse

import pytest

from .helpers import (
    http_get,
    http_request_auth,
    make_wheel,
    origin_owner,
    run_checked,
    sha256_file,
)
from .test_advisories import _wait_log_contains, make_osv_zip
from .test_chaos_upstream import (
    FAULT_GET_TIMEOUT,
    SLOW_TRANSFER_SECS,
    _FaultServer,
    _proxy_over_fault,
)

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

# Padding that puts the built wheel comfortably over the shipped 16 MiB
# threshold without making the suite pay for a 100 MB transfer.
LARGE_PAYLOAD = 18 * 1024 * 1024
# The `corrupt`/`truncate` modes make the fill burn its full retry budget
# (3 attempts, 2s + 4s backoff) after the client has already been cut off, so
# "storage is clean" can only be asserted once that has run its course.
FILL_GIVEUP_SECS = 45.0
# The mid-download tests need a transfer long enough to land an event inside it:
# push the feed, wait for the worker to load it, and still have the fill running.
MID_DOWNLOAD_PACE = 20.0
MAL_MID_ID = "MAL-2026-90001"

# --- the balanced-legs overlap measurement (see the test at the bottom) --------
# A wheel well over the threshold, so the streamed path is in play and the two
# legs are long enough to time apart from startup noise.
BALANCED_PAYLOAD = 24 * 1024 * 1024
# How long the upstream leg (pypiron <- PyPI) takes. Both legs run at this pace.
BALANCED_LEG_SECS = 3.0
# The client reads this much per pause. Coarse on purpose: ~200 sleeps of ~15ms
# each, not thousands of micro-sleeps a loaded box cannot honor.
CLIENT_CHUNK = 128 * 1024
# Overlapping the legs is ideally 2x — assert well short of that, so the test
# reports a real regression rather than the scheduler's mood.
OVERLAP_RATIO = 0.75


@pytest.fixture()
def proxy_over_fault_tee(
    tmp_path_factory, pypiron_bin: Path, tmp_path: Path
) -> Iterator[Tuple[Dict, _FaultServer]]:
    """The chaos harness with the shipped streaming threshold in force."""
    yield from _proxy_over_fault(tmp_path_factory, pypiron_bin, tmp_path)


@pytest.fixture()
def proxy_over_fault_no_tee(
    tmp_path_factory, pypiron_bin: Path, tmp_path: Path
) -> Iterator[Tuple[Dict, _FaultServer]]:
    """The same, with streaming switched off — every fill buffers."""
    yield from _proxy_over_fault(
        tmp_path_factory,
        pypiron_bin,
        tmp_path,
        extra_env={"PYPIRON_PROXY_STREAM_THRESHOLD": "off"},
    )


@pytest.fixture()
def proxy_over_fault_malware(
    tmp_path_factory, pypiron_bin: Path, tmp_path: Path
) -> Iterator[Tuple[Dict, _FaultServer]]:
    """The chaos harness with malware blocking armed off a seed OSV feed the test
    can replace at runtime by pushing a new one to `/advisories/feed`."""
    seed = make_osv_zip(
        tmp_path / "seed-osv.zip",
        {"MAL-2026-90000": _mal_record("MAL-2026-90000", "someone-elses-package")},
    )
    yield from _proxy_over_fault(
        tmp_path_factory,
        pypiron_bin,
        tmp_path,
        worker_args=("--malware-block", "true", "--advisory-feed", str(seed)),
    )


def _mal_record(adv_id: str, pkg: str) -> dict:
    """An OSV MAL advisory condemning every version of `pkg`."""
    return {
        "id": adv_id,
        "summary": f"malicious code in {pkg}",
        "affected": [
            {
                "package": {"ecosystem": "PyPI", "name": pkg},
                "ranges": [{"type": "ECOSYSTEM", "events": [{"introduced": "0"}]}],
            }
        ],
    }


def _downloads_counted(proxy: Dict) -> int:
    """`pypiron_downloads_total` — bumped at each delivery exit, so it counts a
    streamed body only once its verified tail has gone out."""
    code, body, _ = http_get(f"{proxy['base_url']}/metrics")
    assert code == 200
    m = re.search(r"^pypiron_downloads_total (\d+)$", body.decode(), re.MULTILINE)
    assert m, body.decode()
    return int(m.group(1))


def _big_wheel(pkg: str, tmp_path: Path) -> Path:
    wheel = make_wheel(pkg, "1.0", tmp_path, payload_bytes=LARGE_PAYLOAD)
    assert wheel.stat().st_size > 16 * 1024 * 1024, "test wheel is under the streaming threshold"
    return wheel


class _Read:
    """One incremental GET: what arrived, when, and whether it was complete."""

    def __init__(self, status: int, declared: Optional[int], marks: List[Tuple[float, int]]):
        self.status = status
        self.declared = declared
        self.marks = marks

    @property
    def received(self) -> int:
        return self.marks[-1][1] if self.marks else 0

    @property
    def complete(self) -> bool:
        return self.declared is not None and self.received == self.declared

    @property
    def first_byte(self) -> float:
        return self.marks[0][0] if self.marks else float("inf")

    @property
    def last_byte(self) -> float:
        return self.marks[-1][0] if self.marks else float("inf")


def _read_incrementally(url: str, *, timeout: float, headers=None) -> Tuple[_Read, bytes]:
    """GET `url`, recording when each chunk of body arrives. A body cut short of
    its Content-Length is not an error here — it is the thing under test — so the
    caller inspects `.complete` rather than catching an exception."""
    parsed = urlparse(url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=timeout)
    body = bytearray()
    marks: List[Tuple[float, int]] = []
    started = time.monotonic()
    try:
        conn.request("GET", parsed.path, headers=headers or {})
        resp = conn.getresponse()
        raw = resp.getheader("Content-Length")
        declared = int(raw) if raw is not None else None
        try:
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                body += chunk
                marks.append((time.monotonic() - started, len(body)))
        except (http.client.HTTPException, OSError):
            # A mid-body abort can surface either way depending on where the
            # connection dies; both mean the same short read.
            pass
        return _Read(resp.status, declared, marks), bytes(body)
    finally:
        conn.close()


def _client_pause(size: int) -> float:
    """The sleep per `CLIENT_CHUNK` that makes a client swallow `size` bytes in
    about `BALANCED_LEG_SECS` — the client leg throttled to the upstream's pace."""
    chunks = max(1, math.ceil(size / CLIENT_CHUNK))
    pause = BALANCED_LEG_SECS / chunks
    assert pause >= 0.010, f"client pacing is too fine-grained to be honored ({pause * 1e3:.1f}ms)"
    return pause


def _read_at_client_pace(url: str, *, pause: float, timeout: float) -> Tuple[float, bytes]:
    """GET `url` over a link that carries `CLIENT_CHUNK` bytes per `pause`, and
    return the wall-clock time to last byte. Reads block until the chunk is full,
    so this consumes at a fixed rate and never races ahead to catch up — a slow
    link, not a fast client that stalled."""
    parsed = urlparse(url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=timeout)
    body = bytearray()
    started = time.monotonic()
    try:
        conn.request("GET", parsed.path)
        resp = conn.getresponse()
        assert resp.status == 200, f"cold miss answered {resp.status}"
        while True:
            time.sleep(pause)
            piece = resp.read(CLIENT_CHUNK)
            if not piece:
                break
            body += piece
        return time.monotonic() - started, bytes(body)
    finally:
        conn.close()


def _timed_cold_miss(
    tmp_path_factory,
    pypiron_bin: Path,
    run_dir: Path,
    pkg: str,
    wheel: Path,
    *,
    pause: float,
    extra_env: Optional[Dict[str, str]] = None,
) -> Tuple[float, bytes]:
    """Time to last byte for one cold-miss download of `wheel` through a
    throttled upstream, on a server of its own."""
    run_dir.mkdir()
    gen = _proxy_over_fault(tmp_path_factory, pypiron_bin, run_dir, extra_env=extra_env)
    proxy, upstream = next(gen)
    try:
        filename = upstream.register(pkg, wheel)
        upstream.set_fault(pkg, "slow", pace=BALANCED_LEG_SECS)
        return _read_at_client_pace(
            f"{proxy['base_url']}/files/{pkg}/{filename}",
            pause=pause,
            timeout=FAULT_GET_TIMEOUT,
        )
    finally:
        gen.close()


def _wait_for_clean_storage(proxy: Dict, pkg: str, filename: str) -> None:
    """No artifact committed, no orphaned temp sibling, spool drained — after the
    fill has exhausted its retries, which outlives the aborted client."""
    pkg_dir = proxy["data_dir"] / "packages" / pkg
    deadline = time.monotonic() + FILL_GIVEUP_SECS
    while time.monotonic() < deadline and list(proxy["spool_dir"].iterdir()):
        time.sleep(0.2)
    assert not list(proxy["spool_dir"].iterdir()), (
        f"spool not drained after a failed streamed download: {list(proxy['spool_dir'].iterdir())}"
    )
    if pkg_dir.exists():
        assert not (pkg_dir / filename).exists(), (
            f"a failed upstream fetch left an artifact in storage: {filename}"
        )
        stray = [p.name for p in pkg_dir.iterdir() if p.name.startswith(".tmp")]
        assert not stray, f"orphaned temp files in {pkg_dir}: {stray}"
        # Reclamation belongs to the leader audit, so the mirror claim stays.
        assert origin_owner((pkg_dir / ".origin").read_text()) == "mirror"


# --------------------------------- tests --------------------------------------


def test_large_cold_miss_streams_while_it_downloads(proxy_over_fault_tee, tmp_path):
    """Bytes reach the client while the upstream transfer is still running, the
    complete body still verifies, and the artifact lands in the cache."""
    proxy, upstream = proxy_over_fault_tee
    pkg = "teestream"
    wheel = _big_wheel(pkg, tmp_path)
    filename = upstream.register(pkg, wheel)
    upstream.set_fault(pkg, "slow")  # the full body, paced over SLOW_TRANSFER_SECS

    counted = _downloads_counted(proxy)
    read, body = _read_incrementally(
        f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
    )
    assert read.status == 200
    assert _downloads_counted(proxy) == counted + 1, (
        "a streamed body that completed was not counted exactly once"
    )
    assert read.complete, f"streamed body was short: {read.received} of {read.declared}"
    assert hashlib.sha256(body).hexdigest() == sha256_file(wheel)
    # The discriminating assertion, and the one that survives a loaded box: with
    # a buffered fill every byte lands in one burst at the very end, so the first
    # byte's arrival is ~100% of the total. Teed, it is a small fraction of it.
    # Both terms scale together under xdist load, so this is a ratio, not a clock.
    assert read.last_byte > SLOW_TRANSFER_SECS / 2, (
        f"the throttled upstream did not actually throttle ({read.last_byte:.1f}s total)"
    )
    assert read.first_byte < read.last_byte / 2, (
        f"first byte at {read.first_byte:.1f}s of a {read.last_byte:.1f}s transfer — "
        "the response was buffered, not streamed"
    )

    # It was a cache fill, not just a passthrough: the artifact is committed and
    # the second request is served from storage without touching upstream.
    artifact = proxy["data_dir"] / "packages" / pkg / filename
    deadline = time.monotonic() + FILL_GIVEUP_SECS
    while time.monotonic() < deadline and not artifact.exists():
        time.sleep(0.1)
    assert artifact.exists(), "the streamed fill never committed to storage"
    assert sha256_file(artifact) == sha256_file(wheel)
    assert origin_owner((artifact.parent / ".origin").read_text()) == "mirror"

    fetches = upstream.hits(pkg)
    warm, warm_body = _read_incrementally(
        f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
    )
    assert warm.status == 200 and warm.complete
    assert hashlib.sha256(warm_body).hexdigest() == sha256_file(wheel)
    assert upstream.hits(pkg) == fetches, "the warm request re-downloaded a cached artifact"
    assert warm.last_byte < SLOW_TRANSFER_SECS, (
        f"the cached artifact took {warm.last_byte:.1f}s to serve"
    )


def test_hash_mismatch_never_completes_a_streamed_body(proxy_over_fault_tee, tmp_path):
    """Full-length but corrupt bytes: the client's connection is cut before the
    withheld tail, so it never holds a complete artifact, and nothing is cached."""
    proxy, upstream = proxy_over_fault_tee
    pkg = "teecorrupt"
    wheel = _big_wheel(pkg, tmp_path)
    filename = upstream.register(pkg, wheel)
    upstream.set_fault(pkg, "corrupt")

    counted = _downloads_counted(proxy)
    read, body = _read_incrementally(
        f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
    )
    assert not read.complete, (
        "a body that failed verification was delivered whole "
        f"({read.received} of {read.declared} bytes)"
    )
    assert hashlib.sha256(body).hexdigest() != sha256_file(wheel)
    # An aborted stream is not a download: the counter is bumped where the tail
    # is handed over, and no tail was.
    assert _downloads_counted(proxy) == counted, "an aborted stream was counted as a download"
    _wait_for_clean_storage(proxy, pkg, filename)

    # And the failure is not sticky: a healed upstream still caches and serves.
    upstream.heal(pkg)
    code, healed, _ = http_get(
        f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
    )
    assert code == 200
    assert hashlib.sha256(healed).hexdigest() == sha256_file(wheel)


def test_truncated_upstream_never_completes_a_streamed_body(proxy_over_fault_tee, tmp_path):
    """An upstream that stops mid-body: same shape — short read, clean storage."""
    proxy, upstream = proxy_over_fault_tee
    pkg = "teetrunc"
    wheel = _big_wheel(pkg, tmp_path)
    filename = upstream.register(pkg, wheel)
    upstream.set_fault(pkg, "truncate")

    read, body = _read_incrementally(
        f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
    )
    assert not read.complete, (
        f"a truncated upstream produced a complete body ({read.received} of {read.declared})"
    )
    assert hashlib.sha256(body).hexdigest() != sha256_file(wheel)
    _wait_for_clean_storage(proxy, pkg, filename)

    upstream.heal(pkg)
    code, healed, _ = http_get(
        f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
    )
    assert code == 200
    assert hashlib.sha256(healed).hexdigest() == sha256_file(wheel)


def test_advisory_landing_mid_download_aborts_the_stream(proxy_over_fault_malware, tmp_path):
    """A download big enough to stream is big enough to outlive the decision that
    let it start. Condemn the version while its bytes are still moving and the
    body is cut off before the withheld tail — nothing lands in the cache, and
    the client is left holding an artifact it cannot use."""
    proxy, upstream = proxy_over_fault_malware
    pkg = "teelatemal"
    wheel = _big_wheel(pkg, tmp_path)
    filename = upstream.register(pkg, wheel)
    upstream.set_fault(pkg, "slow", pace=MID_DOWNLOAD_PACE)

    result: Dict[str, object] = {}

    def _download() -> None:
        result["read"], result["body"] = _read_incrementally(
            f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
        )

    reader = threading.Thread(target=_download)
    reader.start()
    try:
        # Let the stream get going, then push a feed that condemns this version.
        time.sleep(2.0)
        feed = make_osv_zip(tmp_path / "mal-osv.zip", {MAL_MID_ID: _mal_record(MAL_MID_ID, pkg)})
        code, _, _ = http_request_auth(
            "PUT",
            f"{proxy['base_url']}/advisories/feed",
            username=proxy["admin_user"],
            password=proxy["admin_password"],
            data=feed.read_bytes(),
            timeout=30.0,
        )
        assert code == 204, f"pushing the advisory feed returned {code}"
        _wait_log_contains(Path(proxy["log_path"]), "advisory snapshot loaded")
    finally:
        reader.join(timeout=FAULT_GET_TIMEOUT + MID_DOWNLOAD_PACE)
    assert not reader.is_alive(), "the streamed download never finished"

    read = result["read"]
    assert read.status == 200, "the advisory landed after the headers, so this is a 200"
    assert not read.complete, (
        "a version condemned mid-download was delivered whole "
        f"({read.received} of {read.declared} bytes)"
    )
    assert hashlib.sha256(result["body"]).hexdigest() != sha256_file(wheel)
    _wait_for_clean_storage(proxy, pkg, filename)

    # And the refusal is durable: the retry a client would issue is a 403, not a
    # second chance at the same bytes.
    code, _, _ = http_get(f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT)
    assert code == 403, f"a blocked artifact answered {code}"


def test_threshold_off_buffers_the_whole_download(proxy_over_fault_no_tee, tmp_path):
    """`--proxy-stream-threshold off` restores the download-then-serve path: no
    early bytes, and the body still arrives whole and correct."""
    proxy, upstream = proxy_over_fault_no_tee
    pkg = "teeoff"
    wheel = _big_wheel(pkg, tmp_path)
    filename = upstream.register(pkg, wheel)
    upstream.set_fault(pkg, "slow")

    read, body = _read_incrementally(
        f"{proxy['base_url']}/files/{pkg}/{filename}", timeout=FAULT_GET_TIMEOUT
    )
    assert read.status == 200 and read.complete
    assert hashlib.sha256(body).hexdigest() == sha256_file(wheel)
    assert read.last_byte > SLOW_TRANSFER_SECS / 2, "the throttled upstream did not throttle"
    assert read.first_byte > read.last_byte / 2, (
        f"first byte at {read.first_byte:.1f}s of a {read.last_byte:.1f}s transfer — "
        "bytes were streamed early with streaming switched off"
    )


def test_balanced_legs_overlap_instead_of_adding_up(tmp_path_factory, pypiron_bin, tmp_path):
    """The point of teeing, timed: when the upstream leg and the client leg run
    at the same speed — the VPN user pulling from a distant PyPI — the download
    costs about one leg, not two. Buffering pays them in series (fetch it all,
    then send it all); streaming overlaps them, because the fill writes to the
    spool at upstream speed no matter how slowly the client drains it.

    Both measurements are made the same way against the same throttles, so the
    assertion is the ratio between them and not a clock. Both throttles are
    load-bearing for a pass: kill either one and the two runs converge."""
    pkg = "teebalance"
    wheel = make_wheel(pkg, "1.0", tmp_path, payload_bytes=BALANCED_PAYLOAD)
    size = wheel.stat().st_size
    assert size > 16 * 1024 * 1024, "test wheel is under the streaming threshold"
    pause = _client_pause(size)
    digest = sha256_file(wheel)

    buffered_secs, buffered_body = _timed_cold_miss(
        tmp_path_factory,
        pypiron_bin,
        tmp_path / "buffered",
        pkg,
        wheel,
        pause=pause,
        extra_env={"PYPIRON_PROXY_STREAM_THRESHOLD": "off"},
    )
    streamed_secs, streamed_body = _timed_cold_miss(
        tmp_path_factory,
        pypiron_bin,
        tmp_path / "streamed",
        pkg,
        wheel,
        pause=pause,
    )

    # Faster is only worth having if it is the same artifact.
    for label, body in (("buffered", buffered_body), ("streamed", streamed_body)):
        assert len(body) == size, f"{label} body was {len(body)} of {size} bytes"
        assert hashlib.sha256(body).hexdigest() == digest, f"{label} body did not match the wheel"

    assert streamed_secs < buffered_secs * OVERLAP_RATIO, (
        f"streamed cold miss took {streamed_secs:.1f}s against {buffered_secs:.1f}s buffered "
        f"({streamed_secs / buffered_secs:.0%} of it): the client transfer is not overlapping "
        "the upstream fetch"
    )


def test_ranged_request_on_a_large_cold_miss_uses_the_buffered_path(proxy_over_fault_tee, tmp_path):
    """A range read needs the committed artifact, so it never tees — and still
    answers with the right bytes."""
    proxy, upstream = proxy_over_fault_tee
    pkg = "teerange"
    wheel = _big_wheel(pkg, tmp_path)
    filename = upstream.register(pkg, wheel)

    read, body = _read_incrementally(
        f"{proxy['base_url']}/files/{pkg}/{filename}",
        timeout=FAULT_GET_TIMEOUT,
        headers={"Range": "bytes=0-99"},
    )
    assert read.status == 206, f"ranged cold miss returned {read.status}"
    assert body == wheel.read_bytes()[:100]
    assert (proxy["data_dir"] / "packages" / pkg / filename).exists(), (
        "the ranged request did not cache the artifact"
    )


def test_real_client_installs_through_a_streamed_fill(
    proxy_over_fault_tee, tmp_path, uv_path, uv_venv
):
    """End to end: uv installs a large wheel served from a still-running fill."""
    proxy, upstream = proxy_over_fault_tee
    pkg = "teeinstall"
    wheel = _big_wheel(pkg, tmp_path)
    upstream.register(pkg, wheel)
    upstream.set_fault(pkg, "slow")

    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(uv_venv),
            "--index-url",
            proxy["simple"],
            "--no-cache-dir",
            f"{pkg}==1.0",
        ],
        timeout=300,
    )
    run_checked([str(uv_venv), "-c", f"import {pkg}"])
    assert (proxy["data_dir"] / "packages" / pkg / wheel.name).exists()
