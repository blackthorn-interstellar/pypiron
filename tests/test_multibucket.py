"""Multi-bucket replication and selection (dev/MULTIBUCKET.md §4-§7).

Real pypiron processes share two or three buckets on MinIO. Some tests give two
nodes independent reachability views so both writable sides of a partition are
exercised. Assertions inspect each bucket through `mc`, independent of what a
server reports.

Covered: fan-out, durable backlog/drain, per-bucket indexes, failover under an
upload storm, hysteresis/flap damping, bidirectional partition heal, duplicate
freeze, tombstone/yank/status convergence, proxy-to-private demotion, cold-start
divergence, origin-only claims, reserved namespaces, and three-bucket fallback.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pytest

from .conftest import (
    _s3_env,
    _start_s3_server,
    minio_get_key_bytes_in,
    minio_get_key_in,
    minio_key_exists_in,
    minio_list_keys_in,
    minio_make_bucket,
    minio_put_key_in,
    minio_remove_bucket,
)
from .helpers import (
    ACCEPT_PEP691,
    http_get,
    http_request_auth,
    make_wheel,
    origin_owner,
    run_returncode,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = [pytest.mark.integration, pytest.mark.s3]

_SELECTED_RE = re.compile(r'^pypiron_bucket_selected\{bucket="([^"]+)",index="\d+"\} ([01])$')
_HEALTH_RE = re.compile(r'^pypiron_bucket_health_state\{bucket="([^"]+)",index="\d+"\} (-?1|0)$')
_BACKLOG_RE = re.compile(r'^pypiron_replication_marker_backlog\{dest="([^"]+)"\} (\d+)$')


def _eventually(predicate, *, timeout: float = 30.0, interval: float = 0.3, what: str = ""):
    """Poll `predicate` until it returns truthy or the deadline passes."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (AssertionError, ConnectionError, RuntimeError, json.JSONDecodeError) as exc:
            last = exc
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s: {what} (last={last!r})")


def _metrics(server) -> list[str]:
    code, body, _ = http_get(f"{server['base_url']}/metrics", timeout=3)
    assert code == 200, f"metrics returned {code}"
    return body.decode().splitlines()


def _selected_bucket(server) -> str:
    selected = [
        match.group(1)
        for line in _metrics(server)
        if (match := _SELECTED_RE.match(line)) and match.group(2) == "1"
    ]
    assert len(selected) == 1, f"expected one selected bucket, got {selected}"
    return selected[0]


def _bucket_health(server, bucket: str) -> int:
    for line in _metrics(server):
        match = _HEALTH_RE.match(line)
        if match and match.group(1) == bucket:
            return int(match.group(2))
    raise AssertionError(f"missing health metric for {bucket}")


def _selection_generation(server) -> int:
    prefix = "pypiron_bucket_selection_generation "
    for line in _metrics(server):
        if line.startswith(prefix):
            return int(line.removeprefix(prefix))
    raise AssertionError("missing bucket selection generation metric")


def _marker_backlog(server, bucket: str) -> int:
    for line in _metrics(server):
        match = _BACKLOG_RE.match(line)
        if match and match.group(1) == bucket:
            return int(match.group(2))
    raise AssertionError(f"missing marker backlog metric for {bucket}")


def _writes_fenced(server) -> bool:
    return "pypiron_bucket_topology_write_fenced 1" in _metrics(server)


def _selection_trace(server, seconds: float, *, interval: float = 0.2) -> list[str]:
    deadline = time.monotonic() + seconds
    samples = []
    while time.monotonic() < deadline:
        samples.append(_selected_bucket(server))
        time.sleep(interval)
    return samples


def _fail_to_second(server) -> tuple[str, str]:
    a, b = server["minio"]["buckets"][:2]
    _eventually(
        lambda: _selected_bucket(server) == a,
        timeout=10,
        what="preferred bucket initially selected",
    )
    _eventually(
        lambda: _bucket_health(server, b) == 1,
        timeout=10,
        what="fallback bucket known healthy",
    )
    server["faults"].fail(a)
    _eventually(
        lambda: _selected_bucket(server) == b,
        timeout=30,
        what="selected bucket changes after preferred bucket returns 503",
    )
    return a, b


def _index_has_file(minio, bucket, pkg, filename) -> bool:
    key = f"simple/{pkg}/index.json"
    if not minio_key_exists_in(minio, bucket, key):
        return False
    data = json.loads(minio_get_key_in(minio, bucket, key))
    return any(f.get("filename") == filename for f in data.get("files", []))


def _global_has_project(minio, bucket, pkg) -> bool:
    key = "simple/index.json"
    if not minio_key_exists_in(minio, bucket, key):
        return False
    data = json.loads(minio_get_key_in(minio, bucket, key))
    return any(p.get("name") == pkg for p in data.get("projects", []))


def _upload(server, wheel):
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
    )


def _retry_upload(server, wheel, *, timeout: float = 30.0) -> None:
    """Model the ordinary client retry that follows a bucket-switch failure."""
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            _upload(server, wheel)
            return
        except RuntimeError as exc:
            last_error = exc
            time.sleep(0.2)
    raise AssertionError(f"upload did not survive selection switch: {last_error}")


def _claim_owner(minio, bucket: str, pkg: str) -> str:
    return origin_owner(minio_get_key_in(minio, bucket, f"packages/{pkg}/.origin"))


def _project_status(minio, bucket: str, pkg: str) -> dict:
    return json.loads(minio_get_key_in(minio, bucket, f"packages/{pkg}/.project-status.json"))


def _assert_mirror_publish_order(faults, bucket: str, artifact_key: str) -> None:
    """Sidecar must be durable, then origin fenced, before artifact publish."""
    requests = faults.requests()

    def indices(method: str, key: str) -> list[int]:
        return [
            index
            for index, (seen_bucket, seen_method, target) in enumerate(requests)
            if seen_bucket == bucket
            and seen_method == method
            and target.split("?", 1)[0].endswith(f"/{key}")
        ]

    artifact_puts = indices("PUT", artifact_key)
    assert artifact_puts, f"no artifact PUT recorded for {bucket}/{artifact_key}"
    artifact_put = artifact_puts[0]
    sidecar_puts = [
        index for index in indices("PUT", f"{artifact_key}.meta.json") if index < artifact_put
    ]
    pkg = artifact_key.split("/", 2)[1]
    origin_gets = [
        index for index in indices("GET", f"packages/{pkg}/.origin") if index < artifact_put
    ]
    assert sidecar_puts, "mirror sidecar was not written before the artifact"
    assert origin_gets, "origin was not re-read before the artifact"
    assert max(sidecar_puts) < max(origin_gets) < artifact_put


def _seed_private_record(minio, bucket: str, pkg: str, filename: str, body: str) -> None:
    """Seed committed private truth before any pypiron process starts."""
    key = f"packages/{pkg}/{filename}"
    sidecar = {
        "sha256": hashlib.sha256(body.encode()).hexdigest(),
        "size": len(body.encode()),
        "version": "1.0",
        "upload-time": "2020-01-01T00:00:00Z",
        "yanked": False,
        "origin": "private",
    }
    claim = {"origin": "private", "nonce": hashlib.sha256(pkg.encode()).hexdigest()[:32]}
    minio_put_key_in(minio, bucket, f"packages/{pkg}/.origin", json.dumps(claim))
    minio_put_key_in(minio, bucket, f"{key}.meta.json", json.dumps(sidecar))
    minio_put_key_in(minio, bucket, key, body)


def _partition_nodes(cluster) -> tuple[str, str]:
    """Give the two nodes opposite reachability views of the same topology."""
    a, b = cluster["minio"]["buckets"]
    cluster["left"]["faults"].fail(b)
    cluster["right"]["faults"].fail(a)
    _eventually(
        lambda: _selected_bucket(cluster["left"]) == a,
        timeout=10,
        what="left node remains on bucket A",
    )
    _eventually(
        lambda: _selected_bucket(cluster["right"]) == b,
        timeout=30,
        what="right node selects bucket B",
    )
    return a, b


def _heal_nodes(cluster, a: str, b: str) -> None:
    cluster["left"]["faults"].recover(b)
    cluster["right"]["faults"].recover(a)


def test_preferred_bucket_503_switches_and_uploads_continue(s3_server_multi_failover, tmp_path):
    server = s3_server_multi_failover
    started = time.monotonic()
    _, b = _fail_to_second(server)
    assert time.monotonic() - started < 30, "503 failover must finish within seconds"
    assert _selection_generation(server) >= 1

    wheel = make_wheel("failoverupload", "1.0", tmp_path)
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], "failoverupload", wheel.name)
    key = f"packages/failoverupload/{wheel.name}"
    _eventually(
        lambda: minio_key_exists_in(server["minio"], b, key),
        timeout=10,
        what="upload stored in the selected fallback bucket",
    )


def test_shipped_defaults_leave_a_blackholed_bucket_within_seconds(
    s3_server_multi_default_failover, tmp_path
):
    """Health calls have their own deadline; SDK retries cannot turn failover into minutes."""
    server = s3_server_multi_default_failover
    a, b = server["minio"]["buckets"]
    _eventually(
        lambda: _bucket_health(server, b) == 1,
        timeout=10,
        what="fallback bucket known healthy",
    )
    started = time.monotonic()
    server["faults"].blackhole(a)
    _eventually(
        lambda: _selected_bucket(server) == b,
        timeout=12,
        what="default three-failure policy leaves a blackhole",
    )
    assert time.monotonic() - started < 12

    wheel = make_wheel("defaultblackhole", "1.0", tmp_path)
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], "defaultblackhole", wheel.name)
    assert minio_key_exists_in(server["minio"], b, f"packages/defaultblackhole/{wheel.name}")
    server["faults"].recover(a)


def test_fresh_startup_skips_a_blackholed_preferred_bucket(
    minio_two_proxy, pypiron_bin, tmp_path_factory
):
    """Cold boot bounds topology I/O instead of inheriting artifact timeouts."""
    minio = minio_two_proxy
    a, b = minio["buckets"]
    minio["faults"].blackhole(a)
    started = time.monotonic()
    server_gen = _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        extra_env={"PYPIRON_AUDIT_ON_BOOT": "false"},
    )
    try:
        server = next(server_gen)
        assert time.monotonic() - started < 8
        _eventually(
            lambda: _selected_bucket(server) == b,
            timeout=5,
            what="startup selects the reachable fallback",
        )
    finally:
        minio["faults"].recover(a)
        server_gen.close()


def test_multi_bucket_s3_stream_survives_more_than_read_idle_timeout(
    s3_server_multi_failover, tmp_path
):
    """Regular body progress must not be mistaken for a stalled S3 request."""
    server = s3_server_multi_failover
    bucket = _selected_bucket(server)
    wheel = make_wheel("steadystream", "1.0", tmp_path)
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], "steadystream", wheel.name)
    key = f"packages/steadystream/{wheel.name}"
    _eventually(
        lambda: all(
            minio_key_exists_in(server["minio"], candidate, key)
            for candidate in server["minio"]["buckets"]
        ),
        timeout=15,
        what="artifact is replicated before the slow read",
    )

    server["faults"].drip(bucket, key, duration=11.0)
    started = time.monotonic()
    code, body, _ = http_get(
        f"{server['base_url']}/files/steadystream/{wheel.name}",
        timeout=20,
    )
    elapsed = time.monotonic() - started

    assert elapsed > 10, f"fixture did not cross the read-idle timeout: {elapsed:.2f}s"
    assert code == 200
    assert body == wheel.read_bytes()


def test_multi_bucket_s3_put_survives_more_than_read_idle_timeout(
    s3_server_multi_failover, tmp_path
):
    """A steadily progressing upload may outlive the S3 read-idle timeout."""
    server = s3_server_multi_failover
    bucket = _selected_bucket(server)
    wheel = make_wheel("steadyput", "1.0", tmp_path)
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr(
            "steadyput/payload.bin",
            b"x" * (16 * 1024 * 1024),
            compress_type=zipfile.ZIP_STORED,
        )
    expected = wheel.read_bytes()
    key = f"packages/steadyput/{wheel.name}"
    server["faults"].drip_put(bucket, key, duration=11.0)

    started = time.monotonic()
    code, _ = upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
        timeout=25,
    )
    elapsed = time.monotonic() - started

    assert elapsed > 10, f"fixture did not cross the read-idle timeout: {elapsed:.2f}s"
    assert code == 200
    assert minio_get_key_bytes_in(server["minio"], bucket, key) == expected


def test_selected_bucket_deletion_switches_and_retry_succeeds(minio_two, s3_server_multi, tmp_path):
    """S3 NoSuchBucket is an outage, not a healthy missing-object response."""
    server = s3_server_multi
    a, b = minio_two["buckets"]
    _eventually(
        lambda: _bucket_health(server, b) == 1,
        timeout=10,
        what="fallback bucket known healthy",
    )
    minio_remove_bucket(minio_two, a)
    wheel = make_wheel("missingselected", "1.0", tmp_path)
    _retry_upload(server, wheel, timeout=15)
    assert _selected_bucket(server) == b
    assert _bucket_health(server, a) == -1
    key = f"packages/missingselected/{wheel.name}"
    assert minio_key_exists_in(minio_two, b, key)

    # Restore the fixture member and prove the durable marker heals it while B
    # remains selected (the default return window is five minutes).
    minio_make_bucket(minio_two, a)
    _eventually(
        lambda: minio_key_exists_in(minio_two, a, key),
        timeout=45,
        what="upload heals into recreated preferred bucket",
    )


def test_upload_storm_retries_through_selection_switch(s3_server_multi_failover, tmp_path):
    """Concurrent publishers keep making progress while the selected bucket dies."""
    server = s3_server_multi_failover
    minio = server["minio"]
    a, b = minio["buckets"]
    _eventually(
        lambda: _bucket_health(server, b) == 1,
        timeout=10,
        what="fallback bucket known healthy before the storm",
    )
    wheels = [make_wheel(f"switchstorm{i}", "1.0", tmp_path) for i in range(6)]

    server["faults"].fail(a)
    with ThreadPoolExecutor(max_workers=len(wheels)) as pool:
        uploads = [pool.submit(_retry_upload, server, wheel) for wheel in wheels]
        for upload in uploads:
            upload.result(timeout=40)

    assert _selected_bucket(server) == b
    for i, wheel in enumerate(wheels):
        pkg = f"switchstorm{i}"
        key = f"packages/{pkg}/{wheel.name}"
        assert minio_key_exists_in(minio, b, key)
        wait_for_file_in_index(server["simple"], pkg, wheel.name)
    _eventually(
        lambda: (
            len(
                [
                    key
                    for key in minio_list_keys_in(minio, b)
                    if key.startswith("_repl/0/switchstorm")
                ]
            )
            >= len(wheels)
        ),
        timeout=20,
        what="every fallback upload leaves durable work for failed A",
    )

    server["faults"].recover(a)
    for i, wheel in enumerate(wheels):
        key = f"packages/switchstorm{i}/{wheel.name}"
        _eventually(
            lambda key=key: minio_key_exists_in(minio, a, key),
            timeout=45,
            what=f"{wheel.name} drains home after recovery",
        )
    _eventually(
        lambda: not any(key.startswith("_repl/") for key in minio_list_keys_in(minio, b)),
        timeout=45,
        what="storm backlog drains after recovery",
    )


def test_recovery_waits_for_return_hysteresis(s3_server_multi_failover):
    server = s3_server_multi_failover
    a, b = _fail_to_second(server)
    generation = _selection_generation(server)

    recovered_at = time.monotonic()
    server["faults"].recover(a)
    _eventually(
        lambda: _bucket_health(server, a) == 1,
        timeout=30,
        what="preferred bucket becomes continuously healthy",
    )
    trace = _selection_trace(server, 2.0)
    assert trace and set(trace) == {b}

    _eventually(
        lambda: _selected_bucket(server) == a,
        timeout=8,
        what="selection returns after the four-second healthy window",
    )
    assert time.monotonic() - recovered_at >= 3.5
    assert _selection_generation(server) == generation + 1


def test_repeated_flap_settles_without_oscillation(s3_server_multi_failover):
    server = s3_server_multi_failover
    a, b = _fail_to_second(server)
    observed = []
    generation = _selection_generation(server)

    for _ in range(3):
        server["faults"].recover(a)

        def _healthy_without_switching():
            observed.append(_selected_bucket(server))
            return _bucket_health(server, a) == 1

        _eventually(
            _healthy_without_switching,
            timeout=30,
            what="flapping bucket briefly recovers",
        )
        time.sleep(1)
        observed.append(_selected_bucket(server))
        server["faults"].fail(a)

        def _unhealthy_without_switching():
            observed.append(_selected_bucket(server))
            return _bucket_health(server, a) == -1

        _eventually(
            _unhealthy_without_switching,
            timeout=30,
            what="flapping bucket fails again",
        )
        assert _selection_generation(server) == generation

    assert set(observed) == {b}, f"selection oscillated during flap: {observed}"

    server["faults"].recover(a)
    _eventually(
        lambda: _bucket_health(server, a) == 1,
        timeout=30,
        what="preferred bucket recovers for good",
    )
    assert set(_selection_trace(server, 2.0)) == {b}
    _eventually(
        lambda: _selected_bucket(server) == a,
        timeout=8,
        what="selector settles back on preferred bucket",
    )
    assert set(_selection_trace(server, 2.0)) == {a}
    assert _selection_generation(server) == generation + 1


def test_recovered_topology_mismatch_fences_writes(s3_server_multi_failover, tmp_path):
    """A healed bucket with a foreign stamp is fenced and never selected."""
    server = s3_server_multi_failover
    minio = server["minio"]
    a, b = _fail_to_second(server)

    stamp_key = "_topology/stamp.json"
    stamp = json.loads(minio_get_key_in(minio, a, stamp_key))
    stamp["buckets"] = ["foreign-topology"]
    stamp["hash"] = "0" * 64
    minio_put_key_in(minio, a, stamp_key, json.dumps(stamp))
    server["faults"].recover(a)

    _eventually(
        lambda: _writes_fenced(server),
        timeout=30,
        what="runtime topology mismatch raises the sticky write fence",
    )
    assert set(_selection_trace(server, 6.0)) == {b}, (
        "topology-mismatched preferred bucket became selected after its four-second return window"
    )
    code, _, _ = http_get(f"{server['base_url']}/simple/")
    assert code != 503, "reads stay available on the last validated bucket"
    wheel = make_wheel("fencedwrite", "1.0", tmp_path)
    code, _ = upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
        expect_status=503,
    )
    assert code == 503


def test_three_bucket_outage_uses_next_reachable_bucket(s3_server_three_failover, tmp_path):
    server = s3_server_three_failover
    a, b, c = server["minio"]["buckets"]
    _eventually(
        lambda: _bucket_health(server, b) == 1 and _bucket_health(server, c) == 1,
        timeout=10,
        what="both fallback buckets known healthy",
    )
    # B withholds responses instead of returning a fast error. C must still
    # receive eager fan-out, and later selection work must not queue behind B.
    server["faults"].blackhole(b)
    warm = make_wheel("slowmiddlewarm", "1.0", tmp_path)
    _upload(server, warm)
    warm_key = f"packages/slowmiddlewarm/{warm.name}"
    _eventually(
        lambda: minio_key_exists_in(server["minio"], c, warm_key),
        timeout=8,
        what="healthy C receives eager copy while B hangs",
    )
    _eventually(
        lambda: _bucket_health(server, b) == -1 and _selected_bucket(server) == a,
        timeout=30,
        what="second bucket is known unavailable before preferred bucket fails",
    )
    server["faults"].fail(a)
    _eventually(
        lambda: _selected_bucket(server) == c,
        timeout=30,
        what="third bucket selected while the first two return 503",
    )

    wheel = make_wheel("thirdbucket", "1.0", tmp_path)
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], "thirdbucket", wheel.name)
    key = f"packages/thirdbucket/{wheel.name}"
    _eventually(
        lambda: minio_key_exists_in(server["minio"], c, key),
        timeout=10,
        what="upload stored in third bucket",
    )
    server["faults"].recover(b)
    _eventually(
        lambda: minio_key_exists_in(server["minio"], b, key),
        timeout=45,
        what="recovered middle bucket drains while A remains unavailable",
    )


def test_three_bucket_marker_bypasses_blackholed_middle(s3_server_three_failover):
    """A full B backlog cannot occupy every lane and starve healthy C."""
    server = s3_server_three_failover
    minio = server["minio"]
    a, b, c = minio["buckets"]
    pkg = "markeraroundmiddle"
    filename = "markeraroundmiddle-1.0-py3-none-any.whl"
    key = f"packages/{pkg}/{filename}"
    reverse_pkg = "sourcearoundmiddle"
    reverse_filename = f"{reverse_pkg}-1.0-py3-none-any.whl"
    reverse_key = f"packages/{reverse_pkg}/{reverse_filename}"
    server["faults"].blackhole(b)
    for i in range(20):
        blocked_pkg = f"blockedmiddle{i:02d}"
        blocked_filename = f"{blocked_pkg}-1.0-py3-none-any.whl"
        _seed_private_record(minio, a, blocked_pkg, blocked_filename, f"blocked {i}")
        minio_put_key_in(
            minio,
            a,
            f"_repl/1/{blocked_pkg}/{blocked_filename}!blackholed",
            "",
        )
    _seed_private_record(minio, a, pkg, filename, "durable marker bytes")
    minio_put_key_in(minio, a, f"_repl/2/{pkg}/{filename}!lost-eager", "")
    _seed_private_record(minio, c, reverse_pkg, reverse_filename, "later source bytes")
    minio_put_key_in(
        minio,
        c,
        f"_repl/0/{reverse_pkg}/{reverse_filename}!later-source",
        "",
    )

    _eventually(
        lambda: minio_key_exists_in(minio, c, key),
        timeout=8,
        what="A-to-C marker bypasses blackholed B",
    )
    _eventually(
        lambda: minio_key_exists_in(minio, a, reverse_key),
        timeout=8,
        what="C source sweep bypasses blackholed B source LIST",
    )
    _eventually(
        lambda: (
            not any(
                marker.startswith(f"_repl/2/{pkg}/{filename}!")
                for marker in minio_list_keys_in(minio, a)
            )
        ),
        timeout=8,
        what="healthy marker is consumed without waiting for B",
    )
    server["faults"].recover(b)


def test_write_nudges_do_not_multiply_periodic_bucket_scans(s3_server_multi_cadence, tmp_path):
    server = s3_server_multi_cadence
    a, b = server["minio"]["buckets"]
    faults = server["faults"]
    _eventually(
        lambda: _bucket_health(server, b) == 1,
        timeout=10,
        what="initial periodic probe completed",
    )
    _eventually(
        lambda: (
            faults.count(method="GET", needle="prefix=_repl/") >= 2
            and faults.count(bucket=b, method="GET", needle="prefix=_dirty/") >= 1
        ),
        timeout=10,
        what="initial periodic maintenance completed",
    )
    before = (
        faults.count(bucket=b, method="GET", needle="/_topology/stamp.json"),
        faults.count(method="GET", needle="prefix=_repl/"),
        faults.count(bucket=b, method="GET", needle="prefix=_dirty/"),
    )
    for i in range(5):
        _upload(server, make_wheel(f"cadencenudge{i}", "1.0", tmp_path))
    # Each successful upload wakes selected-bucket indexing. None may start a
    # second 60-second probe/marker/warm-index cycle.
    time.sleep(2)
    after = (
        faults.count(bucket=b, method="GET", needle="/_topology/stamp.json"),
        faults.count(method="GET", needle="prefix=_repl/"),
        faults.count(bucket=b, method="GET", needle="prefix=_dirty/"),
    )
    assert after == before, f"periodic bucket scans escaped their cadence: {before=} {after=}"


def test_full_diff_does_not_list_staging_once_per_package(s3_server_multi_reconcile_cost):
    """Full-diff LIST cost stays linear in buckets, not packages times buckets."""
    server = s3_server_multi_reconcile_cost
    minio = server["minio"]
    a, b = minio["buckets"]
    filenames = []
    for i in range(6):
        pkg = f"costpkg{i}"
        filename = f"costpkg{i}-1.0-py3-none-any.whl"
        filenames.append((pkg, filename))
        _seed_private_record(minio, a, pkg, filename, f"cost proof {i}")

    for pkg, filename in filenames:
        _eventually(
            lambda pkg=pkg, filename=filename: minio_key_exists_in(
                minio, b, f"packages/{pkg}/{filename}"
            ),
            timeout=20,
            what=f"full diff copies {pkg}",
        )
    for pkg, _ in filenames:
        assert server["faults"].count(needle=f"prefix=_staging/repl/{pkg}") == 0, (
            f"full diff performed a per-package staging LIST for {pkg}"
        )


def test_upload_replicates_and_second_bucket_builds_its_own_index(
    minio_two, s3_server_multi, tmp_path
):
    """A private upload lands in bucket A and, within the eager window, in bucket
    B — sidecar, artifact, and a private origin claim — and B rebuilds its OWN
    per-package and global indexes from the truth replicated into it."""
    server = s3_server_multi
    minio = minio_two
    a, b = minio["buckets"]
    pkg = "repltest"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _upload(server, wheel)

    # The selected bucket indexes it as usual.
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    akey = f"packages/{pkg}/{wheel.name}"
    _eventually(
        lambda: minio_key_exists_in(minio, b, akey),
        what="artifact replicated to bucket B",
    )
    assert minio_key_exists_in(minio, b, f"{akey}.meta.json"), "sidecar must replicate too"
    assert minio_key_exists_in(minio, b, f"{akey}.metadata"), "PEP 658 metadata must replicate"
    # Origin claim replicated as private (ahead of / alongside the artifact).
    claim = json.loads(minio_get_key_in(minio, b, f"packages/{pkg}/.origin"))
    assert claim["origin"] == "private"

    # B derives its own indexes (the leader drains B's replicated dirty markers).
    _eventually(
        lambda: _index_has_file(minio, b, pkg, wheel.name),
        what="bucket B's per-package index lists the file",
    )
    _eventually(
        lambda: _global_has_project(minio, b, pkg),
        what="bucket B's global index lists the project",
    )


def test_repl_marker_accumulates_then_drains_when_destination_returns(
    minio_two, s3_server_multi, tmp_path
):
    """With bucket B unreachable, the eager push fails and drops a `_repl/`
    marker in bucket A; A keeps serving. When B returns, the leader's marker
    sweep delivers the record and deletes the marker."""
    server = s3_server_multi
    minio = minio_two
    a, b = minio["buckets"]
    pkg = "markertest"

    # Kill tier-1: the destination bucket is gone.
    minio_remove_bucket(minio, b)

    wheel = make_wheel(pkg, "2.0", tmp_path)
    _upload(server, wheel)
    # The selected bucket is unaffected — uploads and indexing keep flowing.
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    # A todo marker for destination index 1 (bucket B) accumulates in bucket A.
    def _marker_present():
        return any(k.startswith(f"_repl/1/{pkg}/") for k in minio_list_keys_in(minio, a))

    _eventually(_marker_present, what="_repl/ marker for the down bucket")
    _eventually(
        lambda: _marker_backlog(server, b) > 0,
        what="reachable-source backlog is visible while B is unavailable",
    )

    # Bring the destination back; the per-tick sweep drains the backlog.
    minio_make_bucket(minio, b)
    akey = f"packages/{pkg}/{wheel.name}"
    _eventually(
        lambda: minio_key_exists_in(minio, b, akey),
        what="record delivered after the bucket returned",
    )
    _eventually(
        lambda: not any(k.startswith("_repl/") for k in minio_list_keys_in(minio, a)),
        what="marker consumed after successful delivery",
    )
    _eventually(
        lambda: _marker_backlog(server, b) == 0,
        what="backlog metric clears after delivery",
    )


def test_delete_propagates_tombstone_and_drops_from_both_indexes(
    minio_two, s3_server_multi, tmp_path
):
    """Deleting a private file on bucket A propagates: a tombstone appears in both
    buckets, the artifact is gone from both, and it leaves both indexes."""
    server = s3_server_multi
    minio = minio_two
    a, b = minio["buckets"]
    pkg = "deltest"
    wheel = make_wheel(pkg, "3.0", tmp_path)
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    akey = f"packages/{pkg}/{wheel.name}"
    _eventually(
        lambda: minio_key_exists_in(minio, b, akey),
        what="file replicated before the delete",
    )

    code, _, _ = http_request_auth(
        "DELETE",
        f"{server['base_url']}/files/{pkg}/{wheel.name}",
        username=server["user"],
        password=server["password"],
    )
    assert code == 204, f"delete returned {code}"

    tkey = f"{akey}.tombstone"
    _eventually(lambda: minio_key_exists_in(minio, a, tkey), what="tombstone in bucket A")
    _eventually(lambda: minio_key_exists_in(minio, b, tkey), what="tombstone replicated to B")
    _eventually(lambda: not minio_key_exists_in(minio, a, akey), what="artifact gone from A")
    _eventually(lambda: not minio_key_exists_in(minio, b, akey), what="artifact gone from B")
    _eventually(
        lambda: not _index_has_file(minio, b, pkg, wheel.name),
        what="file dropped from bucket B's index",
    )


def test_byte_conflict_freezes_on_both_buckets(minio_two, s3_server_multi, tmp_path):
    """Two buckets holding different bytes under one filename is a split-brain.
    The reconcile diff quarantines both bodies, drops a `.frozen` suppression
    marker on both, retains inert canonical evidence, removes the name from both
    indexes, and counts the freeze — never picking a winner."""
    server = s3_server_multi
    minio = minio_two
    a, b = minio["buckets"]
    pkg = "conflicttest"
    wheel = make_wheel(pkg, "4.0", tmp_path)
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    akey = f"packages/{pkg}/{wheel.name}"
    _eventually(
        lambda: minio_key_exists_in(minio, b, akey),
        what="identical copy replicated to B before the conflict",
    )

    # Manufacture the conflict: overwrite B's artifact and sidecar with different
    # bytes under the same filename, still claimed private — the exact shape a
    # dual-write split would leave behind.
    other = "these are different bytes than the wheel A published"
    other_sha = hashlib.sha256(other.encode()).hexdigest()
    minio_put_key_in(minio, b, akey, other)
    sidecar = json.dumps(
        {
            "sha256": other_sha,
            "size": len(other),
            "version": "4.0",
            "upload-time": "2020-01-01T00:00:00Z",
            "yanked": False,
            "origin": "private",
        }
    )
    minio_put_key_in(minio, b, f"{akey}.meta.json", sidecar)

    # The reconcile diff freezes both sides.
    fkey = f"{akey}.frozen"
    _eventually(lambda: minio_key_exists_in(minio, a, fkey), what="freeze marker on A")
    _eventually(lambda: minio_key_exists_in(minio, b, fkey), what="freeze marker on B")

    # Both bodies are preserved under quarantine; canonical keys remain occupied
    # and inert so cleanup cannot delete a replacement that raced the freeze.
    _eventually(
        lambda: any(k.startswith(f"_quarantine/{pkg}/") for k in minio_list_keys_in(minio, a)),
        what="bucket A body quarantined",
    )
    _eventually(
        lambda: any(k.startswith(f"_quarantine/{pkg}/") for k in minio_list_keys_in(minio, b)),
        what="bucket B body quarantined",
    )

    _eventually(lambda: minio_key_exists_in(minio, a, akey), what="A evidence retained")
    _eventually(lambda: minio_key_exists_in(minio, b, akey), what="B evidence retained")
    _eventually(
        lambda: not _index_has_file(minio, a, pkg, wheel.name),
        what="name suppressed on A",
    )
    _eventually(
        lambda: not _index_has_file(minio, b, pkg, wheel.name),
        what="name suppressed on B",
    )

    # A freeze carries the same permanent filename-reuse fence as a delete.
    # The existing one tombstone HEAD in the upload path rejects this before a
    # client can receive a false 200 for a body that reconciliation will hide.
    code, _ = upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
        expect_status=409,
    )
    assert code == 409
    assert minio_key_exists_in(minio, a, akey)
    code, _, _ = http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}", timeout=10)
    assert code == 404

    # The freeze is counted (multi-bucket only metric family).
    _, body, _ = http_get(f"{server['base_url']}/metrics")
    freeze_line = next(
        (
            ln
            for ln in body.decode().splitlines()
            if ln.startswith("pypiron_replication_freezes_total ")
        ),
        None,
    )
    assert freeze_line is not None, "freeze metric family must be present on a multi-bucket node"
    assert int(freeze_line.rsplit(" ", 1)[1]) >= 1, freeze_line


def test_follower_with_working_path_drains_leaders_marker(s3_servers_multi, tmp_path):
    """Selected-bucket leadership may suppress duplicate index work, never replication."""
    cluster = s3_servers_multi
    minio = cluster["minio"]
    a, b = minio["buckets"]

    def _leader_node():
        if not minio_key_exists_in(minio, a, "_leader/lease.json"):
            return None
        holder = json.loads(minio_get_key_in(minio, a, "_leader/lease.json"))["holder"]
        for node in (cluster["left"], cluster["right"]):
            if holder.startswith(f"{node['proc'].pid}-"):
                return node
        return None

    leader = _eventually(_leader_node, timeout=15, what="selected-bucket lease holder")
    follower = cluster["right"] if leader is cluster["left"] else cluster["left"]
    assert _selected_bucket(leader) == a
    assert _selected_bucket(follower) == a
    _eventually(
        lambda: _bucket_health(follower, b) == 1,
        timeout=10,
        what="follower can reach warm bucket",
    )

    leader["faults"].blackhole(b)
    wheel = make_wheel("followerdrain", "1.0", tmp_path)
    _upload(leader, wheel)
    key = f"packages/followerdrain/{wheel.name}"
    _eventually(
        lambda: minio_key_exists_in(minio, b, key),
        timeout=8,
        what="non-leader node drains marker through its healthy path",
    )
    _eventually(
        lambda: (
            not any(
                marker.startswith(f"_repl/1/followerdrain/{wheel.name}!")
                for marker in minio_list_keys_in(minio, a)
            )
        ),
        timeout=8,
        what="follower consumes leader's marker",
    )
    leader["faults"].recover(b)


def test_follower_with_working_path_runs_full_diff_and_warms_index(
    s3_servers_multi_short_lease,
):
    """Lost markers and warm indexes heal through a non-leader's healthy path."""
    cluster = s3_servers_multi_short_lease
    minio = cluster["minio"]
    a, b = minio["buckets"]

    def _leader_node():
        if not minio_key_exists_in(minio, a, "_leader/lease.json"):
            return None
        holder = json.loads(minio_get_key_in(minio, a, "_leader/lease.json"))["holder"]
        for node in (cluster["left"], cluster["right"]):
            if holder.startswith(f"{node['proc'].pid}-"):
                return node
        return None

    leader = _eventually(_leader_node, timeout=15, what="selected-bucket lease holder")
    follower = cluster["right"] if leader is cluster["left"] else cluster["left"]
    assert _selected_bucket(leader) == a
    assert _selected_bucket(follower) == a
    _eventually(
        lambda: _bucket_health(follower, b) == 1,
        timeout=10,
        what="follower can reach warm bucket",
    )

    # Remove the selected-bucket leader's path to B before creating truth.
    # With no replication marker, only the periodic full diff can discover it.
    leader["faults"].blackhole(b)
    _eventually(
        lambda: _bucket_health(leader, b) == -1,
        timeout=10,
        what="selected-bucket leader cannot use warm bucket",
    )
    pkg = "followerfulldiff"
    filename = f"{pkg}-1.0-py3-none-any.whl"
    key = f"packages/{pkg}/{filename}"
    _seed_private_record(minio, a, pkg, filename, "truth with no marker")
    assert not any(marker.startswith(f"_repl/1/{pkg}/") for marker in minio_list_keys_in(minio, a))

    _eventually(
        lambda: minio_key_exists_in(minio, b, key),
        timeout=15,
        what="non-leader full diff repairs lost-marker truth",
    )
    _eventually(
        lambda: _index_has_file(minio, b, pkg, filename),
        timeout=15,
        what="non-leader warm worker rebuilds package index",
    )
    _eventually(
        lambda: _global_has_project(minio, b, pkg),
        timeout=15,
        what="non-leader warm worker rebuilds global index",
    )
    leader["faults"].recover(b)


def test_partitioned_nodes_accept_writes_and_heal_both_directions(s3_servers_multi, tmp_path):
    """Two real nodes may select different buckets without losing writes.

    Each side accepts an upload while it cannot reach the other bucket. Durable
    markers accumulate on both sides, then drain after the partition heals.
    """
    cluster = s3_servers_multi
    minio = cluster["minio"]
    a, b = _partition_nodes(cluster)
    left_wheel = make_wheel("partitionleft", "1.0", tmp_path)
    right_wheel = make_wheel("partitionright", "1.0", tmp_path)

    with ThreadPoolExecutor(max_workers=2) as pool:
        left_result = pool.submit(_upload, cluster["left"], left_wheel)
        right_result = pool.submit(_upload, cluster["right"], right_wheel)
        left_result.result(timeout=30)
        right_result.result(timeout=30)

    left_key = f"packages/partitionleft/{left_wheel.name}"
    right_key = f"packages/partitionright/{right_wheel.name}"
    assert minio_key_exists_in(minio, a, left_key)
    assert minio_key_exists_in(minio, b, right_key)
    assert not minio_key_exists_in(minio, b, left_key)
    assert not minio_key_exists_in(minio, a, right_key)
    _eventually(
        lambda: any(
            key.startswith("_repl/1/partitionleft/") for key in minio_list_keys_in(minio, a)
        ),
        timeout=15,
        what="A retains its marker for unreachable B",
    )
    _eventually(
        lambda: any(
            key.startswith("_repl/0/partitionright/") for key in minio_list_keys_in(minio, b)
        ),
        timeout=15,
        what="B retains its marker for unreachable A",
    )

    _heal_nodes(cluster, a, b)
    for bucket, key, description in (
        (b, left_key, "left-side upload reaches B"),
        (a, right_key, "right-side upload reaches A"),
    ):
        _eventually(
            lambda bucket=bucket, key=key: minio_key_exists_in(minio, bucket, key),
            timeout=45,
            what=description,
        )
    for bucket, pkg, filename in (
        (a, "partitionright", right_wheel.name),
        (b, "partitionleft", left_wheel.name),
    ):
        _eventually(
            lambda bucket=bucket, pkg=pkg, filename=filename: _index_has_file(
                minio, bucket, pkg, filename
            ),
            timeout=45,
            what=f"{bucket} rebuilds {pkg}'s index",
        )
    _eventually(
        lambda: (
            not any(
                key.startswith("_repl/")
                for bucket in (a, b)
                for key in minio_list_keys_in(minio, bucket)
            )
        ),
        timeout=45,
        what="bidirectional marker backlog drains",
    )


def test_partitioned_duplicate_uploads_freeze_after_heal(s3_servers_multi, tmp_path):
    """Two acknowledged uploads with one filename and different bytes freeze."""
    cluster = s3_servers_multi
    minio = cluster["minio"]
    a, b = _partition_nodes(cluster)
    pkg = "realduplicate"
    left_wheel = make_wheel(pkg, "1.0", tmp_path / "left", description="left build")
    right_wheel = make_wheel(pkg, "1.0", tmp_path / "right", description="right build")
    assert left_wheel.name == right_wheel.name
    assert (
        hashlib.sha256(left_wheel.read_bytes()).digest()
        != hashlib.sha256(right_wheel.read_bytes()).digest()
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        left = pool.submit(_upload, cluster["left"], left_wheel)
        right = pool.submit(_upload, cluster["right"], right_wheel)
        left.result(timeout=30)
        right.result(timeout=30)

    key = f"packages/{pkg}/{left_wheel.name}"
    assert minio_key_exists_in(minio, a, key)
    assert minio_key_exists_in(minio, b, key)
    _heal_nodes(cluster, a, b)
    for bucket in (a, b):
        _eventually(
            lambda bucket=bucket: minio_key_exists_in(minio, bucket, f"{key}.frozen"),
            timeout=45,
            what=f"duplicate name freezes in {bucket}",
        )
        _eventually(
            lambda bucket=bucket: minio_key_exists_in(minio, bucket, key),
            timeout=45,
            what=f"frozen canonical evidence remains occupied in {bucket}",
        )
        _eventually(
            lambda bucket=bucket: any(
                item.startswith(f"_quarantine/{pkg}/{left_wheel.name}@")
                for item in minio_list_keys_in(minio, bucket)
            ),
            timeout=45,
            what=f"conflicting body is preserved in {bucket}",
        )
        _eventually(
            lambda bucket=bucket: not _index_has_file(minio, bucket, pkg, left_wheel.name),
            timeout=45,
            what=f"frozen duplicate is suppressed in {bucket}'s index",
        )

    _eventually(
        lambda: _selected_bucket(cluster["left"]) == a,
        timeout=30,
        what="left reads frozen evidence from A",
    )
    code, _, _ = http_get(
        f"{cluster['left']['base_url']}/files/{pkg}/{left_wheel.name}", timeout=10
    )
    assert code == 404
    cluster["right"]["faults"].fail(a)
    _eventually(
        lambda: _selected_bucket(cluster["right"]) == b,
        timeout=30,
        what="right reads frozen evidence from B",
    )
    code, _, _ = http_get(
        f"{cluster['right']['base_url']}/files/{pkg}/{right_wheel.name}", timeout=10
    )
    assert code == 404
    cluster["right"]["faults"].recover(a)


def test_partitioned_delete_beats_concurrent_yank(s3_servers_multi, tmp_path):
    """A tombstone wins over a live, newly-yanked peer after partition heal."""
    cluster = s3_servers_multi
    minio = cluster["minio"]
    a, b = minio["buckets"]
    pkg = "deleteyankrace"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _upload(cluster["left"], wheel)
    key = f"packages/{pkg}/{wheel.name}"
    _eventually(
        lambda: minio_key_exists_in(minio, b, key),
        timeout=30,
        what="baseline artifact is warm in B",
    )

    _partition_nodes(cluster)
    delete_url = f"{cluster['left']['base_url']}/files/{pkg}/{wheel.name}"
    yank_url = f"{cluster['right']['base_url']}/files/{pkg}/{wheel.name}/yank"
    with ThreadPoolExecutor(max_workers=2) as pool:
        deleted = pool.submit(
            http_request_auth,
            "DELETE",
            delete_url,
            username=cluster["left"]["user"],
            password=cluster["left"]["password"],
        )
        yanked = pool.submit(
            http_request_auth,
            "POST",
            yank_url,
            username=cluster["right"]["user"],
            password=cluster["right"]["password"],
            data=b"withdrawn during partition",
        )
        assert deleted.result(timeout=30)[0] == 204
        assert yanked.result(timeout=30)[0] == 200

    assert minio_key_exists_in(minio, a, f"{key}.tombstone")
    peer_sidecar = json.loads(minio_get_key_in(minio, b, f"{key}.meta.json"))
    assert peer_sidecar["yanked"] == "withdrawn during partition"
    assert peer_sidecar["yank-epoch"] == 1
    _eventually(
        lambda: (
            any(
                marker.startswith(f"_repl/1/{pkg}/{wheel.name}!")
                for marker in minio_list_keys_in(minio, a)
            )
            and any(
                marker.startswith(f"_repl/0/{pkg}/{wheel.name}!")
                for marker in minio_list_keys_in(minio, b)
            )
        ),
        timeout=15,
        what="delete and yank markers remain durable across the outage",
    )

    _heal_nodes(cluster, a, b)
    for bucket in (a, b):
        _eventually(
            lambda bucket=bucket: minio_key_exists_in(minio, bucket, f"{key}.tombstone"),
            timeout=45,
            what=f"tombstone converges to {bucket}",
        )
        _eventually(
            lambda bucket=bucket: not minio_key_exists_in(minio, bucket, key),
            timeout=45,
            what=f"tombstone removes live body from {bucket}",
        )
        _eventually(
            lambda bucket=bucket: not _index_has_file(minio, bucket, pkg, wheel.name),
            timeout=45,
            what=f"tombstoned file is suppressed in {bucket}'s index",
        )


def test_partitioned_project_status_converges_and_clear_advances_epoch(s3_servers_multi, tmp_path):
    """Equal-epoch status conflict restricts; a later clear wins everywhere."""
    cluster = s3_servers_multi
    minio = cluster["minio"]
    a, b = minio["buckets"]
    pkg = "statusrace"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _upload(cluster["left"], wheel)
    key = f"packages/{pkg}/{wheel.name}"
    _eventually(
        lambda: minio_key_exists_in(minio, b, key),
        timeout=30,
        what="status test starts with a warm private record",
    )
    _partition_nodes(cluster)
    left_url = f"{cluster['left']['base_url']}/project/{pkg}/status"
    right_url = f"{cluster['right']['base_url']}/project/{pkg}/status"
    with ThreadPoolExecutor(max_workers=2) as pool:
        restricted = pool.submit(
            http_request_auth,
            "POST",
            left_url,
            username=cluster["left"]["user"],
            password=cluster["left"]["password"],
            data=b'{"status":"quarantined","reason":"security review"}',
        )
        active = pool.submit(
            http_request_auth,
            "DELETE",
            right_url,
            username=cluster["right"]["user"],
            password=cluster["right"]["password"],
        )
        assert restricted.result(timeout=30)[0] == 200
        assert active.result(timeout=30)[0] == 200

    assert _project_status(minio, a, pkg)["status"] == "quarantined"
    assert _project_status(minio, b, pkg)["status"] == "active"
    _heal_nodes(cluster, a, b)
    for bucket in (a, b):
        _eventually(
            lambda bucket=bucket: _project_status(minio, bucket, pkg)["status"] == "quarantined",
            timeout=45,
            what=f"fail-closed status winner reaches {bucket}",
        )
        status = _project_status(minio, bucket, pkg)
        assert status["pypiron-epoch"] == 1

    code, _, _ = http_request_auth(
        "DELETE",
        left_url,
        username=cluster["left"]["user"],
        password=cluster["left"]["password"],
    )
    assert code == 200
    for bucket in (a, b):
        _eventually(
            lambda bucket=bucket: (
                _project_status(minio, bucket, pkg)["status"] == "active"
                and _project_status(minio, bucket, pkg)["pypiron-epoch"] == 2
            ),
            timeout=45,
            what=f"newer active clear reaches {bucket}",
        )
        _eventually(
            lambda bucket=bucket: _index_has_file(minio, bucket, pkg, wheel.name),
            timeout=45,
            what=f"active clear restores {bucket}'s package index",
        )


def test_legacy_mirror_upload_publishes_sidecar_before_artifact(s3_servers_multi, tmp_path):
    cluster = s3_servers_multi
    server = cluster["left"]
    bucket = cluster["minio"]["buckets"][0]
    _eventually(
        lambda: _selected_bucket(server) == bucket,
        timeout=10,
        what="preferred bucket selected before mirror upload",
    )
    pkg = "mirrororder"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
        fields={"mirror": "true", "upload_time": "2020-01-01T00:00:00Z"},
    )

    _assert_mirror_publish_order(
        server["faults"],
        bucket,
        f"packages/{pkg}/{wheel.name}",
    )


def test_proxy_fill_vs_private_upload_demotes_and_quarantines(s3_servers_multi_proxy, tmp_path):
    """A proxy fill on B and private publish on A converge private, not frozen.

    The operations overlap while each node can reach only its selected bucket.
    On heal the package-level demotion stages the private record, changes the
    claim once, and preserves the mirror loser under quarantine.
    """
    cluster = s3_servers_multi_proxy
    minio = cluster["minio"]
    pkg = "demotionrace"
    mirror_wheel = make_wheel(pkg, "1.0", tmp_path)
    private_wheel = make_wheel(pkg, "2.0", tmp_path)
    _upload(cluster["upstream"], mirror_wheel)
    wait_for_file_in_index(cluster["upstream"]["simple"], pkg, mirror_wheel.name)
    a, b = _partition_nodes(cluster)

    with ThreadPoolExecutor(max_workers=2) as pool:
        mirror_fill = pool.submit(
            http_get,
            f"{cluster['right']['base_url']}/files/{pkg}/{mirror_wheel.name}",
            timeout=30,
        )
        private_fill = pool.submit(_upload, cluster["left"], private_wheel)
        code, body, _ = mirror_fill.result(timeout=40)
        private_fill.result(timeout=40)
    assert code == 200
    assert hashlib.sha256(body).hexdigest() == hashlib.sha256(mirror_wheel.read_bytes()).hexdigest()
    _assert_mirror_publish_order(
        cluster["right"]["faults"],
        b,
        f"packages/{pkg}/{mirror_wheel.name}",
    )
    _eventually(
        lambda: _claim_owner(minio, a, pkg) == "private",
        timeout=15,
        what="A holds the private claim",
    )
    _eventually(
        lambda: _claim_owner(minio, b, pkg) == "mirror",
        timeout=15,
        what="B holds the mirror claim",
    )

    private_key = f"packages/{pkg}/{private_wheel.name}"
    mirror_key = f"packages/{pkg}/{mirror_wheel.name}"
    _heal_nodes(cluster, a, b)
    _eventually(
        lambda: _claim_owner(minio, b, pkg) == "private",
        timeout=45,
        what="B is atomically demoted to private",
    )
    _eventually(
        lambda: minio_key_exists_in(minio, b, private_key),
        timeout=45,
        what="staged private artifact is promoted in B",
    )
    _eventually(
        lambda: minio_key_exists_in(minio, b, f"{mirror_key}.mirror-quarantined"),
        timeout=45,
        what="mirror loser is marked inert in B's live package tree",
    )
    assert minio_key_exists_in(minio, b, mirror_key), "cleanup must not open an ABA window"
    assert not minio_key_exists_in(minio, a, mirror_key), "mirror cache must never replicate"
    _eventually(
        lambda: any(
            key.startswith(f"_quarantine/{pkg}/{mirror_wheel.name}@")
            for key in minio_list_keys_in(minio, b)
        ),
        timeout=45,
        what="B preserves the mirror loser under quarantine",
    )
    _eventually(
        lambda: (
            _index_has_file(minio, b, pkg, private_wheel.name)
            and not _index_has_file(minio, b, pkg, mirror_wheel.name)
        ),
        timeout=45,
        what="B indexes only private truth after demotion",
    )
    cluster["right"]["faults"].fail(a)
    _eventually(
        lambda: _selected_bucket(cluster["right"]) == b,
        timeout=30,
        what="right node selects bucket with retained quarantined mirror bytes",
    )
    code, _, _ = http_get(
        f"{cluster['right']['base_url']}/files/{pkg}/{mirror_wheel.name}", timeout=10
    )
    assert code == 404, "retained mirror quarantine bytes must not be directly downloadable"
    cluster["right"]["faults"].recover(a)


def test_proxy_page_and_companion_respect_local_freeze_fence(s3_servers_multi_proxy, tmp_path):
    cluster = s3_servers_multi_proxy
    node = cluster["right"]
    pkg = "proxyfence"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _upload(cluster["upstream"], wheel)
    wait_for_file_in_index(cluster["upstream"]["simple"], pkg, wheel.name)

    code, body, _ = http_get(f"{node['simple']}{pkg}/index.json", headers={"Accept": ACCEPT_PEP691})
    assert code == 200
    assert any(file["filename"] == wheel.name for file in json.loads(body)["files"])
    code, metadata, _ = http_get(f"{node['base_url']}/files/{pkg}/{wheel.name}.metadata")
    assert code == 200
    assert b"Metadata-Version" in metadata

    bucket = _selected_bucket(node)
    artifact_key = f"packages/{pkg}/{wheel.name}"
    minio_put_key_in(cluster["minio"], bucket, f"{artifact_key}.frozen", "{}")

    code, body, _ = http_get(f"{node['simple']}{pkg}/index.json", headers={"Accept": ACCEPT_PEP691})
    assert code == 200
    assert not any(file["filename"] == wheel.name for file in json.loads(body)["files"])
    code, _, _ = http_get(f"{node['base_url']}/files/{pkg}/{wheel.name}.metadata")
    assert code == 404


def test_multi_bucket_refuses_mirror_cache_delete(s3_servers_multi_proxy, tmp_path):
    """A cache eviction must not race demotion into a fleet-wide private delete."""
    cluster = s3_servers_multi_proxy
    pkg = "mirrorcachedelete"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _upload(cluster["upstream"], wheel)
    wait_for_file_in_index(cluster["upstream"]["simple"], pkg, wheel.name)

    node = cluster["right"]
    code, body, _ = http_get(f"{node['base_url']}/files/{pkg}/{wheel.name}", timeout=30)
    assert code == 200
    assert body == wheel.read_bytes()
    bucket = _selected_bucket(node)
    key = f"packages/{pkg}/{wheel.name}"
    assert minio_key_exists_in(cluster["minio"], bucket, key)
    assert _claim_owner(cluster["minio"], bucket, pkg) == "mirror"

    code, body, _ = http_request_auth(
        "DELETE",
        f"{node['base_url']}/files/{pkg}/{wheel.name}",
        username=node["user"],
        password=node["password"],
    )
    assert code == 409
    assert b"Mirror cache eviction is disabled" in body
    assert minio_key_exists_in(cluster["minio"], bucket, key)
    assert not minio_key_exists_in(cluster["minio"], bucket, f"{key}.tombstone")


def test_cold_start_merges_preexisting_bucket_divergence(minio_two, pypiron_bin, tmp_path_factory):
    """A node boots on already-divergent buckets and unions private truth."""
    minio = minio_two
    a, b = minio["buckets"]
    left_pkg = "coldleft"
    right_pkg = "coldright"
    left_filename = "coldleft-1.0-py3-none-any.whl"
    right_filename = "coldright-1.0-py3-none-any.whl"
    _seed_private_record(minio, a, left_pkg, left_filename, "private bytes from A")
    _seed_private_record(minio, b, right_pkg, right_filename, "private bytes from B")

    server_gen = _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        extra_env={
            "PYPIRON_AUDIT_ON_BOOT": "false",
            "PYPIRON_RECONCILE_INTERVAL_SECS": "3",
        },
    )
    server = next(server_gen)
    try:
        for bucket, pkg, filename in (
            (b, left_pkg, left_filename),
            (a, right_pkg, right_filename),
        ):
            key = f"packages/{pkg}/{filename}"
            _eventually(
                lambda bucket=bucket, key=key: minio_key_exists_in(minio, bucket, key),
                timeout=45,
                what=f"cold-start diff copies {pkg} into {bucket}",
            )
            _eventually(
                lambda bucket=bucket, pkg=pkg: _claim_owner(minio, bucket, pkg) == "private",
                timeout=20,
                what=f"cold-start diff establishes {pkg}'s private claim in {bucket}",
            )
            _eventually(
                lambda bucket=bucket, pkg=pkg, filename=filename: _index_has_file(
                    minio, bucket, pkg, filename
                ),
                timeout=45,
                what=f"cold-start destination index includes {pkg}",
            )
        assert _selected_bucket(server) == a
    finally:
        server_gen.close()


def test_origin_release_updates_every_empty_bucket_claim(minio_two, pypiron_bin):
    """The explicit repurpose command CAS-releases an empty name everywhere."""
    minio = minio_two
    pkg = "releasedname"
    for index, bucket in enumerate(minio["buckets"]):
        claim = {"origin": "private", "nonce": f"{index + 1:032x}"}
        minio_put_key_in(minio, bucket, f"packages/{pkg}/.origin", json.dumps(claim))

    env = _s3_env(minio, "127.0.0.1:0")
    rc, out, err = run_returncode(
        [str(pypiron_bin), "origin", "release", pkg],
        env=env,
        timeout=30,
    )
    assert rc == 0, f"origin release failed:\n{out}\n{err}"
    for bucket in minio["buckets"]:
        assert _claim_owner(minio, bucket, pkg) == "unclaimed"

    rc, _, err = run_returncode(
        [str(pypiron_bin), "origin", "release", pkg],
        env=env,
        timeout=30,
    )
    assert rc != 0
    assert "no live origin claim" in err


def test_buckets_migrate_stamps_every_reachable_bucket(minio_two, pypiron_bin):
    """The explicit topology command CAS-bumps one shared generation."""
    minio = minio_two
    env = _s3_env(minio, "127.0.0.1:0")
    for expected_generation in (1, 2):
        rc, out, err = run_returncode(
            [str(pypiron_bin), "buckets", "migrate"],
            env=env,
            timeout=30,
        )
        assert rc == 0, f"topology migration failed:\n{out}\n{err}"
        for bucket in minio["buckets"]:
            stamp = json.loads(minio_get_key_in(minio, bucket, "_topology/stamp.json"))
            assert stamp["buckets"] == minio["buckets"]
            assert stamp["generation"] == expected_generation


def test_fresh_startup_repairs_member_that_missed_topology_migration(
    minio_three_proxy, pypiron_bin, tmp_path_factory
):
    """A recovered lower-generation member cannot brick the next deployment."""
    minio = minio_three_proxy
    a, b, c = minio["buckets"]
    env = _s3_env(minio, "127.0.0.1:0")
    env["PYPIRON_S3_BUCKETS"] = f"{a},{b}"
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=30)
    assert rc == 0, f"initial topology migration failed:\n{out}\n{err}"

    minio["faults"].fail(b)
    env["PYPIRON_S3_BUCKETS"] = f"{a},{b},{c}"
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=30)
    assert rc == 0, f"partial topology migration failed:\n{out}\n{err}"
    minio["faults"].recover(b)

    server_gen = _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        extra_env={"PYPIRON_AUDIT_ON_BOOT": "false"},
    )
    server = next(server_gen)
    try:
        stamp = json.loads(minio_get_key_in(minio, b, "_topology/stamp.json"))
        assert stamp["buckets"] == [a, b, c]
        assert stamp["generation"] == 2
        assert _selected_bucket(server) == a
    finally:
        server_gen.close()


def test_legacy_singular_s3_bucket_env_still_selects_s3(minio_two, pypiron_bin):
    env = _s3_env(minio_two, "127.0.0.1:0")
    bucket = minio_two["buckets"][0]
    pkg = "legacyenvclaim"
    minio_put_key_in(
        minio_two,
        bucket,
        f"packages/{pkg}/.origin",
        json.dumps({"origin": "private", "nonce": "1" * 32}),
    )
    env.pop("PYPIRON_S3_BUCKETS")
    env["PYPIRON_S3_BUCKET"] = bucket
    rc, out, err = run_returncode([str(pypiron_bin), "origin", "release", pkg], env=env, timeout=30)
    assert rc == 0, f"legacy singular env was rejected:\n{out}\n{err}"
    assert f"storage backend s3 · {bucket}" in err
    assert _claim_owner(minio_two, bucket, pkg) == "unclaimed"


def test_s3_bucket_rejects_an_explicit_other_cloud(minio_two, pypiron_bin):
    env = _s3_env(minio_two, "127.0.0.1:0")
    rc, _, err = run_returncode(
        [
            str(pypiron_bin),
            "buckets",
            "migrate",
            "--storage",
            "gcs",
            "--gcs-bucket",
            "other-cloud",
            "--s3-bucket",
            minio_two["buckets"][0],
        ],
        env=env,
        timeout=30,
    )
    assert rc != 0
    assert "cannot be combined" in err


def test_reserved_prefix_blocks_proxy_during_origin_only_divergence(
    s3_server_multi_proxy_prefixed, tmp_path
):
    """A reserved private name never falls through while its claim is remote.

    The only private truth initially present is B's package-level claim: no
    artifact exists to pull the full diff along. The name stays unavailable
    instead of proxying public bytes, and the claim itself converges to A.
    """
    server = s3_server_multi_proxy_prefixed
    minio = server["minio"]
    a, b = minio["buckets"]
    pkg = "acme-secrets"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _upload(server["upstream"], wheel)
    wait_for_file_in_index(server["upstream"]["simple"], pkg, wheel.name)

    claim = json.dumps({"origin": "private", "nonce": "a" * 32})
    minio_put_key_in(minio, b, f"packages/{pkg}/.origin", claim)
    assert not minio_key_exists_in(minio, a, f"packages/{pkg}/.origin")

    code, _, _ = http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}")
    assert code == 404
    assert not minio_key_exists_in(minio, a, f"packages/{pkg}/{wheel.name}")
    claim_key = f"packages/{pkg}/.origin"
    _eventually(
        lambda: (
            minio_key_exists_in(minio, a, claim_key) and _claim_owner(minio, a, pkg) == "private"
        ),
        timeout=30,
        what="origin-only private claim converges without an artifact marker",
    )
