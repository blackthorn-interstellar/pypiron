"""Multi-bucket replication and selection.

Real pypiron processes share two or three buckets on MinIO. Some tests give two
nodes independent reachability views so both writable sides of a partition are
exercised. Assertions inspect each bucket through `mc`, independent of what a
server reports.

Covered: fan-out, durable backlog/drain, per-bucket indexes, failover under an
upload storm, hysteresis/flap damping, bidirectional partition heal, duplicate
conflict quarantine, tombstone/yank/status convergence, proxy-to-private
demotion, cold-start divergence, origin-only claims, reserved namespaces, and
three-bucket fallback.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import subprocess
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pytest

from .conftest import (
    _s3_env,
    _start_s3_server,
    minio_delete_key_in,
    minio_get_key_bytes_in,
    minio_get_key_in,
    minio_key_exists_in,
    minio_list_keys_in,
    minio_make_bucket,
    minio_object_sha256,
    minio_put_key_in,
    minio_remove_bucket,
    s3_buckets_uri,
)
from .helpers import (
    ACCEPT_PEP691,
    find_free_port,
    http_get,
    http_get_no_redirect,
    http_request_auth,
    kill_process_tree,
    make_wheel,
    origin_owner,
    run_returncode,
    upload_legacy,
    wait_for_file_in_index,
    wait_http_ok,
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


def _bare(name: str) -> str:
    """Strip a bucket identity's scheme prefix (`s3://`, `gs://`, `az://`) so a
    metric label compares equal to the plain MinIO bucket name a test holds."""
    _, _, rest = name.partition("://")
    return rest or name


def _selected_bucket(server) -> str:
    selected = [
        match.group(1)
        for line in _metrics(server)
        if (match := _SELECTED_RE.match(line)) and match.group(2) == "1"
    ]
    assert len(selected) == 1, f"expected one selected bucket, got {selected}"
    return _bare(selected[0])


def _bucket_health(server, bucket: str) -> int:
    for line in _metrics(server):
        match = _HEALTH_RE.match(line)
        if match and _bare(match.group(1)) == bucket:
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
        if match and _bare(match.group(1)) == bucket:
            return int(match.group(2))
    raise AssertionError(f"missing marker backlog metric for {bucket}")


def _counter_value(server, name: str) -> int:
    prefix = f"{name} "
    for line in _metrics(server):
        if line.startswith(prefix):
            return int(line.removeprefix(prefix))
    raise AssertionError(f"missing counter metric {name}")


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


def test_ready_degrades_only_when_all_buckets_down_health_stays_up(s3_server_multi_failover):
    """The health split under failover. /health is pure liveness — 200 while the
    process serves, no storage I/O. /ready tracks the read path: one bucket down
    and the survivor still serves reads, so /ready holds 200; both down and no
    node can serve reads, so /ready turns 503 degraded while /health stays 200
    (liveness must not flap, or Kubernetes would restart the pod mid-outage);
    recover and /ready returns."""
    server = s3_server_multi_failover
    base = server["base_url"]

    def health() -> tuple[int, dict]:
        code, body, _ = http_get(f"{base}/health", timeout=3)
        return code, (json.loads(body) if body else {})

    def ready() -> tuple[int, dict]:
        code, body, _ = http_get(f"{base}/ready", timeout=3)
        return code, (json.loads(body) if body else {})

    # One bucket down: the node fails over and the survivor serves reads.
    a, b = _fail_to_second(server)
    assert _bucket_health(server, a) == -1, "the downed bucket reports unhealthy"
    assert health() == (200, {"status": "ok"})
    assert ready() == (200, {"status": "ready"}), "the survivor keeps this node ready"

    # Both buckets down: no bucket can serve reads, so /ready degrades — but the
    # process is still up, so /health must not flap.
    server["faults"].fail(b)
    _eventually(
        lambda: ready() == (503, {"status": "degraded"}),
        timeout=30,
        what="/ready degrades to 503 when no bucket can serve reads",
    )
    assert health() == (200, {"status": "ok"}), "liveness holds while both buckets are down"

    # Recovery: a bucket serves reads again, so /ready comes back.
    server["faults"].recover(a, b)
    _eventually(
        lambda: ready() == (200, {"status": "ready"}),
        timeout=30,
        what="/ready recovers once a bucket serves reads again",
    )
    assert health() == (200, {"status": "ok"})


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
    # Existence is not convergence: the fan-out must land the exact source bytes.
    # Artifact bytes equal the uploaded wheel; sidecar and PEP 658 companion bytes
    # equal the selected bucket's, so every bucket serves an identical record.
    wheel_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
    assert minio_object_sha256(minio, b, akey) == wheel_sha
    assert minio_object_sha256(minio, b, akey) == minio_object_sha256(minio, a, akey)
    assert minio_object_sha256(minio, b, f"{akey}.meta.json") == minio_object_sha256(
        minio, a, f"{akey}.meta.json"
    )
    assert minio_object_sha256(minio, b, f"{akey}.metadata") == minio_object_sha256(
        minio, a, f"{akey}.metadata"
    )
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
    # The drained record must be the source bytes, not merely a present key.
    wheel_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
    _eventually(
        lambda: minio_object_sha256(minio, b, akey) == wheel_sha,
        what="drained artifact bytes match the source upload",
    )
    assert minio_object_sha256(minio, b, f"{akey}.meta.json") == minio_object_sha256(
        minio, a, f"{akey}.meta.json"
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


def test_byte_conflict_keeps_first_upload_and_quarantines_loser(s3_servers_multi, tmp_path):
    """An ordered private/private conflict keeps the older upload everywhere.

    The losing bytes remain recoverable under quarantine and the alarm counter
    records that human review is required.
    """
    cluster = s3_servers_multi
    server = cluster["left"]
    minio = cluster["minio"]
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

    winner = wheel.read_bytes()
    winner_sha = hashlib.sha256(winner).hexdigest()
    winner_sidecar = json.loads(minio_get_key_in(minio, a, f"{akey}.meta.json"))
    winner_epoch = winner_sidecar["upload-epoch-ms"]

    # Isolate the two bucket views while manufacturing the otherwise-rare
    # partition conflict, so reconcile cannot observe a half-written record.
    _partition_nodes(cluster)

    # Manufacture the later side of a partition conflict. Sidecar first and
    # artifact last mirrors the server's crash-safe publish ordering.
    other = "these are different bytes than the wheel A published"
    other_sha = hashlib.sha256(other.encode()).hexdigest()
    sidecar = json.dumps(
        {
            "sha256": other_sha,
            "size": len(other),
            "version": "4.0",
            "upload-time": "2020-01-01T00:00:00Z",
            "upload-epoch-ms": winner_epoch + 3_000,
            "yanked": False,
            "origin": "private",
        }
    )
    minio_put_key_in(minio, b, f"{akey}.meta.json", sidecar)
    minio_put_key_in(minio, b, akey, other)
    _heal_nodes(cluster, a, b)

    loser_key = f"_quarantine/{pkg}/{wheel.name}@{other_sha[:12]}"
    _eventually(
        lambda: minio_key_exists_in(minio, b, loser_key),
        what="later bucket B body quarantined",
    )
    assert minio_get_key_bytes_in(minio, b, loser_key) == other.encode()
    _eventually(
        lambda: minio_get_key_bytes_in(minio, b, akey) == winner,
        what="older bucket A bytes replace the loser on B",
    )
    assert minio_get_key_bytes_in(minio, a, akey) == winner
    assert not minio_key_exists_in(minio, a, f"{akey}.frozen")
    assert not minio_key_exists_in(minio, b, f"{akey}.frozen")
    _eventually(
        lambda: _index_has_file(minio, a, pkg, wheel.name),
        what="winner remains indexed on A",
    )
    _eventually(
        lambda: _index_has_file(minio, b, pkg, wheel.name),
        what="winner remains indexed on B",
    )

    # Filename immutability still rejects a duplicate, while reads keep serving
    # the first-uploaded bytes rather than suppressing the filename.
    code, _ = upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
        expect_status=409,
    )
    assert code == 409
    code, body, _ = http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}", timeout=10)
    assert code == 200
    assert hashlib.sha256(body).hexdigest() == winner_sha
    _eventually(
        lambda: (
            max(
                _counter_value(cluster["left"], "pypiron_replication_conflict_quarantines_total"),
                _counter_value(cluster["right"], "pypiron_replication_conflict_quarantines_total"),
            )
            >= 1
        ),
        what="byte-conflict alarm counter increments",
    )


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


def test_partitioned_duplicate_uploads_keep_first_after_heal(s3_servers_multi, tmp_path):
    """Two partitioned uploads converge to the older bytes with an alarm."""
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

    _upload(cluster["left"], left_wheel)
    # The tiebreak deliberately distrusts clocks within two seconds. Keep this
    # blackbox conflict outside that skew window so it exercises first-wins.
    time.sleep(2.1)
    _upload(cluster["right"], right_wheel)

    key = f"packages/{pkg}/{left_wheel.name}"
    assert minio_key_exists_in(minio, a, key)
    assert minio_key_exists_in(minio, b, key)
    left_sidecar = json.loads(minio_get_key_in(minio, a, f"{key}.meta.json"))
    right_sidecar = json.loads(minio_get_key_in(minio, b, f"{key}.meta.json"))
    assert right_sidecar["upload-epoch-ms"] - left_sidecar["upload-epoch-ms"] > 2_000

    winner = left_wheel.read_bytes()
    loser = right_wheel.read_bytes()
    loser_sha = hashlib.sha256(loser).hexdigest()
    loser_key = f"_quarantine/{pkg}/{left_wheel.name}@{loser_sha[:12]}"
    _heal_nodes(cluster, a, b)
    for bucket in (a, b):
        _eventually(
            lambda bucket=bucket: minio_get_key_bytes_in(minio, bucket, key) == winner,
            timeout=45,
            what=f"first-uploaded bytes converge in {bucket}",
        )
        _eventually(
            lambda bucket=bucket: _index_has_file(minio, bucket, pkg, left_wheel.name),
            timeout=45,
            what=f"winning duplicate remains indexed in {bucket}",
        )
        assert not minio_key_exists_in(minio, bucket, f"{key}.frozen")

    _eventually(
        lambda: minio_key_exists_in(minio, b, loser_key),
        timeout=45,
        what="later upload is preserved in B's quarantine",
    )
    assert minio_get_key_bytes_in(minio, b, loser_key) == loser
    _eventually(
        lambda: (
            max(
                _counter_value(cluster["left"], "pypiron_replication_conflict_quarantines_total"),
                _counter_value(cluster["right"], "pypiron_replication_conflict_quarantines_total"),
            )
            >= 1
        ),
        timeout=45,
        what="partition-conflict alarm counter increments",
    )

    _eventually(
        lambda: _selected_bucket(cluster["left"]) == a,
        timeout=30,
        what="left reads the winner from A",
    )
    code, body, _ = http_get(
        f"{cluster['left']['base_url']}/files/{pkg}/{left_wheel.name}", timeout=10
    )
    assert code == 200
    assert body == winner
    cluster["right"]["faults"].fail(a)
    _eventually(
        lambda: _selected_bucket(cluster["right"]) == b,
        timeout=30,
        what="right reads the winner from B",
    )
    code, body, _ = http_get(
        f"{cluster['right']['base_url']}/files/{pkg}/{right_wheel.name}", timeout=10
    )
    assert code == 200
    assert body == winner
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
    sidecar = json.loads(
        minio_get_key_in(cluster["minio"], bucket, f"packages/{pkg}/{wheel.name}.meta.json")
    )
    assert "upload-epoch-ms" not in sidecar


def test_proxy_fill_vs_private_upload_demotes_and_quarantines(s3_servers_multi_proxy, tmp_path):
    """Private bytes supersede a same-filename mirror fill per artifact.

    The operations overlap while each node can reach only its selected bucket.
    On heal B's claim is demoted, the mirror bytes are preserved under
    quarantine, and the private record lands directly without a promotion
    barrier.
    """
    cluster = s3_servers_multi_proxy
    minio = cluster["minio"]
    pkg = "demotionrace"
    mirror_wheel = make_wheel(pkg, "1.0", tmp_path / "mirror", description="upstream mirror build")
    private_wheel = make_wheel(pkg, "1.0", tmp_path / "private", description="private build")
    assert mirror_wheel.name == private_wheel.name
    assert mirror_wheel.read_bytes() != private_wheel.read_bytes()
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

    artifact_key = f"packages/{pkg}/{private_wheel.name}"
    mirror_sha = hashlib.sha256(mirror_wheel.read_bytes()).hexdigest()
    quarantine_key = f"_quarantine/{pkg}/{mirror_wheel.name}@{mirror_sha[:12]}"
    _heal_nodes(cluster, a, b)
    _eventually(
        lambda: _claim_owner(minio, b, pkg) == "private",
        timeout=45,
        what="B's mirror claim is demoted to private",
    )
    _eventually(
        lambda: minio_get_key_bytes_in(minio, b, artifact_key) == private_wheel.read_bytes(),
        timeout=45,
        what="private bytes supersede the mirror body in B",
    )
    _eventually(
        lambda: minio_key_exists_in(minio, b, quarantine_key),
        timeout=45,
        what="B preserves the mirror loser under quarantine",
    )
    assert minio_get_key_bytes_in(minio, b, quarantine_key) == mirror_wheel.read_bytes()
    assert minio_get_key_bytes_in(minio, a, artifact_key) == private_wheel.read_bytes()
    _eventually(
        lambda: not minio_key_exists_in(minio, b, f"{artifact_key}.mirror-quarantined"),
        timeout=45,
        what="supersede clears B's inert mirror marker",
    )
    _eventually(
        lambda: _index_has_file(minio, b, pkg, private_wheel.name),
        timeout=45,
        what="B indexes the private winner after demotion",
    )
    cluster["right"]["faults"].fail(a)
    _eventually(
        lambda: _selected_bucket(cluster["right"]) == b,
        timeout=30,
        what="right node selects the converged bucket B",
    )
    code, body, _ = http_get(
        f"{cluster['right']['base_url']}/files/{pkg}/{private_wheel.name}", timeout=10
    )
    assert code == 200
    assert body == private_wheel.read_bytes()
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
    left_body = "private bytes from A"
    right_body = "private bytes from B"
    _seed_private_record(minio, a, left_pkg, left_filename, left_body)
    _seed_private_record(minio, b, right_pkg, right_filename, right_body)

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
        for bucket, pkg, filename, body in (
            (b, left_pkg, left_filename, left_body),
            (a, right_pkg, right_filename, right_body),
        ):
            key = f"packages/{pkg}/{filename}"
            _eventually(
                lambda bucket=bucket, key=key: minio_key_exists_in(minio, bucket, key),
                timeout=45,
                what=f"cold-start diff copies {pkg} into {bucket}",
            )
            # The repaired copy must carry the seeded bytes, not just occupy the
            # key, and its sidecar must describe those exact bytes.
            body_sha = hashlib.sha256(body.encode()).hexdigest()
            _eventually(
                lambda bucket=bucket, key=key, body_sha=body_sha: (
                    minio_object_sha256(minio, bucket, key) == body_sha
                ),
                timeout=45,
                what=f"cold-start diff copies {pkg}'s exact bytes into {bucket}",
            )
            dest_sidecar = json.loads(minio_get_key_in(minio, bucket, f"{key}.meta.json"))
            assert dest_sidecar["sha256"] == body_sha
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
            assert stamp["buckets"] == [f"s3://{name}" for name in minio["buckets"]]
            assert stamp["generation"] == expected_generation


def test_migrate_refuses_to_drop_a_removed_bucket_holding_notes_then_succeeds(
    minio_three, pypiron_bin
):
    """Shrinking the list must not strand a `_repl/` note on the bucket being
    removed: a stranded note there can be a record's sole copy. Migrate refuses
    while the removed bucket holds one, then succeeds once it is drained."""
    minio = minio_three
    a, b, c = minio["buckets"]
    env = _s3_env(minio, "127.0.0.1:0")

    # Establish the [a, b, c] topology.
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b, c)
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=30)
    assert rc == 0, f"initial migration failed:\n{out}\n{err}"

    # A fan-out note that only bucket c (the one we are about to drop) holds:
    # `_repl/<dest>/<pkg>/<file>!<nonce>`.
    note_key = "_repl/0/lonelypkg/lonelypkg-1.0-py3-none-any.whl!stranded"
    minio_put_key_in(minio, c, note_key, "")

    # Shrinking to [a, b] must be refused: c is being removed but still holds a note.
    shrink_env = _s3_env(minio, "127.0.0.1:0")
    shrink_env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b)
    rc, out, err = run_returncode(
        [str(pypiron_bin), "buckets", "migrate"], env=shrink_env, timeout=30
    )
    assert rc != 0, f"migrate dropped a removed bucket that still held notes:\n{out}\n{err}"
    assert c in (out + err) and "undrained" in (out + err), f"unexpected error:\n{out}\n{err}"
    # The topology was not shrunk: a and b still carry the three-bucket stamp.
    for bucket in (a, b):
        stamp = json.loads(minio_get_key_in(minio, bucket, "_topology/stamp.json"))
        assert stamp["buckets"] == [f"s3://{name}" for name in (a, b, c)]

    # Drain the note; the shrink now succeeds.
    minio_delete_key_in(minio, c, note_key)
    rc, out, err = run_returncode(
        [str(pypiron_bin), "buckets", "migrate"], env=shrink_env, timeout=30
    )
    assert rc == 0, f"migrate refused a clean shrink:\n{out}\n{err}"
    for bucket in (a, b):
        stamp = json.loads(minio_get_key_in(minio, bucket, "_topology/stamp.json"))
        assert stamp["buckets"] == [f"s3://{a}", f"s3://{b}"]


def test_migrate_refuses_to_drop_a_bucket_holding_unique_content_unless_forced(
    minio_three, pypiron_bin
):
    """The removal gate blind spot: `_repl/` notes prove nothing about content
    the fleet never fanned out (an out-of-band seed, an unconverged backfill).
    Migrate must diff the departing bucket's `packages/` against the survivors
    and refuse to drop the sole copy of any artifact — `--force` accepts the
    loss."""
    minio = minio_three
    a, b, c = minio["buckets"]
    env = _s3_env(minio, "127.0.0.1:0")
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b, c)
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=30)
    assert rc == 0, f"initial migration failed:\n{out}\n{err}"

    # An artifact only bucket c holds — its sole copy in the fleet.
    unique_key = "packages/onlyonc/onlyonc-1.0-py3-none-any.whl"
    minio_put_key_in(minio, c, unique_key, "sole copy bytes")
    # A second artifact that IS replicated onto a survivor: it must not be named.
    shared_key = "packages/shared/shared-1.0-py3-none-any.whl"
    minio_put_key_in(minio, c, shared_key, "shared bytes")
    minio_put_key_in(minio, b, shared_key, "shared bytes")

    shrink_env = _s3_env(minio, "127.0.0.1:0")
    shrink_env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b)
    rc, out, err = run_returncode(
        [str(pypiron_bin), "buckets", "migrate"], env=shrink_env, timeout=60
    )
    assert rc != 0, f"migrate dropped a bucket holding the only copy:\n{out}\n{err}"
    blob = out + err
    assert c in blob and "only copy" in blob, f"unexpected error:\n{blob}"
    assert unique_key in blob and "--force" in blob, f"error must name the artifact:\n{blob}"
    assert shared_key not in blob, f"a replicated artifact must not be named:\n{blob}"
    # Refused: the three-bucket stamp still stands.
    for bucket in (a, b):
        stamp = json.loads(minio_get_key_in(minio, bucket, "_topology/stamp.json"))
        assert stamp["buckets"] == [f"s3://{x}" for x in (a, b, c)]

    # --force accepts the data loss and drops c.
    rc, out, err = run_returncode(
        [str(pypiron_bin), "buckets", "migrate", "--force"], env=shrink_env, timeout=60
    )
    assert rc == 0, f"--force did not override the content gate:\n{out}\n{err}"
    for bucket in (a, b):
        stamp = json.loads(minio_get_key_in(minio, bucket, "_topology/stamp.json"))
        assert stamp["buckets"] == [f"s3://{a}", f"s3://{b}"]


def test_migrate_allows_dropping_a_fully_replicated_bucket(minio_three, pypiron_bin):
    """When every artifact the departing bucket holds also lives on a surviving
    bucket, the content diff is clean and the shrink needs no `--force`."""
    minio = minio_three
    a, b, c = minio["buckets"]
    env = _s3_env(minio, "127.0.0.1:0")
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b, c)
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=30)
    assert rc == 0, f"initial migration failed:\n{out}\n{err}"

    # Everything c holds is also on a survivor.
    key = "packages/replicated/replicated-1.0-py3-none-any.whl"
    minio_put_key_in(minio, c, key, "replicated bytes")
    minio_put_key_in(minio, a, key, "replicated bytes")

    shrink_env = _s3_env(minio, "127.0.0.1:0")
    shrink_env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b)
    rc, out, err = run_returncode(
        [str(pypiron_bin), "buckets", "migrate"], env=shrink_env, timeout=60
    )
    assert rc == 0, f"migrate refused a safe shrink:\n{out}\n{err}"
    for bucket in (a, b):
        stamp = json.loads(minio_get_key_in(minio, bucket, "_topology/stamp.json"))
        assert stamp["buckets"] == [f"s3://{a}", f"s3://{b}"]


def test_migrate_seeds_backfill_sentinel_and_reconcile_drains_it(
    minio_three, pypiron_bin, tmp_path_factory
):
    """Adding a bucket seeds an O(1) `_repl/<dest>/_backfill!<nonce>` sentinel on
    every existing peer, so the fresh bucket serves no region reads until the
    corpus converges. A clean reconcile pass — every bucket proven caught up —
    drains it."""
    minio = minio_three
    a, b, c = minio["buckets"]
    env = _s3_env(minio, "127.0.0.1:0")

    # Establish a two-bucket fleet [a, b].
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b)
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=30)
    assert rc == 0, f"initial migration failed:\n{out}\n{err}"

    # Add c (new index 2): the sentinel lands on both peers, none on c itself.
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b, c)
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=30)
    assert rc == 0, f"adding a bucket failed:\n{out}\n{err}"

    def _sentinels(bucket) -> list[str]:
        return [k for k in minio_list_keys_in(minio, bucket) if k.startswith("_repl/2/_backfill!")]

    for peer in (a, b):
        assert _sentinels(peer), f"no backfill sentinel seeded on peer {peer}"
    assert not [k for k in minio_list_keys_in(minio, c) if k.startswith("_repl/")], (
        "the added bucket owes itself no note"
    )

    # Bring up the three-bucket fleet; the first clean reconcile drains it.
    server_gen = _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        extra_env={
            "PYPIRON_RECONCILE_INTERVAL_SECS": "2",
            "PYPIRON_AUDIT_ON_BOOT": "false",
        },
    )
    next(server_gen)
    try:
        _eventually(
            lambda: not _sentinels(a) and not _sentinels(b),
            timeout=30,
            what="reconcile drains the backfill sentinel once the fleet converges",
        )
    finally:
        server_gen.close()


def test_migrate_single_to_multi_fences_the_new_empty_bucket(minio_two, pypiron_bin):
    """The single-bucket -> multi-bucket expansion. Single mode never writes a
    topology stamp, so `buckets migrate` has no previous member list to diff and
    cannot learn which bucket it added. It identifies the addition by content
    instead: the bucket already holding `packages/` is the established source and
    keeps serving region reads, while the new empty bucket is fenced by a backfill
    sentinel until a clean reconcile proves it caught up. Without this fence the
    fresh, empty region bucket serves reads the instant migrate returns -- the
    exact failover hole the sentinel exists to close."""
    minio = minio_two
    a, b = minio["buckets"]
    # 'a' is an existing single-bucket deployment: it holds a corpus and, because
    # single-bucket mode never stamps, carries no topology stamp.
    minio_put_key_in(minio, a, "packages/existing/existing-1.0-py3-none-any.whl", "bytes")
    env = _s3_env(minio, "127.0.0.1:0")
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b)
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=30)
    assert rc == 0, f"single->multi migration failed:\n{out}\n{err}"

    # The new empty bucket b (index 1) is fenced by a sentinel on its peer a.
    assert [k for k in minio_list_keys_in(minio, a) if k.startswith("_repl/1/_backfill!")], (
        f"single->multi seeded no backfill sentinel for the new empty bucket:\n{out}\n{err}"
    )
    # The established corpus bucket a (index 0) is fenced nowhere: it keeps serving.
    for bucket in (a, b):
        assert not [k for k in minio_list_keys_in(minio, bucket) if k.startswith("_repl/0/")], (
            "the bucket that already holds the corpus must keep serving region reads"
        )
    # The new bucket owes itself no note.
    assert not [k for k in minio_list_keys_in(minio, b) if k.startswith("_repl/")]


def test_backfill_sentinel_does_not_wedge_a_later_migrate(minio_three, pypiron_bin):
    """A seeded backfill sentinel is an empty gate marker, not a stranded repair
    note: it must never block the next `buckets migrate`. Adding a third bucket
    while the second's sentinel is still undrained (no server has reconciled yet)
    succeeds, seeds the third's fence, and leaves the earlier one in place -- the
    fence's durability does not depend on the added-set being recomputed."""
    minio = minio_three
    a, b, c = minio["buckets"]
    # 'a' holds a corpus; establish [a, b] so b is fenced by a live sentinel.
    minio_put_key_in(minio, a, "packages/existing/existing-1.0-py3-none-any.whl", "bytes")
    env = _s3_env(minio, "127.0.0.1:0")
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b)
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=30)
    assert rc == 0, f"establishing [a, b] failed:\n{out}\n{err}"
    assert [k for k in minio_list_keys_in(minio, a) if k.startswith("_repl/1/_backfill!")]

    # Add c while b's sentinel is still undrained.
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b, c)
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=30)
    assert rc == 0, f"a live backfill sentinel wedged the next migrate:\n{out}\n{err}"
    # c (index 2) is now fenced on both its peers, and b's earlier fence survives.
    for bucket in (a, b):
        assert [
            k for k in minio_list_keys_in(minio, bucket) if k.startswith("_repl/2/_backfill!")
        ], f"no sentinel for the newly-added bucket c on peer {bucket}"
    assert [k for k in minio_list_keys_in(minio, a) if k.startswith("_repl/1/_backfill!")], (
        "the earlier sentinel for b was lost across the second migrate"
    )


def test_fresh_startup_repairs_member_that_missed_topology_migration(
    minio_three_proxy, pypiron_bin, tmp_path_factory
):
    """A recovered lower-generation member cannot brick the next deployment."""
    minio = minio_three_proxy
    a, b, c = minio["buckets"]
    env = _s3_env(minio, "127.0.0.1:0")
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b)
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=30)
    assert rc == 0, f"initial topology migration failed:\n{out}\n{err}"

    minio["faults"].fail(b)
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b, c)
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
        assert stamp["buckets"] == [f"s3://{name}" for name in (a, b, c)]
        assert stamp["generation"] == 2
        assert _selected_bucket(server) == a
    finally:
        server_gen.close()


def test_single_entry_buckets_keeps_single_bucket_guarantees(
    minio, tmp_path_factory, pypiron_bin, tmp_path
):
    """A one-entry `--buckets s3://x` list is ordinary single-bucket mode: it
    writes no topology stamp object and still serves presigned redirects — the
    old single-bucket guarantees survive the move off the singular flag."""
    server_gen = _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        extra_env={"PYPIRON_ARTIFACT_DELIVERY": "redirect"},
    )
    server = next(server_gen)
    try:
        bucket = minio["bucket"]
        pkg = "singlebucket"
        wheel = make_wheel(pkg, "1.0", tmp_path)
        upload_legacy(server["legacy"], wheel, username=server["user"], password=server["password"])
        wait_for_file_in_index(server["simple"], pkg, wheel.name)

        # Single-bucket S3 still signs artifact GETs (302 to a presigned URL).
        code, _, headers = http_get_no_redirect(f"{server['base_url']}/files/{pkg}/{wheel.name}")
        assert code == 302, f"expected a presigned redirect, got {code}"
        assert "X-Amz-Signature" in headers["location"], "single-bucket URL must be presigned"

        # No topology stamp: single-bucket mode never stamps a topology identity.
        assert not minio_key_exists_in(minio, bucket, "_topology/stamp.json"), (
            "single-bucket mode must not write a topology stamp"
        )
    finally:
        server_gen.close()


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


# ======================= Crash-mid-fan-out kill-point sweep ====================
#
# The single-bucket disk/S3 kill-point sweeps (test_crash_consistency.py) crash
# across one bucket's write protocol. This extends the pattern to the v2 pre-ack
# fan-out on a 2-bucket topology: sweep PYPIRON_FAULT_ABORT_AFTER_WRITES over a
# private upload so the process aborts in every gap of the *selected* bucket's
# write protocol (origin claim, sidecar, companions, artifact-last), the pre-ack
# repair-note writes, and the post-ack index writes. After each crash a clean
# node restarts with the boot audit, full-diff reconcile, and `_repl/` sweep on;
# both buckets must converge to the same complete byte-equal record or a clean
# absence, and neither may serve a half-record.
#
# Reachability note (reported per the task cap): FaultInjectStorage wraps bucket
# 0 only — the selected/source bucket, which is never itself a fan-out
# destination (src.rs `fanout_sync` skips `src_index`). A process-abort therefore
# cannot land *inside* a secondary copy (mid-multipart, or after the secondary
# sidecar before its artifact); those bucket-1 writes are uncounted. That window
# is a graceful secondary failure rather than a crash, and is already covered by
# the fault-proxy note-and-heal tests (`test_repl_marker_accumulates_then_drains`
# and the partition suite). This sweep owns the selected-write and pre-ack
# windows the existing machinery can reach.

# Bounded so the whole-file gate stays under its time cap; each point still
# recovers and verifies both buckets independently. The reachable windows are
# bucket 0's (see the reachability note above), and these span boot init through
# the selected-write protocol and the post-ack index writes.
MULTIBUCKET_CRASH_KILL_POINTS = range(1, 7)


def _start_multibucket_node(
    pypiron_bin, minio, tmp_path, label, *, fault_after=None, extra_env=None
):
    """Launch one pypiron node against the two-bucket topology. Returns as soon
    as the node answers or the process exits — a fault node may self-abort during
    boot writes, and recovery proceeds either way."""
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _s3_env(minio, bind)
    env["PYPIRON_AUDIT_ON_BOOT"] = "false"
    if extra_env:
        env.update(extra_env)
    if fault_after is not None:
        env["PYPIRON_FAULT_ABORT_AFTER_WRITES"] = str(fault_after)
    log_path = tmp_path / f"{label}.log"
    log = open(log_path, "w")
    proc = subprocess.Popen(
        [str(pypiron_bin), "serve"], env=env, stdout=log, stderr=subprocess.STDOUT
    )
    base = f"http://{bind}"
    node = {
        "proc": proc,
        "base_url": base,
        "legacy": f"{base}/legacy/",
        "simple": f"{base}/simple/",
        "log_path": log_path,
        "user": "admin",
        "password": "secret",
    }
    deadline = time.time() + 20
    while time.time() < deadline and proc.poll() is None:
        try:
            http_get(f"{base}/simple/index.json", timeout=1)
            break
        except (ConnectionError, OSError):
            time.sleep(0.2)
    return node


def _sigkill(proc) -> None:
    """Ungraceful death: SIGKILL, no drain, no lease release."""
    try:
        os.kill(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def _verify_bucket_rc(pypiron_bin, minio, bucket) -> int:
    """Run the read-only `verify-index` oracle against one bucket in isolation.
    verify builds only the default handle, so a single-bucket env points it at
    exactly `bucket`: exit 0 means that bucket's served views match its own truth
    (no sidecar/index without a matching artifact — no half-record)."""
    env = _s3_env(minio, "127.0.0.1:0")
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(bucket)
    env["PYPIRON_AUDIT_ON_BOOT"] = "false"
    cp = subprocess.run(
        [str(pypiron_bin), "verify-index"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return cp.returncode


def _multibucket_converged(pypiron_bin, minio, a, b, pkg, filename, wheel_sha, *, acked) -> bool:
    """Both buckets hold the same complete byte-equal record or a clean absence,
    and neither serves a half-record."""
    akey = f"packages/{pkg}/{filename}"
    a_has = minio_key_exists_in(minio, a, akey)
    b_has = minio_key_exists_in(minio, b, akey)
    if a_has != b_has:
        return False
    if acked and not a_has:
        # A 200 promised a durable record; a clean absence would be a lie.
        return False
    if a_has:
        if minio_object_sha256(minio, a, akey) != wheel_sha:
            return False
        if minio_object_sha256(minio, b, akey) != wheel_sha:
            return False
        for bucket in (a, b):
            if not minio_key_exists_in(minio, bucket, f"{akey}.meta.json"):
                return False
            if not _index_has_file(minio, bucket, pkg, filename):
                return False
    # Only run the (subprocess) oracle once cheap byte-convergence already holds.
    return (
        _verify_bucket_rc(pypiron_bin, minio, a) == 0
        and _verify_bucket_rc(pypiron_bin, minio, b) == 0
    )


def test_multibucket_crash_during_fanout_upload_converges(minio_two, pypiron_bin, tmp_path):
    """SIGABRT the node in each gap of the selected-write + pre-ack fan-out, then
    prove both buckets converge — same complete byte-equal record or clean
    absence, never a served half-record, and an acked upload never vanishes."""
    minio = minio_two
    a, b = minio["buckets"]
    for kill_point in MULTIBUCKET_CRASH_KILL_POINTS:
        pkg = f"fanoutcrash{kill_point}"
        filename = f"{pkg}-1.0-py3-none-any.whl"
        wheel = make_wheel(pkg, "1.0", tmp_path)
        wheel_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()

        node = _start_multibucket_node(
            pypiron_bin, minio, tmp_path, f"crash-{kill_point}", fault_after=kill_point
        )
        acked = False
        try:
            upload_legacy(node["legacy"], wheel, username="admin", password="secret", timeout=10)
            acked = True
        except (RuntimeError, ConnectionError, OSError):
            acked = False
        # Give the nudged worker a moment to walk into the kill point too.
        time.sleep(1.2)
        _sigkill(node["proc"])

        recovery = _start_multibucket_node(
            pypiron_bin,
            minio,
            tmp_path,
            f"recover-{kill_point}",
            extra_env={
                "PYPIRON_AUDIT_ON_BOOT": "true",
                "PYPIRON_RECONCILE_INTERVAL_SECS": "3",
                "PYPIRON_REPL_SWEEP_INTERVAL_SECS": "2",
            },
        )
        try:
            _eventually(
                lambda: _multibucket_converged(
                    pypiron_bin, minio, a, b, pkg, filename, wheel_sha, acked=acked
                ),
                timeout=90,
                interval=2.0,
                what=(
                    f"both buckets converge after crash at kill point {kill_point} (acked={acked})"
                ),
            )
        finally:
            kill_process_tree(recovery["proc"])


# ========================= Startup topology refusal ===========================


def _serve_exits(pypiron_bin, minio, bucket_names, *, timeout=30):
    """Launch `serve` with an explicit bucket list and wait for it to exit.
    Returns (returncode, combined_output). Fails the test if it stays up — the
    topology check runs before the bind, so a refusal is fast."""
    port = find_free_port()
    env = _s3_env(minio, f"127.0.0.1:{port}")
    env["PYPIRON_BUCKETS"] = s3_buckets_uri(*bucket_names)
    env["PYPIRON_AUDIT_ON_BOOT"] = "false"
    proc = subprocess.Popen(
        [str(pypiron_bin), "serve"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        out, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        kill_process_tree(proc)
        out, _ = proc.communicate(timeout=5)
        raise AssertionError(f"serve did not refuse to start; it stayed up:\n{out}")
    return proc.returncode, out


def test_changed_bucket_list_without_migrate_refuses_to_start(minio_three, pypiron_bin, tmp_path):
    """A node whose configured bucket list no longer matches the fleet's stamp —
    a bucket added or the order changed without running `buckets migrate` — must
    refuse to start. The matching list is the positive control and boots."""
    minio = minio_three
    a, b, c = minio["buckets"]

    # Stamp the fleet at generation 1 for the [a, b] topology.
    migrate_env = _s3_env(minio, "127.0.0.1:0")
    migrate_env["PYPIRON_BUCKETS"] = s3_buckets_uri(a, b)
    rc, out, err = run_returncode(
        [str(pypiron_bin), "buckets", "migrate"], env=migrate_env, timeout=30
    )
    assert rc == 0, f"initial topology migration failed:\n{out}{err}"

    # Positive control: the migrated list boots.
    node = _start_multibucket_node(
        pypiron_bin,
        minio,
        tmp_path,
        "same-list",
        extra_env={"PYPIRON_BUCKETS": s3_buckets_uri(a, b)},
    )
    try:
        wait_http_ok(f"{node['base_url']}/simple/index.json", timeout=20)
        assert node["proc"].poll() is None, "server exited despite a matching topology"
    finally:
        kill_process_tree(node["proc"])

    # Negative: an added bucket without re-migrating refuses to start.
    rc, out = _serve_exits(pypiron_bin, minio, (a, b, c))
    assert rc != 0, f"adding a bucket without migrating should refuse to start:\n{out}"
    assert "different bucket topology" in out and "refusing to start" in out, out

    # Negative: a reordered list without re-migrating refuses to start.
    rc, out = _serve_exits(pypiron_bin, minio, (b, a))
    assert rc != 0, f"reordering buckets without migrating should refuse to start:\n{out}"
    assert "different bucket topology" in out, out


# ======================= Advisory control singletons ==========================
#
# The advisory feed (`_advisories/osv-pypi.zip`) and the worker-derived
# quarantined set (`_advisories/quarantined.json`) are leader-authored control
# SINGLETONS: written write-through to every healthy bucket and reseed-if-absent
# healed, so a failover to any bucket stays armed. These prove the fail-closed
# behavior the archetype (a failover clearing the byte gate) used to break.

from .test_advisories import canonical_records, make_osv_zip  # noqa: E402

_FEED_KEY = "_advisories/osv-pypi.zip"
_QUARANTINED_KEY = "_advisories/quarantined.json"


def _mirror_upload_s3(server, wheel) -> None:
    """Publish `wheel` as a mirror-origin file — the origin a byte-gate block
    requires (a private-origin name is never gated)."""
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
        fields={"mirror": "true"},
    )


def _poll_status(url: str, accept: set, *, timeout: float = 30.0):
    """Poll `url` (no redirect) until its status is in `accept`."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        code, body, _ = http_get_no_redirect(url, timeout=3)
        if code in accept:
            return code, body
        last = code
        time.sleep(0.3)
    raise AssertionError(f"{url} never returned {accept} within {timeout}s (last={last})")


def _assert_status_stays(url: str, accept: set, *, hold: float = 4.0) -> None:
    """Assert `url`'s status stays within `accept` across `hold` seconds."""
    deadline = time.time() + hold
    while time.time() < deadline:
        code, _, _ = http_get_no_redirect(url, timeout=3)
        assert code in accept, f"{url} returned {code}, expected one of {accept}"
        time.sleep(0.3)


def test_quarantine_survives_singleton_loss_across_two_buckets(
    minio_two, pypiron_bin, tmp_path, tmp_path_factory
):
    """A quarantined project's cached bytes stay refused on a 2-bucket fleet even
    after the quarantined-set singleton is deleted from BOTH buckets — the fail-
    open the archetype used to hit on failover. The set is written write-through
    to both buckets, an absent key retains the loaded set (never un-quarantines),
    and reseed-if-absent restores the singleton on both buckets."""
    minio = minio_two
    a, b = minio["buckets"]
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    server_gen = _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        extra_args=[
            "--advisory-feed",
            str(feed),
            "--malware-block",
            "true",
            "--reconcile-interval-secs",
            "2",
        ],
    )
    try:
        server = next(server_gen)
        base = server["base_url"]
        admin = {"username": server["user"], "password": server["password"]}

        pkg = "quarantinee"
        wheel = make_wheel(pkg, "1.0.0", tmp_path)
        _mirror_upload_s3(server, wheel)
        wait_for_file_in_index(server["simple"], pkg, wheel.name)

        file_url = f"{base}/files/{pkg}/{wheel.name}"
        # Clean mirror file serves before quarantine.
        _poll_status(file_url, {200, 302})

        # Relay a PEP 792 quarantine (as a sync would).
        code, _, _ = http_request_auth(
            "POST", f"{base}/project/{pkg}/status", data=b'{"status":"quarantined"}', **admin
        )
        assert code == 200, code

        # The byte gate refuses once the sweep derives the quarantined set.
        _poll_status(file_url, {403})

        # The singleton was written write-through to BOTH buckets.
        _eventually(
            lambda: (
                minio_key_exists_in(minio, a, _QUARANTINED_KEY)
                and minio_key_exists_in(minio, b, _QUARANTINED_KEY)
            ),
            what="quarantined.json replicated to both buckets",
        )

        # Delete the singleton from both buckets: a lost control file must NOT
        # un-block. The in-memory set is retained on an absent key, and reseed
        # restores the object — the download stays refused throughout.
        minio_delete_key_in(minio, a, _QUARANTINED_KEY)
        minio_delete_key_in(minio, b, _QUARANTINED_KEY)
        _assert_status_stays(file_url, {403})

        # reseed-if-absent heals the singleton back onto both buckets.
        _eventually(
            lambda: (
                minio_key_exists_in(minio, a, _QUARANTINED_KEY)
                and minio_key_exists_in(minio, b, _QUARANTINED_KEY)
            ),
            what="quarantined.json reseeded onto both buckets after deletion",
        )
    finally:
        server_gen.close()


def test_advisory_feed_reseeds_onto_a_starved_second_bucket(
    minio_two, pypiron_bin, tmp_path, tmp_path_factory
):
    """The advisory feed snapshot lands on every bucket: it is written through to
    the peer and reseed-if-absent restores it if a bucket loses it. A second
    bucket that never held the feed (or lost it) is healed without a resync — so
    a fresh node booting onto it comes up armed, not armed-but-unfed."""
    minio = minio_two
    a, b = minio["buckets"]
    feed = make_osv_zip(tmp_path / "osv.zip", canonical_records())
    server_gen = _start_s3_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        extra_args=[
            "--advisory-feed",
            str(feed),
            "--malware-block",
            "true",
            "--reconcile-interval-secs",
            "2",
        ],
    )
    try:
        next(server_gen)  # start the server; this test inspects buckets directly
        # The feed snapshot reaches both buckets (selected write + peer reseed).
        _eventually(
            lambda: (
                minio_key_exists_in(minio, a, _FEED_KEY)
                and minio_key_exists_in(minio, b, _FEED_KEY)
            ),
            what="advisory feed replicated to both buckets",
        )

        # Starve the second (peer) bucket, then confirm reseed restores it.
        minio_delete_key_in(minio, b, _FEED_KEY)
        assert not minio_key_exists_in(minio, b, _FEED_KEY)
        _eventually(
            lambda: minio_key_exists_in(minio, b, _FEED_KEY),
            what="advisory feed reseeded onto the starved second bucket",
        )
    finally:
        server_gen.close()
