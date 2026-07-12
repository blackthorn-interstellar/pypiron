"""Mixed-backend (multi-cloud) topology blackbox tests.

The multi-bucket replication and failover machinery is meant to be
backend-agnostic: a topology may mix S3, GCS, and Azure buckets. These tests
prove that end to end on a *mixed pair* — one S3 bucket (MinIO, fault-proxied)
and one Azure container (Azurite) — driven through the real binary over HTTP.

Azurite is the faithful local second cloud: fake-gcs-server does not implement
object_store's GCS data plane faithfully (see the GCS note in conftest.py), so
the GCS leg of the three-cloud claim is covered by the real-GCS job, not here.
Everything skips cleanly without Docker, like the single-cloud suites.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import time

import pytest

from .conftest import (
    _mixed_env,
    azurite_get_blob,
    azurite_key_exists,
    azurite_list_keys,
    azurite_object_sha256,
    minio_get_key_in,
    minio_key_exists_in,
    minio_object_sha256,
)
from .helpers import (
    find_free_port,
    http_get,
    http_request_auth,
    kill_process_tree,
    make_wheel,
    run_returncode,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = [pytest.mark.integration, pytest.mark.s3, pytest.mark.azure]

_SELECTED_RE = re.compile(r'^pypiron_bucket_selected\{bucket="([^"]+)",index="\d+"\} ([01])$')
_HEALTH_RE = re.compile(r'^pypiron_bucket_health_state\{bucket="([^"]+)",index="\d+"\} (-?1|0)$')


def _eventually(predicate, *, timeout: float = 40.0, interval: float = 0.3, what: str = ""):
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
    metric label compares equal to the plain bucket/container name a test holds."""
    _, _, rest = name.partition("://")
    return rest or name


def _selected_bucket(server) -> str:
    selected = [
        m.group(1)
        for line in _metrics(server)
        if (m := _SELECTED_RE.match(line)) and m.group(2) == "1"
    ]
    assert len(selected) == 1, f"expected one selected bucket, got {selected}"
    return _bare(selected[0])


def _bucket_health(server, bucket: str) -> int:
    for line in _metrics(server):
        m = _HEALTH_RE.match(line)
        if m and _bare(m.group(1)) == bucket:
            return int(m.group(2))
    raise AssertionError(f"missing health metric for {bucket}")


def _upload(server, wheel):
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
        timeout=20,
    )


def _names(mixed) -> tuple[str, str]:
    """(S3 bucket, Azure container) — the topology in preference order."""
    return mixed["s3"]["bucket"], mixed["azure"]["container"]


def _s3_index_has_file(minio, bucket, pkg, filename) -> bool:
    key = f"simple/{pkg}/index.json"
    if not minio_key_exists_in(minio, bucket, key):
        return False
    data = json.loads(minio_get_key_in(minio, bucket, key))
    return any(f.get("filename") == filename for f in data.get("files", []))


def _azure_index_has_file(azure, pkg, filename) -> bool:
    body = azurite_get_blob(azure, f"simple/{pkg}/index.json")
    if body is None:
        return False
    data = json.loads(body)
    return any(f.get("filename") == filename for f in data.get("files", []))


# ------------------------------- topology CAS ---------------------------------


def test_topology_stamp_round_trips_verify_and_migrate_on_the_mixed_pair(mixed_cloud, pypiron_bin):
    """`buckets migrate` CAS-stamps one shared generation onto both the S3 and the
    Azure bucket, and re-running bumps it — proving put_if_none_match/put_if_match
    round-trip identically on both backends (the open question the phase gates)."""
    s3_bucket, az_container = _names(mixed_cloud)
    minio = mixed_cloud["s3"]
    azure = mixed_cloud["azure"]
    env = _mixed_env(mixed_cloud, "127.0.0.1:0")

    for expected_generation in (1, 2):
        rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=40)
        assert rc == 0, f"mixed topology migration failed:\n{out}\n{err}"

        s3_stamp = json.loads(minio_get_key_in(minio, s3_bucket, "_topology/stamp.json"))
        az_stamp = json.loads(azurite_get_blob(azure, "_topology/stamp.json").decode())
        for stamp in (s3_stamp, az_stamp):
            assert stamp["buckets"] == [f"s3://{s3_bucket}", f"az://{az_container}"]
            assert stamp["generation"] == expected_generation
        # Byte-identical stamp on both backends — the same record, not two.
        assert s3_stamp == az_stamp


def test_reordered_mixed_list_without_migrate_refuses_to_start(mixed_cloud, pypiron_bin):
    """Reversing the mixed bucket order without re-migrating is a topology
    mismatch and must fail closed before binding — same rule as same-cloud."""
    s3_bucket, az_container = _names(mixed_cloud)
    env = _mixed_env(mixed_cloud, "127.0.0.1:0")
    rc, out, err = run_returncode([str(pypiron_bin), "buckets", "migrate"], env=env, timeout=40)
    assert rc == 0, f"initial migration failed:\n{out}\n{err}"

    port = find_free_port()
    reordered = _mixed_env(mixed_cloud, f"127.0.0.1:{port}")
    reordered["PYPIRON_BUCKETS"] = f"az://{az_container},s3://{s3_bucket}"
    reordered["PYPIRON_AUDIT_ON_BOOT"] = "false"
    proc = subprocess.Popen(
        [str(pypiron_bin), "serve"],
        env=reordered,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        out, _ = proc.communicate(timeout=40)
    except subprocess.TimeoutExpired:
        kill_process_tree(proc)
        raise AssertionError("serve did not refuse a reordered mixed topology; it stayed up")
    assert proc.returncode != 0, f"reordering should refuse to start:\n{out}"
    assert "different bucket topology" in out and "refusing to start" in out, out


# ------------------------------- upload fan-out -------------------------------


def test_upload_fans_out_byte_equal_to_s3_and_azure(mixed_cloud_server, tmp_path):
    """A single upload is durable on both clouds at ack: the artifact and its
    sidecar land byte-for-byte identical on the S3 bucket and the Azure
    container, and both indexes list the file."""
    server = mixed_cloud_server
    minio = server["s3"]
    azure = server["azure"]
    s3_bucket, _ = _names(server["mixed"])
    pkg = "fanmixed"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    wheel_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    akey = f"packages/{pkg}/{wheel.name}"
    meta = f"{akey}.meta.json"
    _eventually(lambda: azurite_key_exists(azure, akey), what="artifact replicated to Azure")

    # Same artifact bytes on both backends.
    assert minio_object_sha256(minio, s3_bucket, akey) == wheel_sha
    assert azurite_object_sha256(azure, akey) == wheel_sha
    # Same sidecar bytes on both backends.
    assert minio_object_sha256(minio, s3_bucket, meta) == azurite_object_sha256(azure, meta)
    # Both indexes list the file.
    assert _s3_index_has_file(minio, s3_bucket, pkg, wheel.name)
    _eventually(
        lambda: _azure_index_has_file(azure, pkg, wheel.name),
        what="Azure index lists the file",
    )


# ----------------------------- tombstone fan-out ------------------------------


def test_delete_tombstone_fans_out_and_suppresses_on_both(mixed_cloud_server, tmp_path):
    """Deleting a private file fans the tombstone out to both clouds: the
    artifact is gone from S3 and Azure, both carry the tombstone, and neither
    index lists it anymore."""
    server = mixed_cloud_server
    minio = server["s3"]
    azure = server["azure"]
    s3_bucket, _ = _names(server["mixed"])
    pkg = "delmixed"
    wheel = make_wheel(pkg, "2.0", tmp_path)
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    akey = f"packages/{pkg}/{wheel.name}"
    _eventually(lambda: azurite_key_exists(azure, akey), what="artifact replicated to Azure")

    code, _, _ = http_request_auth(
        "DELETE",
        f"{server['base_url']}/files/{pkg}/{wheel.name}",
        username=server["user"],
        password=server["password"],
    )
    assert code == 204, f"delete returned {code}"

    tkey = f"{akey}.tombstone"
    _eventually(lambda: minio_key_exists_in(minio, s3_bucket, tkey), what="tombstone on S3")
    _eventually(lambda: azurite_key_exists(azure, tkey), what="tombstone on Azure")
    _eventually(
        lambda: not minio_key_exists_in(minio, s3_bucket, akey), what="artifact gone from S3"
    )
    _eventually(lambda: not azurite_key_exists(azure, akey), what="artifact gone from Azure")
    _eventually(
        lambda: not _azure_index_has_file(azure, pkg, wheel.name),
        what="file dropped from the Azure index",
    )
    # The delete never resurrects: a fresh GET is a clean 404 on the live node.
    code, _, _ = http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}")
    assert code == 404


# --------------------------- failover + repair note ---------------------------


def test_failover_to_azure_serves_reads_and_repair_note_drains_after_heal(
    mixed_cloud_server, tmp_path
):
    """With the preferred S3 bucket down, reads fail over to Azure and an upload
    still acks — landing on Azure with a durable repair note aimed back at S3.
    When S3 returns, the sweep drains the note and S3 converges byte-equal."""
    server = mixed_cloud_server
    minio = server["s3"]
    azure = server["azure"]
    faults = server["faults"]
    assert faults is not None, "mixed S3 leg must be fault-proxied"
    s3_bucket, az_container = _names(server["mixed"])

    # A record that exists on both before the outage.
    first = make_wheel("healmixed", "1.0", tmp_path)
    _upload(server, first)
    wait_for_file_in_index(server["simple"], "healmixed", first.name)
    first_key = f"packages/healmixed/{first.name}"
    _eventually(lambda: azurite_key_exists(azure, first_key), what="first record on Azure")

    _eventually(
        lambda: _selected_bucket(server) == s3_bucket,
        timeout=15,
        what="S3 preferred and selected before the outage",
    )
    _eventually(
        lambda: _bucket_health(server, az_container) == 1,
        timeout=15,
        what="Azure known healthy",
    )

    # Take S3 out; selection must move to Azure.
    faults.fail(s3_bucket)
    _eventually(
        lambda: _selected_bucket(server) == az_container,
        timeout=30,
        what="selection fails over to Azure",
    )

    # Reads keep working, served from Azure.
    code, body, _ = http_get(f"{server['base_url']}/files/healmixed/{first.name}", timeout=20)
    assert code == 200, f"failover read returned {code}"
    assert hashlib.sha256(body).hexdigest() == hashlib.sha256(first.read_bytes()).hexdigest()

    # An upload during the outage still acks; the fan-out to S3 fails, so a repair
    # note aimed at S3 is written to the working (Azure) bucket before the ack.
    second = make_wheel("healmixed", "2.0", tmp_path)
    _upload(server, second)
    wait_for_file_in_index(server["simple"], "healmixed", second.name)
    second_key = f"packages/healmixed/{second.name}"
    assert azurite_key_exists(azure, second_key), "second record must be durable on Azure at ack"
    # The durable ground truth: a repair note aimed at the down S3 bucket
    # (dest index 0) sits on the working Azure bucket, written before the ack.
    note = _eventually(
        lambda: [k for k in azurite_list_keys(azure, "_repl/") if k.startswith("_repl/0/")],
        what="a repair note aimed at the down S3 bucket is durable on Azure",
    )
    assert any("healmixed" in k for k in note), f"note should reference the missed record: {note}"

    # S3 returns: the heal-triggered sweep drains the note and delivers the record.
    faults.recover(s3_bucket)
    second_sha = hashlib.sha256(second.read_bytes()).hexdigest()
    _eventually(
        lambda: minio_key_exists_in(minio, s3_bucket, second_key),
        timeout=60,
        what="missed record delivered to S3 after heal",
    )
    _eventually(
        lambda: minio_object_sha256(minio, s3_bucket, second_key) == second_sha,
        timeout=60,
        what="delivered S3 bytes match the source upload",
    )
    _eventually(
        lambda: not any(k.startswith("_repl/") for k in azurite_list_keys(azure, "_repl/")),
        timeout=60,
        what="repair note consumed from the Azure bucket after delivery",
    )
