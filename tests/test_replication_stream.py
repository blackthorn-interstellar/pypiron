"""The streaming replication transport (real binary, two real MinIO clusters).

Every bucket pair that is NOT server-side-copy eligible — different provider,
different endpoint, different credentials — moves its bytes through this node:
GET the source, PUT the destination. That read used to assemble the whole
artifact in a `Vec<u8>`, once per destination, with sixteen copies in flight per
destination, which is the same shape of OOM the upload path removed by spooling.
It now streams: hashed chunk by chunk, staged to a temp file past the multipart
threshold, and handed to the destination write as a file.

Two MinIO containers with different credentials are the cheapest real instance of
that pair (`storage::copy_pair_eligible` refuses it on both endpoint and
credential identity), so these drive the streaming transport for real, with an
artifact big enough to cross the staging threshold.

There is no disk equivalent to test: a `--buckets` list is object storage only
(`s3://`, `gs://`, `az://`), so a disk deployment is always single-bucket and
never replicates at all.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time

import pytest

from .conftest import (
    minio_delete_key_in,
    minio_key_exists_in,
    minio_object_sha256,
    minio_put_key_in,
    minio_try_get_key_bytes_in,
)
from .helpers import (
    find_free_port,
    kill_process_tree,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
    wait_http_ok,
)

pytestmark = [pytest.mark.integration, pytest.mark.s3]

# Past `replicate::STAGE_TO_DISK_ABOVE`, which IS `storage::MULTIPART_THRESHOLD`
# (64 MiB): below it the destination write reads the whole file back into a Vec to
# issue its one conditional PUT, so staging earlier would buy a disk round-trip and
# bound nothing. Only here does the copy take the staged-file branch and stream out
# in parts.
STAGED_PAYLOAD = 65 * 1024 * 1024


def _eventually(check, *, timeout: float = 40.0, what: str = "condition") -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def _two_cluster_server(minio, minio_alt, pypiron_bin, tmp_path):
    """Start a server over two buckets on two MinIO endpoints with different
    credentials — a pair `copy_pair_eligible` refuses, so every artifact moves
    by streaming. Returns the started process plus what the tests need."""
    a = minio["bucket"]
    b = minio_alt["bucket"]
    config = tmp_path / "pypiron.toml"
    config.write_text(
        f"""
[serve.bucket."s3://{b}"]
endpoint-url = "{minio_alt["endpoint"]}"
force-path-style = true
env-prefix = "BUCKETB_"
"""
    )

    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("BUCKETB_"):
            del env[key]
    env.update(
        {
            "PYPIRON_BIND_ADDR": bind,
            "PYPIRON_BUCKETS": f"s3://{a},s3://{b}",
            "PYPIRON_WORKER_INTERVAL_SECS": "1",
            "PYPIRON_REPL_SWEEP_INTERVAL_SECS": "2",
            # The full diff is the healer for a record deleted out from under a
            # peer; at its 24h default nothing in a test's lifetime would run it.
            "PYPIRON_RECONCILE_INTERVAL_SECS": "2",
            "PYPIRON_AUDIT_ON_BOOT": "false",
            "PYPIRON_ADMIN_USER": "admin",
            "PYPIRON_ADMIN_PASS": "secret",
            "AWS_REGION": "us-east-1",
            "PYPIRON_S3_ENDPOINT_URL": minio["endpoint"],
            "PYPIRON_S3_FORCE_PATH_STYLE": "true",
            "AWS_ACCESS_KEY_ID": minio["access_key"],
            "AWS_SECRET_ACCESS_KEY": minio["secret_key"],
            "BUCKETB_AWS_ACCESS_KEY_ID": minio_alt["access_key"],
            "BUCKETB_AWS_SECRET_ACCESS_KEY": minio_alt["secret_key"],
            "RUST_LOG": "info,pypiron=debug",
        }
    )

    log_path = tmp_path / "server.log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [str(pypiron_bin), "serve", "--config", str(config)],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    wait_http_ok(f"http://{bind}/simple/index.json", timeout=30.0)
    return {
        "proc": proc,
        "bind": bind,
        "legacy": f"http://{bind}/legacy/",
        "simple": f"http://{bind}/simple/",
        "log_path": log_path,
        "a": a,
        "b": b,
    }


def test_a_multi_mib_artifact_streams_to_an_ineligible_peer(
    minio, minio_alt, pypiron_bin, tmp_path
):
    """The whole point of the transport: a body too big to hold reaches the peer
    intact, and the copy staged it to a file instead of assembling it.

    Both halves of the record are asserted — the body AND the sidecar that names
    it — because a peer holding one without the other is the state the ordering
    rules exist to prevent, and byte equality is what proves the streamed
    reassembly is not subtly wrong (a dropped or reordered chunk hashes
    differently and the copy would have refused, so equality here is the check
    that the refusal was not silently skipped).
    """
    server = _two_cluster_server(minio, minio_alt, pypiron_bin, tmp_path)
    try:
        wheel = make_wheel("streambig", "1.0", tmp_path, payload_bytes=STAGED_PAYLOAD)
        wheel_bytes = wheel.read_bytes()
        assert len(wheel_bytes) > 64 * 1024 * 1024, "the wheel must cross the staging threshold"
        wheel_sha = hashlib.sha256(wheel_bytes).hexdigest()

        upload_legacy(
            server["legacy"],
            wheel,
            username="admin",
            password="secret",
            timeout=120,
        )
        wait_for_file_in_index(server["simple"], "streambig", wheel.name)

        key = f"packages/streambig/{wheel.name}"
        sc_key = f"{key}.meta.json"
        assert minio_key_exists_in(minio, server["a"], key)
        _eventually(
            lambda: minio_key_exists_in(minio_alt, server["b"], key),
            what="the artifact streamed to the second-endpoint bucket",
        )
        _eventually(
            lambda: minio_key_exists_in(minio_alt, server["b"], sc_key),
            what="the peer's sidecar was published after its bytes",
        )

        assert minio_object_sha256(minio, server["a"], key) == wheel_sha
        assert minio_object_sha256(minio_alt, server["b"], key) == wheel_sha
        peer_sidecar = json.loads(
            minio_try_get_key_bytes_in(minio_alt, server["b"], sc_key).decode()
        )
        assert peer_sidecar["sha256"] == wheel_sha
        assert peer_sidecar["size"] == len(wheel_bytes)

        # The upload fan-out reads the local verified spool, so it hashes a file
        # it already holds. Force the OTHER read — out of the source bucket,
        # which is what the sweep and the full diff do — by dropping the peer's
        # record and letting the diff heal it. That copy is the one that has to
        # stage, and the log line is the proof it did rather than assembling the
        # body in memory again.
        minio_delete_key_in(minio_alt, server["b"], key)
        minio_delete_key_in(minio_alt, server["b"], sc_key)
        _eventually(
            lambda: (
                minio_key_exists_in(minio_alt, server["b"], key)
                and minio_key_exists_in(minio_alt, server["b"], sc_key)
            ),
            what="the full diff re-copied the record out of the source bucket",
        )
        assert minio_object_sha256(minio_alt, server["b"], key) == wheel_sha
        assert "staging the replication source to a spool file" in server["log_path"].read_text(
            errors="replace"
        ), "a body past the staging threshold was assembled in memory instead of staged"
    finally:
        kill_process_tree(server["proc"])


def test_a_corrupt_source_puts_no_bytes_on_the_peer(minio, minio_alt, pypiron_bin, tmp_path):
    """A source body that no longer hashes to its own sidecar must not put bytes
    on the peer. The stream transport hashes as it reads and compares before the
    FIRST destination write, so there is never a half-landed body to clean up —
    the copy is refused with nothing written.

    That "nothing written" is asserted deterministically one level down, in
    `replicate::tests::source_sha_is_verified_before_destination_sidecar_publish`;
    it is not a stable assertion out here, because the source repairs itself:
    dropping the stale sidecar and rebuilding one from the live bytes makes the
    record self-consistent again within a few seconds, and replicating it then is
    correct. So this test asserts the invariant that survives that repair — the
    peer never, at any point, holds the corrupt body under the ORIGINAL sidecar
    — plus the log line proving the hash check is what refused the copy.
    """
    server = _two_cluster_server(minio, minio_alt, pypiron_bin, tmp_path)
    try:
        wheel = make_wheel("streamcorrupt", "1.0", tmp_path, payload_bytes=64 * 1024)
        wheel_bytes = wheel.read_bytes()
        wheel_sha = hashlib.sha256(wheel_bytes).hexdigest()
        upload_legacy(
            server["legacy"],
            wheel,
            username="admin",
            password="secret",
            timeout=60,
        )
        wait_for_file_in_index(server["simple"], "streamcorrupt", wheel.name)

        key = f"packages/streamcorrupt/{wheel.name}"
        sc_key = f"{key}.meta.json"
        _eventually(
            lambda: minio_key_exists_in(minio_alt, server["b"], key),
            what="the first, honest copy reached the peer",
        )
        assert minio_object_sha256(minio_alt, server["b"], key) == wheel_sha

        # Replace the source body with same-size wrong bytes, leaving its sidecar
        # naming the original sha, and wipe the peer's record so the sweep has to
        # copy the now-corrupt source again.
        corrupt = "C" * len(wheel_bytes)
        minio_put_key_in(minio, server["a"], key, corrupt)
        minio_delete_key_in(minio_alt, server["b"], key)
        minio_delete_key_in(minio_alt, server["b"], sc_key)
        assert minio_object_sha256(minio, server["a"], key) != wheel_sha

        def peer_contradicts() -> bool:
            art = minio_try_get_key_bytes_in(minio_alt, server["b"], key)
            sc = minio_try_get_key_bytes_in(minio_alt, server["b"], sc_key)
            if art is None or sc is None:
                return False
            return hashlib.sha256(art).hexdigest() != json.loads(sc.decode())["sha256"]

        # Sampled across several sweep and full-diff cycles. The peer is allowed
        # to end up holding the corrupt bytes — once the source's torn-record
        # repair drops the sidecar those bytes contradict and the rebuild
        # refabricates one from the live body, the record is self-consistent
        # again and replicating it is correct. What must never appear, at any
        # point, is the corrupt body under the ORIGINAL sidecar: that is a peer
        # serving bytes its own published sha256 contradicts, which every reader
        # on every hot path trusts and no reader re-derives.
        for _ in range(50):
            assert not peer_contradicts(), (
                "the peer serves bytes its own sidecar contradicts — a copy "
                "published truth about bytes it never verified"
            )
            time.sleep(0.3)

        # And it was the hash check that stopped it, not luck or a dead sweep.
        assert "source artifact sha mismatch" in server["log_path"].read_text(errors="replace"), (
            "the copy path never even looked at the corrupt source"
        )
    finally:
        kill_process_tree(server["proc"])
