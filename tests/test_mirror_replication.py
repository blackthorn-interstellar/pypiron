"""Mirror snapshots replicate as truth across buckets.

`sync --to` uploads (``mirror=true``) are the operator's chosen corpus, so a
multi-bucket fleet replicates them exactly like private truth: fan-out before
ack, then any single bucket serves the whole mirror index. Proxy-cache fills
(``replicate=false``) stay bucket-local — the documented carve-out. Assertions
inspect each bucket directly through `mc`, independent of what a server reports.

Covered: snapshot fan-out + failover, the cache carve-out under reconcile, and
mirror-yank convergence (the confirmed "mirror yank never converges" gap).
"""

from __future__ import annotations

import hashlib
import json
import time

import pytest

from .conftest import (
    minio_get_key_in,
    minio_key_exists_in,
    minio_make_bucket,
    minio_object_sha256,
    minio_put_key_in,
    minio_remove_bucket,
)
from .helpers import (
    http_get,
    http_request_auth,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
)
from .test_multibucket import (
    _counter_value,
    _heal_nodes,
    _partition_nodes,
)

pytestmark = [pytest.mark.integration, pytest.mark.s3]


def _eventually(predicate, *, timeout: float = 30.0, interval: float = 0.3, what: str = ""):
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


def _admin(server) -> tuple[str, str]:
    return server.get("admin_user", server["user"]), server.get(
        "admin_password", server["password"]
    )


def _mirror_upload(server, wheel) -> None:
    """Publish `wheel` as a `sync --to` snapshot: the admin `mirror=true` path
    that carries the upstream upload time and yank state."""
    user, password = _admin(server)
    upload_legacy(
        server["legacy"],
        wheel,
        username=user,
        password=password,
        fields={
            "mirror": "true",
            "yanked": "false",
            "upload_time": "2021-06-01T00:00:00Z",
        },
    )


def _seed_mirror_cache_record(minio, bucket, pkg, filename, body: str) -> None:
    """Seed a proxy-cache-shaped mirror record (no `replicate` bit) directly
    into one bucket, the way a proxy fill lands it: bucket-local truth that
    must NEVER ride the replicator."""
    key = f"packages/{pkg}/{filename}"
    sidecar = {
        "sha256": hashlib.sha256(body.encode()).hexdigest(),
        "size": len(body.encode()),
        "version": "1.0",
        "upload-time": "2020-01-01T00:00:00Z",
        "yanked": False,
        "origin": "mirror",
    }
    claim = {"origin": "mirror", "nonce": hashlib.sha256(pkg.encode()).hexdigest()[:32]}
    minio_put_key_in(minio, bucket, f"packages/{pkg}/.origin", json.dumps(claim))
    minio_put_key_in(minio, bucket, f"{key}.meta.json", json.dumps(sidecar))
    minio_put_key_in(minio, bucket, key, body)


def _seed_mirror_snapshot_record(minio, bucket, pkg, filename, body: str) -> None:
    """Seed a `sync --to` snapshot record — a mirror sidecar carrying
    ``replicate=true`` — directly into one bucket, the way an existing
    single-bucket mirror corpus already holds it. Unlike a proxy-cache record it
    MUST ride the replicator, so a bucket added later backfills it."""
    key = f"packages/{pkg}/{filename}"
    sidecar = {
        "sha256": hashlib.sha256(body.encode()).hexdigest(),
        "size": len(body.encode()),
        "version": "1.0",
        "upload-time": "2021-06-01T00:00:00Z",
        "yanked": False,
        "origin": "mirror",
        "replicate": True,
    }
    claim = {"origin": "mirror", "nonce": hashlib.sha256(pkg.encode()).hexdigest()[:32]}
    minio_put_key_in(minio, bucket, f"packages/{pkg}/.origin", json.dumps(claim))
    minio_put_key_in(minio, bucket, f"{key}.meta.json", json.dumps(sidecar))
    minio_put_key_in(minio, bucket, key, body)


def _seed_private_record(minio, bucket, pkg, filename, body: str) -> None:
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


def test_mirror_snapshot_replicates_and_survives_write_bucket_loss(
    minio_two, s3_server_multi, tmp_path
):
    """The archetype: a `sync --to` snapshot lands in BOTH buckets (origin
    mirror, replicate=true), so killing the write bucket still serves the
    mirror bytes from the peer — what multi-region.md promised but the old
    bucket-local mirror could never deliver."""
    server = s3_server_multi
    minio = minio_two
    a, b = minio["buckets"]
    pkg = "mirrorsnap"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _mirror_upload(server, wheel)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    akey = f"packages/{pkg}/{wheel.name}"
    _eventually(
        lambda: minio_key_exists_in(minio, b, akey),
        what="mirror snapshot fanned out to the peer bucket",
    )
    wheel_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
    assert minio_object_sha256(minio, b, akey) == wheel_sha
    assert minio_object_sha256(minio, b, akey) == minio_object_sha256(minio, a, akey)

    # Both buckets carry a replicating mirror snapshot sidecar and a mirror
    # origin claim — the peer is a full, servable copy of the corpus.
    for bucket in (a, b):
        sidecar = json.loads(minio_get_key_in(minio, bucket, f"{akey}.meta.json"))
        assert sidecar["origin"] == "mirror", bucket
        assert sidecar["replicate"] is True, bucket
        claim = json.loads(minio_get_key_in(minio, bucket, f"packages/{pkg}/.origin"))
        assert claim["origin"] == "mirror", bucket

    # Kill the write bucket; the peer serves the mirror bytes.
    minio_remove_bucket(minio, a)
    try:

        def _peer_serves():
            code, body, _ = http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}", timeout=15)
            return code == 200 and hashlib.sha256(body).hexdigest() == wheel_sha

        _eventually(_peer_serves, timeout=30, what="peer bucket serves the mirror bytes")
    finally:
        minio_make_bucket(minio, a)


def test_proxy_cache_mirror_record_is_not_replicated(minio_two, s3_server_multi, tmp_path):
    """The carve-out: a proxy-cache mirror record (replicate=false) is
    re-derivable per bucket, so it must stay bucket-local even as the tier-3
    diff converges everything else. A private canary that DOES replicate proves
    the reconciler actually ran. The running `s3_server_multi` node is what
    drives the tier-3 diff between the two buckets."""
    minio = minio_two
    a, b = minio["buckets"]

    _seed_private_record(minio, a, "cacheproof", "cacheproof-1.0-py3-none-any.whl", "private truth")
    _seed_mirror_cache_record(
        minio, a, "cachelocal", "cachelocal-1.0-py3-none-any.whl", "cache-only bytes"
    )
    priv_key = "packages/cacheproof/cacheproof-1.0-py3-none-any.whl"
    cache_key = "packages/cachelocal/cachelocal-1.0-py3-none-any.whl"

    _eventually(
        lambda: minio_key_exists_in(minio, b, priv_key),
        timeout=30,
        what="private truth reconciles to the peer (proves the diff ran)",
    )
    # The reconciler visited the tree; the cache record must not have ridden along.
    assert not minio_key_exists_in(minio, b, cache_key), "a proxy cache must stay bucket-local"
    assert not minio_key_exists_in(
        minio, b, "packages/cachelocal/cachelocal-1.0-py3-none-any.whl.meta.json"
    )


def test_mirror_snapshot_yank_converges_across_buckets(minio_two, s3_server_multi, tmp_path):
    """The confirmed gap: a mirror yank fanned out only for private truth, so a
    withdrawn snapshot never converged to peers. It now rides the same fan-out —
    both buckets record the yank with a bumped epoch."""
    server = s3_server_multi
    minio = minio_two
    a, b = minio["buckets"]
    pkg = "mirroryank"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _mirror_upload(server, wheel)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    akey = f"packages/{pkg}/{wheel.name}"
    _eventually(
        lambda: minio_key_exists_in(minio, b, akey),
        what="snapshot replicated before the yank",
    )

    yank_url = f"{server['base_url']}/files/{pkg}/{wheel.name}/yank"
    user, password = _admin(server)
    code, _, _ = http_request_auth(
        "POST",
        yank_url,
        username=user,
        password=password,
        data=b"withdrawn upstream",
    )
    assert code == 200, f"yank returned {code}"

    for bucket in (a, b):
        _eventually(
            lambda bucket=bucket: (
                json.loads(minio_get_key_in(minio, bucket, f"{akey}.meta.json")).get("yanked")
                == "withdrawn upstream"
            ),
            what=f"mirror yank converges to {bucket}",
        )
        sidecar = json.loads(minio_get_key_in(minio, bucket, f"{akey}.meta.json"))
        assert sidecar["yank-epoch"] == 1, bucket
        assert sidecar["origin"] == "mirror", bucket


def test_existing_mirror_corpus_backfills_onto_an_added_bucket(
    minio_two, s3_server_multi, tmp_path
):
    """single -> multi migration of an existing mirror corpus. Two snapshot
    packages already live on bucket a (a single-bucket mirror's corpus); one of
    them is also pre-seeded onto bucket b out-of-band (the `aws s3 sync` seed of
    a huge corpus). Bringing up the two-bucket fleet must:

      * backfill the un-seeded package onto b (reconcile converges it), and
      * leave the pre-seeded package intact (reconcile degrades to a verify pass,
        never a duplicate or a corruption),

    then a fresh `sync --to` upload of a new package stamps ``replicate=true`` and
    fans out to both buckets — the re-sync path the migration guide points at.
    """
    server = s3_server_multi
    minio = minio_two
    a, b = minio["buckets"]

    # The existing single-bucket corpus: two snapshot packages on a.
    backfill = ("corpusbackfill", "corpusbackfill-1.0-py3-none-any.whl", "backfill bytes")
    preseeded = ("corpuspreseed", "corpuspreseed-1.0-py3-none-any.whl", "preseed bytes")
    for pkg, filename, bytes_ in (backfill, preseeded):
        _seed_mirror_snapshot_record(minio, a, pkg, filename, bytes_)
    # The huge-corpus seed: b already holds one of the two packages.
    _seed_mirror_snapshot_record(minio, b, *preseeded)

    bf_key = f"packages/{backfill[0]}/{backfill[1]}"
    ps_key = f"packages/{preseeded[0]}/{preseeded[1]}"

    # reconcile converges the un-seeded package onto b, byte-for-byte.
    _eventually(
        lambda: minio_key_exists_in(minio, b, bf_key),
        timeout=30,
        what="existing corpus backfilled onto the added bucket",
    )
    assert minio_object_sha256(minio, b, bf_key) == minio_object_sha256(minio, a, bf_key)
    assert json.loads(minio_get_key_in(minio, b, f"{bf_key}.meta.json"))["replicate"] is True

    # The pre-seeded package is untouched and still matches a.
    assert minio_object_sha256(minio, b, ps_key) == minio_object_sha256(minio, a, ps_key)

    # Re-run `sync --to`: a fresh mirror upload on the fleet stamps replicate=true
    # and fans out to both buckets.
    fresh = make_wheel("corpusfresh", "2.0", tmp_path)
    _mirror_upload(server, fresh)
    wait_for_file_in_index(server["simple"], "corpusfresh", fresh.name)
    fresh_key = f"packages/corpusfresh/{fresh.name}"
    fresh_sha = hashlib.sha256(fresh.read_bytes()).hexdigest()
    for bucket in (a, b):
        _eventually(
            lambda bucket=bucket: minio_key_exists_in(minio, bucket, fresh_key),
            what=f"fresh re-sync upload fanned out to {bucket}",
        )
        assert minio_object_sha256(minio, bucket, fresh_key) == fresh_sha, bucket
        assert (
            json.loads(minio_get_key_in(minio, bucket, f"{fresh_key}.meta.json"))["replicate"]
            is True
        ), bucket


def test_divergent_snapshot_bytes_freeze_both_buckets(s3_servers_multi, tmp_path):
    """Two snapshots that committed different bytes under one immutable filename
    during a partition are a split-brain a supply-chain product must never
    auto-pick between. Mirror sidecars carry no upload-epoch, so there is no
    trustworthy order: both sides freeze and quarantine, and the alarm fires."""
    cluster = s3_servers_multi
    minio = cluster["minio"]
    a, b = _partition_nodes(cluster)
    pkg = "snapfreeze"
    left_wheel = make_wheel(pkg, "1.0", tmp_path / "left", description="left snapshot")
    right_wheel = make_wheel(pkg, "1.0", tmp_path / "right", description="right snapshot")
    assert left_wheel.name == right_wheel.name
    assert left_wheel.read_bytes() != right_wheel.read_bytes()

    _mirror_upload(cluster["left"], left_wheel)
    _mirror_upload(cluster["right"], right_wheel)

    key = f"packages/{pkg}/{left_wheel.name}"
    _heal_nodes(cluster, a, b)
    for bucket in (a, b):
        _eventually(
            lambda bucket=bucket: minio_key_exists_in(minio, bucket, f"{key}.frozen"),
            timeout=45,
            what=f"divergent snapshot freezes {bucket}",
        )
        _eventually(
            lambda bucket=bucket: not minio_key_exists_in(minio, bucket, key),
            timeout=45,
            what=f"frozen body removed from {bucket}",
        )
    _eventually(
        lambda: (
            max(
                _counter_value(cluster["left"], "pypiron_replication_freezes_total"),
                _counter_value(cluster["right"], "pypiron_replication_freezes_total"),
            )
            >= 1
        ),
        timeout=45,
        what="freeze alarm counter increments",
    )
