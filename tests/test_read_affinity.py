"""Read affinity: every region serves reads from its own bucket, writes keep a
single home (dev/READ_AFFINITY_VISION.md).

One real pypiron node runs over two region-labeled buckets on MinIO
(`s3://a@left,s3://b@right`) and declares its region as `right`, so its write
pin is the config-order home A and its read pin is the region bucket B. A single
bucket-aware fault proxy sees every S3 request, so each test asserts WHICH bucket
answered a read from the ordered request log, independent of what the node
reports. Ground truth is read through `mc`, bypassing the proxy.

Covered: read locality (bytes from the region bucket, fan-out to both), the
polarity rule's read-through on an absent artifact (no client 404 for an acked
file), the accepted lag window for a yank the region bucket missed, failover off
a sustained outage with a drain-gated return, the fail-closed floor for a
private name whose claim the region bucket has not yet seen, and the no-region
fail-safe default.
"""

from __future__ import annotations

import hashlib
import json
import re
import time

import pytest

from .conftest import (
    minio_delete_key_in,
    minio_get_key_in,
    minio_key_exists_in,
    minio_list_keys_in,
    minio_object_sha256,
    minio_put_key_in,
)
from .helpers import (
    ACCEPT_PEP691,
    http_get,
    http_request_auth,
    make_wheel,
    origin_owner,
    run_checked,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = [pytest.mark.integration, pytest.mark.s3]

_WRITE_RE = re.compile(r'^pypiron_bucket_selected\{bucket="([^"]+)",index="\d+"\} ([01])$')
_READ_RE = re.compile(r'^pypiron_bucket_read_selected\{bucket="([^"]+)",index="\d+"\} ([01])$')
_HEALTH_RE = re.compile(r'^pypiron_bucket_health_state\{bucket="([^"]+)",index="\d+"\} (-?1|0)$')


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
    """Strip a bucket identity's scheme prefix (`s3://`) so a metric label
    compares equal to the plain MinIO bucket name a test holds."""
    _, _, rest = name.partition("://")
    return rest or name


def _one_hot(server, pattern) -> str:
    hot = [
        _bare(match.group(1))
        for line in _metrics(server)
        if (match := pattern.match(line)) and match.group(2) == "1"
    ]
    assert len(hot) == 1, f"expected exactly one selected bucket, got {hot}"
    return hot[0]


def _write_bucket(server) -> str:
    return _one_hot(server, _WRITE_RE)


def _read_bucket(server) -> str:
    return _one_hot(server, _READ_RE)


def _bucket_health(server, bucket: str) -> int:
    for line in _metrics(server):
        match = _HEALTH_RE.match(line)
        if match and _bare(match.group(1)) == bucket:
            return int(match.group(2))
    raise AssertionError(f"missing health metric for {bucket}")


def _upload(server, wheel) -> None:
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
    )


def _install(server, uv_path: str, py, pkg: str, version: str = "1.0") -> None:
    """Resolve and install `pkg` through the node with a real client, then import
    it — the end-to-end proof that a client is served entirely off the node."""
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(py),
            "--index-url",
            server["simple"],
            "--no-cache-dir",
            f"{pkg}=={version}",
        ],
        timeout=120,
    )
    module = re.sub(r"\W+", "_", pkg).strip("_").lower()
    run_checked([str(py), "-c", f"import {module}"])


def _claim_owner(minio, bucket: str, pkg: str) -> str:
    return origin_owner(minio_get_key_in(minio, bucket, f"packages/{pkg}/.origin"))


def _sidecar(minio, bucket: str, akey: str) -> dict:
    return json.loads(minio_get_key_in(minio, bucket, f"{akey}.meta.json"))


def _artifact_gets(faults, bucket: str, akey: str) -> int:
    """How many times `bucket` served the artifact body itself — an exact
    suffix match on the request log, so a companion GET (`…whl.meta.json`) whose
    path merely contains the key is not miscounted."""
    return sum(
        1
        for seen_bucket, method, target in faults.requests()
        if seen_bucket == bucket and method == "GET" and target.split("?", 1)[0].endswith(akey)
    )


def _served_file_yanked(server, pkg: str, filename: str):
    """The `yanked` value the node reports for one file in its served JSON index
    (False/None when live, a reason string once withdrawn)."""
    code, body, _ = http_get(
        f"{server['simple']}{pkg}/index.json", headers={"Accept": ACCEPT_PEP691}
    )
    assert code == 200, f"index returned {code}"
    for file in json.loads(body)["files"]:
        if file["filename"] == filename:
            return file.get("yanked", False)
    raise AssertionError(f"{filename} missing from {pkg}'s served index")


def test_reads_are_served_from_the_region_bucket(
    s3_server_read_affinity, tmp_path, uv_path, uv_venv
):
    """Locality: the read pin is the region bucket B while the write pin stays on
    A; a real install and a direct download are both served from B, and the
    upload fanned out to both buckets byte-identically."""
    server = s3_server_read_affinity
    minio = server["minio"]
    a, b = minio["buckets"]
    faults = server["faults"]

    _eventually(lambda: _read_bucket(server) == b, what="reads pin to the region bucket B")
    _eventually(lambda: _write_bucket(server) == a, what="writes home to the config-order bucket A")

    pkg = "regionlocal"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    akey = f"packages/{pkg}/{wheel.name}"
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    # Pre-ack fan-out already made B a full copy — the same bytes as A.
    _eventually(lambda: minio_key_exists_in(minio, b, akey), what="artifact fanned out to B")
    wheel_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
    assert minio_object_sha256(minio, a, akey) == wheel_sha
    assert minio_object_sha256(minio, b, akey) == wheel_sha
    # The upload PUTs landed in BOTH buckets (a write is never region-local).
    assert faults.count(bucket=a, method="PUT", needle=akey) > 0
    _eventually(
        lambda: faults.count(bucket=b, method="PUT", needle=akey) > 0,
        what="artifact PUT fanned out to B",
    )

    # A real client resolves and installs entirely through the node.
    _install(server, uv_path, uv_venv, pkg)

    # A direct download is served by reading the region bucket B, not A.
    before = _artifact_gets(faults, b, akey)
    code, body, _ = http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}", timeout=30)
    assert code == 200
    assert body == wheel.read_bytes()
    _eventually(
        lambda: _artifact_gets(faults, b, akey) > before,
        what="download served from the region bucket B",
    )
    # The package index the resolver read is also served from B.
    assert faults.count(bucket=b, method="GET", needle=f"simple/{pkg}/") > 0, (
        "package index served from the region bucket B"
    )


def test_absent_artifact_reads_through_without_a_client_404(
    s3_server_read_affinity_sticky, tmp_path, uv_path, uv_venv
):
    """Absence reads through: B misses a publish's fan-out (a `_repl` note is
    owed to it) yet the read pin never leaves B. An acked file must never 404 —
    every read is served by reading through to the write home — and once the
    sweep drains, B converges and serves the bytes itself."""
    server = s3_server_read_affinity_sticky
    minio = server["minio"]
    a, b = minio["buckets"]
    faults = server["faults"]

    _eventually(lambda: _read_bucket(server) == b, what="reads pin to B")
    _eventually(lambda: _write_bucket(server) == a, what="writes home to A")

    pkg = "absencepkg"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    akey = f"packages/{pkg}/{wheel.name}"

    # B is unreachable across the publish, then recovers at once; the high leave
    # threshold means the blip never moves the read pin off B.
    faults.fail(b)
    _upload(server, wheel)
    faults.recover(b)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    # Ground truth: A holds the record, B does not, and A owes B a repair note.
    assert minio_key_exists_in(minio, a, akey)
    assert not minio_key_exists_in(minio, b, akey), "B missed the fan-out"
    assert any(k.startswith(f"_repl/1/{pkg}/") for k in minio_list_keys_in(minio, a)), (
        "A carries the _repl note owed to B"
    )
    assert _read_bucket(server) == b, "the read pin stayed on the lagging region bucket"

    # The contract: no client-visible 404 for an acked file while B lags. Hold it
    # across the window so the assertion covers both the divergent and (once the
    # sweep runs) the converged state — every response is 200 with the true bytes.
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        code, body, _ = http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}", timeout=15)
        assert code == 200, f"acked file 404'd on the lagging region bucket: {code}"
        assert body == wheel.read_bytes()
        time.sleep(0.15)

    # A real client installs it through the node — zero visible 404s.
    _install(server, uv_path, uv_venv, pkg)

    # The sweep drains the note: B converges to the exact source bytes and the
    # note is gone.
    _eventually(
        lambda: (
            minio_object_sha256(minio, b, akey) == hashlib.sha256(wheel.read_bytes()).hexdigest()
        ),
        what="B converges to the source bytes",
    )
    _eventually(
        lambda: not any(k.startswith("_repl/") for k in minio_list_keys_in(minio, a)),
        what="repair note drains",
    )
    # Now the region bucket serves the bytes itself again (presence is proof).
    before = _artifact_gets(faults, b, akey)
    code, body, _ = http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}", timeout=15)
    assert code == 200
    assert body == wheel.read_bytes()
    _eventually(
        lambda: _artifact_gets(faults, b, akey) > before,
        what="after convergence the region bucket B serves the bytes",
    )


def test_lagging_region_bucket_holds_the_accepted_yank_window(
    s3_server_read_affinity_sticky, tmp_path
):
    """The accepted yank window: a yank the region bucket missed leaves it
    serving the stale (un-yanked) view until the sweep drains — the same bounded
    window a failed-over node has today. The file stays installable during the
    window; after convergence both buckets agree and the node surfaces the yank."""
    server = s3_server_read_affinity_sticky
    minio = server["minio"]
    a, b = minio["buckets"]
    faults = server["faults"]

    _eventually(lambda: _read_bucket(server) == b, what="reads pin to B")
    _eventually(lambda: _write_bucket(server) == a, what="writes home to A")

    pkg = "yankwindow"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    akey = f"packages/{pkg}/{wheel.name}"
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)
    _eventually(
        lambda: minio_key_exists_in(minio, b, akey), what="B holds the record before the yank"
    )

    # Yank while B is unreachable: only A records the withdrawal; B keeps the
    # stale sidecar and is owed a repair note.
    reason = "withdrawn for test"
    faults.fail(b)
    code, _, _ = http_request_auth(
        "POST",
        f"{server['base_url']}/files/{pkg}/{wheel.name}/yank",
        username=server["user"],
        password=server["password"],
        data=reason.encode(),
    )
    assert code == 200, f"yank returned {code}"
    faults.recover(b)
    assert _read_bucket(server) == b, "the read pin stayed on the lagging region bucket"

    # Ground truth of the accepted lag: A is yanked, B is not.
    assert _sidecar(minio, a, akey)["yanked"] == reason
    assert _sidecar(minio, b, akey).get("yanked", False) in (False, None), (
        "B still shows the file un-yanked during the accepted window"
    )

    # During the window the file remains installable through the node — a yank
    # withdraws a preference, it never removes bytes.
    code, body, _ = http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}", timeout=15)
    assert code == 200
    assert body == wheel.read_bytes()

    # After the sweep drains, B carries the yank, both buckets agree, and the
    # node's served index surfaces the withdrawal.
    _eventually(
        lambda: _sidecar(minio, b, akey).get("yanked", False) == reason,
        what="B converges to the yank",
    )
    _eventually(
        lambda: _served_file_yanked(server, pkg, wheel.name) == reason,
        what="the node surfaces the yank once B caught up",
    )
    assert _sidecar(minio, a, akey)["yanked"] == reason


def test_region_bucket_failover_and_drain_gated_return(
    s3_server_read_affinity, tmp_path, uv_path, uv_venv
):
    """Failover and drain-gated return: a sustained region-bucket outage moves
    reads to the write home (installs keep working); a recovery returns reads to
    the region bucket only after it is caught up — reads are never on B while it
    is missing an acked file."""
    server = s3_server_read_affinity
    minio = server["minio"]
    a, b = minio["buckets"]
    faults = server["faults"]

    _eventually(lambda: _read_bucket(server) == b, what="reads pin to B")
    _eventually(lambda: _write_bucket(server) == a, what="writes home to A")
    _eventually(lambda: _bucket_health(server, b) == 1, what="region bucket known healthy")

    base_pkg = "failbaseline"
    base_wheel = make_wheel(base_pkg, "1.0", tmp_path)
    _upload(server, base_wheel)
    wait_for_file_in_index(server["simple"], base_pkg, base_wheel.name)
    _eventually(
        lambda: minio_key_exists_in(minio, b, f"packages/{base_pkg}/{base_wheel.name}"),
        what="baseline fanned out to B",
    )

    # Sustained region-bucket outage: reads abandon B for the write home A.
    faults.fail(b)
    _eventually(
        lambda: _read_bucket(server) == a,
        what="reads fail over to the write bucket when the region bucket dies",
    )
    assert _write_bucket(server) == a, "the write pin never moved"
    # Installs keep working while B is down — served from A.
    _install(server, uv_path, uv_venv, base_pkg)

    # A publish during the outage owes B a fresh repair note.
    out_pkg = "duringoutage"
    out_wheel = make_wheel(out_pkg, "1.0", tmp_path)
    out_key = f"packages/{out_pkg}/{out_wheel.name}"
    _upload(server, out_wheel)
    _eventually(
        lambda: any(k.startswith(f"_repl/1/{out_pkg}/") for k in minio_list_keys_in(minio, a)),
        what="outage publish owes B a note",
    )
    assert not minio_key_exists_in(minio, b, out_key)

    # The real note above drains in a blink once B is back — too fast to observe
    # the gate holding reads off B. Seed a second note B can never satisfy on its
    # own so the drain gate is provably what keeps reads on A, isolated from the
    # healthy-return window. Its source record's artifact bytes do not match its
    # sidecar sha, so the copy verifier rejects it and the sweep retains the note.
    hold_pkg = "holdopen"
    hold_file = "holdopen-1.0-py3-none-any.whl"
    hold_akey = f"packages/{hold_pkg}/{hold_file}"
    hold_note = f"_repl/1/{hold_pkg}/{hold_file}!hold"
    minio_put_key_in(
        minio,
        a,
        f"packages/{hold_pkg}/.origin",
        json.dumps({"origin": "private", "nonce": "b" * 32}),
    )
    minio_put_key_in(
        minio,
        a,
        f"{hold_akey}.meta.json",
        json.dumps(
            {
                "sha256": "0" * 64,
                "size": 5,
                "version": "1.0",
                "upload-time": "2020-01-01T00:00:00Z",
                "yanked": False,
                "origin": "private",
            }
        ),
    )
    minio_put_key_in(minio, a, hold_akey, "bytes")  # sha256("bytes") != "0" * 64
    minio_put_key_in(minio, a, hold_note, "")

    # Recover B. The healthy window elapses and the real note drains — B ends up
    # holding the outage record — yet reads must stay on A because a note is still
    # owed to B.
    faults.recover(b)
    _eventually(lambda: _bucket_health(server, b) == 1, timeout=15, what="B observed healthy again")
    _eventually(
        lambda: minio_key_exists_in(minio, b, out_key), timeout=30, what="real note drains to B"
    )

    # B is healthy, the window has passed, B even holds the outage record — the
    # only thing left is the held note. Reads stay on A for well over the return
    # window and never touch B's stale copy.
    settle = time.monotonic() + 4.0
    while time.monotonic() < settle:
        assert _read_bucket(server) == a, "reads returned to B while a repair note was still owed"
        time.sleep(0.2)

    # Remove the held note (and its unresolvable source): now nothing is owed to
    # B, so reads return to it.
    for key in (hold_note, hold_akey, f"{hold_akey}.meta.json", f"packages/{hold_pkg}/.origin"):
        minio_delete_key_in(minio, a, key)
    _eventually(
        lambda: _read_bucket(server) == b,
        timeout=15,
        what="reads return to B once it is caught up",
    )

    # Post-return locality: reads hit B again, and B holds the caught-up record.
    assert minio_key_exists_in(minio, b, out_key)
    before = _artifact_gets(faults, b, out_key)
    code, body, _ = http_get(f"{server['base_url']}/files/{out_pkg}/{out_wheel.name}", timeout=30)
    assert code == 200
    assert body == out_wheel.read_bytes()
    _eventually(
        lambda: _artifact_gets(faults, b, out_key) > before,
        what="post-return reads hit B",
    )


def test_private_name_never_falls_through_on_a_region_bucket_absence(
    s3_server_read_affinity_proxy, tmp_path
):
    """Fail-closed floor: a private name whose origin claim the region bucket has
    not yet seen is served from local truth on the write home — never proxied
    upstream. A public build of the same name sits upstream to make a
    fall-through visible; the node serves the private bytes and the upstream
    access log shows it was never asked."""
    server = s3_server_read_affinity_proxy
    minio = server["minio"]
    upstream = server["upstream"]
    a, b = minio["buckets"]
    faults = server["faults"]

    _eventually(lambda: _read_bucket(server) == b, what="reads pin to B")
    _eventually(lambda: _write_bucket(server) == a, what="writes home to A")

    pkg = "regionprivate"
    public_wheel = make_wheel(pkg, "1.0", tmp_path / "public", description="upstream public build")
    private_wheel = make_wheel(pkg, "1.0", tmp_path / "private", description="private build")
    assert public_wheel.name == private_wheel.name
    assert public_wheel.read_bytes() != private_wheel.read_bytes()

    # The public build exists upstream: a fall-through would serve these bytes.
    _upload(upstream, public_wheel)
    wait_for_file_in_index(upstream["simple"], pkg, public_wheel.name)

    # The private upload lands on the write home A while B is unreachable, so B
    # gets neither the private claim nor the bytes. Long repair cadences keep B
    # divergent for a stable observation window.
    akey = f"packages/{pkg}/{private_wheel.name}"
    faults.fail(b)
    _upload(server, private_wheel)
    faults.recover(b)
    assert _read_bucket(server) == b, "the read pin stayed on B"
    _eventually(lambda: _claim_owner(minio, a, pkg) == "private", what="A owns the private claim")
    assert not minio_key_exists_in(minio, b, f"packages/{pkg}/.origin"), "B never saw the claim"
    assert not minio_key_exists_in(minio, b, akey), "B never saw the bytes"

    # Snapshot the upstream log AFTER the seeding upload, so anything new is a
    # fall-through.
    upstream_before = upstream["log_path"].read_text()

    # Request the name through the node. B lacks the claim (the dangerous absence
    # that could permit upstream), but the decision is settled on the write home,
    # which owns it privately: the node serves the PRIVATE bytes, read through
    # from A, and never consults upstream.
    code, body, _ = http_get(f"{server['base_url']}/files/{pkg}/{private_wheel.name}", timeout=15)
    assert code == 200
    assert body == private_wheel.read_bytes()
    assert hashlib.sha256(body).hexdigest() != hashlib.sha256(public_wheel.read_bytes()).hexdigest()

    # The index the resolver reads is private and local too.
    code, idx, _ = http_get(
        f"{server['simple']}{pkg}/index.json", headers={"Accept": ACCEPT_PEP691}
    )
    assert code == 200
    assert any(f["filename"] == private_wheel.name for f in json.loads(idx)["files"])

    # The upstream proves it: not one request carried this name after the seed.
    new_upstream = upstream["log_path"].read_text()[len(upstream_before) :]
    assert pkg not in new_upstream, f"upstream was consulted for a private name:\n{new_upstream}"


def test_no_region_node_reads_from_the_write_bucket(s3_server_read_affinity_no_region, tmp_path):
    """Fail-safe default: a node that matches no region bucket keeps its read pin
    equal to its write pin (A). The fan-out still copies to B, but reads never
    touch it."""
    server = s3_server_read_affinity_no_region
    minio = server["minio"]
    a, b = minio["buckets"]
    faults = server["faults"]

    _eventually(lambda: _write_bucket(server) == a, what="writes home to A")
    _eventually(lambda: _read_bucket(server) == a, what="reads follow the write bucket A")
    assert _read_bucket(server) == _write_bucket(server)

    pkg = "noregion"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    akey = f"packages/{pkg}/{wheel.name}"
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)
    _eventually(lambda: minio_key_exists_in(minio, b, akey), what="fan-out still copies to B")

    before_a = _artifact_gets(faults, a, akey)
    before_b = _artifact_gets(faults, b, akey)
    code, body, _ = http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}", timeout=30)
    assert code == 200
    assert body == wheel.read_bytes()
    _eventually(
        lambda: _artifact_gets(faults, a, akey) > before_a,
        what="download served from the write bucket A",
    )
    assert _artifact_gets(faults, b, akey) == before_b, "the region bucket B is never read"
