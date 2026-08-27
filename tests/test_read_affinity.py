"""Read affinity: every region serves reads from its own bucket, writes keep a
single home.

One real pypiron node runs over two region-labeled buckets on MinIO
(`s3://a@left,s3://b@right`) and declares its region as `right`, so its write
pin is the config-order home A and its read pin is the region bucket B. A single
bucket-aware fault proxy sees every S3 request, so each test asserts WHICH bucket
answered a read from the ordered request log, independent of what the node
reports. Ground truth is read through `mc`, bypassing the proxy.

Covered: read locality (bytes from the region bucket, fan-out to both), the
polarity rule's read-through on an absent artifact (no client 404 for an acked
file), the accepted lag window for a yank the region bucket missed, failover off
a sustained outage with a drain-gated return, the read pin a dead write home
borrows and gets back when it returns, the fail-closed floor for a private name
whose claim the region bucket has not yet seen, and the no-region fail-safe
default.

The two pins fail over independently, so both directions are covered: the node's
own region bucket dying (reads move, writes do not) and the *preferred* bucket —
the first in config order, the fleet-wide write home — dying while this node
reads elsewhere (writes move, reads do not). The write-home case runs a
three-region topology on purpose, so "the read pin stayed home" and "the read pin
followed the write pin" are distinguishable outcomes. A node whose region bucket
is already dark when it boots is covered too: startup succeeds, reads follow the
write bucket, and they move home once it recovers. So is the reverse boot — the
write home already dark: both pins land on the region bucket as a loan, and when
the home returns, writes go back to it while reads re-earn the region bucket
through the drain gate instead of following the write pin forever.
"""

from __future__ import annotations

import hashlib
import json
import re
import time

import pytest

from .conftest import (
    READ_AFFINITY_NODE_REGION,
    READ_AFFINITY_WRITE_REGION,
    _start_read_affinity_server,
    minio_delete_key_in,
    minio_get_key_in,
    minio_key_exists_in,
    minio_list_keys_in,
    minio_object_sha256,
    minio_put_key_in,
    s3_repl_tag,
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


def _owe_the_region_bucket_forever(
    minio, write_bucket: str, region_bucket: str, pkg: str, filename: str
) -> list[str]:
    """Seed a `_repl/` note owed to the region bucket that the sweep can never
    drain, so the drain gate is provably what holds reads — isolated from the
    healthy-return window, and for as long as a test wants. The source record's
    sidecar is unreadable, which nothing may fabricate over (an operator problem
    by doctrine — a sha mismatch no longer qualifies, since reconcile heals
    those), so the copy fails and the sweep retains the note. Returns the keys to
    delete when the test wants nothing owed."""
    akey = f"packages/{pkg}/{filename}"
    minio_put_key_in(
        minio,
        write_bucket,
        f"packages/{pkg}/.origin",
        json.dumps({"origin": "private", "nonce": "b" * 32}),
    )
    minio_put_key_in(minio, write_bucket, f"{akey}.meta.json", "not json")
    minio_put_key_in(minio, write_bucket, akey, "bytes")
    note = f"_repl/{s3_repl_tag(region_bucket)}/{pkg}/{filename}!hold"
    minio_put_key_in(minio, write_bucket, note, "")
    return [note, akey, f"{akey}.meta.json", f"packages/{pkg}/.origin"]


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
    assert any(
        k.startswith(f"_repl/{s3_repl_tag(b)}/{pkg}/") for k in minio_list_keys_in(minio, a)
    ), "A carries the _repl note owed to B"
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
    # Mirror image of the loan test's wait: the outage below leaves reads on A
    # with no read-through behind them, so A must hold its own package index —
    # a derived view the worker rebuilds after the upload acks — before B dies.
    _eventually(
        lambda: base_wheel.name in minio_get_key_in(minio, a, f"simple/{base_pkg}/index.json"),
        what="A built its own index for the baseline",
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
        lambda: any(
            k.startswith(f"_repl/{s3_repl_tag(b)}/{out_pkg}/") for k in minio_list_keys_in(minio, a)
        ),
        what="outage publish owes B a note",
    )
    assert not minio_key_exists_in(minio, b, out_key)

    # The real note above drains in a blink once B is back — too fast to observe
    # the gate holding reads off B. Seed a second note B can never satisfy on its
    # own, so what keeps reads on A is provably the drain gate.
    hold_keys = _owe_the_region_bucket_forever(
        minio, a, b, "holdopen", "holdopen-1.0-py3-none-any.whl"
    )

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
    for key in hold_keys:
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


def test_the_read_pin_is_lent_to_the_region_bucket_and_taken_back(
    s3_server_read_affinity, tmp_path, uv_path, uv_venv
):
    """The loan: while the write home is dead, reads are served from the region
    bucket even though the drain gate has not opened — a reachable bucket beats a
    dead one, and installs keep working. That grant is only borrowed from the
    write pin: when the write home returns, reads leave the region bucket again
    until the gate lets them back, because it still owes a repair note.

    Reads must first be off the region bucket for the loan to exist at all: a
    node whose region bucket was converged at startup already serves reads from
    it, and that pin is earned, not borrowed. So the test first moves reads to
    the write home with a sustained region outage and keeps them there with a
    note the region bucket can never satisfy."""
    server = s3_server_read_affinity
    minio = server["minio"]
    a, b = minio["buckets"]
    faults = server["faults"]

    _eventually(lambda: _read_bucket(server) == b, what="reads pin to B")
    _eventually(lambda: _write_bucket(server) == a, what="writes home to A")

    # A record both buckets hold, so a client can install while either is dark.
    pkg = "loanpkg"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    akey = f"packages/{pkg}/{wheel.name}"
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)
    _eventually(lambda: minio_key_exists_in(minio, b, akey), what="record fanned out to B")
    # The bytes are not enough: indexes are per-bucket derived views rebuilt from
    # the `_dirty/` markers replication drops, so B holds the record well before
    # it can answer a resolver from its own index. The served index reads through
    # to the write home meanwhile — which is exactly what killing A below takes
    # away, so wait for B to stand alone or the install races the rebuild.
    _eventually(
        lambda: wheel.name in minio_get_key_in(minio, b, f"simple/{pkg}/index.json"),
        what="B built its own index for the record",
    )

    # Owe B a note it can never satisfy, then move reads off B with a sustained
    # outage. B heals, but the owed note keeps reads on A: reads now have no
    # earned claim on B, which is the precondition for a loan.
    hold_keys = _owe_the_region_bucket_forever(
        minio, a, b, "loanhold", "loanhold-1.0-py3-none-any.whl"
    )
    faults.fail(b)
    _eventually(lambda: _read_bucket(server) == a, what="reads leave the dead region bucket")
    faults.recover(b)
    _eventually(lambda: _bucket_health(server, b) == 1, timeout=15, what="B healthy again")
    settle = time.monotonic() + 3.0
    while time.monotonic() < settle:
        assert _read_bucket(server) == a, "reads returned to B while a repair note was owed"
        time.sleep(0.2)

    # Kill the write home. Writes fail over onto the region bucket, and reads are
    # lent to it: pinning reads to a dead A would serve nothing.
    faults.fail(a)
    _eventually(lambda: _write_bucket(server) == b, what="writes fail over to the region bucket")
    _eventually(lambda: _read_bucket(server) == b, what="reads are lent to the new write home")
    _install(server, uv_path, uv_venv, pkg)

    # The write home returns: the loan ends with it. B still owes the note, so
    # reads go back to A instead of latching on a bucket that never passed the
    # gate — and they stay there.
    faults.recover(a)
    _eventually(lambda: _write_bucket(server) == a, what="writes return home")
    _eventually(
        lambda: _read_bucket(server) == a,
        timeout=20,
        what="the read loan is surrendered when the write pin goes home",
    )
    settle = time.monotonic() + 3.0
    while time.monotonic() < settle:
        assert _read_bucket(server) == a, "reads stayed on B while a repair note was still owed"
        time.sleep(0.2)

    # Nothing owed: reads earn their way back through the gate.
    for key in hold_keys:
        minio_delete_key_in(minio, a, key)
    _eventually(
        lambda: _read_bucket(server) == b,
        timeout=20,
        what="reads return to B once it is caught up",
    )


def test_read_affinity_survives_a_boot_with_the_write_home_down(
    s3_server_read_affinity_home_down, tmp_path, uv_path, uv_venv
):
    """A node that boots while its write home is dark must not lose region read
    affinity for the rest of its life.

    Startup fails the write pin over onto the region bucket B, and cannot call B
    converged — an unreachable peer alone forces that verdict — so reads sit on B
    only because the write pin does. Recording that as a pin B *earned* is fatal
    in a quiet way: when A returns and the write pin goes home there is no loan to
    expire, so the read pin never moves again, the worker never proposes a read
    switch, the distinct read pin is never activated, and every read is served
    from A forever. Reads being correct the whole time is exactly why nothing
    else catches it. The end state asserted here is the point: writes home on A,
    reads back on B."""
    server = s3_server_read_affinity_home_down
    minio = server["minio"]
    a, b = minio["buckets"]
    faults = server["faults"]

    # Boot landed on the region bucket for both pins: A is dark, B is all there is.
    _eventually(lambda: _write_bucket(server) == b, what="writes fail over to B at boot")
    _eventually(lambda: _read_bucket(server) == b, what="reads follow the write pin to B")

    # A record published during the outage, so there is something to serve.
    pkg = "bootloan"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _upload(server, wheel)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    # The write home comes back and the write pin returns to it.
    faults.recover(a)
    _eventually(lambda: _bucket_health(server, a) == 1, timeout=20, what="A healthy again")
    _eventually(lambda: _write_bucket(server) == a, timeout=30, what="writes return home to A")

    # The loan is surrendered with the write pin, and B then earns the read pin
    # back through the ordinary drain gate. Before the fix reads stayed on A for
    # the life of the process, so this is the assertion that fails pre-fix.
    _eventually(
        lambda: _read_bucket(server) == b,
        timeout=30,
        what="region read affinity is alive after the write pin goes home",
    )
    settle = time.monotonic() + 3.0
    while time.monotonic() < settle:
        assert _write_bucket(server) == a, "writes drifted off the home bucket"
        assert _read_bucket(server) == b, "reads drifted off the region bucket"
        time.sleep(0.2)

    _install(server, uv_path, uv_venv, pkg)


#: Region label on the third bucket of the write-failover topology — a region no
#: node in these tests claims, so it is only ever reached as the write fallback.
THIRD_REGION = "middle"


def _three_region_buckets_uri(minio) -> str:
    """`PYPIRON_BUCKETS` for a three-region topology whose node region sits LAST:
    the preferred bucket A (`@left`, the fleet-wide write home), an unrelated
    region C (`@middle`), then the node's own region B (`@right`). The order is
    the whole point — when A dies the write selection takes the most-preferred
    healthy bucket, which is C and NOT the node's region bucket, so "reads stayed
    home" and "reads followed the write pin" are different observations. A
    two-bucket topology cannot tell them apart."""
    a, c, b = minio["buckets"][:3]
    return (
        f"s3://{a}@{READ_AFFINITY_WRITE_REGION},"
        f"s3://{c}@{THIRD_REGION},"
        f"s3://{b}@{READ_AFFINITY_NODE_REGION}"
    )


def test_preferred_bucket_failover_leaves_the_region_read_pin_alone(
    tmp_path_factory, pypiron_bin, minio_three_proxy, tmp_path, uv_path, uv_venv
):
    """The other direction of failover: the *preferred* bucket — first in config
    order, the fleet-wide write home — dies while this node reads from a different
    region. Writes move to the next healthy bucket in preference order; the read
    pin never budges, and no `read bucket changed` is ever logged. A publish during
    the outage lands on the new write home, still fans out to the region bucket
    pre-ack, and owes the dead preferred bucket a repair note — and the client
    reading that brand-new file is still served in-region."""
    minio = minio_three_proxy
    a, c, b = minio["buckets"][:3]
    faults = minio["faults"]
    server_gen = _start_read_affinity_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        node_region=READ_AFFINITY_NODE_REGION,
        # 3, not 1: the proxy hard-fails every request to A, so its failover
        # still lands in a few probes — but the healthy region bucket B must
        # tolerate an isolated slow request on a loaded CI runner. Strikes are
        # consecutive and reset on success, so one blip at 1 moved the read pin
        # off B and the drain-gated return kept it away for the rest of the test.
        leave_failures=3,
        return_healthy_secs=2,
        extra_env={"PYPIRON_BUCKETS": _three_region_buckets_uri(minio)},
    )
    try:
        server = next(server_gen)
        _eventually(lambda: _read_bucket(server) == b, what="reads pin to the region bucket B")
        _eventually(
            lambda: _write_bucket(server) == a, what="writes home to the preferred bucket A"
        )

        base_pkg = "writehomebase"
        base_wheel = make_wheel(base_pkg, "1.0", tmp_path)
        base_key = f"packages/{base_pkg}/{base_wheel.name}"
        _upload(server, base_wheel)
        wait_for_file_in_index(server["simple"], base_pkg, base_wheel.name)
        _eventually(
            lambda: minio_key_exists_in(minio, b, base_key), what="baseline fanned out to B"
        )

        # Kill the preferred bucket. This is the fleet's write home, not this
        # node's read region.
        faults.fail(a)
        _eventually(
            lambda: _write_bucket(server) == c,
            what="writes move to the next healthy bucket in preference order",
        )
        assert _bucket_health(server, a) == -1, "the preferred bucket is known unhealthy"
        assert _read_bucket(server) == b, "the read pin left its own healthy region bucket"
        assert "read bucket changed" not in server["log_path"].read_text(), (
            "a write-side failover perturbed the read pin"
        )

        # A publish during the outage: the new write home takes it, the region
        # bucket still gets its pre-ack copy, and the dark preferred bucket (index
        # 0) is owed a repair note.
        out_pkg = "writehomeoutage"
        out_wheel = make_wheel(out_pkg, "1.0", tmp_path)
        out_key = f"packages/{out_pkg}/{out_wheel.name}"
        _upload(server, out_wheel)
        wait_for_file_in_index(server["simple"], out_pkg, out_wheel.name)
        assert minio_key_exists_in(minio, c, out_key), "the new write home holds the record"
        _eventually(
            lambda: minio_key_exists_in(minio, b, out_key),
            what="the outage publish still fanned out to the region bucket B",
        )
        _eventually(
            lambda: any(
                k.startswith(f"_repl/{s3_repl_tag(a)}/{out_pkg}/")
                for k in minio_list_keys_in(minio, c)
            ),
            what="the dead preferred bucket is owed a repair note",
        )

        # The pins are still where they belong, and the client read of the
        # brand-new file is served in-region.
        assert _write_bucket(server) == c
        assert _read_bucket(server) == b
        before = _artifact_gets(faults, b, out_key)
        code, body, _ = http_get(
            f"{server['base_url']}/files/{out_pkg}/{out_wheel.name}", timeout=30
        )
        assert code == 200
        assert body == out_wheel.read_bytes()
        _eventually(
            lambda: _artifact_gets(faults, b, out_key) > before,
            what="reads are still served from the node's own region bucket",
        )
        # And a real client resolves and installs through the node throughout.
        _install(server, uv_path, uv_venv, out_pkg)
    finally:
        server_gen.close()


def test_boot_with_the_region_bucket_unreachable_reads_from_the_write_bucket(
    tmp_path_factory, pypiron_bin, minio_two_proxy, tmp_path, uv_path, uv_venv
):
    """The outage read affinity exists for, present before the node ever starts:
    the region bucket is already dark at boot. Startup must succeed anyway, warn
    that reads follow the write bucket until it recovers, and serve every read —
    including a real install — from the write home. Once the region bucket is back
    and owes nothing, reads move home on their own."""
    minio = minio_two_proxy
    a, b = minio["buckets"]
    faults = minio["faults"]
    faults.fail(b)
    server_gen = _start_read_affinity_server(
        tmp_path_factory,
        pypiron_bin,
        minio,
        node_region=READ_AFFINITY_NODE_REGION,
        leave_failures=1,
        return_healthy_secs=2,
    )
    try:
        # Startup completes and serves (the fixture waits on a real HTTP 200) even
        # though the node's region bucket answered nothing.
        server = next(server_gen)
        assert "read affinity: region bucket unreachable at startup" in (
            server["log_path"].read_text()
        ), "the startup warn line for an unreachable region bucket is missing"
        _eventually(lambda: _write_bucket(server) == a, what="writes home to A")
        _eventually(lambda: _read_bucket(server) == a, what="reads follow the write bucket A")
        assert _read_bucket(server) == _write_bucket(server), "no distinct read pin was seeded"

        # Publishing and serving work on the write home while B is dark; B is owed
        # the fan-out it missed.
        pkg = "bootdark"
        wheel = make_wheel(pkg, "1.0", tmp_path)
        akey = f"packages/{pkg}/{wheel.name}"
        _upload(server, wheel)
        wait_for_file_in_index(server["simple"], pkg, wheel.name)
        assert minio_key_exists_in(minio, a, akey)
        assert not minio_key_exists_in(minio, b, akey), "B was dark for the fan-out"
        _eventually(
            lambda: any(
                k.startswith(f"_repl/{s3_repl_tag(b)}/{pkg}/") for k in minio_list_keys_in(minio, a)
            ),
            what="A carries the note owed to the dark region bucket",
        )

        before = _artifact_gets(faults, a, akey)
        code, body, _ = http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}", timeout=30)
        assert code == 200
        assert body == wheel.read_bytes()
        _eventually(
            lambda: _artifact_gets(faults, a, akey) > before,
            what="the download is served from the write bucket A",
        )
        _install(server, uv_path, uv_venv, pkg)

        # "until it recovers": B comes back, the note drains, and reads move home.
        faults.recover(b)
        _eventually(
            lambda: _bucket_health(server, b) == 1, timeout=15, what="B observed healthy again"
        )
        _eventually(
            lambda: _read_bucket(server) == b,
            timeout=30,
            what="reads move to the region bucket once it recovers and owes nothing",
        )
        assert minio_key_exists_in(minio, b, akey), "B caught up before it served a read"
        assert _write_bucket(server) == a, "the write pin never moved"
        # The positive form of the signal the write-home failover test asserts is
        # absent: a read pin that actually moves says so in the log.
        assert "read bucket changed" in server["log_path"].read_text(), (
            "moving the read pin home is logged"
        )
    finally:
        server_gen.close()


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

    # The index the resolver reads is private and local too. The write home
    # materializes its package index asynchronously after the claim lands, and
    # the region-bucket read-through only surfaces it once A has built it — so
    # poll for the file instead of racing that indexer with a bare GET.
    idx = wait_for_file_in_index(server["simple"], pkg, private_wheel.name)
    assert any(f["filename"] == private_wheel.name for f in idx["files"])

    # The upstream proves it: not one request carried this name after the seed.
    new_upstream = upstream["log_path"].read_text()[len(upstream_before) :]
    assert pkg not in new_upstream, f"upstream was consulted for a private name:\n{new_upstream}"


def test_a_short_read_return_window_is_warned_about_at_startup(
    tmp_path_factory, pypiron_bin, minio_two_proxy
):
    """A return window no longer than one health cycle lets a recovered region
    bucket take reads back on a check that is already a cycle old. The cost is
    bounded — a missed file is read through from the write bucket — so startup
    says so and boots. Both sides: 2 s over two buckets is under the 5 s floor and
    warns; 600 s clears it and says nothing, with the read-affinity lines proving
    the node really did configure region reads. Servers run one at a time so the
    two nodes never share the topology."""
    short_gen = _start_read_affinity_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_proxy,
        node_region=READ_AFFINITY_NODE_REGION,
        leave_failures=3,
        return_healthy_secs=2,
    )
    try:
        log = next(short_gen)["log_path"].read_text()
        assert "read affinity: --bucket-return-healthy-secs 2 is at or below the 5s" in log, (
            f"no startup warning for a below-floor return window:\n{log}"
        )
        assert "Raise --bucket-return-healthy-secs above 5" in log, "the warning states no remedy"
    finally:
        short_gen.close()

    long_gen = _start_read_affinity_server(
        tmp_path_factory,
        pypiron_bin,
        minio_two_proxy,
        node_region=READ_AFFINITY_NODE_REGION,
        leave_failures=3,
        return_healthy_secs=600,
    )
    try:
        log = next(long_gen)["log_path"].read_text()
        assert "region bucket" in log, (
            "read affinity was never configured, so absence proves nothing"
        )
        assert "--bucket-return-healthy-secs" not in log, (
            f"a window well above the floor was warned about anyway:\n{log}"
        )
    finally:
        long_gen.close()


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
