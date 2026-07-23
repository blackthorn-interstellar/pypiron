"""CI lane of the per-endpoint micro-benchmarks (dev/bench/MICROBENCH.md).

Deterministic asserts only: the endpoint table covers every route in the
router (drift check), each endpoint's exact per-request storage-op counts
match its pins, response sizes stay sane, and the write-probe's
snapshot/restore leaves the tree byte-identical (proven here by a full walk —
cheap at this tier — which is what keeps the tracked lane's restore-set list
honest at 50k where a full walk is not cheap).

Wall-times are never asserted here; the tracked lane (make microbench) owns
timing.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "dev" / "bench"))

import endpoints as eptab  # noqa: E402
import fabricate  # noqa: E402
import microbench  # noqa: E402

pytestmark = [pytest.mark.integration]


def test_route_table_drift():
    """Every router route has a bench entry; every bench entry has a route."""
    in_router = eptab.parse_app_routes()
    covered = eptab.covered_routes()
    missing = in_router - covered
    stale = covered - in_router
    assert not missing, f"routes with no bench entry (add to dev/bench/endpoints.py): {missing}"
    assert not stale, f"bench entries for routes that no longer exist: {stale}"


def _full_tree_digest(data_dir: Path) -> dict:
    out = {}
    for p in sorted(Path(data_dir).rglob("*")):
        if p.is_file():
            out[str(p.relative_to(data_dir))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def test_endpoint_op_pins(tmp_path, pypiron_bin):
    """Walk the whole table in order; measured op counts must equal the pins.

    The walk mirrors the pins-harvest exactly (same order, same iteration
    indices) because global endpoints share caches: cold pins are defined by
    this fixed order. To refresh pins after an intentional storage-access
    change: dev/bench/microbench.py pins --bin target/debug/pypiron
    """
    data = tmp_path / "data"
    manifest = fabricate.fabricate_tree(data, microbench.CI_PACKAGES)
    server = eptab.start_server(pypiron_bin, data, tmp_path / "server.log", sweep=True)
    try:
        base = server["base"]
        eptab.wait_swept(base, manifest["packages"], timeout=120)
        ctx = eptab.assign_targets(data)

        # Everything the write probes may touch, snapshotted before any write.
        snap = microbench.snapshot_aux(data, tmp_path / "aux-snapshot")
        pre_writes = _full_tree_digest(data)

        failures = []
        for ep in eptab.ENDPOINTS:
            cold = eptab.measure_ops(base, ep, ctx, 0)
            warm = eptab.measure_ops(base, ep, ctx, 1)
            warm2 = eptab.measure_ops(base, ep, ctx, 2)
            _, _, body = eptab.hit(base, ep, ctx, 3)
            if ep.cold_ops is not None and cold != ep.cold_ops:
                failures.append(f"{ep.name}: cold ops {cold} != pinned {ep.cold_ops}")
            if ep.warm_ops is not None:
                if warm != ep.warm_ops:
                    failures.append(f"{ep.name}: warm ops {warm} != pinned {ep.warm_ops}")
                if warm2 != warm:
                    failures.append(f"{ep.name}: warm ops unstable: {warm} then {warm2}")
            if ep.bytes_range is not None and not ep.mutates:
                lo, hi = ep.bytes_range
                if not lo <= len(body) <= hi:
                    failures.append(f"{ep.name}: {len(body)} bytes outside [{lo}, {hi}]")
        assert not failures, "op pins drifted:\n  " + "\n  ".join(failures)
    finally:
        eptab.stop_server(server)

    # Constraint 7: restore, then prove byte-identity with a full walk. Any
    # file the write probes touched that snapshot_aux did not cover shows up
    # here — the tracked lane relies on that set being complete.
    microbench.restore_aux(data, tmp_path / "aux-snapshot", snap, "mb-probe")
    microbench.verify_pristine(data, snap, "mb-probe")
    assert _full_tree_digest(data) == pre_writes, (
        "write probes touched files outside the snapshot/restore set"
    )
