"""Unit tests for capacity.analyze_ramp (the one pure function — ramp -> verdict).

Run: uv run -- pytest bench/install/test_capacity.py
(Not part of `make check`'s blackbox suite, which targets tests/.)
"""

from __future__ import annotations

import capacity


def step(c, rps, p99, ok=100.0):
    return {
        "connections": c,
        "rps": rps,
        "p50_ms": p99 / 2,
        "p95_ms": p99 * 0.9,
        "p99_ms": p99,
        "status_ok_pct": ok,
    }


def test_latency_breach_picks_last_good_knee():
    # rps keeps rising but p99 crosses the 50ms ceiling at c=64 -> knee is c=32.
    steps = [step(8, 400, 10), step(32, 1200, 40), step(64, 1500, 80)]
    out = capacity.analyze_ramp(steps, ceiling_ms=50.0)
    assert out["c_knee"] == 32
    assert out["mst_rps"] == 1200
    assert out["breach_mode"] == "latency"
    assert out["broke"] is True


def test_error_breach():
    steps = [step(8, 400, 10), step(32, 1200, 20), step(64, 1300, 30, ok=97.0)]
    out = capacity.analyze_ramp(steps, ceiling_ms=50.0)
    assert out["c_knee"] == 32
    assert out["breach_mode"] == "errors"


def test_collapse_breach():
    # throughput goes retrograde (thrashing) at the top step.
    steps = [step(32, 1500, 20), step(64, 1600, 30), step(128, 900, 45)]
    out = capacity.analyze_ramp(steps, ceiling_ms=50.0)
    assert out["breach_mode"] == "collapse"
    assert out["peak_rps"] == 1600


def test_no_break_within_ladder():
    steps = [step(8, 400, 10), step(64, 1500, 20), step(256, 3000, 35)]
    out = capacity.analyze_ramp(steps, ceiling_ms=50.0)
    assert out["broke"] is False
    assert out["mst_rps"] == 3000
    assert out["breach_mode"] == "none(ladder-cap)"
