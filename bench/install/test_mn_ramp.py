"""Unit tests for mn_ramp's pure adaptive ceiling finder (find_ceiling / run_ladder
/ summarize). No rig needed — a synthetic `measure(c)` models each server's
throughput-vs-concurrency curve, so we can prove the search converges on the knee
and classifies the bottleneck without spending an AWS fleet.

Run: uv run -- pytest bench/install/test_mn_ramp.py
(Not part of `make check`'s blackbox suite, which targets tests/.)
"""

from __future__ import annotations

import mn_ramp


def mk(c: int, installs: float, cpu: float, mbs: float | None = None, ok: float = 100.0) -> dict:
    """A ramp-step dict shaped like measure_step() output (N=1: agg == per-node)."""
    return {
        "per_node_c": c,
        "agg_concurrency": c,
        "agg_rps": round(installs * 13.0, 1),
        "installs_per_sec": round(installs, 1),
        "agg_mb_per_sec": round(installs if mbs is None else mbs, 1),
        "p99_ms": 100.0,
        "ok_pct": ok,
        "server_cpu_pct": cpu,
    }


def cliff(knee: int, ceil: float, cpu_at_knee: float):
    """pypiron/bandersnatch-like: installs rise ~linearly to `ceil` at `knee`, then
    over-concurrency collapses throughput and craters wheel bytes."""

    def measure(c: int) -> dict:
        if c <= knee:
            f = c / knee
            return mk(c, ceil * f, cpu_at_knee * f, mbs=ceil * f)
        return mk(c, ceil * 0.55, cpu_at_knee * 1.05, mbs=ceil * 0.2)  # bytes crater

    return measure


def plateau(knee: int, ceil: float, cpu_sat: float):
    """pypiserver-like: rises to `ceil` at a low knee, then CPU-saturated and flat."""

    def measure(c: int) -> dict:
        if c < knee:
            f = c / knee
            return mk(c, ceil * f, min(cpu_sat, 60 + 80 * f), mbs=ceil * f)
        return mk(c, ceil, cpu_sat, mbs=ceil)

    return measure


def rising(slope: float, cpu_per_c: float):
    """Monotonic — the server never saturates within c_max (loadgen/rig-limited)."""

    def measure(c: int) -> dict:
        return mk(c, slope * c, min(50.0, cpu_per_c * c), mbs=slope * c)

    return measure


def saturate_low(peak_c: int, peak_val: float, cpu_sat: float):
    """Single-worker-like (proxpi): peaks at a TINY concurrency, then over-
    concurrency thrashes throughput down (~1/c); CPU pegged from the first step."""

    def measure(c: int) -> dict:
        inst = peak_val * (c / peak_c if c <= peak_c else peak_c / c)
        return mk(c, inst, cpu_sat, mbs=inst)

    return measure


def test_cliff_pins_knee_above_the_coarse_step_and_reports_server_bound():
    # Knee at c=10000: coarse doubling lands 8192 (healthy) then 16384 (collapse).
    # The 65k->98k gap that motivated this: the refine MUST sample between them and
    # find a higher sustained throughput than the coarse 8192 step.
    measure = cliff(knee=10000, ceil=2900, cpu_at_knee=180)
    ramp, breach = mn_ramp.find_ceiling(measure, c_start=64, c_max=65536, cpu_break=190)
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=190)

    assert breach == "collapse"
    assert bound == "server-bound"  # cpu peaked ~180-189 >= 0.85*190
    assert peak["installs_per_sec"] > 2900 * 8192 / 10000  # beats the coarse 8192 step
    assert peak["per_node_c"] <= 10000  # never reports a collapsed point
    assert any(8192 < s["per_node_c"] < 16384 for s in ramp)  # closed the gap


def test_plateau_stops_at_cpu_saturation_without_over_ramping():
    measure = plateau(knee=512, ceil=85, cpu_sat=100)
    ramp, breach = mn_ramp.find_ceiling(measure, c_start=64, c_max=65536, cpu_break=95)
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=95)

    assert breach == "server-cpu"
    assert bound == "server-bound"
    assert abs(peak["installs_per_sec"] - 85) < 1.0
    assert max(s["per_node_c"] for s in ramp) < 5000  # didn't push to c_max


def test_rig_limited_when_server_never_saturates():
    measure = rising(slope=0.05, cpu_per_c=0.0001)  # throughput grows, cpu pinned low
    ramp, breach = mn_ramp.find_ceiling(measure, c_start=64, c_max=8192, cpu_break=190)
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=190)

    assert breach == "rig-cap"
    assert bound == "rig-limited"
    assert peak["per_node_c"] == 8192  # the cap is the reported lower bound


def test_fixed_ladder_back_compat_picks_last_sustained():
    seq = {
        256: mk(256, 500, 80),
        512: mk(512, 900, 140),
        1024: mk(1024, 1200, 175),
        2048: mk(2048, 800, 178),
    }
    ramp, breach = mn_ramp.run_ladder(lambda c: seq[c], [256, 512, 1024, 2048], cpu_break=190)
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=190)

    assert breach == "collapse"  # 1200 -> 800 is retrograde
    assert peak["installs_per_sec"] == 1200  # the 1024 step, not the collapsed 2048
    assert bound == "server-bound"


def test_is_collapse_signals():
    assert mn_ramp.is_collapse(mk(1, 100, 50, ok=97.0), 100, 100) == "errors"
    assert mn_ramp.is_collapse(mk(1, 100, 50, mbs=10), 100, 100) == "collapse"  # bytes cratered
    assert (
        mn_ramp.is_collapse(mk(1, 50, 50, mbs=100), 100, 100) == "collapse"
    )  # installs retrograde
    assert mn_ramp.is_collapse(mk(1, 100, 50, mbs=100), 100, 100) is None  # healthy


def test_walks_down_when_c_start_overshoots_the_knee():
    # Knee at c=2, but c_start=16 starts well past it (the proxpi case). The
    # downward walk must recover the real peak, not the saturated declining tail.
    measure = saturate_low(peak_c=2, peak_val=30, cpu_sat=180)
    ramp, _ = mn_ramp.find_ceiling(measure, c_start=16, c_max=4096, cpu_break=100)
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=100)

    assert peak["per_node_c"] <= 4  # found the low knee, not the c>=16 tail
    assert peak["installs_per_sec"] > 25  # ~30 at the knee, not ~4 at c=16
    assert bound == "server-bound"


def test_collapse_below_cpu_bar_is_server_bound_not_rig_limited():
    # The pypiron case: throughput knees then COLLAPSES (a server breaking point),
    # yet peak CPU (~121% of 200%) never reaches the naive 0.85*cpu_break bar. A
    # rig-limited run can't collapse the server, so the collapse settles it.
    measure = cliff(knee=8000, ceil=3000, cpu_at_knee=115)  # max cpu ~121
    ramp, breach = mn_ramp.find_ceiling(measure, c_start=64, c_max=65536, cpu_break=190)
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=190)

    assert breach == "collapse"
    assert max(s["server_cpu_pct"] for s in ramp) < 0.85 * 190  # under the naive bar
    assert bound == "server-bound"  # but the collapse is dispositive
