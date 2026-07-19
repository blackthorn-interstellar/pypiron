"""Unit tests for mn_ramp's pure byte-truthful ceiling finder (parse_oha /
parse_cpu_line / cpu_pct_from_stat / mix_ratio / aggregate_step / is_collapse /
summarize / find_ceiling / run_ladder). No rig needed — a synthetic `measure(c)`
models each server's throughput-vs-concurrency curve, and hand-built oha/proc-stat
fixtures exercise the parsers, so we prove the search converges on the WHEEL-count
knee and classifies the bottleneck without spending an AWS fleet.

Run: uv run -- pytest bench/install/test_mn_ramp.py
(Not part of `make check`'s blackbox suite, which targets tests/.)
"""

from __future__ import annotations

import json

import mn_ramp
import pytest

# Corpus constants for the synthetic curves (lite tier, tier-bleed inflated).
MEAN_MB = 3.263
WPI = 6.553
MIX_TOL = 0.20
CONSTS = {
    "wheels_per_install": WPI,
    "mean_wheel_bytes": MEAN_MB * 1e6,
    "wheel_frac": 0.5,
    "mix_tol": MIX_TOL,
}


def mk(
    c: int,
    installs: float,
    cpu: float,
    *,
    served_mean_mb: float = MEAN_MB,
    index_rps: float = 0.0,
    wheel_ok_pct: float = 100.0,
) -> dict:
    """A ramp-step dict shaped like aggregate_step() output (N=1: agg == per-node).
    A byte-crater is modelled by dropping `served_mean_mb` (trips mix_ok=False);
    real wheel failures by `wheel_ok_pct`."""
    wheel_rps = installs * WPI
    wheel_ok = int(round(wheel_rps * 15))
    wheel_bytes = wheel_ok * served_mean_mb * 1e6
    agg_mb = round(wheel_rps * served_mean_mb, 1)
    r = served_mean_mb / MEAN_MB
    mb_per_install = MEAN_MB * WPI
    return {
        "per_node_c": c,
        "agg_concurrency": c,
        "c_idx": c,
        "c_whl": c,
        "agg_rps": round(index_rps + wheel_rps, 1),
        "installs_per_sec": round(installs, 1),
        "agg_mb_per_sec": agg_mb,
        "p99_ms": 100.0,
        "ok_pct": wheel_ok_pct,
        "server_cpu_pct": cpu,
        "wheel_ok": wheel_ok,
        "wheel_total": max(wheel_ok, 1),
        "wheel_ok_pct": wheel_ok_pct,
        "wheel_bytes": wheel_bytes,
        "wheel_mb_per_sec": agg_mb,
        "wheel_rps": round(wheel_rps, 1),
        "wheel_p99_ms": 100.0,
        "index_ok": int(index_rps * 15),
        "index_total": max(int(index_rps * 15), 1),
        "index_ok_pct": 100.0,
        "index_rps": round(index_rps, 1),
        "index_p99_ms": 50.0,
        "mb_per_install": round(served_mean_mb * WPI, 3),
        "installs_from_bytes": round(agg_mb / mb_per_install, 1) if mb_per_install else 0.0,
        "mix_r": round(r, 3),
        "mix_ok": bool(wheel_ok > 0 and r >= 1 - MIX_TOL),
    }


# --- synthetic server curves -----------------------------------------------


def cliff(knee: int, ceil: float, cpu_at_knee: float):
    """pypiron/bandersnatch-like: installs rise ~linearly to `ceil` at `knee`, then
    over-concurrency craters wheel bytes (mix breach) and drives installs down."""

    def measure(c: int) -> dict:
        if c <= knee:
            f = c / knee
            return mk(c, ceil * f, cpu_at_knee * f)
        return mk(c, ceil * 0.55, cpu_at_knee * 1.05, served_mean_mb=MEAN_MB * 0.2)  # bytes crater

    return measure


def plateau(knee: int, ceil: float, cpu_sat: float):
    """pypiserver-like: rises to `ceil` at a low knee, then CPU-saturated and flat."""

    def measure(c: int) -> dict:
        if c < knee:
            f = c / knee
            return mk(c, ceil * f, min(cpu_sat, 60 + 80 * f))
        return mk(c, ceil, cpu_sat)

    return measure


def rising(slope: float, cpu_per_c: float):
    """Monotonic — the server never saturates within c_max (loadgen/rig-limited)."""

    def measure(c: int) -> dict:
        return mk(c, slope * c, min(50.0, cpu_per_c * c))

    return measure


def saturate_low(peak_c: int, peak_val: float, cpu_sat: float):
    """Single-worker-like (proxpi): peaks at a TINY concurrency, then over-
    concurrency thrashes throughput down (~1/c); CPU pegged from the first step."""

    def measure(c: int) -> dict:
        inst = peak_val * (c / peak_c if c <= peak_c else peak_c / c)
        return mk(c, inst, cpu_sat)

    return measure


# --- R4: server CPU from /proc/stat deltas ----------------------------------


def _stat(agg, c0, c1) -> str:
    return f"cpu {' '.join(map(str, agg))}\ncpu0 {' '.join(map(str, c0))}\ncpu1 {' '.join(map(str, c1))}\n"


def test_cpu_two_cores_90pct_busy_is_180():
    z = [0, 0, 0, 0, 0, 0, 0, 0]
    stat0 = _stat(z, z, z)
    # 900 user + 100 idle over the window = 90% busy; ×2 cores ×100 = 180.
    stat1 = _stat(
        [900, 0, 0, 100, 0, 0, 0, 0], [450, 0, 0, 50, 0, 0, 0, 0], [450, 0, 0, 50, 0, 0, 0, 0]
    )
    assert mn_ramp.cpu_pct_from_stat(stat0, stat1) == 180.0


def test_cpu_idle_is_zero():
    z = [0, 0, 0, 0, 0, 0, 0, 0]
    stat0 = _stat(z, z, z)
    stat1 = _stat(
        [0, 0, 0, 1000, 0, 0, 0, 0], [0, 0, 0, 500, 0, 0, 0, 0], [0, 0, 0, 500, 0, 0, 0, 0]
    )
    assert mn_ramp.cpu_pct_from_stat(stat0, stat1) == 0.0


def test_cpu_iowait_and_steal_excluded_from_busy():
    z = [0, 0, 0, 0, 0, 0, 0, 0]
    stat0 = _stat(z, z, z)
    # iowait (idx 4) + steal (idx 7) only — neither counts as busy.
    stat1 = _stat(
        [0, 0, 0, 0, 500, 0, 0, 500], [0, 0, 0, 0, 250, 0, 0, 250], [0, 0, 0, 0, 250, 0, 0, 250]
    )
    assert mn_ramp.cpu_pct_from_stat(stat0, stat1) == 0.0


def test_cpu_single_core_full_is_100():
    stat0 = "cpu 0 0 0 0 0 0 0 0\ncpu0 0 0 0 0 0 0 0 0\n"
    stat1 = "cpu 100 0 0 0 0 0 0 0\ncpu0 100 0 0 0 0 0 0 0\n"
    assert mn_ramp.cpu_pct_from_stat(stat0, stat1) == 100.0


def test_cpu_irq_softirq_count_as_busy():
    z = [0, 0, 0, 0, 0, 0, 0, 0]
    stat0 = _stat(z, z, z)
    # 300 irq (idx 5) + 300 softirq (idx 6) + 400 idle = 60% busy ×2 cores = 120.
    stat1 = _stat(
        [0, 0, 0, 400, 0, 300, 300, 0],
        [0, 0, 0, 200, 0, 150, 150, 0],
        [0, 0, 0, 200, 0, 150, 150, 0],
    )
    assert mn_ramp.cpu_pct_from_stat(stat0, stat1) == 120.0


def test_cpu_malformed_empty_and_zero_total_are_sentinel():
    assert mn_ramp.cpu_pct_from_stat("", "") == -1.0
    same = "cpu 5 0 5 90 0 0 0 0\ncpu0 5 0 5 90 0 0 0 0\n"
    assert mn_ramp.cpu_pct_from_stat(same, same) == -1.0  # zero delta -> zero total
    short0 = "cpu 1 2 3\ncpu0 1 2 3\n"
    short1 = "cpu 4 5 6\ncpu0 4 5 6\n"
    assert mn_ramp.cpu_pct_from_stat(short0, short1) == -1.0  # < 7 fields


def test_parse_cpu_line_counts_cores_not_aggregate():
    fields, cores = mn_ramp.parse_cpu_line(_stat([1, 0, 2, 3, 0, 0, 0, 0], [0] * 8, [0] * 8))
    assert cores == 2
    assert fields == [1, 0, 2, 3, 0, 0, 0, 0]


# --- R1/R3: parse_oha -------------------------------------------------------


def _sec(status, total_data, err=None, total_secs=15.0, p99=0.05) -> str:
    d = {
        "summary": {"totalData": total_data, "total": total_secs, "requestsPerSec": 9999.0},
        "statusCodeDistribution": status,
        "latencyPercentiles": {"p99": p99},
    }
    if err is not None:
        d["errorDistribution"] = err
    return json.dumps(d)


def test_parse_oha_wheel_section_counts_completed_200_and_bytes():
    sec = _sec({"200": 4139}, 4340056064, err={"aborted due to deadline": 6})
    r = mn_ramp.parse_oha(sec, "15s")
    assert r["ok"] == 4139
    assert r["bytes"] == 4340056064
    assert r["total"] == 4139  # deadline aborts excluded


def test_parse_oha_index_section_counts_completed_200():
    sec = _sec({"200": 5813}, 11701569, err={"aborted due to deadline": 6})
    assert mn_ramp.parse_oha(sec, "15s")["ok"] == 5813


def test_parse_oha_deadline_aborts_not_counted_as_failures():
    sec = _sec({"200": 100}, 1000, err={"aborted due to deadline": 50})
    r = mn_ramp.parse_oha(sec, "15s")
    assert r["ok"] == 100
    assert r["total"] == 100  # 50 deadline aborts are NOT failures


def test_parse_oha_real_errors_counted_in_total():
    sec = _sec({"200": 100}, 1000, err={"error reading a body from connection": 10})
    r = mn_ramp.parse_oha(sec, "15s")
    assert r["ok"] == 100
    assert r["total"] == 110  # real errors ARE in the denominator


def test_parse_oha_garbage_and_empty_are_safe():
    for junk in ("not json at all", "", "{"):
        r = mn_ramp.parse_oha(junk, "15s")
        assert r["ok"] == 0
        assert r["total"] >= 1


def test_parse_oha_rps_is_completed_200_rate_not_summary_rps():
    sec = _sec({"200": 300}, 1000, total_secs=15.0)  # summary.requestsPerSec = 9999
    assert mn_ramp.parse_oha(sec, "15s")["rps"] == 20.0  # 300/15, not 9999


# --- R1/R2/R6: aggregate_step + guards --------------------------------------


def _orow(*, w_ok, w_bytes, i_ok, i_bytes, w_dur=15.0, i_dur=15.0, c_idx=1, c_whl=1) -> dict:
    return {
        "c_idx": c_idx,
        "c_whl": c_whl,
        "whl": {
            "ok": w_ok,
            "total": max(w_ok, 1),
            "bytes": float(w_bytes),
            "dur": w_dur,
            "p99_ms": 100.0,
            "rps": w_ok / w_dur,
        },
        "idx": {
            "ok": i_ok,
            "total": max(i_ok, 1),
            "bytes": float(i_bytes),
            "dur": i_dur,
            "p99_ms": 50.0,
            "rps": i_ok / i_dur,
        },
    }


def test_installs_track_wheels_not_index():
    # Huge index completions, tiny wheel completions -> installs follows wheels.
    row = _orow(w_ok=30, w_bytes=30 * MEAN_MB * 1e6, i_ok=100000, i_bytes=100000 * 2000)
    step = mn_ramp.aggregate_step(512, [row], 50.0, CONSTS)
    assert step["index_rps"] > 6000  # index is enormous
    assert step["installs_per_sec"] < 1.0  # but installs follow the tiny wheel count
    assert step["wheel_rps"] == 2.0  # 30/15


def test_mix_ok_false_when_served_bytes_below_band():
    row = _orow(w_ok=1000, w_bytes=1000 * MEAN_MB * 0.2 * 1e6, i_ok=1000, i_bytes=1000 * 2000)
    step = mn_ramp.aggregate_step(512, [row], 50.0, CONSTS)
    assert step["mix_ok"] is False
    assert mn_ramp.is_collapse(step, best_installs=1000.0) == "mix"


def test_mix_ok_true_on_matching_bytes():
    row = _orow(w_ok=1000, w_bytes=1000 * MEAN_MB * 1e6, i_ok=1000, i_bytes=1000 * 2000)
    step = mn_ramp.aggregate_step(512, [row], 50.0, CONSTS)
    assert step["mix_ok"] is True
    assert step["mix_r"] == 1.0


def test_installs_from_bytes_agrees_with_wheel_count_on_healthy_step():
    row = _orow(w_ok=655, w_bytes=655 * MEAN_MB * 1e6, i_ok=655, i_bytes=655 * 2000)
    step = mn_ramp.aggregate_step(512, [row], 50.0, CONSTS)
    lo, hi = 0.8 * step["installs_per_sec"], 1.2 * step["installs_per_sec"]
    assert lo <= step["installs_from_bytes"] <= hi


def test_mix_ratio_math():
    assert mn_ramp.mix_ratio(1000 * MEAN_MB * 1e6, 1000, MEAN_MB * 1e6) == 1.0
    assert mn_ramp.mix_ratio(0.0, 0, MEAN_MB * 1e6) == 0.0
    assert abs(mn_ramp.mix_ratio(500 * MEAN_MB * 1e6, 1000, MEAN_MB * 1e6) - 0.5) < 1e-9


def test_aggregate_step_raises_on_impossible_upward_anchor_well_sampled():
    row = _orow(w_ok=100, w_bytes=100 * MEAN_MB * 1.5 * 1e6, i_ok=100, i_bytes=100 * 2000)
    with pytest.raises(RuntimeError, match="byte-anchor broken"):
        mn_ramp.aggregate_step(512, [row], 50.0, CONSTS)


def test_aggregate_step_does_not_raise_on_tiny_sample_high_ratio():
    row = _orow(w_ok=10, w_bytes=10 * MEAN_MB * 2.0 * 1e6, i_ok=10, i_bytes=10 * 2000)
    step = mn_ramp.aggregate_step(512, [row], 50.0, CONSTS)  # wheel_ok < 30 -> no raise
    assert step["mix_r"] == 2.0


# --- is_collapse ------------------------------------------------------------


def test_is_collapse_signals():
    assert mn_ramp.is_collapse(mk(1, 100, 50, wheel_ok_pct=97.0), 100) == "errors"
    assert mn_ramp.is_collapse(mk(1, 100, 50, served_mean_mb=MEAN_MB * 0.2), 100) == "mix"
    assert mn_ramp.is_collapse(mk(1, 50, 50), 100) == "collapse"  # installs retrograde, mix healthy
    assert mn_ramp.is_collapse(mk(1, 100, 50), 100) is None  # healthy


# --- R5: summarize / bound + the search -------------------------------------


def test_cliff_pins_knee_above_the_coarse_step_and_reports_server_bound():
    measure = cliff(knee=10000, ceil=2900, cpu_at_knee=180)
    ramp, breach = mn_ramp.find_ceiling(measure, c_start=64, c_max=65536, cpu_break=190)
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=190)

    assert breach == "mix"  # the byte-crater over the knee
    assert bound == "server-bound"  # cpu peaked ~175-189 >= 0.85*190
    assert peak["installs_per_sec"] > 2900 * 8192 / 10000  # beats the coarse 8192 step
    assert peak["per_node_c"] <= 10000  # never reports a cratered point
    assert any(8192 < s["per_node_c"] < 16384 for s in ramp)  # closed the gap
    assert peak["mix_ok"]


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
        2048: mk(2048, 800, 178),  # retrograde installs, mix still healthy
    }
    ramp, breach = mn_ramp.run_ladder(lambda c: seq[c], [256, 512, 1024, 2048], cpu_break=190)
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=190)

    assert breach == "collapse"  # 1200 -> 800 is retrograde
    assert peak["installs_per_sec"] == 1200  # the 1024 step, not the collapsed 2048
    assert bound == "server-bound"


def test_walks_down_when_c_start_overshoots_the_knee():
    measure = saturate_low(peak_c=2, peak_val=30, cpu_sat=180)
    ramp, _ = mn_ramp.find_ceiling(measure, c_start=16, c_max=4096, cpu_break=100)
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=100)

    assert peak["per_node_c"] <= 4  # found the low knee, not the c>=16 tail
    assert peak["installs_per_sec"] > 25  # ~30 at the knee, not ~4 at c=16
    assert bound == "server-bound"


def test_collapse_below_cpu_bar_is_server_bound_not_rig_limited():
    # Throughput knees then craters (a real server break, index does NOT climb), yet
    # peak CPU (~121% of 200%) never reaches the naive 0.85*cpu_break bar. The genuine
    # post-knee decline while busy settles it as server-bound.
    measure = cliff(knee=8000, ceil=3000, cpu_at_knee=115)  # max cpu ~121
    ramp, breach = mn_ramp.find_ceiling(measure, c_start=64, c_max=65536, cpu_break=190)
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=190)

    assert breach == "mix"
    assert max(s["server_cpu_pct"] for s in ramp) < 0.85 * 190  # under the naive bar
    assert bound == "server-bound"  # but the post-knee decline while busy is dispositive


def test_peak_never_a_mix_collapse_or_errors_step():
    ramp = [
        mk(512, 900, 70),
        mk(1024, 1500, 90),  # highest installs but marked mix below
        mk(2048, 400, 40),
    ]
    ramp[1]["breach"] = "mix"
    ramp[1]["mix_ok"] = False
    peak, _, _ = mn_ramp.summarize(ramp, cpu_break=190)
    assert peak["per_node_c"] == 512  # not the higher-installs mix step


def test_fleet_thrash_2026_07_19_is_rig_limited_never_peak():
    cb = 190  # 2-core r7i.large, cpu_break = cores*95
    ramp = [
        mk(1024, 974, 66.0, served_mean_mb=MEAN_MB, index_rps=600),
        mk(32768, 520, 80.0, served_mean_mb=MEAN_MB * 0.22, index_rps=6000),
        mk(131072, 300, 30.0, served_mean_mb=MEAN_MB * 0.15, index_rps=60000),
        mk(311744, 210, 40.0, served_mean_mb=MEAN_MB * 0.09, index_rps=90000),
    ]
    for s, br in zip(ramp, [None, "mix", "mix", "mix"]):
        s["breach"] = br
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=cb)
    assert peak["per_node_c"] == 1024 and abs(peak["installs_per_sec"] - 974) < 1
    assert peak["mix_ok"] and all(not s["mix_ok"] for s in ramp[1:])
    assert bound == "rig-limited"


def test_genuine_post_knee_decline_while_busy_is_server_bound():
    # Sibling of the 2026-07-19 case: the deepest step now declines while the server
    # is BUSY and index did NOT climb -> a real knee -> server-bound.
    cb = 190
    ramp = [
        mk(1024, 974, 66.0, served_mean_mb=MEAN_MB, index_rps=600),
        mk(32768, 520, 80.0, served_mean_mb=MEAN_MB * 0.22, index_rps=6000),
        mk(131072, 300, 30.0, served_mean_mb=MEAN_MB * 0.15, index_rps=60000),
        mk(311744, 210, 170.0, served_mean_mb=MEAN_MB, index_rps=500),
    ]
    for s, br in zip(ramp, [None, "mix", "mix", "collapse"]):
        s["breach"] = br
    peak, bound, _ = mn_ramp.summarize(ramp, cpu_break=cb)
    assert peak["per_node_c"] == 1024
    assert bound == "server-bound"


# --- R6: run-level fail-loud in main ----------------------------------------


def test_main_refuses_when_byte_anchor_never_holds(monkeypatch):
    m = {
        "index_regex": "x",
        "wheel_regex": "y",
        "reqs_per_install": 13.1,
        "wheels_per_install": WPI,
        "mean_wheel_bytes": MEAN_MB * 1e6,
        "wheel_frac": 0.5,
        "n_index": 100,
        "n_wheel": 100,
        "dropped": 0,
    }
    # Every wheel-traffic step is off the anchor -> no step ever matched the mix.
    bad = mk(1024, 500, 50, served_mean_mb=MEAN_MB * 0.1)
    monkeypatch.setattr(mn_ramp, "build_mix", lambda tier: m)
    monkeypatch.setattr(mn_ramp, "push_runner", lambda *a, **k: None)
    monkeypatch.setattr(mn_ramp, "find_ceiling", lambda *a, **k: ([bad], "mix"))
    monkeypatch.setattr("sys.argv", ["mn_ramp", "--output", "results/_unused.json"])
    with pytest.raises(SystemExit, match="byte anchor never held"):
        mn_ramp.main()
