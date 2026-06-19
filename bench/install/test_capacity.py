"""Unit tests for capacity.analyze_ramp (the one pure function — ramp -> verdict).

Run: uv run -- pytest bench/install/test_capacity.py
(Not part of `make check`'s blackbox suite, which targets tests/.)
"""

from __future__ import annotations

import json

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


def test_pick_canonical_prefers_cp311_manylinux():
    fns = [
        "numpy-2.3.5-cp312-cp312-manylinux_2_28_x86_64.whl",
        "numpy-2.3.5-cp311-cp311-musllinux_1_2_x86_64.whl",  # not glibc -> excluded
        "numpy-2.3.5-cp311-cp311-manylinux_2_28_x86_64.whl",
    ]
    assert (
        capacity.pick_canonical(fns, "x86_64")
        == "numpy-2.3.5-cp311-cp311-manylinux_2_28_x86_64.whl"
    )


def test_pick_canonical_pure_wheel():
    assert (
        capacity.pick_canonical(["flask-3.0.0-py3-none-any.whl"], "x86_64")
        == "flask-3.0.0-py3-none-any.whl"
    )


def test_find_wheel_href_exact_match_resolves_relative():
    page = "http://nginx:8080/root/pypi/+simple/numpy/"
    body = json.dumps(
        {
            "files": [
                {"url": "../../+f/aa/numpy-2.3.5-cp311-cp311-manylinux_2_28_x86_64.whl"},
                {"url": "../../+f/bb/numpy-9.9.9-cp311-cp311-manylinux_2_28_x86_64.whl"},
            ]
        }
    ).encode()
    got = capacity.find_wheel_href(page, body, "numpy-2.3.5-cp311-cp311-manylinux_2_28_x86_64.whl")
    assert (
        got == "http://nginx:8080/root/pypi/+f/aa/numpy-2.3.5-cp311-cp311-manylinux_2_28_x86_64.whl"
    )


def test_find_wheel_href_html_strips_fragment_and_misses_absent():
    page = "http://web:8080/simple/flask/"
    body = b'<a href="../../packages/fl/flask/flask-3.0.0-py3-none-any.whl#sha256=abc">x</a>'
    assert (
        capacity.find_wheel_href(page, body, "flask-3.0.0-py3-none-any.whl")
        == "http://web:8080/packages/fl/flask/flask-3.0.0-py3-none-any.whl"
    )
    assert capacity.find_wheel_href(page, body, "flask-9.9.9-py3-none-any.whl") is None


def test_regex_escape_char_classes_specials():
    esc = capacity.regex_escape("numpy-2.3.5+cu12.whl")
    # dots/plus wrapped in char classes (rand_regex literal); dash left bare
    assert esc == "numpy-2[.]3[.]5[+]cu12[.]whl"
