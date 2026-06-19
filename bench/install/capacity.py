#!/usr/bin/env python3
"""Breaking-point / capacity metric for the install benchmark.

The install ramp (drive.py) spawns one uv PROCESS per concurrent install, so on a
finite loadgen it saturates before a fast server does — it can't locate a fast
server's breaking point. This module drives the lightweight `oha` HTTP load
tester (its own runner — the frozen meter.run_oha uses an older oha CLI) to ramp
connections against each server's hot serving path until it breaks, and reports:

  MST (Max Sustained Throughput) — the highest rps at which a steady window still
  meets success >= 99.5% AND p99 <= max(floor, mult x unloaded_p99). Plus c_knee,
  p99 at the knee, the breach mode (latency | errors | collapse), bytes/req, and —
  when a static control target is given — loadgen_headroom = R_ceiling / MST and a
  bound_class (server-bound | caution | rig-bound) so a server too fast for this
  rig to break is flagged honestly rather than reported as a flat ceiling.

Runs inside the loadgen container (needs the oha binary; bench.py provisions it).

  capacity.py --index-url http://pypiron:8080/simple/ --host pypiron:8080 \
              --rep-pkg flask --oha /repo/bench/install/.bin/oha \
              --control-url http://control/control-index.json --output results/cap.json
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlsplit

from benchlib import closures_dir, http_get, is_glibc_wheel

LADDER = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]


def pep503(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


_REGEX_SPECIAL = set(".+*?()|[]{}^$\\")


def regex_escape(s: str) -> str:
    """Make a literal string for oha's rand_regex by wrapping each metachar in a
    char class — `[.]`, `[+]`, etc. Backslash-escaping (`\\.`) is unreliable in
    rand_regex (its "dot disabled" mode mangles it); char classes are the
    documented-reliable literal construct."""
    out = []
    for ch in s:
        if ch in _REGEX_SPECIAL:
            out.append(f"[\\{ch}]" if ch in "]^\\" else f"[{ch}]")
        else:
            out.append(ch)
    return "".join(out)


def parse_wheel_url(page_url: str, body: bytes, arch: str) -> Optional[str]:
    """Extract one wheel URL the server serves for a package, from its index page
    (PEP 691 JSON or legacy HTML). Resolves relative hrefs against the page URL."""
    text = body.decode("utf-8", "replace")
    try:
        urls = [f.get("url", "") for f in json.loads(text).get("files", [])]
    except json.JSONDecodeError:
        urls = re.findall(r'href="([^"]+\.whl[^"]*)"', text)
    cleaned = [u.split("#", 1)[0] for u in urls if ".whl" in u]
    for u in cleaned:
        if is_glibc_wheel(u.rsplit("/", 1)[-1], arch):
            return urljoin(page_url, u)
    return urljoin(page_url, cleaned[0]) if cleaned else None


def build_install_mix(index_url: str, arch: str) -> Tuple[str, float, int, int]:
    """Build the realistic install-traffic URL mix: every corpus package's index
    page + one wheel it serves. Returns (rand_regex, reqs_per_install, n_index,
    n_wheel). reqs_per_install ~= 2 x avg packages/closure (each package = one
    index GET + one wheel GET during a cold install)."""
    closures = sorted(closures_dir(arch).glob("*.txt"))
    pkgs_per: List[int] = []
    names: set = set()
    for c in closures:
        n = 0
        for line in c.read_text().splitlines():
            line = line.strip()
            if "==" in line and not line.startswith(("--", "#")):
                names.add(pep503(line.split("==")[0].strip()))
                n += 1
        if n:
            pkgs_per.append(n)
    reqs_per_install = (sum(pkgs_per) / len(pkgs_per)) * 2 if pkgs_per else 1.0

    base = index_url.rstrip("/") + "/"
    index_urls, wheel_urls = [], []
    hdr = {"Accept": "application/vnd.pypi.simple.v1+json"}
    for nm in sorted(names):
        iu = base + nm + "/"
        index_urls.append(iu)
        status, body, _ = http_get(iu, headers=hdr)
        if status == 200:
            w = parse_wheel_url(iu, body, arch)
            if w:
                wheel_urls.append(w)
    mix = index_urls + wheel_urls
    # oha --rand-regex-url mishandles a varying scheme/host/port, so keep the
    # common scheme://host:port a literal prefix and vary only the PATH in the
    # alternation (matches oha's documented working form). Bench hosts are
    # dot-free service names, so the prefix needs no escaping.
    parts = urlsplit(mix[0])
    prefix = f"{parts.scheme}://{parts.netloc}"
    paths = []
    for u in mix:
        p = urlsplit(u)
        paths.append(p.path + (f"?{p.query}" if p.query else ""))
    regex = prefix + "(" + "|".join(regex_escape(pp) for pp in paths) + ")"
    return regex, round(reqs_per_install, 1), len(index_urls), len(wheel_urls)


def oha_run(
    oha: str,
    url: str,
    duration: str,
    connections: int,
    headers: Optional[List[str]] = None,
    expect_status: int = 200,
    regex: bool = False,
) -> Dict:
    """Run oha and return rps + latency percentiles + success%. Uses oha's current
    `--json` CLI (the frozen meter.run_oha passes the old `--output-format json`),
    but the JSON shape is identical. `-r 0`: never follow redirects (a 302 measures
    the 302). regex=True: `url` is a rand_regex the install mix is drawn from."""
    cmd = [oha, "--no-tui", "--json", "-r", "0", "-z", duration, "-c", str(connections)]
    if regex:
        cmd.append("--rand-regex-url")
    for h in headers or []:
        cmd += ["-H", h]
    cmd.append(url)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(f"oha failed: {proc.stderr[:300]}")
    data = json.loads(proc.stdout)
    summary = data.get("summary", {})
    pct = data.get("latencyPercentiles", {})
    statuses = data.get("statusCodeDistribution", {})
    total = sum(statuses.values()) or 1
    ok = statuses.get(str(expect_status), 0)

    def ms(key: str) -> float:
        v = pct.get(key)
        return round(v * 1000, 2) if isinstance(v, (int, float)) else 0.0

    time.sleep(1.0)  # settle between steps (mirrors meter.run_oha)
    return {
        "rps": round(summary.get("requestsPerSec") or 0.0, 1),
        "p50_ms": ms("p50"),
        "p95_ms": ms("p95"),
        "p99_ms": ms("p99"),
        "status_ok_pct": round(100.0 * ok / total, 2),
        "size_per_request": summary.get("sizePerRequest"),
        "total_data_bytes": summary.get("totalData"),
    }


def passes(step: Dict, ceiling_ms: float) -> bool:
    return step["status_ok_pct"] >= 99.5 and step["p99_ms"] <= ceiling_ms


def analyze_ramp(steps: List[Dict], ceiling_ms: float) -> Dict:
    """Pure: reduce a list of ramp steps to the breaking-point summary.

    Each step: {connections, rps, p50_ms, p95_ms, p99_ms, status_ok_pct}.

    MST = peak rps among SLO-passing steps (NOT merely the last — throughput can
    go retrograde while latency still passes). A break is the first of, scanning
    up by connections: an SLO failure (latency/errors) or a throughput collapse
    (rps < 85% of peak beyond the knee).
    """
    peak = max((s["rps"] for s in steps), default=0.0)
    good = [s for s in steps if passes(s, ceiling_ms)]
    best = max(good, key=lambda s: s["rps"]) if good else None
    mst = best["rps"] if best else 0.0
    c_knee = best["connections"] if best else 0
    p99_at_knee = best["p99_ms"] if best else None

    breach: Optional[str] = None
    for s in steps:
        if not passes(s, ceiling_ms):
            breach = "errors" if s["status_ok_pct"] < 99.5 else "latency"
            break
        if peak and s["rps"] < 0.85 * peak and s["connections"] > c_knee:
            breach = "collapse"
            break
    broke = breach is not None
    if not broke and steps and steps[-1]["connections"] == max(s["connections"] for s in steps):
        breach = "none(ladder-cap)"  # never broke within the ladder

    return {
        "mst_rps": round(mst, 1),
        "c_knee": c_knee,
        "p99_at_knee_ms": p99_at_knee,
        "peak_rps": round(peak, 1),
        "breach_mode": breach,
        "ceiling_ms": round(ceiling_ms, 1),
        "broke": broke,
    }


def ramp(
    oha: str,
    url: str,
    headers: List[str],
    expect: int,
    duration: str,
    ceiling_ms: float,
    ladder: List[int],
    regex: bool = False,
) -> List[Dict]:
    """Run the connection ladder, stopping once the server breaks (or collapses)."""
    steps: List[Dict] = []
    peak = 0.0
    collapse_streak = 0
    for c in ladder:
        r = oha_run(oha, url, duration, c, headers=headers, expect_status=expect, regex=regex)
        step = {
            "connections": c,
            "rps": r["rps"],
            "p50_ms": r["p50_ms"],
            "p95_ms": r["p95_ms"],
            "p99_ms": r["p99_ms"],
            "status_ok_pct": r["status_ok_pct"],
            "bytes_per_request": r.get("size_per_request"),
        }
        steps.append(step)
        peak = max(peak, r["rps"])
        if r["rps"] < 0.85 * peak:
            collapse_streak += 1
        else:
            collapse_streak = 0
        # Stop climbing once it has clearly broken: SLO violation, hard errors,
        # or two consecutive retrograde (thrashing) steps.
        if r["status_ok_pct"] < 95.0 or collapse_streak >= 2:
            break
        if not passes(step, ceiling_ms):
            break
    return steps


def measure(
    oha: str,
    url: str,
    headers: List[str],
    expect: int,
    duration: str,
    slo_floor_ms: float,
    slo_mult: float,
    ladder: List[int],
    regex: bool = False,
) -> Dict:
    base = oha_run(oha, url, "5s", 1, headers=headers, expect_status=expect, regex=regex)
    ceiling = max(slo_floor_ms, slo_mult * base["p99_ms"])
    steps = ramp(oha, url, headers, expect, duration, ceiling, ladder, regex=regex)
    out = analyze_ramp(steps, ceiling)
    out.update(
        {
            "url": url,
            "expect_status": expect,
            "unloaded_p50_ms": base["p50_ms"],
            "unloaded_p99_ms": base["p99_ms"],
            "bytes_per_request": steps[-1].get("bytes_per_request") if steps else None,
            "ramp": steps,
        }
    )
    return out


def classify_bound(primary: Dict, r_ceiling: float) -> Tuple[float, str]:
    """server-bound (real break, MST << rig ceiling) | caution | rig-bound."""
    mst = primary["mst_rps"]
    headroom = round(r_ceiling / mst, 2) if mst else 0.0
    if not primary["broke"]:
        return headroom, "rig-bound"
    if headroom >= 2.0:
        return headroom, "server-bound"
    if headroom >= 1.3:
        return headroom, "caution"
    return headroom, "rig-bound"


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--index-url", required=True, help="PEP 503 index root")
    ap.add_argument("--host", required=True)
    ap.add_argument("--arch", default="x86_64")
    ap.add_argument(
        "--rep-pkg", default="flask", help="a package present on every server (small clean index)"
    )
    ap.add_argument(
        "--install-mix",
        action="store_true",
        help="ramp the realistic install URL mix (index+wheel) -> installs/sec",
    )
    ap.add_argument("--oha", default="oha")
    ap.add_argument("--duration", default="10s")
    ap.add_argument("--slo-floor-ms", type=float, default=50.0)
    ap.add_argument("--slo-mult", type=float, default=10.0)
    ap.add_argument("--control-url", default=None, help="static control target for R_ceiling")
    ap.add_argument("--label", default="")
    ap.add_argument("--output", default=None)
    args = ap.parse_args()

    ladder = LADDER
    headers = ["Accept: application/vnd.pypi.simple.v1+json"]
    result = {"label": args.label, "host": args.host}

    if args.install_mix:
        regex, reqs_per_install, n_index, n_wheel = build_install_mix(args.index_url, args.arch)
        print(
            f"capacity {args.label}: install-mix ramp ({n_index} index + {n_wheel} wheel URLs, "
            f"~{reqs_per_install} reqs/install)"
        )
        primary = measure(
            args.oha,
            regex,
            headers,
            200,
            args.duration,
            args.slo_floor_ms,
            args.slo_mult,
            ladder,
            regex=True,
        )
        primary["mix_index_urls"], primary["mix_wheel_urls"] = n_index, n_wheel
        primary["reqs_per_install"] = reqs_per_install
        primary["installs_per_sec"] = (
            round(primary["mst_rps"] / reqs_per_install, 1) if reqs_per_install else 0.0
        )
        bpr = primary.get("bytes_per_request") or 0
        primary["mb_per_sec"] = round(primary["mst_rps"] * bpr / 1e6, 1)
        key = "install_mix"
        result[key] = primary
    else:
        index_url = args.index_url.rstrip("/") + "/" + args.rep_pkg + "/"
        print(f"capacity {args.label}: index ramp {index_url}")
        primary = measure(
            args.oha,
            index_url,
            headers,
            200,
            args.duration,
            args.slo_floor_ms,
            args.slo_mult,
            ladder,
        )
        key = "index"
        result["rep_pkg"] = args.rep_pkg
        result[key] = primary

    if args.control_url:
        print(f"capacity {args.label}: control ramp {args.control_url}")
        ctl = measure(
            args.oha,
            args.control_url,
            [],
            200,
            args.duration,
            args.slo_floor_ms,
            args.slo_mult,
            ladder,
        )
        headroom, bound = classify_bound(primary, ctl["peak_rps"])
        result["control"] = {"r_ceiling_rps": ctl["peak_rps"], "headroom": headroom}
        result["bound_class"] = bound
        primary["headroom"], primary["bound_class"] = headroom, bound

    p = result[key]
    if key == "install_mix":
        print(
            f"  install MST: {p.get('installs_per_sec')} installs/s  ({p.get('mb_per_sec')} MB/s, "
            f"req MST {p['mst_rps']} @ c={p['c_knee']}, breach {p['breach_mode']}, "
            f"bound {result.get('bound_class', 'n/a')})"
        )
    else:
        print(
            f"  MST={p['mst_rps']} rps @ c={p['c_knee']}  p99@knee={p['p99_at_knee_ms']}ms  "
            f"breach={p['breach_mode']}  bound={result.get('bound_class', 'n/a')}"
        )

    blob = json.dumps(result, indent=2)
    if args.output:
        Path(args.output).write_text(blob + "\n")
        print(f"wrote {args.output}")
    else:
        print(blob)


if __name__ == "__main__":
    main()
