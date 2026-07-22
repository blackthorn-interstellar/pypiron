#!/usr/bin/env python3
"""Phase 3 scenarios against a large S3 corpus (run on the loadgen box):

  S1: package rebuild latency vs files-per-package (10/100/1000/5000)
  S2: upload→visible latency while the corpus is large (proves prefix-scoping)
  S4: global index rebuild latency when the package set changes
  S5: read p99 during a full reconcile sweep (sweep must not disturb reads)

Stdlib only. S1 seeds its ladder packages through /legacy/ uploads.
"""

from __future__ import annotations

import argparse
import json
import time

from meter import (
    http_get,
    make_wheel_bytes,
    percentile,
    run_oha,
    upload_wheel,
    wait_healthy,
    wait_visible,
    wheel_filename,
)


def upload(base: str, pkg: str, version: str, user: str, password: str, size: int = 1024) -> str:
    fname = wheel_filename(pkg, version)
    s, b = upload_wheel(
        f"{base}/legacy/", fname, make_wheel_bytes(pkg, version, size), pkg, version, user, password
    )
    if s not in (200, 409):
        raise RuntimeError(f"upload {fname}: {s} {b[:200]!r}")
    return fname


def s1_rebuild_ladder(args) -> dict:
    """Dirty→visible latency as a function of files already in the package."""
    base = args.base_url.rstrip("/")
    out = {}
    for n_files in (10, 100, 1000, 5000):
        pkg = f"bench-s1-{n_files}"
        print(f"S1: building {pkg} ({n_files} files)...", flush=True)
        # Fill the package (idempotent via 409), then measure one more upload.
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=32) as pool:
            list(
                pool.map(
                    lambda i: upload(base, pkg, f"1.{i}.0", args.user, args.password),
                    range(n_files - 1),
                )
            )
        fname = upload(base, pkg, f"2.0.{int(time.time())}", args.user, args.password)
        t = wait_visible(base, pkg, fname, timeout=600, poll=0.05)
        out[str(n_files)] = round(t, 2)
        print(f"S1[{n_files} files]: upload→visible {t:.2f}s", flush=True)
    return out


def s2_visibility_at_scale(args) -> dict:
    """Same as the meter's W3, but with the large corpus as background truth."""
    base = args.base_url.rstrip("/")
    lat = []
    for i in range(args.s2_iterations):
        pkg = "bench-s2"
        fname = upload(base, pkg, f"0.{int(time.time())}.{i}", args.user, args.password)
        lat.append(wait_visible(base, pkg, fname, timeout=300))
    return {
        "iterations": args.s2_iterations,
        "p50_s": round(percentile(lat, 0.5), 3),
        "p99_s": round(percentile(lat, 0.99), 3),
        "max_s": round(max(lat), 3),
    }


def s4_global_rebuild(args) -> dict:
    """A brand-new package name must appear in the global index; time it."""
    base = args.base_url.rstrip("/")
    pkg = f"bench-s4-{int(time.time())}"
    fname = upload(base, pkg, "1.0.0", args.user, args.password)
    t0 = time.perf_counter()
    deadline = time.time() + 600
    while time.time() < deadline:
        status, body, _ = http_get(f"{base}/simple/index.json", timeout=30)
        if status == 200 and pkg.encode() in body:
            return {"new_name_visible_s": round(time.perf_counter() - t0, 2), "package": pkg}
        time.sleep(0.25)
    return {"new_name_visible_s": None, "package": pkg, "error": "timeout", "last_file": fname}


def s5_reads_during_sweep(args) -> dict:
    """Read latency while a full reconcile sweep is running. The restart makes
    the new leader sweep immediately, so oha overlaps the sweep window."""
    base = args.base_url.rstrip("/")
    if not args.restart_cmd:
        return {"skipped": "no --restart-cmd"}
    import shlex
    import subprocess

    subprocess.run(shlex.split(args.restart_cmd) + ["default"], check=True, timeout=180)
    wait_healthy(base)
    # Lease steal takes up to the TTL (30s); the sweep follows. 60s of read
    # load brackets it.
    return run_oha(args.oha, f"{base}/simple/bench-small/index.json", "60s", 64)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://127.0.0.1:8080")
    ap.add_argument("--user", default="admin")
    ap.add_argument("--password", default="secret")
    ap.add_argument("--oha", default="oha")
    ap.add_argument("--restart-cmd", default=None)
    ap.add_argument("--s2-iterations", type=int, default=20)
    ap.add_argument("--skip", default="", help="comma list: s1,s2,s4,s5")
    ap.add_argument("--output", default=None)
    args = ap.parse_args()

    base = args.base_url.rstrip("/")
    wait_healthy(base)
    skip = set(args.skip.split(","))
    out = {}
    if "s1" not in skip:
        out["S1_rebuild_ladder"] = s1_rebuild_ladder(args)
    if "s2" not in skip:
        out["S2_visibility_at_scale"] = s2_visibility_at_scale(args)
    if "s4" not in skip:
        out["S4_global_rebuild"] = s4_global_rebuild(args)
    if "s5" not in skip:
        out["S5_reads_during_sweep"] = s5_reads_during_sweep(args)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(out, f, indent=2)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
