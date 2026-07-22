#!/usr/bin/env python3
"""Phase 2 (brag box) scenarios beyond the meter: concurrent torch-class
uploads with read-interference measurement, and high-concurrency read ceilings.

Run on the loadgen box against a c7gn.2xlarge server. Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import threading
import time

from meter import (
    TORCH_PKG,
    RssSampler,
    http_get,
    make_wheel_bytes,
    run_oha,
    upload_wheel,
    wait_healthy,
    wheel_filename,
)

UPLOAD_PKG = "bench-w2"


def w2_concurrent_uploads(args) -> dict:
    """N torch-class uploads at once; reads must stay alive throughout."""
    base = args.base_url.rstrip("/")
    legacy = f"{base}/legacy/"
    n, mb = args.upload_count, args.upload_mb
    print(f"W2: {n} concurrent {mb} MB uploads + read load")

    blobs = []
    stamp = int(time.time())
    for i in range(n):
        v = f"0.{stamp}.{i}"
        fname = wheel_filename(UPLOAD_PKG, v)
        blobs.append((fname, v, make_wheel_bytes(UPLOAD_PKG, v, mb << 20)))

    results: list = [None] * n

    def one(i: int) -> None:
        fname, v, blob = blobs[i]
        t0 = time.perf_counter()
        try:
            s, b = upload_wheel(
                legacy, fname, blob, UPLOAD_PKG, v, args.user, args.password, timeout=1800
            )
            results[i] = {"status": s, "wall_s": round(time.perf_counter() - t0, 1)}
        except Exception as e:  # noqa: BLE001 — record, don't crash the run
            results[i] = {
                "status": f"EXC({type(e).__name__})",
                "wall_s": round(time.perf_counter() - t0, 1),
            }

    # Read load runs throughout: did uploads degrade reads?
    read_during: dict = {}

    def reads() -> None:
        read_during.update(
            run_oha(args.oha, f"{base}/simple/{TORCH_PKG}/index.json", f"{max(30, n * 20)}s", 32)
        )

    with RssSampler(args.rss_cmd) as rss:
        rt = threading.Thread(target=reads)
        rt.start()
        threads = [threading.Thread(target=one, args=(i,)) for i in range(n)]
        t0 = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        total_wall = round(time.perf_counter() - t0, 1)
        rt.join()

    ok = sum(1 for r in results if r and r["status"] == 200)
    return {
        "uploads": results,
        "succeeded": ok,
        "count": n,
        "size_mb": mb,
        "total_wall_s": total_wall,
        "aggregate_gbps": round(ok * mb * 8 / 1000 / total_wall, 2) if total_wall else 0,
        "peak_rss_mb": rss.peak_mb,
        "reads_during": {
            k: read_during.get(k) for k in ("rps", "p50_ms", "p99_ms", "status_ok_pct")
        },
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://127.0.0.1:8080")
    ap.add_argument("--user", default="admin")
    ap.add_argument("--password", default="secret")
    ap.add_argument("--oha", default="oha")
    ap.add_argument("--rss-cmd", default=None)
    ap.add_argument("--upload-count", type=int, default=8)
    ap.add_argument("--upload-mb", type=int, default=900)
    ap.add_argument("--big-conns", type=int, default=1024)
    ap.add_argument("--duration", default="30s")
    ap.add_argument("--output", default=None)
    args = ap.parse_args()

    base = args.base_url.rstrip("/")
    wait_healthy(base)
    out: dict = {}

    status, body, _ = http_get(f"{base}/simple/{TORCH_PKG}/index.json")
    torch_file = json.loads(body)["files"][0]["filename"]

    print(f"R1 @ {args.big_conns} conns")
    out["R1_json_1k"] = run_oha(
        args.oha, f"{base}/simple/bench-small/index.json", args.duration, args.big_conns
    )
    print(f"R3 @ {args.big_conns} conns")
    _, _, hdrs = http_get(f"{base}/simple/bench-small/index.json")
    etag = next((v for k, v in hdrs.items() if k.lower() == "etag"), "")
    out["R3_304_1k"] = run_oha(
        args.oha,
        f"{base}/simple/bench-small/index.json",
        args.duration,
        args.big_conns,
        headers=[f"If-None-Match: {etag}"],
        expect_status=304,
    )
    print(f"R2 torch idx @ {args.big_conns} conns")
    out["R2_torch_1k"] = run_oha(
        args.oha, f"{base}/simple/{TORCH_PKG}/index.json", args.duration, args.big_conns
    )
    print(f"R6 302 @ {args.big_conns} conns")
    out["R6_302_1k"] = run_oha(
        args.oha,
        f"{base}/files/{TORCH_PKG}/{torch_file}",
        args.duration,
        args.big_conns,
        headers=["User-Agent: uv/0.7.0"],
        expect_status=302,
    )
    print(f"R7 metadata @ {args.big_conns} conns")
    out["R7_meta_1k"] = run_oha(
        args.oha,
        f"{base}/files/{TORCH_PKG}/{torch_file}.metadata",
        args.duration,
        args.big_conns,
        headers=["User-Agent: uv/0.7.0"],
        expect_status=200,
    )

    out["W2_concurrent_uploads"] = w2_concurrent_uploads(args)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(out, f, indent=2)
    print("\n--- tier2 results ---")
    for k, v in out.items():
        if "rps" in v:
            print(
                f"{k}: {v['rps']} rps p50={v['p50_ms']}ms p99={v['p99_ms']}ms ok%={v['status_ok_pct']}"
            )
        else:
            print(f"{k}: {json.dumps({kk: vv for kk, vv in v.items() if kk != 'uploads'})}")


if __name__ == "__main__":
    main()
