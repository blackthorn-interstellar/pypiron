#!/usr/bin/env python3
"""Drive uv against a server-under-test and measure serving performance.

Runs inside the loadgen (native uv). The corpus is frozen and hash-pinned, so
every server is asked for byte-identical wheels; the only variable is how fast it
answers (docs/BENCHMARK_INSTALL.md §5).

Workload B (the CI-fleet headline): each "runner" is a fresh process with a fresh
uv cache and a fresh install target, installing one sampled project's frozen
closure. Concurrency is swept; sampling is uniform (long-tail stress) or zipf
(popularity-weighted headline). Resolve-only (`--dry-run`) isolates metadata
serving from byte transfer.

  drive.py --index-url http://pypiron:8080/simple/ --host pypiron:8080 \
           --tier lite --arch aarch64 --concurrency 1,8,32 --samples 24
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List

from benchlib import closures_dir, percentile


def uv_version() -> str:
    return subprocess.run(["uv", "--version"], capture_output=True, text=True).stdout.strip()


def wait_index(index_url: str, timeout: float = 120.0) -> None:
    import urllib.error
    import urllib.request

    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(index_url, timeout=5):
                return  # any 2xx/3xx means the server is up and serving
        except urllib.error.HTTPError:
            return  # a 4xx (e.g. proxpi 406 on the bare index root) is still "up"
        except (urllib.error.URLError, OSError) as e:
            last = repr(e)
        time.sleep(1.0)
    raise SystemExit(f"index {index_url} not ready after {timeout}s: {last}")


def list_closures(tier_arch_dir: Path) -> List[str]:
    return sorted(p.stem for p in tier_arch_dir.glob("*.txt"))


def weighted_order(names: List[str], mode: str) -> List[float]:
    """Sampling weights. uniform: equal. zipf: 1/rank over the committed order
    (projects.toml is roughly popularity-ordered; real download counts can be
    wired in later via top-pypi-packages)."""
    if mode == "uniform":
        return [1.0] * len(names)
    return [1.0 / (i + 1) for i in range(len(names))]


def install_cmd(
    closure: Path, index_url: str, host: str, python: str, cache: Path, target: Path, dry: bool
) -> List[str]:
    cmd = [
        "uv",
        "pip",
        "install",
        "--no-deps",
        "--require-hashes",
        "--only-binary",
        ":all:",
        "--default-index",
        index_url,
        "--allow-insecure-host",
        host,
        "--python",
        python,
        "--cache-dir",
        str(cache),
        "--quiet",
        "-r",
        str(closure),
    ]
    if dry:
        # Resolve-only: no install, no venv needed; --system resolves against the
        # loadgen interpreter (uv still fetches index metadata from the server).
        cmd += ["--dry-run", "--system"]
    else:
        cmd += ["--target", str(target)]
    return cmd


def run_one(closure: Path, index_url: str, host: str, python: str, dry: bool) -> Dict:
    cache = Path(tempfile.mkdtemp(prefix="uvc-"))
    target = Path(tempfile.mkdtemp(prefix="uvt-"))
    cmd = install_cmd(closure, index_url, host, python, cache, target, dry)
    t0 = time.perf_counter()
    try:
        # cwd under /tmp so uv never discovers a stray ancestor .venv (e.g. the
        # mounted host repo's macOS .venv) when resolving --dry-run.
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600, cwd=cache)
        wall = time.perf_counter() - t0
        ok = proc.returncode == 0
        err = "" if ok else (proc.stderr.strip()[-300:] or f"exit {proc.returncode}")
    except subprocess.TimeoutExpired:
        wall = time.perf_counter() - t0
        ok, err = False, "timeout"
    finally:
        shutil.rmtree(cache, ignore_errors=True)
        shutil.rmtree(target, ignore_errors=True)
    return {"project": closure.stem, "wall_ms": round(wall * 1000, 1), "ok": ok, "err": err}


def sweep(
    closures: Dict[str, Path],
    names: List[str],
    weights: List[float],
    rng: random.Random,
    index_url: str,
    host: str,
    python: str,
    c: int,
    samples: int,
    dry: bool,
) -> Dict:
    picks = rng.choices(names, weights=weights, k=samples)
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=c) as pool:
        results = list(
            pool.map(lambda n: run_one(closures[n], index_url, host, python, dry), picks)
        )
    wall_total = time.perf_counter() - t0
    walls = [r["wall_ms"] for r in results if r["ok"]]
    errors = [r for r in results if not r["ok"]]
    out = {
        "concurrency": c,
        "samples": samples,
        "ok": len(walls),
        "errors": len(errors),
        "wall_total_s": round(wall_total, 2),
        "installs_per_min": round(len(walls) / wall_total * 60, 1) if wall_total else 0.0,
    }
    if walls:
        out.update(
            {
                "p50_ms": round(percentile(walls, 0.50), 1),
                "p95_ms": round(percentile(walls, 0.95), 1),
                "p99_ms": round(percentile(walls, 0.99), 1),
                "mean_ms": round(sum(walls) / len(walls), 1),
            }
        )
    if errors:
        out["error_sample"] = errors[0]["err"]
    return out


def warm(
    closures: Dict[str, Path],
    names: List[str],
    index_url: str,
    host: str,
    python: str,
    concurrency: int,
) -> Dict:
    """Install every closure once (each project at least once) to fill a proxy's
    cache (egress on) or prove it is fully cached (egress off, sanity)."""
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        results = list(
            pool.map(lambda n: run_one(closures[n], index_url, host, python, dry=False), names)
        )
    errs = [r for r in results if not r["ok"]]
    out = {
        "projects": len(names),
        "ok": len(names) - len(errs),
        "errors": len(errs),
        "wall_s": round(time.perf_counter() - t0, 1),
    }
    if errs:
        out["error_sample"] = errs[0]
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--index-url", required=True, help="PEP 503 index root, e.g. http://pypiron:8080/simple/"
    )
    ap.add_argument("--host", required=True, help="host:port for --allow-insecure-host")
    ap.add_argument("--mode", default="measure", choices=["measure", "warm"])
    ap.add_argument("--tier", default="lite")
    ap.add_argument("--arch", default="x86_64")
    ap.add_argument("--python", default="3.11")
    ap.add_argument("--concurrency", default="1,8,32", help="comma-separated sweep")
    ap.add_argument("--warm-concurrency", type=int, default=4)
    ap.add_argument("--samples", type=int, default=24, help="installs per concurrency level")
    ap.add_argument("--sampling", default="uniform", choices=["uniform", "zipf"])
    ap.add_argument("--seed", type=int, default=1729)
    ap.add_argument("--label", default="", help="server label for the result")
    ap.add_argument("--output", default=None)
    args = ap.parse_args()

    # Closures live in lock/<arch>/closures (all frozen projects for that arch).
    cdir = closures_dir(args.arch)
    names = list_closures(cdir)
    if not names:
        raise SystemExit(f"no closures in {cdir}; run freeze.py first")
    closures = {n: cdir / f"{n}.txt" for n in names}
    wait_index(args.index_url)

    if args.mode == "warm":
        w = warm(closures, names, args.index_url, args.host, args.python, args.warm_concurrency)
        print(f"warm {args.label}: {w['ok']}/{w['projects']} ok, {w['errors']} err, {w['wall_s']}s")
        if w["errors"]:
            print(f"  first error: {w['error_sample']['project']}: {w['error_sample']['err']}")
            raise SystemExit(1)
        return

    weights = weighted_order(names, args.sampling)
    levels = [int(x) for x in args.concurrency.split(",")]

    print(
        f"driving {args.label or args.index_url}: {len(names)} projects, "
        f"sampling={args.sampling}, sweep={levels}, samples={args.samples}"
    )
    sweeps = {}
    for c in levels:
        s = sweep(
            closures,
            names,
            weights,
            random.Random(args.seed),
            args.index_url,
            args.host,
            args.python,
            c,
            args.samples,
            dry=False,
        )
        sweeps[str(c)] = s
        extra = f"p50 {s.get('p50_ms', '-')}ms p99 {s.get('p99_ms', '-')}ms" if s["ok"] else ""
        print(
            f"  C={c:<4} ok={s['ok']}/{s['samples']} err={s['errors']} "
            f"{s['installs_per_min']}/min {extra}"
        )

    print("  resolve-only (C=1, --dry-run)")
    resolve = sweep(
        closures,
        names,
        weights,
        random.Random(args.seed),
        args.index_url,
        args.host,
        args.python,
        1,
        min(args.samples, 12),
        dry=True,
    )

    out = {
        "label": args.label,
        "index_url": args.index_url,
        "tier": args.tier,
        "arch": args.arch,
        "python": args.python,
        "uv_version": uv_version(),
        "sampling": args.sampling,
        "seed": args.seed,
        "projects": len(names),
        "sweeps": sweeps,
        "resolve_only": resolve,
    }
    blob = json.dumps(out, indent=2)
    if args.output:
        Path(args.output).write_text(blob + "\n")
        print(f"wrote {args.output}")
    else:
        print(blob)


if __name__ == "__main__":
    main()
