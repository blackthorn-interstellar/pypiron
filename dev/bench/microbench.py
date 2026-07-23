#!/usr/bin/env python3
"""Per-endpoint micro-benchmarks at PyPI shape. Spec: dev/bench/MICROBENCH.md.

Subcommands:
  cache  - ensure the post-sweep tier cache exists:  microbench.py cache --packages 50000
  run    - tracked lane against a cached tier:       microbench.py run --packages 50000 --bin PATH
  pins   - measure op counts on a small fresh tree (refresh the table's pins):
           microbench.py pins --bin PATH

The cache is fabricated once (0-byte artifacts + real sidecars), swept once by
the server's boot audit (indexes rendered), then reused forever: a `run` boots
against it in ~a second, restarts per endpoint for honest cold hits, and never
re-seeds or re-sweeps. Write endpoints run against the same tree behind a
snapshot/restore of everything they touch; the cache stays byte-pristine.

Stdlib only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import endpoints as epmod
import fabricate
from endpoints import ENDPOINTS, Ctx, assign_targets, hit, measure_ops, start_server, stop_server

REPO = Path(__file__).resolve().parent.parent.parent
CACHE_ROOT = REPO / ".local" / "microbench"
RESULTS_DIR = Path(__file__).resolve().parent / "results"

WARM_N = 100
WRITE_N = 10
CI_PACKAGES = 300


# --------------------------------- tier cache --------------------------------


def cache_dir(packages: int, seed: int) -> Path:
    return CACHE_ROOT / f"tier-{packages}-seed{seed}-fmt{fabricate.TREE_FORMAT}"


def ensure_cache(bin_path: str, packages: int, seed: int = 42) -> Path:
    """Fabricate + boot-sweep + mark READY, once; later calls return instantly."""
    tier = cache_dir(packages, seed)
    data = tier / "data"
    ready = tier / "READY"
    dirty = tier / "DIRTY"
    if ready.exists() and not dirty.exists():
        return tier
    if tier.exists():
        print(f"cache {tier.name}: incomplete or dirty, rebuilding")
        shutil.rmtree(tier)
    tier.mkdir(parents=True)
    t0 = time.monotonic()
    print(f"cache {tier.name}: fabricating {packages:,} packages")
    manifest = fabricate.fabricate_tree(
        data,
        packages,
        seed=seed,
        include_largest=True,
        progress=lambda p, f: print(f"  {p:,} pkgs / {f:,} files"),
    )
    print(f"  fabricated {manifest['files']:,} files in {time.monotonic() - t0:.0f}s; sweeping")
    server = start_server(bin_path, data, tier / "sweep.log", sweep=True)
    try:
        swept = epmod.wait_swept(server["base"], manifest["packages"], timeout=7200)
        print(f"  swept in {swept:.0f}s")
    finally:
        stop_server(server)
    ready.write_text(json.dumps({"swept_secs": round(swept, 1)}))
    return tier


# --------------------------- write-probe protection ---------------------------
#
# The mutation phase only ever touches (a) per-package dirs it creates for the
# probe packages — under packages/ AND under the rendered simple/ tree — and
# (b) shared files outside any per-package dir (global indexes, _sync/,
# _advisories/, _transparency/, intent markers). So: snapshot the shared files
# byte-for-byte, restore them + delete the probe dirs after, verify by re-hash.
# Never treat per-package dirs of OTHER packages as snapshot material: at the
# 780k tier that is ~1.6M rendered index files, which overflows any tmp dir
# (learned the ENOSPC way). The CI lane proves the narrow set is complete with
# a full-tree walk on its small tier.

# Trees with one subdirectory per package; only their top-level files are aux.
_PER_PACKAGE_TREES = ("packages", "simple")


def _aux_files(data_dir: Path) -> list:
    """Every shared file: everything except contents of per-package dirs."""
    data_dir = Path(data_dir)
    out = []
    for entry in data_dir.iterdir():
        if entry.name in _PER_PACKAGE_TREES:
            out.extend(p.relative_to(data_dir) for p in entry.iterdir() if p.is_file())
        elif entry.is_file():
            out.append(entry.relative_to(data_dir))
        else:
            out.extend(p.relative_to(data_dir) for p in entry.rglob("*") if p.is_file())
    return out


def _per_package_dirs(data_dir: Path) -> dict:
    return {
        tree: {p.name for p in (Path(data_dir) / tree).iterdir() if p.is_dir()}
        for tree in _PER_PACKAGE_TREES
        if (Path(data_dir) / tree).is_dir()
    }


def tree_digest(data_dir: Path, rels: list) -> str:
    h = hashlib.sha256()
    for rel in sorted(rels, key=str):
        h.update(str(rel).encode())
        h.update((Path(data_dir) / rel).read_bytes())
    return h.hexdigest()


def snapshot_aux(data_dir: Path, dest: Path) -> dict:
    rels = _aux_files(data_dir)
    for rel in rels:
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(Path(data_dir) / rel, target)
    return {
        "rels": rels,
        "digest": tree_digest(data_dir, rels),
        "dirs": _per_package_dirs(data_dir),
    }


def restore_aux(data_dir: Path, dest: Path, snap: dict, probe_prefix: str) -> None:
    data_dir = Path(data_dir)
    # Probe dirs vanish wholesale from every per-package tree; nothing else
    # under those trees was touched.
    for tree in _PER_PACKAGE_TREES:
        for p in (data_dir / tree).glob(f"{probe_prefix}-*"):
            shutil.rmtree(p)
    # Delete aux files that did not exist before, restore the rest byte-for-byte.
    before = set(map(str, snap["rels"]))
    for rel in _aux_files(data_dir):
        if str(rel) not in before:
            (data_dir / rel).unlink()
    for rel in snap["rels"]:
        shutil.copy2(dest / rel, data_dir / rel)


def verify_pristine(data_dir: Path, snap: dict, probe_prefix: str) -> None:
    data_dir = Path(data_dir)
    for tree in _PER_PACKAGE_TREES:
        leftovers = list((data_dir / tree).glob(f"{probe_prefix}-*"))
        if leftovers:
            raise AssertionError(f"probe leftovers: {leftovers}")
    dirs = _per_package_dirs(data_dir)
    if dirs != snap["dirs"]:
        diff = {
            tree: dirs.get(tree, set()) ^ snap["dirs"].get(tree, set())
            for tree in set(dirs) | set(snap["dirs"])
        }
        raise AssertionError(f"per-package dir sets changed: {diff}")
    rels = _aux_files(data_dir)
    if {str(r) for r in rels} != {str(r) for r in snap["rels"]}:
        raise AssertionError(
            f"aux file set changed: {set(map(str, rels)) ^ set(map(str, snap['rels']))}"
        )
    digest = tree_digest(data_dir, rels)
    if digest != snap["digest"]:
        raise AssertionError("aux files differ after restore")


# --------------------------------- measuring ---------------------------------


def rss_mb(pid: int) -> float:
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)])
    return int(out.split()[0]) / 1024


def percentile(sorted_ms: list, q: float) -> float:
    return sorted_ms[min(len(sorted_ms) - 1, int(q * len(sorted_ms)))]


def warm_stats(samples: list) -> dict:
    s = sorted(samples)
    return {
        "n": len(s),
        "p50_ms": round(percentile(s, 0.50), 3),
        "p95_ms": round(percentile(s, 0.95), 3),
        "max_ms": round(s[-1], 3),
    }


def run_read_endpoint(bin_path: str, data: Path, log: Path, ep, ctx: Ctx) -> dict:
    """Fresh quiesced process: one honest cold hit, then a warm loop."""
    server = start_server(bin_path, data, log, sweep=False)
    try:
        base = server["base"]
        # Cold ops and cold latency come from the same first-ever request.
        before = epmod.ops_snapshot(base)
        _, cold_ms, body = hit(base, ep, ctx, 0)
        cold_ops = epmod.ops_delta(before, epmod.ops_snapshot(base))
        warm_ops = measure_ops(base, ep, ctx, 0)
        samples = [hit(base, ep, ctx, 0)[1] for _ in range(WARM_N)]
        return {
            "cold_ms": round(cold_ms, 3),
            "cold_ops": cold_ops,
            "warm_ops": warm_ops,
            "warm": warm_stats(samples),
            "bytes": len(body),
            "startup_secs": round(server["startup_secs"], 3),
            "rss_mb": round(rss_mb(server["proc"].pid), 1),
        }
    finally:
        stop_server(server)


def run_write_phase(bin_path: str, data: Path, log: Path, snap_dir: Path) -> dict:
    """All mutating endpoints against one process, snapshot/restore around it."""
    snap = snapshot_aux(data, snap_dir)
    results: dict = {}
    server = start_server(bin_path, data, log, sweep=False)
    try:
        base = server["base"]
        ctx = assign_targets(data)
        writers = [ep for ep in ENDPOINTS if ep.mutates]
        for ep in writers:
            cold_ops = measure_ops(base, ep, ctx, 0)
            warm_ops = None
            for i in range(1, WRITE_N + 1):
                warm_ops = measure_ops(base, ep, ctx, i)
            # measure_ops already performed those requests; time fresh ones.
            samples = [hit(base, ep, ctx, i)[1] for i in range(WRITE_N + 1, WRITE_N + 6)]
            results[ep.name] = {
                "cold_ops": cold_ops,
                "warm_ops": warm_ops,
                "warm": warm_stats(samples),
            }
        results["upload-visible"] = {"secs": upload_visible(base, ctx)}
    finally:
        stop_server(server)
    restore_aux(data, snap_dir, snap, "mb-probe")
    verify_pristine(data, snap, "mb-probe")
    return results


def upload_visible(base: str, ctx: Ctx) -> float:
    """Upload one more probe wheel, poll its index until the file appears."""
    up = next(ep for ep in ENDPOINTS if ep.name == "upload-legacy")
    i = 9999
    t0 = time.monotonic()
    hit(base, up, ctx, i)
    filename = ctx.probe_filenames[i]
    deadline = t0 + 120
    while time.monotonic() < deadline:
        status, _, body = epmod.fetch(base, "GET", f"/simple/{ctx.probe_pkg(i)}/index.json")
        if status == 200 and filename in body.decode():
            return round(time.monotonic() - t0, 3)
        time.sleep(0.02)
    raise TimeoutError("upload never became visible")


def git_rev() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(REPO), "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except subprocess.CalledProcessError:
        return "unknown"


def cmd_run(args) -> None:
    tier = ensure_cache(args.bin, args.packages, args.seed)
    data = tier / "data"
    manifest = json.loads((data / "scale-manifest.json").read_text())
    with tempfile.TemporaryDirectory(prefix="microbench-") as tmp:
        tmp = Path(tmp)
        result = {
            "schema": 1,
            "tier": {k: manifest[k] for k in ("packages", "files", "seed", "tree_format")},
            "git_rev": git_rev(),
            "host": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "cpus": os.cpu_count(),
            },
            "endpoints": {},
        }
        only = set(args.only.split(",")) if args.only else None
        ctx = assign_targets(data)
        readers = [ep for ep in ENDPOINTS if not ep.mutates and (only is None or ep.name in only)]
        for ep in readers:
            r = run_read_endpoint(args.bin, data, tmp / f"{ep.name}.log", ep, ctx)
            result["endpoints"][ep.name] = r
            print(
                f"{ep.name}: cold={r['cold_ms']}ms warm p50={r['warm']['p50_ms']}ms "
                f"p95={r['warm']['p95_ms']}ms ops(cold)={r['cold_ops']} ops(warm)={r['warm_ops']}"
            )

        if only is None or "monster" in only:
            # Worst-case first hit: the corpus's biggest package, fresh process.
            monster = manifest["largest"]["name"]
            mctx = Ctx(targets={"simple-pkg-html": monster}, versions={monster: "0.0.0"})
            mep = next(ep for ep in ENDPOINTS if ep.name == "simple-pkg-html")
            server = start_server(args.bin, data, tmp / "monster.log", sweep=False)
            try:
                _, cold_ms, body = hit(server["base"], mep, mctx, 0)
                result["monster"] = {
                    "package": monster,
                    "files": manifest["largest"]["files"],
                    "cold_ms": round(cold_ms, 3),
                    "bytes": len(body),
                    "rss_mb": round(rss_mb(server["proc"].pid), 1),
                }
                print(f"monster {monster} ({manifest['largest']['files']} files): {cold_ms:.1f}ms")
            finally:
                stop_server(server)

        if only is None or any(ep.mutates and ep.name in only for ep in ENDPOINTS):
            print("write phase (snapshot -> mutate -> restore -> verify)")
            writes = run_write_phase(args.bin, data, tmp / "writes.log", tmp / "aux-snapshot")
            result["endpoints"].update({k: v for k, v in writes.items() if k != "upload-visible"})
            result["upload_visible_secs"] = writes["upload-visible"]["secs"]
        starts = [r["startup_secs"] for r in result["endpoints"].values() if "startup_secs" in r]
        if starts:
            result["startup_secs_median"] = sorted(starts)[len(starts) // 2]

    if args.dry or only is not None:
        # Partial or exploratory runs never overwrite the committed series.
        print(json.dumps(result, indent=2, sort_keys=True))
        return
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"microbench-{args.packages}.json"
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(f"wrote {out}")


def cmd_cache(args) -> None:
    tier = ensure_cache(args.bin, args.packages, args.seed)
    print(f"ready: {tier}")


def cmd_pins(args) -> None:
    """Fresh small tree, sweep, walk the table, print measured pins."""
    with tempfile.TemporaryDirectory(prefix="microbench-pins-") as tmp:
        tmp = Path(tmp)
        data = tmp / "data"
        manifest = fabricate.fabricate_tree(data, args.packages, seed=args.seed)
        server = start_server(args.bin, data, tmp / "server.log", sweep=True)
        try:
            base = server["base"]
            swept = epmod.wait_swept(base, manifest["packages"], timeout=600)
            print(f"swept {manifest['packages']} packages in {swept:.1f}s")
            ctx = assign_targets(data)
            for ep in ENDPOINTS:
                cold = measure_ops(base, ep, ctx, 0)
                warms = [measure_ops(base, ep, ctx, i) for i in (1, 2, 3)]
                stable = all(w == warms[0] for w in warms)
                _, _, body = hit(base, ep, ctx, 4)
                print(
                    f"{ep.name:24s} cold={cold} warm={warms[0]}"
                    f"{'' if stable else ' UNSTABLE ' + str(warms)} bytes={len(body)}"
                )
        finally:
            stop_server(server)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in [("cache", cmd_cache), ("run", cmd_run), ("pins", cmd_pins)]:
        p = sub.add_parser(name)
        p.add_argument("--bin", default=str(REPO / "target" / "release" / "pypiron"))
        p.add_argument("--packages", type=int, default=50000 if name != "pins" else CI_PACKAGES)
        p.add_argument("--seed", type=int, default=42)
        if name == "run":
            p.add_argument("--only", help="comma-separated endpoint names (or 'monster')")
            p.add_argument("--dry", action="store_true", help="print JSON, never write results/")
        p.set_defaults(fn=fn)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
