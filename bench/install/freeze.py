#!/usr/bin/env python3
"""Freeze the corpus: resolve each project's dependency closure once with uv.

Each project in lock/projects.toml is resolved INDEPENDENTLY, constrained to the
glibc/x86_64 Linux runtime and the target Python versions, wheels-only. The
result is committed and reproducible:

  lock/closures/<name>.txt   - the project's fully-pinned, hashed closure
                               (the Workload B replay input)
  lock/wheelhouse.<tier>.json - the union of all wheels across all closures
                               (url, size, sha256), the wheelhouse download list

Re-freeze only on an explicit, committed corpus bump. Run:

  freeze.py --tier lite                 # all lite projects
  freeze.py --tier lite --only requests,flask,numpy   # a subset (smoke)
"""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple

import tomllib
from benchlib import (
    ARCHES,
    LOCK,
    closures_dir,
    is_glibc_wheel,
    load_projects,
    manifest_path,
    select_projects,
)

PYPROJECT = """\
[project]
name = "corpus-{slug}"
version = "0.0.0"
requires-python = "{requires_python}"
dependencies = [{dep!r}]

[tool.uv]
environments = [{envs}]
"""


def requires_python(pythons: List[str]) -> str:
    lo = min(pythons, key=lambda v: tuple(map(int, v.split("."))))
    hi = max(pythons, key=lambda v: tuple(map(int, v.split("."))))
    hi_excl = f"{hi.split('.')[0]}.{int(hi.split('.')[1]) + 1}"
    return f">={lo},<{hi_excl}"


def resolve_one(proj: Dict, meta: Dict, arch: str) -> Tuple[Dict, List[Dict], str]:
    """Resolve one project. Returns (project, wheels[], error). wheels=[] on error."""
    name, dep = proj["name"], proj["requirement"]
    env = f"sys_platform == 'linux' and platform_machine == '{arch}'"
    closure = closures_dir(arch) / f"{name}.txt"

    def fail(msg: str) -> Tuple[Dict, List[Dict], str]:
        # A dropped project must leave no closure behind, or drive.py's glob
        # would try (and fail) to install it.
        closure.unlink(missing_ok=True)
        return proj, [], msg

    with tempfile.TemporaryDirectory(prefix=f"freeze-{name}-") as td:
        tdp = Path(td)
        (tdp / "pyproject.toml").write_text(
            PYPROJECT.format(
                slug=name.replace("_", "-"),
                requires_python=requires_python(meta["target_pythons"]),
                dep=dep,
                envs=repr(env),
            )
        )

        def uv(*args: str, **kw) -> subprocess.CompletedProcess:
            return subprocess.run(
                ["uv", *args], cwd=td, capture_output=True, text=True, timeout=600, **kw
            )

        r = uv("lock", "--quiet")
        if r.returncode != 0:
            return fail(f"lock failed: {r.stderr.strip()[:300]}")

        r = uv(
            "export",
            "--format",
            "requirements.txt",
            "--no-emit-project",
            "--no-header",
            "--no-annotate",
            "-o",
            str(closure),
        )
        if r.returncode != 0:
            return fail(f"export requirements failed: {r.stderr.strip()[:300]}")

        r = uv("export", "--format", "pylock.toml", "-o", "pylock.toml")
        if r.returncode != 0:
            return fail(f"export pylock failed: {r.stderr.strip()[:300]}")
        with open(tdp / "pylock.toml", "rb") as f:
            pylock = tomllib.load(f)

    wheels: List[Dict] = []
    sdist_only: List[str] = []
    for pkg in pylock.get("packages", []):
        whls = pkg.get("wheels", []) or []
        kept = [
            {
                "name": pkg["name"],
                "version": pkg.get("version", ""),
                "filename": w["url"].rsplit("/", 1)[-1],
                "url": w["url"],
                "size": w.get("size"),
                "sha256": (w.get("hashes") or {}).get("sha256", ""),
            }
            for w in whls
            if is_glibc_wheel(w["url"].rsplit("/", 1)[-1], arch)
        ]
        if not kept:
            # No glibc/x86_64 wheel -> the --only-binary client cannot install it.
            sdist_only.append(pkg["name"])
        wheels.extend(kept)
    if sdist_only:
        return fail(f"no {arch} wheel for: {', '.join(sorted(set(sdist_only)))}")
    return proj, wheels, ""


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--tier", default="lite", choices=["lite", "heavy", "all"])
    ap.add_argument("--arch", default="x86_64", choices=list(ARCHES))
    ap.add_argument("--only", default=None, help="comma-separated project names (smoke subset)")
    ap.add_argument("--jobs", type=int, default=12)
    args = ap.parse_args()

    doc = load_projects()
    meta = doc["meta"]
    only = [s.strip() for s in args.only.split(",")] if args.only else None
    projects = select_projects(doc, args.tier, only)
    if not projects:
        raise SystemExit("no projects selected")
    closures_dir(args.arch).mkdir(parents=True, exist_ok=True)

    print(
        f"freezing {len(projects)} projects (tier={args.tier}, arch={args.arch}, pythons={meta['target_pythons']})"
    )
    union: Dict[str, Dict] = {}  # url -> wheel
    ok: List[str] = []
    failed: List[Tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futs = {pool.submit(resolve_one, p, meta, args.arch): p for p in projects}
        for fut in as_completed(futs):
            proj, wheels, err = fut.result()
            if err:
                failed.append((proj["name"], err))
                print(f"  ✗ {proj['name']}: {err}")
                continue
            for w in wheels:
                union[w["url"]] = w
            ok.append(proj["name"])
            print(f"  ✓ {proj['name']}: {len(wheels)} wheels")

    wheels = sorted(union.values(), key=lambda w: (w["name"], w["version"], w["filename"]))
    total_bytes = sum(w["size"] or 0 for w in wheels)
    manifest = {
        "tier": args.tier,
        "arch": args.arch,
        "target_pythons": meta["target_pythons"],
        "projects_ok": sorted(ok),
        "projects_failed": {n: e for n, e in failed},
        "wheel_count": len(wheels),
        "total_bytes": total_bytes,
        "wheels": wheels,
    }
    out = manifest_path(args.tier, args.arch)
    out.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n")

    print()
    print(f"resolved {len(ok)}/{len(projects)} projects; {len(failed)} dropped")
    print(f"union: {len(wheels)} wheels, {total_bytes / 1e6:.1f} MB")
    print(f"wrote {out.relative_to(LOCK.parent)} and {len(ok)} closures/")
    if failed:
        print("\ndropped (no x86_64 wheel / resolution error):")
        for n, e in sorted(failed):
            print(f"  {n}: {e}")


if __name__ == "__main__":
    main()
