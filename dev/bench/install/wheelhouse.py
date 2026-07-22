#!/usr/bin/env python3
"""Download the frozen wheel union into a local wheelhouse, sha256-verified.

The wheelhouse is the single shared byte source every private-host server is
seeded from (proxies/mirrors instead warm-by-install pinned to the same lock, so
their caches converge on these exact files). Gitignored; rebuilt from the
committed lock/wheelhouse.<tier>.json on demand.

  wheelhouse.py --tier lite
"""

from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from benchlib import ARCHES, download, manifest_path, wheelhouse_dir


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--tier", default="lite", choices=["lite", "heavy", "all"])
    ap.add_argument("--arch", default="x86_64", choices=list(ARCHES))
    ap.add_argument("--jobs", type=int, default=16)
    args = ap.parse_args()

    mpath = manifest_path(args.tier, args.arch)
    if not mpath.exists():
        raise SystemExit(
            f"{mpath} missing; run freeze.py --tier {args.tier} --arch {args.arch} first"
        )
    manifest = json.loads(mpath.read_text())
    wheels = manifest["wheels"]
    dest_dir = wheelhouse_dir(args.tier, args.arch)
    dest_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"downloading {len(wheels)} wheels ({manifest['total_bytes'] / 1e6:.1f} MB) -> {dest_dir}"
    )
    done = 0
    errs = []
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futs = {
            pool.submit(download, w["url"], dest_dir / w["filename"], w["sha256"]): w
            for w in wheels
        }
        for fut in as_completed(futs):
            w = futs[fut]
            try:
                fut.result()
                done += 1
                if done % 25 == 0 or done == len(wheels):
                    print(f"  {done}/{len(wheels)}")
            except RuntimeError as e:
                errs.append((w["filename"], str(e)))

    on_disk = sum(p.stat().st_size for p in dest_dir.glob("*.whl"))
    print(f"\n{done}/{len(wheels)} wheels verified, {on_disk / 1e6:.1f} MB on disk in {dest_dir}")
    if errs:
        print(f"{len(errs)} failures:")
        for fn, e in errs[:20]:
            print(f"  {fn}: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
