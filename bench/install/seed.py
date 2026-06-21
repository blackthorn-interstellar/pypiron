#!/usr/bin/env python3
"""Seed a server-under-test with the shared wheelhouse.

Population is SETUP, not measurement (dev/BENCHMARK_INSTALL.md §0): every server
ends up serving the identical frozen byte universe. How it gets there depends on
the server class:

  upload  - private hosts (pypiron, pypiserver, pypicloud, devpi private index):
            push every wheel in once. pypiron/pypicloud speak the PEP 503 legacy
            (twine) upload API; this module implements that path.
  copy    - pypiserver: drop the wheels straight into its packages volume
            (handled by the orchestrator's volume mount, not here).
  warm    - proxies/mirrors (devpi cache, proxpi): install the corpus once WITH
            egress so the cache fills (handled by drive.py --warm).

Runs inside the loadgen container, reaching the server on the bench network.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from benchlib import http_get, upload_wheel, wait_healthy, wheelhouse_dir


def parse_name_version(filename: str) -> tuple[str, str]:
    parts = filename[: -len(".whl")].split("-")
    return parts[0], parts[1]


def pep503(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def wait_all_visible(base_url: str, names: set[str], timeout: float = 300.0) -> None:
    """Block until every package name appears in the global /simple/ index.

    The worker materializes indexes from storage after upload; on S3 this lags
    enough that a measured run firing immediately can miss a not-yet-indexed
    package (the pypiron Track-2 C=1 seed-race). Wait for full visibility first.
    """
    url = base_url.rstrip("/") + "/simple/"
    want = {pep503(n) for n in names}
    deadline = time.time() + timeout
    last_missing: set[str] = set(want)

    def seen(n: str, text: str) -> bool:
        # match absolute href /simple/n/, relative href "n/", or link text >n<
        return f"/{n}/" in text or f'"{n}/"' in text or f">{n}<" in text

    while time.time() < deadline:
        status, body, _ = http_get(url, timeout=15.0)
        if status == 200:
            text = body.decode("utf-8", "replace")
            last_missing = {n for n in want if not seen(n, text)}
            if not last_missing:
                return
        time.sleep(1.0)
    raise SystemExit(
        f"{len(last_missing)} packages never became visible: {sorted(last_missing)[:5]}"
    )


def seed_upload(base_url: str, tier: str, arch: str, user: str, password: str, jobs: int) -> None:
    legacy = base_url.rstrip("/") + "/legacy/"
    wheels = sorted(wheelhouse_dir(tier, arch).glob("*.whl"))
    if not wheels:
        raise SystemExit(f"empty wheelhouse for {tier}/{arch}; run wheelhouse.py first")
    print(f"uploading {len(wheels)} wheels to {legacy}")

    def one(path) -> tuple[str, int]:
        name, version = parse_name_version(path.name)
        status, body = upload_wheel(
            legacy, path.name, path.read_bytes(), name, version, user, password
        )
        if status not in (200, 409):  # 409 = already present (idempotent re-seed)
            raise RuntimeError(f"{path.name}: {status} {body[:200]!r}")
        return path.name, status

    done = 0
    names = set()
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        futs = [pool.submit(one, p) for p in wheels]
        for p in wheels:
            names.add(parse_name_version(p.name)[0])
        for fut in as_completed(futs):
            fut.result()
            done += 1
            if done % 25 == 0 or done == len(wheels):
                print(f"  {done}/{len(wheels)}")
    print(f"uploaded {done} wheels; waiting for index visibility ({len(names)} packages)")
    wait_all_visible(base_url, names)
    print("all packages visible")


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--server", required=True)
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--tier", default="lite")
    ap.add_argument("--arch", default="x86_64")
    ap.add_argument("--user", default="admin")
    ap.add_argument("--password", default="secret")
    ap.add_argument("--jobs", type=int, default=12)
    args = ap.parse_args()

    wait_healthy(args.base_url)
    if args.server in ("pypiron", "pypicloud"):
        seed_upload(args.base_url, args.tier, args.arch, args.user, args.password, args.jobs)
    else:
        raise SystemExit(f"no upload-seed path for server '{args.server}' (use drive.py --warm)")

    # Confirm a sample is actually visible in the index before timing.
    status, body, _ = http_get(args.base_url.rstrip("/") + "/simple/")
    if status != 200:
        print(f"WARNING: /simple/ returned {status}", file=sys.stderr)


if __name__ == "__main__":
    main()
