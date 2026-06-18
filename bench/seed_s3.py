#!/usr/bin/env python3
"""Phase 3 corpus seeder: write the storage layout directly to S3.

The layout is the schema (docs/DESIGN.md), so seeding bypasses the server:
artifact + .meta.json sidecar + .metadata + .origin per package, then a
_dirty/<pkg> marker so the server's worker materializes the indexes.

Runs on the loadgen box (instance profile carries bucket access).
Requires boto3 (sudo dnf install -y python3-pip && pip3 install boto3).

  python3 seed_s3.py --bucket B --packages 10000 --files-per-package 10
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from meter import make_wheel_bytes, wheel_filename


def seed_package(s3, bucket: str, pkg: str, n_files: int) -> int:
    ops = 0
    s3.put_object(Bucket=bucket, Key=f"packages/{pkg}/.origin", Body=b"private")
    ops += 1
    for i in range(n_files):
        version = f"1.{i}.0"
        fname = wheel_filename(pkg, version)
        wheel = make_wheel_bytes(pkg, version, 1024)
        sha = hashlib.sha256(wheel).hexdigest()
        sidecar = json.dumps(
            {
                "sha256": sha,
                "size": len(wheel),
                "version": version,
                "upload-time": "2026-01-01T00:00:00Z",
                "requires-python": ">=3.8",
                "yanked": False,
            }
        ).encode()
        metadata = f"Metadata-Version: 2.1\nName: {pkg}\nVersion: {version}\n".encode()
        base = f"packages/{pkg}/{fname}"
        s3.put_object(Bucket=bucket, Key=base, Body=wheel)
        s3.put_object(Bucket=bucket, Key=f"{base}.meta.json", Body=sidecar)
        s3.put_object(Bucket=bucket, Key=f"{base}.metadata", Body=metadata)
        ops += 3
    s3.put_object(Bucket=bucket, Key=f"_dirty/{pkg}", Body=b"")
    return ops + 1


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--packages", type=int, default=10000)
    ap.add_argument("--files-per-package", type=int, default=10)
    ap.add_argument("--prefix", default="scale", help="package name prefix")
    ap.add_argument("--threads", type=int, default=64)
    ap.add_argument("--start", type=int, default=0, help="resume offset")
    args = ap.parse_args()

    session = boto3.session.Session()
    # One client shared across threads: boto3 clients are thread-safe and the
    # connection pool does the rest.
    s3 = session.client("s3", config=boto3.session.Config(max_pool_connections=args.threads * 2))

    names = [f"{args.prefix}-{i:06d}" for i in range(args.start, args.packages)]
    t0 = time.time()
    done = 0
    ops = 0
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        for n in pool.map(
            lambda pkg: seed_package(s3, args.bucket, pkg, args.files_per_package), names
        ):
            ops += n
            done += 1
            if done % 500 == 0:
                rate = done / (time.time() - t0)
                print(
                    f"{done}/{len(names)} packages, {ops} PUTs, {rate:.0f} pkg/s, "
                    f"eta {(len(names) - done) / rate / 60:.1f} min",
                    flush=True,
                )
    dt = time.time() - t0
    print(f"seeded {done} packages ({ops} PUTs) in {dt:.0f}s = {ops / dt:.0f} PUT/s")
    if done < args.packages - args.start:
        sys.exit(1)


if __name__ == "__main__":
    main()
