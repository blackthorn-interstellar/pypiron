#!/usr/bin/env python3
"""Seed a pypiron data dir with a trace's artifacts.

Fabricate the shape, not the real bytes (the scale.py insight): pypiron's read
path never opens an artifact when a sidecar exists, so a correctly-sized file
plus a real sidecar serves `/files/...` at the right size and throughput without
downloading gigabytes of real wheels. A `.metadata` companion is written for any
artifact the trace fetches metadata for, so the PEP 658 tier returns 200.

Sizes come from the manifest (real, from pypi.projects). `--max-artifact-mb`
caps them for local runs; the sidecar size is capped to match so Content-Length
stays honest. Boot pypiron against the resulting dir and its reconciler builds
the indexes.

Stdlib only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

CHUNK = os.urandom(1 << 20)  # 1 MiB of incompressible bytes, reused across files


def parse_version(filename: str) -> str:
    """Best-effort version from a distribution filename (cosmetic: the sidecar
    version only groups releases in the index; file serving doesn't need it)."""
    if filename.endswith(".whl"):
        parts = filename[:-4].split("-")
        if len(parts) >= 2:
            return parts[1]
    stem = re.sub(r"\.(tar\.gz|tar\.bz2|tgz|zip|egg|exe|msi|rpm|whl)$", "", filename)
    m = re.search(r"-(\d[^-]*)", stem)
    return m.group(1) if m else "0.0.0"


def write_artifact(root: Path, art: dict, cap_bytes: int) -> int:
    project, filename = art["project"], art["filename"]
    size = min(art["size"], cap_bytes) if cap_bytes else art["size"]
    pkg_dir = root / "packages" / project
    pkg_dir.mkdir(parents=True, exist_ok=True)

    artifact_path = pkg_dir / filename
    with artifact_path.open("wb") as fh:
        remaining = size
        while remaining > 0:
            fh.write(CHUNK[: min(remaining, len(CHUNK))])
            remaining -= min(remaining, len(CHUNK))

    sidecar = {
        "sha256": hashlib.sha256(filename.encode()).hexdigest(),
        "size": size,
        "version": parse_version(filename),
        "upload-time": "2025-01-01T00:00:00Z",
        "yanked": False,
    }
    (pkg_dir / f"{filename}.meta.json").write_text(json.dumps(sidecar))

    if art["needs_metadata"]:
        meta = (
            f"Metadata-Version: 2.1\nName: {project}\n"
            f"Version: {sidecar['version']}\nSummary: pypiron replay corpus\n"
        )
        (pkg_dir / f"{filename}.metadata").write_text(meta)
    return size


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--manifest", default=str(Path(__file__).resolve().parent / "trace" / "manifest.json")
    )
    ap.add_argument("--data-dir", required=True, help="pypiron data dir to populate")
    ap.add_argument(
        "--max-artifact-mb",
        type=float,
        default=8.0,
        help="cap fabricated artifact size (0 = real sizes; costs real disk)",
    )
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    manifest = json.loads(Path(args.manifest).read_text())
    artifacts = manifest["needed_artifacts"]
    cap = int(args.max_artifact_mb * (1 << 20))
    root = Path(args.data_dir)
    root.mkdir(parents=True, exist_ok=True)

    print(f"seeding {len(artifacts):,} artifacts -> {root} (cap {args.max_artifact_mb} MB/file)")
    start = time.monotonic()
    total_bytes = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for i, n in enumerate(pool.map(lambda a: write_artifact(root, a, cap), artifacts), 1):
            total_bytes += n
            if i % 2000 == 0:
                print(f"  {i:,} artifacts, {total_bytes / 1e6:,.0f} MB")
    elapsed = time.monotonic() - start
    print(
        f"seeded {len(artifacts):,} artifacts / {total_bytes / 1e6:,.1f} MB in {elapsed:,.1f}s\n"
        f"boot: target/release/pypiron serve --data-dir {root} "
        f"--admin-user admin --admin-pass secret --bind-addr 127.0.0.1:8080"
    )


if __name__ == "__main__":
    main()
