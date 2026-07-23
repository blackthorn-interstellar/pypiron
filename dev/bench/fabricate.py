"""Fabricate a PyPI-shaped pypiron storage tree: 0-byte artifacts, real sidecars.

The one seeder (dev/bench/MICROBENCH.md constraint 5), shared by scale.py, the
microbench harness, and the blackbox op-count tests. Truth in pypiron is
artifacts plus sidecars, and nothing on the read/sweep path opens artifact
bytes when a sidecar exists — so a fabricated tree exercises the same code as
a real mirror at ~1/40,000th of the bytes. Realism comes from the committed
corpus: real project names, real files-per-project distribution, sampled
deterministically.

Stdlib only.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import random
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
FILECOUNTS = BENCH_DIR / "corpus" / "pypi-project-filecounts.tsv.gz"

# Bump when the storage tree or sidecar layout this module writes changes.
# Cached microbench tiers are keyed on it; a stale key is deleted and rebuilt,
# never migrated. The CI lane fabricates fresh every run, so a forgotten bump
# shows up there as failing op pins or 404s.
TREE_FORMAT = 1


def normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower().strip("-")


def load_projects() -> list[tuple[str, int]]:
    """(normalized name, file count) for every real PyPI project, deduped."""
    counts: dict[str, int] = {}
    with gzip.open(FILECOUNTS, "rt", encoding="utf-8") as fh:
        for line in fh:
            name, _, cnt = line.rstrip("\n").partition("\t")
            norm = normalize(name)
            if norm:
                counts[norm] = counts.get(norm, 0) + int(cnt)
    return sorted(counts.items())


def seed_package(pkg_dir: Path, name: str, n_files: int) -> int:
    pkg_dir.mkdir(parents=True, exist_ok=True)
    for i in range(n_files):
        version = f"0.{i}.0"
        filename = f"{name}-{version}.tar.gz"
        artifact = pkg_dir / filename
        artifact.touch()
        sidecar = {
            "sha256": hashlib.sha256(filename.encode()).hexdigest(),
            "size": 0,
            "version": version,
            "upload-time": f"2025-01-{(i % 28) + 1:02d}T00:00:00Z",
            "yanked": False,
        }
        (pkg_dir / f"{filename}.meta.json").write_text(json.dumps(sidecar))
    return n_files


def sample_projects(
    packages: int, seed: int = 42, include_largest: bool = False
) -> list[tuple[str, int]]:
    """A deterministic sample of (name, file count) from the real corpus.

    `include_largest` swaps the corpus's biggest project into the sample so
    worst-case per-package cost is present by construction, not sample luck.
    """
    projects = load_projects()
    rng = random.Random(seed)
    sample = projects if packages >= len(projects) else rng.sample(projects, packages)
    if include_largest and packages < len(projects):
        largest = max(projects, key=lambda item: item[1])
        if largest not in sample:
            sample[0] = largest
    return sample


def fabricate_tree(
    dest: Path,
    packages: int,
    seed: int = 42,
    workers: int = 16,
    include_largest: bool = False,
    progress=None,
) -> dict:
    """Seed `dest` with a PyPI-shaped tree; returns (and writes) the manifest."""
    sample = sample_projects(packages, seed=seed, include_largest=include_largest)
    total_files = sum(c for _, c in sample)
    packages_root = Path(dest) / "packages"
    done_files = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(seed_package, packages_root / name, name, count) for name, count in sample
        ]
        for i, fut in enumerate(futures, 1):
            done_files += fut.result()
            if progress and i % 5000 == 0:
                progress(i, done_files)
    largest = max(sample, key=lambda item: item[1])
    manifest = {
        "packages": len(sample),
        "files": total_files,
        "seed": seed,
        "tree_format": TREE_FORMAT,
        "include_largest": include_largest,
        "largest": {"name": largest[0], "files": largest[1]},
        "sample_names": [n for n, _ in sample[:50]],
    }
    (Path(dest) / "scale-manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest
