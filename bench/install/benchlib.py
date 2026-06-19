#!/usr/bin/env python3
"""Shared helpers for the realistic install benchmark (bench/install/).

Stdlib only, like meter.py. Reuses the frozen bench/meter.py helpers by path
import rather than duplicating them — meter.py must not be edited (its shape is
comparability-load-bearing for the meter series).
"""

from __future__ import annotations

import hashlib
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

HERE = Path(__file__).resolve().parent  # bench/install
BENCH = HERE.parent  # bench
REPO = BENCH.parent  # repo root
LOCK = HERE / "lock"
WHEELHOUSE = HERE / "wheelhouse"
RESULTS = HERE / "results"
COMPOSE = HERE / "compose"

# Frozen corpus artifacts are namespaced by CPU arch: the committed AWS corpus is
# x86_64; aarch64 supports Graviton rigs and fast native local validation on
# Apple Silicon. uv installs only wheels matching the loadgen runtime, so the
# corpus arch must match the loadgen container arch.
ARCHES = ("x86_64", "aarch64")


def closures_dir(arch: str) -> Path:
    return LOCK / arch / "closures"


def manifest_path(tier: str, arch: str) -> Path:
    return LOCK / arch / f"wheelhouse.{tier}.json"


def wheelhouse_dir(tier: str, arch: str) -> Path:
    return WHEELHOUSE / arch / tier


# Reuse the frozen meter helpers (read-only import; never edit meter.py).
sys.path.insert(0, str(BENCH))
from meter import (  # noqa: E402
    RssSampler,
    http_get,
    percentile,
    run_oha,
    upload_wheel,
    wait_healthy,
    wait_visible,
)

__all__ = [
    "HERE",
    "BENCH",
    "REPO",
    "LOCK",
    "WHEELHOUSE",
    "RESULTS",
    "COMPOSE",
    "ARCHES",
    "closures_dir",
    "manifest_path",
    "wheelhouse_dir",
    "RssSampler",
    "http_get",
    "percentile",
    "run_oha",
    "upload_wheel",
    "wait_healthy",
    "wait_visible",
    "load_projects",
    "select_projects",
    "is_glibc_wheel",
    "sha256_file",
    "download",
]


def load_projects() -> Dict[str, Any]:
    """Parse lock/projects.toml (the curated corpus input)."""
    import tomllib

    with open(LOCK / "projects.toml", "rb") as f:
        return tomllib.load(f)


def select_projects(doc: Dict[str, Any], tier: str, only: List[str] | None = None) -> List[Dict]:
    """Projects in `tier` ('lite'|'heavy'|'all'), optionally filtered to `only` names."""
    out = []
    for p in doc.get("project", []):
        if tier not in ("all", p.get("tier", "lite")):
            continue
        if only and p["name"] not in only:
            continue
        out.append(p)
    return out


def is_glibc_wheel(filename: str, arch: str = "x86_64") -> bool:
    """True for wheels a glibc Linux runtime on `arch` (AL2023/Debian) will fetch.

    Keeps manylinux <arch> + pure (`-none-any`); drops musllinux and every other
    platform. The env-constrained lock only emits linux/<arch> + universal files,
    so this mainly strips the musllinux duplicates.
    """
    if not filename.endswith(".whl"):
        return False
    if "musllinux" in filename:
        return False
    other = {"x86_64": ("aarch64", "arm64"), "aarch64": ("x86_64", "amd64")}[arch]
    bad = ("macosx", "win32", "win_amd64", "i686", "ppc64", "s390x", "universal2", *other)
    if any(b in filename for b in bad):
        return False
    if filename.endswith("-none-any.whl"):
        return True
    return "manylinux" in filename and arch in filename


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download(url: str, dest: Path, expected_sha: str | None = None, retries: int = 3) -> int:
    """Download url -> dest atomically, verifying sha256. Returns bytes written."""
    if dest.exists() and expected_sha and sha256_file(dest) == expected_sha:
        return dest.stat().st_size
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    last: Exception | None = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=120) as resp, open(tmp, "wb") as out:
                while True:
                    chunk = resp.read(1 << 20)
                    if not chunk:
                        break
                    out.write(chunk)
            got = sha256_file(tmp)
            if expected_sha and got != expected_sha:
                raise RuntimeError(f"sha256 mismatch for {url}: got {got}, want {expected_sha}")
            tmp.rename(dest)
            return dest.stat().st_size
        except (urllib.error.URLError, OSError, RuntimeError) as e:
            last = e
            time.sleep(1.0 * (attempt + 1))
    tmp.unlink(missing_ok=True)
    raise RuntimeError(f"download failed after {retries} tries: {url}: {last}")
