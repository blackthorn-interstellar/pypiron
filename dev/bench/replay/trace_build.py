#!/usr/bin/env python3
"""Build a PyPI traffic-replay trace from real download events.

Data source (real): the public ClickHouse `pypi.pypi` table — one row per
download event, carrying `project`, `filename`, `installer`, `packagetype` and
a `date`. It is the same playground the corpus check uses (src/corpus_check.rs).
Because `.whl.metadata` (PEP 658) fetches are logged as their own rows, the
artifact-vs-metadata request split is *real*, not modeled. Artifact byte sizes
join from `pypi.projects.size`.

What is real vs modeled (kept honest — see README.md):
  REAL     which files are fetched, how often, by which installer, and the
           artifact/metadata split — the actual PyPI request body at event
           granularity, plus real artifact sizes.
  MODELED  (1) sub-day arrival timing: the source only has day resolution, so
           arrivals are drawn as a homogeneous Poisson process over --window-secs;
           (2) `/simple/<project>/` index reads: file-download logs don't include
           index-page hits, so we synthesize --index-ratio index reads per
           artifact fetch (set 0 to replay only the real file stream).

Output: a portable `trace.jsonl` (one request per line, time-ordered) plus a
`manifest.json` (provenance, honest caveats, and the distinct artifacts seed.py
must fabricate). Stdlib only, like meter.py / scale.py.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

CLICKHOUSE_URL = "https://sql-clickhouse.clickhouse.com/?user=demo"

# Fallback artifact size (bytes) by packagetype, from median(size) over
# pypi.projects — used only when a filename has no size row in the snapshot.
FALLBACK_SIZE = {
    "bdist_wheel": 149164,
    "sdist": 26111,
    "bdist_egg": 77389,
    "bdist_wininst": 306628,
    "bdist_dumb": 23017,
    "bdist_msi": 262144,
    "bdist_rpm": 61894,
    "bdist_dmg": 4067174,
}
DEFAULT_SIZE = 150000
METADATA_SUFFIX = ".metadata"


def normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower().strip("-")


def clickhouse(sql: str, timeout: float = 300.0) -> str:
    """Run a query, return the raw TSV body. Raises on a ClickHouse error."""
    req = urllib.request.Request(
        CLICKHOUSE_URL, data=sql.encode("utf-8"), headers={"Content-Type": "text/plain"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        raise RuntimeError(f"clickhouse HTTP {e.code}: {body[:500]}") from e


def base_filename(filename: str) -> str:
    """Strip the PEP 658 `.metadata` suffix to get the underlying artifact."""
    return filename[: -len(METADATA_SUFFIX)] if filename.endswith(METADATA_SUFFIX) else filename


# ------------------------------ pull real events -----------------------------


# The playground's demo user breaks any scan at 10B rows read (non-deterministic
# partial result). A single day of pypi.pypi is ~19B rows, so a whole-day GROUP
# BY is unusable. But `project` is the first primary-key column, so a
# `project >= lo AND project < hi` slice prunes granules and reads only that
# range — one first-character slice is ~1-3B rows, well under the cap, and its
# counts come back complete and deterministic. We bucket by first character and
# merge; buckets are disjoint by project, so no group is split across two.
BUCKET_CHARS = "-0123456789abcdefghijklmnopqrstuvwxyz"


def _bucket_bounds() -> list[tuple[str | None, str | None]]:
    chars = sorted(set(BUCKET_CHARS))
    bounds: list[tuple[str | None, str | None]] = [(None, chars[0])]
    for a, b in zip(chars, chars[1:]):
        bounds.append((a, b))
    bounds.append((chars[-1], None))
    return bounds


def fetch_bucket(date: str, lo: str | None, hi: str | None, per_bucket: int) -> list[dict]:
    """Top-`per_bucket` file groups for one project-prefix range (complete)."""
    where = [f"date = '{date}'", "filename != ''"]
    if lo is not None:
        where.append(f"project >= '{lo}'")
    if hi is not None:
        where.append(f"project < '{hi}'")
    sql = (
        "SELECT project, filename, installer, type, count() AS c FROM pypi.pypi "
        f"WHERE {' AND '.join(where)} "
        "GROUP BY project, filename, installer, type "
        f"ORDER BY c DESC LIMIT {per_bucket} FORMAT TSV"
    )
    groups = []
    for line in clickhouse(sql).splitlines():
        project, filename, installer, ptype, count = line.split("\t")
        groups.append(
            {
                "project": project,
                "filename": filename,
                "installer": installer or "unknown",
                "type": ptype,
                "count": int(count),
            }
        )
    return groups


def fetch_groups(date: str, per_bucket: int, concurrency: int = 4) -> list[dict]:
    """Real download-count groups for `date`, bucketed by project prefix to stay
    under the demo read cap. Returns the union, globally ordered by count."""
    bounds = _bucket_bounds()
    print(f"  pulling {len(bounds)} project-prefix buckets (top {per_bucket:,} each)...")
    groups: list[dict] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for chunk in pool.map(lambda b: fetch_bucket(date, b[0], b[1], per_bucket), bounds):
            groups.extend(chunk)
    groups.sort(key=lambda g: g["count"], reverse=True)
    return groups


def fetch_sizes(filenames: list[str], batch: int = 4000) -> dict[str, int]:
    """Real artifact sizes from pypi.projects, batched by filename IN-list.

    Filters `size > 0`: the snapshot carries the filename row but leaves `size`
    unpopulated (0) for many recent uploads. A 0 is not a real size, so it falls
    through to the median-by-type fallback rather than seeding an empty file.
    """
    sizes: dict[str, int] = {}
    for i in range(0, len(filenames), batch):
        chunk = filenames[i : i + batch]
        in_list = ",".join("'" + f.replace("'", "''") + "'" for f in chunk)
        sql = (
            "SELECT filename, max(size) FROM pypi.projects "
            f"WHERE size > 0 AND filename IN ({in_list}) GROUP BY filename FORMAT TSV"
        )
        for line in clickhouse(sql).splitlines():
            name, size = line.split("\t")
            sizes[name] = int(size)
    return sizes


# --------------------------------- build trace -------------------------------


def build(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    print(f"querying clickhouse for {args.date}...")
    groups = fetch_groups(args.date, args.per_bucket)
    if args.max_groups and len(groups) > args.max_groups:
        groups = groups[: args.max_groups]
    represented = sum(g["count"] for g in groups)
    print(f"  {len(groups):,} file groups covering {represented:,} real download events")

    # Weighted sample of the real file stream (artifacts + metadata fetches),
    # collecting only the base files the trace actually touches — those are what
    # seed.py fabricates and what we look up sizes for.
    n_file = args.requests
    weights = [g["count"] for g in groups]
    sampled = rng.choices(groups, weights=weights, k=n_file)
    requests: list[dict] = []
    artifact_projects: list[str] = []
    artifact_weights: list[int] = []
    proj_seen: dict[str, int] = {}
    touched: dict[str, dict] = {}  # base filename -> {project, type, needs_metadata}
    for g in sampled:
        npkg = normalize(g["project"])
        is_meta = g["filename"].endswith(METADATA_SUFFIX)
        bf = base_filename(g["filename"])
        rec = touched.setdefault(bf, {"project": npkg, "type": g["type"], "needs_metadata": False})
        rec["needs_metadata"] = rec["needs_metadata"] or is_meta
        requests.append(
            {
                "kind": "metadata" if is_meta else "artifact",
                "path": f"/files/{npkg}/{g['filename']}",
                "installer": g["installer"],
            }
        )
        if not is_meta and npkg not in proj_seen:
            proj_seen[npkg] = len(artifact_projects)
            artifact_projects.append(npkg)
            artifact_weights.append(0)
        if not is_meta:
            artifact_weights[proj_seen[npkg]] += g["count"]

    # Modeled index reads: index_ratio per artifact fetch, projects drawn by
    # their real download weight so hot projects get hot indexes.
    n_artifact = sum(1 for r in requests if r["kind"] == "artifact")
    n_index = round(args.index_ratio * n_artifact)
    if n_index and artifact_projects:
        for npkg in rng.choices(artifact_projects, weights=artifact_weights, k=n_index):
            requests.append({"kind": "index", "path": f"/simple/{npkg}/", "installer": "modeled"})

    # Real sizes for just the touched files (metadata shares its base artifact).
    touched_files = sorted(touched)
    print(f"looking up sizes for {len(touched_files):,} touched artifacts...")
    real_sizes = fetch_sizes(touched_files)
    matched = len(real_sizes)

    def size_of(fname: str) -> int:
        if fname in real_sizes:
            return real_sizes[fname]
        return FALLBACK_SIZE.get(touched[fname]["type"], DEFAULT_SIZE)

    # Homogeneous Poisson arrivals over the window: uniform times, then sorted
    # (the order statistics of a Poisson process). Shuffle first so kind order
    # is independent of the weighted-sample order.
    rng.shuffle(requests)
    times = sorted(rng.uniform(0.0, args.window_secs) for _ in requests)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    trace_path = out_dir / "trace.jsonl"
    with trace_path.open("w") as fh:
        for t, r in zip(times, requests):
            fh.write(json.dumps({"t": round(t, 4), **r}) + "\n")

    # Distinct artifacts seed.py must fabricate: exactly the files the trace hits.
    needed = [
        {
            "project": touched[bf]["project"],
            "filename": bf,
            "size": size_of(bf),
            "needs_metadata": touched[bf]["needs_metadata"],
        }
        for bf in touched_files
    ]

    kinds = {
        "artifact": n_artifact,
        "metadata": len(requests) - n_artifact - n_index,
        "index": n_index,
    }
    manifest = {
        "source": "clickhouse pypi.pypi (real per-download events) + pypi.projects (sizes)",
        "date": args.date,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "seed": args.seed,
        "window_secs": args.window_secs,
        "index_ratio": args.index_ratio,
        "sampled_groups": len(groups),
        "represented_download_events": represented,
        "requests": {"total": len(requests), **kinds},
        "sizes": {"matched": matched, "fallback": len(touched_files) - matched},
        "distinct_artifacts": len(needed),
        "caveats": [
            "Arrival timing is modeled (homogeneous Poisson over window_secs); "
            "the source has day resolution only.",
            f"/simple/ index reads are modeled at index_ratio={args.index_ratio} per "
            "artifact fetch; download logs exclude index-page hits.",
            f"{len(touched_files) - matched} of {len(touched_files)} artifact "
            "sizes fell back to median-by-type (snapshot has no row, or size=0 for "
            "recent uploads); the rest are real.",
            "Which files, how often, by which installer, and the artifact/metadata "
            "split are REAL, at per-download-event granularity.",
            "Groups are the top files per project-prefix bucket (the demo read cap "
            "prevents a whole-day scan); this captures the head and near tail, not "
            "the full cold tail.",
        ],
        "needed_artifacts": needed,
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    print(f"\nwrote {trace_path} ({len(requests):,} requests)")
    print(f"wrote {manifest_path} ({len(needed):,} distinct artifacts to seed)")
    print(
        f"  requests: {kinds['artifact']:,} artifact / {kinds['metadata']:,} metadata "
        f"(real) + {kinds['index']:,} index (modeled)"
    )
    print(
        f"  window: {args.window_secs}s   sizes: {matched:,} real / "
        f"{len(touched_files) - matched:,} fallback"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default="2026-06-28", help="UTC day to sample (YYYY-MM-DD)")
    ap.add_argument(
        "--requests", type=int, default=10000, help="file-stream requests to synthesize"
    )
    ap.add_argument(
        "--per-bucket",
        type=int,
        default=5000,
        help="top file groups to pull per project-prefix bucket (~37 buckets)",
    )
    ap.add_argument(
        "--max-groups",
        type=int,
        default=0,
        help="cap total groups kept after merge (0 = keep all buckets' groups)",
    )
    ap.add_argument(
        "--window-secs", type=float, default=3600.0, help="wall-clock span of the trace"
    )
    ap.add_argument(
        "--index-ratio",
        type=float,
        default=1.0,
        help="modeled /simple/ reads per artifact fetch (0 = real file stream only)",
    )
    ap.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "trace"))
    ap.add_argument("--seed", type=int, default=42)
    ap.set_defaults()
    args = ap.parse_args()
    build(args)


if __name__ == "__main__":
    main()
