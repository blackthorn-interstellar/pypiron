#!/usr/bin/env python3
"""Build bandersnatch's static web tree directly from the frozen wheelhouse.

The bandersnatch measurement is nginx sendfile over a PEP 503 tree — how the
tree got there is setup, not measurement (BENCHMARK_INSTALL.md §0). A real
`bandersnatch mirror` run re-downloads from pypi.org the very bytes the
wheelhouse already holds (hash-pinned from the same origin), taking many
minutes of egress for zero measurement difference. This builds the identical
serving surface in seconds: web/simple/<project>/index.html anchors with
#sha256 fragments (bandersnatch's shape) + web/packages/<file> hardlinks.

  bander_tree.py --manifest lock/x86_64/wheelhouse.lite.json \
                 --wheelhouse wheelhouse/x86_64/lite --out /tmp/bander-web
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import shutil
from collections import defaultdict
from pathlib import Path


def canonical(name: str) -> str:
    """PEP 503 name normalization."""
    return re.sub(r"[-_.]+", "-", name).lower()


def build_tree(manifest: dict, wheelhouse: Path, out: Path) -> int:
    """Write the simple/ + packages/ tree; returns the project count. A wheel
    listed in the manifest but missing from the wheelhouse is a harness bug —
    fail loud rather than serve a tree that 404s mid-benchmark."""
    packages = out / "packages"
    packages.mkdir(parents=True, exist_ok=True)
    projects: dict[str, list[dict]] = defaultdict(list)
    for w in manifest["wheels"]:
        src = wheelhouse / w["filename"]
        if not src.is_file():
            raise FileNotFoundError(f"manifest wheel missing from wheelhouse: {src}")
        dst = packages / w["filename"]
        if not dst.exists():
            # Hardlink is the fast, space-free path; fall back to a copy when the
            # OS refuses it (fs.protected_hardlinks on root-owned wheels, or a
            # cross-device src). nginx serves either as an identical sendfile.
            try:
                os.link(src, dst)
            except OSError:
                shutil.copyfile(src, dst)
        projects[canonical(w["name"])].append(w)

    for proj, wheels in projects.items():
        d = out / "simple" / proj
        d.mkdir(parents=True, exist_ok=True)
        anchors = "\n".join(
            f'    <a href="../../packages/{html.escape(w["filename"])}'
            f'#sha256={w["sha256"]}">{html.escape(w["filename"])}</a><br/>'
            for w in sorted(wheels, key=lambda w: w["filename"])
        )
        d.joinpath("index.html").write_text(
            "<!DOCTYPE html>\n<html>\n  <head><title>Links for "
            f"{proj}</title></head>\n  <body>\n    <h1>Links for {proj}</h1>\n"
            f"{anchors}\n  </body>\n</html>\n"
        )

    root = out / "simple"
    listing = "\n".join(f'    <a href="{p}/">{p}</a><br/>' for p in sorted(projects))
    root.joinpath("index.html").write_text(
        "<!DOCTYPE html>\n<html>\n  <head><title>Simple index</title></head>\n"
        f"  <body>\n{listing}\n  </body>\n</html>\n"
    )
    return len(projects)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--wheelhouse", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    n = build_tree(manifest, Path(args.wheelhouse), Path(args.out))
    print(f"built {n} projects / {manifest['wheel_count']} wheels -> {args.out}")


if __name__ == "__main__":
    main()
