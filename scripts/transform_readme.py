"""Rewrite the README's relative links and images to absolute URLs for PyPI.

GitHub renders README.md with repo-relative links, so the committed file keeps
its clean `](docs/...)` / `<img src="docs/...">` references. PyPI (and any other
off-repo host) does not resolve those — on the project page the logo 404s and
every doc link is dead. CI runs this before `maturin build` so the README baked
into the wheel/sdist metadata points at absolute github.com / raw URLs, while the
file on GitHub stays relative. Mirrors uv's scripts/transform_readme.py.

Run from the repo root: `python scripts/transform_readme.py --target pypi`.
"""

from __future__ import annotations

import argparse
import re
import urllib.parse
from pathlib import Path

REPO = "brycedrennan/pypiron"
# Links render as pages (github blob); images need the raw byte host.
BLOB = f"https://github.com/{REPO}/blob/{{ref}}/"
RAW = f"https://raw.githubusercontent.com/{REPO}/{{ref}}/"


def _ref() -> str:
    """Pin URLs to the release tag once CI has stamped the version, else master.

    The repo permanently carries version 0.0.0 (see RELEASE.md); CI stamps the
    real vX.Y.Z into Cargo.toml before building. Read it with a regex rather than
    tomllib so the script runs on the older Pythons some CI runners default to.
    """
    text = Path("Cargo.toml").read_text(encoding="utf8")
    match = re.search(
        r'(?ms)^\[package\][^\[]*?^version\s*=\s*"([^"]+)"', text
    )
    if not match:
        raise ValueError("could not find [package] version in Cargo.toml")
    version = match.group(1)
    return "master" if version == "0.0.0" else f"v{version}"


def _is_relative(url: str) -> bool:
    return not url.startswith(("http://", "https://", "#", "mailto:"))


def main(target: str) -> None:
    if target != "pypi":
        raise ValueError(f"unknown target: {target}")

    ref = _ref()
    blob = BLOB.format(ref=ref)
    raw = RAW.format(ref=ref)
    content = Path("README.md").read_text(encoding="utf8")

    def link(match: re.Match) -> str:
        url = match.group(1)
        if not _is_relative(url):
            return match.group(0)
        return f"]({urllib.parse.urljoin(blob, url)})"

    def image(match: re.Match) -> str:
        url = match.group(1)
        if not _is_relative(url):
            return match.group(0)
        return f'src="{urllib.parse.urljoin(raw, url)}"'

    content = re.sub(r"\]\(([^)]+)\)", link, content)
    content = re.sub(r'src="([^"]+)"', image, content)

    Path("README.md").write_text(content, encoding="utf8")
    print(f"transformed README.md for {target} (ref: {ref})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=("pypi",))
    main(parser.parse_args().target)
