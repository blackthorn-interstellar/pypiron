"""Generate docs/index.md from README.md.

README.md is the single source for the landing page; the docs site serves the
same content at /. Repo-relative links are rewritten for the site: docs/-prefixed
links become site-relative, and links into the repo tree (dev/, src/, tests/,
examples/, bench/, LICENSE) become absolute GitHub URLs.

Run via `make docs` (or directly: python dev/scripts/readme_to_index.py).
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GITHUB = "https://github.com/blackthorn-interstellar/pypiron/blob/master/"

FRONT_MATTER = """---
description: A self-hosted PyPI server in Rust. 100x faster installs, private packages, an on-demand PyPI cache, and supply-chain defense built in.
---

"""

HEADER = "<!-- Generated from README.md by dev/scripts/readme_to_index.py — edit README.md, not this file. -->\n\n"


def convert(text: str) -> str:
    # Markdown links/images into docs/ become site-relative.
    text = re.sub(r"\]\(docs/", "](", text)
    # HTML img/source attributes into docs/assets become site-relative.
    text = text.replace('src="docs/assets/', 'src="assets/')
    text = text.replace('srcset="docs/assets/', 'srcset="assets/')
    # HTML link hrefs into docs/ become site-relative.
    text = text.replace('href="docs/', 'href="')
    # Repo-tree links become absolute GitHub URLs.
    text = re.sub(r"\]\((dev|src|tests|examples|bench)/", rf"]({GITHUB}\1/", text)
    text = text.replace("](LICENSE)", f"]({GITHUB}LICENSE)")
    return text


def main() -> None:
    readme = (ROOT / "README.md").read_text()
    (ROOT / "docs" / "index.md").write_text(FRONT_MATTER + HEADER + convert(readme))
    print("wrote docs/index.md from README.md")


if __name__ == "__main__":
    main()
