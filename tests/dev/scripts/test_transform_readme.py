from __future__ import annotations

import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "dev/scripts/transform_readme.py"


@pytest.mark.parametrize("version,ref", [("0.0.0", "master"), ("1.2.3", "v1.2.3")])
def test_packaged_readme_links_and_images_work_outside_github(tmp_path, version, ref):
    (tmp_path / "Cargo.toml").write_text(f'[package]\nversion = "{version}"\n')
    readme = tmp_path / "README.md"
    readme.write_text(
        "[Guide](docs/guides/publish-install.md)\n"
        '<a href="docs/concepts.md#public-packages">Public packages</a>\n'
        '<img src="docs/assets/server-gui.png">\n'
        '<source srcset="docs/assets/chart.svg 1x, docs/assets/chart-dark.svg 2x">\n'
        '<a href="#getting-started">Start</a>\n'
        '<a href="https://pypiron.com/">Site</a>\n'
        '<img src="//example.com/image.png">\n'
    )

    subprocess.run(
        [sys.executable, str(SCRIPT), "--target", "pypi"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )

    content = readme.read_text()
    blob = f"https://github.com/blackthorn-interstellar/pypiron/blob/{ref}/"
    raw = f"https://raw.githubusercontent.com/blackthorn-interstellar/pypiron/{ref}/"
    assert f"[Guide]({blob}docs/guides/publish-install.md)" in content
    parser = HTMLParser()
    attributes = []
    parser.handle_starttag = lambda tag, attrs: attributes.extend(attrs)
    parser.feed(content)
    assert ("href", f"{blob}docs/concepts.md#public-packages") in attributes
    assert ("src", f"{raw}docs/assets/server-gui.png") in attributes
    assert (
        "srcset",
        f"{raw}docs/assets/chart.svg 1x, {raw}docs/assets/chart-dark.svg 2x",
    ) in attributes
    assert ("href", "#getting-started") in attributes
    assert ("href", "https://pypiron.com/") in attributes
    assert ("src", "//example.com/image.png") in attributes
