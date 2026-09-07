from __future__ import annotations

import importlib.util
from pathlib import Path


def test_html_feature_links_point_to_published_pages():
    script = Path(__file__).resolve().parents[3] / "dev/scripts/readme_to_index.py"
    spec = importlib.util.spec_from_file_location("readme_to_index", script)
    assert spec is not None and spec.loader is not None
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)

    converted = generator.convert(
        '<a href="docs/compare/index.md">Compare</a>\n'
        '<a href="docs/concepts.md#public-packages">Public packages</a>\n'
        '<a href="docs/index.md#getting-started">Start</a>\n'
        '<a href="docs/assets/server-gui.png">Dashboard</a>\n'
        "[Guide](docs/guides/standard-cloud.md)\n"
    )

    assert '<a href="compare/">Compare</a>' in converted
    assert '<a href="concepts/#public-packages">Public packages</a>' in converted
    assert '<a href="./#getting-started">Start</a>' in converted
    assert '<a href="assets/server-gui.png">Dashboard</a>' in converted
    assert "[Guide](guides/standard-cloud.md)" in converted
