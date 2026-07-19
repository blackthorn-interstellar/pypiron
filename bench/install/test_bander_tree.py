"""Unit tests for bander_tree (static bandersnatch-shaped tree from the
wheelhouse). Run: uv run -- pytest bench/install/test_bander_tree.py"""

from __future__ import annotations

import pytest
from bander_tree import build_tree, canonical


def test_canonical_pep503():
    assert canonical("Charset_Normalizer") == "charset-normalizer"
    assert canonical("ruamel.yaml") == "ruamel-yaml"


def _fixture(tmp_path, filenames):
    wh = tmp_path / "wh"
    wh.mkdir()
    wheels = []
    for fn in filenames:
        (wh / fn).write_bytes(b"x" * 10)
        name = fn.split("-")[0]
        wheels.append({"name": name, "filename": fn, "size": 10, "sha256": "ab" * 32})
    return {"wheels": wheels, "wheel_count": len(wheels)}, wh


def test_build_tree_shape_and_anchors(tmp_path):
    manifest, wh = _fixture(
        tmp_path, ["flask-3.0.0-py3-none-any.whl", "idna-3.7-py3-none-any.whl"]
    )
    out = tmp_path / "web"
    assert build_tree(manifest, wh, out) == 2
    idx = (out / "simple" / "flask" / "index.html").read_text()
    assert '../../packages/flask-3.0.0-py3-none-any.whl#sha256=' in idx
    assert (out / "packages" / "idna-3.7-py3-none-any.whl").stat().st_size == 10
    root = (out / "simple" / "index.html").read_text()
    assert '<a href="flask/">flask</a>' in root


def test_missing_wheel_fails_loud(tmp_path):
    manifest, wh = _fixture(tmp_path, ["flask-3.0.0-py3-none-any.whl"])
    manifest["wheels"].append(
        {"name": "ghost", "filename": "ghost-1.0-py3-none-any.whl", "size": 1, "sha256": "cd" * 32}
    )
    with pytest.raises(FileNotFoundError, match="ghost-1.0"):
        build_tree(manifest, wh, tmp_path / "web")
