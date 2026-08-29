from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def load_release_notes():
    path = Path(__file__).parents[1] / "dev" / "scripts" / "release_notes.py"
    spec = importlib.util.spec_from_file_location("release_notes", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


release_notes = load_release_notes()


def test_parse_subject_accepts_bracket_scope():
    commit = release_notes.parse_subject(
        "71d4272",
        "feature: [mirror] content-selection redesign with a package denylist",
    )

    assert commit.kind == "feature"
    assert commit.scope == "mirror"
    assert commit.message == "content-selection redesign with a package denylist"
    assert not commit.breaking


def test_render_notes_groups_commits_and_links_compare():
    commits = [
        release_notes.parse_subject("6818a25", "feature: add `config init`"),
        release_notes.parse_subject("95b8648", "fix(auth)!: reject partial credentials"),
        release_notes.parse_subject("ff14e22", "chore: cargo update"),
    ]

    notes = release_notes.render_notes(
        "v0.0.12",
        "v0.0.13",
        commits,
        "blackthorn-interstellar/pypiron",
    )

    assert "## Features" in notes
    assert "- add `config init`" in notes
    assert "## Fixes" in notes
    assert "- [auth] **Breaking:** reject partial credentials" in notes
    assert "## Maintenance" in notes
    assert "cargo update" in notes
    assert (
        "**Full Changelog**: "
        "https://github.com/blackthorn-interstellar/pypiron/compare/v0.0.12...v0.0.13"
    ) in notes


def test_format_commit_escapes_markdown_in_the_subject():
    """A commit subject reaches the official release body verbatim: no author
    gets to plant a link or a tracking-pixel image in it."""
    commit = release_notes.parse_subject(
        "deadbee",
        "fix: oops [click me](https://evil.example) <img src=https://evil.example/x.png>",
    )

    line = release_notes.format_commit(commit, "blackthorn-interstellar/pypiron")

    assert "[click me](https://evil.example)" not in line
    assert "<img src" not in line.replace("\\<", "<>")
    assert "\\[click me\\]\\(https://evil.example\\)" in line
    assert "\\<img src=https://evil.example/x.png\\>" in line
    # The commit's own link still renders.
    assert "([deadbee](https://github.com/blackthorn-interstellar/pypiron/commit/deadbee))" in line


def test_format_commit_keeps_backticks_as_code_spans():
    """Commit subjects quote flags and identifiers in backticks constantly. A
    backtick can only open an inert code span, so escaping it bought nothing and
    cost every one of those subjects a pair of visible backslashes."""
    commit = release_notes.parse_subject("deadbee", "fix: rename `--filter` to `--mirror`")

    line = release_notes.format_commit(commit, None)

    assert line == "- rename `--filter` to `--mirror` (deadbee)"


def test_format_commit_escapes_a_hostile_scope():
    commit = release_notes.parse_subject("deadbee", "fix(a](b): tidy up")

    line = release_notes.format_commit(commit, None)

    assert line == "- [a\\]\\(b] tidy up (deadbee)"
