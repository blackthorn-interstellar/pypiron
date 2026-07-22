"""Generate GitHub Release notes from this repo's Conventional Commit history."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

_COMMIT_RE = re.compile(
    r"^(?P<kind>[a-z]+)(?:\((?P<scope>[^)]+)\))?(?P<breaking>!)?:\s*(?P<message>.+)$"
)
_BRACKET_SCOPE_RE = re.compile(r"^\[(?P<scope>[^\]]+)\]\s+(?P<message>.+)$")
_SEMVER_TAG_RE = re.compile(
    r"^v(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)$"
)

_CATEGORY_BY_KIND = {
    "security": "Security",
    "feature": "Features",
    "feat": "Features",
    "fix": "Fixes",
    "performance": "Performance",
    "perf": "Performance",
    "docs": "Docs",
    "ci": "CI",
    "build": "Maintenance",
    "chore": "Maintenance",
    "refactor": "Maintenance",
    "test": "Maintenance",
}
_CATEGORY_ORDER = [
    "Security",
    "Features",
    "Fixes",
    "Performance",
    "Docs",
    "CI",
    "Maintenance",
    "Other",
]


@dataclass(frozen=True)
class Commit:
    short_hash: str
    kind: str
    scope: str | None
    message: str
    breaking: bool = False


def run_git(args: list[str]) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def semver_key(tag: str) -> tuple[int, int, int]:
    match = _SEMVER_TAG_RE.match(tag)
    if not match:
        raise ValueError(f"not a vX.Y.Z tag: {tag}")
    return (
        int(match.group("major")),
        int(match.group("minor")),
        int(match.group("patch")),
    )


def release_tags_merged_into(ref: str) -> list[str]:
    tags = run_git(["tag", "--merged", ref, "--list", "v[0-9]*.[0-9]*.[0-9]*"]).splitlines()
    return sorted((tag for tag in tags if _SEMVER_TAG_RE.match(tag)), key=semver_key)


def previous_release_tag(current_tag: str, ref: str) -> str | None:
    current = semver_key(current_tag)
    previous = [tag for tag in release_tags_merged_into(ref) if semver_key(tag) < current]
    return previous[-1] if previous else None


def latest_release_tag(ref: str) -> str | None:
    tags = release_tags_merged_into(ref)
    return tags[-1] if tags else None


def tag_pointing_at(ref: str) -> str | None:
    tags = run_git(["tag", "--points-at", ref]).splitlines()
    release_tags = [tag for tag in tags if _SEMVER_TAG_RE.match(tag)]
    return sorted(release_tags, key=semver_key)[-1] if release_tags else None


def repo_slug() -> str | None:
    if repo := os.environ.get("GITHUB_REPOSITORY"):
        return repo

    try:
        remote = run_git(["remote", "get-url", "origin"])
    except subprocess.CalledProcessError:
        return None

    patterns = [
        r"^git@github\.com:(?P<repo>[^/]+/[^/]+?)(?:\.git)?$",
        r"^https://github\.com/(?P<repo>[^/]+/[^/]+?)(?:\.git)?$",
    ]
    for pattern in patterns:
        if match := re.match(pattern, remote):
            return match.group("repo")
    return None


def parse_subject(short_hash: str, subject: str) -> Commit:
    match = _COMMIT_RE.match(subject)
    if not match:
        return Commit(short_hash, "other", None, subject)

    kind = match.group("kind")
    scope = match.group("scope")
    message = match.group("message").strip()
    if not scope and (scope_match := _BRACKET_SCOPE_RE.match(message)):
        scope = scope_match.group("scope")
        message = scope_match.group("message").strip()

    return Commit(
        short_hash=short_hash,
        kind=kind,
        scope=scope,
        message=message,
        breaking=bool(match.group("breaking")),
    )


def commits_between(base: str | None, ref: str) -> list[Commit]:
    range_ref = f"{base}..{ref}" if base else ref
    output = run_git(["log", "--format=%h%x1f%s", range_ref])
    commits: list[Commit] = []
    for line in output.splitlines():
        if not line:
            continue
        short_hash, subject = line.split("\x1f", 1)
        commits.append(parse_subject(short_hash, subject))
    return commits


def category(commit: Commit) -> str:
    return _CATEGORY_BY_KIND.get(commit.kind, "Other")


def format_commit(commit: Commit, repo: str | None) -> str:
    scope = f"[{commit.scope}] " if commit.scope else ""
    message = f"**Breaking:** {commit.message}" if commit.breaking else commit.message
    if repo:
        link = f"https://github.com/{repo}/commit/{commit.short_hash}"
        return f"- {scope}{message} ([{commit.short_hash}]({link}))"
    return f"- {scope}{message} ({commit.short_hash})"


def render_notes(base: str | None, ref: str, commits: list[Commit], repo: str | None) -> str:
    grouped = {name: [] for name in _CATEGORY_ORDER}
    for commit in commits:
        grouped[category(commit)].append(commit)

    lines: list[str] = []
    for name in _CATEGORY_ORDER:
        if not grouped[name]:
            continue
        lines.extend([f"## {name}", ""])
        lines.extend(format_commit(commit, repo) for commit in grouped[name])
        lines.append("")

    if not lines:
        lines.extend(["No user-facing changes.", ""])

    if repo:
        if base:
            lines.append(f"**Full Changelog**: https://github.com/{repo}/compare/{base}...{ref}")
        else:
            lines.append(f"**Full Changelog**: https://github.com/{repo}/commits/{ref}")
    return "\n".join(lines).rstrip() + "\n"


def write_notes(base: str | None, ref: str, tag: str | None, output: Path | None) -> None:
    release_base = base
    if release_base is None:
        release_base = previous_release_tag(tag, ref) if tag else latest_release_tag(ref)

    notes = render_notes(release_base, ref, commits_between(release_base, ref), repo_slug())
    if output:
        output.write_text(notes, encoding="utf8")
    else:
        print(notes, end="")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="base", help="base tag/ref; defaults to previous release")
    parser.add_argument("--to", default="HEAD", help="release tag/ref to describe")
    parser.add_argument("--tag", help="current vX.Y.Z tag; used to find the previous release")
    parser.add_argument("--output", type=Path, help="write notes to this file instead of stdout")
    args = parser.parse_args()

    tag = args.tag or tag_pointing_at(args.to)
    write_notes(args.base, args.to, tag, args.output)


if __name__ == "__main__":
    main()
