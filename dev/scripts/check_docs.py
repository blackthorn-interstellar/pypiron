"""Advisory drift check: does the CLI still match the docs?

`make docs-truth` runs this. It fails when a `--flag`, its `PYPIRON_*` env var,
or a clap-rendered default drifts out of sync between the three places a knob is
described:

  * the real `pypiron <sub> --help` output (clap is the source of truth),
  * the tables in docs/reference/configuration.md, and
  * src/config_template.toml (which `pypiron config init` prints verbatim).

It is mechanical on purpose — flags, env vars, defaults, template keys, not
prose — so it stays quiet unless a real rename or default change slips past the
docs. Stdlib only, like dev/scripts/transform_readme.py. Run from the repo root:

    python dev/scripts/check_docs.py --bin target/debug/pypiron
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

DOCS = Path("docs/reference/configuration.md")
TEMPLATE = Path("src/config_template.toml")

# Every subcommand whose --help contributes to the flag surface.
SUBCOMMANDS = (
    ("serve",),
    ("sync",),
    ("create-token",),
    ("healthcheck",),
    ("verify-index",),
    ("rebuild-index",),
    ("verify-chain",),
    ("buckets", "migrate"),
    ("origin", "release"),
    ("config", "init"),
)

# create-token's attribution flags are per-invocation and carry no PYPIRON_* env
# by design; the docs list their env as "none". Every other flag must pair with
# one — the repo's "every --flag is also a PYPIRON_FLAG" rule.
NO_ENV_OK = frozenset({"--role", "--repo", "--commit", "--user"})

# A clap long-help option line is `      --flag` or `      --flag <VALUE>` and
# nothing else, so a backticked `--flag` in prose or `-h, --help` never matches.
HELP_FLAG = re.compile(r"^\s+(--[a-z][a-z0-9-]*)(?:\s+<[^>]+>)?\s*$")
HELP_ENV = re.compile(r"^\s*\[env: (PYPIRON_[A-Z0-9_]+)")
HELP_DEFAULT = re.compile(r"^\s*\[default: (.+?)\]\s*$")

# A configuration.md cell is a flag cell only when it *starts* with the backticked
# flag, so a `--flag` mentioned mid-sentence is never taken for a table entry.
DOC_FLAG = re.compile(r"^`(--[a-z][a-z0-9-]*)")
DOC_ENV = re.compile(r"PYPIRON_[A-Z0-9_]+")
# A commented `# kebab-key = ...` line in the template.
TMPL_KEY = re.compile(r"^#\s*([a-z][a-z0-9-]+)\s*=")


def help_flags(binary: str) -> dict[str, dict[str, str | None]]:
    """Union every subcommand's --help into {flag: {env, default}}, deduped.

    Globals (`--config`, `--log-format`) repeat under each subcommand; the union
    collapses them and keeps the first env/default seen for a flag.
    """
    flags: dict[str, dict[str, str | None]] = {}
    for sub in SUBCOMMANDS:
        out = subprocess.run(
            [binary, *sub, "--help"], capture_output=True, text=True, check=True
        ).stdout
        current: str | None = None
        for line in out.splitlines():
            hit = HELP_FLAG.match(line)
            if hit and hit.group(1) not in ("--help", "--version"):
                current = hit.group(1)
                flags.setdefault(current, {"env": None, "default": None})
            elif current:
                env, default = HELP_ENV.match(line), HELP_DEFAULT.match(line)
                if env:
                    flags[current]["env"] = flags[current]["env"] or env.group(1)
                if default:
                    flags[current]["default"] = flags[current]["default"] or default.group(1)
    return flags


def split_cells(row: str) -> list[str]:
    r"""Split a markdown row on UNESCAPED pipes; `\|` (enum alternation) stays."""
    return [c.replace("\0", "|").strip() for c in row.replace(r"\|", "\0").split("|")]


def doc_flags() -> dict[str, dict[str, object]]:
    """Flags documented in configuration.md pipe tables -> {env, default, line}."""
    flags: dict[str, dict[str, object]] = {}
    for num, row in enumerate(DOCS.read_text(encoding="utf8").splitlines(), 1):
        if "|" not in row:
            continue
        cells = split_cells(row)
        found = next(
            ((i, m.group(1)) for i, c in enumerate(cells) if (m := DOC_FLAG.match(c))), None
        )
        if found is None:
            continue
        idx, flag = found
        env = DOC_ENV.search(cells[idx + 1]) if idx + 1 < len(cells) else None
        default = cells[idx + 2].strip("`") if idx + 2 < len(cells) else ""
        flags.setdefault(
            flag,
            {"env": env.group(0) if env else None, "default": default or None, "line": num},
        )
    return flags


def template_problems(binary: str) -> list[str]:
    """Every commented template key is documented, and `config init` == the file."""
    problems: list[str] = []
    docs_text = DOCS.read_text(encoding="utf8")
    template = TEMPLATE.read_text(encoding="utf8")
    for num, line in enumerate(template.splitlines(), 1):
        key = TMPL_KEY.match(line.strip())
        if key and key.group(1) not in docs_text:
            problems.append(f"{TEMPLATE}:{num}: key `{key.group(1)}` is not in {DOCS}")
    emitted = subprocess.run(
        [binary, "config", "init"], capture_output=True, text=True, check=True
    ).stdout
    if emitted != template:
        problems.append(f"`{binary} config init` output no longer matches {TEMPLATE}")
    return problems


def check(binary: str) -> list[str]:
    """Return every drift found between the CLI, the docs, and the template."""
    problems: list[str] = []
    cli, docs = help_flags(binary), doc_flags()
    for flag in sorted(cli):
        env, default, doc = cli[flag]["env"], cli[flag]["default"], docs.get(flag)
        if doc is None:
            problems.append(f"{flag}: in `{binary} --help` but not documented in {DOCS}")
        if env is None and flag not in NO_ENV_OK:
            problems.append(f"{flag}: CLI flag has no PYPIRON_* env var")
        if doc and env != doc["env"]:
            problems.append(f"{flag}: env {env!r} in CLI vs {doc['env']!r} at {DOCS}:{doc['line']}")
        if doc and default is not None and default != doc["default"]:
            problems.append(
                f"{flag}: default {default!r} in CLI vs {doc['default']!r} at {DOCS}:{doc['line']}"
            )
    for flag in sorted(docs):
        if flag not in cli:
            problems.append(f"{flag}: documented at {DOCS}:{docs[flag]['line']} but not a CLI flag")
    return problems + template_problems(binary)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bin", default="target/debug/pypiron", help="pypiron binary to introspect"
    )
    problems = check(parser.parse_args().bin)
    if problems:
        print(f"docs-truth: {len(problems)} drift(s) between the CLI and the docs:\n")
        for problem in problems:
            print(f"  - {problem}")
        return 1
    print("docs-truth: CLI --help, configuration.md, and config_template.toml agree.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
