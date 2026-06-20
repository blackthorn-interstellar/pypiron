"""Bare `pypiron` (no args) prints a short top-level help: subcommands plus the
global flags only. Serve-specific flags live under `pypiron serve --help`."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

# The four verbs the top-level help must advertise.
SUBCOMMANDS = ("serve", "sync", "verify", "resync")


def _run(bin_path: Path, *args: str) -> subprocess.CompletedProcess:
    # A short timeout doubles as the "it didn't start serving" assertion: a
    # server would block here until killed; help returns immediately.
    return subprocess.run(
        [str(bin_path), *args],
        capture_output=True,
        text=True,
        timeout=15,
    )


def test_bare_invocation_prints_help(pypiron_bin: Path):
    cp = _run(pypiron_bin)
    out = cp.stdout + cp.stderr
    for sub in SUBCOMMANDS:
        assert sub in out, f"help missing subcommand {sub!r}:\n{out}"
    assert "Usage:" in out, out
    # The global flag is shown...
    assert "--log-format" in out, out
    # ...but serve-specific flags are NOT dumped at the top level anymore; they
    # moved under `serve`.
    assert "--bind-addr" not in out, f"serve flag leaked into top-level help:\n{out}"


def test_bare_matches_help_flag(pypiron_bin: Path):
    """`pypiron` alone surfaces the same commands/flags as `pypiron --help`."""
    bare = _run(pypiron_bin)
    helped = _run(pypiron_bin, "--help")
    assert helped.returncode == 0
    for token in (*SUBCOMMANDS, "--log-format", "Commands:"):
        assert token in (bare.stdout + bare.stderr), token
        assert token in (helped.stdout + helped.stderr), token


def test_serve_help_lists_serve_flags(pypiron_bin: Path):
    """The serve flags are reachable under the `serve` subcommand."""
    cp = _run(pypiron_bin, "serve", "--help")
    assert cp.returncode == 0
    out = cp.stdout + cp.stderr
    for flag in ("--bind-addr", "--storage", "--admin-user", "--proxy-upstream"):
        assert flag in out, f"`serve --help` missing {flag!r}:\n{out}"
