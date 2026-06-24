"""Bare `pypiron` (no args) prints a short top-level help: subcommands plus the
global flags only. Serve-specific flags live under `pypiron serve --help`."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

# The four verbs the top-level help must advertise.
SUBCOMMANDS = ("serve", "sync", "verify-index", "rebuild-index")


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


# `verify-index` exit codes follow the grep/diff idiom: 0 converged, 1 diverged,
# 2 could-not-run. CI scripts branch on these, so they are a CLI contract.


def test_verify_index_converged_exits_0(pypiron_bin: Path, tmp_path: Path):
    """An empty (or already-consistent) store has nothing to diverge."""
    cp = _run(pypiron_bin, "verify-index", "--storage", "disk", "--data-dir", str(tmp_path))
    assert cp.returncode == 0, cp.stdout + cp.stderr


def test_verify_index_diverged_exits_1(pypiron_bin: Path, tmp_path: Path):
    """A materialized view with no backing package is an orphan-view divergence.

    Exit 1 (not the generic error exit 2), and the divergence is reported on
    stdout — a found difference is data, not a tool crash.
    """
    orphan = tmp_path / "simple" / "orphanpkg" / "index.html"
    orphan.parent.mkdir(parents=True)
    orphan.write_text("<!DOCTYPE html><html><body></body></html>")

    cp = _run(pypiron_bin, "verify-index", "--storage", "disk", "--data-dir", str(tmp_path))
    assert cp.returncode == 1, f"expected diverged exit 1:\n{cp.stdout}{cp.stderr}"
    assert "orphan-view" in cp.stdout, cp.stdout
    # The expected outcome must not masquerade as a tool error on stderr.
    assert "Error:" not in cp.stderr, cp.stderr


def test_verify_index_could_not_run_exits_2(pypiron_bin: Path):
    """An unworkable config (s3 with no bucket) is an operational failure, not a
    divergence — exit 2 keeps it distinct from a real diff."""
    cp = _run(pypiron_bin, "verify-index", "--storage", "s3")
    assert cp.returncode == 2, f"expected could-not-run exit 2:\n{cp.stdout}{cp.stderr}"
    assert "Error:" in cp.stderr, cp.stderr
