"""Bare `pypiron` (no args) prints a short top-level help: subcommands plus the
global flags only. Serve-specific flags live under `pypiron serve --help`."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from .helpers import find_free_port

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


def test_serve_rejects_out_of_range_counter_knob(pypiron_bin: Path, tmp_path: Path):
    """An out-of-range counter knob fails closed at startup instead of silently
    clamping to 1 — a 0 retention would prune every finished day on the next
    compaction. The config is validated before the listener binds, so the short
    `_run` timeout doubles as the "it never started serving" assertion."""
    cp = _run(
        pypiron_bin,
        "serve",
        "--data-dir",
        str(tmp_path),
        "--counters-retention-days",
        "0",
    )
    assert cp.returncode != 0, cp.stdout + cp.stderr
    assert "retention-days must be at least 1" in (cp.stdout + cp.stderr)


def test_verify_index_could_not_run_exits_2(pypiron_bin: Path):
    """An unworkable config (s3 with no bucket) is an operational failure, not a
    divergence — exit 2 keeps it distinct from a real diff."""
    cp = _run(pypiron_bin, "verify-index", "--storage", "s3")
    assert cp.returncode == 2, f"expected could-not-run exit 2:\n{cp.stdout}{cp.stderr}"
    assert "Error:" in cp.stderr, cp.stderr


# `healthcheck` is the container HEALTHCHECK / orchestrator liveness probe: exit 0
# means healthy, nonzero means pull this node. It carries no curl/wget dependency.


def test_healthcheck_ok_via_url(pypiron_bin: Path, disk_server):
    """A healthy server's /health makes `healthcheck` exit 0."""
    cp = _run(pypiron_bin, "healthcheck", "--url", f"{disk_server['base_url']}/health")
    assert cp.returncode == 0, cp.stdout + cp.stderr


def test_healthcheck_follows_bind_addr_env(pypiron_bin: Path, disk_server):
    """With no --url, the probe derives the port from PYPIRON_BIND_ADDR — the same
    knob `serve` reads — so the baked-in container HEALTHCHECK follows a port
    override for free."""
    env = os.environ.copy()
    env["PYPIRON_BIND_ADDR"] = disk_server["bind"]
    env.pop("PYPIRON_HEALTHCHECK_URL", None)
    cp = subprocess.run(
        [str(pypiron_bin), "healthcheck"], capture_output=True, text=True, timeout=15, env=env
    )
    assert cp.returncode == 0, cp.stdout + cp.stderr


def test_healthcheck_unreachable_exits_nonzero(pypiron_bin: Path):
    """Nothing listening → connection refused → nonzero exit (orchestrator pulls
    the node), reported on stderr rather than crashing."""
    dead_port = find_free_port()
    cp = _run(pypiron_bin, "healthcheck", "--url", f"http://127.0.0.1:{dead_port}/health")
    assert cp.returncode != 0, "expected nonzero exit for an unreachable server"
    assert "health probe" in cp.stderr.lower(), cp.stderr
