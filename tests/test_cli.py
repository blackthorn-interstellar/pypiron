"""Bare `pypiron` (no args) prints a short top-level help: subcommands plus the
global flags only. Serve-specific flags live under `pypiron serve --help`."""

from __future__ import annotations

import os
import re
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
    for flag in ("--bind-addr", "--buckets", "--admin-user", "--proxy-upstream"):
        assert flag in out, f"`serve --help` missing {flag!r}:\n{out}"


# `verify-index` exit codes follow the grep/diff idiom: 0 converged, 1 diverged,
# 2 could-not-run. CI scripts branch on these, so they are a CLI contract.


def test_verify_index_converged_exits_0(pypiron_bin: Path, tmp_path: Path):
    """An empty (or already-consistent) store has nothing to diverge."""
    cp = _run(pypiron_bin, "verify-index", "--data-dir", str(tmp_path))
    assert cp.returncode == 0, cp.stdout + cp.stderr


def test_verify_index_diverged_exits_1(pypiron_bin: Path, tmp_path: Path):
    """A materialized view with no backing package is an orphan-view divergence.

    Exit 1 (not the generic error exit 2), and the divergence is reported on
    stdout — a found difference is data, not a tool crash.
    """
    orphan = tmp_path / "simple" / "orphanpkg" / "index.html"
    orphan.parent.mkdir(parents=True)
    orphan.write_text("<!DOCTYPE html><html><body></body></html>")

    cp = _run(pypiron_bin, "verify-index", "--data-dir", str(tmp_path))
    assert cp.returncode == 1, f"expected diverged exit 1:\n{cp.stdout}{cp.stderr}"
    assert "orphan-view" in cp.stdout, cp.stdout
    # The expected outcome must not masquerade as a tool error on stderr.
    assert "Error:" not in cp.stderr, cp.stderr


# `config init` prints an annotated pypiron.toml to stdout. It is the guided
# path to a first config: `pypiron config init > pypiron.toml`, then uncomment.


def test_config_init_prints_annotated_template(pypiron_bin: Path):
    """stdout carries every section header and a sampling of keys, all commented."""
    cp = _run(pypiron_bin, "config", "init")
    assert cp.returncode == 0, cp.stdout + cp.stderr
    out = cp.stdout
    for header in ("[serve]", "[mirror]", "[sync]"):
        assert header in out, f"template missing {header!r}:\n{out}"
    for key in ("bind-addr", "buckets", "proxy-upstream", "include-packages", "to"):
        assert f"# {key} = " in out, f"template missing commented `{key}`:\n{out}"
    # Nothing is uncommented, so the emitted file is a no-op until edited.
    assert "\nbuckets = " not in out, "template should ship buckets commented out"


def test_config_init_output_loads_as_config(pypiron_bin: Path, tmp_path: Path):
    """The emitted file is accepted by the real config loader end-to-end: write
    it, then have `verify-index` load it via --config against an empty store
    (converged → exit 0). Proves `config init` output is valid, not just pretty."""
    cfg = tmp_path / "pypiron.toml"
    cfg.write_text(_run(pypiron_bin, "config", "init").stdout)
    store = tmp_path / "store"
    store.mkdir()
    cp = _run(
        pypiron_bin,
        "verify-index",
        "--config",
        str(cfg),
        "--data-dir",
        str(store),
    )
    assert cp.returncode == 0, cp.stdout + cp.stderr


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
    """An unworkable config (a bucket URI with no scheme) is an operational
    failure, not a divergence — exit 2 keeps it distinct from a real diff."""
    cp = _run(pypiron_bin, "verify-index", "--buckets", "not-a-uri")
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


# `--help` renders every flag's env var. For a credential that means the running
# secret gets printed to whatever captured the help — a CI log, a screen share, a
# bug report. clap hides the value (not the name) with `hide_env_values`; this
# pins that every credential env var has it.

# env var -> the value exported while the help is rendered. Each is unique so a
# failure names the exact leaking var.
CREDENTIAL_ENV = {
    "PYPIRON_ADMIN_PASS": "leaked-admin-pass",
    "PYPIRON_UPLOADER_PASS": "leaked-uploader-pass",
    "PYPIRON_READ_PASS": "leaked-read-pass",
    "PYPIRON_TOKEN_SIGNING_KEY": "leaked-signing-key",
    "PYPIRON_AUTH": "leaked-user:leaked-auth-pass",
    "PYPIRON_SYNC_SOURCE_PASS": "leaked-source-pass",
    "PYPIRON_SYNC_ADMIN_PASS": "leaked-sync-admin-pass",
    "PYPIRON_AZURE_ACCESS_KEY": "leaked-azure-key",
}

# A clap `Commands:` entry: exactly two spaces of indent, then the verb.
# Wrapped description lines are indented further, so they don't match.
_SUBCOMMAND_RE = re.compile(r"^  ([a-z][a-z0-9-]*)(?:\s|$)")


def _help_paths(bin_path: Path, path: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """Every `--help` reachable from `path`, walking nested subcommands.

    Discovered rather than listed so a subcommand added later is covered without
    anyone remembering to extend this test.
    """
    cp = _run(bin_path, *path, "--help")
    out = cp.stdout + cp.stderr
    found = [(*path, name) for name in _subcommand_names(out) if name != "help"]
    return [path, *(p for child in found for p in _help_paths(bin_path, child))]


def _subcommand_names(help_text: str) -> list[str]:
    names: list[str] = []
    in_commands = False
    for line in help_text.splitlines():
        if line.startswith("Commands:"):
            in_commands = True
            continue
        if in_commands:
            if not line.strip():
                break
            m = _SUBCOMMAND_RE.match(line)
            if m:
                names.append(m.group(1))
    return names


def test_help_never_prints_credential_env_values(pypiron_bin: Path, monkeypatch):
    """No `--help` anywhere in the CLI echoes a credential's current value."""
    for var, value in CREDENTIAL_ENV.items():
        monkeypatch.setenv(var, value)

    paths = _help_paths(pypiron_bin)
    # The walk found the real tree, not an empty list that vacuously passes.
    assert ("serve",) in paths, paths
    assert ("create-token",) in paths, paths
    assert ("sync",) in paths, paths

    for path in paths:
        cp = _run(pypiron_bin, *path, "--help")
        out = cp.stdout + cp.stderr
        for var, value in CREDENTIAL_ENV.items():
            assert value not in out, (
                f"`pypiron {' '.join((*path, '--help'))}` printed the value of {var}:\n{out}"
            )


def test_help_still_names_credential_env_vars(pypiron_bin: Path, monkeypatch):
    """Hiding the value must not hide the var — operators need to know the knob
    exists and what to export."""
    for var, value in CREDENTIAL_ENV.items():
        monkeypatch.setenv(var, value)

    expected = {
        ("serve",): (
            "PYPIRON_ADMIN_PASS",
            "PYPIRON_UPLOADER_PASS",
            "PYPIRON_READ_PASS",
            "PYPIRON_TOKEN_SIGNING_KEY",
            "PYPIRON_AZURE_ACCESS_KEY",
        ),
        ("create-token",): ("PYPIRON_AUTH",),
        ("sync",): ("PYPIRON_SYNC_SOURCE_PASS", "PYPIRON_SYNC_ADMIN_PASS"),
    }
    for path, variables in expected.items():
        cp = _run(pypiron_bin, *path, "--help")
        out = cp.stdout + cp.stderr
        for var in variables:
            assert var in out, f"`pypiron {' '.join(path)} --help` lost {var}:\n{out}"
