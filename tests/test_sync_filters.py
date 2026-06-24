"""Sync filtering: PEP 440 specifiers, upload-time bounds, pypiron.toml.

All filters gate what a run adds — never what is already mirrored. Sync mirrors
over HTTP, so these drive the real binary with `--to` against a real server; the
server's storage tree is the contract, so tree assertions are blackbox
assertions.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime

import pytest

from .conftest import _start_disk_server
from .helpers import sync_to

PACKAGE = "six"

pytestmark = pytest.mark.integration


def _wheels(server):
    pkg_dir = server["data_dir"] / "packages" / PACKAGE
    if not pkg_dir.exists():
        return []
    return sorted(p.name for p in pkg_dir.iterdir() if p.name.endswith(".whl"))


@contextmanager
def _extra_server(tmp_path_factory, pypiron_bin):
    """A second, independent destination server for tests that compare two
    fully separate sync runs."""
    gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    try:
        yield next(gen)
    finally:
        gen.close()


def _packages_list(tmp_path, spec: str):
    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{spec}\n")
    return pkg_list


def test_version_specifiers_limit_releases(disk_server, pypiron_bin, tmp_path):
    pkg_list = _packages_list(tmp_path, f"{PACKAGE}>=1.15,<1.17")
    rc, out, err = sync_to(
        pypiron_bin, disk_server, "--filter-packages-list", str(pkg_list), "--filter-only-wheels"
    )
    assert rc == 0, f"sync failed:\n{out}\n{err}"

    wheels = _wheels(disk_server)
    assert wheels, "constraint should still match something"
    for name in wheels:
        version = name.split("-")[1]
        assert version in ("1.15.0", "1.16.0"), f"{name} violates >=1.15,<1.17"


def test_pkg_flag_is_repeatable_with_no_list_file(disk_server, pypiron_bin):
    """--filter-package selects packages with no packages-list file at all; it is repeatable
    and accepts the same line syntax (specifiers included)."""
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--filter-package",
        f"{PACKAGE}==1.16.0",
        "--filter-package",
        "iniconfig==2.0.0",
        "--filter-only-wheels",
    )
    assert rc == 0, f"sync failed:\n{out}\n{err}"
    assert _wheels(disk_server) == ["six-1.16.0-py2.py3-none-any.whl"]
    # The second --filter-package occurrence was honored too (accumulating, not overwriting).
    ini_dir = disk_server["data_dir"] / "packages" / "iniconfig"
    assert any(p.name.endswith(".whl") for p in ini_dir.iterdir()), (
        "second --filter-package not mirrored"
    )


def test_exclude_newer_bounds_mirroring(disk_server, pypiron_bin, tmp_path):
    cutoff = "2016-01-01T00:00:00Z"
    pkg_list = _packages_list(tmp_path, PACKAGE)
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--filter-packages-list",
        str(pkg_list),
        "--filter-only-wheels",
        "--filter-exclude-newer",
        cutoff,
    )
    assert rc == 0, f"sync failed:\n{out}\n{err}"

    wheels = _wheels(disk_server)
    assert wheels, "six has pre-2016 wheels"
    cutoff_dt = datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
    pkg_dir = disk_server["data_dir"] / "packages" / PACKAGE
    for name in wheels:
        sc = json.loads((pkg_dir / f"{name}.meta.json").read_text())
        uploaded = datetime.fromisoformat(sc["upload-time"].replace("Z", "+00:00"))
        assert uploaded < cutoff_dt, f"{name} uploaded {uploaded}, after the cutoff"


def test_duration_cutoff_is_accepted(disk_server, pypiron_bin, tmp_path):
    """A cutoff may be a relative duration (uv-style), not just an RFC 3339
    timestamp: `--filter-exclude-older "1 day"` resolves to ~yesterday, so six's
    long-old wheels are all filtered out and nothing is mirrored."""
    pkg_list = _packages_list(tmp_path, PACKAGE)
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--filter-packages-list",
        str(pkg_list),
        "--filter-only-wheels",
        "--filter-exclude-older",
        "1 day",
    )
    assert rc == 0, f"a duration cutoff must be accepted:\n{out}\n{err}"
    assert _wheels(disk_server) == [], "every six wheel predates a 1-day cutoff"


def test_filters_never_remove_mirrored_files(disk_server, pypiron_bin, tmp_path):
    def sync(spec, *extra):
        pkg_list = _packages_list(tmp_path, spec)
        rc, out, err = sync_to(
            pypiron_bin,
            disk_server,
            "--filter-packages-list",
            str(pkg_list),
            "--filter-only-wheels",
            *extra,
        )
        assert rc == 0, f"sync failed:\n{out}\n{err}"

    sync(f"{PACKAGE}==1.16.0")
    assert _wheels(disk_server) == ["six-1.16.0-py2.py3-none-any.whl"]

    sync(f"{PACKAGE}==1.17.0")
    assert _wheels(disk_server) == [
        "six-1.16.0-py2.py3-none-any.whl",
        "six-1.17.0-py2.py3-none-any.whl",
    ], "a narrower later run must only add, never remove"

    # A filter matching nothing changes nothing.
    sync(PACKAGE, "--filter-exclude-newer", "2001-01-01T00:00:00Z")
    assert len(_wheels(disk_server)) == 2


def test_toml_config_with_cli_precedence(disk_server, pypiron_bin, tmp_path, tmp_path_factory):
    config = tmp_path / "pypiron.toml"
    config.write_text(
        f"""
[filter]
packages = ["{PACKAGE}==1.16.0"]
only-wheels = true
exclude-newer = "2030-01-01T00:00:00Z"
"""
    )

    # Config alone drives the run: inline packages, only-wheels, permissive cutoff.
    rc, out, err = sync_to(pypiron_bin, disk_server, "--config", str(config))
    assert rc == 0, f"sync failed:\n{out}\n{err}"
    assert _wheels(disk_server) == ["six-1.16.0-py2.py3-none-any.whl"]

    # CLI overrides the file: an impossible cutoff means nothing is mirrored.
    # A fresh destination so the first run's file can't be mistaken for a hit.
    with _extra_server(tmp_path_factory, pypiron_bin) as dest2:
        rc, out, err = sync_to(
            pypiron_bin,
            dest2,
            "--config",
            str(config),
            "--filter-exclude-newer",
            "2001-01-01T00:00:00Z",
        )
        assert rc == 0, f"sync failed:\n{out}\n{err}"
        assert _wheels(dest2) == []

    # Typos in the config are hard errors, not silent no-ops.
    bad = tmp_path / "bad.toml"
    bad.write_text("[sync]\nonly-weels = true\n")
    rc, out, err = sync_to(pypiron_bin, disk_server, "--config", str(bad))
    assert rc != 0
    assert "only-weels" in (out + err)


def test_exclude_only_platform_tag_keeps_sdists(disk_server, pypiron_bin, tmp_path):
    """--filter-exclude-platform-tag must not silently drop sdists (they have no tag)."""
    pkg_list = _packages_list(tmp_path, f"{PACKAGE}==1.16.0")
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--filter-packages-list",
        str(pkg_list),
        "--filter-exclude-platform-tag",
        "win*",
    )
    assert rc == 0, f"sync failed:\n{out}\n{err}"
    pkg_dir = disk_server["data_dir"] / "packages" / PACKAGE
    files = sorted(p.name for p in pkg_dir.iterdir() if not p.name.startswith("."))
    assert any(f.endswith(".tar.gz") for f in files), (
        "the sdist must survive an exclusion-only filter"
    )


def test_only_wheels_and_only_sdists_conflict(disk_server, pypiron_bin, tmp_path):
    pkg_list = _packages_list(tmp_path, PACKAGE)
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--filter-packages-list",
        str(pkg_list),
        "--filter-only-wheels",
        "--filter-only-sdists",
        timeout=60,
    )
    assert rc != 0, "contradictory filters must fail, not silently mirror nothing"


def test_missing_destination_is_an_error(pypiron_bin, tmp_path):
    """Sync mirrors over HTTP; with no --to (and no [sync].to) it must refuse,
    not fall back to anything."""
    from .helpers import run_returncode

    pkg_list = _packages_list(tmp_path, PACKAGE)
    rc, out, err = run_returncode(
        [str(pypiron_bin), "sync", "--filter-packages-list", str(pkg_list)],
        timeout=60,
    )
    assert rc != 0
    assert "--to" in (out + err) or "destination" in (out + err)


def test_zero_concurrency_is_an_error(pypiron_bin, tmp_path):
    """A 0 concurrency means no work in flight (chunks(0) panics); refuse it at
    startup rather than silently coercing the typo to 1."""
    from .helpers import run_returncode

    pkg_list = _packages_list(tmp_path, PACKAGE)
    rc, out, err = run_returncode(
        [
            str(pypiron_bin),
            "sync",
            "--to",
            "http://127.0.0.1:1",
            "--filter-packages-list",
            str(pkg_list),
            "--concurrency",
            "0",
        ],
        timeout=60,
    )
    assert rc != 0
    assert "--concurrency must be at least 1" in (out + err)


def test_cli_packages_list_overrides_config_packages(disk_server, pypiron_bin, tmp_path):
    config = tmp_path / "pypiron.toml"
    config.write_text('[filter]\npackages = ["this-name-does-not-exist-xyz"]\n')
    pkg_list = tmp_path / "mine.txt"
    pkg_list.write_text(f"{PACKAGE}==1.16.0\n")

    # An explicit --filter-packages-list fully replaces the file's inline list; the
    # bogus config entry must not be attempted (it would fail the run).
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--config",
        str(config),
        "--filter-packages-list",
        str(pkg_list),
        "--filter-only-wheels",
    )
    assert rc == 0, f"sync failed:\n{out}\n{err}"
    assert _wheels(disk_server) == ["six-1.16.0-py2.py3-none-any.whl"]
    assert not (disk_server["data_dir"] / "packages" / "this-name-does-not-exist-xyz").exists()


def _files(server, pkg: str, *suffixes: str):
    pkg_dir = server["data_dir"] / "packages" / pkg
    if not pkg_dir.exists():
        return []
    return sorted(p.name for p in pkg_dir.iterdir() if p.name.endswith(suffixes))


def test_max_size_drops_oversize_artifacts(disk_server, pypiron_bin, tmp_path, tmp_path_factory):
    """--filter-max-size gates by the listing's PEP 700 size. A 1-byte ceiling
    drops every real artifact; a generous one keeps them."""
    pkg_list = _packages_list(tmp_path, f"{PACKAGE}==1.16.0")

    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--filter-packages-list",
        str(pkg_list),
        "--filter-max-size",
        "1",
    )
    assert rc == 0, f"sync failed:\n{out}\n{err}"
    assert _files(disk_server, PACKAGE, ".whl", ".tar.gz") == [], (
        "a 1-byte ceiling must drop every artifact"
    )

    # A fresh destination: a roomy ceiling (six wheels are ~11 KB) keeps them.
    with _extra_server(tmp_path_factory, pypiron_bin) as dest2:
        rc, out, err = sync_to(
            pypiron_bin,
            dest2,
            "--filter-packages-list",
            str(pkg_list),
            "--filter-only-wheels",
            "--filter-max-size",
            "250MB",
        )
        assert rc == 0, f"sync failed:\n{out}\n{err}"
        assert _files(dest2, PACKAGE, ".whl") == ["six-1.16.0-py2.py3-none-any.whl"]


def test_exclude_prereleases_keeps_only_stable(
    disk_server, pypiron_bin, tmp_path, tmp_path_factory
):
    """--filter-exclude-prereleases drops alpha/beta/rc/dev, keeping stable
    releases. The window pulls click 8.0.0's pre-releases plus its 8.0.0 final;
    after exclusion only the final survives."""
    window = "click>=8.0.0a1,<8.0.1"
    pkg_list = _packages_list(tmp_path, window)

    rc, out, err = sync_to(
        pypiron_bin, disk_server, "--filter-packages-list", str(pkg_list), "--filter-only-wheels"
    )
    assert rc == 0, f"sync failed:\n{out}\n{err}"
    baseline = _files(disk_server, "click", ".whl")
    assert "click-8.0.0-py3-none-any.whl" in baseline, baseline
    assert [w for w in baseline if w.split("-")[1] != "8.0.0"], (
        "precondition: the window must include pre-release wheels to filter"
    )

    with _extra_server(tmp_path_factory, pypiron_bin) as dest2:
        rc, out, err = sync_to(
            pypiron_bin,
            dest2,
            "--filter-packages-list",
            str(pkg_list),
            "--filter-only-wheels",
            "--filter-exclude-prereleases",
        )
        assert rc == 0, f"sync failed:\n{out}\n{err}"
        assert _files(dest2, "click", ".whl") == ["click-8.0.0-py3-none-any.whl"], (
            "only the stable 8.0.0 wheel may survive"
        )


def test_config_packages_list_resolves_relative_to_config(disk_server, pypiron_bin, tmp_path):
    cfgdir = tmp_path / "cfgdir"
    cfgdir.mkdir()
    (cfgdir / "pypiron.toml").write_text(
        '[filter]\npackages-list = "pkgs.txt"\nonly-wheels = true\n'
    )
    (cfgdir / "pkgs.txt").write_text(f"{PACKAGE}==1.16.0\n")

    # Run from a different cwd (tmp_path) — the relative path must resolve
    # against the config file's directory, not the process cwd.
    rc, out, err = sync_to(
        pypiron_bin,
        disk_server,
        "--config",
        str(cfgdir / "pypiron.toml"),
        cwd=tmp_path,
    )
    assert rc == 0, f"sync failed:\n{out}\n{err}"
    assert _wheels(disk_server) == ["six-1.16.0-py2.py3-none-any.whl"]
