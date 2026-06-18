"""Sync filtering: PEP 440 specifiers, upload-time bounds, pypiron.toml.

All filters gate what a run adds — never what is already mirrored. These run
the real binary in direct-storage mode against a tmpdir; the storage layout
is the contract, so tree assertions are blackbox assertions.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from .helpers import run_checked, run_returncode

PACKAGE = "six"

pytestmark = pytest.mark.integration


def _wheels(data_dir):
    pkg_dir = data_dir / "packages" / PACKAGE
    if not pkg_dir.exists():
        return []
    return sorted(p.name for p in pkg_dir.iterdir() if p.name.endswith(".whl"))


def test_version_specifiers_limit_releases(pypiron_bin, tmp_path):
    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}>=1.15,<1.17\n")
    data_dir = tmp_path / "data"

    run_checked(
        [
            str(pypiron_bin),
            "sync",
            "--packages-list",
            str(pkg_list),
            "--data-dir",
            str(data_dir),
            "--only-wheels",
        ],
        timeout=600,
    )

    wheels = _wheels(data_dir)
    assert wheels, "constraint should still match something"
    for name in wheels:
        version = name.split("-")[1]
        assert version in ("1.15.0", "1.16.0"), f"{name} violates >=1.15,<1.17"


def test_exclude_newer_bounds_mirroring(pypiron_bin, tmp_path):
    cutoff = "2016-01-01T00:00:00Z"
    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}\n")
    data_dir = tmp_path / "data"

    run_checked(
        [
            str(pypiron_bin),
            "sync",
            "--packages-list",
            str(pkg_list),
            "--data-dir",
            str(data_dir),
            "--only-wheels",
            "--exclude-newer",
            cutoff,
        ],
        timeout=600,
    )

    wheels = _wheels(data_dir)
    assert wheels, "six has pre-2016 wheels"
    cutoff_dt = datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
    pkg_dir = data_dir / "packages" / PACKAGE
    for name in wheels:
        sc = json.loads((pkg_dir / f"{name}.meta.json").read_text())
        uploaded = datetime.fromisoformat(sc["upload-time"].replace("Z", "+00:00"))
        assert uploaded < cutoff_dt, f"{name} uploaded {uploaded}, after the cutoff"


def test_filters_never_remove_mirrored_files(pypiron_bin, tmp_path):
    data_dir = tmp_path / "data"
    pkg_list = tmp_path / "packages.txt"

    def sync(spec, *extra):
        pkg_list.write_text(f"{spec}\n")
        run_checked(
            [
                str(pypiron_bin),
                "sync",
                "--packages-list",
                str(pkg_list),
                "--data-dir",
                str(data_dir),
                "--only-wheels",
                *extra,
            ],
            timeout=600,
        )

    sync(f"{PACKAGE}==1.16.0")
    assert _wheels(data_dir) == ["six-1.16.0-py2.py3-none-any.whl"]

    sync(f"{PACKAGE}==1.17.0")
    assert _wheels(data_dir) == [
        "six-1.16.0-py2.py3-none-any.whl",
        "six-1.17.0-py2.py3-none-any.whl",
    ], "a narrower later run must only add, never remove"

    # A filter matching nothing changes nothing.
    sync(PACKAGE, "--exclude-newer", "2001-01-01T00:00:00Z")
    assert len(_wheels(data_dir)) == 2


def test_toml_config_with_cli_precedence(pypiron_bin, tmp_path):
    config = tmp_path / "pypiron.toml"
    config.write_text(
        f"""
[sync]
packages = ["{PACKAGE}==1.16.0"]
only-wheels = true
exclude-newer = "2030-01-01T00:00:00Z"
"""
    )

    # Config alone drives the run: inline packages, only-wheels, permissive cutoff.
    data_dir = tmp_path / "from-toml"
    run_checked(
        [
            str(pypiron_bin),
            "sync",
            "--config",
            str(config),
            "--data-dir",
            str(data_dir),
        ],
        timeout=600,
    )
    assert _wheels(data_dir) == ["six-1.16.0-py2.py3-none-any.whl"]

    # CLI overrides the file: an impossible cutoff means nothing is mirrored.
    data_dir2 = tmp_path / "cli-wins"
    run_checked(
        [
            str(pypiron_bin),
            "sync",
            "--config",
            str(config),
            "--data-dir",
            str(data_dir2),
            "--exclude-newer",
            "2001-01-01T00:00:00Z",
        ],
        timeout=600,
    )
    assert _wheels(data_dir2) == []

    # Typos in the config are hard errors, not silent no-ops.
    bad = tmp_path / "bad.toml"
    bad.write_text("[sync]\nonly-weels = true\n")
    rc, out, err = run_returncode(
        [str(pypiron_bin), "sync", "--config", str(bad), "--data-dir", str(tmp_path / "x")],
        timeout=60,
    )
    assert rc != 0
    assert "only-weels" in (out + err)


def test_exclude_only_platform_tag_keeps_sdists(pypiron_bin, tmp_path):
    """--exclude-platform-tag must not silently drop sdists (they have no tag)."""
    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}==1.16.0\n")
    data_dir = tmp_path / "data"
    run_checked(
        [
            str(pypiron_bin),
            "sync",
            "--packages-list",
            str(pkg_list),
            "--data-dir",
            str(data_dir),
            "--exclude-platform-tag",
            "win*",
        ],
        timeout=600,
    )
    pkg_dir = data_dir / "packages" / PACKAGE
    files = sorted(p.name for p in pkg_dir.iterdir() if not p.name.startswith("."))
    assert any(f.endswith(".tar.gz") for f in files), (
        "the sdist must survive an exclusion-only filter"
    )


def test_only_wheels_and_only_sdists_conflict(pypiron_bin, tmp_path):
    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PACKAGE}\n")
    rc, out, err = run_returncode(
        [
            str(pypiron_bin),
            "sync",
            "--packages-list",
            str(pkg_list),
            "--data-dir",
            str(tmp_path / "data"),
            "--only-wheels",
            "--only-sdists",
        ],
        timeout=60,
    )
    assert rc != 0, "contradictory filters must fail, not silently mirror nothing"


def test_cli_packages_list_overrides_config_packages(pypiron_bin, tmp_path):
    config = tmp_path / "pypiron.toml"
    config.write_text('[sync]\npackages = ["this-name-does-not-exist-xyz"]\n')
    pkg_list = tmp_path / "mine.txt"
    pkg_list.write_text(f"{PACKAGE}==1.16.0\n")
    data_dir = tmp_path / "data"

    # An explicit --packages-list fully replaces the file's inline list; the
    # bogus config entry must not be attempted (it would fail the run).
    run_checked(
        [
            str(pypiron_bin),
            "sync",
            "--config",
            str(config),
            "--packages-list",
            str(pkg_list),
            "--data-dir",
            str(data_dir),
            "--only-wheels",
        ],
        timeout=600,
    )
    assert _wheels(data_dir) == ["six-1.16.0-py2.py3-none-any.whl"]
    assert not (data_dir / "packages" / "this-name-does-not-exist-xyz").exists()


def test_config_packages_list_resolves_relative_to_config(pypiron_bin, tmp_path):
    cfgdir = tmp_path / "cfgdir"
    cfgdir.mkdir()
    (cfgdir / "pypiron.toml").write_text('[sync]\npackages-list = "pkgs.txt"\nonly-wheels = true\n')
    (cfgdir / "pkgs.txt").write_text(f"{PACKAGE}==1.16.0\n")
    data_dir = tmp_path / "data"

    # Run from a different cwd (tmp_path) — the relative path must resolve
    # against the config file's directory, not the process cwd.
    run_checked(
        [
            str(pypiron_bin),
            "sync",
            "--config",
            str(cfgdir / "pypiron.toml"),
            "--data-dir",
            str(data_dir),
        ],
        cwd=tmp_path,
        timeout=600,
    )
    assert _wheels(data_dir) == ["six-1.16.0-py2.py3-none-any.whl"]
