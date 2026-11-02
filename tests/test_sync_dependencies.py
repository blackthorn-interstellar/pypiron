"""`sync --include-dependencies`: mirror what the listed packages need.

A source pypiron holds a small dependency graph; the destination is synced from
it with only the top package listed, then real uv installs from the destination
alone. Every extra and every platform is followed, each dependency takes the
versions some requirement allows, and a listed package keeps its own spec.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from .conftest import _start_disk_server
from .helpers import make_wheel, run_checked, sync_to, upload_legacy, wait_for_file_in_index

pytestmark = pytest.mark.integration

# (name, version, Requires-Dist lines)
GRAPH = [
    (
        "app",
        "1.0",
        [
            "lib>=2",
            'extra-dep; extra == "x"',
            'win-only; sys_platform == "win32"',
            # Deleted upstream long ago: reported, never fatal.
            'gone-upstream; python_version < "3"',
        ],
    ),
    ("lib", "1.0", []),
    ("lib", "2.0", ["leaf==1.0"]),
    ("lib", "3.0", ["leaf>=2"]),
    ("leaf", "0.5", []),
    ("leaf", "1.0", []),
    ("leaf", "2.0", []),
    ("extra-dep", "1.0", []),
    ("win-only", "1.0", []),
    ("unrelated", "1.0", []),
]


@pytest.fixture()
def source(tmp_path_factory, pypiron_bin, tmp_path):
    gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    server = next(gen)
    for name, version, requires in GRAPH:
        extra = "".join(f"Requires-Dist: {r}\n" for r in requires)
        wheel = make_wheel(name, version, tmp_path / "dists", metadata_extra=extra)
        upload_legacy(
            server["legacy"],
            wheel,
            username=server["admin_user"],
            password=server["admin_password"],
            fields={"mirror": "true"},
        )
        wait_for_file_in_index(server["simple"], name, wheel.name)
    try:
        yield server
    finally:
        gen.close()


def _sync(pypiron_bin, source, dest, *extra):
    rc, out, err = sync_to(
        pypiron_bin,
        dest,
        "--include-dependencies",
        "--exclude-newer",
        "",
        "--advisory-feed",
        "",
        *extra,
        source=source["base_url"],
        env={**os.environ, "RUST_LOG": "info,pypiron=info"},
    )
    assert rc == 0, f"sync failed:\n{out}\n{err}"
    return out + err


def _mirrored(dest) -> dict[str, list[str]]:
    root: Path = dest["data_dir"] / "packages"
    return {
        pkg.name: sorted(f.name.split("-")[1] for f in pkg.iterdir() if f.name.endswith(".whl"))
        for pkg in sorted(root.iterdir())
        if any(f.name.endswith(".whl") for f in pkg.iterdir())
    }


def test_dependencies_follow_every_extra_platform_and_allowed_version(
    source, disk_server, pypiron_bin, tmp_path, uv_path
):
    out = _sync(pypiron_bin, source, disk_server, "--include-package", "app")

    assert _mirrored(disk_server) == {
        "app": ["1.0"],
        "extra-dep": ["1.0"],
        # lib 2.0 wants leaf==1.0 and lib 3.0 wants leaf>=2; nothing wants 0.5.
        "leaf": ["1.0", "2.0"],
        "lib": ["2.0", "3.0"],
        "win-only": ["1.0"],
    }
    assert "gone-upstream: required by a mirrored package but not found" in out, out

    # The destination alone satisfies a real install, extras included.
    venv = tmp_path / "venv"
    run_checked([uv_path, "venv", str(venv)])
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(venv),
            "--default-index",
            disk_server["simple"],
            "--no-cache",
            "app[x]",
        ],
        timeout=120,
    )

    # The memo lets the next run start from the whole closure: one round, and
    # nothing left to copy.
    cursors = json.loads((disk_server["data_dir"] / "_sync/cursors.json").read_text())
    assert "lib>=2" in cursors["app"]["deps"]["requires"]
    again = _sync(pypiron_bin, source, disk_server, "--include-package", "app")
    assert "following dependencies" not in again, again
    assert "· 0 files ·" in again, again


def test_listed_package_keeps_its_own_spec(source, disk_server, pypiron_bin):
    _sync(
        pypiron_bin,
        source,
        disk_server,
        "--include-package",
        "app",
        "--include-package",
        "leaf==2.0",
        "--exclude-package",
        "win-only",
    )
    mirrored = _mirrored(disk_server)
    # lib 2.0 asks for leaf==1.0, but the listed leaf==2.0 is the approval.
    assert mirrored["leaf"] == ["2.0"]
    assert "win-only" not in mirrored
    assert "unrelated" not in mirrored
