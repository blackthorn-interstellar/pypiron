"""`--include-dependencies`: serve what the included packages need.

A source pypiron holds a small dependency graph. With only the top package
included, `sync` mirrors from it and the proxy serves it on demand; both must
reach the same projects and versions, and real uv installs through either.
Every extra of an included package and every platform the filters keep is
followed, from every wheel of a release, each dependency takes the versions
some requirement allows, and an included package keeps its own spec.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from .conftest import _start_disk_server
from .helpers import (
    http_get,
    make_wheel,
    run_checked,
    sync_to,
    upload_legacy,
    wait_for_file_in_index,
)

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


# What both sync and the proxy must reach with only `app` included.
REACHED = {
    "app": ["1.0"],
    "extra-dep": ["1.0"],
    # lib 2.0 wants leaf==1.0 and lib 3.0 wants leaf>=2; nothing wants 0.5.
    "leaf": ["1.0", "2.0"],
    "lib": ["2.0", "3.0"],
    "win-only": ["1.0"],
}


def _publish(server, dists: Path, name, version, requires, platform_tag="any") -> None:
    extra = "".join(f"Requires-Dist: {r}\n" for r in requires)
    wheel = make_wheel(name, version, dists, metadata_extra=extra, platform_tag=platform_tag)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["admin_user"],
        password=server["admin_password"],
        fields={"mirror": "true"},
    )
    wait_for_file_in_index(server["simple"], name, wheel.name)


def _seed(server, dists: Path) -> None:
    for name, version, requires in GRAPH:
        _publish(server, dists, name, version, requires)


@pytest.fixture()
def source(tmp_path_factory, pypiron_bin, tmp_path):
    gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    server = next(gen)
    _seed(server, tmp_path / "dists")
    try:
        yield server
    finally:
        gen.close()


def _install(uv_path, simple: str, venv: Path, req: str) -> None:
    run_checked([uv_path, "venv", str(venv)])
    run_checked(
        [
            uv_path,
            "pip",
            "install",
            "--python",
            str(venv),
            "--default-index",
            simple,
            "--no-cache",
            req,
        ],
        timeout=120,
    )


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

    assert _mirrored(disk_server) == REACHED
    assert "gone-upstream: required by a mirrored package but not found" in out, out

    # The destination alone satisfies a real install, extras included.
    _install(uv_path, disk_server["simple"], tmp_path / "venv", "app[x]")

    # The memo lets the next run start from the whole closure: one round, and
    # nothing left to copy.
    cursors = json.loads((disk_server["data_dir"] / "_sync/cursors.json").read_text())
    assert ["lib>=2"] in [r[:1] for r in cursors["app"]["deps"]["metadata"]["1.0"].values()]
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


def test_full_resets_dependencies_a_narrowed_selection_dropped(
    source, disk_server, pypiron_bin, tmp_path
):
    """The memo keeps what deselected releases required, so a narrowed selection
    keeps following their dependencies; `--full` is the documented reset, and
    its save must not merge the old requirements back."""
    _sync(pypiron_bin, source, disk_server, "--include-package", "lib")
    assert _mirrored(disk_server)["leaf"] == ["1.0", "2.0"]

    # lib 1.0 requires nothing, so after the reset nothing leads to leaf.
    _sync(pypiron_bin, source, disk_server, "--include-package", "lib==1.0", "--full")
    wheel = make_wheel("leaf", "3.0", tmp_path / "late")
    upload_legacy(
        source["legacy"],
        wheel,
        username=source["admin_user"],
        password=source["admin_password"],
        fields={"mirror": "true"},
    )
    wait_for_file_in_index(source["simple"], "leaf", wheel.name)
    out = _sync(pypiron_bin, source, disk_server, "--include-package", "lib==1.0")
    assert "3.0" not in _mirrored(disk_server)["leaf"], out


# A release whose wheels disagree, as torch's do: only the Linux wheel needs
# linux-dep, only the Windows one win-dep. Markers name what can't apply on a
# Linux mirror above Python 3.9.
PLATFORM_WHEELS = [
    (
        "manylinux_2_17_x86_64",
        ["linux-dep", 'mac-only; sys_platform == "darwin"', 'old-py; python_version < "3.8"'],
    ),
    ("win_amd64", ["win-dep"]),
]
PLATFORM_DEPS = ["linux-dep", "mac-only", "old-py", "win-dep"]
LINUX_ONLY = ["--exclude-platform-tag", "win*,macosx_*", "--exclude-python-below", "3.9"]


@pytest.fixture()
def platform_source(source, tmp_path):
    for platform, requires in PLATFORM_WHEELS:
        _publish(source, tmp_path / "multi", "multi", "1.0", requires, platform)
    for name in PLATFORM_DEPS:
        _publish(source, tmp_path / "multi", name, "1.0", [])
    return source


def test_every_wheel_of_a_release_is_read(platform_source, disk_server, pypiron_bin):
    _sync(pypiron_bin, platform_source, disk_server, "--include-package", "multi")
    mirrored = _mirrored(disk_server)
    assert {name: mirrored.get(name) for name in PLATFORM_DEPS} == dict.fromkeys(
        PLATFORM_DEPS, ["1.0"]
    )


def test_filters_drop_dependencies_only_other_targets_need(
    platform_source, disk_server, pypiron_bin
):
    _sync(pypiron_bin, platform_source, disk_server, "--include-package", "multi", *LINUX_ONLY)
    mirrored = _mirrored(disk_server)
    assert {name for name in PLATFORM_DEPS if name in mirrored} == {"linux-dep"}, mirrored


def test_proxy_drops_dependencies_only_other_targets_need(
    platform_source, tmp_path_factory, pypiron_bin
):
    gen = _proxy(
        tmp_path_factory, pypiron_bin, platform_source, "--include-package", "multi", *LINUX_ONLY
    )
    proxy = next(gen)
    try:
        assert _served(proxy, "linux-dep") == ["1.0"]
        assert [_served(proxy, n) for n in ("mac-only", "old-py", "win-dep")] == [None] * 3
    finally:
        gen.close()


def _served(server, name: str) -> list[str] | None:
    """The versions a server lists for `name`, or None when it 404s."""
    status, body, _ = http_get(
        f"{server['simple']}{name}/",
        headers={"Accept": "application/vnd.pypi.simple.v1+json"},
        timeout=60,
    )
    if status == 404:
        return None
    assert status == 200, (status, body)
    files = json.loads(body)["files"]
    return sorted({f["filename"].split("-")[1] for f in files})


def _proxy(tmp_path_factory, pypiron_bin, source, *args):
    # The source is a loopback pypiron on plain http; no MITM on 127.0.0.1.
    return _start_disk_server(
        tmp_path_factory,
        pypiron_bin,
        extra_args=[
            "--proxy-upstream",
            source["base_url"],
            "--allow-insecure-upstream",
            "--exclude-newer",
            "",
            "--include-dependencies",
            *args,
        ],
    )


def test_proxy_serves_the_same_dependencies_as_sync(
    source, tmp_path_factory, pypiron_bin, tmp_path, uv_path
):
    gen = _proxy(tmp_path_factory, pypiron_bin, source, "--include-package", "app")
    proxy = next(gen)
    try:
        _install(uv_path, proxy["simple"], tmp_path / "venv", "app[x]")

        names = sorted({name for name, _, _ in GRAPH} | {"gone-upstream"})
        served = {name: _served(proxy, name) for name in names}
        assert {k: v for k, v in served.items() if v} == REACHED, served
        assert served["unrelated"] is None

        # What was read is saved, so a restart reads only new releases.
        saved = json.loads((proxy["data_dir"] / "_state/proxy-dependencies.json").read_text())
        read = saved["projects"]["app"]["metadata"]["1.0"].values()
        assert ["lib>=2"] in [r[:1] for r in read], saved
    finally:
        gen.close()


def test_proxy_included_package_keeps_its_own_spec(source, tmp_path_factory, pypiron_bin):
    gen = _proxy(
        tmp_path_factory,
        pypiron_bin,
        source,
        "--include-package",
        "app",
        "--include-package",
        "leaf==2.0",
        "--exclude-package",
        "win-only",
    )
    proxy = next(gen)
    try:
        assert _served(proxy, "app") == ["1.0"]
        assert _served(proxy, "lib") == ["2.0", "3.0"]
        # lib 2.0 asks for leaf==1.0, but the included leaf==2.0 is the approval.
        assert _served(proxy, "leaf") == ["2.0"]
        assert _served(proxy, "win-only") is None
        assert _served(proxy, "unrelated") is None
    finally:
        gen.close()
