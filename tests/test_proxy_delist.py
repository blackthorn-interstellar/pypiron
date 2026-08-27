"""Denylist delisting (Codex finding F12).

An `--exclude-package` entry delists a package from the indexes installers
resolve against — even a package already cached — instead of only gating fresh
upstream fetches. The bytes are delisted, not deleted: they stay fetchable by
direct `/files/` URL until removed. Unblocking relists the package with no
upstream re-download, and a denylist change is rebuilt into the affected
listings on restart (the denylist is startup config).

These drive the real binary: a package is cached through a proxy, the proxy is
restarted with the denylist changed, and the resulting indexes are inspected
both over HTTP and as the materialized files on disk.
"""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import time

import pytest

from .conftest import _DISK_SERVER_CREDS, _start_disk_server
from .helpers import (
    ACCEPT_PEP691,
    find_free_port,
    get_index_json,
    http_get,
    kill_process_tree,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
    wait_http_responding,
)

pytestmark = pytest.mark.integration


def _upload(server, dist, package):
    upload_legacy(server["legacy"], dist, username=server["user"], password=server["password"])
    wait_for_file_in_index(server["simple"], package, dist.name)


@contextlib.contextmanager
def _proxy(pypiron_bin, data_dir, upstream_base_url, *extra_args, env_extra=None):
    """(Re)start a proxy over `data_dir`, proxying `upstream_base_url`, with any
    extra flags (e.g. --exclude-package) and env overrides (e.g. an env-set
    exclude). The global /simple/ index is served from local storage, so the
    readiness probe works even when upstream is down."""
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    args = [
        str(pypiron_bin),
        "serve",
        "--bind-addr",
        bind,
        "--data-dir",
        str(data_dir),
        *_DISK_SERVER_CREDS["full"]["args"],
        "--worker-interval-secs",
        "1",
        "--proxy-upstream",
        upstream_base_url,
        "--allow-insecure-upstream",
        # No cooldown: cache a just-uploaded release without waiting out the
        # default 7-day quarantine.
        "--exclude-newer",
        "",
        *extra_args,
    ]
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info,pypiron=debug")
    env.setdefault("PYPIRON_ADVISORY_FEED", "")
    if env_extra:
        env.update(env_extra)
    log_file = open(data_dir.parent / f"{data_dir.name}-proxy.log", "w")
    proc = subprocess.Popen(args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    try:
        wait_http_responding(f"http://{bind}/simple/index.json", timeout=20.0)
        yield {
            "proc": proc,
            "base_url": f"http://{bind}",
            "simple": f"http://{bind}/simple/",
            "data_dir": data_dir,
            **_DISK_SERVER_CREDS["full"]["extra"],
        }
    finally:
        kill_process_tree(proc)
        log_file.close()


def _poll(predicate, *, timeout=25.0, interval=0.25):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _global_names(simple_url):
    return {p["name"] for p in get_index_json(simple_url)["projects"]}


def _index_filenames(data_dir, package):
    """Filenames in the materialized local `simple/<pkg>/index.json`, or None if
    the file has been deleted (a fully-delisted package)."""
    path = data_dir / "simple" / package / "index.json"
    if not path.exists():
        return None
    return {f["filename"] for f in json.loads(path.read_text())["files"]}


def _global_names_file(data_dir):
    """Names in the materialized global `simple/index.json` (read off disk, no
    server needed — how an offline maintenance command's output is inspected)."""
    path = data_dir / "simple" / "index.json"
    if not path.exists():
        return set()
    return {p["name"] for p in json.loads(path.read_text())["projects"]}


def _write_maintenance_config(path, data_dir, excludes):
    """A pypiron.toml the offline commands read: `[serve]` points them at the same
    disk store, `[mirror].exclude-packages` is the denylist they must honor."""
    excl = ", ".join(f'"{e}"' for e in excludes)
    path.write_text(f'[serve]\ndata-dir = "{data_dir}"\n\n[mirror]\nexclude-packages = [{excl}]\n')


def _run_pypiron(pypiron_bin, *args, timeout=90):
    return subprocess.run(
        [str(pypiron_bin), *args], capture_output=True, text=True, timeout=timeout
    )


def test_rebuild_index_preserves_delisting(tmp_path_factory, pypiron_bin, tmp_path):
    """Codex B1: `rebuild-index` must honor `--exclude-package` from config, or it
    re-materializes a delisted package's index from truth and durably un-delists
    it. A delisted-then-rebuilt package must stay gone — from its own index, the
    global list, and a subsequent serve boot with the exclude still set."""
    upstream_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    upstream = next(upstream_gen)
    data_dir = tmp_path_factory.mktemp("pypiron-rebuild-proxy")
    config = tmp_path / "pypiron.toml"
    _write_maintenance_config(config, data_dir, ["delistme"])
    wheel = make_wheel("delistme", "1.0", tmp_path)
    try:
        _upload(upstream, wheel, "delistme")

        # Cache it through an open proxy (no exclude yet), then delist it via a
        # serve restart with the exclude set.
        with _proxy(pypiron_bin, data_dir, upstream["base_url"]) as proxy:
            assert http_get(f"{proxy['base_url']}/files/delistme/{wheel.name}")[0] == 200
            assert _poll(lambda: _index_filenames(data_dir, "delistme") == {wheel.name})
        with _proxy(pypiron_bin, data_dir, upstream["base_url"], "--exclude-package", "delistme"):
            assert _poll(lambda: _index_filenames(data_dir, "delistme") is None)
            # The tick deletes the per-package index before its one batched
            # global-index rewrite, so the global file must be polled too —
            # asserting it after killing the server races that second write.
            assert _poll(lambda: "delistme" not in _global_names_file(data_dir))

        # rebuild-index over the same store, with the exclude in config, must
        # PRESERVE the delisting — not re-materialize the index from truth.
        cp = _run_pypiron(pypiron_bin, "rebuild-index", "--config", str(config))
        assert cp.returncode == 0, cp.stdout + cp.stderr
        assert _index_filenames(data_dir, "delistme") is None, (
            "rebuild-index re-materialized a delisted package's index"
        )
        assert "delistme" not in _global_names_file(data_dir), (
            "rebuild-index put a delisted package back in the global list"
        )

        # And the delisting survives a serve boot with the exclude still set
        # (the enforced-excludes stamp rebuild-index left is consistent, so the
        # reconcile doesn't conclude the config changed and re-list it).
        with _proxy(
            pypiron_bin, data_dir, upstream["base_url"], "--exclude-package", "delistme"
        ) as proxy:
            code, _, _ = http_get(
                f"{proxy['simple']}delistme/index.json", headers={"Accept": ACCEPT_PEP691}
            )
            assert code == 404, "a delisted package re-listed by rebuild-index across a restart"
            assert "delistme" not in _global_names(proxy["simple"])
    finally:
        upstream_gen.close()


def test_verify_index_clean_after_delist(tmp_path_factory, pypiron_bin, tmp_path):
    """Codex B2: `verify-index` must model the denylist, or it reports permanent
    false divergences (missing-view + stale-global-index, exit 1) for every
    delisted-but-cached package. With the exclude in config it must read clean
    (exit 0): a fully-denied cached package SHOULD have no index and no global
    entry."""
    upstream_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    upstream = next(upstream_gen)
    data_dir = tmp_path_factory.mktemp("pypiron-verify-proxy")
    config = tmp_path / "pypiron.toml"
    _write_maintenance_config(config, data_dir, ["delistme"])
    wheel = make_wheel("delistme", "1.0", tmp_path)
    try:
        _upload(upstream, wheel, "delistme")

        with _proxy(pypiron_bin, data_dir, upstream["base_url"]) as proxy:
            assert http_get(f"{proxy['base_url']}/files/delistme/{wheel.name}")[0] == 200
            assert _poll(lambda: _index_filenames(data_dir, "delistme") == {wheel.name})
        with _proxy(pypiron_bin, data_dir, upstream["base_url"], "--exclude-package", "delistme"):
            assert _poll(lambda: _index_filenames(data_dir, "delistme") is None)

        # The oracle models delisting: the cached-but-delisted package is not a
        # divergence, so the store reads clean (exit 0).
        cp = _run_pypiron(pypiron_bin, "verify-index", "--config", str(config))
        assert cp.returncode == 0, (
            f"verify-index flagged a correctly delisted package:\n{cp.stdout}{cp.stderr}"
        )
    finally:
        upstream_gen.close()


def test_excluding_a_cached_package_delists_it(tmp_path_factory, pypiron_bin, tmp_path):
    """F12: a package cached through the proxy, then added to --exclude-package,
    disappears from /simple/<pkg>/ and the global /simple/ so a resolver can't
    find it — while its bytes stay fetchable by direct /files/ URL. Unblocking
    relists it with no upstream re-fetch."""
    upstream_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    upstream = next(upstream_gen)
    data_dir = tmp_path_factory.mktemp("pypiron-delist-proxy")
    wheel = make_wheel("delistme", "1.0", tmp_path)
    try:
        _upload(upstream, wheel, "delistme")

        # (1) Cache it through an open proxy: the file GET downloads+commits the
        # bytes and marks the package dirty, so the worker materializes its
        # local index and adds it to the global name list.
        with _proxy(pypiron_bin, data_dir, upstream["base_url"]) as proxy:
            assert http_get(f"{proxy['base_url']}/files/delistme/{wheel.name}")[0] == 200
            assert _poll(lambda: _index_filenames(data_dir, "delistme") == {wheel.name}), (
                "proxy never materialized the cached package's local index"
            )
            assert "delistme" in _global_names(proxy["simple"])

        # (2) Restart with the package on the denylist. The startup reconcile
        # delists it: the per-package index is deleted, the name leaves the
        # global list, and a resolver 404s where it used to resolve.
        with _proxy(
            pypiron_bin, data_dir, upstream["base_url"], "--exclude-package", "delistme"
        ) as proxy:
            assert _poll(lambda: _index_filenames(data_dir, "delistme") is None), (
                "excluding a cached package must delete its local /simple/ index"
            )
            code, _, _ = http_get(
                f"{proxy['simple']}delistme/index.json", headers={"Accept": ACCEPT_PEP691}
            )
            assert code == 404, "a delisted package must 404 like a name pypiron does not hold"
            assert "delistme" not in _global_names(proxy["simple"]), (
                "a delisted package must drop out of the global /simple/ name list"
            )
            assert http_get(f"{proxy['base_url']}/project/delistme/")[0] == 404, (
                "a delisted package must lose its human /project/ page too"
            )

            # (3) Delist is not delete: the bytes remain fetchable by direct URL
            # (the accepted, documented residual — the /files/ route is unchanged).
            assert http_get(f"{proxy['base_url']}/files/delistme/{wheel.name}")[0] == 200, (
                "the stored bytes must stay fetchable by direct /files/ URL"
            )

        # (4) Kill upstream, then unblock. Relisting must rebuild from the bytes
        # already on disk — no upstream re-download — so it works with upstream
        # dead and the name reappears in the global list.
        kill_process_tree(upstream["proc"])
        with _proxy(pypiron_bin, data_dir, upstream["base_url"]) as proxy:
            assert _poll(lambda: "delistme" in _global_names(proxy["simple"])), (
                "unblocking must relist the package with no upstream re-fetch"
            )
            assert _index_filenames(data_dir, "delistme") == {wheel.name}
            assert http_get(f"{proxy['base_url']}/files/delistme/{wheel.name}")[0] == 200
    finally:
        upstream_gen.close()


def test_version_pinned_exclude_drops_only_that_version_from_the_cached_index(
    tmp_path_factory, pypiron_bin, tmp_path
):
    """A version-pinned denial (`pinned<2.0`) drops just the matching versions
    from a cached package's local index, leaving the rest. Upstream is killed
    before the restart so the per-package page is served from the local index —
    exercising the rebuild filter, not the upstream renderer."""
    upstream_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    upstream = next(upstream_gen)
    data_dir = tmp_path_factory.mktemp("pypiron-pin-proxy")
    old = make_wheel("pinnedpkg", "1.0", tmp_path)
    new = make_wheel("pinnedpkg", "2.0", tmp_path)
    try:
        _upload(upstream, old, "pinnedpkg")
        _upload(upstream, new, "pinnedpkg")

        # Cache both releases through an open proxy.
        with _proxy(pypiron_bin, data_dir, upstream["base_url"]) as proxy:
            assert http_get(f"{proxy['base_url']}/files/pinnedpkg/{old.name}")[0] == 200
            assert http_get(f"{proxy['base_url']}/files/pinnedpkg/{new.name}")[0] == 200
            assert _poll(lambda: _index_filenames(data_dir, "pinnedpkg") == {old.name, new.name}), (
                "proxy never cached both releases into the local index"
            )

        # Kill upstream so the per-package page can only come from the local
        # index, then restart with the version-pinned exclude.
        kill_process_tree(upstream["proc"])
        with _proxy(
            pypiron_bin, data_dir, upstream["base_url"], "--exclude-package", "pinnedpkg<2.0"
        ) as proxy:
            assert _poll(lambda: _index_filenames(data_dir, "pinnedpkg") == {new.name}), (
                "a version-pinned exclude must drop only the matching release from the index"
            )
            data = get_index_json(proxy["simple"], "pinnedpkg")
            filenames = {f["filename"] for f in data["files"]}
            assert new.name in filenames
            assert old.name not in filenames, "the pinned-out release must not list"

            # Delist is not delete for the pinned-out file either: both sets of
            # bytes stay fetchable by direct URL (the residual).
            assert http_get(f"{proxy['base_url']}/files/pinnedpkg/{new.name}")[0] == 200
            assert http_get(f"{proxy['base_url']}/files/pinnedpkg/{old.name}")[0] == 200
    finally:
        upstream_gen.close()


def test_maintenance_honors_an_env_set_exclude(tmp_path_factory, pypiron_bin, tmp_path):
    """Nit 1: the exclude is set only via PYPIRON_EXCLUDE_PACKAGE on serve — never
    in a config file or on the maintenance command line. verify-index and
    rebuild-index must still honor it (they read the enforced-excludes stamp serve
    wrote), so they agree with the running server whatever channel set the exclude.
    Before the fix, maintenance resolved excludes from config only and would flag
    false divergences / re-list the package."""
    upstream_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    upstream = next(upstream_gen)
    data_dir = tmp_path_factory.mktemp("pypiron-envexclude-proxy")
    wheel = make_wheel("delistme", "1.0", tmp_path)
    try:
        _upload(upstream, wheel, "delistme")

        # Cache it (no exclude), then delist via a serve whose ONLY exclude source
        # is the env var — no --exclude-package flag, no config file.
        with _proxy(pypiron_bin, data_dir, upstream["base_url"]) as proxy:
            assert http_get(f"{proxy['base_url']}/files/delistme/{wheel.name}")[0] == 200
            assert _poll(lambda: _index_filenames(data_dir, "delistme") == {wheel.name})
        with _proxy(
            pypiron_bin,
            data_dir,
            upstream["base_url"],
            env_extra={"PYPIRON_EXCLUDE_PACKAGE": "delistme"},
        ):
            assert _poll(lambda: _index_filenames(data_dir, "delistme") is None)
            # Poll before killing the server: the global rewrite lands after the
            # per-package deletion within the tick.
            assert _poll(lambda: "delistme" not in _global_names_file(data_dir))

        # verify-index with NO config and NO env exclude reads the stamp → clean.
        cp = _run_pypiron(pypiron_bin, "verify-index", "--data-dir", str(data_dir))
        assert cp.returncode == 0, (
            f"verify-index ignored the env-set exclude serve enforced:\n{cp.stdout}{cp.stderr}"
        )

        # rebuild-index likewise reads the stamp and PRESERVES the delisting.
        cp = _run_pypiron(pypiron_bin, "rebuild-index", "--data-dir", str(data_dir))
        assert cp.returncode == 0, cp.stdout + cp.stderr
        assert _index_filenames(data_dir, "delistme") is None, (
            "rebuild-index re-listed a package the env-set exclude delisted"
        )
        assert "delistme" not in _global_names_file(data_dir)
    finally:
        upstream_gen.close()
