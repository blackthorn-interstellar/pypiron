"""Milestone 5: the reconciler is the self-heal backbone.

A lost dirty marker must be harmless: files written straight to storage with
no marker (the end state of "marker deleted mid-flight") get indexed by the
periodic sweep, and stale index entries pointing at deleted files get pruned.
The sweep also owns the two halves of the global index: they are written
separately, HTML first, so a crash between them tears the pair — and the sweep
must close that tear on a store with no churn left to drive it.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time

import pytest

from .conftest import _DISK_SERVER_CREDS
from .helpers import (
    ACCEPT_PEP691,
    download_pypi_wheel,
    find_free_port,
    get_index_json,
    http_get,
    kill_process_tree,
    sha256_file,
    upload_legacy,
    wait_for_file_in_index,
    wait_for_project_in_global,
    wait_http_responding,
)

PACKAGE = "six"
OLD_VERSION = "1.16.0"
NEW_VERSION = "1.17.0"

pytestmark = pytest.mark.integration


def test_lost_marker_is_harmless(disk_server_fast_reconcile, tmp_path):
    server = disk_server_fast_reconcile

    # Artifact dropped straight into the truth tree: no upload, no sidecar,
    # no dirty marker. Exactly what remains if a marker is lost mid-flight.
    wheel_path = download_pypi_wheel(PACKAGE, OLD_VERSION, tmp_path)
    pkg_dir = server["data_dir"] / "packages" / PACKAGE
    pkg_dir.mkdir(parents=True)
    (pkg_dir / wheel_path.name).write_bytes(wheel_path.read_bytes())

    # The sweep indexes it and backfills the sidecar without any event.
    index = wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name, timeout=15.0)
    (entry,) = [f for f in index["files"] if f["filename"] == wheel_path.name]
    assert entry["hashes"]["sha256"] == sha256_file(wheel_path)
    assert (pkg_dir / f"{wheel_path.name}.meta.json").exists()

    # The global index is a separate view written after the package index, so
    # poll for it rather than racing the sweep with an instant assert.
    wait_for_project_in_global(server["simple"], PACKAGE, timeout=15.0)
    global_idx = get_index_json(server["simple"])
    assert PACKAGE in [p["name"] for p in global_idx["projects"]]


def test_reconcile_prunes_stale_views(disk_server_fast_reconcile, tmp_path):
    server = disk_server_fast_reconcile
    wheel_path = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
    upload_legacy(
        server["legacy"], wheel_path, username=server["user"], password=server["password"]
    )
    wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name)

    # Nuke the package's truth directory outright, leaving stale views behind.
    shutil.rmtree(server["data_dir"] / "packages" / PACKAGE)

    deadline = time.time() + 15.0
    while time.time() < deadline:
        code, _, _ = http_get(f"{server['simple']}{PACKAGE}/", headers={"Accept": ACCEPT_PEP691})
        if code == 404:
            break
        time.sleep(0.2)
    else:
        pytest.fail("reconcile did not prune the stale package index")

    # The global index is a separate view updated moments after the package
    # prune, so it gets the same deadline rather than an instant assert.
    deadline = time.time() + 15.0
    while time.time() < deadline:
        global_idx = get_index_json(server["simple"])
        if PACKAGE not in [p["name"] for p in global_idx["projects"]]:
            break
        time.sleep(0.2)
    else:
        pytest.fail("reconcile must remove vanished packages from the global index")


def _serve(pypiron_bin, data_dir, *extra_args):
    """Start a disk server over `data_dir` and return (proc, base_url). The
    caller kills it; the log lands beside the data dir."""
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
        *extra_args,
    ]
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info,pypiron=debug")
    env.setdefault("PYPIRON_ADVISORY_FEED", "")
    log = open(data_dir.parent / f"{data_dir.name}-serve.log", "w")
    proc = subprocess.Popen(args, env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        wait_http_responding(f"http://{bind}/simple/index.json", timeout=20.0)
    except Exception:
        # A server that never answers must not outlive the test that started it.
        kill_process_tree(proc)
        raise
    return proc, f"http://{bind}"


def _global_html_names(data_dir):
    return set(
        re.findall(r'<a href="/simple/([^"]+)/">', (data_dir / "simple" / "index.html").read_text())
    )


def test_sweep_heals_a_torn_global_index_pair_on_an_empty_store(pypiron_bin, tmp_path):
    """`simple/index.html` and `simple/index.json` are two separate writes, HTML
    first, so a crash between them leaves the derived view listing a package the
    canonical JSON does not.

    Every healer for that tear used to ride a delta — the tick's dirty markers,
    the sweep's live package set — and a store with nothing live produces
    neither, so the sweep returned early and the tear stood forever: HTML
    advertising a name no JSON backed, `verify-index` red on every pass, and no
    churn left in the store to dislodge it. The sweep must close it unprompted.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # Boot once so the store holds a materialized (empty) global pair.
    proc, _ = _serve(pypiron_bin, data_dir, "--reconcile-interval-secs", "100000")
    kill_process_tree(proc)
    assert _global_html_names(data_dir) == set()

    # Strand the HTML ahead of the JSON, exactly as a crash between the two
    # writes does — with no dirty marker and nothing live to rebuild.
    html = data_dir / "simple" / "index.html"
    head, _, _ = html.read_text().partition("<body>")
    html.write_text(f'{head}<body><a href="/simple/ghost/">ghost</a><br/></body></html>')
    assert _global_html_names(data_dir) == {"ghost"}

    # A sweep — not the boot path: `--audit-on-boot false` defers the first one
    # to the interval — must reconcile the pair back together.
    proc, _ = _serve(
        pypiron_bin, data_dir, "--audit-on-boot", "false", "--reconcile-interval-secs", "2"
    )
    try:
        deadline = time.time() + 30.0
        while time.time() < deadline:
            if _global_html_names(data_dir) == set():
                break
            time.sleep(0.25)
        else:
            pytest.fail("the sweep left the global HTML listing a package no JSON backs")
    finally:
        kill_process_tree(proc)

    cp = subprocess.run(
        [str(pypiron_bin), "verify-index", "--data-dir", str(data_dir)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert cp.returncode == 0, f"torn global pair survived the sweep:\n{cp.stdout}{cp.stderr}"
