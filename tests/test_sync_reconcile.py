"""A re-sync truly converges to upstream: yank state propagates, files gone
upstream are flagged, project status (PEP 792) is relayed, and an unchanged
upstream is skipped via a conditional 304. Driven against two real pypiron
processes — a mutable SOURCE and a DEST — so the source can be
yanked/deleted/quarantined and the dest re-synced over HTTP (`--to`).
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, Iterator

import pytest

from .conftest import _start_disk_server
from .helpers import (
    get_index_json,
    http_get,
    http_request_auth,
    make_wheel,
    sync_to,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration


@pytest.fixture()
def source_dest(tmp_path_factory, pypiron_bin) -> Iterator[Dict]:
    """A mutable SOURCE pypiron and a DEST pypiron, both disk-mode. Same
    two-generator pattern as the proxy-pair fixture — each gets its own data dir
    and free port."""
    src_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    src = next(src_gen)
    dst_gen = _start_disk_server(tmp_path_factory, pypiron_bin)
    dst = next(dst_gen)
    try:
        yield {"source": src, "dest": dst}
    finally:
        dst_gen.close()
        src_gen.close()


def _admin(server) -> Dict[str, str]:
    return {"username": server["admin_user"], "password": server["admin_password"]}


def _seed(server, name: str, version: str, tmp_path):
    """Build a wheel locally (no network) and upload it to `server`."""
    wheel = make_wheel(name, version, tmp_path)
    upload_legacy(server["legacy"], wheel, **_admin(server))
    return wheel


def _run_sync(pypiron_bin, source, dest, pkg_list, *extra):
    # Pin the log filter so the 304/"Syncing" assertions don't depend on an
    # ambient RUST_LOG; the messages are emitted at info!.
    env = {**os.environ, "RUST_LOG": "info,pypiron=info"}
    return sync_to(
        pypiron_bin,
        dest,
        "--include-packages-from",
        str(pkg_list),
        *extra,
        source=source["base_url"],
        env=env,
    )


def _yank_value(idx: dict, filename: str):
    """The `yanked` value for a file in a PEP 691 index (False, or a reason
    string); None if the file isn't listed."""
    for f in idx.get("files", []):
        if f["filename"] == filename:
            return f.get("yanked", False)
    return None


def _wait_yank(simple_url: str, pkg: str, filename: str, expected, *, timeout: float = 30.0):
    deadline = time.time() + timeout
    last = "<unset>"
    while time.time() < deadline:
        last = _yank_value(get_index_json(simple_url, pkg), filename)
        if last == expected:
            return
        time.sleep(0.2)
    raise AssertionError(f"{filename}: yanked={last!r}, expected {expected!r} within {timeout}s")


def test_yank_set_and_clear_propagate_on_resync(source_dest, pypiron_bin, tmp_path):
    source, dest = source_dest["source"], source_dest["dest"]
    pkg = "reconcileyank"
    _seed(source, pkg, "1.0", tmp_path)
    w2 = _seed(source, pkg, "2.0", tmp_path)
    wait_for_file_in_index(source["simple"], pkg, w2.name)

    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{pkg}\n")

    # Initial sync: both files land, unyanked.
    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list)
    assert rc == 0, f"initial sync failed:\n{out}\n{err}"
    wait_for_file_in_index(dest["simple"], pkg, w2.name)
    assert _yank_value(get_index_json(dest["simple"], pkg), w2.name) is False

    # Yank v2 on the SOURCE, then wait for the source index to reflect it (so
    # its ETag changes and the next sync doesn't 304 past the yank).
    yank_url = f"{source['base_url']}/files/{pkg}/{w2.name}/yank"
    code, _, _ = http_request_auth("POST", yank_url, data=b"broken release", **_admin(source))
    assert code == 200
    _wait_yank(source["simple"], pkg, w2.name, "broken release")

    # Re-sync: the yank reaches the dest, reason and all.
    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list)
    assert rc == 0, f"yank re-sync failed:\n{out}\n{err}"
    _wait_yank(dest["simple"], pkg, w2.name, "broken release")

    # Un-yank on the SOURCE and re-sync: upstream is authoritative, so the dest
    # clears too.
    code, _, _ = http_request_auth("DELETE", yank_url, **_admin(source))
    assert code == 200
    _wait_yank(source["simple"], pkg, w2.name, False)
    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list)
    assert rc == 0, f"un-yank re-sync failed:\n{out}\n{err}"
    _wait_yank(dest["simple"], pkg, w2.name, False)


def test_removed_upstream_is_flagged_yet_downloadable(source_dest, pypiron_bin, tmp_path):
    source, dest = source_dest["source"], source_dest["dest"]
    pkg = "reconcileremoved"
    w1 = _seed(source, pkg, "1.0", tmp_path)
    w2 = _seed(source, pkg, "2.0", tmp_path)
    wait_for_file_in_index(source["simple"], pkg, w2.name)

    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{pkg}\n")

    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list)
    assert rc == 0, f"initial sync failed:\n{out}\n{err}"
    wait_for_file_in_index(dest["simple"], pkg, w1.name)
    wait_for_file_in_index(dest["simple"], pkg, w2.name)

    # Delete v1 from the SOURCE; wait until its index drops the file.
    code, _, _ = http_request_auth(
        "DELETE", f"{source['base_url']}/files/{pkg}/{w1.name}", **_admin(source)
    )
    assert code == 204
    deadline = time.time() + 30.0
    while (
        time.time() < deadline
        and _yank_value(get_index_json(source["simple"], pkg), w1.name) is not None
    ):
        time.sleep(0.2)
    assert _yank_value(get_index_json(source["simple"], pkg), w1.name) is None

    # Re-sync: v1 stays mirrored but is flagged removed; v2 is untouched.
    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list)
    assert rc == 0, f"removal re-sync failed:\n{out}\n{err}"
    _wait_yank(dest["simple"], pkg, w1.name, "removed upstream")
    assert _yank_value(get_index_json(dest["simple"], pkg), w2.name) is False

    # The removed file is yanked, not deleted — its bytes still download.
    code, _, _ = http_get(f"{dest['base_url']}/files/{pkg}/{w1.name}")
    assert code == 200


def test_unchanged_resync_skips_via_conditional_304(source_dest, pypiron_bin, tmp_path):
    source, dest = source_dest["source"], source_dest["dest"]
    pkg = "reconcile304"
    w1 = _seed(source, pkg, "1.0", tmp_path)
    wait_for_file_in_index(source["simple"], pkg, w1.name)

    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{pkg}\n")

    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list)
    assert rc == 0, f"initial sync failed:\n{out}\n{err}"
    assert f"Syncing {pkg}" in (out + err)
    wait_for_file_in_index(dest["simple"], pkg, w1.name)

    # The cursor was persisted server-side — in HTTP mode via PUT /sync/cursors,
    # which the server writes into its own storage tree, same as direct mode.
    cursors_file = dest["data_dir"] / "_sync" / "cursors.json"
    assert cursors_file.exists(), "sync did not persist a cursor"
    assert pkg in json.loads(cursors_file.read_text())

    # Second run, nothing changed upstream: the conditional fetch 304s (in HTTP
    # mode the cursor round-trips through GET /sync/cursors) and the whole
    # project is skipped — no re-selection, no reconcile.
    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list)
    assert rc == 0, f"second sync failed:\n{out}\n{err}"
    combined = out + err
    assert "upstream unchanged since last sync (304)" in combined, combined
    assert f"Syncing {pkg}" not in combined

    # A changed run config (here --include-format wheel) must invalidate the cursor's
    # config key and force a re-fetch, even though upstream is unchanged.
    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list, "--include-format", "wheel")
    assert rc == 0, f"config-change sync failed:\n{out}\n{err}"
    assert f"Syncing {pkg}" in (out + err), "config change did not invalidate the 304 shortcut"

    # --full ignores the memo and re-fetches even though nothing changed.
    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list, "--full")
    assert rc == 0, f"--full sync failed:\n{out}\n{err}"
    assert f"Syncing {pkg}" in (out + err)


def test_resync_skips_files_already_mirrored(source_dest, pypiron_bin, tmp_path):
    """A re-run uploads only genuinely new files: one the dest already holds is
    skipped before its bytes are even downloaded (no wasted download + 409),
    while a newly published file still lands."""
    source, dest = source_dest["source"], source_dest["dest"]
    pkg = "reconskip"
    w1 = _seed(source, pkg, "1.0", tmp_path)
    wait_for_file_in_index(source["simple"], pkg, w1.name)

    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{pkg}\n")

    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list)
    assert rc == 0, f"initial sync failed:\n{out}\n{err}"
    wait_for_file_in_index(dest["simple"], pkg, w1.name)

    # Publish a second version upstream; its appearance changes the source index
    # ETag, so the re-sync re-selects (no 304) and sees both files.
    w2 = _seed(source, pkg, "2.0", tmp_path)
    wait_for_file_in_index(source["simple"], pkg, w2.name)

    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list)
    assert rc == 0, f"resync failed:\n{out}\n{err}"
    combined = out + err
    # Only the new file is uploaded; the already-mirrored one is skipped.
    assert f"Syncing {pkg} (1 new, 1 already mirrored)" in combined, combined
    assert f"uploading {w2.name}" in combined, combined
    assert f"uploading {w1.name}" not in combined, combined
    wait_for_file_in_index(dest["simple"], pkg, w2.name)

    # A forced --full re-run bypasses the 304 and re-selects every file, yet
    # uploads nothing — all are already present.
    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list, "--full")
    assert rc == 0, f"--full resync failed:\n{out}\n{err}"
    combined = out + err
    assert f"Syncing {pkg} (0 new, 2 already mirrored)" in combined, combined
    assert "- uploading" not in combined, combined


def test_relative_duration_exclude_older_still_304s_on_resync(source_dest, pypiron_bin, tmp_path):
    """A relative `--exclude-older` ("1 day") resolves to a fresh instant every
    run, but the sync cursor must still match its own prior config so an
    unchanged upstream 304s. Regression: the resolved instant used to be hashed
    into the cursor's config key, so every relative-duration run re-fetched and
    re-tried every file (each a wasted download + a 409)."""
    source, dest = source_dest["source"], source_dest["dest"]
    pkg = "reconcutoff"
    w1 = _seed(source, pkg, "1.0", tmp_path)
    wait_for_file_in_index(source["simple"], pkg, w1.name)

    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{pkg}\n")

    # Fresh wheels upload "now", so a 1-day older-bound still includes them.
    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list, "--exclude-older", "1 day")
    assert rc == 0, f"initial sync failed:\n{out}\n{err}"
    assert f"Syncing {pkg}" in (out + err)
    wait_for_file_in_index(dest["simple"], pkg, w1.name)

    # Second run, same relative bound, nothing changed upstream: the cursor's
    # config key is hashed from the raw "1 day", not its (now-shifted) instant,
    # so the conditional fetch 304s and the whole project is skipped.
    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list, "--exclude-older", "1 day")
    assert rc == 0, f"second sync failed:\n{out}\n{err}"
    combined = out + err
    assert "upstream unchanged since last sync (304)" in combined, combined
    assert f"Syncing {pkg}" not in combined, combined
    assert "- uploading" not in combined, combined


def test_quarantine_status_propagates_without_mass_removal(source_dest, pypiron_bin, tmp_path):
    source, dest = source_dest["source"], source_dest["dest"]
    pkg = "reconcilestatus"
    w1 = _seed(source, pkg, "1.0", tmp_path)
    wait_for_file_in_index(source["simple"], pkg, w1.name)

    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{pkg}\n")

    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list)
    assert rc == 0, f"initial sync failed:\n{out}\n{err}"
    wait_for_file_in_index(dest["simple"], pkg, w1.name)

    # Quarantine the SOURCE project (no API for it — drop the PEP 792 marker and
    # a dirty marker so the source worker rebuilds its index). A quarantined
    # index serves no files, which exercises the "empty listing ≠ all removed"
    # guard.
    (source["data_dir"] / "packages" / pkg / ".project-status.json").write_text(
        json.dumps({"status": "quarantined", "reason": "security hold"})
    )
    dirty_dir = source["data_dir"] / "_dirty"
    dirty_dir.mkdir(exist_ok=True)
    (dirty_dir / f"{pkg}!reconciletest.commit").write_text("")

    deadline = time.time() + 30.0
    while time.time() < deadline:
        idx = get_index_json(source["simple"], pkg)
        if idx.get("project-status", {}).get("status") == "quarantined":
            break
        time.sleep(0.2)
    assert (
        get_index_json(source["simple"], pkg).get("project-status", {}).get("status")
        == "quarantined"
    )

    # Re-sync. No files change (a quarantine offers none), yet the status must
    # still reach the dest — relayed over HTTP through its status endpoint.
    rc, out, err = _run_sync(pypiron_bin, source, dest, pkg_list)
    assert rc == 0, f"status re-sync failed:\n{out}\n{err}"

    deadline = time.time() + 30.0
    while time.time() < deadline:
        idx = get_index_json(dest["simple"], pkg)
        if idx.get("project-status", {}).get("status") == "quarantined":
            break
        time.sleep(0.2)
    assert (
        get_index_json(dest["simple"], pkg).get("project-status", {}).get("status") == "quarantined"
    )

    # The guard held: the file was not mass-flagged "removed upstream" off the
    # quarantine's empty listing — its sidecar still reads not-yanked.
    sidecar = json.loads((dest["data_dir"] / "packages" / pkg / f"{w1.name}.meta.json").read_text())
    assert sidecar["yanked"] is False
