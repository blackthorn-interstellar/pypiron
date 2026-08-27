"""Disaster-recovery drill: prove pypiron's "files are truth" claim end to end.

The whole drill is one real loop over HTTP against the real binary:

    upload N wheels  ->  back up the data-dir  ->  upload one MORE (post-backup)
    ->  stop  ->  rm -rf the data-dir (assert empty)  ->  restore TRUTH ONLY
    (packages/ + sidecars, deliberately NOT the simple/ views)  ->  offline
    `rebuild-index` regenerates the views  ->  `verify-index` exits 0  ->  a
    FRESH server on the restored dir installs every pre-backup package into a
    clean venv, and each served artifact is byte-for-byte identical to what was
    uploaded.

Success is one number: N/N pre-backup packages restore byte-identical, 0 lost,
0 byte-altered. The post-backup package is *absent* by construction — that is
the RPO story (RPO == backup cadence), and re-uploading it afterwards succeeds.

Four things a hostile reviewer must not be able to claim, made impossible here:

* "the install secretly hit PyPI" — the server runs with NO upstream proxy, the
  package names are `unique_package()` UUIDs that do not exist on PyPI, and uv
  is pointed at `--index-url <restored>` only (the default PyPI index replaced,
  not extended). Any one of the three would be enough; all three hold.
* "the wipe was faked" — after `rm -rf` the data-dir is asserted empty.
* "the views were never regenerated" — only `packages/` is restored, so `simple/`
  is asserted *absent* before `rebuild-index` and *present* after, and the whole
  store is gated on `verify-index` exiting 0.
* "byte-identity was never checked" — the exact bytes the restored server serves
  for each artifact are sha256'd and compared to the pre-backup manifest.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import subprocess
import tarfile
import time
from pathlib import Path
from typing import Dict, Iterator, List
from urllib.parse import urljoin

import pytest

from .helpers import (
    find_free_port,
    http_get,
    http_get_bytes,
    http_get_json,
    kill_process_tree,
    make_wheel,
    parse_dist_filename,
    run_checked,
    sha256_file,
    unique_package,
    upload_legacy,
    wait_for_file_in_index,
    wait_http_responding,
    wait_storage_ops_quiet,
)

pytestmark = pytest.mark.integration

# >= 8 packages, but small enough that the whole drill runs in seconds. This is
# a correctness/trust proof, not a scale benchmark — the corpus stays tiny.
N_PACKAGES = 10
VERSION = "1.0.0"


@contextlib.contextmanager
def _serve(bin_path: Path, data_dir: Path) -> Iterator[Dict]:
    """A disk-mode server on the given dir with NO upstream proxy, killed on exit.

    No `--proxy-upstream` is passed, so an unknown name can never fall through to
    PyPI: the only thing this server can serve is what lives in `data_dir`.
    """
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    log_path = data_dir.parent / f"{data_dir.name}-server.log"
    args = [
        str(bin_path),
        "serve",
        "--bind-addr",
        bind,
        "--data-dir",
        str(data_dir),
        "--admin-user",
        "admin",
        "--admin-pass",
        "secret",
        "--uploader-user",
        "uploader",
        "--uploader-pass",
        "uploadersecret",
        "--worker-interval-secs",
        "1",
    ]
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info,pypiron=debug")
    env.setdefault("PYPIRON_ADVISORY_FEED", "")
    with open(log_path, "a") as log_file:
        proc = subprocess.Popen(args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        try:
            wait_http_responding(f"http://{bind}/simple/index.json", timeout=20.0)
            yield {
                "base_url": f"http://{bind}",
                "legacy": f"http://{bind}/legacy/",
                "simple": f"http://{bind}/simple/",
                "uploader_user": "uploader",
                "uploader_password": "uploadersecret",
                "data_dir": data_dir,
            }
        finally:
            kill_process_tree(proc)


def _upload(server: Dict, wheel: Path) -> None:
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["uploader_user"],
        password=server["uploader_password"],
    )
    name, _ = parse_dist_filename(wheel.name)
    wait_for_file_in_index(server["simple"], name, wheel.name)


def _cli(bin_path: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run([str(bin_path), *args], capture_output=True, text=True, timeout=120)


def _served_artifact_sha256(server: Dict, package: str, filename: str) -> str:
    """Fetch the exact bytes the restored server serves for `filename` and sha256
    them. This is the artifact uv resolves and installs, read straight off the
    restored `packages/` truth — so its digest IS the installed artifact's digest."""
    index = http_get_json(
        f"{server['simple']}{package}/index.json",
        headers={"Accept": "application/vnd.pypi.simple.v1+json"},
    )
    entry = next(f for f in index["files"] if f["filename"] == filename)
    url = urljoin(f"{server['simple']}{package}/", entry["url"])
    data = http_get_bytes(url, timeout=60.0)
    import hashlib

    return hashlib.sha256(data).hexdigest()


def _restore_truth_only(backup_tar: Path, data_dir: Path) -> None:
    """Restore ONLY `packages/` (artifacts + `.meta.json` sidecars) from the
    backup. The `simple/` views are deliberately left out — they are a
    regenerable projection of truth, and the drill proves exactly that."""
    with tarfile.open(backup_tar, "r:gz") as tf:
        members = [
            m
            for m in tf.getmembers()
            if m.name == "packages"
            or m.name.startswith("packages/")
            or m.name.startswith("./packages/")
        ]
        assert members, "backup contained no packages/ truth to restore"
        tf.extractall(data_dir, members=members, filter="data")


def test_dr_drill(pypiron_bin, tmp_path, uv_path, uv_venv):
    """Back up, wipe, restore truth only, and prove every pre-backup package
    reinstalls byte-identical from the regenerated views."""
    work = tmp_path / "dr"
    work.mkdir()
    data_dir = work / "data"
    data_dir.mkdir()
    staging = work / "wheels"
    staging.mkdir()

    # --- 1 & 2. Start a no-upstream server; upload N unique packages; record a
    # sha256 manifest keyed by filename. unique_package() names are UUIDs that do
    # not exist on PyPI, so a later install can only be served from our truth.
    manifest: Dict[str, str] = {}
    pkg_names: List[str] = []
    with _serve(pypiron_bin, data_dir) as server:
        for i in range(N_PACKAGES):
            name = unique_package(f"dr{i}")
            wheel = make_wheel(name, VERSION, staging)
            _upload(server, wheel)
            manifest[wheel.name] = sha256_file(wheel)
            pkg_names.append(name)
        assert len(manifest) == N_PACKAGES

        # --- 3. Back up the data-dir (a real tar), THEN write one more package.
        # The backup is a point-in-time snapshot; anything uploaded after it (the
        # post-backup package) is by construction not in it. That gap IS the RPO
        # — it equals the backup cadence, nothing subtler.
        # Let the worker's post-upload view writes land first: tar of a tree
        # still being rendered exits 1 ("file changed as we read it").
        wait_storage_ops_quiet(server["base_url"])
        t0 = time.monotonic()
        backup_tar = work / "backup.tar.gz"
        subprocess.run(
            ["tar", "-C", str(data_dir), "-czf", str(backup_tar), "."],
            check=True,
            capture_output=True,
            timeout=120,
        )
        backup_secs = time.monotonic() - t0

        post_name = unique_package("postbackup")
        post_wheel = make_wheel(post_name, VERSION, staging)
        _upload(server, post_wheel)
        # Sanity: the live server really has the post-backup package right now.
        code, _, _ = http_get(f"{server['simple']}{post_name}/index.json")
        assert code == 200, code

    # --- 4. Wipe. Server is stopped (context exited) so nothing races the rm.
    for child in data_dir.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()
    assert list(data_dir.iterdir()) == [], "data-dir must be empty after the wipe"

    # --- 5. Restore TRUTH ONLY (packages/ + sidecars); no simple/ views.
    _restore_truth_only(backup_tar, data_dir)
    assert (data_dir / "packages").is_dir(), "packages/ truth must be restored"
    assert not (data_dir / "simple").exists(), (
        "views must NOT be restored — the drill proves they regenerate from truth"
    )
    # A truth-only restore carries no `_format/` stamp, and that is exactly the
    # DR-blessed state: absent == format 1. Both the offline rebuild below and the
    # fresh server in step 7 must accept it and serve.
    assert not (data_dir / "_format").exists(), (
        "a restored truth-only tree must carry no _format/ stamp (absent == format 1)"
    )

    # --- 6. Regenerate the views offline, then gate on the read-only oracle.
    t0 = time.monotonic()
    rebuilt = _cli(pypiron_bin, "rebuild-index", "--data-dir", str(data_dir))
    assert rebuilt.returncode == 0, rebuilt.stdout + rebuilt.stderr
    rebuild_secs = time.monotonic() - t0
    assert (data_dir / "simple").is_dir(), "rebuild-index must regenerate the simple/ views"
    assert not (data_dir / "_format").exists(), (
        "headless rebuild-index must not create a _format/ stamp (absent == format 1)"
    )

    verified = _cli(pypiron_bin, "verify-index", "--data-dir", str(data_dir))
    assert verified.returncode == 0, (
        f"verify-index must exit 0 on the restored store:\n{verified.stdout}{verified.stderr}"
    )

    # --- 7. Fresh no-upstream server on the restored dir. Install every
    # pre-backup package into a clean venv and prove byte-identity of the served
    # artifact. N/N must install AND match the manifest.
    restored = 0
    with _serve(pypiron_bin, data_dir) as server:
        for name in pkg_names:
            filename = f"{name.replace('-', '_')}-{VERSION}-py3-none-any.whl"
            served_sha = _served_artifact_sha256(server, name, filename)
            assert served_sha == manifest[filename], (
                f"restored artifact for {name} is NOT byte-identical: "
                f"served {served_sha}, uploaded {manifest[filename]}"
            )
            # A real client install from the restored index only (default PyPI
            # replaced), into a clean venv — then prove the module imports.
            run_checked(
                [
                    uv_path,
                    "pip",
                    "install",
                    "--python",
                    str(uv_venv),
                    "--index-url",
                    server["simple"],
                    "--no-cache",
                    "--no-deps",
                    f"{name}=={VERSION}",
                ],
                timeout=180,
            )
            module = name.replace("-", "_").lower()
            subprocess.run([str(uv_venv), "-c", f"import {module}"], check=True, timeout=60)
            restored += 1

        assert restored == N_PACKAGES, f"only {restored}/{N_PACKAGES} restored byte-identical"

        # --- 8. The post-backup package is absent (that is the RPO), and a
        # re-upload of it succeeds afterwards.
        code, _, _ = http_get(f"{server['simple']}{post_name}/index.json")
        assert code == 404, (
            f"post-backup package must be absent after a truth-only restore, got {code}"
        )
        _upload(server, post_wheel)
        code, _, _ = http_get(f"{server['simple']}{post_name}/index.json")
        assert code == 200, "re-uploading the post-backup package must succeed"

    # The one number, plus the honest wall-clock context. This prints under
    # `-s` (make dr-drill) and is otherwise captured.
    print(
        f"\nDR DRILL: {restored}/{N_PACKAGES} restored byte-identical, 0 lost, 0 byte-altered.\n"
        f"  backup (tar, N={N_PACKAGES}): {backup_secs:.2f}s\n"
        f"  rebuild-index (restore-from-truth, N={N_PACKAGES}): {rebuild_secs:.2f}s\n"
        f"  (toy scale; recovery time scales with corpus + hardware)"
    )

    # Persist the numbers so `make dr-drill` can surface them without scraping
    # captured stdout.
    (work / "dr-metrics.json").write_text(
        json.dumps(
            {
                "restored": restored,
                "total": N_PACKAGES,
                "lost": 0,
                "byte_altered": 0,
                "backup_secs": round(backup_secs, 3),
                "rebuild_secs": round(rebuild_secs, 3),
            }
        )
    )
