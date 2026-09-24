"""Upgrade and rollback against the last published release, end to end.

An operator upgrades by swapping the binary over the same store; a bad release
is undone by swapping it back. Both directions are claims about the store, so
they are tested the only honest way: the latest pypiron release from PyPI
(installed via uv) writes real state — uploads, a yank, a project status,
downloads — then the binary under test boots on that exact directory, must
serve every byte of it and pass `verify-index`, and writes its own state. Then
the old release boots again on what the new one wrote and must serve all of it.

Rollback succeeding is today's contract: no storage-format stamp is written
(see dev/DESIGN.md, "Storage-format bump policy"). A future format bump makes
an old binary refuse to start instead — `test_format_stamp.py` covers that
refusal; if one lands, this test's rollback leg must be updated alongside it.

Runs on disk and on S3 (MinIO; the s3 leg skips without Docker, like the rest
of the S3 suite). Skips cleanly when the release cannot be fetched (offline, no
PyPI).
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import platform
import subprocess
import time
import urllib.parse
from pathlib import Path
from typing import Dict, Iterator

import pytest

from .conftest import _s3_env
from .helpers import (
    find_free_port,
    get_index_json,
    http_get,
    http_get_bytes,
    http_request_auth,
    kill_process_tree,
    make_sdist,
    make_wheel,
    run_checked,
    upload_legacy,
    uv_python_path,
    wait_for_file_in_index,
    wait_for_project_in_global,
    wait_http_responding,
)

pytestmark = [pytest.mark.integration, pytest.mark.upgrade]

ADMIN = {"username": "admin", "password": "secret"}
UPLOADER = {"username": "uploader", "password": "uploadersecret"}


@pytest.fixture(scope="session")
def released_bin(tmp_path_factory, uv_path: str) -> tuple[str, Path]:
    """(version, binary) of the latest pypiron release on PyPI."""
    venv = tmp_path_factory.mktemp("pypiron-release")
    try:
        code, body, _ = http_get("https://pypi.org/pypi/pypiron/json", timeout=20.0)
        if code != 200:
            raise RuntimeError(f"PyPI JSON API answered {code}")
        version = json.loads(body)["info"]["version"]
        run_checked([uv_path, "venv", str(venv)], timeout=120)
        run_checked(
            [
                uv_path,
                "pip",
                "install",
                "--python",
                str(uv_python_path(venv)),
                f"pypiron=={version}",
            ],
            timeout=300,
        )
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.TimeoutExpired) as e:
        pytest.skip(f"latest pypiron release unavailable (cannot fetch/install): {e}")
    exe = "pypiron.exe" if platform.system().lower().startswith("win") else "pypiron"
    bin_path = uv_python_path(venv).parent / exe
    assert bin_path.exists(), f"release wheel installed no binary at {bin_path}"
    return version, bin_path


def _store_env(store: Dict, bind: str) -> Dict[str, str]:
    """Environment pointing a binary at the store: a disk dir, or MinIO."""
    if store["minio"] is None:
        return {**os.environ, "PYPIRON_DATA_DIR": str(store["data_dir"])}
    return _s3_env(store["minio"], bind)


@contextlib.contextmanager
def _serve(bin_path: Path, store: Dict) -> Iterator[Dict]:
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    log_path = store["log_path"]
    args = [
        str(bin_path),
        "serve",
        "--bind-addr",
        bind,
        "--admin-user",
        ADMIN["username"],
        "--admin-pass",
        ADMIN["password"],
        "--uploader-user",
        UPLOADER["username"],
        "--uploader-pass",
        UPLOADER["password"],
        "--worker-interval-secs",
        "1",
    ]
    env = {**_store_env(store, bind), "RUST_LOG": "info", "PYPIRON_ADVISORY_FEED": ""}
    with open(log_path, "a") as log_file:
        log_file.write(f"\n===== {bin_path}\n")
        log_file.flush()
        proc = subprocess.Popen(args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        try:
            wait_http_responding(f"http://{bind}/simple/index.json", timeout=30.0)
            assert proc.poll() is None, f"server exited at startup; log: {log_path}"
            yield {
                "base_url": f"http://{bind}",
                "legacy": f"http://{bind}/legacy/",
                "simple": f"http://{bind}/simple/",
                "log_path": log_path,
            }
            # Stop the way an orchestrator does: SIGTERM and a grace period. On
            # S3 a graceful stop takes a few seconds (pre-drain pause, then the
            # leader lease hand-off); cutting it short leaves the successor
            # waiting out the lease TTL — a stop problem, not an upgrade one.
            proc.terminate()
            assert proc.wait(timeout=30) == 0, f"{bin_path} did not stop cleanly; log: {log_path}"
        finally:
            kill_process_tree(proc)


def _publish(server: Dict, dist: Path, name: str) -> None:
    upload_legacy(server["legacy"], dist, **UPLOADER)
    wait_for_file_in_index(server["simple"], name, dist.name)


def _yank(server: Dict, name: str, filename: str, reason: bytes) -> None:
    url = f"{server['base_url']}/files/{name}/{filename}/yank"
    code, body, _ = http_request_auth("POST", url, data=reason, **ADMIN)
    assert code == 200, (code, body)
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        files = get_index_json(server["simple"], name)["files"]
        if any(f["filename"] == filename and f["yanked"] for f in files):
            return
        time.sleep(0.2)
    raise TimeoutError(f"yank of {filename} never reached the index")


def _verify_index(bin_path: Path, store: Dict) -> None:
    cp = subprocess.run(
        [str(bin_path), "verify-index"],
        env=_store_env(store, "127.0.0.1:0"),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert cp.returncode == 0, f"{bin_path} verify-index:\n{cp.stdout}{cp.stderr}"


def _assert_serves(server: Dict, published: Dict[str, Dict[str, Path]], yanked: Dict) -> None:
    """Every published dist is listed (JSON and HTML), carries the right yank
    state, and downloads byte-identical with the advertised sha256."""
    for name, dists in published.items():
        wait_for_project_in_global(server["simple"], name)
        doc = get_index_json(server["simple"], name)
        listed = {f["filename"]: f for f in doc["files"]}
        assert set(listed) == set(dists), (name, sorted(listed))
        code, html, _ = http_get(f"{server['simple']}{name}/")
        assert code == 200, (name, code)
        for filename, local in dists.items():
            entry = listed[filename]
            assert filename in html.decode(), (name, filename)
            assert entry["yanked"] == yanked.get(filename, False), (filename, entry["yanked"])
            url = urllib.parse.urljoin(f"{server['simple']}{name}/", entry["url"].split("#")[0])
            body = http_get_bytes(url)
            assert body == local.read_bytes(), filename
            assert entry["hashes"]["sha256"] == hashlib.sha256(body).hexdigest(), filename


@pytest.fixture(params=["disk", pytest.param("s3", marks=pytest.mark.s3)])
def store(request, tmp_path: Path) -> Dict:
    """One store the old and new binaries take turns serving."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    minio = request.getfixturevalue("minio") if request.param == "s3" else None
    return {"data_dir": data_dir, "minio": minio, "log_path": tmp_path / "server.log"}


def test_upgrade_then_rollback_serves_everything(
    released_bin, pypiron_bin: Path, store: Dict, tmp_path: Path, uv_venv: Path, uv_path: str
):
    version, old_bin = released_bin
    dists = tmp_path / "dists"
    published: Dict[str, Dict[str, Path]] = {"upgrade-alpha": {}, "upgrade-beta": {}}
    yanked: Dict[str, object] = {}

    def add(server, name, dist):
        _publish(server, dist, name)
        published.setdefault(name, {})[dist.name] = dist

    # 1. The released binary writes real state.
    with _serve(old_bin, store) as old:
        for v in ("1.0.0", "1.1.0", "2.0.0"):
            add(old, "upgrade-alpha", make_wheel("upgrade-alpha", v, dists))
        add(old, "upgrade-alpha", make_sdist("upgrade-alpha", "2.0.0", dists))
        add(old, "upgrade-beta", make_wheel("upgrade-beta", "0.1.0", dists))
        _yank(old, "upgrade-alpha", "upgrade_alpha-1.1.0-py3-none-any.whl", b"broken build")
        yanked["upgrade_alpha-1.1.0-py3-none-any.whl"] = "broken build"
        code, _, _ = http_request_auth(
            "POST",
            f"{old['base_url']}/project/upgrade-beta/status",
            data=b'{"status":"archived","reason":"superseded"}',
            **ADMIN,
        )
        assert code == 200, code
        _assert_serves(old, published, yanked)  # also records downloads

    # 2. The binary under test takes over the same store.
    _verify_index(pypiron_bin, store)
    with _serve(pypiron_bin, store) as new:
        _assert_serves(new, published, yanked)
        assert get_index_json(new["simple"], "upgrade-beta")["project-status"] == {
            "status": "archived",
            "reason": "superseded",
        }
        # A real installer resolves against the upgraded store: the yanked
        # 1.1.0 is skipped for a range, the newest unyanked version wins.
        run_checked(
            [
                uv_path,
                "pip",
                "install",
                "--python",
                str(uv_venv),
                "--no-cache",
                "--index-url",
                new["simple"],
                "upgrade-alpha>=1.0,<2",
            ],
            timeout=120,
        )
        out = run_checked(
            [str(uv_venv), "-c", "import upgrade_alpha; print(upgrade_alpha.__version__)"]
        ).stdout
        assert out.strip() == "1.0.0", out
        # ...and writes state of its own.
        add(new, "upgrade-alpha", make_wheel("upgrade-alpha", "3.0.0", dists))
        add(new, "upgrade-gamma", make_wheel("upgrade-gamma", "1.0.0", dists))
        _yank(new, "upgrade-alpha", "upgrade_alpha-2.0.0-py3-none-any.whl", b"regression")
        yanked["upgrade_alpha-2.0.0-py3-none-any.whl"] = "regression"
        _assert_serves(new, published, yanked)
    _verify_index(pypiron_bin, store)

    # 3. Rollback: the released binary reads everything the new one wrote.
    _verify_index(old_bin, store)
    with _serve(old_bin, store) as rolled_back:
        _assert_serves(rolled_back, published, yanked)
    if store["minio"] is not None:
        return
    assert not (store["data_dir"] / "_format").exists(), (
        f"the binary under test stamped a storage format; rolling back to {version} "
        "would refuse — update this test's rollback leg with the bump"
    )
