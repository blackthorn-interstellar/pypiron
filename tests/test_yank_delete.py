"""Milestone 6: deletion and yank (PEP 592), proven against real pip."""

from __future__ import annotations

import os
import time

import pytest

from .helpers import (
    ACCEPT_PEP691,
    download_pypi_wheel,
    get_index_json,
    http_get,
    http_get_json,
    http_request_auth,
    make_wheel,
    run_checked,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
OLD_VERSION = "1.16.0"
NEW_VERSION = "1.17.0"

pytestmark = pytest.mark.integration


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX directory permissions")
def test_disk_delete_reports_permission_failure_and_can_retry(disk_server, tmp_path):
    server = disk_server
    pkg = "deletepermission"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    creds = {"username": server["user"], "password": server["password"]}
    # Mirror eviction writes no tombstone in this directory, so the permission
    # failure reaches artifact deletion, after the index has been rewritten.
    upload_legacy(server["legacy"], wheel, fields={"mirror": "true"}, **creds)
    wait_for_file_in_index(server["simple"], pkg, wheel.name)
    directory = server["data_dir"] / "packages" / pkg
    stored = directory / wheel.name
    mode = directory.stat().st_mode
    url = f"{server['base_url']}/files/{pkg}/{wheel.name}"
    directory.chmod(0o555)
    try:
        if os.access(directory, os.W_OK):
            pytest.skip("current user can bypass directory write permissions")
        code, body, _ = http_request_auth("DELETE", url, **creds)
        assert code == 500, body
        assert b"artifact delete failed" in body, body
        assert stored.exists()
        assert (directory / f"{wheel.name}.meta.json").exists()
    finally:
        directory.chmod(mode)

    code, body, _ = http_request_auth("DELETE", url, **creds)
    assert code == 204, body
    assert not stored.exists()
    assert http_get(url)[0] == 404


@pytest.fixture()
def stocked_server(disk_server, tmp_path):
    """Disk server with six 1.16.0 and 1.17.0 uploaded and indexed."""
    wheels = {}
    for version in (OLD_VERSION, NEW_VERSION):
        wheel = download_pypi_wheel(PACKAGE, version, tmp_path)
        upload_legacy(
            disk_server["legacy"],
            wheel,
            username=disk_server["user"],
            password=disk_server["password"],
        )
        wheels[version] = wheel
    for wheel in wheels.values():
        wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel.name)
    return {**disk_server, "wheels": wheels}


def _wait_index(server, predicate, timeout=15.0):
    url = f"{server['simple']}{PACKAGE}/index.json"
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = http_get_json(url, headers={"Accept": ACCEPT_PEP691})
            if predicate(last):
                return last
        except (RuntimeError, ConnectionError):
            pass
        time.sleep(0.2)
    raise TimeoutError(f"index never satisfied predicate; last: {last}")


def _pip_version(py: str) -> str:
    return run_checked([py, "-c", "import six; print(six.__version__)"]).stdout.strip()


@pytest.mark.compat("pip", "yank")
def test_pip_skips_yanked_unless_pinned(stocked_server, pip_venv):
    server = stocked_server
    new_wheel = server["wheels"][NEW_VERSION]
    creds = {"username": server["user"], "password": server["password"]}
    yank_url = f"{server['base_url']}/files/{PACKAGE}/{new_wheel.name}/yank"

    code, _, _ = http_request_auth("POST", yank_url, data=b"broken release", **creds)
    assert code == 200
    _wait_index(
        server,
        lambda doc: any(
            f["filename"] == new_wheel.name and f["yanked"] == "broken release"
            for f in doc["files"]
        ),
    )

    # PEP 592 surface: data-yanked attribute in HTML too.
    _, body, _ = http_get(f"{server['simple']}{PACKAGE}/")
    assert 'data-yanked="broken release"' in body.decode()

    py = str(pip_venv)
    install = [py, "-m", "pip", "install", "--index-url", server["simple"], "--no-cache-dir"]

    # Unpinned: pip must skip the yanked 1.17.0 and pick 1.16.0.
    run_checked([*install, PACKAGE], timeout=180)
    assert _pip_version(py) == OLD_VERSION

    # Pinned: the yanked version is still installable.
    run_checked([*install, f"{PACKAGE}=={NEW_VERSION}"], timeout=180)
    assert _pip_version(py) == NEW_VERSION

    # Un-yank restores normal resolution.
    code, _, _ = http_request_auth("DELETE", yank_url, **creds)
    assert code == 200
    _wait_index(
        server,
        lambda doc: any(
            f["filename"] == new_wheel.name and f["yanked"] is False for f in doc["files"]
        ),
    )
    run_checked([*install, "--upgrade", PACKAGE], timeout=180)
    assert _pip_version(py) == NEW_VERSION


def test_delete_removes_file_then_package(stocked_server):
    server = stocked_server
    old_wheel = server["wheels"][OLD_VERSION]
    new_wheel = server["wheels"][NEW_VERSION]
    creds = {"username": server["user"], "password": server["password"]}

    # Unauthenticated deletes are rejected.
    code, _, _ = http_request_auth(
        "DELETE",
        f"{server['base_url']}/files/{PACKAGE}/{new_wheel.name}",
        username="nope",
        password="wrong",
    )
    assert code == 401

    code, _, _ = http_request_auth(
        "DELETE", f"{server['base_url']}/files/{PACKAGE}/{new_wheel.name}", **creds
    )
    assert code == 204

    _wait_index(
        server,
        lambda doc: new_wheel.name not in [f["filename"] for f in doc["files"]],
    )
    code, _, _ = http_get(f"{server['base_url']}/files/{PACKAGE}/{new_wheel.name}")
    assert code == 404
    pkg_dir = server["data_dir"] / "packages" / PACKAGE
    assert not (pkg_dir / new_wheel.name).exists()
    assert not (pkg_dir / f"{new_wheel.name}.meta.json").exists(), (
        "sidecar must go with the artifact"
    )

    # Deleting the last file removes the package from both indexes.
    code, _, _ = http_request_auth(
        "DELETE", f"{server['base_url']}/files/{PACKAGE}/{old_wheel.name}", **creds
    )
    assert code == 204

    deadline = time.time() + 15.0
    while time.time() < deadline:
        code, _, _ = http_get(f"{server['simple']}{PACKAGE}/", headers={"Accept": ACCEPT_PEP691})
        global_idx = get_index_json(server["simple"])
        if code == 404 and PACKAGE not in [p["name"] for p in global_idx["projects"]]:
            break
        time.sleep(0.2)
    else:
        pytest.fail("deleting the last file must 404 the package index and prune the global index")


def test_deleted_filename_cannot_be_reused(stocked_server):
    """A deleted filename is barred from reuse. Once the
    artifact bytes are gone, plain create-if-absent would readmit the re-upload;
    the tombstone written at delete time is what makes it 409 instead."""
    server = stocked_server
    wheel = server["wheels"][NEW_VERSION]
    creds = {"username": server["user"], "password": server["password"]}

    code, _, _ = http_request_auth(
        "DELETE", f"{server['base_url']}/files/{PACKAGE}/{wheel.name}", **creds
    )
    assert code == 204
    _wait_index(server, lambda doc: wheel.name not in [f["filename"] for f in doc["files"]])

    status, body = upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
        expect_status=409,
    )
    assert status == 409
    assert b"deleted and cannot be reused" in body, body


def test_yank_is_refused_on_a_file_no_index_lists(disk_server, tmp_path):
    """A yank is acked only for a file the index shows. A crashed delete (a
    tombstone beside the live record), a torn freeze (`.frozen` beside it) and
    a sidecar whose body is gone are all hidden from every index; each used to
    take the edit with a 200 and rewrite the sidecar anyway."""
    server = disk_server
    creds = {"username": server["user"], "password": server["password"]}
    pkg = "yankfence"
    pkg_dir = server["data_dir"] / "packages" / pkg
    cases = {
        "1.0": (lambda name: (pkg_dir / f"{name}.tombstone").write_text("{}"), 409),
        "2.0": (lambda name: (pkg_dir / f"{name}.frozen").write_text("{}"), 409),
        "3.0": (lambda name: (pkg_dir / name).unlink(), 404),
    }
    wheels = {version: make_wheel(pkg, version, tmp_path) for version in [*cases, "4.0"]}
    for wheel in wheels.values():
        upload_legacy(server["legacy"], wheel, **creds)
        wait_for_file_in_index(server["simple"], pkg, wheel.name)

    for version, (plant, expected) in cases.items():
        name = wheels[version].name
        plant(name)
        sidecar = pkg_dir / f"{name}.meta.json"
        before = sidecar.read_bytes()
        url = f"{server['base_url']}/files/{pkg}/{name}/yank"
        code, body, _ = http_request_auth("POST", url, data=b"withdrawn", **creds)
        assert code == expected, (version, body)
        assert sidecar.read_bytes() == before, (
            f"{version}: a refused yank must not edit the sidecar"
        )

    # The listed file next to them still takes the edit.
    live = wheels["4.0"].name
    url = f"{server['base_url']}/files/{pkg}/{live}/yank"
    code, body, _ = http_request_auth("POST", url, data=b"withdrawn", **creds)
    assert code == 200, body
    deadline = time.time() + 15.0
    while time.time() < deadline:
        doc = http_get_json(f"{server['simple']}{pkg}/", headers={"Accept": ACCEPT_PEP691})
        if any(f["filename"] == live and f["yanked"] for f in doc["files"]):
            break
        time.sleep(0.2)
    else:
        pytest.fail("the listed file's yank never reached the index")


def test_frozen_filename_cannot_be_uploaded_on_one_bucket(disk_server, tmp_path):
    """`.frozen` bars its filename in single-bucket mode too — a fleet shrunk to
    one bucket keeps its torn freezes. The upload used to pass a tombstone-only
    check, get acked, and then stay unlisted and 404 by direct URL for good."""
    server = disk_server
    creds = {"username": server["user"], "password": server["password"]}
    pkg = "frozenname"
    anchor = make_wheel(pkg, "1.0", tmp_path)
    upload_legacy(server["legacy"], anchor, **creds)
    wait_for_file_in_index(server["simple"], pkg, anchor.name)

    wheel = make_wheel(pkg, "2.0", tmp_path)
    pkg_dir = server["data_dir"] / "packages" / pkg
    (pkg_dir / f"{wheel.name}.frozen").write_text("{}")
    status, body = upload_legacy(server["legacy"], wheel, expect_status=409, **creds)
    assert status == 409
    assert b"frozen or deleted" in body, body
    assert not (pkg_dir / wheel.name).exists(), "the refused upload must not leave its body"
    assert http_get(f"{server['base_url']}/files/{pkg}/{wheel.name}")[0] == 404
