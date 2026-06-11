"""Milestone 6: deletion and yank (PEP 592), proven against real pip."""

from __future__ import annotations

import time

import pytest

from .helpers import (
    ACCEPT_PEP691,
    download_pypi_wheel,
    http_get,
    http_get_json,
    http_request_auth,
    run_checked,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
OLD_VERSION = "1.16.0"
NEW_VERSION = "1.17.0"

pytestmark = pytest.mark.integration


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
    assert not (pkg_dir / f"{new_wheel.name}.meta.json").exists(), "sidecar must go with the artifact"

    # Deleting the last file removes the package from both indexes.
    code, _, _ = http_request_auth(
        "DELETE", f"{server['base_url']}/files/{PACKAGE}/{old_wheel.name}", **creds
    )
    assert code == 204

    deadline = time.time() + 15.0
    while time.time() < deadline:
        code, _, _ = http_get(f"{server['simple']}{PACKAGE}/", headers={"Accept": ACCEPT_PEP691})
        global_idx = http_get_json(
            f"{server['simple']}index.json", headers={"Accept": ACCEPT_PEP691}
        )
        if code == 404 and PACKAGE not in [p["name"] for p in global_idx["projects"]]:
            break
        time.sleep(0.2)
    else:
        pytest.fail("deleting the last file must 404 the package index and prune the global index")
