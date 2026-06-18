"""pip client compatibility matrix coverage."""

from __future__ import annotations

import re

import pytest

from .helpers import (
    download_pypi_wheel,
    run_checked,
    run_returncode,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
VERSION = "1.17.0"

pytestmark = pytest.mark.integration


@pytest.fixture()
def pip_six_server(disk_server, tmp_path):
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel_path,
        username=disk_server["user"],
        password=disk_server["password"],
        fields={"requires_python": ">=2.7"},
    )
    index = wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)
    return {**disk_server, "wheel_path": wheel_path, "package_index": index}


@pytest.mark.compat("pip", "pep658-metadata")
@pytest.mark.compat("pip", "resolve")
def test_pip_resolves_with_pep658_metadata(pip_six_server, pip_venv):
    wheel_name = pip_six_server["wheel_path"].name
    cp = run_checked(
        [
            str(pip_venv),
            "-m",
            "pip",
            "install",
            "--dry-run",
            "--no-cache-dir",
            "--index-url",
            pip_six_server["simple"],
            f"{PACKAGE}=={VERSION}",
        ],
        timeout=180,
    )
    output = f"{cp.stdout}\n{cp.stderr}"
    assert f"Would install {PACKAGE}-{VERSION}" in output

    log = pip_six_server["log_path"].read_text()
    assert f"GET /files/{PACKAGE}/{wheel_name}.metadata" in log, (
        "pip should fetch the PEP 658 metadata companion"
    )
    wheel_fetches = re.findall(rf"GET /files/{PACKAGE}/{re.escape(wheel_name)}$", log, re.MULTILINE)
    assert not wheel_fetches, "resolution must not download the wheel itself"


@pytest.mark.compat("pip", "hash-check")
def test_pip_installs_with_required_hash(pip_six_server, pip_venv, tmp_path):
    (entry,) = pip_six_server["package_index"]["files"]
    digest = entry["hashes"]["sha256"]
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(f"{PACKAGE}=={VERSION} --hash=sha256:{digest}\n")

    run_checked(
        [
            str(pip_venv),
            "-m",
            "pip",
            "install",
            "--require-hashes",
            "--no-cache-dir",
            "--index-url",
            pip_six_server["simple"],
            "-r",
            str(requirements),
        ],
        timeout=180,
    )
    run_checked([str(pip_venv), "-c", f"import {PACKAGE}"])


@pytest.mark.compat("pip", "hash-check")
def test_pip_rejects_bad_required_hash(pip_six_server, pip_venv, tmp_path):
    (entry,) = pip_six_server["package_index"]["files"]
    digest = entry["hashes"]["sha256"]
    bad_digest = ("0" if digest[0] != "0" else "1") + digest[1:]
    requirements = tmp_path / "requirements-bad.txt"
    requirements.write_text(f"{PACKAGE}=={VERSION} --hash=sha256:{bad_digest}\n")

    rc, out, err = run_returncode(
        [
            str(pip_venv),
            "-m",
            "pip",
            "install",
            "--require-hashes",
            "--no-cache-dir",
            "--index-url",
            pip_six_server["simple"],
            "-r",
            str(requirements),
        ],
        timeout=180,
    )

    output = f"{out}\n{err}"
    assert rc != 0
    assert "THESE PACKAGES DO NOT MATCH THE HASHES FROM THE REQUIREMENTS FILE" in output
