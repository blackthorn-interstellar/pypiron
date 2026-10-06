"""pip client compatibility matrix coverage."""

from __future__ import annotations

import re
import sys

import pytest

from .helpers import (
    cmd_exists,
    download_pypi_wheel,
    make_wheel,
    module_name,
    run_checked,
    run_returncode,
    unique_package,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
VERSION = "1.17.0"

pytestmark = pytest.mark.integration


@pytest.fixture()
def pip_six_server(disk_server_access_log, tmp_path):
    disk_server = disk_server_access_log  # access log on: lets us see client fetches
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
    assert f"path=/files/{PACKAGE}/{wheel_name}.metadata" in log, (
        "pip should fetch the PEP 658 metadata companion"
    )
    # Trailing space selects bare-wheel fetches, excluding the `.metadata` companion.
    wheel_fetches = re.findall(rf"path=/files/{re.escape(PACKAGE)}/{re.escape(wheel_name)} ", log)
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


# Every CPython of the last decade, each with the newest pip it supports: what a
# user who keeps pip current gets on that Python (2.7 tops out at pip 20.3, 3.6
# at 21.3, 3.7 at 24.0 ...). Those old pips speak only the HTML simple API.
PYTHONS = ["2.7", "3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12", "3.13", "3.14"]
# Then every pip minor of the last decade, each on the oldest Python above it
# supports. Bugs hide between the newest-per-Python pips: 22.3–23.1 crashed on
# a JSON `dist-info-metadata: true`, and no Python's newest pip is in that range.
PIP_MINORS = {
    "3.6": ["9.0", "10.0", "18.0", "18.1", "19.0", "19.1", "19.2", "19.3"]
    + ["20.0", "20.1", "20.2", "20.3", "21.0", "21.1", "21.2", "21.3"],
    "3.7": ["22.0", "22.1", "22.2", "22.3", "23.0", "23.1", "23.2", "23.3", "24.0"],
    "3.8": ["24.1", "24.2", "24.3", "25.0"],
    "3.9": ["25.1", "25.2", "25.3", "26.0"],
    "3.10": ["26.1", "26.2"],
}
# None: the pip a fresh `python -m venv` bundles, never upgraded — what most
# users actually run. 3.7–3.10 still bundle 23.0.1, inside the 22.3–23.1 bug.
CLIENTS = [pytest.param(py, None, id=f"py{py}-venv") for py in PYTHONS if py != "2.7"]
CLIENTS += [pytest.param(py, "pip", id=f"py{py}-latest") for py in PYTHONS]
CLIENTS += [
    pytest.param(py, f"pip=={minor}.*", id=f"py{py}-pip{minor}")
    for py, minors in PIP_MINORS.items()
    for minor in minors
]
# Deliberately not `compat`-marked: it gates every PR, not just the weekly run.


@pytest.mark.parametrize(("python", "pip_spec"), CLIENTS)
def test_pip_versions_install(disk_server, tmp_path, python, pip_spec: str | None):
    if not cmd_exists("docker"):
        pytest.skip("docker is required for the pip-per-Python matrix; not found on PATH")
    pkg = unique_package("pip")
    # 2.0 claims a Python nobody has: every pip must read Requires-Python off
    # the index and settle on 1.0, not download 2.0 and fail.
    for version, requires in (("1.0", ">=2.7"), ("2.0", ">=3.99")):
        wheel = make_wheel(
            pkg,
            version,
            tmp_path,
            metadata_extra=f"Requires-Python: {requires}\n",
            python_tag="py2.py3",
        )
        upload_legacy(
            disk_server["legacy"],
            wheel,
            username=disk_server["user"],
            password=disk_server["password"],
            fields={"requires_python": requires},
        )
        wait_for_file_in_index(disk_server["simple"], pkg, wheel.name)

    # Linux shares the host's loopback; Docker Desktop/colima reach it by name.
    host, network = (
        ("127.0.0.1", ["--network", "host"])
        if sys.platform == "linux"
        else ("host.docker.internal", [])
    )
    port = disk_server["bind"].rsplit(":", 1)[1]
    if pip_spec is None:
        setup = "python -m venv /venv\nPATH=/venv/bin:$PATH"
    else:
        setup = f"python -m pip install --quiet --upgrade '{pip_spec}'"
    pip = "python -m pip --disable-pip-version-check"
    script = f"""set -e
{setup}
{pip} --version
{pip} install --no-cache-dir --index-url http://{host}:{port}/simple/ --trusted-host {host} {pkg}
python -c 'import {module_name(pkg)} as m; print("installed " + m.__version__)'
"""
    cp = run_checked(
        ["docker", "run", "--rm", *network, "-e", "PIP_ROOT_USER_ACTION=ignore"]
        + [f"python:{python}-slim", "sh", "-c", script],
        timeout=600,
    )
    assert cp.stdout.strip().endswith("installed 1.0"), cp.stdout + cp.stderr
