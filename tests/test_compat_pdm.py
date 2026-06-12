"""Client compatibility: PDM upload, resolve, and install."""

from __future__ import annotations

import os
import re
import sys
import uuid
from pathlib import Path
from typing import Optional

import pytest

from .helpers import (
    make_wheel,
    run_checked,
    upload_legacy,
    uvx_client,
    wait_for_file_in_index,
)

VERSION = "1.0.0"

pytestmark = pytest.mark.integration


def _unique_package() -> str:
    return f"pypiron-compat-pdm-{uuid.uuid4().hex[:8]}"


def _module_name(package: str) -> str:
    return re.sub(r"\W+", "_", package).strip("_").lower()


def _pdm_env(tmp_path: Path, *, simple_url: Optional[str] = None) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "PDM_CACHE_DIR": str(tmp_path / "pdm-cache"),
            "PDM_CHECK_UPDATE": "false",
            "PDM_HOME": str(tmp_path / "pdm-home"),
            "PDM_IGNORE_SAVED_PYTHON": "1",
            "PDM_USE_VENV": "true",
            "PDM_VENV_IN_PROJECT": "true",
        }
    )
    if simple_url is not None:
        env["PDM_PYPI_URL"] = simple_url
    return env


def _write_pyproject(
    project_dir: Path,
    *,
    name: str,
    dependencies: Optional[list[str]] = None,
    distribution: Optional[bool] = None,
) -> None:
    lines = [
        "[project]",
        f'name = "{name}"',
        f'version = "{VERSION}"',
        'requires-python = ">=3.9"',
    ]
    if dependencies:
        lines.append("dependencies = [")
        lines.extend(f'    "{dependency}",' for dependency in dependencies)
        lines.append("]")
    if distribution is not None:
        lines.extend(["", "[tool.pdm]", f"distribution = {str(distribution).lower()}"])
    lines.append("")
    (project_dir / "pyproject.toml").write_text("\n".join(lines), encoding="utf-8")


@pytest.mark.compat("pdm", "upload")
def test_pdm_publish_uploads_to_legacy_endpoint(disk_server, tmp_path):
    package = _unique_package()
    project_dir = tmp_path / "publisher"
    dist_dir = project_dir / "dist"
    project_dir.mkdir()
    wheel_path = make_wheel(package, VERSION, dist_dir)
    _write_pyproject(project_dir, name=package)

    run_checked(
        [
            *uvx_client("pdm"),
            "-n",
            "publish",
            "--no-build",
            "--repository",
            disk_server["legacy"],
            "--username",
            disk_server["user"],
            "--password",
            disk_server["password"],
        ],
        cwd=project_dir,
        env=_pdm_env(tmp_path),
        timeout=300,
    )

    wait_for_file_in_index(disk_server["simple"], package, wheel_path.name)


@pytest.mark.compat("pdm", "install")
@pytest.mark.compat("pdm", "resolve")
def test_pdm_locks_and_installs_from_simple_index(disk_server, tmp_path):
    package = _unique_package()
    wheel_path = make_wheel(package, VERSION, tmp_path / "dist")
    upload_legacy(
        disk_server["legacy"],
        wheel_path,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_file_in_index(disk_server["simple"], package, wheel_path.name)

    consumer_dir = tmp_path / "consumer"
    consumer_dir.mkdir()
    _write_pyproject(
        consumer_dir,
        name="consumer",
        dependencies=[f"{package}=={VERSION}"],
        distribution=False,
    )
    pdm = uvx_client("pdm")
    env = _pdm_env(tmp_path, simple_url=disk_server["simple"])

    run_checked(
        [*pdm, "-n", "use", "--first", sys.executable],
        cwd=consumer_dir,
        env=env,
        timeout=300,
    )
    run_checked([*pdm, "-n", "lock"], cwd=consumer_dir, env=env, timeout=300)
    assert package in (consumer_dir / "pdm.lock").read_text(encoding="utf-8")

    run_checked([*pdm, "-n", "install"], cwd=consumer_dir, env=env, timeout=300)
    run_checked(
        [*pdm, "run", "python", "-c", f"import {_module_name(package)}"],
        cwd=consumer_dir,
        env=env,
        timeout=300,
    )
