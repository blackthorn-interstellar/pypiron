from __future__ import annotations

import os
import re
import uuid
from pathlib import Path

import pytest

from .helpers import (
    make_wheel,
    run_checked,
    upload_legacy,
    uvx_client,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration


def _unique_package_name() -> str:
    return f"pypiron-compat-poetry-{uuid.uuid4().hex[:8]}"


def _module_name(package_name: str) -> str:
    return re.sub(r"\W+", "_", package_name).strip("_").lower()


def _poetry_env(
    tmp_path: Path, disk_server, *, in_project_venv: bool = False
) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "POETRY_CACHE_DIR": str(tmp_path / "poetry-cache"),
            "POETRY_CONFIG_DIR": str(tmp_path / "poetry-config"),
            "POETRY_DATA_DIR": str(tmp_path / "poetry-data"),
            "POETRY_REPOSITORIES_PYPIRON_URL": disk_server["legacy"],
            "POETRY_HTTP_BASIC_PYPIRON_USERNAME": disk_server["user"],
            "POETRY_HTTP_BASIC_PYPIRON_PASSWORD": disk_server["password"],
        }
    )
    if in_project_venv:
        env["POETRY_VIRTUALENVS_IN_PROJECT"] = "true"
    return env


def _write_poetry_package(project_dir: Path, package_name: str, version: str) -> None:
    module_dir = project_dir / _module_name(package_name)
    module_dir.mkdir(parents=True)
    (module_dir / "__init__.py").write_text(f'__version__ = "{version}"\n')
    (project_dir / "pyproject.toml").write_text(
        f"""
[project]
name = "{package_name}"
version = "{version}"
description = "pypiron Poetry compatibility package"
requires-python = ">=3.8"

[build-system]
requires = ["poetry-core>=2.0.0,<3.0.0"]
build-backend = "poetry.core.masonry.api"
""".lstrip()
    )


def _write_poetry_consumer(project_dir: Path) -> None:
    (project_dir / "pyproject.toml").write_text(
        """
[project]
name = "pypiron-compat-poetry-consumer"
version = "0.1.0"
description = "Consumer for pypiron Poetry compatibility tests"
requires-python = ">=3.8"
dependencies = []

[tool.poetry]
package-mode = false

[build-system]
requires = ["poetry-core>=2.0.0,<3.0.0"]
build-backend = "poetry.core.masonry.api"
""".lstrip()
    )


@pytest.mark.compat("poetry", "upload")
def test_poetry_build_publish_uploads_to_pypiron(disk_server, tmp_path):
    package_name = _unique_package_name()
    version = "0.1.0"
    project_dir = tmp_path / "publisher"
    project_dir.mkdir()
    _write_poetry_package(project_dir, package_name, version)

    env = _poetry_env(tmp_path, disk_server)
    run_checked(
        [*uvx_client("poetry"), "-n", "build"],
        cwd=project_dir,
        env=env,
        timeout=300,
    )
    run_checked(
        [*uvx_client("poetry"), "-n", "publish", "--repository", "pypiron"],
        cwd=project_dir,
        env=env,
        timeout=300,
    )

    wheels = list((project_dir / "dist").glob("*.whl"))
    assert len(wheels) == 1
    wait_for_file_in_index(disk_server["simple"], package_name, wheels[0].name)


@pytest.mark.compat("poetry", "install")
@pytest.mark.compat("poetry", "resolve")
def test_poetry_add_resolves_from_pypiron_and_installs(disk_server, tmp_path):
    package_name = _unique_package_name()
    version = "0.1.0"
    wheel_path = make_wheel(package_name, version, tmp_path / "packages")
    upload_legacy(
        disk_server["legacy"],
        wheel_path,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_file_in_index(disk_server["simple"], package_name, wheel_path.name)

    consumer_dir = tmp_path / "consumer"
    consumer_dir.mkdir()
    _write_poetry_consumer(consumer_dir)
    env = _poetry_env(tmp_path, disk_server, in_project_venv=True)
    run_checked(
        [
            *uvx_client("poetry"),
            "-n",
            "source",
            "add",
            "--priority=primary",
            "pypiron",
            disk_server["simple"],
        ],
        cwd=consumer_dir,
        env=env,
        timeout=300,
    )
    run_checked(
        [*uvx_client("poetry"), "-n", "add", f"{package_name}=={version}"],
        cwd=consumer_dir,
        env=env,
        timeout=300,
    )

    lock_text = (consumer_dir / "poetry.lock").read_text()
    assert package_name in lock_text
    run_checked(
        [
            *uvx_client("poetry"),
            "-n",
            "run",
            "python",
            "-c",
            f"import {_module_name(package_name)}",
        ],
        cwd=consumer_dir,
        env=env,
        timeout=300,
    )
