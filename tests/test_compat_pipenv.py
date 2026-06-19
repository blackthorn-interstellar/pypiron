"""Client compatibility: pipenv resolve and install.

pipenv has no publish command, so it only consumes the Simple index — the same
shape as the pip tests. It reads sources from a Pipfile `[[source]]`, locks to
Pipfile.lock, and installs into a virtualenv it manages.
"""

from __future__ import annotations

import os
import re
import sys
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

VERSION = "1.0.0"

pytestmark = pytest.mark.integration


def _unique_package() -> str:
    return f"pypiron-compat-pipenv-{uuid.uuid4().hex[:8]}"


def _module_name(package: str) -> str:
    return re.sub(r"\W+", "_", package).strip("_").lower()


def _pipenv_env(tmp_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            # Keep the venv and all caches inside tmp_path so runs are hermetic
            # and leave nothing in the user's WORKON_HOME / pip cache.
            "PIPENV_VENV_IN_PROJECT": "1",
            "PIPENV_IGNORE_VIRTUALENVS": "1",
            "PIPENV_NOSPIN": "1",
            "PIPENV_DONT_LOAD_ENV": "1",
            "WORKON_HOME": str(tmp_path / "workon"),
            "PIPENV_CACHE_DIR": str(tmp_path / "pipenv-cache"),
            "PIP_CACHE_DIR": str(tmp_path / "pip-cache"),
        }
    )
    return env


def _write_pipfile(project_dir: Path, *, simple_url: str, package: str) -> None:
    # The loopback simple URL is http, which pip/pipenv treat as a secure origin
    # for 127.0.0.1, so verify_ssl = false needs no extra trusted-host wiring.
    contents = "\n".join(
        [
            "[[source]]",
            f'url = "{simple_url}"',
            "verify_ssl = false",
            'name = "pypiron"',
            "",
            "[packages]",
            f'{package} = "=={VERSION}"',
            "",
            "[requires]",
            'python_version = "3"',
            "",
        ]
    )
    (project_dir / "Pipfile").write_text(contents, encoding="utf-8")


@pytest.mark.compat("pipenv", "install")
@pytest.mark.compat("pipenv", "resolve")
def test_pipenv_locks_and_installs_from_simple_index(disk_server, tmp_path):
    package = _unique_package()
    wheel_path = make_wheel(package, VERSION, tmp_path / "dist")
    upload_legacy(
        disk_server["legacy"],
        wheel_path,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_file_in_index(disk_server["simple"], package, wheel_path.name)

    project_dir = tmp_path / "consumer"
    project_dir.mkdir()
    _write_pipfile(project_dir, simple_url=disk_server["simple"], package=package)

    pipenv = uvx_client("pipenv")
    env = _pipenv_env(tmp_path)

    # `install` resolves against our source, writes Pipfile.lock, and installs
    # into the in-project venv — covering both the resolve and install features.
    run_checked(
        [*pipenv, "install", "--python", sys.executable],
        cwd=project_dir,
        env=env,
        timeout=300,
    )
    assert package in (project_dir / "Pipfile.lock").read_text(encoding="utf-8")

    run_checked(
        [*pipenv, "run", "python", "-c", f"import {_module_name(package)}"],
        cwd=project_dir,
        env=env,
        timeout=300,
    )
