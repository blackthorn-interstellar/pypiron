from __future__ import annotations

import os
import uuid

import pytest

from .helpers import make_wheel, run_checked, uvx_client, wait_for_file_in_index

VERSION = "0.1.0"

pytestmark = pytest.mark.integration


@pytest.mark.compat("flit", "upload")
def test_flit_publish_uploads_to_legacy_index(disk_server, tmp_path):
    package = f"pypiron-compat-flit-{uuid.uuid4().hex[:8]}"
    module = package.replace("-", "_")
    project_dir = tmp_path / "flit-project"
    project_dir.mkdir()

    (project_dir / "pyproject.toml").write_text(
        f"""[build-system]
requires = ["flit_core>=3.2,<4"]
build-backend = "flit_core.buildapi"

[project]
name = "{package}"
version = "{VERSION}"
description = "pypiron flit compatibility package"
requires-python = ">=3.8"
""",
        encoding="utf-8",
    )
    (project_dir / f"{module}.py").write_text(
        f'"""pypiron flit compatibility package."""\n\n__version__ = "{VERSION}"\n',
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.update(
        {
            "FLIT_INDEX_URL": disk_server["legacy"],
            "FLIT_USERNAME": disk_server["user"],
            "FLIT_PASSWORD": disk_server["password"],
        }
    )
    run_checked([*uvx_client("flit"), "publish"], cwd=project_dir, env=env, timeout=300)

    wheels = sorted((project_dir / "dist").glob("*.whl"))
    assert len(wheels) == 1
    wait_for_file_in_index(disk_server["simple"], package, wheels[0].name)


@pytest.mark.compat("hatch", "upload")
def test_hatch_publish_uploads_to_legacy_index(disk_server, tmp_path):
    package = f"pypiron-compat-hatch-{uuid.uuid4().hex[:8]}"
    wheel_path = make_wheel(package, VERSION, tmp_path / "dist")

    # hatch errors out if HATCH_CONFIG points at a missing file.
    config_path = tmp_path / "hatch-config.toml"
    config_path.write_text("", encoding="utf-8")

    env = os.environ.copy()
    env.update(
        {
            "HATCH_CACHE_DIR": str(tmp_path / "hatch-cache"),
            "HATCH_CONFIG": str(config_path),
            "HATCH_INTERACTIVE": "false",
        }
    )
    run_checked(
        [
            *uvx_client("hatch"),
            "publish",
            "--yes",
            "--repo",
            disk_server["legacy"],
            "--user",
            disk_server["user"],
            "--auth",
            disk_server["password"],
            str(wheel_path),
        ],
        env=env,
        timeout=300,
    )

    wait_for_file_in_index(disk_server["simple"], package, wheel_path.name)
