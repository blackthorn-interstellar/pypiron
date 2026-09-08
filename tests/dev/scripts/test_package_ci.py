"""Exercise the user-facing script, including a packaging error source tests miss."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]


@pytest.mark.integration
@pytest.mark.parametrize("missing_resource", [False, True])
def test_package_ci_checks_the_installed_wheel(pypiron_bin, tmp_path, missing_resource):
    project = tmp_path / "project"
    shutil.copytree(REPO / "examples/package-ci", project)
    if missing_resource:
        config = project / "pyproject.toml"
        config.write_text(config.read_text() + '\nexclude = ["**/*.txt"]\n')
    result = subprocess.run(
        [
            "bash",
            str(REPO / "dev/scripts/package-ci.sh"),
            str(project),
            "pypiron-ci-example",
            str(project / "check_installed.py"),
        ],
        env={
            **os.environ,
            "PYPIRON_BIN": str(pypiron_bin),
            "TMPDIR": str(tmp_path),
            # An existing deployment's read auth must not contaminate the
            # temporary registry or make this example require user secrets.
            "PYPIRON_READ_USER": "existing-deployment",
            "PYPIRON_READ_PASS": "existing-password",
        },
        capture_output=True,
        text=True,
        timeout=120,
    )
    output = result.stdout + result.stderr
    if missing_resource:
        assert result.returncode != 0, output
        assert "FileNotFoundError" in output
        assert "greeting.txt" in output
    else:
        assert result.returncode == 0, output
        assert "PASS: installed wheel, packaged resource, and public dependency" in output
