"""Execute both production artifact verifiers against fuzz-lock tampering."""

import hashlib
import os
import re
import subprocess
import textwrap
from pathlib import Path

import pytest

WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/nightly-deps.yml"


@pytest.mark.parametrize("consumer", [0, 1], ids=["publisher", "repair-agent"])
@pytest.mark.parametrize("mutation", ["none", "changed", "deleted", "missing-digest"])
def test_fuzz_lock_artifact_verification(tmp_path, consumer, mutation):
    blocks = WORKFLOW.read_text().split(
        "      - name: The banked lockfiles and report are the ones the bump produced\n"
    )[1:]
    assert len(blocks) == 2
    body = blocks[consumer].split("        run: |\n", 1)[1]
    script = textwrap.dedent(re.split(r"\n      - ", body, maxsplit=1)[0])
    bank = tmp_path / ("deps" if consumer == 0 else ".local/deps")
    env = dict(os.environ, RUNNER_TEMP=str(tmp_path))
    for path, var in [
        ("uv.lock", "UV_LOCK_SHA"),
        ("Cargo.lock", "CARGO_LOCK_SHA"),
        ("fuzz/Cargo.lock", "FUZZ_LOCK_SHA"),
        ("report.md", "REPORT_SHA"),
    ]:
        dest = bank / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(f"banked {path}\n")
        env[var] = hashlib.sha256(dest.read_bytes()).hexdigest()
    fuzz = bank / "fuzz/Cargo.lock"
    if mutation == "changed":
        fuzz.write_text("tampered\n")
    elif mutation == "deleted":
        fuzz.unlink()
    elif mutation == "missing-digest":
        env.pop("FUZZ_LOCK_SHA")
    result = subprocess.run(
        ["bash", "-c", script], cwd=tmp_path, env=env, capture_output=True, text=True
    )
    assert result.returncode == (0 if mutation == "none" else 1), result.stdout + result.stderr
    if mutation != "none":
        assert "::error::fuzz/Cargo.lock" in result.stdout
