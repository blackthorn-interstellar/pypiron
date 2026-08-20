"""Orphan-companion cleanup: the leader audit drops sidecar/.metadata/.provenance
objects stranded beside an artifact that no longer exists.

The hazard is that a live writer looks exactly like debris while it is between
its two writes — the proxy cache fill and the replication copy both publish the
sidecar *before* the artifact body, so the "no body beside this sidecar" shape
holds for a whole download. The sweep must therefore only touch companions that
are older than the intent grace and whose package has no live intent marker.
`pypiron rebuild-index` runs the sweep as a one-shot audit in its own process,
so each scenario is deterministic: seed the tree, run one audit, look.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest

from .helpers import make_wheel, upload_legacy, wait_for_file_in_index

pytestmark = pytest.mark.integration

# The headless audit (`rebuild-index`) always runs with the default 900s intent
# grace, so "aged" here means older than that.
INTENT_GRACE_SECS = 900


def _rebuild_index(bin_path: Path, data_dir: Path) -> subprocess.CompletedProcess:
    cp = subprocess.run(
        [str(bin_path), "rebuild-index", "--data-dir", str(data_dir)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert cp.returncode == 0, cp.stdout + cp.stderr
    return cp


def _strand_companions(server, tmp_path: Path, pkg: str) -> tuple[Path, list[Path]]:
    """Upload a wheel, then remove just the artifact body — leaving its
    companions behind exactly as a half-finished writer would."""
    wheel = make_wheel(pkg, "1.0.0", tmp_path)
    upload_legacy(server["legacy"], wheel, username=server["user"], password=server["password"])
    wait_for_file_in_index(server["simple"], pkg, wheel.name)

    pkg_dir = Path(server["data_dir"]) / "packages" / pkg
    artifact = pkg_dir / wheel.name
    companions = [p for p in pkg_dir.iterdir() if p.name.startswith(wheel.name) and p != artifact]
    assert companions, f"upload must leave companions beside {wheel.name}"
    artifact.unlink()
    return artifact, companions


def _age(paths: list[Path], seconds: float) -> None:
    old = time.time() - seconds
    for path in paths:
        os.utime(path, (old, old))


def test_fresh_stranded_companions_survive_the_sweep(disk_server, pypiron_bin, tmp_path):
    """A sidecar written seconds ago is a writer mid-flight, not debris: the
    sweep must leave it alone even though no artifact anchors it yet."""
    _, companions = _strand_companions(disk_server, tmp_path, "orphanfresh")

    _rebuild_index(pypiron_bin, disk_server["data_dir"])

    still_there = [p.name for p in companions if p.exists()]
    assert still_there == [p.name for p in companions], (
        "companions younger than the intent grace must survive — the proxy cache "
        "fill and the replication copy both publish the sidecar before the body"
    )


def test_aged_stranded_companions_are_cleaned(disk_server, pypiron_bin, tmp_path):
    """Debris is old by definition: past the intent grace, with no live writer,
    the sweep still removes it."""
    _, companions = _strand_companions(disk_server, tmp_path, "orphanaged")
    _age(companions, INTENT_GRACE_SECS * 2)

    _rebuild_index(pypiron_bin, disk_server["data_dir"])

    left = [p.name for p in companions if p.exists()]
    assert left == [], f"aged anchor-less companions must be dropped, found {left}"


def test_one_fresh_companion_saves_the_whole_base(disk_server, pypiron_bin, tmp_path):
    """The age gate is all-or-nothing per artifact: a writer that has published
    some of its companions and is still writing the rest must not be half-swept,
    so one fresh companion protects every companion of that base."""
    _, companions = _strand_companions(disk_server, tmp_path, "orphanmixed")
    assert len(companions) > 1, f"need more than one companion to mix ages: {companions}"
    _age(companions[1:], INTENT_GRACE_SECS * 2)

    _rebuild_index(pypiron_bin, disk_server["data_dir"])

    left = {p.name for p in companions if p.exists()}
    assert left == {p.name for p in companions}, (
        f"one in-grace companion must hold back its aged siblings, missing "
        f"{set(p.name for p in companions) - left}"
    )


def test_aged_companions_kept_while_an_intent_is_live(disk_server, pypiron_bin, tmp_path):
    """A slow writer holds an unpaired intent marker for its package. Aged
    companions plus a live intent means "still writing", not "debris"."""
    pkg = "orphanintent"
    _, companions = _strand_companions(disk_server, tmp_path, pkg)
    _age(companions, INTENT_GRACE_SECS * 2)
    intent = Path(disk_server["data_dir"]) / "_dirty" / f"{pkg}!slow-writer-nonce.intent"
    intent.parent.mkdir(parents=True, exist_ok=True)
    intent.write_bytes(b"")

    _rebuild_index(pypiron_bin, disk_server["data_dir"])

    left = [p.name for p in companions if p.exists()]
    assert left == [p.name for p in companions], (
        f"a live intent marker must hold off the sweep, missing {set(p.name for p in companions) - set(left)}"
    )
