"""Tamper-evident checkpoints: the leader audit writes a hash-chained log under
`_transparency/chain/`, and `pypiron verify-chain` replays it against storage to
catch an out-of-band artifact rewrite — the one attack that fools every check
that trusts the sidecar.

The chain is driven two ways here, both real:
- `pypiron rebuild-index` runs a one-shot leader audit (a separate process, no
  laundering audit can slip in behind it) — used where a tamper must be caught
  before any further audit re-commits it.
- a `--reconcile-interval-secs 2` server sweeps on its own — used to show the
  chain appends on churn.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from .helpers import (
    find_free_port,
    http_request_auth,
    kill_process_tree,
    make_wheel,
    sha256_file,
    upload_legacy,
    wait_for_file_in_index,
    wait_http_responding,
)

pytestmark = pytest.mark.integration


def _cli(bin_path: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(bin_path), *args],
        capture_output=True,
        text=True,
        timeout=60,
    )


def _verify_chain(bin_path: Path, data_dir: Path) -> subprocess.CompletedProcess:
    return _cli(bin_path, "verify-chain", "--data-dir", str(data_dir))


def _rebuild_index(bin_path: Path, data_dir: Path) -> subprocess.CompletedProcess:
    return _cli(bin_path, "rebuild-index", "--data-dir", str(data_dir))


def _chain_links(data_dir: Path) -> list[Path]:
    chain_dir = data_dir / "_transparency" / "chain"
    if not chain_dir.is_dir():
        return []
    # A write-to-tmp-then-rename in flight leaves a `.tmp-<nonce>-...json` sibling
    # (see tmp_sibling in src/storage.rs); it is not committed chain data. Skip it
    # so a parallel run never reads — or parses the sequence of — an uncommitted
    # link (that is how `int(p.stem)` hit `.tmp-...-0000000000000001`).
    return sorted(p for p in chain_dir.glob("*.json") if not p.name.startswith(".tmp-"))


def _artifact_and_sidecar(data_dir: Path, pkg: str) -> tuple[Path, Path]:
    pkg_dir = data_dir / "packages" / pkg
    (artifact,) = [p for p in pkg_dir.glob("*.whl") if not p.name.endswith(".meta.json")]
    sidecar = pkg_dir / f"{artifact.name}.meta.json"
    return artifact, sidecar


@contextlib.contextmanager
def _serve(bin_path: Path, data_dir: Path, extra_args: list[str]):
    """A disk-mode server on a fresh port, killed on exit. Mirrors the conftest
    fixture but lets a test pass one-off flags (e.g. `--transparency false`)."""
    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    log_path = data_dir.parent / f"{data_dir.name}-server.log"
    args = [
        str(bin_path),
        "serve",
        "--bind-addr",
        bind,
        "--data-dir",
        str(data_dir),
        "--admin-user",
        "admin",
        "--admin-pass",
        "secret",
        "--uploader-user",
        "uploader",
        "--uploader-pass",
        "uploadersecret",
        "--worker-interval-secs",
        "1",
        *extra_args,
    ]
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info,pypiron=debug")
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        try:
            wait_http_responding(f"http://{bind}/simple/index.json", timeout=20.0)
            yield {
                "base_url": f"http://{bind}",
                "legacy": f"http://{bind}/legacy/",
                "simple": f"http://{bind}/simple/",
                "user": "admin",
                "password": "secret",
                "data_dir": data_dir,
            }
        finally:
            kill_process_tree(proc)


def _upload(server, wheel: Path) -> None:
    upload_legacy(server["legacy"], wheel, username=server["user"], password=server["password"])
    wait_for_file_in_index(server["simple"], _pkg_from_wheel(wheel), wheel.name)


def _pkg_from_wheel(wheel: Path) -> str:
    return wheel.name.split("-")[0].lower()


# --------------------------------------------------------------------------- #


def test_verify_chain_passes_after_an_audit(disk_server, pypiron_bin, tmp_path):
    """Upload, run one leader audit (rebuild-index), verify-chain exits 0."""
    server = disk_server
    wheel = make_wheel("transchecka", "1.0.0", tmp_path)
    _upload(server, wheel)

    rebuilt = _rebuild_index(pypiron_bin, server["data_dir"])
    assert rebuilt.returncode == 0, rebuilt.stdout + rebuilt.stderr
    assert _chain_links(server["data_dir"]), "audit must have written a chain link"

    cp = _verify_chain(pypiron_bin, server["data_dir"])
    assert cp.returncode == 0, f"expected valid exit 0:\n{cp.stdout}{cp.stderr}"


def test_no_chain_exits_0(pypiron_bin, tmp_path):
    """A store that never ran an audit has no chain — that is not a violation."""
    store = tmp_path / "store"
    store.mkdir()
    cp = _verify_chain(pypiron_bin, store)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert "no chain" in cp.stdout, cp.stdout


def test_consistent_artifact_rewrite_is_caught(disk_server, pypiron_bin, tmp_path):
    """The whole point: an attacker rewrites the artifact bytes AND its sidecar
    sha256 together — consistent, so every check that trusts the sidecar passes.
    The chain committed the original sha, so verify-chain names the file and
    exits 1."""
    server = disk_server
    wheel = make_wheel("transtampa", "2.0.0", tmp_path)
    _upload(server, wheel)
    assert _rebuild_index(pypiron_bin, server["data_dir"]).returncode == 0
    assert _verify_chain(pypiron_bin, server["data_dir"]).returncode == 0

    artifact, sidecar = _artifact_and_sidecar(server["data_dir"], _pkg_from_wheel(wheel))
    # Rewrite the body and rewrite the sidecar's sha256 + size to match: a tamper
    # that is internally consistent and defeats a plain sidecar check.
    tampered = artifact.read_bytes() + b"malicious-payload"
    artifact.write_bytes(tampered)
    meta = json.loads(sidecar.read_text())
    meta["sha256"] = sha256_file(artifact)
    meta["size"] = len(tampered)
    sidecar.write_text(json.dumps(meta))

    cp = _verify_chain(pypiron_bin, server["data_dir"])
    assert cp.returncode == 1, f"tamper must be caught (exit 1):\n{cp.stdout}{cp.stderr}"
    assert "hash-changed" in cp.stdout, cp.stdout
    assert artifact.name in cp.stdout, cp.stdout


def test_in_chain_fingerprint_change_is_a_reviewable_finding_not_a_default_fault(
    disk_server, pypiron_bin, tmp_path
):
    """F37: a filename re-committed under new bytes — the shape a legitimate
    mirror->private supersede AND a forged re-commit both produce — must surface as
    a first-class, counted finding on stdout, non-fatal by default, and fatal only
    under --strict.

    Driven through the real audit: upload (chain seq 0 commits filename@shaM,
    sidecar shaM), consistently rewrite the artifact + sidecar to shaP, re-audit
    (chain seq 1 commits filename@shaP). Storage is now internally consistent, so
    the replay-vs-storage `hash-changed` check stays silent — the ONLY trace is the
    chain holding two shas for one filename, which verify-chain now reports.

    The RED this locks down: the old code emitted the change as a buried stderr
    line and exited 0 with nothing on stdout and no count. GREEN: a
    `fingerprint-changed` row and a `fingerprint-change(s)` count on stdout.
    """
    server = disk_server
    wheel = make_wheel("transfingera", "1.0.0", tmp_path)
    _upload(server, wheel)
    assert _rebuild_index(pypiron_bin, server["data_dir"]).returncode == 0

    artifact, sidecar = _artifact_and_sidecar(server["data_dir"], _pkg_from_wheel(wheel))
    old_sha = json.loads(sidecar.read_text())["sha256"]

    # A consistent rewrite: new bytes + a sidecar sha that matches them, so nothing
    # replay-vs-storage disagrees on.
    tampered = artifact.read_bytes() + b"superseded-or-forged-payload"
    artifact.write_bytes(tampered)
    meta = json.loads(sidecar.read_text())
    new_sha = sha256_file(artifact)
    meta["sha256"] = new_sha
    meta["size"] = len(tampered)
    sidecar.write_text(json.dumps(meta))

    # Re-audit: the chain now commits the same filename under the new sha at a later
    # seq, so both shas live in the chain.
    assert _rebuild_index(pypiron_bin, server["data_dir"]).returncode == 0
    assert len(_chain_links(server["data_dir"])) >= 2, "the re-audit must append a link"

    # Default: reported + counted on stdout, but non-fatal (exit 0).
    cp = _verify_chain(pypiron_bin, server["data_dir"])
    assert cp.returncode == 0, (
        f"a fingerprint change must be non-fatal by default:\n{cp.stdout}{cp.stderr}"
    )
    assert "fingerprint-changed" in cp.stdout, f"the finding must be on stdout:\n{cp.stdout}"
    assert artifact.name in cp.stdout, cp.stdout
    assert old_sha in cp.stdout and new_sha in cp.stdout, cp.stdout
    assert "1 fingerprint-change(s)" in cp.stdout, f"the count must be in the summary:\n{cp.stdout}"
    # It must NOT be counted as a violation.
    assert "hash-changed" not in cp.stdout, cp.stdout
    assert "0 violation(s)" in cp.stdout, f"no genuine violation here:\n{cp.stdout}"

    # --strict promotes it to a hard failure for operators who want a CI gate.
    cp_strict = _cli(pypiron_bin, "verify-chain", "--data-dir", str(server["data_dir"]), "--strict")
    assert cp_strict.returncode == 1, f"--strict must fault:\n{cp_strict.stdout}{cp_strict.stderr}"
    assert "fingerprint-changed" in cp_strict.stdout, cp_strict.stdout


def test_vanished_artifact_is_caught_but_a_real_delete_is_not(disk_server, pypiron_bin, tmp_path):
    """An artifact + sidecar deleted straight off disk (no tombstone) is a
    violation; a delete through the API leaves a tombstone and, after the next
    audit, is clean."""
    server = disk_server

    # An out-of-band disk delete: no tombstone, no audit → "vanished".
    victim = make_wheel("transvanisha", "3.0.0", tmp_path)
    _upload(server, victim)
    assert _rebuild_index(pypiron_bin, server["data_dir"]).returncode == 0
    v_art, v_side = _artifact_and_sidecar(server["data_dir"], _pkg_from_wheel(victim))
    v_art.unlink()
    v_side.unlink()

    cp = _verify_chain(pypiron_bin, server["data_dir"])
    assert cp.returncode == 1, f"disk delete must be caught:\n{cp.stdout}{cp.stderr}"
    assert "vanished" in cp.stdout, cp.stdout
    assert victim.name in cp.stdout, cp.stdout

    # A legitimate API delete: tombstone written, then the audit re-commits the
    # package without the file → verify-chain is clean again.
    keep = make_wheel("transdeleteb", "4.0.0", tmp_path)
    _upload(server, keep)
    code, _, _ = http_request_auth(
        "DELETE",
        f"{server['base_url']}/files/{_pkg_from_wheel(keep)}/{keep.name}",
        username=server["user"],
        password=server["password"],
    )
    assert code == 204, code
    # Re-run the audit so the chain reflects the delete.
    assert _rebuild_index(pypiron_bin, server["data_dir"]).returncode == 0

    # The vanished victim above still fails; verify only the legit-delete package
    # in isolation by rebuilding a clean store would be overkill — instead assert
    # the deleted file is NOT among the violations named.
    cp = _verify_chain(pypiron_bin, server["data_dir"])
    assert keep.name not in cp.stdout, f"a tombstoned delete must not be a violation:\n{cp.stdout}"


def test_a_demoted_mirror_record_is_not_a_vanish(disk_server, pypiron_bin, tmp_path):
    """A settled mirror→private supersede drops the demoted record and leaves only
    its `.mirror-quarantined` fence standing. The chain keeps committing the
    withdrawn filename until the next audit, so verify-chain must read that fence
    as the operator's own authorization — otherwise every supersede raises a
    fleet-wide tamper alarm.

    Both halves run against one frozen copy of the store (no server, no audit can
    slip in): the same disappearance faults without the fence and is clean with
    it, so the fence is provably what is doing the work.

    The settle itself is multi-bucket only and is driven end to end over MinIO in
    test_multibucket.py; what is pinned here is verify-chain's verdict on the
    state it leaves behind.
    """
    server = disk_server
    wheel = make_wheel("transdemota", "5.0.0", tmp_path)
    _upload(server, wheel)
    assert _rebuild_index(pypiron_bin, server["data_dir"]).returncode == 0

    # Snapshot the audited store: everything below is offline, so the chain
    # cannot silently re-commit the package and pass the test vacuously.
    store = tmp_path / "demoted-store"
    shutil.copytree(server["data_dir"], store)
    assert _verify_chain(pypiron_bin, store).returncode == 0

    artifact, sidecar = _artifact_and_sidecar(store, _pkg_from_wheel(wheel))
    artifact.unlink()
    sidecar.unlink()

    # Control: the identical disappearance with nothing authorizing it faults.
    cp = _verify_chain(pypiron_bin, store)
    assert cp.returncode == 1, f"an unauthorized disappearance must fault:\n{cp.stdout}{cp.stderr}"
    assert "vanished" in cp.stdout, cp.stdout

    # The fence a demotion leaves behind (`replicate::write_mirror_quarantine_marker`).
    fence = artifact.with_name(f"{artifact.name}.mirror-quarantined")
    fence.write_text(json.dumps({"filename": artifact.name}))

    cp = _verify_chain(pypiron_bin, store)
    assert cp.returncode == 0, f"a demotion fence must not fault:\n{cp.stdout}{cp.stderr}"
    assert "vanished" not in cp.stdout, cp.stdout


def test_chain_appends_on_churn(disk_server_fast_reconcile, pypiron_bin, tmp_path):
    """Two audits with an upload between: the chain grows, links are append-only
    and gapless (seq 0, 1, ...)."""
    server = disk_server_fast_reconcile

    _upload(server, make_wheel("transchurna", "1.0.0", tmp_path))
    _wait_links(server["data_dir"], at_least=1)
    first = _chain_links(server["data_dir"])

    _upload(server, make_wheel("transchurnb", "1.0.0", tmp_path))
    _wait_links(server["data_dir"], at_least=len(first) + 1)

    links = _chain_links(server["data_dir"])
    seqs = [int(p.stem) for p in links]
    assert seqs == list(range(seqs[0], seqs[0] + len(seqs))), f"non-gapless chain: {seqs}"
    assert seqs[0] == 0, f"chain must start at seq 0: {seqs}"
    assert _verify_chain(pypiron_bin, server["data_dir"]).returncode == 0


def test_transparency_false_writes_no_links(pypiron_bin, tmp_path):
    """`--transparency false` stops the audit writing links; the namespace stays
    empty even after churn and repeated sweeps."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    with _serve(
        pypiron_bin,
        data_dir,
        extra_args=["--transparency", "false", "--reconcile-interval-secs", "2"],
    ) as server:
        _upload(server, make_wheel("transoffa", "1.0.0", tmp_path))
        # Give the sweep several intervals to (not) write anything.
        import time

        time.sleep(6)
        assert _chain_links(data_dir) == [], "no links must be written with transparency off"

    # verify-chain against the (chainless) store is still a clean 0.
    cp = _verify_chain(pypiron_bin, data_dir)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    assert "no chain" in cp.stdout, cp.stdout


def _wait_links(data_dir: Path, at_least: int, timeout: float = 15.0) -> None:
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        if len(_chain_links(data_dir)) >= at_least:
            return
        time.sleep(0.2)
    raise AssertionError(
        f"chain did not reach {at_least} link(s); have {len(_chain_links(data_dir))}"
    )
