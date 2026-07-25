"""Storage-format read gate, end to end against the real binary.

A fresh tree is format 1 and carries no stamp; pypiron writes nothing under
`_format/`. A future format bump will stamp `_format/stamp.json` with a higher
number, and an older binary must REFUSE to start against it instead of writing
format-1 shapes into a format-2 tree. These tests prove that refusal — for
`serve` and for the headless `rebuild-index` write path — and prove the happy
path (absent stamp, format-1 stamp, and a format-1 stamp carrying an unknown
future field) still starts and serves.

The gate never creates or rewrites the stamp: a fresh tree stays stampless, and
a hand-written format-1 stamp is byte-identical after a restart.
"""

from __future__ import annotations

import contextlib
import os
import subprocess
from pathlib import Path
from typing import Dict, Iterator

import pytest

from .helpers import (
    find_free_port,
    http_get,
    kill_process_tree,
    make_wheel,
    parse_dist_filename,
    unique_package,
    upload_legacy,
    wait_for_file_in_index,
    wait_http_responding,
)

pytestmark = pytest.mark.integration

FORMAT_STAMP_REL = Path("_format") / "stamp.json"


def _write_stamp(data_dir: Path, body: bytes) -> Path:
    """Hand-write the format stamp into a disk tree, the way foreign tooling (or a
    future format-2 pypiron) would."""
    stamp = data_dir / FORMAT_STAMP_REL
    stamp.parent.mkdir(parents=True, exist_ok=True)
    stamp.write_bytes(body)
    return stamp


@contextlib.contextmanager
def _serve(bin_path: Path, data_dir: Path) -> Iterator[Dict]:
    """A disk-mode server on `data_dir`, killed on exit. No upstream proxy."""
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
        "--uploader-user",
        "uploader",
        "--uploader-pass",
        "uploadersecret",
        "--worker-interval-secs",
        "1",
    ]
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info,pypiron=debug")
    env.setdefault("PYPIRON_ADVISORY_FEED", "")
    with open(log_path, "a") as log_file:
        proc = subprocess.Popen(args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        try:
            wait_http_responding(f"http://{bind}/simple/index.json", timeout=20.0)
            yield {
                "base_url": f"http://{bind}",
                "legacy": f"http://{bind}/legacy/",
                "simple": f"http://{bind}/simple/",
                "uploader_user": "uploader",
                "uploader_password": "uploadersecret",
            }
        finally:
            kill_process_tree(proc)


def _serve_expect_refusal(bin_path: Path, data_dir: Path) -> subprocess.CompletedProcess:
    """Run `serve` expecting the gate to refuse before the listener binds. The
    short timeout doubles as the "it never started serving" assertion: a healthy
    server would block until killed, a refusal exits at once."""
    port = find_free_port()
    env = os.environ.copy()
    env.setdefault("PYPIRON_ADVISORY_FEED", "")
    try:
        return subprocess.run(
            [
                str(bin_path),
                "serve",
                "--bind-addr",
                f"127.0.0.1:{port}",
                "--data-dir",
                str(data_dir),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:
        raise AssertionError(
            "serve did not refuse a newer/corrupt storage format; it started serving instead"
        ) from exc


def _upload(server: Dict, wheel: Path) -> None:
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["uploader_user"],
        password=server["uploader_password"],
    )
    name, _ = parse_dist_filename(wheel.name)
    wait_for_file_in_index(server["simple"], name, wheel.name)


def test_fresh_tree_writes_no_format_object(pypiron_bin: Path, tmp_path: Path):
    """A fresh tree is format 1: after a real publish, nothing under `_format/`
    exists. This is the proof that no stamp-creation shipped."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    with _serve(pypiron_bin, data_dir) as server:
        name = unique_package("fresh")
        wheel = make_wheel(name, "1.0.0", tmp_path / "wheels")
        _upload(server, wheel)
        code, _, _ = http_get(f"{server['simple']}{name}/index.json")
        assert code == 200, code
    assert not (data_dir / "_format").exists(), "a fresh tree must carry no _format/ object"


def test_format_one_stamp_starts_and_is_untouched(pypiron_bin: Path, tmp_path: Path):
    """A hand-written format-1 stamp serves, and its bytes are byte-identical
    after the server has run — the gate reads, never writes."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    body = b'{"format": 1}'
    stamp = _write_stamp(data_dir, body)
    with _serve(pypiron_bin, data_dir) as server:
        code, _, _ = http_get(f"{server['simple']}index.json")
        assert code == 200, code
    assert stamp.read_bytes() == body, "the format-1 stamp bytes must be left untouched"


def test_format_one_unknown_field_starts(pypiron_bin: Path, tmp_path: Path):
    """Forward tolerance: a format-1 stamp carrying an unknown future field still
    starts (unknown fields are ignored, no deny_unknown_fields)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_stamp(data_dir, b'{"format": 1, "future_field": "ignored"}')
    with _serve(pypiron_bin, data_dir) as server:
        code, _, _ = http_get(f"{server['simple']}index.json")
        assert code == 200, code


def test_format_two_refuses_naming_both_numbers_and_writer(pypiron_bin: Path, tmp_path: Path):
    """A tree stamped with a newer format refuses startup, nonzero, and the error
    names both formats (2 and 1) and the writer identity."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_stamp(data_dir, b'{"format": 2, "writer": "pypiron 9.9.9 (deadbeef)"}')
    cp = _serve_expect_refusal(pypiron_bin, data_dir)
    output = cp.stdout + cp.stderr
    assert cp.returncode != 0, output
    assert "storage format 2" in output, output
    assert "supported format 1" in output, output
    assert "pypiron 9.9.9 (deadbeef)" in output, output


def test_corrupt_stamp_refuses_naming_key_and_recovery(pypiron_bin: Path, tmp_path: Path):
    """A corrupt stamp refuses (foreign interference, never silently recreated),
    and the error names `_format/stamp.json` and that removal is the recovery."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_stamp(data_dir, b"\x00 not json at all \xff")
    cp = _serve_expect_refusal(pypiron_bin, data_dir)
    output = cp.stdout + cp.stderr
    assert cp.returncode != 0, output
    assert "_format/stamp.json" in output, output
    assert "remove the object" in output, output


def test_rebuild_index_refuses_on_format_two(pypiron_bin: Path, tmp_path: Path):
    """The headless write path is gated too: `rebuild-index` against a format-2
    tree refuses with the same error shape — proof the gate is reachable outside
    serve."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_stamp(data_dir, b'{"format": 2, "writer": "pypiron 9.9.9 (deadbeef)"}')
    cp = subprocess.run(
        [str(pypiron_bin), "rebuild-index", "--data-dir", str(data_dir)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    output = cp.stdout + cp.stderr
    assert cp.returncode != 0, output
    assert "storage format 2" in output, output
    assert "supported format 1" in output, output
    assert "pypiron 9.9.9 (deadbeef)" in output, output


# ------------------------------- multi-bucket --------------------------------


@pytest.mark.s3
def test_multibucket_refuses_when_one_bucket_is_newer(
    tmp_path_factory, pypiron_bin: Path, minio_two: Dict
):
    """A multi-bucket fleet refuses startup when ANY reachable bucket is stamped
    with a newer format, naming the offending bucket."""
    from .conftest import _s3_env, minio_put_key_in

    buckets = minio_two["buckets"]
    newer = buckets[1]
    minio_put_key_in(
        minio_two,
        newer,
        "_format/stamp.json",
        '{"format": 2, "writer": "pypiron 9.9.9 (deadbeef)"}',
    )

    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _s3_env(minio_two, bind)
    env["PYPIRON_ADVISORY_FEED"] = ""
    log_path = tmp_path_factory.mktemp("pypiron-fmt-s3") / "server.log"
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            [str(pypiron_bin), "serve"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        # Bind before the try so a communicate() timeout leaves `finally` a valid
        # value and the intended AssertionError propagates, not an UnboundLocalError.
        out = ""
        try:
            out, _ = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            kill_process_tree(proc)
            raise AssertionError(
                "multi-bucket serve did not refuse a newer-format bucket; it started serving"
            )
        finally:
            log_file.write(out or "")
    assert proc.returncode != 0, out
    assert "storage format 2" in out, out
    assert newer in out, out


@pytest.mark.s3
def test_format_gate_front_runs_topology_parse(
    tmp_path_factory, pypiron_bin: Path, minio_two: Dict
):
    """A tree that is BOTH format-newer AND topology-mismatched must refuse with
    the actionable format error, never the confusing topology error: the format
    gate is contractually wired before the topology parse."""
    from .conftest import _s3_env, minio_put_key_in

    buckets = minio_two["buckets"]
    newer = buckets[1]
    minio_put_key_in(
        minio_two,
        newer,
        "_format/stamp.json",
        '{"format": 2, "writer": "pypiron 9.9.9 (deadbeef)"}',
    )
    # A deliberately foreign topology stamp on the same bucket: were the format
    # gate to run second, this would raise the topology-mismatch error instead.
    minio_put_key_in(
        minio_two,
        newer,
        "_topology/stamp.json",
        '{"buckets": ["foreign-topology"], "hash": "deadbeef", "generation": 1}',
    )

    port = find_free_port()
    bind = f"127.0.0.1:{port}"
    env = _s3_env(minio_two, bind)
    env["PYPIRON_ADVISORY_FEED"] = ""
    log_path = tmp_path_factory.mktemp("pypiron-fmt-order-s3") / "server.log"
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            [str(pypiron_bin), "serve"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        out = ""
        try:
            out, _ = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            kill_process_tree(proc)
            raise AssertionError(
                "serve did not refuse a format-newer + topology-mismatched tree; it started serving"
            )
        finally:
            log_file.write(out or "")
    assert proc.returncode != 0, out
    assert "storage format 2" in out, out
    assert "different bucket topology" not in out, out
