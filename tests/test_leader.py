"""Milestone 11: sloppy leader election over an S3 conditional-write lease.

Two nodes share one bucket. Only the leader rebuilds indexes; killing it
hands the lease to the survivor within the TTL and uploads keep flowing.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager

import pytest

from .conftest import _start_s3_server
from .helpers import (
    download_pypi_wheel,
    get_index_json,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
OLD_VERSION = "1.16.0"
NEW_VERSION = "1.17.0"

pytestmark = [pytest.mark.integration, pytest.mark.s3]

LEASE_ENV = {"PYPIRON_LEASE_TTL_SECS": "3"}


def test_leader_failover(minio, pypiron_bin, tmp_path_factory, tmp_path):
    start = contextmanager(_start_s3_server)
    with ExitStack() as stack:
        # A starts first and takes the lease; B joins as a follower.
        server_a = stack.enter_context(
            start(tmp_path_factory, pypiron_bin, minio, extra_env=LEASE_ENV)
        )
        server_b = stack.enter_context(
            start(tmp_path_factory, pypiron_bin, minio, extra_env=LEASE_ENV)
        )

        # Uploads land on any node; the leader indexes them.
        old_wheel = download_pypi_wheel(PACKAGE, OLD_VERSION, tmp_path)
        upload_legacy(
            server_b["legacy"], old_wheel, username=server_b["user"], password=server_b["password"]
        )
        wait_for_file_in_index(server_b["simple"], PACKAGE, old_wheel.name)

        log_a = server_a["log_path"].read_text()
        log_b = server_b["log_path"].read_text()
        assert "lease acquired" in log_a, "first node must take the lease"
        assert "lease acquired" not in log_b and "lease stolen" not in log_b, (
            "second node must be a follower while the leader lives"
        )

        # Crash the leader (SIGKILL — no graceful lease release; a SIGTERM
        # hands the lease over and the survivor merely *acquires* it). The
        # survivor must steal the expired lease and keep the pipeline moving.
        server_a["proc"].kill()
        server_a["proc"].wait(timeout=5.0)

        new_wheel = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
        upload_legacy(
            server_b["legacy"], new_wheel, username=server_b["user"], password=server_b["password"]
        )
        wait_for_file_in_index(server_b["simple"], PACKAGE, new_wheel.name, timeout=30.0)
        assert "lease stolen" in server_b["log_path"].read_text()


def test_wait_on_upload_is_visible_on_a_warm_follower(
    minio, pypiron_bin, tmp_path_factory, tmp_path
):
    """`--wait-on-upload` promises index visibility at the 200. On a follower
    whose index cache is warm (any busy node), that means outlasting the cache
    entry filled before the leader rebuilt the index, not just seeing the
    rebuilt index in storage."""
    start = contextmanager(_start_s3_server)
    env = {
        "PYPIRON_WAIT_ON_UPLOAD": "true",
        "PYPIRON_AUDIT_ON_BOOT": "false",
        "PYPIRON_INDEX_CACHE_TTL_SECS": "5",
    }
    with ExitStack() as stack:
        stack.enter_context(start(tmp_path_factory, pypiron_bin, minio, extra_env=env))
        follower = stack.enter_context(start(tmp_path_factory, pypiron_bin, minio, extra_env=env))
        creds = {"username": follower["user"], "password": follower["password"]}
        first = make_wheel("waitpkg", "1.0", tmp_path)
        upload_legacy(follower["legacy"], first, **creds)
        wait_for_file_in_index(follower["simple"], "waitpkg", first.name)
        for minor in range(1, 5):
            get_index_json(follower["simple"], "waitpkg")  # warm the cache
            wheel = make_wheel("waitpkg", f"1.{minor}", tmp_path)
            upload_legacy(follower["legacy"], wheel, **creds)
            files = [f["filename"] for f in get_index_json(follower["simple"], "waitpkg")["files"]]
            assert wheel.name in files, f"{wheel.name} missing right after its 200"
