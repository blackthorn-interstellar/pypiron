"""Milestone 11: sloppy leader election over an S3 conditional-write lease.

Two nodes share one bucket. Only the leader rebuilds indexes; killing it
hands the lease to the survivor within the TTL and uploads keep flowing.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager

import pytest

from .conftest import _start_s3_server
from .helpers import download_pypi_wheel, kill_process_tree, upload_legacy, wait_for_file_in_index

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

        # Kill the leader; the survivor steals the expired lease and the
        # pipeline keeps moving.
        kill_process_tree(server_a["proc"])

        new_wheel = download_pypi_wheel(PACKAGE, NEW_VERSION, tmp_path)
        upload_legacy(
            server_b["legacy"], new_wheel, username=server_b["user"], password=server_b["password"]
        )
        wait_for_file_in_index(server_b["simple"], PACKAGE, new_wheel.name, timeout=30.0)
        assert "lease stolen" in server_b["log_path"].read_text()
