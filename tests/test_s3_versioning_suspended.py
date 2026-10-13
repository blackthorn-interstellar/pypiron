"""A bucket whose versioning was enabled, then suspended (issue #53).

Such a bucket answers PUT without `x-amz-version-id` but GET/HEAD with
`x-amz-version-id: null`. The first write to a new project claims its name with
a PUT and re-checks the claim with a GET; reading those two answers as different
versions refused that write with a 409 after the claim had landed, so every new
project silently lost its first file. MinIO is driven through the fault proxy,
which reproduces the real S3 headers.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterator

import pytest

from .conftest import PRIVATE_PATTERN_ARGS, _minio_fault_proxy, _start_s3_server
from .helpers import make_wheel, sha256_file, sync_to, upload_legacy, wait_for_file_in_index
from .test_migrate_sources import _start_source

pytestmark = [pytest.mark.integration, pytest.mark.s3]


@pytest.fixture()
def server(tmp_path_factory, pypiron_bin: Path, minio: Dict) -> Iterator[Dict]:
    proxy_gen = _minio_fault_proxy(minio)
    proxied = next(proxy_gen)
    proxied["faults"].suspend_versioning(proxied["bucket"])
    try:
        yield from _start_s3_server(
            tmp_path_factory, pypiron_bin, proxied, extra_args=PRIVATE_PATTERN_ARGS
        )
    finally:
        proxy_gen.close()


def test_first_upload_to_a_new_project_lands(server, tmp_path):
    wheel = make_wheel("blueowl-fresh", "1.0", tmp_path)
    upload_legacy(server["legacy"], wheel, username=server["user"], password=server["password"])
    wait_for_file_in_index(server["simple"], "blueowl-fresh", wheel.name)


def test_as_private_sync_lands_every_file_of_new_projects(server, pypiron_bin, tmp_path):
    projects = {"blueowl-multi": ["1.0", "2.0", "3.0"], "blueowl-single": ["1.0"]}
    routes = {}
    wheels = {}
    for pkg, versions in projects.items():
        wheels[pkg] = [make_wheel(pkg, v, tmp_path) for v in versions]
        files = []
        for w in wheels[pkg]:
            routes[f"/files/{w.name}"] = (200, "application/octet-stream", w.read_bytes())
            files.append(
                {
                    "filename": w.name,
                    "url": f"/files/{w.name}",
                    "hashes": {"sha256": sha256_file(w)},
                    "size": w.stat().st_size,
                }
            )
        doc = {"meta": {"api-version": "1.0"}, "name": pkg, "files": files}
        routes[f"/simple/{pkg}/"] = (
            200,
            "application/vnd.pypi.simple.v1+json",
            json.dumps(doc).encode(),
        )

    source_gen = _start_source(routes)
    source_url, _ = next(source_gen)
    try:
        rc, out, err = sync_to(
            pypiron_bin,
            server,
            *(arg for pkg in projects for arg in ("--include-package", pkg)),
            "--as-private",
            source=f"{source_url}/simple",
        )
    finally:
        source_gen.close()
    assert rc == 0, f"{out}\n{err}"
    for pkg, pkg_wheels in wheels.items():
        for w in pkg_wheels:
            wait_for_file_in_index(server["simple"], pkg, w.name)
