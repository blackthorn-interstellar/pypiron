"""Performance floors for the hot read endpoints, release binary only.

Numbers are comparative (the Python client is the bottleneck); assertions only
catch catastrophic regressions. Run with `make perf`.
"""

from __future__ import annotations

import pytest

from .helpers import (
    bench_endpoint,
    download_pypi_wheel,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
VERSION = "1.17.0"

pytestmark = [pytest.mark.perf, pytest.mark.integration]


def test_hot_read_endpoints(disk_server_release, tmp_path):
    server = disk_server_release
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(
        server["legacy"], wheel_path, username=server["user"], password=server["password"]
    )
    wait_for_file_in_index(server["simple"], PACKAGE, wheel_path.name)

    endpoints = {
        "global-index-json": f"{server['simple']}index.json",
        "package-index-html": f"{server['simple']}{PACKAGE}/",
        "package-index-json": f"{server['simple']}{PACKAGE}/index.json",
        "artifact-download": f"{server['base_url']}/files/{PACKAGE}/{wheel_path.name}",
    }

    failures = []
    for label, url in endpoints.items():
        result = bench_endpoint(url, duration=2.0, concurrency=8)
        print(
            f"{label}: {result['rps']:.0f} rps, "
            f"p50={result['p50_ms']:.1f}ms p95={result['p95_ms']:.1f}ms p99={result['p99_ms']:.1f}ms"
        )
        # Loose floor: an order of magnitude below any sane localhost number.
        if result["rps"] < 50:
            failures.append((label, result))

    assert not failures, f"Catastrophically slow endpoints: {failures}"
