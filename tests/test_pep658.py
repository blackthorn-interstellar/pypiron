"""Milestone 7: PEP 658/714 — wheel METADATA served as a static companion file.

The end-to-end proof: uv resolves dependencies by fetching
`<artifact-url>.metadata` and never downloads the wheel itself.
"""

from __future__ import annotations

import re

import pytest

from .helpers import (
    download_pypi_wheel,
    http_get,
    run_checked,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
VERSION = "1.17.0"

pytestmark = pytest.mark.integration


@pytest.fixture()
def metadata_server(disk_server_access_log, tmp_path):
    disk_server = disk_server_access_log  # access log on: lets us see client fetches
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel_path,
        username=disk_server["user"],
        password=disk_server["password"],
        fields={"requires_python": ">=2.7"},
    )
    index = wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)
    return {**disk_server, "wheel_path": wheel_path, "package_index": index}


def test_metadata_file_served(metadata_server):
    wheel_name = metadata_server["wheel_path"].name
    url = f"{metadata_server['base_url']}/files/{PACKAGE}/{wheel_name}.metadata"
    code, body, headers = http_get(url)
    assert code == 200
    text = body.decode("utf-8")
    assert text.startswith("Metadata-Version:")
    assert "Name: six" in text
    # Tied to an immutable artifact, so cached like one.
    assert headers["cache-control"] == "public, max-age=31536000, immutable"

    # Sidecar JSON is truth, not API surface: never served.
    code, _, _ = http_get(f"{metadata_server['base_url']}/files/{PACKAGE}/{wheel_name}.meta.json")
    assert code == 404


def test_index_advertises_core_metadata_and_requires_python(metadata_server):
    (entry,) = metadata_server["package_index"]["files"]
    assert entry["core-metadata"] is True
    assert entry["dist-info-metadata"] is True
    assert entry["requires-python"] == ">=2.7"

    _, body, _ = http_get(f"{metadata_server['simple']}{PACKAGE}/")
    html = body.decode("utf-8")
    assert 'data-core-metadata="true"' in html
    assert 'data-dist-info-metadata="true"' in html
    assert 'data-requires-python="&gt;=2.7"' in html


@pytest.mark.compat("uv", "pep658-metadata")
@pytest.mark.compat("uv", "resolve")
def test_uv_resolves_without_downloading_the_wheel(metadata_server, tmp_path, uv_path):
    wheel_name = metadata_server["wheel_path"].name
    reqs = tmp_path / "requirements.in"
    reqs.write_text(f"{PACKAGE}\n")
    out = tmp_path / "requirements.txt"

    run_checked(
        [
            uv_path,
            "pip",
            "compile",
            str(reqs),
            "-o",
            str(out),
            "--index-url",
            metadata_server["simple"],
            "--no-cache",
        ],
        timeout=120,
    )
    assert f"{PACKAGE}=={VERSION}" in out.read_text()

    log = metadata_server["log_path"].read_text()
    assert f"path=/files/{PACKAGE}/{wheel_name}.metadata" in log, (
        "uv should fetch the PEP 658 metadata companion"
    )
    # The access-log path is `path=/files/<pkg>/<wheel> ` (space before the next
    # field); the metadata companion is `<wheel>.metadata`, so a trailing space
    # selects bare-wheel fetches only.
    wheel_fetches = re.findall(rf"path=/files/{re.escape(PACKAGE)}/{re.escape(wheel_name)} ", log)
    assert not wheel_fetches, "resolution must not download the wheel itself"
