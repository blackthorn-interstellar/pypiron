"""Standards conformance over HTTP: PEP 503, 629, 691, 700.

One wheel is uploaded, then every assertion runs against the read endpoints
exactly as a client would see them.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from .helpers import (
    ACCEPT_PEP691,
    download_pypi_wheel,
    http_get,
    sha256_file,
    upload_legacy,
    wait_for_file_in_index,
)

PACKAGE = "six"
VERSION = "1.17.0"

pytestmark = pytest.mark.integration


@pytest.fixture()
def indexed_server(disk_server, tmp_path):
    """A running server with one real wheel uploaded and indexed."""
    wheel_path = download_pypi_wheel(PACKAGE, VERSION, tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel_path,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    index = wait_for_file_in_index(disk_server["simple"], PACKAGE, wheel_path.name)
    return {**disk_server, "wheel_path": wheel_path, "package_index": index}


def test_pep503_global_html(indexed_server):
    code, body, headers = http_get(f"{indexed_server['simple']}")
    assert code == 200
    assert headers["content-type"].startswith("text/html")
    html = body.decode("utf-8")
    # PEP 629 repository version meta tag
    assert '<meta name="pypi:repository-version"' in html
    # The project is linked under its normalized name
    assert f'href="/simple/{PACKAGE}/"' in html


def test_pep503_package_html_hash_fragment(indexed_server):
    code, body, headers = http_get(f"{indexed_server['simple']}{PACKAGE}/")
    assert code == 200
    assert headers["content-type"].startswith("text/html")
    html = body.decode("utf-8")
    assert '<meta name="pypi:repository-version"' in html
    sha = sha256_file(indexed_server["wheel_path"])
    assert f"#sha256={sha}" in html, "anchor must carry the sha256 fragment"


def test_pep691_content_negotiation(indexed_server):
    # PEP 691 media type gets JSON
    code, body, headers = http_get(
        f"{indexed_server['simple']}{PACKAGE}/", headers={"Accept": ACCEPT_PEP691}
    )
    assert code == 200
    assert headers["content-type"].startswith("application/vnd.pypi.simple.v1+json")
    # No Accept header gets HTML
    code, _, headers = http_get(f"{indexed_server['simple']}{PACKAGE}/")
    assert code == 200
    assert headers["content-type"].startswith("text/html")


def test_pep691_global_json(indexed_server):
    code, body, headers = http_get(
        f"{indexed_server['simple']}", headers={"Accept": ACCEPT_PEP691}
    )
    assert code == 200
    import json

    doc = json.loads(body)
    assert doc["meta"]["api-version"] == "1.1"
    assert PACKAGE in [p["name"] for p in doc["projects"]]


def test_pep700_package_json_fields(indexed_server):
    doc = indexed_server["package_index"]
    wheel_path = indexed_server["wheel_path"]

    assert doc["meta"]["api-version"] == "1.1"
    assert doc["name"] == PACKAGE
    assert VERSION in doc["versions"]

    (entry,) = [f for f in doc["files"] if f["filename"] == wheel_path.name]
    assert entry["hashes"]["sha256"] == sha256_file(wheel_path)
    assert entry["size"] == wheel_path.stat().st_size

    # upload-time: RFC 3339, and recent (this server received it moments ago)
    uploaded = datetime.fromisoformat(entry["upload-time"].replace("Z", "+00:00"))
    assert abs(datetime.now(timezone.utc) - uploaded) < timedelta(hours=1)


def test_pep503_name_normalization(indexed_server):
    # Non-normalized lookups serve the normalized package
    for variant in ("SIX", "Six"):
        code, body, _ = http_get(
            f"{indexed_server['simple']}{variant}/", headers={"Accept": ACCEPT_PEP691}
        )
        assert code == 200, f"/simple/{variant}/ should resolve to {PACKAGE}"
        import json

        assert json.loads(body)["name"] == PACKAGE
