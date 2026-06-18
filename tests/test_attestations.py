"""PEP 740 — provenance relayed through the mirror and proxy paths.

pypiron is a relay, not a verifier: it carries provenance verbatim (the same
play as the PEP 658 `.metadata` companion) so an offline consumer can verify the
original publisher end-to-end. These tests drive the real binary over HTTP.

Most are hermetic — provenance is opaque to pypiron, so a synthetic object
pushed through the mirror-upload path exercises store/serve/advertise/delete
without touching PyPI. One test syncs a genuinely-attested package from PyPI and
asserts the served bytes match its integrity API.
"""

from __future__ import annotations

import json

import pytest

from .helpers import (
    ACCEPT_PEP691,
    get_index_json,
    http_get,
    http_request_auth,
    make_wheel,
    pypi_provenance,
    run_checked,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration

# A minimal but structurally-valid provenance object. pypiron never parses it —
# it relays the bytes — so the exact contents only need to round-trip.
PROVENANCE = json.dumps(
    {
        "version": 1,
        "attestation_bundles": [
            {"publisher": {"kind": "pytest", "claims": {}}, "attestations": []}
        ],
    }
)


def _mirror_upload(server, dist, package, provenance=PROVENANCE):
    """Mirror-upload a dist with a provenance object, as `sync --to` does."""
    fields = {"mirror": "true"}
    if provenance is not None:
        fields["provenance"] = provenance
    upload_legacy(
        server["legacy"],
        dist,
        username=server["admin_user"],
        password=server["admin_password"],
        fields=fields,
    )
    return wait_for_file_in_index(server["simple"], package, dist.name)


def test_mirror_upload_stores_serves_and_advertises_provenance(disk_server, tmp_path):
    pkg = "attestdemo"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    index = _mirror_upload(disk_server, wheel, pkg)

    prov_url = f"{disk_server['base_url']}/files/{pkg}/{wheel.name}.provenance"
    code, body, headers = http_get(prov_url)
    assert code == 200
    assert headers["content-type"] == "application/json"
    # Relayed verbatim, byte-for-byte.
    assert json.loads(body) == json.loads(PROVENANCE)
    # Tied to an immutable artifact, cached like one.
    assert headers["cache-control"] == "public, max-age=31536000, immutable"

    # The index advertises it (JSON `provenance` + api-version 1.4).
    assert index["meta"]["api-version"] == "1.4"
    (entry,) = index["files"]
    assert entry["provenance"] == f"/files/{pkg}/{wheel.name}.provenance"

    # ...and the HTML carries the matching data-provenance attribute.
    _, html, _ = http_get(f"{disk_server['simple']}{pkg}/")
    assert f'data-provenance="/files/{pkg}/{wheel.name}.provenance"' in html.decode()

    # Truth on disk; the sidecar JSON itself is never API surface.
    pkg_dir = disk_server["data_dir"] / "packages" / pkg
    assert (pkg_dir / f"{wheel.name}.provenance").exists()
    code, _, _ = http_get(f"{disk_server['base_url']}/files/{pkg}/{wheel.name}.meta.json")
    assert code == 404


def test_mirror_upload_without_provenance_advertises_nothing(disk_server, tmp_path):
    pkg = "noattest"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    index = _mirror_upload(disk_server, wheel, pkg, provenance=None)

    (entry,) = index["files"]
    assert "provenance" not in entry
    code, _, _ = http_get(f"{disk_server['base_url']}/files/{pkg}/{wheel.name}.provenance")
    assert code == 404


def test_first_party_attestations_are_rejected(disk_server, tmp_path):
    pkg = "firstparty"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    # A non-mirror upload carrying attestations is refused fail-closed: pypiron
    # cannot mint a verifiable provenance object without a Trusted Publisher.
    upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["uploader_user"],
        password=disk_server["uploader_password"],
        fields={"attestations": '[{"version": 1}]'},
        expect_status=400,
    )
    # The same wheel without attestations publishes normally.
    upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["uploader_user"],
        password=disk_server["uploader_password"],
    )
    wait_for_file_in_index(disk_server["simple"], pkg, wheel.name)


def test_delete_removes_the_provenance_companion(disk_server, tmp_path):
    pkg = "delprov"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _mirror_upload(disk_server, wheel, pkg)

    pkg_dir = disk_server["data_dir"] / "packages" / pkg
    assert (pkg_dir / f"{wheel.name}.provenance").exists()

    code, _, _ = http_request_auth(
        "DELETE",
        f"{disk_server['base_url']}/files/{pkg}/{wheel.name}",
        username=disk_server["admin_user"],
        password=disk_server["admin_password"],
    )
    assert code == 204
    assert not (pkg_dir / f"{wheel.name}.provenance").exists()
    code, _, _ = http_get(f"{disk_server['base_url']}/files/{pkg}/{wheel.name}.provenance")
    assert code == 404


def test_proxy_relays_and_caches_provenance(proxy_pair, tmp_path):
    upstream, proxy = proxy_pair["upstream"], proxy_pair["proxy"]
    pkg = "proxyprov"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _mirror_upload(upstream, wheel, pkg)

    # The proxied page re-advertises provenance under the proxy's own URL.
    entry = next(f for f in get_index_json(proxy["simple"], pkg)["files"] if f["filename"] == wheel.name)
    assert entry["provenance"] == f"/files/{pkg}/{wheel.name}.provenance"

    # Before the wheel is cached, provenance streams through from upstream.
    prov_url = f"{proxy['base_url']}/files/{pkg}/{wheel.name}.provenance"
    code, body, _ = http_get(prov_url)
    assert code == 200
    assert json.loads(body) == json.loads(PROVENANCE)
    assert not (proxy["data_dir"] / "packages" / pkg / f"{wheel.name}.provenance").exists()

    # Caching the wheel persists the provenance companion alongside it.
    code, _, _ = http_get(f"{proxy['base_url']}/files/{pkg}/{wheel.name}")
    assert code == 200
    assert (proxy["data_dir"] / "packages" / pkg / f"{wheel.name}.provenance").exists()
    code, body, _ = http_get(prov_url)
    assert code == 200
    assert json.loads(body) == json.loads(PROVENANCE)


# A package genuinely published to PyPI with attestations (GitHub Trusted
# Publishing). Pinned so the synced file is deterministic.
PYPI_ATTESTED_PKG = "sampleproject"
PYPI_ATTESTED_VERSION = "4.0.0"
PYPI_ATTESTED_WHEEL = "sampleproject-4.0.0-py3-none-any.whl"


def test_sync_relays_provenance_from_pypi(disk_server, pypiron_bin, tmp_path):
    # Confirm upstream still serves this file's provenance; skip if PyPI changed.
    try:
        upstream_provenance = pypi_provenance(
            PYPI_ATTESTED_PKG, PYPI_ATTESTED_VERSION, PYPI_ATTESTED_WHEEL
        )
    except RuntimeError as e:
        pytest.skip(f"{PYPI_ATTESTED_WHEEL} provenance unavailable on PyPI: {e}")

    pkg_list = tmp_path / "packages.txt"
    pkg_list.write_text(f"{PYPI_ATTESTED_PKG}=={PYPI_ATTESTED_VERSION}\n")
    run_checked(
        [
            str(pypiron_bin),
            "sync",
            "--packages-list",
            str(pkg_list),
            "--data-dir",
            str(disk_server["data_dir"]),
            "--only-wheels",
        ],
        timeout=600,
    )
    index = wait_for_file_in_index(disk_server["simple"], PYPI_ATTESTED_PKG, PYPI_ATTESTED_WHEEL)

    entry = next(f for f in index["files"] if f["filename"] == PYPI_ATTESTED_WHEEL)
    assert entry["provenance"] == f"/files/{PYPI_ATTESTED_PKG}/{PYPI_ATTESTED_WHEEL}.provenance"

    # The served provenance is byte-identical to PyPI's — so whatever a consumer
    # verifies against PyPI, it verifies against this mirror, offline.
    url = f"{disk_server['base_url']}/files/{PYPI_ATTESTED_PKG}/{PYPI_ATTESTED_WHEEL}.provenance"
    code, body, _ = http_get(url, timeout=30.0)
    assert code == 200
    served = json.loads(body)
    assert served == upstream_provenance
    assert served["version"] == 1
    assert served["attestation_bundles"]
