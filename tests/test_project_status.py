"""PEP 792 (Simple API 1.4) — project status markers, relayed verbatim.

pypiron is a relay, not an author (the same play as PEP 740 provenance): the
per-project marker is truth on disk at `packages/<pkg>/.project-status.json`,
carried into the rendered index and propagated through the proxy. These drive
the real binary over HTTP.

Status is set the on-brand way for Phase 1 — by writing the marker file before
the upload that materializes the index (truth is files on disk; there is no
authoring endpoint yet).
"""

from __future__ import annotations

import json
import time

import pytest

from .helpers import (
    ACCEPT_PEP691,
    get_index_json,
    http_get,
    http_get_json,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration


def _set_status(server, pkg, status):
    """Drop the per-project status marker into the server's storage."""
    pkg_dir = server["data_dir"] / "packages" / pkg
    pkg_dir.mkdir(parents=True, exist_ok=True)
    (pkg_dir / ".project-status.json").write_text(json.dumps(status))


def _mirror_upload(server, dist):
    upload_legacy(
        server["legacy"],
        dist,
        username=server["admin_user"],
        password=server["admin_password"],
        fields={"mirror": "true"},
    )


def _wait_for_status(simple_url, pkg, *, timeout=30.0):
    """Poll the package index until it carries a `project-status` object."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            doc = http_get_json(f"{simple_url}{pkg}/index.json", headers={"Accept": ACCEPT_PEP691})
            if "project-status" in doc:
                return doc
        except (RuntimeError, ConnectionError):
            pass
        time.sleep(0.2)
    raise TimeoutError(f"project-status never appeared for {pkg} within {timeout}s")


def test_archived_status_served_in_html_and_json(disk_server, tmp_path):
    pkg = "archiveddemo"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _set_status(disk_server, pkg, {"status": "archived", "reason": 'moved to "foo"'})
    _mirror_upload(disk_server, wheel)

    # Archived keeps its files installable, so the file still appears.
    index = wait_for_file_in_index(disk_server["simple"], pkg, wheel.name)
    assert index["meta"]["api-version"] == "1.4"
    # Top-level object (never nested in meta), reason round-trips verbatim.
    assert index["project-status"] == {"status": "archived", "reason": 'moved to "foo"'}
    assert "project-status" not in index["meta"]

    _, html, _ = http_get(f"{disk_server['simple']}{pkg}/")
    html = html.decode()
    assert '<meta name="pypi:project-status" content="archived">' in html
    # The arbitrary reason is attribute-escaped.
    assert '<meta name="pypi:project-status-reason" content="moved to &quot;foo&quot;">' in html


def test_active_project_has_no_status_marker(disk_server, tmp_path):
    pkg = "activedemo"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _mirror_upload(disk_server, wheel)
    index = wait_for_file_in_index(disk_server["simple"], pkg, wheel.name)

    # PEP 792 lets the active marker be omitted; we omit it, in both formats.
    assert "project-status" not in index
    _, html, _ = http_get(f"{disk_server['simple']}{pkg}/")
    assert "pypi:project-status" not in html.decode()


def test_quarantined_project_omits_file_links_but_keeps_marker(disk_server, tmp_path):
    pkg = "quarantineddemo"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    # Quarantine before the index is built: the rebuild renders the marker with
    # no file links (PEP 792 — a quarantined project offers no distributions).
    _set_status(disk_server, pkg, {"status": "quarantined"})
    _mirror_upload(disk_server, wheel)

    # The file never appears in the index, so wait on the marker instead.
    index = _wait_for_status(disk_server["simple"], pkg)
    assert index["project-status"]["status"] == "quarantined"
    assert index["files"] == []
    assert index["versions"] == []

    _, html, _ = http_get(f"{disk_server['simple']}{pkg}/")
    html = html.decode()
    assert '<meta name="pypi:project-status" content="quarantined">' in html
    assert "<a href" not in html

    # The artifact bytes still exist on disk — Phase 1 relays the marker but
    # does not gate downloads (that fail-closed gate is deferred to Phase 2).
    assert (disk_server["data_dir"] / "packages" / pkg / wheel.name).exists()


def test_proxy_relays_upstream_project_status(proxy_pair, tmp_path):
    upstream, proxy = proxy_pair["upstream"], proxy_pair["proxy"]
    pkg = "proxystatusdemo"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _set_status(upstream, pkg, {"status": "archived", "reason": "upstream said so"})
    _mirror_upload(upstream, wheel)
    wait_for_file_in_index(upstream["simple"], pkg, wheel.name)

    # The proxy parses the upstream listing and re-emits the marker as its own.
    index = get_index_json(proxy["simple"], pkg)
    assert index["meta"]["api-version"] == "1.4"
    assert index["project-status"] == {"status": "archived", "reason": "upstream said so"}
