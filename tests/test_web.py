"""The human-facing pages: the root landing page (/) and the dashboard
(/dashboard). Root is a public front door; the dashboard is gated by read auth
when a read credential is configured."""

from __future__ import annotations

import re
import time

import pytest

from .helpers import (
    _encode_basic_auth,
    http_get,
    make_wheel,
    upload_legacy,
    wait_for_project_in_global,
)

pytestmark = pytest.mark.integration


def _auth_header(user: str, password: str) -> dict:
    return {"Authorization": _encode_basic_auth(user, password)}


def test_root_serves_landing_with_embedded_logo_and_index_url(disk_server):
    code, body, headers = http_get(f"{disk_server['base_url']}/")
    assert code == 200
    assert headers.get("content-type") == "text/html; charset=utf-8"
    html = body.decode()
    # Embedded logo, not an external request.
    assert "data:image/png;base64," in html
    assert "An ultra-fast PyPI server written in Rust." in html
    # The one index URL, with the host the request actually arrived on...
    base = disk_server["base_url"]
    assert f"{base}/simple/" in html
    assert "navigator.clipboard.writeText" in html  # working copy button
    # ...and no per-client command boxes.
    assert "uv pip install" not in html
    assert "poetry source add" not in html
    assert "twine upload" not in html


def test_root_url_follows_forwarded_headers(disk_server):
    code, body, _ = http_get(
        f"{disk_server['base_url']}/",
        headers={
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "pkgs.example.com",
        },
    )
    assert code == 200
    assert "https://pkgs.example.com/simple/" in body.decode()


def test_homepage_shows_live_activity_when_reads_public(disk_server):
    # disk_server has public reads, so the activity panel renders inline on /.
    # Generate a little traffic so the numbers aren't all zero.
    http_get(f"{disk_server['simple']}index.json")
    code, body, headers = http_get(f"{disk_server['base_url']}/")
    assert code == 200
    assert headers.get("content-type") == "text/html; charset=utf-8"
    html = body.decode()
    # Config card and activity panel coexist on the one page.
    assert f"{disk_server['base_url']}/simple/" in html
    assert 'class="activity"' in html
    for label in (
        "Total requests",
        "Files served",
        "Index cache hit rate",
        "Packages hosted",
        "Top projects",
        "Top route groups",
    ):
        assert label in html, label
    # Bars are inline SVG.
    assert "<svg" in html


def test_homepage_counts_hosted_packages(disk_server, tmp_path):
    wheel = make_wheel("dashpkg", "1.0.0", tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    # The panel reads the worker's in-memory name set; wait until the upload is
    # materialized in the global index before asserting the count.
    wait_for_project_in_global(disk_server["simple"], "dashpkg")

    code, body, _ = http_get(f"{disk_server['base_url']}/")
    assert code == 200
    m = re.search(
        r'<div class="num">([\d,]+)</div><div class="lbl">Packages hosted</div>',
        body.decode(),
    )
    assert m, "Packages hosted tile not found"
    assert int(m.group(1).replace(",", "")) >= 1


def _inv_count(html: str, label: str) -> int | None:
    m = re.search(rf"<b>([\d,]+)</b> {label}", html)
    return int(m.group(1).replace(",", "")) if m else None


def test_homepage_inventory_counts_projects_releases_files(disk_server_fast_reconcile, tmp_path):
    server = disk_server_fast_reconcile
    for name in ("invone", "invtwo"):
        wheel = make_wheel(name, "1.0.0", tmp_path)
        upload_legacy(
            server["legacy"],
            wheel,
            username=server["user"],
            password=server["password"],
        )
        wait_for_project_in_global(server["simple"], name)

    # Inventory is recomputed by the audit sweep (fast-reconcile = 2s here);
    # poll the homepage until the sweep has counted both uploads.
    deadline = time.time() + 25
    html = ""
    while time.time() < deadline:
        _, body, _ = http_get(f"{server['base_url']}/")
        html = body.decode()
        if (_inv_count(html, "projects") or 0) >= 2:
            break
        time.sleep(0.5)

    assert (_inv_count(html, "projects") or 0) >= 2, html
    # Two single-wheel projects → two files (sidecars excluded) and two releases.
    assert (_inv_count(html, "files") or 0) >= 2
    assert (_inv_count(html, "releases") or 0) >= 2


def test_project_page_shows_metadata_files_and_unrendered_readme(disk_server, tmp_path):
    wheel = make_wheel(
        "projpkg",
        "2.1.0",
        tmp_path,
        metadata_extra=(
            "Author: Ada Lovelace\n"
            "License-Expression: MIT\n"
            "Requires-Python: >=3.9\n"
            "Keywords: demo,example\n"
            "Project-URL: Source, https://github.com/ada/projpkg\n"
            "Classifier: Programming Language :: Python :: 3\n"
            "Requires-Dist: requests>=2\n"
        ),
        description="# Heading\n\nSome **markdown** body that must not be rendered.\n",
    )
    upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_project_in_global(disk_server["simple"], "projpkg")

    code, body, headers = http_get(f"{disk_server['base_url']}/project/projpkg/")
    assert code == 200
    assert headers.get("content-type") == "text/html; charset=utf-8"
    html = body.decode()
    # Headline + sidebar metadata.
    assert "projpkg" in html
    assert "2.1.0" in html
    assert "Ada Lovelace" in html
    assert "MIT" in html
    assert "&gt;=3.9" in html  # Requires-Python, escaped
    assert "https://github.com/ada/projpkg" in html
    assert "requests&gt;=2" in html  # dependency, escaped
    assert "Programming Language :: Python :: 3" in html
    # README shown verbatim, NOT rendered to HTML.
    assert "shown unrendered" in html
    assert "# Heading" in html
    assert "**markdown**" in html
    assert "<strong>markdown</strong>" not in html
    # The release file links to the artifact.
    assert 'href="/files/projpkg/projpkg-2.1.0-py3-none-any.whl"' in html


def test_projects_index_lists_packages_linking_to_project_pages(disk_server, tmp_path):
    for name in ("alpha-pkg", "beta-pkg"):
        wheel = make_wheel(name, "1.0.0", tmp_path)
        upload_legacy(
            disk_server["legacy"],
            wheel,
            username=disk_server["user"],
            password=disk_server["password"],
        )
        wait_for_project_in_global(disk_server["simple"], name)

    code, body, headers = http_get(f"{disk_server['base_url']}/projects/")
    assert code == 200
    assert headers.get("content-type") == "text/html; charset=utf-8"
    html = body.decode()
    assert 'href="/project/alpha-pkg/"' in html
    assert 'href="/project/beta-pkg/"' in html
    # Has the client-side filter box.
    assert 'class="filter"' in html
    # The landing page points humans here.
    _, root_body, _ = http_get(f"{disk_server['base_url']}/")
    assert 'href="/projects/"' in root_body.decode()


def test_projects_index_gated_under_read_auth(disk_server_read_auth):
    server = disk_server_read_auth
    code, _, headers = http_get(f"{server['base_url']}/projects/")
    assert code == 401
    assert headers.get("www-authenticate") == 'Basic realm="PypIron"'

    code, _, _ = http_get(
        f"{server['base_url']}/projects/",
        headers=_auth_header(server["read_user"], server["read_password"]),
    )
    assert code == 200


def test_project_page_normalizes_name_with_redirect(disk_server, tmp_path):
    wheel = make_wheel("Norm.Me", "1.0.0", tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_project_in_global(disk_server["simple"], "norm-me")

    from .helpers import http_get_no_redirect

    code, _, headers = http_get_no_redirect(f"{disk_server['base_url']}/project/Norm.Me/")
    assert code == 301
    assert headers.get("location") == "/project/norm-me/"


def test_project_page_404_for_unknown(disk_server):
    code, _, _ = http_get(f"{disk_server['base_url']}/project/does-not-exist/")
    assert code == 404


def test_project_page_renders_without_metadata(disk_server, tmp_path):
    # An sdist with only PKG-INFO basics still produces a usable page.
    from .helpers import make_sdist

    sdist = make_sdist("baremeta", "0.1.0", tmp_path)
    upload_legacy(
        disk_server["legacy"],
        sdist,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_project_in_global(disk_server["simple"], "baremeta")

    code, body, _ = http_get(f"{disk_server['base_url']}/project/baremeta/")
    assert code == 200
    html = body.decode()
    assert "baremeta" in html
    assert 'href="/files/baremeta/' in html


def test_project_page_gated_under_read_auth(disk_server_read_auth, tmp_path):
    server = disk_server_read_auth
    wheel = make_wheel("gatedpkg", "1.0.0", tmp_path)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
    )
    wait_for_project_in_global(
        server["simple"],
        "gatedpkg",
        headers=_auth_header(server["read_user"], server["read_password"]),
    )

    code, _, headers = http_get(f"{server['base_url']}/project/gatedpkg/")
    assert code == 401
    assert headers.get("www-authenticate") == 'Basic realm="PypIron"'

    code, _, _ = http_get(
        f"{server['base_url']}/project/gatedpkg/",
        headers=_auth_header(server["read_user"], server["read_password"]),
    )
    assert code == 200


def test_root_public_but_activity_panel_gated_under_read_auth(disk_server_read_auth):
    server = disk_server_read_auth

    # Root stays public — it's how a human discovers they need credentials —
    # but the activity panel (traffic stats, project-tag names) is withheld.
    code, body, _ = http_get(f"{server['base_url']}/")
    assert code == 200
    public = body.decode()
    assert "Reads require a credential" in public
    assert f"{server['base_url']}/simple/" in public  # index URL is public
    assert 'class="activity"' not in public  # but no stats leak
    assert "Top projects" not in public

    # A read credential (or stronger) folds the activity panel in.
    code, body, _ = http_get(
        f"{server['base_url']}/",
        headers=_auth_header(server["read_user"], server["read_password"]),
    )
    assert code == 200
    authed = body.decode()
    assert 'class="activity"' in authed
    assert "Top projects" in authed
