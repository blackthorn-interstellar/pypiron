"""The human-facing pages: the root landing page (/) and the dashboard
(/dashboard). Root is a public front door; the dashboard is gated by read auth
when a read credential is configured."""

from __future__ import annotations

import json
import re
import time

import pytest

from .helpers import (
    _encode_basic_auth,
    http_get,
    http_get_no_redirect,
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


def test_favicon_served_as_multisize_ico(disk_server):
    code, body, headers = http_get(f"{disk_server['base_url']}/favicon.ico")
    assert code == 200
    assert headers.get("content-type") == "image/x-icon"
    # Immutable per build, so it's cacheable (unlike the no-cache HTML pages).
    assert "max-age" in headers.get("cache-control", "")
    # A real ICO: 2 reserved bytes (00 00) then image type 1 (01 00).
    assert body[:4] == b"\x00\x00\x01\x00"
    assert len(body) > 1000  # carved from the logo, not an empty placeholder


def test_pages_declare_the_favicon(disk_server):
    _, body, _ = http_get(f"{disk_server['base_url']}/")
    assert '<link rel="icon" href="/favicon.ico"' in body.decode()


def test_favicon_is_public_under_read_auth(disk_server_read_auth):
    # Browsers fetch /favicon.ico before any credential is in play, so it must
    # stay outside read auth — same posture as /health and /metrics.
    code, _, headers = http_get(f"{disk_server_read_auth['base_url']}/favicon.ico")
    assert code == 200
    assert headers.get("content-type") == "image/x-icon"


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


def test_homepage_leads_with_a_search_form(disk_server):
    code, body, _ = http_get(f"{disk_server['base_url']}/")
    assert code == 200
    html = body.decode()
    # Search is the front-and-center focus: a GET form to the results page.
    assert 'class="search"' in html
    assert 'action="/projects/"' in html
    assert 'name="q"' in html
    assert 'type="search"' in html
    # A browse-all link sits right under the search box.
    assert 'class="browse"' in html
    assert 'href="/projects/"' in html
    # Config section is labelled and its settings carry hover tooltips.
    assert ">Configuration</h2>" in html
    assert "data-tip=" in html
    # The index URL is present top-right as a copy field (no label).
    assert 'class="install idxbox"' in html
    assert f"{disk_server['base_url']}/simple/" in html


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
        "Top route groups",
    ):
        assert label in html, label
    # The old "Top projects" chart was replaced by the "Most Downloaded Packages"
    # marquee, which renders only when there is download traffic (none here).
    assert "Top projects" not in html
    # The redundant "Packages hosted" tile was dropped — registry size lives in
    # the inventory row instead.
    assert "Packages hosted" not in html
    # Bars are inline SVG.
    assert "<svg" in html


def _inv_count(html: str, label: str) -> int | None:
    m = re.search(rf"<b>([\d,]+)</b> {label}", html)
    return int(m.group(1).replace(",", "")) if m else None


def _wait_homepage(server, timeout=25.0, **needs):
    """Poll `/` until every `label=min_count` is met; return the last HTML."""
    deadline = time.time() + timeout
    html = ""
    while time.time() < deadline:
        _, body, _ = http_get(f"{server['base_url']}/")
        html = body.decode()
        if all((_inv_count(html, label) or -1) >= n for label, n in needs.items()):
            return html
        time.sleep(0.5)
    return html


def test_homepage_inventory_updates_incrementally_without_a_sweep(disk_server, tmp_path):
    # disk_server uses the DEFAULT reconcile interval (daily), so no further
    # audit fires during the test. After the boot audit establishes the baseline
    # (ready, shown by the inventory row), every count change here can only come
    # from the per-tick incremental update — exactly option 4's between-sweep
    # path.
    server = disk_server
    # Wait for the boot audit to publish the (empty) baseline so `ready` is set
    # before we upload — this both proves readiness and avoids an upload racing
    # the boot sweep's re-baseline.
    baseline = _wait_homepage(server, projects=0)
    assert 'class="inv"' in baseline, baseline

    for name in ("invone", "invtwo"):
        wheel = make_wheel(name, "1.0.0", tmp_path)
        upload_legacy(
            server["legacy"],
            wheel,
            username=server["user"],
            password=server["password"],
        )
        wait_for_project_in_global(server["simple"], name)

    html = _wait_homepage(server, projects=2, files=2, releases=2)
    assert (_inv_count(html, "projects") or 0) >= 2, html
    # Two single-wheel projects → two files (sidecars excluded) and two releases.
    assert (_inv_count(html, "files") or 0) >= 2
    assert (_inv_count(html, "releases") or 0) >= 2
    # Space used (sum of artifact sizes) renders right after the file count, as
    # a human size — e.g. "1.2 KB stored" or "523 B stored".
    assert re.search(r"<b>[\d.]+ [KMGT]?B</b> stored", html), html


def test_inventory_view_is_persisted_to_storage(disk_server, tmp_path):
    # The storage-backed view is what makes inventory multi-node + restart-safe.
    # It lives at _state/inventory.json on disk (the data dir), not on any route.
    server = disk_server
    wheel = make_wheel("persistpkg", "1.0.0", tmp_path)
    upload_legacy(
        server["legacy"],
        wheel,
        username=server["user"],
        password=server["password"],
    )
    wait_for_project_in_global(server["simple"], "persistpkg")
    _wait_homepage(server, projects=1)

    inv_path = server["data_dir"] / "_state" / "inventory.json"
    deadline = time.time() + 10
    while time.time() < deadline and not inv_path.exists():
        time.sleep(0.25)
    assert inv_path.exists(), f"{inv_path} not written"
    data = json.loads(inv_path.read_text())
    assert data["projects"] >= 1
    assert data["files"] >= 1
    assert data["bytes"] >= 1
    # _state/ is internal — it must not be reachable over HTTP.
    code, _, _ = http_get(f"{server['base_url']}/files/_state/inventory.json")
    assert code in (403, 404), code


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


def test_project_page_renders_markdown_readme_through_whitelist(disk_server, tmp_path):
    """A wheel declaring text/markdown gets its README rendered through the
    locked-down whitelist: real markdown becomes HTML, raw script is dropped."""
    wheel = make_wheel(
        "mdpkg",
        "3.2.1",
        tmp_path,
        metadata_extra="Description-Content-Type: text/markdown\n",
        description=(
            "# Title\n\n"
            "Some **bold** text and a [link](https://example.com).\n\n"
            "<script>alert('xss')</script>\n"
        ),
    )
    upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_project_in_global(disk_server["simple"], "mdpkg")

    code, body, _ = http_get(f"{disk_server['base_url']}/project/mdpkg/")
    assert code == 200
    html = body.decode()
    # Markdown rendered to HTML...
    assert '<div class="readme-md">' in html
    assert "<strong>bold</strong>" in html
    assert '<a href="https://example.com" rel="nofollow noopener noreferrer">link</a>' in html
    # ...but the raw <script> is dropped, never echoed back.
    assert "<script>alert" not in html
    assert "shown unrendered" not in html


def test_per_version_page_pins_install_and_validates_version(disk_server, tmp_path):
    for v in ("1.0.0", "1.2.0"):
        wheel = make_wheel("verpkg", v, tmp_path)
        upload_legacy(
            disk_server["legacy"],
            wheel,
            username=disk_server["user"],
            password=disk_server["password"],
        )
        wait_for_project_in_global(disk_server["simple"], "verpkg")

    # Latest page: unpinned `uv add`, release history links to per-version pages.
    code, body, _ = http_get(f"{disk_server['base_url']}/project/verpkg/")
    assert code == 200
    latest = body.decode()
    assert "1.2.0" in latest
    assert 'href="/project/verpkg/1.0.0/"' in latest
    assert "uv add --index" in latest

    # Per-version page: pinned install, current release flagged.
    code, body, _ = http_get(f"{disk_server['base_url']}/project/verpkg/1.0.0/")
    assert code == 200
    page = body.decode()
    assert "verpkg==1.0.0" in page
    assert "This version" in page

    # An unknown version 404s rather than reflecting the path segment.
    code, _, _ = http_get(f"{disk_server['base_url']}/project/verpkg/9.9.9/")
    assert code == 404


def test_version_page_redirect_does_not_reflect_traversal(disk_server, tmp_path):
    """A non-normalized name 301s to the canonical URL, but the (not-yet-
    validated) version segment is percent-encoded so a `..%2f..%2f` payload can't
    cross a path boundary in Location — it lands back here and 404s."""
    wheel = make_wheel("RedirPkg", "1.0.0", tmp_path)
    upload_legacy(
        disk_server["legacy"],
        wheel,
        username=disk_server["user"],
        password=disk_server["password"],
    )
    wait_for_project_in_global(disk_server["simple"], "redirpkg")

    base = disk_server["base_url"]
    # Non-normalized name + traversal version: the encoded slash stays in-segment.
    code, _, headers = http_get_no_redirect(f"{base}/project/RedirPkg/..%2f..%2fsimple/")
    assert code == 301
    loc = headers.get("location", "")
    assert "/simple/" not in loc  # did not collapse to another route
    assert "%2F" in loc.upper()  # slash stayed percent-encoded
    # Following it dead-ends at a 404, never at /simple/.
    code, _, _ = http_get(f"{base}/project/RedirPkg/..%2f..%2fsimple/")
    assert code == 404


def test_default_page_skips_prerelease(disk_server, tmp_path):
    for v in ("2.0.0", "2.1.0rc1"):
        wheel = make_wheel("prerelpkg", v, tmp_path)
        upload_legacy(
            disk_server["legacy"],
            wheel,
            username=disk_server["user"],
            password=disk_server["password"],
        )
        wait_for_project_in_global(disk_server["simple"], "prerelpkg")

    code, body, _ = http_get(f"{disk_server['base_url']}/project/prerelpkg/")
    assert code == 200
    html = body.decode()
    # The bare page headlines the newest stable release, not the pre-release...
    assert '<span class="pver">2.0.0</span>' in html
    assert '<span class="pver">2.1.0rc1</span>' not in html
    # ...but the pre-release is still listed and directly reachable.
    assert 'href="/project/prerelpkg/2.1.0rc1/"' in html
    code, _, _ = http_get(f"{disk_server['base_url']}/project/prerelpkg/2.1.0rc1/")
    assert code == 200


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


def test_projects_search_filters_to_matching_names(disk_server, tmp_path):
    for name in ("searchapple", "searchberry"):
        wheel = make_wheel(name, "1.0.0", tmp_path)
        upload_legacy(
            disk_server["legacy"],
            wheel,
            username=disk_server["user"],
            password=disk_server["password"],
        )
        wait_for_project_in_global(disk_server["simple"], name)

    # The search box on / submits here as ?q=; results are filtered server-side.
    code, body, _ = http_get(f"{disk_server['base_url']}/projects/?q=apple")
    assert code == 200
    html = body.decode()
    assert 'href="/project/searchapple/"' in html
    assert 'href="/project/searchberry/"' not in html
    # The box is pre-filled with the query so it reads as a search result page.
    assert 'value="apple"' in html
    assert "matching" in html

    # An empty query falls back to listing everything (plain browse).
    code, body, _ = http_get(f"{disk_server['base_url']}/projects/?q=")
    assert code == 200
    html = body.decode()
    assert 'href="/project/searchapple/"' in html
    assert 'href="/project/searchberry/"' in html


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
    assert "Top route groups" in authed
    assert "Top projects" not in authed
