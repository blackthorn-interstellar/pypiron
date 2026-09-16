"""Upload aliases for a hostname cutover from pypicloud: `POST /simple/`,
`POST /simple` and `POST /` are the same handler as `POST /legacy/`. pypicloud
documented `/simple/` as its upload URL (one URL for pip and twine) and
`uv publish` posts to `/` when its publish URL has no path, so publishers
configured for the old server keep working; `GET` on those paths is still the
index and the front door."""

from __future__ import annotations

import pytest

from .helpers import http_get, make_wheel, run_checked, upload_legacy, wait_for_file_in_index

pytestmark = pytest.mark.integration


@pytest.mark.compat("uv", "upload")
def test_uv_publish_to_the_simple_url(disk_server, tmp_path, uv_path):
    """The migration's exact shape: a publisher whose one URL variable serves
    both `uv build --index` and `uv publish --publish-url`."""
    wheel = make_wheel("cutover-pkg", "1.0", tmp_path)
    run_checked(
        [
            uv_path,
            "publish",
            "--publish-url",
            disk_server["simple"],
            "--username",
            disk_server["user"],
            "--password",
            disk_server["password"],
            str(wheel),
        ],
        timeout=120,
    )
    wait_for_file_in_index(disk_server["simple"], "cutover-pkg", wheel.name)


@pytest.mark.parametrize("path", ["/simple/", "/simple", "/"])
def test_post_aliases_are_the_upload_handler(disk_server, tmp_path, path):
    wheel = make_wheel("alias-pkg", "1.0", tmp_path)
    url = disk_server["base_url"] + path

    # The alias is the real handler: the same credential gate as /legacy/.
    upload_legacy(url, wheel, expect_status=401)
    upload_legacy(url, wheel, username=disk_server["user"], password=disk_server["password"])
    wait_for_file_in_index(disk_server["simple"], "alias-pkg", wheel.name)

    # GET on the same path still answers as before.
    code, _, _ = http_get(url)
    assert code == 200
