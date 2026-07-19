"""PEP 503: non-normalized package URLs 301 to the canonical normalized path."""

from __future__ import annotations

import pytest

from .helpers import (
    http_get_no_redirect,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration


def test_non_normalized_name_redirects(disk_server, tmp_path):
    server = disk_server
    wheel = make_wheel("RedirDemo", "1.0", tmp_path)
    upload_legacy(server["legacy"], wheel, username=server["user"], password=server["password"])
    wait_for_file_in_index(server["simple"], "redirdemo", wheel.name)

    code, _, headers = http_get_no_redirect(f"{server['simple']}RedirDemo/")
    assert code == 301
    assert headers["location"] == "/simple/redirdemo/"

    code, _, headers = http_get_no_redirect(f"{server['simple']}Redir.Demo/index.json")
    assert code == 301
    assert headers["location"] == "/simple/redir-demo/index.json"

    # The canonical URL serves directly, no redirect hop.
    code, _, _ = http_get_no_redirect(f"{server['simple']}redirdemo/")
    assert code == 200

    # Unknown-but-normalized names still 404 (not a redirect loop).
    code, _, _ = http_get_no_redirect(f"{server['simple']}no-such-package/")
    assert code == 404


def test_publish_is_never_redirected(disk_server, tmp_path):
    """Regression guard for uv#19748: a publish is answered by this index
    directly and is never redirected toward upstream.

    Redirects are disabled on the client so a stray 3xx surfaces raw instead of
    being chased transparently."""
    server = disk_server
    wheel = make_wheel("PublishNoRedirect", "1.0", tmp_path)
    creds = {"username": server["user"], "password": server["password"]}

    # Fresh upload: a normal 2xx, never a 3xx.
    code, _ = upload_legacy(
        server["legacy"], wheel, expect_status=200, follow_redirects=False, **creds
    )
    assert code == 200
    assert not 300 <= code < 400
    wait_for_file_in_index(server["simple"], "publishnoredirect", wheel.name)

    # Duplicate filename: rejected with 409, still never a 3xx.
    code, _ = upload_legacy(
        server["legacy"], wheel, expect_status=409, follow_redirects=False, **creds
    )
    assert code == 409
    assert not 300 <= code < 400
