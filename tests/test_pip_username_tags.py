"""pip and the index-username tags (`ci+repo=org/repo,delivery=stream`).

pip takes the username from the index URL, percent-decodes it, and sends it on
every request to that host — index pages and wheel downloads alike, from the
first request. It hides a refusal's reason unless run with `-vv`.
"""

from __future__ import annotations

import pytest

from .conftest import _start_disk_server
from .helpers import run_checked, run_returncode
from .test_require_repo import REASON, _publish

pytestmark = pytest.mark.integration


@pytest.fixture()
def enforcing(tmp_path_factory, pypiron_bin):
    yield from _start_disk_server(
        tmp_path_factory, pypiron_bin, extra_args=["--require-repo", "enforce", "--access-log"]
    )


def _pip_install(pip_venv, server, userinfo: str, *flags: str) -> list[str]:
    return [
        str(pip_venv),
        "-m",
        "pip",
        "install",
        "--no-cache-dir",
        "--no-input",
        "--disable-pip-version-check",
        *flags,
        "--index-url",
        f"http://{userinfo}{server['bind']}/simple",
        "repopkg==1.0",
    ]


def test_pip_sends_the_tags_on_every_request(enforcing, pip_venv, tmp_path):
    """`/` in the repo is written `%2F` in the URL; pip decodes it. Under
    `enforce` a single untagged request would be a 428, so all-200 proves the
    wheel download carried the username too."""
    server = enforcing
    _publish(server, tmp_path)
    run_checked(_pip_install(pip_venv, server, "ci+repo=acme%2Fapi,delivery=stream@"), timeout=180)

    requests = [
        line
        for line in server["log_path"].read_text().splitlines()
        if "method=GET" in line and "repopkg" in line and 'ua="pip/' in line
    ]
    assert any("path=/files/repopkg/repopkg-1.0-py3-none-any.whl " in ln for ln in requests)
    for line in requests:
        assert "status=200" in line, line
        assert 'tags="repo=acme/api,delivery=stream"' in line, line


def test_pip_shows_a_refusals_reason_only_at_vv(enforcing, pip_venv, tmp_path):
    """pip reports a refused index page as "no matching distribution"; the
    status line's reason appears only in `-vv` output."""
    server = enforcing
    _publish(server, tmp_path)

    code, out, err = run_returncode(_pip_install(pip_venv, server, ""), timeout=180)
    assert code != 0
    assert "No matching distribution found" in err
    assert REASON not in out + err

    for userinfo, reason in (
        ("", f"428 Client Error: {REASON}"),
        ("ci+project=x@", "400 Client Error: Bad index username: unknown key project"),
    ):
        code, out, err = run_returncode(
            _pip_install(pip_venv, server, userinfo, "-vv"), timeout=180
        )
        assert code != 0, userinfo
        assert reason in out + err, userinfo
