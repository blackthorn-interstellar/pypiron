"""A quarantine has to take hold everywhere, fast, without an audit sweep.

PEP 792 quarantine used to reach the byte gate down exactly one road: the leader's
audit sweep derived the set from every package's status sidecar and published it,
and every node picked it up on the advisory tick. Both halves of that road are
slow — the sweep walks the whole mirror, the tick runs on `reconcile-interval`
(a day, by default) behind a 32 MB OSV refetch — so freezing a compromised project
left every other node serving it, for up to a day. Releasing one was just as slow
in the other direction.

Now a status write publishes its own one-name increment and arms the node that
received it in memory, and every node re-polls the shared set on its own short
cadence. The sweep is repair, not propagation.

Every test here runs with `--audit-on-boot false` and a day-long reconcile
interval, so no sweep can rescue an assertion: what these measure is the write
path and the poll, or nothing.
"""

from __future__ import annotations

import time

import pytest

from .helpers import (
    http_get,
    http_request_auth,
    kill_process_tree,
    make_wheel,
    upload_legacy,
    wait_for_file_in_index,
)
from .test_advisories import _hand_start

pytestmark = pytest.mark.integration

# The poll cadence these servers run at, passed explicitly below so the bound
# holds whatever the shipped default becomes. A node that missed a publish by a
# hair waits one full interval, so the promise is two of them; the slack absorbs
# a loaded CI box.
POLL_INTERVAL = 30.0
PROPAGATION_BOUND = 2 * POLL_INTERVAL + 15.0

# No sweep may fire: the audit is what these tests exist to route around.
NO_SWEEP = [
    "--audit-on-boot",
    "false",
    "--reconcile-interval-secs",
    "86400",
    "--quarantine-poll-secs",
    str(int(POLL_INTERVAL)),
    "--advisory-feed",
    "",
]


def _start(pypiron_bin, data_dir):
    return _hand_start(pypiron_bin, data_dir, NO_SWEEP)


def _mirror_upload(base: str, dist) -> None:
    """Publish as a mirror-origin file, the way a sync would. The byte gate exempts
    private-origin names from the quarantine set (a private package that shares a
    public name is not that package), so only a mirror-origin file can be blocked."""
    upload_legacy(
        f"{base}/legacy/",
        dist,
        username="admin",
        password="secret",
        fields={"mirror": "true"},
    )


def _set_status(base: str, pkg: str, body: bytes) -> None:
    code, response, _ = http_request_auth(
        "POST",
        f"{base}/project/{pkg}/status",
        username="admin",
        password="secret",
        data=body,
    )
    assert code == 200, (code, response)


def _clear_status(base: str, pkg: str) -> None:
    code, response, _ = http_request_auth(
        "DELETE",
        f"{base}/project/{pkg}/status",
        username="admin",
        password="secret",
    )
    assert code == 200, (code, response)


def _await_status(url: str, accept: set, *, timeout: float) -> float:
    """Poll `url` until its status is in `accept`; return how long that took.
    Fails with the last status seen — a bare timeout tells you nothing."""
    started = time.time()
    deadline = started + timeout
    last = None
    while time.time() < deadline:
        last, _, _ = http_get(url)
        if last in accept:
            return time.time() - started
        time.sleep(0.25)
    pytest.fail(f"{url} never reached {sorted(accept)} within {timeout:.0f}s (last status {last})")


def _seed_mirror_wheel(base: str, pkg: str, tmp_path) -> str:
    """Publish a mirror-origin wheel and return its direct-download path."""
    wheel = make_wheel(pkg, "1.0.0", tmp_path)
    _mirror_upload(base, wheel)
    wait_for_file_in_index(f"{base}/simple/", pkg, wheel.name)
    return f"/files/{pkg}/{wheel.name}"


def test_a_freeze_blocks_the_next_request_on_the_node_that_received_it(pypiron_bin, tmp_path):
    """No sweep, no poll, no restart: the request after the admin call is refused.
    The handler arms this node's byte gate in memory before it answers, so there is
    no window at all on the node an operator (or a `sync` relay) talks to."""
    pkg = "freezenow"
    proc, base, _log = _start(pypiron_bin, tmp_path / "data")
    try:
        file_path = _seed_mirror_wheel(base, pkg, tmp_path)
        code, _, _ = http_get(f"{base}{file_path}")
        assert code in {200, 302}, f"the clean mirror file was not served (status {code})"

        _set_status(base, pkg, b'{"status":"quarantined","reason":"compromised release"}')

        # Deliberately a single request, not a poll: any retry loop here would
        # hide exactly the window this asserts is closed.
        code, body, _ = http_get(f"{base}{file_path}")
        assert code == 403, (
            f"a quarantined artifact was served (status {code}); the byte gate on the receiving node was not armed by the write"
        )
        assert b"quarantined" in body, body

        # And releasing it is just as immediate.
        _clear_status(base, pkg)
        code, _, _ = http_get(f"{base}{file_path}")
        assert code in {200, 302}, f"the released project is still refused (status {code})"
    finally:
        kill_process_tree(proc)


def test_a_freeze_and_a_release_both_reach_the_other_node(pypiron_bin, tmp_path):
    """Two nodes, one storage. The node that never saw the admin call has to pick
    the change up from the shared set on its own, in both directions, with the
    audit sweep switched off. Before the poll existed, node B served the frozen
    artifact until its next sweep — a day, by default."""
    pkg = "twonodes"
    data_dir = tmp_path / "data"
    proc_a, base_a, _log_a = _start(pypiron_bin, data_dir)
    proc_b = None
    try:
        file_path = _seed_mirror_wheel(base_a, pkg, tmp_path)
        proc_b, base_b, _log_b = _start(pypiron_bin, data_dir)
        code, _, _ = http_get(f"{base_b}{file_path}")
        assert code in {200, 302}, f"node B did not serve the clean file (status {code})"

        _set_status(base_a, pkg, b'{"status":"quarantined","reason":"compromised release"}')
        code, _, _ = http_get(f"{base_a}{file_path}")
        assert code == 403, f"node A did not block its own freeze (status {code})"

        took = _await_status(f"{base_b}{file_path}", {403}, timeout=PROPAGATION_BOUND)
        assert took <= PROPAGATION_BOUND

        # The same road, the other way: a release must not wait for a sweep either.
        _clear_status(base_a, pkg)
        code, _, _ = http_get(f"{base_a}{file_path}")
        assert code in {200, 302}, f"node A did not release its own clear (status {code})"

        _await_status(f"{base_b}{file_path}", {200, 302}, timeout=PROPAGATION_BOUND)
    finally:
        for proc in (proc_b, proc_a):
            if proc is not None:
                kill_process_tree(proc)
