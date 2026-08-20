"""On-demand proxying (--proxy-upstream): package pages from upstream
metadata, artifacts downloaded-verified-cached as mirror-origin packages, the
origin model enforced throughout. The upstream in these tests is a second
pypiron instance — it speaks the same PEP 691 + PEP 700 the proxy consumes."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time

import pytest

from .conftest import _start_proxy_pair
from .helpers import (
    ACCEPT_PEP691,
    find_free_port,
    get_index_json,
    http_get,
    http_request_auth,
    kill_process_tree,
    make_sdist,
    make_wheel,
    origin_owner,
    sha256_file,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration


def _upload(server, dist, package):
    upload_legacy(server["legacy"], dist, username=server["user"], password=server["password"])
    wait_for_file_in_index(server["simple"], package, dist.name)


def test_default_cooldown_withholds_a_fresh_release(tmp_path_factory, pypiron_bin, tmp_path):
    """With no --exclude-newer configured, the proxy applies a default 7-day
    quarantine: a release uploaded moments ago is withheld until it ages past the
    window, so an install-then-yank attack has a week to be caught first."""
    gen = _start_proxy_pair(tmp_path_factory, pypiron_bin, exclude_newer=None)
    pair = next(gen)
    try:
        upstream, proxy = pair["upstream"], pair["proxy"]
        wheel = make_wheel("freshpkg", "1.0", tmp_path)
        _upload(upstream, wheel, "freshpkg")  # upstream lists it immediately

        # The proxy withholds the just-uploaded release: every file is inside the
        # cooldown, so the project page carries no files (or 404s outright).
        code, body, _ = http_get(f"{proxy['simple']}freshpkg/", headers={"Accept": ACCEPT_PEP691})
        files = json.loads(body).get("files", []) if code == 200 else []
        assert not files, f"default cooldown must withhold a fresh release (status {code})"
    finally:
        gen.close()


def test_proxy_serves_and_caches_upstream_package(proxy_pair, tmp_path):
    upstream, proxy = proxy_pair["upstream"], proxy_pair["proxy"]
    wheel = make_wheel("proxydemo", "1.0", tmp_path)
    _upload(upstream, wheel, "proxydemo")

    # The package was never uploaded to the proxy, yet its page resolves.
    data = get_index_json(proxy["simple"], "proxydemo")
    entry = next(f for f in data["files"] if f["filename"] == wheel.name)
    assert entry["hashes"]["sha256"] == sha256_file(wheel)
    # PEP 700 upload-time rides through — --exclude-newer keeps working.
    assert entry.get("upload-time")

    # First artifact GET downloads, verifies, commits, serves.
    code, body, _ = http_get(f"{proxy['base_url']}/files/proxydemo/{wheel.name}")
    assert code == 200
    assert hashlib.sha256(body).hexdigest() == sha256_file(wheel)

    pkg_dir = proxy["data_dir"] / "packages" / "proxydemo"
    assert (pkg_dir / wheel.name).exists()
    assert (pkg_dir / f"{wheel.name}.meta.json").exists()
    assert origin_owner((pkg_dir / ".origin").read_text()) == "mirror"

    # Upstream dies; the cached artifact still serves (lockfiles keep working).
    kill_process_tree(upstream["proc"])
    code, body2, _ = http_get(f"{proxy['base_url']}/files/proxydemo/{wheel.name}")
    assert code == 200
    assert body2 == body


def test_metadata_passthrough_does_not_cache_the_wheel(proxy_pair, tmp_path):
    upstream, proxy = proxy_pair["upstream"], proxy_pair["proxy"]
    wheel = make_wheel("mdpass", "1.0", tmp_path)
    _upload(upstream, wheel, "mdpass")

    # The proxied page advertises the PEP 658 companion...
    data = get_index_json(proxy["simple"], "mdpass")
    entry = next(f for f in data["files"] if f["filename"] == wheel.name)
    assert entry.get("core-metadata")

    # ...and serving it streams from upstream without committing anything:
    # a resolver probing candidate wheels must not stampede them into storage.
    code, body, _ = http_get(f"{proxy['base_url']}/files/mdpass/{wheel.name}.metadata")
    assert code == 200
    assert b"Metadata-Version" in body
    pkg_dir = proxy["data_dir"] / "packages" / "mdpass"
    assert not (pkg_dir / wheel.name).exists()
    assert not (pkg_dir / f"{wheel.name}.metadata").exists()


def test_private_package_never_falls_through(proxy_pair, tmp_path):
    upstream, proxy = proxy_pair["upstream"], proxy_pair["proxy"]

    # The name is claimed private on the proxy itself...
    local = make_wheel("mixedpkg", "1.0", tmp_path / "local")
    _upload(proxy, local, "mixedpkg")
    # ...while upstream serves the same name with a newer version.
    upstream_wheel = make_wheel("mixedpkg", "2.0", tmp_path / "up")
    _upload(upstream, upstream_wheel, "mixedpkg")

    data = get_index_json(proxy["simple"], "mixedpkg")
    filenames = [f["filename"] for f in data["files"]]
    assert local.name in filenames
    assert upstream_wheel.name not in filenames, (
        "private name resolved from upstream — dependency confusion"
    )

    code, _, _ = http_get(f"{proxy['base_url']}/files/mixedpkg/{upstream_wheel.name}")
    assert code == 404
    assert (
        origin_owner((proxy["data_dir"] / "packages" / "mixedpkg" / ".origin").read_text())
        == "private"
    )


def test_local_origin_warm_download_stays_local(proxy_pair, tmp_path):
    """The warm-hit fast path serves already-local bytes without running the
    eligibility fence, so it must never substitute upstream bytes for a
    local-origin name. A private name that also exists upstream (a different
    version — the dependency-confusion shape) keeps serving the LOCAL file on
    every repeat, and the upstream version is never served or cached. As a
    control, a name that exists only upstream still fills on a miss: the fence is
    skipped only when the bytes are already local, which is always safe."""
    upstream, proxy = proxy_pair["upstream"], proxy_pair["proxy"]

    # Same name owned locally (1.0) and upstream (2.0). The proxy must serve only
    # its own local file, warm path included.
    local = make_wheel("warmmixed", "1.0", tmp_path / "local")
    _upload(proxy, local, "warmmixed")
    upstream_wheel = make_wheel("warmmixed", "2.0", tmp_path / "up")
    _upload(upstream, upstream_wheel, "warmmixed")

    # Warm path: repeated downloads of the local file all serve the LOCAL bytes.
    for _ in range(4):
        code, body, _ = http_get(f"{proxy['base_url']}/files/warmmixed/{local.name}")
        assert code == 200
        assert hashlib.sha256(body).hexdigest() == sha256_file(local)
    # The upstream version is never reachable through the private name.
    code, _, _ = http_get(f"{proxy['base_url']}/files/warmmixed/{upstream_wheel.name}")
    assert code == 404, "upstream version served for a private name — dependency confusion"
    assert (
        origin_owner((proxy["data_dir"] / "packages" / "warmmixed" / ".origin").read_text())
        == "private"
    )

    # Control: a name that exists only upstream still fills from upstream on a miss
    # and serves the verified bytes, cached as mirror origin.
    up_only = make_wheel("warmupstream", "3.0", tmp_path / "only")
    _upload(upstream, up_only, "warmupstream")
    code, body, _ = http_get(f"{proxy['base_url']}/files/warmupstream/{up_only.name}")
    assert code == 200
    assert hashlib.sha256(body).hexdigest() == sha256_file(up_only)
    assert (proxy["data_dir"] / "packages" / "warmupstream" / up_only.name).exists()
    assert (
        origin_owner((proxy["data_dir"] / "packages" / "warmupstream" / ".origin").read_text())
        == "mirror"
    )


def test_private_prefix_blocks_proxy(proxy_pair_prefixed, tmp_path):
    upstream, proxy = proxy_pair_prefixed["upstream"], proxy_pair_prefixed["proxy"]
    wheel = make_wheel("acme-tool", "1.0", tmp_path)
    _upload(upstream, wheel, "acme-tool")

    # Inside the reserved namespace nothing falls through, claimed or not.
    code, _, _ = http_get(
        f"{proxy['simple']}acme-tool/index.json", headers={"Accept": ACCEPT_PEP691}
    )
    assert code == 404
    code, _, _ = http_get(f"{proxy['base_url']}/files/acme-tool/{wheel.name}")
    assert code == 404


def test_proxy_mirror_rules_gate_what_is_served(proxy_pair_wheels_only, tmp_path):
    upstream, proxy = (
        proxy_pair_wheels_only["upstream"],
        proxy_pair_wheels_only["proxy"],
    )
    wheel = make_wheel("filterpkg", "1.0", tmp_path)
    sdist = make_sdist("filterpkg", "1.0", tmp_path)
    _upload(upstream, wheel, "filterpkg")
    _upload(upstream, sdist, "filterpkg")

    data = get_index_json(proxy["simple"], "filterpkg")
    filenames = [f["filename"] for f in data["files"]]
    assert wheel.name in filenames
    assert sdist.name not in filenames

    # Excluded files aren't downloadable either — the mirror rules gate the cache.
    code, _, _ = http_get(f"{proxy['base_url']}/files/filterpkg/{sdist.name}")
    assert code == 404
    code, _, _ = http_get(f"{proxy['base_url']}/files/filterpkg/{wheel.name}")
    assert code == 200


def test_proxy_allowlist_gates_names_and_versions(proxy_pair_scoped, tmp_path):
    """The package scope (`--include-package`/`[mirror].include-packages`) is fail-closed
    on the proxy: only approved names fall through, and a version-pinned entry
    serves only matching versions — the pull twin of what `sync` mirrors."""
    upstream, proxy = proxy_pair_scoped["upstream"], proxy_pair_scoped["proxy"]
    allowed = make_wheel("allowed", "1.0", tmp_path)
    pinned_old = make_wheel("pinned", "1.0", tmp_path)
    pinned_new = make_wheel("pinned", "2.0", tmp_path)
    blocked = make_wheel("blocked", "1.0", tmp_path)
    _upload(upstream, allowed, "allowed")
    _upload(upstream, pinned_old, "pinned")
    _upload(upstream, pinned_new, "pinned")
    _upload(upstream, blocked, "blocked")

    # An approved name (no version pin) falls through and serves.
    data = get_index_json(proxy["simple"], "allowed")
    assert [f["filename"] for f in data["files"]] == [allowed.name]
    assert http_get(f"{proxy['base_url']}/files/allowed/{allowed.name}")[0] == 200

    # A version-scoped name serves only matching versions; the rest never cache.
    data = get_index_json(proxy["simple"], "pinned")
    filenames = [f["filename"] for f in data["files"]]
    assert pinned_new.name in filenames
    assert pinned_old.name not in filenames, "out-of-range version must not fall through"
    assert http_get(f"{proxy['base_url']}/files/pinned/{pinned_new.name}")[0] == 200
    assert http_get(f"{proxy['base_url']}/files/pinned/{pinned_old.name}")[0] == 404

    # An unapproved name is 404'd — fail-closed, even though upstream has it.
    code, _, _ = http_get(f"{proxy['simple']}blocked/index.json", headers={"Accept": ACCEPT_PEP691})
    assert code == 404
    assert http_get(f"{proxy['base_url']}/files/blocked/{blocked.name}")[0] == 404


def test_proxy_denylist_subtracts_from_open_proxy(proxy_pair_denylist, tmp_path):
    upstream, proxy = proxy_pair_denylist["upstream"], proxy_pair_denylist["proxy"]
    allowed = make_wheel("allowedopen", "1.0", tmp_path)
    blocked = make_wheel("blocked", "1.0", tmp_path)
    pinned_old = make_wheel("pinned", "1.0", tmp_path)
    pinned_new = make_wheel("pinned", "2.0", tmp_path)
    _upload(upstream, allowed, "allowedopen")
    _upload(upstream, blocked, "blocked")
    _upload(upstream, pinned_old, "pinned")
    _upload(upstream, pinned_new, "pinned")

    data = get_index_json(proxy["simple"], "allowedopen")
    assert [f["filename"] for f in data["files"]] == [allowed.name]
    assert http_get(f"{proxy['base_url']}/files/allowedopen/{allowed.name}")[0] == 200

    code, _, _ = http_get(f"{proxy['simple']}blocked/index.json", headers={"Accept": ACCEPT_PEP691})
    assert code == 404
    assert http_get(f"{proxy['base_url']}/files/blocked/{blocked.name}")[0] == 404

    data = get_index_json(proxy["simple"], "pinned")
    filenames = [f["filename"] for f in data["files"]]
    assert pinned_new.name in filenames
    assert pinned_old.name not in filenames
    assert http_get(f"{proxy['base_url']}/files/pinned/{pinned_new.name}")[0] == 200
    assert http_get(f"{proxy['base_url']}/files/pinned/{pinned_old.name}")[0] == 404


def test_proxy_deny_wins_over_allow(proxy_pair_deny_wins, tmp_path):
    upstream, proxy = proxy_pair_deny_wins["upstream"], proxy_pair_deny_wins["proxy"]
    wheel = make_wheel("both", "1.0", tmp_path)
    _upload(upstream, wheel, "both")

    code, _, _ = http_get(f"{proxy['simple']}both/index.json", headers={"Accept": ACCEPT_PEP691})
    assert code == 404
    assert http_get(f"{proxy['base_url']}/files/both/{wheel.name}")[0] == 404


def test_unknown_package_404s_through_proxy(proxy_pair):
    proxy = proxy_pair["proxy"]
    code, _, _ = http_get(
        f"{proxy['simple']}no-such-package-anywhere/index.json",
        headers={"Accept": ACCEPT_PEP691},
    )
    assert code == 404


def test_deleted_mirror_file_reheals_within_presence_ttl(proxy_pair, tmp_path):
    """A mirror-cached artifact deleted through the admin API must re-mirror on the
    next request, not serve a stale 404. The warm-hit presence cache records a file
    only once it is already on disk, so the *second* GET arms the proof; the delete
    then has to invalidate it, or a re-request inside PRESENCE_TTL (60s) hits the
    stale "present", skips the re-mirror HEAD, and 404s where upstream still has the
    file."""
    upstream, proxy = proxy_pair["upstream"], proxy_pair["proxy"]
    wheel = make_wheel("reheal", "1.0", tmp_path)
    _upload(upstream, wheel, "reheal")

    url = f"{proxy['base_url']}/files/reheal/{wheel.name}"
    cached = proxy["data_dir"] / "packages" / "reheal" / wheel.name

    # GET #1 downloads-verifies-caches the file; GET #2 finds it on disk and records
    # the presence proof (the warm-hit fast path). Both serve the verified bytes.
    for _ in range(2):
        code, body, _ = http_get(url)
        assert code == 200
        assert hashlib.sha256(body).hexdigest() == sha256_file(wheel)
    assert cached.exists()

    # Admin-delete the cached artifact — a single-bucket mirror eviction.
    code, _, _ = http_request_auth(
        "DELETE", url, username=proxy["user"], password=proxy["password"]
    )
    assert code == 204
    assert not cached.exists(), "delete must remove the cached artifact"

    # Re-request immediately, well inside PRESENCE_TTL: the presence proof must have
    # been invalidated by the delete, so the proxy re-mirrors from upstream and
    # serves 200. Before the fix the stale presence entry served a local 404.
    code, body, _ = http_get(url)
    assert code == 200, "a deleted mirror file must re-mirror from upstream, not 404"
    assert hashlib.sha256(body).hexdigest() == sha256_file(wheel)
    assert cached.exists(), "the re-request must re-cache the artifact from upstream"


def test_proxy_persists_upstream_quarantine_status(proxy_pair, tmp_path):
    """The proxy relayed upstream PEP 792 status only into its in-memory listing,
    so a lockfile-pinned `/files/` URL served cached bytes of an upstream-
    quarantined project ungated. The proxy now persists the observed status to
    `.project-status.json` (mirror origin) — the durable set the malware/
    quarantine byte gate consults, so those bytes get refused."""
    upstream, proxy = proxy_pair["upstream"], proxy_pair["proxy"]
    pkg = "quarantinedup"
    wheel = make_wheel(pkg, "1.0", tmp_path)
    _upload(upstream, wheel, pkg)

    # Upstream quarantines the project (the admin PEP 792 endpoint).
    admin_user = upstream.get("admin_user", upstream["user"])
    admin_pass = upstream.get("admin_password", upstream["password"])
    code, _, _ = http_request_auth(
        "POST",
        f"{upstream['base_url']}/project/{pkg}/status",
        username=admin_user,
        password=admin_pass,
        data=json.dumps({"status": "quarantined", "reason": "compromised"}).encode(),
    )
    assert code == 200, f"upstream status set returned {code}"

    # Wait until the upstream's own listing carries the quarantine…
    deadline = time.time() + 30
    while time.time() < deadline:
        code, body, _ = http_get(f"{upstream['simple']}{pkg}/", headers={"Accept": ACCEPT_PEP691})
        doc = json.loads(body) if code == 200 else {}
        if doc.get("project-status", {}).get("status") == "quarantined":
            break
        time.sleep(0.3)
    else:
        raise AssertionError("upstream never published the quarantine")

    # …then a proxy listing fetch observes it and persists it durably. Before the
    # fix, the proxy showed the freeze in its listing but never wrote it to disk.
    status_path = proxy["data_dir"] / "packages" / pkg / ".project-status.json"
    deadline = time.time() + 30
    while time.time() < deadline:
        http_get(f"{proxy['simple']}{pkg}/", headers={"Accept": ACCEPT_PEP691})
        if status_path.exists():
            break
        time.sleep(0.3)
    else:
        raise AssertionError("proxy never persisted the upstream quarantine status")

    doc = json.loads(status_path.read_text())
    assert doc["status"] == "quarantined"
    assert doc.get("pypiron-origin") == "mirror", "persisted proxy status must be mirror-origin"


# ---------------------- empty package scope is a startup error ----------------
#
# An explicitly-provided-but-empty package list used to resolve to zero specs,
# and zero specs reads as "no scope configured" — i.e. an open proxy. So
# `PYPIRON_INCLUDE_PACKAGE=""` in the environment silently erased a curated
# `[mirror].include-packages` and reopened the proxy to all of PyPI. The empty
# value is now refused at startup, before the listener binds; the short timeout
# below doubles as the "it never started serving" assertion.


def _serve(pypiron_bin, *args, env=None):
    proc_env = dict(os.environ)
    if env:
        proc_env.update(env)
    return subprocess.run(
        [str(pypiron_bin), "serve", "--bind-addr", f"127.0.0.1:{find_free_port()}", *args],
        capture_output=True,
        text=True,
        timeout=20,
        env=proc_env,
    )


def _scoped_config(tmp_path, key: str = "include-packages") -> str:
    cfg = tmp_path / "pypiron.toml"
    cfg.write_text(f'[mirror]\n{key} = ["requests"]\n')
    return str(cfg)


def test_empty_env_include_package_does_not_erase_config_scope(pypiron_bin, tmp_path):
    """An empty PYPIRON_INCLUDE_PACKAGE outranks the config file's package
    scope (CLI/env replaces the file's list), so before the fix it left the
    proxy scopeless and open to every name on PyPI."""
    cp = _serve(
        pypiron_bin,
        "--data-dir",
        str(tmp_path / "data"),
        "--config",
        _scoped_config(tmp_path),
        "--proxy-upstream",
        "https://pypi.org",
        env={"PYPIRON_INCLUDE_PACKAGE": ""},
    )
    out = cp.stdout + cp.stderr
    assert cp.returncode != 0, f"empty include scope must refuse startup:\n{out}"
    assert "include-package" in out, out
    assert "Omit the flag" in out, out


def test_empty_env_exclude_package_does_not_erase_config_denylist(pypiron_bin, tmp_path):
    """Same erasure on the deny axis: an empty PYPIRON_EXCLUDE_PACKAGE would
    wipe a configured denylist and widen what the proxy serves."""
    cp = _serve(
        pypiron_bin,
        "--data-dir",
        str(tmp_path / "data"),
        "--config",
        _scoped_config(tmp_path, "exclude-packages"),
        "--proxy-upstream",
        "https://pypi.org",
        env={"PYPIRON_EXCLUDE_PACKAGE": ""},
    )
    out = cp.stdout + cp.stderr
    assert cp.returncode != 0, f"empty exclude scope must refuse startup:\n{out}"
    assert "exclude-package" in out, out


def test_empty_config_package_list_refuses_startup(pypiron_bin, tmp_path):
    """`include-packages = []` in pypiron.toml reads like "scope it to nothing"
    but used to mean "serve everything"; it is a startup error instead."""
    cfg = tmp_path / "pypiron.toml"
    cfg.write_text("[mirror]\ninclude-packages = []\n")
    cp = _serve(
        pypiron_bin,
        "--data-dir",
        str(tmp_path / "data"),
        "--config",
        str(cfg),
        "--proxy-upstream",
        "https://pypi.org",
    )
    out = cp.stdout + cp.stderr
    assert cp.returncode != 0, f"empty include-packages must refuse startup:\n{out}"
    assert "include-packages" in out, out


def test_no_package_scope_still_starts_open(proxy_pair, tmp_path):
    """The documented default is unchanged: with no package scope configured at
    all, the proxy serves any non-private name."""
    upstream, proxy = proxy_pair["upstream"], proxy_pair["proxy"]
    wheel = make_wheel("openscope", "1.0", tmp_path)
    _upload(upstream, wheel, "openscope")
    data = get_index_json(proxy["simple"], "openscope")
    assert [f["filename"] for f in data["files"]] == [wheel.name]
