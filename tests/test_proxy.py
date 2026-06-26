"""On-demand proxying (--proxy-upstream): package pages from upstream
metadata, artifacts downloaded-verified-cached as mirror-origin packages, the
origin model enforced throughout. The upstream in these tests is a second
pypiron instance — it speaks the same PEP 691 + PEP 700 the proxy consumes."""

from __future__ import annotations

import hashlib

import pytest

from .helpers import (
    ACCEPT_PEP691,
    get_index_json,
    http_get,
    kill_process_tree,
    make_sdist,
    make_wheel,
    sha256_file,
    upload_legacy,
    wait_for_file_in_index,
)

pytestmark = pytest.mark.integration


def _upload(server, dist, package):
    upload_legacy(server["legacy"], dist, username=server["user"], password=server["password"])
    wait_for_file_in_index(server["simple"], package, dist.name)


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
    assert (pkg_dir / ".origin").read_text().strip() == "mirror"

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
        proxy["data_dir"] / "packages" / "mixedpkg" / ".origin"
    ).read_text().strip() == "private"


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
