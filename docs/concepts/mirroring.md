# Mirroring & proxying

Serve packages from public PyPI on the same URL as your private ones. Two ways
to fill the cache — pick by whether your serving node can reach the internet:

- **On-demand proxy** — for a network with internet access. A package is pulled
  from PyPI on first install, then cached forever. Nothing to curate up front.
- **Bulk sync** — for an air-gapped network, or when you want a known set of
  packages waiting before anyone asks. A separate client with internet access
  pre-loads an approved package list onto the server, which never needs egress.

Both land the same files in the same storage, so one server can do both at once:
the proxy fills cache misses while `sync` pre-loads what you'll need.

## On-demand proxy

Best when your serving node has internet access and you'd rather not maintain a
list. Point `serve` at PyPI:

```bash
pypiron serve --admin-pass "$ADMIN" --proxy-upstream https://pypi.org
```

Any package you don't already host is fetched from PyPI on first install,
checksum-verified, and cached. Later installs serve straight from your own
storage — **whether PyPI is up or down**.

- A new release on PyPI shows up within 60s; once cached, a file never changes
  and is kept forever.
- If PyPI is unreachable, the last good listing is reused and everything already
  cached keeps installing.
- The same selection rules `sync` uses — `--include-format wheel`,
  `--exclude-newer`, the tag selectors — also gate what the proxy serves and
  caches. Set the slice once in `[mirror]` and it applies to both. See
  [Configuration](../reference/configuration.md#mirror-selection).
- Set an **approved package list** (`--include-package` /
  `[mirror].include-packages`) and the proxy goes **fail-closed**: only those
  names reach PyPI, every other name is refused, and a version-pinned entry
  serves only matching versions. With no list, any public name is served on
  demand (the default). It's the same list `sync` uses — set it once in
  `[mirror]` and both paths agree.

!!! note
    A name reserved by `--private-prefix`, already reserved by a private upload,
    or outside a configured approved list never falls through to PyPI. That is
    the dependency-confusion defense — see
    [Authentication](authentication.md) and
    [Add public PyPI](../guides/deploy.md#add-public-pypi).

## Bulk sync

Best when your serving node has no internet access, or when you want a known set
of packages in place before anyone installs. A separate client with internet
access pre-loads your approved package list onto the server — the server never
needs egress.

`pypiron sync` reads from PyPI, downloads the packages you've approved, and
uploads each one to the destination as a **mirror upload** — preserving PyPI's
real upload time and yank state. The server handles every storage write, so
`sync` needs nothing but the destination URL and an admin credential:

```bash
pypiron sync \
  --to http://HOST:8080 \
  --admin-user admin --admin-pass "$ADMIN" \
  --include-package "requests>=2.20,<3" --include-package numpy
```

Or put the approved list and mirror rules in `pypiron.toml` (auto-discovered in
the working directory). They live in `[mirror]` — shared with the proxy; only
the destination is sync-specific (`[sync]`):

```toml
[mirror]
include-packages = ["requests>=2.20,<3", "numpy", "pandas"]
include-format = ["wheel"]
exclude-newer = "2026-01-01T00:00:00Z"

[sync]
to = "http://HOST:8080"
admin-user = "admin"                     # password via PYPIRON_SYNC_ADMIN_PASS
```

```bash
export PYPIRON_SYNC_ADMIN_PASS="$ADMIN"
pypiron sync
```

`--to` is required — `sync` always writes through the server, never directly to
storage. The full option list and config-file layering rules are in
[Configuration](../reference/configuration.md#sync-mirror-over-http).

### Re-syncing keeps the mirror current

Run `sync` again anytime — it does more than pull new files. It also reconciles
the metadata of files you already hold with PyPI:

- a yank is applied, cleared, or its reason updated to match upstream,
- a release pulled from PyPI is marked yanked (`removed upstream`) — still
  downloadable, skipped by installers,
- each project's status (active, archived, and so on) is kept in sync with
  upstream.

**Files are never deleted. A mirror only ever grows.**

Re-runs are cheap: projects unchanged upstream are skipped — only those whose
listing changed are re-fetched and reconciled. A yank or removal *changes* the
listing, so an ordinary `sync` picks it up; you don't need `--full` for a fresh
yank:

```bash
pypiron sync          # re-run anytime; unchanged projects are skipped
```

Run a full sweep periodically (say, nightly) to catch the rare things a quick
re-run can miss — a stale PyPI CDN response that hides a fresh yank, or drift on
your own side like a manual admin yank toggle or a restore from backup:

```bash
pypiron sync --full   # re-fetch and fully reconcile every project
```

!!! tip
    Skipping unchanged projects relies on a cursor that's pure cache. Delete it
    (`/sync/cursors`, admin-only) and the next run re-fetches from scratch.
    Changing the source, a filter, or a package's version specifiers invalidates
    it automatically. Details in
    [Configuration](../reference/configuration.md#re-sync-reconcile-and-conditional-fetch).

## Proxy vs sync

|                     | On-demand proxy (`--proxy-upstream`) | Bulk sync (`pypiron sync`)            |
| ------------------- | ------------------------------------ | ------------------------------------- |
| When files arrive   | Lazily, on first request             | Ahead of time, when you run `sync`    |
| Who initiates       | The serving node, per cache miss     | A separate client (any host with egress) |
| Package set         | The `[mirror]` slice — any non-private name, or only an approved list if you set one | An explicit approved list (required) |
| Egress from server  | Required (until cached)              | None — the syncing host needs it      |
| Offline serving     | Cached files only                    | Everything you synced                 |
| Reconcile yanks/removals | Listings refresh every 60s      | On re-sync (`--full` for a sweep)     |

## See also

- [Air-gapped mirror](../guides/air-gapped-mirror.md) — `sync` when the serving
  node has no egress.
- [Deploy → Add public PyPI](../guides/deploy.md#add-public-pypi) — one index
  for uploads, synced, and proxied packages.
- [Supply-chain defense](supply-chain.md) — `--exclude-newer` for a
  reproducible, historically-correct cutoff on both paths.
- [Configuration](../reference/configuration.md#mirror-selection) — the shared
  mirror-selection surface, plus the `pypiron.toml` reference.
