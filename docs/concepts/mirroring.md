# Mirroring & proxying

pypiron has two ways to serve public packages alongside your private ones, on the
same index URL. Pick by who pulls and when:

- **On-demand proxy** (`--proxy-upstream`) — lazy pull-through. A file is fetched
  from upstream the first time someone asks for it, then cached in storage
  forever. The serving node needs egress to upstream.
- **Bulk sync** (`pypiron sync`) — push an allowlist ahead of time. A separate
  HTTP client downloads selected packages and uploads them to the server. Only
  the syncing host needs egress.

Both write the same files to the same storage tree, so a server can run both: the
proxy fills cache misses, and `sync` pre-loads what you know you need.

## On-demand proxy

Add `--proxy-upstream` to `serve`:

```bash
pypiron serve --admin-pass "$ADMIN" --proxy-upstream https://pypi.org
```

A request for a package the server doesn't hold is resolved against the upstream
PEP 691 index. The package page is rendered with pypiron's own `/files/` URLs.
On the first artifact GET, pypiron streams the file from upstream, verifies it
against the upstream sha256, and commits it to storage. Every later request is
served from local storage — **whether upstream is up or down**.

- Listings are cached for 60 seconds, so a new upstream release shows up within
  about a minute; cached artifacts are immutable and kept forever.
- If upstream is unreachable, a still-valid cached listing is reused and
  already-cached packages keep installing.
- Mirror-selection flags gate what the proxy serves and caches — the *same*
  flags, env vars, and `[mirror]` table that `sync` uses (`--include-format
  wheel`, `--exclude-newer`, the tag selectors). Set the slice once; it applies
  to whichever you run. See
  [Configuration](../reference/configuration.md#mirror-selection).
- An **approved-package list** (`--include-package` / `[mirror].include-packages`) makes
  the proxy **fail-closed**: with a list set, only those names fall through to
  upstream and every other name is `404`'d, and a version-pinned entry serves
  only matching versions. With no list, the proxy serves any non-private name on
  demand (the default). This is the same list `sync` mirrors — set it once, in
  `[mirror]`, and the push and pull paths agree.

!!! note
    A name reserved by `--private-prefix`, already claimed `private` by an
    upload, or outside a configured approved list never falls through to
    upstream. That is the dependency-confusion defense — see
    [Authentication](authentication.md) and the
    [Private + public guide](../guides/private-and-public.md).

## Bulk sync

`pypiron sync` is a pure HTTP client. It reads packages from a PEP 691 source
(PyPI by default), downloads the selected files, and POSTs each one to the
destination's `/legacy/` endpoint as a **mirror upload** — carrying PyPI's true
`upload-time` and yank state. The server owns every storage write, so `sync`
needs nothing but the destination URL and an admin credential:

```bash
pypiron sync \
  --to http://HOST:8080 \
  --admin-user admin --admin-pass "$ADMIN" \
  --include-package "requests>=2.20,<3" --include-package numpy
```

Put the allowlist and mirror rules in `pypiron.toml` instead (auto-discovered in
the working directory). The allowlist and mirror rules live in `[mirror]` — shared with
the proxy; only the destination is sync-specific (`[sync]`):

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

`--to` is required; there is no direct-to-storage mode. Full option list and the
config-file layering rules are in
[Configuration](../reference/configuration.md#sync-mirror-over-http).

### Re-sync: reconcile and conditional fetch

A re-`sync` does more than add new files. It **reconciles** the mutable metadata
of files it already holds:

- yank state is brought in line with upstream (set, cleared, or its reason
  updated),
- a file gone from upstream is flagged yanked `removed upstream` — kept
  downloadable, just skipped by installers,
- PEP 792 project status is relayed.

**Artifacts are never deleted.** A mirror only ever grows.

To keep "reconcile every run" cheap, each project is fetched conditionally. The
last upstream ETag is remembered server-side and replayed as `If-None-Match`, so
an unchanged upstream answers `304` and the whole project is skipped:

```bash
pypiron sync          # re-run anytime; unchanged projects 304 and are skipped
```

A normal run therefore reconciles every project whose upstream listing actually
changed — and a yank or removal *does* change the listing, so it gets a `200` and
is reconciled on an ordinary `sync`. `--full` is not required to pick up a fresh
yank. Run it periodically (e.g. nightly) as the self-heal: it ignores the cursor
and re-fetches every project unconditionally, closing the gaps a `304` can't
see — a stale upstream-CDN response that 304s right after a yank, or dest-side
drift (a manual admin yank toggle, a restore from backup) that no upstream change
reflects:

```bash
pypiron sync --full   # ignore the cursor; re-fetch and fully reconcile everything
```

!!! tip
    The cursor is a pure cache. Delete it (`/sync/cursors`, admin-only) and the
    next run re-fetches. Changing the source, a filter, or a package's
    specifiers invalidates it automatically. Details in
    [Configuration](../reference/configuration.md#re-sync-reconcile-and-conditional-fetch).

## Proxy vs sync

|                     | On-demand proxy (`--proxy-upstream`) | Bulk sync (`pypiron sync`)            |
| ------------------- | ------------------------------------ | ------------------------------------- |
| When files arrive   | Lazily, on first request             | Ahead of time, when you run `sync`    |
| Who initiates       | The serving node, per cache miss     | A separate client (any host with egress) |
| Package set         | The `[mirror]` slice — any non-private name, or only an approved list if you set one | An explicit allowlist (required) |
| Egress from server  | Required (until cached)              | None — the syncing host needs it      |
| Offline serving     | Cached files only                    | Everything you synced                 |
| Reconcile yanks/removals | Listings refresh every 60s      | On re-sync (`--full` for a sweep)     |

## See also

- [Air-gapped mirror](../guides/air-gapped-mirror.md) — `sync` when the serving
  node has no egress.
- [Private + public](../guides/private-and-public.md) — one index for uploads,
  synced, and proxied packages.
- [Supply-chain defense](supply-chain.md) — `--exclude-newer` for a
  reproducible, historically-correct cutoff on both paths.
- [Configuration](../reference/configuration.md#mirror-selection) — the shared
  mirror-selection surface, plus the `pypiron.toml` reference.
