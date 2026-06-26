# Air-gapped mirror

The serving node has no egress: it can't reach PyPI. Pre-load an allowlist of
packages with `pypiron sync`, run from a host that *can* reach PyPI.

`sync` is a pure HTTP client. It needs the server's URL and the admin
credential — nothing about where the server keeps its bytes (disk, S3, GCS,
Azure). Each file is downloaded from the source and POSTed to the server's
`/legacy/`; the server owns every storage write.

```text
PyPI  ──▶  sync host  ──▶  pypiron server  ──▶  developers / CI
          (has egress)     (air-gapped)
```

Developers and CI installing from the air-gapped server need no internet either.

## 1. Start the server

On the air-gapped node. `--admin-pass` enables the admin role (username
defaults to `admin`), which is the credential `sync` authenticates with.

```bash
uvx pypiron serve --admin-pass "$ADMIN"
```

With no `--proxy-upstream`, the server never tries to reach PyPI on its own.
Everything it serves is what you push into it.

## 2. Define the allowlist

On the sync host, put the destination, credential, package set, and mirror rules in
`pypiron.toml` — auto-discovered in the working directory.

```toml
[mirror]
include-packages = ["requests>=2.20,<3", "numpy", "pandas"]
include-format = ["wheel"]
exclude-newer = "2026-01-01T00:00:00Z"   # reproducible, historically-correct cutoff

[sync]
to = "http://HOST:8080"
admin-user = "admin"                     # password via PYPIRON_SYNC_ADMIN_PASS
```

Each `[mirror].include-packages` entry is a name with optional PEP 440 specifiers, so you
pin exactly the versions you want mirrored. The whole `[mirror]` slice — the
allowlist included — is shared with the proxy: `include-format = ["wheel"]`
skips sdists; `exclude-newer` mirrors only files PyPI received before the cutoff.

## 3. Run the sync

The password comes from the environment, never the config file.

```bash
export PYPIRON_SYNC_ADMIN_PASS="$ADMIN"
pypiron sync
```

`sync` preflights the destination once (server reachable, credentials accepted),
then mirrors each package in parallel and prints a live progress meter.

Re-run it anytime. Each project is fetched conditionally: an unchanged upstream
answers `304` and is skipped, and files the server already holds are skipped, so
a re-run only moves what's new.

!!! tip
    `pypiron sync --dry-run` prints what would be mirrored and writes nothing.
    Use it to size a run before committing to the download.

## 4. Keep it current

A normal run already reconciles yanks and removals: yanking or removing a file
upstream changes the project's listing, so the conditional fetch gets a `200`
(not a `304`) and reconciles it. You don't need `--full` to pick up a fresh yank.

Run a full pass on a schedule (e.g. nightly) as the *self-heal* — it ignores the
conditional-fetch memo and re-reconciles every project unconditionally, which
closes the gaps a `304` can't see: a stale upstream-CDN response that answers
`304` right after a yank, or dest-side drift (a manual admin yank toggle, a
restore from backup) that no upstream change reflects.

```bash
pypiron sync --full
```

Either way: yank state is brought in line with upstream, and a file gone from
upstream is flagged yanked `removed upstream` (kept downloadable, skipped by
installers). Mirrored artifacts are never deleted.

## Install from the mirror

Point clients at the air-gapped server as their **only** index, so resolution
never falls through to an unreachable PyPI.

=== "uv"
    ```bash
    uv add --default-index http://HOST:8080/simple/ requests numpy
    ```

=== "pip"
    ```bash
    pip install --index-url http://HOST:8080/simple/ requests numpy
    ```

If you set a read credential (`--read-user`/`--read-pass`), `/simple/` and
`/files/` require auth — put the credentials in the index URL or your client's
config. See [Authentication](../concepts/authentication.md).

## Mirror selection

`exclude-newer` and `include-format` are two of the mirror rules that gate what a
run adds. The full set — package include/exclude, format, python/abi/platform
tags, date cutoffs — is in
[Configuration](../reference/configuration.md#mirror-selection), and the same
`[mirror]` slice governs the on-demand proxy. For how mirroring reconciles yanks,
removals, and project status, see [Mirroring](../concepts/mirroring.md).
