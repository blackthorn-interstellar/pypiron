# Air-gapped mirror

Run a package server that never touches the internet — and still install
everything your team needs from it. Choose which packages it carries by
pre-loading an approved list with `pypiron sync` from a host that *can* reach
PyPI.

This is the no-egress option from [Deploy](deploy.md): the serve node is the
same `serve` process, without `--proxy-upstream`. Run it on any platform with
the same [launchers](deploy.md#run-it-on-your-platform).

`sync` runs from any host that can reach PyPI, whatever the server's storage —
disk, S3, GCS, or Azure. It needs only the server URL and an admin credential:
it downloads each file from PyPI and uploads it to the server's `/legacy/`
endpoint.

```text
PyPI  ──▶  sync host  ──▶  pypiron server  ──▶  developers / CI
          (has egress)     (air-gapped)
```

Developers and CI installing from the air-gapped server need no internet either.

## 1. Start the server

On the air-gapped node. `--admin-pass` enables the admin role (username
defaults to `admin`) — the credential `sync` authenticates with.

```bash
uvx pypiron serve --admin-pass "$ADMIN"
```

Without `--proxy-upstream`, the server never reaches PyPI on its own. It serves
only what you push into it.

## 2. Define the approved package list

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

Each `[mirror].include-packages` entry is a name and optional version constraint
(e.g. `requests>=2.20,<3`) — pin the versions you want mirrored. The same
`[mirror]` settings drive both the sync and the on-demand proxy:
`include-format = ["wheel"]` skips sdists; `exclude-newer` mirrors only files
PyPI received before the cutoff.

## 3. Run the sync

The password comes from the environment, never the config file.

```bash
export PYPIRON_SYNC_ADMIN_PASS="$ADMIN"
pypiron sync
```

`sync` checks the destination once (server reachable, credentials accepted),
then mirrors each package in parallel with a live progress meter.

Re-runs are cheap: sync skips projects unchanged upstream and files you already
hold, so a repeat moves only what's new. See
[Mirroring](../concepts/mirroring.md) for how it decides.

!!! tip
    `pypiron sync --dry-run` prints what would be mirrored and writes nothing.
    Size a run before committing to the download.

## 4. Keep it current

A normal run already picks up yanks and removals: installers stop seeing a file
the moment upstream pulls it. Run a full pass on a schedule (e.g. nightly) to
catch removals and yanks a quick re-sync misses:

```bash
pypiron sync --full
```

Either way, yank state matches upstream, and a file gone from upstream is marked
yanked `removed upstream` — still downloadable, skipped by installers. Mirrored
files are never deleted.

## Install from the mirror

Point clients at the air-gapped server as their **only** index, so installs
never fall through to an unreachable PyPI.

=== "uv"
    ```bash
    uv add --default-index http://HOST:8080/simple/ requests numpy
    ```

=== "pip"
    ```bash
    pip install --index-url http://HOST:8080/simple/ requests numpy
    ```

With a read credential (`--read-user`/`--read-pass`), `/simple/` and `/files/`
require auth — put it in the index URL or your client's config. See
[Authentication](../concepts/authentication.md).

## Mirror selection

`exclude-newer` and `include-format` are two of the rules that gate what a run
adds. The full set — package include/exclude, format, python/abi/platform tags,
date cutoffs — is in
[Configuration](../reference/configuration.md#mirror-selection), and the same
`[mirror]` settings govern the on-demand proxy. For how mirroring reconciles
yanks, removals, and project status, see [Mirroring](../concepts/mirroring.md).
