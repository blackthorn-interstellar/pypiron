---
description: Serve packages without internet access by syncing an approved package list over a private link or removable media.
---

# Run without internet access

An offline pypiron server never contacts PyPI. Put every required package in
its storage before clients need it. `sync` does not resolve dependencies, so
your list must include direct and transitive packages.

Choose one transfer path:

- A private network link: `sync` uploads directly to the offline server.
- Removable media: `sync` fills a temporary staging server; you stop it before
  copying its data directory.

Install the same pypiron release on both hosts. On the connected host:

```bash
uv tool install pypiron
```

For the offline host, carry the matching binary from
[GitHub Releases](https://github.com/blackthorn-interstellar/pypiron/releases)
through your approved software-transfer process.

## The serving host

The examples use `/etc/pypiron` for configuration and `/var/lib/pypiron` for
data. Create both directories first and grant the pypiron service account read
access to the config and read/write access to the data directory.

Save this as `/etc/pypiron/pypiron.toml` on the offline host:

```toml
private-prefix = "acme"

[serve]
bind-addr = "0.0.0.0:8080"
data-dir = "/var/lib/pypiron"
```

There is no `proxy-upstream`. Start the server:

```bash
export PYPIRON_ADMIN_PASS='change-this-password'
pypiron serve --config /etc/pypiron/pypiron.toml
```

Leave the password unset for a read-only server. Direct network sync needs it;
the removable-media path does not.

## The connected host

Create `packages.txt` with one approved package requirement per line:

```text
six==1.17.0
```

For a real uv project, export its locked dependency set:

```bash
uv export --frozen --no-dev --no-emit-project \
  --no-hashes --no-annotate --no-header \
  | sed -E 's/[[:space:]]*;.*$//' > packages.txt
```

The command removes environment markers, so the result is a conservative union
of locked registry packages. Review it before syncing and remove local paths or
Git dependencies, which cannot be fetched from PyPI. Use
[mirror filters](../reference/configuration.md#mirror-selection) to restrict
wheels by the offline clients' Python versions and platforms.

For a private network transfer, save this as `pypiron.toml` beside the list:

```toml
[mirror]
include-packages-from = "packages.txt"
exclude-newer = "7 days"

[sync]
from = "https://pypi.org"
to = "https://pypi.internal"
admin-user = "admin"
advisory-feed = "https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip"
```

Preview the transfer, then run it:

```bash
export PYPIRON_SYNC_ADMIN_PASS='change-this-password'
pypiron sync --config pypiron.toml --dry-run
pypiron sync --config pypiron.toml
```

The password belongs to the offline destination. Use TLS or a private transfer
network. Re-run the same command on a schedule to add new approved releases,
apply upstream yanks, and refresh the advisory feed.

Confirm an install from inside the offline network:

```bash
uv venv verify-install
uv pip install --python verify-install/bin/python \
  --default-index https://pypi.internal/simple/ six==1.17.0
```

<a id="no-network-path-at-all-carry-it-on-media"></a>

## Transfer on removable media

On the connected host, start a staging server in one terminal:

```bash
export ADMIN='temporary-password'
STAGING="$PWD/pypiron-staging-$(date +%Y%m%d-%H%M%S)"

test ! -e "$STAGING" &&
  mkdir "$STAGING" &&
  PYPIRON_DATA_DIR="$STAGING" PYPIRON_ADMIN_PASS="$ADMIN" \
    pypiron serve --bind-addr 127.0.0.1:8081
```

Leave it running. In a second terminal, check readiness and transfer the
packages:

```bash
export ADMIN='temporary-password'
curl -fsS http://localhost:8081/ready &&
PYPIRON_SYNC_ADMIN_USER=admin PYPIRON_SYNC_ADMIN_PASS="$ADMIN" \
  pypiron sync --config pypiron.toml \
    --to http://localhost:8081
```

When sync finishes, press ++ctrl+c++ in the first terminal. Then build and
verify the index before creating the archive:

```bash
pypiron rebuild-index --data-dir "$STAGING" &&
  pypiron verify-index --data-dir "$STAGING" --deep &&
  tar -C "$STAGING" -cf mirror.tar . &&
  sha256sum mirror.tar > mirror.tar.sha256
```

The config's advisory-feed setting puts the current malware data in the same
tree. Carry both files across the boundary and scan them according to your
normal media process.

On the offline host, stop pypiron and extract into a new directory:

```bash
(
  set -eu
  sha256sum -c mirror.tar.sha256
  STAMP=$(date +%Y%m%d-%H%M%S)
  NEW="/var/lib/pypiron.new-$STAMP"
  OLD="/var/lib/pypiron.old-$STAMP"
  test ! -e "$NEW" && test ! -e "$OLD"
  mkdir "$NEW"
  tar -C "$NEW" -xf mirror.tar
  pypiron verify-index --data-dir "$NEW" --deep
  mv /var/lib/pypiron "$OLD" && mv "$NEW" /var/lib/pypiron
)
```

The swap runs only if the checksum, extraction, and deep verification succeed.
Start pypiron again, then keep the old directory until clients have installed
from the replacement. Use new `STAGING`, `NEW`, and `OLD` names for every
delivery. Never extract over the live directory: removed and yanked files would
remain.

<a id="ferry-the-advisory-feed"></a>

## Transfer the advisory feed

The examples transfer the OSV advisory feed with the packages. A network sync
pushes it directly; a media transfer carries it in the data directory. The
offline server loads the new feed without an internet connection.

Set `PYPIRON_MALWARE_BLOCK=true` on the serving host to refuse startup until a
delivered feed has loaded. Without that explicit setting, the binary's bundled
block list protects first boot and the delivered feed replaces it.

## Keep it fresh

Alert on `pypiron_advisory_snapshot_age_seconds`. A networked transfer can run
from cron; a media transfer is current only as of its last delivery.

See [mirror selection](../reference/configuration.md#mirror-selection) for
package filters and [security](../security.md) for the cooldown and malware
behavior.

## Watch an offline install

[Watch the 40-second demonstration](../assets/offline-demo.mp4): a private
wheel and its prepared public dependency install with external networking
disabled and the client cache empty. A copied data directory passes the same
check; an unprepared public package fails.

To run the experiment yourself, follow the
[Docker reproduction](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/OFFLINE_DEMO.md).
Preparation requires internet access. The demonstration disables advisory-feed
refresh and uses one Python version and platform; it is not a substitute for
the deployment and feed-transfer steps above.
