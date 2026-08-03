---
description: "Serve packages on a host with no internet access: sync an approved list from a connected host and ferry the malware advisory feed alongside it."
---

# Run without internet access

Two hosts. A connected host runs `pypiron sync` against PyPI with an approved
package list; the serving host runs `pypiron serve` and never touches the
internet. It answers installs from what sync delivered — no upstream, and
none needed.

## The serving host

An ordinary server with no `proxy-upstream`:

```toml
private-prefix = "acme"

[serve]
bind-addr = "0.0.0.0:8080"
```

```bash
export PYPIRON_ADMIN_PASS="$ADMIN"
pypiron serve
```

`private-prefix` reserves your package names. `PYPIRON_ADMIN_PASS` enables
publishing — which is how sync delivers, so set it here.

## The connected host

`sync` needs an approval list. `packages.txt`:

```text
requests>=2.32,<3
urllib3
six
```

List syntax, version specifiers, and excludes:
[Approval lists](../concepts.md#what-it-keeps-out).

`pypiron.toml` on the connected host:

```toml
[mirror]
include-packages-from = "packages.txt"
exclude-newer = "7 days"

[sync]
from = "https://pypi.org"
to = "http://airgapped:8080"
package-concurrency = 8
```

Run it:

```bash
export PYPIRON_SYNC_ADMIN_PASS="$ADMIN"
pypiron sync --config pypiron.toml --dry-run
pypiron sync --config pypiron.toml
```

`PYPIRON_SYNC_ADMIN_PASS` is the serving host's admin password: sync delivers
over HTTP like any other publisher. Re-running sync is normal — existing files
stay. Yanks, removals, and project status follow upstream.

For a full offline copy — no cooldown, yanked files included — set
`exclude-newer = ""` and `include-yanked = true` in `[mirror]`.

## Ferry the advisory feed

The serving host refuses downloads of known malware, but with no internet it
can't fetch the advisory feed itself — you deliver it. Point sync at the OSV
export on the connected side and the feed travels with the packages:

```bash
pypiron sync --to http://airgapped:8080 \
  --advisory-feed https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip
```

When the source is another pypiron instead of public PyPI, add nothing: sync
relays the source server's own feed alongside the packages by default.

No sync? Point the server's own `--advisory-feed` at a local file and have
your ferry drop a fresh copy there on whatever schedule it runs:

```bash
pypiron serve --advisory-feed /var/lib/pypiron/osv-pypi-all.zip
```

However the feed arrives, blocking behaves the same. An unfed box says so
in its logs until the first feed lands, then arms itself without a restart.

## Keep it fresh

A server with internet access blocks a new advisory minutes after it's
published. A ferried mirror is only as fresh as its last delivery, so run
`pypiron sync` on an hourly cron. One run picks up new versions of approved
packages and the advisory snapshot together. The
`pypiron_advisory_snapshot_age_seconds` gauge tracks the loaded feed's age.
Alert when it climbs past your refresh window:
[Monitoring](../concepts.md#what-it-tells-you).

## See also

- [Security features](../security.md) — the cooldown, malware blocking, and
  name protection the serving host enforces.
- [Configuration → Sync](../reference/configuration.md#sync) — every sync flag
  and env var.
- [Survive a region or cloud outage](multi-region.md) — a failover keeps the
  mirror intact with no upstream to re-fetch from: the synced packages
  replicate to every bucket.
