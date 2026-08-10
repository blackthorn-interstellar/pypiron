---
description: "Serve packages on a host with no internet access: sync an approved list from a connected host and ferry the malware advisory feed alongside it."
---

# Run without internet access

With a locked-down network path between a connected host and the serving
host, `pypiron sync` pushes over it. With nothing crossing the boundary but
scanned media, sync into a staging server on the connected side and carry the
storage tree. Either way the serving host never touches the internet: it
answers installs from what sync delivered, and anything it doesn't hold is an
immediate 404 — nothing waits on a network that isn't there.

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

The list is literal — nothing resolves dependencies for you. Generate it from
a lockfile (`uv export` emits the full closure) so a transitive dependency
doesn't 404 on the inside. List syntax, version specifiers, and excludes:
[Mirror selection](../reference/configuration.md#mirror-selection).

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
over HTTP like any other publisher, so this setup needs a network path from
the connected host to the serving host — put TLS in front or keep it on a
locked-down transfer network. Re-running sync is normal — existing files stay.
Yanks, removals, and project status follow upstream.

## No network path at all? Carry it on media

When nothing may cross the boundary but scanned media, sync into a staging
server on the connected side and carry the storage itself:

```bash
# connected side: a throwaway staging server on local disk
PYPIRON_DATA_DIR=/srv/staging PYPIRON_ADMIN_PASS="$ADMIN" pypiron serve &
pypiron sync --config pypiron.toml --to http://localhost:8080
kill %1 && wait          # stop the staging server: tar a tree at rest

tar -C /srv/staging -cf mirror.tar . && sha256sum mirror.tar > mirror.tar.sha256
```

Carry both files across. On the serving host, with the server stopped,
replace the tree — never untar over the old one, or upstream removals and
yanks can't follow through on this path:

```bash
sha256sum -c mirror.tar.sha256
mkdir /var/lib/pypiron.new && tar -C /var/lib/pypiron.new -xf mirror.tar
PYPIRON_DATA_DIR=/var/lib/pypiron.new pypiron verify-index --deep
mv /var/lib/pypiron /var/lib/pypiron.old && mv /var/lib/pypiron.new /var/lib/pypiron
```

`sha256sum -c` proves the media crossed intact; `verify-index --deep`
re-hashes every file against what clients will verify and exits 0 on a clean
tree. On this path the serving host publishes nothing — leave
`PYPIRON_ADMIN_PASS` unset and it runs read-only, one less open write path
inside the fence. Clients inside point at it like any index:
`pip install --index-url http://airgapped:8080/simple/ …` (plain HTTP needs
pip's `--trusted-host`). Deliver the advisory feed the same way — the
local-file option below needs no network.

For a full offline copy — no cooldown, yanked files included — set
`exclude-newer = ""` and `include-yanked = true` in `[mirror]`. That switches
both safety gates off — a deliberate trade for byte-complete mirrors; keep
them on unless completeness is the requirement.

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

The file is re-read on the daily refresh cycle
(`--reconcile-interval-secs`) — time the ferry and the staleness alarm to
that.

However the feed arrives, blocking behaves the same. Before the first
delivery, the block set baked into the binary at release covers the box; the
ferried feed supersedes it without a restart. An enclave usually wants
fail-closed on top: set `PYPIRON_MALWARE_BLOCK=true` explicitly and the
server refuses to start until a live feed is loaded.

## Keep it fresh

A server with internet access blocks a new advisory minutes after it's
published. A ferried mirror is only as fresh as its last delivery: with a
network path, run `pypiron sync` on a cron; over media, run it before each
transfer day. One run picks up new versions of approved packages and the
advisory snapshot together. The
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
