---
description: One index URL serves three sources — private uploads, an on-demand PyPI cache, and a pre-loaded mirror. Each name is private or public, never both.
---

# Where packages come from

Clients configure one index URL. Behind it, pypiron serves packages from three
sources:

1. **Your private uploads.** Publish to `/legacy/`, install from `/simple/`.
2. **The on-demand proxy.** A public package is fetched from PyPI on first
   request, then served locally from then on.
3. **The sync mirror.** An approved subset of PyPI, pre-loaded ahead of time.

One install can draw on all three:

```bash
uv add --default-index http://HOST:8080/simple/ requests acme-widgets
```

`acme-widgets` comes from your uploads, `requests` from the proxy cache or the
mirror. The client can't tell the difference.

## Private uploads

Setting `PYPIRON_ADMIN_PASS` enables publishing. Publish to `/legacy/`, install
from `/simple/`:

```bash
uv publish --publish-url http://HOST:8080/legacy/ \
  --username admin --password "$PYPIRON_ADMIN_PASS" dist/*

uv add --default-index http://HOST:8080/simple/ acme-widgets
```

## The on-demand proxy

Set `proxy-upstream` and any public package a client asks for is fetched from
PyPI once, cached, and served locally afterward — for every later install and
every other client. No package list to maintain. The server needs egress to
PyPI; clients only ever talk to pypiron.

## The sync mirror

`pypiron sync` pre-loads packages from an approved list, so the serving node
never talks to PyPI. CI installs exactly what the list names; an air-gapped
node serves with no egress at all. What belongs on the list and how to maintain
it: [Approval lists](../concepts/approval-lists.md).

## Proxy or sync?

| Use | Pick | Why |
| --- | --- | --- |
| Normal private index plus cached public PyPI | proxy | No package list. Fetches on first install. |
| CI mirror with approved dependencies | sync | Pre-loads exactly what is in `packages.txt`. |
| Air-gapped serving node | sync | The server never needs egress. |

The same `[mirror]` policy — cooldown, format and platform filters — drives
both, so a package passes the same rules whichever way it arrives. Proxy can
run open; sync needs an approved list. Run either, or both: proxy for the long
tail, sync to guarantee the approved set is present before anyone asks for it.
Every knob: [Configuration](../reference/configuration.md).

Deployment, step by step: [Standard cloud](../guides/standard-cloud.md) for the
proxy, [Air-gapped](../guides/air-gapped.md) for sync.

## One index, not two

Do not point clients at PyPI as an extra index. With two indexes the resolver —
not the server — chooses where each name comes from, and an internal name that
also exists on public PyPI can resolve to the public copy. That is dependency
confusion. It works even when your index is listed first.

Point clients at this one index and let the server decide what exists: with uv
use `--default-index`; with pip use `--index-url`, not `--extra-index-url
https://pypi.org/simple`. How pypiron closes the rest of the attack:
[Security](../security.md).

## Each name is private or public, never both

The first upload or the first sync reserves a name for its world, and it stays
reserved:

- A private upload to a mirror-owned name is rejected.
- `sync` refuses a name you already own privately.
- Collisions are hard errors, never merges — a package's index never mixes
  private and upstream files.
- Deleting every file of a package does not release the name. Repurposing one
  takes direct operator action in storage.

`private-prefix = "acme"` reserves the whole namespace up front: `acme` and
every `acme-*` name belong to your uploads before the first one lands, and
`sync` can never touch them. Matching is on normalized names — `acme_foo`,
`acme.foo`, and `acme-foo` are the same name.

A name that could flip between worlds is exactly the opening
dependency-confusion attacks need, so no configuration relaxes this rule.
