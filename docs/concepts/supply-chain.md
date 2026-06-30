# Supply-chain defense

Mixing private packages with a public PyPI mirror is where supply-chain attacks
slip in. pypiron shuts three doors: dependency confusion, malicious fresh
releases, and tampered files. Each protection below answers one threat.

| Threat | How pypiron stops it | Control |
| --- | --- | --- |
| Dependency confusion | Each name is private or public, never both; private names stay reserved | `--private-prefix`, point clients at one index |
| Malicious recent releases | Hold back releases younger than a cutoff | `--exclude-newer` (sync or serve) |
| Tampered or unattributed files | Filenames can never be overwritten; PyPI's provenance travels with each file | `<filename>.provenance` |

## Dependency confusion

The attack (Birsan, 2021): you use a package name privately, an attacker
publishes that same name — or a higher version — on public PyPI, and a resolver
consulting both indexes pulls the attacker's copy.

First defense: point clients at this one index. With uv use
`--default-index`/`--index`; with pip use `--index-url`, never `--extra-index-url
https://pypi.org/simple`. pip merges extra indexes by version with no priority —
that merge *is* the vulnerability. The server, not the client, decides what
exists.

pypiron enforces the rest: **each name is private or public, never both.** The
first upload — a private push or a mirror sync — reserves the name for that
world. It stays reserved.

- A private upload to a mirror-owned name is rejected.
- `sync` refuses a name you already own privately.
- Collisions are hard errors, never merges — a package belongs to exactly one
  world, so its index never mixes private and upstream files.

That reservation is durable. Deleting every file of a package does *not* release
the name — otherwise a credentialed client could empty a mirror-owned public
name and re-upload it as private, the dependency-confusion direction.
Repurposing a name across worlds takes a deliberate operator action with direct
storage access. ([storage-layout
contract](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/DESIGN.md#storage-layout-the-contract))

### Reserve a namespace

`--private-prefix` reserves a namespace (e.g. `acme-*`) for private uploads and
forbids `sync` from touching it. Intent is auditable, and an internal package
can't be published under a name that later collides upstream. Matching is on
normalized names, so `acme_foo`, `acme.foo`, and `acme-foo` are the same name.

```bash
pypiron serve --admin-pass "$ADMIN" --private-prefix acme
```

Claim a private name by uploading under the prefix. The name never falls through
to upstream:

=== "uv"

    ```bash
    uv publish --publish-url http://HOST:8080/legacy/ \
      --username admin --password "$ADMIN" dist/*
    ```

=== "twine"

    ```bash
    twine upload --repository-url http://HOST:8080/legacy/ \
      -u admin -p "$ADMIN" dist/*
    ```

!!! tip
    Defense in depth: register your private names, or the prefix stem, on
    pypi.org itself. Some laptop somewhere will always run `pip install` against
    the defaults.

See [Deploy → Private packages](../guides/deploy.md#private-packages) and
[Deploy → Add public PyPI](../guides/deploy.md#add-public-pypi).

## Malicious recent releases

A compromised maintainer account or a typosquat is most dangerous in its first
hours, before anyone notices. pypiron holds back releases younger than a cutoff,
so resolution lands on versions old enough to have been caught.

**On by default: new releases wait seven days, on a sliding window.** The proxy
applies it on every read (re-checked per request, so the window keeps sliding);
`sync` applies it to what a run mirrors. Widen the window, pin an absolute "as
of" date, or disable it entirely:

```bash
pypiron serve --admin-pass "$ADMIN" \
  --proxy-upstream https://pypi.org \
  --exclude-newer "7 days"     # the default; pass "30 days" to widen
```

```bash
pypiron sync --exclude-newer "2026-01-01T00:00:00Z"   # pin an absolute cutoff
pypiron sync --exclude-newer ""                        # disable: mirror everything
```

All `<when>` formats — durations, absolute timestamps, what slides versus what
stays pinned — live in [Configuration → Mirror
selection](../reference/configuration.md#mirror-selection).

This composes with uv's own client-side `--exclude-newer`. pypiron stamps every
file with an upload time — private uploads get their receipt time, mirrored files
carry PyPI's true timestamp — and uv filters against it, so resolution is
reproducible "as of" any date.

```bash
uv pip install --index-url http://HOST:8080/simple/ \
  --exclude-newer 2026-01-01T00:00:00Z requests
```

!!! note
    uv treats files without an upload time as unavailable and drops them from
    resolution. See [Why this timestamp is the minimum
    bar](../reference/standards.md#why-pep-700-is-the-minimum-bar-exclude-newer).

!!! note "Only admins can backdate"
    An ordinary upload can only claim its receipt time, so a publisher can't
    sneak a package in under a cutoff. Setting any other timestamp — including
    mirror uploads that carry PyPI's original time — requires the admin
    credential; with none configured, those uploads are refused.

## Provenance and immutability

A filename, once uploaded, is never replaced (PyPI's own rule); pypiron rejects
re-uploads of an existing filename. Nobody can swap bytes under a version already
in someone's lockfile.

For attribution, PyPI's provenance travels with each package. When `sync` or the
proxy carries a file across, PyPI's already-verified attestation comes along as a
`<filename>.provenance` companion (advertised by a `provenance` URL in JSON and a
`data-provenance` attribute in HTML). Consumers verify it end-to-end and
offline — Sigstore bundles check against a cached trust root with no egress — so
even an air-gapped build confirms the original publisher.

pypiron is a relay, not a verifier: it never runs Sigstore or mints provenance,
so a direct upload carrying first-party attestations is refused. See
[Standards support](../reference/standards.md) for the spec-level detail.

## The air-gapped endgame

The proxy still talks to live PyPI on a cache miss. An [air-gapped
mirror](../guides/air-gapped-mirror.md) removes that surface: the serving node
has no egress, and `sync` pre-loads an approved, vetted package list from a host
that does. Combine it with `--exclude-newer` and a private prefix, and the
serving node resolves a fixed, reviewed corpus with no live upstream to attack.

## See also

- [Mirroring](mirroring.md) — how `sync` carries timestamps, yank state, and
  provenance forward.
- [Authentication](authentication.md) — the admin/uploader/reader roles that
  gate backdating and mirror uploads.
- [Standards support](../reference/standards.md) — what is verified against real
  clients.
- [Configuration](../reference/configuration.md#mirror-selection) — the shared
  mirror-selection surface, with env-var and `[mirror]`-table equivalents.
