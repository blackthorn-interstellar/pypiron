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

The trap: an internal package name also exists on public PyPI, and a resolver
pulling from both indexes chooses the public copy.

First defense: point clients at this one index. With uv use
`--default-index`; with pip use `--index-url`, not `--extra-index-url
https://pypi.org/simple`. Let the server decide what exists.

pypiron enforces the rest: **each name is private or public, never both.** The
first upload — a private push or a mirror sync — reserves the name for that
world. It stays reserved.

- A private upload to a mirror-owned name is rejected.
- `sync` refuses a name you already own privately.
- Collisions are hard errors, never merges — a package belongs to exactly one
  world, so its index never mixes private and upstream files.

Deleting every file of a package does not release the name. Repurposing a name
takes direct operator action in storage.

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

See [Setup → Private packages](../guides/setup.md#private-packages) and
[Setup → Add public PyPI](../guides/setup.md#add-public-pypi).

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
so a direct upload carrying first-party attestations is refused.

## The air-gapped endgame

The proxy still talks to live PyPI on a cache miss. `sync` removes that surface:
pre-load an approved package list from a host with egress, then serve from a node
without egress.

## See also

- [Configuration](../reference/configuration.md#mirror-selection) — the shared
  mirror-selection surface, with env-var and `[mirror]`-table equivalents.
