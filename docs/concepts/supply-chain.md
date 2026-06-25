# Supply-chain defense

pypiron serves private uploads, synced mirrors, and on-demand-proxied packages
from one URL and one namespace. That mix is exactly where supply-chain attacks
live. Each protection below answers one threat.

| Threat | Mechanism | Control |
| --- | --- | --- |
| Dependency confusion | Origin exclusivity + namespace reservation | `.origin` marker, `--private-prefix` |
| Malicious recent releases | Quarantine window | `--filter-exclude-newer` (sync or serve) |
| Tampered or unattributed bytes | Filename immutability, PEP 740 provenance relay | `<filename>.provenance` |

## Dependency confusion

The attack (Birsan, 2021): you use a package name privately, an attacker
publishes that same name — or a higher version — on public PyPI, and a resolver
that consults both indexes pulls the attacker's copy.

The first defense is **closed-world resolution**: point clients at this registry
only. With uv use `--default-index`/`--index`; with pip use `--index-url`, never
`--extra-index-url https://pypi.org/simple`. pip merges extra indexes by version
with no priority — that merge *is* the vulnerability. The registry, not the
client, decides what exists.

The server enforces the rest with **origin exclusivity**. Every package
directory carries a marker in the truth tree, `packages/<pkg>/.origin`, set to
`private` or `mirror` on first write and never changed automatically.

- A private upload to a `mirror`-owned name is rejected.
- `sync` refuses a name that is `private`-owned.
- Collisions are hard errors, never merges — a package belongs to exactly one
  world, so its index never mixes private and upstream files.

The claim is durable. Deleting every artifact of a package does *not* release
`.origin` — otherwise a credentialed client could empty a mirror-owned public
name and re-upload it as private, which is the dependency-confusion direction.
Repurposing a name across worlds requires deleting the `.origin` file directly,
an operator action gated on storage access.

### Reserve a namespace

`--private-prefix` reserves a namespace (e.g. `acme-*`) for private uploads and
forbids `sync` from touching it. It makes intent auditable and stops an internal
package from being published under a name that later collides upstream. Matching
is on PEP 503 normalized names, so `acme_foo`, `acme.foo`, and `acme-foo` are the
same name.

```bash
pypiron serve --admin-pass "$ADMIN" --private-prefix acme
```

Claim a private name by uploading under the prefix. The name then never falls
through to upstream:

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

See [Private packages](../guides/private-packages.md) and [Private and
public, one index](../guides/private-and-public.md).

## Malicious recent releases

A compromised maintainer account or a typosquat is most dangerous in its first
hours, before anyone notices. A quarantine window hides releases younger than a
cutoff so resolution lands on versions that have had time to be caught.

The proxy applies it on the read path; `sync` applies it to what a run mirrors:

```bash
pypiron serve --admin-pass "$ADMIN" \
  --proxy-upstream https://pypi.org \
  --filter-exclude-newer "7 days"
```

```bash
pypiron sync --filter-exclude-newer "2026-01-01T00:00:00Z"
```

`<when>` is an RFC 3339 timestamp (`2026-01-01T00:00:00Z`), a friendly duration
ago (`"30 days"`, `"24 hours"`), or an ISO 8601 duration ago (`P30D`, `PT24H`).
Durations resolve to a fixed number of seconds; calendar months and years are
rejected.

This composes with uv's client-side `--exclude-newer`. pypiron serves PEP 700
`upload-time` for every file (private uploads get receipt time; mirrored files
carry PyPI's true timestamp), and uv filters distributions against that field —
so resolution is reproducible "as of" a date.

```bash
uv pip install --index-url http://HOST:8080/simple/ \
  --exclude-newer 2026-01-01T00:00:00Z requests
```

!!! note
    uv treats files without `upload-time` as unavailable and drops them from
    resolution. See [Why PEP 700 is the minimum
    bar](../reference/standards.md#why-pep-700-is-the-minimum-bar-exclude-newer).

Backdating a timestamp is an **admin** privilege, not an uploader one. An
ordinary upload can only claim receipt time, so a publisher cannot sneak a
package under a cutoff. Mirror uploads carry timestamps and require the admin
credential; with no admin credential configured they are refused outright.

## Provenance and immutability

Once a filename is uploaded it can never be replaced (the pypi.org rule).
pypiron rejects re-uploads of an existing filename. Nobody can swap bytes under
a version that is already in a lockfile.

For attribution, pypiron relays **PEP 740 provenance**. PyPI's already-verified
attestation object travels verbatim through `sync` and the proxy as a
`<filename>.provenance` companion, advertised by a `provenance` URL (JSON) and
`data-provenance` attribute (HTML).

pypiron is a **relay, not a verifier**. It never runs Sigstore and never mints
provenance — a direct upload carrying first-party `attestations` is refused,
because pypiron has no Trusted Publisher identity to bind. Verification is the
consumer's end-to-end job, and it works offline: Sigstore bundles verify against
a cached trust root with no egress. So an air-gapped consumer can still confirm
the original publisher.

## The air-gapped endgame

The proxy still talks to live PyPI on a cache miss. An [air-gapped
mirror](../guides/air-gapped-mirror.md) removes that surface entirely: the
serving node has no egress, and `sync` pre-loads a vetted allowlist from a host
that does. Combine the allowlist with `--filter-exclude-newer` and a private
prefix and the serving node resolves a fixed, reviewed corpus with no live
upstream to attack.

## See also

- [Mirroring](mirroring.md) — how `sync` carries timestamps, yank state, and
  provenance forward.
- [Authentication](authentication.md) — the admin/uploader/reader roles that
  gate backdating and mirror uploads.
- [Standards support](../reference/standards.md) — what is verified against real
  clients.
- [Configuration](../reference/configuration.md#filters) — the shared `--filter-*`
  surface, with env-var and `[filter]`-table equivalents.
