---
description: How pypiron blocks malware, delays new releases, protects private names, limits access, audits vulnerabilities, and verifies releases.
---

# Security features

**pypiron puts the security policy in the server, so every package client gets
the same protection.** That includes current uv and pip, old lockfiles, CI, and
tools with no security plugin of their own.

## Malware blocking and the release cooldown

### Known public malware is refused

pypiron checks public packages against the OSV PyPI advisory feed before it
serves or caches them. A file marked by an OSV `MAL-*` advisory is refused, and
the response names the advisory. A cached file that becomes known malware
stops downloading even when an old lockfile asks for its direct URL.

The binary includes a block set for first boot. pypiron refreshes the full OSV
snapshot daily, and each node checks for new malware advisories every two
minutes by default. Connected nodes therefore apply new blocks within minutes.
If OSV is unreachable, pypiron continues serving with its last snapshot and
exposes freshness metrics for alerting.

Air-gapped servers use the block set included in their binary until
`pypiron sync` delivers a newer advisory snapshot. Their update speed is the
delivery schedule you choose.

Malware blocking applies to public packages. A private package with the same
name is still your package. Set `--malware-block false` to disable blocking, or
`--advisory-feed ""` to disable both advisory blocking and the organization
audit. [Advisory settings](reference/configuration.md#server)

### New public releases wait seven days

The default **dependency cooldown** hides a public release until it is seven
days old. This gives maintainers, PyPI, and advisory services time to identify
a compromised account, typosquat, or malicious release before your resolver
can select it. The window moves forward continuously and applies to both proxy
requests and `sync`.

The measured benefit is substantial:

- In an analysis run in July 2026, OSV contained 17,043 malicious PyPI releases
  in 11,517 projects. **72% had a public advisory within the default seven-day
  window.**
- For compromised established packages published in 2024 or later, the
  advisory check blocked 34% on release day, a seven-day cooldown blocked 56%,
  and a 30-day cooldown blocked 86%.

Set `--exclude-newer "30 days"` for a longer window, pass a date for a fixed
snapshot, or pass an empty value to disable the cooldown.

[Cooldown formats and mirror selection](reference/configuration.md#mirror-selection)

### PyPI withdrawals remain withdrawn

pypiron follows upstream yanks, removed files, and PEP 792 project quarantine.
A quarantined project has no resolvable files, and direct artifact requests are
refused. A freeze takes effect immediately on the server that receives it and
within `--quarantine-poll-secs` on other nodes; the default is 30 seconds.

With object-storage redirects, a signed download URL already issued can remain
valid for up to one hour. Use `--artifact-delivery stream` when every download
must pass through the live server decision, or remove the object to invalidate
existing links.

## Dependency confusion

Dependency confusion happens when an internal name also exists on public PyPI
and a client chooses the public package. pypiron prevents this with one rule:
**a package name is private or public, never both.**

The first private upload or public fetch claims the normalized name. A private
name never falls through to upstream, and a public name cannot be replaced by a
private upload. Deleting every file does not release the claim. Repurposing an
empty name requires `pypiron origin release PACKAGE`.

`--private-prefix acme` also reserves `acme` and `acme-*` before those packages
exist. Point clients only at pypiron: use uv's default index or pip's
`--index-url`, rather than adding PyPI with `--extra-index-url`.

## Approval lists

The `[mirror]` configuration can allow or deny package names and versions, then
filter by file type, Python or ABI tag, platform, size, release age, and
pre-release status. The on-demand proxy and `sync` share these rules.

Approval does not bypass the cooldown, malware block, hash check, or upstream
withdrawals. [Configure mirror selection](reference/configuration.md#mirror-selection)

## Air-gapped deploys

Use `pypiron sync` to load approved packages and an advisory snapshot from a
connected machine. The isolated server needs no upstream access; malware data
stays as fresh as your delivery schedule.

[Air-gapped deployment](guides/air-gapped.md)

## Trust boundaries

### Public files are checked before storage

pypiron verifies each upstream file against the SHA-256 hash in the upstream
index. A truncated, corrupt, or mismatched response fails and leaves no usable
cache entry. Listing-provided artifact, metadata, provenance, and redirect URLs
are also blocked from private, loopback, link-local, and cloud-metadata
addresses unless you explicitly allow a host or network.

A corporate forward proxy changes where hostname filtering happens: pypiron
still blocks literal private IP addresses, while the proxy's egress policy must
control hostnames it resolves. [Forward proxy and private CA setup](reference/configuration.md#behind-a-forward-proxy-or-tls-interception)

When PyPI provides a Sigstore provenance bundle, pypiron independently verifies
the signature, publisher identity, and artifact digest against its built-in
trust root. The project page shows the verified publisher identity. This proves
who signed those bytes; it does not prove that the publisher is trustworthy or
the package is safe. pypiron relays public provenance but does not mint
attestations for private uploads.

## Vulnerability audit

Malware is blocked. Ordinary vulnerabilities are reported because refusing an
affected version could break a pinned build.

`/audit` and `/audit.json` list affected public packages in your store, their
advisories and fixed versions, ranked by downloads over the last 30 days. The
HTML and JSON reports require an admin credential. Each project page also shows
the advisories for that package.

## Login throttling

pypiron serves HTTP. Terminate TLS at a reverse proxy or load balancer before
credentials cross an untrusted network.

- With no write credential, the server is read-only.
- Uploader and reader credentials require both a username and password. For
  admin, `--admin-pass` alone uses the default username `admin`; setting a
  different admin username without a password makes startup fail.
- Reader, uploader, and admin roles are cumulative and use constant-time secret
  comparison.
- Five failed logins from one address trigger a five-minute lockout by default.
  Lockouts are per server instance and IPv6 clients are grouped by `/64`.
- Cross-site state-changing requests are rejected, and every response includes
  `X-Content-Type-Options: nosniff`.
- Client-supplied forwarding headers are ignored unless `--trusted-proxy` is
  enabled.

Enable `--trusted-proxy` only behind a reverse proxy that replaces forwarding
headers. Without it, all clients behind that proxy share one address and one
login-failure budget. [Authentication and throttle settings](reference/configuration.md#server)

## What pypiron does not defend

pypiron does not protect against:

- **Stolen storage credentials.** Storage is authoritative. Someone who can
  rewrite stored files and records controls what the server reads.
- **A malicious but valid upstream release.** Hash and provenance verification
  prove integrity and signing identity, not safety. The cooldown and advisory
  feed add separate defenses.
- **Volumetric denial of service.** Put request-rate limits at your reverse
  proxy or load balancer. pypiron throttles failed logins, not general traffic.

The access log records failed logins as `401` and throttled requests as `429`
at `info` level for fail2ban or SIEM rules.

## pypiron's own dependencies

Every pull request checks the dependency tree for published security
advisories. [Dependency and adversarial testing](testing.md#fuzzing-and-dependency-checks)

## Verify a release

Wheels, source distributions, release binaries, and container images ship with
GitHub build-provenance attestations. Download an artifact, save the wheel as
`downloaded-wheel.whl`, and verify it with GitHub CLI 2.49 or newer:

```bash
WHEEL='./downloaded-wheel.whl'
gh attestation verify "$WHEEL" \
  --repo blackthorn-interstellar/pypiron

gh attestation verify oci://ghcr.io/blackthorn-interstellar/pypiron:latest \
  --repo blackthorn-interstellar/pypiron
```

Exit status `0` means the artifact digest matches an attestation issued by this
repository's GitHub Actions. Verification normally needs GitHub access and
`gh auth login`. For an air-gapped check, download the bundle on a connected
machine with `gh attestation download`, then verify with `--bundle`.

[How these protections are tested](testing.md)
