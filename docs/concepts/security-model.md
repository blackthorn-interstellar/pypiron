---
description: The security model of a fail-closed PyPI server. Private names never reach public PyPI, secrets compare in constant time, releases you can verify.
---

# Security model

pypiron is fail-closed by default: a half-configured credential refuses startup,
secrets compare in constant time, and a private name never falls through to
public PyPI. This page states the rest plainly — what pypiron protects, where it
draws the line, and how to prove the release you're about to run is authentic.

## What's at stake

- **Your private packages.** The files you upload, and the names reserved for
  them.
- **Every file's integrity.** pypiron records a sha256 beside each artifact;
  `verify-chain` replays them and names any that no longer match.
- **Your credentials.** The admin, upload, and read secrets that gate writes and
  (optionally) reads.
- **The audit history.** An append-only, hash-chained log of what was served.

## Trust boundaries

**Untrusted — every client request.** Anything a client sends is hostile until
proven otherwise. Credentials compare in constant time, so a wrong guess leaks no
timing. A half-configured credential disables its role instead of enabling a
bypassable one. Private names never fall through to upstream, so nobody can shadow
one with a public package of the same name. A browser can't be turned against you:
cross-site state-changing requests are rejected, so cached Basic credentials can't
be ridden from another site to forge an upload or a yank, and every response
carries `X-Content-Type-Options: nosniff`. Client-set `X-Forwarded-For`/`X-Real-IP`
are ignored for the audit log unless you enable `--trusted-proxy`, so a direct
caller can't forge its logged address.

**Trusted — your storage backend.** The S3, GCS, Azure, or disk backend you
configure is your data behind your keys, and pypiron treats its responses as
trusted input. That trust has exactly one sharp edge — see [Dependency
advisories](#dependency-advisories).

**Trusted — PyPI, for public packages.** For anything mirrored, pypiron is a
relay: it carries PyPI's files and provenance across unchanged and does not
re-verify them. Trusting a mirrored package is trusting PyPI.

**Trusted — the release pipeline.** Releases are built by GitHub Actions with
every action pinned to a full commit SHA and every build run `--locked` against a
committed lockfile. Each wheel, sdist, binary, and image ships a signed provenance
attestation you can check yourself — see [Verify a release](#verify-a-release).

## Adversaries pypiron is built to stop

Each links to how it works:

- **Dependency confusion** — a public name colliding with a private one. Each
  name is private or public, never both.
  [Supply-chain defense](supply-chain.md#dependency-confusion)
- **Malicious fresh releases** — a compromised account or a typosquat, most
  dangerous in its first hours. New releases wait seven days.
  [Supply-chain defense](supply-chain.md#new-releases-wait)
- **Known malware** — a file a public advisory flags is refused: no install, no
  cache. [Supply-chain defense](supply-chain.md#known-malware-never-installs)
- **In-place tampering** — an artifact swapped after upload. `verify-chain`
  replays the hash-chained log and names anything that changed.
  [Tamper-evident checkpoints](transparency.md)

## What pypiron does not defend

An honest boundary is worth more than a long list. These are out of scope by
design; know them before you lean on the rest.

- **A stolen storage credential.** Whoever holds your object-storage keys can
  rewrite an artifact and its recorded hash in one motion. Tamper-evident
  checkpoints raise the cost — the hash-chained log seals each entry, so
  `verify-chain` still catches the swap — but the defense is detection, not
  prevention. Guard the storage credential like the root secret it is.
- **A rolled-back audit log with no lock.** Without write-once retention on the
  checkpoint history, that same attacker can delete old checkpoints and start a
  fresh, clean-looking chain. `verify-chain` catches every in-place rewrite but
  can't prove the log wasn't rolled back. Object Lock closes it —
  [no lock, no rollback guarantee](transparency.md#close-the-rollback-gap).
- **The trustworthiness of upstream PyPI.** For mirrored public packages, pypiron
  relays PyPI's provenance; it never mints or re-verifies attestations itself. A
  malicious upload that PyPI accepted and signed is one pypiron will carry across.
  Your own private uploads are a separate world you control.
- **Online credential guessing and request floods.** pypiron does no in-process
  rate limiting — a single stateless binary can't count requests across replicas,
  and the job belongs at the edge. Put a request-rate limit on your reverse proxy
  or load balancer. Failed logins already appear in the audit log as `401`/`403`
  events at `info` level (so keep `pypiron::access=info` if you feed them to
  fail2ban or a SIEM — raising it to `warn` keeps only 5xx and drops them); key any
  ban on the real peer address, or trust the logged client field only when pypiron
  sits behind a proxy with `--trusted-proxy`, since otherwise an attacker sets that
  `X-Forwarded-For` themselves.

## Dependency advisories

Every change runs `cargo audit` with no ignore flags. The audit is clean.

It was not always. Through v0.0.14 pypiron carried two denial-of-service
advisories in `quick-xml` (RUSTSEC-2026-0194, quadratic parsing on duplicate
attributes; RUSTSEC-2026-0195, unbounded allocation on namespace declarations),
pulled in through `object_store` — the library pypiron uses to talk to S3, GCS,
and Azure. The vulnerable code parsed only XML from the storage endpoint you
configured and authenticated to, never anything from a package client, and the
default disk backend never invoked it at all. No released `object_store`
permitted the fixed `quick-xml`, so the advisories were documented here instead
of hidden behind audit exceptions, with the gate set to turn red the day a fix
shipped.

That day came: `object_store` 0.14.1 allows the fixed `quick-xml` 0.41, pypiron
took the bump, and releases after v0.0.14 carry no known advisories.

## Verify a release

Every wheel, sdist, release binary, and container image is published with a signed
build-provenance attestation — proof it was built by this repository's CI and
hasn't been swapped since. Check one with the GitHub CLI:

```bash
# A wheel or sdist you downloaded from PyPI
gh attestation verify ./pypiron-<version>-<platform>.whl \
  --repo blackthorn-interstellar/pypiron

# A release binary
curl -LO https://github.com/blackthorn-interstellar/pypiron/releases/latest/download/pypiron-x86_64-unknown-linux-musl.tar.gz
gh attestation verify pypiron-x86_64-unknown-linux-musl.tar.gz \
  --repo blackthorn-interstellar/pypiron

# The container image, checked by digest without pulling it
gh attestation verify oci://ghcr.io/blackthorn-interstellar/pypiron:latest \
  --repo blackthorn-interstellar/pypiron
```

**Exit status 0 is the signal** — the artifact's digest matched an attestation
issued by this repo's GitHub Actions. A non-zero exit means it didn't; treat the
artifact as unverified.

Three things it needs:

- **GitHub CLI 2.49 or newer.** Older builds (2.21, for one) have no `attestation`
  command at all.
- **Network and a login.** Verification fetches the attestation from GitHub, so it
  needs egress and `gh auth login`. Air-gapped? Download the attestation on a
  connected machine with `gh attestation download`, then verify offline against it
  with `--bundle`.
- **A public repository.** Attestation is a public-repo feature; it works here
  because this repository is public. If it ever goes private, verification stops
  until it's public again.
