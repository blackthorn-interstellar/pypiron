# Security Policy

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue or PR.

Use GitHub's private vulnerability reporting: open the **Security** tab on this
repository and choose **Report a vulnerability**.

We aim to acknowledge a report within 3 business days and to keep you updated as
we work on a fix. Coordinated disclosure is appreciated — give us a reasonable
window to ship a patch before any public write-up.

## Supported versions

pypiron is pre-1.0. Only the latest release receives security fixes.

## Hardening notes

pypiron is a self-hosted package server and is **fail-closed by default**: a
half-configured credential refuses startup, secrets compare in constant time,
and private package names never fall through to upstream.

Several features are opt-in and widen the attack surface when enabled — notably
upstream `sync`/proxy (which makes outbound requests on the server's behalf) and
anonymous uploads. Review
[docs/reference/configuration.md](docs/reference/configuration.md) and keep
untrusted features disabled in production.

## Verify a release

Every wheel, sdist, release binary, and container image is published with a
signed build-provenance attestation — proof it was built by this repository's CI
and hasn't been swapped since. Check one with the GitHub CLI:

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

**Exit status 0 is the signal**: the artifact's digest matched an attestation
issued by this repo's GitHub Actions. A non-zero exit means it didn't — treat the
artifact as unverified. Verification needs the GitHub CLI **2.49 or newer** (2.21
has no `attestation` command), network access, and a `gh auth login` session.

The full trust model — what pypiron defends, what it does not, and the two
acknowledged dependency advisories — is in
[docs/concepts/security-model.md](docs/concepts/security-model.md).
