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
