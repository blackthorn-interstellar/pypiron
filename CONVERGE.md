# Converge

State for the `/converge` loop. `dev/VISION.md` is the fixed reference; this
file is the loop's memory. Only a human raises the ceiling.

## Ceiling

- Non-test lines: **45330** measured 2026-09-19 at `74b7d62`.
- Ceiling: **47597** (measured + 5%).
- Measure with, from the repo root:

```
for f in $(find src -name '*.rs'); do awk '{ if (pending && /^(pub )?mod /) exit; if (pending) n++; pending = ($0 ~ /^#\[cfg\(test\)\]$/); if (!pending) n++ } END { print n+0 }' "$f"; done | paste -sd+ - | bc
```

(Every `src/**/*.rs` line before the file's `#[cfg(test)] mod …` block.)

## Done

- 2026-09-19 Bug: an invalid inline `--private-pattern` / TOML `private-patterns` entry (`#bolt`, empty) was silently dropped by the pattern-file comment filter, so a name the operator believed reserved could fall through to the proxy; now refuses startup naming the entry. Unit test on `PrivateArgs::resolve` and a blackbox startup-refusal test were red first. Lines 45330 -> 45339.

## Rejected

- 2026-09-19 `--private-prefix` of 255 or 256 bytes now refuses startup because `{prefix}-*` exceeds the 256-byte pattern cap (a regression in c030441). Real, but no deployment has a 255-byte namespace; not worth a change until someone hits it. One-line fix if ever wanted: build the `-*` pattern in `PrivateNames::new` without re-parsing.

## Empty iterations

0 consecutive.

## Open questions

- **Should failed logins on reads be logged by default?** `docs/security.md`
  promises "the access log records failed logins as `401` and throttled requests
  as `429` at `info` level for fail2ban or SIEM rules." With the default
  (`--access-log` off) the middleware in `src/app.rs` (`log_requests`) only
  considers mutations, so a wrong install credential on `GET /simple/<pkg>/` or
  `GET /files/...` — the read-side brute force a reader credential exists to
  defend — is never logged, and neither is its `429`. Options: (A) also log a
  read when it presented credentials and ended `401`/`429` (the credential
  check `login_ip` is already computed on the hot path, so anonymous reads stay
  free; a small code change plus a blackbox test); (B) narrow the doc to
  "mutations by default; every request with `--access-log`". Recommendation: A —
  the sentence exists for fail2ban rules, and the rules are only useful if the
  guess-heavy path is in the log. Cost of choosing wrong: low either way.

## Leads (not yet adjudicated)

From a 2026-09-19 docs-versus-code sweep; each is a "wrong docs" candidate for a
later iteration, not a done task:

- `docs/for-agents.md` row "every `--flag` has a `PYPIRON_FLAG` environment
  variable" is false: `create-token`'s `--role`/`--repo`/`--commit`/`--user`
  have no env var by design (`dev/scripts/check_docs.py` allowlists them), and
  the naming is not mechanical (`--from` is `PYPIRON_SYNC_FROM`, `--deep` is
  `PYPIRON_VERIFY_DEEP`, `--force` is `PYPIRON_MIGRATE_FORCE`).
- `docs/reference/configuration.md` "A failed storage delete returns `500`" is
  incomplete: the existence probe, intent-marker write, origin read/re-check and
  the replication-gap path in `src/publish.rs` return `503`; only the artifact
  delete itself is `500`.
