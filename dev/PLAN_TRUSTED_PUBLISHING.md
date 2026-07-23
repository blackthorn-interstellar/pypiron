# Plan: Trusted-publisher upload auth + PEP 740 accept-and-verify

Status: approved plan, not yet implemented. Two phases, shippable independently
and in order. Phase 1 has no new cryptography beyond JWT verification and
delivers the core security win; Phase 2 adds the Sigstore machinery and is
gated on Phase 1 landing first.

## Goal

Kill the stolen-credential publish: today anyone holding the uploader password
(or a leaked basic-auth credential) can publish `acme-payments 2.0.1` from a
laptop. After this work:

1. **Phase 1 — trusted-publisher upload auth.** pypiron mints its existing
   short-lived install tokens in exchange for a CI OIDC token (GitHub Actions /
   GitLab CI), verified against the provider's JWKS and matched to configured
   publisher identities. Packages bound to a publisher can *only* be published
   through that exchange — a password alone is no longer sufficient.
2. **Phase 2 — PEP 740 accept-and-verify on first-party uploads.** Uploads that
   arrived through the Phase 1 exchange may carry `attestations` (as sent by
   `twine --attestations`). pypiron verifies the Sigstore bundle at ingest —
   digest, DSSE signature, cert chain to a pinned trust root, identity match —
   synthesizes the PEP 740 provenance object, and stores/serves it exactly like
   relayed mirror provenance. The permanent, independently verifiable receipt.

Non-goals (do NOT build): verifying *mirrored* provenance (PyPI already did the
identity binding; we relay verbatim — `dev/DESIGN.md` §"PEP 740"); attestation
"downgrade watch" on mirrored projects; durable scoped API tokens (separate
roadmap item); pypiron signing anything itself.

## Positioning: experimental, opt-in, invisible by default (binding on both phases)

pypiron's vibe is secure systems that just work. The typical operator must
never have to understand — or even notice — any of this. Hard requirements:

- **Unconfigured = inert, provably.** With no `trusted-publisher` entries and
  no `sigstore-trust-root`: zero behavior change, zero startup work, zero new
  log lines, no new words in any error message a normal user can hit. The
  only visible seam is `POST /tokens` with an `oidc_token` body returning a
  terse 4xx ("trusted publishing is not configured"). Add one blackbox test
  asserting exactly this inertness; the rest of the existing suite passing
  unchanged is the broader proof.
- **Experimental label.** Everything here ships marked *Experimental*: the
  docs pages, the `configuration.md` entries, the `config_template.toml`
  block (commented out, at the bottom, headed "Experimental"), the ROADMAP
  lines. While experimental, config keys, claim formats, and the pubid string
  may change without migration shims.
- **Buried, not promoted.** Docs live under an Advanced/Experimental section
  (`docs/advanced/trusted-publishing.md`), carry an "Experimental" admonition
  up top, and are NOT linked from the README, the quickstart, the landing
  page, or any happy-path guide. No marketing copy until it stabilizes.
- **No nagging.** Never hint, warn, or suggest that an operator "should"
  configure trusted publishing. No startup notes, no docs callouts in
  unrelated pages, no dashboard badges. The existing free layer — mirrored
  provenance relayed and displayed, malware blocking, origin exclusivity —
  is what "secure by default" means here; this feature is opt-in hardening
  for orgs that come looking for it.
- **What stays free.** Nothing in this plan can be free-by-default, because
  trusting a publisher inherently means the operator naming who they trust
  (one config line, but a decision). The free tier is already shipped and
  stays untouched: PEP 740 relay on mirrors, publisher display on package
  pages, token attribution. Do not add any always-on behavior in this work.

## Current state (read these before starting)

- Relay is shipped: `.provenance` companions via `sync`/proxy
  (`src/proxy.rs` `fetch_provenance*`, `src/sidecar.rs` `PROVENANCE_SUFFIX`,
  `provenance_key`), `provenance` key / `data-provenance` attr in indexes,
  publisher summary on the human page (`src/provenance.rs::parse_publisher`,
  `src/pages.rs::load_provenance`).
- First-party `attestations` are refused fail-closed in `src/publish.rs`
  (in `legacy_upload`). Phase 2 replaces this refusal for
  publisher-authenticated uploads; it stays for everything else.
- `PublishRequest.provenance: Option<String>` already threads a provenance JSON
  body through `publish_record` into the companion write — Phase 2 reuses it
  verbatim. Confirm the sidecar records `provenance: true` for private-origin
  files and the index/render paths surface it origin-agnostically (they appear
  to be; verify with a blackbox test, fix if gated on mirror).
- Token machinery: `src/token.rs` (HMAC-SHA256 stateless tokens, `Claims`
  {role, repo, commit, user, iat, exp}), minted at `POST /tokens`
  (`src/admin.rs` `mint_token`, route `POST /tokens`), presented as basic-auth user
  `__token__`, verified in `AppState::token_role` (~line 5877). 5-minute TTL.
- Auth tiers: `is_admin` / `is_uploader` / `is_reader` in `src/app.rs`
  (~5849–5940). Constant-time compares (`ct_eq`). Fail-closed philosophy:
  half-configured credentials refuse startup.
- Config: every knob is `--flag` + `PYPIRON_FLAG` env + `pypiron.toml` key
  (`src/config.rs`, `src/config_template.toml`). Secrets never live in the
  toml file. Structured, non-secret config (publisher entries) belongs in the
  toml file.

House rules that bind this work: no `unwrap`/`expect`/`panic!` on request
paths; fail-closed everywhere; blackbox-first testing (`dev/TESTING.md` — real
binary over HTTP, Rust unit tests only for pure functions); don't add a
dependency to avoid a few lines, but don't hand-roll security-critical parsing
either; `make check` clean, `make test` for anything touching HTTP/upload.

---

## Phase 1 — Trusted-publisher upload auth (OIDC token exchange)

### Design

Reuse the existing token exchange rather than teaching the upload endpoint a
second auth scheme. CI flow:

1. The CI job obtains an OIDC JWT from its platform (GitHub:
   `ACTIONS_ID_TOKEN_REQUEST_URL` with `audience=pypiron`; GitLab:
   `id_tokens:` with `aud: pypiron`).
2. Job POSTs it to `POST /tokens` as `{"oidc_token": "<jwt>"}` (no basic auth).
3. pypiron verifies the JWT (signature via provider JWKS, `iss`, `aud`,
   `exp`/`nbf`/`iat`) and matches its claims against a configured
   trusted-publisher entry. On match it mints the existing short-lived HMAC
   token with role `uploader`, attribution filled from claims, plus new
   scope/identity claims (below).
4. Job publishes with `twine`/`uv publish` using `__token__` / that token,
   exactly like today.

This is PyPI's mint-token shape adapted to pypiron's existing stateless token.
No new endpoint, no session state, multi-node safe (same signing key).

### Config surface

Config names *who may exchange*, never *which package belongs to whom* —
per-package binding is data, claimed at first publish (see binding semantics).
Adding a new package must require zero config changes and zero redeploys.

Array-of-tables in `pypiron.toml` (documented in `src/config_template.toml`),
non-secret, safe to commit:

```toml
[[serve.trusted-publisher]]
provider = "github"                      # "github" | "gitlab"
repository = "acme/*"                    # required; exact or trailing-* glob over
                                         # owner/repo (GitHub) / group/project path (GitLab)
workflow = ".github/workflows/release.yml"  # optional; match job_workflow_ref (GitHub) / ci_config_ref_uri (GitLab)
environment = "release"                  # optional
# provider-url = "https://gitlab.example.com"  # gitlab only: self-hosted issuer; https required
# jwks-file = "/etc/pypiron/gitlab-jwks.json"  # optional: pinned JWKS for zero-egress
                                         # servers; no network fetch for this entry's
                                         # issuer, operator refreshes on key rotation
```

Scalar knobs:
- `--oidc-audience` / `PYPIRON_OIDC_AUDIENCE` / `serve.oidc-audience`, default
  `"pypiron"` — the required `aud` claim.
- `--require-trusted-publisher` / `PYPIRON_REQUIRE_TRUSTED_PUBLISHER` /
  `serve.require-trusted-publisher`, default false — when true, *every*
  private-origin publish must authenticate via the exchange; password uploads
  publish nothing. The end-state enforcement switch for an org once all
  pipelines are migrated.

Startup validation (fail-closed, at config load):
- Any `trusted-publisher` entry without `--token-signing-key` set → refuse
  startup with a clear message (the exchange mints tokens; without a key it
  cannot work, and a half-configured credential must not silently do nothing).
- `provider-url` on a github entry, or a non-https `provider-url` → refuse.
- Empty `repository` → refuse.

### Matching and claims

Fixed issuer/JWKS per provider — GitHub: `iss
https://token.actions.githubusercontent.com`, JWKS at
`<iss>/.well-known/jwks`; GitLab: `iss` = provider-url (default
`https://gitlab.com`), JWKS at `<iss>/oauth/discovery/keys`. Claim mapping:

| entry field  | GitHub claim         | GitLab claim        |
|--------------|----------------------|---------------------|
| repository   | `repository`         | `project_path`      |
| workflow     | `job_workflow_ref` (strip `@<ref>` suffix before compare) | `ci_config_ref_uri` (strip `@<ref>`) |
| environment  | `environment`        | `environment`       |

`repository` comparisons are case-insensitive; exact match, or prefix match
when the configured value ends in `*` (trailing-glob only — no mid-pattern
wildcards). `workflow`/`environment` are exact when configured, unconstrained
when absent. First matching entry wins; no match → 403 with a log line naming
the presented identity (never echo the raw JWT).

Attribution into the minted token: `repo` = repository claim, `commit` =
`sha`/`ci_commit_sha`, `user` = `actor`/`user_login`. Extend `token::Claims`
with one optional field (omitted when absent, keeping old tokens verifiable —
serde defaults already handle this):
- `pubid: Option<String>` — canonical publisher identity string built from the
  *claims*, not the entry, e.g.
  `github:acme/payments:.github/workflows/release.yml:release` (empty segments
  for unset fields; repository segment is the concrete repo, never the glob).
  The binding claim and Phase 2 both consume this.

### Binding semantics (the actual security win)

Per-package binding mirrors origin exclusivity: **claimed at first write,
stored as data, never configured.**

- On a private-origin publish authenticated by a `pubid` token: if the package
  has no publisher claim, record `pubid` in the package's origin/claim record
  (same crash-safe write discipline as the origin claim itself — extend the
  existing origin record; do NOT invent a new storage-tree variant, see the
  DESIGN storage contract). If it has one, the token's `pubid` repository must
  match the claimed repository — mismatch → 403 naming the claimed publisher.
  (Match on the repository segment; workflow/environment may legitimately be
  renamed. Tightening to full-pubid match can be a later knob.)
- Once a package carries a publisher claim it is **bound**: basic-auth uploads
  (uploader *and* admin passwords) are refused with a message naming the
  binding. The ratchet only tightens — publishing once via CI permanently
  closes the password path for that package. Migration of an existing org is
  therefore just: run the new CI once per package.
- Break-glass / repo moves: `pypiron publisher release <pkg>` (admin,
  mirroring `pypiron origin release`) clears the claim; the next publisher
  publish re-claims. Also `pypiron publisher show <pkg>` for inspection. No
  bypass flag on the upload path itself.
- Unclaimed packages still accept password uploads (unless
  `--require-trusted-publisher`), so nothing breaks on day one.
- Mirror-origin uploads (`mirror=true`, admin) are untouched — binding applies
  to private-origin publishes only. Origin exclusivity already guarantees a
  bound private name can't arrive via the mirror path.
- Known tradeoff, accept and document: any repo matching a trusted entry's
  glob can claim any *unclaimed* package name (internal first-come-first-
  served, exactly like origin exclusivity and `--private-prefix` today). Scope
  globs to the groups that publish Python, and the exposure is repos you
  already trust to publish.

### JWKS fetching

- An entry with `jwks-file` never fetches: keys load from the file at startup
  (unreadable/invalid → refuse startup, fail-closed) and reload on
  config-reload/restart. Unknown `kid` → mint fails with a message telling the
  operator to refresh the file. Same lifecycle discipline as the Phase 2 trust
  root.
- Otherwise: lazy fetch on first mint per issuer; cache keys by `kid` with a
  ~15 min TTL; on unknown `kid`, refetch once (key rotation) then fail.
- Fetch failure → 503 on the mint request, fail-closed, `anyhow` context, no
  retry storm (single in-flight fetch per issuer; a `tokio::sync::Mutex`
  around the cache is fine — mint is not a hot path).
- Use the existing `reqwest` client patterns; issuer URLs are operator-fixed
  (github) or startup-validated https (gitlab), so the SSRF guard concern is
  satisfied by validation, but route the fetch through the same builder
  hygiene as other upstream calls (timeouts, no redirects-to-anywhere).

### Dependency

JWT RS256 verification: add `jsonwebtoken` (checks: it pulls `ring` — confirm
via `cargo tree` whether `ring`/`aws-lc-rs` is already in the lockfile through
`rustls`; either way this is the smallest sane choice — do not hand-roll RSA).
Parse the JWT header/payload with it exclusively; never `serde_json` the
payload before signature verification for anything security-relevant.

### Tests

- Unit (pure): claim→entry matching table (case, trailing-glob repository,
  workflow `@ref` stripping, environment, gitlab vs github mapping), `pubid`
  construction, `Claims` round-trip with and without the new field (old-token
  compat).
- Blackbox (`tests/test_trusted_publisher.py`, per `dev/TESTING.md`): stand up
  a fake self-hosted GitLab issuer in the test process — a tiny HTTP server
  serving `/oauth/discovery/keys` (JWKS for a test RSA key, via Python
  `cryptography`) — and configure a gitlab entry with `provider-url` pointing
  at it. This exercises the real prod code path with zero test-only backdoors.
  Cover: mint→twine publish claims the package (verify via
  `pypiron publisher show`); second publish from the same repo succeeds; a
  token from a *different* repo under the same glob refused on the claimed
  package; basic-auth publish of a claimed package refused (both uploader and
  admin); `pypiron publisher release` reopens it; wrong `aud` refused; expired
  JWT refused; repository outside every entry refused; JWKS server down → 503
  mint, upload path unaffected; unclaimed packages still publish with
  passwords; `--require-trusted-publisher` refuses password publishes of
  unclaimed names too; claim survives an index rebuild
  (`pypiron rebuild-index`); a `jwks-file` entry mints with the issuer's
  network endpoint unreachable, and refuses startup on a garbage file.

### Docs

- `docs/reference/configuration.md`: the `[[serve.trusted-publisher]]` table
  and `oidc-audience`, grouped under an "Experimental: trusted publishing"
  subsection.
- New user-manual page `docs/advanced/trusted-publishing.md` (house style per
  `dev/DOCS_STYLE.md`: outcome-first, happy path; "Experimental" admonition at
  the top; unlinked from README/quickstart per Positioning): the GitHub
  Actions and GitLab CI snippets end-to-end (permissions/`id_tokens` block →
  curl or one-liner exchange → `twine upload`). Lead with the outcome: "a
  password is not enough to publish this package."
- `private/ROADMAP.md` Security & access: add the shipped line, marked
  experimental.

### Acceptance

`make check` and `make test` clean; the blackbox file above passes; a bound
package demonstrably cannot be published with any password.

---

## Deployment environments — who can use what (document this matrix)

Three network legs matter, and they are independent:

1. **runner → pypiron** (mint + upload). Self-managed runners inside the corp
   network make this work even when pypiron is not internet-exposed — the
   standard "GitLab.com/GitHub.com control plane + internal runners" topology.
2. **pypiron → issuer JWKS** (HTTPS, tiny, rare). Internal host for
   self-hosted GitLab; gitlab.com/github.com egress for the SaaS issuers. For
   zero-egress servers, a per-entry `jwks-file` pin (below) removes this leg
   entirely.
3. **runner → Fulcio/Rekor** (Phase 2 generation only). Public-internet
   egress from the runner. Fulcio additionally only issues certificates for
   OIDC issuers *it* trusts — github.com, gitlab.com (SaaS tiers included),
   and a short public list; a corporate self-hosted GitLab is not on it.

| environment                                                  | Phase 1 | Phase 2 |
|--------------------------------------------------------------|---------|---------|
| github.com / gitlab.com (incl. enterprise cloud SaaS), runners with internet egress | yes | yes |
| github.com / gitlab.com, zero-egress runners                 | yes     | no (can't reach Fulcio/Rekor) |
| self-hosted GitLab (any network)                             | yes     | no (Fulcio won't issue for the issuer) |
| fully airgapped (issuer + runners inside)                    | yes     | no      |
| org running a private Sigstore stack                         | yes     | yes, `--sigstore-trust-root` points at their private root |

pypiron's *verification* side is airgap-clean in every row (trust root and
pinned JWKS are local files, no runtime Sigstore calls) — the Phase 2
constraints are upstream of us. Running a private Fulcio/Rekor is explicitly
out of scope for pypiron. Consequence: Phase 1 alone is the complete feature
for self-hosted/zero-egress orgs and the docs must position it that way;
Phase 2 is the add-on wherever row 1 applies. (A pypiron-native signed publish
receipt for the "no" rows — pypiron attesting what it verified at ingest — is
a conceivable future rung; it is NOT this plan, do not build it.)

## Phase 2 — PEP 740 accept-and-verify (first-party attestations)

Precondition: Phase 1 merged. Do not start Phase 2 in the same change.
Audience per the matrix above: cloud-CI (github.com / gitlab.com) or
private-Sigstore orgs only.

### Design

`twine --attestations` sends, per file, an `attestations` form field: a JSON
array of PEP 740 attestation objects (`{version, verification_material:
{certificate, transparency_entries}, envelope: {statement, signature}}`).
Today `legacy_upload` refuses the field. New behavior:

- Upload authenticated by a Phase 1 token (Claims has `pubid`) **and**
  attestation acceptance enabled (`--sigstore-trust-root` set): verify the
  bundle (below). Success → synthesize the PEP 740 provenance object and pass
  it through the existing `PublishRequest.provenance`; failure → 400 with the
  reason, nothing stored. Never store an unverified bundle.
- Any other upload with `attestations` (basic auth, no `pubid`, or trust root
  unset): keep today's refusal, message updated to point at the docs.

Synthesized provenance (what PyPI serves, shape already consumed by
`src/provenance.rs::parse_publisher` — reuse its test fixtures as the shape
reference):

```json
{"version": 1, "attestation_bundles": [{
   "publisher": {"kind": "GitHub", "repository": "acme/payments",
                  "workflow": "...", "environment": "..."},
   "attestations": [ ...verbatim objects from the upload... ]}]}
```

`publisher` is filled from the *verified* identity (cert + token, which must
agree), never from client-supplied fields. Storage/serving is byte-for-byte
the mirror relay path: `<filename>.provenance` companion, `provenance: true`
sidecar bit, `data-provenance`/`provenance` in indexes, publisher box on the
human page. No new storage variants (DESIGN storage contract).

### Verification requirements (all hard-fail unless marked)

1. Each attestation's `envelope.statement` is a base64 in-toto v1 Statement;
   its single subject digest (`sha256`) equals the upload's already-verified
   sha256, and the subject name equals the filename.
2. DSSE PAE signature (`envelope.signature`) verifies with the leaf
   certificate's public key (ECDSA P-256 — the only algorithm to accept).
3. The leaf certificate (from `verification_material.certificate`, DER/base64)
   chains to a CA in the configured trust root; leaf validity is checked
   against the Rekor integrated time when a transparency entry is verified,
   else against a policy decision recorded in code comments (see 5).
4. The certificate's Fulcio identity — SAN URI plus the Fulcio OID extensions
   (build trigger / repository / workflow ref) — matches the minting token's
   `pubid` and the matching trusted-publisher entry. Cert and transport
   telling different stories is a hard refusal.
5. Transparency (Rekor) inclusion proof: verify if the chosen library provides
   it (checkpoint signature + Merkle inclusion against the trust root's Rekor
   key). If it does not, v1 may skip it — record the limitation in the design
   note and standards page explicitly, and validate the leaf cert window
   against upload wall-clock time instead. Do not hand-build Merkle/checkpoint
   verification in v1.

### Library decision (do this first, timebox it)

Evaluate the `sigstore` crate (sigstore-rs) for bundle verification against a
`trusted_root.json`. Take it if it (a) verifies DSSE bundles offline against a
supplied trust root, (b) doesn't drag in a second TLS/crypto stack beyond
what's already in the lockfile, (c) is maintained. Otherwise compose the
narrow verifier from `x509-cert` + a P-256 ECDSA crate + ~50 lines of DSSE PAE
— parsing X.509 by hand is forbidden, but PAE encoding is trivial and fine.
Record the choice and rationale in the commit message and DESIGN note.

### Trust root

- `--sigstore-trust-root PATH` / `PYPIRON_SIGSTORE_TRUST_ROOT` /
  `serve.sigstore-trust-root`: a standard Sigstore `trusted_root.json`
  (operators fetch it with `cosign` or from the sigstore/root-signing repo;
  document the one-liner). Setting it enables attestation acceptance.
- Fail-closed: set-but-unreadable/unparseable → refuse startup. Not set →
  attestations refused as today (Phase 1-only deployments keep working).
- Loaded once at startup; rotation = replace file + restart. No runtime
  network dependency on Sigstore infrastructure, ever (airgap works). A
  `pypiron trust-root fetch` convenience command is optional follow-up, not
  v1.

### Policy knob

Per-entry `require-attestations = true` (default false) on
`[[serve.trusted-publisher]]`: uploads minted through that entry must carry
verifying attestations or be refused. Startup-refuse an entry setting it while
`sigstore-trust-root` is unset (half-configured → closed).

### Tests

- Unit (pure): DSSE PAE encoding vectors; statement digest/subject matching;
  Fulcio extension → identity extraction; identity-vs-pubid agreement;
  synthesized provenance shape round-trips through
  `provenance::parse_publisher`.
- Blackbox (`tests/test_attestations.py`): a Python fixture factory (test-time,
  `cryptography` lib) builds a fake Fulcio: test CA → leaf cert with SAN URI +
  Fulcio OIDs for the test identity → DSSE-signs a real in-toto statement over
  the uploaded file's digest → emits a matching minimal `trusted_root.json`.
  Combined with Phase 1's fake issuer, the whole flow runs offline against the
  real binary. Cover: attested publish → `.provenance` served, `provenance`
  key in PEP 691 JSON, `data-provenance` in HTML, publisher box rendered;
  tampered digest refused; signature by a non-chaining cert refused; cert
  identity ≠ token identity refused; `require-attestations` entry refuses an
  unattested upload; basic-auth upload with attestations still refused; trust
  root unset → old refusal message; `uv`/`pip` install of the attested package
  still works (no client regression).
- Manual compat check (not CI): one real `twine --attestations` publish from a
  real GitHub Actions run against a test instance with the real Sigstore trust
  root, before calling the feature done.

### Docs & bookkeeping

- Revise the recorded stance: `dev/DESIGN.md` §"PEP 740" (~line 581) — relay
  for mirror stays; first-party acceptance now exists behind
  publisher-verified uploads + pinned trust root; pypiron still never
  *synthesizes* attestations. `private/ROADMAP.md` line ~13 updated to match.
- `docs/reference/standards.md`: PEP 740 row → served + accepted-with-
  verification, flagged experimental (note the Rekor caveat if item 5 was
  skipped).
- `docs/reference/configuration.md`: `sigstore-trust-root`,
  `require-attestations`, in the same experimental subsection as Phase 1's
  keys.
- Extend the trusted-publishing guide with the attestations section
  (`twine --attestations`, what gets verified, what the `.provenance` file
  proves and how to re-verify it independently) — leading with the
  environment matrix above so self-hosted/airgapped readers know Phase 1 is
  their complete feature.

### Acceptance

`make check` + `make test` clean; blackbox file passes; a tampered bundle
cannot land; an attested private package serves provenance indistinguishably
(in mechanism) from a mirrored one; the DESIGN/ROADMAP stance is updated in
the same change.

---

## Sequencing summary for the implementing agent

1. Phase 1 end-to-end (config → mint → binding → tests → docs). Commit.
2. Library evaluation spike for Phase 2; record decision. Commit (note only).
3. Phase 2 end-to-end. Commit.

Keep phases in separate commits/PRs. If anything here contradicts what you
find in the code, the code and `dev/DESIGN.md` win — update this plan file as
you go rather than silently diverging.
