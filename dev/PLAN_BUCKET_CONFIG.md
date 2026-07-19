# Plan: one bucket knob, per-bucket credentials

Status: proposed · 2026-07-19
Owner: bucket-config cleanup

## Problem

Storage selection has two overlapping surfaces. `--storage disk|s3|gcs|azure`
plus three per-backend name flags (`--s3-bucket`, `--gcs-bucket`,
`--azure-container`) configure a single bucket; `--buckets` configures a list.
They interact badly:

- Contradictory config is silently accepted: `--storage gcs --gcs-bucket a
  --s3-bucket b --buckets s3://c` starts cleanly on `s3://c` and drops three
  flags with zero diagnostics. That violates the fail-closed rule.
- `--s3-bucket` implies S3 but `--gcs-bucket` doesn't imply GCS — arbitrary
  asymmetry.
- All credentials, endpoints, and the S3 region are process-global. Two S3
  buckets with different credentials or endpoints (two accounts, AWS + MinIO,
  two Azure storage accounts) are unconfigurable, while the docs say "mix
  backends freely."

No external users; no back-compat obligation. The one live deployment (the
public mirror, `bench/deploy.sh`, env `PYPIRON_S3_BUCKET`) is ours and gets
redeployed as part of this work.

## Goal

One way to say where artifacts live:

```
pypiron serve                                  # disk (default), --data-dir
pypiron serve --buckets s3://my-bucket         # single S3 bucket, ambient AWS creds
pypiron serve --buckets s3://east@us-east-1,gs://backup   # multi-cloud
```

The normal case stays zero-ceremony: point `--buckets` (or `PYPIRON_BUCKETS`)
at one bucket and the standard credential chain (env vars, instance role, ADC)
just works — no TOML required, nothing else to set. Complexity is opt-in: a
per-bucket override table in `pypiron.toml` for the rare setups that need
different credentials or endpoints per bucket.

## Non-goals

- No new backends, no `file://` URIs (disk stays `--data-dir`, single-node).
- No secrets in TOML, ever. Secrets stay in env; TOML may only *name* an env
  prefix.
- No change to the storage tree, sidecars, topology stamps, or the
  multi-bucket runtime (health, fan-out, `_repl/`, read affinity). This is
  configuration surface only.
- No re-litigating shipped roadmap items (ordered bucket list, `@region`
  read affinity, `buckets migrate`).

## Final surface

### Deleted (5 flags + their env vars + TOML keys)

| Gone | Replaced by |
|---|---|
| `--storage disk\|s3\|gcs\|azure` | implicit: `--buckets` present → object storage; absent → disk |
| `--s3-bucket NAME` | `--buckets s3://NAME` |
| `--gcs-bucket NAME` | `--buckets gs://NAME` |
| `--azure-container NAME` | `--buckets az://NAME` |
| `--aws-region REGION` | `@region` in the URI; else ambient `AWS_REGION`/`AWS_DEFAULT_REGION` (already read by `AmazonS3Builder::from_env()`), else SDK default |

`--aws-region` deserves the axe too: it was the only flag violating the
`PYPIRON_*` env convention, and it's redundant — the per-bucket `@region`
suffix covers explicit config and `from_env()` already honors the ambient AWS
env vars, so behavior for env-driven deployments is unchanged.

### Kept

- `--buckets URI,...` / `PYPIRON_BUCKETS` / `buckets = [...]` — the one knob.
  Grammar unchanged: `s3://name[@region]`, `gs://name[@region]`,
  `az://container[@region]`, comma-separated, order is preference. A
  single-entry list keeps today's guarantee: behaves exactly like a directly
  configured backend — no topology stamp, no health machinery, SDK retries on.
- `--data-dir`, `--storage-prefix` — unchanged.
- Backend-wide defaults, applying to every bucket of that backend (the common
  case is one endpoint / one account, and the MinIO/Azurite test fixtures
  lean on these): `--s3-endpoint-url`, `--s3-force-path-style`,
  `--gcs-service-account-path`, `--gcs-endpoint-url`, `--azure-account`,
  `--azure-access-key` (env/CLI only, never TOML), `--azure-endpoint-url`,
  `--azure-use-emulator`.

### New: per-bucket overrides (TOML only)

```toml
[serve]
buckets = ["s3://iron-east@us-east-1", "s3://minio-cache"]

[serve.bucket."s3://minio-cache"]
endpoint-url = "http://minio.internal:9000"
force-path-style = true
env-prefix = "MINIO_CACHE_"     # secrets: MINIO_CACHE_AWS_ACCESS_KEY_ID / ..._AWS_SECRET_ACCESS_KEY
```

- Keyed by bucket identity. The key is parsed with `parse_bucket_uri`, so
  `"s3://minio-cache"` and `"s3://minio-cache@us-west-2"` both resolve to the
  same bucket (identity excludes `@region`, matching topology semantics).
- Fields, each valid only for the matching scheme (wrong scheme → startup
  error): `endpoint-url` (any), `force-path-style` (s3), `env-prefix`
  (s3/azure secrets), `service-account-path` (gcs), `account` (azure).
  `#[serde(deny_unknown_fields)]` like every other config struct.
- `env-prefix` is the secret indirection — TOML names *where* the secret
  lives, never the secret itself:
  - s3: `<P>AWS_ACCESS_KEY_ID`, `<P>AWS_SECRET_ACCESS_KEY`, optional
    `<P>AWS_SESSION_TOKEN`, applied via `with_access_key_id`/
    `with_secret_access_key`/`with_token` (explicit beats `from_env()`).
  - azure: `<P>AZURE_ACCESS_KEY` via `with_access_key`.
  - gs://: `env-prefix` is a startup error; GCS creds are a key file — use
    per-bucket `service-account-path` (which also enables presigning for
    that bucket, per the existing `can_sign` logic).
- Per-bucket overrides are TOML-only by design. Multi-credential fleets
  already need config management; inventing an indexed env-var namespace
  (`PYPIRON_BUCKET_2_...`) to avoid a 5-line TOML file is complexity with no
  customer.

### Fail-closed rules (new validation, all at startup, before any bucket I/O)

1. `[serve.bucket."..."]` naming a bucket not in the configured list → error
   listing the valid identities (typo protection).
2. `env-prefix` set but only one credential half present → error, mirroring
   `credential_pair_error` (`src/app.rs:6122`). Prefix set and *neither* half
   present → error too: scoped creds were promised and not delivered.
   Empty-string env values count as unset (`nonempty`, `src/app.rs:6088`).
3. Override field invalid for the bucket's scheme → error naming field,
   bucket, and scheme.
4. `--data-dir` explicitly set alongside `--buckets` → startup *warning*
   (not error: the Dockerfile ships `ENV PYPIRON_DATA_DIR=/data` as a benign
   default, and `--data-dir` has an implicit default anyway — it names a
   fallback, not a second truth).

With the singular flags gone, the silent-contradiction bug class is deleted
structurally rather than patched.

## Implementation phases

Each phase lands green (`make check` + `make test`) with its docs. Sizing from
the blast-radius audit: ~5 Rust files, ~7 test files (1 heavy: `conftest.py`),
~10 docs/scripts.

### Phase 1 — collapse the selection surface

Rust (`src/storage.rs`, `src/config.rs`, `src/app.rs`, `src/config_template.toml`):
- Delete `StorageBackend` enum, `storage`, `s3_bucket`, `gcs_bucket`,
  `azure_container`, `aws_region` fields; delete `effective_backend()`,
  `has_s3_bucket()`.
- `build_backends()`: `--buckets` non-empty → build each spec (unchanged);
  empty → disk at `resolved_data_dir()`. Delete the four-way backend match.
- `bucket_names()` / `describe()`: always URI identities or the disk path.
  Note: single-bucket S3 metric/log labels change from bare `name` to
  `s3://name`. Safe — single-bucket mode never wrote a topology stamp, so no
  stored identity exists to mismatch; only dashboards notice.
- `merge_storage_file()`: drop the deleted keys; drop the
  `arg_from_cli_or_env` special-case for `storage`.
- `ServeConfig`: drop `storage`, `s3_bucket`, `gcs_bucket`, `azure_container`,
  `aws_region`; template + round-trip test updated.
- Fix the stale "shared by serve and sync" doc comment (`src/storage.rs:107`)
  — sync never embeds `StorageArgs`.
- Region plumbing in `build_one_s3`: `@region` else builder default (which
  `from_env()` seeds from ambient `AWS_REGION`).

Tests:
- `tests/conftest.py`: `_s3_env()` always emits `PYPIRON_BUCKETS` (delete the
  `PYPIRON_S3_BUCKET`/`PYPIRON_STORAGE` branch); GCS/Azure fixtures switch to
  `PYPIRON_BUCKETS=gs://...` / `az://...`. The ~15 fixture-consuming test
  files need no edits.
- Mechanical updates in `test_multibucket.py`, `test_cli.py`,
  `test_transparency.py`, `test_advisories.py`, `test_crash_consistency.py`,
  `test_multicloud.py`; `src/storage.rs` unit tests; `src/config.rs` tests.
- New blackbox assertion: single-entry `--buckets s3://x` writes no topology
  stamp and serves presigned URLs (the old single-bucket guarantees).

### Phase 2 — per-bucket overrides

- `config.rs`: `BucketOverride` struct + `bucket: Option<HashMap<String,
  BucketOverride>>` on `ServeConfig` (first nested table in the config; keep
  it the only one).
- `StorageArgs` gains a `#[clap(skip)] overrides` field populated by
  `merge_storage_file()`, so all six `#[command(flatten)]` embeds — serve and
  the maintenance commands (`verify-index`, `rebuild-index`, `verify-chain`,
  `origin release`, `buckets migrate`) — resolve identical per-bucket config
  from the same `[serve]` table. `build_one_by_identity()` (migrate's
  removed-bucket path) resolves overrides by the same identity match.
- `build_one_s3`/`build_one_gcs`/`build_one_azure` take the resolved override:
  start `from_env()`, apply backend-wide defaults, then explicit `with_*`
  overrides (explicit wins over env — confirmed object_store 0.13.2
  semantics).
- All fail-closed rules above, parsed and validated up front, one error names
  the offending entry (the `bucket_specs()` pattern).
- Tests: a second MinIO container with different root credentials in
  `conftest.py`; blackbox tests for (a) two-bucket fan-out where each bucket
  is only reachable with its own creds, (b) startup refusal on half-configured
  `env-prefix`, (c) startup refusal on an override keyed to an unknown bucket,
  (d) per-bucket `endpoint-url` steering (one bucket on each MinIO).

### Phase 3 — ecosystem sweep

- `docs/reference/configuration.md`: rewrite the storage section around the
  single knob; document per-bucket overrides and the env-prefix contract;
  state plainly what is per-bucket vs backend-wide. `docs/guides/setup.md`,
  `docs/guides/multi-region.md`, `docs/for-agents.md` touch-ups.
- `dev/MULTIBUCKET.md` (the `--buckets` contract) + `dev/TESTING.md` +
  `dev/BENCHMARK_INSTALL.md`.
- `bench/deploy.sh`, `bench/install/rig.sh`, `rig2.sh`, the two pypiron
  compose files, `Makefile` (`run` target unchanged — disk), Dockerfile
  (keep `ENV PYPIRON_DATA_DIR=/data`).
- CI real-GCS leg (`.github/workflows/ci.yml:157-210`) to `PYPIRON_BUCKETS`.
- Redeploy the public mirror with `PYPIRON_BUCKETS=s3://pypiron-mirror-...`
  (instance role — no other change).
- `ops/soak/` untouched: `PYPIRON_SOAK_S3_BUCKET` is the harness's own sink,
  a different namespace.

## Decisions taken (so nobody re-derives them)

- Keep the name `--buckets` even for one bucket. One flag, one grammar; a
  singular alias is a second spelling of the same thing.
- Disk is not a URI. It's the default, it's single-node, and `file://` would
  imply multi-bucket disk combinations we refuse to support.
- Backend-wide flags stay as defaults; per-bucket tables override them.
  Deleting them would force TOML on every MinIO/Azurite user for no gain.
- TOML-only per-bucket overrides; no indexed env namespace.
- `--azure-access-key` stays env/CLI-only (existing secrets rule); per-bucket
  Azure secrets go through `env-prefix`.

## Risks

- Metric label change (`s3://name` vs bare name) breaks any dashboard keyed
  on the old label — one live Grafana to check.
- `conftest.py` is the single heavy edit; the fixture indirection means a
  mistake there fails loudly across the whole suite, which is the point.
- Second MinIO container adds ~1 container per xdist worker in the two-creds
  tests; keep those tests in one file so the fixture stays narrowly scoped.
