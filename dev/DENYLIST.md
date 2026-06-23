# Denylist + mirror freshness reconcile

_Status: design spec. Nothing here is implemented._

## The gap

PypIron already verifies a sha256 on every ingest (upload, `sync`, proxy) and
claims each name `private` or `mirror` at first write. What it has **no answer
for** is *time*:

1. **Stale malware.** A package you mirrored or proxied is point-in-time. When
   PyPI later **yanks** it (PEP 592) or **removes** it outright (a malware
   takedown), your copy is untouched and keeps installing forever. The mirror
   is a freezer; upstream's "this is poison, stop using it" signal never
   arrives.
2. **No off-switch.** An operator who learns `evilpkg==1.2.3` is compromised has
   no way to say "never serve this" short of hand-deleting objects and racing
   the next `sync` that re-adds them.

This spec adds two cooperating pieces:

- **A denylist** — truth-as-files saying "never serve / never ingest this,"
  enforced everywhere, needing **zero network**. This is the off-switch and the
  enforcement substrate.
- **A mirror-freshness reconcile** — a leader-side job that re-checks
  `mirror`-origin packages against upstream and propagates yanks, removals, and
  (for hostile upstreams) hash changes into local truth, optionally feeding the
  denylist from a malicious-package feed.

The hard constraint, called out throughout: **the serving node may have no
internet egress.** So enforcement is pure storage, and every fetch is factored
out into a job that can run on a connected host and ship its result across the
gap as files.

## Design principles (inherited, non-negotiable)

- **No DB.** The denylist is files in storage. The in-memory form is a
  regenerable view, ETag-pinned exactly like `worker::GlobalNames`.
- **Truth heals views.** Yank propagation writes the sidecar `yanked` field —
  already truth, already self-healing through the existing rebuild path. A
  blocked file is *omitted from the index* by the same mechanism a corrupt
  sidecar already uses (`load_file_metadata` → `None`, `worker.rs:805`).
- **Fetch ≠ apply.** Anything that touches the network lives in a job that
  produces a file. Applying that file is pure storage and runs on the airgapped
  node. This is the whole airgap story.
- **Never destroy on suspicion.** A by-hash block *quarantines* (moves the
  object aside), it does not delete. A false-positive feed entry must be
  recoverable. (Consistent with "look before you overwrite.")

---

## Part A — The denylist (enforcement substrate, zero egress)

### Truth: one document

```
_deny/denylist.json        # truth (CAS-written, like simple/index.json)
```

The `_deny/` prefix sits beside the existing control prefixes (`_dirty/`,
`_state/`) — outside `packages/` and `simple/`, so the audit's shard
enumeration ignores it for free.

Schema:

```json
{
  "version": 1,
  "entries": [
    {
      "match": { "sha256": "9f86d0818..." },
      "mode": "block",
      "reason": "OSV MAL-2024-1234: credential stealer",
      "source": "osv",
      "added": "2026-06-17T00:00:00Z"
    },
    {
      "match": { "name": "evilpkg", "version": "1.2.3" },
      "mode": "block",
      "reason": "internal IR-2026-08",
      "source": "operator",
      "added": "2026-06-17T00:00:00Z"
    },
    {
      "match": { "name": "leftpad", "filename": "leftpad-*-win_amd64.whl" },
      "mode": "yank",
      "reason": "compromised win build, upstream-removed",
      "source": "reconcile",
      "added": "2026-06-17T00:00:00Z"
    }
  ]
}
```

- **`match`** is exactly one of: `sha256` (lowercase hex), `name`
  (+ optional `version` or `filename` glob). `name` is matched
  **normalized** (`normalize_pkg_name`) so `Evil_Pkg` and `evil-pkg` are the
  same rule. A bare `name` with no version blocks the whole project.
- **`mode`**:
  - `block` — hard. Refuse ingest, omit from every index, 404 on direct serve,
    and (for `sha256` matches) quarantine the stored artifact.
  - `yank` — soft, PEP 592 semantics. Sets the sidecar `yanked` reason; the file
    stays installable *only* if a resolver already pinned it. This is the
    natural target for "upstream yanked/removed it" where deletion isn't proof
    of malice.

One JSON document, not a million marker files: a denylist is read on the hot
path and shipped across air gaps; a single CAS-written object is cheaper to load
(one GET, ETag-revalidated) and trivial to move. If a deployment's denylist ever
outgrows a single object (tens of thousands of entries), shard it by
`sha256[0]` under `_deny/by-hash/<c>.json` — the same move the corpus index
already makes. Not worth the code until measured.

### View: in-memory `DenyIndex`

Loaded once, refreshed on ETag change, mirroring `worker::GlobalNames`
(`worker.rs:605`):

```rust
struct DenyIndex {
    etag: Option<String>,
    by_hash: HashSet<String>,                 // sha256 → block
    by_name: HashMap<String, Vec<NameRule>>,  // normalized name → rules
}
struct NameRule { version: Option<String>, filename_glob: Option<String>, mode: Mode, reason: String }
```

Lookups are `O(1)` hash / small-vec scan — cheap enough for the artifact serve
path. It lives in `AppState` as `deny: Arc<RwLock<DenyIndex>>`, refreshed at the
top of each worker tick (a one-object conditional GET; `None`/404 → empty
denylist, fail-open is correct here because an *absent* denylist is the default
state, not a security downgrade).

### Enforcement points

| Where | Code site | Check | Action |
|---|---|---|---|
| **Upload ingest** | `legacy_upload`, `main.rs:643` | by-name/version/filename **and** by-hash (sha256 is computed on ingest) | `block` → 403 `denied: <reason>`; `yank` → store but write sidecar yanked |
| **Mirror ingest** | `sync::mirror_to_storage`, `proxy::ensure_artifact_cached` | same | `block` → skip-with-log, never claim/cache; `yank` → cache with yanked sidecar |
| **Proxy listing render** | `proxy::fetch_listing`, `proxy.rs:531` | by-name/version/filename | drop `block` files before rendering; mark `yank` files yanked |
| **Index build** | `worker::load_file_metadata`, `worker.rs:805` | by-hash (sidecar carries it) + by-name/version | `block` → return `None` (file vanishes from index, exactly like a corrupt sidecar); `yank` → override sidecar `yanked` in the emitted `FileMetadata` |
| **Direct artifact serve** | `files_get`, `main.rs:1294` | by-name/version/filename (cheap, in-memory) | `block` → 404 |

**Deliberate asymmetry — by-hash is not checked on the hot serve path.**
`files_get` presigns or streams without reading the sidecar (that's the whole
point of the fast path), so it has the *name+filename* but not the *hash*. We
check name/filename there (covers `evilpkg==1.2.3` and per-file blocks — the
realistic operator action) and let by-hash blocks get their teeth from **index
omission + quarantine**: the reconcile/apply step moves a hash-blocked object to
`_quarantine/<pkg>/<filename>`, after which the direct URL 404s and the file is
already gone from the index. A pure-hash block with the artifact still resident
is closed within one reconcile pass, not synchronously on first GET — an
acceptable window, documented, not papered over.

### Quarantine, not delete

A `block` on a stored artifact moves it (and its sidecar/metadata) to
`_quarantine/<pkg>/<filename>` and drops a `_dirty/<pkg>` marker. The artifact
leaves `packages/` truth → it 404s and the worker rebuilds the index without it.
Recovery is a move back. We never `delete_keys` on a denylist hit: a feed false
positive must not be a data-loss event.

### Making a denylist change heal

Adding `_deny/denylist.json` doesn't change any `packages/<pkg>/` listing, so
the churn-proportional audit (`worker.rs:298`) won't notice. So every writer of
the denylist (`deny add`, `deny import`, reconcile) **also drops `_dirty/<pkg>`
markers for the affected names** — which it always knows, because every entry
resolves to one or more packages (a by-hash feed entry carries the
project/version; a pure-hash-only entry is resolved to its `(pkg, filename)` at
apply time by scanning sidecars, the one expensive case, gated behind
`--rescan`). The existing tick then heals those packages' views. No new view
machinery.

### CLI (all pure storage — runs on the airgapped node)

```
pypiron deny add  --sha256 <hex> | --name <pkg> [--version V | --file GLOB]
                  --mode block|yank --reason "<text>"
pypiron deny remove <selector>
pypiron deny list [--mode block|yank]
pypiron deny import <denylist.json>     # merge a denylist document into storage
pypiron deny export [--mode ...]        # emit the current denylist document
```

`add`/`remove`/`import` CAS-update `_deny/denylist.json` and mark affected
packages dirty. These are new `Commands` variants alongside `Sync`/`VerifyIndex`/
`RebuildIndex` (`main.rs:61`), each taking `StorageArgs` — they need storage, never
the network.

### Config & server flags

```
--deny-refresh-interval <dur>   # how often the server reloads the denylist view (default 60s)
```

A management HTTP surface (`PUT/DELETE /deny/...`, admin-only) is a natural
follow-up but **not required** — the CLI against shared storage covers it and
keeps the serving binary's attack surface smaller.

---

## Part B — Mirror freshness reconcile (the populator; needs egress)

A new leader-only worker sub-task, scheduled like the audit but on its own
cadence, **off by default**:

```
--mirror-revalidate-interval <dur>     # e.g. 6h; unset = disabled
--on-upstream-removal yank|quarantine|ignore   # default: yank
--mirror-upstream <url>                # where to re-check (defaults to --proxy-upstream)
--malicious-feed <url|path>            # optional OSV-format MAL feed → denylist
```

### What it does, per `mirror`-origin package

It walks names whose `.origin` is `mirror` (`origin::read_origin`), and for each
fetches the upstream PEP 691 listing — reusing `proxy::Proxy::fetch_listing` /
`sync::fetch_selected_files` verbatim — then diffs upstream against local truth:

| Upstream says | Local truth | Action |
|---|---|---|
| file now **yanked** (or yank reason changed) | sidecar `yanked: false` | rewrite sidecar `yanked` (truth → heals into index); `mark_dirty` |
| file **gone from listing** (removed/takedown) | file present locally | `--on-upstream-removal`: `yank` (reason "removed upstream") / `quarantine` / `ignore` |
| same filename, **different sha256** | sidecar sha256 differs | **tamper signal** — PyPI artifacts are immutable, so this only happens against a hostile/compromised upstream. Quarantine + `mirror_hash_mismatches` metric + error log. Never overwrite the local bytes. |
| file **un-yanked** upstream | sidecar `yanked: true` from a prior propagation | clear it (only if `source` was `reconcile`; never un-yank an operator yank) |
| new file appears upstream | absent locally | **out of scope** — adding content is `sync`/proxy's job, not freshness. Freshness only ever *restricts*. |

The reconcile **only ever restricts or annotates**; it never adds artifacts.
That keeps it safe to run unattended: the worst it can do is over-yank (a false
removal), which is reversible and non-destructive.

### Reuse, not new code

- Upstream fetch: `proxy::Proxy` (already has the timeout-bounded client,
  retry, PEP 691 parse, filter logic).
- Mirror enumeration: list `packages/<shard>` (audit already does this), keep
  the `.origin == mirror` names.
- Healing: write sidecar + `mark_dirty` — the existing yank path
  (`set_yanked`, `main.rs:1561`) already proves this is the right primitive.
- Scheduling: clone the audit's `sweep_running`-guarded `tokio::spawn` block
  (`worker.rs:186`) with its own `last_*` timer.

### Cost & throttle

One PEP 691 GET per mirror package per interval. For a large mirror this is the
same shape as the proxy listing load, so it inherits the same answer: bounded
concurrency (reuse `PACKAGE_SWEEP_CONCURRENCY`), and the interval defaults long
(6h) because yank/removal is not a latency-sensitive event. A global outbound
semaphore (the SECURITY_AUDIT L10 fix) bounds it further if both land together.

### SSRF note

The reconcile fetches upstream-controlled file URLs only to *re-read listings*
(the PEP 691 index), never to follow per-file `url`s — so it does **not**
re-open the `sync` SSRF surface (SECURITY_AUDIT M1). It reads the index at
`--mirror-upstream/simple/<pkg>/` and compares metadata; it downloads nothing.

---

## Part C — The airgapped serving node (no egress)

The serving node cannot fetch OSV, cannot reach PyPI, cannot run Part B. So Part
B's *output* is delivered to it as files, through whichever channel already
moves wheels into the gap. Two supported topologies:

### C1 — Shared object store (connected reconciler, airgapped servers)

The common "S3 mirror" shape: serving nodes have **no internet** but share a
bucket with a small connected reconciler host (or a cron container with egress).

```
[ connected reconciler ]  --internet-->  PyPI / OSV
        |  writes _deny/denylist.json + sidecar yanks + quarantine moves
        v
   [ shared S3 bucket ]
        ^
        |  reads (ETag refresh + audit) — NO internet
[ airgapped pypiron serving nodes x N ]
```

- The reconciler runs `pypiron` with `--mirror-revalidate-interval` set (or a
  cron'd `pypiron mirror-revalidate` one-shot). It is the **only** node with
  egress and the **only** writer of freshness data.
- Serving nodes pick everything up through mechanisms they already have: the
  worker's denylist ETag refresh, and the existing audit/`_dirty` healing for
  sidecar yanks and quarantine moves. **Zero new transport, zero new egress.**
- Serving nodes can even run with read-only storage credentials (the roadmap's
  `--read-only` replicas): they enforce the denylist they can see and never need
  to write.

### C2 — Fully offline (sneakernet bundle)

No shared store — the gap is crossed by copying files (the same way wheels get
in). Split the work into **plan (egress) → apply (airgap)**:

**On a connected host** (has internet, *read* access to a manifest of what the
airgapped node holds — e.g. an exported `simple/index.json` or a `pypiron verify-index`
listing carried out):

```
pypiron mirror-revalidate --offline \
    --inventory inventory.json \      # what the airgapped node currently serves
    --malicious-feed osv-mal.json \
    --out freshness-plan.json
```

`freshness-plan.json` is a flat, self-contained instruction list — no URLs, no
fetches required to apply it:

```json
{
  "version": 1,
  "generated": "2026-06-17T00:00:00Z",
  "deny": [ { "match": {"sha256": "..."}, "mode": "block", "reason": "OSV MAL-..." } ],
  "yanks": [ { "name": "leftpad", "filename": "leftpad-1.0-...whl", "reason": "removed upstream" } ],
  "quarantine": [ { "name": "evilpkg", "filename": "evilpkg-1.2.3-...whl", "reason": "..." } ]
}
```

Carry `freshness-plan.json` across the gap (USB, data diode, review gate —
whatever the org's process is). **On the airgapped node:**

```
pypiron mirror-apply freshness-plan.json    # pure storage: deny writes, sidecar yanks, quarantine moves, dirty markers
```

`mirror-apply` makes only storage calls. It merges the `deny` block into
`_deny/denylist.json`, rewrites the named sidecars' `yanked`, moves quarantined
objects aside, and drops `_dirty/<pkg>` markers — then the normal worker heals
the views. It is idempotent (re-applying the same plan is a no-op) and
auditable (the plan is a reviewable artifact — a natural place to bolt a
human/data-diode approval gate before poison-pill instructions enter the gap).

### Why this split is the whole design

The denylist is *data*, not *behavior*. Enforcement (Parts A) is pure storage
and runs anywhere. Discovery (Part B) is the only thing that needs egress, and
it is packaged so its output is a file. C1 ships that file through a shared
bucket; C2 ships it by hand. The airgapped serving node runs identical code in
both — it only ever *reads a denylist and applies a plan*, never reaches out.

---

## Metrics (hand-rolled, `metrics.rs`)

```
pypiron_deny_entries{mode}                  # gauge: loaded denylist size
pypiron_deny_blocks_total{point}            # ingest|index|serve|proxy
pypiron_mirror_revalidate_runs_total
pypiron_mirror_yanks_propagated_total
pypiron_mirror_removals_total{action}       # yank|quarantine|ignore
pypiron_mirror_hash_mismatches_total        # tamper alarm — should always be 0
pypiron_mirror_apply_total{kind}            # deny|yank|quarantine (C2)
```

`mirror_hash_mismatches_total > 0` is the one to page on: against an immutable
upstream it can only mean a compromised or impersonated mirror source.

## Failure modes

- **Denylist unreadable / absent** → empty denylist, fail-open. Correct: absent
  is the default, not a downgrade. A *corrupt* (present-but-unparseable)
  `_deny/denylist.json` is logged at `error` and treated as empty, with a metric
  — matching how a corrupt sidecar is handled (loudly, not silently).
- **Reconcile upstream down** → no writes; retry next interval. Never yanks on a
  transient fetch failure (a 503 is not a removal — only an authoritative 404 /
  absence-from-a-200-listing counts as "removed").
- **Quarantine then false-positive** → `pypiron deny remove` + move the object
  back (a documented one-liner). Nothing was destroyed.
- **Two reconcilers (split-brain)** → idempotent like every other writer;
  denylist writes are CAS, sidecar yanks converge.

## Phasing

1. **Part A, operator-only.** Denylist document + `DenyIndex` + the five
   enforcement points + `deny add/remove/list/import/export` + quarantine. This
   alone delivers the off-switch and works fully airgapped. Highest value/cost.
2. **Part B yank+removal propagation** against `--mirror-upstream`, `yank` mode
   default. Closes the stale-mirror hole for connected/shared-bucket (C1)
   deployments.
3. **Part C2 offline plan/apply** (`mirror-revalidate --offline` +
   `mirror-apply`). Serves the fully-disconnected audience.
4. **`--malicious-feed`** (OSV `MAL-*`) ingestion → denylist. The automated
   feed; rides on 1–3.

## Not planned

- **In-server vulnerability *database* / live OSV queries on the serve path.**
  The denylist is a flat allow/deny set; resolving "is this version vulnerable"
  belongs in the feed job (Part B/C), not the hot path. The server enforces a
  decision; it doesn't make one per request.
- **Blocking by transitive dependency.** PypIron serves files; it doesn't
  resolve. Dependency-graph policy is a client/CI concern.
- **Auto-deleting blocked artifacts.** Quarantine, never delete — a feed is not
  infallible and storage is cheap.
- **A bespoke transport for the offline bundle.** It's one JSON file; it crosses
  the gap however wheels already do. No diode protocol, no daemon.
