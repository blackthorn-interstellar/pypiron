# Landscape data

A feature census of the Python package-server space. The data is the source of
truth; the markdown is generated.

```
taxonomy.json        canonical SUPERSET feature hierarchy (categories → features)
products/<id>.json   one record per product, scored against the taxonomy
exclude.json         ids hidden by default in the HTML ("non-contenders") — the honing list
compile.py           renders the data → LANDSCAPE.md + LANDSCAPE.html (+ merged data.json)
LANDSCAPE.md         generated markdown report (do not hand-edit)
LANDSCAPE.html       generated interactive report (do not hand-edit)
data.json            generated single-file merge of taxonomy + products
```

## Interactive HTML

`LANDSCAPE.html` is a single self-contained file (data embedded, no deps, opens
over `file://` or any static server). One wide table: a pinned product column +
pinned header, every feature as a real column under its category band, horizontal
scroll for the rest. Bold rules separate the feature-category groups; pypiron is
rendered bold (not highlighted). JS filters: text search, type toggles (Defunct
off by default), a min-coverage slider, an "Updated since" year filter (hides
solutions whose latest known update predates the chosen year), per-category column
show/hide, and a per-row ✕ to hide a product (persists in `localStorage`).

**Sortable** — click any column header (meta or feature) to sort; click again to
reverse. The active header is highlighted with a ▲/▼ arrow. Feature columns sort
by support level (supported first). "Reset filters" clears the sort and restores
the default order (pypiron pinned, then by type and name).

**Normalized fields** — the compiler derives two scannable, sortable columns from
the free-text records: **Pricing** buckets the cost blurb into
`Free / Freemium / Usage-based / Paid / Quote`, and **Updated** is reduced to the
latest ISO date mentioned. The full original text is in the cell tooltip and the
row's ⓘ detail panel. (License/Impl are likewise truncated with the full value on
hover.) Tweak the heuristics in `cost_model()` / `norm_updated()` in `compile.py`.

**Stars** — a sortable popularity column fed by the stored `stars` / `repo_pushed`
fields on each record (GitHub/Codeberg stargazers + last-push date of the project's
*own* source repo). It is blank for products whose only public repo is a client/CLI
or that live off those hosts — a CLI's star count is not the product's popularity.
Sort by it (click the header) to rank the field by adoption; null sorts last. These
are a point-in-time snapshot — refresh them from the repo before quoting.

### Honing the "serious contenders" list

The min-coverage slider is the quick lens. To make a hide permanent:

1. Open `LANDSCAPE.html`, click ✕ on the products that aren't serious contenders.
2. Click **Copy hidden IDs**, paste the array into `exclude.json` → `hidden_by_default`.
3. Recompile. Those rows are now hidden by default (the **Show non-contenders**
   toggle still reveals them).

## Regenerate

```sh
python docs/landscape/compile.py
# or, matching the repo's tooling:
uv run -- python docs/landscape/compile.py
```

Stdlib only — no dependencies.

## Editing

- Add or correct a product by editing `products/<id>.json`. Each record carries
  the same shape: identity/cost/license fields, `stars` (int or `null`) and
  `repo_pushed` (ISO date or `null`) for the popularity column, plus a `features`
  object keyed by every feature in `taxonomy.json`
  (value ∈ `yes|partial|planned|no|unknown`).
- Add a feature dimension by adding it to a category in `taxonomy.json`, then set
  the new key in each product (missing keys render as ❔ "unknown").
- The taxonomy is the **union** of every capability seen across the field, so it
  doubles as pypiron's feature backlog — see the "pypiron vs. the superset"
  section of the report for current gaps.

Ratings are a researched snapshot with per-product `sources`; verify against
current vendor docs before relying on a cell.
