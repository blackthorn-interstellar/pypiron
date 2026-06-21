#!/usr/bin/env python3
"""Compile the package-server landscape data into LANDSCAPE.md.

Data store (JSON, hand- or agent-editable):
  - taxonomy.json          the canonical *superset* feature hierarchy
  - products/<id>.json     one record per product, scored against the taxonomy

Run:  python docs/landscape/compile.py   (or: uv run -- python docs/landscape/compile.py)
Outputs:  LANDSCAPE.md  (rendered) and data.json (merged single-file view).
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent

VAL_EMOJI = {
    "yes": "✅",
    "partial": "🟡",
    "planned": "🟦",
    "no": "⬜",
    "unknown": "❔",
}
SCORE = {"yes": 1.0, "partial": 0.5, "planned": 0.0, "no": 0.0, "unknown": 0.0}

# Display order for the product type buckets.
TYPE_ORDER = [
    "open-source",
    "open-core",
    "tool",
    "commercial",
    "saas",
    "cloud-managed",
    "defunct",
]
TYPE_LABEL = {
    "open-source": "Open source",
    "open-core": "Open core",
    "tool": "Tool / generator",
    "commercial": "Commercial (self-hosted)",
    "saas": "SaaS",
    "cloud-managed": "Cloud-managed",
    "defunct": "Defunct",
}

PINNED_FIRST = "pypiron"  # the product this repo builds


def load_taxonomy() -> list[dict[str, Any]]:
    data = json.loads((HERE / "taxonomy.json").read_text())
    return data["categories"]


def load_products() -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    for path in sorted((HERE / "products").glob("*.json")):
        try:
            products.append(json.loads(path.read_text()))
        except json.JSONDecodeError as exc:
            print(f"WARNING: skipping unparseable {path.name}: {exc}", file=sys.stderr)
    return products


def load_excludes() -> list[str]:
    """IDs hidden by default in the HTML view ("not serious contenders").

    Edit exclude.json to hone the list; the HTML can still reveal them with a toggle.
    """
    path = HERE / "exclude.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return []
    return [str(x) for x in data.get("hidden_by_default", []) if x]


def anchor(name: str) -> str:
    out = []
    for ch in name.lower():
        if ch.isalnum():
            out.append(ch)
        elif ch in " -_":
            out.append("-")
    slug = "".join(out)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-")


def feat(product: dict[str, Any], key: str) -> str:
    return (product.get("features") or {}).get(key, "unknown")


def coverage_pct(product: dict[str, Any], keys: list[str]) -> float:
    if not keys:
        return 0.0
    total = sum(SCORE.get(feat(product, k), 0.0) for k in keys)
    return 100.0 * total / len(keys)


def sort_key(product: dict[str, Any]) -> tuple[int, int, str]:
    pid = product.get("id", "")
    pinned = 0 if pid == PINNED_FIRST else 1
    try:
        tindex = TYPE_ORDER.index(product.get("type", ""))
    except ValueError:
        tindex = len(TYPE_ORDER)
    return (pinned, tindex, product.get("name", "").lower())


def link(url: str, text: str | None = None) -> str:
    url = (url or "").strip()
    if not url:
        return "—"
    return f"[{text or url}]({url})"


def md_escape(text: str) -> str:
    return (text or "").replace("|", "\\|").replace("\n", " ").strip()


def short(text: str, n: int = 42) -> str:
    t = (text or "").strip()
    return (t[: n - 1].rstrip() + "…") if len(t) > n else t


_DATE_RE = re.compile(r"(?<!\d)(\d{4})[-/](\d{2})(?:[-/](\d{2}))?(?!\d)")


def norm_updated(text: str) -> str:
    """Reduce a free-text 'last updated' note to the latest ISO date it mentions.

    Records carry messy strings like '2026-06-17 (latest commit; tag v0.0.2)' or
    'Latest release v0.10.4 on 2023-12-04; last commit 2024-04-26'. We take the
    newest YYYY-MM[-DD] found; fall back to a bare year. Full text stays in tooltips.
    """
    if not text:
        return ""
    best_key = ""
    best_iso = ""
    for m in _DATE_RE.finditer(text):
        year, month, day = m.group(1), m.group(2), m.group(3)
        if not ("1990" <= year <= "2099") or not ("01" <= month <= "12"):
            continue
        iso = f"{year}-{month}" + (f"-{day}" if day else "")
        key = f"{year}-{month}-{day or '00'}"
        if key > best_key:
            best_key, best_iso = key, iso
    if best_iso:
        return best_iso
    year = re.search(r"(?<!\d)(?:19|20)\d{2}(?!\d)", text)
    return year.group(0) if year else ""


def fmt_stars(stars: Any) -> str:
    """Render a star count for the markdown tables; em-dash when unknown."""
    return f"{stars:,}" if isinstance(stars, int) else "—"


def cost_model(text: str) -> str:
    """Bucket a free-text pricing blurb into one scannable, sortable label."""
    if not text:
        return "Unknown"
    t = text.lower()
    has_free = (
        "free" in t or "$0" in t or "no cost" in t or "open source" in t or "open-source" in t
    )
    paid = any(s in t for s in ("$", "€", "/mo", "/yr", "/year", "per user", "per month", "per seat"))
    usage = any(
        s in t
        for s in (
            "pay-as-you-go",
            "pay as you go",
            "usage-based",
            "consumption",
            "per gb",
            "/gb",
            "per 10,000",
            "per-gb",
        )
    )
    quote = any(s in t for s in ("contact sales", "custom pricing", "quote", "contact us"))
    if usage:
        return "Usage-based"
    if has_free and paid:
        return "Freemium"
    if has_free:
        return "Free"
    if paid:
        return "Paid"
    if quote:
        return "Quote"
    return "Unknown"


def build(
    products: list[dict[str, Any]], taxonomy: list[dict[str, Any]], date: str
) -> str:
    all_keys = [f["key"] for cat in taxonomy for f in cat["features"]]
    std_keys = next(c["features"] for c in taxonomy if c["key"] == "standards")
    std_keys = [f["key"] for f in std_keys]

    products = sorted(products, key=sort_key)
    n = len(products)

    L: list[str] = []
    L.append("# The Python package-server landscape")
    L.append("")
    L.append(
        f"_Generated by `docs/landscape/compile.py` on {date} — do not edit by hand. "
        f"Edit `taxonomy.json` / `products/*.json` and recompile._"
    )
    L.append("")
    L.append(
        f"A feature census of **{n} products** that can index, host, proxy, mirror, or serve "
        f"Python/PyPI packages — open source, commercial, and cloud-managed. The feature "
        f"hierarchy below is the **superset** of every capability observed across the field "
        f"({len(all_keys)} features in {len(taxonomy)} categories); each product is scored against it."
    )
    L.append("")

    # Methodology / legend
    L.append("## How to read this")
    L.append("")
    L.append("Each feature is rated:")
    L.append("")
    L.append("| Symbol | Meaning |")
    L.append("|---|---|")
    L.append(f"| {VAL_EMOJI['yes']} | Supported (verified or clearly documented) |")
    L.append(f"| {VAL_EMOJI['partial']} | Partial — limited, or with notable caveats |")
    L.append(f"| {VAL_EMOJI['planned']} | Planned / beta / on a public roadmap |")
    L.append(f"| {VAL_EMOJI['no']} | Not supported |")
    L.append(f"| {VAL_EMOJI['unknown']} | Could not be determined |")
    L.append("")
    L.append(
        "**Coverage** is a single rollup across all "
        f"{len(all_keys)} features (supported = 1, partial = ½). **Std** counts fully-supported "
        f"items in the {len(std_keys)} PEP/protocol standards. **Stars** is the GitHub (or "
        "Codeberg) stargazers of the project's *own* source repo as a rough popularity proxy — "
        "blank for products whose only public repo is a client/CLI or that live off these hosts, "
        "since a CLI's stars aren't the product's. Treat ratings as a research "
        "snapshot, not a contract — verify against current docs before relying on any cell. "
        "See the per-product **Sources** for provenance."
    )
    L.append("")

    # Glance table
    L.append("## Products at a glance")
    L.append("")
    L.append(
        "| Product | Category | Hosting | Impl | License | Pricing | Stars | Std | Coverage | Updated |"
    )
    L.append("|---|---|---|---|---|:---:|---:|:---:|:---:|:---:|")
    for p in products:
        std = sum(1 for k in std_keys if feat(p, k) == "yes")
        cov = coverage_pct(p, all_keys)
        name_cell = f"[{md_escape(p.get('name', p.get('id', '?')))}](#{anchor(p.get('name', p.get('id', '')))})"
        row = [
            name_cell,
            TYPE_LABEL.get(p.get("type", ""), p.get("type", "—")),
            p.get("hosting", "—"),
            md_escape(short(p.get("language", ""))) or "—",
            md_escape(short(p.get("license", ""))) or "—",
            cost_model(p.get("cost", "")),
            fmt_stars(p.get("stars")),
            f"{std}/{len(std_keys)}",
            f"{cov:.0f}%",
            norm_updated(p.get("last_updated", "")) or "—",
        ]
        L.append("| " + " | ".join(row) + " |")
    L.append("")

    # Feature matrix per category (numeric headers to stay narrow)
    L.append("## Feature matrix")
    L.append("")
    L.append(
        "Columns are numbered per category; the numbered legend precedes each table. "
        "Rows are products, sorted as in the table above."
    )
    L.append("")
    for cat in taxonomy:
        feats = cat["features"]
        L.append(f"### {cat['category']}")
        L.append("")
        legend = "  ".join(f"**{i + 1}.** {f['name']}" for i, f in enumerate(feats))
        L.append(legend)
        L.append("")
        header = (
            "| Product | " + " | ".join(str(i + 1) for i in range(len(feats))) + " |"
        )
        sep = "|---|" + "|".join([":---:"] * len(feats)) + "|"
        L.append(header)
        L.append(sep)
        for p in products:
            cells = [
                VAL_EMOJI.get(feat(p, f["key"]), VAL_EMOJI["unknown"]) for f in feats
            ]
            label = md_escape(p.get("name", p.get("id", "?")))
            L.append(f"| {label} | " + " | ".join(cells) + " |")
        L.append("")

    # Per-product details
    L.append("## Product details")
    L.append("")
    for p in products:
        name = p.get("name", p.get("id", "?"))
        L.append(f"### {name}")
        L.append("")
        if p.get("summary"):
            L.append(f"> {md_escape(p['summary'])}")
            L.append("")
        L.append(
            f"- **Category:** {TYPE_LABEL.get(p.get('type', ''), p.get('type', '—'))} · hosting: {p.get('hosting', '—')}"
        )
        if p.get("vendor"):
            L.append(f"- **Vendor:** {md_escape(p['vendor'])}")
        L.append(f"- **License:** {md_escape(p.get('license', '—')) or '—'}")
        L.append(f"- **Cost:** {md_escape(p.get('cost', '—')) or '—'}")
        L.append(f"- **Implementation:** {md_escape(p.get('language', '—')) or '—'}")
        rel = md_escape(p.get("first_released", "")) or "—"
        upd = md_escape(p.get("last_updated", "")) or "—"
        L.append(f"- **First released / last updated:** {rel} / {upd}")
        links = " · ".join(
            x
            for x in [
                link(p.get("repo", ""), "repo"),
                link(p.get("homepage", ""), "homepage"),
            ]
            if x != "—"
        )
        if links:
            L.append(f"- **Links:** {links}")
        if isinstance(p.get("stars"), int):
            pushed = p.get("repo_pushed") or "?"
            L.append(
                f"- **Popularity:** {p['stars']:,} stars · repo last pushed {pushed}"
            )
        cov = coverage_pct(p, all_keys)
        std = sum(1 for k in std_keys if feat(p, k) == "yes")
        L.append(
            f"- **Coverage:** {cov:.0f}% overall · {std}/{len(std_keys)} standards · confidence: {p.get('confidence', '—')}"
        )
        if p.get("perf_notes"):
            L.append(f"- **Performance:** {md_escape(p['perf_notes'])}")
        if p.get("scale_notes"):
            L.append(f"- **Scale / HA:** {md_escape(p['scale_notes'])}")
        if p.get("extras"):
            L.append("- **Notable extras:**")
            for e in p["extras"]:
                L.append(f"  - {md_escape(e)}")
        srcs = [s for s in (p.get("sources") or []) if s]
        if srcs:
            L.append("- **Sources:** " + " · ".join(link(s) for s in srcs[:8]))
        L.append("")

    # pypiron vs the superset
    me = next((p for p in products if p.get("id") == PINNED_FIRST), None)
    if me:
        gaps_by_cat: list[str] = []
        for cat in taxonomy:
            missing = [
                f["name"]
                for f in cat["features"]
                if feat(me, f["key"]) in ("no", "planned", "unknown")
            ]
            if missing:
                gaps_by_cat.append(f"- **{cat['category']}:** " + ", ".join(missing))
        L.append("## pypiron vs. the superset")
        L.append("")
        cov = coverage_pct(me, all_keys)
        L.append(
            f"pypiron covers **{cov:.0f}%** of the superset by design — it is a deliberately "
            "scoped static-file server, not an everything-registry. Features it does **not** "
            "implement (by category) are the explicit non-goals and the candidate roadmap:"
        )
        L.append("")
        L.extend(gaps_by_cat if gaps_by_cat else ["- (none — full coverage)"])
        L.append("")

    return "\n".join(L) + "\n"


HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>The Python package-server landscape</title>
<style>
:root { --top: 0px; --grp-h: 0px; }
* { box-sizing: border-box; }
body { margin: 0; font: 13px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; color: #1b1f24; background: #fff; }
a { color: #0969da; text-decoration: none; }
a:hover { text-decoration: underline; }
header.page { padding: 14px 16px 4px; }
header.page h1 { font-size: 19px; margin: 0 0 4px; }
header.page p { margin: 2px 0; color: #57606a; font-size: 12px; max-width: 1000px; }
.toolbar { position: sticky; top: 0; z-index: 60; background: #f6f8fa; border-bottom: 1px solid #d0d7de; padding: 8px 12px; display: flex; flex-wrap: wrap; gap: 8px 18px; align-items: center; }
.toolbar .group { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.toolbar .group > b { font-size: 11px; text-transform: uppercase; letter-spacing: .04em; color: #57606a; }
.toolbar label { font-size: 12px; color: #24292f; display: inline-flex; gap: 4px; align-items: center; white-space: nowrap; cursor: pointer; }
.toolbar input[type=search] { padding: 4px 8px; border: 1px solid #d0d7de; border-radius: 6px; min-width: 190px; font-size: 13px; }
.toolbar input[type=range] { vertical-align: middle; }
.toolbar select { padding: 3px 6px; border: 1px solid #d0d7de; border-radius: 6px; font-size: 12px; background: #fff; }
.status { font-size: 12px; color: #57606a; }
.status b { color: #1b1f24; }
button.btn { font-size: 12px; padding: 3px 9px; border: 1px solid #d0d7de; border-radius: 6px; background: #fff; cursor: pointer; }
button.btn:hover { background: #eef1f4; }
.legend { display: flex; gap: 14px; flex-wrap: wrap; font-size: 12px; color: #57606a; padding: 6px 14px; align-items: center; }
.legend .chip { display: inline-flex; gap: 5px; align-items: center; }
.swatch { display: inline-block; width: 16px; height: 16px; border-radius: 3px; border: 1px solid #d0d7de; text-align: center; line-height: 15px; font-weight: 700; font-size: 11px; }
.tablewrap { padding: 0 0 50vh; }
table { border-collapse: separate; border-spacing: 0; font-size: 12px; }
th, td { border-right: 1px solid #eaecef; border-bottom: 1px solid #eaecef; padding: 3px 6px; text-align: left; white-space: nowrap; background: #fff; }
thead th { background: #fff; vertical-align: bottom; }
thead tr.groups th { position: sticky; top: var(--top); z-index: 42; border-bottom: 1px solid #d0d7de; }
thead tr.feats th { position: sticky; top: calc(var(--top) + var(--grp-h)); z-index: 40; border-bottom: 2px solid #d0d7de; }
th.grp { text-align: center; background: #eef1f4; font-weight: 700; cursor: default; font-size: 11px; }
/* bold boundary between feature-category groups (header + body) */
th.grp, th.feat.catstart, td.cell.catstart { border-left: 3px solid #8c959f; }
th.meta { font-weight: 700; background: #fff; }
.name-cell { position: sticky; left: 0; z-index: 30; background: #fff; min-width: 220px; max-width: 270px; }
thead tr.groups th.corner { position: sticky; left: 0; top: var(--top); z-index: 56; }
th.feat { height: 168px; vertical-align: bottom; padding: 4px 1px; width: 24px; min-width: 24px; max-width: 24px; }
th.feat .rot { writing-mode: vertical-rl; transform: rotate(180deg); white-space: nowrap; font-weight: 500; font-size: 11px; display: inline-block; max-height: 156px; overflow: hidden; text-overflow: ellipsis; }
td.cell { text-align: center; width: 24px; min-width: 24px; max-width: 24px; font-weight: 700; cursor: default; }
.truncate { max-width: 150px; overflow: hidden; text-overflow: ellipsis; }
td.num, th.num { text-align: center; }
tbody tr:hover td, tbody tr:hover th.name-cell { background: #f1f8ff; }
.v-yes { background: #d2f7dd; color: #116329; }
.v-partial { background: #fff3c4; color: #8a6200; }
.v-planned { background: #d6ecff; color: #0860ca; }
.v-no { color: #d0d7de; }
.v-unknown { background: #f6f8fa; color: #afb8c1; }
tr.pinned th.name-cell, tr.pinned td:not(.cell) { font-weight: 700; }
tr.pinned .nm { font-weight: 800; }
.name-cell .nm { font-weight: 600; }
.name-cell .vendor { color: #8c959f; font-weight: 400; font-size: 11px; }
.hidebtn, .infobtn { cursor: pointer; border: none; background: none; font-size: 12px; padding: 0 3px; line-height: 1; }
.hidebtn { color: #cf222e; }
.infobtn { color: #57606a; }
tr.detail.hidden { display: none; }
tr.detail td { white-space: normal; background: #fbfcfd; font-size: 12px; padding: 9px 14px 12px; }
tr.detail .dgrid { display: grid; grid-template-columns: max-content 1fr; gap: 2px 12px; max-width: 1100px; }
tr.detail .dgrid dt { color: #57606a; font-weight: 600; }
tr.detail .dgrid dd { margin: 0; }
tr.detail ul { margin: 3px 0; padding-left: 18px; }
tr.excluded td, tr.excluded th.name-cell { font-style: italic; }
.col-hidden { display: none; }
th.sortable { cursor: pointer; user-select: none; }
th.meta.sortable:hover, th.feat.sortable:hover { background: #e3e8ee; }
th.sortcol { background: #fff3c4 !important; }
td.pill { text-align: center; }
</style>
</head>
<body>
<header class="page">
  <h1>The Python package-server landscape</h1>
  <p>Interactive feature census &mdash; one row per product, one column per capability across the full superset. Generated __DATE__ from <code>taxonomy.json</code> + <code>products/*.json</code>. Drag horizontally to see all feature columns; the product column and header stay pinned.</p>
</header>
<div class="toolbar" id="toolbar"></div>
<div class="legend" id="legend"></div>
<div class="tablewrap"><table id="grid"><thead></thead><tbody></tbody></table></div>
<script>
const DATA = __DATA__;
const EXCLUDE = new Set(__EXCLUDE__);
const LS_KEY = "pl-hidden-v1";

const TYPE_ORDER = ["open-source","open-core","tool","commercial","saas","cloud-managed","defunct"];
const TYPE_LABEL = {
  "open-source":"Open source","open-core":"Open core","tool":"Tool / generator",
  "commercial":"Commercial","saas":"SaaS","cloud-managed":"Cloud-managed","defunct":"Defunct"
};
const VAL = {
  yes:{sym:"✓", cls:"v-yes", label:"Supported"},
  partial:{sym:"◐", cls:"v-partial", label:"Partial"},
  planned:{sym:"+", cls:"v-planned", label:"Planned"},
  no:{sym:"·", cls:"v-no", label:"Not supported"},
  unknown:{sym:"?", cls:"v-unknown", label:"Unknown"},
};
const SCORE = {yes:1, partial:0.5, planned:0, no:0, unknown:0};

const taxonomy = DATA.taxonomy;
const allFeatures = taxonomy.flatMap(c => c.features.map(f => ({key:f.key, name:f.name, cat:c.key})));
const allKeys = allFeatures.map(f => f.key);
const stdKeys = (taxonomy.find(c => c.key === "standards") || {features:[]}).features.map(f => f.key);
// first feature of each category — gets the bold group-boundary border
const CATSTART = new Set();
{ let pc = null; for (const f of allFeatures){ if (f.cat !== pc) CATSTART.add(f.key); pc = f.cat; } }
const catStartCls = f => CATSTART.has(f.key) ? " catstart" : "";

function featVal(p, k){ return (p.features || {})[k] || "unknown"; }
function coverage(p){ let t = 0; for (const k of allKeys) t += SCORE[featVal(p,k)] || 0; return allKeys.length ? 100*t/allKeys.length : 0; }
function stdCount(p){ let n = 0; for (const k of stdKeys) if (featVal(p,k) === "yes") n++; return n; }
function fmtStars(n){ if (n == null) return "—"; if (n >= 10000) return Math.round(n/1000) + "k"; if (n >= 1000) return (n/1000).toFixed(1) + "k"; return "" + n; }
function starsTitle(p){ return p.stars == null ? "" : (p.stars.toLocaleString() + " stars · repo last push " + (p.repo_pushed || "?")); }

let hidden = new Set(JSON.parse(localStorage.getItem(LS_KEY) || "[]"));
function saveHidden(){ localStorage.setItem(LS_KEY, JSON.stringify([...hidden])); }

function sortKey(p){
  const pin = p.id === "pypiron" ? 0 : 1;
  let ti = TYPE_ORDER.indexOf(p.type); if (ti < 0) ti = TYPE_ORDER.length;
  return [pin, ti, (p.name || "").toLowerCase()];
}
const products = DATA.products.slice().sort((a,b) => {
  const ka = sortKey(a), kb = sortKey(b);
  for (let i = 0; i < ka.length; i++){ if (ka[i] < kb[i]) return -1; if (ka[i] > kb[i]) return 1; }
  return 0;
});

const META = [
  {key:"type", label:"Type", sort:"type", get:p => TYPE_LABEL[p.type] || p.type || "—"},
  {key:"hosting", label:"Hosting", sort:"hosting", get:p => p.hosting || "—"},
  {key:"language", label:"Impl", sort:"language", get:p => p.language || "—", cls:"truncate", full:p => p.language || ""},
  {key:"license", label:"License", sort:"license", get:p => p.license || "—", cls:"truncate", full:p => p.license || ""},
  {key:"cost", label:"Pricing", sort:"cost", get:p => p.n_cost || "—", cls:"pill", full:p => p.cost || ""},
  {key:"stars", label:"Stars", sort:"stars", get:p => fmtStars(p.stars), cls:"num", full:starsTitle},
  {key:"std", label:"Std", sort:"std", get:p => stdCount(p) + "/" + stdKeys.length, cls:"num"},
  {key:"cov", label:"Cover", sort:"cov", get:p => Math.round(coverage(p)) + "%", cls:"num"},
  {key:"last_updated", label:"Updated", sort:"last_updated", get:p => p.n_updated || "—", cls:"num", full:p => p.last_updated || ""},
];
const TOTAL_COLS = 1 + META.length + allFeatures.length;

const COST_ORDER = {"Free":0, "Freemium":1, "Usage-based":2, "Paid":3, "Quote":4, "Unknown":5};
const VAL_RANK = {yes:4, partial:3, planned:2, no:1, unknown:0};
const SORTERS = {
  name: {dir:1, val:p => (p.name || "").toLowerCase()},
  type: {dir:1, val:p => { const i = TYPE_ORDER.indexOf(p.type); return i < 0 ? 99 : i; }},
  hosting: {dir:1, val:p => p.hosting || ""},
  language: {dir:1, val:p => (p.language || "").toLowerCase()},
  license: {dir:1, val:p => (p.license || "").toLowerCase()},
  cost: {dir:1, val:p => (COST_ORDER[p.n_cost] != null ? COST_ORDER[p.n_cost] : 9)},
  stars: {dir:-1, val:p => (p.stars == null ? -1 : p.stars)},
  std: {dir:-1, val:p => stdCount(p)},
  cov: {dir:-1, val:p => coverage(p)},
  last_updated: {dir:-1, val:p => p.n_updated || ""},
};
for (const f of allFeatures) SORTERS["feat:" + f.key] = {dir:-1, val:p => VAL_RANK[featVal(p, f.key)]};
let sortState = null;       // {id, dir} — null = default (pypiron pinned, then type, name)
const rowMap = {};          // id -> {tr, detail}

function cmp(a, b, id, dir){
  const va = SORTERS[id].val(a), vb = SORTERS[id].val(b);
  let c = va < vb ? -1 : va > vb ? 1 : 0;
  if (c === 0){ const na = (a.name||"").toLowerCase(), nb = (b.name||"").toLowerCase(); c = na < nb ? -1 : na > nb ? 1 : 0; }
  return c * dir;
}
function orderedProducts(){
  if (!sortState) return products;
  return products.slice().sort((a, b) => cmp(a, b, sortState.id, sortState.dir));
}
function layout(){
  const tbody = document.querySelector("#grid tbody");
  for (const p of orderedProducts()){
    const m = rowMap[p.id]; if (!m) continue;
    tbody.appendChild(m.tr); tbody.appendChild(m.detail);
  }
}
function markSort(){
  document.querySelectorAll("thead th[data-sortid]").forEach(th => {
    const id = th.getAttribute("data-sortid");
    const active = !!(sortState && sortState.id === id);
    th.classList.toggle("sortcol", active);
    const base = th.getAttribute("data-base") || "";
    if (th.classList.contains("feat")){
      const span = th.querySelector(".rot");
      if (span) span.textContent = (active ? (sortState.dir > 0 ? "▲ " : "▼ ") : "") + base;
    } else {
      th.textContent = base + (active ? (sortState.dir > 0 ? " ▲" : " ▼") : "");
    }
  });
}
function setSort(id){
  if (!SORTERS[id]) return;
  if (sortState && sortState.id === id) sortState.dir *= -1;
  else sortState = {id, dir: SORTERS[id].dir};
  layout(); markSort(); apply();
}

function el(tag, props, ...kids){
  const e = document.createElement(tag);
  props = props || {};
  for (const k in props){
    const v = props[k];
    if (k === "class") e.className = v;
    else if (k === "text") e.textContent = v;
    else if (k === "title") e.title = v;
    else if (k.slice(0,2) === "on" && typeof v === "function") e.addEventListener(k.slice(2), v);
    else if (v != null) e.setAttribute(k, v);
  }
  for (const c of kids){ if (c == null) continue; e.appendChild(typeof c === "string" ? document.createTextNode(c) : c); }
  return e;
}

function buildHead(){
  const thead = document.querySelector("#grid thead");
  thead.innerHTML = "";
  const r1 = el("tr", {class:"groups"});
  r1.appendChild(el("th", {class:"meta corner name-cell sortable", "data-sortid":"name",
    "data-base":"Product", text:"Product", title:"Click to sort by name", onclick:() => setSort("name")}));
  for (const m of META) r1.appendChild(el("th", {class:"meta sortable", rowspan:"2",
    "data-sortid":m.sort, "data-base":m.label, text:m.label, title:"Click to sort", onclick:() => setSort(m.sort)}));
  for (const cat of taxonomy){
    r1.appendChild(el("th", {class:"grp grp-" + cat.key, colspan:String(cat.features.length), text:cat.category}));
  }
  // corner + meta span both header rows
  r1.firstChild.setAttribute("rowspan", "2");
  thead.appendChild(r1);

  const r2 = el("tr", {class:"feats"});
  for (const f of allFeatures){
    const th = el("th", {class:"feat sortable col-" + f.cat + catStartCls(f), title:f.name + " — click to sort",
      "data-sortid":"feat:" + f.key, "data-base":f.name, onclick:() => setSort("feat:" + f.key)});
    th.appendChild(el("span", {class:"rot", text:f.name}));
    r2.appendChild(th);
  }
  thead.appendChild(r2);
}

function detailRow(p){
  const tr = el("tr", {class:"detail hidden", "data-detail-for":p.id});
  const td = el("td", {colspan:String(TOTAL_COLS)});
  if (p.summary) td.appendChild(el("p", {text:p.summary, style:"margin:0 0 6px;font-style:italic"}));
  const dl = el("dl", {class:"dgrid"});
  const add = (k, v) => { if (!v) return; dl.appendChild(el("dt", {text:k})); dl.appendChild(el("dd", {text:v})); };
  add("Vendor", p.vendor);
  add("Cost", p.cost);
  add("Released / updated", [p.first_released, p.last_updated].filter(Boolean).join("  →  "));
  add("Popularity", starsTitle(p));
  add("Performance", p.perf_notes);
  add("Scale / HA", p.scale_notes);
  add("Confidence", p.confidence);
  td.appendChild(dl);
  if (p.extras && p.extras.length){
    td.appendChild(el("div", {text:"Notable extras:", style:"margin-top:6px;color:#57606a;font-weight:600"}));
    const ul = el("ul");
    for (const e of p.extras) ul.appendChild(el("li", {text:e}));
    td.appendChild(ul);
  }
  const srcs = (p.sources || []).filter(Boolean);
  if (srcs.length){
    const div = el("div", {style:"margin-top:4px"});
    div.appendChild(el("span", {text:"Sources: ", style:"color:#57606a;font-weight:600"}));
    srcs.slice(0,10).forEach((s,i) => {
      if (i) div.appendChild(document.createTextNode(" · "));
      div.appendChild(el("a", {href:s, target:"_blank", rel:"noopener", text:"[" + (i+1) + "]"}));
    });
    td.appendChild(div);
  }
  tr.appendChild(td);
  return tr;
}

function buildBody(){
  const tbody = document.querySelector("#grid tbody");
  tbody.innerHTML = "";
  for (const p of products){
    const isExcluded = EXCLUDE.has(p.id);
    const tr = el("tr", {
      "data-id":p.id, "data-type":p.type || "", "data-hosting":p.hosting || "",
      "data-name":((p.name||"") + " " + (p.vendor||"") + " " + (p.summary||"")).toLowerCase(),
      "data-cov":String(Math.round(coverage(p))), "data-excluded":isExcluded ? "1" : "0",
      "data-updated":p.n_updated || "",
    });
    if (p.id === "pypiron") tr.classList.add("pinned");
    if (isExcluded) tr.classList.add("excluded");

    const name = el("th", {class:"name-cell"});
    name.appendChild(el("button", {class:"hidebtn", title:"Hide this product", text:"✕",
      onclick:() => { hidden.add(p.id); saveHidden(); apply(); }}));
    name.appendChild(el("button", {class:"infobtn", title:"Details", text:"ⓘ",
      onclick:() => { const d = tbody.querySelector('tr.detail[data-detail-for="'+p.id+'"]'); if (d) d.classList.toggle("hidden"); }}));
    const url = p.homepage || p.repo || "";
    const label = url ? el("a", {href:url, target:"_blank", rel:"noopener", class:"nm", text:p.name})
                      : el("span", {class:"nm", text:p.name});
    name.appendChild(document.createTextNode(" "));
    name.appendChild(label);
    tr.appendChild(name);

    for (const m of META){
      const td = el("td", {class:m.cls || "", text:String(m.get(p))});
      if (m.full){ const f = m.full(p); if (f) td.title = f; }
      tr.appendChild(td);
    }
    for (const f of allFeatures){
      const v = featVal(p, f.key);
      const meta = VAL[v] || VAL.unknown;
      tr.appendChild(el("td", {class:"cell col-" + f.cat + catStartCls(f) + " " + meta.cls, title:f.name + ": " + meta.label, text:meta.sym}));
    }
    rowMap[p.id] = {tr, detail: detailRow(p)};
  }
}

function checkedTypes(){
  const s = new Set();
  document.querySelectorAll(".type-cb:checked").forEach(cb => s.add(cb.value));
  return s;
}

function apply(){
  const q = (document.querySelector("#q").value || "").trim().toLowerCase();
  const types = checkedTypes();
  const showExcluded = document.querySelector("#showExcluded").checked;
  const minCov = parseInt(document.querySelector("#minCov").value, 10) || 0;
  document.querySelector("#minCovLabel").textContent = minCov + "%";
  const minDate = document.querySelector("#minDate").value;  // "" or a "YYYY" threshold

  let shown = 0;
  const tbody = document.querySelector("#grid tbody");
  for (const tr of tbody.querySelectorAll("tr[data-id]")){
    const id = tr.getAttribute("data-id");
    const type = tr.getAttribute("data-type");
    const cov = parseInt(tr.getAttribute("data-cov"), 10) || 0;
    const excluded = tr.getAttribute("data-excluded") === "1";
    const name = tr.getAttribute("data-name");
    const updated = tr.getAttribute("data-updated");
    let vis = types.has(type) && cov >= minCov && !hidden.has(id);
    if (excluded && !showExcluded) vis = false;
    if (q && name.indexOf(q) === -1) vis = false;
    // ISO dates compare lexicographically; "" (unknown date) fails any threshold
    if (minDate && !(updated && updated >= minDate)) vis = false;
    tr.style.display = vis ? "" : "none";
    const d = tbody.querySelector('tr.detail[data-detail-for="'+id+'"]');
    if (d){ if (!vis) d.classList.add("hidden"); d.style.display = vis ? "" : "none"; }
    if (vis) shown++;
  }
  const liveHidden = [...hidden].filter(id => products.some(p => p.id === id));
  document.querySelector("#status").innerHTML =
    "Showing <b>" + shown + "</b> of " + products.length + " products" +
    (liveHidden.length ? " · " + liveHidden.length + " manually hidden" : "");
}

function applyColumns(){
  document.querySelectorAll(".cat-cb").forEach(cb => {
    const on = cb.checked;
    document.querySelectorAll(".col-" + cb.value).forEach(c => c.classList.toggle("col-hidden", !on));
  });
}

function buildToolbar(){
  const tb = document.querySelector("#toolbar");
  tb.innerHTML = "";

  const g1 = el("div", {class:"group"});
  g1.appendChild(el("input", {type:"search", id:"q", placeholder:"Search name / vendor / summary…", oninput:apply}));
  tb.appendChild(g1);

  const g2 = el("div", {class:"group"});
  g2.appendChild(el("b", {text:"Type"}));
  for (const t of TYPE_ORDER){
    const lbl = el("label");
    const cb = el("input", {type:"checkbox", class:"type-cb", value:t, onchange:apply});
    if (t !== "defunct") cb.checked = true;
    lbl.appendChild(cb); lbl.appendChild(document.createTextNode(TYPE_LABEL[t]));
    g2.appendChild(lbl);
  }
  tb.appendChild(g2);

  const g3 = el("div", {class:"group"});
  const exLbl = el("label", {title:"Reveal products marked as non-contenders in exclude.json"});
  exLbl.appendChild(el("input", {type:"checkbox", id:"showExcluded", onchange:apply}));
  exLbl.appendChild(document.createTextNode("Show non-contenders (" + [...EXCLUDE].length + ")"));
  g3.appendChild(exLbl);
  tb.appendChild(g3);

  const g4 = el("div", {class:"group"});
  g4.appendChild(el("b", {text:"Min coverage"}));
  g4.appendChild(el("input", {type:"range", id:"minCov", min:"0", max:"100", step:"5", value:"0", oninput:apply}));
  g4.appendChild(el("span", {id:"minCovLabel", class:"status", text:"0%"}));
  tb.appendChild(g4);

  const g4b = el("div", {class:"group"});
  g4b.appendChild(el("b", {text:"Updated since"}));
  const sel = el("select", {id:"minDate", onchange:apply, title:"Hide solutions whose latest known update predates this"});
  sel.appendChild(el("option", {value:"", text:"Any"}));
  for (const y of ["2026","2025","2024","2023","2022","2020","2015"]) sel.appendChild(el("option", {value:y, text:y}));
  g4b.appendChild(sel);
  tb.appendChild(g4b);

  const g5 = el("div", {class:"group"});
  g5.appendChild(el("b", {text:"Columns"}));
  for (const cat of taxonomy){
    const lbl = el("label", {title:cat.category});
    const cb = el("input", {type:"checkbox", class:"cat-cb", value:cat.key, checked:"checked",
      onchange:applyColumns});
    cb.checked = true;
    lbl.appendChild(cb); lbl.appendChild(document.createTextNode(cat.category.replace(/ .*/, "")));
    g5.appendChild(lbl);
  }
  tb.appendChild(g5);

  const g6 = el("div", {class:"group"});
  g6.appendChild(el("button", {class:"btn", text:"Reset filters", onclick:() => {
    document.querySelector("#q").value = "";
    document.querySelectorAll(".type-cb").forEach(cb => cb.checked = cb.value !== "defunct");
    document.querySelector("#showExcluded").checked = false;
    document.querySelector("#minCov").value = "0";
    document.querySelector("#minDate").value = "";
    document.querySelectorAll(".cat-cb").forEach(cb => cb.checked = true);
    sortState = null; layout(); markSort();
    applyColumns(); apply();
  }}));
  g6.appendChild(el("button", {class:"btn", text:"Unhide all", onclick:() => { hidden.clear(); saveHidden(); apply(); }}));
  g6.appendChild(el("button", {class:"btn", title:"Copy the manually-hidden IDs so they can be baked into exclude.json", text:"Copy hidden IDs", onclick:() => {
    const ids = [...hidden].filter(id => products.some(p => p.id === id));
    const txt = JSON.stringify(ids);
    navigator.clipboard && navigator.clipboard.writeText(txt);
    window.prompt("Manually-hidden IDs (paste into exclude.json → hidden_by_default):", txt);
  }}));
  tb.appendChild(g6);

  tb.appendChild(el("div", {class:"group"}, el("span", {id:"status", class:"status"})));
}

function buildLegend(){
  const lg = document.querySelector("#legend");
  lg.innerHTML = "";
  for (const k of ["yes","partial","planned","no","unknown"]){
    const v = VAL[k];
    const chip = el("span", {class:"chip"});
    chip.appendChild(el("span", {class:"swatch " + v.cls, text:v.sym}));
    chip.appendChild(document.createTextNode(v.label));
    lg.appendChild(chip);
  }
  lg.appendChild(el("span", {class:"status", text:"— click ⓘ for details, ✕ to hide a product (persists locally)."}));
}

function measure(){
  const tb = document.querySelector("#toolbar");
  document.documentElement.style.setProperty("--top", (tb ? tb.offsetHeight : 0) + "px");
  const grp = document.querySelector("thead tr.groups");
  document.documentElement.style.setProperty("--grp-h", (grp ? grp.offsetHeight : 0) + "px");
}

buildHead();
buildBody();
layout();
buildToolbar();
buildLegend();
applyColumns();
markSort();
apply();
measure();
window.addEventListener("resize", measure);
</script>
</body>
</html>
"""


def build_html(data: dict[str, Any], excluded: list[str], date: str) -> str:
    # Attach normalized, sortable fields without mutating the source records.
    enriched = dict(data)
    enriched["products"] = [
        {
            **p,
            "n_updated": norm_updated(p.get("last_updated", "")),
            "n_cost": cost_model(p.get("cost", "")),
        }
        for p in data["products"]
    ]
    payload = json.dumps(enriched, ensure_ascii=False)
    # Make it safe to embed inside <script>: escape the only chars that can break out.
    payload = payload.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    return (
        HTML_TEMPLATE.replace("__DATA__", payload)
        .replace("__EXCLUDE__", json.dumps(excluded))
        .replace("__DATE__", date)
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compile the landscape data into markdown."
    )
    parser.add_argument(
        "--date",
        default=datetime.date.today().isoformat(),
        help="Generation date stamp.",
    )
    args = parser.parse_args()

    taxonomy = load_taxonomy()
    products = load_products()
    if not products:
        print("WARNING: no product records found in products/*.json", file=sys.stderr)

    # Scaffold the honing list on first run so editing it is obvious.
    exclude_path = HERE / "exclude.json"
    if not exclude_path.exists():
        exclude_path.write_text(
            json.dumps(
                {
                    "$comment": "Product ids listed here are hidden by default in LANDSCAPE.html "
                    "(non-serious contenders). The HTML 'Show non-contenders' toggle reveals them. "
                    "Hone by adding ids here (use the HTML 'Copy hidden IDs' button), then recompile.",
                    "hidden_by_default": [],
                },
                indent=2,
            )
            + "\n"
        )
    excluded = load_excludes()

    markdown = build(products, taxonomy, args.date)
    (HERE / "LANDSCAPE.md").write_text(markdown)

    merged = {
        "generated": args.date,
        "taxonomy": taxonomy,
        "products": sorted(products, key=sort_key),
    }
    (HERE / "data.json").write_text(
        json.dumps(merged, indent=2, ensure_ascii=False) + "\n"
    )

    html = build_html(merged, excluded, args.date)
    (HERE / "LANDSCAPE.html").write_text(html)

    print(
        f"Wrote LANDSCAPE.md ({len(markdown.splitlines())} lines), "
        f"LANDSCAPE.html ({len(html.splitlines())} lines), and data.json "
        f"from {len(products)} products ({len(excluded)} excluded by default)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
