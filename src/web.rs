//! Human-facing HTML: the root landing page (`/`) — config card with a live
//! activity panel folded in for authorized readers — plus the package browser
//! (`/projects/`) and per-project pages. Self-contained — inline CSS, an
//! embedded base64 logo, and a few lines of copy-button JS. No external
//! requests, no framework, no new
//! dependency. The PEP simple-API rendering lives in [`crate::render`]; this is
//! the front door a human sees in a browser.
//!
//! Everything here is a pure `&str -> String` function so it unit-tests without
//! a running server. Anything derived from the request (the index URL is built
//! from the `Host`/`X-Forwarded-*` headers) is HTML-escaped: the page reflects
//! a client-controlled header, so it must never let it break out of its text.

use std::sync::OnceLock;

use base64::Engine;
use html_escape::{encode_double_quoted_attribute, encode_text};

use crate::coremeta::CoreMetadata;
use crate::metrics::{Inventory, MetricsSnapshot};
use crate::render::FileMetadata;
use crate::sidecar::Yanked;

/// Downscaled logo (110×128, ~26 KB), embedded so the page makes zero external
/// requests. The full-resolution asset stays in `docs/`.
const LOGO_PNG: &[u8] = include_bytes!("../assets/pypiron-logo-128.png");

/// The logo as a `data:` URI, base64-encoded once on first use.
fn logo_data_uri() -> &'static str {
    static URI: OnceLock<String> = OnceLock::new();
    URI.get_or_init(|| {
        let b64 = base64::engine::general_purpose::STANDARD.encode(LOGO_PNG);
        format!("data:image/png;base64,{b64}")
    })
}

/// Request-derived context shared by both pages.
pub struct PageContext {
    /// `scheme://host`, no trailing slash, built from the request headers.
    pub base_url: String,
    pub version: &'static str,
    pub proxy_enabled: bool,
    /// Artifact delivery mode label (`auto`/`redirect`/`stream`).
    pub delivery: &'static str,
    /// Whether a read credential is configured (reads are gated).
    pub reads_authenticated: bool,
}

/// Live counters for the homepage's activity panel. Only built (and only
/// rendered) for an authorized reader, so the public front door never leaks
/// traffic stats or client project-tag names.
pub struct DashboardData<'a> {
    pub snapshot: &'a MetricsSnapshot,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub packages_hosted: usize,
}

const PAGE_CSS: &str = "\
:root{color-scheme:light dark;--bg:#fafafa;--fg:#15171a;--muted:#6b7280;--card:#fff;--border:#e6e7eb;--accent:#3b6cff;--code:#f3f4f6;--bar:#3b6cff;--track:#e9ecf3}\
@media(prefers-color-scheme:dark){:root{--bg:#0e1014;--fg:#e7e9ee;--muted:#9aa1ac;--card:#161922;--border:#252a35;--accent:#7aa0ff;--code:#1a1e27;--bar:#7aa0ff;--track:#222733}}\
*{box-sizing:border-box}\
body{margin:0;min-height:100vh;background:var(--bg);color:var(--fg);font:15px/1.55 ui-sans-serif,system-ui,-apple-system,\"Segoe UI\",Roboto,sans-serif;display:flex;justify-content:center}\
main{width:100%;max-width:720px;padding:52px 24px 72px}\
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}\
.hero{text-align:center;margin-bottom:36px}\
.logo{height:104px;width:auto}\
.hero.compact .logo{height:56px}\
h1{margin:14px 0 4px;font-size:30px;letter-spacing:-.02em}\
.tag{margin:0;color:var(--muted)}\
.inv{display:flex;flex-wrap:wrap;justify-content:center;gap:14px 30px;margin:0 0 30px;color:var(--muted);font-size:14px}\
.inv b{color:var(--fg);font-size:21px;font-weight:650;margin-right:7px;letter-spacing:-.01em}\
.snip{margin:14px 0}\
.snip-h{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px}\
.snip-label{font-size:12px;font-weight:600;letter-spacing:.03em;text-transform:uppercase;color:var(--muted)}\
.copy{font:inherit;font-size:12px;cursor:pointer;border:1px solid var(--border);background:var(--card);color:var(--muted);border-radius:6px;padding:2px 10px}\
.copy:hover{color:var(--fg)}\
.copy.ok{color:#16a34a;border-color:#16a34a}\
pre{margin:0;background:var(--code);border:1px solid var(--border);border-radius:8px;padding:12px 14px;overflow-x:auto}\
code{font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}\
.caps{margin:28px 0 0;color:var(--muted);font-size:14px;text-align:center}\
.caps b{color:var(--fg);font-weight:600}\
.note{margin:14px 0 0;padding:10px 14px;background:var(--code);border:1px solid var(--border);border-radius:8px;font-size:13px;color:var(--muted)}\
.note code{color:var(--fg)}\
.links{margin-top:30px;text-align:center;color:var(--muted)}\
.ver{margin-top:8px;text-align:center;color:var(--muted);font-size:13px}\
.stats{display:grid;grid-template-columns:repeat(2,1fr);gap:14px;margin:8px 0 30px}\
@media(min-width:560px){.stats{grid-template-columns:repeat(4,1fr)}}\
.stat{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:16px}\
.stat .num{font-size:26px;font-weight:650;letter-spacing:-.01em}\
.stat .lbl{margin-top:2px;color:var(--muted);font-size:12px}\
.chart{margin:22px 0}\
.chart h2{font-size:14px;text-transform:uppercase;letter-spacing:.03em;color:var(--muted);margin:0 0 6px}\
.bars{display:block;font-family:ui-sans-serif,system-ui,sans-serif}\
.bars .bl{fill:var(--fg);font-size:13px}\
.bars .bv{fill:var(--muted);font-size:13px}\
.bars .track{fill:var(--track)}\
.bars .bar{fill:var(--bar)}\
.empty{color:var(--muted);font-size:14px;font-style:italic}\
.activity{margin-top:44px;border-top:1px solid var(--border);padding-top:24px}\
.activity .cap{margin:0 0 16px;color:var(--muted);font-size:13px;text-align:center}\
main.wide{max-width:980px}\
.summary{margin:6px 0 0;color:var(--muted);font-size:16px}\
.pcols{display:grid;gap:32px;margin-top:8px}\
@media(min-width:760px){.pcols{grid-template-columns:minmax(0,1fr) 260px}}\
.pmeta h2,.files h2,.readme-sec .snip-label{font-size:12px;font-weight:600;letter-spacing:.03em;text-transform:uppercase;color:var(--muted)}\
.pmeta>div{margin-bottom:18px}\
.pmeta h2{margin:0 0 5px}\
.pmeta .vals{list-style:none;margin:0;padding:0}\
.pmeta .vals li{margin:2px 0;word-break:break-word}\
.pmeta .pill{font-size:13px}\
.readme{white-space:pre-wrap;word-wrap:break-word;max-height:560px;overflow:auto}\
.readme-sec{margin:0 0 26px}\
.files h2{margin:0 0 8px}\
table.files-t{width:100%;border-collapse:collapse;font-size:13px}\
table.files-t th{text-align:left;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border);padding:6px 8px}\
table.files-t td{border-bottom:1px solid var(--border);padding:6px 8px;vertical-align:top}\
.yank{color:#b91c1c;font-weight:600}\
.muted-s{color:var(--muted);font-size:13px}\
.filter{width:100%;font:inherit;padding:9px 12px;margin:0 0 18px;background:var(--card);color:var(--fg);border:1px solid var(--border);border-radius:8px}\
.pkglist{list-style:none;margin:0;padding:0;columns:3 200px;column-gap:24px}\
.pkglist li{break-inside:avoid;padding:3px 0}";

/// Copy-to-clipboard wiring for the landing page's snippet blocks. Tiny and
/// dependency-free; the page is fully readable without it.
const COPY_JS: &str = "<script>\
document.querySelectorAll('.copy').forEach(function(b){b.addEventListener('click',function(){\
var c=b.closest('.snip').querySelector('code').innerText;\
navigator.clipboard.writeText(c).then(function(){var o=b.textContent;b.textContent='Copied';b.classList.add('ok');\
setTimeout(function(){b.textContent=o;b.classList.remove('ok')},1200)})})});\
</script>";

/// Live filter for the package browser. Progressive enhancement — the full list
/// is server-rendered, so the page works fine with JS disabled.
const FILTER_JS: &str = "<script>\
(function(){var f=document.querySelector('.filter');if(!f)return;\
var items=Array.prototype.slice.call(document.querySelectorAll('.pkglist>li'));\
f.addEventListener('input',function(){var q=f.value.toLowerCase();\
items.forEach(function(li){li.style.display=li.textContent.toLowerCase().indexOf(q)>=0?'':'none'})})})();\
</script>";

/// Wrap a page body in the shared document shell. `wide` widens the content
/// column for the two-pane project page.
fn shell(title: &str, body: &str, copy_js: bool, wide: bool) -> String {
    format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
<title>{title}</title><style>{PAGE_CSS}</style></head><body><main{cls}>{body}</main>{js}</body></html>",
        title = encode_text(title),
        cls = if wide { " class=\"wide\"" } else { "" },
        js = if copy_js { COPY_JS } else { "" },
    )
}

/// A labelled, copyable command block. `code` is escaped as text — it carries
/// the request-derived base URL.
fn snippet(label: &str, code: &str) -> String {
    format!(
        "<div class=\"snip\"><div class=\"snip-h\"><span class=\"snip-label\">{label}</span>\
<button class=\"copy\" type=\"button\">Copy</button></div><pre><code>{code}</code></pre></div>",
        label = encode_text(label),
        code = encode_text(code),
    )
}

/// The registry-size row shown under the header: projects · releases · files.
/// Public (counts only, no names), measured by the last sweep.
fn inventory_row(inv: &Inventory) -> String {
    format!(
        "<section class=\"inv\">\
<span><b>{projects}</b> projects</span>\
<span><b>{releases}</b> releases</span>\
<span><b>{files}</b> files</span></section>",
        projects = group_thousands(inv.projects),
        releases = group_thousands(inv.releases),
        files = group_thousands(inv.files),
    )
}

/// The root landing page: what this server is, plus the one index URL a client
/// needs. `inventory` (registry size, public) renders under the header when a
/// sweep has measured it. When `dash` is present (the viewer is an authorized
/// reader) the live activity panel is rendered inline below; otherwise the
/// front door stays a public config card with no traffic stats.
pub fn landing_html(
    ctx: &PageContext,
    inventory: Option<&Inventory>,
    dash: Option<&DashboardData>,
) -> String {
    let index_url = format!("{}/simple/", ctx.base_url);
    let snippet = snippet("Index URL", &index_url);

    let inv = inventory.map(inventory_row).unwrap_or_default();

    let caps = format!(
        "<p class=\"caps\">proxy <b>{proxy}</b> · delivery <b>{delivery}</b> · reads <b>{reads}</b></p>",
        proxy = if ctx.proxy_enabled { "on" } else { "off" },
        delivery = encode_text(ctx.delivery),
        reads = if ctx.reads_authenticated {
            "authenticated"
        } else {
            "public"
        },
    );
    let note = if ctx.reads_authenticated {
        "<p class=\"note\">🔒 Reads require a credential — embed it in the index URL \
(<code>https://user:pass@host/simple/</code>) or your client's keyring.</p>"
    } else {
        ""
    };

    let activity = dash.map(metrics_section).unwrap_or_default();

    let body = format!(
        "<header class=\"hero\"><img class=\"logo\" src=\"{logo}\" width=\"110\" height=\"128\" \
alt=\"pypiron logo\"><h1>pypiron</h1><p class=\"tag\">An ultra-fast PyPI server written in \
Rust.</p></header>{inv}{snippet}{caps}{note}{activity}\
<nav class=\"links\"><a href=\"/projects/\">Browse packages</a> · <a href=\"/health\">Health</a> \
· <a href=\"/metrics\">Metrics</a></nav>\
<p class=\"ver\">pypiron v{version}</p>",
        logo = logo_data_uri(),
        version = encode_text(ctx.version),
    );
    shell("pypiron", &body, true, false)
}

/// The human package browser (`/projects/`): every hosted package, linked to
/// its project page, with a client-side filter box. `packages` must be sorted.
pub fn projects_html(ctx: &PageContext, packages: &[String]) -> String {
    let count = packages.len();
    let list = if packages.is_empty() {
        "<p class=\"empty\">No packages hosted yet.</p>".to_string()
    } else {
        let items: String = packages
            .iter()
            .map(|p| {
                format!(
                    "<li><a href=\"/project/{href}/\">{name}</a></li>",
                    href = encode_double_quoted_attribute(p),
                    name = encode_text(p),
                )
            })
            .collect();
        format!(
            "<input class=\"filter\" type=\"search\" placeholder=\"Filter packages…\" \
aria-label=\"Filter packages\" autocomplete=\"off\"><ul class=\"pkglist\">{items}</ul>"
        )
    };

    let body = format!(
        "<header class=\"hero compact\"><img class=\"logo\" src=\"{logo}\" width=\"110\" height=\"128\" \
alt=\"pypiron logo\"><h1>Packages</h1><p class=\"tag\">{count} hosted</p></header>{list}\
<nav class=\"links\"><a href=\"/\">← Home</a> · <a href=\"/simple/\">Simple index</a></nav>\
<p class=\"ver\">pypiron v{version}</p>{FILTER_JS}",
        logo = logo_data_uri(),
        version = encode_text(ctx.version),
    );
    shell("pypiron · packages", &body, false, false)
}

/// A human-readable project page modelled on pypi.org: install snippets, the
/// README shown **verbatim** (never rendered — that's a separate opt-in), a
/// metadata sidebar, and the list of release files. `files` is the package's
/// artifacts (any order — sorted here, newest first); `meta` is the parsed
/// core metadata of the representative file, absent for legacy/minimal packages.
pub fn project_html(
    ctx: &PageContext,
    pkg: &str,
    files: &[FileMetadata],
    meta: Option<&CoreMetadata>,
) -> String {
    let index_url = format!("{}/simple/", ctx.base_url);
    let install = [
        snippet(
            "uv",
            &format!("uv pip install --index-url {index_url} {pkg}"),
        ),
        snippet("pip", &format!("pip install --index-url {index_url} {pkg}")),
    ]
    .concat();

    let version = meta
        .and_then(|m| m.version.as_deref())
        .or_else(|| files.iter().find_map(|f| f.version.as_deref()))
        .unwrap_or("");
    let summary = meta
        .and_then(|m| m.summary.as_deref())
        .map(|s| format!("<p class=\"summary\">{}</p>", encode_text(s)))
        .unwrap_or_default();

    let readme = match meta.and_then(|m| m.description.as_deref()) {
        Some(desc) if !desc.trim().is_empty() => format!(
            "<section class=\"readme-sec\"><div class=\"snip-h\">\
<span class=\"snip-label\">Readme</span><span class=\"muted-s\">shown unrendered</span></div>\
<pre class=\"readme\"><code>{}</code></pre></section>",
            encode_text(desc),
        ),
        _ => String::new(),
    };

    let body = format!(
        "<header class=\"hero compact\"><img class=\"logo\" src=\"{logo}\" width=\"110\" height=\"128\" \
alt=\"pypiron logo\"><h1>{name}{ver}</h1>{summary}</header>\
<div class=\"pcols\"><div class=\"pcontent\">{install}{readme}{files_table}</div>\
<aside class=\"pmeta\">{sidebar}</aside></div>\
<nav class=\"links\"><a href=\"/\">← Home</a> · <a href=\"/projects/\">All packages</a> · \
<a href=\"/simple/{name}/\">Simple index</a></nav>\
<p class=\"ver\">pypiron v{appver}</p>",
        logo = logo_data_uri(),
        name = encode_text(pkg),
        ver = if version.is_empty() {
            String::new()
        } else {
            format!(" <span class=\"summary\">{}</span>", encode_text(version))
        },
        files_table = files_table(pkg, files),
        sidebar = sidebar(meta),
        appver = encode_text(ctx.version),
    );
    shell(&format!("{pkg} · pypiron"), &body, true, true)
}

/// The release-files table: filename (download link), version, size, upload
/// date, and a yank badge. Newest upload first.
fn files_table(pkg: &str, files: &[FileMetadata]) -> String {
    if files.is_empty() {
        return "<section class=\"files\"><h2>Files</h2><p class=\"empty\">No files.</p></section>"
            .to_string();
    }
    let mut sorted: Vec<&FileMetadata> = files.iter().collect();
    // Newest upload first; ties broken by filename for deterministic output.
    sorted.sort_by(|a, b| {
        b.upload_time
            .cmp(&a.upload_time)
            .then_with(|| a.filename.cmp(&b.filename))
    });
    let mut rows = String::new();
    for f in sorted {
        let yank = match &f.yanked {
            Yanked::Flag(false) => String::new(),
            Yanked::Flag(true) => " <span class=\"yank\">yanked</span>".to_string(),
            Yanked::Reason(r) => {
                format!(" <span class=\"yank\">yanked: {}</span>", encode_text(r))
            }
        };
        rows.push_str(&format!(
            "<tr><td><a href=\"/files/{pkg}/{href}\">{name}</a>{yank}</td>\
<td>{ver}</td><td>{size}</td><td>{when}</td></tr>",
            pkg = encode_double_quoted_attribute(pkg),
            href = encode_double_quoted_attribute(&f.filename),
            name = encode_text(&f.filename),
            ver = encode_text(f.version.as_deref().unwrap_or("")),
            size = human_size(f.size),
            when = encode_text(f.upload_time.as_deref().map(date_only).unwrap_or("")),
        ));
    }
    format!(
        "<section class=\"files\"><h2>Files</h2><table class=\"files-t\">\
<thead><tr><th>File</th><th>Version</th><th>Size</th><th>Uploaded</th></tr></thead>\
<tbody>{rows}</tbody></table></section>",
    )
}

/// The metadata sidebar. Returns a note when there's no metadata to show.
fn sidebar(meta: Option<&CoreMetadata>) -> String {
    let Some(m) = meta else {
        return "<p class=\"muted-s\">No metadata available for this package.</p>".to_string();
    };
    let mut out = String::new();

    let links: String = m
        .project_urls
        .iter()
        .filter_map(|(label, url)| {
            safe_href(url).map(|href| {
                format!(
                    "<li><a href=\"{href}\" rel=\"nofollow noopener noreferrer\">{label}</a></li>",
                    href = encode_double_quoted_attribute(href),
                    label = encode_text(label),
                )
            })
        })
        .collect();
    if !links.is_empty() {
        out.push_str(&format!(
            "<div><h2>Links</h2><ul class=\"vals\">{links}</ul></div>"
        ));
    }

    meta_block(&mut out, "License", m.license.as_deref());
    meta_block(&mut out, "Requires Python", m.requires_python.as_deref());
    let author = match (m.author.as_deref(), m.author_email.as_deref()) {
        (Some(a), Some(e)) => Some(format!("{a} <{e}>")),
        (Some(a), None) => Some(a.to_string()),
        (None, Some(e)) => Some(e.to_string()),
        (None, None) => None,
    };
    meta_block(&mut out, "Author", author.as_deref());
    meta_block(&mut out, "Keywords", m.keywords.as_deref());

    if !m.requires_dist.is_empty() {
        out.push_str(&list_block("Dependencies", &m.requires_dist));
    }
    if !m.classifiers.is_empty() {
        out.push_str(&list_block("Classifiers", &m.classifiers));
    }

    if out.is_empty() {
        return "<p class=\"muted-s\">No metadata available for this package.</p>".to_string();
    }
    out
}

/// A single labelled sidebar value, omitted when absent.
fn meta_block(out: &mut String, label: &str, value: Option<&str>) {
    if let Some(v) = value.map(str::trim).filter(|v| !v.is_empty()) {
        out.push_str(&format!(
            "<div><h2>{label}</h2><div class=\"pill\">{value}</div></div>",
            label = encode_text(label),
            value = encode_text(v),
        ));
    }
}

/// A labelled list of values (dependencies, classifiers).
fn list_block(label: &str, values: &[String]) -> String {
    let items: String = values
        .iter()
        .map(|v| format!("<li>{}</li>", encode_text(v)))
        .collect();
    format!(
        "<div><h2>{label}</h2><ul class=\"vals pill\">{items}</ul></div>",
        label = encode_text(label),
    )
}

/// Allow only `http`/`https` URLs into an `href` — author-controlled metadata
/// must never smuggle in `javascript:` or `data:` schemes.
fn safe_href(url: &str) -> Option<&str> {
    let lower = url.trim_start();
    let scheme_ok = lower.len() >= 7
        && (lower[..7].eq_ignore_ascii_case("http://")
            || (lower.len() >= 8 && lower[..8].eq_ignore_ascii_case("https://")));
    scheme_ok.then_some(url.trim())
}

/// `2026-06-19T12:34:56Z` -> `2026-06-19`. Anything else passes through.
fn date_only(ts: &str) -> &str {
    ts.split_once('T').map(|(d, _)| d).unwrap_or(ts)
}

/// Bytes as a compact human size (`12.3 MB`). Boring, no dependency.
fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

/// The homepage activity panel: glanceable counters and two bar charts, shown
/// inline on `/` to authorized readers. Numbers are this node's, since process
/// start — said plainly so nobody reads them as cluster-wide.
fn metrics_section(d: &DashboardData) -> String {
    let snap = d.snapshot;
    let total = snap.total_requests();
    let files_served = snap.files_served();
    let cache_total = d.cache_hits + d.cache_misses;
    let hit_rate = if cache_total == 0 {
        "—".to_string()
    } else {
        format!("{:.0}%", 100.0 * d.cache_hits as f64 / cache_total as f64)
    };

    let stats = format!(
        "<section class=\"stats\">\
<div class=\"stat\"><div class=\"num\">{total}</div><div class=\"lbl\">Total requests</div></div>\
<div class=\"stat\"><div class=\"num\">{files_served}</div><div class=\"lbl\">Files served</div></div>\
<div class=\"stat\"><div class=\"num\">{hit_rate}</div><div class=\"lbl\">Index cache hit rate</div></div>\
<div class=\"stat\"><div class=\"num\">{packages}</div><div class=\"lbl\">Packages hosted</div></div>\
</section>",
        total = group_thousands(total),
        files_served = group_thousands(files_served),
        packages = group_thousands(d.packages_hosted as u64),
    );

    let mut projects = snap.project_totals();
    projects.retain(|(_, v)| *v > 0);
    projects.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    projects.truncate(10);
    let projects_chart = svg_bar_chart(
        &projects,
        "No per-project traffic yet — requests attribute to a project only when \
they carry a basic-auth username tag.",
    );

    let mut routes = snap.route_totals();
    routes.retain(|(_, v)| *v > 0);
    routes.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    let routes: Vec<(String, u64)> = routes
        .into_iter()
        .map(|(n, v)| (n.to_string(), v))
        .collect();
    let routes_chart = svg_bar_chart(&routes, "No requests recorded yet.");

    format!(
        "<section class=\"activity\"><p class=\"cap\">live activity · this node · since process start</p>\
{stats}\
<section class=\"chart\"><h2>Top projects</h2>{projects_chart}</section>\
<section class=\"chart\"><h2>Top route groups</h2>{routes_chart}</section></section>",
    )
}

// Horizontal bar chart geometry (SVG user units; the chart scales to width).
const CHART_W: u32 = 680;
const BAR_X: u32 = 158;
const ROW_H: u32 = 30;
const BAR_H: u32 = 18;
const VALUE_GAP: u32 = 8;
const MAX_BAR: u32 = CHART_W - BAR_X - 80;

/// A self-contained inline-SVG horizontal bar chart. `items` must be pre-sorted
/// and non-empty for a chart; an empty slice renders `empty_msg` instead.
fn svg_bar_chart(items: &[(String, u64)], empty_msg: &str) -> String {
    if items.is_empty() {
        return format!("<p class=\"empty\">{}</p>", encode_text(empty_msg));
    }
    let max = items.iter().map(|(_, v)| *v).max().unwrap_or(1).max(1);
    let height = ROW_H * items.len() as u32;
    let mut rows = String::new();
    for (i, (name, value)) in items.iter().enumerate() {
        let y0 = ROW_H * i as u32;
        let cy = y0 + ROW_H / 2;
        let bar_y = y0 + (ROW_H - BAR_H) / 2;
        // Proportional width; give any nonzero value at least a sliver.
        let mut bar_w = (*value as f64 / max as f64 * MAX_BAR as f64).round() as u32;
        if *value > 0 {
            bar_w = bar_w.max(2);
        }
        let value_x = BAR_X + MAX_BAR + VALUE_GAP;
        rows.push_str(&format!(
            "<text class=\"bl\" x=\"0\" y=\"{cy}\" dominant-baseline=\"middle\">{label}</text>\
<rect class=\"track\" x=\"{BAR_X}\" y=\"{bar_y}\" width=\"{MAX_BAR}\" height=\"{BAR_H}\" rx=\"4\"/>\
<rect class=\"bar\" x=\"{BAR_X}\" y=\"{bar_y}\" width=\"{bar_w}\" height=\"{BAR_H}\" rx=\"4\"/>\
<text class=\"bv\" x=\"{value_x}\" y=\"{cy}\" dominant-baseline=\"middle\">{value_txt}</text>",
            label = encode_text(&truncate(name, 18)),
            value_txt = group_thousands(*value),
        ));
    }
    format!(
        "<svg class=\"bars\" viewBox=\"0 0 {CHART_W} {height}\" width=\"100%\" height=\"{height}\" \
role=\"img\" preserveAspectRatio=\"xMinYMin meet\">{rows}</svg>",
    )
}

/// Truncate to `max` characters on a char boundary, appending `…` if cut.
fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() > max {
        let mut t: String = s.chars().take(max.saturating_sub(1)).collect();
        t.push('…');
        t
    } else {
        s.to_string()
    }
}

/// `1234567` -> `1,234,567`. ASCII digits only; no locale machinery.
fn group_thousands(n: u64) -> String {
    let digits = n.to_string();
    let len = digits.len();
    let mut out = String::with_capacity(len + len / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (len - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;

    fn ctx() -> PageContext {
        PageContext {
            base_url: "https://pkgs.example.com".to_string(),
            version: "9.9.9",
            proxy_enabled: true,
            delivery: "stream",
            reads_authenticated: false,
        }
    }

    #[test]
    fn landing_shows_index_url_logo_and_tagline_only() {
        let html = landing_html(&ctx(), None, None);
        assert!(html.contains("data:image/png;base64,"));
        assert!(html.contains("An ultra-fast PyPI server written in Rust."));
        // The one index URL is offered (with a copy button)...
        assert!(html.contains("https://pkgs.example.com/simple/"));
        assert!(html.contains("navigator.clipboard.writeText"));
        // ...and the per-client command boxes are gone.
        assert!(!html.contains("uv pip install"));
        assert!(!html.contains("poetry source add"));
        assert!(!html.contains("twine upload"));
        assert!(html.contains("proxy <b>on</b>"));
        assert!(html.contains("reads <b>public</b>"));
        // No inventory and no dashboard data -> neither panel.
        assert!(!html.contains("class=\"inv\""));
        assert!(!html.contains("class=\"activity\""));
        assert!(!html.contains("Top projects"));
    }

    #[test]
    fn landing_escapes_a_hostile_host_header() {
        let mut c = ctx();
        c.base_url = "https://x\"><script>alert(1)</script>".to_string();
        let html = landing_html(&c, None, None);
        assert!(!html.contains("<script>alert(1)"));
        assert!(html.contains("&lt;script&gt;alert(1)"));
    }

    #[test]
    fn landing_renders_inventory_row_under_header() {
        let inv = Inventory {
            projects: 1234,
            releases: 56789,
            files: 90123,
        };
        let html = landing_html(&ctx(), Some(&inv), None);
        assert!(html.contains("class=\"inv\""));
        assert!(html.contains("<b>1,234</b> projects"));
        assert!(html.contains("<b>56,789</b> releases"));
        assert!(html.contains("<b>90,123</b> files"));
    }

    #[test]
    fn landing_notes_auth_when_reads_gated() {
        let mut c = ctx();
        c.reads_authenticated = true;
        let html = landing_html(&c, None, None);
        assert!(html.contains("reads <b>authenticated</b>"));
        assert!(html.contains("Reads require a credential"));
    }

    #[test]
    fn landing_with_activity_renders_numbers_and_bars() {
        let m = Metrics::new();
        m.record_request(crate::metrics::route_group("/simple/"), 200);
        m.record_request(crate::metrics::route_group("/files/six/six.whl"), 200);
        m.record_request(crate::metrics::route_group("/files/six/six.whl"), 200);
        m.record_project("billing-api", crate::metrics::route_group("/files/x"));
        let snap = m.snapshot();
        let dash = DashboardData {
            snapshot: &snap,
            cache_hits: 9,
            cache_misses: 1,
            packages_hosted: 1234,
        };
        let html = landing_html(&ctx(), None, Some(&dash));
        // Index URL still present...
        assert!(html.contains("https://pkgs.example.com/simple/"));
        // ...with the activity panel folded in.
        assert!(html.contains("class=\"activity\""));
        assert!(html.contains("Total requests"));
        assert!(html.contains("Packages hosted"));
        assert!(html.contains("1,234")); // grouped package count
        assert!(html.contains("90%")); // cache hit rate 9/(9+1)
        assert!(html.contains("billing-api"));
        assert!(html.contains("<svg")); // inline SVG charts
    }

    #[test]
    fn activity_hit_rate_is_dash_when_no_cache_traffic() {
        let snap = Metrics::new().snapshot();
        let dash = DashboardData {
            snapshot: &snap,
            cache_hits: 0,
            cache_misses: 0,
            packages_hosted: 0,
        };
        let html = landing_html(&ctx(), None, Some(&dash));
        assert!(html.contains("—"));
        // empty project chart falls back to the explanatory message
        assert!(html.contains("No per-project traffic yet"));
    }

    #[test]
    fn group_thousands_groups() {
        assert_eq!(group_thousands(0), "0");
        assert_eq!(group_thousands(999), "999");
        assert_eq!(group_thousands(1000), "1,000");
        assert_eq!(group_thousands(1234567), "1,234,567");
    }

    #[test]
    fn truncate_adds_ellipsis_only_when_cut() {
        assert_eq!(truncate("short", 18), "short");
        assert_eq!(truncate("0123456789abcdefghij", 18).chars().count(), 18);
        assert!(truncate("0123456789abcdefghij", 18).ends_with('…'));
    }
}
