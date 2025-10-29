use html_escape::encode_text;
use serde::Serialize;

/// Render minimal PEP 503 per‑package HTML.
pub fn pep503_package_html(package: &str, files: &[String]) -> String {
    // Links are relative to the API under /files/<pkg>/<filename>
    let mut body = String::new();
    body.push_str(&format!(
        r"<html><head><title>Links for {}</title></head><body>",
        encode_text(package)
    ));
    for f in files {
        let fname = encode_text(f);
        body.push_str(&format!(
            r#"<a href="/files/{}/{fname}">{fname}</a><br/>"#,
            encode_text(package)
        ));
    }
    body.push_str("</body></html>");
    body
}

/// Render minimal PEP 503 global HTML index.
pub fn pep503_global_html(packages: &[String]) -> String {
    let mut body = String::new();
    body.push_str(r"<html><head><title>Simple index</title></head><body>");
    for p in packages {
        let p = encode_text(p);
        body.push_str(&format!(r#"<a href="/simple/{p}/">{p}</a><br/>"#));
    }
    body.push_str("</body></html>");
    body
}

#[derive(Serialize)]
struct Pep691File {
    filename: String,
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    yanked: Option<bool>,
}

#[derive(Serialize)]
struct Pep691PkgIndex<'a> {
    #[serde(rename = "meta")]
    meta: Pep691Meta<'a>,
    name: &'a str,
    files: Vec<Pep691File>,
}

#[derive(Serialize)]
struct Pep691Meta<'a> {
    #[serde(rename = "api-version")]
    api_version: &'a str,
}

/// Minimal PEP 691 package JSON.
pub fn pep691_package_json(package: &str, files: &[String]) -> String {
    let files: Vec<Pep691File> = files
        .iter()
        .map(|f| Pep691File {
            filename: f.clone(),
            url: format!("/files/{package}/{f}"),
            yanked: None,
        })
        .collect();

    let doc = Pep691PkgIndex {
        meta: Pep691Meta { api_version: "1.0" },
        name: package,
        files,
    };
    serde_json::to_string(&doc).unwrap_or_else(|_| "{\"meta\":{\"api-version\":\"1.0\"}}".into())
}

#[derive(Serialize)]
struct Pep691ProjectRef<'a> {
    name: &'a str,
    url: String,
}

#[derive(Serialize)]
struct Pep691Global<'a> {
    #[serde(rename = "meta")]
    meta: Pep691Meta<'a>,
    projects: Vec<Pep691ProjectRef<'a>>,
}

/// Minimal PEP 691 global index JSON.
pub fn pep691_global_json(packages: &[String]) -> String {
    let projects: Vec<Pep691ProjectRef> = packages
        .iter()
        .map(|p| Pep691ProjectRef {
            name: p,
            url: format!("/simple/{p}/"),
        })
        .collect();

    let doc = Pep691Global {
        meta: Pep691Meta { api_version: "1.0" },
        projects,
    };
    serde_json::to_string(&doc).unwrap_or_else(|_| "{\"meta\":{\"api-version\":\"1.0\"}}".into())
}
