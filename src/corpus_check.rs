//! Corpus check: run the filename parsers over every file ever uploaded to
//! PyPI (17M+ rows) and measure how they hold up against ground truth.
//!
//! The corpus is a gzipped TSV of `name \t version \t filename \t packagetype`
//! exported from the public `pypi.projects` dataset (ClickHouse playground,
//! mirroring BigQuery `bigquery-public-data.pypi.distribution_metadata`):
//!
//! ```sh
//! curl -s 'https://sql-clickhouse.clickhouse.com/?user=demo&enable_http_compression=1' \
//!   -H 'Accept-Encoding: gzip' \
//!   --data 'SELECT name, version, filename, packagetype FROM pypi.projects
//!           ORDER BY name, filename FORMAT TSV' \
//!   -o bench/corpus/pypi-files.tsv.gz
//! ```
//!
//! Run with: `cargo test --release corpus_full_pypi -- --ignored --nocapture`
//!
//! Why this matters in production (not just upload fallback): the proxy
//! writes `infer_version_from_filename` results into sidecars for every
//! artifact it caches from pypi.org (proxy.rs), and the reconciler backfills
//! sidecars the same way (worker.rs). These parsers therefore face the full
//! historical zoo of PyPI filenames.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use flate2::read::GzDecoder;

use crate::names::{
    infer_package_from_filename, infer_version_from_filename, normalize_pkg_name, parse_wheel_tags,
};
use crate::sidecar::is_artifact;

const SAMPLES_PER_BUCKET: usize = 8;

#[derive(Default)]
struct Bucket {
    count: u64,
    samples: Vec<String>,
}

impl Bucket {
    fn hit(&mut self, sample: impl FnOnce() -> String) {
        self.count += 1;
        if self.samples.len() < SAMPLES_PER_BUCKET {
            self.samples.push(sample());
        }
    }
}

/// Coarse filename class by extension; the parsers branch on these.
fn ext_class(filename: &str) -> &'static str {
    let f = filename.to_ascii_lowercase();
    for (suffix, class) in [
        (".whl", ".whl"),
        (".tar.gz", ".tar.gz"),
        (".tar.bz2", ".tar.bz2"),
        (".tar.xz", ".tar.xz"),
        (".zip", ".zip"),
        (".egg", ".egg"),
        (".exe", ".exe"),
        (".msi", ".msi"),
        (".rpm", ".rpm"),
        (".dmg", ".dmg"),
        (".deb", ".deb"),
        (".tgz", ".tgz"),
        (".tar.z", ".tar.Z"),
        (".tar", ".tar"),
    ] {
        if f.ends_with(suffix) {
            return class;
        }
    }
    "other"
}

#[derive(Default)]
struct Report {
    rows: u64,
    name_ok: u64,
    not_artifact: Bucket,
    name_unnormalizable: Bucket,
    by_ext: BTreeMap<&'static str, u64>,
    // name inference
    name_mismatch_by_ext: BTreeMap<&'static str, Bucket>,
    // version inference
    version_none_by_ext: BTreeMap<&'static str, Bucket>,
    version_wrong_by_ext: BTreeMap<&'static str, Bucket>,
    version_ok: u64,
    // wheel tags
    wheels: u64,
    wheel_tags_unparseable: Bucket,
}

fn check_row(report: &mut Report, name: &str, version: &str, filename: &str) {
    report.rows += 1;
    let ext = ext_class(filename);
    *report.by_ext.entry(ext).or_default() += 1;

    if !is_artifact(filename) {
        report.not_artifact.hit(|| format!("{name} :: {filename}"));
    }

    let truth = normalize_pkg_name(name);
    if truth.is_empty() || !crate::names::is_normalized(&truth) {
        report.name_unnormalizable.hit(|| name.to_string());
    }

    if infer_package_from_filename(filename) == truth {
        report.name_ok += 1;
    } else {
        report
            .name_mismatch_by_ext
            .entry(ext)
            .or_default()
            .hit(|| format!("{name} :: {filename}"));
    }

    match infer_version_from_filename(filename) {
        None => report
            .version_none_by_ext
            .entry(ext)
            .or_default()
            .hit(|| format!("{version} :: {filename}")),
        Some(v) if v != version => report
            .version_wrong_by_ext
            .entry(ext)
            .or_default()
            .hit(|| format!("got {v}, want {version} :: {filename}")),
        Some(_) => report.version_ok += 1,
    }

    if filename.ends_with(".whl") {
        report.wheels += 1;
        if parse_wheel_tags(filename).is_none() {
            report.wheel_tags_unparseable.hit(|| filename.to_string());
        }
    }
}

fn print_bucket_map(
    title: &str,
    total_by_ext: &BTreeMap<&'static str, u64>,
    map: &BTreeMap<&'static str, Bucket>,
) {
    println!("\n== {title} ==");
    for (ext, b) in map {
        let total = total_by_ext.get(ext).copied().unwrap_or(0);
        let pct = 100.0 * b.count as f64 / total.max(1) as f64;
        println!("  {ext:>9}: {:>9} / {total:>9} ({pct:.3}%)", b.count);
        for s in &b.samples {
            println!("              e.g. {s}");
        }
    }
}

fn run(corpus: &Path) -> Report {
    let file = File::open(corpus).unwrap_or_else(|e| {
        panic!(
            "corpus not found at {} ({e}); see module docs",
            corpus.display()
        )
    });
    let reader = BufReader::with_capacity(1 << 20, GzDecoder::new(file));
    let mut report = Report::default();
    for line in reader.lines() {
        let line = line.expect("corpus read");
        let mut parts = line.split('\t');
        let (Some(name), Some(version), Some(filename)) =
            (parts.next(), parts.next(), parts.next())
        else {
            panic!("malformed corpus row: {line:?}");
        };
        check_row(&mut report, name, version, filename);
    }
    report
}

impl Report {
    fn print(&self) {
        println!("rows: {}", self.rows);
        println!("\n== files by extension class ==");
        let mut exts: Vec<_> = self.by_ext.iter().collect();
        exts.sort_by_key(|(_, n)| std::cmp::Reverse(**n));
        for (ext, n) in exts {
            println!("  {ext:>9}: {n:>9}");
        }

        println!(
            "\n== is_artifact() == false (would be invisible): {}",
            self.not_artifact.count
        );
        for s in &self.not_artifact.samples {
            println!("              e.g. {s}");
        }
        println!(
            "\n== unnormalizable project names: {}",
            self.name_unnormalizable.count
        );
        for s in &self.name_unnormalizable.samples {
            println!("              e.g. {s}");
        }

        print_bucket_map(
            "name inference mismatches",
            &self.by_ext,
            &self.name_mismatch_by_ext,
        );
        print_bucket_map(
            "version inference: None",
            &self.by_ext,
            &self.version_none_by_ext,
        );
        print_bucket_map(
            "version inference: wrong",
            &self.by_ext,
            &self.version_wrong_by_ext,
        );

        println!(
            "\nname ok: {} / {} ({:.3}%)",
            self.name_ok,
            self.rows,
            100.0 * self.name_ok as f64 / self.rows.max(1) as f64
        );
        println!(
            "version ok: {} / {} ({:.3}%)",
            self.version_ok,
            self.rows,
            100.0 * self.version_ok as f64 / self.rows.max(1) as f64
        );
        println!(
            "wheel tags unparseable: {} / {} wheels",
            self.wheel_tags_unparseable.count, self.wheels
        );
        for s in &self.wheel_tags_unparseable.samples {
            println!("              e.g. {s}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// The full-corpus gate. Thresholds encode measured reality so a parser
    /// regression that worsens real-world coverage fails loudly. Requires the
    /// corpus download (see module docs); ignored by default.
    #[test]
    #[ignore = "needs bench/corpus/pypi-files.tsv.gz (95MB download)"]
    fn corpus_full_pypi() {
        let corpus =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench/corpus/pypi-files.tsv.gz");
        let report = run(&corpus);
        report.print();

        assert!(
            report.rows > 17_000_000,
            "corpus truncated: {}",
            report.rows
        );
        // Every real PyPI filename must be visible to listings.
        assert_eq!(
            report.not_artifact.count, 0,
            "real files invisible to is_artifact"
        );
        // Every real project name must normalize to a valid storage segment.
        assert_eq!(report.name_unnormalizable.count, 0);

        // Measured 2026-06-12: name 99.784%, version 98.914%, 126 bad wheels.
        // Slack covers corpus growth, not parser regressions.
        let name_rate = report.name_ok as f64 / report.rows as f64;
        assert!(
            name_rate > 0.995,
            "name inference regressed: {name_rate:.4}"
        );
        let version_rate = report.version_ok as f64 / report.rows as f64;
        assert!(
            version_rate > 0.985,
            "version inference regressed: {version_rate:.4}"
        );
        let tag_fail_rate = report.wheel_tags_unparseable.count as f64 / report.wheels as f64;
        assert!(
            tag_fail_rate < 0.0001,
            "wheel tag parse regressed: {tag_fail_rate:.6}"
        );
    }
}
