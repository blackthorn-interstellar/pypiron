//! The nightly VOPR matrix must not starve its durability oracles.
//!
//! DURABILITY, VISIBILITY and CONSERVATION (`examples/vopr.rs`) skip any
//! filename whose bytes an authorized delete, tombstone or conflict freeze
//! removed — the exemption is correct, and it means their entire universe is
//! the `--packages x --files` corpus. Run enough ops against a small enough
//! corpus and every name is tombstoned by quiescence: the three oracles then
//! iterate an empty set, and the run still prints a healthy five-figure hit
//! count because hits are summed over 50,000 seeds.
//!
//! That is what the nightly did. At 160 ops with a 10% delete weight against
//! the harness default of 2 packages x 2 files, measured per seed over 400
//! seeds per row: 20% of `single-bucket-crash-only` and `multi-bucket-crash-only`
//! seeds evaluated durability at all, 39-40% of the two fault rows. Widening to
//! 12 filenames puts every row at 99-100% and costs ~40% of the seed rate.
//!
//! So this test pins the corpus in the workflow, where the defect lived. The
//! rotating row is exempt: it derives entity counts from the seed (1-6 packages
//! x 1-4 files) and its small draws are deliberate coverage of the dense,
//! high-contention end.

/// Filenames a non-rotating nightly row must give the durability oracles.
/// Measured knee: 4 names reach 20% of seeds, 8 reach 89%, 12 reach ~100%.
const MIN_CORPUS: usize = 12;

const WORKFLOW: &str = include_str!("../.github/workflows/simulation.yml");

/// The integer argument following `flag` on a matrix row. Words carry the YAML
/// quoting around them (`extra: '--packages 6 ...'`), so both sides are stripped
/// of everything that is not the flag or the number.
fn flag_value(row: &str, flag: &str) -> Option<usize> {
    row.split_whitespace()
        .map(|word| word.trim_matches(|c: char| c == '\'' || c == '"'))
        .skip_while(|word| *word != flag)
        .nth(1)?
        .trim_matches(|c: char| !c.is_ascii_digit())
        .parse()
        .ok()
}

#[test]
fn nightly_profiles_pin_a_durability_corpus() {
    let rows: Vec<&str> = WORKFLOW
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("- { name:"))
        .filter(|line| !line.contains("--rotate"))
        .collect();
    assert!(
        rows.len() >= 4,
        "expected the nightly matrix's fixed profile rows; parsed {} — has the matrix moved?",
        rows.len()
    );
    for row in rows {
        let packages = flag_value(row, "--packages");
        let files = flag_value(row, "--files");
        let corpus = packages.zip(files).map(|(p, f)| p * f).unwrap_or(0);
        assert!(
            corpus >= MIN_CORPUS,
            "nightly profile has a {corpus}-filename corpus (packages={packages:?} \
             files={files:?}); below {MIN_CORPUS} the deletes tombstone it and \
             DURABILITY/VISIBILITY/CONSERVATION verify nothing on most seeds:\n  {row}"
        );
    }
    assert!(
        WORKFLOW.contains("${{ matrix.profile.args }}"),
        "the profile rows' flags no longer reach the vopr command line"
    );
}

/// `--rotate` derives the whole workload from the seed, so the harness rejects a
/// workload flag beside it rather than parsing and discarding one. The nightly
/// used to paste `--nodes/--buckets/--ops` onto *every* row from a shared
/// template, including the rotating one — where they were documented as
/// "ignored". Under that rejection the job no longer starts, so the shape of
/// this matrix is now load-bearing: each row must carry its own flags.
#[test]
fn no_nightly_row_mixes_rotation_with_a_workload_flag() {
    const WORKLOAD: [&str; 6] = [
        "--nodes",
        "--buckets",
        "--packages",
        "--files",
        "--ops",
        "--fail-percent",
    ];
    for row in WORKFLOW
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("- { name:"))
        .filter(|line| line.contains("--rotate"))
    {
        for flag in WORKLOAD {
            assert!(
                !row.contains(flag),
                "a rotating profile carries {flag}, which the harness refuses — \
                 the nightly would panic before its first seed:\n  {row}"
            );
        }
    }
    // The per-row check alone would have missed the original defect: the
    // rotating row never spelled `--nodes`, it inherited one from the shared
    // invocation that pasted a topology onto every profile. So the invocation
    // itself must pass no workload flag — every one has to come from the row it
    // belongs to, or a future template edit reintroduces the same silent paste.
    let invocation: String = WORKFLOW
        .lines()
        .skip_while(|line| !line.contains("example vopr --"))
        .take_while(|line| !line.contains("tee vopr.out"))
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        !invocation.is_empty(),
        "could not find the nightly's vopr invocation — has the workflow moved?"
    );
    for flag in WORKLOAD {
        assert!(
            !invocation.contains(flag),
            "the shared nightly invocation passes {flag} to every profile, including the \
             rotating one, which the harness refuses — it must come from `args` instead:\n{invocation}"
        );
    }
}
