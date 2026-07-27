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

/// Lines of the workflow belonging to the job whose key is `job`: everything
/// under it until the next line at job depth (the next job key, or the comment
/// block introducing it — job-level commentary sits at that same depth, and it
/// describes the job it precedes, not the one it follows).
///
/// There is more than one vopr job now, so every assertion has to name the one
/// it means; searching the whole file, or for the first `example vopr --` in
/// it, silently pins itself to whichever job is declared first.
fn job_lines(job: &'static str) -> impl Iterator<Item = &'static str> {
    /// Two spaces of indent and no more — where job keys and the comment blocks
    /// introducing them sit. Everything inside a job is indented further.
    fn at_job_depth(line: &str) -> bool {
        line.starts_with("  ") && !line.starts_with("   ")
    }
    WORKFLOW
        .lines()
        .skip_while(move |line| line.trim_start() != job)
        .skip(1)
        .take_while(|line| !at_job_depth(line))
}

/// That job's vopr command line, joined into one line: from `example vopr --`
/// down to the `tee vopr.out` that ends the invocation.
fn job_invocation(job: &'static str) -> String {
    job_lines(job)
        .skip_while(|line| !line.contains("example vopr --"))
        .take_while(|line| !line.contains("tee vopr.out"))
        .collect::<Vec<_>>()
        .join(" ")
}

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
    let invocation = job_invocation("vopr:");
    assert!(
        invocation.contains("matrix.profile.args"),
        "the extracted invocation is not the matrix job's — has the workflow moved?\n{invocation}"
    );
    for flag in WORKLOAD {
        assert!(
            !invocation.contains(flag),
            "the shared nightly invocation passes {flag} to every profile, including the \
             rotating one, which the harness refuses — it must come from `args` instead:\n{invocation}"
        );
    }
}

/// The partitioned lane is real coverage that does not pass yet. Measured
/// 2026-07-26 over 404,247 fresh seeds: 3 failing = 0.00074% per seed, a 215x
/// improvement on the 0.160% of the previous census. Two distinct root causes
/// remain — a DURABILITY drop that outlives the fence exempting it, and a
/// CONSERVATION arm that destroys an acked byte-set instead of moving it. (The
/// third, a non-crash-atomic `supersede_record` leaving a bucket serving bytes
/// contradicting its own published sha256, is closed: the supersede now fences
/// its torn window with `.superseding` and the next rebuild finishes it.) It is
/// in the nightly because at
/// `--partition 0` the merge algebra never executes — every verdict beyond the
/// trivial ones reads `[never presented]` — so without it the aligned rows
/// prove their invariants only about a fleet whose buckets never disagree.
///
/// That makes two opposite mistakes possible, and this test exists for both.
/// Dropping `continue-on-error` from the partitioned job turns the nightly
/// permanently red, and a lane nobody reads is worse than no lane. Adding
/// `continue-on-error` to the gated matrix job silently converts the one thing
/// that must never regress into decoration.
#[test]
fn the_partitioned_lane_is_non_blocking_and_the_gated_matrix_is_not() {
    assert!(
        !job_lines("vopr:").any(|line| line.contains("continue-on-error")),
        "the gated nightly matrix carries continue-on-error — the five aligned \
         profiles are the invariant that must never regress, and a soft failure \
         there is indistinguishable from a pass"
    );
    assert!(
        job_lines("vopr-partitioned:").any(|line| line.trim() == "continue-on-error: true"),
        "the partitioned lane lost continue-on-error. A small rate is not a \
         green gate: at the measured 0.00074% per seed (3 of 404,247) a draw of \
         N seeds reds with probability 1-(1-p)^N, so this job's ~90,000-100,000 \
         seeds red 49-52% of nights and even a 50,000-seed draw reds 31%. One \
         red night in twenty would need 1 failure per 1.75M seeds. Two root \
         causes are still open and both are live correctness defects, so gating \
         only makes the nightly permanently red. Remove this assertion with the \
         failures, not before — and never by lowering --partition, which is a \
         share of seeds and buys a quieter lane by testing less."
    );
}

/// Whatever else the partitioned lane changes, it has to stay comparable to the
/// `multi-bucket` row it shadows: same durability corpus, an actual partition
/// percentage, and a time budget rather than a seed count — `--max-secs`
/// supersedes `--seeds`, so passing both would discard one silently, which is
/// the exact defect the matrix rows above are shaped to prevent.
///
/// The corpus floor cuts against finding the CONSERVATION root cause still open
/// here, which needs contention concentrated on ONE filename and was found on a
/// narrow profile (1-2 packages, 1 file). That is not a reason to
/// shrink this row — 12 names is what keeps its durability oracles from
/// evaluating an empty set. Narrow coverage belongs in a lane of its own.
#[test]
fn the_partitioned_lane_keeps_its_corpus_and_partitions_something() {
    let invocation = job_invocation("vopr-partitioned:");
    assert!(
        !invocation.is_empty(),
        "could not find the partitioned lane's vopr invocation — has it moved?"
    );
    let partition = flag_value(&invocation, "--partition").unwrap_or(0);
    assert!(
        partition > 0,
        "the partitioned lane passes --partition {partition}: at 0% no seed \
         draws a split fleet, every writer pins bucket 0, the merge algebra \
         never runs, and the lane is a duplicate of the `multi-bucket` \
         row:\n{invocation}"
    );
    let packages = flag_value(&invocation, "--packages");
    let files = flag_value(&invocation, "--files");
    let corpus = packages.zip(files).map(|(p, f)| p * f).unwrap_or(0);
    assert!(
        corpus >= MIN_CORPUS,
        "the partitioned lane has a {corpus}-filename corpus (packages={packages:?} \
         files={files:?}); below {MIN_CORPUS} the deletes tombstone it and the \
         durability oracles verify nothing on most seeds — and DURABILITY was \
         live on this lane, 45 of the 291 failures measured before the \
         `.mirror-quarantined` fence replicated:\n{invocation}"
    );
    assert!(
        invocation.contains("--max-secs"),
        "the partitioned lane must run to a time budget: without one it stops at \
         its first failing seed (~300 seeds in) and reports no rate to watch \
         trend to zero:\n{invocation}"
    );
    assert!(
        !invocation.contains("--seeds"),
        "the partitioned lane passes both --seeds and --max-secs; --max-secs \
         supersedes it, so --seeds is parsed and discarded:\n{invocation}"
    );
}
