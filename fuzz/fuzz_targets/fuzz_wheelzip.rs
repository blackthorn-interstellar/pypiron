//! Structure-aware fuzzing of the wheel METADATA *selection* in `src/wheel.rs`.
//!
//! The sibling `fuzz_wheel` target throws raw bytes at the zip parser — great
//! for the malformed/truncated/lying-central-directory cases, but it almost
//! never stumbles onto a *valid* archive that actually contains a
//! `…dist-info/METADATA` member, so the selection-and-cap logic past
//! `ZipArchive::new` is left uncovered. This target builds well-formed zips on
//! purpose — a mix of matching METADATA members, decoys (`RECORD`, ordinary
//! files), and nested `dir/…dist-info/METADATA` (which has the wrong slash
//! count) — and pins the contract a hostile wheel could try to subvert:
//!
//!   `extract_metadata_from_reader` returns exactly the bytes of the FIRST
//!   member, in central-directory order, whose name ends in `.dist-info/METADATA`
//!   with exactly one `/` — and nothing when there is no such member.
//!
//! A wheel that smuggles its payload into `evil/…dist-info/METADATA` or a second
//! dist-info dir must not be able to get those bytes chosen.
#![no_main]
#![allow(dead_code)]

use std::io::{Cursor, Write};

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use zip::write::{SimpleFileOptions, ZipWriter};
use zip::CompressionMethod;

#[path = "../../src/wheel.rs"]
mod wheel;

const MAX_METADATA_BYTES: u64 = 16 * 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);

    // 0..=12 members. Names are made unique by index so the zip writer accepts
    // them and central-directory order == insertion order (the oracle leans on
    // that). `kind` decides whether a member is a real match or a decoy.
    let count = u8::arbitrary(&mut u).unwrap_or(0) % 13;
    let deflate = bool::arbitrary(&mut u).unwrap_or(false);

    let opts = SimpleFileOptions::default().compression_method(if deflate {
        CompressionMethod::Deflated
    } else {
        CompressionMethod::Stored
    });
    let mut zw = ZipWriter::new(Cursor::new(Vec::new()));

    // (matches_predicate, content) in the order actually written to the zip.
    let mut written: Vec<(bool, Vec<u8>)> = Vec::new();

    for i in 0..count {
        let kind = u8::arbitrary(&mut u).unwrap_or(0) % 4;
        let len = (u16::arbitrary(&mut u).unwrap_or(0) as usize) % 4097;
        let content = u.bytes(len).map(<[u8]>::to_vec).unwrap_or_default();

        let (name, matches) = match kind {
            // The one shape the extractor accepts: `*.dist-info/METADATA`, one slash.
            0 => (format!("d{i}-1.0.dist-info/METADATA"), true),
            // Right directory, wrong file.
            1 => (format!("d{i}-1.0.dist-info/RECORD"), false),
            // A `…dist-info/METADATA` buried one level deep — two slashes, rejected.
            2 => (format!("sub{i}/d-1.0.dist-info/METADATA"), false),
            // An ordinary payload file.
            _ => (format!("pkg{i}/__init__.py"), false),
        };

        if zw.start_file(&name, opts).is_err() || zw.write_all(&content).is_err() {
            continue;
        }
        written.push((matches, content));
    }

    let Ok(cursor) = zw.finish() else { return };
    let bytes = cursor.into_inner();

    // Oracle: first member (in write order) whose name matches the predicate.
    let expected = written
        .iter()
        .find(|(matches, _)| *matches)
        .map(|(_, content)| content.clone());

    let got = wheel::extract_metadata_from_reader(Cursor::new(bytes));
    assert_eq!(
        got, expected,
        "wheel METADATA selection diverged from first-matching-member"
    );
    if let Some(out) = &got {
        assert!(
            out.len() as u64 <= MAX_METADATA_BYTES,
            "extracted METADATA exceeds the byte cap"
        );
    }
});
