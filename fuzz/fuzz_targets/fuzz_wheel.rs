//! Fuzz the wheel METADATA extractor in `src/wheel.rs`.
//!
//! On every wheel upload the server hands arbitrary bytes to a zip parser to
//! pull out `<dist>-<ver>.dist-info/METADATA` (PEP 658). The bytes are whatever
//! the uploader sent. The property: never panic / hang / OOM, whatever the
//! archive looks like (truncated, zip64, encrypted, lying central directory,
//! zip bomb). The 16 MiB cap + `take()` guard are what keep it bounded.
#![no_main]
#![allow(dead_code)]

use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

#[path = "../../src/wheel.rs"]
mod wheel;

fuzz_target!(|data: &[u8]| {
    let out = wheel::extract_metadata_from_reader(Cursor::new(data));
    // Whatever it returns, the 16 MiB size guard must hold.
    if let Some(md) = out {
        assert!(
            md.len() as u64 <= 16 * 1024 * 1024,
            "extracted METADATA exceeds the byte cap"
        );
    }
});
