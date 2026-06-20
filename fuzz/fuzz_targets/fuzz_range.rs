//! Fuzz the HTTP `Range` header parser in `src/range.rs`.
//!
//! `parse_range` turns a client-controlled `Range` header plus the object size
//! into a resolved `RangeSpec`. The serving backends in `src/storage.rs` then
//! `seek(start)` and read `end - start + 1` bytes from a `Partial(start, end)`.
//! So the security-critical invariant — checked here over arbitrary headers and
//! sizes — is that a `Partial(start, end)` is ALWAYS a satisfiable, in-bounds,
//! non-empty slice: `start <= end < size`. A reversed range (`start > end`)
//! would underflow the read length; an out-of-bounds `end` would read past the
//! object. Either is a parser bug, not a 416. Also: never panic, on any bytes.
#![no_main]
#![allow(dead_code)]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

#[path = "../../src/range.rs"]
mod range;
use range::RangeSpec;

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    // A header that's present some of the time and absent the rest, against a
    // size drawn from the whole u64 domain (including 0 and u64::MAX, the
    // overflow-bait values).
    let header = Option::<String>::arbitrary(&mut u).unwrap_or(None);
    let size = u64::arbitrary(&mut u).unwrap_or(0);

    match range::parse_range(header.as_deref(), size) {
        RangeSpec::Partial(start, end) => {
            assert!(
                start <= end,
                "reversed range start={start} end={end} (header={header:?}, size={size})"
            );
            assert!(
                end < size,
                "range end={end} out of bounds for size={size} (header={header:?})"
            );
        }
        // Full = serve the whole body; Unsatisfiable = 416. Both are always fine.
        RangeSpec::Full | RangeSpec::Unsatisfiable => {}
    }
});
