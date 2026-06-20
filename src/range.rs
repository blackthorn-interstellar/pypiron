//! HTTP `Range` header parsing (RFC 9110 §14), resolved against a known object
//! size. Pure and std-only — the byte values are client-controlled, so this is
//! fuzzed (`fuzz/fuzz_targets/fuzz_range.rs`) for the one invariant that matters:
//! a `Partial(start, end)` is always a satisfiable, in-bounds slice
//! (`start <= end < size`), never an underflowed or reversed range. The serving
//! backends in [`crate::storage`] trust that invariant when they `seek`.

/// A single HTTP byte range resolved against a known size.
#[derive(Debug, PartialEq)]
pub enum RangeSpec {
    Full,
    Partial(u64, u64),
    Unsatisfiable,
}

/// Parse a single-range `Range` header. Multi-range and malformed headers
/// fall back to the full body (RFC 9110 lets a server ignore Range).
pub fn parse_range(header: Option<&str>, size: u64) -> RangeSpec {
    let Some(spec) = header.and_then(|h| h.strip_prefix("bytes=")) else {
        return RangeSpec::Full;
    };
    let spec = spec.trim();
    if spec.contains(',') {
        return RangeSpec::Full;
    }
    if let Some(suffix) = spec.strip_prefix('-') {
        // suffix range: the last N bytes
        let Ok(n) = suffix.parse::<u64>() else {
            return RangeSpec::Full;
        };
        if n == 0 || size == 0 {
            return RangeSpec::Unsatisfiable;
        }
        let n = n.min(size);
        return RangeSpec::Partial(size - n, size - 1);
    }
    let Some((start_s, end_s)) = spec.split_once('-') else {
        return RangeSpec::Full;
    };
    let Ok(start) = start_s.parse::<u64>() else {
        return RangeSpec::Full;
    };
    if start >= size {
        return RangeSpec::Unsatisfiable;
    }
    let end = if end_s.is_empty() {
        size - 1
    } else {
        match end_s.parse::<u64>() {
            Ok(e) if e >= start => e.min(size - 1),
            _ => return RangeSpec::Full,
        }
    };
    RangeSpec::Partial(start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_parsing() {
        use RangeSpec::*;
        assert_eq!(parse_range(None, 100), Full);
        assert_eq!(parse_range(Some("bytes=0-49"), 100), Partial(0, 49));
        assert_eq!(parse_range(Some("bytes=50-"), 100), Partial(50, 99));
        assert_eq!(parse_range(Some("bytes=-10"), 100), Partial(90, 99));
        // end clamps to size
        assert_eq!(parse_range(Some("bytes=0-1000"), 100), Partial(0, 99));
        // suffix larger than the file means the whole file
        assert_eq!(parse_range(Some("bytes=-1000"), 100), Partial(0, 99));
        // out of bounds start
        assert_eq!(parse_range(Some("bytes=100-"), 100), Unsatisfiable);
        assert_eq!(parse_range(Some("bytes=-0"), 100), Unsatisfiable);
        // ignorable: multi-range, malformed, non-byte units
        assert_eq!(parse_range(Some("bytes=0-1,5-9"), 100), Full);
        assert_eq!(parse_range(Some("bytes=junk"), 100), Full);
        assert_eq!(parse_range(Some("items=0-5"), 100), Full);
        assert_eq!(parse_range(Some("bytes=9-5"), 100), Full);
    }
}
