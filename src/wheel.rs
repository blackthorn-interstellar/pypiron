//! Wheel introspection: pull `METADATA` out of a wheel at upload time
//! (PEP 658). Wheels are zip files with metadata at
//! `<dist>-<version>.dist-info/METADATA`.

use std::io::{Read, Seek};
use std::path::Path;

use zip::ZipArchive;

/// Core metadata is text; anything past this is a zip bomb, not a METADATA.
/// The proxy reuses this to bound upstream `.metadata`/`.provenance` fetches.
pub(crate) const MAX_METADATA_BYTES: u64 = 16 * 1024 * 1024;

/// Ceiling on a wheel's central-directory entry count. `ZipArchive::new`
/// materializes every record — name string included — before any per-entry cap
/// applies, so an upload spool of minimum-size entries is millions of resident
/// records for a body well under the upload limit. Measured on 2026-08-29
/// against the largest wheel of each heavyweight project on PyPI: tensorflow
/// 24,117 entries (573 MB), torch 12,911 (527 MB), scipy 1,558, numpy 1,049,
/// pyarrow 814, matplotlib 634 — so 2^18 leaves the worst real wheel ~10.9x
/// headroom and no honest publisher can reach it.
pub(crate) const MAX_WHEEL_ENTRIES: usize = 262_144;

/// Why a wheel yielded no `METADATA`. The split matters to the uploader: a
/// broken wheel is refused, while an oversized-but-valid one is stored and only
/// loses its PEP 658 fast path.
#[derive(Debug, PartialEq, Eq)]
pub enum WheelError {
    /// Not a readable zip, or no `<dist>.dist-info/METADATA` at its top level.
    Invalid,
    /// Over [`MAX_WHEEL_ENTRIES`] or [`MAX_METADATA_BYTES`]; never walked or read.
    TooLarge,
}

/// Extract `METADATA` from a wheel on disk without loading the wheel into
/// memory — zip needs only the central directory plus the one entry.
pub fn extract_metadata_from_file(path: &Path) -> Result<Vec<u8>, WheelError> {
    extract_metadata_from_reader(std::fs::File::open(path).map_err(|_| WheelError::Invalid)?)
}

pub(crate) fn extract_metadata_from_reader<R: Read + Seek>(
    reader: R,
) -> Result<Vec<u8>, WheelError> {
    let mut zip = ZipArchive::new(reader).map_err(|_| WheelError::Invalid)?;
    if zip.len() > MAX_WHEEL_ENTRIES {
        return Err(WheelError::TooLarge);
    }
    let name = zip
        .file_names()
        .find(|n| n.ends_with(".dist-info/METADATA") && n.matches('/').count() == 1)
        .map(str::to_string)
        .ok_or(WheelError::Invalid)?;
    let entry = zip.by_name(&name).map_err(|_| WheelError::Invalid)?;
    if entry.size() > MAX_METADATA_BYTES {
        return Err(WheelError::TooLarge);
    }
    let mut out = Vec::new();
    // take() guards against central directories that lie about the size.
    entry
        .take(MAX_METADATA_BYTES + 1)
        .read_to_end(&mut out)
        .map_err(|_| WheelError::Invalid)?;
    if out.len() as u64 > MAX_METADATA_BYTES {
        return Err(WheelError::TooLarge);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};
    use zip::write::SimpleFileOptions;

    fn fake_wheel(metadata: Option<&[u8]>) -> Vec<u8> {
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
        zip.start_file("demo/__init__.py", opts).unwrap();
        zip.write_all(b"").unwrap();
        if let Some(md) = metadata {
            zip.start_file("demo-1.0.dist-info/METADATA", opts).unwrap();
            zip.write_all(md).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    #[test]
    fn extracts_metadata_from_wheel() {
        let md = b"Metadata-Version: 2.1\nName: demo\nVersion: 1.0\n";
        let wheel = fake_wheel(Some(md));
        assert_eq!(
            extract_metadata_from_reader(Cursor::new(&wheel)).as_deref(),
            Ok(md.as_slice())
        );
    }

    /// A wheel whose central directory is over [`MAX_WHEEL_ENTRIES`] is refused
    /// before the METADATA search walks it — even though the METADATA is there.
    /// Built at the cap rather than at a hostile 14M so the test stays fast; the
    /// blackbox counterpart is `tests/test_zip_bounds.py`.
    #[test]
    fn over_entry_cap_is_too_large() {
        let md = b"Metadata-Version: 2.1\nName: demo\nVersion: 1.0\n";
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
        zip.start_file("demo-1.0.dist-info/METADATA", opts).unwrap();
        zip.write_all(md).unwrap();
        for i in 0..MAX_WHEEL_ENTRIES {
            zip.start_file(format!("demo/f{i}"), opts).unwrap();
        }
        let wheel = zip.finish().unwrap().into_inner();
        assert_eq!(
            extract_metadata_from_reader(Cursor::new(&wheel)),
            Err(WheelError::TooLarge)
        );
    }

    #[test]
    fn missing_metadata_and_garbage_are_invalid() {
        assert_eq!(
            extract_metadata_from_reader(Cursor::new(&fake_wheel(None))),
            Err(WheelError::Invalid)
        );
        assert_eq!(
            extract_metadata_from_reader(Cursor::new(b"not a zip" as &[u8])),
            Err(WheelError::Invalid)
        );
    }

    #[test]
    fn file_and_bytes_extraction_agree() {
        let md = b"Metadata-Version: 2.1\nName: demo\nVersion: 1.0\n";
        let wheel = fake_wheel(Some(md));
        let dir = std::env::temp_dir().join(format!("pypiron-wheel-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("demo-1.0-py3-none-any.whl");
        std::fs::write(&path, &wheel).unwrap();
        assert_eq!(
            extract_metadata_from_file(&path),
            extract_metadata_from_reader(Cursor::new(&wheel)),
            "spooled-file extraction must match in-memory extraction"
        );
        assert_eq!(
            extract_metadata_from_file(Path::new("/nonexistent.whl")),
            Err(WheelError::Invalid)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
