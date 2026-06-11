//! Wheel introspection: pull `METADATA` out of a wheel at upload time
//! (PEP 658). Wheels are zip files with metadata at
//! `<dist>-<version>.dist-info/METADATA`.

use std::io::{Cursor, Read};

use zip::ZipArchive;

/// Core metadata is text; anything past this is a zip bomb, not a METADATA.
const MAX_METADATA_BYTES: u64 = 16 * 1024 * 1024;

/// Extract the core `METADATA` file from wheel bytes, if present.
pub fn extract_metadata(wheel_bytes: &[u8]) -> Option<Vec<u8>> {
    let mut zip = ZipArchive::new(Cursor::new(wheel_bytes)).ok()?;
    let name = zip
        .file_names()
        .find(|n| n.ends_with(".dist-info/METADATA") && n.matches('/').count() == 1)
        .map(str::to_string)?;
    let entry = zip.by_name(&name).ok()?;
    if entry.size() > MAX_METADATA_BYTES {
        return None;
    }
    let mut out = Vec::new();
    // take() guards against central directories that lie about the size.
    entry
        .take(MAX_METADATA_BYTES + 1)
        .read_to_end(&mut out)
        .ok()?;
    if out.len() as u64 > MAX_METADATA_BYTES {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
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
        assert_eq!(extract_metadata(&wheel).as_deref(), Some(md.as_slice()));
    }

    #[test]
    fn missing_metadata_and_garbage_are_none() {
        assert_eq!(extract_metadata(&fake_wheel(None)), None);
        assert_eq!(extract_metadata(b"not a zip"), None);
    }
}
