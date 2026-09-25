//! One redrawable status line at the bottom of a terminal. Log records go
//! through [`LogWriter`], which lifts the line out of the way, prints the record,
//! and redraws the line under it — so the sync meter stays readable instead of
//! being shredded by every "uploading …" line.

use std::io::{self, Write};
use std::sync::{Mutex, MutexGuard};

/// The line currently drawn on stderr; empty when none is.
static LINE: Mutex<String> = Mutex::new(String::new());

fn line() -> MutexGuard<'static, String> {
    // A panic mid-draw leaves a stale string at worst; keep drawing.
    LINE.lock().unwrap_or_else(|e| e.into_inner())
}

/// Draw (or redraw) the status line. Call only when stderr is a terminal.
pub(crate) fn set(text: String) {
    let mut line = line();
    let mut err = io::stderr().lock();
    let _ = write!(err, "\r\x1b[2K{text}");
    let _ = err.flush();
    *line = text;
}

/// Erase the status line so what follows prints on a clean row.
pub(crate) fn clear() {
    let mut line = line();
    if !line.is_empty() {
        let mut err = io::stderr().lock();
        let _ = write!(err, "\r\x1b[2K");
        let _ = err.flush();
        line.clear();
    }
}

/// The log sink: stdout, stepping around the status line when one is drawn.
/// The fmt layer hands over each record in a single `write_all`, so a record
/// never splits across a redraw.
pub struct LogWriter;

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let line = line();
        if line.is_empty() {
            return io::stdout().write(buf);
        }
        let mut err = io::stderr().lock();
        let _ = write!(err, "\r\x1b[2K");
        let _ = err.flush();
        let mut out = io::stdout().lock();
        out.write_all(buf)?;
        out.flush()?;
        let _ = write!(err, "{line}");
        let _ = err.flush();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stdout().flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogWriter {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter
    }
}
