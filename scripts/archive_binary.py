"""Re-wrap the pypiron executable from a built wheel as a release archive.

maturin's `bin` wheels carry the compiled executable in the wheel's
`*.data/scripts/` directory (a wheel is just a zip). For GitHub Releases we pull
that binary out and re-wrap it as a `.tar.gz` (Unix) or `.zip` (Windows) named by
Rust target triple. No recompiling: the binary already exists inside the wheel
CI built for PyPI, so the standalone downloads and the wheels come from one build.
"""

from __future__ import annotations

import argparse
import tarfile
import tempfile
import zipfile
from pathlib import Path

_BINARY_NAMES = ("pypiron", "pypiron.exe")


def _find_binary(wheel: Path, dest: Path) -> Path:
    """Extract `wheel` into `dest` and return the path to the pypiron executable."""
    with zipfile.ZipFile(wheel) as zf:
        zf.extractall(dest)
    for path in dest.rglob("*"):
        if path.is_file() and path.parent.name == "scripts" and path.name in _BINARY_NAMES:
            return path
    raise SystemExit(f"no pypiron executable found inside {wheel.name}")


def _write_archive(binary: Path, triple: str, out_dir: Path) -> Path:
    """Wrap `binary` as pypiron-<triple>.{tar.gz,zip} in `out_dir`; return the path."""
    out_dir.mkdir(parents=True, exist_ok=True)
    if "windows" in triple:
        archive = out_dir / f"pypiron-{triple}.zip"
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(binary, "pypiron.exe")
        return archive
    archive = out_dir / f"pypiron-{triple}.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        info = tf.gettarinfo(str(binary), arcname="pypiron")
        info.mode = 0o755  # executable, regardless of how it was stored in the wheel
        with binary.open("rb") as fh:
            tf.addfile(info, fh)
    return archive


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist", type=Path, required=True, help="dir holding the built wheel")
    parser.add_argument("--triple", required=True, help="Rust target triple for the archive name")
    parser.add_argument("--out", type=Path, default=Path("binaries"), help="output dir")
    args = parser.parse_args()

    wheels = sorted(args.dist.glob("*.whl"))
    if len(wheels) != 1:
        names = [w.name for w in wheels]
        raise SystemExit(f"expected exactly one wheel in {args.dist}, found {len(wheels)}: {names}")

    with tempfile.TemporaryDirectory() as tmp:
        binary = _find_binary(wheels[0], Path(tmp))
        archive = _write_archive(binary, args.triple, args.out)
    print(f"wrote {archive}")


if __name__ == "__main__":
    main()
