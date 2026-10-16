# ZIP dependency repair — 2026-10-16

The October 16 Fuzz and Nightly deps failures were Cargo resolution failures:
the registry offered no non-yanked version satisfying ZIP 2. They were not
evidence of parser crashes. A root lockfile alone cannot repair the standalone
fuzz workspace or a fresh dependency resolution.

Use ZIP 9.0.4 with default features disabled and only `deflate` enabled in both
manifests. Upstream's [support policy](https://github.com/zip-rs/zip2/blob/v9.0.4/SECURITY.md)
supports only its latest release. Older releases appearing unyanked again do
not make them supported. [9.0.4](https://github.com/zip-rs/zip2/releases/tag/v9.0.4)
is the supported stable release on this date; it requires Rust 1.88 or newer.

## Release-age exception

This deliberate migration admits **zip 9.0.4 only**, published on
2026-10-16 at 09:09:57 UTC, inside the normal 14-day cooldown. The reason is to
restore fresh resolution on a supported upstream release. This is a one-time
reviewed lockfile change, not a bypass in the nightly updater. `typed-path`
0.12.3, the new root dependency, was published on 2026-02-11 and needs no
exception. No advisory ignores, scanner exclusions, or workflow failure
suppression are added.

The fuzz lockfile starts from the parent's resolved versions and adds the
fuzz-only dependencies. `arbitrary` stays at 1.4.2: 1.5.0 was published on
October 6 and has not passed the 14-day cooldown. `libfuzzer-sys` 0.4.13 was
published on June 4. Commit this lockfile. CI validates it with
`cargo metadata --locked` before cargo-fuzz and checks that cargo-fuzz left it
unchanged. The nightly dependency lane updates both Cargo workspaces, applies
the same fail-closed 14-day cooldown to each, and banks, verifies, and publishes
all three lockfiles. Its validation builds the standalone fuzz targets too.
Refresh the parent and fuzz locks together when updating ZIP.

## Reader and writer compatibility

ZIP 9 returns a fallible decoded filename. Wheel selection still chooses the
first top-level `*.dist-info/METADATA` in directory order and preserves the
entry and byte limits. Invalid filename decoding rejects a wheel. Advisory
ingestion continues to skip individual unreadable or malformed members.
Neither path extracts archive entries onto the filesystem. Stored and deflated
writer fixtures exercise both formats, Unicode names, selection, and corrupt
payload handling. Existing bounds tests and fuzz dictionaries/corpora remain
in force.

Validation must include `make check`, ZIP bounds/HTTP tests, all eight fuzz
targets, and the unchanged CI/security workflows. A compiler failure is not a
fuzz crash; a scanner finding must be fixed independently before merging.
