# Converge

State for the `/converge` loop. `dev/VISION.md` is the fixed reference; this
file is the loop's memory. Only a human raises the ceiling.

## Ceiling

- Non-test lines: **45330** measured 2026-09-19 at `74b7d62`.
- Ceiling: **47597** (measured + 5%).
- Measure with, from the repo root:

```
for f in $(find src -name '*.rs'); do awk '{ if (pending && /^(pub )?mod /) exit; if (pending) n++; pending = ($0 ~ /^#\[cfg\(test\)\]$/); if (!pending) n++ } END { print n+0 }' "$f"; done | paste -sd+ - | bc
```

(Every `src/**/*.rs` line before the file's `#[cfg(test)] mod …` block.)

## Done

- 2026-09-19 Bug: an invalid inline `--private-pattern` / TOML `private-patterns` entry (`#bolt`, empty) was silently dropped by the pattern-file comment filter, so a name the operator believed reserved could fall through to the proxy; now refuses startup naming the entry. Unit test on `PrivateArgs::resolve` and a blackbox startup-refusal test were red first. Lines 45330 -> 45339.

## Rejected

- 2026-09-19 `--private-prefix` of 255 or 256 bytes now refuses startup because `{prefix}-*` exceeds the 256-byte pattern cap (a regression in c030441). Real, but no deployment has a 255-byte namespace; not worth a change until someone hits it. One-line fix if ever wanted: build the `-*` pattern in `PrivateNames::new` without re-parsing.

## Empty iterations

0 consecutive.

## Open questions
