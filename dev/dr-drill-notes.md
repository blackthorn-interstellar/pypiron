# Disaster-recovery drill

`make dr-drill` (or `uv run -- pytest tests/test_dr_drill.py -s -n 0`) runs a real
backup -> wipe -> restore -> reinstall loop against the actual binary over HTTP.
It is a **correctness/trust proof**, not a benchmark. The single number it prints:

```
DR DRILL: 10/10 restored byte-identical, 0 lost, 0 byte-altered.
```

## What it proves (and how it forecloses the obvious objections)

The drill uploads N=10 wheels under `unique_package()` UUID names, takes a `tar`
snapshot of the data-dir, uploads one *more* package after the snapshot, stops
the server, `rm -rf`s the data-dir, restores **only `packages/`** (artifacts +
`.meta.json` sidecars — not the `simple/` views), regenerates the views offline
with `rebuild-index`, gates on `verify-index` exiting 0, then stands up a fresh
server on the restored dir and reinstalls every pre-backup package.

A hostile reviewer's four claims are made impossible by construction:

- *"the install secretly hit PyPI."* The server runs with **no upstream proxy**,
  the names are UUIDs absent from PyPI, and uv is pointed at `--index-url
  <restored>` only (default PyPI replaced, not extended). Any one suffices.
- *"the wipe was faked."* The data-dir is asserted empty after `rm -rf`.
- *"the views were never regenerated."* Only `packages/` is restored, so `simple/`
  is asserted absent before `rebuild-index` and present after, gated on
  `verify-index` == 0.
- *"byte-identity was never checked."* The exact bytes the restored server serves
  for each artifact are sha256'd against the pre-backup manifest.

## Recovery time (measured, at toy scale — read honestly)

On this laptop with N=10 packages the maintenance steps are effectively instant:

| step                                   | observed (N=10) |
| -------------------------------------- | --------------- |
| `tar` backup                           | ~0.15 s         |
| `rebuild-index` (restore-from-truth)   | ~0.05 s         |

These numbers are meaningless as a scale figure — ten wheels fit in a page cache.
Recovery cost scales with the corpus and the hardware. The real datapoint is in
[BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md): a cold rebuild-everything of a
**5,001-package** store (the explicit `rebuild-index`/restore path) measured
**~140 s** on that rig. Do not quote the drill's sub-second numbers anywhere
outward-facing.

## RPO == backup cadence, by construction

The package uploaded *after* the snapshot is absent from the restore — the drill
asserts its index 404s, then re-uploads it successfully. That gap is the whole
RPO story: whatever lands between two backups is what a restore loses. The RPO
is exactly your backup interval, nothing subtler. Truth is `packages/`; the views
are a regenerable projection, so a backup only has to capture truth.
