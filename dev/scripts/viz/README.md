# The run visualizer

A recorded pypiron run, played back in a browser. One JSONL schema, three
producers that already own the truth, one self-contained HTML file that plays
it. No new dependency in Rust, Python or JavaScript; no build step; no npm; no
CDN. Nothing it produces is committed — `make viz` writes into `.local/viz/`.

```
make viz                      # build every page into .local/viz, then open .local/viz/index.html
make viz VIZ_LIVE=0           # skip the 20-second live measurement on the scale page
python dev/scripts/viz/build.py --only wedge --skip-inertness   # one page, fast
python dev/scripts/viz/build.py --list                          # what is in the pack
```

`viz` is advisory and deliberately outside `make check`, exactly like
`docs-truth`. A scenario that stops reproducing fails this target, and that is a
finding to adjudicate — not a broken build.

## What it shows

| page | producer | what you are looking at |
|---|---|---|
| `converge`, `ladder`, `dead-code-*`, `all-oracles` | `examples/vopr.rs --trace-jsonl` | the deterministic simulator: every storage op, every injected failure, every crash, every clock jump, and the bucket contents at each boundary |
| `determinism` | same, under `--recheck-every 1` | one seed executed twice, compared op-for-op and world-for-world |
| `catch-it`, `wedge` | same, under `--break` | one deliberate defect and the one oracle that catches it |
| `model-conflict`, `model-events` | `tests/model_*.rs` under `PYPIRON_VIZ_GRAPH` | the exhaustive state spaces stateright checks, as a layered graph with the real merge `Verdict` on each edge |
| `region-*` | `tests/test_viz_region_trace.py` under `PYPIRON_VIZ_OUT` | the real binary against MinIO behind a fault proxy: region read failover, sampled from `/metrics` and `/ready` on a fixed tick |
| `scale` | `dev/scripts/viz/scale.json` | the numbers, each with the file and line it was measured at |

The fleet is lanes (`n0`…`nN` × buckets); the world is bucket columns of
`key → <len>:<sha8>`; a key whose value differs across buckets is outlined in
accent in every column, because divergence is the one thing that must be
impossible to miss.

## The three payload shapes

Frozen contract. The player ignores unknown fields, counts and skips unknown
event types, and treats a missing optional field as `null` — which is what lets
a producer add an event without shipping a new player.

**trace** — JSONL, one object per line. Line 0 is `meta`, the last line is
`summary`, and every line carries `t` (event type), `i` (0-based index, no
gaps), `sim` (RFC3339 sim clock, `null` for the region producer) and `ts`
(ms since start, `null` for the simulator, which has virtual time only).

Event types the simulator emits, all of them — this list is the contract, and
the producer is `examples/vopr.rs`:

| event | one per |
|---|---|
| `meta` | run (line 0) |
| `phase` | phase boundary — exactly three: `chaos`, `heal`, `oracle` |
| `step` | workload op drawn for a node that is up — a step skipped because the node was down emits nothing, so `n` has gaps |
| `round` / `pass` | heal round / drain pass inside it |
| `op` | **storage op, at admission** — this is the number a narration means by "storage ops" |
| `drop` | `_repl/` note blackholed by `--break fanout` (workload only, never during heal) |
| `crash_sched` / `crash` / `restart` | a scheduled kill, a node dying, a node returning |
| `clock` | sim-clock jump |
| `ack` | upload or delete the workload issued — `ok` says whether the server took it |
| `world` | bucket-contents delta (the only source of state) |
| `decide` | merge verdict a merge would actually apply (`Noop` is not emitted) |
| `repair` | audit-repair finding, with its classifier class |
| `oracle` | reach slot, at the end: `held`, `violated` or `not-executed` |
| `violation` | invariant violation string |
| `summary` | run (last line) |

The region producer emits `meta phase pin probe ready note ack world oracle
summary`.

**`summary` carries the recorded run's own verdict, not the process exit code.**
The simulator names the field `seed_exit_code` for exactly that reason. It is 0
(clean), 2 (this seed hit a violation) or 3 (`--recheck-every` re-executed the
seed to a different op trace or a different final world) — and it can never be
4, however the process exits, because `--require-reach` fails a *whole run* on
an oracle that never executed and no single seed can see that. The region
producer still calls the same field `exit_code`; `build.py` reads whichever is
present and pins it against the scenario's `expect_exit`.

`summary.trace_events` is likewise **not** the storage-op count: it is the
determinism hasher's record count, which is the admitted ops plus one tuple per
CAS outcome. Count `op` events for storage ops (`expect_event_counts.op`); the
two differ by a handful on any run with contended writes.

**Bucket state comes from `world` events and nothing else.** The normative fold:

```
state(T) = for each bucket b: fold every `world` event with i <= T and .b == b, in i order:
             if full: map = {}          // then apply put
             for k,v in put: map[k] = v
             for k in del: delete map[k]
```

An `op` is emitted at *admission*, before the call executes, so its byte effect
is unknown when it is written down. `op` events animate activity; they never
imply state. Guessing would put invented bytes on screen.

Values are `"<len>:<sha8>"`. The sha is load-bearing, not decoration:
`--break durability` plants same-length corrupt bytes, so length alone cannot
tell the convergence story.

**graph** — one JSON object: `counts`, `props`, `nodes[]`, `edges[]`, `paths[]`.
`tree:false` edges are the joins where independent interleavings reconverge on
the same state; they are drawn dashed because they are the exhaustiveness
argument. `raw` on each node is `format!("{state:#?}")`, the anti-rot guard: if a
field is added to the model and the hand-written projection misses it, the raw
dump still carries it.

**scale** — one JSON object: `stats[]`, `bars[]`, each with a mandatory
`source`.

`build.py` inlines a payload into the player as
`{"kind":"trace","lines":[...]}` inside
`<script type="application/json" id="payload">`. The same player also takes
`?src=a.jsonl,b.jsonl` (two values stack two panels on one clock) and a
drag-dropped file.

## The gates

They are why this is a Python driver and not a shell loop.

- **Staleness.** Every number a narration says out loud is pinned in
  `scenarios.json` and re-checked on every build: the exit code, the violation
  text, the storage-op count, the ack split, the objects left at quiescence, a
  named oracle's rendered verdict (`expect_oracles`), the node and edge counts
  of a graph. A number the narration says and the pack does
  not pin is the bug — that is how "22 client-visible acks" survived next to a
  verdict pill reading "10 acked", when the run answered 22 requests, accepted
  12 of them, and left 10 distinct filenames acked in the ledger. Every one of
  those three now has its own `expect_acks` key and its own name.
  A scenario that stops reproducing fails the build. A page
  that quietly goes green is the stale-evidence failure commit `1e80be3` already
  fixed once. Seeds are not stable across product commits, so every one is
  stored as `(commit, seed, flags)`.
- **Page/terminal agreement.** Every vopr `argv` in the pack carries
  `--recheck-every 0`. The determinism recheck re-executes a seed when
  `seed % --recheck-every == 0` (default 10) and the two executions are not
  symmetric on a page: the trace brackets one of them, the terminal's reach and
  merge-evidence tables are process-cumulative and count both. Paste the command
  such a page printed and every oracle count and every merge-evidence count came
  back at exactly twice the page's figure. Disabling the recheck makes the traced
  run the only run, so every number a page shows is a number the command prints. `determinism` is the deliberate
  exception — `--recheck-every 1`, the double execution as the subject — and it
  spends a caveat saying which of its numbers are one execution and which are
  two.
- **Number sources.** A `stats`/`bars` entry with an empty `source` fails, and
  so does one citing `dr-drill` or `make perf` — `dev/TESTING.md:149-153` and
  `:1336-1339` both say never to quote those outward.
- **Inertness.** Every traced profile runs twice, with and without
  `--trace-jsonl`, and the recorder must produce a byte-identical op
  interleaving and an identical printed summary. Same rule the repo applies to
  every other addition to that binary, applied to the recorder.
- **Expired excuses.** A `pending` scenario names the file it `needs`; the build
  fails once that file exists, so the excuse cannot outlive its cause.

A finding that gets adjudicated away becomes `status: retired` with the commit
that closed it, and the index renders it as closed rather than deleting it.

## Recording

The player has a **Record .webm** button: `canvas.captureStream(30)` +
`MediaRecorder` on the stage canvas, hidden entirely when the browser lacks
either. It records the canvas — the fleet lanes and the world table — not the
surrounding DOM.

For a full-page capture, drive the player in recording mode and use QuickTime or
OBS:

```
open '.local/viz/converge.html?chrome=off&autoplay=1&speed=4&loop=1'
```

`?chrome=off` hides the controls, `?theme=light|dark` pins the theme, and
`?at=N` opens on event N for a poster frame.

We add no browser recorder. The repo's one recording dependency is `vhs`
(`dev/scripts/demo-gif/record.sh`), which renders a *terminal* — ttyd plus
ffmpeg, no browser primitive, no screenshot-of-a-URL. Adding playwright or a
headless Chrome toolchain for a contributor-only tool would be the repo's first
browser toolchain, to save a screen recording.

## The side-by-side page

`dead-code-aligned` and `dead-code-partitioned` are the same seed and topology
with one flag between them. Two panels on one clock need two `?src=` payloads,
which a browser will only fetch over http:

```
cd .local/viz && python3 -m http.server 8009
# http://localhost:8009/player.html?src=data/dead-code-aligned.jsonl,data/dead-code-partitioned.jsonl
```

Every standalone page works from `file://`; only that one link needs the server.

## Files

```
dev/scripts/viz/player.html      the player: one file, vanilla ES, no deps
dev/scripts/viz/build.py         the driver: stdlib only, no shebang
dev/scripts/viz/scenarios.json   the pack, with every pinned expectation
dev/scripts/viz/scale.json       the numbers, with every source
.local/viz/*.html                the built pages (gitignored, never committed)
.local/viz/data/                 the raw payloads: `python3 -m json.tool data/scale.json`
```

Producer-side details — the `--trace-jsonl` flag, the `PYPIRON_VIZ_GRAPH` and
`PYPIRON_VIZ_OUT` env gates, and the inertness proof — are documented in
[dev/TESTING.md](../../TESTING.md).
