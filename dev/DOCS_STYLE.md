# Writing the user manual

This is how the `docs/` manual is written. It exists because the manual kept
getting written by the people who built pypiron, for people like themselves —
leading with *how it's built* and narrating *the mechanism*, when the reader
wants to know *what they get* and *what to do*.

One rule underneath all the others: **write for the user, not the builder.**
Everything below is that rule, made concrete.

## Voice

Write like a hybrid of Guido, Levelsio, and Linus: plain and correct, short and
shipped, blunt and precise. You're talking to a sharp engineer who's busy. No
fluff, no marketing, no hedging. Fragments are fine. Cut every word that isn't
load-bearing. The landing page (`docs/index.md`) is the reference for tone —
match it.

Delete on sight:

- **Filler:** just, simply, easily, basically, essentially, actually, really,
  very, quite, of course, note that, it's worth noting; "in order to" → "to".
- **Marketing:** just works, seamless, powerful, robust, blazing/lightning-fast,
  trivial, effortless, enterprise-grade.
- **Hedges:** usually, generally, typically, roughly, more or less, pretty much,
  in most cases — say the thing, or give the number.
- **Dead constructions:** "you can X" → "X"; "allows you to / lets you / enables
  you to" → cut or "can"; "there is/are X that" → "X"; "this means that" → ":";
  passive voice → active.

Then ask of every sentence: shorter? Two sentences that should be one? A
sentence that says nothing — cut it.

Sentence-level before/after:

- "pypiron aims to be the fastest, most reliable PyPI server available." → state
  the number: "5–90× faster than any other PyPI server."
- "Releases younger than a sliding 7-day window are held back, which means most
  supply-chain attacks are caught before they reach you." → "New releases wait 7
  days. Most attacks surface first."
- "You can point any number of nodes at one bucket with no coordination to set
  up." → "Point any number of nodes at one bucket. No coordination."

## Two trees, two audiences

- **`docs/`** is the user manual (mkdocs-material → GitHub Pages). Audience: a
  Python dev or platform engineer **evaluating or running** a package server.
  They do not care how pypiron is built.
- **`dev/`** is for contributors. Architecture, the storage-layout contract,
  benchmark methodology, design rationale. ([DESIGN.md](DESIGN.md) owns the
  internals.)

When a passage in `docs/` explains internals, you have three choices, in order
of preference: **reword it as a benefit**, **delete it**, or **move it to
`dev/`**. Never leave it as raw mechanism in the manual.

## The seven rules

1. **Lead with the outcome, never the architecture.** The first sentence of
   every page is a benefit or a task. Banned openers: "one binary, no database",
   "the files are truth", "indexes are regenerable views", "static site
   generator wearing a PyPI costume". If a reader of a *competitor* would care
   ("nothing to back up but a directory"), say *that* — not the implementation.

2. **Speak the reader's language.** No in-house vocabulary. See the word list
   below; every banned term has a plain replacement.

3. **Sell the outcome; the flag and the mechanism go to reference.** Say what a
   feature *buys* the reader. The flag name, endpoint, HTTP status code, and PEP
   number belong on the reference pages, not in the pitch. "Catches most
   supply-chain attacks before they reach you", not "`--exclude-newer`, tunable
   or `""` to disable".

4. **Cut the trivia, or move it to `dev/`.** Build system (maturin), arch
   matrices (`amd64/arm64/ppc64le/...`), image internals (distroless, uid
   65532), `RUST_LOG`, crate names, distributed-systems internals (Raft, leases,
   fencing tokens), the storage layout, and load-test mechanics are not
   user-facing.

5. **Happy path first; advanced and auth later.** Show the bare command that
   works, then a "When you need authentication / at scale" turn. The default
   case is open reads on a private network — don't front `--read-user` /
   `--read-pass` / the full role table.

6. **Have a spine.** State the ambition (the fastest, most reliable PyPI server)
   and name what pypiron replaces (pypicloud, pypiserver, bandersnatch, proxpi,
   devpi, dumb-pypi, simpleindex, pypiprivate). Make claims **concrete and
   quantified** — "3,026 installs/s on 2 vCPU, 5–90× the next server", not
   "reads need zero coordination".

7. **Say it once.** Each fact has exactly one home (map below); everything else
   links to it. Don't duplicate a quickstart, a credential table, or a backend
   setup across pages — they drift out of sync.

## Word list — say this, not that

| Don't write | Write |
| --- | --- |
| truth / truth tree | the stored files / what's on disk |
| sidecar | metadata file / per-file metadata |
| regenerable view / materialized view | the index is rebuilt from the files (or cut) |
| dirty marker, delta segments, shards, leader compaction | *(internal — cut from the manual)* |
| claimed at first write / the claim is durable | reserved on first upload / stays reserved |
| bulk-sync an allowlist | pre-load an approved package list |
| closed-world resolution | point clients at one index |
| origin exclusivity / `.origin` | each name is private or public, never both |
| heals / self-heals | recovers / rebuilds itself |
| CAS conflicts, write intents, fencing tokens, Raft, lease | *(internal — cut)* |
| O(files) not O(bytes) | fast even with millions of packages |
| knee, server-bound, load fleet | peak throughput *(mechanics → benchmark methodology)* |
| presigned redirect / 302 | hands the download straight to object storage |
| Gmail-style subaddressing | username tags (`reader+billing-api`) |
| PEP 503 / 691 / 658 / 700 / 740 / 792 | name the behavior; keep PEP numbers only on `reference/standards.md` |

PEP numbers, crate names, and storage internals are *correct* on
`reference/standards.md` and in `dev/` — that's their home. They're noise
everywhere else.

## Say it once — who owns what

| Fact | Owner | Everyone else |
| --- | --- | --- |
| Start / publish / install loop | `index.md` | link, don't re-teach |
| Storage backends + credentials | `concepts/storage.md` | `guides/deploy.md` shows the minimal "point at a bucket" + links |
| Auth roles & model | `concepts/authentication.md` | `reference/configuration.md` lists the flags; others link |
| Mirror reconcile / re-sync detail | `concepts/mirroring.md` | `guides/air-gapped-mirror.md` is task steps + links |
| Download stats | `concepts/download-stats.md` | `reference/api.md` + `configuration.md` keep a one-liner + link |
| Mirror-selection & `<when>` formats | `reference/configuration.md#mirror-selection` | show one example + link |
| Throughput numbers + chart | `reference/benchmarks.md` | `index.md` hooks with the headline once |
| Storage layout / metadata schema / markers | `dev/DESIGN.md` | no user page reproduces it |

## Before / after

> **Before:** One binary, no database. pypiron serves your private uploads,
> mirrors public PyPI on demand, and bulk-syncs allowlists — all behind one URL
> and one namespace. Truth is files on disk or object storage; indexes are
> regenerable views.

> **After:** Serve your team's private packages and a full cache of public PyPI
> from one URL — fast enough to replace pypiserver, bandersnatch, or pypicloud,
> simple enough to run as a single binary you point at a folder or an S3 bucket.

## Checklist before you commit a doc change

- First sentence is a benefit or a task — not an architecture fact.
- No word from the "don't write" list survives (outside `standards.md` / `dev/`).
- Every feature says *why it matters* before *which flag*.
- The simplest working command comes first; auth/advanced is a later turn.
- Nothing here is already owned by another page (check the map).
- `make docs` builds clean (no broken cross-links).
