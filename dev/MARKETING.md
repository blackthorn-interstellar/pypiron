# pypiron marketing — insights & plan

Recovered from a Fable-model Claude Code session (2026-07-06/07) that closed on a
crash. Prompted by a boosted tweet that got ~0 engagement despite thousands of
views. Three cheap research subagents (Sonnet ×2, Haiku ×1) studied comparable
devtool launches, buyer pain points, and distribution channels; findings below.

## The core diagnosis

**pypiron doesn't have a marketing problem — it hasn't launched yet.** The repo
had 1 star, 0 forks at the time. A launch that lands on a 1-star repo wastes the
traffic. Fix social proof *before* spending any attention.

### Why the boosted tweet died (5 causes, only one was targeting)
1. **Paid interruption ads don't work for OSS devtools.** Of 9 tools studied
   (ruff, uv, rye, pixi, pnpm, caddy, minio, verdaccio, athens), *zero* grew via
   paid ads. Developers are ad-blind; the "Promoted" tag is a credibility tax.
2. **Wrong moment.** Server infra is adopted when a trigger fires (airgap,
   compliance, private packages, a supply-chain scare) — not while scrolling.
   Ads can't create the trigger; you must be *findable* when it fires.
3. **Five claims in 280 chars** (fastest, easiest, all-of-PyPI, mirroring,
   supply-chain, cloud storage). Every breakout launch led with exactly ONE
   claim. The copy also had a grammar glitch reading as spam, and no link.
4. **Targeting shrank a tiny audience further.** "Males 21+, Bay Area" filtered a
   global niche to a sliver. The charliermarsh-lookalike instinct was right; the
   demo/geo filters were noise.
5. **No social proof at the landing zone** (1 star).

## Strategic frame

- **Server infra tools don't go viral.** Verdaccio (npm-registry twin of
  pypiron) peaked at **47 HN points ever**; Athens (Go module proxy) never cracked
  3. They grew by being the boring known-good choice at the moment of need:
  Docker pulls, runbooks, Stack Overflow answers, and **embedding in other
  projects' CI** (verdaccio is the test registry for create-react-app, Storybook,
  Babel, Angular CLI).
- **But pypiron has the two things that DO produce breakout launches:**
  1. *A single audacious benchmark claim* (the ruff/uv pattern — ruff's repost
     hit 148 points on "extremely fast, written in Rust" alone).
  2. *A live news wave to ride* (the Caddy pattern — launched one day after Let's
     Encrypt as "the web server with automatic HTTPS"). pypiron's wave is the
     ongoing **2026 PyPI supply-chain campaign**.
- **One claim per channel.** Speed is the launch hook. Quarantine is the
  evergreen content engine. Proof is the credibility floor under both. Never mix
  them in a headline.

### The third pillar — "proven, not just fast"

The market is now saturated with benchmark-led Rust rewrites, and buyers have
learned to discount them. The sharpest version of the skepticism: a rewrite ships
nice-looking benchmarks and graphs, and you still have no idea whether it's
actually good or usable. That reaction is *correct*, and it's the objection every
speed claim now walks into.

pypiron's counter is not a louder number — it's that every claim has a runnable
harness behind it, and we link straight to it. The proof list is the pillar:

- Real binary driven by 8 real clients over HTTP; parsers checked against all
  17.1M files ever uploaded to PyPI; chaos suites that `SIGKILL` mid-write and
  inject upstream faults and show the tree never corrupts; nightly coverage-guided
  fuzzing; every PR gated on `cargo-audit`; and benchmark rigs anyone can re-run.
- **Lead with the proof list and the live demo, *before* any graph.** The graph is
  the hook only after the reader believes the graph. Order: "here's a server you
  can hit right now, here's how we know it's not lying, *then* here's how fast."
- **This is the pillar for the uv community specifically** (Astral Discord
  #showcase). That audience builds Rust tooling, has seen every benchmark-rewrite
  pitch, and rewards receipts over adjectives. Speed opens the door there; proof
  is what makes them trust it enough to run it.

## Key research facts (load-bearing, spot-checked)

- **Ruff's founder HN launch flopped** (Aug 30 2022, 4 points, 0 comments). A
  third party reposted identical content the next day → **148 points, 46
  comments** (~35× the founder's own post). *Verified via HN Algolia API.*
  Lesson: if a Show HN gets <10 points, **reposting a few days later is normal
  and expected.**
- **uv (2024) harvested ruff's (2022) warm audience** — 647 points, the biggest
  launch in the set. Sequential launches from the same team compound.
- **litellm compromise, Mar 24 2026** (~95M monthly downloads): maintainer
  account hijacked, malicious `.pth` auto-executes on Python startup (no import
  needed), harvests SSH/cloud/crypto creds. Timeline: 10:39am published →
  11:48am first flag → 1:38pm PyPI quarantined. **Tens of thousands of installs
  in that ~40–60 min window. A 7-day quarantine would have caught it** — and
  telnyx (Mar 27) and elementary-data (Apr 24) too. Chainguard recommends a
  7-day cooldown *verbatim* as best practice.
  - **Honesty caveat:** those incidents are largely ONE campaign (actor
    "TeamPCP"). Frame as "one active campaign, monthly cadence" — don't overstate
    as four independent proof points.
- **bandersnatch full mirror is ~18TB** (not 30TB) and growing — the contrast
  point for pypiron's on-demand proxy-only-what's-used model.
- **devpi is actively maintained** — position against operational complexity
  (staging indexes, inheritance, web+server split), *never* claim abandonment.
- **pypiserver** = too minimal (no caching/proxy/security) — the opposite gap.
- **Artifactory** = cost-surprise complaints ($150→$600–800/mo, billing on
  storage+transfer). **Nexus** = config fragility, proxy silently bypassed.
  **CodeArtifact** = single-region.

## The plan

### Phase 0 — pre-launch hardening (1–2 weeks)
- **Raise the social-proof floor** to a few dozen stars organically before any
  launch.
- **Write two launch assets:**
  - *Benchmark post*: "3,026 installs/s on 2 vCPUs — how we measured it."
    Reproducible, skeptic-proof, from the existing `bench/install/` setups vs 5
    competitors. This is the Show HN backbone.
  - *Security post*: "litellm was malicious on PyPI for 40 minutes. A 7-day
    cooldown would have caught it." Minute-by-minute timeline (public in litellm
    issue #24518) → how quarantine-by-default works.
- **Make the live demo prominent** (already runs a public EC2+S3 mirror). Show HN
  requires a working demo; "try `uv add --index https://…` right now" is a great
  one. Put it at the top of the README.

### Phase 1 — coordinated organic launch (weeks 3–4)
- **Show HN**, Tue–Thu morning ET, one-claim neutral title:
  *"Show HN: Pypiron – a PyPI server fast enough that one box could serve all of
  PyPI."* First comment = backstory + benchmark + live demo + why Rust/no-DB.
  Answer every comment for the first 6 hours. **<10 points → repost days later.**
- **Same week, one per day:** r/Python (read sidebar, disclose authorship, frame
  as "I built this"), discuss.python.org #packaging (cite implemented standards —
  `docs/reference/standards.md`), Lobsters (**needs an account >70 days old —
  create it now**), Astral Discord #showcase (your audience literally is uv users).
- **Submissions (one afternoon):** PyCoder's Weekly form
  (pycoders.com/submissions), **Python Bytes** email (contact@pythonbytes.fm —
  features 2–3 small tools *every week*, single best-fit channel), Console.dev
  (hello@console.dev).

### Phase 2 — content engine + ecosystem embedding (months 2–4)
- **Incident-response content within 48h of each new PyPI incident** (~monthly).
  Timeline + blast radius + "what a cooldown/private index would have done." The
  repeatable Caddy pattern; **your #1 recurring channel.**
- **awesome-selfhosted PR** (exact category fit, CI-checked). awesome-python later
  (needs 100+ stars, >1 month age).
- **The verdaccio play (highest-leverage long game):** make pypiron the easiest
  local PyPI for *other projects' CI* — ship a GitHub Action / one-liner fixture
  ("throwaway index in CI in 2 seconds"). A single-binary instant-start server is
  better suited to this than verdaccio ever was.
- **SEO/comparison pages on pypiron.com** for moment-of-need searches using
  verbatim pain phrases: "air-gapped pip install", "self-hosted PyPI mirror",
  "private Python package hosting", "devpi alternative", "bandersnatch too big".
  One page per query.

### Phase 3 — slow burns (Q4 2026 →)
- **PyCon US 2027 CFP opens ~Oct 2026** (all 2026 CFPs are closed). Angle:
  "Serving all of PyPI from one box" or the quarantine story. DevOpsDays roll
  year-round with a lower bar — good practice runs.
- **Talk Python guest pitch** once there's adoption to narrate. **PackagingCon no
  longer exists — skip it.**

### Twitter's actual role
Keep it, spend **$0**. Build in public from the personal account: benchmark
charts, incident-timeline threads, changelog moments; reply usefully in
uv/pip/packaging threads. Rewritten tweet (one claim + proof + link):

> pypiron sustained 3,026 package installs/s on 2 vCPUs — enough that one 8-core
> box could serve all of PyPI's traffic. Single Rust binary, no database.
> [benchmark chart] github.com/blackthorn-interstellar/pypiron

## Metrics & anti-goals
- **Track:** Docker pulls, `pypiron` PyPI downloads, live-demo traffic. For server
  infra these are the real adoption signals; **stars are a vanity trailing
  indicator.**
- **Anti-goals:** no paid ads for ≥90 days (zero OSS devtools grew that way), no
  more boosts, no multi-claim copy anywhere.

## Best-fit channels, ranked
1. **Python Bytes** (contact@pythonbytes.fm) — weekly tool features, perfect fit
2. **awesome-selfhosted** PR — exact match, high traffic
3. **Show HN** — reaches CTOs/SREs, only with a working demo
4. **PyCoder's Weekly** form — explicit, 100K+ subscribers
5. **Astral Discord** — uv community, low friction
