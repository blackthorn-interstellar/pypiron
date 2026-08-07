---
name: persona-review
description: Run the validated persona-simulation fleet over one or more docs pages and apply confirmed findings. Use when a user-facing page changed, before committing it, or when asked whether a page has been reviewed.
---

# Persona review

Simulated readers (dev/personas/) review a docs page the way real evaluators
do: by stalling, eye-rolling, and leaving. Protocol and iron rules live in
`dev/personas/README.md` — read it first. Never put product taxonomy or
"find defects" framing in an actor brief.

## 1. Pick actors from the routing table

| Page | Actors |
| --- | --- |
| README.md / docs/index.md | platform-veteran, python-hater, artifactory-refugee, junior-dev |
| docs/concepts.md | all six |
| docs/security.md | security-lead, platform-veteran |
| docs/testing.md | security-lead, grumpy-sre, platform-veteran |
| docs/guides/standard-cloud.md | grumpy-sre, artifactory-refugee, junior-dev |
| docs/guides/air-gapped.md | airgapped-ops |
| docs/guides/multi-region.md | grumpy-sre, artifactory-refugee |
| docs/guides/migrate.md | artifactory-refugee, junior-dev |
| docs/compare/* | artifactory-refugee, python-hater, platform-veteran |
| docs/reference/configuration.md | grumpy-sre, platform-veteran (prose only — tables are tables) |

platform-veteran (the taste probe) is cheap and catches frame defects on any
page; include it whenever in doubt. Opus is the primary actor model; Grok is
a second opinion only (benchmarked thinner).

## 2. Run actors, then one analyst

Spawn each actor as a background agent (or one Workflow if the user opted
into ultracode). Actor prompt shape — verbatim skeleton, fill the brackets:

> Persona file: dev/personas/<persona>.md — read it and become that person
> completely. Scenario: [a ticket + a time budget in their world; never
> "review this"]. Read: [absolute page path]. Produce a first-person,
> present-tense trace: skips, stalls ("wait — what's X?"), eye-rolls,
> Ctrl-Fs, mutters; STOP mid-page if an exit condition or the clock
> triggers — politely finishing is a protocol violation. No feedback lists,
> no balance; a smooth read with nothing notable is a valid trace. End with
> the in-character verdict.
> (platform-veteran instead uses its restatement method: one-pager
> restatements per section, margin notes only where words resist.)

Then one analyst agent converts all traces to events: (actor, exact page
quote, event ∈ confused | bored-knows-this | mistrusts | misled |
cross-examined | left | smooth | praised, trace evidence). Extraction only.

## 3. Judge, apply, verify

Adversarially judge each negative event yourself (or one judge agent):
confirm only if a sharp reader genuinely stalls AND the fix keeps every fact
and is at least as terse — churn on good copy is a defect. Apply confirmed
fixes, run `make docs` (strict), commit. Findings that are product gaps or
positioning calls go to the user, each self-contained, not into edits.

## 4. Record it

Update `dev/personas/REVIEWS.md`: page, actors, date, fix commit. This
ledger is the answer to "has X been reviewed" — keep it true.

## Fleet health

Re-run the planted-defect benchmark (`dev/personas/bench/`, scored against
`answer-key.md`) whenever actor models change. A fleet that stops abandoning
pages or starts agreeing across personas is window dressing again.
