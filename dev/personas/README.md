# Persona simulation fleet — docs QA

Simulated readers that find confusing and distracting docs the way real
evaluators do: by stalling, eye-rolling, and leaving. Marketing personas that
actually read.

## The protocol

Two roles, never combined:

- **Actor** — an agent that reads ONE persona file, becomes that person, and
  reads a docs page in character. It emits a first-person behavioral **trace**
  (what it skims, where it stalls, what it Ctrl-Fs, when it quits mid-page),
  NOT a review. No feedback lists, no balance, no completeness. A smooth read
  with nothing notable is a valid trace. If an exit condition triggers, the
  actor stops mid-page — politely finishing is a protocol violation.
- **Analyst** — a separate agent that converts raw traces into structured
  events: `(actor, quote from the page, event)` where event is one of
  confused / bored-knows-this / mistrusts / misled / left / cross-examined /
  smooth. Extraction only; no opinions, no severity.

Iron rules, learned the hard way:

1. **Actor briefs contain zero product taxonomy.** The moment a brief says
   "the three sources" or "the byte gate", the actor confirms the writer's
   frame instead of testing it.
2. **Actors are never told to find defects.** The task framing is a ticket and
   a time budget, not a review. "Review this page" produces an assistant with
   a costume; a ticket produces a reader.
3. **The persona spec is a knowledge inventory, not demographics.**
   *Knows* (prose covering it reads as filler), *doesn't know* (prose assuming
   it reads as confusion), *mistrusts*, prior tools, time budget, and explicit
   exit conditions. Age and employer are set dressing; the inventory is the
   instrument.
4. **Mix models across actors** (Opus / Grok / Codex). Different models have
   different sycophancy profiles; agreement across models is signal.

## The benchmark (`bench/`)

Before trusting the fleet, prove it isn't one polite reviewer in six costumes.
`bench/seeded-concepts.md` is a deliberately corrupted copy of the concepts
page with planted defects; `bench/answer-key.md` says which personas must hit,
and — as important — which must NOT flag each plant. `bench/clean-page.md` is
a deliberately fine page: the correct trace for it is "fine, no notes", and an
actor that manufactures findings there fails.

Score per persona: sensitivity (hit the plants aimed at you), specificity
(ignore the plants aimed at others), and the fleet-level check: **span overlap
across personas**. High overlap in what gets flagged = window dressing, no
diversity of thought. The python-hater persona is a calibration probe for
disagreeableness: if it finishes the page and gives balanced feedback, the
fleet is broken.

Re-run the benchmark whenever the actor models change. The seeded pages test
persona fidelity, not doc freshness — they do not need to track the live docs.

## Findings pipeline

Validated actors read the real manual → analyst extracts events →
bored-knows-this and confused events become candidate edits → the usual
adversarial judge pass (churn on good copy is a defect) → apply.
