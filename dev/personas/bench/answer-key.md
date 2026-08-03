# Benchmark answer key

Planted defects in `seeded-concepts.md`, with per-persona expectations.
"MUST hit" = a validated persona reacts to this span (the event type in
parentheses). "Must NOT flag" = a validated persona passes it without
negative reaction — flagging it is a false positive that costs specificity.
Personas not listed may react or not; no score either way.

| ID | Planted text (locator) | MUST hit | Must NOT flag |
| -- | --- | --- | --- |
| D1 | "Object storage like S3 keeps every file redundantly across multiple data centers…" (Where packages live) | artifactory-refugee, grumpy-sre (bored-knows-this) | junior-dev, airgapped-ops |
| D2 | "Restart policy on `/ready`, load balancer on `/health`" — endpoints deliberately swapped (What it tells you) | grumpy-sre (misled/caught-wrong) | junior-dev |
| D3 | "This makes pypiron the remote repository for PyPI; existing virtual repository layouts carry over unchanged." (Connected: cache on demand) | junior-dev, airgapped-ops (confused) | artifactory-refugee |
| D4 | "Together, these gates make supply-chain attacks against your registry effectively impossible." (What it keeps out) | security-lead (cross-examined/mistrusts) | junior-dev |
| D5 | "Install tokens require a one-time online activation against pypiron.com…" (Who can do what, mid-paragraph) | airgapped-ops (hard exit), artifactory-refugee (mistrusts) | — |
| D6 | "Names are normalized per PEP 503 and served as PEP 691 JSON projections…" (intro) | junior-dev (confused) | — |

## Calibration probes

- **CAL1 — python-hater × the real landing page.** Pass: visceral, early,
  in-voice rejection or grudging concession; does NOT finish politely, does
  NOT produce balanced feedback. Fail: a courteous complete review.
- **CAL2 — grumpy-sre × `clean-page.md`.** Pass: "fine, worth a spike"-class
  trace with no manufactured findings. Fail: a list of invented nits.

## Fleet-level checks

- **Overlap:** compare flagged spans across personas on the seeded page. The
  personas' hit sets must diverge along their knowledge inventories; high
  overlap everywhere = one reviewer in six costumes.
- **Abandonment exists:** at least one actor (airgapped-ops at D5; the hater
  on CAL1; grumpy-sre at 3 minutes) must stop mid-page. A fleet where
  everyone finishes everything is not simulating readers.

Scoring: per persona, sensitivity = MUST-hit rows hit; specificity =
must-NOT-flag rows passed. Validate a persona at full sensitivity + at most
one specificity miss. Re-run on any actor-model change.
