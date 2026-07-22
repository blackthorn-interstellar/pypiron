export const meta = {
  name: 'vopr-verified-fix',
  description:
    'Reproduce one VOPR finding, fix the protocol in src/, and prove the fix cures the cause without weakening any invariant before it may commit. The verify gate is the point: an autonomous fixer must never turn a red seed green by loosening a check.',
  phases: [
    { title: 'Diagnose' },
    { title: 'Fix' },
    { title: 'Verify' },
    { title: 'Decide' },
  ],
}

// args:
//   repro:     "cargo run --release --example vopr -- --seed N --nodes.. --buckets.. --ops.. --fail-percent.."  (required)
//   signature: the seed-agnostic violation signature (issue title)          (optional, sharpens review)
//   issue:     GitHub issue number to reference/close                        (optional)
//   commit:    true to push to master on a clean pass; false = dry run       (default false)
const repro = args && args.repro
if (!repro) throw new Error('args.repro (the vopr repro command) is required')
const signature = (args && args.signature) || '(none supplied)'
const issue = (args && args.issue) || null
const commit = !!(args && args.commit)
const seed = (repro.match(/--seed\s+(\d+)/) || [])[1] || 'unknown'

// ---- schemas ----------------------------------------------------------------
const DIAGNOSIS = {
  type: 'object',
  additionalProperties: false,
  required: ['reproduced', 'in_protocol', 'root_cause', 'src_files', 'approach'],
  properties: {
    reproduced: { type: 'boolean', description: 'Did the exact repro command fail as reported?' },
    in_protocol: {
      type: 'boolean',
      description:
        'Is the defect in the production protocol (src/), as opposed to only the checker/classifier in examples/vopr.rs? If the checker itself is wrong, say so — that is a different, human-reviewed change.',
    },
    root_cause: { type: 'string' },
    src_files: { type: 'array', items: { type: 'string' } },
    approach: { type: 'string', description: 'How to fix the CAUSE in src/, not mask the symptom.' },
  },
}
const FIX = {
  type: 'object',
  additionalProperties: false,
  required: ['applied', 'checks_pass', 'seed_now_passes', 'sweep_green', 'diff', 'notes'],
  properties: {
    applied: { type: 'boolean' },
    checks_pass: { type: 'boolean', description: '`make check` is green.' },
    seed_now_passes: { type: 'boolean', description: 'The original failing seed now passes.' },
    sweep_green: { type: 'boolean', description: 'A broad --rotate sweep stayed all-green.' },
    diff: { type: 'string', description: 'Unified `git diff` of the fix (patch text).' },
    notes: { type: 'string' },
  },
}
const LOCUS = {
  type: 'object',
  additionalProperties: false,
  required: ['pass', 'weakens_checker', 'touches_src', 'reason'],
  properties: {
    pass: { type: 'boolean' },
    weakens_checker: {
      type: 'boolean',
      description:
        'True if the diff deletes/loosens any invariant assertion, the repair classifier, a class threshold, or a violation gate in examples/vopr.rs (additive breadcrumb *recording* is fine; removing/relaxing a *check* is not).',
    },
    touches_src: { type: 'boolean', description: 'Does the fix change production code in src/?' },
    reason: { type: 'string' },
  },
}
const REVIEW = {
  type: 'object',
  additionalProperties: false,
  required: ['genuine_cure', 'weakens_invariant', 'reasoning'],
  properties: {
    genuine_cure: {
      type: 'boolean',
      description: 'Fixes the root CAUSE (e.g. a missing durable breadcrumb before a truth mutation), not the symptom.',
    },
    weakens_invariant: {
      type: 'boolean',
      description: 'Weakens/deletes a check, over-heals to hide the bug, or narrows the classifier so the bug stops being reported.',
    },
    reasoning: { type: 'string' },
  },
}
const EMPIRICAL = {
  type: 'object',
  additionalProperties: false,
  required: ['applies_clean', 'checks_pass', 'seed_passes', 'all_green', 'failing_seeds'],
  properties: {
    applies_clean: { type: 'boolean', description: 'The diff applies to a clean HEAD worktree.' },
    checks_pass: { type: 'boolean' },
    seed_passes: { type: 'boolean' },
    all_green: { type: 'boolean', description: 'The broad rotate sweep found no violations.' },
    failing_seeds: { type: 'array', items: { type: 'string' } },
  },
}
const COMMIT = {
  type: 'object',
  additionalProperties: false,
  required: ['pushed', 'sha', 'message'],
  properties: {
    pushed: { type: 'boolean' },
    sha: { type: 'string' },
    message: { type: 'string' },
  },
}

// ---- Diagnose ---------------------------------------------------------------
phase('Diagnose')
const diagnosis = await agent(
  `A VOPR soak finding needs fixing. Reproduce and diagnose it — do NOT fix yet.\n\n` +
    `Repro (deterministic): \`${repro}\`\nViolation signature: ${signature}\n\n` +
    `Steps:\n` +
    `1. Run the repro command and confirm it fails with that violation.\n` +
    `2. Read the relevant protocol code in src/ (the event protocol: worker, replicate, sidecar, storage) and the checker in examples/vopr.rs to understand what invariant broke and WHY.\n` +
    `3. Decide whether the defect is in the production protocol (src/) or in the checker/classifier itself (examples/vopr.rs). A checker bug is a legitimate but different, human-reviewed change — flag it, don't paper over it.\n` +
    `Return the diagnosis.`,
  { schema: DIAGNOSIS, phase: 'Diagnose', effort: 'high' },
)
if (!diagnosis || !diagnosis.reproduced) {
  return { seed, issue, outcome: 'not-reproduced', diagnosis }
}
if (!diagnosis.in_protocol) {
  // The checker/classifier itself looks wrong. Never let the autonomous loop
  // edit its own invariants — escalate to a human.
  return { seed, issue, outcome: 'escalate-checker-suspect', diagnosis }
}

// ---- Fix (isolated worktree; returns a patch, never touches the live tree) ---
phase('Fix')
const fix = await agent(
  `Fix this VOPR finding in the PROTOCOL (src/). You are in an isolated worktree at HEAD.\n\n` +
    `Repro: \`${repro}\`\nDiagnosis: ${JSON.stringify(diagnosis)}\n\n` +
    `HARD RULES:\n` +
    `- Fix the CAUSE in src/. Do NOT weaken, delete, or relax any invariant, the repair classifier, a class threshold, or a violation gate in examples/vopr.rs. Do NOT add "healing" whose only effect is to hide the violation. Additive, honest breadcrumb *recording* in the harness is allowed only if the diagnosis shows the harness was under-observing; prefer touching src/.\n` +
    `- If the finding cannot be fixed without weakening a check, STOP and return applied=false with notes — that is an escalation, not a failure.\n\n` +
    `Then verify your own work:\n` +
    `1. \`make check\` must pass.\n` +
    `2. Re-run the exact repro command — it must now pass (exit 0, all invariants held).\n` +
    `3. Run a broad sweep: \`cargo run --release --example vopr -- --forever --rotate --max-secs 120\` — it must stay all-green (0 failed, 0 determinism violations).\n` +
    `Return applied, the three check results, and the unified \`git diff\` of your change.`,
  { schema: FIX, phase: 'Fix', effort: 'high', isolation: 'worktree' },
)
if (!fix || !fix.applied || !fix.diff) {
  return { seed, issue, outcome: 'fix-escalated', diagnosis, fix }
}

// ---- Verify: mechanical locus gate + adversarial panel + independent empirical
phase('Verify')
const locusP = agent(
  `Mechanically audit this fix diff for INVARIANT WEAKENING — the one thing an autonomous fixer must never do.\n\n` +
    `Violation being fixed: ${signature}\n\nDiff:\n\`\`\`diff\n${fix.diff}\n\`\`\`\n\n` +
    `Reject (pass=false) if the diff, in examples/vopr.rs, deletes or loosens any assertion/invariant, the repair classifier (classify_round / class thresholds), or a violation gate — OR if it "fixes" the seed without changing any src/ production code. Additive breadcrumb recording is acceptable. Be strict; when unsure, reject.`,
  { schema: LOCUS, phase: 'Verify', effort: 'high', label: 'verify:locus' },
)
// Two independent adversarial reviewers with distinct hunting lenses; both must
// clear the fix. Default Claude agents so this runs anywhere (a cloud routine
// may not have custom agent types). To harden with cross-vendor independence,
// add `agentType: 'codex-exec'` / `'grok-exec'` where those are available.
const REVIEWERS = [
  {
    name: 'symptom-hunter',
    lens: 'Assume this fix is a cheat. Try to show it MASKS the symptom — over-heals, adds aggressive re-derivation that merely hides the ordering/consumption bug, or narrows the classifier so the violation stops being reported — rather than curing the cause.',
  },
  {
    name: 'invariant-guardian',
    lens: 'Assume this fix weakens the checker. Try to show it deletes or relaxes an invariant, a class threshold, or a violation gate in examples/vopr.rs — anything that makes the harness catch LESS than before.',
  },
]
const panelP = parallel(
  REVIEWERS.map((r) => () =>
    agent(
      `Adversarially review this VOPR protocol fix. ${r.lens}\n\n` +
        `Violation: ${signature}\nDiagnosis: ${JSON.stringify(diagnosis)}\n\nDiff:\n\`\`\`diff\n${fix.diff}\n\`\`\`\n\n` +
        `Report genuine_cure (fixes the root cause) and weakens_invariant. Default genuine_cure=false when unsure.`,
      { schema: REVIEW, phase: 'Verify', label: `verify:${r.name}` },
    ),
  ),
)
const empiricalP = agent(
  `Independently verify this fix diff on a CLEAN HEAD worktree (you are isolated).\n\n` +
    `Repro: \`${repro}\`\n\nDiff:\n\`\`\`diff\n${fix.diff}\n\`\`\`\n\n` +
    `1. \`git apply\` the diff (report applies_clean).\n2. \`make check\`.\n3. Re-run the exact repro — must pass.\n4. \`cargo run --release --example vopr -- --forever --rotate --max-secs 180\` — must be all-green; list any failing seeds.`,
  { schema: EMPIRICAL, phase: 'Verify', effort: 'high', isolation: 'worktree', label: 'verify:empirical' },
)
const [locus, panelRaw, empirical] = await Promise.all([locusP, panelP, empiricalP])
const panel = panelRaw.filter(Boolean)

const unanimousCure =
  panel.length === REVIEWERS.length && panel.every((r) => r.genuine_cure && !r.weakens_invariant)
const locusOk = !!(locus && locus.pass && locus.touches_src && !locus.weakens_checker)
const empiricalOk = !!(
  empirical &&
  empirical.applies_clean &&
  empirical.checks_pass &&
  empirical.seed_passes &&
  empirical.all_green
)
const verdict = {
  seed,
  issue,
  signature,
  locus,
  panel,
  empirical,
  gates: { locusOk, unanimousCure, empiricalOk },
  diff: fix.diff,
}

// ---- Decide -----------------------------------------------------------------
phase('Decide')
if (!(locusOk && unanimousCure && empiricalOk)) {
  return { ...verdict, outcome: 'rejected' }
}
if (!commit) {
  return { ...verdict, outcome: 'verified-not-committed' }
}
const committed = await agent(
  `The fix for VOPR seed ${seed} passed every gate. Apply it to the real checkout and commit to master, then push.\n\n` +
    `Diff:\n\`\`\`diff\n${fix.diff}\n\`\`\`\n\n` +
    `Use a conventional commit (spell out \`fix\`): state the root cause and how it was addressed${
      issue ? `, and include "Closes #${issue}"` : ''
    }. Run \`make check\` once more before committing. Return the pushed sha and message.`,
  { schema: COMMIT, phase: 'Decide', effort: 'high' },
)
return { ...verdict, outcome: committed && committed.pushed ? 'committed' : 'commit-failed', committed }
