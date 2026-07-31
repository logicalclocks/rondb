# GSN Signal-Scope Audit — Workflow Script Plan

Concrete design for the fan-out Workflow script that classifies the remaining
unclassified GSNs. Implements Phases 1-4 of `gsn_scope_audit_plan.md` (calibrate
→ extract → adversarially verify → coverage/sort). Human review (Phase 5) and
integration (Phase 6) happen outside the script.

Method contract: `.claude/skills/gsn-signal-scope-audit/SKILL.md`.
Ground truth: `signal_scope_calibration_set.md` (60 verified entries).

**Implemented as `gsn_scope_audit_workflow.js`** (same dir), run via
`Workflow({scriptPath: ".../gsn_scope_audit_workflow.js"})`. Resolved build
decisions: **args-driven** worklist (`args.gsns`; no self-enumeration); the
classification **method is inlined** in the script (self-contained — does not
depend on the `.claude/skills/` skill or on reading `SignalScopes.hpp`); the 60
calibration pairs are **embedded** in the script for automatic pass/fail; both
modes derive scope from send sites and ignore existing declarations. **Validated
2026-07-29:** a `mode:"calibrate"` run passed 60/60 (`pass:true`, empty
`mismatches`/`tighteningErrors`/`missing`) — the inlined method reproduced every
verified classification, traps included. Open decisions below are resolved
accordingly.

## What the script does / does not do

- DOES: partition the work-list, run per-GSN send-site extraction + first-pass
  classification in parallel, adversarially verify every restrictive proposal,
  check coverage, and emit a bucketed, evidence-cited results table.
- DOES NOT: land any `DECLARE_SIGNAL_SCOPE` (Phase 6, human-gated), decide final
  truth (proposals only), or run enforcement.

## Inputs (`args`)

```
{
  gsns: ["GSN_...", ...],   // work-list from Phase 0; if omitted, script greps GlobalSignalNumbers.h
  mode: "calibrate" | "audit",  // calibrate = run against the 60 known, compare; audit = classify unknowns
  batchSize: 12             // GSNs per extraction agent
}
```

## Structured schemas (force clean, mergeable output)

EXTRACT (Phase 2), one object per GSN:
```
{
  gsn: string,
  proposedScope: "Local" | "Remote" | "Management" | "External" | "Unclassified",
  confidence: "high" | "medium" | "low",
  versionVariance: "none" | string,
  sendSites: [{ file: string, line: number, senderNodeType: "DB"|"API"|"MGM"|"local", crossNode: boolean }],
  reasoning: string
}
```

VERIFY (Phase 3), one per restrictive proposal:
```
{ gsn: string, refuted: boolean, counterExampleSendSite: string|null, looserScope: string|null, note: string }
```

## Script structure

```
export const meta = {
  name: 'gsn-scope-audit',
  description: 'Classify NDB GSNs into SignalScope with adversarial verification',
  phases: [{title:'Calibrate'},{title:'Extract'},{title:'Verify'},{title:'Coverage'}],
}

// Phase 0 done inline before the workflow (work-list passed via args.gsns)
const SKILL = <inline the skill's method + the four discriminator heuristics>
const batches = chunk(args.gsns, args.batchSize)

// PHASE 1 — calibrate gate (only when mode === 'calibrate')
// Runs the SAME extract logic on the 60 known GSNs; compares to ground truth.
// (see "Blind calibration" below — must not read existing declarations)

// PHASE 2+3 — pipeline: extract each batch, then verify restrictive findings per GSN
const results = await pipeline(
  batches,
  batch => agent(extractPrompt(SKILL, batch), {phase:'Extract', schema: EXTRACT_ARRAY}),
  (extracted) => parallel(
    extracted
      .filter(r => ['Local','Remote','Management'].includes(r.proposedScope))
      .map(r => () => agent(verifyPrompt(SKILL, r), {phase:'Verify', schema: VERIFY})
                        .then(v => ({...r, verify: v})))
  )
)

// PHASE 4 — coverage + bucketing (deterministic, in-script)
const flat = results.flat().filter(Boolean)
const missing = args.gsns.filter(g => !flat.find(r => r.gsn === g))  // dropped/crashed agents
const buckets = bucketByReview(flat)   // autopass vs needs-review
return { flat, missing, buckets }
```

## Phase details

**Phase 1 — blind calibration gate.** Run extraction on the 60 calibration GSNs
and diff `proposedScope` against ground truth. PASS criterion: zero
tightening-direction mismatches (proposing Local/Remote/Management for something
truly External/Management — the would-be false-positive-disconnect). Must
reproduce the four traps (esp. DUMP_STATE_ORD → External, EVENT_SUBSCRIBE_REQ vs
SUB_START_REQ). If it fails, fix the skill/prompt and rerun; do not run `audit`
mode until it passes. Report the confusion matrix.

**Phase 2 — extract.** One agent per batch. Prompt = inlined skill + the batch's
GSNs + the discriminator heuristics (ndbapi/ send ⇒ External; mgmsrv/ minus
ndbapi/ + DB receiver ⇒ Management; remote-node dest ref + DB-only ⇒ Remote;
never-on-wire ⇒ Local). Agent must cite send sites; a GSN it cannot fully trace →
`Unclassified` + low confidence (never a guessed restrictive scope).

**Phase 3 — adversarial verify.** Only restrictive proposals. Independent agent
prompted to REFUTE ("find a sender contradicting scope X; default refuted=true if
unsure"). Runs as the second pipeline stage so a batch's findings verify while
other batches still extract (no barrier). `External`/`Unclassified` skip verify.

**Phase 4 — coverage + bucket.** Deterministic JS: confirm every work-list GSN
has a result (report `missing` from crashed/dropped agents — do NOT silently
drop). Bucket into auto-pass (External/Unclassified + high confidence, not
refuted) vs needs-review (any restrictive, any low/medium confidence, any
refuted/disagreement, any versionVariance). Emit the table for human Phase 5.

## Blind calibration — contamination hazard (important)

The 60 calibration GSNs now have entries in the central `SignalScopes.hpp` table
(just landed). Contamination is now SHARPER: every answer sits in one trivially
greppable file, so an agent could read the whole ground truth at once. Two
mitigations, in order of rigor:

1. **Gold standard:** run calibration mode in a worktree checked out at the commit
   BEFORE the classifications/table landed (`isolation: 'worktree'` won't do this
   by itself — check out the parent commit). Truly blind.
2. **Practical:** instruct extract agents to derive scope ONLY from send sites and
   to explicitly IGNORE `SignalScopes.hpp` and any `signal_property` declaration.
   Add this to the skill's "Do not" list. Cheaper, slightly weaker.

Recommend (1) for the formal gate, (2) as the standing rule for `audit` mode too
(so the 831 unknowns are classified from evidence, not from stray pre-existing
labels).

## Concurrency, batching, cost

- 831 remaining GSNs ÷ batchSize 12 ≈ 70 extract agents; restrictive fraction
  (likely 40-60%) drives verify-agent count. Concurrency caps at ~16; total
  agent-runs in the low hundreds — a real but one-time spend. This is opt-in
  scale; log the batch count up front.
- Batch size trade: bigger = cheaper/less parallel but risks starving per-GSN
  attention (each GSN needs real send-site tracing, not one grep). 10-15 is the
  sweet spot; drop to ~8 if extraction quality dips on complex blocks (DBTC/DBDIH).

## Output artifacts

- `results.flat`: every GSN with proposedScope, confidence, evidence, verify verdict.
- `results.missing`: work-list GSNs with no result (coverage failures to rerun).
- `results.buckets`: auto-pass vs needs-review for Phase 5 routing.
Persist as a table the reviewers sort/filter; the auto-pass External/Unclassified
set needs only a light pass, the needs-review set gets full human scrutiny.

## Resume / determinism

Same script + same args → cached agent results on resume (relaunch with
`resumeFromRunId`). Read the run journal before trusting a cached empty result.
No `Date.now()`/random in the script (stamp timestamps after return).

## Open decisions (resolve before building)

1. Calibration mode: worktree-at-prior-commit (rigorous) vs ignore-declarations
   instruction (simple)? Recommend running (1) once as the formal gate.
2. Default scope for a GSN the workflow cannot trace: `Unclassified` (fail-open,
   recommended) — confirm this is acceptable vs forcing a low-confidence guess.
3. Whether to also re-audit the 40 gold-set Local signals in `audit` mode as a
   continuous self-check, or treat them as frozen.
