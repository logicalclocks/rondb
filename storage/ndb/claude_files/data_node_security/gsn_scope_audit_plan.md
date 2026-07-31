# GSN Signal-Scope Audit — Plan (the classification step)

Detailed plan for the audit step of the signal-scope security work: classifying
every kernel GSN into a `SignalScope` so the receive-time check
(`checkSignalSender`) can reject signals from illegitimate sender node types.
Scope of this doc = the classification audit only. The enforcement rewire and the
optional boundary bitmap are separate steps (see Dependencies).

Companion artifacts:
- Method contract: `.claude/skills/gsn-signal-scope-audit/SKILL.md`
- Ground truth: `signal_scope_calibration_set.md`

## Taxonomy (frozen — do not broaden/simplify)

Four runtime scopes, a monotonic chain over NDB's node-type trust tiers, plus one
bookkeeping state. This is settled; the audit classifies against it and does not
redesign it.

| Scope | Legitimate senders | Cutoff |
|---|---|---|
| `Local` | same node only | never crosses the wire (EXECUTE_DIRECT, self-send, fixed local ref) |
| `Remote` | + DB nodes | crosses the wire but only from a data node |
| `Management` | + MGM nodes | also legitimately from an MGM node, never API |
| `External` | + API nodes (anyone) | an API node legitimately sends it |
| `Unclassified` | (runtime = External) | not yet audited — bookkeeping state (**implemented**), behaves as External at runtime but distinguishes audited-open from never-looked-at |

The taxonomy is still **four enforced scopes** — `Unclassified` is the default
bookkeeping rung, not a fifth enforcement tier, so the frozen decision holds.
Absence from `SIGNAL_SCOPES` means "never audited"; an explicit
`X(GSN_x, Unclassified)` entry means "audited, sender set could not be traced".

Assignment is the UNION over all legitimate senders across all supported
versions. Overriding bias: **errors toward strictness disconnect healthy nodes**
(the failure mode to avoid); when uncertain, choose looser and flag.

## Process (6 phases)

**Phase 0 — Build the work-list (inline scout).** Grep `GlobalSignalNumbers.h`
for every `#define GSN_` → the definitive list (MAX_GSN=981, 891 defined, ~90
gaps + reserved placeholders like `GSN_649`). Partition into batches (~10-15
GSNs each) such that every GSN belongs to exactly one batch. Split off the
already-classified set for calibration. Iterate over real `#define`s, not
operation names (see the ENTER_SINGLE_USER_REQ trap).

**Phase 1 — Calibrate blind (quality gate).** Run the method against
`signal_scope_calibration_set.md` with answers stripped. Gate: any mismatch in
the *tightening* direction (proposing a restrictive scope for something truly
External/Management — a would-be false-positive disconnect) blocks the full run.
Must reproduce the four traps, especially DUMP_STATE_ORD. Do not proceed until it
passes.

**Phase 2 — Fan-out: evidence extraction + first-pass classification.** One agent
per batch applies the skill: enumerate all send sites (all `sendSignal` variants,
`EXECUTE_DIRECT`, GSN-as-variable, routed sends via `LOCAL_ROUTE_ORD`/TRPMAN),
resolve each sender's node type, take the cross-version union, apply the cutoff,
emit `{scope, confidence, version-variance, cited send sites}` via a fixed schema.

**Phase 3 — Fan-out: adversarial verification.** For every *restrictive*
proposal (Local/Remote/Management — the disconnect-capable ones), an independent
agent tries to refute it ("find a sender that contradicts scope X; default to
looser if unsure"). External/Unclassified skip this. Runs as a pipeline stage
after extraction (per-GSN, no barrier). With no runtime soak (decision:
dropped), this adversarial pass + human review are the ONLY backstop before a
classification can disconnect a node — so it must be genuinely skeptical.

**Phase 4 — Coverage + consistency (deterministic, in-script).** Confirm every
work-list GSN has a result (flag any dropped by a crashed agent: count-in ==
count-out). Flag extract/verify disagreements. Bucket: auto-pass
(External/Unclassified, high confidence) vs needs-review (all restrictive,
low/medium confidence, disagreements, version-variance flags).

**Phase 5 — Human review (concentrated).** Several reviewers examine only the
needs-review bucket — every restrictive classification, all uncertainty, all
disagreements, all version-variance. External/Unclassified get a light pass. This
is where scarce human judgment lands on the disconnect-capable decisions.

**Phase 6 — Integrate.** Append approved entries to the central
`SignalScopes.hpp` X-macro table (`X(GSN_x, Scope)`), committed in reviewable
batches by block/subsystem. (Scopes are now centralized — one table, one source
of truth — not scattered across signal-class headers.)

## Findings folded in (from the gold-set + calibration audits)

1. **~~Add~~ the `Unclassified` bookkeeping state** — DONE. Last value of the
   `SignalScope` enum and the default for any GSN with no `SIGNAL_SCOPES` entry;
   unrestricted at runtime, so it landed as a no-op. Enum order is pinned by a
   `static_assert` (`addSignalScopeImpl` uses `MIN()`, so the default must be the
   most permissive rung or it could loosen a real classification). The work-list
   is now the grep difference between `GlobalSignalNumbers.h` and
   `SignalScopes.hpp` — see `gsn_scope_handoff.md` item 2. `FSSYNCREQ` was the
   motivating case: undeclared, silently read as `External`, should be `Local`
   (fixed separately, see #2).
2. **Fix `GSN_FSSYNCREQ` now**: add `DECLARE_SIGNAL_SCOPE(GSN_FSSYNCREQ, Local)`
   (its siblings FSSYNCCONF/FSSYNCREF are already Local). Small, isolated,
   pre-audit cleanup.
3. **Blind + extended calibration is mandatory** (Phase 1). The original gold set
   (39 Local/1 Remote) only validates easy Local recognition; the extended set
   adds the wire-crossing node-type distinctions where real errors occur.
4. **`Management` is rare and the hardest call.** In calibration, 3 of 5
   Management guesses were wrong (1 was actually External, 1 a phantom, 1 dead).
   Every proposed `Management` gets extra scrutiny in Phase 3/5.
5. **Handle deprecated/`_vX_Y_Z` and phantom GSNs explicitly** (Phase 0/2):
   deprecated no-op signals (e.g. SET_LOGLEVELORD_v9_4_0) and
   operation-names-that-aren't-GSNs (ENTER_SINGLE_USER_REQ) need a defined
   disposition, not a guessed scope.
6. **Note the dormant `CREATE_FRAGMENTATION_CONF` legacy wire path** (pre-7.0.4);
   Local is safe today but add a code comment against reactivation.
7. **`FAIL_REP` was never actually scoped.** Its only `DECLARE_SIGNAL_SCOPE`
   lived *inside a doc comment* in SignalData.hpp (a format example), so it
   silently defaulted to External — the "sole Remote example" was illusory.
   Now a real `Remote` entry in the central table. Lesson for the workflow:
   grep for `DECLARE_SIGNAL_SCOPE` can match commented text; parse real
   declarations only.
8. **Scopes are now centralized** in `SignalScopes.hpp` (done). The 60 verified
   classifications were migrated there; per-signal-header declarations were
   removed. This is also the intended source for the future boundary bitmap.

## Method-reliability evidence (from calibration)

- The fan-out approach works: 8 independent agents produced well-evidenced,
  high-confidence verdicts on 60 signals; the External and Remote batches were
  fully clean, and the empirical `mgmsrv`-minus-`ndbapi` sweep found 9 solid
  Management examples where name-based guessing had a 60% miss rate.
- Reliable discriminators the workflow should exploit: a `src/ndbapi/` send ⇒
  External; a `src/mgmsrv/` send with no `ndbapi/` send + DB receiver ⇒
  Management; a remote-node destination ref (`numberToRef(…, remoteNodeId)`) with
  only DB senders ⇒ Remote; wire-crossing-never ⇒ Local.
- Caveat carried into the full run: calibration agents were given targeted hints;
  the blind full-audit agents will not be — hence Phase 1 must be blind, and
  Phase 3's adversarial pass carries the load the removed soak used to.

## Dependencies (ordering outside this step)

- **Enforcement rewire must land and be tested BEFORE any new restrictive
  classification is enforced.** Today a scope violation calls
  `handle_sender_error → ndbabort()` (crashes the *receiver*). Replace with
  `reportMaliciousSignal(...)` so QMGR Tier A disconnects the *sender* and the
  violation gets counters + ndbinfo observability. Verify Tier A actually fires
  (per the enforce-mode test convention) first.
- The optional transporter-boundary bitmap (flood defense) comes after the audit
  and reuses these same classifications; undefined GSN slots there default to
  deny.
