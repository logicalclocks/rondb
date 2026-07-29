# GSN Signal-Scope Audit — Handoff / Next Steps

Status as of 2026-07-29. The mechanism and enforcement are in place and tested;
the remaining work is the bulk classification audit and a few enablers. This
doc is the pickup point — start here, then read the linked docs.

## Where things stand (done)

- **Mechanism centralized.** All per-GSN scopes live in one X-macro table:
  `storage/ndb/include/kernel/signaldata/SignalScopes.hpp`, expanded by
  `SignalData.hpp` into `signal_property<>` specialisations and into
  `g_signal_scope_table`, which seeds every block's handler array.
- **`Unclassified` is the default state**, so an unaudited GSN is distinguishable
  from a reviewed-open (`External`) one. Unrestricted at runtime; see item 2.
- **Enforcement rewired** from crash → tiered report: `checkSignalSender`
  (`SimulatedBlock.hpp`) drops the signal and calls `reportMaliciousSignal`
  (Tier A → QMGR disconnects the sender) instead of `ndbabort`. Includes a
  `nodeId == 0` bypass for system/startup-origin signals.
- **60 of 891 GSNs classified** (39 Local, 6 Remote, 9 Management, 6 External),
  each hand-verified from send sites.
- **Tests green:** `ndb_security_scope.test` (new, QMGR/catalog side), plus fixes
  to `ndb_security.test` and the `security_violation_counts` view.

## Frozen decisions (do NOT relitigate — settled with rationale in the docs)

- **Four scopes** (Local/Remote/Management/External) — not broadened, not
  simplified. Rationale: `gsn_scope_audit_plan.md`. (`Unclassified`, added since,
  is a bookkeeping default with the same runtime effect as `External` — not a
  fifth enforcement tier, so this decision still holds.)
- **Tier A** for scope violations (structural; no honest client can trigger).
- **Three violation types** (one per scope class) for per-boundary telemetry.
- **No soak period** — thorough audit + multi-person review is the only backstop.
  This raises the stakes on the restrictive (Local/Remote/Management) calls.
- The central table is also the intended source for a future boundary bitmap.

## Remaining work (in suggested order)

1. **~~Build the~~ Run the fan-out workflow script.** DONE (authored + validated):
   `gsn_scope_audit_workflow.js` (this dir), run via
   `Workflow({scriptPath: ".../gsn_scope_audit_workflow.js"})`.
   **The team's next action is the real audit:** `mode:"audit"` with `args.gsns`
   = the unclassified worklist (build command in the script's header comment).
   Returns `needsReview` / `autoPass` / `missing` buckets for Phase 5 human
   review; approved entries are appended by hand to `SignalScopes.hpp`. The script
   inlines the method, so it works even though `.claude/` is not in the PR. (Pass
   `args` as an object OR a JSON string — the script parses both.)

2. **~~Implement the~~ `Unclassified` bookkeeping state.** DONE. `Unclassified`
   is now the last `SignalScope` enum value (`SignalData.hpp`) and the default
   for any GSN with no `SIGNAL_SCOPES` entry — unrestricted at runtime, exactly
   like `External`, so this landed as a strict no-op. Enum order is pinned by a
   `static_assert` because `addSignalScopeImpl` resolves duplicates with `MIN()`;
   `Unclassified` being the most permissive rung is what stops the default from
   loosening a real classification. Coverage is read straight from source:

   ```
   grep -c '^#define GSN_' storage/ndb/include/kernel/GlobalSignalNumbers.h
   grep -cE '^  X\(GSN_' storage/ndb/include/kernel/signaldata/SignalScopes.hpp
   ```

   The set difference is the audit work-list for step 4. Also folded in: the
   table now seeds every block's handler array via `g_signal_scope_table`, so a
   classification applies even to handlers installed by direct assignment
   (`installSimulatedBlockFunctions`) rather than `addRecSignal` — previously
   those silently ignored it, and the 7 `FS*REF` signals needed hand-written
   scopes to compensate. Those hand-written lines are gone; a `static_assert`
   pins that the table still gives them `Local`.

3. **Refresh the table against the current 26.04 GSN set.** Post-retarget
   `MAX_GSN=981` / 891 defines; any GSN not in `SIGNAL_SCOPES` is `Unclassified`.
   Enumerate the 831 unclassified (grep pair in item 2) and feed them to the
   audit.

4. **Run the audit** (the 831 GSNs). Follow `gsn_scope_audit_plan.md`: fan-out
   extract → adversarially verify the restrictive proposals → concentrated
   human review of restrictive/low-confidence → integrate approved entries into
   `SignalScopes.hpp`. Method contract:
   `.claude/skills/gsn-signal-scope-audit/SKILL.md`.

5. **Add an end-to-end enforcement test.** `ndb_security_scope.test` covers the
   QMGR/catalog side via the DUMP 9100 injector; it does NOT exercise a real
   scope-violating signal through `checkSignalSender → drop → report`. Add a debug
   `ERROR_INSERT` that restricts a chosen GSN's scope WITHOUT arming the 10054
   crash, send it from the wrong node type, and assert the sender is disconnected
   and the receiver stays up.

6. **(Optional, later) Transporter-boundary bitmap** for flood resistance —
   reject scope-violating signals before section import. Deferred until the audit
   is done; reuses the same classifications. If built as 2 cache lines it must be
   a 1-bit "restricted?" filter + a small secondary scope table (4 scopes don't
   fit 2 bits × 891 in 2 lines). See the design discussion referenced in the
   audit plan.

## Watch-outs

- **Errors toward strictness are the dangerous ones.** Mislabelling a
  user-reachable signal as Local/Remote/Management disconnects a healthy node
  (worse during a rolling upgrade). When uncertain, classify looser and flag.
- **`security_violations` grep can match commented text** — `GSN_FAIL_REP` sat in
  a doc comment for years and read as "classified" when it wasn't. Parse real
  declarations, not raw grep hits.
- **Known traps** (name-based intuition is wrong): e.g. `DUMP_STATE_ORD` is
  External not Management; `EVENT_SUBSCRIBE_REQ` (→CMVMI, Management) vs
  `SUB_START_REQ` (→SUMA, External). Full list in `signal_scope_calibration_set.md`.
- A send from `src/ndbapi/` ⇒ External (decisive); from `src/mgmsrv/` with no
  ndbapi sender + a DB receiver ⇒ Management; remote-node dest ref + DB-only ⇒
  Remote; never-on-wire ⇒ Local.

## Entry-point docs

- `gsn_scope_audit_plan.md` — the audit-step plan (taxonomy, phases, findings).
- `gsn_scope_workflow_plan.md` — concrete fan-out script design.
- `signal_scope_calibration_set.md` — 60 verified classifications + known traps.
- `.claude/skills/gsn-signal-scope-audit/SKILL.md` — per-GSN classification method.
- `tiered_response_policy.md` — the Tier A/B response system this plugs into.
