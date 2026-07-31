---
name: gsn-signal-scope-audit
description: >
  Classify an NDB kernel signal (GSN) into its SignalScope — Local, Remote,
  Management, or External — by auditing every place it is sent and resolving
  each sender's node type. Use when auditing GSNs for the data-node signal-scope
  security system: classifying one or many GSNs, producing DECLARE_SIGNAL_SCOPE
  declarations, or reviewing a proposed classification. This is the shared
  contract for both human review and the fan-out audit workflow, so that every
  pass classifies identically.
---

# GSN Signal-Scope Audit

The goal: for a given GSN, determine the complete set of node **types** that may
legitimately send it, then map that set to one of four scopes. The output is a
`DECLARE_SIGNAL_SCOPE` declaration plus the evidence that justifies it.

## The four scopes (the cutoff rule)

Node types are `DB` (data node), `MGM` (management node), `API` (client:
mysqld, NdbApi apps, RDRS/Rondis). Local signals never cross the wire at all.
The scopes form a permissiveness chain — each rung adds one sender class:

| Scope | Legitimate senders | Assign when… |
|---|---|---|
| `Local` | same node only | the signal is **never** sent to a remote node — only same-node (`EXECUTE_DIRECT`, self-send, or send to a fixed local block ref) |
| `Remote` | + DB nodes | it crosses the wire but **only** ever from a data node |
| `Management` | + MGM nodes | it may also legitimately come from an MGM node, never from API |
| `External` | + API nodes (i.e. anyone) | an API node legitimately sends it |
| `Unclassified` | (runtime = External) | evidence is incomplete or you cannot determine the sender set — **fail open**, do not guess restrictive |

`Unclassified` is a bookkeeping state that behaves exactly like `External` at
runtime (allows all senders). It exists only to distinguish "reviewed and
deliberately open" from "not yet audited." Never emit `External` for a GSN you
could not fully trace — emit `Unclassified`.

## The overriding bias: errors toward strictness are the dangerous ones

Enforcement disconnects the sending node (Tier A). So:

- **Too strict** (e.g. calling something `Remote` that MGM legitimately sends) →
  a healthy node gets **disconnected**, potentially mid-rolling-upgrade. This is
  the failure mode to avoid.
- **Too loose** (`External` where it could be tighter) → no worse than today's
  behavior; fail-open.

When uncertain, choose the **looser** scope and flag low confidence for review.
Never tighten on incomplete evidence.

## Procedure for one GSN

1. **Enumerate every send site.** Search for the GSN across all of these — a
   single missed sender makes the classification wrong:
   - `sendSignal`, `sendSignalNoRelease`, `sendFragmentedSignal`, `sendDelayed`
   - `EXECUTE_DIRECT` (always same-node → contributes only to a `Local` verdict)
   - the GSN passed as a **variable**, not a literal (grep the GSN, then trace
     any variable assigned from it)
   - **routed/relayed** sends: `LOCAL_ROUTE_ORD`, relays through TRPMAN, and any
     signal re-sent by a forwarding block — the apparent sender is not the origin

2. **Resolve each sender's node context.** For every send site, determine which
   block sends it and therefore which node type(s) that block runs on
   (DB/API/MGM). A signal sent only within the local node contributes to
   `Local`; a signal sent to a remote node ref contributes its sender's type.

3. **Take the union across ALL supported versions, not just current code.**
   Record whether the sender set differs in any still-supported older version
   (a signal whose senders *narrowed* over time is the classic upgrade hazard).
   This becomes the "version-variance" field; flag any variance for close review.

4. **Apply the cutoff rule** above to the union set → propose a scope.

5. **Emit the declaration + evidence** (see output format).

## Output format (per GSN)

```
GSN: GSN_XXX
Proposed scope: Remote
Confidence: high | medium | low
Version-variance: none | <describe how senders differ across supported versions>
Send sites:
  - <file:line> block=<BLOCK> sender-node-type=<DB|API|MGM|local>
  - ...
Declaration: DECLARE_SIGNAL_SCOPE(GSN_XXX, Remote);
Place in: <signal-class header, or SimulatedBlock.cpp inline install>
```

Restrictive proposals (`Local`/`Remote`/`Management`) and any `low`/`medium`
confidence require human review before landing. `External`/`Unclassified` get
light review.

## Mechanical facts to rely on

- Declarations are centralized in one table: `signaldata/SignalScopes.hpp`, as
  `X(GSN_x, Scope)` entries in the `SIGNAL_SCOPES(X)` X-macro (expanded by
  `SignalData.hpp` into `signal_property<>` specialisations *and* into
  `g_signal_scope_table`, which seeds every block's handler array — so an entry
  applies regardless of whether the block installs its handler via `addRecSignal`
  or by direct assignment). To classify a signal, append one entry there — do not
  scatter declarations into signal-class headers.
- A GSN with no entry is `Unclassified`, the enum's last and most permissive
  value. `Unclassified` and `External` are both unrestricted at runtime; the
  difference is bookkeeping. Absence = never audited; an explicit
  `X(GSN_x, Unclassified)` entry = audited, sender set could not be traced.
- Multiple registrations are fine — `addSignalScopeImpl` keeps `MIN()`
  (most-restrictive wins), so declaring a scope never loosens an existing one.
  This is why `Unclassified` sits at the top of the enum: the default can never
  override a real classification. A `static_assert` in `SignalData.hpp` pins the
  ordering.
- The runtime check (`checkSignalSender`) runs only on the async
  (remote-delivered) path and early-returns when `nodeId == theNodeId`, so a
  `Local` verdict only ever fires against genuinely cross-node arrivals.
- GSN space is sparse: `MAX_GSN = 981`, 891 defined, ~90 gaps plus reserved
  placeholders (e.g. `GSN_649`). Undefined GSN numbers are out of scope for this
  audit (no signal to classify); they matter only for the later boundary bitmap,
  where an undefined slot must default to **deny**.

## Calibrate before trusting the method (gold set)

Before classifying unaudited GSNs, run the procedure **blind** on the GSNs that
are already classified and confirm you reproduce them:

- ~38 `Local` (e.g. `GSN_CONTINUEB`, the `FS*` filesystem callbacks, `STTOR`/
  `NDB_STTOR` startup, `READ_CONFIG`, `ALLOC_MEM`, `BUILDINDX`)
- `GSN_FAIL_REP` = `Remote`
- The 7 `FS*REF` signals (`GSN_FSOPENREF`, `FSCLOSEREF`, `FSWRITEREF`,
  `FSREADREF`, `FSREMOVEREF`, `FSSYNCREF`, `FSAPPENDREF`) resolve to `Local`.
  Their handlers are installed by direct assignment in
  `installSimulatedBlockFunctions` rather than `addRecSignal`, so they only get
  their scope from `g_signal_scope_table` (the constexpr table built from
  `SignalScopes.hpp`) — there is no per-handler override left in
  `SimulatedBlock.cpp` to fall back on if the table entry is ever wrong.

If the procedure cannot re-derive `CONTINUEB` = Local and `FAIL_REP` = Remote
from the code alone, do not trust it on the remaining GSNs — fix the method
first.

## Do not

- Do not emit a restrictive scope from incomplete send-site evidence — emit
  `Unclassified` and flag it.
- Do not classify from the signal's name or intent — classify from actual send
  sites in code.
- Do not read `SignalScopes.hpp` or existing `signal_property` declarations to
  decide a scope — derive it independently from send sites. Existing entries can
  be wrong, and reading them contaminates a blind calibration/audit. (Also: a
  `DECLARE_SIGNAL_SCOPE` match may be inside a comment — `FAIL_REP` was, for
  years — so never trust a raw grep hit as an active declaration.)
- Do not consider only current code — a signal's senders may have been broader
  in a supported older version.
