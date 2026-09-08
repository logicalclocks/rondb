# Phase 5D — nullable columns without a null-tracking matrix

**Status: 5D-1 DONE & VERIFIED (2026-08-20; regen clean, bridge_tests
79/79, coldcall_tests 24/24, nullable canary green under 4060 incl.
the nullable-expression Q3, full ndb_push_agg sweep to completion).
5D-2 DONE & VERIFIED (2026-08-20; regen clean, bridge_tests 81/81,
coldcall_tests 26/26, double canary Q9 + unsigned canary Q6 green
under 4060, full ndb_push_agg sweep to completion).
5D-3 DONE & VERIFIED (2026-08-20 — NO regen, bridge + mysqld only;
bridge_tests 102/102, portable ndb_push_agg_case_null green,
rondb_jit_case_nullable_canary green under 4060, census
case_nullable 0 / case_string 1 / int_sum 1 as expected, full
ndb_push_agg sweep passed; see the 5D-3 record below). The phase is
COMPLETE.**

## 5D-3 implementation record (2026-08-20)

Investigating "embedded READ_ATTR null branching" surfaced TWO
pre-existing mysqld bugs UNDER the JIT slice — for nullable CASE
condition columns the interpreter path itself was broken, so there
was no correct equivalence target to lower:

1. **Integer conditions ERRORED on NULL rows.** The planner emitted
   READ_ATTR + LOAD_CONST + BRANCH_XX_REG_REG with no null guard; the
   interpreter's REG_REG branches return -ZREGISTER_INIT_ERROR when
   either register is null tagged → ZAGG_EMBEDDED_INTERP_ERROR (1872)
   — a hard query error where SQL semantics say the WHEN condition is
   UNKNOWN → falls to the next WHEN / ELSE. Never caught because the
   CASE tests' nullable-declared columns never held an actual NULL.
2. **String NE conditions counted NULL as matching.** BranchCol used
   NULL_CMP_EQUAL (NULL orders lowest and COMPARES): a NULL attr vs a
   literal gives res1 = -1, so NE took the branch — `c <> 'x'` counted
   NULL rows into the THEN arm.

**mysqld fix (standalone, backportable — ha_ndbcluster_push_agg.cc):**
nullable integer condition columns get a BRANCH_REG_EQ_NULL guard
right after their READ_ATTR — jumping past the compare = "condition
failed" (searched CASE: to the next condition; simple-CASE search
register: one guard at the top, to the ELSE output block). Guards are
emitted ONLY for nullable columns, so NOT NULL programs stay
byte-identical. String conditions switch NULL_CMP_EQUAL →
IF_NULL_CONTINUE (never take the branch on NULL; identical for
non-NULL values). Safe against any data node: the kernel validator
and the agg handler table have accepted BRANCH_REG_EQ_NULL all along,
and a pre-5D-3 JIT rejects the unknown opcode → whole-program
interpreter fallback (correct, just slower). Portable MTR test
`ndb_push_agg_case_null` (OFF/ON differential; all 6 int operators,
string EQ+NE, simple + searched + mixed + multi-WHEN, an all-NULL
group, LOAD_CONST64 path, control on a NOT NULL column).

**JIT side (bridge only — NO new stencil, NO regen):** the embedded
translator fuses the READ_ATTR + BRANCH_REG_EQ_NULL PAIR into one
OP_LOAD_COL_NDB_NB (a = dst, b = col_id from the load's c, c = the
guard's target through the pending_target_emb_pc fixup — the existing
5D-1 stencil and helper carry the full small-int decode already). The
guard emits no Op; its emb_pc maps to the next emitted Op like every
no-op pc. A guard NOT directly after a READ_ATTR of the same register
rejects (JIT registers carry no null state — and pre-5D-3 behavior
was reject anyway, so 5D-3 only ADDS coverage). Soundness:
`emb_null_path_reg_safe` walks every forward path from the guard's
target and vetoes fusion if the register could be READ before being
overwritten (the fused load leaves it UNDEFINED on the taken edge
where the interpreter holds a null tag; falling off the block's end
is safe because the outer tracker resets all registers to UNKNOWN
after an embedded block). Planner shapes pass by construction: next
condition = READ_ATTR overwrite; output blocks never touch condition
registers.

**Known gap kept (documented, not 5D-3):** STRING conditions in CASE
aggregates still fall back whole-program — OP_BRANCH_ATTR_OP_ARG's
helper reads the instruction via ctx->prog_buf, which only the
scan-filter path sets (aggregation would need per-embedded-block
base plumbing). Census probe `case_string` (expected 1) tracks it.

**Second gap FOUND during 5D-3 verification (the canary's first Q6
crash):** the OUTER kOpLoadCol admission is BIGINT-only — every
aggregate over a plain INT / SMALLINT / MEDIUMINT / TINYINT column
(signed or unsigned) is a whole-program fallback, even though
ndb_jit_h_load_col and its _nb sibling ALREADY decode every narrow
width (the decode was added for the embedded READ_ATTR, which shares
the helpers). Almost certainly the highest-demand gap left — INT is
MySQL's default integer type — and likely a small bridge-admission
slice since the runtime side exists. Census probe `int_sum`
(expected 1) tracks it; the canary's THEN-value columns are BIGINT
by construction.

Tests: bridge 102/102 (T60a single guarded condition; T60b guard
into an output block; T60c guard without READ_ATTR rejects; T60d
register mismatch rejects; T60e null-path-reads-register rejects —
the scanner's veto; T60f two guarded conditions — guard-to-next-
condition overwrite rule). Coldcall unchanged (31 — no new stencil).
Census: `case_nullable` compiles (0), `case_string` documents the
string gap (1). New canary `rondb_jit_case_nullable_canary`:
searched multi-WHEN / grouped-with-all-NULL-group / simple-CASE all
4060 must-JIT; string and mixed shapes as interpreter-correctness
probes; a mixed program with the THEN arm loading the nullable
column itself (5D-1 + 5D-3 composition).

## 5D-2 implementation record (2026-08-20)

Mechanical siblings as planned: `op_load_col_ndb_f64_nb` /
`op_load_col_ndb_u64_nb` (same cold-call-branch shape, KEEP_ALL tail
policy; helpers `ndb_jit_h_load_col_f64_nb` / `_u64_nb` with the same
strict declared-type contracts as their void counterparts),
`nb_convert_loads` extended to the OP_LOAD_COL_NDB_F64/_U64
candidates via a kind-mapping switch. One shape worth recording from
the updated T50 battery: in the double-family program, the FIRST load
(r0) correctly DEGRADES — its skip range ends at the SUM while the
r1 load sits untainted inside it and r1 is read after the range
(MIN/MAX) — while the SECOND load (r1) converts with its branch
targeting past MAX. Per-load degradation working exactly as designed.

Tests: bridge 81 (T50/T52 updated for conversion + degradation,
T56a/T56b simple f64/u64 conversions), coldcall 26 (T24/T25: NULL
rows leave acc and every mask — updated/initialized/double/unsigned —
untouched; non-null rows accumulate), double canary Q9 + unsigned
canary Q6 (nullable columns under 4060 must-JIT; the unsigned one
crosses 2^63). Regen done.

## 5D-1 implementation record (2026-08-20)

Landed as planned: `op_load_col_ndb_nb` (cold-call branch, KEEP_ALL
tail policy — learned from 5C-2), helper `ndb_jit_h_load_col_nb`
(NULL returns 1 → branch; read errors / unrepresentable declared
types keep the row_fallback defense), `nb_convert_loads` post-pass in
the bridge (runs LAST, after all fixups — op indexes are final):
taint walk finds the last transitively-dependent op, then a
safety pass verifies every op in the range is skippable — dependent
ops, earlier-converted NB loads whose own target stays in bounds, or
pure register writes whose value is dead after the range; anything
else (independent accumulators, branches, embedded-emitted ops)
degrades THAT LOAD to the row-fallback form. Embedded spans are
tracked via a per-op flag recorded around translate_embedded_block.
COUNT is deliberately classified as READING its source register —
the stencil ignores it, but the interpreter's Count kernel skips
null registers, and range membership is what gives COUNT(nullable)
its null-skip. Jumps INTO a skip range (CASE-arm dispositions) are
safe: the null branch only reroutes rows that went through the load;
arm-fed rows still reach the accumulator with their staged register.

Tests: bridge 79 (T5/T6/T6b/T22b/T22c/T47 updated for conversion;
new T55a expression chain, T55b interleaved-accumulator degradation,
T55c branch-in-range degradation, T55d COUNT-in-range, T55e
same-register reload), coldcall 24 (T23: NULL row branches past the
accumulator leaving acc/masks untouched, non-null row accumulates),
`rondb_jit_nullable_canary` upgraded to 4060 MUST-JIT (the per-row
fallback would abort under 4060 — surviving it proves NULL rows,
including the all-NULL group and the nullable-expression Q3, stayed
on the JIT). Original plan follows.

## Where null handling stands (post-5C)

Since 5A, a NULL column value in any JIT load takes the **per-row
interpreter fallback**: the helper sets `JitState::row_fallback`, the
blob runs to completion (results discarded), the glue returns
`NDB_JIT_ROW_FALLBACK`, and the interpreter re-runs THAT ROW with
exact null semantics. Correct, crash-free — but every NULL row costs
a discarded JIT run PLUS a full interpreter run. On NULL-heavy
workloads the JIT is a pure overhead. 5D keeps NULL rows on the JIT.

## Interpreter null semantics (audited, the equivalence targets)

- `kOpLoadCol` of a NULL value → register `is_null = true`.
- Aggregate kernels (`SumBigint/SumDouble/Min*/Max*/Count`):
  `if (a.is_null) return 1;` — the row contributes NOTHING to that
  aggregate; `AggResItem` metadata untouched (per-group SQL NULL for
  all-NULL groups falls out of this).
- Arithmetic (`RegPlus*/RegMinus*/RegMul*/RegDiv*`): any null operand
  → result register null, NO overflow check, propagate.
- Embedded REG_REG comparisons on a null register →
  `ZREGISTER_INIT_ERROR` (post-5D-3: unreachable for planner programs
  — the BRANCH_REG_EQ_NULL guard jumps around the compare; the guard
  + READ_ATTR pair fuses on the JIT, see the 5D-3 record above).

## Design decision: FUSED null-branching loads

Three candidates were on the table:

1. **Null-aware variants of every consumer stencil** (the original
   plan's ~2× matrix multiplier): register null flags in JitState,
   every accumulator/arith stencil grows a check. Doubles the stencil
   set AND taxes the NOT-NULL hot path. Rejected.
2. **Branch-on-null prelude** (the roadmap's alternative): a reg_null
   flag array + one `OP_BRANCH_IF_REG_NULL` stencil emitted before
   each consumer. Keeps the matrix flat but still adds JitState
   state, per-row clearing, and one extra op per consumer.
3. **CHOSEN — fuse the null test into the load's cold call.** The
   load helper already inspects the AttributeHeader; give it a
   RETURN VALUE (1 = value was NULL) and make the load stencil a
   cold-call BRANCH — exactly `op_branch_attr_eq_null`'s proven
   shape:

   ```c
   if (ndb_jit_h_load_col_nb(s, col_id, dst)) {
     [[clang::musttail]] return HOLE_LCNB_TGT(s);   /* null: skip */
   }
   TAIL_NEXT(s);
   ```

   The branch target skips the register's ENTIRE consumer chain —
   which reproduces the kernels' null-skip exactly (see equivalence
   below). No JitState change, no null state, no per-row clearing,
   no consumer-stencil variants. NOT-NULL rows pay one test of an
   already-returned value.

## The bridge's skip-range (taint) analysis

For each `kOpLoadCol`, the bridge computes where a NULL must branch
TO: the first subsequent op that does not depend on the loaded
register.

- Walk forward with a taint set initialized to {dst}. An op reading a
  tainted register joins the skip range (its own dst becomes
  tainted); an op overwriting a tainted register with an untainted
  value removes it. The target is the first op outside the range —
  resolved to an output-op index by the same machinery as the CASE
  skip fixups; end-of-program maps to the tail OP_EXIT.
- **Per-load graceful degradation**: if the skip range is
  NON-CONTIGUOUS (interleaved independent chains — planner-emitted
  programs are per-aggregate contiguous, so rare) or TOUCHES an
  embedded block (5A REG_REG compares could read the register), that
  load simply keeps the CURRENT row-fallback stencil. No program-
  level rejection, no regression — 5D never makes anything worse
  than today, and needs NO nullability metadata from the caller
  (a NOT NULL column's null branch is simply never taken).

## Equivalence arguments

- `SUM(a)` / `COUNT(a)` / `MIN(a)` / `MAX(a)`, a NULL: interpreter —
  kernel skips; JIT — branch past the accumulator(s). Identical:
  no acc update, no value_updated/initialized/unsigned/double marks,
  so per-group SQL NULL (all-NULL group) is preserved.
- `SUM(a), MIN(a)` (two consumers, one load): skip range covers both
  — both kernels would have skipped. Identical.
- `SUM(a + c)` (a nullable): interpreter — Plus propagates null (no
  overflow check!), Sum skips; JIT — branch past Plus AND Sum, so the
  arithmetic never runs on garbage (no spurious OVERFLOW_EXIT), and
  the skipped `LoadCol c` has no observable effect. Identical.
- `AVG(a)`: SUM slot + COUNT slot in one chain — one branch skips
  both, matching both kernels' null-skip. Identical.
- Rows where EVERY load is non-null: byte-identical behavior to
  today minus one predictable branch per load.

## Operand layout + engine touch points

`OP_LOAD_COL_NDB_NB` (and 5D-2's `_F64_NB` / `_U64_NB`):
**a = dst reg, b = col_id, c = null-branch target pc** — col_id moves
from c to b (free in the load ops; 16-bit, holds ≤ 4095) because the
engine patches HK_BRANCH_TAKE displacement from op->c. Add the new
kinds to `bc_op_is_branch` (forward-only + in-range admission comes
free). Extractor: cold-call + two narrow operand holes + one
HK_BRANCH_TAKE — the op_branch_attr_eq_null pattern, no extractor
changes expected. New helpers `ndb_jit_h_load_col_nb` (+`_f64_nb`,
`_u64_nb`): identical decode to their void siblings, but NULL returns
1 (take the branch) instead of setting row_fallback; read errors and
declared-type mismatches KEEP the row_fallback defense (return 0,
blob continues, glue discards).

## Slices

### 5D-1 — i64 null-branching loads (regen, 1 stencil)

`op_load_col_ndb_nb` + helper; the bridge taint walk + target
resolution + per-load degradation; bc_op_is_branch + names + dump
support. Tests: bridge (target computation for the shapes above,
contiguity degradation, embedded-block degradation, end-of-program
target), coldcall (null branch skips the accumulator and leaves all
masks unmarked; non-null proceeds; helper-arg round trip). Canary:
upgrade `rondb_jit_nullable_canary` to MUST-JIT under 4060 (its
NULL-row queries stop being fallbacks — the all-NULL-group query
becomes a pure-JIT run) + assert the `programs_fallback` counter
does NOT move across the nullable queries.

### 5D-2 — f64/u64 null-branching loads (regen, 2 stencils)

Mechanical siblings (`_f64_nb`, `_u64_nb`) so nullable DOUBLE/FLOAT
and BIGINT UNSIGNED aggregation also stays on the JIT. Extend the
double/unsigned canaries with a nullable column each.

### 5D-3 — embedded READ_ATTR null branching — DONE (see record above)

Original deferral note: CASE conditions over nullable columns took
the per-row fallback; the fused pattern needed the embedded fixup
machinery. Implemented 2026-08-20 together with the underlying
mysqld null-guard fix — see the 5D-3 implementation record at the
top of this file.

## Non-goals

- Register null flags in JitState / null-aware consumer stencils —
  subsumed by the fused design.
- Null semantics for embedded comparisons (ZREGISTER_INIT_ERROR) —
  unreachable while 5D-3 is deferred.
- String/DECIMAL nullable loads — those types don't load at all yet.

## Verification pattern

bridge_tests target/degradation cases; coldcall null-skip execution;
`rondb_jit_nullable_canary` upgraded to 4060 must-JIT + fallback-
counter-zero asserts; full `ndb_push_agg` sweep with `--force` to
completion (nullable-column tests across the suite silently stop
falling back — they are the real net).
