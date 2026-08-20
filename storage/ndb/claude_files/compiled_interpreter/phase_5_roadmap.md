# Phase 5 roadmap — stencil-matrix expansion, in sub-phases

**Status: PLANNED (2026-08-19). Code: not started.**

Phase 5 is the coverage grind: everything the bridge still rejects.
The original design (`phase_5_implementation.md`) is one monolith; a
lot of it has since been overtaken by events — the cold-call-branch
pattern, the embedded null/comparison families, checked arithmetic,
CASE skip offsets, and COUNT all shipped in Phases 5.0–8. This roadmap
re-cuts what remains into sub-phases 5A–5F, ordered by measured demand
where we have it. `phase_5_implementation.md` stays the reference for
stencil-level mechanics (lattice design §3, stencil families §6,
helper inventory §5); this doc is the sequencing authority.

**Prioritization rule:** after 5A (already evidenced by the groupby
canary), the 5B–5F order below is provisional. Re-rank between
sub-phases using `ndbinfo.jit` `programs_fallback` plus the
rate-limited fallback log (they name the rejecting site + opcode) from
Mikael's benchmarks and RonSQL workloads — measured demand beats the
a-priori order.

**MEASURED (2026-08-20, the `rondb_jit_fallback_census` MTR test —
its recorded result file is the living coverage tracker):**

- All controls compile: Phase 4 SUM, 5A CASE, 5C-2 double division,
  5C-3 unsigned MIN/MAX, 5D nullable SUM.
- **5E has ZERO SQL-side demand**: `SUM(a DIV b)`, `SUM(a % b)` and
  integer `SUM(a / b)` never reach the bridge — the SQL planner does
  not push them (integer '/' is DECIMAL-typed in MySQL; DIV/% have no
  emit path). 5E's opcodes arrive only from the NDB API / RonSQL.
  Demoted below 5F; the Test 27 kOpMod fallback canary stays valid.
- **5F confirmed real**: `MIN/MAX(VARCHAR)` is pushed and falls back
  (reason=2 NON_BIGINT, detail=7 kOpLoadCol).
- **NEW census-surfaced items:** (1) unsigned checked arithmetic —
  `SUM(u + 1)` is pushed and rejected by the 5C-3 tracker fence
  (reason=8 TYPE_MISMATCH, detail=20 kOpPlusBigint); ~3 stencils,
  best effort-to-value on the board → slice 5C-4. (2) a DECIMAL load
  path — `SUM(DECIMAL)` is pushed and rejected at the load; the
  optimizer maps DECIMAL scale=0 to the BIGINT track and scale>0 to
  DOUBLE, so a cold-call DECIMAL load helper may unlock it with no
  new accumulator stencils (audit queued).
- `MIN/MAX(DATE)` is NOT pushed by the SQL planner (RonSQL-only per
  the D17 notes) — no JIT work applicable.
- Minor observation: rejected programs re-attempt compilation per
  fragment per execution (8 attempts/query here) — no negative
  caching in the progcache; revisit if programs_fallback gets hot.

## Where coverage stands (2026-08-19)

Admitted today — outer: `kOpLoadCol` (non-null BIGINT), `kOpLoadConst`,
`kOpMov`, checked `kOpPlus/Minus/MulBigint`, checked `kOpSumBigint`,
`kOpCount`, `kOpEmbeddedInterp` (subset). Embedded:
`BRANCH_ATTR_*_NULL`, `BRANCH_ATTR_OP_ARG/PARAM/ATTR` (int + string,
cold-call), `READ_LINKED_TO_MEM` + `BRANCH_LINKED_*_NULL`,
`LOAD_CONST16`, `WRITE_INTERPRETER_OUTPUT` slot 0 (incl. CASE skip
offsets → `OP_JUMP`), `EXIT_OK`, `EXIT_REFUSE`. Rejected: everything
else — notably MIN/MAX, all DOUBLE ops, div/mod, string aggregation,
nullable-column loads, and the embedded `READ_ATTR` /
`BRANCH_*_REG_REG` family the SQL planner emits for CASE.

## 5A — SQL CASE unblock — **DONE & VERIFIED (2026-08-19)**

Verified the hard way: the first "all green" was premature — the full
`ndb_push_agg` sweep then surfaced `ndb_join_pushdown_agg` regressions
that took three debugging rounds (the findings below, newest first).
Final state: bridge_tests (52, incl. T47 family + T48 Skip→JUMP, T22b/
T47 asserting the zero-skip jump), coldcall_tests,
`rondb_jit_groupby_canary` (Q3 CASE must-JIT), the new
`rondb_jit_nullable_canary`, and the COMPLETE `ndb_push_agg` suite all
pass. SQL CASE aggregation runs the JIT (INT and BIGINT columns);
nullable-column aggregation no longer crashes (per-row fallback).
Lesson recorded: only a full-suite pass counts as verified — every
newly admitted opcode silently changes which existing tests JIT.

Shipped, deviating from the sketch below where reality demanded:
- **Null handling resolved by a new mechanism, better than options
  (a)/(b): per-row interpreter fallback.** New
  `JitState::row_fallback` (helpers-only — no stencil touches it, so
  NO regen): the load helper flags a row on a NULL column value, the
  glue discards that row's JIT run (no writeback) and returns
  `NDB_JIT_ROW_FALLBACK`; both ProcessRec's fall through and re-run
  THE ROW on the interpreter (exact null semantics — null-skip
  kernels, ZREGISTER_INIT_ERROR on null comparisons). Non-NULL rows
  keep the JIT.
- **This fixed a latent production crash:** `ndb_jit_h_load_col`
  abort()ed on NULL (its comment wrongly claimed admission rejects
  nullable columns — the bridge only sees bytecode and the planner
  pushes nullable aggregation), so `SUM(nullable_col)` with any NULL
  row crashed the node under CompiledInterpreter=AUTO. New canary
  `rondb_jit_nullable_canary` (scalar + grouped incl. an all-NULL
  group → per-group SQL NULL, finally provable end-to-end).
- **Second latent bug fixed — the aggregate mask index:** the bridge
  set `op->c` (value_updated/value_unsigned index) from a per-op
  ordinal; the interpreter's `agg_index` IS the AggResItem index and
  the writeback pairs `acc_i64[i]` with result `i`, so multi-arm CASE
  (several Sum ops, same agg_index) mis-marked the mask. Now
  `c = agg_index` for kOpSumBigint + kOpCount; the ordinal (and its
  >4-agg-OPS cap) is gone — the `agg_index < BC_MAX_ACCS` check
  remains.
- **Lowerings** (agg-embedded mode only, new `allow_reg_ops` param;
  scan-filter mode rejects — T47c): `READ_ATTR_INTO_REG` →
  `OP_LOAD_COL_NDB`; `LOAD_CONST64` (+2 data words, low first) →
  `OP_LOAD_CONST_INT`; `BRANCH_{EQ,NE,LT,LE,GT,GE}_REG_REG` (12–17) →
  the Phase 1 hot `OP_BRANCH_*_INT_INT` stencils with the standard
  forward-only fixup. `LOAD_CONST16` now ALSO materializes its
  register in agg mode (a REG_REG compare may read it; the staged
  skip-offset copy still works; T22b/T22c op counts updated).
- **Third-run finding (Test 42 grp1 = 6): a zero-skip output block in
  the MIDDLE of the embedded stream fell through into the next
  block.** The "skip_offset 0 emits nothing, fall through to the outer
  ops" convention is only correct for the LAST output block; multi-arm
  CASE lays several LC16/WRITE/EXIT_OK blocks back to back, and arm-0
  rows fell into arm-1's disposition jump. Fixed: WRITE_INTERPRETER_
  OUTPUT now ALWAYS emits the disposition OP_JUMP (target
  outer_after_emb_pos + skip_offset, resolved by the existing pass) —
  one wasted jmp for a final block, correct everywhere. T22b/T47
  updated (+1 op each).
- **Second-run finding (the join-CASE all-ELSE bug): `READ_ATTR` must
  decode by the column's DECLARED type.** The embedded wire format
  carries no type, and the load helper blindly decoded 8 bytes — an
  INT column's 4-byte cell picked up a garbage high word, every CASE
  condition compared false, and every row took the ELSE arm
  (`ndb_join_pushdown_agg` Tests 36/42; the standalone canaries missed
  it because they use BIGINT columns and the failing test's outer
  programs load only constants, so the bridge's declared-BIGINT check
  never saw the column). Fixed: `ndb_jit_h_load_col` now mirrors
  `handleReadAttrIntoReg`'s descriptor inspection — all signed int
  widths sign-extend, narrower unsigned widths zero-extend (compare
  correctly as non-negative i64), and BIGUNSIGNED / FLOAT / DOUBLE /
  strings / pseudo columns take the per-row interpreter fallback
  (>= 2^63 would misorder under the hot stencils' signed compare).
  The outer kOpLoadCol path is admitted only for declared-BIGINT
  programs and lands in the BIGINT case — unchanged.
  **Diagnostics fallout fixed too:** the DblqhProxy compile-time
  error inserts were unreachable — `all error N` routes 4xxx to DBTUP
  and 5xxx to DBLQH (`Cmvmi::execTAMPER_ORD`), so the proxy's
  4061/4062 gates never armed. Renumbered to **5119** (dump program +
  translation) and **5120** (compile failure fatal).
- **First-run finding: `kOpSkip` (outer, 29) was the missing piece.**
  The planner emits a Skip after each CASE arm to jump past the
  remaining arms; the bridge rejected it, so Q3's 4060 aborted the
  node on the first run. Lowered to `OP_JUMP` reusing the CASE
  skip-offset resolution pass (end-of-program → tail exit, forward
  only). Diagnosis note: the reject line was suppressed by the 10s
  fallback-log rate limit (a stats-scan fallback ate the window) —
  worth a future "first occurrence per site" refinement. `RepeatAgg`
  needed nothing: it re-emits the aggregate opcode with the same
  agg id, exactly what the mask-index fix makes correct.
- **Tests:** bridge T47/b/c/d + T48 (Skip→JUMP) + updated
  T22b/T22c/T46; groupby-canary Q3 flipped to must-JIT (4060 +
  counter delta); new `rondb_jit_nullable_canary`. **No stencil regen
  needed.**
- Verify: rebuild ndbmtd (+ host tools), `bridge_tests` (51),
  `coldcall_tests`, then `./mtr --suite=ndb_push_agg
  rondb_jit_groupby_canary rondb_jit_nullable_canary` + full suite +
  `testCaseAgg`.

### Original sketch (for reference)

The groupby-canary finding, and the cheapest sub-phase: the planner's
CASE condition emits exactly `Interpreter::Read(attr_id, reg)` +
`LoadConst16(reg, v)` or `LoadConst64(reg)`+2 data words +
`BRANCH_XX_REG_REG` (`ha_ndbcluster_push_agg.cc
emit_int_comparison_branch`); the accept-path machinery is already
lowered. Mostly bridge work reusing existing stencils:

- `BRANCH_*_REG_REG` → the existing Phase 1 hot stencils
  `OP_BRANCH_{LT,LE,EQ,GT,GE,NE}_INT_INT` (register-register compare +
  forward target — same shape, already in the engine). Bridge maps the
  6 wire opcodes, register fields (bits 6..8 / 9..11), and the forward
  offset through the embedded fixup pass.
- `LOAD_CONST16` already lowers; add `LOAD_CONST64` (2 inline data
  words) → existing `OP_LOAD_CONST_INT` (imm64 hole).
- `READ_ATTR` (column → interpreter register): reuse the
  `OP_LOAD_COL_NDB` cold-call shape. **The design decision of this
  sub-phase is null handling**: the interpreter's `Read` sets the
  register's null flag; there is no register null-tracking in the JIT
  yet (that's 5D). v1 options: (a) new helper variant that mirrors the
  interpreter for the *comparison* use only — NULL reads make the
  branch behave exactly as the interpreter's null-register comparison
  does (audit `handleBranchRegReg` first); or (b) translate-time
  restriction to NOT NULL columns (needs schema knowledge the bridge
  lacks — would have to ride on a helper runtime check + a per-row
  fallback that does not exist). (a) is the plausible path; audit
  before building.
- Tests: bridge_tests embedded REG_REG cases (accept + lowering +
  backward-branch reject), coldcall execution case, flip
  groupby-canary Q3 (`delta == 0` → `>= 1`, restore 4060), and run
  `testCaseAgg` under 4060 once green.
- No regen unless a new stencil variant proves necessary.

## 5B — MIN/MAX BIGINT — **DONE & VERIFIED (2026-08-19)**

All tests pass post-regen: bridge_tests (54), coldcall_tests (14 —
incl. the garbage-accumulator first-row-init test), the groupby (Q7)
and nullable (Q2+MIN) canaries, the repointed unsupported-fallback
canary, and the COMPLETE ndb_push_agg sweep.

Shipped as designed. **Build REQUIRES `regen-stencils` first** (two new
stencils + the `JitState::value_initialized[]` accessors):
- `OP_MIN_BIGINT`/`OP_MAX_BIGINT` (kinds 35/36) with shared MM_* fold
  holes; first-row init via the new `value_initialized` INPUT mask (set
  per row by the glue from `AggResItem::type != UNDEFINED && !is_null`
  — per-group state on the grouped path; the stencil stores 1 after
  any reaching row, marks value_updated always). Signed i64 compares
  only (the kernel's unsigned branches are unreachable for admitted
  programs). Unchecked — no arithmetic.
- Bridge: `kOpMinBigint`(18)/`kOpMaxBigint`(16) — the optimizer's typed
  rewrite (kOpMin/kOpMax → typed when the source register is in the
  BIGINT track; strings stay generic → still rejected) runs on BOTH
  compile paths (Create for standalone/scan, DblqhProxy before the
  join-agg compile), so the bridge always sees the typed forms.
  c = agg_index like every aggregate op.
- **Test 27/28 canary repointed** (its durability note foresaw this):
  `MAX(amount)` now compiles, so the unsupported-fallback canary uses
  `SUM(amount % amount)` → kOpMod (durable until 5E; then repoint to a
  DOUBLE shape (5C) or string MIN/MAX (5F)). Result strings synced
  (`sum=0 via interpreter fallback`).
- Tests: bridge T49/T49b, coldcall T14 (first-row init overwrites a
  garbage accumulator — the min-vs-0 bug class), groupby-canary Q7
  (grouped MIN/MAX under 4060 + counter delta; all values > 0 so a
  broken init shows as MIN=0), nullable-canary Q2 + MIN (NULL rows per-
  row-fallback; all-NULL group → NULL MIN).
- Verify order: (1) `regen-stencils`, (2) rebuild ndbmtd + host tools +
  testJoinAggNdbApi, (3) bridge_tests (54) + coldcall_tests (14) +
  admission/microbench, (4) `./mtr --suite=ndb_push_agg` FULL sweep
  with `--force` — existing MIN/MAX tests (testVarcharMinMax stays
  interpreter: strings stay generic) now silently JIT.

### Original sketch (for reference)

The most common missing aggregates (`kOpMinBigint` / `kOpMaxBigint`,
plus generic `kOpMin`/`kOpMax` when the lattice says BIGINT). Two new
hot stencils + one real design point: **first-row initialization**.
The interpreter's Min/Max initialize the result from the first
(non-null) row (`res->type == UNDEFINED` check); the JIT's copy-in
gives a fresh accumulator the value 0, and `min(0, xs)` is wrong. The
glue must pass an *input* initialized-mask into `JitState` (set from
`AggResItem::type != NDB_TYPE_UNDEFINED` at copy-in, per group — the
grouped path makes this per-row state, not per-scan); the MIN/MAX
stencil branches on it: uninitialized → store + mark, else compare +
conditionally store. Marks `value_updated` either way.
- Regen required (2 stencils + the JitState input-mask field).
- **Canary durability:** Test 27/28's unsupported-fallback canary uses
  `MAX(amount)` and depends on `kOpMaxBigint` rejecting — repoint it
  to `kOpMod` (5E is later) *in the same commit*.
- Tests: coldcall (init semantics: first row wins; grouped fresh-slot
  case), bridge lowering, MTR canary extension (grouped MIN/MAX under
  4060 + counter delta; all-rejected group stays NULL).

## 5C — DOUBLE family + unsigned BIGINT — **5C-1 + 5C-2 DONE & VERIFIED (2026-08-20, full ndb_push_agg sweep green post-regen); 5C-3 DONE & VERIFIED (2026-08-20). See `phase_5c_plan.md`**

**Planning finding (2026-08-19): the full type-state lattice is NOT
needed.** `OptimizeProgramBuffer` already type-specializes the outer
bytecode on both compile paths, so the bridge sees typed opcodes; a
linear register-type tracker with embedded-block invalidation
suffices (join meets deferred until an emitter produces cross-branch
register flows). Slices: 5C-1 tracker (no regen) — **done**;
5C-2 DOUBLE family (8 stencils, bit-cast f64 storage; the planned
OP_ROW_FALLBACK_EXIT proved unnecessary — div-by-zero sets
row_fallback inline and continues) — **done & verified 2026-08-20**;
5C-3 unsigned BIGINT (4 stencils reusing the SUM_*/MM_* holes; u64
tracker state + acc-family guard) — **done & verified 2026-08-20;
the phase is complete**. Implementation
record incl. deviations (COUNT type-check removal for AVG(double),
generic kOpDiv lowering): `phase_5c_plan.md`. The section below is
the pre-planning sketch.

**BIGINT UNSIGNED belongs here** (decided 2026-08-19). Today it never
JITs and fails safe twice: the outer `kOpLoadCol` gate rejects any
declared type other than signed BIGINT (whole-program fallback), and
the embedded `READ_ATTR` decode routes `NDB_TYPE_BIGUNSIGNED` to the
per-row fallback — values ≥ 2^63 would misorder under the hot
stencils' signed compares. (Narrower unsigned ints ARE supported in
embedded reads: they zero-extend to non-negative i64.) Full support
needs exactly what this sub-phase builds: a signedness dimension in
the lattice, unsigned variants of the compare / MIN/MAX / checked-SUM
stencils (unsigned compare is a different instruction; unsigned
overflow is a carry check — regen work), bridge admission for the
BIGUNSIGNED type id, and per-result `is_unsigned` writeback — the
`value_unsigned` mask built for COUNT already provides that mechanism.

The architectural chunk. Registers get types (i64 / f64 / unknown) via
the forward dataflow with join meets from `phase_5_implementation.md`
§3 (decision Q1(a), ~150 LOC, `jit_stencil_picker`), and the DOUBLE
stencils land against it: `kOpSumDouble`, `kOpMin/MaxDouble`,
`kOpPlus/Minus/Mul/DivDouble`, double `LoadCol`/`LoadConst`. Needs an
f64 view of the register file (bit-cast in the existing `regs_i64`
storage or a parallel array — decide with the lattice). FP semantics:
no overflow checks (IEEE), div-by-zero per the interpreter's kernel
(audit `SumDouble`/`DivDouble` for NULL-vs-inf behaviour before
emitting). Largest regen of the roadmap (~15–20 stencils).

## 5D — nullable columns + register null-tracking — **PLANNED, see `phase_5d_plan.md`**

**Planning finding (2026-08-20): NEITHER of the two candidate designs
below is needed — the null test FUSES into the load's cold call.**
The load helper already inspects the AttributeHeader; giving it a
return value turns the load stencil into a cold-call branch
(op_branch_attr_eq_null's proven shape) whose taken edge skips the
register's whole consumer chain — exactly the kernels' null-skip.
No JitState null state, no consumer-stencil variants, no ~2× matrix,
no nullability metadata from callers, and per-load graceful
degradation to today's row-fallback load when the skip range is
non-contiguous or touches an embedded block. Slices: 5D-1 i64
(1 stencil + bridge taint walk; nullable canary upgraded to 4060
must-JIT), 5D-2 f64/u64 siblings, 5D-3 embedded READ_ATTR (deferred
until fallback data). Full detail: `phase_5d_plan.md`. Original
sketch below.

Lifts the "non-null BIGINT only" LoadCol contract. Registers gain a
null bit (lattice dimension + runtime representation), nullable
LoadCol stops being a helper-abort, and the aggregate kernels get the
interpreter's skip-null semantics (SUM/MIN/MAX/COUNT skip null inputs;
COUNT's null-register skip — deliberately not lowered in Phase 8 —
becomes real). This is the ~2× matrix multiplier from the original
plan: decide per-family between nullable stencil variants and a
branch-on-null prelude emitted by the bridge (prelude reuses existing
branch stencils and avoids doubling the stencil set — evaluate first).
Also revisits per-group NULL end-to-end proof (the optional grouped
NDB API canary from the Phase 8 plan becomes natural here).

## 5E — integer division / modulo

`kOpDivInt`/`kOpDivIntBigint`, `kOpMod`, `kOpDiv`: cold-call helpers
with MySQL semantics — division by zero yields SQL NULL, which
requires null-capable results, hence after 5D. This also consumes the
Test 27/28 fallback-canary op again: when `kOpMod` lowers, repoint the
canary at a synthetic invalid opcode or a deliberately-reserved one
(document it as permanently unsupported).

## 5F — string aggregation MIN/MAX

`minMaxString` (CHAR/VARCHAR MIN/MAX): cold-call helpers carrying the
charset compare (the scan-filter string path already proved the
pattern: read via the big coutBuffer, `m_cmp` + charset). Last by
default — re-rank up if fallback data shows string-heavy workloads.

## Cross-cutting (applies to every sub-phase)

- **Exit ramp per sub-phase**: each lands with bridge_tests +
  coldcall_tests coverage, an MTR canary assertion (4060 + ndbinfo
  counter delta where the shape is SQL-reachable; NDB API test where
  not), and a full `ndb_push_agg` sweep. The suite is the regression
  net — every newly admitted shape silently starts JITting in existing
  tests.
- **Fallback observability keeps working**: newly admitted ops leave
  `programs_fallback`; verify the counter drops on the target
  workload after each sub-phase (that is the success metric).
- **Regen discipline**: stencil regen (pinned clang 20.1.8) only in
  sub-phases that add stencils (5B, 5C, likely 5D); bridge-only
  sub-phases (5A, mostly 5E/5F) don't touch the generated headers.
- **Out of scope for all of Phase 5**: DECIMAL aggregation, GROUP BY
  prologue optimization, aarch64 perf tuning, multi-leaf join-agg
  programs, Phase 6 (`--force-jit` differential — handled when the
  parallel pushdown-join + CTE branch merges; every sub-phase here
  widens what that harness will eventually verify).
