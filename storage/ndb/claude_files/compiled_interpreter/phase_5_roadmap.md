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

## 5G — DECIMAL loads + generic-aggregate lowering — **DONE & VERIFIED (2026-08-20, census-driven; regen clean, bridge 91/91 + coldcall 30/30, decimal canary green under 4060 on all three tracks incl. a past-2^63 unsigned DECIMAL sum, census decimal_sum flipped to 0, double canary Q7 flipped from negative control to must-JIT, full ndb_push_agg sweep to completion)**

The census showed `SUM(DECIMAL)` pushed and rejected at the load. Two
structural facts made this cheap: (1) the interpreter's DECIMAL load
converts into the EXISTING register tracks (`AlignedType`: scale 0 →
BIGINT — unsigned for DECIMALUNSIGNED — scale > 0 → DOUBLE), so no new
accumulator stencils are needed; (2) the scale is IN THE WIRE (the
DECIMAL kOpLoadCol's second word carries precision<<16|scale), so the
bridge knows the track statically even though the OPTIMIZER leaves
DECIMAL registers untyped.

Implementation: one cold-call stencil `op_load_col_ndb_dec` (operands
a=dst, c=col, b=packed (is_unsigned<<15)|(precision<<8)|scale) +
helper `ndb_jit_h_load_col_dec` mirroring bin2decimal +
decimal2{longlong,ulonglong,double}; NULL and every parse/convert
error (incl. negative-in-unsigned) take the per-row fallback, which
reproduces the interpreter's exact ZAGG_DECIMAL_* errors. Because the
optimizer leaves the aggregates GENERIC over DECIMAL registers, the
bridge also gained **generic kOpSum/kOpMin/kOpMax lowering** driven by
the tracker (F64/U64/I64/NNC → the corresponding typed accumulator;
UNKNOWN keeps the fallback) — which incidentally benefits any future
untyped-track source. First cut has NO null-branching DECIMAL load
(nullable DECIMAL NULL rows use the per-row fallback; extend if
fallback data demands). Tests: bridge 91 (T58a-f: scale-0/scale-2/
unsigned lowering, truncation, bad precision/scale, generic-over-
unknown reject), coldcall 30 (T30: 3-operand cold call round trip),
new `rondb_jit_decimal_canary` (all three tracks under 4060 incl. a
past-2^63 unsigned sum; nullable differential), census decimal_sum
flips 1 → 0. Regen done.

**Verification uncovered ANOTHER pre-existing mysqld consumption bug
(the RONDB-733 pattern, 2026-08-20): pushed MIN/MAX over DECIMAL
printed NULL** — with the interpreter as much as the JIT (pre-5G the
program fell back at the bridge but was STILL pushed, so this was
always broken; nothing tested MIN/MAX(dec) until the 5G canary).
`Item_sum_hybrid::val_str`'s pushed branch handled only STRING pushed
values and otherwise fell through to the unpushed code path, reading
the never-populated internal `value` cache. SUM was unaffected
(Item_sum_sum::val_str routes DECIMAL via val_decimal); integer/
double MIN/MAX were unaffected (they display via val_int/val_real).
Fixed (sql-layer, backportable): val_str's pushed branch now routes
non-string values by representation (DECIMAL_RESULT →
val_string_from_decimal, double → from_real, int → from_int), and the
val_real/val_int pushed branches convert across representations
(int-pushed read as real honors unsignedness; double-pushed read as
int rounds). Portable OFF/ON test: `ndb_push_agg_decimal_minmax`.

## 5H — narrow-int admission — **DONE & VERIFIED (2026-08-21; no regen, bridge 108/108, rondb_jit_int_canary green under 4060 on all 8 widths, census int_sum flipped to 0, full ndb_push_agg sweep passed)**

Census-driven (probe `int_sum`, found by the 5D-3 canary's first run):
the OUTER kOpLoadCol admission was BIGINT-only, so every aggregate
over a plain TINYINT / SMALLINT / MEDIUMINT / INT column — MySQL's
default integer types — was a whole-program fallback, even though
`ndb_jit_h_load_col` and its `_nb` sibling already decoded every
width (added for the embedded READ_ATTR, which shares them).

Slice contents — bridge admission + u64-helper decode only, NO new
stencil, NO regen, NO mysqld change:

- Bridge: SIGNED widths (TINYINT/SMALLINT/MEDIUMINT/INT) sign-extend
  into the i64 track (`OP_LOAD_COL_NDB`, existing stencils); UNSIGNED
  widths zero-extend into the u64 track (`OP_LOAD_COL_NDB_U64`). This
  is the interpreter's own IsUnsigned(type) split: unsigned registers
  accumulate as u64 (SUM to 2^64-1 with the u64 overflow check) and
  set the result's is_unsigned — riding the i64 track instead would
  overflow-exit at 2^63 and drop the metadata. The 5D null-branching
  conversion applies unchanged to both (same opcodes).
- Helpers: `ndb_jit_h_load_col_u64` / `_u64_nb` extend their decode
  from strict BIGUNSIGNED to all five unsigned widths (zero-extend,
  mirroring loadColumnTypedFromBuf); the signed helpers already
  decoded everything. Schema drift keeps the per-row fallback via the
  descriptor switch default.
- Everything downstream is untouched: tracker tracks, generic-
  aggregate lowering, u64 checked arithmetic (5C-4 rules — narrow
  unsigned + NNC constants compile; mixed unsigned/signed VARIABLES
  keep the fallback), acc families, writeback masks.

Tests: bridge 108/108 (T61a INT→i64 SUM; T61b SMALLUNSIGNED→u64
Sum/Min/Max; T61c TINYINT signed MIN/MAX — a negative value must
order below 0; T61d mixed narrow unsigned+signed variable arith
rejects; T61e narrow signed arith chain; T61f narrow unsigned +
const → u64 checked arith). New canary `rondb_jit_int_canary`: all
8 widths under 4060 with boundary values (signed minima prove sign
extension, unsigned maxima prove zero extension, INT UNSIGNED SUM
crosses 2^32), AVG, grouped both-tracks program, nullable narrow
columns on both NB families, narrow arithmetic. Census `int_sum`
flips 1 → 0.

## 5L — double trunc-DIV / fmod — **DONE & VERIFIED (2026-08-21 — regen clean, bridge 141/141, coldcall 34/34, RonSQL canary Q19/Q20 green under 4060, full ndb_push_agg sweep passed). THE OPCODE SPACE IS FULLY COVERED — the only unlowered opcode is kOpSetRegNull, the permanent unsupported-fallback canary, by design.**

The last demand-bearing unlowered shapes: kOpDivInt / kOpMod with a
DOUBLE-track operand (RonSQL-only — the SQL planner never pushes
DIV/%). One cold-call stencil `OP_DIVMOD_CONV` (63; regen) + a
ctx-free helper: sel 0 = truncating DIV (RegDivReg's is_div_int
double arm — divide, isfinite → per-row fallback for overflow, then
floor/ceil toward zero into a SIGNED BIGINT result; the bridge
retypes dst to the i64 track, and a truncated quotient outside int64
also falls back so the interpreter defines that edge); sel 1 = fmod
(RegModReg's double arm — result stays F64; fmod of finite operands
is finite). Divisor 0 → NULL result via the per-row fallback. The
TYPED kOpDivIntBigint with an f64 operand stays rejected (the
optimizer only emits it on both-BIGINT proofs). After 5L the ONLY
unlowered opcode is kOpSetRegNull — the permanent
unsupported-fallback canary, by design.

Tests: bridge 141/141 (T69a trunc-DIV with the i64-track retype
pinned by a SIGNED checked SUM, T69b fmod staying F64, T69c the
typed-with-f64 reject), coldcall 34/34 (T34 packed round trip).
RonSQL canary Q19 (SUM(d DIV 2) + SUM(d % e), binary-exact) and
Q20 (negative trunc-DIV: toward zero, not floor) — 4060 must-JITs
with mysqld server-side baselines.

## 5K — temporal MIN/MAX (RonSQL) — **DONE & VERIFIED (2026-08-21 — no regen; bridge 138/138, RonSQL canary Q16-Q18 green under 4060 incl. the hand-predicted temporal display formats, census green, full ndb_push_agg sweep passed)**

RonSQL pushes MIN/MAX over DATE / YEAR / TIME2 / DATETIME2 /
TIMESTAMP2 (NdbAggregator::TypeSupported admits them; Sum/AVG are
rejected at program build) and fell back wholesale; mysqld never
pushes temporals (its result-decode gap — unchanged). NO regen:

- Bridge: the five temporal type ids (19/26/31/32/33 — the ≥32 pair
  exercising the wire's 6-bit bit-20 type encoding) join the u64
  track. The interpreter loads each type's native packed value as an
  unsigned integer and tags the register unsigned BIGINT, so
  unsigned MIN/MAX over the packed value IS temporal MIN/MAX, and
  any other consumer sees exactly the interpreter's register
  semantics — no fencing needed.
- Helpers: the u64 load pair grows the interpreter's exact decode
  arms — DATE uint3korr, YEAR single byte, and the *2 types'
  big-endian byte fold over header->getByteSize() (memcmp order ==
  chronological order). Nullable temporals ride the NB variant like
  every u64 column.

Tests: bridge 138/138 (T68a DATE MIN/MAX with the NB-converted u64
load; T68b DATETIME2/TIMESTAMP2 through the wide-type encoding).
RonSQL canary Q16-Q18: DATE/YEAR and DATETIME(3)/TIME(3) MIN/MAX as
4060 must-JITs with mysqld server-side baselines, TIMESTAMP(3) as a
counter-only probe (RonSQL displays UTC vs mysqld's session
timezone — outputs not comparable by design). Census date_min
comment updated (still 0 = never pushed by the SQL planner).

## 5J — string CASE conditions on the aggregation path — **DONE & VERIFIED (2026-08-21 — no regen; bridge 136/136, case-nullable canary Q4/Q5 green as 4060 must-JITs, census case_string flipped to 0, full ndb_push_agg sweep passed — the census is FULLY GREEN for every planner-pushed shape, no exceptions)**

The LAST census-confirmed red (`case_string` = 1: the planner pushes
`SUM(CASE WHEN char_col = '...' ...)` and the bridge rejected it).
The block was plumbing, not lowering: OP_BRANCH_ATTR_OP_ARG's helper
reads the condition's instruction words via ctx->prog_buf + op->b,
which only the scan-filter dispatch wired. NO regen, NO new ops:

- translate_embedded_block gains `attr_op_arg_base`: the scan filter
  passes 0 (its prog_buf IS the embedded words); the aggregation
  path passes header_pos + 1, so op->b becomes the ABSOLUTE word
  offset within the compiled region (≤ 1024, comfortably inside the
  16-bit operand hole).
- dbtup_jit_invoke now sets ctx->prog_buf = agg_program() +
  agg_prog_start_pos() — the same base both compile paths use
  (PushdownInterpreter Create and the DBLQH leaf-program compile).
  param_buf stays null: the kernel validator excludes OP_PARAM from
  aggregation programs.
- The helper's NULL semantics (incl. IF_NULL_CONTINUE from the 5D-3
  mysqld fix) already lived in evalBranchColForJit — string
  conditions over NULLABLE columns are 4060-safe day one.

Tests: bridge 136/136 (T67a embedded string condition at program
start — b = 1; T67b mid-program block — b tracks the header
position). rondb_jit_case_nullable_canary Q4/Q5 UPGRADE from
interpreter-correctness probes to 4060 must-JITs. Census
`case_string` flips 1 → 0 — the census scoreboard is fully green
for every shape the SQL planner pushes, with no exceptions.

## 5I — mixed int/double arithmetic — **DONE & VERIFIED (2026-08-21 — regen clean, bridge 134/134, coldcall 33/33, mixed + RonSQL canaries and census green, full ndb_push_agg sweep passed)**

Assessment-driven (post-5E review): the SQL planner pushes +/-/*
over ANY numeric mix and the data node optimizer leaves mixed
operands generic — so every mixed program fell back wholesale,
INCLUDING the TPC-H Q9 pattern (integer-constant arithmetic over
scaled DECIMAL: `price * (1 - discount)` is NNC ⊕ F64 at the
bridge), which undercut the 5E-1 decimal win for exactly its target
queries.

One cold-call stencil `OP_ARITH_CONV_F64` (62; regen) + ctx-free
helper mirroring Reg{Plus,Minus,Mul}Reg's double arm exactly: plain
int→double casts (the kernels have NO ±2^53 guard here, unlike
division), the op, isfinite → per-row fallback (=
ZAGG_MATH_OVERFLOW via the interpreter re-run). packed =
(op_sel << 12) | (flags << 8) | (dst << 4) | src. The 5E-1 generic
case's reject arm becomes the emit; uniform tracks keep their
hot/typed paths; STR/UNKNOWN still reject.

Tests: bridge 134/134 (T66a mixed plus with packed assertions +
SUM_F64 typing, T66b the Q9 shape — mixed sub then HOT both-F64
mul, T66c u64×f64), coldcall 33/33 (T33 packed round trip). RonSQL
canary Q6 FLIPS from the whole-program-fallback negative control to
a 4060 must-JIT. Census gains a `mixed_arith` compile probe
(counter-only — decimal→double display digits are conversion-
dependent). New canary `rondb_jit_mixed_canary`: binary-exact
values, BIGINT⊕DOUBLE, the Q9 decimal shape, u64×double grouped —
all 4060 must-JIT with mysqld-computed baselines.

## 5E — division/modulo + GENERIC arithmetic — **PLANNED, see `phase_5e_plan.md`**

**Scoping finding (2026-08-21, corrected in review): RonSQL emits
the GENERIC arithmetic opcodes** (kOpPlus/kOpMinus/kOpMul/kOpDiv via
NdbAggregator::Add etc.) — but the data node's OptimizeProgramBuffer
rewrites them to the typed forms BEFORE compilation whenever operand
types are statically inferable, so RonSQL's int/double arithmetic
already compiled. What stays generic at the bridge: arithmetic with
DECIMAL operands (the optimizer types decimal loads UNDEFINED — the
5E-1 generic case lowers these via the bridge's richer 5G typing),
mixed int/double, and kOpDiv/kOpMod/untyped-kOpDivInt always. 5E-1 lowers the generic ops through the existing 5C-4/f64
classifiers (no regen) and establishes RonSQL as the test platform
(RONSQL_CLI + rondb_jit_ronsql_canary); 5E-2 adds the four integer
DIV/MOD hot stencils (div-by-zero → per-row fallback, the op_div_f64
pattern; LLONG_MIN/-1 guarded before the hardware divide — x86 idiv
traps; Test 27's fallback canary repoints from kOpMod to
kOpSetRegNull, the permanent unsupported op); 5E-3 adds the generic
'/'-over-integers cold call with RegDivReg's ±2^53 conversion
guards. The SQL planner never pushes /, DIV, or % at all — mysqld
serves as the ground-truth differential in every RonSQL canary.

## 5F — string aggregation MIN/MAX — **PLANNED, see `phase_5f_plan.md`**

**Planning finding (2026-08-20): NO string register model.** The
interpreter's string machinery is pure cold-call territory (collation
compares + winner-buffer management), and its kernel (`minMaxString`,
public) mutates the group's AggResItem directly. 5F fuses load +
kernel into ONE cold call per string aggregate via a small public
façade on AggInterpreterBase (exact kernel reuse: charsets, sidecar,
AGG_CHAR wire, eviction, API merge all come for free), with the
writeback-mask discipline keeping the glue's hands off string slots.
The JIT's real value: MIXED programs (SUM(int), MIN(str)) stay hot
instead of falling back whole; nullable string MIN/MAX is 4060-safe
from day one (the kernel's null skip — no fallback of any kind).
Slices: 5F-1 local columns (1 stencil + bridge fusion +
BR_REG_STR/BR_ACC_STR tracker states), 5F-2 linked columns deferred.
Original sketch below.

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
