# Phase 5C — DOUBLE family + unsigned BIGINT (and why the lattice shrank)

**Status: PHASE COMPLETE — 5C-1, 5C-2 and 5C-3 all DONE & VERIFIED
(2026-08-20; full ndb_push_agg sweeps green post-regen).**

## Headline finding: the full type-state lattice is NOT needed

The original Phase 5 design (`phase_5_implementation.md` §3) called for
a forward-dataflow lattice with join meets inside the JIT. That design
predates a structural fact we established during 5B:
**`PushdownInterpreter::OptimizeProgramBuffer` already type-specializes
the outer bytecode on BOTH compile paths** (in
`PushdownInterpreterFactory::Create` for standalone/scan, and in
`DblqhProxy` before the join-agg compile). The bridge never sees
generic `kOpSum`/`kOpMin`/`kOpPlus` for typed tracks — it sees
`kOp*Bigint` / `kOp*Double`, and loads carry explicit type fields.

What the bridge does need is a **linear register-type tracker**:
`reg_type[8] ∈ {i64, f64, unknown}`, set by loads (from their explicit
type fields) and constants, consumed to validate each typed op's
operands (defense-in-depth against optimizer bugs → reject, fall back),
and **invalidated at embedded blocks** — mirroring the optimizer's own
convention (it skips embedded blocks entirely: `exec_pos += emb_len`;
the planner re-loads outer registers after embedded blocks). No join
meets: no current emitter produces cross-branch register flows in the
outer stream. The full lattice is deferred until an emitter does.

## Key semantics audited (2026-08-19)

- **`SumDouble`** (`AggInterpreterBase.cpp:1108`): first-row-initialize
  (same as MIN/MAX → needs the 5B `value_initialized` mask!) and an
  `isfinite` check on every accumulate — non-finite ⇒
  `ZAGG_MATH_OVERFLOW`. So the JIT double-SUM needs BOTH the init mask
  and an overflow branch (d-hole → `OP_OVERFLOW_EXIT`, like checked
  BIGINT SUM).
- **`RegDivDouble`** (`InterpreterCommonOp.hpp:1089`): divisor == 0.0
  ⇒ the RESULT REGISTER becomes NULL (SQL semantics), and downstream
  kernels skip null inputs. The JIT has no register null-tracking
  until 5D — solved WITHOUT 5D by a new terminator:
  **`OP_ROW_FALLBACK_EXIT`** (sets `JitState::row_fallback = 1` and
  returns). The div stencil branches there on divisor == 0; the glue
  discards the row and the interpreter re-runs it — exact
  null-register semantics per row. Non-finite results branch to
  `OP_OVERFLOW_EXIT` (⇒ ZAGG_MATH_OVERFLOW), matching the kernel.
  This terminator also gives every FUTURE hot stencil a per-row escape
  hatch (the cold-call helpers already have one via the field).
- **`kOpLoadConst`** carries BIGINT / BIGUNSIGNED / DOUBLE (2 raw value
  words). Double constants need NO new stencil: bits are bits —
  `OP_LOAD_CONST_INT`'s imm64 hole carries the double's bit pattern
  (bridge_tests T9's double-const reject flips to accept).
- **Storage decision**: f64 values live bit-cast in the existing
  `regs_i64` / `acc_i64` arrays. Copy-in already bit-copies the
  `AggResItem` value union, so it is type-agnostic as-is. Writeback
  needs a new **`value_double[BC_MAX_ACCS]`** mask (the
  `value_unsigned` pattern): double-agg stencils mark it; the glue sets
  `type = NDB_TYPE_DOUBLE` + `val_double` for marked results.

## Slices

### 5C-1 — bridge register-type tracker (no regen) — **IMPLEMENTED (2026-08-19)**

In the tree: `reg_type[8] ∈ {UNKNOWN, I64, F64}` in the outer translate
walk; producers set types (loads/consts → I64 today, Mov copies,
Bigint arithmetic → I64), i64 consumers (arith operands, SUM/COUNT/
MIN/MAX source registers) reject F64 with the new
`JIT_BRIDGE_TYPE_MISMATCH` reason but tolerate UNKNOWN (documented
invariant: everything producible today, incl. 5A's embedded writes, is
i64); embedded blocks invalidate all registers. **Behavior-neutral by
construction until 5C-2 introduces an F64 producer — the reject paths
are unreachable today, so verification = the existing bridge_tests +
suite still pass unchanged; the F64-mismatch tests land with 5C-2.**

`reg_type[8]` in the outer translate walk: set by `kOpLoadCol` /
`kOpLoadConst` type fields and `kOpMov`; embedded blocks invalidate all
(5A's READ_ATTR / LOAD_CONST64 write registers invisible to the outer
tracker — same convention as the optimizer). Typed ops verify operand
types and reject on mismatch (JIT_BRIDGE_NON_BIGINT reason reused or a
new TYPE_MISMATCH). Pure hardening + the foundation 5C-2/3 hang off;
behavior-neutral for all currently admitted programs (i64-only).

### 5C-2 — the DOUBLE family — **DONE & VERIFIED (2026-08-20)**

Verified: regen-stencils clean (incl. the fold-magic audit's new f64
expected counts), bridge_tests 68/68, coldcall_tests 20/20,
`rondb_jit_double_canary` green (Q1–Q5 must-JIT under 4060 + counter
delta all held — the planner does push every double shape the canary
assumes), full ndb_push_agg sweep to completion. Two verification
findings: (1) the extractor's `classify_tail` keyed KEEP_ALL off the
`_checked` name pattern, so the f64 overflow-branch stencils (same
shape: taken jmp + fall-through jmp) initially fell to the strip path
and their `HOLE_F64_OVF_TGT` PLT32 reloc was rejected as an operand
hole — the five names are now listed explicitly; (2) the div-by-zero
canary query raises "Division by 0" warnings whose count is an
evaluation-frequency implementation detail — suppressed via
`--disable_warnings`, the values are the assertion.

Landed as planned with four deviations worth recording:

1. **`OP_ROW_FALLBACK_EXIT` was not needed.** The 5A cold-call
   convention already covers div-by-zero: `op_div_f64` sets
   `JitState::row_fallback` INLINE (a plain baked-offset field store,
   like `op_filter_reject_exit`'s), stores 0.0, and the blob keeps
   running to completion — the glue discards the row and the
   interpreter re-runs it. No new terminator, no two-target stencil.
2. **COUNT's 5C-1 type check was wrong and is removed.**
   `AVG(double_col)` decomposes into `kOpSumDouble` + `kOpCount` over
   the SAME f64 register; the COUNT stencil never reads the register's
   bits (`acc += 1`), so any register type is fine. The 5C-1
   `BR_REJECT_IF_F64` there would have knocked out every double AVG
   (bridge_tests T51g locks the shape in).
3. **Generic `kOpDiv` lowers when both operands are proven f64** (the
   optimizer never rewrites it to `kOpDivDouble` — it only marks dst
   DOUBLE — so SQL division arrives as generic `kOpDiv`); non-f64
   operands reject UNSUPPORTED (an unimplemented conversion, not a
   type bug → distinct from TYPE_MISMATCH in the fallback logs).
4. **No new f64 register-file accessors.** The stencils load the i64
   bits through the existing fold holes and reinterpret via
   `__builtin_memcpy` (lowers to fmov/movq); finiteness is checked on
   the BIT PATTERN (`(bits & ~sign) >= exp-mask`) because
   `__builtin_isfinite` materialises an FP constant in .rodata — a
   relocation class the extractor doesn't support.

In the tree: 8 new OpKinds (`OP_LOAD_COL_NDB_F64`, `OP_{ADD,MINUS,MUL,
DIV}_F64`, `OP_{SUM,MIN,MAX}_F64`, OP_KIND_MAX = 44); 8 stencils with
shared `FAR_*` / `FSUM_*` / `FMM_*` fold holes and ONE shared
`HOLE_F64_OVF_TGT` overflow symbol (relocations are per-stencil, so
sharing the name is safe); `JitState::value_double[]` mask (appended
last) with writeback to `type = NDB_TYPE_DOUBLE` +
`value.val_double`; helper `ndb_jit_h_load_col_f64` (DOUBLE bit copy,
FLOAT promote via floatget, NULL/unexpected type → per-row fallback);
bridge admission for DOUBLE consts (bits ride the imm64 path),
DOUBLE/FLOAT loads (full 6-bit type decode), the four arithmetic ops
(both operands proven f64; same d-target overflow fixup list) and the
three accumulators (source proven f64; SUM checked). SUM_F64 keeps the
`value_initialized` first-row init — 0.0 + -0.0 would flip the sign of
a single-row SUM(-0.0) (coldcall T15b).

Tests: bridge_tests 68 total (T9 flipped to accept + T9b, T50/T50b lowering
batteries, T51a–g tracker rejects/accepts), coldcall_tests 20
(T15/T15b/T16/T17/T18/T19: accumulator battery, -0.0 init, arith
chain, overflow exit, div-by-zero fallback, f64 cold-call), MTR canary
`rondb_jit_double_canary` (Q1–Q8: scalar/grouped/FLOAT/AVG/arith under
4060 + counter delta; division and DECIMAL as differentials; -0.0
baseline comparison).

Regen-stencils done — the regenerated headers carry the 8 new
stencils and the audit validated the f64 fold-magic counts.

Original plan follows.

- `OP_LOAD_COL_NDB_F64` (cold-call, new helper `ndb_jit_h_load_col_f64`):
  DOUBLE columns load their bits; FLOAT columns promote to double
  (mirroring `handleReadAttrIntoReg`'s FLOAT case); anything else ⇒
  `row_fallback`. NULL ⇒ `row_fallback` (as in 5A).
- Double constants → existing `OP_LOAD_CONST_INT` (bit pattern).
- `OP_ADD_F64` / `OP_MINUS_F64` / `OP_MUL_F64`: fadd/fsub/fmul +
  isfinite check → `OP_OVERFLOW_EXIT` (audit kOpPlusDouble et al.
  kernels in-slice to confirm the isfinite ⇒ overflow pattern).
- `OP_DIV_F64`: divisor == 0 → `OP_ROW_FALLBACK_EXIT`; non-finite →
  `OP_OVERFLOW_EXIT`.
- `OP_SUM_F64`: `value_initialized` + isfinite → overflow branch;
  marks `value_updated` + `value_double`.
- `OP_MIN_F64` / `OP_MAX_F64`: 5B's shape with double compares (plain
  `<`/`>`, matching the kernel; NaN can't reach them — loads/arith
  guarantee finite).
- New `OP_ROW_FALLBACK_EXIT` terminator stencil (shared).
- JitState: append `value_double[BC_MAX_ACCS]`; glue writeback branch.
- Bridge: `kOp{Sum,Min,Max,Plus,Minus,Mul,Div}Double` + double-typed
  loads/consts, validated by the 5C-1 tracker.
- In-slice audits: AVG's pushed decomposition (the ndb_push_agg AVG
  tests — what ops does `AVG(val * 2)` actually emit?); DECIMAL
  constants (the planner claims to push them — as doubles?); the
  double arithmetic kernels' exact isfinite/null behavior.
- Canaries: double-column SUM/MIN/MAX grouped under 4060 + counter
  delta; a div-by-zero row (⇒ per-row fallback ⇒ SQL NULL semantics)
  differential; AVG queries flipping from fallback to JIT; the
  existing ta1/ta2 AVG tests in the full sweep as the regression net.

### 5C-3 — unsigned BIGINT — **DONE & VERIFIED (2026-08-20; regen clean, bridge_tests 74/74, coldcall_tests 23/23, unsigned canary green, full ndb_push_agg sweep to completion)**

In the tree: 4 new stencils (`op_load_col_ndb_u64` with helper
`ndb_jit_h_load_col_u64` — descriptor-BIGUNSIGNED-only, NULL/other →
per-row fallback; `op_sum_u64_checked` — u64 add with carry check →
the shared SUM overflow target, matching SumBigint's uniform-unsigned
path incl. TestIfSumOverflowsUint64; `op_min_u64` / `op_max_u64` —
u64 compares with the 5B first-row-init mask). All three accumulators
mark `value_unsigned` → the existing writeback mirrors is_unsigned.
The u64 stencils REUSE the SUM_* / MM_* fold holes (shared across
signedness variants like ADD_* across checked/unchecked) — only two
new narrow magics (LU64_COL/LU64_DST) for the load helper args.

Bridge: the tracker grows `BR_REG_U64` (declared-BIGUNSIGNED loads
and BIGUNSIGNED constants produce it; the optimizer types BIGUNSIGNED
into the same BIGINT track and the kernels dispatch on the register's
is_unsigned at runtime, so `kOpSumBigint` / `kOpMin/MaxBigint` over a
proven-u64 register lower to the UNSIGNED variants). Signed checked
arithmetic rejects u64 operands (SUM(u_col + 1) falls back whole —
unsigned arith stencils deferred until fallback data demands them).
New **acc-family guard** (`acc_family[BC_MAX_ACCS]` ∈ {unset, i64,
u64, f64}): a multi-arm CASE feeding one agg slot from mixed-family
arms would leave the per-row writeback masks reflecting whichever arm
ran last — mixed-family programs reject (TYPE_MISMATCH) and fall back
whole; the guard also covers the 5C-2 double accumulators.

Interpreter-semantics deltas: none for admitted programs — the
kernels' mixed signed/unsigned branches are unreachable under the
uniform-signedness contract, and the embedded READ_ATTR keeps its
BIGUNSIGNED per-row fallback (so u64 bits never enter registers from
embedded paths — the tracker's UNKNOWN-tolerance invariant holds).

**LATENT DATA-NODE CRASH found & fixed during verification
(2026-08-20)**: `JoinAggInterpreter::ProcessRec`'s JIT dispatch
segfaulted on OUTER-JOIN NULL-EXTENDED rows
(`Dblqh::handleOuterJoinAggKeyNotFound` → `processNullExtendedRow` →
`ProcessRec(nullptr, nullptr, ...)`): the dispatch unconditionally
wrote `req_struct->m_linked_attr_data` with `req_struct == nullptr`.
Latent since the outer-join merge (RONDB-1035/1036) — whether it fired
depended on the shared interpreter instance having a compiled entry
when an unmatched parent arrived; the 5C-3 sweep's scheduling landed
on it (ndb_join_pushdown_agg Test 36, LEFT JOIN + COUNT(*) + GROUP
BY). Fix: null-extended rows never dispatch to the JIT
(`m_jit_entry != nullptr && !m_null_local_columns`) — there is no
local tuple, only the interpreter's kOpLoadCol knows to synthesize
NULL AttributeHeaders (`m_null_local_columns`), and the special entry
points may have switched `m_prog` to a different leaf program than
the compiled one. Same convention as the per-row fallback; no 4060
exemption needed (block_tup is nullptr there, which the 4060 check
already skips). The guard also covers `processRecWithLinkedAttrs`'s
CTE_LOOKUP feed (block_tup == nullptr sets the same flag).

**PRE-EXISTING upstream bug discovered during verification
(2026-08-20), NOT a JIT issue — FIXED the same day in the standalone,
backportable commit `d815f2c1d18` ("RONDB-733: Fix signed-wrapped
SUM/AVG over BIGINT UNSIGNED under pushdown"; verified: new JIT-free
`ndb_push_agg_unsigned` test + full ndb_push_agg sweep green). The
unsigned canary's Q3 has since been upgraded back to past-2^63
values. Original record follows.**: `SUM(bigunsigned_col)` whose sum
exceeds 2^63 prints SIGNED-WRAPPED under aggregation pushdown — with
the interpreter as much as with the JIT. The entire kernel/API chain
is faithful (verified hop by hop: the u64 SUM stencil stores
value_unsigned — confirmed by disassembling the regenerated bytes —
the glue writeback mirrors it, `PrepareAggResIfNeeded` serializes
AggResItem verbatim, NdbAggregator's merge adds val_uint64 and
propagates is_unsigned, and `Result` maps Bigint+is_unsigned →
Bigunsigned). The loss is the LAST hop:
`ha_ndbcluster_push_agg.cc`'s Bigunsigned case calls
`set_pushed_value_int(static_cast<int64_t>(res.data_uint64()))` and
`Item_sum::m_pushed_value_int` is a plain signed int64 with no
unsigned companion flag — SUM's DECIMAL-typed result item then
converts it signed. MIN/MAX are unaffected only because their result
items carry the column's unsigned_flag and reinterpret the bits.
`set_pushed_avg` has the same cast (AVG over huge unsigned sums
wraps too). Fix (separate change, sql-layer): add an
is-unsigned flag next to `m_pushed_value_int` (or a
`set_pushed_value_uint`) and honor it in the pushed-value readers.
The unsigned canary's Q3 keeps its sum below 2^63 until then; the
past-2^63 accumulation is proven at the JitState level by coldcall
T21.

Tests: bridge_tests 74 total (T52/T52b lowering + const, T53a–d arith
reject / mixed-acc reject / COUNT accept / Mov preserves), coldcall 23 total
(T20 min/max ordering across the signed boundary, T21 carry check
past 2^63 then a genuine u64 carry, T22 u64 cold-call), MTR canary
`rondb_jit_unsigned_canary` (Q1–Q4 must-JIT with boundary values
under 4060 + counter delta; Q5 arithmetic negative control on a
no-WHERE scan so a scan-filter compile can't move the counters).

Regen done — headers carry the 4 u64 stencils. Original sketch follows.

Pre-implementation sketch: outer aggregation over BIGUNSIGNED columns:
unsigned MIN/MAX (u64 compares), checked unsigned SUM (carry check),
unsigned load admission (type field BIGUNSIGNED → a load variant that
stores the bits and distinguishes itself from the signed contract; the
embedded READ_ATTR keeps its BIGUNSIGNED per-row fallback). Writeback
via the existing `value_unsigned` mask. Embedded unsigned comparisons
stay on the interpreter (rare; needs unsigned REG_REG stencils — defer
until fallback data demands it).

## Follow-up unlocked by 5C-3 (not scheduled)

`TypeSupported` admits DATE / YEAR / DATETIME2 / TIME2 / TIMESTAMP2
for MIN/MAX precisely because they reduce to unsigned integers
(packed values monotonic with chronological order — see the comments
there). The u64 MIN/MAX stencils are the natural lowering vehicle:
all that's missing is bridge admission for those wire types plus a
load-helper variant that decodes each packed width. Defer until the
fallback counters show temporal MIN/MAX traffic.

## Non-goals (explicit)

- Register null-tracking (5D) — `OP_ROW_FALLBACK_EXIT` covers 5C's
  needs per row.
- Embedded DOUBLE conditions (CASE WHEN double_col …): READ_ATTR's
  FLOAT/DOUBLE per-row fallback stays; revisit with 5D or when the
  fallback counters show demand.
- DECIMAL as a first-class type; FLOAT beyond promote-on-load.
- The join-meet lattice — only when an emitter produces cross-branch
  register flows.

## Verification pattern (per slice)

bridge_tests lowering/reject cases; coldcall execution cases (incl.
bit-pattern round-trips and the div-by-zero fallback exit); MTR canary
with 4060 + `ndbinfo.jit` counter delta; **full `ndb_push_agg` sweep
with `--force` to completion** — the AVG/double tests silently start
JITting and are the real net.
