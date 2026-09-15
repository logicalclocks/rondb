# M2 — detailed plan: type fidelity (exact DECIMAL, SUM overflow, display rules)

Milestone M2 of `ronsql_fs_support_plan.md`, expanded to implementation
detail after the M1 close (2026-09-15, `requirements_reports/2026-09-15/`:
15 supported, R-A5-types the only unsupported row, through F4 / F5 / F6).
Sources: the RONDB-1121 ledgers (`findings/smoke.md` F2 F3 F4 F5 F6 F21
F22) and a survey of the numeric aggregation path made for this plan
(kernel interpreter, wire record, NDB API merge, compiled interpreter,
RonSQL compiler and printer; the file:line anchors below are from it).

| package | finding | requirement row | effort | order |
|---|---|---|---|---|
| M2.1 | F5 / F21 / F2: exact DECIMAL aggregation, wide SUM | R-A5-types | 2–3 weeks | first (kernel, wire, API, printer) |
| M2.2 | F22 / F6: one overflow semantics on every path | R-A5-types (F6 row) | 3–4 days | inside M2.1 (same wire change) |
| M2.3 | F3 / F4: AVG scale and FLOAT display rules | R-A5-types (F4 row) | 3–4 days | after M2.1 (printer already open) |
| M2.4 | JIT lowering of the wide accumulators | JIT arm parity | 1–2 weeks | before re-arming `ronsql_fs_jit` strict |
| M2.5 | framework and tests: retire F4 / F5 / F6 / F22, acceptance PASS | R-A5-types | 3 days | last |

Exit: `.fs_verify --requirements --all --vectors` reports
`acceptance=PASS` on the base, JIT and ng2r2 arms (20 requirements, 0
unsupported); `ronsql_fs_ng4r2` records the same outputs as the base
suite; `Known["F4"]`, `["F5"]`, `["F6"]`, `["F22"]` retired; the MTR
numeric canonicalization hook (`CanonNumeric`) removed from the templates
renderer; the JIT fallback pins of the four strict-armed Hopsworks tests
stay 0.

---

## 0. What the engine does today (survey summary)

**Kernel.** One 16-byte register serves every aggregate slot,
`struct Register { DataType type; DataValue value; bool is_unsigned; bool is_null; }`
with `DataValue = union { Int64; Uint64; double; void* }`
(`NdbAggregationCommon.hpp:191-213`); `typedef Register AggResItem` is
also the wire item.  `AggInterpreterBase::AlignedType()`
(`AggInterpreterBase.cpp:843-883`) collapses every numeric column to
BIGINT or DOUBLE — **`DECIMAL → scale == 0 ? BIGINT : DOUBLE`** — and the
column load (`loadColumnTypedFromBuf`, `:292-409`) runs `bin2decimal()`
then `decimal2double()` (scale > 0) or `decimal2longlong()` (scale 0), so
a scaled DECIMAL is lossy from the first row; FLOAT is widened to double
the same way (`:276-283`).  `Sum()` (`:905-1023`) checks Int64 / Uint64
overflow per row and returns −1 → `ZAGG_MATH_OVERFLOW` (1860,
`Dbtup.hpp:250`); the DOUBLE arm only rejects non-finite results.  MIN /
MAX / COUNT never report.  The normal (non-aggregating) interpreter
already does mixed-signedness `+ − × ÷ %` in `__int128` with range checks
(`DbtupExecQuery.cpp:6928-7010`), and `decimal_t` is linked into dbtup
(`AggInterpreterBase.hpp:55, :557`) but used only for loading.

**Wire and merge.** Per-fragment partials are the raw `AggResItem`
array memcpy'd into the result record (`AggInterpreter.cpp:420-600`,
markers `AGG_RESULT` 0xFF00 / `AGG_CHAR_RESULT` 0xFF02,
`AttributeHeader.hpp:110-120`).  The API and the kernel's cross-node CTE
merge share `aggMergeSum / Min / Max / NumericSlot`
(`NdbAggregationCommon.hpp:264-412`); `aggMergeSum` is the unchecked
`dst->value.val_uint64 += src.value.val_uint64` (`:316`) with the
documented policy "Overflow reporting is not introduced here because
neither of the existing distributed merge call chains propagates it
reliably" (`:271-277`); `ProcessRes` (`NdbAggregator.cpp:352-704`) has
no error return.  A DOUBLE contribution promotes the slot through
`aggSlotAsDouble`.  `NdbAggregator::Result` exposes only
`data_int64 / data_uint64 / data_double / data_str`
(`NdbAggregator.hpp:155-217`).  AVG on the CTE path is SUM + a hidden
COUNT divided once as double on the owner (`JoinAggInterpreter.cpp:333-359`);
on the main path RonSQL emits SUM and COUNT slots and divides client
side.

**JIT.** Three numeric accumulator families, i64 / u64 / f64 (+ string),
all stored as one `int64_t` per slot in `JitState::acc_i64`
(`jit/ndb_jit_bridge.c:786-787, :2800-2801`; `jit/jit1.h`); checked SUM
stencils use `__builtin_add_overflow` and raise the interpreter's 1860
(`jit/stencils_src.c:583-593`, `DbtupJitGlue.cpp:1943-1945`); a slot
claimed by two families or an unknown track is a whole-program
`JIT_BRIDGE_TYPE_MISMATCH / UNSUPPORTED_OP` fallback to the interpreter
(`:2810-2819`, `:3841-3844`); DECIMAL loads are a cold call mirroring the
interpreter (`DbtupJitGlue.cpp:824-915`).  Writeback emits only DOUBLE or
BIGINT (`:1962-1983`).

**RonSQL.** The SVM has no DECIMAL notion (`AggregationAPICompiler.hpp:39-118`);
scale and precision are display metadata only
(`ColumnMetadata`, `ResultPrinter.hpp:51-58`, filled in
`build_result_column_metadata`, `RonSQLPreparer.cpp:6868-6957`; no
source-type field).  `print_aggregate_result` prints a scaled DOUBLE with
`%.*f` only when `aggregate_arg_scale` finds a bare column load under
MIN / MAX / SUM **and** the source precision is ≤ 15
(`ResultPrinter.cpp:521-528`, `:2478-2545`); AVG divides two registers
and prints `my_fcvt(result, 4, …)` for every argument type
(`:886-908`, duplicated for the ORDER BY path at `:1954-1990`); FLOAT
results print through `my_fcvt_compact` (17 significant digits,
`:2329-2343`).  The CTE path already derives MySQL's AVG scale
(`RonSQLPreparer.cpp:10453-10523`, `:10856-10930`) — the model for M2.3.
NDB 1860 surfaces as a SEMANTIC `RonSQLPermanentError` with NDB's text
(`RonSQLPreparer.cpp:12453-12543`).  JSON writes every number bare.

## 1. Target semantics: stock MySQL

| statement | MySQL result | RonSQL today | after M2 |
|---|---|---|---|
| `SUM(INT / BIGINT)` | DECIMAL, exact, never overflows | Int64, 1860 in a fragment, wraps at the merge (F6 / F22) | exact wide integer, printed as an integer |
| `SUM(DECIMAL(p,s))` | DECIMAL(·, s), exact | double, last digit topology-dependent (F5 / F21), `%.2f` when p ≤ 15 | exact scaled wide integer, printed with scale s |
| `MIN / MAX(DECIMAL(p,s))` | DECIMAL(p,s) | double, scale dropped or `%.*f` (F2 / F5) | exact, printed with scale s |
| `AVG(INT)` | DECIMAL scale 4, rounded half up | double, `%.4f` | wide SUM ÷ COUNT in decimal, scale 4 |
| `AVG(DECIMAL(p,s))` | DECIMAL scale min(s+4, 30) | double, `%.4f` (F3) | decimal division, scale min(s+4, 30) |
| `AVG(FLOAT / DOUBLE)` | DOUBLE, shortest round-trip text | `%.4f` (F3) | double, `my_fcvt_compact` |
| `SUM / AVG(FLOAT)` | DOUBLE | double | unchanged |
| `MIN / MAX(FLOAT)` | FLOAT, FLT_DIG (6) significant digits | 17 digits (F4) | 6 significant digits (`my_gcvt` float) |
| `COUNT` | BIGINT | unchanged | unchanged |
| arithmetic inside an aggregate argument (`SUM(a*b)`) | BIGINT ops error on overflow (1690); DECIMAL ops exact with scale rules | Int64 checked (1860); DECIMAL via double | unchanged: integers error like MySQL, DECIMAL expressions stay on the double path (Hopsworks emits none; documented) |

DOUBLE sums stay order-dependent in the last ulp on both engines
(MySQL sums rows in scan order, RonSQL per fragment); the framework's
relative tolerance covers it and `benchmarks.md` / `BUGS_TODO.md` record
the ng4r2 display flake of that class.

## 2. Design: a wide exact register, opted in per program

One representation carries everything exact: a **scaled 128-bit
integer** — `Int128 value` plus `Uint8 scale` — where DECIMAL(p,s) is
`value = digits × 1` with the column's scale, integers have scale 0, and
SUM / MIN / MAX / COUNT are integer operations.  `__int128` covers 38
decimal digits: every Hopsworks money / measure column (DECIMAL(12,2),
(18,2), (30,10)) and any realistic SUM over them fit; columns with
p > 38 keep today's double path (documented, `decimal_t` would be the
extension if ever needed).  This subsumes the overflow question: a wide
SUM of BIGINTs cannot overflow in practice (2^64 rows × 2^63), matching
MySQL's DECIMAL widening.

**Kernel register.** `Register` gains the wide value without growing the
narrow case: keep the 16-byte `Register` for BIGINT / DOUBLE / string
and add a parallel wide store — `DataType NDB_TYPE_WIDE` (new value in
the aggregation type enum) whose `DataValue.val_ptr`-sized payload
indexes an `Int128 m_wide[slot]` array plus `Uint8 m_wide_scale[slot]`
allocated with the slot arrays in `AggInterpreterBase::Init` (and per
group for GROUP BY, next to the per-group `AggResItem` array).  Loading:
new opcode `kOpLoadColExact` (two program words like the DECIMAL load:
`decimal_info = precision << 16 | scale`) → `bin2decimal()` →
`decimal2int128` (a 20-line helper over `decimal_t::buf` — no
`decimal_t` arithmetic) for DECIMAL, plain sign-extension for integer
columns; `kOpSumWide / kOpMinWide / kOpMaxWide` on the wide store with
`__builtin_add_overflow` on `__int128` (→ 1860 only for the impossible
case), and COUNT unchanged.  Mixed narrow/wide operands in one slot are
a compile-time error in the API (`NdbAggregator` refuses), never a
runtime promotion.

**Wire.** A record that carries wide slots uses a new marker
`AGG_RESULT_WIDE` (0xFF03) and 32-byte items
`{ type, is_unsigned, is_null, scale, Int128 value }`; narrow programs
keep `AGG_RESULT` unchanged, so the MySQL pushdown (`ha_ndbcluster_push_agg`)
and any old API are untouched.  Producer: `PrepareAggResIfNeeded`
(`AggInterpreter.cpp:420-600`) and the JoinAgg finalize / redistribute
path; consumer: `NdbAggregator::ProcessRes` (`NdbAggregator.cpp:352-704`)
selects the item size by marker.  Rolling upgrade: RonSQL emits wide
programs only when `Ndb::getMinDbNodeVersion()` is ≥ the release that
understands `kOpLoadColExact`, otherwise it compiles the legacy program
(today's behaviour, with today's findings) — the same gate pattern the
JIT feature flag uses.

**API merge.** `aggMergeSum / Min / Max` gain the wide arm (exact
`__int128` add / compare; a scale mismatch is an assert, both sides come
from the same column); `NdbAggregator::Result` gains
`is_wide()`, `wide_scale()`, `data_int128()` and a
`data_decimal_str(char*, size_t)` that renders `value / 10^scale` with
the scale (sign, integer part, `.`, zero-padded fraction) — the one
formatter the printer and the CTE virtual-column path share.
`aggMergeNumericSlot`'s `kOpAvg` arm is unchanged (CTE AVG stays double
in M2.1; see M2.3 for the exact CTE AVG).

**RonSQL compiler.** `AggregationAPICompiler` learns the operand type
at `Load` (it has the dictionary column): DECIMAL with p ≤ 38 and every
integer column under SUM / MIN / MAX → `LoadColumn(…, exact)`; the SUM
of an integer column also goes wide (that is the F22 fix); expressions
(arithmetic, GREATEST / LEAST) keep the narrow path.  `ColumnMetadata`
gains `NdbDictionary::Column::Type source_type`.  The printer's
`print_aggregate_result` gets a `Wide` arm that calls
`data_decimal_str` (both copies: `:2478` and the ORDER BY twin
`:1954-1990`); the `pr <= 15` gate and `aggregate_arg_scale`'s `%.*f`
arm disappear with it.  JSON keeps numbers bare (a DECIMAL is a JSON
number; clients that need the digits read the text — MySQL's JSON
functions do the same), documented next to the binary rule on
`RonSQLExecParams::OutputFormat`.

**CTE / join path.** CTE MIN / MAX / SUM over DECIMAL currently ride the
double virtual column with a derived scale (`RonSQLPreparer.cpp:10964-10967`);
with wide slots the virtual column becomes a DECIMAL(38, s) typed
column filled from `data_decimal_str` → `decimal_str2bin`
(`decimal_utils.hpp:60-81`), and the pass-through printer's existing
DECIMAL arm (`ResultPrinter.cpp:1423-1436`, `decimal_bin2str`) prints it
— exact through CTE_SCAN / CTE_LOOKUP consumers without new printer
code.  The kernel-side CTE materialization (`JoinAggInterpreter`,
`Dblqh::checkCteReady`) must carry the wide items through
`mergeAccumulators` (`JoinAggInterpreter.cpp:1556-1588`) and the
virtual-table row build; this is the largest single piece of M2.1.

**Alternatives considered.** (a) `decimal_t` accumulation in the kernel:
exact to 65 digits but ~9 words per slot, `decimal_add` per row, and a
new JIT cold path per opcode — rejected for cost; int128 with the p ≤ 38
cap covers the requirement.  (b) Widening the merge only (checked add or
int128 at `aggMergeSum`): fixes F22 but not F5 / F21 / F2, which need
exactness before the first accumulation.  (c) Client-side correction
(re-scaling doubles in the printer): cannot recover lost digits.

## M2.1 — F5 / F21 / F2: exact DECIMAL aggregation and wide SUM

**Steps.**

1. `NdbAggregationCommon.hpp`: the wide `DataType`, the 32-byte wire item,
   `aggMergeSum / Min / Max` wide arms, `decimal2int128` /
   `int128_to_decimal_str` helpers (header-only, shared by kernel and
   API), unit-tested in a new `AggWideUnitTest.cpp` next to
   `OverflowUnitTest.cpp` (the int128 overflow predicate there is the
   model).
2. Kernel: `kOpLoadColExact / kOpSumWide / kOpMinWide / kOpMaxWide` in
   `AggInterpreterBase` (interpreter loop, program scanner word counts,
   `AlignedType`), the wide store in `Init` and per group, the record
   producer with `AGG_RESULT_WIDE`, `JoinAggInterpreter::mergeAccumulators`
   and the CTE virtual-table row build.
3. API: `NdbAggregator::LoadColumnExact`, `SumWide / MinWide / MaxWide`
   (program builder + `agg_ops_` merge dispatch), `ProcessRes` item size
   by marker, `Result` accessors; a `testNdbApi`-style regression
   (`testAggregation` if present, else a new `testAggWide`) that runs
   SUM / MIN / MAX over DECIMAL(18,2) rows beyond 2^53 cents and over
   BIGINT rows whose total exceeds 2^63 on a 2-fragment table and
   asserts the exact strings.
4. RonSQL: the compiler's exact-load decision, `ColumnMetadata.source_type`,
   the `Wide` printer arm in both print programs, the CTE virtual column
   as DECIMAL(38, s), the min-version gate, the legacy fallback.
5. Tests: `suite/ronsql/t/ronsql_decimal_exact.test` (+ `ronsql_jit`
   mirror) through `ronsql_compare.inc` strict: DECIMAL(18,2) beyond
   2^53 cents, DECIMAL(30,10), DECIMAL(38,2) at the cap, DECIMAL(40,2)
   (documented double fallback, recorded diff), scale-0 DECIMAL(25,0)
   (was rejected "wider than the 64-bit integer range" — now exact),
   negative and NULL groups, GROUP BY with many groups, a CTE body SUM
   consumed by CTE_SCAN and by CTE_LOOKUP, SUM(BIGINT) over ±2^63
   rows, JSON forms through `ronsql_json_check.inc`.  The fs suites:
   `EDGE-decimal-large`, `EDGE-null-decimal`, `S1-decimal-k31` become
   strict; `EDGE-float-exact` regains `SUM(dec_val)` (F21); the ng2r2
   and ng4r2 mirrors must record the same bytes as the base suite.

**Effort.** 2–3 weeks (the CTE materialization and the wire producer are
the bulk); the wire change needs a release note.

## M2.2 — F22 / F6: one overflow semantics on every path

With M2.1 every RonSQL integer SUM is wide, so `SUM(BIGINT)` returns
MySQL's value (`9223372036854775808` for the probe) on every topology
and neither 1860 nor a wrapped value can appear for a plain column SUM.
What stays:

- Arithmetic inside an aggregate argument stays Int64-checked (1860),
  as MySQL errors on BIGINT expression overflow; the message classifies
  SEMANTIC (M1.0).  `EDGE-big-overflow` moves from `REJECT(expected)` /
  `KNOWN-WRONG` to `PASS`.
- The narrow merge (`aggMergeSum` BIGINT arm) is still reachable by
  legacy programs and the MySQL pushdown.  Replace the modular add with
  `__builtin_add_overflow` and propagate: `aggMergeNumericSlot` returns
  a bool, `ProcessRes` records `m_merge_overflow`, `NdbAggregator`'s
  drain reports NDB error 1860 through the scan operation's error the
  way a fragment error does; the kernel `mergeAccumulators` arm sets the
  CTE error the finalize barrier already propagates.  Then the policy
  comment at `NdbAggregationCommon.hpp:271-277` is retired.  This is the
  "overflow overhaul" noted on 2026-09-12; it lands inside M2.1 because
  it edits the same functions.
- `ronsql_overflow.test` gains the two missing pins: a fragment-level
  SUM overflow (now widened: MySQL's value) and an expression overflow
  (still 1860); the JIT mirror pin stays.

**Effort.** 3–4 days.

## M2.3 — F3 / F4: AVG scale and FLOAT display rules

1. **AVG.** The main-path printer has SUM and COUNT registers; with
   M2.1 the SUM of an integer or DECIMAL operand is wide, so
   `print_avg` computes `SUM / COUNT` in decimal: scale
   `s + 4` capped at 30 (`div_precision_increment`), ROUND_HALF_UP,
   rendered by `int128_to_decimal_str` after the scaled division
   (`(value × 10^4 + count/2) / count` in `__int128`, exact for the
   p ≤ 38 domain); FLOAT / DOUBLE operands keep the double division and
   print with `my_fcvt_compact` (MySQL's shortest round-trip text)
   instead of `my_fcvt(…, 4, …)`.  `Cmd::print_avg` gains the operand
   type and scale.  The CTE path's `finalizeAvgSlotArray` stays double
   in M2 (its consumers compare and re-aggregate doubles today); the
   exact CTE AVG is a follow-up once a consumer needs it — record it in
   `BUGS_TODO.md`.
2. **FLOAT.** `ColumnMetadata.source_type` (M2.1) marks FLOAT sources;
   MIN / MAX (and pass-through and GROUP BY FLOAT columns, which have
   the same 17-digit rendering today) print with `my_gcvt(value,
   MY_GCVT_ARG_FLOAT, …)` — FLT_DIG significance, MySQL's rule
   (`strings/dtoa.cc:256-313`).  SUM / AVG over FLOAT stay double.
3. **DECIMAL MIN / MAX scale (F2)** is solved by M2.1's exact path; the
   remaining F2 face — arithmetic under SUM (`SUM(a*(1-b))` in
   `ronsql_dbt3_1_2`) — stays on the double path and keeps its recorded
   diff, documented as the DECIMAL-expression limitation.

Tests: `ronsql_formatting.test` extended (AVG over INT / DECIMAL(12,2) /
FLOAT / DOUBLE, FLOAT MIN / MAX / pass-through / GROUP BY) with strict
compare; fs `S1-avg-k31`, `EDGE-null-avg`, `EDGE-float-rounding` strict,
`Known["F4"]` retired.

**Effort.** 3–4 days.

## M2.4 — JIT lowering of the wide accumulators

Until lowered, a program with a wide slot is a whole-program bridge
fallback (`JIT_BRIDGE_UNSUPPORTED_OP`), which the strict-armed
`ronsql_fs_jit` Hopsworks tests turn into query failures for every
statement with a DECIMAL or integer SUM — i.e. most of the corpus.  So
M2.4 lands before the JIT mirrors are re-recorded (M2.1 and M2.4 on the
branch together, or the strict arm is dropped to pin-only for the
interim and the fallback pins move; the plan prefers the former).

- Bridge: new track / family `BR_REG_W128 / BR_ACC_W128`
  (`ndb_jit_bridge.c:786-787, :2800-2801`), `kOpLoadColExact` lowered
  as a cold call (`ndb_jit_h_load_col_exact`, the model is
  `ndb_jit_h_load_col_dec`, `DbtupJitGlue.cpp:824-915`), `kOpSumWide /
  MinWide / MaxWide` in the aggregate lowering switch (`:3773-3857`).
- Storage: `JitState` gains `__int128 acc_w128[BC_MAX_ACCS]` (or two
  `int64_t` per slot) with the copy-in / copy-out in the glue
  (`DbtupJitGlue.cpp:1911-1920`, `:1962-1983`) writing the wide items.
- Stencils: `op_sum_w128_checked` (`__builtin_add_overflow` on
  `__int128`, musttail to the overflow exit), `op_min_w128 / op_max_w128`,
  in `jit/stencils_src.c`, regenerated `stencils_{arm64,x86_64}.h`.
- Coverage: `ronsql_jit` mirrors of `ronsql_decimal_exact` and the
  extended `ronsql_formatting` with fallback pin 0; `ndbinfo.jit`
  fallback census unchanged for the fs corpus.

**Effort.** 1–2 weeks (RONDB-1056 owners; the stencil regeneration on
both architectures is the slow part).

## M2.5 — framework and tests

| change | package |
|---|---|
| retire `Known["F5"]` (`EDGE-decimal-large`), `Known["F6"]` + `Known["F22"]` (`EDGE-big-overflow`), `Known["F4"]` (`EDGE-float-rounding`) in `cases/known.go` / `cases.go` | M2.1 / M2.2 / M2.3 |
| `canon`: `AvgTolerance` → exact compare (keep the constant for `--tolerance` users), DECIMAL stays exact | M2.3 |
| `mtr/templates.go`: drop `CanonNumeric` and `needsNumericCanon` (F2 / F3 hook); regenerate `ronsql_fs_templates.test`, re-record base / jit / ng2r2 / ng4r2 | M2.3 |
| `EDGE-float-exact` regains `SUM(dec_val)`; smoke `PROBE EDGE-FLOAT-A` too (F21 retired) | M2.1 |
| envelope fuzzer: `probe` production's F6 overflow probe becomes a positive case (`SUM(big_val)` = MySQL's value); expectation row `F6` removed | M2.2 |
| spec fuzzer: no change (DECIMAL / AVG already sampled; the canonicalizer's tolerance was masking F3) | — |
| `ronsql_fs_ng4r2`: results identical to the base suite (the topology mirror's reason to exist) | M2.1 |
| requirements: R-A5-types SUPPORTED → `acceptance=PASS`; new `requirements_reports/<date>/` on three arms | M2.5 |
| `benchmarks.md` §8: re-run `fs_hw` (`agg_point`, `agg_batch*`, `collect*`) — the wide path costs one 128-bit add per row and 16 more bytes per slot on the wire; pin the plan lines | M2.5 |

## Order and dependencies

1. **M2.1 wire and kernel first** (steps 1–3), behind the new opcodes so
   nothing changes for existing programs; the API regression test is the
   gate.
2. **M2.2** inside the same change set (same functions).
3. **M2.1 RonSQL side** (step 4) and **M2.4 JIT** together, then the MTR
   tests (step 5) and the JIT mirrors.
4. **M2.3** on the open printer.
5. **M2.5** last: retire expectations, regenerate, re-record, the
   requirements run on three arms, the bench run.

Risks: the wire item size change (mitigated by the marker and the
min-version gate); CTE materialization of wide items (the largest
unknown — prototype it first on `JoinAggInterpreter` with a single-op
CTE body); JIT stencil regeneration on both architectures; the DOUBLE
sum order dependence is out of scope and stays under the tolerance.
