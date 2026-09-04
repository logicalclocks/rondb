# ronsql_jit / ronsql_cte_jit — full-corpus JIT enforcement for RonSQL

**Status: Slice 1 DONE & VERIFIED (2026-08-27 — all four suites
green at --parallel=6: both recorded jit arms, pin-stability re-runs,
and the first-ever pure-interpreter OFF runs of both corpora). The
recorded corpus census is below; slice 2 executes against it.**

**Goal (Mikael, 2026-08-25):** suites `ronsql_jit` and
`ronsql_cte_jit` that ENSURE every query in the `ronsql` and
`ronsql_cte` categories executes on compiled interpreter code —
extending Phase 6-2's conformance instrument from hand-picked
canaries to the full RonSQL query corpus. Successor milestone to
Phase 6 (`phase_6_plan.md` §5 holds the design notes recorded at
phase close).

## The three slices

**Slice 1 — census-first mirrors (enforcement OFF).** The
`ndb_push_agg`/`ndb_push_agg_jit` structure applied to the RonSQL
corpora:
- `suite/ronsql/my.cnf` and `suite/ronsql_cte/my.cnf` pin
  `CompiledInterpreter=OFF` (they had been running the JIT via the
  AUTO default since Phase 4 — the OFF arm is the first-ever pure
  interpreter run of these corpora; any result difference would be a
  JIT correctness bug, which five phases of differential testing say
  won't happen).
- NEW `suite/ronsql_jit` (55 mirrors) and `suite/ronsql_cte_jit`
  (25 mirrors + `ronsql_cte_jit_census` moved here natively — its
  must-JIT asserts belong in the JIT arm, and the OFF'd base suite
  would abort them). Each mirror is a one-line `--source` of the
  original bracketed by a recorded `programs_fallback` delta pin —
  the recorded value IS the pin, and the first recording IS the
  census: it enumerates, per test, every program the corpus submits
  that the bridge rejects.
- The ×4 `ronsql_cte_ng*` topology suites keep their own my.cnf
  (JIT on via AUTO): they test distribution, and running them with
  the JIT active is extra coverage, not a differential arm.

**Slice 2 — lower the gaps the census names.** Expected fronts, from
the Phase 6 backlog:
- Scan-filter (Phase 7) expansion: RonSQL WHERE filters emit beyond
  the v1 NULL-branch + comparison subset — the largest expected
  reject source (`testInterpreterTypedRegs` pinned 1888 counts of
  this family).
- CTE consumer filters (unparks Phase 6-5): structurally never reach
  the JIT today; consumer-side WHERE/HAVING shapes in the corpus
  need the CTE-filter compile + dispatch slice.
- Aggregation-op leftovers the census names (embedded
  `READ_AGG_REG_TO_REG` is already known from `testCteNdbApi`;
  GREATEST/LEAST, subquery shapes, whatever else appears).
Each front is its own implement → verify → commit slice, and the
pins flip from N to a smaller N (re-recorded deliberately) as gaps
close.

**Slice 3 — flip on enforcement.** A NEW error insert ("4060-lite"):
abort ONLY on a program-level miss (`m_jit_entry == 0` at an
aggregation dispatch; scan-filter equivalent for eligible filters) —
UNLIKE 4060 it lets by-design per-row fallbacks (NULL edges,
div-by-zero, ±2^53 guards) pass, because the corpora legitimately
contain them and "uses compiled interpreter code" must not outlaw
the per-row escape hatch. Armed suite-wide via the mirror wrappers
once slice 2 gets the pins to their floor; the remaining nonzero
pins become the committed exemption list.

## Slice 1 implementation record (2026-08-25)

- Both base my.cnf files gained the OFF pin with a pointer note;
  both jit suites `!include` the base config and override
  `CompiledInterpreter=ON` — same include-chain + later-wins
  mechanics as `ndb_push_agg_jit`.
- 80 mirror wrappers (55 + 25) in the exact 6-2 pattern; result
  files copied from the base suites (byte-identical bodies — the
  delta lines get their values on the first `--record`).
- `ronsql_cte_jit_census` moved (`git mv`) with no content change:
  its 4060 + counter-sum asserts now run under the suite whose
  config guarantees the JIT is on.
- Nothing else in either base corpus references `ndbinfo.jit` or
  arms JIT error inserts (checked), so the OFF arm is clean;
  `ronsql_hopsworks.pl` is referenced by absolute
  `$MYSQL_TEST_DIR` path, mirror-safe; base `disabled.def` is empty.

**Verification (Mikael runs; --parallel=6 for these suites on this
machine).**
1. Record the census (the 80 mirror pins; recording the census test
   too is safe — its assert.inc guards fail loudly even under
   --record):
   `./mtr --record --suite=ronsql_jit --parallel=6`
   `./mtr --record --suite=ronsql_cte_jit --parallel=6`
2. Normal runs prove pin stability:
   `./mtr --suite=ronsql_jit --parallel=6`
   `./mtr --suite=ronsql_cte_jit --parallel=6`
3. The OFF arms (first pure-interpreter run of these corpora):
   `./mtr --suite=ronsql --parallel=6`
   `./mtr --suite=ronsql_cte --parallel=6`

**Exit (slice 1) — MET (2026-08-27).** All four suites green; the
recorded deltas below are the corpus census.

## The corpus census (recorded 2026-08-27)

~1234 program rejects across 30 of 80 tests (50 tests are already
reject-free). Ranked:

| Test | Delta | | Test | Delta |
|---|---|---|---|---|
| ronsql_hopsworks | **304** | | ronsql_cte_greatest_least | 22 |
| ronsql_basic | **168** | | ronsql_orderby_stress | 16 |
| ronsql_emptytable_and_nulls | **144** | | ronsql_cte_scalar | 16 |
| ronsql_cte_greatest_least_v4 | **104** | | ronsql_constants | 16 |
| ronsql_cte_greatest_least_v5 | 54 | | ronsql_cte_greatest_least_v2a | 12 |
| ronsql_cte_dd_filter | 48 | | ronsql_cte_case | 10 |
| ronsql_cte_or_body | 44 | | ronsql_dbt3_1_2 | 8 |
| ronsql_cte_dd_bigquery | 44 | | ronsql_date_sub | 8 |
| ronsql_subquery_agg_ext | 38 | | ronsql_cte_greatest_least_v6 | 8 |
| ronsql_minmax_string | 36 | | ronsql_feature_view | 4 |
| ronsql_cte_dd_dtwide | 36 | | ronsql_cte_dd_chain_scalar | 4 |
| ronsql_cte_subquery | 32 | | ronsql_join_agg | 2 |
| ronsql_overflow | 24 | | ronsql_cte_dd_orderby_limit | 2 |
| ronsql_cte_greatest_least_v2b | 24 | | ronsql_cte_dd_main_root_index | 2 |
| ronsql_cte_dd_agg | 2 | | ronsql_cte_dd_index | 2 |

Family reading (from test names; slice 2 opens with a reason/opcode
attribution pass — 5119 or the fallback log — before lowering):
- **GREATEST/LEAST family ≈ 224** (v4 104, v5 54, v2b 24, base 22,
  v2a 12, v6 8): a coherent unlowered aggregation-op family — likely
  the single biggest structured win.
- **ronsql_hopsworks 304**: the feature-store workload test — the
  largest single source, mixed shapes, attribution needed first.
- **Core-shape tests** (ronsql_basic 168, emptytable_and_nulls 144):
  plain WHERE filters land here — expected to be dominated by the
  Phase 7 scan-filter v1 subset boundary.
- **Filter tests** (cte_dd_filter 48, cte_or_body 44): CTE-body WHERE
  = scan filters on the producer scan, plus consumer-side filters
  (the 6-5 surface).
- **Subquery family** (subquery_agg_ext 38, cte_subquery 32):
  decorrelation-generated shapes.
- **Strings/types/overflow** (minmax_string 36, dtwide 36,
  overflow 24): beyond-5F string shapes, wide types, overflow-probe
  programs.
- **The long tail of 2s** (dd_agg/index/orderby_limit/
  main_root_index/join_agg): likely one shared program shape each.

**Slice 2 order (provisional, re-rank after attribution):**
1. Attribution pass (5119 per top test → reason/opcode table).
2. GREATEST/LEAST lowering (~224, one family).
3. Scan-filter expansion for RonSQL's WHERE emission (ronsql_basic /
   emptytable_and_nulls / hopsworks share this).
4. CTE filters (6-5) for the consumer-side share of the filter tests.
5. The remaining named families by measured size.

## Slice 2 — attribution (opened 2026-08-27)

**Static attribution, GREATEST/LEAST — SOLVED.** RonSQL's
`emit_pair_op_embedded` (`RonSQLPreparer.cpp:11446`) shows the exact
program shape per Greatest2/Least2 pair-op:
- Embedded body: `READ_AGG_REG_TO_REG` ×2 (acc → reg; embedded op 43
  — the SAME opcode `testCteNdbApi` pinned), then (null-check
  variant) `BRANCH_REG_EQ_NULL` ×2, then `BRANCH_GE/LE_REG_REG` +
  the LoadConst16/WriteInterpreterOutput/ExitOK skip-offset trellis
  (all lowered since 5A/5J).
- Outer trailer: `Mov(dest, src)`, `Skip(1)`, **`kOpSetRegNull(dest)`**
  — the "permanently unsupported canary opcode" is a REAL RonSQL
  emission: the NULL path of every null-checked GREATEST/LEAST.

Lowering decomposition:
- **P1 — acc→reg with null tracking**: new embedded lowering for
  `READ_AGG_REG_TO_REG` (value from the glue's acc copy + the slot's
  null state into a NEW per-register null mask in `JitState`), and
  `BRANCH_REG_EQ_NULL`/`NE` over mask-tracked registers. New
  stencils → regen. Unblocks the null-check bodies AND
  `testCteNdbApi`'s pinned pattern.
- **P2 — `kOpSetRegNull` → per-row fallback stencil**: a row that
  EXECUTES it needs interpreter null semantics — set
  `s->row_fallback = 1`. In GREATEST/LEAST only NULL-input rows
  reach it (the skip trellis jumps past it otherwise), so non-null
  rows stay fully native and NULL rows take the standard per-row
  escape. Converts the family's whole-program rejection into
  per-row-on-NULL. The fast path (NOT NULL inputs,
  `needs_null_check == false`) needs only P1's value load — no
  branches on null, no SetRegNull in path.
- **P3 — canary repointing**: once `kOpSetRegNull` lowers, the
  deliberate-fallback canaries (`testJoinAggJit` Test 4,
  `rondb_jit_ndbapi_unsupported_fallback`, census comments) repoint
  to a durable reject — a load targeting register ≥ `BC_MAX_REGS`
  (wire field is 4 bits/16 regs, bridge admits 8: reason 5 forever).

**Main-scan WHERE (ronsql_basic 168 / emptytable_and_nulls 144)**:
RonSQL emits filters via `NdbScanFilter` — the family the compound
canary PROVED compiles for column-vs-const AND/OR shapes. The reject
source is therefore NOT obvious statically (per-storedProc
multiplicity means ~10 rejecting filter-scans could produce 168) —
needs the runtime breakdown.

**Instrument (IMPLEMENTED, this slice): the fallback breakdown.**
`dbtup_jit_note_fallback` now keeps exact per-(site, reason, opcode)
counts since node start; the FIRST occurrence of each distinct
family logs immediately (unthrottled — visible in every parallel
worker's log), and the 10 s rate-limited line prints the whole
cumulative table. Attribution after any suite run =
`grep "JIT fallback" <var>/**/ndbd.log`. This permanently retires
the Phase 6 backlog's observability item and the rate-limiter
blindness that limited the 6-0 harvest.

**Instrument verified 2026-08-27** (both jit suites green, breakdown
harvested from the worker logs).

## The reject breakdown (2026-08-27; counts are lower bounds — the
final table dump per node lags the last rejects by up to the 10 s
window; the pins remain the exact per-test totals)

| Site | Reason | Opcode | Count | Meaning |
|---|---|---|---|---|
| aggregation | 5 | 14/16/13/10/18/12/15 | **≈501** | REG_OUT_OF_RANGE at kOpSum*/Min*/Max*/Count — **register indices ≥ 8**: RonSQL's SVM allocator uses the interpreter's 16 registers, `BC_MAX_REGS` admits 8. 55% of everything. |
| agg+join-agg | 1 | 43 | **180** | embedded `READ_AGG_REG_TO_REG` — GREATEST/LEAST bodies + the CTE consumer-compare pattern |
| scan-filter | 1 | 9 | **116** | unconditional `BRANCH` — RonSQL's DNF filter trellis (`branch_label` to ACCEPT/REJECT); the v1 subset never mapped it although `OP_JUMP` exists since 5J |
| join-agg | 1 | 44 | 50 | `READ_LINKED_COLUMN_TO_REG` (typed-reg linked load) |
| aggregation | 1 | 6 | 16 | `kOpMod` with statically-unknown operand types (the 5C-4/5E reject arm) |
| aggregation | 8 | 22 | 16 | TYPE_MISMATCH at `kOpMinusBigint` |
| join-agg | 1 | 38/40 | 16 | `BRANCH_MEM_OP_ARG(_INLINE_TYPE)` |
| join-agg | 1 | 45 | 8 | `LOAD_DOUBLE_CONST` |
| join-agg | 1 | 51 | 4 | `READ_UINT32_MEM_TO_REG` |
| aggregation | 3 | 3/4 | 3 | PROG_TOO_LARGE (> BC_MAX_OPS — long GREATEST chains) |

**Slice 2 lowering order (fixed by the data):**
1. **`BC_MAX_REGS` 8 → 16** (≈501, 55%): the wire field is 4 bits =
   16 registers and the interpreter supports 16 — the bridge cap is
   the only barrier. Audit: `JitState.regs_i64[]` sizing, stencil
   reg-offset hole encodings (reg×8 displacements — 15×8 fits every
   hole form), the bridge boundary tests (reg-8-rejects flips to
   reg-16), and the canary implication: after 16 there is NO
   expressible out-of-range register, so durable-reject canaries
   must repoint (see P3').
2. **GREATEST/LEAST enablers** (≈180 + the family's reason-5 share):
   P1 `READ_AGG_REG_TO_REG` + per-register null mask in `JitState`
   with `BRANCH_REG_EQ/NE_NULL` over tracked regs; P2 `kOpSetRegNull`
   → per-row-fallback stencil; P3' repoint the deliberate-fallback
   canaries to embedded `WRITE_ATTR_FROM_REG` (a real-tuple WRITE in
   an aggregation program — permanently outside the JIT's scope).
3. **Scan-filter unconditional `BRANCH` → `OP_JUMP`** (116): likely
   a small bridge-only change — the jump machinery exists.
4. **`READ_LINKED_COLUMN_TO_REG`** (50): linked→reg, rides item 2's
   null mask.
5. Tail by size: `BRANCH_MEM_OP_ARG(_INLINE_TYPE)`, kOpMod-unknown,
   `kOpMinusBigint` TYPE_MISMATCH, `LOAD_DOUBLE_CONST`,
   `READ_UINT32_MEM_TO_REG`, BC_MAX_OPS for the chain tails.

### Lowering item 1 — IMPLEMENTED (2026-08-27): the accumulator cap

**Correction from the audit**: the ≈501 reason-5 rejects are NOT
register indices — `kRegTotal` is 8 and `NdbAggregator` rejects
higher at build time. `JIT_BRIDGE_REG_OUT_OF_RANGE` also covers
**accumulator slots**, and `BC_MAX_ACCS` was **4** against the
interpreter's `MAX_AGG_N_RESULTS = 256`: every query with ≥ 5
aggregates rejected. That is the 55% family (wide SELECT lists,
GREATEST/LEAST chains, hopsworks feature queries).

Changes (**regen-stencils REQUIRED** — both the cap and the layout
change stencil field offsets):
- `BC_MAX_ACCS` 4 → 32 (`bytecode1.h`, with the raise-again note;
  programs with > 32 aggregates still reject and the pins will say
  if 32 is not enough).
- **JitState restructured**: scalar prefix (registers, pointers,
  flags incl. `row_fallback`) FIRST, the five `BC_MAX_ACCS`-indexed
  arrays LAST — because at 32 slots a full-struct memset costs
  ~1.4 KB per row. The dispatch glue now zeroes only
  `offsetof(JitState, acc_i64)` (96 B) plus the USED prefix of the
  flag arrays inside the existing copy-in loop; the scan-filter
  invoke zeroes the prefix only (admitted filters never touch accs).
  Slots ≥ n_agg_results hold stack garbage — writeback never reads
  them, and only a lying program header could reference them (the
  interpreter would corrupt group records on such a program: shared,
  pre-existing exposure, not new).
- Bridge/jit1/boundary tests: all keyed on the constant — the
  acc-boundary bridge tests (`enc_count(reg, BC_MAX_ACCS)` etc.)
  reject at 32 automatically; agg_index stays 16-bit on the wire so
  the boundary remains expressible (unlike a register raise, which
  the 4-bit wire field would have made untestable).

**VERIFIED 2026-08-27 (all tests passed incl. stability re-runs).**
Recovered: **678 rejects in the RonSQL corpora** — ronsql_hopsworks
304→0 (the whole feature-store workload was blocked by the 4-slot
cap alone), emptytable_and_nulls 144→0, basic 168→72,
minmax_string 36→0, constants/orderby_stress 16→0, date_sub/dbt3
8→0, feature_view 4→0, dtwide 36→2, bigquery 44→38, three dd 2s→0 —
plus the fleet suite: testVarcharMinMax 8→0, ndb_pushdown_agg 16→8,
ndb_join_pushdown_agg{,_types,_evict} 3/6/2→1/1/0, testCteNdbApi
2→1 (stable across re-runs). Corpus census ~1234 → ~556. The
GREATEST/LEAST family (224) did not move — those programs reject at
the FIRST offending op (43/SetRegNull) before the acc check, exactly
item 2's territory. `rondb_jit_groupby_canary` Q6 — the negative
control for the OLD cap — was reframed as a must-JIT
(`q6_jit_compiled`) rather than accepting the blind-recorded
contradiction. The deliberate kOpSetRegNull canaries still reject on
cue (breakdown: join-agg reason=1 detail=30 ×2).

### Lowering item 2 — DONE & VERIFIED (2026-08-27): GREATEST/LEAST enablers

Target family: **≈224 rejects** — embedded `READ_AGG_REG_TO_REG`
(op 43, 180) plus the trailer's `kOpSetRegNull` share of reason-5.
RonSQL emits GREATEST/LEAST as an embedded compare body over two
OUTER registers plus an outer trailer `Mov(dest,src) / Skip(1) /
SetRegNull(dest)` selected by the CASE-style output value (0/1/2).

**Design discoveries that collapsed the plan** (vs the original
"per-register null mask" sketch):

1. **Op 43 reads OUTER registers, not accumulators** — the
   interpreter's `handleReadAggRegToReg` reads
   `ctx.aggRegisters = m_registers`. So it is an outer→embedded
   register IMPORT, not an accumulator read.
2. **The register-file split**: the interpreter keeps TWO register
   files (outer `m_registers`, embedded `TregMemBuffer`). The JIT
   has one `regs_i64[]`. Renaming embedded regs to slots 8-15
   (`BC_EMB_REG_BASE = 8`, `BC_MAX_REGS` 8 → 16) removes the
   aliasing hazard wholesale; outer wire checks stay at 8.
3. **The completing-row invariant (5D)**: on any row the JIT
   completes, NO register holds SQL-NULL — nullable outer loads
   per-row-fallback on NULL, NB loads branch away, consts are
   non-null, and `kOpSetRegNull` rows fall back. Therefore embedded
   `BRANCH_REG_EQ_NULL` **folds to nothing** (never taken) and
   `BRANCH_REG_NE_NULL` lowers to an unconditional `OP_JUMP`. NO
   per-register null mask needed at all.

Changes (**regen-stencils REQUIRED** — one new stencil + the
`BC_MAX_REGS` 16 layout change moves `JitState` offsets):
- `bytecode1.h`: `BC_MAX_REGS` 16, `BC_EMB_REG_BASE` 8 (rename doc),
  `OP_SET_REG_NULL_FB = 64` (the only new op kind).
- `stencils_src.c`: `op_set_reg_null_fb` = `s->row_fallback = 1;`
  + tail-next — no holes, so no audit_magics entries.
- `ndb_jit_bridge.c`:
  - embedded emits rename regs `+BC_EMB_REG_BASE`; outer bound
    checks pinned to `BC_EMB_REG_BASE` (12 sites);
  - op 43 → `OP_MOV_INT_INT(8+dst_emb, src_outer)` with type
    admission: outer track I64/NNC always; **U64 only if
    `reg_u63_safe`** (narrow unsigned Tiny/Small/Medium/Unsigned +
    DATE/YEAR/TIME2 — NOT Bigunsigned/DATETIME2/TIMESTAMP2, whose
    sign-bit-set packs would misorder under the embedded signed
    compares); F64/STR reject TYPE_MISMATCH. `reg_u63_safe[]` is
    tracked at LoadCol and propagated by `kOpMov`;
  - embedded `BRANCH_REG_EQ_NULL` non-fusion → FOLD (was reject);
    new `BRANCH_REG_NE_NULL` → `OP_JUMP`;
  - outer `kOpSetRegNull` → `OP_SET_REG_NULL_FB` (was THE permanent
    reject); `nb_op_reads_reg` treats it as a conservative reader;
  - post-embedded-block tracker invalidation REMOVED — the register
    split isolates the files, and GL's outer trailer needs live
    tracking across the block.
- Tests: `bridge_tests.c` T12 flipped to accept; T60c/d/e reframed
  as fold-acceptance; new **T70a-e** (fast-path GL 13-op shape with
  import MOVs + GE; null-check GL 17-op shape with exactly one
  SET_REG_NULL_FB and folded guards; F64 import reject; BIGUNSIGNED
  reject vs SMALLUNSIGNED accept; NE_NULL → JUMP).
- **Canary repoint** (P3' resolved): `testJoinAggJit` Test 4 now
  uses a **33-aggregate SUM(0) program** (agg index 32 one past
  `BC_MAX_ACCS = 32` → durable REG_OUT_OF_RANGE; the 16-bit wire
  agg_index keeps the boundary expressible). Chosen over
  WRITE_ATTR_FROM_REG for simplicity. Wrapper + census texts
  updated.

**VERIFIED 2026-08-27 (all tests passed).** Measured on the
re-recorded pins: **178 rejects recovered** — greatest_least_v4
104→0, v2b 24→0, greatest_least 22→2, cte_scalar 16→2, v2a 12→6,
v6 8→2, v5 54→52, dd_bigquery 38→36 — plus `ndb_push_agg_jit`
testCteNdbApi 1→0 (Test 21 GREATEST(MAX,MIN), hand-updated pin).
Corpus census **~556 → 378** (63/80 tests reject-free). The log
harvest confirms a clean family kill: `reason=1 detail=43` and
`detail=30` (kOpSetRegNull) are GONE; the GL survivors log as
`reason=8 detail=43` — the deliberate type-admission rejects
(F64/BIGUNSIGNED import sources; v5's 52 is exactly this plus
op-44 linked variants — items 4/tail territory). Remaining corpus
by family: scan-filter BRANCH item 3 (basic 72, dd_filter 48,
or_body 44, overflow 24 ...), op-44, then the tail (38/40,
kOpMod-unknown, MinusBigint, 45, 51, PROG_TOO_LARGE).

Test fallout fixed during verification (each pinning OLD
behavior, bridge was right): regen preflight caught a bare
`TAIL_NEXT` (macro takes `(s)`); T51f/T62f/T64d manufactured
UNKNOWN registers via an empty embedded block → reframed to
never-written registers (same reject arms, word 0) with NEW
**T51g** pinning that outer tracking now SURVIVES embedded blocks
(the old T51f program, accepted; load NB-converts per 5D — the
first T51g run's kind=50 "failure" was the assert, not the
bridge).

### Lowering item 3 — DONE & VERIFIED (2026-08-27): scan-filter unconditional BRANCH

Target family: **116+ rejects** — `scan-filter reason=1 detail=9`,
the corpus's largest post-item-2 family. RonSQL's DNF filter trellis
wires every condition block to the shared ACCEPT/REJECT exits with
the interpreter's unconditional `BRANCH` (NdbScanFilter's
`branch_label`); the v1 embedded subset never mapped it even though
`OP_JUMP` has existed since 5J.

Changes (bridge + tests ONLY — **no new stencil, no layout change,
regen-stencils NOT needed**):
- `ndb_jit_bridge.c`:
  - **Define fix**: `BR_EMB_BRANCH` was 3 — which is
    `LOAD_CONST_NULL` in Interpreter.hpp; BRANCH is **9**. The bad
    value was only ever used in the diagnostic name table (no
    translate case existed), so nothing mistranslated; the name
    table now labels both correctly.
  - New `translate_embedded_block` case `BR_EMB_BRANCH` → `OP_JUMP`
    via the standard pending-target fixup. No registers involved, so
    it lowers on BOTH the scan-filter and aggregation-embedded paths
    (no `allow_reg_ops` gate). Forward-only like every embedded
    branch; **zero branch length rejects MALFORMED** (the
    interpreter's brancher would re-execute the word forever — a
    lowered self-jump would hang the JIT; no emitter produces it).
  - `emb_null_path_reg_safe` DFS scanner: BRANCH has ONE successor
    (the target) — follow it instead of returning unsafe.
- `bridge_tests.c` **T72a-e**: scan-filter trellis accept
  (JUMP + FILTER_REJECT_EXIT + EXIT + appended EXIT, target fixup
  c=2); backward reject; zero-length MALFORMED; past-end MALFORMED;
  aggregation-embedded accept (jump to trailing EXIT_OK resolves to
  the tail OP_EXIT).

**VERIFIED 2026-08-27 (all tests passed).** Measured on the
re-recorded pins: **172 rejects recovered** — cte_or_body 44→0,
cte_subquery 32→0, ronsql_basic 72→32, dd_filter 48→24,
dd_bigquery 36→4. Corpus census **378 → 206** (65/80 tests
reject-free). The harvest confirms the clean family kill:
`scan-filter detail=9` is GONE — in fact NO scan-filter families of
any kind remain in the corpus; every residual is an aggregation-path
family. `testInterpreterTypedRegs` initially appeared unmoved —
CORRECTED at item 5's verification: its Test 13i is a PURE
unconditional-branch filter (`[BRANCH +2, EXIT_NOK, EXIT_OK]`, no
REG ops) and compiles since this item — pin 1888 → 1880 (the -8 =
its per-fragment compile events; the suite had not re-measured the
binary until then). Every other program in the matrix carries REG
ops and stays rejected (`allow_reg_ops=0`).
`ndb_push_agg_jit` and the OFF arms: unchanged.

Remaining 206 by family: op-44 `READ_LINKED_COLUMN_TO_REG` ≈50
(item 4, next), reason-8 op-43 type-admission ≈52 (v5 —
BY DESIGN: F64/BIGUNSIGNED import sources), `BRANCH_MEM_OP_ARG`
38/40 ≈16, kOpMod-unknown ≈16, MinusBigint TYPE_MISMATCH ≈16,
`LOAD_DOUBLE_CONST` ≈8, op-51 ≈4, PROG_TOO_LARGE ≈3.

### Lowering item 4 — DONE & VERIFIED (2026-08-28): READ_LINKED_COLUMN_TO_REG

Target family: **≈50 rejects** — `join-agg reason=1 detail=44`, the
type-aware linked-attr-buffer load into an embedded register
(Interpreter.hpp op 44; the CTE consumer-compare pattern's linked
operand, GL-over-linked bodies). Wire: bits 6-8 dst reg, 16-23
buffer position, 24-31 NDB type — the type is ON the wire, so
admission is fully static.

New stencil ⇒ **regen-stencils REQUIRED**:
- `bytecode1.h`: `OP_LOAD_LINKED_COL = 65` (op->a = renamed dst slot,
  op->b = `(position << 8) | ndb_type`), OP_KIND_MAX bumped.
- `stencils_src.c`: `op_load_linked_col` — cold call
  `ndb_jit_h_load_linked_col(s, HOLE(LLC_PT), HOLE(LLC_DST))`.
- `hole_kinds.h`: `HOLE_LLC_PT`→HK_OP_B / `HOLE_LLC_DST`→HK_OP_A
  string entries (x86_64 relocs) + `MAGIC_LLC_PT_NARROW 0xafe8` /
  `MAGIC_LLC_DST_NARROW 0x8f07` (v1 narrow salt, derivation verified
  against MAGIC_BAOA_OFF_NARROW; collision-checked against all
  narrow + fold magics) + kHoleNarrowMagicTable rows.
- `extract_stencils.c` kOpkindMap + `audit_magics.c`
  kNarrowMagicToStencil ×2.
- `DbtupJitGlue.cpp`: `ndb_jit_h_load_linked_col` mirrors the
  interpreter's `handleReadLinkedColumnToReg` walk of
  `req_struct->m_linked_attr_data` (entry = tableId, schemaVersion,
  AttrHeader, data) and its integer decodes 1:1. Where the
  interpreter sets the register's NULL_INDICATOR (NULL value,
  missing buffer, out-of-range position), the helper sets
  `row_fallback` instead — the folded reg-null guards' path replays
  on the interpreter, preserving the completing-row invariant.
  4063 trace + registration like its siblings.
- `ndb_jit_bridge.c`: `BR_EMB_READ_LINKED_COL_TO_REG 44` case —
  gated `allow_linked_ops && allow_reg_ops` (scan filters reject →
  interpreter, same as op 39); type admission = signed widths +
  narrow unsigned (exact in i64); **BIGUNSIGNED / FLOAT / DOUBLE
  reject TYPE_MISMATCH** (op-43 policy — embedded compares are
  signed i64; the interpreter runtime also handles FLOAT/DOUBLE via
  typed registers, Interpreter.hpp's doc understates it). DFS
  scanner: write-overwrite ends the tracked path (shares the op-43
  case). `nb_op_written_reg`: writes op->a.
- `bridge_tests.c` **T73a-e**: BIGINT accept (a=9,
  b=(1<<8)|9), SMALLINT accept, BIGUNSIGNED reject, DOUBLE reject,
  scan-filter-path reject.

**VERIFIED 2026-08-28 (all tests passed).** Measured on the
re-recorded pins: **46 rejects recovered** — greatest_least_v2a
6→0 (clean), greatest_least_v5 52→28, subquery_agg_ext 38→22.
Corpus census **206 → 160** (66/80 tests reject-free). Harvest:
`reason=1 detail=44` GONE; new `reason=8 detail=44` entries are the
deliberate type-admission rejects (DOUBLE/BIGUNSIGNED linked
sources), symmetric with op-43's. (Record run at --parallel=10 hit
the known transient RDRS connect blips — retries green, no crashes;
validation at --parallel=6 clean.)

Remaining 160 by family: reason-8 43/44 type-admission (v5's 28 +
share elsewhere — BY DESIGN until an F64-compare embedded model
exists), `BRANCH_MEM_OP_ARG` 38/40 ≈16, kOpMod-unknown ≈16,
MinusBigint TYPE_MISMATCH ≈16, `LOAD_DOUBLE_CONST` (45) ≈8,
op-51 ≈4, PROG_TOO_LARGE ≈3 — the big test-level residuals are
ronsql_basic 32, dd_filter 24, overflow 24 (agg-path tail
families).

### Lowering item 5 — DONE & VERIFIED (2026-08-28): BRANCH_MEM_OP_ARG family

Target family: **≈16 rejects** — `join-agg reason=1 detail=38/40`.
`BRANCH_MEM_OP_ARG` (38) and `BRANCH_MEM_OP_ARG_INLINE_TYPE` (40)
are THE CTE-filter compare: the CTE row value is pre-loaded into
`cheapMemory[0]` by `READ_LINKED_TO_MEM` (which the JIT already
lowers) and compared against an inline literal via the type's
NdbSqlUtil comparator — 38 resolves type/charset through
`tablerec[tableId]` + a schemaVersion check (4 header words), 40
carries them inline for synthesized aggregate values with no real
column (3 header words).

New stencil ⇒ **regen-stencils REQUIRED**:
- `bytecode1.h`: `OP_BRANCH_MEM_OP_ARG = 66` (b = prog_buf word
  offset, c = branch target); added to `bc_op_is_branch`.
- `stencils_src.c`: `op_branch_mem_op_arg` — same 1-hole shape as
  `op_branch_attr_op_arg` (narrow `BMOA_OFF` + `HOLE_BMOA_TGT`
  branch-take).
- `hole_kinds.h`: string entries + `MAGIC_BMOA_OFF_NARROW 0x51fd`
  (v1 salt, collision-checked) + narrow-table row; extractor/audit
  maps updated.
- `DbtupExecQuery.cpp`: new `Dbtup::evalBranchMemForJit(inst)` —
  mirrors handleBranchMemOpArg / handleBranchMemOpArgInlineType
  1:1 (dispatches the two layouts on the opcode in inst[0]),
  reading `cheapMemory` directly as a Dbtup method. Same return
  convention as evalBranchColForJit (1 take / 0 fall / <0 error).
  Declared in `Dbtup.hpp`.
- `DbtupJitGlue.cpp`: `ndb_jit_h_branch_mem_op_arg(s, off)` — like
  the attr_op_arg helper but with NO tablePtrP requirement (these
  ops never read the local tuple — that is their point: CTE
  consumer virtual rows), and rc<0 → **per-row fallback** rather
  than abort (stale schemaVersion is a legitimate runtime
  condition; the interpreter re-run produces the proper error).
- `ndb_jit_bridge.c`: translate case gated
  `allow_attr_op_arg && allow_linked_ops` (scan filters reject —
  nothing populates the mem value there); admission = cond EQ..GE,
  forward in-range target, word-count validation
  (38 = 4 + ceil(argLen/4), 40 = 3 + ceil(argLen/4)); emits with
  b = attr_op_arg_base + emb_pc like op 23. DFS scanner forks like
  ATTR_OP_ARG with the per-op header sizes.
- `bridge_tests.c` **T74a-e**: 38 accept (agg path, b=1 c=1),
  40 accept, LIKE-cond reject, scan-filter-path reject, truncated
  literal MALFORMED.

**VERIFIED 2026-08-28 (tests green; ronsql_basic value-diff flake
waived — see below).** `detail=38/40` is GONE from the harvest and
T74a-e pin the lowering at unit level — but the RonSQL pins did
NOT move: the programs that used to reject on 38/40 now reject
deeper, on remaining tail ops (45 / 51 / reason-8 type-admission)
— a program rejects at its FIRST inadmissible op, so per-family
census counts overlap within a program. Net corpus recovery
lands when those blockers fall. Bonus correction:
`testInterpreterTypedRegs` pin 1888 → 1880 — its Test 13i is a
pure unconditional-branch filter that compiles since ITEM 3 (see
the amended item-3 record); this was the suite's first
re-measure since.

Deferred within the tail (next items): kOpMod-unknown ≈16,
MinusBigint TYPE_MISMATCH ≈16, LOAD_DOUBLE_CONST ≈8 (F64-embedded
territory, pairs with the reason-8 family), op-51 ≈4,
PROG_TOO_LARGE ≈3.

Known flake (2026-08-28, pre-existing): `ronsql_jit.ronsql_basic`
can fail its mysql-vs-ronsql VALUE diff on the float-SUM query
(`sum(14/3 + 18/5*cfloat + uint8)`) by one ulp — per-fragment
partial sums merge in nondeterministic order and RonSQL's
shortest-round-trip double printing amplifies the last-ulp wobble
into a different digit count. Not JIT-related (the fallback pin
holds; no numeric path changed). Mikael has a fix on another
branch (deterministic comparison/formatting); ignore until it
lands.

### Lowering item 6 — DONE & VERIFIED (2026-08-28): F64 embedded compares + heap-memory reads

Targets: `LOAD_DOUBLE_CONST` (45, ≈8) — the WHERE
`col <op> float_literal` family — and `READ_*_MEM_TO_REG` (49-52,
≈4 as op 51) — RonSQL's legacy linked-value reads. Both were
identified as the actual blockers left in the former-38/40
programs (dd_filter, cte_case).

Two new op kinds, both cold calls ⇒ **regen-stencils REQUIRED**:

**OP_BRANCH_F64 = 67** — the F64 embedded compare model:
- The bridge now tracks per-embedded-reg F64 state
  (`emb_reg_f64[8]`) and the defining plain-READ_ATTR op index
  (`emb_reg_read_attr[8]`), updated at EVERY reg write site
  (READ_ATTR, LC16, LC64, op-43, op-44, 45, 49-52).
- `LOAD_DOUBLE_CONST` lowers to plain `OP_LOAD_CONST_INT` with the
  double's bit pattern (F64 lives bit-cast in regs_i64) and marks
  the reg F64.
- A REG_REG compare with either side F64-tracked takes the new arm:
  a non-F64 side defined by a plain READ_ATTR is RETROACTIVELY
  converted to `OP_LOAD_COL_NDB_F64` (same operand layout; its
  helper per-row-falls-back on non-FLOAT/DOUBLE declared types, so
  a mistyped program degrades per row, never miscompares); any
  other int side converts signed-i64 → double at runtime — exactly
  the interpreter's `compareTypedRegs` float arm (embedded int
  values are non-negative-or-signed-exact by the admission rules).
  NaN compares "equal" in both engines.
- One packed narrow ARG hole (`MAGIC_BF64_ARG_NARROW 0x0ab8`,
  HK_OP_IMM): cond 0-3 | side flags 4-5 | regs 8-15; op->a/b
  duplicate the regs for the NB reader analysis; op->c target.
  Chosen as a COLD CALL (like attr_op_arg) instead of fold-hole
  inline compares — the fold-magic recipe comment does not
  reproduce the historical constants, so no new fold magics.
- The 5D-1 NB pass is unaffected (`op_from_emb` already excludes
  embedded loads, incl. retroactively converted ones).

**OP_READ_MEM_TO_REG = 68** — ops 49-52 in one kind:
- a = dst slot, b = byte offset, c = (width_code << 8) | dst.
  Narrow holes `MAGIC_RMR_OFF_NARROW 0x48e7` (HK_OP_B) /
  `MAGIC_RMR_WD_NARROW 0x0ef6` (HK_OP_C).
- New `Dbtup::readCheapMemForJit(off, width)`: 1/2/4-byte reads
  zero-extend (< 2^33 — exact under signed compares; the
  interpreter tags REG_TYPE_UINT but the values compare
  identically), 8-byte reads raw Int64 — handler-for-handler
  parity. The interpreter's runtime MAX_HEAP_OFFSET check becomes
  a compile-time bound on the wire-constant offset (out-of-range
  → MALFORMED → fallback → the interpreter reproduces the error).

Tests: `bridge_tests` **T75a-e** — col-vs-literal GE (load
converted to F64, ARG 0xA935), int-vs-literal LT (LC16 kept, ARG
0xA922, runtime convert), mem-read u32 accept (a=9 b=4 c=0x209),
offset-bound MALFORMED, scan-filter reject of op 45.

**VERIFIED 2026-08-28 (all tests passed).** Families 45/51
ELIMINATED from the harvest (T75 pins them at unit level); pins
unchanged again — the onion peeled one more layer and exposed the
next blockers in the same programs (worker-correlated to
ronsql_basic): **embedded arithmetic** — `reason=1 detail=7`
(ADD_REG_REG) and `detail=30` (MUL_REG_REG), i.e. WHERE-clause
arithmetic — plus a suspicious `reason=4 detail=28` (outer
namespace: kOpEmbeddedInterp MALFORMED — "declared emb_len runs
past the program words"; a valid program should never trip it —
possible program-length accounting defect for large DNF filters,
investigate before treating as a family). Remaining tail: embedded
arith (item 7), kOpMod-unknown ≈16, MinusBigint ≈16,
PROG_TOO_LARGE ≈3, by-design reason-8 type-admissions.

### Lowering item 7 — DONE & VERIFIED (2026-08-31): embedded WHERE arithmetic + instrument word

Target: the post-item-6 layer in ronsql_basic/overflow —
`reason=1 detail=7` (ADD_REG_REG) and `detail=30` (MUL_REG_REG),
WHERE-clause arithmetic; SUB_REG_REG (8) rides along.

**OP_ARITH_FB = 69** (one new stencil ⇒ regen-stencils REQUIRED):
- Cold call; wire src1 bits 6-8, src2 9-11, dst 16-18; op->imm
  packs (code<<12)|(dst<<8)|(src1<<4)|src2, a/b/c carry dst/src1/
  src2 for the generic passes. Narrow magic
  `MAGIC_AFB_ARG_NARROW 0x6fd9`.
- Signed-i64 math with __builtin_*_overflow. On overflow — and on
  ANY NEGATIVE SUB RESULT — the helper sets row_fallback and the
  stencil EXITS (plain ret, like op_exit): the interpreter re-run
  applies its runtime-signedness-tagged semantics exactly
  (typed-regs Test 19j: unsigned subtract underflow is a RUNTIME
  ERROR — the bridge cannot see declared signedness, so the
  negative-SUB ambiguity must replay; signed columns pay a per-row
  fallback only on negative results).
- F64-tracked operands reject TYPE_MISMATCH (double WHERE
  arithmetic = future item); dst clears F64/read-attr tracking.
- DFS scanner: sources are reads (unsafe), dst overwrite ends the
  path. nb: reads b/c, writes a.
- Tests **T76a-e**: full a+b>const shape (ARG 0xB9A), SUB/MUL
  codes, F64-operand reject, scan-filter reject.

**Instrument**: `dbtup_jit_note_fallback` now takes the offending
WORD; the NEW-family line prints `word=` and the table dump prints
`first_word=` — next harvest disambiguates the suspicious
`reason=4 detail=28` (outer kOpEmbeddedInterp MALFORMED "emb_len
past program end") by pinpointing the word.

### Lowering item 8 — DONE & VERIFIED (2026-08-31): two real defects + capacity

The word-instrumented harvest (item 7) cracked both mysteries:

**Defect 1 — flat-format program length (the r4/d28 word=65546).**
`DblqhProxy`'s JOIN_AGG_SETUP parser set the flat-format (0x0721)
leaf's `m_agg_program_len = totalWords` (the SECTION size). The
program's own header word 0 is `(0x0721 << 16) | progLen`, and a
CTE consumer feed's section carries trailing data after the
program. The interpreter never noticed — the base interpreter
re-reads the header, and consumer rows end via the embedded
STOP-or-skip — but the JIT bridge walks `[start, len)` at compile
time and marched into the trailing words: out-of-bounds reads,
garbage-guided leaps (words decoding as embedded blocks jump
`pos += 1 + emb_len`), and a MALFORMED reject at positions like
65546. FIX: the flat arm now uses `word0 & 0xFFFF` with
validation (reject setup as InvalidRequest if < 8 or >
totalWords); the new multi-leaf format's per-leaf lengths are now
bounds-checked against the section the same way (hardening — a
malformed frame can no longer walk out of the copied buffer).

**Defect 2 — silent post-admission compile failures
("aggregation compile reason=0").** `jit1_compile` can fail AFTER
`admit_program` passes (codemem alloc, hole patch, fixups, seal),
leaving the stale JIT_ADMIT_OK in the sidecar — the census logged
reason=0. Root cause of the actual failures: cold-call-heavy
programs (the newly lowered arith/compare families) at up to
BC_MAX_OPS ops exceeded the LARGEST CODE CLASS (8 KB). FIXES:
- `kClassBytes` + {16384, 32768} (both divide the 64 KB slab).
- `BC_MAX_OPS` 64 → 128 (also recovers the bridge-side
  PROG_TOO_LARGE ≈3 family — long GREATEST chains; op indices stay
  well under the uint8 0xFF sentinel; no JitState/stencil
  dependency ⇒ NO regen needed).
- New sidecar reasons `JIT_ADMIT_CODEMEM = 7` /
  `JIT_ADMIT_PATCH_FAILED = 8` recorded at every post-admission
  failure path — "compile" fallback families are now diagnosable.

No new stencils, no regen (items 7+8 verified together).

**VERIFIED 2026-08-31 (all tests passed).** Measured:
- **ronsql_overflow 24 → 0** (first real RonSQL pin drop since
  item 4 — the full layered stack cleared it). Corpus census
  **160 → 136**.
- **Fleet pins: ndb_pushdown_agg 8 → 0, ndb_push_agg_case_null
  8 → 0, ndb_join_pushdown_agg 1 → 0** (the BC_MAX_OPS raise —
  their big CASE-trellis programs were op-cap blocked; hand-updated
  pins). The ndb_push_agg_jit fleet is now reject-free outside the
  deliberate canaries + typed-regs census (1880) +
  ndb_join_pushdown_agg_types (1).
- PROG_TOO_LARGE (r3) families GONE; arith families (7/30) GONE.
- Survivors for item 9: `aggregation compile reason=8` (6 events —
  hole-patch/fixup class; errno now logged as the family detail for
  the next harvest), `r4/d28 word=65546` (STILL present — the
  flat-format fix did not cover this path; colvscol-family CTE
  tests; needs the 5119 setup dump or per-leaf logging),
  kOpMod-unknown ≈16 (d6 w21), MinusBigint (d22 w34), by-design
  reason-8 43/44.

### Lowering item 9 — DONE & VERIFIED (2026-08-31): the defect-hunt cycle

**r4/d28 round two.** With the flat-format fix in place, the
persisting `word=65546` requires `n_words > 65546` — only possible
via the MULTI-LEAF (0x0722) arm, whose frames also carry trailing
consumer data after the program (the exact flat-format finding
again). Fix: after framing, each leaf's `m_agg_program_len` now
comes from the program's own `(0x0721 << 16) | hdrLen` header
word (validated 8 ≤ hdrLen ≤ frame length; a present-but-invalid
header rejects the setup as InvalidRequest). The interpreter is
unaffected (its bound only tightens to the region it could ever
execute — the base interpreter asserts hdrLen == prog_len on the
non-join path).

**Self-diagnosing census.** `dbtup_jit_note_fallback` now takes
the program pointer/length; the FIRST sighting of each family also
logs a hexdump of up to 16 words centred on the offending word
(clamped to the buffer). Every remaining family — kOpMod-unknown
(d6 w21), MinusBigint TYPE_MISMATCH (d22 w34), compile-r8 (errno
now in detail) — becomes ground-truth-diagnosable from a normal
run, no debug build needed.

Verification: r4/d28 must vanish (colvscol-family consumer
programs compile or reject in-bounds); the next harvest's dumps
drive the kOpMod / MinusBigint / compile-r8 decisions.

### Item 9 CORRECTION + the real fixes (2026-08-31, same session)

The hexdump instrument (this item) immediately disproved the item-8/9
framing theory and named both survivors:

**r4/d28 was NEVER an out-of-bounds walk.** The dump shows
`bc_words = 17` — tiny, in-bounds programs. The offending "word
65546" is the pending-case-jump TARGET, not a position: the CASE
trellis writes `LC16(0xFFFF)` → WRITE_INTERPRETER_OUTPUT, and
0xFFFF is **AGG_EMBEDDED_INTERP_STOP_PROGRAM** ("skip the rest of
this ROW's outer program"; the interpreter sets exec_pos =
prog_len). The bridge computed `outer_after + 65535 = 65546` and
the resolver rejected the unmatched target as MALFORMED — with the
hardcoded detail BR_kOpEmbeddedInterp(28) and the target as the
logged word. FIX: the WRITE case maps skip 0xFFFF to a
`BR_CASE_JUMP_STOP` sentinel and the resolver points it at the
tail OP_EXIT — exactly the interpreter's semantics. Test **T77**.
(The two DblqhProxy framing changes from items 8/9 chased a
phantom but stay as legitimate hardening: lengths are now
validated and header-bounded.)

**compile-r8 was ENOENT — helpers silently unregistered.**
`J1_MAX_HELPERS` was 16; the registry held 15 before this
milestone, so `load_linked_col` took the last slot and
`branch_mem_op_arg` / `branch_f64` / `read_mem_to_reg` /
`arith_fb` were dropped — `jit1_register_helper`'s ENOSPC return
was ignored. Every program needing those helpers failed compile
with ENOENT, which is why items 5-7's lowerings eliminated their
BRIDGE families without moving pins (the programs died one stage
later) — ronsql_basic's stubborn 32 in particular. FIX:
J1_MAX_HELPERS 16 → 32, and `dbtup_jit_register_helpers` now
aborts on any registration failure (a startup bug must not ship a
JIT that ENOENTs at compile time).

Bonus from this run's harvest: kOpMod-unknown (d6) and MinusBigint
(d22) did NOT reappear — re-check after the helper fix; they may
have been downstream shadows too.

Expected next run: r4/d28 and compile-r8 GONE; ronsql_basic,
dd_filter, cte_case, subquery_agg_ext, dtwide, chain_scalar,
orderby_limit pins drop — potentially the corpus's first
near-clean sweep outside the by-design reason-8 admissions.

### Item 9 round three (2026-08-31): the M11 wrong-result post-mortem

The verification run (first with all helpers REGISTERED) failed
`ronsql_subquery_agg_ext` M11 with WRONG RESULTS (extra unfiltered
rows) plus "overload" symptoms elsewhere. Root-cause work:

1. **The multi-leaf hdrLen override (item 9 round two) was the
   regression** — for filter-carrying leaves the FRAME length is
   the execution length; truncating `m_agg_program_len` to the
   header word's length cut the cross-table filter off the
   interpreter-fallback path: rows aggregated UNFILTERED (M11's
   count 4 = all rows). REVERTED — only the pure bounds validation
   remains. (The flat-format fix from item 8 stands: it verified
   green.)
2. **False alarm withdrawn**: the "missing HK_BRANCH_TAKE holes" in
   op_branch_f64 / op_branch_mem_op_arg were an artifact of a
   truncated read of the generated header (sed window cut the hole
   list). Verified by diffing a fresh extractor run against the
   committed header: IDENTICAL, 4 holes each, extraction correct.
3. **Real ABI defect fixed — op_arith_fb's early `return`**: byte
   decode showed the fallback edge as ldp-own-frame + plain `ret`,
   which skips the engine preamble's teardown (stp x20/x30 frame):
   SP leaks 16 bytes and the C caller's callee-saved x20 comes back
   clobbered whenever the overflow/negative-SUB edge fires.
   Redesigned as a branch-shaped stencil: the edge tail-calls
   HOLE_AFB_TGT and the bridge points op->c at the tail OP_EXIT via
   the pending-case-jump STOP sentinel (src2 moved to op->d; nb
   reader + bc_op_is_branch + extractor KEEP_ALL policy + T76
   asserts updated). ⇒ **regen-stencils REQUIRED**.

Standing fixes from rounds 1-2: helper registry 16→32 + fail-fast
registrar (the items-5-7 silent unregistration), STOP_PROGRAM
sentinel → tail OP_EXIT (the real r4/d28), errno + hexdump census.

### Item 9 round four (2026-08-31): M11 root cause — Mul's dst field

The 4063 row-trace (throwaway `tmp_m11_diag` wrapper, since removed)
nailed it in one run: for the M11 row with price 20, the trace shows
`read_mem dst=r10 value=3`, `arith_fb code=2 l=3 r=10 res=30` — the
product computed CORRECTLY — and then `after reg[8]=30, reg[10]=3`:
**the product landed in reg 8**, and the compare read the
un-multiplied min_qty from reg 10 (20 > 3 → row passed).

Root cause: **`Interpreter::Mul` encodes its destination at bits
12-14 (`Dcoleg << 12`, `getReg3`) while `Add`/`Sub` use bits 16-18
(`getReg4`)** — the encoders genuinely differ. The bridge's item-7
arith case decoded all three at bits 16-18, so Mul's dst read as 0 →
`8 + 0 = 8`. Why every earlier check missed it: the bridge unit
tests' `enc_emb_arith` encoder copied the same wrong uniform-layout
assumption, so T76 validated the bridge against itself; and M10
(ADD) passed because Add's layout matched.

Fixes: per-op dst decode in the translate case and the DFS scanner;
`enc_emb_arith` in bridge_tests encodes per-op (T76 expected values
unchanged — dst nibble is the same once both sides are correct).
Bridge-only — no regen.

Lesson recorded: when lowering a FAMILY of wire ops, verify EACH
member's encoder against Interpreter.hpp — do not extrapolate the
layout from one sibling (this is the second encoder-layout surprise
after BR_EMB_BRANCH=3-vs-9 in item 3).

**VERIFIED 2026-08-31 (full checklist green: record + repeat=2 +
ndb_push_agg_jit + OFF arms).** With items 5-7's lowerings finally
LIVE (helpers registered, STOP sentinel lowered, Mul dst correct,
arith fallback edge ABI-sound): **48 more rejects recovered** —
subquery_agg_ext 22→0 (M10/M11 values CORRECT), cte_case 10→0,
greatest_least 2→0, join_agg 2→0, v5 28→16. Corpus census
**136 → 88**. Item 9's net: four real defects fixed (helper-cap
silent unregistration, STOP_PROGRAM sentinel, arith early-ret ABI,
Mul dst field) + one self-inflicted regression caught and reverted
(multi-leaf hdrLen override) + the census made self-diagnosing
(word, errno, first-sighting hexdump). Residue for item 10:
ronsql_basic 32 and dd_filter 24 did NOT move — attribute from the
next harvest; v5's 16 = by-design reason-8 type admissions.

### Item 10 — DONE (2026-08-31): residue classification; lowering campaign CLOSED

The final harvest shows exactly FOUR reject families across the
whole RonSQL corpus (census 88):

| Family | ≈Count | Classification |
|---|---|---|
| `r8/d43 w4` + `r8/d44 w1` | ~52 (v5 16, dd_filter 24, dd stragglers) | **BY DESIGN** — the op-43/44 type admissions (GREATEST/LEAST and linked loads over F64/BIGUNSIGNED sources; the embedded signed-i64 compare model cannot represent them). Lowering these means an embedded F64-compare extension of the GL pair-op path — a design item, not a gap. |
| `r8/d22 w34` (kOpMinusBigint) | ~16 (ronsql_basic) | **CORRECTLY REJECTED** — the operand register is F64-tracked (a generic-`/` conv result) feeding an optimizer-typed BIGINT minus. `RegMinusBigint`'s stated precondition is both-BIGINT (raw `val_int64`, no runtime dispatch), so routing this through ARITH_CONV_F64 would NOT match the interpreter; the interpreter/optimizer typing interplay here is RonSQL territory (flagged for the RonSQL owners: the optimizer emits kOpMinusBigint over a register the same program's generic `/` made DOUBLE). |
| `r1/d6 w21` (kOpMod) | ~4 (ronsql_basic) | UNKNOWN/mixed-track operand in the kitchen-sink arith program — same optimizer-typing cluster as d22, tiny count. |

`ronsql_basic`'s 32 = d22 + d6 + their per-fragment multiples;
`dd_filter`'s 24 = the d43/d44 admissions. Nothing else remains.

**The slice-2 lowering campaign closes at corpus census
~1234 → 88** (93% of all rejects eliminated), every remaining
reject classified as by-design type admission or a
correctly-conservative reject pending upstream RonSQL typing work.
Next: slice 3 — the "4060-lite" program-level error insert and
suite-wide arming, locking in everything that now compiles.

## Slice 3 — DONE & VERIFIED (2026-09-01): strict-compile enforcement, suite-wide

The "4060-lite" design, finalized:

**ERROR_INSERT 4064 (DBTUP, new)** — strict JIT compile for the
non-join aggregation path: after PushdownInterpreterFactory::Create,
a program with a compilable region whose m_jit_entry is null (bridge
reject OR engine-compile failure) aborts the node with a diagnostic.
Checked at the Create call site in DbtupExecQuery (instance access —
no plumbing). Per-row fallbacks NEVER trip it (they don't go through
the compile path), so whole suites can arm it — the property 4060
lacks. Scan filters are out of scope by design (the SQL planner
legitimately pushes non-JIT-able filters).

**ERROR_INSERT 5120 (DBLQH proxy, existing)** — the join-agg per-leaf
compile-fatal insert, LIFTED out of DEBUG_JIT (now plain
ERROR_INSERT): suite arming must work on every test build. 5119
(setup dump) stays DEBUG_JIT-gated.

**Arming**: Cmvmi routes `all error N` by range (4xxx → DBTUP,
5xxx → DBLQH), so `jit_strict_arm.inc` issues BOTH commands
back-to-back (each block keeps its own value); `all error 0`
broadcasts the clear. **71 of 81 corpus wrappers armed**
(arm → source body → disarm around every clean test); results
unchanged (exec lines emit nothing) — no re-record. Exempt (the
classified residue, pins remain their enforcement): ronsql_basic,
gl_v5, gl_v6, cte_scalar; dd_bigquery, dd_chain_scalar, dd_dtwide,
dd_filter, dd_orderby_limit; plus ronsql_cte_jit_census (arms its
own inserts).

Enforcement model after slice 3: a regression that stops a corpus
program from compiling now CRASHES the armed test's data node
(loud, immediate) in ADDITION to moving the fallback-delta pin;
the exempt tests keep pin-only enforcement until their residue is
lowered or reclassified.

**VERIFIED 2026-09-01: all 71 armed tests green (single run +
repeat=2), OFF arms untouched.**

# MILESTONE COMPLETE

The ronsql_jit / ronsql_cte_jit milestone ("ensure that all queries
in those categories are using compiled interpreter code") closes:
- Corpus census ~1234 → 88 JIT rejects (93% eliminated) across ten
  lowering/defect items; every residual classified (by-design type
  admissions or correctly-conservative pending upstream RonSQL
  typing).
- 71/81 corpus tests run under strict-compile enforcement (4064 +
  5120): any compile regression is an immediate node abort, not a
  silent pin drift.
- Five real defects fixed along the way (helper-registry silent
  drop, STOP_PROGRAM sentinel, arith early-ret ABI,
  Interpreter::Mul dst field, flat-format program length) plus the
  BC_MAX_OPS/code-class capacity raises and the fleet pins
  (pushdown_agg, case_null, join_pushdown_agg) going to zero.
- The fallback census is permanently self-diagnosing (offending
  word, compile errno, first-sighting hexdump).

## Backlog design sketch: GL F64/BIGUNSIGNED compare extension (2026-09-01)

The ~52 by-design reason-8 residues (op-43/44 admissions) and what
lowering them takes. The GL/consumer-compare body imports operands
into embedded registers and compares with the SIGNED-i64 stencils;
sources whose values are not exact under a signed compare reject:
BIGUNSIGNED / DATETIME2 / TIMESTAMP2 (sign-bit-set packs order
negative) and FLOAT/DOUBLE (IEEE bits misorder as i64). The
interpreter is immune via its typed registers + compareTypedRegs.

**Part A — F64 GL (one normal item; item 6 built the machinery):**
- op-43: admit BR_REG_F64 outer sources (the MOV is bit-preserving)
  and mark the embedded dst in emb_reg_f64[] — the compare then
  takes the existing OP_BRANCH_F64 arm automatically, incl. mixed
  int-vs-double (runtime promotion = compareTypedRegs' float arm).
- op-44: add FLOAT/DOUBLE arms to ndb_jit_h_load_linked_col
  (mirror the interpreter handler: FLOAT widens to double; store
  bits) + mark dst F64.
- Null-guard folds and the outer trailer (Mov propagates F64
  track) need no change. Likely ZERO new stencils.
- Verification focus: NaN-pair "equal" parity, -0.0, FLOAT
  widening. Unlocks most of dd_filter's 24 + v5's float share.

**Part B — BIGUNSIGNED GL (a new compare mode):**
- Generalize OP_BRANCH_F64 → OP_BRANCH_TYPED: per-side "unsigned"
  ARG flags; helper implements compareTypedRegs' full lattice
  (u64/u64 unsigned; mixed rule: negative signed < any unsigned).
- Needs emb_reg_u64[] tracking fed by op-43 (outer BR_REG_U64) and
  op-44 (BIGUNSIGNED wire type).
- DATETIME2/TIMESTAMP2 admission additionally requires a per-type
  proof that the packed wire format orders correctly under
  unsigned-i64 compare.
- Value is mostly synthetic (real GREATEST args ≥ 2^63 are rare) —
  reject-forever remains defensible.

Both parts stay measurable via the existing pins; exempt tests
move under slice-3 strict arming as they go clean.

### GL Part A — IMPLEMENTED (2026-09-01): F64 GREATEST/LEAST

Exactly per the backlog sketch — bridge + one helper widening,
ZERO new stencils, NO regen:
- op-43: `BR_REG_F64` outer sources admitted (the MOV is
  bit-preserving); the embedded dst is marked in `emb_reg_f64[]`,
  so the GL body's compare takes item 6's OP_BRANCH_F64 arm
  automatically — including mixed double-vs-int imports (runtime
  int→double promotion = compareTypedRegs' float arm).
- op-44: FLOAT/DOUBLE wire types admitted; the helper gained the
  two arms mirroring handleReadLinkedColumnToReg (FLOAT widens to
  double; the double's BITS land in regs_i64) and the dst is
  F64-marked.
- Unchanged by design: BIGUNSIGNED/DATETIME2/TIMESTAMP2 rejects
  (Part B), STR/UNKNOWN rejects, null-guard folds (the invariant
  is type-independent), the outer trailer (kOpMov propagates F64;
  the F64 aggregate family consumes it).
- Tests: T70c and T73d flipped from reject pins to accept pins;
  new **T78a-c** (F64 GL fast path — BRANCH_F64 ARG 0xA935 with
  both double flags; mixed import ARG 0xA912 with one flag;
  op-44 DOUBLE+FLOAT linked compare ARG 0xA934).

Expected on the pins: dd_filter 24 and v5's float share drop; the
newly-clean tests then join the slice-3 strict arming. Verification
focus: NaN-pair "equal" parity, -0.0, FLOAT widening equivalence —
all inherited from the already-verified OP_BRANCH_F64/load-f64
machinery.

### GL Part A — review addendum (2026-09-01): T78b fix + the merge-point guard

- **T78b was a test bug**: its `kOpEmbeddedInterp` header declared 6
  embedded words for a 5-word body, so the bridge parsed the trailing
  `kOpSumDouble` word as embedded opcode 0 (Mikael's run: reason 1,
  word 8). Fixed (`emb_len` 5).
- **A soundness hole Part A opened — CLOSED.** The outer register
  tracker is linear, and the GL trellis merges two paths right after
  `Mov(dest, src)` (output=0 runs the Mov, output=1 skips it).
  Same-class pairs were always fine; Part A's F64 admission made a
  DOUBLE-vs-BIGINT pair compilable, and for that pair the tracker's
  post-Mov type is WRONG on the output=1 path — a trailing
  `SumDouble(dest)` would add int bits as a double (or `SumBigint`
  double bits as an int). RonSQL's operand check
  (`validate_greatest_least_pair_loads`, RonSQLPreparer.cpp ~11758:
  "GREATEST/LEAST column operand must be an integer type") covers
  STORED columns only and explicitly SKIPS CTE-column / aggregate-
  projection operands, so `GREATEST(cte.int_col, cte.double_col)` is
  constructible today — the very op-43/44 F64 residue family Part A
  lowers; the corpus has no mixed pair yet (its GL tests pair int
  columns / int constants). Fix in the
  bridge: `PendingCaseJump` now carries a snapshot of the outer
  tracker at the jump's source (embedded output blocks: the block
  entry state; outer `kOpSkip`: the state at the Skip). When the
  outer walk arrives at a word that pending jumps target, each
  snapshot must agree with the linear state on every register's
  bit-reinterpretation CLASS (int / F64 / STR) wherever both know it
  — else `TYPE_MISMATCH` at that word. Same-class disagreements
  (I64/U64/NNC) keep the linear value exactly as before (u63 ANDs).
  If the linear predecessor is DEAD (the previous word was an
  unconditional `kOpSkip`), the first arriving jump's snapshot IS the
  state: CASE arms may reuse a register the previous arm typed
  differently, and that must not false-reject. (A join-to-UNKNOWN
  lattice was rejected as the fix: the bridge's I64 consumers
  TOLERATE UNKNOWN by design — kOpMin/MaxBigint treat it as i64 — so
  a poisoned register would still reach a typed consumer.)
- **Interpreter finding — flag upstream (same bucket as the
  kOpMinusBigint typing flag):** `PushdownInterpreter::
  OptimizeProgramBuffer` types registers the same linear way
  (`kOpMov: reg_types[dst] = reg_types[src]`, embedded blocks
  skipped) and rewrites `kOpSum` → `kOpSumDouble`/`kOpSumBigint`
  statically, so a mixed GL pair mis-sums on the interpreter too
  (`SumDouble` reads `val_double` of a BIGINT register on output=1
  rows). The JIT now rejects the shape (fallback = the interpreter's
  answer; no second wrong answer). The real fix is RonSQL-side:
  operand-type unification for GREATEST/LEAST, or a parser reject of
  mixed-class operand pairs.
- Tests: **T78d** (mixed trellis, BIGINT dest / DOUBLE src → rejects
  at the SumDouble word 13 with TYPE_MISMATCH — pre-guard this was
  ACCEPTED), **T78e** (F64 nullable GL shape accepted: 17 ops, one
  BRANCH_F64 0xA935, one SUM_F64, one SET_REG_NULL_FB), **T78f**
  (dead-arm adoption: a two-arm CASE reloading a formerly-F64
  register as BIGINT in both arms — accepted, 16 ops).
- Accepted false-reject exposure of the guard (flagged): a LIVE merge
  whose paths disagree on a register's class while the register is
  dead afterwards — a fall-through-arm shape no real emitter
  produces. It would cost a fallback and surface as a pin / 4064
  move, not a wrong result.

**First pin movement (Mikael's run, 2026-09-01 evening):**
`ronsql_cte_jit.ronsql_cte_dd_chain_scalar` fallback delta **4 → 0**
(all four residue rejects recovered). Re-pinned at 0 and the wrapper
moved under slice-3 strict arming (arm/disarm includes) — first of
the nine exempt tests to join; confirm with a re-run of that test.
`ronsql_cte_jit.ronsql_cte_dd_orderby_limit` **2 → 0** — same
treatment (re-pinned, armed).
`ronsql_jit.ronsql_cte_greatest_least_v5` **16 → 2** — re-pinned at
2, STAYS exempt: the residue is the BIGUNSIGNED op-43/44 share
(`o_biguns` / `c_biguns`), Part B territory (confirm in the
fallback log: `reason=8 detail=43|44` with a Bigunsigned source).
Running tally of exempt tests joined: dd_chain_scalar,
dd_orderby_limit.

**Full-run result (Mikael, 2026-09-01 evening: `./mtr --suite=ronsql_jit,
ronsql_cte_jit,ndb_push_agg_jit,ronsql,ronsql_cte --parallel=10
--force`): exactly the three pin moves above, nothing else.** So:
- The merge-point guard is SILENT corpus-wide, including every
  mysqld CASE program in `ndb_push_agg_jit` (the false-reject
  exposure did not materialise).
- Part A recovered **20** rejects (dd_chain_scalar 4, dd_orderby_limit
  2, gl_v5 14); corpus census 88 → **68**. Two exempt tests joined
  strict arming (dd_chain_scalar, dd_orderby_limit).
- dd_filter (24), dd_bigquery, dd_dtwide, cte_scalar (2), gl_v6 (2),
  ronsql_basic did NOT move — item 10's "F64 GL / linked" attribution
  of those was WRONG; they are not F64 admissions. Harvest of the run's
  surviving ndbd logs (workers 2-4 + the failed-test copies) decodes
  every family seen:
  - `join-agg reason=8 detail=44 word=1`, 19-word program, op-44 word
    `0a01006c` → wire type 0x0a = BIGUNSIGNED linked load — **v5's
    residue 2 (Part B)**.
  - `join-agg reason=8 detail=43 word=4`, 21-word program: outer
    LoadCol BIGINT r0 / **BIGUNSIGNED r1**, GL body imports r1 →
    `GREATEST(bigint, bigunsigned)` U64 non-u63 import — **Part B**
    (last test on workers 3 and 4; gl_v6 / cte_scalar / dd_bigquery
    territory — attribute in the follow-up run).
  - `aggregation reason=1 detail=6 word=21` (kOpMod) and
    `reason=8 detail=22 word=34 count=16` (kOpMinusBigint) — the
    known ~4 / ~16.
  - **NEW named class — CASE arm family conflict**: `join-agg reason=8
    detail=16 word=23` (104-word program: `LoadCol BIGUNSIGNED r0;
    MaxBigint(r0, slot 1); Skip 4; LoadConst BIGINT r1 = 0;
    MaxBigint(r1, slot 1)`) and `detail=14 word=26` (27-word program,
    same shape with SumBigint over slot 3). The THEN arm claims the
    slot as U64, the ELSE arm's constant 0 is NNC → mapped to the I64
    family → `BR_CLAIM_ACC_FAMILY` rejects (5C-3). Pre-existing and
    correctly conservative in general, but a NON-NEGATIVE CONSTANT is
    exactly representable in either family, so this is a cheap
    **lowering candidate (item 11)**: an NNC operand ADOPTS the slot's
    already-claimed family (emit the U64 op variant when the slot is
    U64). Column-first / constant-second is the common CASE shape;
    the constant-first order would need a pre-scan (defer). Likely
    the bulk of dd_filter's 24 — confirm by attribution first.

**Attribution run for the seven exempt tests (Mikael):** one mtr
invocation PER TEST, so every test starts a fresh cluster and gets its
own complete set of first-sighting lines (a "NEW family" line prints
only once per node lifetime — several tests on one cluster would hide
every class a later test shares with an earlier one). Fully qualified
`suite.test` names; run from the build tree's mysql-test (that is
where `var/` lives); harvest = the NEW-family lines with their dumps
plus the last cumulative family table (the rate-limited "totals"
block, printed while rejects keep occurring):
```sh
cd /Users/mikael/mysql_trees/rondb_1056/debug_build/mysql-test
OUT=/Users/mikael/mysql_trees/rondb_1056/jit_attribution.txt; : > $OUT
for t in ronsql_cte_jit.ronsql_cte_dd_filter \
         ronsql_cte_jit.ronsql_cte_dd_bigquery \
         ronsql_cte_jit.ronsql_cte_dd_dtwide \
         ronsql_jit.ronsql_cte_scalar \
         ronsql_jit.ronsql_cte_greatest_least_v6 \
         ronsql_jit.ronsql_basic \
         ronsql_jit.ronsql_cte_greatest_least_v5; do
  ./mtr --parallel=1 $t > /dev/null 2>&1
  echo "##### $t" >> $OUT
  grep -h -A1 'JIT fallback NEW family' $(find var -name ndbd.log) >> $OUT
  grep -h 'JIT fallback family:' $(find var -name ndbd.log) | tail -24 >> $OUT
done
```
Post `jit_attribution.txt`. Each block decodes as: `reason=` the
JitBridgeReason (1 unsupported op, 8 type mismatch, 5 reg out of
range …), `detail=` the offending opcode (outer kOp number, or the
embedded op number when the site is an embedded word), `word=` its
index in the compile region, and the dump = 16 program words around
it (outer words: opcode in bits 31-26; embedded words: opcode in bits
5-0 plus bit 15). Two data nodes may each print the same family —
that is one class, not two.

### Residue attribution — COMPLETE (2026-09-01, per-test harvest)

Every one of the 68 remaining corpus rejects decoded from its
first-sighting dump (`jit_attribution.txt`, one cluster per test):

| Test | Pin | Family (site / reason / detail) | Decoded program | Class |
|---|---|---|---|---|
| dd_filter | 24 | scan-filter r=1 d=23 | 5-word `BRANCH_ATTR_OP_ARG` w0=`00047097`: cond field 7 = **NOT_LIKE**, nulls 2, attr 2, arg `'A%'` | **Item 12: LIKE / NOT LIKE scan filters** |
| dd_bigquery | 4 | join-agg r=8 d=14 / d=16 | `LoadCol BIGUNSIGNED r; Max/SumBigint(r, slot); Skip; LoadConst 0; Max/SumBigint(const, slot)` | **Item 11: CASE arm family conflict** |
| dd_dtwide | 2 | join-agg r=1 d=7 w=11 | wide COUNT program: `LoadCol VARCHAR r0; Count(r0)` — string load with a non-MIN/MAX consumer rejects at the load | **Item 13: COUNT over string columns** |
| cte_scalar | 2 | join-agg r=8 d=43 w=4 | `LoadCol BIGINT r0 (linked); LoadCol BIGUNSIGNED r1 (linked); GL body imports r1` — a CTE COUNT slot is Bigunsigned (6-2 fix) | Part B |
| gl_v6 | 2 | same | same shape, attrs 1/2 | Part B |
| gl_v5 | 2 | join-agg r=8 d=44 w=1 | op-44 `0a01006c`: wire type 0x0a BIGUNSIGNED linked load | Part B |
| ronsql_basic | 32 | aggregation r=8 d=22 (≈28) + r=1 d=6 (≈4) | kOpMinusBigint over a generic-`/` DOUBLE-track operand; kOpMod(u64, f64) | do NOT lower (RonSQL typing flag / fmod) |

Item 10's "F64 GL / linked" attribution of dd_filter, dd_bigquery,
dd_dtwide was wrong — none of them were F64; the F64 class was
exactly the 20 Part A recovered.

**Revised lowering backlog (ordered by payoff / cost):**

- **Item 12 — LIKE / NOT LIKE in scan filters (24, dd_filter).**
  `Dbtup::evalBranchColForJit` (DbtupExecQuery.cpp ~5648) implements
  EQ..GE only and returns -40 for LIKE / mask; the interpreter's
  handler (~9506) has the LIKE arm: `res1 = (*sqlType.m_like)(cs, s1,
  attrLen, s2, argLen)` with the null rule `r1_null && r2_null ? 0 :
  -1`, then `LIKE: res1 == 0`, `NOT_LIKE: res1 == 1`. Work: add that
  arm to the JIT eval (mirror, incl. the -40 when `m_like == 0`) and
  lift `BR_EMB_MAX_BINARY_COND` admission for cond 6/7 at the two
  bridge sites (OP_ARG; OP_PARAM if the pattern can be a parameter —
  OP_ATTR stays out). No stencils. Verification: bridge pin + a
  `rondb_jit_scan_filter_canary` Q13 (LIKE / NOT LIKE, prefix and
  `%x%`, NULL column, collation-sensitive case) under 4060 + the
  dd_filter pin → 0 then armed. Mask conditions (AND_*) stay
  rejected (no emitter).
- **Item 11 — CASE arm family conflict (4, dd_bigquery).** NNC operand
  adopts the slot's already-claimed family (emit the U64 op variant
  when the slot is U64; a non-negative constant is exact in either).
  Bridge-only; column-first order only, constant-first would need a
  pre-scan (defer). Verification: bridge pin + dd_bigquery → 0, armed.
- **Item 13 — COUNT over string columns (2, dd_dtwide).** The 5F-1
  string-load fusion admits only kOpMin/kOpMax consumers. Extend the
  fusion to kOpCount: emit a count-if-not-null op keyed on the
  column (a cold-call helper reading only the AttrHeader null bit —
  no value decode; one new op kind + stencil, or reuse the NB-load
  null path with a "no decode" flag). Verification: bridge pin +
  dd_dtwide → 0, armed.
- **Part B — BIGUNSIGNED compare mode (6: cte_scalar, gl_v6, gl_v5).**
  As sketched above (OP_BRANCH_TYPED lattice). Note the real-world
  driver is CTE COUNT slots being Bigunsigned, i.e. `GREATEST(x,
  COUNT(..))` — values never reach 2^63 in practice but the type says
  so; still requires the typed compare to be correct.
- **Not lowered (32, ronsql_basic):** kOpMinusBigint over a DOUBLE-track
  operand = RonSQL optimizer typing (flagged upstream); kOpMod mixed
  u64/f64 = fmod path (durable). ronsql_basic stays pin-only.

After items 11-13 the corpus residue would be 38 (6 Part B + 32
not-lowered), with 77 of 81 tests armed.

### Item 12 — IMPLEMENTED (2026-09-01, pending verify): LIKE / NOT LIKE scan filters

Target: dd_filter's 24 (one family: scan-filter `BRANCH_ATTR_OP_ARG`,
cond 7 = NOT_LIKE — NdbScanFilter emits a LIKE predicate as its
branch-on-false to the reject label — arg `'A%'` on `c_mktsegment`).

- **Kernel eval** (`Dbtup::evalBranchColForJit`, DbtupExecQuery.cpp):
  the interpreter's LIKE arm mirrored verbatim — `cond <= GE` keeps the
  m_cmp path; LIKE / NOT_LIKE take `sqlType.m_like` (NdbSqlUtil
  likeChar / likeVarchar / likeLongvarchar + binary siblings = the
  column charset's wildcmp) with the null rule `r1_null && r2_null ? 0
  : -1`, mapped `LIKE: res1 == 0`, `NOT_LIKE: res1 == 1`; a type without
  m_like returns -40 exactly like handleBranchAttrOp; AND_*_MASK still
  -40. Operand-source-agnostic, so OP_ARG / OP_PARAM / OP_ATTR all get
  it (the bridge admits all three at the same site).
- **The abort had to go.** The helper `ndb_jit_h_branch_attr_op_arg`
  aborted the node on any negative eval ("admission keeps these off
  the JIT path"). That premise breaks for LIKE: `m_like` exists only for
  string types, the NDB API's `branch_col_like` does not guard the
  column type, RonSQL's `apply_filter_like` checks only "stored column"
  (mysqld's pushdown does require a string field), and the scan-filter
  cache is keyed on bytecode alone so a program compiled against a
  VARCHAR column can be reused where the same attrId is numeric. The
  interpreter answers that with error 40 to the client. Now: the
  helper records `-rc` in the new `dbtup_jit_call_ctx::error_code` and
  raises `row_fallback`; aggregation paths replay the row on the
  interpreter (same error there); `dbtup_jit_invoke_scan_filter` gained
  an `int *out_error` and returns the code, and interpreterStartLab
  `TUPKEY_abort(req_struct, jit_error)`s — interpreterNextLab's exact
  disposition for a negative handler return. The fail-fast for a
  row_fallback WITHOUT an error code is unchanged. No JitState change
  (the code lives in the glue's ctx), so NO stencil regen.
- **Bridge**: new `BR_EMB_MAX_ATTR_COND 7` for the ATTR_OP_ARG / _PARAM
  / _ATTR site; `BR_EMB_MAX_BINARY_COND 5` stays for the MEM_OP_ARG
  family (evalBranchMemForJit has no LIKE arm; no CTE-consumer LIKE
  in the corpus).
- **Tests**: T42 flipped to accept (LIKE, same 4-op shape as T41), new
  **T42b** (NOT_LIKE with the real dd_filter word layout, attr 2,
  `'A%'`), **T42c** (AND_EQ_MASK 8 still rejects).
  `rondb_jit_scan_filter_canary` **Q13-Q17** under 4060 on t2
  (apple / NULL / cherry / date / cherry): `LIKE 'ch%'` → 3,5;
  `NOT LIKE 'ch%'` → 1,4 (NULL excluded); `LIKE '%e'` → 1,4 (leading
  wildcard); `LIKE 'CH%'` → 3,5 (collation-driven case-insensitivity);
  Q17 differential. Expected results hand-derived from the data.
- **dd_filter re-pinned 24 → 0 and ARMED pre-emptively** (its only
  family is this one); the run confirms or corrects it.
- First bridge_tests run (Mikael): two TEST bugs, bridge right. T42b
  gave a 2-byte literal two inline words (it is one: `2 + ((argLen +
  3) >> 2)` = 3 instruction words) so the stray zero word decoded as
  opcode 0 — rebuilt as dd_filter's exact 5-word program. T42c
  expected op 23 but a mask condition (8+) sets bit 15 = the opcode's
  OVERFLOW bit, so the word decodes as 87 (`BRANCH_ATTR_OP_ARG +
  OVERFLOW_OPCODE`, which the kernel dispatches to the same handler)
  and the bridge's default arm rejects it — pinned as 87. Worth
  knowing: every cond ≥ 8 reaches the bridge as op 87, never as 23.
- Suite run (Mikael, 2026-09-02): `ndb_push_agg_jit.testInterpreterTypedRegs`
  pin **1880 → 1824** with every NDB API case OK — exactly its seven
  LIKE / NOT LIKE cases (15l, 15m, 22h-j, 22n, 22o) × 8 compile
  attempts now compiling, and the CHAR-padding / literal-percent /
  NOT LIKE semantics holding on the JIT (the binary asserts row
  outcomes). Re-pinned; the residual 1824 remains the Phase 7 subset
  boundary (register ops, mask conditions 15n-q, memory ops …).
  `rondb_jit_compound_canary` Q8 was the NEGATIVE control for exactly
  this boundary ("LIKE stays on the interpreter, counters must not
  move") — flipped into a positive control: `b LIKE 'c%'` under 4060,
  `q8_like_compiles` = counters moved.

**Verify (Mikael):**
```sh
cmake --build debug_build --target bridge_tests ndbmtd mysqld -j 8
debug_build/storage/ndb/test/jit_proto/bridge_tests
cd debug_build/mysql-test
./mtr --parallel=1 ndb_push_agg_jit.rondb_jit_scan_filter_canary
./mtr --parallel=1 ronsql_cte_jit.ronsql_cte_dd_filter ronsql_cte.ronsql_cte_dd_filter
./mtr --suite=ronsql_jit,ronsql_cte_jit,ndb_push_agg_jit,ronsql,ronsql_cte --parallel=10 --force
```
Expected: bridge_tests all green (T42/T42b/T42c); canary Q13-Q17 pass
under 4060 with the hand-derived rows; dd_filter delta 0 under arming
with results identical to the OFF arm; no other pin moves. Corpus
census 68 → 44.

### Items 11 + 13 — DONE & VERIFIED (2026-09-03: bridge_tests + five-suite run green)

**Item 11 — CASE arm family conflict (dd_bigquery 4).** Bridge-only.
In the three integer aggregate handlers (typed kOpSumBigint, typed
kOpMin/MaxBigint, generic kOpSum/Min/Max) a BR_REG_NNC operand —
a non-negative constant, exact in either integer family — ADOPTS
the family the slot has already claimed: when `acc_family[slot] ==
BR_ACC_U64` it lowers to the U64 op variant (OP_SUM_U64_CHECKED /
OP_MIN_U64 / OP_MAX_U64) instead of forcing I64 into the 5C-3
conflict. Column-arm-first is the CASE shape the emitters produce
(`THEN unsigned_col ELSE 0`); the constant-FIRST order still claims
I64 and rejects on the later U64 arm (would need a pre-scan; pinned
as the documented limit). A negative constant is I64, not NNC, and
still rejects. Interpreter parity: NdbAggregationCommon's Sum/Min/Max
handle the mixed-signedness pair with the unsigned rule for a
non-negative signed operand — identical outcome. Tests **T79a-d**.

**Item 13 — COUNT over string columns (dd_dtwide 2).** The 5F-1
string-load fusion admitted only kOpMin/kOpMax consumers; a
`LoadCol VARCHAR; Count` rejected at the load. Now kOpCount consumers
are admitted too via a PRESENCE-ONLY load: the bridge emits
`OP_LOAD_COL_NDB` with `col_id | NDB_JIT_COL_PRESENCE_FLAG` (0x4000 —
bit 14, never set in an attribute id; bit 15 stays the linked flag;
new contract define in ndb_jit_bridge.h) followed by the plain
`OP_COUNT_BIGINT` ops, then the fused MINMAX ops (emission order
matters: the NB null branch lands on the first non-reader after the
last COUNT, and MINMAX is not a reader). nb_convert_loads turns the
presence load into the NB form (col moves c → b, flag rides along),
so NULL rows skip the COUNTs natively — the interpreter's Count
null-skip. Glue: `ndb_jit_h_load_col` / `_nb` detect the flag and call
the new `jit_col_presence` (linked columns via the shared linked walk;
local columns via the NEW public `Dbtup::readAttributeIsNullForJit`,
which reads into the block's full-size coutBuffer — a 4-word stack
buffer fails readSingleAttribute's max_read for any real string),
store 0 and return the NULL-ness (NB: 1 = take the branch; non-NB:
NULL = per-row fallback like a NULL value; read failure = fallback).
NO stencil change (the flag rides the existing col operand). Tests
**T80a-c** (presence load + COUNT; COUNT + MIN; MAX then COUNT).

dd_bigquery and dd_dtwide re-pinned 4 → 0 / 2 → 0 and ARMED
pre-emptively; the run confirms or corrects. **Corrected by the run
(2026-09-03): dd_bigquery is 4 → 2, NOT 0, and stays EXEMPT.** The
pre-emptive arming did its job loudly — 5120 aborted both nodes on
the 104-word program at word 83: `LoadCol BIGUNSIGNED r1 (linked);
MaxBigint(r1, slot 4); Skip 4; LoadConst r1 = -9; MaxBigint(r1, slot
4)` and, at words 97-103, the same with MinBigint / slot 5 — i.e.
`MAX/MIN(CASE .. THEN biguns_col ELSE -9 END)`. A NEGATIVE constant is
I64, not NNC (T79d pins exactly this), and the shape is not lowerable
with static accumulator families: the interpreter answers it by
flipping the AggResItem's signedness at runtime (a -9 row makes MIN
signed; MAX stays unsigned unless every row took the ELSE arm), which
the JIT cannot represent. By-design residue, same bucket as Part B.
The 27-word `ELSE 0` program (SumBigint, slot 3) does compile now, so
the pin is 2 (one program × two nodes). Wrapper un-armed, pin 2.

First bridge_tests run (Mikael, 2026-09-03): T59d — the 5F-1 negative
pin "string load with a non-Min/Max consumer rejects" — used COUNT as
its consumer, exactly the shape item 13 admits; re-pointed at a SUM
consumer (still rejects), the accept side is T80a-c. No bridge change.
Suite run (2026-09-03): `ndb_push_agg_jit.ndb_join_pushdown_agg_types`
pin **1 → 0** with identical results — the mysqld join-pushdown types
test's last reject (its string columns make this item 13's COUNT over
a CHAR/VARCHAR, the only new admission that fits a mysqld program).
Re-pinned (that suite is pin-only by convention; the fleet mirrors
carry no arm includes).

**Verify (Mikael):**
```sh
cmake --build debug_build --target bridge_tests ndbmtd -j 8
debug_build/storage/ndb/test/jit_proto/bridge_tests
cd debug_build/mysql-test
./mtr --parallel=1 ronsql_cte_jit.ronsql_cte_dd_bigquery ronsql_cte_jit.ronsql_cte_dd_dtwide
./mtr --suite=ronsql_jit,ronsql_cte_jit,ndb_push_agg_jit,ronsql,ronsql_cte --parallel=10 --force
```
Expected (revised): T79a-d / T80a-c green; dd_dtwide 0 under arming,
dd_bigquery 2 unarmed, results identical to the OFF arm; no other pin
moves (the fleet's join_pushdown_agg_types 1 → 0 already re-pinned).
Corpus census 44 → 40 (6 Part B + 2 negative-constant CASE arms + 32
not-lowered); 76 of 81 tests armed. Exempt for good: ronsql_basic,
gl_v5, gl_v6, cte_scalar, dd_bigquery (+ the census).

### GL Part B — DONE & VERIFIED (2026-09-03: bridge_tests + five-suite run green): BIGUNSIGNED compare mode

Target: the last 6 lowerable rejects — cte_scalar 2 + gl_v6 2
(`GREATEST(bigint_linked, biguns_linked)`: a CTE COUNT slot is
Bigunsigned, so RonSQL emits an op-43 import of a non-u63 U64
register) and gl_v5 2 (op-44 BIGUNSIGNED linked load).

- **Typed compare helper = compareTypedRegs' full lattice.**
  `ndb_jit_h_branch_f64` (OP_BRANCH_F64 keeps its name; the stencil is
  just the call + branch — ZERO stencil change) gains per-side unsigned
  flags (arg bit 6 left-u64, bit 7 right-u64, next to the F64 bits
  4/5): any double side → double compare with the int side converted
  signed or unsigned per its flag; both u64 → unsigned; both signed →
  signed; mixed → a negative signed operand is strictly less than any
  unsigned, else unsigned compare. Mirrors DbtupExecQuery.cpp's
  compareTypedRegs arm for arm.
- **Bridge, embedded tracker**: new `emb_reg_u64[8]` next to
  `emb_reg_f64` (every reg-defining op clears it). op-43 admits a
  non-u63 U64 outer source (BIGUNSIGNED column / constant,
  DECIMALUNSIGNED, u64 arithmetic) and marks the dst U64; op-44 admits
  the BIGUNSIGNED wire type (helper: raw 8 bytes) and marks the dst.
  The REG_REG compare takes the typed helper when either side is F64
  OR U64, with the four flags; the READ_ATTR → F64-load conversion
  runs only when a DOUBLE side exists. Embedded ADD/SUB/MUL over a
  U64-marked operand rejects like F64 (the ARITH_FB stencil is signed).
- **Packed temporals stay out.** New outer `reg_u64_pack[]` flag —
  set by DATETIME2 / TIMESTAMP2 loads, cleared by LoadConst / DEC /
  string loads, propagated by kOpMov, OR-merged at CASE-jump targets
  (new `reg_pack_snap` in PendingCaseJump), passed into the embedded
  walker as `outer_reg_pack` — gates the import: whether a big-endian
  temporal pack's loaded word orders under an unsigned compare is
  unproven, and no emitter asks for it. Two stale-flag fixes on the
  way: a BIGUNSIGNED LoadConst now sets u63 from its top bit
  (previously left over from the register's last load), and a
  DECIMALUNSIGNED(scale 0) load clears u63.
- **Tests**: T70d / T73c flipped to accept; new **T81a** (the corpus GL
  shape: right-u64 flag → arg 0xA985, Sum lowers to SUM_U64_CHECKED
  after the trellis Mov, merge int-class), **T81b** (op-44 BIGUNSIGNED
  vs BIGINT → left-u64 0xA942), **T81c** (DATETIME2 import still
  rejects), **T81d** (embedded ADD over a U64 operand rejects),
  **T81e** (DOUBLE vs BIGUNSIGNED imports → 0x10 | 0x80 = 0xA994).
- gl_v5, gl_v6, cte_scalar re-pinned 2 → 0 and ARMED pre-emptively;
  the run confirms or corrects (dd_bigquery taught us the arming
  itself is the fastest diagnostic).
- First bridge_tests run (Mikael): T81c was a test bug — DATETIME2 is
  type 32 and the plain `enc_load_col` has a 5-bit type field, so the
  load decoded as type 0 (NON_BIGINT, reason 2) before the import was
  ever reached; `enc_load_col_wide` (bit 20 = type bit 5, as T68b)
  fixes it. Bridge right.

**Verify (Mikael):**
```sh
cmake --build debug_build --target bridge_tests ndbmtd -j 8
debug_build/storage/ndb/test/jit_proto/bridge_tests
cd debug_build/mysql-test
./mtr --parallel=1 ronsql_jit.ronsql_cte_greatest_least_v5 ronsql_jit.ronsql_cte_greatest_least_v6 ronsql_jit.ronsql_cte_scalar
./mtr --suite=ronsql_jit,ronsql_cte_jit,ndb_push_agg_jit,ronsql,ronsql_cte --parallel=10 --force
```
Expected: T70d / T73c / T81a-e green; the three deltas 0 under arming
with results identical to the OFF arm (v6's `GREATEST(pairs.k,
pairs.amt)` pairs a BIGINT with a COUNT — the unsigned arm on real
values); no other pin moves. Corpus census 40 → **34** = dd_bigquery's
negative-constant arms 2 + ronsql_basic's 32; 79 of 81 tests armed.
Nothing lowerable remains in the corpus.

**VERIFIED 2026-09-03 (all tests passed).** Final corpus state: 34
rejects = dd_bigquery 2 (negative-constant CASE arms; needs dynamic
slot signedness) + ronsql_basic 32 (kOpMinusBigint RonSQL typing +
kOpMod fmod); 79 of 81 tests under strict arming; every remaining
reject classified as not lowerable with static accumulator families.
The lowering campaign is closed for good: ~1234 → 34 (97%).

### Item 14 — ATTEMPTED AND REVERTED (2026-09-03): negative-constant CASE arms

Target: dd_bigquery's last 2 — `MAX/MIN(CASE .. THEN biguns_col ELSE
-9 END)`. **Stays exempt, pin 2.**

**The attempt**: lower the `-9` arm's aggregate to the per-row
fallback (OP_SET_REG_NULL_FB) — rows taking the constant arm replay
on the interpreter, column-arm rows stay native, slot family U64.
bridge_tests green, then the MTR differential caught a WRONG RESULT:
`MAX(CASE .. ELSE -9 END)` returned **18446744073709551607** (= 2^64
- 9) instead of 5; the MIN sibling returned -9 correctly.

**Why it is unsound — the general lesson.** The per-row fallback
replays a row on the interpreter, which may MUTATE THE ACCUMULATOR
INTO A STATE OUTSIDE THE JIT'S STATIC FAMILY: a MAX slot initialised
by a `-9` row is a SIGNED AggResItem; the next column row runs the
native OP_MAX_U64, which reinterprets those bits as unsigned
(2^64 - 9), keeps them as the maximum, and the writeback stamps the
slot unsigned. MIN passed only by row order (a later `-9` replay
re-flipped it). Every earlier use of the per-row fallback is sound
because the replayed row leaves the slots UNTOUCHED (GREATEST null
rows: the aggregate skips a NULL register; kOpSetRegNull rows: same;
item 12/13's error / NULL rows: aborted or skipped) — this was the
first fallback that aggregates a value of a foreign family. **Rule
for future lowerings: a per-row fallback is admissible only if the
interpreter's replay cannot change any accumulator's family.**

**What a sound lowering would take** (not done — 2 synthetic rejects
on `ELSE -9` over a COUNT): a per-row DYNAMIC FAMILY GUARD in the
dispatch glue — the bridge exports the program's static acc_family
table into the compiled product, and for programs flagged "has a
mixed-sign fallback arm" the glue checks each claimed slot's
AggResItem (type, is_unsigned) against its static family before
running the blob, falling the whole row back on any mismatch. Sound
(the blob never sees a foreign-family slot), correct for MAX (the
interpreter flips the slot back to unsigned on the next column row,
so native execution resumes) and for MIN (once signed, every row
falls back — the interpreter's answer), but it adds a per-row loop
to the hot path and product/cache plumbing for a shape nobody
writes. Reject-forever stands. Reverted to the committed state: the
negative constant into a U64 slot rejects TYPE_MISMATCH (T79d), dd_
bigquery pin 2, exempt.

Final corpus state after Part B therefore holds: **34** rejects =
dd_bigquery 2 (this) + ronsql_basic 32; 79 of 81 tests armed.

**ronsql_basic's 32, re-read from the bridge's own rules (2026-09-03,
Mikael's question "are they also deemed rejected" — yes):**
- ≈28 = a typed `MinusBigint` over an UNSIGNED register and a SIGNED
  variable (`unsigned_col * 1000000 - mediumint_col * 1000`). The
  Phase 5C-4 classifier lowers unsigned arithmetic only when both
  sides are U64 or a non-negative constant; with a signed variable
  the kernel's result signedness depends on the runtime values —
  the same dynamic-signedness limit as item 14, at the register
  level. Reject-forever. (Item 10's "RonSQL optimizer typing" note
  was imprecise: not an emitter bug; the interpreter's answer is
  right.)
- ≈4 = `kOpMod` with a DOUBLE operand (`x MOD (a / b)`): the
  interpreter's fmod path, classified a durable negative. Lowerable
  with a small cold-call fmod helper (mirror the double-mod
  semantics incl. divide-by-zero → NULL → per-row fallback, which
  is family-safe: the row is skipped) — on the backlog, not worth
  doing for a shape nobody writes.

### Item 15 — DONE & VERIFIED (2026-09-04: bridge_tests + the three JIT suites green): kOpAvg lowered

Done per the sketch below, plus one precondition the sketch flagged
and which turned out real: the join-agg per-row dispatch took the
LEAF's header slot count (`m_jit_leaf_n_agg = leaf->m_n_agg_results`
at both leaf switches), which does not include the hidden slots Init
appends — the COUNT half of an AVG would have been dropped at
writeback. Now single-leaf adds `m_n_hidden_slots` (multi-leaf,
detected by `m_agg_ops_cached`, keeps the leaf's slice count and does
not lower AVG).
- Bridge: `BR_kOpAvg 31`; new `ndb_jit_bridge_translate_ex(prog, n,
  n_visible_results, ...)` (the plain translate = `NDB_JIT_NO_AVG_SLOTS`,
  which keeps kOpAvg rejected); the handler is the generic kOpSum
  lowering into dst (typed by track, family claimed, overflow-checked)
  followed by OP_COUNT_BIGINT into `n_visible + ordinal`; dst ≥
  visible, duplicate dst, hidden ≥ BC_MAX_ACCS → REG_OUT_OF_RANGE.
- Cache: `dbtup_jit_compile_agg` gains `n_visible_results`, appended
  as ONE trailing key word (identical bytecode with a different
  visible count must not share a blob); the callback decodes it.
  Create passes the sentinel (standalone programs never carry kOpAvg);
  the proxy passes `lp.m_n_agg_results` for single-leaf, the sentinel
  otherwise, to both the diagnostic translate and the compile.
- Tests: **T83a** (first-sighting program: SUM(0) + COUNT(1), NB null
  branch past both), **T83b** (two AVGs around a SUM: hidden 3 and 4,
  DOUBLE AVG = SUM_F64), **T83c** (plain translate still rejects),
  **T83d** (cap / dst / duplicate bounds).
- testCteNdbApiFilter re-pinned 4 → 0.

**Verify (Mikael):**
```sh
cmake --build debug_build --target bridge_tests ndbmtd -j 8
debug_build/storage/ndb/test/jit_proto/bridge_tests
cd debug_build/mysql-test
./mtr --parallel=1 ndb_push_agg_jit.testCteNdbApiFilter ronsql_cte_jit.ronsql_cte_dd_avg
./mtr --suite=ndb_push_agg_jit,ronsql_jit,ronsql_cte_jit --parallel=6 --force
```
Expected: T83 green; testCteNdbApiFilter 0 (its Tests 25/26 assert the
AVG values — the owner's finalize divide over JIT-written SUM/COUNT
slots); dd_avg mirror 0 with results identical to the OFF arm; no
other pin moves.

Original backlog sketch (2026-09-04):

The rebase (RONDB-1107 "AVG in CTE outputs via SUM+COUNT slots and
finalize divide") added aggregation opcode **kOpAvg = 31**: per row it
runs the Sum kernel into the VISIBLE dst slot (low 16 bits) and the
Count kernel into a HIDDEN companion slot; JoinAggInterpreter::Init
scans the program, assigns the k-th kOpAvg the hidden slot
`n_visible_results + k` (m_avg_hidden_map), and grows m_n_agg_results
by the number of AVGs, so the hidden slots merge / redistribute as
ordinary SUM / COUNT slots; the CTE owner divides once after the
redistribute (finalizeAvgSlotsSlice). JoinAggInterpreter-only in v1.

First sightings: `join-agg bridge rejected (reason=1 detail=31
word=1)` on `LoadCol BIGINT r0; kOpAvg r0 -> slot 0` —
testCteNdbApiFilter Tests 25/26 (pin 0 → **4**, re-pinned, that
suite is pin-only); expect the same on the new ronsql_cte_jit dd_avg
mirror and any RonSQL AVG-over-CTE query.

Lowering sketch (no stencils): the bridge emits, for `kOpAvg r ->
dst`, the typed SUM of the register's track into slot dst
(SUM_BIGINT_CHECKED / SUM_U64_CHECKED / SUM_F64, claiming the family)
plus OP_COUNT_BIGINT into slot `n_visible + ordinal` — it must know
`n_visible_results` (the header's n_agg_results before Init grew it),
which the translate entry point does not receive today: add a
parameter (the proxy has `lp.m_n_agg_results`; Create has the
header), and count kOpAvg ordinals in program order exactly like
Init. Bounds: `n_visible + n_avg <= BC_MAX_ACCS` (32) else reject.
The dispatch count must be the grown m_n_agg_results so the writeback
covers the hidden slots (verify m_jit_leaf_n_agg / the standalone
count are taken AFTER Init). Multi-leaf: hidden slots are per
interpreter, appended after the TOTAL — check setTotalAggResults'
new lines before trusting the per-leaf offset. Verification: a
bridge pin (2-word program → SUM + COUNT with the right slots),
testCteNdbApiFilter 4 → 0, dd_avg mirror → 0, results identical to
the OFF arm (the divide is the owner's, untouched).

**Verification list for Part A (Mikael runs):** `bridge_tests`
(T70c, T73d, T78a-f), then the exempt-pin tests under `ronsql_jit`
/ `ronsql_cte_jit` (dd_filter, gl_v5, dd_bigquery, cte_scalar, …) —
pins that DROP are the win; re-record them and move the tests that
reach 0 under slice-3 strict arming (`jit_strict_arm.inc` /
`jit_strict_disarm.inc` around the body include); then the full
`ndb_push_agg_jit` mirror once (the guard also observes mysqld CASE
programs — any pin move there is a shape worth looking at).
