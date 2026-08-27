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
family. `testInterpreterTypedRegs` (1888) did not move, as
predicted (its filters carry REG ops, `allow_reg_ops=0`).
`ndb_push_agg_jit` and the OFF arms: unchanged.

Remaining 206 by family: op-44 `READ_LINKED_COLUMN_TO_REG` ≈50
(item 4, next), reason-8 op-43 type-admission ≈52 (v5 —
BY DESIGN: F64/BIGUNSIGNED import sources), `BRANCH_MEM_OP_ARG`
38/40 ≈16, kOpMod-unknown ≈16, MinusBigint TYPE_MISMATCH ≈16,
`LOAD_DOUBLE_CONST` ≈8, op-51 ≈4, PROG_TOO_LARGE ≈3.
