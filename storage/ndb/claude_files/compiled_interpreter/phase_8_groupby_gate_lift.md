# Phase 8 #5 — GROUP BY gate lift

**Status: IMPLEMENTED & CANARY-VERIFIED (2026-08-19).**
`rondb_jit_groupby_canary` passes after stencil regen: grouped SUM and
COUNT+SUM JIT under 4060 + counter deltas, the 500-group differential
and grouped join-agg are correct, CASE stays on the interpreter
(expected — see findings below), and the >4-aggregates negative holds.
Remaining sweep: the full `ndb_push_agg` suite (existing grouped tests
now dispatch admitted shapes through the JIT with no error insert —
they are the broad regression net).
All six slices are in the tree. **The build REQUIRES `regen-stencils`**
(pinned clang 20.1.8) before anything else: `OP_COUNT_BIGINT` and the
new `JitState::value_unsigned[]` accessor exist only in
`stencils_src.c` until the generated headers are refreshed — until
then the new op admission-rejects cleanly (`g_stencils[34].n_bytes ==
0`) and `coldcall_tests` T13 / `bridge_tests` T46 execution paths and
the canary's Q2/Q4 fail. Verify order: (1) regen-stencils, (2) rebuild
host tools + `ndbmtd`, (3) `bridge_tests` (T46/T46b) + `coldcall_tests`
(T13) + `admission_tests` + `proto_microbench`, (4)
`./mtr --suite=ndb_push_agg rondb_jit_groupby_canary` plus the FULL
`ndb_push_agg` suite — the existing grouped tests (testJoinAgg*,
ndb_pushdown_agg, testCaseAgg…) now dispatch admitted grouped shapes
through the JIT without any error insert, making the whole suite the
regression net. Canary notes: Q5 (grouped join agg) deliberately
has no ndbinfo counter check (the proxy join-agg path compiles outside
the progcache, and the agg 4060 guards abort on ANY fallback, so 4060
alone is sufficient proof for aggregation paths).

**First run findings (2026-08-19; Q1/Q2/Q4/Q5/Q6 passed on the first
sweep — the lift, COUNT, high-cardinality and grouped join-agg all
verified):**
- **SQL-pushed CASE aggregation does not JIT yet.** Two layers: the
  planner (`ha_ndbcluster_push_agg.cc is_pushable_case_expr`) only
  pushes CASE with an ELSE whose value is pushable — `ELSE NULL` is
  not pushed at all (evaluated server-side, no agg program) — and the
  pushed form emits `BRANCH_*_REG_REG` opcodes inside the embedded
  block (`get_branch_opcode`), which the bridge's embedded set does
  not lower. **Concrete Phase 5 work item:** lower the REG_REG
  embedded branch family; canary Q3 asserts delta == 0 today and
  flips when that lands. (NDB API Test 29's CASE canary JITs because
  it emits ATTR-based branches — different shape.)
- **Per-group SQL-NULL is not expressible via pushed SQL** — every
  pushable CASE arm contributes a value, and WHERE filters rows before
  their group exists. The per-group NULL semantics rest on the
  value_updated writeback, which is group-agnostic and scalar-proven
  (NDB API Test 25); an optional grouped NDB API canary would prove it
  end-to-end if wanted.

Lift the `m_n_gb_cols == 0` dispatch gates so grouped aggregation runs
its per-row program through the JIT. This is the last Phase 8 item and
the largest remaining coverage win: GROUP BY is the dominant shape of
real analytics queries.

## Why this is smaller than the status doc feared

The old assumption ("needs grouped accumulator lookup/update *in the
JIT*") is wrong. Verified against the source (2026-08-19):

1. **The group prologue is block-level C++, not bytecode.**
   `AggInterpreter::ProcessRec` (and JoinAgg's richer variant) reads
   the GROUP BY columns via `readSingleAttribute`, does the
   charset-aware (`xfrm`) hash find/insert in `m_gb_map`, allocates and
   zero-inits a new group record on miss (`allocGroupData`, chunk
   allocator, `req_struct->read_length` accounting), and only *then*
   enters the interpreter loop with `agg_res_ptr` pointing at that
   group's `AggResItem` slots (stored inline after the key in the group
   record). None of that is in the interpreted program — the bytecode
   is the same accumulator/filter stream as the scalar case.
2. **The JIT dispatch already takes `agg_res_ptr` as a parameter.**
   `dbtup_jit_invoke(agg, block_tup, req_struct, entry, agg_res_ptr,
   n_agg_results, join_agg)` — pointing it at a group's slots instead
   of `m_agg_results` is the whole per-row change.
3. **Writeback semantics already match fresh groups.** The glue copies
   `agg_res_ptr[i].value.val_int64` in (0 for a fresh group), runs the
   blob, and writes back only `value_updated[i]` slots — setting
   `type=NDB_TYPE_BIGINT / is_null=false`. A never-updated slot keeps
   the fresh-group init (`NDB_TYPE_UNDEFINED / is_null=true`) ⇒ SQL
   NULL over an empty/all-rejected group, per group, for free. (This is
   the same `value_updated[]` mask that fixed scalar all-rejected SUM.)
4. **The compiled region excludes GROUP BY metadata.**
   `m_agg_prog_start_pos = m_cur_pos` is assigned *after* the GB column
   list words are consumed (`AggInterpreterBase.cpp:~560-580`), and
   both the compile and the progcache key use `agg_prog + bc_off`.
   Cache-key soundness: two queries differing only in GB columns but
   with identical instruction streams *correctly* share one blob — the
   blob's behaviour depends only on the instruction stream; grouping is
   the prologue's job. (Same argument as OP_PARAM sharing.)

So the lift = remove two gate conditions + one compile-gate condition,
plus one genuinely new piece of JIT work: **lowering `kOpCount`**,
without which `SELECT grp, COUNT(*), SUM(v) … GROUP BY grp` — the
canonical grouped query — still rejects (the bridge has no
`BR_kOpCount` case today; discovered during the EXIT_OK_LAST
investigation).

## Slices

### Slice 1 — lower kOpCount (new stencil)

The one stencil-regen item. `OP_COUNT_BIGINT`: `acc[slot] += 1` and set
`value_updated[result_index]` — no column read, no operands beyond
slot/result-index, no overflow concern at realistic row counts (Int64).
- `bytecode1.h`: new OpKind (append-only).
- `stencils_src.c` + regen (`stencils_{x86_64,arm64}.h`, pinned clang
  20.1.8) + `hole_kinds.h` magics + `audit_magics.c`.
- `ndb_jit_bridge.c`: `case BR_kOpCount` → emit, mirroring
  `BR_kOpSumBigint`'s slot/result-index handling. NB the interpreter's
  COUNT counts the row when the row *reaches* the op (row-disposition
  filters skip it via EXIT_REFUSE / CASE jumps) — the JIT's linear
  fall-through gives the same semantics.
- Host tests: bridge accept/lowering case, coldcall execute case
  (count over N invocations, value_updated set, rejected row not
  counted), microbench informational.
- Benefits scalar COUNT immediately (implicit `COUNT(*)` still comes
  from `ha_records()`/stats, so `ndb_pushdown_agg` Tests 11-12 are
  unaffected; pushed `COUNT(v)`-style shapes start JITting).
- **Do NOT lower MIN/MAX here** — Test 27/28
  (`testJitUnsupportedFallback`) depends on `kOpMaxBigint` rejecting;
  MIN/MAX belong to Phase 5 (and that test's durability note already
  says to switch it to DivInt/Mod when MAX lowers).

### Slice 2 — lift the compile gate

`PushdownInterpreterFactory::Create`: drop the `n_gb_cols() == 0`
condition around `dbtup_jit_compile_agg` (keep `bc_off < prog_len` and
the Slice-4 `pinned` pass-through). The proxy join-agg path already
compiles without a GB gate (its blobs were simply never dispatched).
Progcache implications: none — key unchanged, sharing sound (above).

### Slice 3 — lift the dispatch gate, standalone

`AggInterpreter::ProcessRec`: gate becomes `m_jit_entry != nullptr`.
Verify the dispatch sits *after* the grouped prologue has resolved
`agg_res_ptr` (it must — the scalar path also needs `agg_res_ptr`
resolved); move it there if today's placement short-circuits earlier.
The prologue's error paths (`initGBTypes` failure, `allocGroupData`
returning null ⇒ `ZAGG_OTHER_ERROR`) all return before dispatch —
unchanged.

### Slice 4 — lift the dispatch gate, join-agg

`JoinAggInterpreter::ProcessRec`: same gate change. Keep the compile
gate `m_num_leaves == 1`; note `m_acc_offset` (multi-leaf accumulator
base shifting) is 0 in the single-leaf case the JIT handles — assert
or verify rather than assume. Linked-attr `req_struct` plumbing around
the dispatch is already in place and grouping-independent. Eviction
(`m_join_agg_evict_rows`) and the chunked CONTINUEB teardown read group
records *between* ProcessRec calls; the JIT writes back before
returning, so records are always current at those points — same
contract as the interpreter.

### Slice 5 — canaries

New MTR `rondb_jit_groupby_canary` (ndb_push_agg), each JIT-asserted
query proven the compound-canary way (ERROR_INSERT 4060 **plus**
`ndbinfo.jit` `programs_compiled + programs_reused` delta ≥ 1 —
remember 4060 alone cannot prove compilation since the
`m_jit_filter_ineligible`-era exemptions):
- Q1 `SELECT grp, SUM(v) … GROUP BY grp` (the basic lift).
- Q2 `SELECT grp, COUNT(v), SUM(v) … GROUP BY grp` (Slice 1).
- Q3 grouped + embedded filter (`SUM(CASE …)` or a WHERE that lowers)
  — all-rejected *group* keeps SQL NULL for SUM (per-group
  `value_updated` semantics).
- Q4 high-cardinality grouping (a few thousand groups — chunk
  allocator growth under JIT dispatch) as a differential (no 4060;
  identical results JIT vs `CompiledInterpreter=OFF` or interpreter
  baseline recorded values).
- Q5 join-agg grouped (`GROUP BY` over a 2-table pushed join) under
  4060 + counter delta.
- Negative: > `BC_MAX_ACCS` (4) aggregates → falls back cleanly
  (counter delta exactly 0, no 4060 armed).
NDB API: extend `testJoinAggNdbApi` (next free number; wrappers use
`--only N` — keep numbering in sync) with a grouped must-JIT test if
MTR-level coverage proves insufficient; start MTR-only.

### Slice 6 — docs + bench

Status doc updates; microbench note: honest expectation is a *smaller*
end-to-end win than scalar (the per-row hash find + GB column reads
stay interpreted C++; the JIT removes only the interpreter loop over
the accumulator/filter stream). Quantify with the existing bench
queries; grouped-path profiling may later motivate prologue work
(outside JIT scope).

## Explicitly out of scope

- MIN/MAX, DOUBLE, string aggregation lowering — Phase 5 (Test 27/28
  fallback canary depends on MAX rejecting).
- Multi-leaf join-agg programs (`m_num_leaves > 1`), `m_acc_offset != 0`.
- GROUP BY string-key or prologue optimization (not JIT work).
- Grouped-drain / eviction changes — contracts untouched.
- Memory-pressure sweep of pinned progcache entries (noted in Slice 4
  of the code-memory doc, still future).

## Risks / verification focus

- **COUNT row-disposition semantics** (Slice 1): a filter-rejected row
  must not count. bridge_tests case with EXIT_REFUSE before COUNT.
- **Fresh-group type init vs writeback**: covered by design (see
  "smaller than feared" #3) but Q3's all-rejected-group NULL assertion
  is the regression net.
- **4060 blast radius**: after the lift, any 4060-armed test running a
  grouped query with only-admitted ops *must* JIT; grouped queries with
  unadmitted ops abort under 4060 exactly like scalar ones — canaries
  choose their queries accordingly (unchanged contract).
- **Batch accounting** (`read_length`, `m_agg_curr_batch_size_*`) is
  prologue-side and untouched — differential Q4 at high cardinality is
  the net.
