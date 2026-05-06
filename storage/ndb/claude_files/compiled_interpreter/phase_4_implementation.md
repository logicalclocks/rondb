# RONDB-1056 Phase 4 — DBTUP thin-slice integration

This document is the implementation plan for Phase 4, the second
major decision gate. Phase 4 wires the JIT into RonDB's real
aggregation signal flow: `JOIN_AGG_SETUP_REQ` arrives at
`DblqhProxy`, the proxy compiles the program once per node, and the
compiled blob is fanned out read-only to every LDM worker's
`JoinAggInterpreter`. Per the user's clarifications:

- Per-node compile exactly matches the existing setup-state
  distribution model — no refcount, no per-LDM compile, no per-row
  JIT/interp re-evaluation.
- `JOIN_AGG_SETUP_REQ` always feeds `JoinAggInterpreter`
  (CTEs / joins). `AggInterpreter` (single-table scans) goes
  through a different setup signal and is **out of Phase 4 scope**.
- Aggregation programs can include `kOpEmbeddedInterp` calls into
  the old/embedded interpreter — Phase 4 admission rejects these,
  the program falls back to the interpreter for all rows.

## 1. Scope

**Decisions taken** (after the three Phase 4 design questions):

- **(b) No cold-call infrastructure in Phase 4.** Defer to Phase 5
  along with the wider opcode set. Phase 4's coverage is
  intentionally narrow enough that no opcode needs a C++ helper.
- **(a) Add hot-arithmetic stencils for bigint Plus/Minus/Mul.**
  Cheap to add (clones of `OP_ADD_INT_INT`) and turns
  `SELECT SUM(c1+c2-c3*c4) FROM t` into a JIT-eligible program —
  a real-shape demo, not a toy.
- **(b) State translation: non-null bigint registers only.** No
  type/null tracking in JitState. Programs touching nullable or
  non-int64 registers fall back via admission rejection. Phase 5
  expands.

**In scope:**

- **Bytecode bridge.** Translates NDB's `Uint32*` aggregation
  bytecode to our `Program/Op` struct. Recognises 11 supported
  opcodes; everything else (including `kOpEmbeddedInterp`) returns
  `JIT_BRIDGE_UNSUPPORTED_OP`.
- **Per-node compile site** in `DblqhProxy::execJOIN_AGG_SETUP_REQ`.
  Existing fan-out plumbing distributes the resulting `Jit1Prog*`
  to every LDM worker's `JoinAggInterpreter`.
- **Per-program dispatch** in `JoinAggInterpreter::Init` /
  `ProcessRec`. Cached entry pointer; no per-row re-check beyond
  what the branch predictor folds away naturally.
- **Two new bigint arithmetic stencils** (Minus, Mul; existing
  `OP_ADD_INT_INT` stays as Phase 1's "Plus" stencil).
- **State translation**: NDB's `Register[kRegTotal]` ↔ JitState's
  `regs_i64[]`. Narrow — only `int64_t value` matters; type /
  nullable / unsigned must already be statically int64-non-null at
  admission.
- **Tests**: a `testJoinAgg`-shape MTR test that runs `SELECT
  SUM(c1)` / `SELECT SUM(c1+c2)` queries with JIT on and off,
  asserts result equality. Plus a per-program-dispatch invariant
  unit test.

**Out of scope (deferred to Phase 5 unless noted):**

- **`AggInterpreter` (single-table scans).** Different setup
  signal. Same JIT machinery would apply, but wiring the
  integration site is its own piece of work — punt.
- **Embedded interpreter blocks** (`kOpEmbeddedInterp`) — programs
  using these (every WHERE / CASE / nullable-aware logic) fall
  back to interpreter.
- **All non-bigint types**: kOpLoadConst-double, kOpSumDouble, etc.
- **Nullable-aware arithmetic.**
- **Division / modulo** (kOpDiv*, kOpMod): needs cold-call
  NULL-fixup helper.
- **GROUP BY / hashing logic**: GB hashing lives in the JoinAgg
  C++ layer outside the per-row JIT'd body. Phase 4 doesn't touch
  it.
- **Cold-call mechanism + helper inventory.**
- **Performance gates** on real queries — Phase 5+.

## 2. File layout

The integration touches three areas: the existing JIT engine
(small additions), a new bridge layer (new files), and DBTUP/DBLQH
block code (modifications).

```
storage/ndb/src/kernel/blocks/dbtup/jit/
├── jit1.h                      (no change)
├── jit1.c                      (no change)
├── bytecode1.h                 (2 new OpKind values: MINUS, MUL)
├── stencils_src.c              (2 new stencils: minus, mul)
├── hole_kinds.h                (4 new MAGIC_* + 6 symbol entries)
├── stencils_x86_64.h           (regenerated)
├── stencils_arm64.h            (regenerated)
└── ndb_jit_bridge.{c,h}        [NEW]
    Translation layer: NDB bytecode → our Program. Pure C, no NDB
    types in the .h interface — DBTUP code includes only this.

storage/ndb/src/kernel/blocks/dbtup/
├── JoinAggInterpreter.{cpp,hpp} (Init reads JitEntry; ProcessRec
│                                 dispatches via cached entry_fn)
├── DbtupJitGlue.{cpp,hpp}      [NEW]
    C++ glue: NDB Register[] ↔ JitState. Inline static helpers.

storage/ndb/src/kernel/blocks/dblqh/
├── DblqhProxy.{cpp,hpp}        (execJOIN_AGG_SETUP_REQ calls bridge
│                                + jit1_compile, stashes Jit1Prog*
│                                in the fan-out record)
├── JoinAggregationState.hpp    (one new field: Jit1Prog* m_jit_prog)
```

**Note on AggInterpreter.** Phase 4 doesn't touch it. The same
bridge + glue files Phase 4 lands are reusable when single-table
aggregation gets its own JIT integration in a future phase — the
bridge is bytecode-level (no interpreter context), the glue
operates on the shared `Register[]` shape that both interpreters
use.

## 3. The bytecode bridge

### 3.1 Goal

Take an NDB aggregation program (a `Uint32*` array, layout per
`AggInterpreter.cpp` / `JoinAggInterpreter.cpp`'s decode loops —
both share the wire format) and produce either:

- A populated `Program` struct that `jit1_compile` will accept, OR
- A clean rejection with a reason code.

The bridge is the only place that knows about both bytecode formats.
`jit1.c` stays oblivious to NDB; `JoinAggInterpreter` stays
oblivious to the JIT's internal `Op` struct.

### 3.2 API

```c
/* In ndb_jit_bridge.h */
typedef enum {
  JIT_BRIDGE_OK              = 0,
  JIT_BRIDGE_UNSUPPORTED_OP  = 1,   /* opcode has no JIT mapping */
  JIT_BRIDGE_NULLABLE_REG    = 2,   /* nullable register reference */
  JIT_BRIDGE_NON_BIGINT      = 3,   /* program touches double / etc */
  JIT_BRIDGE_PROG_TOO_LARGE  = 4,
  JIT_BRIDGE_MALFORMED       = 5,   /* truncated word stream */
} JitBridgeReason;

typedef struct {
  JitBridgeReason reason;
  uint32_t        offending_word;   /* index into the input bytecode */
  uint32_t        offending_op;     /* the kOp* value, if relevant */
} JitBridgeError;

/* Translate an NDB aggregation program into our Program format.
 * Returns JIT_BRIDGE_OK on success; out_prog is populated.
 * Returns the rejection reason otherwise; out_prog is unchanged. */
JitBridgeReason ndb_jit_bridge_translate(const uint32_t *ndb_prog,
                                          uint32_t       n_words,
                                          Program       *out_prog,
                                          JitBridgeError *out_err);
```

The bridge does NOT call `jit1_compile`. Caller chains:

```c
JitBridgeError berr;
JitBridgeReason rc = ndb_jit_bridge_translate(prog, n_words, &p, &berr);
if (rc != JIT_BRIDGE_OK) {
  /* log rejection reason; fall back to interpreter */
  return NULL;
}
Jit1Prog *jp = jit1_compile(arena, &p, NULL);
if (!jp) {
  /* admission walk caught something the bridge missed —
   * defense in depth */
  return NULL;
}
```

### 3.3 NDB → JIT opcode mapping

| NDB opcode | Our OpKind | Notes |
|---|---|---|
| `kOpLoadConst` (bigint) | `OP_LOAD_CONST_INT` | Reject non-bigint variants |
| `kOpLoadCol` (bigint, non-null) | `OP_LOAD_COL_INT` | Bridge consults column descriptor |
| `kOpMov` | `OP_MOV_INT_INT` | Both src and dst must be bigint |
| `kOpPlusBigint` | `OP_ADD_INT_INT` | Existing Phase 1 name |
| `kOpMinusBigint` | `OP_MINUS_INT_INT` (new) | Phase 4 |
| `kOpMulBigint` | `OP_MUL_INT_INT` (new) | Phase 4 |
| `kOpSumBigint` | `OP_SUM_BIGINT` | Phase 1 |
| `kOpSkip` | `OP_SKIP` | Phase 1 |
| program-end | `OP_EXIT` | NDB doesn't emit a kOpExit; bridge appends OP_EXIT when the word stream ends. |

Everything else (`kOpEmbeddedInterp`, `kOpDiv*`, `kOpMod`, all
double / max / min / count variants, nullable-aware arithmetic,
type-promoting variants, `kOpSetRegNull`, generic `kOpPlus` /
`kOpMul` / `kOpSum` / etc.) → `JIT_BRIDGE_UNSUPPORTED_OP`. That's
the vast majority of NDB's ~30-opcode set, so most aggregation
programs in the wild will fall back. Phase 5 expands the table.

**Note on the optimizer.** `PushdownInterpreter::OptimizeProgramBuffer`
already runs at proxy time and replaces generic `kOpPlus` with
`kOpPlusBigint` etc. when types are known. Phase 4's bridge runs
**after** this optimization, so the type-specialised variants are
what we expect to see.

### 3.4 Operand decoding

NDB's bytecode packs operands into successive `Uint32` words. Each
opcode has a known word-length. The bridge:

1. Reads one Uint32 → opcode + flags (top byte) + small operands
2. Reads N additional Uint32s based on opcode's word-length
3. Decodes register numbers, column ids, immediate values
4. Populates an `Op` in the output Program

Hand-built tables in the bridge map (kOp*, operand-layout) → (our
OpKind, our Op fields).

## 4. Admission walk extensions

Phase 3's `admit_program()` already handles range / branch /
unsupported-kind checks. Phase 4 adds nothing — the bridge filters
unsupported opcodes BEFORE `jit1_compile` ever sees the Program.
Admission walk at this point sees the (already-translated, already-
supported) `Program` and verifies its invariants exactly as today.

The reason for keeping admission narrowly scoped (kind range +
branch direction) is that it operates on the post-bridge Program,
where every kind is by construction one we have a stencil for. If a
future change introduces a new translatable opcode without a
stencil yet (Phase 5 in-flight situation), admission's existing
`g_stencils[kind].n_bytes == 0` check is the safety net.

## 5. Compile site — `DblqhProxy::execJOIN_AGG_SETUP_REQ`

### 5.1 Where it fits

The existing handler validates the SETUP request, allocates the
per-program SETUP record, and fans out to LDM workers. Phase 4
inserts a new step between validation and fan-out:

```cpp
// pseudo-code; actual edit lives in DblqhProxy.cpp
void DblqhProxy::execJOIN_AGG_SETUP_REQ(Signal *signal) {
  // ...existing validation + record allocation...

  JoinAggSetup *setup = /* allocated record */;
  setup->m_jit_prog = nullptr;   // default: interpreter path

  // [PHASE 4] Try JIT compile. Failure = continue with interpreter.
  if (m_jit_arena != nullptr) {
    Program p;
    JitBridgeError berr;
    JitBridgeReason brc = ndb_jit_bridge_translate(
        setup->m_prog_words, setup->m_prog_n_words, &p, &berr);
    if (brc == JIT_BRIDGE_OK) {
      Jit1Prog *jp = jit1_compile(m_jit_arena, &p, /*timing=*/nullptr);
      if (jp != nullptr) {
        setup->m_jit_prog = jp;
        // DEBUG_JIT log: "compiled program <id>, blob N bytes"
      } else {
        const Jit1AdmitError *err = jit1_last_admit_error();
        // DEBUG_JIT log: "admission reject reason=<r> pc=<p>"
      }
    } else {
      // DEBUG_JIT log: "bridge reject reason=<r> word=<w>"
    }
  }

  // ...existing fan-out to LDM workers, with setup->m_jit_prog
  //    included in the per-worker payload...
}
```

Lifetime: `m_jit_arena` (the proxy's `NdbJitArena*`) is allocated
at proxy construction, large enough for typical query workloads
(initial guess: 1 MB, tuned by Phase 4 measurements). It's freed at
proxy destruction. Compiled blobs accumulate in the arena across
the proxy's lifetime; release per program is a future Phase 5 add
(needed once the arena's high-water mark becomes a real concern,
not before).

### 5.2 New JoinAggregationState field

`JoinAggregationState.hpp` gains a single field:

```cpp
struct JoinAggSetup {
  // ...existing fields...
  Jit1Prog *m_jit_prog;   // nullptr = interpreter path
};
```

Carried to LDM workers in the SETUP fan-out signal — existing
infrastructure, just one more pointer.

### 5.3 Cross-thread visibility

Per plan §10.1: the proxy writes through the RW mapping, the arena's
`ndb_jit_arena_seal` flips RW→RX with the icache flush, then the
proxy's published `Jit1Prog*` is consumed by workers via the existing
SETUP signal flow. The signal flow already implies a release/acquire
fence per NDB's signal-block contract, so no additional barrier work
is needed in Phase 4. Linux ARM64's `membarrier(SYNC_CORE)` per plan
§10.1 is a Phase 5 / hardening item — current testJoinAgg coverage
runs on x86_64 + macOS aarch64 + Linux aarch64 and the existing
signal fences are sufficient for those environments.

## 6. Per-row dispatch — `JoinAggInterpreter::ProcessRec`

### 6.1 Init: cache the entry pointer

```cpp
bool JoinAggInterpreter::Init(const Uint32 *prog) {
  // ...existing init...

  // [PHASE 4] Pull the JIT entry pointer from the SETUP record.
  // The pointer was published by the proxy; we acquire-load via
  // the existing signal-receive sequence.
  m_jit_entry = (m_setup->m_jit_prog != nullptr)
                  ? jit1_entry(m_setup->m_jit_prog)
                  : nullptr;
  return true;
}
```

Stored as `JitEntry m_jit_entry;` in the `JoinAggInterpreter`
private state.

### 6.2 ProcessRec dispatch

```cpp
Int32 JoinAggInterpreter::ProcessRec(Dbtup *block_tup,
                                      Dbtup::KeyReqStruct *req_struct) {
  if (m_jit_entry != nullptr) {
    JitState s;
    dbtup_jit_glue_state_in(&s, this, block_tup, req_struct);
    m_jit_entry(&s);
    dbtup_jit_glue_state_out(&s, this);
    return /* normal status */;
  }
  // [existing interpreter dispatch loop unchanged] ...
}
```

The branch is per-program in semantic effect: `m_jit_entry` is set
once at Init and constant for the rest of the program's life. The
branch predictor folds it in two iterations. Per plan §10.2's note:
"the *semantic* commitment is per-program and is asserted by a unit
test that flips the entry pointer mid-program and confirms the
change is **not** picked up." Phase 4 lands the unit test alongside.

### 6.3 State glue — `DbtupJitGlue.{cpp,hpp}`

Two inline functions:

```cpp
inline void dbtup_jit_glue_state_in(JitState *s,
                                     JoinAggInterpreter *agg,
                                     Dbtup *block_tup,
                                     Dbtup::KeyReqStruct *req_struct) {
  // Copy register file: NDB Register[].value.val_int64 → JitState.regs_i64[]
  // Phase 4 admission guarantees: all registers are non-null bigint,
  // so we read .val_int64 unconditionally.
  for (int i = 0; i < BC_MAX_REGS; ++i) {
    s->regs_i64[i] = agg->m_registers[i].value.val_int64;
  }
  for (int i = 0; i < BC_MAX_ACCS; ++i) {
    s->acc_i64[i] = agg->m_agg_results[i].value.val_int64;
  }
  s->row_cols_i64 = /* translated attr buffer — see below */;
}

inline void dbtup_jit_glue_state_out(JitState *s,
                                      JoinAggInterpreter *agg) {
  // Write back accumulators only. Registers are per-row (cleared
  // each row anyway by JoinAggInterpreter), so no write-back.
  for (int i = 0; i < BC_MAX_ACCS; ++i) {
    agg->m_agg_results[i].value.val_int64 = s->acc_i64[i];
  }
}
```

The "translated attr buffer" is the hard part. Phase 4 punts:
allocate a `int64_t attr_int64_buf[BC_MAX_COLS]` in
`JoinAggInterpreter`, filled once per row from `m_attr_read_buf` by
an existing helper that already knows column types. Cost: 64 bytes
× 1 copy per row. Phase 5 either reuses NDB's attr buffer directly
(saves the copy) or accepts the cost — well below interp's per-row
cost.

The glue file is self-contained (one .cpp + .hpp) so when single-
table AggInterpreter wiring lands later, the same glue works after
templating on the interpreter type.

## 7. Tests

### 7.1 Unit test: per-program dispatch invariant

In a new `dbtup_jit_unit_test` binary (or an existing unit-test
target), test that flipping `m_jit_entry` mid-program does NOT
take effect on subsequent rows. This is the assertion plan §10.2
calls out: per-program, not per-row, dispatch.

### 7.2 MTR test: canary aggregation queries

`mysql-test/suite/ndb/t/rondb_jit_setup.test` (new):

```sql
CREATE TABLE t1 (id BIGINT PRIMARY KEY, c1 BIGINT, c2 BIGINT) ENGINE=NDB;
INSERT INTO t1 VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300);

# JIT off — interpreter path.
SET ndb_jit = 0;
SELECT SUM(c1) FROM t1;             # expect 60
SELECT SUM(c1+c2) FROM t1;          # expect 660
SELECT SUM(c1-c2) FROM t1;          # expect -540
SELECT SUM(c1*c2) FROM t1;          # expect 14000

# JIT on — JIT path. Same answers via JoinAggInterpreter.
SET ndb_jit = 1;
SELECT SUM(c1) FROM t1;
SELECT SUM(c1+c2) FROM t1;
SELECT SUM(c1-c2) FROM t1;
SELECT SUM(c1*c2) FROM t1;
```

The query shape must trigger JOIN_AGG_SETUP_REQ — i.e., RonSQL must
choose the join/CTE aggregation path even on a single-table query.
If RonSQL routes simple `SELECT SUM(c1) FROM t1` to AggInterpreter
(single-table) instead of JoinAggInterpreter, the canary needs a
slightly more elaborate query (e.g., trivial CTE wrapping) to force
the join path. This is decided at Day 3 by inspecting RonSQL's
planner output.

### 7.3 Fallback verification

A query that the bridge MUST reject (e.g., `SELECT SUM(c1) FROM t1
WHERE c1 > 50`) should still produce the correct result via
interpreter, with a `DEBUG_JIT`-logged rejection reason. Verifies
that fallback is graceful, not crashy.

## 8. Step-by-step task breakdown

**Day 1, AM (~3h):**
- Add `OP_MINUS_INT_INT` and `OP_MUL_INT_INT` to `bytecode1.h`
  (append-only after `OP_BRANCH_NE_INT_INT`).
- Add 2 new STENCILs to `stencils_src.c` (clones of ADD with `-` / `*`).
- Add 4 MAGIC_* + 6 symbol entries to `hole_kinds.h`.
- Add 4 entries to `audit_magics.c::kMagicToStencil[]`.
- Add 2 entries to `extract_stencils.c::kOpkindMap[]`.
- Run `regen-stencils` — audit must PASS for 27 magics across
  15 stencils.
- Extend `microbench_interp.c` + `bc_op_name()`.
- Run `extractor-tests`, `admission_tests`, `proto_microbench` —
  all PASS.

**Day 1, PM (~3h):**
- Write `ndb_jit_bridge.{c,h}` (~250 LOC).
  Bridge supports the 11 opcodes per §3.3. Returns
  `JitBridgeError` with a clean reason on rejection.
- Unit-test the bridge in isolation (~6 cases): each accept/reject
  path covered with hand-built `Uint32[]` programs. CMake target
  `ndb_jit_bridge_tests` parallel to `admission_tests`.

**Day 2, AM (~3h):**
- Look at `DblqhProxy.cpp::execJOIN_AGG_SETUP_REQ`. Identify the
  insertion point, the SETUP-record allocation site, the existing
  fan-out signal layout. Confirm `JoinAggInterpreter` is the
  consumer (no `AggInterpreter` path through this signal).
- Concrete edit: add `m_jit_arena` member to `DblqhProxy`,
  allocate in proxy constructor, free in destructor.

**Day 2, PM (~3h):**
- Add `Jit1Prog *m_jit_prog` to `JoinAggregationState`.
- Wire bridge + jit1_compile into `execJOIN_AGG_SETUP_REQ`.
- Add `DEBUG_JIT` macro + log lines.
- Build the proxy block; verify it compiles.

**Day 3, AM (~3h):**
- `DbtupJitGlue.{cpp,hpp}` with the `state_in` / `state_out`
  helpers.
- `JoinAggInterpreter::Init` reads `m_jit_entry`.
- `JoinAggInterpreter::ProcessRec` dispatches via `m_jit_entry`
  when non-null.
- Build the whole DBTUP block.

**Day 3, PM (~3h):**
- Write the canary MTR test (§7.2).
- Run it. Likely needs iteration on:
  - The attr-buffer translation (§6.3).
  - The signal-flow boundaries (proxy → worker visibility of
    `m_jit_prog`).
  - Forcing the canary query through the join/CTE path so it
    actually hits JoinAggInterpreter.
  - State accumulation across rows (acc[] should carry; regs[]
    should reset each row).

**Day 4, AM (~3h):**
- Whichever of the above broke on Day 3 PM, fix.
- Add the per-program-dispatch invariant unit test (§7.1).
- Add the fallback-verification MTR query (§7.3).

**Day 4, PM (~3h):**
- `phase_4_setup_integration.md` results doc.
- Mark Phase 4 shipped in `plan.md`.

**Total: 4-5 days**, matching plan.md §10's estimate. Critical-path
item is Day 3 PM — first run of the MTR canary — which is where any
signal-flow / state-translation surprise surfaces.

## 9. Verification checklist

Before declaring Phase 4 done:

- [ ] `regen-stencils` produces 15 stencils per arch (13 existing
      + 2 new); audit PASS for 27 magics.
- [ ] `extractor-tests`, `admission_tests`, `proto_microbench` all
      retain PASS.
- [ ] Bridge unit tests (~6 cases) PASS.
- [ ] Per-program-dispatch invariant unit test PASS.
- [ ] Canary MTR test PASS with both `SET ndb_jit = 0` and `= 1`
      paths producing identical results on JoinAggInterpreter.
- [ ] Fallback MTR test: WHERE-bearing query correctly falls back,
      with `DEBUG_JIT` log showing the reason.
- [ ] No new MTR failures across `testJoinAgg`, `testJoinAggSpj`,
      `testJoinAggNdbApi`.
- [ ] `bench_q12_dbtc` shows speedup over interpreted baseline
      (target: ≥ 1.3× end-to-end query) — informational only;
      Phase 4 doesn't gate on this, but record it.

## 10. Out of scope (explicit reminder)

- AggInterpreter (single-table scans) — different signal,
  separate integration; reuse Phase 4's bridge and glue when it
  lands.
- Embedded interpreter blocks (Phase 5).
- Cold-call infrastructure + helpers (Phase 5).
- All non-bigint types / nullable arithmetic (Phase 5).
- Division / modulo (Phase 5; needs cold-call NULL-fixup).
- GROUP BY / hashing logic (always C++ outside JIT).
- Performance gates on real queries (Phase 5).
- ARM64 `membarrier(SYNC_CORE)` hardening (Phase 5).

## 11. Risks / things that may surprise us

1. **Signal-flow opacity.** Wiring `m_jit_prog` through the SETUP
   fan-out signal may turn out to require more than a single field
   add — for example, if the existing signal layout is tightly
   packed and adding a pointer needs a new signal version.
   Mitigation: scout `JoinAggregationState.hpp` and the SETUP
   signal handler on Day 2 AM before writing wire-protocol changes.

2. **Per-row state translation cost.** The per-row `state_in` /
   `state_out` copy is ~64 bytes × 2 = 128 bytes per row. For
   aggregation with millions of rows, this might erode the JIT's
   per-row advantage. Mitigation: measure on Day 3 PM; if
   significant, the JIT can read directly from
   `agg->m_registers[...]` via a different addressing pattern
   (Phase 5 has a similar pivot for column-buffer access).

3. **kOpEmbeddedInterp prevalence in CTE / join queries.** If most
   real RonSQL-generated CTE/join aggregation programs use
   embedded blocks for nullability / filter logic, Phase 4's
   coverage will be near-zero in production. Mitigation:
   acceptable — Phase 4 is the integration proof, not the perf
   payoff. Phase 5 expands.

4. **Attr-buffer column translation.** NDB columns have many
   wire-formats (varbinary, decimal, fixed-size int packed into
   words). Phase 4's "non-null bigint columns" filter narrows
   this, but the attr-read path still needs to deliver an
   `int64_t[]` array. Mitigation: examine `m_attr_read_buf`'s
   existing decoders on Day 3; the translation is well-defined for
   bigint columns.

5. **Arena lifetime mismatch.** If the proxy's lifetime differs
   from what plan §10.1 assumes, the arena could be freed while
   workers still hold `Jit1Prog*` pointers. Mitigation: a
   defensive assert in `~DblqhProxy()` that `m_jit_arena`'s
   high-water mark equals zero at destruction time.

6. **Canary query routing.** A naive `SELECT SUM(c1) FROM t1`
   might route to AggInterpreter (single-table) and bypass
   JOIN_AGG_SETUP_REQ entirely, leaving the JIT path untested.
   Mitigation: write the canary as a CTE-wrapped query (e.g.,
   `WITH cte AS (SELECT ... FROM t1) SELECT SUM(c1) FROM cte`)
   that forces the join/CTE planner branch. Decide on Day 3 by
   inspecting RonSQL's chosen path.

## 12. What we learn from Phase 4

- Whether the bytecode-bridge design generalises — the table-driven
  mapping in §3.3 should remain a clean way to add new opcodes in
  Phase 5. Watch for special cases creeping in.
- Empirical state-translation cost (see Risk 2).
- Whether per-node compile-once-share-many actually delivers the
  expected zero-coordination on hot path (it should; Phase 4 is
  where we confirm).
- Coverage of real CTE/join aggregation programs once admission
  rejects embedded blocks — likely small, gives Phase 5 a target.
