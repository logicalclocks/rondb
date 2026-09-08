# RONDB-1056 Phase 4 — DBTUP thin-slice integration (rev 2)

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
- Aggregation programs may include `kOpEmbeddedInterp` calls into
  the embedded interpreter — Phase 4 admission rejects these,
  the program falls back to the interpreter for all rows.

**Rev 2 (this revision) flips Q1 from defer to land**: cold-call
infrastructure lives in Phase 4 because `kOpLoadCol` cannot be
inlined into stencil bytes — it requires a function call to NDB's
`readAttributes` / `readSingleAttribute`. That same mechanism
generalises to Phase 5's full helper inventory (Div NULL fixup,
StringSearch, EmbeddedInterp dispatch, etc.). Pre-fill outside the
JIT was considered and rejected as a band-aid that wouldn't
generalise.

## 1. Scope

**Decisions taken**:

- **(a) Cold-call infrastructure lands in Phase 4.** Adds
  `HK_COLDCALL` hole kind, a helper registry, extractor + engine
  support for the non-tail-call relocation pattern, and one
  initial helper (`ndb_jit_h_load_col`).
- **(a) Hot-arithmetic stencils for bigint Plus/Minus/Mul.**
  Already landed in Day 1 AM.
- **(b) State translation: non-null bigint registers only.** No
  type/null tracking in JitState; admission rejects nullable /
  non-int64 programs. Phase 5 expands.

**In scope:**

- **Bytecode bridge.** Translates NDB's `Uint32*` aggregation
  bytecode to our `Program/Op` struct. Recognises 11 supported
  opcodes; `kOpLoadCol` now maps to a new cold-call opcode
  `OP_LOAD_COL_NDB`. Everything unsupported (including
  `kOpEmbeddedInterp`) returns `JIT_BRIDGE_UNSUPPORTED_OP`.
- **Cold-call mechanism in the engine.**
  - New `HK_COLDCALL` HoleKind.
  - Extractor recognises the non-tail-call pattern (a regular
    `call rel32` / `bl imm26` against an extern symbol that's NOT
    the trailing `next` tail-call) as `HK_COLDCALL`.
  - Helper registry: a small `jit1_register_helper(name, fn)`
    API + `g_jit_helpers[]` static table. Engine resolves
    `HK_COLDCALL` holes by symbol name → function pointer →
    PC-relative displacement at compile time.
- **One cold-call stencil + helper**: `op_load_col_ndb` →
  `ndb_jit_h_load_col(JitState*, col_id, dst_reg)`. Helper
  bridges `JitState` ↔ NDB context (`Dbtup *block_tup`,
  `KeyReqStruct *req_struct`) and calls `block_tup->readAttributes`.
- **Per-node compile site** in `DblqhProxy::execJOIN_AGG_SETUP_REQ`.
  Existing fan-out plumbing distributes the resulting `Jit1Prog*`
  to every LDM worker's `JoinAggInterpreter`.
- **Per-program dispatch** in `JoinAggInterpreter::Init` /
  `ProcessRec`. Cached entry pointer; the dispatch branch is
  per-program in semantic effect.
- **State glue** in `DbtupJitGlue.{cpp,hpp}` — sets up `JitState`
  with a context pointer for the helper to find NDB context;
  copies registers / accumulators in and out per row.
- **Tests**: bridge unit tests extended for `OP_LOAD_COL_NDB`;
  per-program-dispatch invariant unit test; MTR canary running
  `SELECT SUM(c1)` etc. via JoinAggInterpreter with JIT on/off.

**Out of scope (deferred to Phase 5 unless noted):**

- **`AggInterpreter` (single-table scans).** Different setup
  signal. Same JIT machinery would apply; reuse Phase 4's bridge,
  cold-call mechanism, and helper when its own integration lands.
- **Embedded interpreter blocks** (`kOpEmbeddedInterp`).
- **Other cold-call helpers**: Div NULL fixup, StringSearch,
  BinarySearch, QSort, CompressNumArray. The mechanism is in place;
  Phase 5 populates the inventory.
- **All non-bigint types**: kOpLoadConst-double, kOpSumDouble, etc.
- **Nullable-aware arithmetic.**
- **Division / modulo** (kOpDiv*, kOpMod): need cold-call NULL-fixup
  helper from Phase 5's inventory.
- **GROUP BY / hashing logic**: GB hashing lives in the JoinAgg
  C++ layer outside the per-row JIT'd body. Phase 4 only JITs
  programs where `m_n_gb_cols == 0`.
- **Performance gates** on real queries — Phase 5+.

## 2. File layout

```
storage/ndb/src/kernel/blocks/dbtup/jit/
├── jit1.h                      (small extension: helper-registry API)
├── jit1.c                      (HK_COLDCALL patcher + helper resolution)
├── bytecode1.h                 (1 new OpKind: OP_LOAD_COL_NDB)
├── stencils_src.c              (1 new cold-call stencil: op_load_col_ndb)
├── hole_kinds.h                (1 new HoleKind: HK_COLDCALL; symbol entries
│                                for HOLE_LCN_COL / HOLE_LCN_DST / HELPER_LOAD_COL;
│                                magic-byte constants for aarch64 path)
├── stencils_x86_64.h           (regenerated)
├── stencils_arm64.h            (regenerated)
├── ndb_jit_bridge.{c,h}        (kOpLoadCol → OP_LOAD_COL_NDB)
└── extract_stencils/
    ├── extract_stencils.c      (recognise HK_COLDCALL relocation)
    └── audit_magics.c          (1 new MAGIC entry per arch hole)

storage/ndb/src/kernel/blocks/dbtup/
├── JoinAggInterpreter.{cpp,hpp} (Init reads JitEntry; ProcessRec
│                                 dispatches via cached entry_fn)
└── DbtupJitGlue.{cpp,hpp}      [NEW]
    C++ glue:
      - ndb_jit_h_load_col() — the cold-call helper.
      - jit_state_setup_per_row() — copy regs/accs in.
      - jit_state_writeback_per_row() — copy accs out.
      - dbtup_jit_register_helpers() — calls jit1_register_helper
        for every Phase 4 helper at engine init.

storage/ndb/src/kernel/blocks/dblqh/
├── DblqhProxy.{cpp,hpp}        (execJOIN_AGG_SETUP_REQ calls bridge
│                                + jit1_compile, stashes Jit1Prog*
│                                in the fan-out record)  [DONE Day 2 PM]
└── JoinAggregationState.hpp    (LeafProgram::m_jit_prog field)
                                                          [DONE Day 2 PM]
```

## 3. Cold-call mechanism

### 3.1 The pattern

A cold-call stencil emits a regular C++ function call (NOT a
tail-call), then continues with the standard `TAIL_NEXT(s)`:

```c
extern void ndb_jit_h_load_col(JitState *s,
                                uint32_t col_id, uint32_t dst_reg);

DECLARE_HOLE(LCN_COL);
DECLARE_HOLE(LCN_DST);
STENCIL op_load_col_ndb(JitState *s) {
  ndb_jit_h_load_col(s, HOLE(LCN_COL), HOLE(LCN_DST));
  TAIL_NEXT(s);
}
```

clang lowers this on x86_64 to something like:

```
mov rdi, r12                  ; preserve_none state (r12) -> regular ABI arg0 (rdi)
mov esi, HOLE_LCN_COL         ; col_id (relocation: R_X86_64_32)
mov edx, HOLE_LCN_DST         ; dst_reg (relocation: R_X86_64_32)
call ndb_jit_h_load_col        ; relocation: R_X86_64_PLT32 against extern symbol
                               ; HERE is the HK_COLDCALL hole (4 bytes of rel32)
; r12 is callee-saved across the call (regular ABI)
jmp next                       ; trailing tail-call to next stencil
                               ; (existing HK_BRANCH_FALL / strip-tail handling)
```

The 4-byte `rel32` displacement after the `e8 (call)` opcode is
the patch site for `HK_COLDCALL`.

On aarch64 the analogous shape:

```
mov x0, x20                    ; preserve_none state (x20) -> regular ABI x0
mov w1, HOLE(LCN_COL)          ; magic-byte chain
mov w2, HOLE(LCN_DST)          ; magic-byte chain
bl ndb_jit_h_load_col           ; R_AARCH64_CALL26
; x20 callee-saved -> still holds state after the call
b next                          ; trailing branch to next stencil
```

The `bl` instruction's `imm26` field is the patch site for
`HK_COLDCALL`. clang emits PIC-style `bl` with a relocation against
the extern symbol, identical in shape to a tail-call relocation —
the extractor distinguishes by **call type** (BL keeps return
address; B doesn't) on aarch64, or by **opcode** (`call rel32` is
0xE8; `jmp rel32` is 0xE9) on x86_64.

### 3.2 HK_COLDCALL HoleKind

```c
typedef enum {
  /* ...existing kinds... */
  HK_COLDCALL = 7,   /* helper-function call site */
} HoleKind;
```

The `Hole` struct gains no new fields — `byte_offset`, `kind`,
`width` are all that's needed. But the audit / engine need a way
to know **which** helper the hole targets. We use the same
symbol-name lookup pattern Phase 2 uses for HK_OP_*: the extractor
records the helper symbol name in the generated header alongside
the Hole, and the engine resolves at compile time via the helper
registry.

Concretely, the generated header gains:

```c
static const Hole holes_op_load_col_ndb[] = {
  { .byte_offset = 8,  .kind = HK_OP_C,    .width = 4 },  /* col_id */
  { .byte_offset = 13, .kind = HK_OP_A,    .width = 4 },  /* dst_reg */
  { .byte_offset = 21, .kind = HK_COLDCALL, .width = 4,
    .helper_name = "ndb_jit_h_load_col" },
};
```

`Hole` gains an optional `const char *helper_name` field, populated
by the extractor from the relocation's target-symbol name. Older
holes leave it nullptr.

### 3.3 Helper registry

`jit1.h` adds:

```c
typedef void (*JitHelperFn)(void);  /* opaque — caller casts */

/* Register a helper. Called once per helper at engine init from
 * the C++ glue layer (DbtupJitGlue::dbtup_jit_register_helpers).
 * Idempotent: re-registering the same name is a no-op. Returns
 * 0 on success, -1 on table overflow. */
int jit1_register_helper(const char *name, JitHelperFn fn);

/* Resolve a registered helper. Returns NULL if not registered;
 * compile fails cleanly. */
JitHelperFn jit1_lookup_helper(const char *name);
```

Implementation is a static `JitHelper g_helpers[16]` — small fixed
array, register-once-at-init pattern, no synchronisation needed
because registration happens before any compile.

### 3.4 Engine-side patching

When jit1_compile encounters a `HK_COLDCALL` hole:

```c
case HK_COLDCALL: {
  JitHelperFn helper = jit1_lookup_helper(hole->helper_name);
  if (helper == NULL) {
    /* Helper not registered — caller forgot dbtup_jit_register_helpers().
     * Fail the compile cleanly. */
    errno = ENOENT;
    return NULL;
  }
  uint32_t patch_site_off = this_off + hole->byte_offset;
  int64_t  byte_disp = (int64_t)(intptr_t)helper -
                        (int64_t)(uintptr_t)(blob_rw + patch_site_off);
  patch_branch_disp(blob_rw + patch_site_off, (int32_t)byte_disp);
  break;
}
```

`patch_branch_disp` is the same arch-aware helper Phase 1 uses for
HK_BRANCH_FALL / HK_BRANCH_TAKE — write a PC-relative displacement
to the patch site. On x86_64 the displacement is `target - patch_site - 4`;
on aarch64 it's `(target - patch_site) / 4` packed into imm26.

Range concern: x86_64 `call rel32` covers ±2GB; aarch64 `bl imm26`
covers ±128MB. In ndbmtd, the JIT arena and the `ndb_jit_h_load_col`
function are both in the same process address space. On Linux this
is typically well within range; if a future huge-binary case
overflows, the engine can fall back to indirect calls (`mov reg,
imm64; call reg`) — a Phase 5 hardening option.

### 3.5 Extractor changes

The extractor's existing relocation classification (in
`extract_one_x86` and `extract_one_arm64`) already distinguishes
**trailing** tail-calls (against `next`, stripped) from **branch**
PLT32/CALL26 (against `HOLE_*_TGT`, recorded as HK_BRANCH_TAKE).
HK_COLDCALL adds a third kind:

- **x86_64**: a `R_X86_64_PLT32` relocation against ANY extern
  function symbol that is **not** `next` and **not** a HOLE_*_TGT
  pattern. The extractor checks if the target symbol begins with
  `ndb_jit_h_` (the helper-naming convention) and records as
  `HK_COLDCALL` with the symbol name attached.

- **aarch64**: same logic with `R_AARCH64_CALL26` / `JUMP26`.
  Distinguishing `bl` (call) from `b` (branch) is done by the
  call type — `R_AARCH64_CALL26` is for `bl`, `R_AARCH64_JUMP26`
  is for `b`. Cold-call helpers use `bl` because they're regular
  function calls.

The extractor's helper-name table is implicit — any extern symbol
with `ndb_jit_h_` prefix is a helper. The audit cross-checks
against an explicit list to catch typos.

### 3.6 Hole struct extension

Currently:

```c
typedef struct {
  uint16_t byte_offset;
  uint8_t  kind;
  uint8_t  width;
} Hole;
```

Phase 4 extends to:

```c
typedef struct {
  uint16_t    byte_offset;
  uint8_t     kind;
  uint8_t     width;
  const char *helper_name;   /* nullptr unless kind == HK_COLDCALL */
} Hole;
```

Backward-compatible at the source level (older Hole initialisers
zero-init the new field). The generated headers' `holes_*[]`
arrays gain `.helper_name = nullptr,` for non-coldcall holes
(verbose but explicit).

## 4. NDB → JIT opcode mapping (revised)

| NDB opcode | Our OpKind | Notes |
|---|---|---|
| `kOpLoadConst` (bigint) | `OP_LOAD_CONST_INT` | Reject non-bigint variants |
| `kOpLoadCol` (bigint, non-null) | **`OP_LOAD_COL_NDB`** *(new)* | Cold-call to `ndb_jit_h_load_col`; carries the NDB col_id directly (op->c) |
| `kOpMov` | `OP_MOV_INT_INT` | Both src and dst must be bigint |
| `kOpPlusBigint` | `OP_ADD_INT_INT` | Existing Phase 1 name |
| `kOpMinusBigint` | `OP_MINUS_INT_INT` | Day 1 AM |
| `kOpMulBigint` | `OP_MUL_INT_INT` | Day 1 AM |
| `kOpSumBigint` | `OP_SUM_BIGINT` | Phase 1 |
| `kOpSkip` | `OP_SKIP` | Phase 1 |
| program-end | `OP_EXIT` | Bridge appends |

Microbench's existing `OP_LOAD_COL_INT` (reads `row_cols_i64[]`
flat array, pure stencil, no cold call) stays unchanged — Phase
1-3 microbench tests don't break.

## 5. Compile site & dispatch

Unchanged from rev 1. `DblqhProxy::execJOIN_AGG_SETUP_REQ` calls
the bridge + `jit1_compile` between `OptimizeProgramBuffer` and
`JoinAggInterpreter` allocation (Day 2 PM, already done).

`JoinAggInterpreter::ProcessRec` checks `m_n_gb_cols == 0` AND
`m_jit_entry != nullptr`; if both, takes the JIT path. Otherwise
falls through to the existing interpreter loop.

**State glue per row**:

```cpp
inline void dbtup_jit_glue_state_in(JitState *s,
                                     JoinAggInterpreter *agg,
                                     Dbtup *block_tup,
                                     Dbtup::KeyReqStruct *req_struct) {
  /* Helper finds NDB context via the JitState's ctx pointer. */
  s->ctx = (void *)&dbtup_jit_call_ctx{ agg, block_tup, req_struct };

  /* Reg file: int64 path only (admission already verified). */
  for (int i = 0; i < BC_MAX_REGS; ++i) {
    s->regs_i64[i] = agg->m_registers[i].value.val_int64;
  }
  for (int i = 0; i < BC_MAX_ACCS; ++i) {
    s->acc_i64[i] = agg->m_agg_results[i].value.val_int64;
  }
  /* row_cols_i64 is unused on the cold-call path — the helper
   * fills regs[] directly. */
}

inline void dbtup_jit_glue_state_out(JitState *s,
                                      JoinAggInterpreter *agg) {
  for (int i = 0; i < BC_MAX_ACCS; ++i) {
    agg->m_agg_results[i].value.val_int64 = s->acc_i64[i];
  }
}
```

`JitState` gains a `void *ctx` field — the helper interprets it as
`dbtup_jit_call_ctx*`. (Microbench keeps `ctx == nullptr`; its
helpers, if any, would test for null.)

**The helper**:

```cpp
extern "C" void ndb_jit_h_load_col(JitState *s,
                                    uint32_t col_id,
                                    uint32_t dst_reg) {
  auto *ctx = static_cast<dbtup_jit_call_ctx *>(s->ctx);
  Uint32 attr_id = col_id;
  Uint32 read_buf[2];   /* AttributeHeader + 1 word for bigint */
  int ret = ctx->block_tup->readAttributes(ctx->req_struct,
                                            &attr_id, 1,
                                            read_buf, 2);
  if (ret < 0) {
    /* Read error: leave the register at its prior value and
     * record the error somewhere the caller can see. Phase 4
     * panics; Phase 5 wires proper error propagation. */
    abort();
  }
  /* Decode int64 from read_buf[1] (skipping the AttributeHeader). */
  AttributeHeader *header =
      reinterpret_cast<AttributeHeader *>(&read_buf[0]);
  if (header->isNULL()) {
    /* Phase 4 admission was supposed to reject nullable cols.
     * If we got here, either admission has a bug or the column's
     * non-null guarantee was violated. Panic. */
    abort();
  }
  s->regs_i64[dst_reg] = sint8korr(reinterpret_cast<char *>(&read_buf[1]));
}
```

(NB: the actual `readAttributes` interface may need adjustment;
real implementation in DbtupJitGlue.cpp will track the precise NDB
API.)

## 6. Step-by-step task breakdown

**Days 1-2: DONE.**
- Day 1 AM: 2 new bigint stencils (Minus, Mul). ✓
- Day 1 PM: ndb_jit_bridge.{c,h} + 10-case bridge_tests. ✓
- Day 2 AM: m_jit_arena on DblqhProxy. ✓
- Day 2 PM: bridge + jit1_compile wired into execJOIN_AGG_SETUP_REQ;
  LeafProgram gains m_jit_prog/m_jit_entry; ndbmtd builds & links. ✓

**Day 3: Cold-call mechanism** (~5h, expanded from 3h).
- Add `HK_COLDCALL` to `hole_kinds.h`; extend `Hole` struct with
  `helper_name`; update existing `holes_*[]` initialisers.
- Add `op_load_col_ndb` stencil to `stencils_src.c`. Generate 2
  new MAGIC_*s (LCN_COL, LCN_DST). Add HOLE_* + MAGIC entries.
- Extend `extract_stencils.c` to recognise non-tail-call PLT32 /
  CALL26 against extern `ndb_jit_h_*` symbols as `HK_COLDCALL`.
  Emit `helper_name` in the generated header.
- Run `regen-stencils`; audit must PASS for 31 magics across 16
  stencils (15 existing + 1 new).
- Add `OP_LOAD_COL_NDB` OpKind to `bytecode1.h`.
- Update `extract_stencils.c::kOpkindMap[]` for the new opcode.
- Update `audit_magics.c::kMagicToStencil[]` for the new magics.
- Bridge: change `kOpLoadCol` mapping from `OP_LOAD_COL_INT` to
  `OP_LOAD_COL_NDB`. Bridge tests update accordingly.
- Engine: add `g_helpers[]` registry, `jit1_register_helper()`,
  `jit1_lookup_helper()` API. Add `HK_COLDCALL` case to the
  jit1_compile patcher.
- All four test binaries (extractor-tests, admission_tests,
  bridge_tests, proto_microbench) retain PASS.

**Day 4: NDB-side glue** (~4h).
- `DbtupJitGlue.{cpp,hpp}`: define `dbtup_jit_call_ctx`,
  implement `ndb_jit_h_load_col`, define
  `dbtup_jit_register_helpers()`.
- `JitState` gains `void *ctx` field — propagate to all stencil
  source consumers (most will keep ctx unused).
- `JoinAggInterpreter::Init` reads `m_jit_entry` from
  `state->m_leaf_programs[0]` (resolved earlier in Day 2 PM
  compile path).
- `JoinAggInterpreter::ProcessRec` adds the JIT dispatch branch
  guarded by `m_n_gb_cols == 0 && m_jit_entry != nullptr`.
- `Dbtup::initBlock` (or wherever block init happens) calls
  `dbtup_jit_register_helpers()` once.
- Build ndbmtd; verify clean.

**Day 5: Canary MTR test + iteration** (~5h).
- Write `mysql-test/suite/ndb/t/rondb_jit_setup.test`:
  - CTE-wrapped query that forces JoinAggInterpreter:
    `WITH cte AS (SELECT id FROM t1) SELECT SUM(c1+c2-c3*c4) FROM cte`.
  - Run with `SET ndb_jit = 0` and `SET ndb_jit = 1`; both
    must produce identical results.
  - Plus a fallback query that admission rejects (e.g., one with
    a WHERE clause that triggers kOpEmbeddedInterp) — also must
    match interpreter result.
- Run it. Iterate on:
  - State glue boundaries (regs/acc copy correctness).
  - Helper context lifetime (`block_tup` / `req_struct` must be
    valid for the helper call duration).
  - Per-program-dispatch invariant.

**Day 6: Polish + results doc** (~3h).
- Per-program-dispatch invariant unit test (asserts flipping
  `m_jit_entry` mid-program is NOT picked up).
- `phase_4_setup_integration.md` results doc.
- Mark Phase 4 shipped in `plan.md`.

**Total: 5-6 days** (up from 4-5 in rev 1, the +1 day pays for
landing cold-call infrastructure).

## 7. Verification checklist

- [ ] `regen-stencils` produces 16 stencils per arch (15 + 1 new
      cold-call); audit PASS for 31 magics.
- [ ] `extractor-tests` retains 11/11 PASS, with new T12-ish case
      verifying HK_COLDCALL classification on hand-built input.
- [ ] `admission_tests` retains 12/12 PASS.
- [ ] `bridge_tests` retains 10/10 PASS, with `kOpLoadCol` test
      updated to expect `OP_LOAD_COL_NDB` instead of
      `OP_LOAD_COL_INT`.
- [ ] `proto_microbench` retains PASS — it doesn't use NDB cold-
      call, but verifies the engine's HK_COLDCALL patcher hasn't
      regressed the non-coldcall path.
- [ ] Per-program-dispatch invariant unit test PASS.
- [ ] Canary MTR test PASS with `SET ndb_jit = 0` and `= 1`
      paths producing identical results on JoinAggInterpreter.
- [ ] Fallback MTR test: WHERE-bearing or other unsupported
      query correctly falls back, with `DEBUG_JIT` log.
- [ ] No new MTR failures across `testJoinAgg`,
      `testJoinAggSpj`, `testJoinAggNdbApi`.
- [ ] `bench_q12_dbtc` informational speedup (target ≥ 1.3×) —
      not a Phase 4 gate.

## 8. Out of scope (explicit reminder)

- AggInterpreter (single-table scans).
- Embedded interpreter blocks.
- Other cold-call helpers (Div NULL fixup, StringSearch, etc.).
- Non-bigint types / nullable arithmetic.
- Division / modulo (cold-call mechanism is in place but no helper).
- GROUP BY / hashing.
- Performance gates.
- ARM64 `membarrier(SYNC_CORE)` hardening.
- Multi-leaf programs (m_num_leaves > 1).

## 9. Risks / things that may surprise us

1. **Cold-call call-site bytes vary across clang patch levels.**
   The 5-byte `call rel32` is stable on x86_64; aarch64's `bl`
   encoding is stable. But the surrounding `mov` instructions
   for argument setup could shift. Mitigation: extractor uses
   relocations, not byte-pattern matching, so it's robust.

2. **±2 GB / ±128 MB call range.** If the helper resolves to an
   address out of PC-relative range, the patcher fails the
   compile. Mitigation: log clearly and fall back to interpreter;
   Phase 5 adds indirect-call fallback.

3. **Helper-registry init order.** If `dbtup_jit_register_helpers`
   isn't called before the first compile, lookups return NULL and
   compile fails. Mitigation: register at `Dbtup::initBlock`-
   level, before any aggregation can land.

4. **JitState.ctx clobbering across stencils.** `ctx` is set by
   the glue once per row; stencils never write to it. If a future
   stencil mistakenly uses `ctx` for arithmetic, every cold-call
   on the row breaks. Mitigation: make `ctx` const-by-convention
   in the C source and audit.

5. **Helper signature drift.** If `ndb_jit_h_load_col` changes
   signature without updating the stencil, the call ABI breaks
   silently. Mitigation: helper signature is part of the
   stencils_src.c contract; Phase 5 collision audit can be
   extended to track helper signatures.

6. **Scout issues from rev 1.** Signal-flow opacity, attr-buffer
   translation, query routing — all still apply. Mitigation:
   same as before; surface on Day 5 PM canary run.

## 10. What we learn from Phase 4

- Whether the cold-call mechanism generalises cleanly. By
  Phase 5's helper inventory expansion, we'll know if the
  current `Hole.helper_name` design needs to grow.
- Empirical attr-buffer translation cost (one helper call per
  column-read in the bytecode).
- Whether `JitState.ctx` is the right shape for the C++/C
  bridge, or whether something more typed is needed.
- Coverage of real CTE/join aggregation programs once admission
  rejects embedded blocks — likely small, gives Phase 5 a target.
