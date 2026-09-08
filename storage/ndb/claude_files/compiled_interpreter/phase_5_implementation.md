# RONDB-1056 Phase 5 — Hot-opcode lowering, full set + embedded normal-interp

This document is the implementation plan for Phase 5, the
stencil-set expansion. Phase 4 proved the architecture with one
cold-call helper and three arithmetic stencils. Phase 5 fills
in the matrix:

- ~75 stencils across op-family × type × nullable-mode
- Type-state lattice that picks the right specialization at each
  pc (Phase I.18 typed registers integration)
- Cold-call helper inventory (Div NULL fixup, StringSearch, etc.)
- Embedded normal-interpreter blocks lowered in place
- A **new cold-call-with-branch pattern** for the ATTR/MEM/LINKED
  branch family — `BRANCH_ATTR_OP_ARG`, `BRANCH_MEM_OP_ARG`,
  `BRANCH_MEM_OP_ARG_INLINE_TYPE`, `BRANCH_LINKED_*_NULL`, and
  siblings — which are too complex to inline as hot stencils

## 1. Scope

**Decisions taken** (after the three Phase 5 design questions):

- **Q1 (a) Full forward dataflow with join meets** for the type
  lattice. ~150 LOC. Required for sound null-tracking across
  branches; without it every branch join would degrade to the
  conservative-nullable stencil.
- **Q2 (a) Div NULL fixup first, then linear inventory order.**
  Div is the simple warmup that exercises the helper-inventory
  pattern in real production code. Linear ordering means the
  biggest coverage unlock — embedded-interp dispatch with cold-
  call branches — arrives later in Phase 5, not at the start.
- **Q3 (b) Defer the codegen generator.** Hand-write per
  plan.md §11. Switch trigger: ≥2 days in and <30% pass tests,
  OR same fix lands in ≥4 stencils. The generator helps the
  source but not the per-stencil bytes review (which happens
  one stencil at a time regardless).

**In scope:**

- Type-state lattice + stencil picker (`jit_stencil_picker.c`)
- Full hot-opcode stencil set (~50-60 stencils across
  arithmetic, logical, aggregate, branch families)
- Cold-call helper inventory: Div NULL fixup, StringSearch,
  BinarySearch, QSort, CompressNumArray, accumulator-slot
  resolver, plus the **branch-family helpers** for ATTR / MEM /
  LINKED (~9 helpers covering ~15 cold-call branch stencils)
- New 3-hole pattern: `HK_COLDCALL + HK_BRANCH_TAKE +
  HK_BRANCH_FALL` — helper returns int (0 = fall through,
  non-zero = take branch); stencil dispatches via two musttail
  paths
- Embedded normal-interpreter block admission + lowering
- Coverage assertion: any opcode without a stencil triggers
  fallback; CI canary asserts the canonical hot set is 100%
  covered
- DUMP 2370 toggle for differential-test-mode JIT off (so MTR
  tests can run the same query through both paths)

**Out of scope (deferred to Phase 6+):**

- Cross-branch always-JIT test integration (Phase 6)
- Scan-filter path (Phase 7)
- aarch64 perf optimization (Phase 5+; Phase 5 is correctness)
- Multi-leaf programs (Phase 6+; Phase 4's narrow-leaf check
  stays)

## 2. File layout

```
storage/ndb/src/kernel/blocks/dbtup/jit/
├── jit_stencil_picker.{c,h}    [NEW]
│   Type-state lattice over typed registers + per-pc stencil
│   selection. Called from jit1_compile (replacing the current
│   one-stencil-per-OpKind lookup).
├── stencils_src.c              (~75 stencils, append-only by family)
├── hole_kinds.h                (new MAGIC_* per stencil, no new HK kinds)
├── bytecode1.h                 (~50 new OpKind values, append-only)
├── ndb_jit_bridge.{c,h}        (extends the supported-opcode table)
├── stencils_x86_64.h           (regenerated)
├── stencils_arm64.h            (regenerated)
└── extract_stencils/
    └── audit_magics.c          (kMagicToStencil[] grows with stencils)

storage/ndb/src/kernel/blocks/dbtup/
└── DbtupJitGlue.{cpp,hpp}      Helper inventory grows: ~9 new helpers.
                                dbtup_jit_register_helpers() registers all.

storage/ndb/src/kernel/blocks/dblqh/
└── DblqhProxy.cpp              DUMP 2370 toggle handler for force-interp /
                                force-jit testing.
```

## 3. Type-state lattice + stencil picker

### 3.1 Lattice shape

Per-register state: `{type, nullable}` where:
- `type` ∈ {UNKNOWN, BIGINT, BIGUNSIGNED, DOUBLE, NULL_TYPED}
- `nullable` ∈ {YES, NO, MAYBE}

Lattice ordering (top to bottom):
```
            UNKNOWN
        /     |     |     \
   BIGINT  BIGUINT  DOUBLE  NULL_TYPED
        \     |     |     /
            BOTTOM (unused)
```

Meet operation (used at branch joins):
- `T ⊓ T = T` for any T
- `T ⊓ U = T ⊓ U = UNKNOWN` (different concrete types)
- `non-null ⊓ nullable = nullable`
- `non-null ⊓ MAYBE = MAYBE`

### 3.2 Forward dataflow

Per-program, the picker walks bytecode in pc order:
1. At pc=0: all registers UNKNOWN, MAYBE.
2. For each opcode, compute the **post-state** from the pre-state
   and the opcode's effect (e.g., `kOpLoadConstBigint rN, val`
   sets `regs[N] = (BIGINT, NO)`; `kOpSetRegNull rN` sets
   `regs[N] = (... , YES)`).
3. For branches, the target's pre-state is the current post-state
   (forward propagation). At join points, take the meet.

A second pass resolves joins until fixpoint (typically 1-2
iterations for forward-only programs).

### 3.3 Stencil selection

For each pc, the picker queries:
1. The post-`OptimizeProgramBuffer` opcode kind (e.g.,
   kOpSumBigint).
2. The pre-state's nullability of input registers.
3. Looks up the matching stencil in a per-opcode-family table.

Example: `kOpPlusBigint(dst, lhs, rhs)` with `lhs` MAYBE-nullable
and `rhs` NO-nullable picks `op_plus_bigint_lhs_nullable_stencil`,
not the all-non-null variant.

### 3.4 Falling back

If the picker can't find a matching stencil (e.g., post-state
inference unexpectedly produces UNKNOWN type for a numeric op),
it returns the most conservative variant or fails admission.
Fallback to interpreter.

## 4. Cold-call branch pattern (new in Phase 5)

The Phase 4 cold-call pattern (one-call-then-tail-to-next)
doesn't fit branches because the branch decision comes from the
helper's runtime computation. Phase 5 introduces a **3-hole
pattern**:

```c
extern int ndb_jit_h_branch_attr_op_arg(JitState *s,
                                         uint32_t cond,
                                         uint32_t attr_id,
                                         uint32_t arg_offset);

DECLARE_HOLE(BAOA_COND);
DECLARE_HOLE(BAOA_ATTR);
DECLARE_HOLE(BAOA_OFFSET);
extern __attribute__((preserve_none)) void HOLE_BAOA_TGT(JitState *);
STENCIL op_branch_attr_op_arg(JitState *s) {
  if (ndb_jit_h_branch_attr_op_arg(s,
                                   (uint32_t)HOLE(BAOA_COND),
                                   (uint32_t)HOLE(BAOA_ATTR),
                                   (uint32_t)HOLE(BAOA_OFFSET))) {
    [[clang::musttail]] return HOLE_BAOA_TGT(s);   /* taken */
  }
  TAIL_NEXT(s);                                    /* fall through */
}
```

Stencil holes:
- 3× `HK_OP_*` for the inline operands (cond / attr_id /
  arg_offset)
- 1× `HK_COLDCALL` for the helper call site
- 1× `HK_BRANCH_TAKE` for the taken-branch target
- 1× `HK_BRANCH_FALL` (implicit — trailing tail-call to next,
  stripped by extractor — unchanged from Phase 4)

The helper returns `int`: 0 = fall through, non-zero = take
branch. Stencil's `if` expands to `cmp / jne taken_label /
jmp fall_label` on x86_64 — clang generates this idiomatically.

### 4.1 Variable-length inline arg data

`BRANCH_ATTR_OP_ARG`'s inline constant can be up to 65k bytes
(VARCHAR). The bridge has two strategies:

**Strategy A (chosen): pass an offset into the original NDB
bytecode buffer.** The original `m_prog` buffer lives in the
SETUP record for the program's lifetime. The bridge records the
word-offset of the inline arg in our `Op.imm` field (currently
unused for branches). The helper reads:

```cpp
extern "C" int ndb_jit_h_branch_attr_op_arg(JitState *s,
                                             uint32_t cond,
                                             uint32_t attr_id,
                                             uint32_t arg_word_offset) {
  auto *ctx = static_cast<dbtup_jit_call_ctx *>(s->ctx);
  const Uint32 *arg_data = ctx->prog_buf + arg_word_offset;
  /* ... readAttributes on attr_id, decode, compare to arg_data ... */
}
```

This requires `dbtup_jit_call_ctx` to gain a `prog_buf` pointer
to the original SETUP-record bytecode buffer. Lifetime is fine —
the SETUP record outlives every ProcessRec call.

**Strategy B (rejected): copy inline args into a per-program
arena.** More memory, more bookkeeping, no upside.

### 4.2 The 9 cold-call branch stencils

| Stencil | Helper | Notes |
|---|---|---|
| `op_branch_attr_op_arg` | `ndb_jit_h_branch_attr_op_arg` | Reads via readAttributes; compares to inline arg |
| `op_branch_attr_eq_null` | `ndb_jit_h_branch_attr_null` | Helper takes a "want_null" bit; same helper for both eq/ne via inversion |
| `op_branch_attr_ne_null` | (same helper, eq/ne flag) | |
| `op_branch_attr_op_param` | `ndb_jit_h_branch_attr_op_param` | Compares attr to a parameter stored in NDB context |
| `op_branch_attr_op_attr` | `ndb_jit_h_branch_attr_op_attr` | Compares two attributes |
| `op_branch_mem_op_arg` | `ndb_jit_h_branch_mem_op_arg` | Reads from cheapMemory[0] (set by READ_LINKED_TO_MEM) |
| `op_branch_mem_op_arg_inline_type` | `ndb_jit_h_branch_mem_op_arg_inline_type` | Like above, type info inline |
| `op_branch_linked_eq_null` | `ndb_jit_h_branch_linked_null` | Null-check on cheapMemory[0] |
| `op_branch_linked_ne_null` | (same helper, eq/ne flag) | |

## 5. Helper inventory (Phase 5 order)

| Order | Helper | Day |
|---|---|---|
| 1 | `ndb_jit_h_div_null_fixup` | 2 |
| 2 | `ndb_jit_h_branch_attr_op_arg` | 6 |
| 3 | `ndb_jit_h_branch_attr_null` | 6 |
| 4 | `ndb_jit_h_branch_attr_op_param` | 6 |
| 5 | `ndb_jit_h_branch_attr_op_attr` | 6 |
| 6 | `ndb_jit_h_branch_mem_op_arg` | 6 |
| 7 | `ndb_jit_h_branch_mem_op_arg_inline_type` | 6 |
| 8 | `ndb_jit_h_branch_linked_null` | 6 |
| 9 | `ndb_jit_h_string_search` | 7 |
| 10 | `ndb_jit_h_binary_search` | 7 |
| 11 | `ndb_jit_h_qsort` | 7 |
| 12 | `ndb_jit_h_compress_num_array` | 7 |

Phase 4's `ndb_jit_h_load_col` keeps its existing slot.

## 6. Stencil families

Hot-lowered (~55 stencils):

| Family | Variants | Count |
|---|---|---|
| LoadConst* | int / uint / double | 4 |
| LoadCol* (existing OP_LOAD_COL_NDB extends) | int / uint / double, signed/unsigned | 6 |
| Mov | typed src→dst (4 type pairs) | 4 |
| Plus / Minus / Mul | int-int, uint-uint, mixed-promoted, double-double, plus nullable variants | 30 |
| Sum / Max / Min / Count | bigint, double, int-promoted | 12 |
| Skip / SetRegNull / Exit | unspecialized | 3 (Exit exists; Skip/SetRegNull new) |
| `BRANCH_*_REG_REG` (eq/ne/lt/le/gt/ge × bigint/double/uint, plus REG_CONST overflow variants) | per-comparator × 3 types × 2 (reg-reg vs reg-const) | ~36 |
| `BRANCH_REG_EQ_NULL` / `NE_NULL` | unspecialized | 2 |

Cold-call (~15 stencils):

| Family | Variants | Count |
|---|---|---|
| Div / DivInt / Mod | bigint signed/unsigned, double | 6 |
| Branch ATTR / MEM / LINKED | per §4.2 | 9 |
| StringSearch / BinarySearch / QSort / CompressNumArray | unspecialized | 4 |

Total: ~70-75 stencils, matching plan.md §11's estimate.

## 7. Embedded-interpreter admission + lowering

### 7.1 Admission walk extension

Phase 4's `admit_program()` rejects `kOpEmbeddedInterp`
unconditionally. Phase 5 recurses into embedded blocks:

```
if op is kOpEmbeddedInterp:
  emb_offset = op.embedded_block_offset
  emb_len = op.embedded_block_len
  walk_embedded_block(prog->ops + emb_offset, emb_len)
    same rules apply: every BRANCH_* must be forward;
    CALL/RETURN inside the embedded block also rejects
    the WHOLE program.
```

This is `plan.md` §9.1's commented-out clause activated.

### 7.2 Lowering in place

Phase 4 admits programs as a flat sequence of Op records. Phase 5
embedded-interp blocks are admitted as a **nested sub-program**.
The bridge translates them in place: each embedded opcode becomes
an Op in our flat sequence, with branch targets adjusted to use
our forward-branch infrastructure (Phase 3's HK_BRANCH_TAKE
queue).

The embedded program's `WRITE_INTERPRETER_OUTPUT` (skip-to-end)
becomes a forward branch to the next-after-block pc — clean
mapping onto the existing fixup queue.

### 7.3 Cheap memory

`READ_LINKED_TO_MEM` opcode loads a linked column into
`cheapMemory[0]` — a small area in the interpreter state. The
JIT'd code doesn't manage cheapMemory directly; the cold-call
helpers for `BRANCH_MEM_OP_ARG` and `BRANCH_LINKED_*_NULL` access
it via `ctx->agg->cheapMemory[]` (after a friend-access
wrapper, similar to Phase 4's `readAttributeForJit`).

`READ_LINKED_TO_MEM` itself is a cold-call:
`ndb_jit_h_read_linked_to_mem(s, position)` → fills cheapMemory.

## 8. Step-by-step task breakdown

**Day 1 (~5h)**: Type-state lattice + stencil picker
- `jit_stencil_picker.{c,h}` with the lattice + meet logic
- Forward-dataflow walk (1-2 fixpoint iterations)
- Lattice unit tests (~10 cases: typed propagation, branch joins,
  nullable upgrades)
- Refactor `jit1_compile` to call the picker for stencil
  selection (replacing direct `g_stencils[kind]` lookup)
- Existing tests must still PASS — single-stencil-per-OpKind
  programs trivially pick the same stencil

**Day 2 (~4h)**: Div NULL fixup + 6 divide-family stencils
- `ndb_jit_h_div_null_fixup` helper in DbtupJitGlue
- 6 new stencils (Div/DivInt/Mod × signed/unsigned bigint;
  Div/DivInt for double could come Day 7 with the rest of the
  type matrix)
- Bridge admits kOpDivIntBigint, kOpMod, etc.
- Sample SQL: `SELECT SUM(a / b) FROM t` (where b can be 0)

**Day 3 (~5h)**: Type-specialized arithmetic + LoadConst/LoadCol
- 14 new stencils: typed Plus/Minus/Mul, typed LoadConst,
  typed LoadCol
- Each is a small clone of Phase 4's bigint variants

**Day 4 (~5h)**: Aggregation specialization
- 12 new stencils: Sum/Max/Min/Count × types
- Sum is most important (existing); Max/Min/Count are new

**Day 5 (~5h)**: Embedded-interp admission + hot-lowered
BRANCH_*_REG_*
- Recursive admission walk for embedded blocks
- ~18 new stencils for register-register and register-const
  comparison branches
- 2 BRANCH_REG_*_NULL stencils
- Bridge handles the embedded-block opcode space

**Day 6 (~6h)**: Cold-call branch pattern + ATTR/MEM/LINKED
- New 3-hole stencil pattern (HK_COLDCALL + HK_BRANCH_TAKE +
  HK_BRANCH_FALL)
- 7 new helpers (one per branch family, with eq/ne sharing)
- 9 new cold-call branch stencils
- `dbtup_jit_call_ctx` gains `prog_buf` pointer (to the SETUP
  record's original NDB bytecode for inline arg reads)
- This is the big day — most likely to overrun if surprises

**Day 7 (~4h)**: Other cold-call helpers
- StringSearch, BinarySearch, QSort, CompressNumArray helpers +
  stencils — mostly mechanical wrappers

**Day 8 (~4h)**: Polish + tests + results doc
- Differential MTR tests via `cte_filter_phase_*`
- DUMP 2370 toggle for force-interp/force-jit testing
- `phase_5_full_set.md` results doc
- Mark Phase 5 shipped in plan.md

**Total: 6-8 days** matching plan.md §11. Day 6 is the
critical-path item with most uncertainty (cold-call branches +
ctx changes); the rest is volume work.

## 9. Test approach

- **Lattice unit tests**: ~10 cases covering propagation,
  branch joins, nullable upgrades, conservative meets.
- **Stencil unit tests**: every new family gets at least 1
  case in either `bridge_tests.c`, `coldcall_tests.c`, or a
  new `lattice_tests.c`. ~30 new cases.
- **Microbench extensions**: per-family stencil exercise
  programs.
- **Differential MTR**: every `cte_filter_phase_*` test runs
  via DUMP 2370 force-interp baseline + force-jit, results
  must match.
- **Phase 4 regression**: 5/5 + 12/12 + 10/10 + 11/11 unit-test
  PASS counts retained.
- **Phase 4 MTR canary**: still passes; expanded to also
  exercise the now-admitted WHERE clauses.

## 10. Verification checklist

- [ ] Lattice unit tests PASS.
- [ ] All Phase 4 unit-test binaries PASS unchanged.
- [ ] `regen-stencils` produces ~70-75 stencils per arch; magic-
      byte audit PASS for all magics.
- [ ] Embedded-interp blocks are admitted (no longer reject by
      default).
- [ ] DUMP 2370 toggle exists and works.
- [ ] `cte_filter_phase_*` MTR tests PASS with JIT on AND off,
      identical results (differential).
- [ ] No new MTR failures across `testJoinAgg`,
      `testJoinAggSpj`, `testJoinAggNdbApi`.
- [ ] Phase 4 canary (`rondb_jit_canary`) still PASSes; Q4
      (WHERE-bearing query) now goes through JIT.
- [ ] `bench_q9_dbtc` and `bench_q12_dbtc` show measurable
      end-to-end speedup over interpreter baseline.

## 11. Out of scope (explicit reminder)

- Cross-branch always-JIT test integration (Phase 6).
- Scan-filter path (Phase 7).
- aarch64 perf (Phase 5+).
- Multi-leaf programs (Phase 6+).
- Persistent on-disk cache of compiled programs (post-v1).
- Type-prop refinements beyond the lattice's basic shape
  (e.g., constant-fold across branches) — out of v1 scope.

## 12. Risks / things that may surprise us

1. **Day 6 cold-call branch bridge is the big risk.** Variable-
   length inline arg handling (Strategy A: pointer into original
   bytecode) might surface lifetime issues we haven't
   anticipated. Mitigation: `dbtup_jit_call_ctx.prog_buf` is set
   per-row from the LeafProgram's m_agg_program (alive for the
   program's lifetime); same lifetime as the JIT'd blob. Should
   be sound.

2. **Type lattice fixpoint blowup.** Phase 5 forward-only
   programs converge in 1-2 iterations, but pathological cases
   (highly branchy embedded blocks) could hit more. Mitigation:
   bound iterations at 8; if not converged, fall back to
   conservative-everywhere.

3. **The same fix lands in ≥4 stencils.** Plan.md §11's
   generator-switch trigger fires. Mitigation: pause hand-writing,
   set up the generator (½–1 day), regenerate the affected
   family, continue.

4. **Embedded-interp branches with backward jumps.** Admission
   rejects backward branches by design, but if RonSQL's planner
   ever emits backward-branching embedded interp blocks (e.g., a
   loop), admission rejects the whole program. Mitigation:
   acceptable for Phase 5 — fallback to interpreter for those.

5. **Variable-length arg data crossing a Uint32-word boundary
   incorrectly.** The bridge needs to compute the byte-offset
   into `m_agg_program` precisely. Mitigation: pull the
   existing AggInterpreter's decoding logic (it already handles
   this) into a shared helper rather than re-implementing.

6. **DUMP 2370 implementation complexity.** Needs a session-
   level state mechanism, signal handler, fan-out to LDM workers
   (so the toggle takes effect on existing programs). Mitigation:
   start with a global flag (single-process) and refine later if
   needed.

## 13. What we learn from Phase 5

- Whether the type lattice generalises cleanly. If Phase 5
  needs special-case handling for many opcodes, that's a sign
  the lattice is too simple; if it doesn't, post-Phase-5 work
  on type prop becomes optional.
- Whether the cold-call branch pattern (3-hole, helper-returns-
  bool) generalises beyond the ATTR/MEM/LINKED families.
  Phase 6+ may want it for other helper-driven control flow.
- Empirical embedded-interp coverage in real RonSQL queries.
  Phase 5 unblocks the dominant WHERE / CASE patterns; whether
  those represent 90% or 50% of real-world queries determines
  Phase 6's priority.
- Generator switch-over experience (or not) — informs whether
  future stencil-set additions should default to generated.
