# RONDB-1056 Phase 3 — admission walk + branch coverage

This document is the implementation plan for Phase 3, mirroring
`phase_2_implementation.md`. Phase 3 generalises the JIT engine's
forward-branch handling and tightens the program-admission policy
that gates JIT eligibility — both prerequisites for Phase 4's DBTUP
integration. The Phase 1+2 fixup-queue infrastructure is already in
place; Phase 3 broadens the bytecode it consumes and codifies the
pre-emission check that says "this program is JIT-able, that one
isn't".

## 1. Scope

In:

- A single forward-only **admission walk** at the top of
  `jit1_compile`. One linear pass; no allocation; produces a
  yes/no verdict + reason. Rejected programs cost ~hundreds of ns
  and never touch the arena.
- Five **new branch opcodes**: `branch_eq_int_int`,
  `branch_le_int_int`, `branch_gt_int_int`, `branch_ge_int_int`,
  `branch_ne_int_int` — siblings of Phase 1's existing
  `branch_lt_int_int`. Same hole shape; same engine codepath; one
  comparison flag's worth of difference.
- A **forked microbench program** with two or three forward
  branches creating multiple control-flow segments. Differential
  test (JIT aggregate == interp aggregate) on the new program.
- **Admission unit tests**: table-driven, covering accept and
  reject paths.
- **Engine plumbing** for the new opcodes: extended interpreter,
  extended bytecode generator, regen-stencils handles the new
  stencils automatically (extractor + audit are stencil-shape-
  agnostic).

Out (deferred to later phases):

- **Type-state at branch joins** (plan §9.2) — needs the type-prop
  infrastructure that lands in Phase 5. Phase 3 leaves
  `JitState.regs_i64[]` untyped from the engine's perspective.
- **Short-branch encoding** (Jcc rel8 on x86_64; B.cond inline on
  aarch64). Both arches' current branch stencils already use full-
  width displacements that fit any program we'll see in Phase 3-4.
  Worth revisiting in Phase 5 if profiling shows branch-density
  hot spots.
- **Per-fixup `width` / `arch_kind` fields** in the Fixup struct
  (plan §9.2). Useful only when short-branch encoding lands;
  current arch-implicit width (4 bytes x86_64, 4 bytes aarch64) is
  fine until then.
- **Embedded-normal-interpreter blocks** (plan §9.1's recursive
  walk_embedded_block clause). Phase 5 territory; the admission
  walk's structure has the hook but the case stays inactive.

## 2. File layout

Net new files: 1. Net edits to existing files: 7 (most small).

```
storage/ndb/src/kernel/blocks/dbtup/jit/
├── jit1.h              (small extension: error-reason enum + sidecar)
├── jit1.c              (admission walk + branch opcode dispatch)
├── bytecode1.h         (5 new OpKind values)
├── stencils_src.c      (5 new stencils)
├── hole_kinds.h        (10 new MAGIC_*; 6 new HK-symbol entries; 5 new
│                        HOLE_*_TGT extern function names)
├── stencils_x86_64.h   (regenerated: 6 new stencils' bytes + holes)
├── stencils_arm64.h    (regenerated)
└── extract_stencils/
    └── audit_magics.c  (kMagicToStencil[] gains 10 entries)

storage/ndb/test/jit_proto/
├── microbench_program.c   (forked program builder)
├── microbench_interp.c    (5 new branch opcode handlers)
├── proto_microbench.c     (test runs the forked program too)
└── admission_tests.c      [NEW]
```

The new admission_tests binary is a small C program (~150 LOC) that
links `libndb_jit1.a` and exercises `jit1_compile` with hand-built
programs whose admission verdict is known. Wired as a CMake target
parallel to `proto_microbench`.

## 3. Admission walk

### 3.1 Signature and sidecar

The walk is invoked unconditionally as the first thing
`jit1_compile` does, before the `pc_byte_off[]` allocation. On
reject, jit1_compile sets a thread-local "last admission error"
sidecar with a reason code and offending PC, then returns NULL with
errno=EINVAL.

```c
/* In jit1.h: */
typedef enum {
  JIT_ADMIT_OK             = 0,
  JIT_ADMIT_BACKWARD_BRANCH = 1,  /* op->c <= pc */
  JIT_ADMIT_BRANCH_OOR      = 2,  /* op->c >= n_ops */
  JIT_ADMIT_UNSUPPORTED_OP  = 3,  /* opcode kind has no stencil */
  JIT_ADMIT_INVALID_KIND    = 4,  /* op kind out of OpKind range */
  JIT_ADMIT_EMPTY_PROG      = 5,  /* n_ops == 0 */
  JIT_ADMIT_PROG_TOO_LARGE  = 6,  /* n_ops > BC_MAX_OPS */
} Jit1AdmitReason;

typedef struct {
  Jit1AdmitReason reason;
  uint16_t        offending_pc;     /* if reason refers to one */
  uint16_t        offending_target; /* if reason involves a branch */
  uint8_t         offending_kind;
} Jit1AdmitError;

/* Read-only after jit1_compile returns. Caller may read this once
 * to log the reason; not protected by a lock — single-threaded
 * compile per node per RonSQL §10.1. */
const Jit1AdmitError *jit1_last_admit_error(void);
```

### 3.2 Validation rules

```
admit_program(prog):
  if prog->n_ops == 0:                  reject EMPTY_PROG
  if prog->n_ops > BC_MAX_OPS:          reject PROG_TOO_LARGE
  for pc in 0..n_ops-1:
    op = prog->ops[pc]
    if op.kind == 0 or op.kind > OP_KIND_MAX:
      reject INVALID_KIND  (offending_pc, offending_kind)
    if g_stencils[op.kind].n_bytes == 0:
      reject UNSUPPORTED_OP  (offending_pc, offending_kind)
    if op_is_branch(op.kind):
      target = op.c
      if target <= pc:
        reject BACKWARD_BRANCH  (offending_pc, offending_target)
      if target >= prog->n_ops:
        reject BRANCH_OOR  (offending_pc, offending_target)
  accept
```

`op_is_branch(kind)` is a small static predicate:
`kind in {OP_BRANCH_LT_INT_INT, OP_BRANCH_LE_INT_INT,
OP_BRANCH_EQ_INT_INT, OP_BRANCH_GT_INT_INT, OP_BRANCH_GE_INT_INT,
OP_BRANCH_NE_INT_INT}`.

### 3.3 Where the validation lives

The existing `jit1_compile` body has scattered validation: range
checks on `prog->n_ops`, kind validation in pass 1, branch-direction
check in pass 2. Phase 3 consolidates these into `admit_program()`
called once at function entry. The pass-2 branch-direction check
becomes a defense-in-depth assertion (`abort()` in debug builds, no-
op in release) — admission already proved branches are forward, so
hitting that assertion means a Phase 3 invariant got broken
upstream.

### 3.4 Defensive vs strict — which kinds are unsupported

Phase 3's `UNSUPPORTED_OP` reason fires when `g_stencils[kind].n_bytes
== 0`. Today that happens for: any `kind > OP_KIND_MAX` (caught
earlier by INVALID_KIND), or any kind whose stencil entry was
explicitly skipped by the extractor. With the current set + the 5
new branches, every kind 1..OP_KIND_MAX has a stencil, so
UNSUPPORTED_OP is dormant — but the path is wired so future
"some-opcodes-have-no-stencil" scenarios (Phase 5's cold-call
opcodes per plan §10.3) reuse it.

## 4. New branch opcodes

### 4.1 Stencil source pattern

Each new stencil is a near-clone of `op_branch_lt_int_int` with the
comparison flipped. C source pattern:

```c
DECLARE_HOLE(BLE_A);  DECLARE_HOLE(BLE_B);
extern __attribute__((preserve_none)) void HOLE_BLE_TGT(JitState *);
STENCIL op_branch_le_int_int(JitState *s) {
  if (s->regs_i64[HOLE(BLE_A)] <= s->regs_i64[HOLE(BLE_B)]) {
    [[clang::musttail]] return HOLE_BLE_TGT(s);
  }
  TAIL_NEXT(s);
}
```

Same TAIL_KEEP_ALL extraction policy as `op_branch_lt_int_int`.
Same hole table (HK_OP_A, HK_OP_B for operands; HK_BRANCH_FALL for
the fall-through `b next`; HK_BRANCH_TAKE for the taken
`b HOLE_*_TGT`). The extractor handles them identically — no
extractor changes required.

### 4.2 Magic constants

Each new branch opcode declares two operand magics in `hole_kinds.h`,
generated the same way as Phase 2's (high-entropy SHA-256-derived):

```c
#define MAGIC_BLE_A  0x...ull
#define MAGIC_BLE_B  0x...ull
#define MAGIC_BEQ_A  0x...ull
#define MAGIC_BEQ_B  0x...ull
... (10 total, two per new opcode) ...
```

Plus 6 new HK-symbol entries (one per HOLE_<NAME>_A/B operand) and
5 new HOLE_<NAME>_TGT entries in `kHoleSymbolTable[]`.

Each new MAGIC entry also lands in `audit_magics.c`'s
`kMagicToStencil[]` mapping the magic's name to its declaring stencil.

### 4.3 OpKind enum extension

`bytecode1.h` gains 5 new OpKind values inserted after
OP_BRANCH_LT_INT_INT, before OP_SKIP. Append-only — existing kind
values don't shift, so no Phase 1 stencil bytes change.

```c
typedef enum {
  ...
  OP_BRANCH_LT_INT_INT,
  OP_BRANCH_LE_INT_INT,   /* new */
  OP_BRANCH_EQ_INT_INT,   /* new */
  OP_BRANCH_GT_INT_INT,   /* new */
  OP_BRANCH_GE_INT_INT,   /* new */
  OP_BRANCH_NE_INT_INT,   /* new */
  OP_SKIP,
  OP_EXIT,
  OP_KIND_MAX = OP_EXIT,
} OpKind;
```

The extractor's local `kOpkindMap[]` (in `extract_stencils.c`) gains
5 new entries mapping the symbol name to the OpKind id.

### 4.4 Interpreter coverage

`microbench_interp.c` gets 5 new case arms in the dispatch switch.
Each one mirrors the existing `OP_BRANCH_LT_INT_INT` arm with the
comparison flipped — straightforward `<= == > >= !=` substitutions.

## 5. Forked microbench program

`microbench_program.c::mb_build_30op_program()` already produces a
linear program with one tail branch. Phase 3 adds a sibling builder
`mb_build_forked_program()` that produces ~30 ops including:

- One `OP_BRANCH_EQ_INT_INT` early in the program (skipping a small
  block when a flag column is zero)
- One `OP_BRANCH_GT_INT_INT` in the middle (skipping a sum-fold when
  a column exceeds a threshold)
- Mixed sums + accumulator updates so the resulting aggregate is
  meaningfully data-dependent on which branches are taken

The microbench runs both programs back-to-back; differential test
asserts `interp == jit` on each. Per-arch verdict thresholds stay
the same — Phase 3 doesn't tighten or relax them, just verifies the
new control-flow patterns don't regress.

## 6. Tests

### 6.1 Admission unit tests

`storage/ndb/test/jit_proto/admission_tests.c` — links
`libndb_jit1.a`, exercises `jit1_compile` against hand-built programs.

| Case | Expected verdict | Reason expected |
|---|---|---|
| 30-op linear (no branches) | accept | — |
| 30-op + 1 forward branch | accept | — |
| forked 30-op (2 forward branches) | accept | — |
| empty (n_ops=0) | reject | EMPTY_PROG |
| oversized (n_ops > BC_MAX_OPS) | reject | PROG_TOO_LARGE |
| op.kind = 0 | reject | INVALID_KIND |
| op.kind = OP_KIND_MAX + 1 | reject | INVALID_KIND |
| backward branch (op.c == pc) | reject | BACKWARD_BRANCH |
| backward branch (op.c < pc) | reject | BACKWARD_BRANCH |
| out-of-range branch (op.c == n_ops) | reject | BRANCH_OOR |
| out-of-range branch (op.c > n_ops) | reject | BRANCH_OOR |
| accept then reject — same arena, no leak | both verdicts correct | — |

The arena-no-leak case verifies that a rejected program's
`jit1_compile` call leaves the arena's high-water mark unchanged
(no allocation happens before admission).

### 6.2 Differential microbench

The existing microbench's "interp == jit" check is the differential
test. Phase 3 just adds the forked program to the suite so both
programs are differentially-tested per run.

### 6.3 Regen + audit pass with new magics

After adding 10 new MAGIC_* entries to `hole_kinds.h` and 5 new
stencils to `stencils_src.c`:

1. `cmake --build . --target regen-stencils` regenerates both
   headers. Audit must PASS — every new MAGIC_* found exactly once
   in its declaring stencil.
2. `cmake --build . --target extractor-tests` retains 11/11 PASS;
   T6/T7 (idempotency) confirm the regenerated bytes are stable.
3. `drift_check.sh` retains PASS after committing the regenerated
   headers.

If any audit step fails, most likely cause: a new magic
accidentally collides with bytes in another stencil, OR an existing
stencil's bytes shifted because of the OpKind enum reordering (we
explicitly avoid this by appending new kinds, but worth verifying).

## 7. Step-by-step task breakdown

**Day 1, AM (~3h):**
- Add `Jit1AdmitReason` enum + `Jit1AdmitError` struct + sidecar to
  `jit1.h`.
- Implement `admit_program()` in `jit1.c`. Hoist existing
  validation. Replace pass-2 branch-direction check with an
  assertion.
- Smoke-test: existing microbench must still PASS unchanged on
  both arches. Existing extractor-tests must still PASS.

**Day 1, PM (~3h):**
- New file `admission_tests.c`. ~12 cases per §6.1. CMake target
  `ndb_jit1_admission_tests` parallel to `proto_microbench`.
- All cases pass.

**Day 2, AM (~3h):**
- Extend `bytecode1.h` with 5 new OpKind values + a `bc_op_name()`
  table entry per new opcode + `bc_op_is_branch(kind)` helper.
- Extend `microbench_interp.c` with 5 new case arms. Verify the
  interpreter alone is correct via the existing single-program
  test.

**Day 2, PM (~4h):**
- Generate 10 new high-entropy magics (sha256-derived script in
  the README's troubleshooting section).
- Add MAGIC_* + HOLE_* entries to `hole_kinds.h`. Append-only.
- Add 5 new STENCIL definitions to `stencils_src.c`.
- Extend `extract_stencils.c::kOpkindMap[]` with 5 entries.
- Extend `audit_magics.c::kMagicToStencil[]` with 10 entries.
- Run `regen-stencils` — audit must PASS.
- Run `extractor-tests` — 11/11 must still PASS.
- `drift_check.sh` after committing the regenerated headers.

**Day 3, AM (~3h):**
- New `mb_build_forked_program()` in `microbench_program.c`.
- Microbench runs both programs (linear + forked); differential
  check on each.
- Run microbench on x86_64 + aarch64; verify aggregate matches
  interpreter on both programs.

**Day 3, PM (~2h):**
- `phase_3_branches.md` results doc (template in §8).
- Mark Phase 3 shipped in `plan.md`.

**Total:** ~3 days, in line with plan.md §9's 2-3 day estimate.
The contingency is small because the engine's Pass 1/Pass 2
structure is unchanged — Phase 3 broadens what flows through it
without touching the core.

## 8. `phase_3_branches.md` template

Written at the very end. Sections:

- Outcome on x86_64 + aarch64 (admission accept/reject behaviour;
  forked program differential PASS).
- The 5 new opcodes + their stencil sizes per arch.
- Admission-walk perf (cycles to admit a 30-op program; cost of a
  rejection vs accept).
- Toolchain quirks discovered (likely: none; magic-byte audit and
  regen flow are mature from Phase 2).
- Forward pointers to Phase 4 (DBTUP integration) and Phase 5
  (type-state at joins, embedded normal-interp blocks).

## 9. Verification checklist

Before declaring Phase 3 done:

- [ ] `admit_program()` rejects every malformed-program case in §6.1
      with the documented reason code.
- [ ] Rejected programs leave the arena's high-water mark unchanged.
- [ ] `regen-stencils` produces 6 branch stencils per arch + 7 other
      stencils; audit PASS for all 23 magics (13 existing + 10 new).
- [ ] `extractor-tests` retains 11/11 PASS.
- [ ] `drift_check.sh` PASS after committing regenerated headers.
- [ ] Microbench's linear program: aggregate matches interp on both
      arches. Speedup unchanged from Phase 2 within variance.
- [ ] Microbench's forked program: aggregate matches interp on both
      arches.
- [ ] `admission_tests` binary runs all 12+ cases to PASS.
- [ ] No external dependencies added.

## 10. Out of scope (explicit reminder)

Don't drift into these:

- Type-state vector at branch joins (Phase 5).
- Cold-call stencils for STRING_SEARCH / BINARY_SEARCH / QSORT /
  COMPRESS_NUM_ARRAY (Phase 5; admission-walk has the hook but
  the case stays inactive in Phase 3).
- Short-branch encoding (Phase 5+).
- Embedded normal-interp block recursive walk (Phase 5).
- DBTUP integration (Phase 4).
- Stencil set expansion beyond branches (Phase 5).
- aarch64 perf tuning (Phase 5).

## 11. Risks / things that may surprise us

1. **Magic collision with existing stencil bytes.** A new MAGIC_*
   value might accidentally appear as raw instruction bytes in
   another stencil, tripping the audit's "0× elsewhere" assertion.
   Probability per magic: ~2^-64 with random magics × 8 stencils ×
   ~80 bytes each ≈ vanishingly small. Mitigation: regenerate the
   colliding magic from a different SHA-256 salt.

2. **Clang folding chains across the new magics.** Phase 2 hit this
   when neighbouring magics differed by a small arithmetic constant.
   Mitigation: high-entropy SHA-256-derived magics, same as Phase 2.
   The audit catches the symptom (1× becomes 0×).

3. **OpKind enum reordering accidentally shifts existing values.**
   If someone inserts the new kinds BEFORE existing ones rather
   than after, every existing stencil's index changes and
   `g_stencils[kind]` lookups break. Mitigation: append-only
   discipline; CI has `extractor-tests` T6/T7 as the catch-net.

4. **Extractor's `kOpkindMap[]` gets out of sync with `OpKind`
   enum.** The extractor uses string→enum-id matching; if a new
   stencil function exists but no `kOpkindMap` entry, extraction
   would emit it but the engine wouldn't dispatch to it (or vice
   versa). Mitigation: a Day 2 PM checklist item to update both
   together.

## 12. What we learn from Phase 3

- Whether the admission-walk design generalises cleanly to
  arbitrary opcodes — i.e., whether each future opcode can be
  classified by simple predicates (`is_branch`, `is_call`, etc.)
  rather than per-opcode special cases.
- Whether the magic-byte tooling holds up when the stencil set
  grows ~50% (8 → 13 stencils). Likely yes, but the first time we
  exercise it at this scale.
- An early data point on what real-world programs look like —
  the forked microbench is a step closer to RonSQL planner output
  than the seeded 30-op linear program was.
