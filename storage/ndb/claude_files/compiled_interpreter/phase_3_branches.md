# RONDB-1056 Phase 3 — admission walk + branch coverage

**Status: shipped.** Phase 3 generalises the JIT engine's forward-
branch handling and codifies the program-admission policy that gates
JIT eligibility — both prerequisites for Phase 4's DBTUP integration.
Branch: `RONDB-1056-compiled-interpreter`.

## Outcome

All Phase 3 verification gates clear on x86_64 (Linux) and aarch64
(macOS):

| Gate | x86_64 (debug_build) | aarch64 (debug_build) |
|------|---|---|
| `regen-stencils` 23/23 magic-byte audits PASS | ✓ | ✓ |
| `extractor-tests` 11/11 PASS | ✓ | ✓ |
| `admission_tests` 12/12 PASS | ✓ | ✓ |
| 30-op program JIT == interp aggregate | ✓ 12517131 | ✓ 12517131 |
| 30-op microbench thresholds (Phase 1 budget) | ✓ PASS | ✓ PASS |
| Forked-program JIT == interp aggregate | ✓ 7250995 | ✓ 7250995 |

The forked program (19 ops, three forward branches using BRANCH_LE,
BRANCH_EQ, BRANCH_GT) drives the multi-fixup queue: branches at
pcs 3, 4, 6 with targets at pcs ~8, ~11, ~14 mean three fixups are
pending in the queue simultaneously between pc=6 and pc=8. The
engine handles it without any structural change — Phase 1's
`Fixup` array + drain-on-emit machinery scales naturally.

## What shipped

**New opcodes** (5 sibling branches, all int_int comparison form):

| OpKind | Comparison | Stencil size (x86_64 / aarch64) | Magics |
|---|---|---|---|
| OP_BRANCH_LT_INT_INT | `<`  | 29 / 80 (existing — Phase 1) | BLT_A, BLT_B |
| OP_BRANCH_LE_INT_INT | `<=` | 29 / 80 | BLE_A, BLE_B |
| OP_BRANCH_EQ_INT_INT | `==` | 29 / 80 | BEQ_A, BEQ_B |
| OP_BRANCH_GT_INT_INT | `>`  | 29 / 80 | BGT_A, BGT_B |
| OP_BRANCH_GE_INT_INT | `>=` | 29 / 80 | BGE_A, BGE_B |
| OP_BRANCH_NE_INT_INT | `!=` | 29 / 80 | BNE_A, BNE_B |

All five new stencils are identical in shape and size to the existing
BLT — clang only varies the Jcc condition byte on x86_64 and the
B.cond condition field on aarch64.

**New host tooling**:

- `admission_tests` binary (12 cases) — table-driven verification
  of `jit1_compile`'s admission walk.

**Modified core**:

- `jit1.c` — `admit_program()` runs as the first thing in
  `jit1_compile`, before any allocation. Hoisted the existing
  scattered validation into a single linear pre-pass; downgraded
  the redundant pass-1 / pass-2 inline checks to `assert()`. The
  `_Thread_local Jit1AdmitError g_last_admit` sidecar exposes the
  failing PC, target, and kind for caller logging.
- `jit1.h` — `Jit1AdmitReason` enum (7 codes), `Jit1AdmitError`
  struct, `jit1_last_admit_error()` accessor.
- `bytecode1.h` — 5 new OpKind values inserted before SKIP/EXIT
  (kind values 7-11; SKIP/EXIT shifted to 12/13). Append-only —
  existing kinds keep their numbers, so prior stencil bytes don't
  shift. Added `bc_op_is_branch(kind)` shared predicate.
- `stencils_src.c` — 5 new STENCIL definitions, each cloning BLT
  with the comparison flipped.
- `hole_kinds.h` — 10 new MAGIC_* constants (sha256-derived from
  `RONDB-1056-Phase3-magic-v1|<name>`), 15 new `kHoleSymbolTable`
  entries, 10 new `kHoleMagicTable` entries.
- `extract_stencils.c` — 5 new `kOpkindMap[]` entries. Plus a bug
  fix: branch-target classification was hardcoded to
  `strcmp(tname, "HOLE_BLT_TGT")`; replaced with
  `lookup_hole_kind(tname) == HK_BRANCH_TAKE` so any future branch
  opcode lights up via the symbol table.
- `audit_magics.c` — 10 new `kMagicToStencil[]` entries.
- `microbench_program.{h,c}` — added `mb_build_forked_program()`.
  `bc_op_name()` extended.
- `microbench_interp.c` — 5 new dispatch arms.
- `proto_microbench.c` — runs the forked program differentially
  after the existing 30-op block.
- `stencils_x86_64.h` + `stencils_arm64.h` — regenerated; 13
  stencils each.

## Admission-walk cost

Hoisting validation into a single pre-pass added a linear walk over
`prog->ops`. Cost is invisible at the resolution of the existing
microbench's pass1 timing (~125 ns on aarch64, ~50 ns on x86_64 —
which now includes the admission walk's work). The walk is the only
work `jit1_compile` does on a rejected program, so a malformed
bytecode costs **just** that few hundred ns, not the full
arena-alloc + emit + seal pipeline.

## Toolchain quirks discovered

- **Hardcoded `HOLE_BLT_TGT` in the extractor.** Phase 1 wrote the
  branch-target classification with a literal strcmp (only one
  branch opcode existed). Phase 3 needed five more, so the hardcode
  rejected them as "unexpected reloc type" on x86_64. Fix: consult
  `lookup_hole_kind(tname)` and check for `HK_BRANCH_TAKE`. The
  symbol table in `hole_kinds.h` is the source of truth, and any
  future branch opcode automatically lights up by adding a
  `HOLE_<NAME>_TGT` entry there.

  This was the only surprise. Everything else was append-only.

- **All four 16-bit slots of every magic must be nonzero.**
  Reaffirmed during magic generation: SHA-256-derived values gave
  10/10 magics with all four slots nonzero on the first try — clang
  emits a full movz+3×movk chain for each, no shortened forms that
  would trip the extractor.

## Forward pointers

- **Phase 4** wires `JOIN_AGG_SETUP_REQ` in `DblqhProxy` to call
  `jit1_compile` once per data node, then fans the resulting
  `Jit1Prog*` out to LDM workers. Phase 3's admission walk and
  expanded branch coverage are exactly what makes that integration
  defensible: the proxy can call `jit1_compile`, see a clean
  reject reason via `jit1_last_admit_error()`, and fall back to
  the interpreter without having to second-guess the bytecode.

- **Phase 5** generalises further: type-state at branch joins,
  cold-call stencils for STRING_SEARCH / BINARY_SEARCH / QSORT /
  COMPRESS_NUM_ARRAY, and the operand-folding work that closes the
  aarch64 perf gap discussed in `phase_2_extractor.md`. Phase 3's
  Hole-kind framework + admission-walk hook accommodate all three
  without redesign.

## Verification checklist

| Item | Status |
|------|--------|
| `admit_program()` rejects every malformed-program case in §6.1 with the documented reason code | ✓ — admission_tests T4-T11 |
| Rejected programs leave the arena's high-water mark unchanged | ✓ — admission_tests T12 |
| `regen-stencils` produces 6 branch stencils per arch + 7 other stencils; audit PASS for all 23 magics | ✓ |
| `extractor-tests` retains 11/11 PASS | ✓ |
| `drift_check.sh` PASS after committing regenerated headers | (run after this commit) |
| Microbench's linear program: aggregate matches interp on both arches | ✓ |
| Microbench's forked program: aggregate matches interp on both arches | ✓ |
| `admission_tests` binary runs all 12 cases to PASS | ✓ |
| No external dependencies added | ✓ |
