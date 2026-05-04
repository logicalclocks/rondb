# Phase 1 — Implementation Plan (RONDB-1056)

Status: ready to implement. Branch: `RONDB-1056-compiled-interpreter`.
Companion to `plan.md` §7.

## 1. Scope

Validate copy-and-patch on a representative aggregation program. The
benchmark *is* the test; bit-identical aggregation results between
interpreter and JIT'd path is the correctness check. Three numbers
gate continuation to Phase 2:

- Compile time on Linux x86_64 **< 5 µs** for the 30-op program.
- JIT'd code **≥ 2× faster** than interpreted on the per-row hot path.
- Break-even row count **< 5,000**.

Phase 1 is intentionally narrow: 8 stencils, x86_64 first, hand-
extracted bytes from local `objdump`. ARM64 cross-extraction lives
in Phase 2's extractor, not here.

## 2. File layout

```
storage/ndb/src/kernel/blocks/dbtup/jit/
  jit_arena.h              # already exists (Phase 0)
  jit_arena.c              # already exists
  jit_arena_*.inc.c        # already exists
  CMakeLists.txt           # already exists; will gain a phase 1 lib

  stencils_src.c           # NEW: 8 hand-written stencil functions
  stencils_x86_64.h        # NEW: hand-extracted bytes for x86_64
  stencils_proto.h         # NEW: layout types shared across header + bench
  jit1.h                   # NEW: tiny Phase-1 engine API (compile + run)
  jit1.c                   # NEW: copy-and-patch over a bytecode program
  bytecode1.h              # NEW: opcode enum + Program struct

storage/ndb/test/jit_proto/
  proto_smoke.c            # already exists
  proto_hardened.c         # already exists
  proto_icache_bench.c     # already exists
  proto_microbench.c       # NEW: aggregation-shaped bench
  microbench_interp.c      # NEW: stripped-down interpreter dispatch
  microbench_program.c     # NEW: builds the 30-op test program
  CMakeLists.txt           # already exists; will gain proto_microbench

storage/ndb/claude_files/compiled_interpreter/
  phase_1_implementation.md  # this file
  phase_1_microbench.md      # written at end of Phase 1: stencils, bytes, numbers, verdict
```

Five new C files in `jit/`, three in `jit_proto/`. Total new code:
~600–800 LOC of dense but mechanical work.

## 3. The 8 stencils

Each stencil is a small C function that takes the JIT execution state
and tail-calls `next` after doing one opcode's worth of work. The
"holes" (immediate operands) are emitted as references to extern
placeholder symbols, which clang lowers to PC-relative or absolute
relocations we can patch at JIT time. Phase 1 hole-finding is by
relocation only — magic-byte fallback (per `plan.md` §8 Phase 2) is
not needed for this set, because every operand fits in a 32- or 64-
bit relocatable.

### 3.1 Calling convention

```c
typedef struct JitState JitState;
typedef void (*StencilFn)(JitState *);

#define STENCIL static __attribute__((preserve_none, noinline)) void
#define TAIL_NEXT(state) [[clang::musttail]] return next(state)

extern void next(JitState *);   /* relocation-patched per stencil */
```

`preserve_none` lets clang skip caller-saved register save/restore at
the boundary; combined with `musttail` it produces a small body that
ends in an unconditional `jmp` (x86_64) / `b` (ARM64) to `next`. The
extractor (Phase 2) strips that final `jmp`/`b` so the stencil's last
real instruction flows directly into the next stencil's first
instruction. Phase 1 strips it by hand.

### 3.2 The state struct

```c
typedef struct JitState {
    /* general-purpose register file. 8 i64 + 8 u64 + 8 f64 is
     * generous for the 30-op program; right-size in Phase 5. */
    int64_t   regs_i64[8];
    uint64_t  regs_u64[8];
    double    regs_f64[8];

    /* row pointer — opaque, indexed by the LoadCol stencil via
     * a column descriptor table emitted at compile time. */
    const int64_t *row_cols_i64;     /* width 8: cols 0..7 of i64 */

    /* aggregator slots (one per Sum/Max/Min/Count/...). 4 is plenty
     * for the 30-op program. */
    int64_t   acc_i64[4];

    /* program-level scratch: branch-skip return address, set when
     * an op_skip is emitted. The compiled blob terminates a row by
     * tail-calling this address rather than running off the end. */
    void     *row_end_addr;
} JitState;
```

State lives on the stack of the per-row entry in Phase 1 (no shared
state across rows except `acc_i64`, which the row entry threads in).
Phase 4 replaces this with the real `AggInterpreter` state.

### 3.3 Stencil bodies

```c
/* op_load_const_int — load a baked-in i64 immediate into regs_i64[dst]. */
extern uint64_t HOLE_LCI_DST;     /* hole: register index, fits in 8 bits but
                                   * we encode as i64 to get a relocation */
extern uint64_t HOLE_LCI_VAL;     /* hole: the constant value */
STENCIL op_load_const_int(JitState *s) {
    s->regs_i64[(uint64_t)&HOLE_LCI_DST] = (int64_t)(uint64_t)&HOLE_LCI_VAL;
    TAIL_NEXT(s);
}

/* op_load_col_int — load row column[col_idx] into regs_i64[dst]. */
extern uint64_t HOLE_LRC_DST;
extern uint64_t HOLE_LRC_COL;
STENCIL op_load_col_int(JitState *s) {
    s->regs_i64[(uint64_t)&HOLE_LRC_DST] =
        s->row_cols_i64[(uint64_t)&HOLE_LRC_COL];
    TAIL_NEXT(s);
}

/* op_mov_int_int — regs_i64[dst] = regs_i64[src]. */
extern uint64_t HOLE_MV_DST;
extern uint64_t HOLE_MV_SRC;
STENCIL op_mov_int_int(JitState *s) {
    s->regs_i64[(uint64_t)&HOLE_MV_DST] = s->regs_i64[(uint64_t)&HOLE_MV_SRC];
    TAIL_NEXT(s);
}

/* op_add_int_int — regs_i64[dst] = regs_i64[a] + regs_i64[b]. */
extern uint64_t HOLE_ADD_DST;
extern uint64_t HOLE_ADD_A;
extern uint64_t HOLE_ADD_B;
STENCIL op_add_int_int(JitState *s) {
    s->regs_i64[(uint64_t)&HOLE_ADD_DST] =
        s->regs_i64[(uint64_t)&HOLE_ADD_A] +
        s->regs_i64[(uint64_t)&HOLE_ADD_B];
    TAIL_NEXT(s);
}

/* op_sum_bigint — acc_i64[slot] += regs_i64[src]. */
extern uint64_t HOLE_SUM_SLOT;
extern uint64_t HOLE_SUM_SRC;
STENCIL op_sum_bigint(JitState *s) {
    s->acc_i64[(uint64_t)&HOLE_SUM_SLOT] +=
        s->regs_i64[(uint64_t)&HOLE_SUM_SRC];
    TAIL_NEXT(s);
}

/* op_branch_lt_int_int — if regs_i64[a] < regs_i64[b], skip forward
 * by patching the trailing jmp's displacement. The branch target is
 * encoded as the ABSOLUTE address of the destination stencil; the
 * patcher computes the relative displacement. */
extern void *HOLE_BLT_TGT;        /* absolute address of branch target */
extern uint64_t HOLE_BLT_A;
extern uint64_t HOLE_BLT_B;
STENCIL op_branch_lt_int_int(JitState *s) {
    if (s->regs_i64[(uint64_t)&HOLE_BLT_A] <
        s->regs_i64[(uint64_t)&HOLE_BLT_B]) {
        [[clang::musttail]] return ((StencilFn)&HOLE_BLT_TGT)(s);
    }
    TAIL_NEXT(s);
}

/* op_skip — unconditional forward jump to row_end. Used by the
 * program shape "if cond, skip the Sum and Exit early." */
STENCIL op_skip(JitState *s) {
    [[clang::musttail]] return ((StencilFn)s->row_end_addr)(s);
}

/* op_exit — terminator. Returns from the per-row entry. */
STENCIL op_exit(JitState *s) {
    (void)s;   /* nothing to do */
    /* no tail-call: this is the final stencil. The trailing jmp to
     * `next` is stripped during extraction; what's left is `ret` on
     * x86_64 / `ret` on aarch64. */
    return;
}
```

### 3.4 What hands the stencil set covers, what it doesn't

Covers: int64 arithmetic, int64 column load, int64 const load, int64
sum, forward branch with int64 compare, forward skip-to-row-end,
exit. That is enough to express the 30-op program below: a row loop
with one filter and one aggregate.

Does NOT cover: doubles, NULLs, type-mixed arithmetic, embedded
normal-interp branches, embedded calls, the entire string opcode
family. All Phase 5 work.

## 4. The 30-op aggregation-shaped program

Models a CTE inner aggregation: "for each row, if col1 < threshold,
skip; else sum col2 into acc[0]." A 3-instruction loop body × ~10
unrolled iterations would normally be tighter than 30 ops, but we
want a program large enough that compile time is non-trivial.

### 4.1 Conceptual structure

```
LoadConstInt   r0, 1000          ; threshold
LoadConstInt   r1, 0             ; constant 0
LoadColInt     r2, col=0         ; load col0 (filter key)
BranchLtIntInt r2, r0, label_skip
LoadColInt     r3, col=1         ; load col1 (sum target)
SumBigint      acc=0, src=r3
... repeated, with column index varying ...
Exit
label_skip:
Skip                              ; jump to row_end
```

Concretely, we emit:

| pc  | op                  | operands               |
|-----|---------------------|------------------------|
| 0   | load_const_int      | r0=1000                |
| 1   | load_const_int      | r1=0                   |
| 2   | load_col_int        | r2=col[0]              |
| 3   | branch_lt_int_int   | r2 < r0 → label_skip   |
| 4   | load_col_int        | r3=col[1]              |
| 5   | sum_bigint          | acc[0] += r3           |
| 6   | load_col_int        | r3=col[2]              |
| 7   | sum_bigint          | acc[0] += r3           |
| 8   | load_col_int        | r3=col[3]              |
| 9   | sum_bigint          | acc[0] += r3           |
| 10  | mov_int_int         | r4 = r2                |
| 11  | add_int_int         | r5 = r4 + r1           |
| ... | ...                 | (mostly mov/add chain to grow op count to ~30) |
| 28  | exit                |                        |
| 29  | (label_skip):       |                        |
|     | skip                |                        |

A small program builder in `microbench_program.c` constructs this
deterministically — no parser, no IR. The test harness then runs
the same `Program` through both the interpreter and the JIT.

### 4.2 Why 30 ops, why this shape

`plan.md` §7 calls for "~30 ops shaped like a real aggregation."
The shape — load → conditional skip → load → aggregate, with some
mov/add filler — is what the simplest CTE-driven inner aggregation
in `bench_q12_dbtc` actually looks like at the bytecode level (per
the surrounding code in `AggInterpreter::ProcessRec`). It has:

- one forward branch (the skip);
- one accumulator update;
- a small handful of intermediate register operations.

Anything smaller (e.g. 5 ops) is too easy and won't surface compile-
time issues. Anything larger (e.g. 100 ops) blows up the hand-
extraction work without proportionally improving signal.

## 5. The interpreter dispatch loop

`microbench_interp.c` — a stripped-down `AggInterpreter::ProcessRec`-
shaped loop, in pure C, ~80 LOC. Switch on opcode, call into a small
handler per case, no signals, no kernel dependencies, no
AggInterpreter object construction.

```c
void interp_run(const Program *prog, const Row *rows, size_t nrows,
                int64_t *out_acc) {
    InterpState s = {0};
    for (size_t r = 0; r < nrows; ++r) {
        s.row_cols_i64 = rows[r].cols;
        for (size_t pc = 0; pc < prog->n_ops;) {
            const Op *op = &prog->ops[pc];
            switch (op->kind) {
              case OP_LOAD_CONST_INT:
                s.regs_i64[op->a] = op->imm; pc++; break;
              case OP_LOAD_COL_INT:
                s.regs_i64[op->a] = s.row_cols_i64[op->b]; pc++; break;
              case OP_MOV_INT_INT:
                s.regs_i64[op->a] = s.regs_i64[op->b]; pc++; break;
              case OP_ADD_INT_INT:
                s.regs_i64[op->a] = s.regs_i64[op->b] + s.regs_i64[op->c];
                pc++; break;
              case OP_SUM_BIGINT:
                s.acc_i64[op->a] += s.regs_i64[op->b]; pc++; break;
              case OP_BRANCH_LT_INT_INT:
                if (s.regs_i64[op->a] < s.regs_i64[op->b]) pc = op->c;
                else pc++;
                break;
              case OP_SKIP:           pc = prog->n_ops; break;
              case OP_EXIT:           goto row_done;
              default: __builtin_unreachable();
            }
        }
      row_done: ;
    }
    *out_acc = s.acc_i64[0];
}
```

Real `AggInterpreter::ProcessRec` is more elaborate (NULL handling,
type promotion, accumulator slot resolution), but Phase 1 doesn't
need any of that — the program never produces NULLs and never mixes
types. Modelling the *dispatch shape* is what matters; that's what
copy-and-patch is meant to beat.

## 6. The Phase-1 engine

`jit1.{h,c}` — the copy-and-patch compiler proper. ~150 LOC.

### 6.1 Public API

```c
typedef struct Jit1Prog Jit1Prog;
typedef void (*Jit1Entry)(JitState *);

/* Compile `prog` into the arena. Returns NULL on failure
 * (only failure mode in Phase 1 is arena OOM). */
Jit1Prog *jit1_compile(NdbJitArena *arena, const Program *prog);

/* Get the per-row entry function. Caller threads JitState in;
 * advancing the row pointer between calls is the caller's job. */
Jit1Entry jit1_entry(Jit1Prog *p);
```

### 6.2 Compile algorithm

```
1. First pass: walk the bytecode, count emitted bytes. Record the
   byte-offset where each bytecode pc starts.
2. Second pass:
   for each opcode at pc:
     - look up the stencil for its kind
     - memcpy stencil bytes into arena[off]
     - for each hole in the stencil:
         - read the hole's relocation type and offset within the stencil
         - compute the patched value from the opcode's operands
         - write into arena[off + hole_offset]
     - drain any forward fixups whose source_label == pc
       (compute displacement = (target_off - patch_site_off - width)
        and write at the patch_site)
     - if this opcode is a forward branch, queue a fixup
3. Seal the arena range. Return the entry pointer.
```

No type prop in Phase 1 — there is exactly one stencil per opcode
kind. Phase 5 introduces the picker; Phase 1 hardcodes a flat
stencil-id-by-opcode-kind table.

### 6.3 The hole patching table (per stencil)

For each stencil we know, at compile time, an array of:

```c
typedef enum {
    HOLE_KIND_REG_DST,   /* a register index, written as 8-byte */
    HOLE_KIND_REG_A,
    HOLE_KIND_REG_B,
    HOLE_KIND_REG_C,
    HOLE_KIND_IMM_I64,
    HOLE_KIND_COL_IDX,
    HOLE_KIND_ACC_SLOT,
    HOLE_KIND_BRANCH_TGT,
} HoleKind;

typedef struct {
    uint32_t byte_offset;    /* into stencil bytes */
    HoleKind kind;
    uint8_t  width;          /* 1, 4, or 8 — Phase 1 only uses 8 */
} Hole;
```

Hand-authored in `stencils_x86_64.h`, alongside the bytes themselves.

### 6.4 Forward branch handling

The branch stencil emits a `[[clang::musttail]] return ((StencilFn)&HOLE_BLT_TGT)(s);`
which clang lowers to:

- x86_64: `mov rax, imm64; jmp rax` (12 bytes for the target load)
  — actually clang prefers `jmp rel32` if possible, but with an extern
  placeholder it'll use the absolute form; we patch the imm64.
- aarch64: 4-instruction MOV/MOVK/MOVK/MOVK + BR (or ADRP+ADD+BR);
  we patch the MOVKs.

Phase 1 stays x86_64 only. ARM64 branch encoding is Phase 2's work.

The patcher writes the absolute address of the target stencil's
emit-offset into the imm64 hole. Forward-only by construction:
program builder never generates a backward branch (no `pc = X` where
`X <= current_pc`). No admission walk in Phase 1; that's Phase 3.

## 7. Hand-extracting stencil bytes — the recipe

### 7.1 Compile

```bash
# Linux x86_64, on the user's Rocky box.
clang -O2 -fno-asynchronous-unwind-tables -ffreestanding \
      -fno-stack-protector -fno-pic \
      -c stencils_src.c -o stencils.o
```

Notes:
- `-fno-stack-protector`: removes canary preamble we don't want in
  stencils.
- `-fno-pic`: lets immediates land in `mov imm64, reg` rather than
  GOT loads. The arena is W^X mapped, not relocatable, so PIC is
  unhelpful.
- `-ffreestanding`: keeps clang from assuming hosted libc semantics.

### 7.2 Disassemble + extract bytes

```bash
objdump -d -M intel --no-show-raw-insn stencils.o > stencils.dis
objdump -d -M intel --show-raw-insn stencils.o > stencils.raw
objdump -r stencils.o > stencils.relocs
```

For each `op_*` symbol:

1. Find its byte range in `stencils.raw`.
2. Strip the trailing `jmp <next>` (5 bytes — `e9 ?? ?? ?? ??`).
3. Capture the remaining bytes.
4. From `stencils.relocs`, identify each relocation in that range.
   For Phase 1 we expect every `HOLE_*` extern to produce an
   `R_X86_64_64` (8-byte absolute) relocation. Record offsets.

The bytes go into an array literal:

```c
/* op_add_int_int — stripped of trailing jmp.
 * Source: stencils.dis lines NN..MM.
 * Relocs:
 *   offset 0x?? : R_X86_64_64 → HOLE_ADD_A
 *   offset 0x?? : R_X86_64_64 → HOLE_ADD_B
 *   offset 0x?? : R_X86_64_64 → HOLE_ADD_DST
 */
static const uint8_t bytes_op_add_int_int[] = {
    0x?? , 0x?? , ...
};
static const Hole holes_op_add_int_int[] = {
    { .byte_offset = 0x??, .kind = HOLE_KIND_REG_A,   .width = 8 },
    { .byte_offset = 0x??, .kind = HOLE_KIND_REG_B,   .width = 8 },
    { .byte_offset = 0x??, .kind = HOLE_KIND_REG_DST, .width = 8 },
};
```

A single sentence per stencil in `phase_1_microbench.md` records the
exact `objdump` lines copied, so a future reader can verify against
their own toolchain without spelunking.

### 7.3 Trip-wire

Before extracting all 8, do exactly one (`op_load_const_int`, the
simplest) and run a one-shot sanity test that:

1. Allocates an arena.
2. Memcpy's the bytes.
3. Patches the two holes (dst=0, val=42).
4. Writes a final `ret` byte (0xc3) where the stripped `jmp` was.
5. Calls it.
6. Asserts `regs_i64[0] == 42`.

If that passes, the toolchain pipeline is good and the remaining 7
stencils follow the same recipe. If it fails, the most likely causes:

- `preserve_none` produced an unexpected register move somewhere
  (clang version too old or some flag missing).
- Trailing `jmp` is `eb` (rel8) not `e9` (rel32) — strip 2 bytes
  not 5. Defensive check: verify the last 5 bytes of each stencil
  are `e9 ?? ?? ?? ??` before stripping.
- Relocation type isn't `R_X86_64_64` — likely `R_X86_64_PC32` if
  the placeholder symbol is referenced PC-relatively. Adjust patcher
  accordingly.

## 8. The microbench

`proto_microbench.c` ties it all together:

```c
int main(int argc, char **argv) {
    const size_t nrows = (argc > 1) ? atoll(argv[1]) : 100000;

    Row *rows = generate_rows(nrows);

    Program prog = build_30op_program();

    /* --- Interpreted run --- */
    int64_t acc_interp = 0;
    uint64_t t0 = clock_ns();
    interp_run(&prog, rows, nrows, &acc_interp);
    uint64_t t1 = clock_ns();

    /* --- JIT'd run --- */
    NdbJitArena *arena = ndb_jit_arena_create(64 * 1024);
    uint64_t tc0 = clock_ns();
    Jit1Prog *jp = jit1_compile(arena, &prog);
    uint64_t tc1 = clock_ns();

    Jit1Entry entry = jit1_entry(jp);
    int64_t acc_jit = 0;
    JitState s = {0};
    uint64_t tj0 = clock_ns();
    for (size_t i = 0; i < nrows; ++i) {
        s.row_cols_i64 = rows[i].cols;
        s.acc_i64[0] = acc_jit;
        entry(&s);
        acc_jit = s.acc_i64[0];
    }
    uint64_t tj1 = clock_ns();

    /* --- Correctness --- */
    if (acc_interp != acc_jit) {
        fprintf(stderr, "FAIL acc mismatch: interp=%lld jit=%lld\n",
                (long long)acc_interp, (long long)acc_jit);
        return 1;
    }

    /* --- Numbers --- */
    double interp_ns_per_row = (double)(t1 - t0) / nrows;
    double jit_ns_per_row    = (double)(tj1 - tj0) / nrows;
    double compile_ns        = (double)(tc1 - tc0);
    double speedup           = interp_ns_per_row / jit_ns_per_row;
    double break_even_rows   = compile_ns /
                               (interp_ns_per_row - jit_ns_per_row);

    printf("interp_ns_per_row,jit_ns_per_row,compile_ns,speedup,break_even_rows\n");
    printf("%.2f,%.2f,%.0f,%.2fx,%.0f\n",
           interp_ns_per_row, jit_ns_per_row,
           compile_ns, speedup, break_even_rows);

    /* --- Verdict --- */
    int compile_ok    = (compile_ns < 5000.0);
    int speedup_ok    = (speedup >= 2.0);
    int breakeven_ok  = (break_even_rows < 5000.0);
    printf("%s compile<5us=%s 2xspeedup=%s breakeven<5k=%s\n",
           (compile_ok && speedup_ok && breakeven_ok) ? "PASS" : "FAIL",
           compile_ok    ? "yes" : "NO",
           speedup_ok    ? "yes" : "NO",
           breakeven_ok  ? "yes" : "NO");

    ndb_jit_arena_destroy(arena);
    return (compile_ok && speedup_ok && breakeven_ok) ? 0 : 2;
}
```

Run with `taskset -c 0` on Linux for stable numbers; pin a single
performance core. macOS has no easy equivalent — the user's M-class
hardware is consistent enough that this is a non-issue.

The benchmark deliberately does not warm up the JIT'd code by
running it once before timing; we want the steady-state-after-icache-
miss number, which is what real per-row dispatch sees.

## 9. Step-by-step task breakdown

**Day 1, AM (~3h):**
- Write `bytecode1.h` (opcode enum, Op struct, Program struct).
- Write `microbench_program.c` (30-op program builder).
- Write `microbench_interp.c` (interpreter dispatch).
- Quick standalone test: build the program, run interpreted, assert
  expected acc value over a fixed seed of 100 rows.

**Day 1, PM (~4h):**
- Write `stencils_src.c` (8 stencils per §3.3).
- Compile with clang per §7.1, eyeball `stencils.dis`.
- Hand-extract `op_load_const_int` only.
- Run the trip-wire test (§7.3).
- If green: proceed. If red: debug toolchain issue before any further
  extraction.

**Day 2, AM (~4h):**
- Hand-extract the remaining 7 stencils into `stencils_x86_64.h`.
- Write `jit1.{h,c}`: compile algorithm per §6.2.
- Wire CMake: new static lib `ndb_jit1`, new exec `proto_microbench`.

**Day 2, PM (~3h):**
- Write `proto_microbench.c` (§8).
- Run end-to-end. Expected first-attempt outcome: correctness
  failure (some hole patched wrong, or a stencil's bytes are off by
  one). Iterate.
- Once correctness is green, capture numbers.

**Day 3 (~4h, contingency):**
- Debug whichever stencil is broken. Most likely culprits in order
  of probability:
  1. Trailing `jmp` strip got the wrong width (expected 5, got 2 or
     vice versa).
  2. Relocation offset off by one (clang places the imm64 differently
     than expected).
  3. `preserve_none` not honoured — clang restored a register clang
     thought it needed to save. Verify with `objdump --syms` that
     the function prologue is empty.
  4. `musttail` not used — clang inserted a regular call/ret.
     Cross-check `stencils.dis` for a trailing `jmp` (expected) vs.
     `call ... ; ret` (broken).

**Day 3 PM or Day 4 (~3h):**
- Cross-check correctness on macOS Apple Silicon: the same 30-op
  program, both interpreter and JIT, must produce the same
  aggregate. Skip the JIT-speed numbers on macOS — only the Linux
  x86_64 numbers count for the §2 compile-budget gate. macOS
  correctness is just a sanity check.
- Write `phase_1_microbench.md` (§10 below).
- Decision-gate verdict in the doc; commit + push if PASS.

Total: 3–5 days as `plan.md` predicts. Day 3 is the contingency slot
for the inevitable toolchain surprise.

## 10. `phase_1_microbench.md` template

To be written at the very end. Records:

```
# Phase 1 — copy-and-patch microbench: results

## Outcome
- [ ] Linux x86_64 PASS / FAIL
- [ ] macOS Apple Silicon correctness sanity check (no perf gate)

## Stencil set
| Name | Bytes | Holes | Source line in stencils_src.c |
|------|-------|-------|-------------------------------|
| op_load_const_int | N | dst, val | ... |
| ... | ... | ... | ... |

## Hand-extracted bytes (Linux x86_64)
[For each stencil, the exact bytes + objdump line range. ~50 lines.]

## Bench numbers (Linux x86_64)
| Metric | Value | Threshold | Pass? |
|--------|-------|-----------|-------|
| compile_ns | ___ | < 5,000 | ___ |
| jit_ns_per_row | ___ | — | — |
| interp_ns_per_row | ___ | — | — |
| speedup | ___x | ≥ 2.0x | ___ |
| break_even_rows | ___ | < 5,000 | ___ |

## Verdict (decision gate)
[ Continue to Phase 2 / Stop and reassess — with rationale. ]

## Toolchain quirks discovered
- clang version: ___
- compile flags: -O2 -fno-asynchronous-unwind-tables ...
- objdump version: ___
- Surprises while extracting stencils: ___
```

## 11. Verification checklist

Before declaring Phase 1 done:

- [ ] All 8 stencils extracted and patched correctly.
- [ ] Interpreter and JIT'd execution produce **bit-identical**
      `acc[0]` over the same 100k synthetic rows on Linux x86_64.
- [ ] Same correctness check passes on macOS Apple Silicon.
- [ ] `proto_microbench` exits 0 (all three thresholds met).
- [ ] No use of `_Generic`, no C++ features — pure C11.
- [ ] Stencils in `stencils_x86_64.h` are accompanied by a comment
      block recording the exact `objdump` lines they came from.
- [ ] `phase_1_microbench.md` filled in with the actual measured
      numbers, not placeholders.
- [ ] Verdict recorded in `phase_1_microbench.md`.
- [ ] Commit message follows `RONDB-1056:` prefix.

## 12. Out of scope for Phase 1 (explicit reminder)

Don't drift into these:

- Extractor tool (Phase 2).
- Type propagation / picker (Phase 5).
- DBTUP integration (Phase 4).
- Backward-branch admission walk (Phase 3).
- macOS performance numbers (correctness-only on macOS).
- Linux ARM64 stencils (Phase 2 cross-extraction).
- Stencils for opcodes the 30-op program doesn't use.
- Cleanup / refactoring of Phase 0 code.
- Production-quality error messages.

## 13. Risks / things that may surprise us

1. **`preserve_none` not honoured on Apple Silicon.** Apple's ARM64
   ABI has minor divergences from AAPCS. Phase 1 only requires
   x86_64 numbers, so a macOS aarch64 stencil mismatch is fine — we
   only need correctness there, and the one_shot trip-wire on macOS
   uses Apple clang's own bytes (whatever they are) against itself,
   so consistency is automatic.
2. **Compile time dominated by `objdump`-time hand-extraction.** Not
   a runtime risk, but a calendar-time risk. Mitigation: the trip-
   wire (§7.3) is the early-warning system. If the first stencil
   takes more than an hour to extract correctly, stop and reassess —
   maybe Phase 2's extractor moves up the schedule.
3. **Relocation type isn't `R_X86_64_64`.** If clang emits
   `R_X86_64_PC32` for the placeholder symbol references (likely if
   the symbol is decorated with `hidden` visibility), the patcher
   needs to handle PC-relative immediates. This is mechanical, just
   different. Detect via `objdump -r`; adjust.
4. **`musttail` rejected.** clang refuses `[[clang::musttail]]` when
   the calling convention isn't compatible between the caller and
   the tail-call target. With `preserve_none` on both sides this
   should be fine, but if it isn't, the workaround is to drop
   `musttail` and accept a real `call`+`ret` pair, which adds a few
   ns/row but still beats the interpreter's switch-on-opcode dispatch.
5. **2× speedup not reached.** This is the real decision-gate risk.
   If JIT is only ~1.3× faster than the stripped-down switch
   interpreter, the next question is whether the production
   `AggInterpreter::ProcessRec` (which has more dispatch overhead
   than our microbench's switch) gets a bigger win. Phase 4 would
   answer that, but we'd be committing to Phase 2/3/4 effort to find
   out. Reassess at Phase 1 doc.
6. **Compile time > 5 µs.** Most likely cause: forward-fixup
   resolution doing something dumb. Mitigation: profile with
   `clock_gettime` deltas inside `jit1_compile`. The bump-pointer
   plus 8 stencil memcpys plus ~40 patches should be << 1 µs; if
   it isn't, something's wrong.

## 14. What we learn from Phase 1

If Phase 1 PASSes the decision gate, we know:
- Copy-and-patch with `preserve_none` + `musttail` produces real
  speedup on real-shaped programs.
- Compile time fits the budget for the kind of program AggInterpreter
  actually sees.
- The plan's per-program-amortised model holds: 30 ops compiled in
  < 5 µs, paid back in well under 5k rows.

If Phase 1 FAILs, we know which axis to renegotiate. The doc
captures which one and what the next move is.
