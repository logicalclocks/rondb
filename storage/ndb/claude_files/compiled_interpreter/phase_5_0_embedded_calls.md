# RONDB-1056 Phase 5.0 — embedded interpreter calls

**Status: shipped.** Phase 5.0 lands the first slice of broader
Phase 5: end-to-end JIT compilation of aggregation programs that
contain an embedded normal-interpreter `WHERE` filter using
`IS NULL` / `IS NOT NULL` patterns. Branch:
`RONDB-1056-compiled-interpreter`.

## Outcome

All Phase 5.0 verification gates clear on aarch64 (macOS prod_build):

| Gate | Result |
|---|---|
| `regen-stencils` audit (39 magics + 2 new narrow) | PASS |
| `proto_smoke` arena/JIT smoke | PASS |
| `admission_tests` 12/12 | PASS |
| `bridge_tests` 15/15 (5 new Phase 5.0 cases) | PASS |
| `coldcall_tests` 5/5 | PASS |
| `proto_microbench` differential aggregates | match |
| Phase 1 microbench VERDICT (all 3 thresholds) | PASS |
| `drift_check.sh` (committed headers == regen output) | PASS |
| `rondb_jit_canary` MTR (Phase 4 baseline) | PASS (247 ms) |
| **`rondb_jit_embedded_canary` MTR (new)** | **PASS (246 ms)** |

The new MTR canary covers three query shapes (Q1: `SUM` with
`IS NULL`, Q2: `SUM` with `IS NOT NULL`, Q3: `COUNT(*)` with
`IS NULL`) and confirms the JIT-emitted code produces the
correct results end-to-end.

## What shipped

**Bridge admission walk** (`ndb_jit_bridge.c`):

`kOpEmbeddedInterp` is no longer a flat reject. The bridge
recurses into the embedded NDB normal-interpreter block and
admits a narrow set of opcodes:

| NDB embedded opcode | JIT translation |
|---|---|
| `BRANCH_ATTR_EQ_NULL` | `OP_BRANCH_ATTR_EQ_NULL` + branch-target fixup |
| `BRANCH_ATTR_NE_NULL` | `OP_BRANCH_ATTR_NE_NULL` + branch-target fixup |
| `EXIT_OK` / `EXIT_OK_LAST` | no Op (fall through to outer accumulators) |
| `EXIT_REFUSE` | `OP_EXIT` (early-terminate JIT'd row processing) |

Anything else in the embedded block rejects the whole program
(falling back to the interpreter — correctness preserved, no JIT
speed-up).

The walker uses a 2-pass algorithm: pass 1 emits Ops and tracks
each embedded-pc's output Op index in `emb_pc_to_op_idx[]`;
pass 2 fixes up branch targets (replacing temporary
target-emb-pc values with output Op indices). The fixup-pending
markers ride in the high bit of the `c` field.

**Two new OpKinds** in `bytecode1.h`:

```c
OP_BRANCH_ATTR_EQ_NULL = 21,
OP_BRANCH_ATTR_NE_NULL = 22,
```

Layout: `kind` discriminates eq/ne; `b = attr_id` (8-bit cap,
≤255 — same restriction as `op_load_col_ndb`); `c =
branch target pc`.

**3-hole cold-call branch pattern** in `stencils_src.c` — the
first new stencil pattern of Phase 5+. The two stencils
`op_branch_attr_eq_null` and `op_branch_attr_ne_null` share one
helper via a runtime `want_null` flag:

```c
extern int ndb_jit_h_branch_attr_null(JitState *s,
                                       uint32_t attr_id,
                                       uint32_t want_null);

DECLARE_NARROW_HOLE(BAEN_ATTR);
extern __attribute__((preserve_none)) void HOLE_BAEN_TGT(JitState *);

STENCIL op_branch_attr_eq_null(JitState *s) {
  if (ndb_jit_h_branch_attr_null(s,
                                  (uint32_t)HOLE_NARROW(BAEN_ATTR),
                                  /*want_null=*/1)) {
    [[clang::musttail]] return HOLE_BAEN_TGT(s);
  }
  TAIL_NEXT(s);
}
```

aarch64 stencil shape (44 B post-strip, 4 holes):

```
offset  0: stp x29, x30, [sp, #-16]!     (bl frame prologue)
offset  4: movz w1, #attr_magic           ← HK_OP_B narrow MOVZ
offset  8: mov  x0, x20                   (state ptr → x0)
offset 12: mov  w2, #want_null            (eq=1 / ne=0)
offset 16: mov  x29, sp
offset 20: bl   ndb_jit_h_branch_attr_null  ← HK_COLDCALL
offset 24: cbz  w0, fall_label
offset 28: ldp  x29, x30, [sp], #16
offset 32: b    HOLE_*_TGT                ← HK_BRANCH_TAKE
offset 36: ldp  x29, x30, [sp], #16
offset 40: b    next_                     ← HK_BRANCH_FALL (stripped)
```

**Crucially**: the 3-hole pattern needed **no new extractor or
patcher infrastructure**. Phase 3's HK_BRANCH_TAKE +
HK_BRANCH_FALL machinery and Phase 4's HK_COLDCALL machinery
each already existed; the novelty is just *combining* them in
one stencil. The Day 0 spike confirmed this with a
disassembly-+-relocation check.

**The cold-call helper** in `DbtupJitGlue.cpp`:

```cpp
extern "C" int
ndb_jit_h_branch_attr_null(JitState *s, uint32_t attr_id,
                            uint32_t want_null) {
  auto *ctx = static_cast<dbtup_jit_call_ctx *>(s->ctx);
  /* validate ctx, panic on misformed */
  Uint32 read_buf[4];
  int ret = ctx->agg->readAttributeForJit(ctx->block_tup,
                                            ctx->req_struct,
                                            attr_id, read_buf,
                                            4);
  /* abort on read failure */
  AttributeHeader *header =
      reinterpret_cast<AttributeHeader *>(&read_buf[0]);
  bool is_null = header->isNULL();
  return (is_null == (want_null != 0)) ? 1 : 0;
}
```

Reuses Phase 4's `readAttributeForJit` friend wrapper without
changes — it already writes the AttributeHeader to buf[0] which
contains the null flag. (`ctx->agg = nullptr` in dispatch is a
deliberate Phase 4 trick: `readAttributeForJit` is non-virtual
and doesn't use `this`, so calling through a null pointer works
in practice; the helper inherits the same convention.)

## What changed (commits)

| Day | Work | Commit |
|---|---|---|
| 0 | Investigation spike (6 ★ items) | `105017d244f` |
| 1 | Bridge admission walk + embedded translation | `e9f63fc6dca` |
| 2 | `op_branch_attr_eq/ne_null` stencils + magics | `64e3ca8a9f1` |
| 3 | `ndb_jit_h_branch_attr_null` helper | `c5afde57626` |
| 4 | `rondb_jit_embedded_canary` MTR test | `f7d29ab017b` |
| 5 | This results doc + plan.md flip | (this) |

**Modified core**:

- `ndb_jit_bridge.c` (+ `ndb_jit_bridge.h`) — recurse into
  `kOpEmbeddedInterp`; new `translate_embedded_block` walker;
  two new error codes (`JIT_BRIDGE_EMBEDDED_TOO_LARGE`,
  `JIT_BRIDGE_EMBEDDED_BACKWARD`).
- `bytecode1.h` — `OP_BRANCH_ATTR_EQ_NULL = 21`,
  `OP_BRANCH_ATTR_NE_NULL = 22`.
- `stencils_src.c` — two new stencils sharing one helper via a
  `want_null` flag.
- `hole_kinds.h` — 2 new narrow magics
  (`MAGIC_BAEN_ATTR_NARROW`, `MAGIC_BANN_ATTR_NARROW`); 4 new
  symbol-table entries; 2 new narrow-magic-table entries.
- `extract_stencils.c` (kOpkindMap) + `audit_magics.c`
  (kNarrowMagicToStencil) — 2 new entries each.
- `DbtupJitGlue.{cpp,hpp}` — new `ndb_jit_h_branch_attr_null`
  helper; registered alongside `ndb_jit_h_load_col`.
- `bridge_tests.c` — 5 new test cases (T11-T15) covering the
  embedded admission paths.
- `mysql-test/suite/ndb_push_agg/{t,r}/rondb_jit_embedded_canary.{test,result}`
  — new MTR canary.

**Re-generated**: `stencils_arm64.h` and `stencils_x86_64.h`
both gain bytes for the two new stencils. Existing stencil
bytes byte-identical to Phase 4.7.

## What we learned

1. **The 3-hole pattern is just composition.** The Phase 5
   plan's design of HK_COLDCALL + HK_BRANCH_TAKE + HK_BRANCH_FALL
   for cold-call branches reads as a "new" pattern, but
   implementation-wise it's literally just calling clang with a
   stencil that uses both relocation kinds. No engine changes,
   no extractor changes, no audit changes. The Day 0 spike
   compile-and-disassemble proved this in 30 minutes.

2. **NDB has two opcode encodings.** The aggregation interpreter
   (kOp* family) puts the opcode in bits 31..26. The embedded
   normal interpreter puts it in bits 5..0 + bit 15. Easy to
   miss; surfaced by failing tests T12-T14 in the first
   iteration, fixed in 5 minutes once spotted.

3. **`EXIT_REFUSE` translates cleanly to `OP_EXIT`.** NDB's
   "skip this row" semantic in the embedded interpreter maps
   directly to "stop JIT'd row processing" (= execute the
   function-return sequence). The accumulators that come *after*
   the embedded block in the outer aggregation program get
   skipped naturally because control already returned.

4. **`readAttributeForJit` is reusable.** The Phase 4 friend
   wrapper writes the full AttributeHeader to buf[0]; it
   trivially supports null-flag inspection. No new wrapper
   needed for Phase 5.0.

5. **Bridge unit tests caught the encoding bug instantly.**
   T12-T14 (embedded admit / backward / oor reject) failed
   immediately in the first build — the wrong opcode encoding
   was tripping the malformed-instruction check. Without these
   tests we'd have caught it only at MTR run time.

## What didn't change

- **Engine patcher** (`jit1.c`) — no new HK_* kinds; no new
  width values. The 3-hole pattern uses width=4 for the COLDCALL
  hole (4-byte branch displacement) and width=4 for the
  BRANCH_TAKE/FALL holes (same), all already supported.
- **Phase 4 cold-call helper machinery** — `jit1_register_helper`
  + `jit1_lookup_helper` reused without changes. The new helper
  registers exactly like Phase 4's `ndb_jit_h_load_col`.
- **Existing stencil bytes** — byte-identical to Phase 4.7. Only
  the two new stencils' bytes are added.
- **`prog_buf` in `dbtup_jit_call_ctx`** — deferred to Phase 5.1
  (when the first arg-reading helper lands —
  `ndb_jit_h_branch_attr_op_arg`). `BRANCH_ATTR_EQ_NULL` doesn't
  read inline arg data, so no plumbing change needed today.

## Out of scope (deferred to Phase 5.1+)

- **The other 7 cold-call branch helpers**: `ATTR_OP_ARG`,
  `ATTR_OP_PARAM`, `ATTR_OP_ATTR`, `MEM_OP_ARG`,
  `MEM_OP_ARG_INLINE_TYPE`, `LINKED_EQ_NULL`, `LINKED_NE_NULL`.
  Each follows the same 3-hole pattern; each needs its own
  helper. Cumulative scope: ~7-9 additional days.
- **Hot-lowered embedded `BRANCH_*_REG_*`** — register-register
  comparison branches inside the embedded block. Use the
  existing Phase 3 hot-branch pattern directly (no new
  mechanism). ~3-4 days.
- **`cheapMemory` / `READ_LINKED_TO_MEM`** — a memory-area
  primitive used by the MEM/LINKED branch families. ~2-3 days.
- **Type-state lattice + per-type stencil specialisation** —
  the bigger Phase 5 design; defers cleanly until coverage
  matures.
- **DUMP-based JIT-vs-interp counter** — currently no runtime
  observability for "did this query JIT or fall back". Not
  load-bearing for correctness; useful for performance
  validation in Phase 5+.

## Caveat: correctness-only verification

The MTR canary asserts that JIT-compiled embedded blocks
produce the correct results on a real cluster. It does **not**
assert that the JIT path was actually taken — if the SQL
planner emits an embedded block with an opcode not in our
admit set, the bridge silently rejects and falls back to the
interpreter. Result is still correct (interp does its job), but
JIT isn't exercised.

This is acceptable for Phase 5.0's "first slice" deliverable —
correctness is the load-bearing property; observability is a
follow-up. Phase 5.x can add a stats counter (DUMP code) that
exposes JIT-success vs fallback counts, making the verification
gap close.

## References

- Phase 5.0 implementation plan: `phase_5_0_implementation.md`.
- Phase 5 broader plan: `phase_5_implementation.md`.
- Phase 4 cold-call mechanism: `phase_4_setup_integration.md`.
- NDB embedded-interpreter encoding: `Interpreter.hpp` (opcode
  layout: bits 5..0 + bit 15) and `DbtupExecQuery.cpp` (branch
  decoder via `Dbtup::brancher`).
- The new MTR test: `mysql-test/suite/ndb_push_agg/t/
  rondb_jit_embedded_canary.test`.
