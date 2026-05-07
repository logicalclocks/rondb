# RONDB-1056 Phase 5.0 — embedded interpreter calls

**Status: planning.** Phase 5.0 is a narrow first slice of the
broader Phase 5 work, focused on getting **embedded normal-
interpreter blocks JIT-compiled end-to-end** with a single
representative cold-call branch helper. Goals:

1. Admission walk recurses into `kOpEmbeddedInterp` blocks
   (currently rejected unconditionally).
2. Bridge translates the embedded NDB-bytecode block to flat
   JIT Op records with branch targets remapped to use Phase 3's
   `HK_BRANCH_TAKE` queue.
3. New 3-hole cold-call branch pattern in stencils + extractor +
   patcher.
4. **One** cold-call branch helper + stencil end-to-end:
   `BRANCH_ATTR_EQ_NULL` (the simplest of the family — just a
   null check on a column, no comparison logic).
5. MTR test exercising a `WHERE col IS NULL` query that JIT-
   compiles its embedded filter block.

The other 8 cold-call branches and the broader Phase 5 hot-
opcode lowering land in subsequent slices (5.1, 5.2, ...).
This keeps each slice small enough to land cleanly with full
verification, mirroring the Phase 4.5/4.6/4.7 cadence.

Aarch64 + x86_64 both supported (the cold-call mechanism is
already arch-portable from Phase 4). Branch:
`RONDB-1056-compiled-interpreter`.

## Headline scope (predicted)

| Deliverable | Lines added (rough) |
|---|---:|
| Bridge: recurse into `kOpEmbeddedInterp`, translate inner branches | ~150 |
| `op_branch_attr_eq_null` stencil + 3-hole pattern in extractor | ~80 |
| `ndb_jit_h_branch_attr_eq_null` helper in DbtupJitGlue | ~40 |
| Engine patcher: 3-hole pattern handling | ~30 |
| `dbtup_jit_call_ctx` extension for prog_buf pointer | ~20 |
| MTR test `rondb_jit_embedded_canary` | ~60 |

Roughly 1 query shape (`SELECT SUM(c) FROM t WHERE col IS NULL`)
goes from "rejected, fallback to interp" to "JIT-compiled, runs
through the cold-call helper". The other branch families (8 more)
follow incrementally.

## ★ Items to investigate (before coding starts)

### ★ Investigate I1 — the actual NDB embedded-block layout

Phase 5 plan §7 sketches it but hasn't been verified. The Day 0
spike must:
- Capture an actual embedded block emitted by NDB for a `WHERE
  col IS NULL` query (e.g., via DUMP 2370 or a debug print in
  `validateEmbeddedProgram`). Record the exact word stream.
- Verify the Interpreter::BRANCH_ATTR_EQ_NULL encoding:
  - bit layout of opcode + attr_id + branch offset + (if any)
    inline data length.
- Confirm `Interpreter::getInstructionPreProcessingInfo` returns
  the right instruction length for it.

This nails down the bridge translation logic before we write it.

### ★ Investigate I2 — readAttributes from a JIT helper

The Phase 4 cold-call helper `ndb_jit_h_load_col` reads a column
via `readAttributeForJit`, a friend wrapper around
`Dbtup::readAttributes`. The new BRANCH_ATTR_EQ_NULL helper
needs the same thing — but only the null-flag bit, not the
value. Spike:
- Find the readAttributes path that returns a tristate
  (value / null / not-found). Verify it's accessible from a
  JIT helper through `ctx->dbtup`.
- If the existing wrapper only returns the value, extend it to
  also signal null status.

### ★ Investigate I3 — Op layout for a 3-operand branch

Today's Op struct fields:
```c
struct Op {
  uint8_t kind;
  uint8_t a, b, c;        /* operand register indices */
  int64_t imm;            /* HK_OP_IMM payload */
};
```

For `BRANCH_ATTR_EQ_NULL` the payload is:
- attr_id (16-bit unsigned, fits in `b`)
- branch target pc (16-bit forward offset → handled by
  HK_BRANCH_TAKE queue, not stored in Op)
- "want_null" bit (1 bit — eq/ne discriminator → fits in `c` low
  bit, OR encoded via separate OpKind values)

Spike: pick the encoding. Recommendation: use OpKind to
discriminate eq vs ne (cleaner stencil source). `b = attr_id`,
`a` = (unused for now, reserved for future param index), `c` =
(unused). No imm needed — no inline arg data for null check.

This means BRANCH_ATTR_EQ_NULL and BRANCH_ATTR_NE_NULL are two
OpKinds sharing one stencil pattern but two stencil bodies that
differ only in `if (helper)` vs `if (!helper)`.

### ★ Investigate I4 — the 3-hole pattern's emission shape

Phase 5 §4 sketches:
```c
STENCIL op_branch_attr_eq_null(JitState *s) {
  if (ndb_jit_h_branch_attr_null(s,
                                  (uint32_t)HOLE_NARROW(BAEN_ATTR),
                                  /*want_null=*/1)) {
    [[clang::musttail]] return HOLE_BAEN_TGT(s);
  }
  TAIL_NEXT(s);
}
```

The clang-emitted bytes for this should be:
- Set up arguments (state in x0/rdi, attr_id in x1/rsi, want_null in x2/rdx)
- bl/call helper
- cmp result, 0
- b.eq fall_label (or b.ne taken_label depending on if-form)
- the taken target (musttail to HOLE_BAEN_TGT) — patched via
  HK_BRANCH_TAKE
- the fall-through target (TAIL_NEXT) — stripped trailing tail,
  the engine emits the next stencil's bytes immediately after

**Spike:** compile the analog, count instructions, verify the
HK_COLDCALL relocation lands at the bl/call site and
HK_BRANCH_TAKE at the taken-tail site.

### ★ Investigate I5 — bridge's bytecode-buffer access

The cold-call branch helpers (full Phase 5 set) need to read
inline arg data from the ORIGINAL NDB bytecode buffer (per
Strategy A in §4.1). For Phase 5.0's BRANCH_ATTR_EQ_NULL
specifically there's no inline arg — but plumbing
`ctx->prog_buf` through `dbtup_jit_call_ctx` now means
subsequent helpers (Phase 5.1+) drop in cheaply.

**Spike:** find where `dbtup_jit_call_ctx` is initialised and
add a `prog_buf` field that points to the SETUP record's
NDB-side bytecode buffer. Verify lifetime — the SETUP record
must outlive the cold-call helper invocations.

### ★ Investigate I6 — MTR test feasibility

Phase 4's `rondb_jit_canary` uses 4 query shapes; Q4 is the
"WHERE clause forces interp fallback" case. After Phase 5.0:
- Q4's filter (`WHERE t1.c1 >= 25`) wouldn't JIT yet — the
  embedded BRANCH_ATTR_OP_ARG family isn't covered.
- A new minimal MTR test should specifically exercise
  `WHERE col IS NULL`, which routes to BRANCH_ATTR_EQ_NULL
  inside the embedded block.

**Spike:** sketch the MTR test. Confirm the SQL produces an
embedded block with our target opcode (verifiable via DUMP 2370
or a debug log).

## 1. Scope

**In scope.**
- Bridge admission walk recurses into `kOpEmbeddedInterp` blocks.
  Same forward-only branch policy as the outer program.
- Bridge translates embedded block opcodes to flat JIT Op records.
  For Phase 5.0 we admit ONLY:
  - `Interpreter::BRANCH_ATTR_EQ_NULL` and `BRANCH_ATTR_NE_NULL`
  - `Interpreter::BRANCH` (unconditional forward jump — already
    representable as our existing forward-fixup mechanism)
  - `Interpreter::EXIT_OK` (corresponds to "filter passes,
    process this row" — maps to falling through the embedded
    block)
  - `Interpreter::EXIT_REFUSE` (corresponds to "skip this row" —
    maps to a special skip-to-end-of-block branch)
  Other embedded opcodes → reject the whole program (fallback).
- New 3-hole cold-call branch pattern in stencils_src.c +
  extractor + audit.
- New `op_branch_attr_eq_null` and `op_branch_attr_ne_null`
  stencils. (Two OpKinds sharing the same helper, eq/ne
  discriminator inverts the if.)
- New helper `ndb_jit_h_branch_attr_null` in DbtupJitGlue.
- `dbtup_jit_call_ctx` extension: `prog_buf` pointer
  (load-bearing for Phase 5.1+; harmless for Phase 5.0 since
  BRANCH_ATTR_*_NULL doesn't read it).
- New MTR test `rondb_jit_embedded_canary` exercising a
  `WHERE col IS NULL` query end-to-end.

**Out of scope.**
- The other 7 cold-call branch helpers (Phase 5.1+).
- The hot-lowered embedded BRANCH_*_REG_REG / REG_CONST family
  (Phase 5.2 — these are the easier ones, follow the existing
  hot-branch pattern from Phase 3).
- Type-state lattice + per-type stencil specialisation (deferred
  per Phase 5 plan §3 — Phase 5 plan calls this Day 1, but we
  defer until coverage is mature).
- Div / Mod / typed arithmetic (Phase 5.3+).
- `cheapMemory` / READ_LINKED_TO_MEM (Phase 5.4 — needed for
  BRANCH_MEM_OP_ARG family).
- Embedded blocks containing CALL / RETURN (Phase 5+).

## 2. File layout

```
storage/ndb/src/kernel/blocks/dbtup/jit/
├── ndb_jit_bridge.{c,h}        ← admission walk + bridge translation
├── stencils_src.c              ← op_branch_attr_eq/ne_null stencils
├── hole_kinds.h                ← new HK_BRANCH_FALL_3HOLE? (TBD spike)
├── bytecode1.h                 ← OP_BRANCH_ATTR_EQ_NULL (=21), OP_BRANCH_ATTR_NE_NULL (=22)
├── jit1.{c,h}                  ← admit_program recurse, 3-hole patcher
└── extract_stencils/
    ├── extract_stencils.c      ← 3-hole pattern recognition
    └── audit_magics.c          ← audit for the new pattern

storage/ndb/src/kernel/blocks/dbtup/
├── DbtupJitGlue.{cpp,h}        ← ndb_jit_h_branch_attr_null helper
└── readAttributes wrapper      ← extend to return null-flag

mysql-test/suite/ndb_push_agg/
├── t/rondb_jit_embedded_canary.test
└── r/rondb_jit_embedded_canary.result
```

## 3. Bridge admission walk

Phase 4's `admit_program` rejects `kOpEmbeddedInterp` flat. New:

```c
case BR_kOpEmbeddedInterp: {
  uint32_t emb_len = word & 0xFFFF;
  if (pos + 1 + emb_len > n_words) {
    set_err(out_err, JIT_BRIDGE_MALFORMED, this_pos, op);
    return JIT_BRIDGE_MALFORMED;
  }
  /* Recurse: same word stream, new outer-pc, depth+1. */
  jit_bridge_status_t rc = translate_embedded_block(
      ndb_prog + pos + 1, emb_len, out_prog, out_err, this_pos);
  if (rc != JIT_BRIDGE_OK) return rc;
  pos += 1 + emb_len;
  break;
}
```

`translate_embedded_block` walks the embedded NDB-interpreter
bytecode using `Interpreter::getInstructionPreProcessingInfo` to
advance instruction-by-instruction, and emits flat JIT Ops:

| NDB instruction | Translation |
|---|---|
| `BRANCH_ATTR_EQ_NULL pc+offset, attr_id` | `OP_BRANCH_ATTR_EQ_NULL b=attr_id; HK_BRANCH_TAKE → outer-pc + offset` |
| `BRANCH_ATTR_NE_NULL pc+offset, attr_id` | `OP_BRANCH_ATTR_NE_NULL b=attr_id; ...` |
| `BRANCH pc+offset` | `OP_BRANCH (existing) ...` |
| `EXIT_OK` | (fall through; no Op needed — block-end is reached) |
| `EXIT_REFUSE` | `OP_BRANCH skip-to-end-of-outer-program` (= row skipped) |
| anything else | reject whole program |

Branch targets are translated from embedded-block-relative pc
to outer-program-relative pc. The translated Op stream remains
flat (Phase 4 invariant preserved).

## 4. The 3-hole cold-call branch pattern

Per Phase 5 §4, the stencil source pattern for
`op_branch_attr_eq_null`:

```c
extern int ndb_jit_h_branch_attr_null(JitState *s,
                                       uint32_t attr_id,
                                       uint32_t want_null);

DECLARE_NARROW_HOLE(BAEN_ATTR);  /* 16-bit attr_id */
extern __attribute__((preserve_none)) void HOLE_BAEN_TGT(JitState *);

STENCIL op_branch_attr_eq_null(JitState *s) {
  if (ndb_jit_h_branch_attr_null(s,
                                  (uint32_t)HOLE_NARROW(BAEN_ATTR),
                                  /*want_null=*/1)) {
    [[clang::musttail]] return HOLE_BAEN_TGT(s);   /* taken */
  }
  TAIL_NEXT(s);                                     /* fall through */
}
```

Holes:
- 1× narrow (BAEN_ATTR_NARROW) for the 16-bit attr_id
- 1× HK_COLDCALL for `bl/call ndb_jit_h_branch_attr_null` —
  reuses Phase 4's HK_COLDCALL machinery directly
- 1× HK_BRANCH_TAKE for the taken-musttail to HOLE_BAEN_TGT —
  reuses Phase 3's HK_BRANCH_TAKE queue + drain mechanism
- 1× HK_BRANCH_FALL (implicit — trailing TAIL_NEXT, stripped by
  extractor, engine emits next stencil immediately after)

The patcher needs no new path — all three hole kinds already
exist. The novelty is just the **combination** of HK_COLDCALL +
HK_BRANCH_TAKE in one stencil. The extractor must recognize
both relocations and emit both holes.

## 5. The first helper

```cpp
extern "C" int ndb_jit_h_branch_attr_null(JitState *s,
                                           uint32_t attr_id,
                                           uint32_t want_null) {
  auto *ctx = static_cast<dbtup_jit_call_ctx *>(s->ctx);
  bool is_null = readAttributeIsNull(ctx->dbtup, ctx->key_req,
                                      attr_id);
  return (is_null == (want_null != 0)) ? 1 : 0;
}
```

(`readAttributeIsNull` is a thin friend wrapper around the
existing `readAttributes` path that just returns the null
flag — to be added in DbtupJitGlue.)

## 6. Step-by-step task breakdown

**Day 0 (~½ day)** — investigation spike (★ I1-I6).

**Day 1 (~4h)** — admission + bridge plumbing.
- Bridge: recurse into kOpEmbeddedInterp blocks.
- Translate `BRANCH_ATTR_EQ_NULL`, `BRANCH_ATTR_NE_NULL`,
  `BRANCH`, `EXIT_OK`, `EXIT_REFUSE` only; reject everything
  else within an embedded block.
- Bridge unit tests for the new admission paths.

**Day 2 (~4h)** — stencil + extractor + patcher.
- New OpKinds `OP_BRANCH_ATTR_EQ_NULL = 21`,
  `OP_BRANCH_ATTR_NE_NULL = 22` in bytecode1.h.
- New stencils in stencils_src.c.
- New narrow magic `MAGIC_BAEN_ATTR_NARROW` and `MAGIC_BANN_ATTR_NARROW`.
- Extractor: confirm the 3-hole pattern emits the right Hole
  records. (Should be no extractor changes — HK_COLDCALL +
  HK_BRANCH_TAKE both already work.)
- regen-stencils PASS.

**Day 3 (~4h)** — DbtupJitGlue helper.
- `readAttributeIsNull` wrapper (extend the existing friend
  bridge).
- `ndb_jit_h_branch_attr_null` helper.
- Register the helper via `dbtup_jit_register_helpers`.
- Gain `prog_buf` pointer in `dbtup_jit_call_ctx` (load-bearing
  for Phase 5.1+; populated but unread in Phase 5.0).

**Day 4 (~3h)** — MTR canary.
- New test `rondb_jit_embedded_canary`:
  ```sql
  CREATE TABLE t (pk BIGINT NOT NULL, c1 BIGINT,
                  PRIMARY KEY (pk)) ENGINE=NDB;
  INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30), (4, NULL);
  SET ndb_join_pushdown_aggregate=ON;
  SELECT COUNT(*) FROM t WHERE c1 IS NULL;
  -- expected: 2
  ```
- Verify the query JIT-compiles (rather than falling back).
  DUMP 2370 + log inspection.

**Day 5 (~2h)** — drift check, results doc, plan.md flip.
- `phase_5_0_embedded_calls.md` results doc.
- Add Phase 5.0 to plan.md phase index.

**Total: 4-5 days** (excluding the Day 0 spike).

## 7. Test approach

- **Bridge unit tests**: 4-5 cases covering the new embedded-
  block admission paths (accept BRANCH_ATTR_EQ_NULL, reject
  unsupported embedded opcodes, reject backward branches inside
  embedded block).
- **Cold-call unit test**: at least one case where a JIT'd
  program containing `op_branch_attr_eq_null` runs against a
  mock `dbtup_jit_call_ctx` with stub readAttributes. Verify
  taken / fall-through behavior.
- **MTR canary**: end-to-end SQL → bridge → JIT → row execution
  with a real NDB cluster. Result must match the interpreter
  baseline.
- **Phase 4/4.5/4.6/4.7 regression**: all unit tests still
  PASS, microbench differential aggregates match, original
  `rondb_jit_canary` MTR PASSes.

## 8. Verification checklist

- [ ] All ★ Investigate items resolved.
- [ ] Bridge admits a program containing `kOpEmbeddedInterp`
      with only the supported embedded opcodes.
- [ ] Bridge rejects programs with unsupported embedded
      opcodes (admission fallback to interp).
- [ ] Stencils for both eq/ne variants land in regen-stencils
      with audit PASS.
- [ ] `coldcall_tests` extended with one new case for the
      3-hole pattern; all existing tests still PASS.
- [ ] `rondb_jit_embedded_canary` MTR PASSes (2 expected from
      `WHERE c1 IS NULL`).
- [ ] All Phase 4-4.7 verification gates retained.

## 9. Out of scope (explicit reminder)

- Other cold-call branch families (Phase 5.1: ATTR_OP_ARG,
  ATTR_OP_PARAM, ATTR_OP_ATTR, MEM_OP_ARG,
  MEM_OP_ARG_INLINE_TYPE, LINKED_*_NULL — 7 more).
- Hot-lowered embedded BRANCH_*_REG_* (Phase 5.2 — uses Phase 3
  branch infrastructure directly).
- Type-state lattice + per-type specialisation (Phase 5.3+).
- StringSearch / BinarySearch / QSort cold-call helpers
  (Phase 5.5+).
- DUMP 2370 force-interp/force-jit toggle (orthogonal — could
  be added anytime).

## 10. Risks

1. **The `readAttributes` from JIT-helper path may have
   been written specifically for the value-only case.**
   Extending it to return null-status cleanly might require
   touching the Dbtup friend interface in non-obvious ways. If
   it spirals, we admit the constraint and move on.

2. **Embedded-block branch-target translation.** The embedded
   block has its own pc-space; the JIT engine's HK_BRANCH_TAKE
   queue is keyed on outer-program pcs. The bridge must
   carefully translate between them. Day 0 spike must verify
   this works for at least the simple case (one BRANCH_ATTR_EQ_NULL
   with a forward target inside the same embedded block).

3. **Embedded blocks can contain mixed supported + unsupported
   opcodes.** The admission walk must REJECT the whole program
   if ANY opcode in the embedded block is unsupported — partial
   coverage isn't safe. Easy in principle, but the early-exit
   path through the bridge needs to keep the caller-side
   fallback machinery intact.

4. **MTR canary may fall back to interp despite our new admission
   support.** If the SQL planner generates an embedded program
   with opcodes we don't yet admit (e.g., it adds an unrelated
   BRANCH_REG_LT_REG that we reject), the whole program falls
   back. Day 0 must verify the canary's actual NDB bytecode
   matches our admission set.

## 11. References

- Phase 4 results doc: `phase_4_setup_integration.md` — for
  cold-call mechanism + helper registry.
- Phase 5 plan: `phase_5_implementation.md` §4, §7 — original
  design of the 3-hole pattern + embedded-interp lowering.
- Plan.md §11 — Phase 5 overview.
- NDB code:
  - `AggInterpreter::validateEmbeddedProgram` — current Init-
    time embedded-block walker.
  - `Interpreter::getInstructionPreProcessingInfo` — instruction
    length + branch-target unpacker.
  - `DbtupJitGlue.{cpp,h}` — Phase 4 cold-call helper layer.
