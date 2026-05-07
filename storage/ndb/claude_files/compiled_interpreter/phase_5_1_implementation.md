# RONDB-1056 Phase 5.1 — remaining 7 cold-call branch helpers

**Status: planning.** Phase 5.1 extends Phase 5.0's 3-hole cold-
call branch pattern to the rest of the embedded normal-
interpreter branch family — 7 new stencils across 4 operand-
shape categories. Each one follows the same structural pattern
proven in 5.0 (HK_OP_* narrow + HK_COLDCALL + HK_BRANCH_TAKE +
HK_BRANCH_FALL); the work is per-family bridge translation +
helper implementation.

Branch: `RONDB-1056-compiled-interpreter`.

## Scope

The 7 stencils, ordered by complexity (simplest first):

| # | Stencil | Helper | Complexity | Dependency |
|---|---|---|---|---|
| 1 | `op_branch_linked_eq_null` | `ndb_jit_h_branch_linked_null` | trivial | none |
| 2 | `op_branch_linked_ne_null` | (same helper, ne via flag) | — | shares #1 |
| 3 | `op_branch_attr_op_attr` | `ndb_jit_h_branch_attr_op_attr` | medium | none |
| 4 | `op_branch_attr_op_param` | `ndb_jit_h_branch_attr_op_param` | medium | NDB param-buffer access |
| 5 | `op_branch_attr_op_arg` | `ndb_jit_h_branch_attr_op_arg` | complex | `prog_buf` plumbing |
| 6 | `op_branch_mem_op_arg` | `ndb_jit_h_branch_mem_op_arg` | complex | `READ_LINKED_TO_MEM` admission + cheapMemory |
| 7 | `op_branch_mem_op_arg_inline_type` | `ndb_jit_h_branch_mem_op_arg_inline_type` | complex | same as #6 |

**6 unique helpers** (LINKED_EQ/NE_NULL share one via the
want_null flag, just like Phase 5.0's BRANCH_ATTR_*_NULL).

**3 dependency classes** that must land in order:

- **(A) Standalone** — LINKED_*_NULL, ATTR_OP_ATTR, ATTR_OP_PARAM
  use only existing infrastructure (Phase 5.0's bridge walker +
  cold-call mechanism). Land first.
- **(B) prog_buf** — ATTR_OP_ARG needs the SETUP-record's NDB
  bytecode buffer accessible at helper-call time. Plumb a
  `prog_buf` pointer through `dbtup_jit_call_ctx` (deferred from
  Phase 5.0).
- **(C) cheapMemory + READ_LINKED_TO_MEM** — the MEM_OP_ARG
  family needs `cheapMemory[0]` populated by an admitted
  `READ_LINKED_TO_MEM` instruction. Add the admission rule + the
  cold-call helper for the mem-load step itself.

## Headline scope

| Deliverable | Lines added (rough) |
|---|---:|
| LINKED_*_NULL: 2 stencils + 1 helper + bridge translation | ~150 |
| ATTR_OP_ATTR: 1 stencil + 1 helper + bridge translation | ~180 |
| ATTR_OP_PARAM: 1 stencil + 1 helper + bridge | ~180 |
| ATTR_OP_ARG: 1 stencil + 1 helper + prog_buf plumbing + bridge | ~260 |
| MEM_OP_ARG family: 2 stencils + 2 helpers + READ_LINKED_TO_MEM admission + cheapMemory glue | ~400 |
| MTR test extensions (per family) | ~250 |
| Bridge unit tests for new admission paths | ~200 |

Roughly 1,600 lines of source + tests. Compared to Phase 5.0's
~600 lines, this is ~2.6× the size.

**Effort estimate**: 10-12 days. Could be split into 3 incremental
sub-phases (5.1a / 5.1b / 5.1c) corresponding to the 3 dependency
classes above. Recommend that.

## ★ Items to investigate (before coding starts)

### ★ Investigate I1 — exact NDB encoding of each opcode

Each NDB embedded-interp branch has a different word layout:

| Opcode | Words | Operand layout |
|---|---:|---|
| `BRANCH_LINKED_EQ_NULL` | 1 | opcode (bits 5..0+15) + branch_offset (bits 30..16). Position implicit from preceding `READ_LINKED_TO_MEM`. |
| `BRANCH_LINKED_NE_NULL` | 1 | same |
| `BRANCH_ATTR_OP_ATTR` | 2 | word 0: opcode + cond + null_semantics + branch_offset. word 1: (attrId1 << 16) \| attrId2. |
| `BRANCH_ATTR_OP_PARAM` | 2 | word 0: opcode + cond + ... word 1: (attrId << 16) \| paramNo. |
| `BRANCH_ATTR_OP_ARG` | 2+N | word 0: opcode + cond + ... word 1: (attrId << 16) \| arg_byte_len. words 2..N: inline arg data. |
| `BRANCH_MEM_OP_ARG` | 2+N | same word 0; word 1: (attrId << 16) \| arg_byte_len. attrId for type/charset lookup, data from cheapMemory[0]. |
| `BRANCH_MEM_OP_ARG_INLINE_TYPE` | 3+N | extra word with inline type/length/charset info. |

**Spike (per family)**: capture an actual NDB-emitted block for
each query shape. Decode by hand. Confirm operand bit positions.

### ★ Investigate I2 — comparison condition encoding

`BRANCH_ATTR_OP_ATTR / OP_PARAM / OP_ARG` and the MEM family all
encode a comparison condition (eq, ne, lt, le, gt, ge) within
the opcode word (bits 12..14 in `cond`, plus `null_semantics`
in adjacent bits). Phase 5.1's bridge needs to translate this
into a runtime parameter for the helper, and the helper needs to
dispatch on it.

**Spike**: find the runtime comparison code (likely
`Interpreter::cond_eval` or similar). Determine if our helpers
should:
- (A) call the same NDB comparison primitive directly (re-using
  NDB-side dispatch), OR
- (B) implement comparison inline in the helper (with a
  cond-switch).

(A) is simpler — the helper is just a thin wrapper. (B) is
faster (no double-dispatch) but bigger code. Default to (A)
for Phase 5.1.

### ★ Investigate I3 — readAttributes lifetime + caching

NDB's interpreter caches the most-recently-read attr_id in
`ctx.tmpHabitant` and re-uses it across subsequent
BRANCH_ATTR_*_NULL calls on the same attr. The Phase 5.0 helper
ignores this caching (each call re-reads). For Phase 5.1's
ATTR_OP_ATTR / OP_PARAM / OP_ARG family, ditto.

For Phase 5.1's first slice we accept the per-call read cost.
Future Phase 5.x can plumb caching through a per-row cache
sidecar in `dbtup_jit_call_ctx`.

**Spike**: confirm that re-reading the same attribute multiple
times per row is cheap enough not to matter — it's just a
pointer-arith into the row buffer plus a memcpy of the
AttributeHeader, no I/O. Should be fine.

### ★ Investigate I4 — `prog_buf` plumbing through `dbtup_jit_call_ctx`

`BRANCH_ATTR_OP_ARG` and the MEM_OP_ARG family need to read
inline arg data from the original NDB bytecode buffer. Per Phase
5 plan §4.1 Strategy A: pass an offset into the original NDB
bytecode buffer; the helper resolves it via `ctx->prog_buf +
offset`. The buffer lives in the SETUP record for the program's
lifetime (verified during Phase 4 — SETUP record outlives
ProcessRec invocations).

**Spike**: extend `struct dbtup_jit_call_ctx` to add
`const Uint32 *prog_buf;`. Extend `dbtup_jit_invoke` signature.
Update the JoinAggInterpreter caller to pass `m_prog`. Verify
the canary still passes (no change in behavior since Phase 5.0
helpers don't read prog_buf).

### ★ Investigate I5 — `cheapMemory` access from JIT helpers

`BRANCH_LINKED_*_NULL` and `BRANCH_MEM_OP_ARG` need to read
`cheapMemory[0]` (the area populated by `READ_LINKED_TO_MEM`).
This is part of the embedded interpreter's runtime state, not
the JIT'd state. Phase 5 plan §7.3 sketches access via
`ctx->agg->cheapMemory[]` after a friend-access wrapper.

**Spike**: locate the `cheapMemory` field. Determine its struct
membership (is it on `JoinAggInterpreter`? on the NDB-side
embedded `InterpreterContext`?). Add a friend wrapper similar
to `readAttributeForJit`.

### ★ Investigate I6 — `READ_LINKED_TO_MEM` opcode admission

For the MEM family to work, the embedded block must contain a
preceding `READ_LINKED_TO_MEM` (opcode 39) which sets up
`cheapMemory[0]`. This is itself a cold-call (the bridge can't
inline it).

Add it as another bridge translation:
```
NDB embedded READ_LINKED_TO_MEM position:8 →
  OP_LOAD_LINKED_TO_MEM (new)
  → bl ndb_jit_h_read_linked_to_mem(s, position)
```

Single-word instruction. New OpKind. Same structure as Phase 4's
`op_load_col_ndb` (cold-call without branch). 1 new helper.

**Spike**: read NDB's runtime handler for `READ_LINKED_TO_MEM`
to understand what the helper needs to do (likely just call into
the existing routine that populates cheapMemory).

## Day breakdown — Phase 5.1a (LINKED_*_NULL only)

The smallest standalone slice. Lands the LINKED_EQ/NE_NULL pair
following Phase 5.0's pattern almost verbatim.

**Day 0 (~½ day)** — Investigation spike (I5: cheapMemory
access).

**Day 1 (~3h)** — Bridge admission for `BRANCH_LINKED_EQ_NULL` /
`NE_NULL` in `translate_embedded_block`. New OpKinds
`OP_BRANCH_LINKED_EQ_NULL = 23`, `OP_BRANCH_LINKED_NE_NULL = 24`.
Bridge unit tests.

**Day 2 (~4h)** — Two new stencils + 1 shared helper.
Stencils mirror Phase 5.0's pattern; helper reads
`cheapMemory[0]`'s AttributeHeader instead of `readAttributes`'s
output. New cheapMemory-access friend wrapper in
DbtupJitGlue.

**Day 3 (~2h)** — MTR canary extension. Tricky: getting an
embedded block to use `BRANCH_LINKED_*_NULL` requires a CTE
with a linked column being null-checked. Shape:
`SELECT SUM(c2) FROM cte JOIN t WHERE cte_col IS NULL`.

**Day 4 (~2h)** — drift, results doc, plan flip.

**Total Phase 5.1a: 3-4 days**.

## Day breakdown — Phase 5.1b (ATTR_OP_ATTR + ATTR_OP_PARAM)

Compare-two-things branches without inline arg data.

**Day 0 (~½ day)** — Investigation spike (I1, I2, I3).

**Day 1 (~5h)** — `BRANCH_ATTR_OP_ATTR`: bridge translation,
stencil with attr_id_1/attr_id_2 narrow holes, helper that
reads both columns and dispatches on cond.

**Day 2 (~4h)** — `BRANCH_ATTR_OP_PARAM`: bridge translation,
stencil with attr_id/paramNo narrow holes, helper that reads
the column + accesses the param buffer (new friend wrapper for
param access).

**Day 3 (~3h)** — MTR canary extensions. Hand-derive expected
results.

**Day 4 (~2h)** — drift, results doc.

**Total Phase 5.1b: 4-5 days**.

## Day breakdown — Phase 5.1c (ATTR_OP_ARG + MEM family)

The big one. Variable-length inline arg data + cheapMemory
plumbing + READ_LINKED_TO_MEM admission.

**Day 0 (~1 day)** — Investigation spike (I4, I6).

**Day 1 (~4h)** — `prog_buf` plumbing. Extend `dbtup_jit_call_ctx`
with `prog_buf` pointer; extend `dbtup_jit_invoke` signature;
update JoinAggInterpreter caller.

**Day 2 (~5h)** — `BRANCH_ATTR_OP_ARG`: bridge translation
(needs to record arg-word-offset into `m_prog`); stencil with
attr_id/arg_offset narrow holes; helper that reads attr +
reads inline arg via `ctx->prog_buf` + dispatches on cond.

**Day 3 (~5h)** — `READ_LINKED_TO_MEM` admission + helper. New
OpKind, new cold-call stencil (no branch — just `bl + tail`),
new helper that delegates to NDB's existing routine.

**Day 4 (~5h)** — `BRANCH_MEM_OP_ARG`: bridge translation,
stencil, helper that reads from `cheapMemory[0]` + reads inline
arg + dispatches.

**Day 5 (~5h)** — `BRANCH_MEM_OP_ARG_INLINE_TYPE`: similar to
above but with inline type/length/charset info.

**Day 6 (~3h)** — MTR canary extensions covering each family.
Shape: `WHERE attr op const`, `WHERE cte_col op const`, etc.

**Day 7 (~2h)** — drift, results doc.

**Total Phase 5.1c: 7-8 days**.

## Total scope and recommended sequencing

| Sub-phase | Effort | Risk | Cumulative coverage |
|---|---:|---|---|
| 5.1a (LINKED_*_NULL) | 3-4 d | low | + WHERE on linked-column NULL checks |
| 5.1b (ATTR_OP_ATTR + OP_PARAM) | 4-5 d | medium | + WHERE col1 = col2, WHERE col = ? |
| 5.1c (ATTR_OP_ARG + MEM family) | 7-8 d | high | + WHERE col = const, WHERE cte_col op const |

**Recommendation**: ship 5.1a first (~4 days). Re-evaluate
priority for 5.1b vs 5.1c based on which query patterns are
most common in real workloads.

## Verification approach (per sub-phase)

Each sub-phase mirrors Phase 5.0's verification:
- Bridge unit tests (~3-5 new cases per opcode admitted).
- Cold-call unit test (at least one runs JIT'd code through
  the new helper with a stub ctx).
- MTR canary extension covering the new query shape.
- All Phase 4-5.0 verification gates retained.

## Risks

1. **Day 0 spikes overrun if the NDB encoding turns out to be
   subtly different than Phase 5 plan §4 sketches.** Mitigation:
   commit to Phase 5.1a first (LINKED_*_NULL) — its NDB
   encoding is just the opcode + branch_offset (single word),
   nothing tricky. Earns spike experience cheaply.

2. **`cheapMemory` access requires a friend wrapper.** Phase 4
   added `readAttributeForJit` as a friend method on Dbtup;
   Phase 5.1a follows that pattern but on `JoinAggInterpreter`'s
   private members. Could ripple if cheapMemory turns out to
   live somewhere else.

3. **`prog_buf` lifetime.** The SETUP record is supposed to
   outlive `ProcessRec`. If a future change makes the prog_buf
   pointer invalidate mid-row (unlikely but possible), our
   helpers would dereference freed memory. Phase 5.1c spike
   should add an assertion or sentinel check.

4. **Variable-length inline arg data scaling.** `BRANCH_ATTR_OP_ARG`
   can carry up to 65k bytes (VARCHAR). Our prog_buf-offset
   strategy avoids per-program copying cost, but the helper still
   has to memcmp the bytes. For large args, this dominates the
   helper's runtime.

5. **MEM_OP_ARG_INLINE_TYPE has 3+N words, not 2+N.** The
   third word carries inline type/length/charset info. Bridge
   translation must allocate an Op for this (or pack the
   info into operand fields).

## What we'll learn from Phase 5.1

If 5.1a-c all land cleanly:
- The 3-hole pattern from Phase 5.0 was load-bearing — every
  cold-call branch family slots into the same template with no
  new mechanism.
- `prog_buf` plumbing was a one-shot — once added, ATTR_OP_ARG
  and MEM family share it.
- The bridge admission walk's recursive structure scales
  cleanly to all embedded-block opcodes.

If a sub-phase blocks:
- Probably means an NDB-side encoding or runtime detail we
  didn't catch in the original plan. Adapt the design and
  reschedule.

## Out of scope (deferred to Phase 5.2+)

- Hot-lowered embedded `BRANCH_*_REG_*` (register-register
  comparison branches). These use Phase 3's hot-branch
  pattern — no new mechanism.
- Type-state lattice + per-type stencil specialisation.
- DUMP-based JIT-vs-interp counter (orthogonal observability).

## References

- Phase 5.0 results doc: `phase_5_0_embedded_calls.md`.
- Phase 5 broader plan: `phase_5_implementation.md` §4-§7.
- NDB embedded interpreter: `Interpreter.hpp`,
  `DbtupExecQuery.cpp`, `JoinAggInterpreter.cpp`.
- Phase 5.0 bridge translation: `ndb_jit_bridge.c`'s
  `translate_embedded_block`.
- Cold-call infrastructure: `phase_4_setup_integration.md`.
