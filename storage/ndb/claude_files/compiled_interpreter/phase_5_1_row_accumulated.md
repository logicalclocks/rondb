# RONDB-1056 Phase 5.1 fix — per-aggregate `acc_accumulated` mask

**Status: planning.** Defect fix that must land before any
embedded-filter JIT path (Phase 5.0 / 5.1a) can be considered
correct. Carved out of the broader Phase 5.1 work because it
touches the JIT engine + stencil pipeline (not just bridge / new
opcodes).

Branch: `RONDB-1056-compiled-interpreter`.

## The bug

`storage/ndb/src/kernel/blocks/dbtup/DbtupJitGlue.cpp:226`
unconditionally writes accumulator metadata back to `agg_res_ptr`
after every JIT invocation:

```cpp
for (Uint32 i = 0; i < n_agg_results; i++) {
  agg_res_ptr[i].type        = NDB_TYPE_BIGINT;
  agg_res_ptr[i].is_unsigned = false;
  agg_res_ptr[i].is_null     = false;          // ← always cleared
  agg_res_ptr[i].value.val_int64 = s.acc_i64[i];
}
```

`dbtup_jit_invoke` runs once per row. The initial state set up by
`JoinAggInterpreter::Init` is `is_null = true, value = 0`.

If an embedded filter rejects every row (e.g., `WHERE c1 IS NULL`
on a table with no NULL c1 rows), the JIT'd code takes an early
`OP_EXIT` for each row and never executes `OP_SUM_BIGINT`. The
writeback runs anyway and stomps `is_null` to `false`. Final
result: `SUM = 0`. SQL semantics require `SUM = NULL` for an
aggregate over zero matching rows.

The interpreter path is correct because its accumulator-update
logic is structurally guarded by the embedded-block exit code
(only EXIT_OK leads to accumulation). The JIT collapses both
EXIT_OK (continue) and EXIT_REFUSE (skip row) into a single
`OP_EXIT` and has no side-channel to distinguish them at
writeback time.

## Why per-aggregate, not per-row

A single per-row "row was accepted" flag works for Phase 5.0/5.1a
where embedded blocks gate the WHOLE row. But the NDB interpreter
already has separate exit semantics:

- **Per-row** — EXIT_OK / EXIT_OK_LAST / EXIT_REFUSE control
  whether the row contributes to ANY aggregator.
- **Per-aggregate** — embedded-interp returns can mark a single
  aggregate's value as final/NULL while leaving sibling aggregates
  free to accumulate.

Per-aggregate tracking is the long-term shape. Implementing the
per-row flag now would force a second migration when phase-5.x
adds embedded-interp opcodes that exit one aggregate. Doing it
correctly the first time costs maybe 2× the lines of code but
avoids the rework.

## Design

Add `uint32_t acc_accumulated[BC_MAX_ACCS]` to `JitState`,
parallel to the existing `acc_i64[BC_MAX_ACCS]`. Each accumulator
op (Phase 4/5: only `OP_SUM_BIGINT`) sets its slot to non-zero.
The writeback in `dbtup_jit_invoke` checks per-slot and skips
metadata for unaccumulated slots.

uint32 (not uint8) because:
1. The store from JIT code is one 32-bit `STR` with no
   byte-merge / partial-register hazard.
2. Writeback's check is a uint32 compare-to-zero, single
   instruction on both archs.
3. Memory cost is negligible (BC_MAX_ACCS=4 → 16 bytes added to
   JitState).

Field placement: append at the end of JitState so all existing
stencils' baked-in field offsets stay valid (only stencils that
reference the new field need regen).

## Concrete change set

### 1. `jit1.h` — extend JitState

Add field after `void *ctx`:

```c
uint32_t  acc_accumulated[BC_MAX_ACCS];
```

Comment: per-row mask, slot i set to 1 by accumulator ops,
checked at writeback.

### 2. `stencils_src.c` — new HOLE macro family + SUM_BIGINT change

Add a `HOLE_STORE_ACC_FLAG(name, state, value)` macro for both
arches that writes a 32-bit value to `state->acc_accumulated[idx]`.

**x86_64** (trivial, follows existing pattern):
```c
#define HOLE_STORE_ACC_FLAG(name, state, value)  \
    ((state)->acc_accumulated[HOLE(name)] = (uint32_t)(value))
```

**arm64** (needs new helper because of 4-byte stride and a
different base pointer):
```c
static inline void aarch64_store_acc_flag_(uint32_t magic_byte_off,
                                            JitState *state,
                                            uint32_t value) {
  __asm__ volatile (
    "str %w[v], [%[base], %[off]]"
    :
    : [v]    "r"  (value),
      [base] "r"  (state->acc_accumulated),
      [off]  "n"  (magic_byte_off & 0x7FFCu)   /* 4-byte aligned imm12 */
    : "memory"
  );
}
#define HOLE_STORE_ACC_FLAG(name, state, value)  \
    aarch64_store_acc_flag_(MAGIC_##name##_FOLD * 4u, (state), (value))
```

Modify `op_sum_bigint`:

```c
STENCIL op_sum_bigint(JitState *s) {
  HOLE_STORE_ACC(SUM_SLOT, s,
                 HOLE_LOAD_ACC(SUM_SLOT, s) + HOLE_LOAD_REG(SUM_SRC, s));
  HOLE_STORE_ACC_FLAG(SUM_SLOT, s, 1u);
  TAIL_NEXT(s);
}
```

Note: `SUM_SLOT` is reused as the index. The existing
`MAGIC_SUM_SLOT_FOLD` value (= slot index, used as `* 8u` for
the 8-byte-stride acc_i64 store) feeds both patches. The
patcher distinguishes the two patch sites in the stencil bytes
by their distinct *byte values* (`slot * 8` vs `slot * 4`).
Each must be findable independently by audit_magics.

### 3. `extract_stencils/audit_magics.c` — register the new magic shape

The audit walks each stencil's bytes looking for magic-encoded
imm12 instructions. Currently it knows about `* 8u` byte offsets
for acc_i64. Add entries for `* 4u` byte offsets (the new
acc_accumulated stride). Two patch sites in `op_sum_bigint`
(one for acc_i64 STR/LDR, one for acc_accumulated STR) — audit
must verify both are present and patchable.

### 4. `hole_kinds.h` — add narrow magic for the flag store

If audit treats this as a separate hole kind (e.g.,
`HOLE_SUM_SLOT_FLAG`), define the magic constant. Likely cleanest
to mint a parallel symbol so debug logging distinguishes the two
patch sites; alternative is to reuse `HOLE_SUM_SLOT` and rely on
audit's per-stride lookup.

**Decision**: separate hole kind `HOLE_SUM_SLOT_FLAG` with its
own magic. Cleaner and makes byte-level stencil dumps readable.

### 5. `jit1.c` — patch the new hole

The patcher walks the `holes_op_sum_bigint[]` table. Add one
new entry for the `HOLE_SUM_SLOT_FLAG` patch site; the patch
value is `op->a` (the same SUM_SLOT operand) but written as
`slot * 4` into the imm12 field instead of `slot * 8`.

If audit's hole-kinds table maps each kind to a fixed
patch-value formula, this is a single new row. Otherwise the
patcher needs a small switch addition.

### 6. `DbtupJitGlue.cpp` — gated writeback

```cpp
for (Uint32 i = 0; i < n_agg_results; i++) {
  if (s.acc_accumulated[i] != 0) {
    agg_res_ptr[i].type        = NDB_TYPE_BIGINT;
    agg_res_ptr[i].is_unsigned = false;
    agg_res_ptr[i].is_null     = false;
    agg_res_ptr[i].value.val_int64 = s.acc_i64[i];
  }
  /* else: row didn't contribute to this aggregate — leave
   * agg_res_ptr[i] as initialised (is_null=true on first row,
   * preserved across subsequent rows that also don't accumulate
   * this slot). Final SQL value: NULL. */
}
```

The pre-row zeroing of `s.acc_accumulated` happens via the
existing `std::memset(&s, 0, sizeof(s))` at the top of
`dbtup_jit_invoke` (line 223) — no change needed there.

## Verification

### Unit tests — `proto_microbench`, `admission_tests`,
`bridge_tests`, `extractor-tests`

- `proto_microbench` should regress no metric (the SUM_BIGINT
  path now does one extra 32-bit store per accumulator op).
  Threshold (≥1.5x speedup) holds; report the new vs old number
  as the regression delta.
- `extractor-tests` covers stencil extraction; the new magic
  must round-trip cleanly.
- `admission_tests` and `bridge_tests` unaffected by the change
  but should re-run as a sanity check.

### Targeted unit test — new `accumulator_mask_tests` (microbench-style)

Construct three programs that exercise the gated-writeback paths
in isolation, then drive them via the existing JIT runtime
without going through the full ndbd kernel:

| Test | Program | Expected after N rows |
|---|---|---|
| T1 — all rows accept | `LoadCol; SumBigint acc[0]; ...` | `acc_accumulated[0]=1`, value = sum |
| T2 — all rows reject | `Exit` (early)                  | `acc_accumulated[0]=0`, value = 0 |
| T3 — partial accept | branch-on-condition then SumBigint | `acc_accumulated[0]=1`, value = partial sum |

The microbench harness is the existing
`proto_microbench` infrastructure; add a new test target if it
doesn't already let us inspect post-run JitState.

### MTR canary — `rondb_jit_embedded_canary` (Phase 5.0)

The existing canary's Q1 (`WHERE c1 IS NULL`, 2 matching rows
out of 6) and Q3 (`COUNT(*)` over c1 IS NULL) already exercise
this path but currently rely on bridge fallback to the
interpreter (Phase 5.1a's bridge admits these but the JIT'd
result is wrong, masked by the canary not enabling 4060). Once
this fix lands:

1. Re-enable `ERROR_INSERT 4060` (JIT-fallback-fatal) at the top
   of the canary.
2. Confirm the canary passes — proves the JIT path produced
   correct SUM and COUNT values.
3. Add a Q4: `SELECT SUM(c2) FROM t WHERE c1 IS NULL` against a
   table where NO row has `c1 IS NULL`. Expected:
   `SUM = NULL`. Without this fix, value would be 0 with
   is_null=false. With the fix, value stays NULL.

### Behavior verification — debug build

Add a `DEB_JIT` log line in the writeback loop dumping
`s.acc_accumulated[i]` per slot per row, so the
`rondb_jit_embedded_canary` log shows the mask in action.
Remove before shipping.

## Effort + risk

| Step | Effort | Risk |
|---|---|---|
| 1. JitState field | trivial | none |
| 2. Macro family + stencil change | small | none for x86_64; arm64 needs the new helper to fold cleanly into one STR (verify stencil bytes) |
| 3. audit_magics | small-medium | misregistered magic → extractor failure caught at build |
| 4. hole_kinds | small | none |
| 5. jit1 patcher | small | wrong patch value → silently broken; mitigated by accumulator_mask_tests |
| 6. Glue writeback | trivial | none |
| 7. Verification (microbench-target test, MTR fix-up) | medium | catches the patcher bug if step 5 is wrong |

**Total scope**: ~250 lines source + ~150 lines tests. ~3 days.

**Dependencies**: stencil regeneration (LLVM 20.1.8 at
`/opt/homebrew/opt/llvm@20/bin/clang` on this machine — already
configured in the build).

**Rollback**: revert the JitState field + writeback gate + stencil
change as a single commit. The JIT path returns to its current
behavior (correct except for the all-rejected-rows case).

## Sequencing

| Day | Work |
|---|---|
| 1 | Steps 1, 2 (jit1.h field + stencil_src.c macro + SUM_BIGINT change). Run `regen-stencils`; verify byte diff is just two extra instructions in `op_sum_bigint`. |
| 2 | Steps 3, 4, 5 (audit_magics + hole_kinds + jit1 patcher). Run `extractor-tests`. Build accumulator_mask_tests; verify the three program shapes write the right post-state. |
| 3 | Step 6 (writeback gate). Re-enable ERROR_INSERT 4060 in the canary; add Q4 (zero-matching-rows = SQL NULL). Confirm both canaries pass on debug_build. |

## Out of scope (deferred to later phases)

- **Per-aggregate exit opcodes** (`OP_EXIT_AGGREGATE`-style): the
  embedded-interp opcodes that mark one aggregate's value as
  final/NULL while letting siblings continue. The mask infra
  this fix lays down is the prerequisite — the new opcode would
  set `acc_accumulated[i]` AND additionally toggle a separate
  "is_null_now" bit. Out of scope here; tracked under Phase 5.x.
- **MIN/MAX/COUNT accumulator ops**: Phase 4/5 only ships SUM.
  When MIN/MAX land, each will need the same
  `HOLE_STORE_ACC_FLAG(SLOT, s, 1u)` line — trivial follow-on.
- **Multi-leaf aggregation tracking**: each leaf maintains its
  own JitState today, so the per-aggregate mask works
  per-leaf-per-row. Cross-leaf merge happens above the JIT
  layer. No change needed.
