# Phase 7 — scan-filter comparison predicates (`WHERE col <op> const | ?`)

**Status: OP_ARG verified (2026-06-15); OP_PARAM added (2026-06-16).**
Extends the scan-filter JIT beyond `IS [NOT] NULL` to integer comparison
predicates. Landed:
- **`BRANCH_ATTR_OP_ARG`** — `WHERE int_col <op> const` (inline literal),
  all 6 comparators (EQ/NE/LT/LE/GT/GE). Verified: regen-stencils,
  `bridge_tests` (T41/T42), `rondb_jit_scan_filter_canary` (Q4 `v>25`,
  Q5 `v=30`, Q6). Commits `fa5464df51f` + `d1a710abb92`.
- **`BRANCH_ATTR_OP_PARAM`** — `WHERE int_col <op> ?` (bound parameter, the
  ndbcluster handler's `cmp_param` path). **No stencil regen**: reuses the
  `op_branch_attr_op_arg` stencil — the helper reads the instruction, decodes
  the opcode, and resolves the 2nd operand from the param region
  (`lookupInterpreterParameter`) instead of inline. **Verified by Mikael
  (2026-06-16):** `bridge_tests` T43 + `rondb_jit_scan_filter_canary` Q7
  (`PREPARE … WHERE v > ?`; `EXECUTE USING @lim` under 4060) pass. Commit
  `51f0f6a5718`.

Deferred: OP_ATTR (`col <op> col2`), strings/VARCHAR.

## NDB encoding (`BRANCH_ATTR_OP_ARG`, opcode 23)

`Interpreter.hpp`: `BranchCol(cond,nulls) = 23 + (nulls<<6) + (cond<<12)`.
- **Word 0:** `opcode | (nulls<<6) | (cond<<12) | (branch_offset<<16)`.
  - opcode = bits 0..5 (+ bit15 for the >63 range; 23 needs neither and
    cond≤7 keeps bit15 clear, so the bridge's `(inst&0x3F)|((inst>>15&1)<<6)`
    decode yields 23 for our comparators).
  - `nulls = (w0>>6)&0x3` (0=NULL_CMP_EQUAL, 2=IF_NULL_BREAK_OUT,
    3=IF_NULL_CONTINUE).
  - `cond = (w0>>12)&0xf` (EQ=0,NE=1,LT=2,LE=3,GT=4,GE=5; >5 not admitted).
  - `branch_offset` = bits 16..30, direction bit 31 (forward only) — **same
    branch encoding as `BRANCH_ATTR_*_NULL`**, so reuse that extraction.
- **Word 1:** `(attrId<<16) | argLen_bytes` (`getBranchCol_AttrId` = w1>>16;
  `getBranchCol_Len` = w1&0xFFFF, the literal's **byte** length).
- **Words 2..N:** inline literal data, occupying `ceil(argLen_bytes/4)` words.

Total instruction words = `2 + ceil(argLen_bytes/4)`.

## Comparison semantics (must match `handleBranchAttrOp`, DbtupExecQuery.cpp:8826)

1. Read attr → `s1` (value bytes after the AttributeHeader), `attrLen`,
   `r1_null = ah.isNULL()`.
2. Literal → `s2 = &inst[2]`, `argLen = getBranchCol_Len(w1)`,
   `r2_null = (argLen==0)`.
3. If `r1_null || r2_null`: `IF_NULL_BREAK_OUT` ⇒ take branch;
   `IF_NULL_CONTINUE` ⇒ fall through; `NULL_CMP_EQUAL` ⇒ continue with
   `res1 = r1_null&&r2_null ? 0 : r1_null ? -1 : 1`.
4. Else `res1 = (*sqlType.m_cmp)(cs, s1, attrLen, s2, argLen)` — NdbSqlUtil's
   type/charset-aware compare (handles all types; we test integer).
5. Map `cond → take` (note the interpreter's inversion):
   EQ:`res1==0` NE:`res1!=0` LT:`res1>0` LE:`res1>=0` GT:`res1<0` GE:`res1<=0`.
   Returns take(1)/fall(0).

`sqlType`/`cs` come from the table descriptor:
`tablePtrP->tabDescriptor + attrId*ZAD_SIZE`, `NdbSqlUtil::getType(typeId)`,
charset via `AttributeOffset::getCharsetFlag/Pos` + `charsetArray`.

## Design — helper reads the instruction from `prog_buf` (Strategy A)

Rather than packing cond/nulls/attr/argLen/offset into the few Op operand
fields, the **helper reads the whole instruction from the program buffer**:

- **Stencil `op_branch_attr_op_arg`** — structurally identical to
  `op_branch_attr_eq_null`: one operand hole (the instruction's word offset,
  `HK_OP_IMM` — wide, offsets can exceed 255) + `HK_BRANCH_TAKE` target +
  implicit fall-through. Calls
  `ndb_jit_h_branch_attr_op_arg(s, inst_word_off)`.
- **Helper** `ndb_jit_h_branch_attr_op_arg(JitState*, uint32_t inst_off)`:
  `inst = ctx->prog_buf + inst_off`; returns
  `ctx->block_tup->evalBranchColForJit(ctx->req_struct, inst)`
  (1 take / 0 fall). agg/join_agg stay null.
- **`dbtup_jit_call_ctx.prog_buf`** — the exec-region pointer (what
  interpreterStartLab would pass to interpreterNextLab,
  `&cinBuffer[RinstructionCounter]`). Set per-row in
  `dbtup_jit_invoke_scan_filter`. Offsets are relative to the exec region,
  i.e. the bridge's `emb_pc`. cinBuffer is valid for the row's duration.
- **Shared evaluate** `Dbtup::evalBranchColForJit(KeyReqStruct*,
  const Uint32* inst)` in DbtupExecQuery.cpp next to handleBranchAttrOp —
  does steps 1-5 above, reusing `sqlType.m_cmp` so the byte comparison is
  NDB's own (no drift); only the small null/cond glue is mirrored, with a
  cross-reference comment.

Why this beats operand-packing: 1 hole reuses the proven
narrow-operand+branch-take pattern; the helper naturally reuses the
interpreter's decode+compare; no fragile 5-into-3 packing.

## Bridge lowering (`translate_embedded_block`)

New `case BR_EMB_BRANCH_ATTR_OP_ARG` (opcode 23):
- Reject `cond > GE(5)` (LIKE/mask/etc. not supported) and backward branch
  (direction bit, mirror NULL branch). `nulls` passes through (helper
  handles it).
- `argLen = w1 & 0xFFFF`; `inst_words = 2 + ((argLen+3)>>2)`; bounds-check
  `emb_pc + inst_words <= emb_len`.
- branch target = `emb_pc + ((w0>>16)&0x7FFF)` (forward), bounds-checked.
- `emit_op(OP_BRANCH_ATTR_OP_ARG, a=0, b=0, c=0/*pass-2 target*/,
  d=0, imm=emb_pc)` — **imm = the instruction's offset within the exec
  region** (what the helper adds to prog_buf). Record
  `pending_target_emb_pc[out_op_idx] = target`.
- Advance `emb_pc += inst_words`.
- Cap `emb_pc` (inst offset) to the HK_OP_IMM range; reject if larger
  (realistic filters are tiny).
- Admitted on **both** the scan and aggregation/embedded paths (the agg
  embedded interpreter also uses BRANCH_ATTR_OP_ARG for CASE conditions).
  Note: the agg path's prog_buf must also be wired if we admit there — for
  now, set ctx.prog_buf on the agg path too (point at the agg program) or
  keep agg admission gated off until wired. **Decision: wire scan first;
  leave the agg embedded path emitting OP_EXIT/reject as today by NOT
  adding OP_ARG to the agg-embedded translate** until its prog_buf is set.

## Magic generation

The new stencil's offset hole is `HK_OP_IMM`. On x86_64 it's a
`mov reg32, imm32` relocation-driven hole (symbol-table entry
`HOLE_BAOA_OFF → HK_OP_IMM`), no magic. On aarch64 a 32-bit imm uses a
W-form MOVZ+MOVK chain → a `kHoleMagicTable` entry like the LCI32 vals;
generate `MAGIC_BAOA_OFF_32` via the documented salt
`sha256("RONDB-1056-Phase7-32slot-magic-v1|MAGIC_BAOA_OFF_32")[0:4]` LE,
regen if either 16-bit half is zero. The branch target is a relocation
(no magic). Confirm against `extract_stencils` + `audit_magics` after regen.

## Staging

1. **Foundational (compiles, inert):** ctx.prog_buf, shared evaluate,
   helper + registration, dbtup_jit_invoke_scan_filter signature +
   interpreterStartLab pass-through. Nothing emits the new op yet.
2. **Activate (needs regen):** OP_BRANCH_ATTR_OP_ARG OpKind + jit1 dispatch
   + stencil + magic + bridge lowering + audit. `regen-stencils` + build.
3. **Tests:** bridge_tests (lower), coldcall_tests (execute each comparator),
   MTR canary (`WHERE n > k` etc. under 4060 + correctness).

## Risks

- Getting `m_cmp` operands exactly right (byte pointers/lengths after the
  AttributeHeader) — mirror handleBranchAttrOp precisely; coldcall_tests
  guards it.
- prog_buf lifetime: cinBuffer is the per-thread interpreter buffer, valid
  for the row; the helper runs synchronously within that window. Safe.
- aarch64 magic for the wide offset imm — verify the chain matches an
  existing 32-bit-imm stencil's shape.
- Only the integer path is canary-tested; m_cmp makes the code type-general
  but strings are unexercised (deferred scope).
