# Phase 5F — string MIN/MAX without a string register model

**Status: PLANNED (2026-08-20). Code: not started.**

## The demand (census-confirmed)

`MIN/MAX(VARCHAR)` is pushed by the SQL planner and falls back today
(reason=2 NON_BIGINT at kOpLoadCol) — the last confirmed-demand shape
on the census scoreboard. The optimizer deliberately keeps strings
out of the typed tracks (registers stay UNDEFINED), so the wire is
always `kOpLoadCol(CHAR/VARCHAR/LONGVARCHAR)` + generic
`kOpMin`/`kOpMax`.

## Why a string register model is the WRONG shape here

The interpreter's string machinery (audited 2026-08-20):

- The LOAD (`loadColumnTypedFromBuf`, protected) captures pointer /
  length / prefix_bytes / declared_size / CHARSET into
  `m_register_string_data[reg]` alongside the register's
  type/null flags — charset derivation happens inside it.
- The KERNEL (`minMaxString`, PUBLIC) does everything else: NULL
  skip, lazy `m_string_results` sidecar allocation, first-touch slot
  metadata, collation-aware compare via `NdbSqlUtil::getType(...)
  .m_cmp`, and winner-buffer management (`[len][cap]` header,
  in-place overwrite or alloc-then-free replace) — mutating the
  group's `AggResItem.val_ptr` DIRECTLY.
- The SEND path keys the `AGG_CHAR_RESULT` wire format (appended
  payload region) off `hasStringSlots()`; DBLQH's emit/eviction
  paths and the API's `resolveStringSlots`/string merge already
  handle everything downstream.

Every step is collation calls and buffer management — pure cold-call
territory with zero hot-loop arithmetic. Modeling strings in
`regs_i64`/`acc_i64` would mean reinventing all of the above behind
the copy-in/writeback protocol, for no speed win on the string ops
themselves. The JIT's actual value for string queries is keeping the
REST of a mixed program hot (`SUM(int), MIN(str), COUNT(*)` — today
the whole program falls back because of one string aggregate).

## Design: FUSE load + kernel into one cold call per string aggregate

1. **One public façade method** on `AggInterpreterBase`:
   `jitMinMaxStringCol(block_tup, req_struct, col_id, agg_index,
   is_max, agg_res_ptr)` — resets `m_attr_read_pos`, reads the
   column into the interpreter's own per-LDM attr scratch (the
   kOpLoadCol arm's read), calls the protected
   `loadColumnTypedFromBuf` into a scratch register (the JIT
   dispatch path never runs the interpreter loop, so clobbering
   `m_registers[N]` is harmless; the capture-then-copy is contained
   per call — `minMaxString` copies the payload into its own buffer
   before returning), then calls the PUBLIC `minMaxString`. Exact
   kernel reuse: NULL skip, charsets, sidecar, buffers, and the
   whole AGG_CHAR wire/eviction/API pipeline come for free.
2. **One cold-call stencil** `op_minmax_str_ndb`:
   `ndb_jit_h_minmax_str(s, col_id, packed)` with
   packed = `(is_max << 8) | agg_index` — two narrow holes
   (HK_OP_B = col_id, HK_OP_C = packed) + the coldcall, the plain
   load-stencil shape (STRIP_TAIL, no branch). The helper reaches
   the interpreter instance via `ctx->agg` (base-class call — works
   for JoinAggInterpreter too) and the row's `agg_res_ptr` via a new
   `dbtup_jit_call_ctx::agg_res_ptr` field set per row by
   `dbtup_jit_invoke`. A non-zero kernel return
   (ZAGG_ALLOC_MEM_FAILED) sets `row_fallback` — the interpreter
   re-runs the row and surfaces the exact error.
3. **Writeback discipline — the key trick**: the helper never sets
   `value_updated`, so the glue's masked writeback SKIPS the string
   slots entirely; the kernel has already mutated the `AggResItem`
   in place, exactly as the interpreter would have. Mixed programs
   need no coordination: int/double/unsigned slots ride the masks,
   string slots ride the kernel.
4. **Bridge fusion**: on `kOpLoadCol` with a string type, look ahead
   and consume the consecutive `kOpMin`/`kOpMax` ops reading that
   register, emitting one fused `OP_MINMAX_STR_NDB` per consumer
   (re-reading the attr per consumer — cold path, semantically
   identical). The register is marked with a new tracker state
   `BR_REG_STR`; ANY other consumer rejects (UNSUPPORTED — the
   planner never emits string arithmetic). The accumulator slot
   claims a new `BR_ACC_STR` family (mixed-arm defense). A string
   load with no Min/Max consumer rejects as today.
5. **NULL rows are 4060-safe**: the kernel's null skip means no
   fallback of any kind — nullable string MIN/MAX is pure JIT from
   day one (unlike the numeric families' 5D journey).

## In-slice audits

- `LONGVARCHAR`'s type constant + whether string kOpLoadCol is ever
  multi-word (believed 1 word — only DECIMAL has the extra word).
- The scratch-register choice (any fixed index; verify kRegTotal).
- Grouped path: confirm group-record string slots + eviction flow
  purely off the instance state the kernel maintains (believed yes).
- Collation coverage in the canary: a case-insensitive collation
  column where byte order ≠ collation order (e.g. 'ALPHA' vs
  'alpha') proving `m_cmp` runs, plus a differential vs pushdown
  OFF.

## Slices

### 5F-1 — local-column string MIN/MAX (regen, 1 stencil)

Everything above. Tests: bridge (fusion shapes: single MIN, MIN+MAX
same column, string load with a non-minmax consumer rejects,
mixed program SUM(int)+MIN(str) fuses the string part and lowers the
rest hot), coldcall (mock records col/packed; direct-write discipline
— value_updated untouched), canary `rondb_jit_string_canary`
(scalar + grouped + nullable + collation-sensitivity under 4060 +
counter deltas; mixed-program must-JIT), census `string_min` flips
1 → 0. Full sweep.

### 5F-2 — linked-column strings (join-agg) — DEFERRED

The linked READ path has its own capture flow; defer until fallback
data shows join-agg string MIN/MAX demand.

## Non-goals

- String CONCAT/expressions, LIKE lowering (scan-filter LIKE stays
  the census's durable negative example).
- A string register representation in JitState — explicitly rejected
  above.
