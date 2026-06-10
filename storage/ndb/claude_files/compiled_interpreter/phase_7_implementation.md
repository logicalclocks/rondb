# Phase 7 — SCAN_FRAGREQ scan-filter JIT (runtime glue)

**Status: first vertical slice implemented (2026-06-10), pending build +
test.** Branch `RONDB-1056-compiled-interpreter`.

This doc covers the *runtime* half of Phase 7. The *translation + engine*
half landed earlier (`c955005048b`): `ndb_jit_bridge_translate_scan_filter()`
+ the `OP_FILTER_REJECT_EXIT` opcode/stencils + coldcall test T11. That code
was inert — nothing in DBTUP called it. This slice wires it into the live
scan path so a pushed-down `WHERE` filter is compiled at scan setup and run
natively per row instead of `interpreterNextLab()`.

## Verified scan-filter path (ground truth)

- **Per-row entry:** `Dbtup::interpreterStartLab(signal, req_struct)`
  (`DbtupExecQuery.cpp:5354`). The 5-word header `cinBuffer[0..4]` =
  `RinitReadLen, RexecRegionLen, RfinalUpdateLen, RfinalRLen, RsubLen`. The
  WHERE filter is the **`RexecRegionLen` exec region**, run at the
  `RexecRegionLen > 0` block (was ~5491). `op_type = regOperPtr->op_type`.
- **Exec-region offset is stable:** the filter always begins at word
  `5 + RinitReadLen`. The input-param prefix (`cinBuffer[5]>>16 == 0xFFFF`)
  is part of `RinitReadLen` and cancels in `interpreterStartLab`'s
  `RinstructionCounter` arithmetic, so we can locate the filter from the
  header alone — no param-area walk.
- **Projection runs *after* the filter:** for a `ZREAD` scan the projected
  columns are read by the `RinitReadLen` block at `DbtupExecQuery.cpp:5664`
  (after the interpreter), and any `RfinalRLen` region at ~5693 reads from
  `cinBuffer[RinstructionCounter]`. So a final-read region coexists with a
  JIT'd filter — the accept path just needs `RinstructionCounter +=
  RexecRegionLen` before falling through.
- **Accept/reject contract:** `interpreterNextLab` returns
  `req_struct->log_size` (≥0) on `EXIT_OK`; on `EXIT_REFUSE` it calls
  `TUPKEY_abort(req_struct, code)` → returns −1 + sends a TUPKEY_REF.
  `scanTupkeyRefLab` (`DblqhMain.cpp:23400`) whitelists two filter-reject
  codes as "row filtered, keep scanning": `ZNO_TUPLE_FOUND = 626` and the
  legacy `ZUSER_SEARCH_CONDITION_FALSE_CODE = 899` (`Dblqh.hpp:455,466`).
  Dblqh.hpp documents 626 as the **preferred** code for new programs; we use
  it (`TUP_NO_TUPLE_FOUND`, locally defined in `DbtupExecQuery.cpp:65`).
- **Arena:** `Dbtup::getJitArena()` / `m_jit_arena` — per-LDM-thread, the
  same arena standalone aggregation compiles into. `interpreterStartLab`
  and `scanCopyAttrinfo` both run on that thread, so compiling there is
  W^X-safe and single-threaded.
- **Attribute read:** the cold-call helpers read columns via
  `Dbtup::readSingleAttributeForJit(req_struct, col_id, buf, words)` (a thin
  public forwarder to the private `readSingleAttribute`, the same call
  `AggInterpreterBase::readAttributeForJit` makes). No `AggInterpreter`
  needed — `ctx.agg`/`ctx.join_agg` stay null on the scan path.

## Design decisions (as implemented)

1. **Compile-once on the stored procedure.** `struct storedProc` gains
   `Uint8 m_jit_filter_state` (0 untried / 1 compiled / 2 ineligible) and
   `void* m_jit_filter_entry`. The filter is translated + compiled once per
   prepared scan program and cached on the storedProc; the per-scan fast
   pointer is copied onto `Dblqh::ScanRecord::m_jit_filter_entry` at setup.
   This avoids both arena bloat (no recompile per scan) and a per-row pool
   lookup. **Reset** of both fields happens in the `ZSCAN_PROCEDURE` init
   (`DbtupStoredProcDef.cpp:177-185`) so a pooled record can never run a
   prior program's compiled filter.
2. **Compile location:** `Dbtup::scanCopyAttrinfo` (`DbtupExecQuery.cpp`),
   in a new `!m_has_pushdown` branch alongside the existing pushdown-agg
   `PushdownInterpreterFactory::Create` block. Compiles from
   `storedProc.cachedLinearAttrInfo` (robust — independent of the global
   `cinBuffer` freshness): exec region = `cache[5 + cache[0]]`, length
   `cache[1]`.
3. **Eligibility (v1):** `RexecRegionLen > 0 && RfinalUpdateLen == 0 &&
   RsubLen == 0` (a read filter, not an interpreted UPDATE, no
   subroutine/param region the bridge can't lower). `RfinalRLen` (final
   read / projection) is **allowed** — it runs after the filter.
4. **Per-row dispatch:** `interpreterStartLab` reads
   `scan_rec->m_jit_filter_entry`; if non-null, calls
   `dbtup_jit_invoke_scan_filter(this, req_struct, entry)`. Accept →
   `RinstructionCounter += RexecRegionLen`, fall through. Reject →
   `TUPKEY_abort(req_struct, TUP_NO_TUPLE_FOUND)`. Null entry → existing
   `interpreterNextLab` path, unchanged.
5. **Reject code 626** (`TUP_NO_TUPLE_FOUND`). The bridge collapses all
   `EXIT_REFUSE` to `OP_FILTER_REJECT_EXIT` (drops the per-instruction
   code); 626 is the code Dblqh.hpp recommends for new filter programs (the
   legacy 899 / `ZUSER_SEARCH_CONDITION_FALSE_CODE` is also whitelisted by
   `scanTupkeyRefLab`, but is overloaded with "rowid already allocated").
   Capturing the actual per-instruction code is a possible refinement.

## Glue (DbtupJitGlue.{hpp,cpp})

- `void *dbtup_jit_compile_scan_filter(NdbJitArena*, const Uint32* prog,
  Uint32 n_words)` — `ndb_jit_bridge_translate_scan_filter` →
  `jit1_compile` → `jit1_entry` (as void*); nullptr on non-eligible /
  compile failure (caller stays on the interpreter).
- `bool dbtup_jit_invoke_scan_filter(Dbtup*, KeyReqStruct*, JitEntry)` —
  runs the entry with `agg`/`join_agg` null; returns
  `!row_filter_rejected` (and treats any unexpected `row_overflowed`
  defensively as reject so a miscompile can't leak a row).
- `ndb_jit_h_load_col` / `ndb_jit_h_branch_attr_null` rewired to read via
  `ctx->block_tup->readSingleAttributeForJit` instead of
  `ctx->agg->readAttributeForJit` — behaviour-neutral for the aggregation
  path (same underlying call) and removes the `AggInterpreter` dependency
  so the helpers work for scans.

## Diagnostics / canary

- **ERROR_INSERT 4060** (interpreterStartLab): if a scan with a WHERE
  filter (`scan_rec != null && RexecRegionLen>0`) reaches the interpreter
  with no compiled entry, abort. Mirrors the aggregation 4060 contract —
  the canary only runs JIT-eligible filters under 4060, so any abort is a
  real compile/wiring regression.
- **MTR:** `mysql-test/suite/ndb_push_agg/{t,r}/rondb_jit_scan_filter_canary`.
  Nullable column, mixed NULL/non-NULL rows; `WHERE v IS NOT NULL` and
  `WHERE v IS NULL` under `all error 4060` (must not abort ⇒ JIT ran), plus
  a JIT-on differential. **The `.result` is best-effort (authored without
  running mysqld); run with `--record` if MTR reports a diff.**

## Scope / known limitations (v1)

- **Only the NULL-branch subset compiles.** The scan-filter bridge admits
  `BRANCH_ATTR_*_NULL`, `READ_LINKED_TO_MEM`, `BRANCH_LINKED_*_NULL`,
  `EXIT_OK`, `EXIT_REFUSE`. So the first JIT-able filters are
  `WHERE col IS [NOT] NULL`. Richer predicates (`col > 5`, `col = 'x'`,
  ranges, AND/OR of comparisons) need **Phase 5's full embedded-branch
  family** (`ATTR_OP_ATTR / OP_PARAM / OP_ARG`, MEM family), still open.
  Until then those filters stay on the interpreter (correct, just not
  accelerated).
- **Linked ops on a scan filter would mis-route.** `READ_LINKED_TO_MEM` /
  `BRANCH_LINKED_*_NULL` helpers need `ctx->join_agg`, which is null on the
  scan path. Linked attrs are an SPJ/join concept delivered via JOIN_AGG,
  not SCAN_FRAGREQ, and the path is gated to `!m_has_pushdown` scans, so
  they can't appear here — but tightening `translate_scan_filter` to reject
  linked ops outright is a cheap follow-up to remove the abort risk.
- **No standalone CASE disposition.** `translate_scan_filter` already
  rejects `WRITE_INTERPRETER_OUTPUT` skip-offsets (`n_pending_case_jumps`).
- **Per-instruction EXIT_REFUSE code** is dropped in favour of a fixed 626.

## Files touched

| File | Change |
|---|---|
| `dbtup/Dbtup.hpp` | `storedProc::{m_jit_filter_state,m_jit_filter_entry}` + `JIT_FILTER_*` consts + public `readSingleAttributeForJit` |
| `dbtup/DbtupStoredProcDef.cpp` | reset JIT fields in ZSCAN_PROCEDURE init |
| `dblqh/Dblqh.hpp` | `ScanRecord::m_jit_filter_entry` + ctor init |
| `dbtup/DbtupJitGlue.{hpp,cpp}` | scan-filter compile + invoke; helpers read via block_tup |
| `dbtup/DbtupExecQuery.cpp` | compile-on-storedProc in `scanCopyAttrinfo`; per-row dispatch + 4060 in `interpreterStartLab`; include glue |
| `mysql-test/suite/ndb_push_agg/{t,r}/rondb_jit_scan_filter_canary.*` | MTR canary |

## Build + verify

Multi-file kernel change → rebuild `ndbmtd` (the bridge/glue are kernel
code) + the host JIT unit binaries. Then:

```sh
# host unit layer (no kernel link — confirms engine/bridge unbroken)
debug_build/storage/ndb/test/jit_proto/{bridge_tests,coldcall_tests,admission_tests,proto_microbench}
# data-node layer
cd debug_build/mysql-test && ./mtr --suite=ndb_push_agg --force --nowarnings \
  rondb_jit_scan_filter_canary
# (--record first if the best-effort .result diffs)
```

## Next (Phase 7 continued)

1. Confirm `WHERE col IS [NOT] NULL` actually pushes down as an interpreted
   scan filter (`RexecRegionLen>0`) and compiles; adjust the canary /
   `.result` from a real run.
2. Tighten `translate_scan_filter` to reject linked ops (remove abort risk).
3. Capture the per-instruction EXIT_REFUSE code instead of assuming 626.
4. Lands on top of Phase 5's full embedded-branch family to JIT real
   comparison predicates, not just NULL tests.
