# Phase 7 — SCAN_FRAGREQ scan-filter JIT (runtime glue)

**Status: first vertical slice + follow-ups implemented & verified
(2026-06-10).** Branch `RONDB-1056-compiled-interpreter`. Mikael reported
`bridge_tests` (incl. T36/T38/T39/T40) and the `rondb_jit_scan_filter_canary`
MTR all passing after the two bring-up fixes (reject code; EXIT_OK lowering)
and the two follow-ups (linked-op rejection; per-program EXIT_REFUSE code
capture). `WHERE col IS [NOT] NULL` JIT-compiles, returns correct rows, runs
through the JIT path under ERROR_INSERT 4060, rejects with the program's own
refuse code, and rejects linked ops. Remaining: real comparison predicates
(see "Next" — needs Phase 5's embedded-branch family).

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
5. **Reject code: captured from the program** (done 2026-06-10). The bridge
   now reads each `EXIT_REFUSE`'s code (`inst >> 16` — the field the
   interpreter's `handleExitRefuse` reads) and returns it via
   `out_reject_code`; the runtime `TUPKEY_abort`s with the program's actual
   code, so the JIT reject behaves exactly like the interpreter (the LQH
   scan layer decides skip-row vs abort-scan from the code; 626/899 ⇒ skip).
   A boolean WHERE filter rejects with one uniform code, so a single
   per-program value suffices; a program whose `EXIT_REFUSE` words carry
   *differing* codes is rejected (`JIT_BRIDGE_UNSUPPORTED_OP` → interpreter
   fallback) since one value can't represent them. The code is cached on the
   stored procedure (`m_jit_filter_reject_code`) and copied onto the scan
   record. (True per-instruction codes — different codes hit at runtime
   within one program — would need the `OP_FILTER_REJECT_EXIT` stencil to
   carry the code into `JitState`; boolean filters never need that.)
6. **`EXIT_OK` lowering differs from aggregation** (bring-up bug, fixed).
   NdbScanFilter lays out `col IS NOT NULL` as
   `BRANCH_ATTR_EQ_NULL -> EXIT_OK -> EXIT_REFUSE` — an explicit `EXIT_OK`
   *before* the `EXIT_REFUSE`. The aggregation-embedded bridge lowers
   `EXIT_OK` to **no Op** (accept = fall through to accumulator ops), which
   for a scan filter let an accepted (fall-through) row run straight into
   the following `OP_FILTER_REJECT_EXIT` — so **every row was rejected**
   (the first canary run returned 0 rows for both IS NULL and IS NOT NULL).
   Fix: `translate_embedded_block` takes an `exit_ok_kind` param; the
   scan-filter path passes `OP_EXIT` so `EXIT_OK` lowers to the accept
   terminator (function return, `row_filter_rejected == 0`), while the
   aggregation path keeps the fall-through (`BR_EXIT_OK_FALLTHROUGH`).
   Regression test: `bridge_tests.c` T38. The earlier scan-filter tests
   (T35/T36) used a branch-over-refuse shape with no explicit `EXIT_OK`, so
   they passed while real filters failed — translation-only tests didn't
   exercise the accept fall-through.

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
  a JIT-on differential. **Verified passing 2026-06-10** (the committed
  `.result` matched — no `--record` needed).

## Scope / known limitations (v1)

- **Only the NULL-branch subset compiles.** The scan-filter bridge admits
  `BRANCH_ATTR_*_NULL`, `READ_LINKED_TO_MEM`, `BRANCH_LINKED_*_NULL`,
  `EXIT_OK`, `EXIT_REFUSE`. So the first JIT-able filters are
  `WHERE col IS [NOT] NULL`. Richer predicates (`col > 5`, `col = 'x'`,
  ranges, AND/OR of comparisons) need **Phase 5's full embedded-branch
  family** (`ATTR_OP_ATTR / OP_PARAM / OP_ARG`, MEM family), still open.
  Until then those filters stay on the interpreter (correct, just not
  accelerated).
- **Linked ops are rejected on the scan-filter path (done 2026-06-10).**
  `READ_LINKED_TO_MEM` / `BRANCH_LINKED_*_NULL` helpers need
  `ctx->join_agg`, which is null on the scan path. `translate_embedded_block`
  now takes an `allow_linked_ops` flag; the scan-filter caller passes 0, so
  these ops return `JIT_BRIDGE_UNSUPPORTED_OP` at translate time (the program
  stays on the interpreter) instead of compiling a helper that would
  dereference a null context. Aggregation/test paths pass 1 (unchanged).
  Linked attrs only arise from pushdown joins (JOIN_AGG), never plain
  SCAN_FRAGREQ filters, so this is a safety guard that shouldn't fire in
  practice. Regression: `bridge_tests` T36 (now a reject test).
- **No standalone CASE disposition.** `translate_scan_filter` already
  rejects `WRITE_INTERPRETER_OUTPUT` skip-offsets (`n_pending_case_jumps`).
- **Mixed per-instruction EXIT_REFUSE codes** aren't supported: a single
  per-program code is captured and programs with differing codes fall back
  to the interpreter. True per-instruction (different codes hit at runtime)
  would need a stencil-carried `JitState` field; boolean WHERE filters use
  one uniform code so this isn't needed in practice.

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

1. ~~Confirm `WHERE col IS [NOT] NULL` pushes down + compiles + returns
   correct rows.~~ **DONE 2026-06-10** — canary passes; the filter pushes
   down, JIT-compiles, and runs through the JIT path under 4060.
2. ~~Tighten `translate_scan_filter` to reject linked ops.~~ **DONE
   2026-06-10** — `allow_linked_ops` flag; scan path rejects
   READ_LINKED_TO_MEM / BRANCH_LINKED_*_NULL at translate (bridge_tests T36).
3. ~~Capture the EXIT_REFUSE code instead of assuming 626.~~ **DONE
   2026-06-10** — bridge returns `out_reject_code`, plumbed to the runtime;
   mixed-code programs fall back (bridge_tests T39/T40).
4. Lands on top of Phase 5's full embedded-branch family to JIT real
   comparison predicates (`col > 5`, `col = 'x'`), not just NULL tests —
   the big one for real-world coverage.
