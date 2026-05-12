# Phase A — CTE_LOOKUP_REQ filter support

> **Prereq reading**: [cte_filter_plan.md](cte_filter_plan.md) — context, architecture decision, accepted/unsupported opcode lists, cross-phase risks. Do not start Phase A without reading it.

This phase assembles the CTE-filter dispatch table, implements `Dbtup::interpreterFilterCte`, hooks it into `execCTE_LOOKUP_REQ`, fixes the NdbQueryOperation serializer's double-ExitOK bug, and adds a test binary `testCteNdbApiFilter` with both baseline filter tests (regular key + scan ops) and CTE_LOOKUP filter tests.

---

## A.1 — Create `testCteNdbApiFilter` with baseline regular-op filter tests

**Goal**: prove the handler extraction is still healthy before touching CTE paths. If a baseline scan/key filter test fails here, any later CTE test failure can't be pinned on CTE-specific code.

### Files

- **NEW** `storage/ndb/block_unit_test/testCteNdbApiFilter.cpp` — pattern after `testCteNdbApi.cpp` (6025 lines, 21 tests). Reuse `createTestTables`, `insertTestData`, `TestEntry`, and the main runner loop from `testCteNdbApi.cpp:5935-6025`. Add a new dataset with a `tag CHAR(8)` column to cover string filters.
- **MODIFY** `storage/ndb/block_unit_test/CMakeLists.txt` (after line 60): add
  ```cmake
  NDB_ADD_EXECUTABLE(testCteNdbApiFilter testCteNdbApiFilter.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
  ```

### Tests

| # | Name | Covers |
|---|---|---|
| 1 | `testKeyOpFilterSimple` | PK read with `NdbOperation::setInterpretedCode` filter `val > 10` |
| 2 | `testKeyOpFilterReject` | Filter calls `interpret_exit_nok()`, expect ZTUPLE_ABORTED-style reject |
| 3 | `testScanFilterSingleCol` | Scan with `branch_col_le(&v, sizeof(v), col, PASS) + exit_nok + def_label(PASS) + exit_ok` (inverted-inequality, see `testCteNdbApi.cpp:1002-1019`) |
| 4 | `testScanFilterTwoCol` | `grp=2 AND val>50` (two chained branches) |
| 5 | `testScanFilterStringEq` | `branch_col_eq` on CHAR column |

### Verify

```bash
cd debug_build
make -j$(sysctl -n hw.ncpu) testCteNdbApiFilter
./runtime_output_directory/testCteNdbApiFilter -c localhost:1186 -m 3306 -v
```

---

## A.2 — Declare the CTE-filter dispatch table

**Goal**: produce a compile-time handler table that the new interpreter will use.

### Files

- **MODIFY** `storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp`: insert file-scope table between line 8688 (`typedef int (*InterpreterHandler)(...)`) and line 8690 (`int Dbtup::interpreterNextLab(...)`):

```cpp
static const InterpreterHandler s_cte_filter_handlers[INTERP_HANDLER_TABLE_SIZE] = {
  /* 0    */ nullptr,
  /* READ_ATTR_INTO_REG=1 */ nullptr,   // unsupported in CTE filter
  /* WRITE_ATTR_FROM_REG=2 */ nullptr,  // unsupported
  /* LOAD_CONST_NULL=3 */ &Dbtup::InterpreterContext::handleLoadConstNull,
  /* LOAD_CONST16=4 */     &Dbtup::InterpreterContext::handleLoadConst16,
  /* LOAD_CONST32=5 */     &Dbtup::InterpreterContext::handleLoadConst32,
  /* LOAD_CONST64=6 */     &Dbtup::InterpreterContext::handleLoadConst64,
  /* ADD_REG_REG=7 */      &Dbtup::InterpreterContext::handleAddRegReg,
  /* SUB_REG_REG=8 */      &Dbtup::InterpreterContext::handleSubRegReg,
  /* BRANCH=9 */           &Dbtup::InterpreterContext::handleBranch,
  /* BRANCH_REG_EQ_NULL=10 */ &Dbtup::InterpreterContext::handleBranchRegEqNull,
  /* BRANCH_REG_NE_NULL=11 */ &Dbtup::InterpreterContext::handleBranchRegNeNull,
  /* BRANCH_EQ_REG_REG=12 */  &Dbtup::InterpreterContext::handleBranchEqRegReg,
  /* BRANCH_NE_REG_REG=13 */  &Dbtup::InterpreterContext::handleBranchNeRegReg,
  /* BRANCH_LT_REG_REG=14 */  &Dbtup::InterpreterContext::handleBranchLtRegReg,
  /* BRANCH_LE_REG_REG=15 */  &Dbtup::InterpreterContext::handleBranchLeRegReg,
  /* BRANCH_GT_REG_REG=16 */  &Dbtup::InterpreterContext::handleBranchGtRegReg,
  /* BRANCH_GE_REG_REG=17 */  &Dbtup::InterpreterContext::handleBranchGeRegReg,
  /* EXIT_OK=18 */         &Dbtup::InterpreterContext::handleExitOk,
  /* EXIT_REFUSE=19 */     &Dbtup::InterpreterContext::handleExitRefuseCte,  // OVERRIDE
  /* CALL=20 */            &Dbtup::InterpreterContext::handleCall,
  /* RETURN=21 */          &Dbtup::InterpreterContext::handleReturn,
  /* EXIT_OK_LAST=22 */    &Dbtup::InterpreterContext::handleExitOkLast,
  /* BRANCH_ATTR_OP_ARG=23 */ nullptr,     // unsupported (no real tuple)
  /* BRANCH_ATTR_EQ_NULL=24 */ nullptr,    // unsupported
  /* BRANCH_ATTR_NE_NULL=25 */ nullptr,    // unsupported
  /* BRANCH_ATTR_OP_PARAM=26 */ nullptr,   // unsupported
  /* BRANCH_ATTR_OP_ATTR=27 */ nullptr,    // unsupported
  /* LSHIFT_REG_REG=28 */  &Dbtup::InterpreterContext::handleLshiftRegReg,
  /* RSHIFT_REG_REG=29 */  &Dbtup::InterpreterContext::handleRshiftRegReg,
  /* MUL_REG_REG=30 */     &Dbtup::InterpreterContext::handleMulRegReg,
  /* DIV_REG_REG=31 */     &Dbtup::InterpreterContext::handleDivRegReg,
  /* AND_REG_REG=32 */     &Dbtup::InterpreterContext::handleAndRegReg,
  /* OR_REG_REG=33 */      &Dbtup::InterpreterContext::handleOrRegReg,
  /* XOR_REG_REG=34 */     &Dbtup::InterpreterContext::handleXorRegReg,
  /* MOD_REG_REG=35 */     &Dbtup::InterpreterContext::handleModRegReg,
  /* NOT_REG_REG=36 */     &Dbtup::InterpreterContext::handleNotRegReg,
  /* STR_TO_INT64=37 */    &Dbtup::InterpreterContext::handleStrToInt64,
  /* BRANCH_MEM_OP_ARG=38 */  &Dbtup::InterpreterContext::handleBranchMemOpArg,  // CRITICAL
  /* READ_LINKED_TO_MEM=39 */ &Dbtup::InterpreterContext::handleReadLinkedToMem, // CRITICAL
  /* 40-46 unused */ nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
  /* READ_PARTIAL_ATTR_TO_MEM=47 */ nullptr,  // unsupported
  /* READ_ATTR_TO_MEM=48 */         nullptr,  // unsupported
  /* READ_UINT8_MEM_TO_REG=49 */   &Dbtup::InterpreterContext::handleReadUint8MemToReg,
  /* READ_UINT16_MEM_TO_REG=50 */  &Dbtup::InterpreterContext::handleReadUint16MemToReg,
  /* READ_UINT32_MEM_TO_REG=51 */  &Dbtup::InterpreterContext::handleReadUint32MemToReg,
  /* READ_INT64_MEM_TO_REG=52 */   &Dbtup::InterpreterContext::handleReadInt64MemToReg,
  /* WRITE_UINT8_REG_TO_MEM=53 */  &Dbtup::InterpreterContext::handleWriteUint8RegToMem,
  /* WRITE_UINT16_REG_TO_MEM=54 */ &Dbtup::InterpreterContext::handleWriteUint16RegToMem,
  /* WRITE_UINT32_REG_TO_MEM=55 */ &Dbtup::InterpreterContext::handleWriteUint32RegToMem,
  /* WRITE_INT64_REG_TO_MEM=56 */  &Dbtup::InterpreterContext::handleWriteInt64RegToMem,
  /* WRITE_ATTR_FROM_MEM=57 */    nullptr,  // unsupported (write to tuple)
  /* APPEND_ATTR_FROM_MEM=58 */   nullptr,  // unsupported
  /* LOAD_CONST_MEM=59 */         &Dbtup::InterpreterContext::handleLoadConstMem,
  /* CONVERT_SIZE=60 */           &Dbtup::InterpreterContext::handleConvertSize,
  /* LOAD_OP_TYPE=61 */           nullptr,  // unsupported (no op context)
  /* 62 */ nullptr,
  /* SPECIAL_INSTR=63 */          nullptr,

  /* --- OVERFLOW_OPCODE variants (64+) --- */
  /* BINARY_SEARCH_64=65 */ nullptr,
  /* BINARY_SEARCH_32=66 */ nullptr,
  /* BINARY_SEARCH_16=67 */ nullptr,
  /* BINARY_SEARCH_ODD=68 */ nullptr,
  /* SEARCH_INTERVAL_64=69 */ nullptr,
  /* SEARCH_INTERVAL_32=70 */ nullptr,
  /* ADD_REG_CONST=71 */ &Dbtup::InterpreterContext::handleAddRegConst,
  /* SUB_REG_CONST=72 */ &Dbtup::InterpreterContext::handleSubRegConst,
  /* SEARCH_INTERVAL_16=73 */ nullptr,
  /* SEARCH_INTERVAL_ODD=74 */ nullptr,
  /* STRING_SEARCH=75 */ nullptr,
  /* BRANCH_EQ_REG_CONST=76 */ &Dbtup::InterpreterContext::handleBranchEqRegConst,
  /* BRANCH_NE_REG_CONST=77 */ &Dbtup::InterpreterContext::handleBranchNeRegConst,
  /* BRANCH_LT_REG_CONST=78 */ &Dbtup::InterpreterContext::handleBranchLtRegConst,
  /* BRANCH_LE_REG_CONST=79 */ &Dbtup::InterpreterContext::handleBranchLeRegConst,
  /* BRANCH_GT_REG_CONST=80 */ &Dbtup::InterpreterContext::handleBranchGtRegConst,
  /* BRANCH_GE_REG_CONST=81 */ &Dbtup::InterpreterContext::handleBranchGeRegConst,
  /* QSORT=82 */ nullptr,
  /* COMPRESS_NUM_ARRAY=83 */ nullptr,
  /* LSHIFT_REG_CONST=92 */ &Dbtup::InterpreterContext::handleLshiftRegConst,
  /* RSHIFT_REG_CONST=93 */ &Dbtup::InterpreterContext::handleRshiftRegConst,
  /* MUL_REG_CONST=94 */    &Dbtup::InterpreterContext::handleMulRegConst,
  /* DIV_REG_CONST=95 */    &Dbtup::InterpreterContext::handleDivRegConst,
  /* AND_REG_CONST=96 */    &Dbtup::InterpreterContext::handleAndRegConst,
  /* OR_REG_CONST=97 */     &Dbtup::InterpreterContext::handleOrRegConst,
  /* XOR_REG_CONST=98 */    &Dbtup::InterpreterContext::handleXorRegConst,
  /* MOD_REG_CONST=99 */    &Dbtup::InterpreterContext::handleModRegConst,
  /* INT64_TO_STR=101 */    &Dbtup::InterpreterContext::handleInt64ToStr,
  /* READ_UINT8_REG_TO_REG=113 */   &Dbtup::InterpreterContext::handleReadUint8RegToReg,
  /* READ_UINT16_REG_TO_REG=114 */  &Dbtup::InterpreterContext::handleReadUint16RegToReg,
  /* READ_UINT32_REG_TO_REG=115 */  &Dbtup::InterpreterContext::handleReadUint32RegToReg,
  /* READ_INT64_REG_TO_REG=116 */   &Dbtup::InterpreterContext::handleReadInt64RegToReg,
  /* WRITE_UINT8_REG_TO_REG=117 */  &Dbtup::InterpreterContext::handleWriteUint8RegToReg,
  /* WRITE_UINT16_REG_TO_REG=118 */ &Dbtup::InterpreterContext::handleWriteUint16RegToReg,
  /* WRITE_UINT32_REG_TO_REG=119 */ &Dbtup::InterpreterContext::handleWriteUint32RegToReg,
  /* WRITE_INT64_REG_TO_REG=120 */  &Dbtup::InterpreterContext::handleWriteInt64RegToReg,
  /* READ_INTERPRETER_INPUT=121 */ nullptr,
  /* WRITE_PARTIAL_ATTR_FROM_MEM=122 */ nullptr,
  /* WRITE_INTERPRETER_OUTPUT=123 */ nullptr,
  /* WRITE_SIZE_MEM=124 */  &Dbtup::InterpreterContext::handleWriteSizeMem,
  /* BZERO_MEM=125 */       &Dbtup::InterpreterContext::handleBzeroMem,
  /* all other slots default to nullptr — requires C++20 or value-init array */
};
```

> Note: the exact opcode constant values are in `Interpreter.hpp:62-248`. The comment-per-slot layout above uses those constants for clarity. If designated-initializers aren't usable, populate the table via a one-shot init function called from `Dbtup::initialize` or static constructor.

### Verify

Compile only — `make -j$(sysctl -n hw.ncpu) ndbd` should succeed. No runtime test yet.

---

## A.3 — Implement `Dbtup::interpreterFilterCte(...)`

**Goal**: provide the function body for the declaration at `Dbtup.hpp:2933-2936`.

### Files

- **MODIFY** `DbtupExecQuery.cpp`: insert the function between `interpreterNextLab` (ends line 9158) and `expand_var_part`.

### Body outline

Mirror the top-of-loop setup from `interpreterNextLab` (locals + `InterpreterContext` aggregate init, `DbtupExecQuery.cpp:8699-8761`). Replace the switch with a jump-table dispatch:

```cpp
int Dbtup::interpreterFilterCte(Signal* signal,
                                KeyReqStruct* req_struct,
                                Uint32* mainProgram, Uint32 TmainProgLen,
                                Uint32* subroutineProg, Uint32 TsubroutineLen,
                                Uint32* tmpArea, Uint32 tmpAreaSz) {
  /* State setup: copy locals from interpreterNextLab:8699-8731 */
  Uint32 theRegister;
  Uint32 theInstruction;
  Uint32 TprogramCounter = 0;
  Uint32* TcurrentProgram = mainProgram;
  Uint32 TcurrentSize = TmainProgLen;
  Uint32 RstackPtr = 0;
  union { Uint32 TregMemBuffer[32]; Uint64 align[16]; };
  (void)align;
  Uint32 TstackMemBuffer[32];
  char* TheapMemoryChar = (char*)&cheapMemory[0];
  Uint32& RnoOfInstructions = req_struct->no_exec_instructions;
  ndbassert(RnoOfInstructions == 0);
  TregMemBuffer[0] = TregMemBuffer[4] = TregMemBuffer[8] = TregMemBuffer[12] =
  TregMemBuffer[16] = TregMemBuffer[20] = TregMemBuffer[24] = TregMemBuffer[28] = NULL_INDICATOR;
  Uint32 tmpHabitant = ~0;

  /* Aggregate-init the context — same pattern as interpreterNextLab:8740-8761 */
  InterpreterContext ctx{ this, signal, req_struct,
                          TcurrentProgram, TcurrentSize, TprogramCounter,
                          theInstruction, theRegister,
                          &TregMemBuffer[0], &TstackMemBuffer[0], RstackPtr,
                          TheapMemoryChar, RnoOfInstructions, tmpHabitant,
                          mainProgram, TmainProgLen, subroutineProg, TsubroutineLen,
                          tmpArea, tmpAreaSz };

  /* Jump-table loop */
  while (RnoOfInstructions < 16000) {
    if (unlikely(TprogramCounter >= TcurrentSize)) {
      terrorCode = ZOUTSIDE_OF_PROGRAM_ERROR;
      return -1;
    }
    RnoOfInstructions++;
    theInstruction = TcurrentProgram[TprogramCounter];
    theRegister    = Interpreter::getReg1(theInstruction) << 2;
    const Uint32 prevPC = TprogramCounter;  // dormant until Phase C
    TprogramCounter++;
    const Uint32 opCode = Interpreter::getOpCode(theInstruction);
    const InterpreterHandler h = s_cte_filter_handlers[opCode];
    if (unlikely(h == nullptr)) {
      terrorCode = ZNO_INSTRUCTION_ERROR;
      return -1;
    }
    jamDebug();
    jamDataDebug(opCode);
    const int rc = h(ctx);
    if (likely(rc == INTERP_CONTINUE)) {
      /* Phase C activation:
       * if (flags & IFLAG_DISALLOW_BACKWARD_JUMPS && TprogramCounter < prevPC) {
       *   terrorCode = ZBACKWARD_JUMP_NOT_ALLOWED; return -1;
       * }
       */
      (void)prevPC;
      continue;
    }
    if (rc == INTERP_EXIT) return 0;                              // accept
    if (rc == Dbtup::INTERPRETER_FILTER_REJECT) return rc;        // reject
    return -1;                                                    // error (terrorCode set)
  }
  terrorCode = ZTOO_MANY_INSTRUCTIONS_ERROR;
  return -1;
}
```

### Why this shape

- No `TUPKEY_abort` — CTE rows have no `operPtrP` / `tablePtrP`.
- `prevPC` capture is dead code in Phase A but present so Phase C is a two-line diff (uncomment).
- `nullptr` check is one predictable branch per iteration — cost-equivalent to a `default: TUPKEY_abort` in a switch.

### Verify

Compile only. No test yet.

---

## A.4 — Hook filter into `execCTE_LOOKUP_REQ`

**Goal**: DBLQH calls the filter after the hash lookup and before result delivery.

### Files

- **MODIFY** `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp:19385-19549` (`Dblqh::execCTE_LOOKUP_REQ`). After the hash lookup at line 19510 produces a non-null `groupData`, and before the agg-feed / emit decision at line 19538 / 19547, insert:

```cpp
/* Filter gate — runs on the found group row before result emit / agg feed. */
const Uint32 RinitReadLen = cinBuf[0] & 0xFFFF;  // low 16: program length
if (RinitReadLen > 1) {                          // >1 means more than just ExitOK
  jam();
  /* Build linked_attr_data from the CTE group row */
  Uint32 linkedBuf[MAX_CTE_LINKED_WORDS];
  Uint32 linkedLen = 0;
  if (!buildCteLinkedBuffer(interp, groupData, linkedBuf, &linkedLen)) {
    sendCteLookupRef(signal, req.senderRef, req.senderData, ZCTE_LOOKUP_FILTER_ERROR);
    return;
  }

  KeyReqStruct filterReqStruct;
  filterReqStruct.m_linked_attr_data    = linkedBuf;
  filterReqStruct.m_linked_attr_len     = linkedLen;
  filterReqStruct.no_exec_instructions  = 0;
  filterReqStruct.log_size              = 0;

  Uint32 tmpArea[ZATTR_BUFFER_SIZE];  // reuse existing constant
  const int rc = c_tup->interpreterFilterCte(
      signal, &filterReqStruct,
      cinBuf + 5, RinitReadLen,     // main program (skip 5-word header)
      nullptr, 0,                   // no subroutine
      tmpArea, sizeof(tmpArea) / sizeof(Uint32));
  if (rc == Dbtup::INTERPRETER_FILTER_REJECT) {
    jam();
    sendCteLookupRef(signal, req.senderRef, req.senderData,
                     ZCTE_LOOKUP_GROUP_NOT_FOUND);   // LEFT JOIN NULL-row path
    return;
  }
  if (rc < 0) {
    jam();
    sendCteLookupRef(signal, req.senderRef, req.senderData,
                     ZCTE_LOOKUP_FILTER_ERROR);
    return;
  }
  /* rc >= 0: accept — fall through to existing emit paths */
}
```

- **MODIFY** `storage/ndb/src/kernel/blocks/dblqh/Dblqh.hpp`: add next to other `ZCTE_LOOKUP_*`:
  ```cpp
  static constexpr Uint32 ZCTE_LOOKUP_FILTER_ERROR = 1266;
  ```
- **NEW helper** `Dblqh::buildCteLinkedBuffer(JoinAggInterpreter* interp, const char* groupData, Uint32* buf, Uint32* lenOut)`: extract from the existing agg-feed path (`cteLookupAggFeed` already builds an equivalent buffer). Interface: writes `[tableId, schemaVersion, AttrHeader, data, …]` entries into `buf`, sets `*lenOut` to words written, returns `false` on buffer overflow.
- **MODIFY** `ndberror.cpp` — register `NDBD_EXIT_CTE_LOOKUP_FILTER_ERROR` (or the existing error-number convention in that file) with a human-readable message like `"CTE lookup filter execution error"`.

### Ordering when filter + agg-feed coexist

Filter runs **before** `cteLookupAggFeed` (line 19538). Rejected rows don't increment the target aggregation. This is correct WHERE semantics and is enforced by hook placement.

### Back-compat

Older clients send `RinitReadLen == 1, prog[0] == ExitOK`. The `if (RinitReadLen > 1)` short-circuit means old programs bypass the filter entirely — zero new cost. Even if called, the `ExitOK` handler returns immediately with accept.

### Verify

After A.1-A.4 are all in, re-run baseline tests — they should still pass (they don't exercise CTE code).

---

## A.5 — Fix NdbQueryOperation serializer + add prepare-time validator

**Goal**: (1) fix the latent double-`ExitOK` bug so `setInterpretedCode()` on `lookupCte()` / `scanCte()` actually produces a well-formed AttrInfo; (2) reject programs that use CTE-unsafe opcodes at `qb->prepare()` time instead of at run time.

### Files

- **MODIFY** `storage/ndb/src/ndbapi/NdbQueryOperation.cpp:5536-5574` (`case QueryNodeParameters::QN_CTE_LOOKUP`): change the unconditional stub append into a conditional one. The block at lines 5547-5550 writes a 2-word header `[0u<<16|1u, ExitOK]`; guard this on `!hasInterpretedCode()`:

  ```cpp
  if (!hasInterpretedCode()) {
    requestInfo |= DABits::PI_ATTR_INTERPRET;
    attrInfo.append((0u << 16) | 1u);   // subroutine_len=0, prog_len=1
    attrInfo.append(Uint32(Interpreter::EXIT_OK));
  }
  /* else: the user program was already appended at line 5366 via
     prepareInterpretedCode(). Don't overwrite. */
  ```

  Same change in the `case QueryNodeParameters::QN_CTE_SCAN` at lines 5585-5589.

- **NEW** `storage/ndb/include/kernel/Interpreter.hpp` (append to class): a static method `bool validateCteSafe(const Uint32* prog, Uint32 len, const char** errMsg, Uint32* offendingOpcodeOut)`. Implementation in `Interpreter.cpp` (or wherever the class body lives — find via `grep "validateCteSafe"` after writing). Use `Interpreter::getInstructionPreProcessingInfo` to walk the stream. Reject opcodes not on the accepted list (mirror `s_cte_filter_handlers` nullptr-slot logic; long-term goal: single source of truth in a shared header).

- **MODIFY** `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp`: in `NdbQueryCteLookupOperationDefImpl::serializeOperation` and `NdbQueryCteScanOperationDefImpl::serializeOperation`, after `m_options.getInterpretedCode()` is available (or equivalent accessor), call `Interpreter::validateCteSafe(...)` and return a new error code `QRY_INTERPRETED_CODE_NOT_CTE_SAFE` on failure, with the offending opcode embedded in the message.

### Source-of-truth note

Two lists — server (`s_cte_filter_handlers` nullptr-check) and client (`validateCteSafe`) — will drift if maintained by hand. Long-term: extract the accepted-opcode set to a shared header/enum in `Interpreter.hpp`. Short-term: both files reference the same set of constants from `Interpreter.hpp` and a comment in each notes the other side.

### Verify

- Build `testCteNdbApiFilter` — expects the A.6 test `testCteLookupFilterUnsupportedOpcode` to fail cleanly at `qb->prepare()` with the new error.

---

## A.6 — CTE_LOOKUP filter tests

**Goal**: exercise the full filter path on CTE_LOOKUP_REQ.

### Files

Append to `testCteNdbApiFilter.cpp`. Each test follows the `testCteWithScanFilter` pattern (`testCteNdbApi.cpp:980-1191`) but applies `NdbQueryOptions::setInterpretedCode(...)` to a `lookupCte()` operation instead of `scanTable()`.

### Tests

| # | Name | SQL-equivalent | Verifies |
|---|---|---|---|
| 6 | `testCteLookupFilterSingleCol` | `WHERE cte0.val > 10` | Filter rejects some groups, accepts others |
| 7 | `testCteLookupFilterInAggFeed` | Main aggregates `cte0.val` into SUM with filter | Filter runs BEFORE agg feed (reject doesn't increment SUM) |
| 8 | `testCteLookupFilterNoMatch` | Filter rejects all groups | `CTE_LOOKUP_REF` with `GROUP_NOT_FOUND` |
| 9 | `testCteLookupFilterLeftJoin` | LEFT JOIN + filter rejecting the join row | DBSPJ emits NULL row (no DBSPJ change required — reject maps to NOT_FOUND) |
| 10 | `testCteLookupFilterMultipleCte` | Two CTEs with different filters | No cross-contamination |
| 11 | `testCteLookupFilterUnsupportedOpcode` | Build filter containing `WRITE_ATTR_FROM_REG` | `qb->prepare()` fails with `QRY_INTERPRETED_CODE_NOT_CTE_SAFE` |
| 12 | `testCteLookupFilterBackwardJump` | Filter containing a backward branch | Phase A: accepted (no guard yet). Phase C may flip this for agg mode only |

### Verify

```bash
make -j$(sysctl -n hw.ncpu) testCteNdbApiFilter
./runtime_output_directory/testCteNdbApiFilter -c localhost:1186 -m 3306 -v
```

All tests (baseline + CTE_LOOKUP) must pass. Run full CTE regression suite to prove no regression:

```bash
./runtime_output_directory/testCteNdbApi -c localhost:1186 -m 3306 -v
```

---

## Phase A done when

- `testCteNdbApiFilter` builds and all 12 tests pass.
- `testCteNdbApi` (existing 21 tests) still passes unchanged.
- No new warnings in debug build.
- `s_cte_filter_handlers`, `Dbtup::interpreterFilterCte`, filter hook in `execCTE_LOOKUP_REQ`, and serializer fix are all committed.

## Files touched (checklist)

- [ ] `storage/ndb/block_unit_test/testCteNdbApiFilter.cpp` (new)
- [ ] `storage/ndb/block_unit_test/CMakeLists.txt`
- [ ] `storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp` (~8688, ~9158)
- [ ] `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` (19385-19549)
- [ ] `storage/ndb/src/kernel/blocks/dblqh/Dblqh.hpp` (error code)
- [ ] `storage/ndb/include/kernel/Interpreter.hpp` (validator)
- [ ] `storage/ndb/src/ndbapi/NdbQueryOperation.cpp` (5536-5609)
- [ ] `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp` (validator call sites)
- [ ] `ndberror.cpp` (new error code registration)
