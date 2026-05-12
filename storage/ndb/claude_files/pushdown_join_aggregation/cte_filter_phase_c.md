# Phase C — Aggregation interpreter reuses the jump-table interpreter

> **Prereq reading**: [cte_filter_plan.md](cte_filter_plan.md) — architecture decision (three tables, one function). Then [cte_filter_phase_a.md](cte_filter_phase_a.md) and [cte_filter_phase_b.md](cte_filter_phase_b.md) — Phase C must not merge until A and B regression suites are green.

This phase is a pure refactor: delete the aggregation interpreter's inline embedded-program execution and have it delegate to a generalized `Dbtup::interpreterJumpTable` with a new handler table `s_agg_interp_handlers` and the `IFLAG_DISALLOW_BACKWARD_JUMPS` flag. No user-visible behaviour change. Success criterion: **all existing aggregation tests pass unchanged**.

---

## C.1 — Generalize `interpreterFilterCte` → `interpreterJumpTable`

**Goal**: the interpreter loop accepts the handler table and mode flags as parameters instead of hardcoding `s_cte_filter_handlers` + filter-only behaviour.

### Files

- **MODIFY** `storage/ndb/src/kernel/blocks/dbtup/Dbtup.hpp` (near line 2933): declare new signature, keep old for callers:

```cpp
enum InterpreterJumpTableFlags {
  IFLAG_REJECT_RETURNS_NEG        = 0x1,  // EXIT_REFUSE → INTERPRETER_FILTER_REJECT
  IFLAG_DISALLOW_BACKWARD_JUMPS   = 0x2,  // enforce forward-only branches
};

int interpreterJumpTable(Signal*, KeyReqStruct*,
                         Uint32* main, Uint32 mainLen,
                         Uint32* sub, Uint32 subLen,
                         Uint32* tmp, Uint32 tmpSz,
                         const InterpreterHandler* tbl, Uint32 flags);

/* Preserved declaration — now a wrapper */
int interpreterFilterCte(Signal*, KeyReqStruct*,
                         Uint32* main, Uint32 mainLen,
                         Uint32* sub, Uint32 subLen,
                         Uint32* tmp, Uint32 tmpSz);
```

- **MODIFY** `DbtupExecQuery.cpp`: move the loop body from `interpreterFilterCte` into `interpreterJumpTable`. Rewrite `interpreterFilterCte` as:

```cpp
int Dbtup::interpreterFilterCte(Signal* s, KeyReqStruct* r,
                                Uint32* m, Uint32 ml, Uint32* sub, Uint32 subl,
                                Uint32* t, Uint32 ts) {
  return interpreterJumpTable(s, r, m, ml, sub, subl, t, ts,
                              s_cte_filter_handlers,
                              IFLAG_REJECT_RETURNS_NEG);
}
```

- Activate the **backward-jump guard** in the loop (the `prevPC` capture was placed dormant in Phase A.3):

```cpp
if (likely(rc == INTERP_CONTINUE)) {
  if ((flags & IFLAG_DISALLOW_BACKWARD_JUMPS) &&
      unlikely(TprogramCounter < prevPC)) {
    terrorCode = ZBACKWARD_JUMP_NOT_ALLOWED;
    return -1;
  }
  continue;
}
```

- **Drop the 16000-instruction fuse when `IFLAG_DISALLOW_BACKWARD_JUMPS` is set** — programs without backward jumps terminate by construction. The loop condition becomes:

```cpp
while ((flags & IFLAG_DISALLOW_BACKWARD_JUMPS) ? true : (RnoOfInstructions < 16000)) {
  …
}
```

- **Add error code** in `Dbtup.hpp` (or wherever `ZNO_INSTRUCTION_ERROR` lives):
  ```cpp
  static constexpr Uint32 ZBACKWARD_JUMP_NOT_ALLOWED = /* next free number */;
  ```

### Verify

Compile. All `testCteNdbApiFilter` and `testCteNdbApi` tests from Phase A/B must continue to pass (wrapper is behaviorally identical). Run the full regression:

```bash
./runtime_output_directory/testCteNdbApiFilter -c localhost:1186 -m 3306 -v
./runtime_output_directory/testCteNdbApi        -c localhost:1186 -m 3306 -v
```

---

## C.2 — Build the aggregation handler table

**Goal**: produce `s_agg_interp_handlers` that mirrors the whitelist from `AggInterpreter::validateEmbeddedProgram` (`AggInterpreter.cpp:150-247`).

### Files

- **MODIFY** `DbtupExecQuery.cpp`: add a second file-scope table next to `s_cte_filter_handlers`:

```cpp
static const InterpreterHandler s_agg_interp_handlers[INTERP_HANDLER_TABLE_SIZE] = {
  /* Accepted (from AggInterpreter whitelist at AggInterpreter.cpp:163-208): */
  /* READ_ATTR_INTO_REG=1 */ &Dbtup::InterpreterContext::handleReadAttrIntoReg,
  /* LOAD_CONST_NULL=3 */    &Dbtup::InterpreterContext::handleLoadConstNull,
  /* LOAD_CONST16=4 */       &Dbtup::InterpreterContext::handleLoadConst16,
  /* LOAD_CONST32=5 */       &Dbtup::InterpreterContext::handleLoadConst32,
  /* LOAD_CONST64=6 */       &Dbtup::InterpreterContext::handleLoadConst64,
  /* ADD_REG_REG=7 */        &Dbtup::InterpreterContext::handleAddRegReg,
  /* SUB_REG_REG=8 */        &Dbtup::InterpreterContext::handleSubRegReg,
  /* BRANCH=9 */             &Dbtup::InterpreterContext::handleBranch,
  /* BRANCH_REG_EQ_NULL=10 */ &Dbtup::InterpreterContext::handleBranchRegEqNull,
  /* BRANCH_REG_NE_NULL=11 */ &Dbtup::InterpreterContext::handleBranchRegNeNull,
  /* BRANCH_{EQ,NE,LT,LE,GT,GE}_REG_REG */
  &Dbtup::InterpreterContext::handleBranchEqRegReg,
  &Dbtup::InterpreterContext::handleBranchNeRegReg,
  &Dbtup::InterpreterContext::handleBranchLtRegReg,
  &Dbtup::InterpreterContext::handleBranchLeRegReg,
  &Dbtup::InterpreterContext::handleBranchGtRegReg,
  &Dbtup::InterpreterContext::handleBranchGeRegReg,
  /* EXIT_OK=18 */           &Dbtup::InterpreterContext::handleExitOk,
  /* BRANCH_ATTR_OP_ARG=23 */ &Dbtup::InterpreterContext::handleBranchAttrOpArg,
  /* BRANCH_ATTR_EQ_NULL=24 */ &Dbtup::InterpreterContext::handleBranchAttrEqNull,
  /* BRANCH_ATTR_NE_NULL=25 */ &Dbtup::InterpreterContext::handleBranchAttrNeNull,
  /* WRITE_INTERPRETER_OUTPUT=123 (overflow) */ &Dbtup::InterpreterContext::handleWriteInterpreterOutput,
  /* All other slots: nullptr (rejected by handler-table lookup at runtime) */
};
```

Confirm that each of these handlers exists in the extracted set at `DbtupExecQuery.cpp:5873-8687`. If any is missing, extract it (following the extraction pattern already used for the ~97 existing handlers).

### Why include `READ_ATTR_INTO_REG` and `BRANCH_ATTR_OP_ARG` here but not in the CTE table

`AggInterpreter::ProcessRec` runs against real tuples — the handlers work. CTE filter mode runs against virtual CTE rows (no real tuple) — same handlers would crash on `operPtrP` / `tablePtrP`.

---

## C.3 — Retrofit `AggInterpreter::ProcessRec`

**Goal**: delete the embedded-program inline execution inside `AggInterpreter::ProcessRec` and call `interpreterJumpTable` instead.

### Files

- **MODIFY** `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.cpp` (`AggInterpreter::ProcessRec`, starting ~line 1148 and spanning ~2000 lines):
  - Identify the branch that handles embedded interpreter programs (look for `kOpEmbeddedInterp` or equivalent — the branch that executes user-supplied WHERE / CASE bytecode embedded in the aggregation program).
  - Replace the inline execution with:

  ```cpp
  const int rc = block_tup->interpreterJumpTable(
      signal, req_struct,
      embProg, embLen, nullptr, 0,
      tmpArea, tmpAreaSz,
      s_agg_interp_handlers,
      IFLAG_DISALLOW_BACKWARD_JUMPS);
  if (rc < 0 && rc != Dbtup::INTERPRETER_FILTER_REJECT) {
    /* propagate error */
    return rc;
  }
  /* rc >= 0 → accept row; rc == INTERPRETER_FILTER_REJECT → treat as filter reject */
  ```

  The aggregator state machine (GBHashEntry, accumulators, `kOp*` ops like Plus/Sum/Count) stays in `AggInterpreter` — only the **embedded interpreter program** delegates. No state-transfer complexity: the interpreter only reads columns and writes to registers / heap memory; the AggInterpreter loop continues handling its own aggregator ops in its own switch.

  `WRITE_INTERPRETER_OUTPUT` communicates back to `AggInterpreter` via `req_struct->m_interpreter_output` — no change needed (handler already exists).

- **MODIFY** `AggInterpreter::validateEmbeddedProgram` at `AggInterpreter.cpp:150-247`:
  - **Delete** the opcode whitelist switch (the handler table is now authoritative — `nullptr` slot → rejection at runtime).
  - **Keep** the branch-target-bounds-check (separate concern).
  - **Delete** the forward-only enforcement check at line ~232 (moves to runtime via `IFLAG_DISALLOW_BACKWARD_JUMPS`).
  - Or: replace the body with a single pass that checks `s_agg_interp_handlers[opcode] != nullptr` — early rejection at Init time without duplicating a whitelist.

  Choose based on whether we prefer:
  - **Early rejection at ParseDA / Init**: do the nullptr-walk pre-run, reject bad programs immediately. Better user experience.
  - **Late rejection at runtime**: trust the runtime check. Simpler code, same safety.

  Recommend the early-rejection path — it keeps error reporting close to the user's `qb->prepare()` call.

---

## C.4 — Phase C tests

**Goal**: prove no regression in aggregation; add two new negative tests for the new guards.

### Regression

Run the full aggregation suite — every test must pass unchanged:

```bash
make -j$(sysctl -n hw.ncpu) testJoinAgg testJoinAggSpj testJoinAggNdbApi \
     testCaseAgg testOuterJoinAggNdbApi testMultiOuterJoinAggNdbApi \
     benchJoinAgg bench_q12_tpch bench_q12_dbtc bench_q9_dbtc

./runtime_output_directory/testJoinAgg -c localhost:1186 -v
./runtime_output_directory/testJoinAggSpj -c localhost:1186 -m 3306 -v
./runtime_output_directory/testJoinAggNdbApi -c localhost:1186 -m 3306 -v
./runtime_output_directory/testCaseAgg -c localhost:1186 -v
./runtime_output_directory/testOuterJoinAggNdbApi -c localhost:1186 -m 3306 -v
./runtime_output_directory/testMultiOuterJoinAggNdbApi -c localhost:1186 -m 3306 -v
./runtime_output_directory/bench_q12_tpch -c localhost:1186 -v       # perf ±1%
./runtime_output_directory/bench_q12_dbtc -c localhost:1186 -m 3306 -v
./runtime_output_directory/bench_q9_dbtc  -c localhost:1186 -m 3306 -v
```

### New negative tests (in an existing aggregation test file, e.g. `testJoinAggNdbApi.cpp`)

| Test | Validates |
|---|---|
| `testAggUnsupportedOpcode` | Submit embedded program with `WRITE_ATTR_FROM_REG` (opcode 2 — not in `s_agg_interp_handlers`). Expect `ZNO_INSTRUCTION_ERROR` either at Init (if early rejection) or at runtime |
| `testAggBackwardJump` | Submit embedded program with a backward branch. Expect `ZBACKWARD_JUMP_NOT_ALLOWED` |

Optional (stretch): flip Phase A's `testCteLookupFilterBackwardJump` (from `cte_filter_phase_a.md` test #12) to be called with `IFLAG_DISALLOW_BACKWARD_JUMPS` in the CTE filter mode too. **Do not do this unless the user asks** — the user's stated scope keeps backward jumps allowed in CTE filters.

---

## Phase C done when

- All aggregation regression tests pass unchanged.
- `testAggUnsupportedOpcode` and `testAggBackwardJump` pass.
- `AggInterpreter::ProcessRec`'s embedded-program execution delegates fully to `interpreterJumpTable`.
- `validateEmbeddedProgram` is simplified (or removed in favour of runtime nullptr-check).
- `interpreterFilterCte` becomes a thin wrapper around `interpreterJumpTable`.
- `bench_q12_tpch` / `bench_q9_dbtc` performance unchanged (±1% from Phase B baseline).

## Files touched (checklist)

- [ ] `storage/ndb/src/kernel/blocks/dbtup/Dbtup.hpp` (new signature + flags enum + error code)
- [ ] `storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp` (generalize loop, new `s_agg_interp_handlers` table)
- [ ] `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.cpp` (`ProcessRec` delegates; `validateEmbeddedProgram` simplified)
- [ ] (existing test file, e.g. `testJoinAggNdbApi.cpp`) — two new negative tests
