# CTE Filters + 3rd Interpreter (jump-table) — Overview

## Context

RONDB-1048 added linked-attribute forwarding to `CTE_LOOKUP_REQ`; it now works as both root and child operation. `CTE_SCAN_REQ` still works only as a root scan. The next step (this plan) adds **WHERE-clause filtering** to both signals, delivered by a **third interpreter** in DBTUP — a function-pointer **jump table** instead of the old `switch`/`CASE` dispatch used by `interpreterNextLab`.

Why a jump table: unsupported opcodes become `nullptr` slots in the table, so the new interpreter doesn't need case-by-case guards in the old interpreter code. CTE-specific overrides (`EXIT_REFUSE → INTERPRETER_FILTER_REJECT`) are just different function pointers. A loop-guard flag (disallow backward jumps) lets the aggregation interpreter later reuse the same machinery, collapsing three interpreters into one function with three handler tables.

**Prerequisites already in the tree** (from commits 93d7c6c023b and 58ee26e8b43):

| Item | Location | Status |
|---|---|---|
| `Dbtup::InterpreterContext` (nested struct, loop-local state) | `DbtupExecQuery.cpp:5817-5856` | done |
| ~97 extracted `static inline` opcode handlers on `InterpreterContext` | `DbtupExecQuery.cpp:5873-8687` | done |
| `INTERP_DISPATCH` macro + hoisted error/exit return-check | `DbtupExecQuery.cpp:8783-8784, 9140-9152` | done |
| `INTERP_HANDLER_TABLE_SIZE = 128` (opcode range 0-127) | `DbtupExecQuery.cpp:5806` | done |
| `typedef int (*InterpreterHandler)(Dbtup::InterpreterContext&)` | `DbtupExecQuery.cpp:8688` | done |
| `handleExitRefuseCte`, `handleUnsupportedCte`, `handleTableNotPopulated` | `DbtupExecQuery.cpp:8665-8684` | done |
| `Dbtup::INTERPRETER_FILTER_REJECT = -0x7FFFFFFF` | `Dbtup.hpp:2909` | done |
| `int Dbtup::interpreterFilterCte(...)` declaration | `Dbtup.hpp:2933-2936` | **declared only — body TBD (Phase A)** |

The scaffolding is in place. The work is: assemble the dispatch tables, write the loop body, hook it into DBLQH, fix the serializer.

## Architecture decision

**Three interpreters → one function with three handler tables.** After Phase C:

```cpp
int Dbtup::interpreterJumpTable(Signal*, KeyReqStruct*,
                                Uint32* main, Uint32 mainLen,
                                Uint32* sub, Uint32 subLen,
                                Uint32* tmp, Uint32 tmpSz,
                                const InterpreterHandler* handlerTable,
                                Uint32 flags);
```

Three static handler tables dispatched by `handlerTable`:
- `s_cte_filter_handlers` — for CTE_LOOKUP_REQ / CTE_SCAN_REQ filters.
- `s_agg_interp_handlers` — for `AggInterpreter` embedded programs (replaces its inline loop).
- (Future) additional tables as new modes appear.

Flags:
- `IFLAG_DISALLOW_BACKWARD_JUMPS = 0x1` — enforces forward-only branches (for aggregation mode).
- `IFLAG_REJECT_RETURNS_NEG = 0x2` — filter-reject semantics.

`Dbtup::interpreterFilterCte` (Phase A) starts as a standalone function; Phase C collapses it into a thin wrapper around `interpreterJumpTable`.

## Signal-layer design

**Zero new signal fields. Zero new sections.** The filter program piggybacks on the existing 5-word interpreter header in `AttrInfoSectionNum=1` (CTE_LOOKUP_REQ) / `AttrInfoSectionNum=0` (CTE_SCAN_REQ). Today the program slot contains just `ExitOK`; the filter program replaces that stub.

Current serialization (a **latent bug** to fix in Step A.5): `NdbQueryOperation.cpp:5364-5369` already appends the user program when `hasInterpretedCode()` is true, but then the `QN_CTE_LOOKUP` case at `NdbQueryOperation.cpp:5547-5550` unconditionally appends a redundant `ExitOK` header, producing a malformed AttrInfo. The fix is one line: guard the in-case append on `!hasInterpretedCode()`.

## Phase index

Each phase is in a separate file. Phases are ordered — Phase A must land green before Phase B; Phase C must not merge until A and B regression suites are green. Phase C is a pure refactor with no new user-visible behaviour.

| File | Phase | Scope | Status |
|---|---|---|---|
| [cte_filter_phase_a.md](cte_filter_phase_a.md) | **A** | CTE_LOOKUP_REQ filter support + third interpreter + `testCteNdbApiFilter` with baseline + CTE_LOOKUP tests | pending |
| [cte_filter_phase_b.md](cte_filter_phase_b.md) | **B** | CTE_SCAN_REQ (as root) filter support + CTE_SCAN tests | pending |
| [cte_filter_phase_c.md](cte_filter_phase_c.md) | **C** | Retrofit `AggInterpreter` to delegate to the jump-table interpreter; backward-jump guard enforcement | pending |

## Accepted opcodes (CTE filter mode)

Safe for virtual CTE rows (no real tuple / operation record required). All entries point to existing extracted handlers on `Dbtup::InterpreterContext`.

- **Constant loads**: `LOAD_CONST_NULL`, `LOAD_CONST16`, `LOAD_CONST32`, `LOAD_CONST64`, `LOAD_CONST_MEM`, `BZERO_MEM`.
- **Unconditional / register branches**: `BRANCH`, `BRANCH_REG_EQ_NULL`, `BRANCH_REG_NE_NULL`, all 12 `BRANCH_{EQ,NE,LT,LE,GT,GE}_REG_{REG,CONST}`.
- **Exits**: `EXIT_OK`, `EXIT_OK_LAST`; **overridden** `EXIT_REFUSE → handleExitRefuseCte`.
- **Subroutines**: `CALL`, `RETURN`.
- **Arithmetic / bitwise**: `{ADD,SUB,MUL,DIV,MOD,AND,OR,XOR,LSHIFT,RSHIFT}_REG_{REG,CONST}`, `NOT_REG_REG`.
- **String/convert**: `STR_TO_INT64`, `INT64_TO_STR`, `CONVERT_SIZE`, `WRITE_SIZE_MEM`.
- **Heap R/W**: `READ_{UINT8,UINT16,UINT32,INT64}_MEM_TO_REG` + `_REG_TO_REG` variants; `WRITE_{UINT8,UINT16,UINT32,INT64}_REG_TO_MEM` + `_REG_TO_REG` variants.
- **Critical CTE opcodes**: `READ_LINKED_TO_MEM → handleReadLinkedToMem` (opcode 39) and `BRANCH_MEM_OP_ARG → handleBranchMemOpArg` (opcode 38).

## Unsupported opcodes (CTE filter mode)

All set to `nullptr` in `s_cte_filter_handlers`. The main loop's one-time nullptr check sets `ZNO_INSTRUCTION_ERROR` and returns `-1`.

`READ_ATTR_INTO_REG`, `WRITE_ATTR_FROM_REG`, `WRITE_ATTR_FROM_MEM`, `APPEND_ATTR_FROM_MEM`, `WRITE_PARTIAL_ATTR_FROM_MEM`, `READ_ATTR_TO_MEM`, `READ_PARTIAL_ATTR_TO_MEM`, `BRANCH_ATTR_OP_ARG`, `BRANCH_ATTR_OP_PARAM`, `BRANCH_ATTR_OP_ATTR`, `BRANCH_ATTR_EQ_NULL`, `BRANCH_ATTR_NE_NULL`, `LOAD_OP_TYPE`, `READ_INTERPRETER_INPUT`, `WRITE_INTERPRETER_OUTPUT`, all `BINARY_SEARCH_*`, all `SEARCH_INTERVAL_*`, `STRING_SEARCH`, `QSORT`, `COMPRESS_NUM_ARRAY`, `SPECIAL_INSTR`. All remaining indices also `nullptr`.

## Existing functions to reuse

- `Dbtup::InterpreterContext` + handlers — `DbtupExecQuery.cpp:5817-8687`.
- `handleBranchMemOpArg` (linked-column comparison) — already exists.
- `handleReadLinkedToMem` — already exists.
- `handleExitRefuseCte`, `handleUnsupportedCte`, `handleTableNotPopulated` — `DbtupExecQuery.cpp:8665-8684`.
- `Interpreter::getInstructionPreProcessingInfo` — `Interpreter.hpp:1236-1409` (walk a program's instructions; used by the client-side validator).
- `AggInterpreter::validateEmbeddedProgram` — `AggInterpreter.cpp:150-247` (pattern for program validation).
- `NdbInterpretedCode::branch_col_le` + inverted-inequality pattern from `testCteNdbApi.cpp:1002-1019`.
- `cte_lookup_send` linked-attr expansion at `DbspjMain.cpp:6255-6306` — reuse unchanged.
- `cteLookupAggFeed` / `cteLookupEmitResult` — `DblqhMain.cpp:19538, 19547` — reuse unchanged; only add a filter gate before them.

## Risks & gotchas (cross-phase)

1. **Inverted inequality branches** — `branch_col_le` means "branch if col ≥ val" (see `pushdown_join_aggregation/CLAUDE.md`). Project-wide convention.
2. **`handleBranchMemOpArg` table descriptor** — needs a valid table descriptor for charset/type lookup via the attrId in word 1. Virtual CTE columns must have their type descriptors registered when `defineCte` runs. Test early with a CHAR filter; add a `handleBranchMemOpArgCteVirtual` variant if needed.
3. **`m_linked_attr_data` must be populated for the filter path** even when `joinAggStateKey == RNIL`. Today only the agg-feed branch builds it. Factor into `Dblqh::buildCteLinkedBuffer` (Step A.4).
4. **Double-serialized interpreter header bug** — current code serializes the user program at `NdbQueryOperation.cpp:5366` AND a redundant `ExitOK` at 5547-5550. Fix in Step A.5 is load-bearing.
5. **CALL/RETURN with no subroutine** — pass `subroutineProg=nullptr, subroutineLen=0`. Handlers must bounds-check (`handleCall`/`handleReturn` already do).
6. **Instruction fuse** — keep `RnoOfInstructions < 16000` in filter mode (backward jumps allowed). Drop fuse only when `IFLAG_DISALLOW_BACKWARD_JUMPS` is set.
7. **Phase ordering** — Phase C must not merge until A and B regression suites are green.
8. **Client-side validator drift** — keep the CTE-accepted opcode list in a single header shared between server (`s_cte_filter_handlers`) and client (`validateCteSafe`).

## Verification (end-to-end)

```bash
# From debug_build/
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ndb_mgmd
make -j$(sysctl -n hw.ncpu) testCteNdbApiFilter testCteNdbApi \
     testJoinAgg testJoinAggSpj testJoinAggNdbApi testCaseAgg \
     testOuterJoinAggNdbApi testMultiOuterJoinAggNdbApi \
     benchJoinAgg bench_q12_tpch

# Against a live cluster:
./runtime_output_directory/testCteNdbApiFilter -c localhost:1186 -m 3306 -v     # new
./runtime_output_directory/testCteNdbApi        -c localhost:1186 -m 3306 -v    # regression
./runtime_output_directory/testJoinAggNdbApi    -c localhost:1186 -m 3306 -v    # Phase C regression
./runtime_output_directory/testCaseAgg          -c localhost:1186 -m 3306 -v    # Phase C regression
```

**Debug trace macros** (uncomment `#define DEBUG_CTE 1` etc. in the relevant `.cpp`):
- `DEB_CTE` — CTE signal flow in DBSPJ / DBLQH.
- `DEB_CTE_API` — NDB API serialization.
- `TRACE_INTERPRETER` / `TRACE_INTERPRETER_REGISTERS` — per-instruction jump-table execution.

**Success criteria**:
- New tests in `testCteNdbApiFilter` pass.
- All existing CTE and join-agg tests pass unchanged.
- No new warnings in debug build.
- `bench_q12_tpch` / `bench_q9_dbtc` performance unchanged (±1%).
