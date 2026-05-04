# RONDB-1050: Interpreter Typed Register Test Expansion Plan

This plan extends `testInterpreterTypedRegs.cpp` from broad opcode coverage
into systematic type, boundary-value, memory-helper and error-handler coverage.

The test remains an NDB API executable driven by an MTR wrapper. It should keep
all development isolated to the existing interpreter test file and its MTR
wrapper unless a later phase explicitly needs a second dedicated test file.

Constraints:

- Do not use `auto`.
- Do not duplicate DBTUP interpreter production logic in the test.
- Keep column-write opcodes out of scope unless explicitly requested.
- Keep linked/aggregation-only embedded interpreter opcodes out of this scan
  filter harness unless a separate execution shape is added.
- Each phase should build `testInterpreterTypedRegs` and run
  `ndb_push_agg.testInterpreterTypedRegs` before commit.

## Phase 1: Refactor Test Helpers

Goal: make the current 17 test groups easier to extend without changing their
behavior.

Work:

- Add helper functions for common expected result shapes:
  - all rows.
  - no rows.
  - explicit row list.
  - expected runtime interpreter error.
- Add helper builders for repeated interpreted-code patterns:
  - read attribute into register and compare to constant/register.
  - compare two registers and accept/reject.
  - load memory buffer and check a memory-read result.
- Keep existing test names and expected results stable.
- Avoid changing fixture rows in this phase.

Validation:

- Build `testInterpreterTypedRegs`.
- Run `./mtr --suite=ndb_push_agg testInterpreterTypedRegs`.
- Run `git diff --check`.
- Confirm no new `auto`.

## Phase 2: Integer Width Boundary Matrix

Goal: cover every signed and unsigned integer width at values close to the
limits.

Work:

- Extend the fixture with same-type comparison columns for:
  - signed `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT`.
  - unsigned `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT`.
- Add rows for boundary values:
  - signed: min, min + 1, -1, 0, 1, max - 1, max.
  - unsigned: 0, 1, mid/high-bit boundary, max - 1, max.
- Add tests for:
  - sign extension on each signed width.
  - zero extension on each unsigned width.
  - attr-vs-attr comparisons for each same-type width.
  - const/register comparisons around min/max rows.

Validation:

- Check expected row sets around each exact boundary.
- MTR must pass.

## Phase 3: Signed/Unsigned Promotion Matrix

Goal: stress comparison and arithmetic promotion rules where signedness changes
the result.

Work:

- Add targeted comparisons:
  - negative signed vs unsigned zero/small/max.
  - signed max vs unsigned max.
  - signed -1 vs unsigned max.
  - unsigned values above signed max vs signed positive values.
- Add arithmetic tests mixing signed and unsigned across widths:
  - add, subtract, multiply.
  - non-overflow cases near the boundary.
  - overflow/error cases where the interpreter should reject the operation.

Validation:

- Expected results should reflect interpreter typed-register semantics directly,
  not MySQL SQL coercion assumptions.

## Phase 4: Floating Point Matrix

Goal: cover `FLOAT`, `DOUBLE`, integer promotion, NULL, and precision edges.

Work:

- Add fixture columns:
  - `f_val2 FLOAT NOT NULL`.
  - `d_val2 DOUBLE NOT NULL`.
  - `n_float FLOAT NULL`.
- Add tests for:
  - `FLOAT` read into typed register.
  - `FLOAT` vs `DOUBLE`.
  - signed/unsigned integer vs `FLOAT`.
  - signed/unsigned integer vs `DOUBLE`.
  - +0.0 vs -0.0.
  - fractional comparisons around exact equality.
  - nullable float/double branch behavior.
- Add arithmetic tests for add/sub/mul/div with float/double operands.

Validation:

- Avoid NaN/Inf unless confirmed stable through SQL insert and NDB storage.

## Phase 5: Column Predicate Type Matrix

Goal: expand `branch_col_*` coverage beyond the current `INT`, `CHAR`, and
`BIT` cases.

Work:

- Add column-vs-value predicate tests for each integer width and unsigned width.
- Add attr-vs-attr predicate tests for each same-type width.
- Add `FLOAT` and `DOUBLE` predicate tests.
- Add nullable column predicate tests for each family where the semantics are
  meaningful.
- Add `set_sql_null_semantics()` tests if the API behavior is clear in scan
  filters.

Validation:

- Keep comments near tests that rely on inverted `branch_col_*` inequality
  semantics.

## Phase 6: String And Binary Types

Goal: cover character and binary comparison semantics.

Work:

- Add fixture columns:
  - `BINARY`.
  - `VARBINARY`.
  - wider `CHAR`.
  - wider `VARCHAR`.
- Add values for:
  - empty string.
  - one character.
  - max length.
  - trailing-space `CHAR`.
  - embedded zero for binary.
  - high-bit bytes for binary.
- Add tests for:
  - equality and inequality.
  - ordering.
  - `LIKE` and `NOT LIKE`.
  - binary exact comparisons.

Validation:

- Keep `CHAR` padding expectations explicit.

## Phase 7: Date/Time And Decimal Triage

Goal: determine and cover supported non-numeric scalar types.

Work:

- Add exploratory tests for:
  - `DATE`.
  - `DATETIME`.
  - `TIMESTAMP`.
  - `TIME`.
  - `YEAR`.
  - `DECIMAL`.
- If a type is supported by the interpreter comparison path, add positive
  predicate tests.
- If a type is not supported, add explicit rejection tests where useful.

Validation:

- Unsupported behavior should be visible and intentional, not accidental.

## Phase 8: Memory Opcode Expansion

Goal: cover memory forms thoroughly.

Work:

- Add const-offset and register-offset tests for all memory-read sizes.
- Add const-offset and register-offset tests for all strict memory writers.
- Add `WRITE_REG_TO_MEM_ANY` tests for int, uint and double.
- Add tests for:
  - `convert_size`.
  - `write_size_mem`.
  - `int64_to_str`.
  - memory bounds errors.

Validation:

- Keep memory buffers small, deterministic and easy to inspect.

## Phase 9: Search/Sort/String Library Opcodes

Goal: cover remaining helper/library instructions.

Work:

- Add tests for:
  - `binary_search_16`, `binary_search_32`, `binary_search_64`,
    `binary_search_odd`.
  - `search_interval_16`, `search_interval_32`, `search_interval_64`,
    `search_interval_odd`.
  - `qsort_instr` for multiple element sizes.
  - `compress_num_array`.
  - `string_search`.
- Add success and not-found cases.
- Add search modes beyond exact match where supported.

Validation:

- Verify both exact-match and rank/successor-style search behavior.

## Phase 10: Error Handler Matrix

Goal: cover failure behavior consistently.

Work:

- Add runtime error tests for:
  - uninitialized registers by opcode family.
  - NULL registers by opcode family.
  - float passed to integer-only opcodes.
  - invalid shift counts through const and register forms.
  - divide/modulo by zero.
  - signed overflow in add/sub/mul.
  - invalid memory offset and size.
  - unsupported type paths.

Validation:

- Errors must be expected interpreter/runtime errors, not crashes or MTR
  infrastructure failures.

## Phase 11: Final Coverage Audit

Goal: document exactly what the test covers and what remains intentionally out
of scope.

Work:

- Add a coverage note listing:
  - covered opcode families.
  - covered type families.
  - intentionally excluded column-write opcodes.
  - linked/aggregation-only opcodes needing a separate harness.
  - parameter/input/output opcodes needing a separate execution shape.
- Do a final pass over helper naming and test group ordering.

Validation:

- Build `testInterpreterTypedRegs`.
- Run `ndb_push_agg.testInterpreterTypedRegs`.
- Run `git diff --check`.
- Confirm no new `auto`.

Suggested commit split:

- Plan commit.
- Phase 1 helper refactor.
- Integer boundary phases.
- Signed/unsigned and floating-point phases.
- Column/string/binary predicate phases.
- Memory/search/library phases.
- Error matrix and final coverage audit.
