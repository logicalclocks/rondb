# RONDB-1050: Interpreter Typed Register Coverage Audit

Phase 11 status: complete.

This note records the coverage provided by
`storage/ndb/block_unit_test/testInterpreterTypedRegs.cpp` after the typed
register expansion phases.  The test is an NDB API scan-filter executable run by
`mysql-test/suite/ndb_push_agg/t/testInterpreterTypedRegs.test`.

## Covered Opcode Families

- Register loads from real table attributes through `READ_ATTR_INTO_REG`.
- Constant loads through `LOAD_CONST_NULL`, `LOAD_CONST16`,
  `LOAD_CONST32`, `LOAD_CONST64`, and `LOAD_DOUBLE_CONST`.
- Register branches for null checks, register-vs-register comparisons, and
  register-vs-constant comparisons.
- Column predicate branches for column-vs-value, column-vs-column, `IS NULL`,
  `IS NOT NULL`, `LIKE`, `NOT LIKE`, bit-mask predicates, and SQL null
  semantics.
- Register arithmetic for add, subtract, multiply, divide, and modulo,
  including register/register and register/constant forms.
- Bitwise and shift operations for AND, OR, XOR, NOT, left shift, and right
  shift.
- Memory helper operations for typed memory reads, typed memory writes,
  register-offset forms, const-offset forms, memory zeroing, memory constants,
  `WRITE_REG_TO_MEM_ANY`, `CONVERT_SIZE`, `WRITE_SIZE_MEM`,
  `INT64_TO_STR`, and `STR_TO_INT64`.
- Search/sort/string helper operations for `BINARY_SEARCH_*`,
  `SEARCH_INTERVAL_*`, `QSORT`, `COMPRESS_NUM_ARRAY`, and `STRING_SEARCH`.
- Runtime error paths for uninitialised registers, NULL operands, invalid float
  use in integer-only opcodes, invalid shift counts, divide/modulo by zero,
  signed and unsigned overflow, invalid memory offsets/sizes, and unsupported
  helper arguments.

## Covered Type Families

- Signed integer widths: `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, and
  `BIGINT`.
- Unsigned integer widths: `TINYINT UNSIGNED`, `SMALLINT UNSIGNED`,
  `MEDIUMINT UNSIGNED`, `INT UNSIGNED`, and `BIGINT UNSIGNED`.
- Integer boundary values near signed min/max, unsigned max, zero, one, and
  high-bit transitions.
- Mixed signed/unsigned comparisons and arithmetic promotion cases.
- Floating point values for `FLOAT` and `DOUBLE`, including mixed
  float/integer arithmetic, `+0.0` versus `-0.0`, nullable float handling, and
  divide-by-zero rejection.
- Character and binary comparison paths for `CHAR`, `VARCHAR`, `BINARY`, and
  `VARBINARY`, including padding, empty strings, embedded zero bytes, high-bit
  bytes, max-length values, `LIKE`, and `NOT LIKE`.
- Temporal and decimal predicate paths for `DATE`, `DATETIME`, `TIMESTAMP`,
  `TIME`, `YEAR`, and `DECIMAL`.

## Intentionally Excluded

- Column-write opcodes are out of scope for this scan-filter harness:
  `WRITE_ATTR_FROM_REG`, `WRITE_ATTR_FROM_MEM`,
  `WRITE_PARTIAL_ATTR_FROM_MEM`, and `APPEND_ATTR_FROM_MEM`.
  These mutate tuple attributes and need an update/write execution shape, not
  the read-only scan-filter path used here.
- Parameter-based predicate opcodes, especially `BRANCH_ATTR_OP_PARAM`, need a
  harness that supplies interpreter parameters through the operation shape.
- Interpreter input/output opcodes, `READ_INTERPRETER_INPUT` and
  `WRITE_INTERPRETER_OUTPUT`, need an execution path that provides input/output
  slots.  `WRITE_INTERPRETER_OUTPUT` is primarily relevant to aggregation
  interpreter execution.
- Linked and aggregation-only opcodes need join/aggregation harnesses rather
  than this standalone scan-filter test:
  `READ_LINKED_TO_MEM`, `BRANCH_MEM_OP_ARG`,
  `BRANCH_MEM_OP_ARG_INLINE_TYPE`, `BRANCH_LINKED_EQ_NULL`,
  `BRANCH_LINKED_NE_NULL`, `READ_AGG_REG_TO_REG`, and
  `READ_LINKED_COLUMN_TO_REG`.
- `CALL` and `RETURN` are not part of the systematic typed-register matrix.
  The normal interpreter supports them, but aggregation-mode dispatch
  deliberately omits them for termination reasons; a separate control-flow
  audit would be clearer than mixing them into this data-type matrix.

## Ordering And Naming Audit

- Test groups 1-17 are the original broad coverage groups retained for
  regression continuity.
- Test groups 18-26 follow the expansion plan order:
  integer boundaries, signed/unsigned promotion, floating point, column
  predicates, string/binary, temporal/decimal, memory, search/sort/string
  helpers, and error handlers.
- Helper names are grouped by purpose: finish helpers, scan/result helpers,
  table setup helpers, column predicate helpers, and memory-buffer helpers.
  No helper rename is needed for Phase 11.

## Validation

- Phase 10 test run passed before this audit note was added.
- Phase 11 adds documentation only; no interpreter or test executable code was
  changed in this phase.
- Required hygiene checks for this documentation change:
  `git diff --check` and no forbidden type inference in code changes.
