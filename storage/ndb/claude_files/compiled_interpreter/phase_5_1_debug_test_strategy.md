# RONDB-1056 Phase 5.1 next step - debugging and test strategy

**Status: planning.** This is the next step after the Phase 5.1a
review fixes and the separate plans for `row_accumulated` and overflow
parity.

## Current review state

The latest updates fixed the main Phase 5.1a review findings:

- `dbtup_jit_invoke` now supplies `ctx.agg`, and helper entry points
  check that the aggregation interpreter is present.
- `Op.b` and `Op.c` are widened to `uint16_t`, and the admission helper
  recognises the new cold-call branch opcodes.
- Embedded-block fixups are no longer applied to older program entries,
  and the linked-attribute buffer read now has an explicit destination
  capacity.
- `ERROR_INSERT 4060` exists as a fallback-is-fatal canary, and the
  compile path has more debug-build logging.

The remaining problem is observability at full NDB API to data-node
scope. RonSQL proved that some SQL shapes worked, but it did not make it
obvious which aggregation bytecode was actually built, translated, and
executed. The next step should therefore be deterministic NDB API tests
plus on-demand diagnostics that show the complete path from
`NdbAggregator` program to JIT `Program` to row execution.

## Goals

1. Make JIT compilation and fallback decisions visible without relying
   on unconditional debug-build spam.
2. Add NDB API tests where the intended interpreted program is known and
   printed on failure.
3. Make `ERROR_INSERT 4060` useful as a positive canary: tests that are
   expected to compile should crash immediately if they fall back.
4. Keep unsupported shapes explicit. A fallback is acceptable only in
   tests that are deliberately checking a rejected program.

## Debugging additions

### Program dumps

Add a shared decode/dump helper, preferably close to the bridge code:

- `ndb_jit_bridge_dump_input(...)`: prints the aggregation header and raw
  aggregation program words.
- `ndb_jit_bridge_dump_program(...)`: prints translated JIT ops with op
  names, operand fields, immediates, entry index, and exit targets.
- `ndb_jit_bridge_reject_reason(...)`: converts bridge and admission
  reject reasons into stable text for logs and test failures.

Use these helpers from both DBLQH setup diagnostics and bridge unit
tests. This avoids each layer having a slightly different decoder.

### Error inserts

Keep `4060` as the runtime fallback canary, but split the additional
diagnostics into narrower controls:

- `4060`: fallback is fatal. Use in positive MTR/NDB API tests that must
  compile and execute JIT code.
- `4061`: dump the input aggregation program, translated JIT program,
  and bridge/admission decision at setup time.
- `4062`: compile failure is fatal at setup time, before row execution.
  This catches rejected programs earlier than `4060`.
- `4063`: bounded row trace for the first N invocations. Log branch
  decisions, helper calls, accepted/rejected row state, accumulator input,
  accumulator output, and overflow state. N should come from the error
  insert extra value or a small hard-coded default.

The current debug-build full dump in `DblqhProxy.cpp` is useful while
developing, but it should become on-demand once `4061` exists. Otherwise
full cluster tests will produce too much noise.

### Runtime counters

Add cheap counters that can be dumped by an existing DUMP path or a new
debug-only command:

- compile attempts
- compile successes
- bridge rejects
- admission rejects
- JIT row invocations
- fallback row invocations
- JIT helper failures

Counters make it possible to distinguish "test did not reach JIT setup"
from "test reached setup but rejected" and "test compiled but never ran".

## NDB API test strategy

Add deterministic tests under the existing NDB API aggregation test
framework, preferably by extending `testJoinAggNdbApi.cpp` or by adding a
small sibling binary if isolation is cleaner. The MTR wrapper should run
the binary directly, similar to the existing `testJoinAggNdbApi.test`.

Each test case should:

1. Build the aggregation with `NdbAggregator` or a small raw-program test
   helper when the public API cannot express the exact bytecode needed.
2. Dump the exact program words when verbose mode is enabled or when the
   test fails.
3. Execute once with JIT disabled or forced to fallback, execute once
   with JIT required, and compare records.
4. Enable `ERROR_INSERT 4060` only for cases expected to compile.
5. Enable `4061` on failure-oriented runs so CI logs contain the decoded
   program and rejection point.

### Initial positive cases

Start with local-attribute programs that do not depend on linked data:

- `SUM(col)` with all rows accepted.
- `SUM(col)` where every row is rejected by `BRANCH_ATTR_*_NULL`. This
  should be added after the `row_accumulated` fix, because rejected rows
  currently risk producing `SUM=0` instead of SQL `NULL`.
- `SUM(col)` with a mix of accepted and rejected rows.
- Multiple accumulators in one program to verify that accumulated-state
  tracking is per accumulator, not global.

### Linked-attribute cases

Then add linked cases that cover the Phase 5.1a target shape:

- parent row projects a nullable linked column with
  `addLinkedProjection`;
- child aggregation reads that linked value through
  `READ_LINKED_TO_MEM`;
- embedded branch uses `BRANCH_LINKED_EQ_NULL` or
  `BRANCH_LINKED_NE_NULL`;
- result is compared against the interpreter-forced run.

These should also get bridge unit tests. The current bridge tests cover
embedded `BRANCH_ATTR_*_NULL`; they should add `READ_LINKED_TO_MEM` plus
`BRANCH_LINKED_*_NULL` so the Phase 5.1a path is tested before the full
data-node run.

### Negative and boundary cases

Keep explicit fallback tests for unsupported programs:

- linked `kOpLoadCol 0x8000` until the helper path supports that shape;
- unsupported embedded opcodes;
- branch targets outside the embedded block;
- malformed linked-attribute payloads or payloads larger than the
  destination buffer.

Add operand-width boundary tests once the width policy is decided:

- accepted: 0, 255, 256, and 4095 if Phase 5.1 intends full 12-bit
  attribute ids;
- rejected: 4096 and higher;
- both `BR_kOpLoadCol` and embedded `BRANCH_ATTR_*_NULL`.

### Overflow parity cases

After the overflow parity plan is implemented, add canaries that compare
JIT and interpreter results for:

- signed positive overflow;
- signed negative overflow;
- no-overflow near-boundary values;
- multiple accumulators where one overflows and another does not.

Do not enable `4060` on overflow cases until the JIT path returns the
same status and result metadata as the interpreter.

## Execution order

1. Land the `row_accumulated` fix first. Without it, fallback-vs-JIT
   comparisons can be misleading for all-rejected groups.
2. Add the shared decode/dump helpers and wire `4061`/`4062`.
3. Make the existing `DEBUG_JIT` full program dump conditional on the new
   diagnostics.
4. Add bridge/admission unit tests for linked-null and operand-width
   boundaries.
5. Add the first deterministic NDB API binary cases for local attributes.
6. Add linked-attribute NDB API cases after the bridge-level linked-null
   tests pass.
7. Enable `4060` in only the positive canaries that must compile.
8. Add overflow canaries after the overflow parity implementation lands.

## Additional notes from this review

### Operand width mismatch

`bytecode1.h` now allows 16-bit operands, and the surrounding comments
describe real attribute ids up to 4095. The bridge still has active
8-bit limits in important places: `emit_op` takes `uint8_t` operands,
embedded `BRANCH_ATTR_*_NULL` rejects `attr_id > 255`, and
`BR_kOpLoadCol` rejects `col_index > 255`.

This can be valid as an intermediate Phase 5.1a limit, but the code and
plan should say so explicitly. If Phase 5.1a is meant to support the
full 0..4095 attribute range, change `emit_op` to accept `uint16_t`
operands, remove the casts, and add the boundary tests listed above.

### `ERROR_INSERT 4060` and null `block_tup`

`JoinAggInterpreter::ProcessRec` can be called with `block_tup == nullptr`
for null-extended or CTE-fed paths. The `4060` check currently goes
through `block_tup->jit_error_inserted(4060)`. Guard that check or make
the error insert explicitly DBTUP-row-only before using it in broad NDB
API tests.

### Linked-buffer fallback

The linked-buffer read now has a capacity argument, which is good. If a
linked entry is malformed or too large, consider adding a debug counter
or diagnostic line before writing the NULL fallback. Silent NULL fallback
can otherwise hide data-shape bugs while chasing JIT admission issues.

### Stale comments

Some `DbtupJitGlue.cpp` comments still describe accumulator writeback as
if every accumulator is materialised on every row. Update those comments
when the `row_accumulated` implementation lands.
