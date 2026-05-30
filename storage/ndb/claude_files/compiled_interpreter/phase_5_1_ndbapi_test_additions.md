# RONDB-1056 Phase 5.1 NDB API test additions

**Status: handoff plan.** This plan starts from branch
`RONDB-1056-compiled-interpreter` after these debug commits:

- `238030f82af` - `RONDB-1056: Add JIT setup diagnostics`
- `ca802b26257` - `RONDB-1056: Add bounded JIT row trace`

The goal is to move from RonSQL canaries to deterministic NDB API tests
where the aggregation program is known before it reaches the data node.

## Existing test entry points

Use the existing NDB API harness unless a case becomes too invasive:

- Source: `storage/ndb/block_unit_test/testJoinAggNdbApi.cpp`
- Build rule: `storage/ndb/block_unit_test/CMakeLists.txt`
- MTR wrapper: `mysql-test/suite/ndb_push_agg/t/testJoinAggNdbApi.test`
- MTR result: `mysql-test/suite/ndb_push_agg/r/testJoinAggNdbApi.result`

The binary already supports:

- `--only <N>` to run one numbered test;
- `--skip <N>` to skip one numbered test;
- `-v` / `--verbose` for progress output;
- `NdbRestarter::insertErrorInAllNodes(...)` for data-node error inserts.

Build and quick-run commands:

```sh
cmake --build debug_build --target testJoinAggNdbApi -j 4
debug_build/runtime_output_directory/testJoinAggNdbApi \
  -c localhost:1186 -m 3306 --only 23 -v
```

Run through MTR:

```sh
./mysql-test/mtr --suite=ndb_push_agg testJoinAggNdbApi
```

Keep the existing `debug_build/` and `prod_build/` directories out of
commits.

## Diagnostics to use

Use the new JIT diagnostics while developing the tests:

- `4061`: dump aggregation header/body and translated JIT program at
  setup.
- `4062`: make setup-time JIT failure fatal. This catches bridge reject,
  admission reject, missing arena, multi-leaf skip, and missing bytecode.
- `4063`: trace first N JIT rows. `ERROR_INSERT_EXTRA` is N; default is
  16.
- `4060`: runtime fallback is fatal. Enable only after a case is known to
  compile and execute through JIT.

Recommended development order for a positive canary:

1. Run without error inserts and verify the result.
2. Run with `4061` and inspect the exact program and translated JIT ops.
3. Run with `4062` to require setup compilation.
4. Run with `4063` and a small limit if row-level behavior is unclear.
5. Only then enable `4060` in the automated positive canary.

Do not enable `4060` on unsupported or exploratory cases.

## Test numbering

Append new tests after the current Test 22 in
`testJoinAggNdbApi.cpp`. Proposed numbering:

- Test 23: deterministic local `SUM(amount)` JIT canary, no GROUP BY.
- Test 24: deterministic embedded `BRANCH_ATTR_*_NULL` canary.
- Test 25: deterministic linked `READ_LINKED_TO_MEM` plus
  `BRANCH_LINKED_*_NULL` canary.
- Test 26: negative fallback case for a known unsupported program.
- Test 27: operand-width boundary canary, after the width policy is
  decided.

Add matching expected lines to
`mysql-test/suite/ndb_push_agg/r/testJoinAggNdbApi.result`.

For production builds that do not support `ERROR_INSERT`, follow the
existing skip pattern around Tests 13, 14, 20, and 22: print a stable
`SKIPPED` or `OK` line and keep the MTR result deterministic.

## Test 23: local SUM JIT canary

Purpose: prove a simple NDB API aggregation program compiles and runs
through JIT without RonSQL.

Use the existing base tables from Tests 1-3:

- `jagg_parent(id INT PK, grp INT NOT NULL)`
- `jagg_child(parent_id INT PK, amount BIGINT)`

Query shape:

```text
parent scan -> child lookup by parent.id
child leaf aggregation:
  LoadColumn("amount", r0)
  Sum(0, r0)
  Finalize()
```

Expected bytecode body after optimization:

```text
kOpLoadCol      amount -> r0
kOpSumBigint    agg[0] += r0
implicit exit
```

Expected result:

```text
SUM(amount) = 1500
```

Implementation notes:

- Reuse the query-builder pattern from `testCountSum`.
- Add a helper that prints the raw `NdbAggregator::buffer()` when verbose.
  Print header words and instruction words separately.
- Develop with `4061` and `4062`.
- Automate with `4060` once the program is confirmed to compile.

Success line:

```text
Test 23: JIT canary local SUM(amount) ... OK (sum=1500, JIT required)
```

## Test 24: embedded local NULL-branch canary

Purpose: prove an embedded normal-interpreter NULL branch is translated
and executed through JIT.

Use a small new table pair, or extend the base test setup if it is less
invasive:

```sql
CREATE TABLE t24_parent (
  id INT NOT NULL PRIMARY KEY,
  pad INT NOT NULL
) ENGINE=NDB;

CREATE TABLE t24_child (
  parent_id INT NOT NULL PRIMARY KEY,
  nullable_amount BIGINT NULL
) ENGINE=NDB;
```

Data:

```text
(1, 100), (2, NULL), (3, 300), (4, NULL), (5, 500)
```

Aggregation program:

```text
EmbeddedInterp(3)
  BRANCH_ATTR_EQ_NULL or BRANCH_ATTR_NE_NULL to EXIT_REFUSE
  attrId(nullable_amount)
  EXIT_REFUSE
LoadColumn(nullable_amount, r0)
Sum(0, r0)
Finalize()
```

Choose the branch polarity so the test accepts only non-NULL rows. The
expected result is:

```text
SUM(nullable_amount) = 900
```

Implementation notes:

- Use `NdbAggregator::EmbeddedInterp()` and `EmitEmbeddedWord()` to emit
  the exact embedded words.
- Put local encoder helpers beside the test, not in production code:
  `enc_emb_op_word`, `enc_emb_branch_attr_null`, and
  `enc_emb_attr_id`.
- Verify with `4061` that the translated JIT program contains
  `branch_attr_*_null`, `load_col_ndb`, `sum_bigint`, and `exit`.
- Do not add an all-rejected version until the `row_accumulated` fix
  lands, because all-rejected `SUM` semantics are still being fixed.

Success line:

```text
Test 24: JIT canary embedded local NULL filter ... OK (sum=900)
```

## Test 25: linked NULL-branch canary

Purpose: prove the Phase 5.1a target shape works end to end:
linked-attribute buffer -> `READ_LINKED_TO_MEM` ->
`BRANCH_LINKED_*_NULL` -> JIT execution.

Use new tables to avoid disturbing existing expectations:

```sql
CREATE TABLE t25_parent (
  id INT NOT NULL PRIMARY KEY,
  marker BIGINT NULL
) ENGINE=NDB;

CREATE TABLE t25_child (
  parent_id INT NOT NULL PRIMARY KEY,
  amount BIGINT NOT NULL
) ENGINE=NDB;
```

Data:

```text
parent: (1, 10), (2, NULL), (3, 30), (4, NULL), (5, 50)
child:  (1,100), (2,200), (3,300), (4,400), (5,500)
```

Query shape:

```text
parent scan projects marker via addLinkedProjection()
child lookup aggregates amount
embedded block:
  READ_LINKED_TO_MEM position 0
  BRANCH_LINKED_EQ_NULL or BRANCH_LINKED_NE_NULL to EXIT_REFUSE
  EXIT_REFUSE
LoadColumn(amount, r0)
Sum(0, r0)
Finalize()
```

Choose branch polarity so rows with `parent.marker IS NULL` are rejected.
Expected result:

```text
SUM(amount for marker NOT NULL) = 100 + 300 + 500 = 900
```

Implementation notes:

- Use `NdbQueryOptions::addLinkedProjection(qb->linkedValue(parentOp,
  "marker"))`.
- The embedded position must match the projection order. With one linked
  projection, position is 0.
- Verify with `4061` that the translated program contains
  `load_linked_to_mem` and `branch_linked_*_null`.
- Run once with `4063` limited to 5 rows to confirm linked NULL decisions
  are row-specific.
- After confirmed, enable `4060` for the automated positive run.

Success line:

```text
Test 25: JIT canary linked NULL filter ... OK (sum=900)
```

## Test 26: unsupported-program fallback

Purpose: keep fallback behavior explicit for programs that are not yet
in JIT scope.

Candidate unsupported shapes:

- `LoadLinkedColumn(...)` in the aggregation arithmetic path;
- `kOpSetRegNull`;
- `kOpCount`, `kOpMin`, or `kOpMax` if still unsupported by bridge/JIT
  at the time this test is added.

Run without `4060`/`4062` and verify the interpreter result. Then run a
developer-only local pass with `4062` to confirm it fails at setup with a
clear bridge/admission reason. Do not put the fatal `4062` variant into
normal MTR unless it is in a separate expected-failure test.

Success line:

```text
Test 26: Unsupported JIT shape falls back cleanly ... OK
```

## Test 27: operand-width boundaries

Purpose: lock down the 8-bit vs 12-bit/16-bit attribute-id policy.

Do this only after deciding the implementation policy:

- If Phase 5.1 keeps the active bridge limit at 255, add tests proving
  255 accepts and 256 rejects, and update comments that currently talk
  about 4095.
- If Phase 5.1 supports full schema attr ids 0..4095, add tests proving
  255, 256, and 4095 accept, and 4096 rejects.

Coverage needed:

- `BR_kOpLoadCol` column id boundary;
- embedded `BRANCH_ATTR_*_NULL` attr id boundary;
- bridge unit tests first, then NDB API table tests only if creating
  high-attr-id tables is practical.

## Program-dump helper

Add a small test-local helper in `testJoinAggNdbApi.cpp`:

```c++
static void dumpAggProgram(const char *label, const NdbAggregator &agg)
```

Behavior:

- no output unless `verbose` is true or the caller is about to fail;
- print `instructions_length()`;
- print raw words from `buffer()`;
- identify header/body split using the aggregation header layout:
  8 header words plus `n_gb_cols()` group-by descriptor words.

Do not link the block-unit binary against the kernel bridge library just
for dumps. The production `4061` logs already decode the bridge side.
The test helper only needs to make the NDB API program visible.

## Error-insert handling

Use `NdbRestarter` as existing tests do:

```c++
NdbRestarter restarter;
restarter.insertErrorInAllNodes(4061);
...
restarter.insertErrorInAllNodes(0);
```

For `4063` with a row limit, use a two-word error insert if available in
the test helper API. If `NdbRestarter` only exposes single-value
`insertErrorInAllNodes`, keep `4063` manual for now; the default 16-row
limit is enough for the small canaries.

Always clear error inserts on every return path. Follow the existing
Tests 13-20 style: clear before each early return and once on success.

## MTR result management

After adding tests:

1. Build the binary.
2. Run individual tests with `--only`.
3. Run the full binary.
4. Run MTR and update the result file.

Commands:

```sh
cmake --build debug_build --target testJoinAggNdbApi -j 4
debug_build/runtime_output_directory/testJoinAggNdbApi \
  -c localhost:1186 -m 3306 --only 23 -v
debug_build/runtime_output_directory/testJoinAggNdbApi \
  -c localhost:1186 -m 3306
./mysql-test/mtr --suite=ndb_push_agg testJoinAggNdbApi
```

If `4061` or `4063` logs appear in MTR output, do not leave those error
inserts enabled in the normal test path. They are development tools, not
stable expected result output.

## Minimum handoff checklist

Before moving environments, confirm:

- branch includes commits `238030f82af` and `ca802b26257`;
- `bridge_tests`, `admission_tests`, and `coldcall_tests` pass;
- `ndbmtd` builds;
- `testJoinAggNdbApi` builds;
- a cluster can run `testJoinAggNdbApi --only 23` before adding the rest.

Recommended first task in the new environment:

1. Add Test 23 only.
2. Verify it compiles and passes without error inserts.
3. Enable `4061` manually and inspect the generated program.
4. Enable `4062`, then `4060`, and make the automated test require JIT.
5. Only after Test 23 is stable, add Test 24 and Test 25.
