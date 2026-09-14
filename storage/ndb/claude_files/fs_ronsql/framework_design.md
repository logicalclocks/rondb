# Framework design (P3, v1 — 2026-09-08)

**Status: design complete, nothing built.** Consumed by E2 (spec +
emitter), E3 (executors, canonicalization, `.fs_verify`, golden MTR
test), E4 (vector oracle), E6/E7 (fuzzers). Data and DDL details are
in `data_model.md`; statement shapes in `shape_catalog.md`.

---

## 1. Placement and build

Everything lives in `tools/rondb-cli` (module
`github.com/logicalclocks/rondb/tools/rondb-cli`, Go 1.24), package
tree `internal/fsq/…`, wired into the shell by one new file
`internal/shell/fs.go`. The binary is built by CMake
(`tools/rondb-cli/CMakeLists.txt`) into `runtime_output_directory/rondb`
and MTR exports it as `$RONDB_CLI` (`mysql-test-run.pl:2927-2934`).
No new build targets; no new Go dependencies beyond the ones in
`go.mod` (`go-sql-driver/mysql`, `cobra`, `readline`, …).

Non-interactive use is the existing `-e/--execute` + `--quiet` mode:
`rondb --host 127.0.0.1 --mysql-port P --rdrs-port R --no-rondis
--quiet -e ".fs_verify …"`; cobra turns a returned error into exit
code 1 (`cmd/root.go:260-265`), which is what MTR's `--exec` and
`--error` rely on.

```
tools/rondb-cli/internal/fsq/
  spec/      feature-group + feature-view model, JSON, definition-time gates
  ddl/       port of OnlineFeaturegroupController.buildCreateStatement
  data/      formulas, schema, SQL renderer (MTR include), loader, checksums, key classes
  emit/      port of PreparedStatementBuilder (RonSQL templates + gates)
  mysqltwin/ MySQL serving statements (semantic twins, not Calcite-identical)
  bind/      `?` → typed literals, IN lists, window literal
  exec/      engines: RDRS /ronsql, ronsql_cli, MySQL; timeouts, retries, crash probe
  canon/     result canonicalization + comparison (L1)
  vector/    client-side folds + vector comparison (L2)
  cases/     deterministic case matrix (shape × key class × mode × filter)
  mtr/       emitters for .inc / .test files
  fuzz/      spec-level and envelope-level generators, classification, shrinking (E6/E7)
  report/    case lines, SUMMARY line, JSON report, dump directory
  testdata/  hopsworks_golden/*.json, ddl_golden/*.sql, mtr_golden/*.inc
internal/shell/fs.go   .fs_* command dispatch (thin: flag parsing + calling fsq)
```

Dependency direction: `spec ← ddl, data, emit, mysqltwin ← bind ←
exec ← canon ← vector ← cases ← mtr, fuzz ← report`. `emit` and
`mysqltwin` never touch a network; `exec` is the only package that
does. Every package is unit-testable without a cluster except `exec`
(integration only).

---

## 2. Spec model (`fsq/spec`)

> E2 note: the spec format is the input half of a Hopsworks golden fixture
> (`HopsworksGoldenDump.CaseSpec`); `spec.View` unmarshals fixtures directly.
> The type sketch below predates that and is superseded by `spec/view.go`.

Go types mirror the Hopsworks entities the emitters read, with the
same names so the port can be reviewed side by side. JSON tags make
every spec writable by hand (MTR authors, fuzz dumps).

```go
type FeatureGroup struct {           // Featuregroup + FeatureGroupFeatureDTO list
    Name, Version                    // table `<Name>_<Version>`
    FeaturestoreID int               // cross-featurestore gate (default 1)
    OnlineEnabled  bool              // default true
    EventTime      string            // "" when none
    TTL            *int64            // seconds; nil = off
    Online         OnlineConfig      // PrimaryKeyIndexType "HASH"|"" , SecondaryIndexes [][]string, Comments []string, TableSpace string
    Features       []Feature
}
type Feature struct { Name, Type string; Primary bool; OnlineType string; DefaultValue *string; OfflineOnly, Complex bool }

type FeatureView struct {            // FeatureView / TrainingDataset (query-backed)
    Name string; Version int
    Database string                  // online feature-store DB (Query.project)
    Joins   []Join                   // TrainingDatasetJoin, sorted by Index before use
    Filters []Filter                 // TrainingDatasetFilter tree, flattened form (see below)
}
type Join struct {
    Index, ParentIndex int           // Index 0 = the left/label FG; ParentIndex 0 = star level
    Type JoinType                    // INNER(0) FULL(1) CROSS(2) LEFT(3) RIGHT(4): Calcite ordinals
    FG string                        // FeatureGroup name_version reference
    Prefix string
    Conditions []JoinCondition       // {LeftFeature, RightFeature}
    Features []TDFeature             // {Name, Type, Index, Label, InferenceHelper, TrainingHelper}; Type preserves persisted collect struct schema
    AggSpec map[string][]string      // insertion-ordered (kept as ordered slice internally)
    AggWindow *int64
    CollectN *int; CollectOrderBy string; CollectAscending bool
}
type Filter struct {                 // one AND leaf; Or:true marks an OR node (→ conjunctive == nil)
    FG string; Feature string; Condition SqlCondition; Value *string
    ValueFeatureGroup *string; JoinIndex *int; Or bool
}
type BuildOptions struct { Batch, InferenceHelperColumns, LoggingMetaData, FeatureVectorWithInferenceHelpers bool }
```

`spec.Validate(view)` ports the definition-time gates of
`QueryController.java:323-530` (`validateAggregate…`): function
allow-lists, integer operands for greatest/least, SUM/AVG numeric,
MIN/MAX not complex, duplicate output names, spec length ≤ 2000,
window > 0, TIMESTAMP event time last in PK, window ≤ 100 y and ≤ TTL,
online history-layout guard. It returns a typed `GateError{Code,
Message}` with the Hopsworks `RESTCodes` name so tests can assert the
exact gate.

Also port `QueryController.convertCollect` (:741-843): N > 0,
configured maximum N and N × selected-feature count, explicit/default
order-column resolution, column existence, at least one value field,
online complex/binary value-field rejection, and the last-PK-column
layout with a nonempty entity prefix. Include aggregate/collect mutual
exclusion. Validation settings are explicit fixture inputs; cover both
sides of each limit. An explicit collect order column need not be the
FG event time or a TIMESTAMP.

Type classification helpers (`IsIntegerType`, `IsNumericType`,
`IsComplexType`, `BaseType`) are ported verbatim, including the
`long` alias and the `decimal(…)` prefix handling.

---

## 3. Emitter port (`fsq/emit`)

Entry point:

```go
func Build(fgs map[string]spec.FeatureGroup, view spec.FeatureView, opt spec.BuildOptions) ([]Statement, error)

type Statement struct {              // ServingPreparedStatementDTO, only the fields the clients read
    FeatureGroupID, Index int; Prefix string
    Parameters []Param               // {Name, Index (1-based), Type (offline)}
    QueryOnline, QueryOnlineScan string
    QueryRonsql, RonsqlDatabase string
    AggregateWindow *int64; AggregateFeatureNames []string
    CollectN *int; CollectFeatureName, CollectOrderBy string; CollectAscending bool
    CollectSourceFeatures []string; CollectFilters []CollectFilter; CollectFilterApplied bool
    SnowflakeTemplate bool; SnowflakeTemplates []string
}
```

Internals are one Go function per Java method, same names and the
same order of string appends: `createServingPreparedStatementDTOS`,
`createSnowflakePreparedStatementDTOS`, `constructSnowflakeJoinsDTO`,
`buildRonsqlSnowflakeTemplates`, `buildSnowflakeStatement`,
`applyRonsqlAggregate`, `applyMysqlAggregate`, `aggregateOutputs`,
`aggregateOutputFeatureNames`, `aggregateSourceFeatures`,
`collectSourceFeatures`, `buildRonsqlCollectTemplate`,
`conjunctiveFilterConditions`, `renderRonsqlCondition`,
`renderRonsqlLiteral`, `getSelectFeatures`, `getTrainingDatasetFeatures`,
`buildDTO`, plus `NestedTrainingDatasetJoinTree.constructTree` /
`retrieveRightJoinsLists`. A file header records
`HOPSWORKS_REF = f85a653bc` (commit of the ported source) and each
function carries the Java line range it mirrors.

Divergences that are *not* ported (documented in `emit/doc.go`):
- `buildDTO`'s Calcite `generateSQL` / `wrapOnlineCollect` /
  `wrapOnlineScan` — replaced by `fsq/mysqltwin` (§4). The RonSQL
  side of `buildDTO` (collect template, `ronsqlDatabase`, collect DTO
  fields) is ported exactly.
- Persistence / EJB lookups (`featuregroupController.getFeatures`,
  `trainingDatasetController.resolveCollectSources`) — replaced by the
  spec: retain and parse the persisted collect array<struct<...>> type,
  including field names, types and order. SQL projection sources are
  PKs plus struct fields; the output struct schema is a separate list.
- `getOfflineFeaturestoreDbName` — unused by RonSQL templates.

Gate outcomes are values, not panics: a suppressed template is an
empty string / nil slice exactly like the DTO; thrown Hopsworks
exceptions become `GateError`.

Golden conformance (`emit/golden_test.go`): every file in
`testdata/hopsworks_golden/` is `{name, fgs, view, options, expected:
{statements[], gateOrException}, validationSettings}`. Capture the complete
ordered statement set and every relevant DTO field: SQL variants,
database, index, prefix, parameters, aggregate metadata, collect name,
N, order/direction, source fields, filter metadata and snowflake marker.
Capture persisted collect field types/order alongside the DTOs.
RonSQL text and DTOs must match exactly. Seed set, transcribed from
`TestPreparedStatementBuilder.java`:

| fixture | source assertion |
|---|---|
| `agg_greatest_fold_single.json` | :375-379 (COUNT + MAX(GREATEST) + `WHERE pk_feature = ?`) |
| `agg_batch_group_by.json` | :403-404 |
| `agg_mysql_twin_window.json` | :431-440 (`queryOnline` single + batch, window 3600) |
| `snowflake_inner_single_batch.json` | :533-544 |
| `snowflake_left_chains.json` | :589-599, mixed → null |
| `snowflake_gates.json` | :615-636 (RIGHT → null, partial key → null) |
| `collect_desc.json` | :652-653 |

Java-generated fixtures are required before E2 is complete.
`HopsworksGoldenDump` exercises the complete builder entry paths with
fixture-backed dependencies; private-emitter fixtures remain supplemental.
Record the Hopsworks commit and provenance. Transcribed fixtures carry
`"source": "transcribed"` and cannot establish conformance.
Cover prefixes, helper/label options, self-join filter scoping,
composite keys, collect metadata and all gate outcomes. Check in the
captured Calcite MySQL statements: E4 executes these as independent
references against the reconstructed twins and RonSQL vector path.
No Java runtime or Maven build is required by MTR.

---

## 4. MySQL twins (`fsq/mysqltwin`)

Semantic twins for the L2 oracle; textual identity with Hopsworks'
Calcite output is explicitly out of scope.

| shape | twin |
|---|---|
| S1–S5 aggregate | exactly `applyMysqlAggregate` (string-built in Java, so this one **is** conformant and lives in `emit`) |
| S6 collect single | `SELECT * FROM (SELECT cols, ROW_NUMBER() OVER (PARTITION BY pk… ORDER BY ord DESC) AS hopsworks_collect_rank FROM db.fg WHERE pk = ? [AND filters]) AS hopsworks_collect_src WHERE hopsworks_collect_rank <= ? ORDER BY hopsworks_collect_rank [DESC]` and the scan form `SELECT cols FROM db.fg WHERE pk = ? [AND filters] ORDER BY ord DESC LIMIT ?` |
| S6 collect batch | same window form with `pk IN (?)` |
| S7/S8 snowflake | `SELECT j2.col AS prefixcol, … [, fg0.pk AS pk] FROM db.root AS fg0 [LEFT] JOIN db.child AS j2 ON j2.right = fg0.left [[LEFT] JOIN …] WHERE fg0.pk = ? | fg0.pk IN (?)` with the persisted join type per hop |
| point read (non-agg, non-collect FG) | `SELECT cols FROM db.fg WHERE pk = ? | (pk…) IN (?)` (batch adds the PKs to the select list) |

---

## 5. Binding (`fsq/bind`)

```go
func Bind(template string, params []spec.Param, values []Value, opt BindOptions) (string, error)
```
Replaces the k-th `?` with the k-th value rendered by offline type
(rules in `shape_catalog.md` §0): integers/decimals/doubles bare,
strings single-quoted with `''` doubling, DATE `'YYYY-MM-DD'`,
TIMESTAMP `'YYYY-MM-DD HH:MM:SS[.fff]'` UTC. A batch value is
`[]Value` rendered as a comma list inside the existing parentheses.
`AggregateWindow` appends one more value: `opt.Now - window` where
`opt.Now` defaults to `FS_NOW` (never the wall clock unless
`--now wall` is given). The MySQL twin is bound by the same function
(so `?` conventions stay identical, including MySQL's `LIMIT ?` for
the collect rank cap, which `Bind` fills from `CollectN`).

`bind` also owns `LiteralFor(type, v)`, reused by `fuzz` to generate
filter literals that pass `renderRonsqlLiteral`.

---

## 6. Executors (`fsq/exec`)

```go
type Engine interface {
    Name() string
    Query(ctx, sql string, opt QueryOptions) (Result, error)   // opt: Database, Explain bool
    Probe(ctx) error                                           // cheap liveness check
}
type Result struct {
    Header []string; Rows [][]Cell          // Cell{Value string; Null bool}; numeric text stays exact
    Raw string; Latency time.Duration
    Phases map[string]int64                 // x-ronsql-phases (RDRS only), incl. rows
    Explain string                          // when opt.Explain
}
type Outcome int // OK, CleanReject, Retryable, Timeout, Crash, Error
```

- **RDRS** (`rdrs.go`): `POST /<APIVersion>/ronsql` with
  `{query, database, outputFormat:"JSON", explainMode:"ALLOW"|"FORCE"}`
  through `client.RestClient.PostWithHeader` (header
  `x-ronsql-phases`); per-request timeout from `--timeout`. RDRS
  answers every RonSQL error with HTTP 500 and a text body
  (`ronsql_operation.cpp`): `Caught exception: <msg>` for a
  `RonSQLPermanentError` → `CleanReject`; `Caught RonSQLRetryableError
  after 10 attempts: <msg>` (RDRS already retried) → `Retryable`;
  HTTP 429 → rate limited (retried after a pause); HTTP 400 → request
  or database validation error (`Error`); anything else → `Error`; a
  transport failure or timeout followed by a failed `Probe()` →
  `Crash`. The classification table lives in `random_generator.md` §3.
- Decode JSON preserving nullness, string values and numeric tokens
  (`json.Number`, never a float64 intermediate for integer/decimal
  cells). Preserve/check output aliases, reject duplicate object keys,
  and validate empty-result output schema with a companion metadata or
  EXPLAIN check. TEXT remains a separate formatting check.
- **CLI** (`cli.go`): `ronsql_cli --connect-string C -D db
  --output-format JSON --execute-file f`; exit code 1 → `CleanReject`,
  3 → `Retryable`. Off by default for join shapes (dictionary-cache
  caveat) and enabled with `--engines rdrs,cli,mysql`.
- **MySQL** (`mysql.go`): one `sql.DB` per worker with `SET time_zone
  = '+00:00'` and `USE <db>` at connect; text-protocol values (`[]byte`)
  retain exact value bytes and `nil` sets `Cell.Null`; never convert it
  to the string "NULL". Decode RonSQL string escapes before comparing
  with these bytes. `EXPLAIN` for
  MySQL is `EXPLAIN FORMAT=TREE` (informational only).
- Worker pools: `--threads N` for `.fs_verify` and the fuzzers; each
  worker owns one instance of every engine.

---

## 7. Canonicalization and L1 comparison (`fsq/canon`)

Inputs: two `Result`s and the column type list (from the spec for
projected columns, inferred for aggregates: COUNT → integer, SUM/MIN/
MAX → source type, AVG → double, DECIMAL SUM → decimal).

Rules, applied per cell before comparison:

| type | rule |
|---|---|
| NULL | compare explicit nullness; SQL NULL differs from string `NULL`, empty string and missing |
| integers | exact string compare after stripping a leading `+` |
| DECIMAL | parse with `math/big.Rat`; equal iff exact (`12.3` = `12.30`) |
| DOUBLE / FLOAT | parse; equal iff `|a-b| ≤ tol·max(|a|,|b|)`, `tol` default `1e-9` (`--tolerance`); use zero tolerance only for fixtures whose operation is known exact |
| TIMESTAMP / DATETIME | strip trailing `.000…`; then exact |
| DATE / strings | exact bytes (no case folding: a collation divergence must surface) |

Rows are compared as sorted multisets unless the case is
`Ordered` (S6b, any ORDER BY shape), in which case order matters.
Production output names and column counts must match exactly; an alias
mismatch fails even when row values match. For explicitly designated
envelope cases only, `--relaxed-headers` permits informational
`HEADER-ONLY`. Output names of a zero-row result cannot be checked:
RonSQL's JSON form of an empty result is `{"data":[]}` with no column
list and its TEXT form has no header line, so two empty results compare
equal with an informational note (E3 run 3); a non-empty result against
an empty one, or an empty result with a different header, still fails.

`Diff(a, b) DiffReport` produces a unified diff of the canonical TSVs
(same look as `ronsql_compare.inc` output) for the dump directory.

---

## 8. Vector oracle (`fsq/vector`)

A serving read is modelled as: for each `Statement`, execute the
RonSQL template set (RDRS) and the MySQL twin (MySQL), then apply the
documented client folds to each side and compare the assembled
per-entity vectors.

```go
type Vector map[string]VectorCell      // feature name → {Value string; Null, Missing bool; Array []Vector}; retain struct field order/types separately
func FoldRonsql(st Statement, res map[string]Result, keys []Value) map[EntityKey]Vector
func FoldMysql (st Statement, res Result,           keys []Value) map[EntityKey]Vector
func Compare(a, b map[EntityKey]Vector, policy Policy) []Mismatch   // Policy.MissingEqualsNull
```

Folds (from the DTO comments and the ported code):
- aggregate: one row per entity; RonSQL outputs get `Prefix` applied
  after the fetch, MySQL outputs are already prefixed; batch entities
  absent from the GROUP BY output get COUNT → `0`, others → NULL using
  `AggregateFeatureNames`.
- collect: rows sorted by `CollectOrderBy` (newest first, oldest first
  when `CollectAscending`), folded into one array feature
  `CollectFeatureName` whose elements carry exactly the persisted
  struct fields in schema order, including `CollectOrderBy` even when
  it belongs to the PK. Exclude only projection-only serving keys and
  the MySQL rank helper. Preserve field types/nullness and apply the
  declared feature prefix. Check asc/desc, empty arrays and independently
  specified expected arrays; two identical folds are not sufficient.
- snowflake: per template, overlay projected columns by output alias;
  a chain with no row leaves its features `Missing`; batch rows are
  keyed by the projected root PK. MySQL LEFT JOIN yields NULL cells;
  `MissingEqualsNull` is enabled only for LEFT snowflake comparisons
  and accepts MySQL NULL against a missing RonSQL chain, not the reverse.
  Returned rows must contain their declared columns. Accepted misses are
  counted as `left-miss`; missing aggregate/collect fields remain errors.
- point read: verbatim.

E4 implementation notes (`phase_e4.md` §2): vector feature names are
the feature-view names (prefix applied to RonSQL aggregate outputs after
the fetch, MySQL aliases as emitted, `<prefix><collectFeatureName>` for
the array with unprefixed struct fields inside); batch rows are keyed by
the unprefixed parameter columns on RonSQL and the prefixed aliases on
MySQL; snowflake root features (served by the pk-read path, in no
template) are reported as `not-served`, not compared; the compared set
is the union of template-served features plus declared outputs; the
data-model expectations of `cases/expect.go` check the MySQL fold
independently (`EXPECT-FAIL`), restricted to the modelled features.
They expect explicit NULLs for LEFT-hop misses and use strict missingness.
Duplicate entity rows within one template and unrequested/NULL batch
keys are fold errors; overlays across separate snowflake templates remain
valid. MySQL-only groups are counted as skipped, missing references fail,
and zero-comparison coverage is non-failing UNTESTED, never SUPPORTED.
--allow-reject accepts only clean rejections and cannot override earlier
mismatches. The six review corrections await user verification. A separate
`.fs_verify --golden` runner now implements captured Java MySQL execution
(§3 / A6), Go MySQL twin comparison and RonSQL vector comparison against
matching fixture data with shared keys/time and retained Java provenance.
Its first full user-run corpus regression passed with F0/F7 expected
rejections and MySQL-only coverage reported separately. The user confirmed
Go unit tests and single/repeated A6 MTR runs passed. Earlier L1/L2 MTR
review reruns remain unconfirmed, as recorded in `phase_e4.md` §§6–7 and
`mysql-test/suite/ronsql_fs/findings/BUGS_TODO.md`.

---

## 9. Case matrix (`fsq/cases`)

`cases.Enumerate(schema, sf, opts) []Case` is deterministic (no
randomness) and is the source for `.fs_verify --all`, the golden MTR
test, and the benchmark registry's SQL:

```
Case{ ID "S3-b100-k15-f1", Shape, View spec.FeatureView, Options, Keys []Value (by key class),
      Filters, Ordered bool, ExpectReject string, ExplainPins []string, Notes }
```
Dimensions: shape S1–S10 (S6 in both forms) × key class (from
`data_model.md` §6: 0 rows, 1, 5, 50, 300, bound-aligned, NULL hop,
dangling hop, string key, composite) × mode (single, batch 10, batch
100, batch 1000 for S3/S7 only) × filters (none, one numeric, one
string, LIKE, two leaves) × window (none, 1 h, 7 d, 90 d). The matrix
is pruned by the Hopsworks gates (a gated combination is kept as a
`GATED` case asserting that no template is emitted). Approximately
250 cases at sf 0.01.

`ExpectReject` is set from a small table (`cases/known.go`) so that a
known engine gap (e.g. S6 CTE form, R1) reports `REJECT(expected)` and
does not fail the run; the table is the only place such knowledge
lives and it is cross-referenced to the ledger.

A versioned requirements manifest maps builder branches, gates, DTO
contracts and the edge cases in `data_model.md` §11 to mandatory case
IDs and required L1/L2/golden evidence. Include emitted binary/complex
snowflake projections and composite child hops even when unsupported.
Report each requirement as SUPPORTED, UNSUPPORTED, HOPSWORKS-GATED or
UNTESTED; missing fixtures, skipped cases and absent evidence cannot
count as supported. Native future shapes do not replace emitted ones.

`.fs_verify --requirements --all --vectors` runs the complete manifest.
It fails for unsupported/untested required emitted cases, incorrect
gates, alias/schema/value mismatches or missing Java conformance.
Known-rejection allowances apply only to regression/discovery runs and
cannot override requirements mode. Report engine/Hopsworks commits,
configuration and fixture provenance; correct Hopsworks gates are
reported separately, not counted as RonSQL-supported statements.

---

## 10. CLI surface (`internal/shell/fs.go`)

| command | purpose |
|---|---|
| `.fs_load <sf> [threads] [batch] [--db D] [--hash-twin] [--sink mysql\|rdrs]` | create DB + tables (DDL port) and load (§`data_model.md` 8.3) |
| `.fs_drop [--db D]` | drop the database |
| `.fs_emit_mtr <dir> [--sf 0.01]` | write `fs_schema.inc`, `fs_data.inc`, `fs_drop.inc`, `body_templates.inc` |
| `.fs_show <case\|shape> [--batch]` | print the spec, RonSQL templates, MySQL twins, parameters |
| `.fs_explain <case>` | RonSQL EXPLAIN (FORCE) and MySQL EXPLAIN side by side |
| `.fs_verify [--requirements] [--all \| --shape S1,S3 \| --case ID] [--db D] [--sf 0.01] [--engines rdrs,mysql] [--vectors] [--threads N] [--timeout 30s] [--tolerance 1e-9] [--now FS_NOW] [--dump-dir P] [--json P] [--quiet]` | run cases: L1 (default) and L2 (`--vectors`) |
| `.fs_fuzz spec\|envelope --seed S --count N [--db D] [--sf] [--timeout] [--dump-dir] [--ledger P] [--shrink] [--quiet]` | E6/E7 |
| `.bench_ronsql fs_hw_* / .bench_sql fs_hw_*` | registry entries generated from `cases` (E5) |

Output contract (stable for MTR, `--quiet` suppresses everything
else):

```
CASE <id> <shape> <mode> PASS|FAIL|REJECT|REJECT(expected)|KNOWN-WRONG|KNOWN-ERROR|GATED|SKIP|HEADER-ONLY <latency_ms> [<engine> <message>]
SUMMARY cases=<n> pass=<n> fail=<n> reject=<n> expected_reject=<n> gated=<n> skip=<n> header_only=<n>
```
In regression mode, exit code 0 iff `fail = 0` and unexpected `reject = 0` (`--allow-reject`
downgrades unexpected rejects to a warning during discovery). `--json`
writes the same per case with the bound SQL, both raw outputs, phases,
and the diff. `--dump-dir` writes `<id>.sql`, `<id>.mysql.tsv`,
`<id>.ronsql.tsv`, `<id>.diff` for failures only (all cases with
`--dump-all`).

---

## 11. MTR contract (`mysql-test/suite/ronsql_fs`)

- `my.cnf`: copy of `suite/ronsql_cte/my.cnf` with `DataMemory=200M`.
- `include/fs_schema.inc`, `fs_data.inc`, `fs_drop.inc`: generated by
  `.fs_emit_mtr`, checked in, regenerated only by that command (a Go
  test compares the checked-in files with a fresh render so drift
  fails `go test`).
- `t/ronsql_fs_smoke.test` (E1), `t/ronsql_fs_templates.test` (E3):
  static SQL through `ronsql_compare.inc` with `$strict_diff=yes`,
  `$suppress_ronsql_cli=yes`; string-literal cases use the
  `QUERY_FILE` form; EXPLAIN pins through `ronsql_explain.inc`; row
  pins through `ronsql_phase_rows.inc`. Both files are also generated
  by `.fs_emit_mtr` from `cases` (subset flagged `MTR: true`).
  Cases whose statements use `AVG` or touch a DECIMAL / DOUBLE / FLOAT
  column carry `Canon: CanonNumeric` (set by the case builder from the
  schema) and compare through the include's `$canonicalization_script`
  sed hook with trailing fractional zeros stripped on both sides
  (formatting findings F2/F3); everything else is byte-strict.
- `t/ronsql_fs_vectors.test` (E4), `t/ronsql_fs_fuzz_spec.test` (E6),
  `t/ronsql_fs_fuzz_env.test` (E7): drive the binary:
  ```
  if (!$RONDB_CLI) { --skip rondb-cli binary not found }
  --let $FS=$RONDB_CLI --host=127.0.0.1 --mysql-port=$MASTER_MYPORT --rdrs-host=$RDRS_NOKEY_HOST --rdrs-port=$RDRS_NOKEY_PORT --no-rondis --quiet
  --exec $FS -e ".fs_verify --vectors --db fs_test --sf 0.01 --seed 1 --count 200 --dump-dir $MYSQLTEST_VARDIR/log/fs_vectors --quiet"
  ```
  The `.result` records only the `SUMMARY` line (and `FAIL` lines,
  which must be absent), so it stays stable while the case set grows.
- Time zone: the MTR mysql client path already sets `+00:00`
  (`ronsql_compare.inc`); the Go MySQL engine sets it per connection.

---

## 12. Unit-test matrix (runs with `go test ./internal/fsq/...`, no cluster)

| package | tests |
|---|---|
| `spec` | JSON round-trip; every definition-time gate (table-driven, one row per `RESTCodes` name) |
| `ddl` | DDL strings for all twelve FGs vs `testdata/ddl_golden/*.sql`; hash vs ordered PK; TTL comment; type mapping incl. overrides |
| `data` | formulas at sample coordinates; row-count classes sum to 576/53/18; checksum values; SQL renderer vs `testdata/mtr_golden/fs_data.inc`; `Pick`/`Rows` |
| `emit` | Hopsworks golden fixtures (byte-equal); gate table; prefix / alias / dedup rules; multi-child and chained snowflake; filters rendering (operators, literal validation, `''` doubling, refused literals) |
| `mysqltwin` | one golden per twin shape |
| `bind` | literal rendering per type; IN lists; window literal; `LIMIT ?` fill; error on arity mismatch |
| `canon` | each rule of §7 with positive and negative pairs; ordered vs unordered; header-only |
| `vector` | folds on synthetic results (collect asc/desc, batch defaults, LEFT-miss policy) |
| `cases` | determinism (same input → same IDs), gated cases present, count per sf |
| `mtr` | rendered `.inc`/`.test` text vs golden |
| `fuzz` | seed reproducibility; shrinker converges; classifier table |

Integration tests (`exec`, `.fs_verify`) run only against a live
cluster and are exercised through MTR.

---

## 13. Open points for E2/E3 (not blocking the design)

1. Whether `Join.Index == 0` for the left FG is always present in
   persisted feature views (the Hopsworks tree treats index 0 as the
   star root); the spec model requires exactly one join with index 0.
2. (settled in P4) RDRS error bodies: HTTP 500 for every RonSQL error,
   `Caught exception:` = permanent, `Caught RonSQLRetryableError after
   10 attempts:` = retryable exhausted; see `random_generator.md` §3.
3. Header naming of unaliased aggregates on both engines (affects
   `HEADER-ONLY` frequency) — the Hopsworks templates alias every
   output, so only E7 probes are affected.
