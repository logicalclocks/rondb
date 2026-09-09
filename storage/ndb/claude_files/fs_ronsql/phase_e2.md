# E2 — Spec model, emitter port, golden conformance (2026-09-09)

**Status: DONE 2026-09-09 — all 45 Hopsworks golden fixtures byte-identical
(see §5).**

## 1. Contract: the Hopsworks golden fixtures

`HopsworksGoldenDump.java` (Hopsworks commit `86cba3c1e`, on top of
`f85a653bc`) exports 45 fixtures, imported into
`tools/rondb-cli/internal/fsq/testdata/hopsworks_golden/` (commit
`cf2d6ff1b25`). Each fixture is `{input, expected}`:

- **input** = `CaseSpec`: feature groups (`fgs`), persisted joins
  (`joins`: index, parent, fg, type, prefix, on, features with persisted
  types, aggregate, window, collectN, orderBy, ascending), filter tree
  (`filters`), builder `options`, an optional `definition`, the
  training-dataset flag and the validator settings.
- **expected** = the complete ordered `ServingPreparedStatementDTO` list
  (every field, nulls included; `queryOnline` is the Calcite text) or a
  named exception, or for definition cases the normalized collect /
  aggregate settings.

The Go spec model unmarshals the fixture input directly, so the fixture
schema *is* the spec format (framework_design.md §2 amended).

## 2. What was built

| Path | Content |
|---|---|
| `internal/fsq/spec/spec.go` | `FeatureGroup` gains `ID`, `Online *bool` (fixture "online"), `OnlineDatabase()` (`golden_<fs>_fs`, the dumper's mock); the DDL config field is now `OnlineConfig` (JSON `onlineConfig`). E1 callers updated. |
| `internal/fsq/spec/view.go` | `View`, `Join`, `TDFeature` (persisted type), `Filter`, `Options`, `Definition`, order-preserving `AggSpec` (custom JSON), `JoinType` / `SqlCondition` / `SqlFilterLogic` names, `GateError` with the `RESTCodes` names, `Normalize()` (feature indexes in document order unless given, null feature types resolved from the FG, left feature group of non-root joins), `JoinsSorted()`. |
| `internal/fsq/spec/validate.go` | `ValidateDefinition`: port of `QueryController.convertCollect` (:741-843) and `convertAggregate` (:357-530) with verbatim messages; canonical lowercase functions; window / TTL / history-layout guards. |
| `internal/fsq/mysqltwin/calcite.go` | The Calcite output re-rendered in the observed text: point read, `wrapOnlineCollect` window, `wrapOnlineScan`, nested snowflake join query (`fg<n>` aliases, `INNER JOIN` / `LEFT JOIN`, `IN ?` batch keys), key predicates, filter literals. |
| `internal/fsq/emit/statement.go` | `Statement` = the DTO with the fixture's JSON names; `TemplateCount()`. |
| `internal/fsq/emit/emit.go` | The port of `PreparedStatementBuilder` (method for method, HOPSWORKS_REF `f85a653bc`): star path with select-feature rules, aggregate/collect source features, filter rendering and gates, `buildDTO` (collect DTO fields, RonSQL collect template), `applyRonsqlAggregate`, `applyMysqlAggregate`, snowflake subtree construction (`retrieveRightJoinsLists`, `constructTree`, `validateExtractPrimaryFeatures`, nested query with the `checkJoinOnIsPrimaryKey` exceptions, `buildRonsqlSnowflakeTemplates`, `buildSnowflakeStatement`), collect struct parsing (`parseStructFieldNames`). |
| `internal/fsq/emit/golden_test.go` | `TestGoldenConformance`: every fixture through `ValidateDefinition` or `Build`; the whole `expected` object compared structurally (statements with all fields, or the gate code + user message); per-field diff on mismatch; template-count assertion. Plus unit tests for struct parsing and literal rendering. |

## 3. Verification (user-run)

```
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql/tools/rondb-cli
go test ./internal/fsq/...            # all packages incl. the E1 ones after the field rename
go test -v -run TestGoldenConformance ./internal/fsq/emit 2>&1 | grep -E "^(=== RUN|--- (PASS|FAIL)|\s+golden)" | head -80
```

Expected: 45 subtests. Any `--- FAIL` prints the fixture name and the
field(s) that differ (`<fixture>.statements[i].<field>`), with got/want
text.

## 4. Known gaps and deviations (before the run)

- Calcite forms not covered by any fixture are best-effort: batch point
  reads on a composite key (`(fg0.k1, fg0.k2) IN ?`), filter literals on
  non-aggregate statements, `FULL`/`CROSS` join keywords. A fixture
  covering them would settle the text.
- Two `AGGREGATE_INVALID` messages embed a Java `HashSet.toString()` of
  the allowed functions; their element order is a guess
  (`[sum, min, max, avg, count]`, `[least, greatest]`) until a fixture
  covers them.
- `QUERY_FAILED_FG_DELETED` and the internal errors Java would raise as
  runtime exceptions (aggregate without entity key, selected name that is
  not a column) surface as errors without fixture coverage.
- `GateError` compares code and user message; developer messages are
  always null in the fixtures.

## 5. Results

- 2026-09-09 `go test ./internal/fsq/...`: `ok` for `data`, `ddl`, `emit`
  (two mechanical compile fixes on the first attempt: a missed field
  rename in `ddl.go`, an unused variable in `emit.go`).
- `TestGoldenConformance`: **45 / 45 subtests pass** — every fixture's
  complete `expected` object matches: the RonSQL templates (aggregate
  point / batch / window / string key, all seven filter operators and
  the three rejected filters, self-join filter scope, collect desc / asc /
  explicit order / batch-gated, snowflake INNER combined / LEFT per-chain
  / mixed-gated / binary projection / cross-store gate / helper options /
  batch), every DTO field including the Calcite `queryOnline` and
  `queryOnlineScan` texts, and the definition validators' outputs and
  exception messages (`COLLECT_*`, `AGGREGATE_*`,
  `JOIN_ON_PARTIAL_PRIMARY_KEY`, `COLLECT_UNSUPPORTED_ONLINE_FILTER`).
- Requirement A6 (Java-generated complete builder fixtures pinned to the
  Hopsworks commit) is met for the initial corpus; the §4 gaps remain
  fixture-less until the corpus grows.
