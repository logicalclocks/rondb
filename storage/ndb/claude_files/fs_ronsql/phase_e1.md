# E1 — Schema, data, loaders, smoke test (2026-09-09)

**Status: code written, NOT yet built or run.** The verification steps
below are to be run by the user; their outcome is recorded in §5.

## 1. What was built

| Path | Content |
|---|---|
| `tools/rondb-cli/internal/fsq/spec/spec.go` | `FeatureGroup`, `Feature`, `OnlineConfig` (mirroring `Featuregroup`, `FeatureGroupFeatureDTO`, `OnlineConfigDTO`), `TableName()`, `PrimaryKeys()`, and the type classification of `QueryController.java:330-352` (`BaseType`, `IsIntegerType`, `IsNumericType`, `IsComplexType`). |
| `tools/rondb-cli/internal/fsq/ddl/ddl.go` + `ddl_test.go` | Byte-faithful port of `OnlineFeaturegroupController.buildCreateStatement`, `getOnlineType`, `resolvePkIndexClause`, `addTtlCommentToConfig`, `ensureReadBackupComment` (HOPSWORKS_REF `f85a653bc`). Tests pin the DDL text for the history FG, the hash-PK + TTL + tablespace + default-value case, existing-comment merging, and the error paths. |
| `tools/rondb-cli/internal/fsq/data/formulas.go` | `FSNow`, scale resolution, row-count classes, time formulas, every row formula of `data_model.md` §5 as typed Go, literal rendering helpers. |
| `.../data/schema.go` | The twelve feature groups as `spec.FeatureGroup` values (load order), `SchemaMap()`. |
| `.../data/classes.go` | `KeyClass` + `InClass` + `Pick` (data_model.md §6). |
| `.../data/checksum.go` | Per-table `(COUNT(*), SUM(x), COUNT(y))` expectations regenerated in memory. |
| `.../data/tables.go` | `TableGen` per table: Go tuple emitter (loader) **and** the `INSERT … SELECT` body over the MEMORY helper tables (MTR include), side by side. |
| `.../data/sqlgen.go` | `RenderSchemaInc`, `RenderDataInc` (helper tables, chunked inserts ≤ 4000 rows/statement, checksum assertions that `--die` on mismatch), `RenderDropInc`, `RenderSchemaSQL`. |
| `.../data/loader.go` | `CreateSchema`, `Load` (multi-row `INSERT`, N workers over disjoint entity ranges, one connection each with `time_zone='+00:00'` in the DSN, idempotent per table), `Verify`, `Drop`. |
| `.../data/data_test.go` | Class sums, scale resolution, time formulas at the documented anchor customers (111 daily/offset 0; 1 hourly/17 min; sessions ms), hops (NULL / dangling), value spot checks, **hand-computed** row counts at sf 0.01, tuple counts vs classes, `Pick`, MTR rendering markers, large-scale refusal. |
| `tools/rondb-cli/internal/shell/fs.go` + `repl.go` | `.fs_load`, `.fs_drop`, `.fs_emit_mtr`, `.fs_schema`; help text and completion entries. |
| `mysql-test/suite/ronsql_fs/my.cnf`, `rdrs_config_template.json` | Copy of the `ronsql_cte` cluster config with `DataMemory=200M`. |
| `mysql-test/suite/ronsql_fs/t/ronsql_fs_smoke.test` | Load + row counts + `SHOW CREATE TABLE` round-trip + 5 asserted compares (S1 ×2, S2, S6b ×2) + 11 recorded probes (S3, S6 CTE = R1, S7 ×3, S8b, S10 ×2, S4 collation, hash-only PK, TIMESTAMP(3) window, S9 composite). |

Not written by hand: `mysql-test/suite/ronsql_fs/include/fs_{schema,data,drop}.inc`
are **generated** by `.fs_emit_mtr` (step 3 below) and then checked in.

## 2. Design notes and deviations from the plan

- MTR tables live in database `test`, not `fs_test`: `ronsql_compare.inc`
  hard-codes `-D test` / `"database":"test"`. Bench data uses `fs_bench`.
- The MTR include uses one MEMORY `_seq` helper of 10^k rows (k = digits of
  the largest entity count) plus `_seq300`, `_ntx`, `_nsess`, `_nbal`,
  `_spacing`, `_offset` lookup tables so the SQL formulas read like the
  Go ones. Row-count classes are joined, not computed with `ELT`.
- The hash-only twin is loaded by `INSERT … SELECT * FROM transactions_1`
  in MTR and regenerated from the formulas by the Go loader (no giant
  cross-table transaction at bench scale).
- Checksum assertions in the include use unquoted identifiers because
  mysqltest evaluates the text between the first and last backtick of a
  `--let` value as a query.
- `.fs_load` prefers MySQL multi-row `INSERT` over the RDRS batch write for
  exact DECIMAL / TIMESTAMP typing; default 8 threads × 500 rows.

## 3. Verification (user-run)

```
# 1. Go unit tests (no cluster)
cd tools/rondb-cli && go test ./internal/fsq/...

# 2. Build the CLI (and rdrs2 / ronsql_cli / mysqld / ndbmtd if not built)
cd <build> && make rondb-cli -j8

# 3. Generate the MTR includes (no cluster needed) and check them in
<build>/runtime_output_directory/rondb --no-mysql --no-rdrs --no-rondis \
    -e ".fs_emit_mtr mysql-test/suite/ronsql_fs/include --sf 0.01 --db test"
ls -l mysql-test/suite/ronsql_fs/include/

# 4. Record and run the smoke test
cd <build>/mysql-test
./mtr --suite=ronsql_fs ronsql_fs_smoke --record
./mtr --suite=ronsql_fs ronsql_fs_smoke

# 5. Bench-scale load on an interactive cluster (optional in E1)
./mtr --suite=ronsql_fs ronsql_fs_smoke --start-and-exit
<build>/runtime_output_directory/rondb --mysql-port <MASTER_MYPORT> --rdrs-port <RDRS port> --no-rondis
.fs_load 0.1 8 500           # 10k customers, ~540k rows, hash twin on
.fs_load 0.1 8 500           # second run must skip every table
.fs_drop
```

What to look at in the recorded `r/ronsql_fs_smoke.result`:

1. The include did not `--die`: all checksums matched (data formulas and
   SQL formulas agree).
2. `SHOW CREATE TABLE sessions_1` shows `timestamp(3)`, the
   `ttl_index`, and `COMMENT='NDB_TABLE=TTL=3153600000@event_time,READ_BACKUP=1'`
   (or how the server normalised it); `transactions_hash_1` shows
   `USING HASH`.
3. The five strict compares have empty `== Diff ==` blocks.
4. Each PROBE block's `== Diff ==`: empty = supported; a `Caught
   exception:` body = clean rejection (expected for the S6 CTE form, R1);
   anything else = finding.

## 4. Failure guide

- `go test` failure in `data_test.go` `TestChecksumsSmallScale`: the
  hand-computed counts in the test disagree with the formulas — check
  §4 of `data_model.md` before touching either.
- Include `--die … checksum mismatch`: the SQL rendering of a formula
  differs from the Go formula for that table; compare the `Select` and
  `Rows` closures of that table in `tables.go`.
- `CREATE TABLE` error from the include: the server rejected a piece of
  the Hopsworks DDL (candidates: `STORAGE MEMORY` on the event-time
  column, the `TTL=…@event_time` comment with a 100-year value,
  `timestamp(3)` override). Record the exact error in §5.
- `MaxNoOfConcurrentOperations` / "Out of operation records": a chunk is
  too large; lower `MTROptions.TargetRows` (4000) and regenerate.

## 4b. E1 follow-up (2026-09-09, review A2/A4/A5)

Implements `data_model.md` §11 / §11.1:

- `internal/fsq/data/edge.go`: seven fixed-row edge tables (nullable type
  mix, big integers and decimals, strings, timestamp ticks, explicit
  collect order column, composite-key snowflake parent/child with
  binary/complex projections), named entity constants, checksums.
- `TableGen.Fixed`: literal-row tables render as `INSERT … VALUES` in the
  MTR include and load through the same tuples in `.fs_load`.
- `SQLString` escapes backslash, NUL, tab, newline and carriage return.
- `edge_test.go`: row counts, safe/limit sums, escaping, collation-unique
  keys, timestamp ticks, reversed sequence entity, checksum/schema
  wiring, MTR rendering of fixed tables.
- Smoke test: `SHOW CREATE TABLE` for `edge_ts_1` / `edge_child_1` and
  22 recorded `PROBE EDGE-*` compares (NULL propagation, AVG, all-NULL,
  exact and rounding floats, DATE bounds, big integers incl. the overflow
  probe, quoting / IN lists / NULL-vs-'NULL' / escapes / collation,
  TIMESTAMP(3)/(6) cutoff single and batch, explicit-order collect,
  composite hop incl. swapped binding, batch with dangling/NULL hops,
  VARBINARY projection).

Verification is the same as §3 (regenerate the includes, re-record the
smoke test). The `.fs_load` idempotence check now also covers the edge
tables.

## 5. Results

_(to be filled in after the run: go test output, MTR outcome, the
`== Diff ==` content of every probe, timings of the include load and of
`.fs_load 0.1`)_
