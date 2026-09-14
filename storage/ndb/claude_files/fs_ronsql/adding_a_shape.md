# How to add a shape (one page)

A "shape" is one statement family the Hopsworks online-serving builder
emits (or a framework target shape such as S6b).  Adding one touches the
catalog, the case matrix, the vector specs, the expectation table, the
MTR includes, the Java corpus and the requirements manifest — in that
order.  Everything below runs from `tools/rondb-cli`; builds and tests
are run by the user.

1. **Catalog** — `storage/ndb/claude_files/fs_ronsql/shape_catalog.md`:
   add `### S<n> — <name>` with the builder line reference, the template
   text, parameters, the MySQL twin and the data requirements.  If the
   emitter needs a new branch, port it in `internal/fsq/emit/emit.go`
   with the Java line reference in the comment.

2. **Data** — if the shape needs rows the data model lacks, extend
   `internal/fsq/data/formulas.go` (deterministic formulas only) and
   `data_model.md`; regenerate the MTR data include
   (`.fs_emit_mtr mysql-test/suite/ronsql_fs/include --sf 0.01 --db test`)
   and re-run `ronsql_fs_smoke` (the load checksums are asserted).

3. **Case matrix (L1)** — `internal/fsq/cases/cases.go`: add cases with
   `emitCase` (a spec through the emitter, Origin "hopsworks") or
   `sqlCase` (framework text, Origin "framework").  Ids are
   `S<n>-<variant>-k<key>`; pick keys by class from `data/classes.go`
   (ordinary / no rows / NULL hop / dangling hop / missing).  Mark
   `MTR: true` for the cases the golden MTR test should carry; set
   `Canon` when the engines legitimately format differently.

4. **Expectation table** — `internal/fsq/cases/known.go`: a known engine
   outcome (clean rejection, known wrong value, known error) gets ONE
   entry here, cross-referenced to the ledger finding; cases reference
   it via `ExpectReject` / `KnownWrong` / `KnownError`.  Never put
   engine knowledge anywhere else.

5. **Vector specs (L2)** — `internal/fsq/cases/specs.go`: add a `Spec`
   (view, params, batch, expected features) so `.fs_verify --vectors`
   folds the RonSQL templates against the MySQL twin per entity.

6. **Regenerate the templates MTR test** —
   `.fs_emit_mtr mysql-test/suite/ronsql_fs/include --cases` writes
   `t/ronsql_fs_templates.test`; `./mtr --suite=ronsql_fs
   ronsql_fs_templates --record` records the baseline (rebuild first).

7. **Java corpus** — if Hopsworks emits the shape, add a fixture: put
   a case JSON in an input directory and run
   `scripts/refresh_golden_fixtures.sh --input DIR` (the real Hopsworks
   builder produces the expected object; never hand-write it), review
   the diff, `--apply`, and keep `go test ./internal/fsq/emit/` green.

8. **Requirements manifest** — `internal/fsq/cases/requirements.go`:
   add an `R-S<n>` row (`Emitted: true` for Hopsworks shapes, `false`
   for framework targets), listing the shape, the mandatory case ids,
   the spec shape and the fixtures.  `go test ./internal/fsq/cases/`
   fails if any id is unknown or a Hopsworks-origin case is unclaimed.

9. **Fuzzers** — extend `internal/fsq/fuzz/spec.go` (sampling +
   expectation) or `envelope.go` (production + construct tags) if the
   shape has a random form; the generator unit tests check predictions
   against the emitter.

10. **Verify** — `go test ./internal/fsq/... ./internal/shell`; rebuild
    (`make rondb-cli`); `.fs_verify --shape S<n>`; `.fs_verify --vectors
    --shape S<n>`; `.fs_verify --requirements --req R-S<n> --vectors`;
    the MTR suites `ronsql_fs`, `ronsql_fs_jit`, `ronsql_fs_ng2r2`.
    Record findings in `mysql-test/suite/ronsql_fs/findings/`.
