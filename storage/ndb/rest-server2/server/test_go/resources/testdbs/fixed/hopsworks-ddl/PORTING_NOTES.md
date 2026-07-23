# Hopsworks schema migrations ported into the RDRS2 test fixtures

This directory holds the Hopsworks schema migrations applied on top of the
`hopsworks_40_schema.sql` + `hopsworks_40_data.sql` base fixture. Source of
truth: `hopsworks/docker/migration/sql/ddl/` in the Hopsworks monorepo
(file `V<N>__[JIRA]_name.sql` maps to `V<N>-JIRA-name.sql` here).

Execution model (see `internal/testutils/schemata.go:runQueriesWithConnection`):
the Go test runner strips `--` line comments and splits the concatenated
schema on every `;`. Consequently these files may not contain stored
procedures, triggers, `DELIMITER` directives, or semicolons inside string
literals. The whole set is concatenated in `resources/testdbs/embeddings.go`
(`HopsworksScheme`) in migration order and applied after the base schema +
data. The C++ unit tests embed the same files — `test/CMakeLists.txt` globs
this directory (single source of truth, no duplicate tree).

## Porting status (V11–V83, ported 2026-07-13)

- **68 of 73 files apply verbatim** (V5–V10 predate this port). Dependency
  chains verified: e.g. V28 creates `data_source` + `feature_store_rds_connector`
  and adds `feature_store_connector.rds_id`, which V57 renames and V61 alters;
  V36 renames `remote_group_project_mapping` → `group_project_mapping`;
  V38 creates the `serving_deployment`/`serving_model_artifact`/
  `serving_depl_component` tables that V39/V48/V64/V67/V76 alter.
- **5 files contain data-migration stored procedures** (backfills of
  pre-existing production rows). The procedure blocks are commented out with
  a `-- RDRS-P1-PORT:` marker; they are no-ops on fresh test fixtures and
  cannot run through the `;`-splitting executor. All schema statements in
  those files are kept:
  | File | Commented-out block |
  |---|---|
  | V14 (FSTORE-1642) | entire file — procedure-only backfill of USER scope onto serving api keys; no schema change |
  | V38 (HWORKS-103)  | `SplitServingTable()` backfill from old `serving` rows; the 3 CREATE TABLEs + ALTERs are kept |
  | V39 (HWORKS-1186) | `MigrateArtifactVersionToDeployment()` backfill; ALTERs kept |
  | V48 (HWORKS-2502) | `populate_scaling_config()` backfill; ADD/DROP COLUMN kept |
  | V78 (FSTORE-1412) | `backfill_ingestion_fm_configs()` cursor backfill; all DDL kept |
- Block comments (`/* … */`) in V38/V39 were converted to `--` line comments
  (the executor only strips `--`; a stray `;` inside a block comment would
  break statement splitting).
- **Safe-update mode:** V28, V55 and V83 wrap their data-migration DML in
  `SET SQL_SAFE_UPDATES = 0; … SET SQL_SAFE_UPDATES = 1;`. Under Flyway each
  migration runs in its own session, so the trailing re-enable is harmless —
  but the test executor runs ALL migrations in ONE pinned session, so V28's
  re-enable poisoned every later migration's non-key UPDATE/DELETE
  (Error 1175, first seen on V45's DELETE, then V67's UPDATE). Fixes:
  the three `SET SQL_SAFE_UPDATES = 1;` lines are commented out, and
  `hopsworks_40_schema.sql` disables safe-update mode for the whole seeding
  session (same pattern as its existing `SET FOREIGN_KEY_CHECKS` usage).
- **Pure production-data cleanup DML is commented out** where it serves no
  purpose on a fresh fixture and used unsafe (non-key WHERE) forms: V45's
  `dataset_shared_with` backfill INSERT..SELECT + DELETE, V78's v1-monitoring
  wipe DELETEs and alert status-rename UPDATE/DELETE block, V82's BREWER row
  cleanup. Data-migration UPDATEs that run cleanly (V19, V22, V28, V40, V55,
  V67, V77) are kept.
- **Positional INSERT fixes** (tables grew columns via the patches):
  `dynamic/hopsworks_api_key.sql` (V73 adds `api_key.expiry`) and the
  `cache_event_test.go` fixture constants (V74 extends
  `training_dataset_join`, V40 extends `serving_key`) now use explicit column
  lists. (`dynamic/hopsworks_add_project.sql` is unaffected — no migration
  alters `project`/`project_team`.)
- Validation performed: full re-seed + green runs of the Go
  `feature_store`, `batchfeaturestore`, `pkread`, `batchpkread` suites and
  the C++ `api_key_test` (18) + `feature_store_test` (16) against a live
  cluster, plus a simulation of the executor over the assembled schema
  (559 statements, no DELIMITER leakage, no unbalanced quotes).

## Follow-ups deliberately NOT done here

- `api_key.expiry` (V73) now exists in the schema but RDRS does not enforce
  key expiry — tracked as a fine-grained-phase decision.
- The `shared_feature_store` / `shared_feature_group` / `shared_feature`
  (V45) and `restricted_feature_group_access` / `restricted_feature_access`
  (V46) tables are created but not yet seeded or consulted by RDRS — that is
  the next step of the sharing work.

## How to re-verify after changes

Any fixture change requires dropping the `sentinel` database (or all test
DBs) to force a re-seed, then:

```
cd rondb/build && ./mysql-test/mtr --suite rdrs2-golang --start-and-exit
export RDRS_CONFIG_FILE=$(realpath mysql-test/var/rdrs.1.1_config.json)
cd ../storage/ndb/rest-server2/server/test_go
./script.sh test hopsworks.ai/rdrs2/internal/integrationtests/feature_store
```
