# RDRS2 Feature Sharing — Happy Path v2 (schema port + share-based access)

> **Status: DONE (2026-07, branch RONDB-1088).** All four steps landed:
> - P1 — schema port (V11–V83): `c4ae5aa6985`, `d065578a748`, `b5f7427b4a1`
> - P1.5 — dump-based post-DDL seed data + cleanups: `1dc845d1dc3`,
>   `5b2f2a0e447`, `9a3f19f5f4a`, `fac30068bd2`, `c5440325ad4`
> - P2 — server share union (`find_user_databases`): `ba993bf3c9d`
> - P3 — fixture flip membership → shares: `295e010ce3f`
> - P4 — this docs update + bench sanity
>
> Verified via full re-seed: Go feature_store / batchfeaturestore / pkread /
> batchpkread and C++ api_key_test (19) / feature_store_test (16) all green.
> Operational note: after re-seeding, **restart RDRS** — recreating the
> hopsworks database under a running server leaves its api-key cache
> invalid ("API key found in cache but is invalid").

**Answer to the driving question:** yes — we can apply all the missing
Hopsworks DDLs and express today's "API key sees every database" test model
through `shared_feature_store` rows instead of universal project membership.
The plan below does exactly that, in four steps that each stay green.
Fine-grained sharing (FG/feature level, restricted role, 400 errors, event
watchers) comes later; the old 10-step plan
(`docs/fg_fv_sharing_rdrs2_impl_plan.md`) remains the reference for it.

## How access works today (verified)

- Test fixtures make the API-key user `macho@hopsworks.ai` a **Data-scientist
  member of every project**: hard-coded rows for 999/demo0, 1000/fsdb001,
  1001/fsdb002, 1003/FSDB003, 1004/FSDB004 (`hopsworks_40_data.sql:73-105`)
  — everything except 1002/`fsdb_isolate`, which exists to be inaccessible
  (`Test_GetFeatureVector_NotShared` expects 401) — plus a membership row for
  every generic DB registered via `dynamic/hopsworks_34_add_project.sql`.
- Server: `userDBs` = the key user's project names
  (`api_key.cpp:1079` → `find_all_projects`, `rdrs_hopsworks_dal.cpp:768`);
  every endpoint checks its target DBs against that set.
- Test schema = Hopsworks **4.0 + V5–V10** only
  (`test_go/resources/testdbs/embeddings.go:73`). Missing: **V11–V83**
  (73 files in `hopsworks/docker/migration/sql/ddl/`), including V45/V46
  (sharing tables), V40 (serving_key column), V73 (api_key expiry), etc.

## Target model after this slice

- `macho` is a member of **one home project (999/demo0)** only.
- Every other project's feature store is **shared entirely**
  (`shared_feature_store.shared_entirely=1`) with project 999.
- `fsdb_isolate` stays **unshared** → the NotShared test keeps meaning the
  same thing under the new semantics ("store not shared" instead of "not a
  member").
- Server rule: allowed DBs = **own projects ∪ feature stores shared entirely
  with own projects**. Own-project access stays implicit — real Hopsworks
  never self-shares a store, so share-tables-only would break production.
- Implemented inside the API-key cache (`userDBs` union) ⇒ controllers
  untouched ⇒ feature-store AND pk-read/batch/scan endpoints all follow the
  new mechanism automatically. ronsql deferred (per earlier decision).

## Steps

### P1 — Port all missing DDLs (V11–V83) into the test schema
No server code. Both fixture trees (`test_go/resources/testdbs/` and
`test/resources/testdbs/`).
1. Copy each `V11…V83` file from `hopsworks/docker/migration/sql/ddl/`,
   named per the existing convention (`V45-FSTORE-1905-….sql`), appended to
   `HopsworksScheme` after V10 (`embeddings.go:73`) in migration order. The
   C++ tree is picked up by the CMake glob (`test/CMakeLists.txt:44-63`).
2. Expected friction, handled per-file and recorded in a new
   `resources/testdbs/PORTING_NOTES.md`:
   - **Missing base tables** (kube/serving/agent tables absent from the 4.0
     copy): add the missing CREATE TABLE from Hopsworks, or skip the file
     with a one-line justification if wholly unrelated to RDRS. Preference:
     apply; skip only as last resort.
   - **Data-migration statements** inside DDL files (e.g. V45 lines 48-68
     backfilling from `dataset_shared_with`): keep them — they are no-ops on
     our fixture data (no `dataset_shared_with` rows) but preserve fidelity.
   - **Positional INSERT breakage**: the dynamic templates
     (`hopsworks_34_add_project.sql`, `hopsworks_api_key.sql`) run AFTER the
     V-patches and use positional `VALUES` — any migration altering
     `project`/`project_team`/`api_key` (e.g. **V73 adds api_key expiry**)
     breaks them. Fix: switch the dynamic templates (and any fixed INSERT
     that breaks) to explicit column lists.
3. Green gate: drop `sentinel` DB → full re-seed → existing suites all pass
   (`api_key_test`, `feature_store_test`, Go `feature_store`,
   `batchfeaturestore`, `pkread`, `batchpkread`, `index_scan`).

**Review focus:** DDL fidelity (diffable against the Hopsworks originals),
PORTING_NOTES completeness, no test-visible behavior change.

### P2 — Server: allowed DBs = own ∪ shared-entirely (additive, fixtures unchanged)
Files: `src/rdrs_const.h`, `src/rdrs_hopsworks_dal.{h,cpp}`, `src/api_key.cpp`.
1. `rdrs_const.h`: `#define SHARED_FEATURE_STORE "shared_feature_store"` +
   used column names.
2. New DAL reader
   `find_feature_stores_shared_entirely_with_projects(project_ids)`:
   `shared_feature_store` rows where `shared_with_project ∈ ids AND
   shared_entirely = 1`, resolved to `feature_store.name` via the existing
   fs-by-id reader (`feature_store/feature_store.cpp:224` family). Same
   `METADATA_OP_RETRY_HANDLER` / `HandleSchemaErrors` conventions as peers.
3. `APIKeyCache::get_user_databases` (`api_key.cpp:1079`): union the shared
   store names into `userDBs` (lowercased, existing string-arena ownership).
   Project ids are already available (`HopsworksProjectTeam.project_id`).
4. Untouched: `validate_api_key`, hash-before-authz ordering, controllers,
   error codes (401 on deny). Freshness = existing 180 s `refresh_job` +
   lazy load on cache miss; no event watcher in this slice.
5. Tests (C++, `api_key_test.cpp`): insert a `shared_feature_store` row via
   NDB helper (pattern of `ndb_insert_api_key`, `api_key_test.cpp:83`) →
   fresh key resolves the shared store's DB; refresh picks up insert/delete
   of share rows; membership-only behavior unchanged when no rows exist.
6. Green gate: all existing suites pass **unchanged** (fixtures still grant
   via membership; union adds nothing yet).

**Review focus:** ~1 reader + ~10 lines in `get_user_databases`; memory
ownership; zero per-request cost (resolution at load/refresh time only).

### P3 — Fixture flip: membership → shares (tests now ride the new mechanism)
No server code.
1. `dynamic/hopsworks_34_add_project.sql`: keep the `project` INSERT; **drop
   the `project_team` INSERT**; add a `feature_store` row for the project
   (name = project name, matching fixture convention) + a
   `shared_feature_store` row sharing it **entirely with project 999**.
2. `hopsworks_40_data.sql` (or a new seed patch file): remove `macho`'s
   `project_team` rows for 1000/1001/1003/1004 (keep 999); add
   `shared_feature_store` rows sharing stores 67 (fsdb001), fsdb002's store,
   68 (fsdb003), fsdb004's store entirely with 999. **No row for store 66
   (fsdb_isolate).**
3. Update `api_key_test.cpp` expectations that assert `userDBs` contents
   derived from membership (they now come from the union).
4. Green gate: full re-seed → **all** suites pass. Every existing test that
   touches a non-home DB now traverses the share path end-to-end — this is
   the point of the flip. `Test_GetFeatureVector_NotShared` still 401
   (isolate store unshared). Optionally add one explicit positive:
   share-only access to `fsdb001` asserted against a helper that checks the
   caller is NOT a member (guards against future fixture drift).

**Review focus:** fixture diff only; isolate project untouched; the
"membership rows removed" list is exhaustive (grep `project_team` seeds).

### P4 — Wrap-up
- Docs: update `docs/fg_fv_sharing_how_it_works.md` §7 and the impl-plan doc
  (happy path done; store-level only).
- Bench sanity: `test_go/run_ab_bench.sh` — expect no change (no per-request
  work added).
- Record follow-ups (below) as the input to the fine-grained phase.

## Known deviations / follow-ups after this slice
1. Only store-level `shared_entirely=1` honored; FG-level, feature-level,
   placeholder rows (correctly grant nothing), restricted role — later.
2. `FEATURE_STORE_RESTRICTED` members still get full member access.
3. Denials stay **401** (Hopsworks-parity 400 `FEATURE_NOT_SHARED` comes with
   the fine-grained phase, per decision D3).
4. Share/unshare propagation ≤ 180 s (refresh) or first key use; NDB event
   watcher on `shared_feature_store` is a later step.
5. A store-level share opens the whole database to generic endpoints —
   consistent with "shared entirely" semantics.
6. **api_key.expiry (V73)**: column now exists in the test schema; RDRS does
   not enforce expiry. Decide in the fine-grained phase whether to honor it
   (one WHERE-clause + tests).
7. ronsql unchanged (member-only), deferred.

## Verification recipe
```
cd rondb/build && make -j$(nproc)
./mysql-test/mtr --suite rdrs2-golang --start-and-exit
export RDRS_CONFIG_FILE=$(realpath mysql-test/var/rdrs.1.1_config.json)
# after fixture changes: drop the sentinel DB to force re-seed
./runtime_output_directory/api_key_test
./runtime_output_directory/feature_store_test
cd ../storage/ndb/rest-server2/server/test_go
./script.sh test hopsworks.ai/rdrs2/internal/integrationtests/feature_store
./script.sh test hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore
./script.sh test hopsworks.ai/rdrs2/internal/integrationtests/pkread
```
