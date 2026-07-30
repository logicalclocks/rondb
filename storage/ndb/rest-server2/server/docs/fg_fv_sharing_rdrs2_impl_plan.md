# Fine-Grained FG/Feature Sharing Enforcement in RDRS2 — Implementation Plan

Reviewable, step-wise plan. Each step is a small, independently testable PR.
Implementation is one step at a time; every step ends green on its own tests
plus the existing suites.

---

## 1. Context — why

Hopsworks added fine-grained cross-project feature sharing (FSTORE-1905:
`shared_feature_store` / `shared_feature_group` / `shared_feature`) and
in-project per-user restricted access (FSTORE-1940:
`restricted_feature_group_access` / `restricted_feature_access`), enforced by
recursive feature-view checks (FSTORE-2026, HWORKS-2744). Study docs:

- `storage/ndb/rest-server2/server/docs/fine_grained_fg_fv_sharing.html`
- `storage/ndb/rest-server2/server/docs/fg_fv_sharing_how_it_works.md`

**The gap:** Hopsworks enforces online reads via per-user MySQL GRANTs. RDRS2
authenticates with API keys and reads via the NDB API under a cluster-level
identity — GRANTs never apply. RDRS2's only authorization today is: *every
database a request touches must be one of the API-key owner's own projects*.
Verified consequences:

- A feature view containing an FG **shared** into the caller's project is
  DENIED unless the caller happens to be a member of the FG's owning project
  (shares are invisible).
- A `FEATURE_STORE_RESTRICTED` member gets FULL access to everything in their
  project (restrictions are invisible).

## 2. Decisions taken (user-confirmed)

| # | Decision |
|---|----------|
| D1 | **No project name in requests.** Access = caller's API key may reach the FV if **any** project the key's user belongs to satisfies the Hopsworks rule (own project, or share cascade from that project; restricted grants are user-scoped). |
| D2 | **Scope:** feature-store endpoints first, then **pk-read, batch pk-read, scan** also honor shares. **ronsql deferred** (documented as out of scope). |
| D3 | **Denials return HTTP 400 `FEATURE_NOT_SHARED`** (Hopsworks parity), message names the inaccessible FG/columns. |

## 3. Semantics to implement (reference)

```
caller  = API key → user_id, plus {project_id → team_role} for all memberships
FV      = set of constituent FGs; per FG: fg_id, owning feature_store_id,
          owning project_id, columns the FV uses

per candidate project P of the caller:
  if role(P) == "Feature store restricted":
      FG accessible iff restricted_feature_group_access(fg, user) exists
        can_access_entirely=1 → all columns
        else → columns in restricted_feature_access child rows
  else:
      FG fully accessible if P owns the FG's store
        or shared_feature_store(store→P, shared_entirely=1)
        or shared_feature_group(fg→P, shared_entirely=1)
      else column subset = shared_feature rows (fg→P)
      else inaccessible

FV read allowed iff ∃P such that EVERY constituent FG contributes ALL the
columns the FV uses from it. All-or-nothing: no silent column filtering.
(Restricted grants are user-scoped: they count regardless of P.)

Invariants we rely on (maintained by Hopsworks on write):
- PK + event-time columns are always included in column-level grants.
- shared_feature_store rows with shared_entirely=0 are placeholders — grant
  nothing by themselves.
```

## 4. Current RDRS2 facts (from exploration, verified)

All paths relative to `storage/ndb/rest-server2/server/`.

### Serving path
- Handlers: `src/feature_store_ctrl.cpp:559` (single), `src/batch_feature_store_ctrl.cpp:55` (batch). Request DTOs carry only `(featureStoreName, featureViewName, version)` (`src/feature_store_data_structs.hpp:79,134`).
- Metadata: `FeatureViewMetadataCache_Get` (`src/metadata.cpp:523`), key `"{fs}|{fv}|{ver}"`. Builder reads hopsworks tables via NDB API (`src/feature_store/feature_store.cpp`). `FeatureViewMetadata` (`src/metadata.hpp:149`) already has `featureStoreNames` (all stores the FV touches) and `featureGroupFeatures` (per-FG store name, FG name/version/**fg id**, columns).
- Enforcement point today: `authenticate(api_key, metadata->featureStoreNames)` at `feature_store_ctrl.cpp:679-680` / `batch_feature_store_ctrl.cpp:162-171`.

### Auth path
- `x-api-key` header only; gate `security.apiKey.useHopsworksAPIKeys` (X-macro config, `src/config_structs_def.hpp:335-346`).
- `APIKeyCache` (`src/api_key.hpp:101`): prefix → `UserDBs` entry {secret, salt, **m_user_id**, set of own-project names}. Chain: `api_key` → `users` → `project_team` (**team_role NOT read** — `rdrs_hopsworks_dal.h:44-46` holds only `project_id`) → `project` (names only, **project ids not retained**).
- Hash verified before authz errors (timing side-channel defense, `api_key.cpp:338-382`) — must be preserved.
- Cache machinery to clone: preload, periodic refresh thread (`refresh_job`, default 180 s), NDB event watcher (`api_key.cpp:749-1077`), states VALIDATING/INVALID/VALID, ref-counting. Second watcher copy in `fs_cache.cpp:656` (feature_view I/D). A third copy is due — factor the skeleton.
- Generic endpoints auth call sites: `pk_read_ctrl.cpp:117`, `batch_pk_read_ctrl.cpp:176`, `scan_read_ctrl.cpp:170`, `ronsql_ctrl.cpp:111`.

### Test infrastructure
- **The 5 sharing tables do NOT exist in any test fixture.** Embedded schema = Hopsworks 4.0 (`test_go/resources/testdbs/fixed/hopsworks_40_schema.sql` + `hopsworks_40_data.sql`) + migrations V5–V10, concatenated at `test_go/resources/testdbs/embeddings.go:73`. **Two fixture trees**: Go (`test_go/resources/testdbs/`) and C++ (`test/resources/testdbs/`, globbed into `embedded_content.hpp` by `test/CMakeLists.txt:44-63`) — new DDL goes in both.
- Go integration tests: `test_go/internal/integrationtests/feature_store/handler_test.go`. Templates: `Test_GetFeatureVector_Shared` (`:843`, cross-project positive via joint membership) and `Test_GetFeatureVector_NotShared` (`:864`, expects 401 + `FEATURE_STORE_NOT_SHARED` message). Helper for negative cases: `GetFeatureStoreResponseWithDetail(t, req, expectedMsg, expectedStatus)`.
- Run loop: `cd build && ./mysql-test/mtr --suite rdrs2-golang --start-and-exit`, `export RDRS_CONFIG_FILE=$(realpath mysql-test/var/rdrs.1.1_config.json)`, then `test_go/script.sh test <pkg> [TestName]` (`go test -v -p 1 -count 1`). `script.sh restart` for fast RDRS-only rebuild loop. **After changing fixture SQL, drop the `sentinel` DB to force re-seed.**
- MTR/CI: each Go package needs `mysql-test/suite/rdrs2-golang/t/rdrs2-golang_<x>.test` + a mapping in `include/run_gotest.inc` `%mtr_to_go` + `r/*.result`; suite runs each package twice (no-TLS, TLS).
- C++ tests: `test/api_key_test.cpp`, `test/feature_store_test.cpp` (gtest, live cluster, `RDRS_CONFIG_FILE`). New binary = copy the `NDB_ADD_EXECUTABLE` + `add_test` blocks in `test/CMakeLists.txt`.
- Seed identities: user uid 10000, key `bkYjEz6OTZyevbqt.…` (`testutils/misc.go:31`), projects registered via `dynamic/hopsworks_34_add_project.sql`.

---

## 5. Step-wise implementation plan

Ten steps in five phases. Format: **Goal / Changes / Tests / Review focus**.
Each step is one PR; later steps do not start until the previous is merged.

### Phase A — groundwork (no behavior change)

#### Step 1 — Test fixtures: create + seed the five tables
- **Goal:** both fixture trees contain the V45/V46 DDL and deterministic seed
  rows; zero server-code change.
- **Changes:**
  - New `fixed/V45-FSTORE-1905-column_level_permissions.sql` and
    `fixed/V46-FSTORE-1940-restricted_feature_access.sql` (DDL copied from the
    Hopsworks migrations, minus the legacy-data migration statements) in
    `test_go/resources/testdbs/` **and** `test/resources/testdbs/`.
  - Embed via two `//go:embed` lines + append to `HopsworksScheme`
    (`embeddings.go:73`); C++ side is picked up by the CMake glob.
  - Seed rows in `hopsworks_40_data.sql` (or the V-files) covering: store
    shared entirely; store placeholder (`shared_entirely=0`); FG shared
    entirely; FG shared partially + `shared_feature` rows (incl. PK/event-time
    cols); restricted user with whole-FG grant; restricted user with
    per-feature grant. Add one extra user + API key with
    `FEATURE_STORE_RESTRICTED` team role, and one user in a "receiving"
    project with no membership in the owning project.
- **Tests:** re-seed (drop `sentinel`), run existing Go FS suite +
  `feature_store_test` + `api_key_test` — all green (proves fixtures are
  inert). A trivial Go test reads the seeded rows via
  `fetchMetadataRows` to pin the fixture contents.
- **Review focus:** DDL fidelity vs Hopsworks V45/V46; seed rows match the
  semantic matrix we will test later.

#### Step 2 — DAL: read `project_team.team_role` and retain project ids
- **Goal:** the API-key cache knows, per project, the caller's **project id**
  and **team role** (today: names only, no role).
- **Changes:**
  - `HopsworksProjectTeam` (`rdrs_hopsworks_dal.h:44`) gains `team_role`;
    `find_project_team_int` (`rdrs_hopsworks_dal.cpp:532`) reads the column.
  - `find_all_projects` / `get_user_databases` (`api_key.cpp:1079`) build a
    per-project struct `{project_id, projectname, team_role}`; `UserDBs` keeps
    the existing name-set (hot path untouched) plus the new vector/map.
- **Tests:** extend `api_key_test.cpp`: seeded restricted member resolves with
  role `"Feature store restricted"`; normal member resolves `"Data scientist"`;
  refresh/event paths keep the new fields consistent (reuse existing
  event-test patterns). Existing tests green.
- **Review focus:** no change to `validate_api_key` behavior; hash-before-authz
  ordering untouched; memory ownership of new strings follows the existing
  `UserDBs` string-view arena pattern.

#### Step 3 — DAL: NDB readers for the five share tables (+ fs→project map)
- **Goal:** read-side accessors exist, unused by request paths.
- **Changes:**
  - `rdrs_const.h`: `#define`s for the five table names + used columns.
  - New reader functions (in `rdrs_hopsworks_dal.cpp`, same style as
    `find_api_key`): 
    `find_shared_feature_stores_by_fs(fs_id)`,
    `find_shared_feature_groups_by_fg(fg_id)`,
    `find_shared_features(fg_id, project_id)`,
    `find_restricted_fg_access(fg_id, user_id)` (+ child feature rows),
    plus `find_feature_store_project(fs_id) → project_id` (extend the existing
    `feature_store` reader in `feature_store/feature_store.cpp:224`).
  - Wrapped in `METADATA_OP_RETRY_HANDLER` / `HandleSchemaErrors` like peers.
- **Tests:** new C++ binary `fs_access_test` (clone CMake blocks): each reader
  returns exactly the step-1 seed rows; missing-row and index-miss paths.
- **Review focus:** index usage (PK/ordered scans) and buffer handling match
  existing DAL conventions; no request-path wiring yet.

### Phase B — the decision core

#### Step 4 — Pure decision module `fs_access_control`
- **Goal:** the entire Hopsworks semantic in one pure, cluster-free unit.
- **Changes:** new `src/fs_access_control.{hpp,cpp}`:
  - Plain-data inputs: caller `{user_id, vector<{project_id, role}>}`; FV
    `{vector<FGAccessQuery{fg_id, fs_id, owner_project_id, columns_used}>}`;
    provider callback (or pre-fetched rows) for the five tables.
  - `CheckFeatureViewAccess(...) → AccessDecision{allowed, denied_fg,
    denied_columns}` implementing §3 exactly (any-project ∃, role routing,
    cascade, restricted allow-list, all-or-nothing).
- **Tests:** table-driven gtest (no cluster, rows injected as vectors)
  mirroring Hopsworks' `TestAccessController` matrix (~25 cases): own-project;
  store-entirely; placeholder store row grants nothing; FG-entirely; column
  subset covering / not covering the FV's columns; restricted whole-FG /
  per-feature / no-grant; restricted user ignores project shares; multi-FG FV
  with one FG missing → deny; multi-project caller where only the second
  project satisfies → allow.
- **Review focus:** THE semantics review — decision table in the PR
  description maps 1:1 to Hopsworks behavior (cite
  `docs/fg_fv_sharing_how_it_works.md` §4).

#### Step 5 — Metadata plumbing: ids at the decision point
- **Goal:** `FeatureViewMetadata` carries what the decision needs.
- **Changes:** `metadata.cpp` builder stores per-FG `feature_store_id` and
  `owner_project_id` (via the step-3 fs→project reader) into
  `featureGroupFeatures`; FV's own store likewise.
- **Tests:** extend `feature_store_test.cpp` `Metadata_*` cases: ids populated
  for single-store and joined (cross-store) FVs; cache eviction/reload keeps
  them. Existing tests green.
- **Review focus:** cache-entry memory layout (string arena) and RONDB-1030
  deferred-retry path still correct.

### Phase C — enforcement on feature-store endpoints

#### Step 6 — Wire the check behind a config flag (default OFF)
- **Goal:** end-to-end enforcement on `/feature_store` and
  `/batch_feature_store`, opt-in.
- **Changes:**
  - New X-macro flag `security.apiKey.useFineGrainedFeatureSharing`
    (default **false**) in `config_structs_def.hpp` (+ validator: requires
    `useHopsworksAPIKeys`).
  - New `APIKeyCache` accessor: given api-key prefix → caller identity
    `{user_id, projects+roles}` (from step 2) without re-hashing (secret hash
    still verified via the normal `authenticate` first — ordering preserved).
  - In both controllers, after the existing
    `authenticate(api_key, ...)`-equivalent secret check: flag off → current
    DB-membership check, unchanged. Flag on → build `FGAccessQuery` from
    `metadata->featureGroupFeatures`, fetch share/restricted rows via step-3
    readers (direct reads; caching comes in step 7), call
    `CheckFeatureViewAccess`. Deny → **HTTP 400**, new error
    `FEATURE_NOT_SHARED` (code aligned with Hopsworks 268 message style,
    listing FG + columns) added to `feature_store_error_code.hpp`.
  - Batch endpoint: decision is per-FV → computed once, applied to the whole
    request (same as single).
- **Tests (Go integration, the heart of the suite):** new
  `access_control_test.go` in `internal/integrationtests/feature_store/` (+
  batch twin), server started with the flag ON (new config in the MTR suite
  `my.cnf` or a dedicated `.test` wiring — see §6): the full positive/negative
  matrix from step 1's seeds, asserting 200 vs **400** + message content via
  `GetFeatureStoreResponseWithDetail`. Also: flag OFF run asserting legacy
  behavior intact (existing `Test_GetFeatureVector_Shared/_NotShared`
  unchanged).
- **Review focus:** the only step that touches request handling; error paths,
  no metadata-cache ref-count leaks, flag-off path byte-identical.

### Phase D — performance (make the flag production-viable)

#### Step 7 — `FSAccessCache`: in-memory mirror of the five tables
- **Goal:** zero per-request NDB reads for share data.
- **Changes:** new `src/fs_access_cache.{hpp,cpp}` cloning the `APIKeyCache`
  shape: startup preload (full scans — these tables are small), periodic
  refresh thread (reuse `cacheRefreshIntervalMS` or a sibling knob),
  `shared_mutex`-guarded maps keyed by fs_id / fg_id / (fg_id,user_id).
  Step-6 path swaps direct reads → cache lookups (provider interface from
  step 4 makes this a one-line swap).
- **Tests:** `fs_access_test`: preload correctness vs seeds; refresh picks up
  out-of-band inserted/deleted rows within the interval; concurrent readers.
- **Review focus:** lock ordering, startup wiring in `main.cc` (mirrors
  `start_api_key_cache`), teardown.

#### Step 8 — NDB event watchers for near-real-time grant changes
- **Goal:** share/revoke visible in seconds, not at refresh interval.
- **Changes:** 
  - 8a (optional but recommended): factor the watcher skeleton
    (event create/subscribe/poll/backoff/reconnect) shared by
    `api_key.cpp:749` and `fs_cache.cpp:656` into a helper; mechanical
    refactor, no behavior change — its own mini-PR.
  - 8b: watchers on the five tables (I/U/D), updating `FSAccessCache`;
    gap-recovery re-preload on reconnect.
- **Tests:** event tests in `fs_access_test` following `api_key_test`
  event/reconnect patterns (insert share row via NDB helper → allowed within
  bounded wait; delete → denied; reconnect recovery). Timing-robust waits
  (poll-until, no fixed sleeps).
- **Review focus:** 8a diff-only refactor; 8b PK columns registered in event
  ops (known assert pitfall), pre-values on DELETE.

### Phase E — generic endpoints + finish

#### Step 9 — pk-read / batch pk-read / scan honor shares (D2)
- **Goal:** raw reads of feature-group tables under a share behave like
  Hopsworks JDBC GRANTs. ronsql explicitly deferred.
- **Changes (split into 9a pk-read, 9b batch, 9c scan — same pattern):**
  - Resolve `(db, table)` → is it an online FG table? (`feature_store` by name
    = db → fs_id; `feature_group` by name+version parsed from
    `"{name}_{version}"` → fg_id; cached in `FSAccessCache`).
  - Flag on + caller not a member: if FG resolved, allow when the share/
    restricted decision grants ALL requested read columns (absent explicit
    read-columns = all columns → requires full-FG access); PKs always
    readable per invariant. Non-FG tables: member-only as today.
  - Member-of-owning-project (non-restricted): unchanged fast path.
  - Restricted member of the owning project: narrowed by their grants (this
    corrects today's over-broad access; flag-gated).
- **Tests:** Go suites in `pkread/`, `batchpkread/`, `index_scan/` packages:
  shared-FG table readable by receiving project's key (200), unshared denied
  (400), column-subset share: allowed cols 200 / disallowed col 400,
  restricted member narrowed. Legacy flag-off runs unchanged.
- **Review focus:** table-name parsing edge cases (FG names with `_`,
  version suffix), no per-request scans (cache only), batch mixed-db
  requests.

#### Step 10 — CI wiring, docs, bench, default decision
- **Goal:** everything runs in CI; performance validated; ship posture agreed.
- **Changes:** MTR `t/rdrs2-golang_*.test` + `%mtr_to_go` entries +
  `r/*.result` for any new Go packages; a flag-ON server variant in the suite
  (TLS + no-TLS); update `docs/fg_fv_sharing_how_it_works.md` §7 → "implemented";
  config reference docs. Run `test_go/run_ab_bench.sh` before/after with flag
  ON — cache-hit hot path must be within noise. Separate decision (with user)
  on flipping the default to true.
- **Review focus:** CI drift check passes; bench numbers in PR description.

## 6. Test strategy (full)

**Layers**
1. **Pure unit (no cluster)** — step 4's decision matrix (~25 table-driven
   cases). Runs in seconds; the semantic contract lives here. Any future
   Hopsworks rule change lands here first.
2. **C++ integration (live cluster)** — DAL readers (step 3), cache
   preload/refresh (step 7), event watchers + reconnect (step 8), via the new
   `fs_access_test` binary, same run recipe as `api_key_test`
   (`RDRS_CONFIG_FILE=… build/runtime_output_directory/fs_access_test`).
3. **Go end-to-end HTTP** — steps 6 & 9: status codes, error payloads,
   positive/negative matrix per endpoint, TLS + no-TLS (MTR runs each package
   twice). Single-test loop: `./script.sh test <pkg> <TestName>`;
   `./script.sh restart` for the rebuild loop.
4. **Performance** — step 10: `run_ab_bench.sh` / `bench_preload.sh`
   before/after with flag ON; assert no hot-path regression (share decision
   must be in-memory).

**The semantic matrix** (used at layers 1 and 3; seeds from step 1):

| # | Setup | Expect |
|---|-------|--------|
| 1 | caller member of FV's own project (normal role) | allow |
| 2 | store shared entirely with caller's project | allow |
| 3 | store placeholder row only (`shared_entirely=0`) | deny |
| 4 | all constituent FGs shared entirely | allow |
| 5 | one FG of the FV not shared | deny, names the FG |
| 6 | FG shared partially, FV uses only shared cols | allow |
| 7 | FG shared partially, FV uses one unshared col | deny, names the col |
| 8 | restricted member, whole-FG grants covering FV | allow |
| 9 | restricted member, per-feature grants covering FV | allow |
| 10 | restricted member, grants missing one FV column | deny |
| 11 | restricted member, project has store-share but user no grant | deny (shares invisible to restricted) |
| 12 | caller in two projects; only 2nd satisfies | allow (D1) |
| 13 | key valid, wrong secret | 401 before any authz detail (side-channel) |
| 14 | flag OFF | legacy behavior for all of the above |

Plus event-driven cases (layer 2/3): insert share row → allow within bounded
wait; delete → deny; restricted grant insert/revoke likewise; server restart
(preload) equivalent. All waits poll-until-condition — no fixed sleeps
(timing-robust per project convention).

**Regression gates per step:** `api_key_test`, `feature_store_test`, Go
`feature_store` + `batchfeaturestore` packages green; steps 6/9 additionally
run their new packages flag-ON and legacy tests flag-OFF.

**Fixture hygiene:** any SQL fixture change requires dropping the `sentinel`
DB (or all test DBs) to force re-seed; both fixture trees (Go + C++) must be
updated together.

## 7. Deferred / open items

- **ronsql** endpoint enforcement — deferred by D2; document member-only.
- **Default flip** of `useFineGrainedFeatureSharing` — decide at step 10.
- **Watcher-skeleton refactor (8a)** — recommended, can be dropped if diff
  risk is deemed too high; 8b then clones a third copy.
- Hopsworks writes grants; RDRS2 only reads. Schema drift (new Hopsworks
  migrations touching these tables) tracked by fixture files named after the
  Hopsworks migration numbers (V45/V46).
