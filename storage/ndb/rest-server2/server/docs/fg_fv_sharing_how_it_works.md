# How Feature Group / Feature Sharing Works in Hopsworks

Research doc for porting fine-grained FG/feature access control into the RonDB REST
server (RDRS2). Source of truth: the Hopsworks monorepo at
`/Users/salman/code/hops/hopsworks`. All `file:line` references are relative to that
repo unless stated otherwise.

Companion doc: `fine_grained_fg_fv_sharing.html` (phase-1 research, cross-project
sharing focus). This doc supersedes it on the table-level mechanics and adds the
in-project restricted-access system.

---

## 1. Two independent access systems

Hopsworks has **two orthogonal fine-grained access mechanisms** that are easy to
conflate because both can limit access down to individual features (columns):

| | Cross-project **sharing** | In-project **restricted access** |
|---|---|---|
| Tables | `shared_feature_store`, `shared_feature_group`, `shared_feature` | `restricted_feature_group_access`, `restricted_feature_access` |
| Grantee | a **project** (`shared_with_project`) | a **user** (`granted_to_user`) |
| Semantics | additive grant on top of normal project membership | allow-list for members who otherwise see **nothing** |
| Trigger | Data Owner shares FS/FG/columns with another project | member holds project role `FEATURE_STORE_RESTRICTED` ("Feature store restricted") |
| JIRA / migration | FSTORE-1905, `V45__[FSTORE-1905]_column_level_permissions.sql` | FSTORE-1940, `V46__[FSTORE-1940]_restricted_feature_access.sql` |
| Core commit | `c0957d9d5b` (18-Dec-2025) | `c43ca1153b` (14-Jan-2026) |
| Owning controllers | `ShareFeatureStoreController`, `ShareFeatureGroupController` | `FeatureGroupRestrictedAccessController` |

**They are mutually exclusive per (project, user), not layered.** The single choke
point `AccessController` (`hopsworks-common/.../common/util/AccessController.java`)
routes every access decision by the caller's project team role:

```java
// AccessController.java:393-400
protected boolean checkIfRestricted(Project project, Users user) {
  Optional<ProjectTeam> projectTeam = project.getProjectTeamCollection().stream()
      .filter(t -> t.getUser().equals(user)).findFirst();
  return projectTeam.isPresent() &&
      projectTeam.get().getTeamRole().equals(ProjectRoleTypes.FEATURE_STORE_RESTRICTED.getRole());
}

// AccessController.java:137-145
public boolean hasAccess(Project userProject, Users user, Featuregroup featuregroup) {
  boolean restricted = checkIfRestricted(userProject, user);
  if (restricted) {
    return featureGroupRestrictedAccessController.checkIfUserHasAccess(user, featuregroup);
  } else {
    return shareFeatureGroupController.checkIfShared(userProject, featuregroup);
  }
}
```

If the user is `FEATURE_STORE_RESTRICTED`, only the `restricted_*` tables govern —
the `shared_*` tables are never consulted for them (and vice versa). Restricted
members are also deliberately skipped when new-member share-replay runs
(`ProjectController.java:2124-2127`).

**What if rows exist in both table sets?** They can coexist (e.g. FG shared into
project B *and* a restricted grant for user U in B), but they are never merged —
the role is a switch, not an OR/AND. A restricted member does not inherit
project-level shares (deny unless they hold their own grant, and their view is
only the granted columns even if the project share is broader); a non-restricted
member's `restricted_*` rows are dead metadata. The branch is chosen per
*calling project*: the same user can be restricted in B and a Data Owner in C.
Rationale: sharing is additive, restricted access is default-deny — OR-ing them
would collapse the allow-list guarantee, AND-ing would deny normal users
everything. Role-transition seams (verified in source): demotion to restricted
issues **no MySQL revoke** (`updateUserOnlineFeatureStoreDB`,
`OnlineFeaturestoreController.java:292-302`, matches neither grant branch and the
revoke only runs inside `grantUserPrivileges` when a grant is issued), so the old
`GRANT SELECT ON db.*` survives at the DB layer; promotion leaves `restricted_*`
rows dormant, and they resurrect on re-demotion; `HdfsUsersController.
changeMemberRole` (`:741`) lacks the restricted+FS-dataset guard that
`addNewMember` (`:639-643`) and `PermissionsFixer.testAndFixPermission`
(`:269-272`) both have.

Follow-up commits hardened both systems:
- `a5ab4408a2` **FSTORE-2026** — recursive feature-view / training-dataset checks +
  OpenSearch FLS for restricted users.
- `e1b8b2b295` **HWORKS-2744 append** — the single-FV GET had swallowed the denial;
  re-gated it on `hasAccess`.
- `b622a0dbd0` — external/on-demand FG handling (super-user execution).

---

## 2. The tables

### 2.1 Cross-project: `shared_feature_store` → `shared_feature_group` → `shared_feature`

DDL: `docker/migration/sql/ddl/V45__[FSTORE-1905]_column_level_permissions.sql`.
All FKs `ON DELETE CASCADE` (to `feature_store` / `feature_group` / `project` /
`users`). Engine `ndbcluster`.

```
shared_feature_store (id, feature_store, shared_by, shared_on,
                      shared_with_project, shared_entirely DEFAULT 1)
shared_feature_group (id, feature_store, feature_group, shared_by, shared_on,
                      shared_with_project, shared_entirely DEFAULT 1)
shared_feature       (id, feature_group, feature VARCHAR(63), shared_by,
                      shared_on, shared_with_project)          -- no flag; 1 row = 1 column
```

The three tables form a hierarchy with a "stop-here / shortcut" flag at each level:

**`shared_feature_store.shared_entirely`**
- `1` — the **whole feature store** is shared with the target project: every FG,
  feature view, training dataset. Created by
  `ShareFeatureStoreController.shareFeatureStore` (`ShareFeatureStoreController.java:271-273`).
- `0` — a **placeholder**: "this store is not wholly shared, but at least one FG
  from it is shared with this project." Auto-created the first time an individual
  FG is shared when no store row exists (`ShareFeatureGroupController.java:169-174`).
  It makes the store appear in the receiving project's store listings
  (`FeaturestoreController.getProjectFeaturestores`, `FeaturestoreController.java:156-172`,
  which uses `findSharedWithProject` — all rows, flag ignored) without granting
  store-wide access. It is removed again when the last FG share from that store is
  revoked (`ShareFeatureGroupController.java:433-438`).

**`shared_feature_group.shared_entirely`**
- `1` — the **entire FG**: all columns, offline (HopsFS) + online (RonDB) +
  embeddings. Constructed as `new SharedFeatureGroup(..., features.isEmpty())`
  (`ShareFeatureGroupController.java:177`).
- `0` — only **specific columns**, enumerated as `shared_feature` rows. Offline
  HopsFS access is NOT granted in this case (files are all-or-nothing); only
  per-column online GRANTs plus the signed super-user offline path (§5).

**`shared_feature`** rows exist only under a `shared_entirely=0` FG row. Primary-key
and event-time columns are **force-added** to any column share
(`addPrimaryKeysAndEventTime`, `ShareFeatureGroupController.java:237-248`) so the
shared subset stays queryable and joinable — you cannot hide PKs or event time.

Invariants enforced on insert (`ShareFeatureGroupController.java:155-168`): sharing
an FG fails with `FEATURE_STORE_ALREADY_SHARED` if the store is already shared
entirely, and with `FEATURE_GROUP_ALREADY_SHARED` if an FG row for that project
already exists (no silent widening/narrowing — unshare first).

JPA entities: `hopsworks-persistence/.../entity/featurestore/share/{SharedFeatureStore,SharedFeatureGroup,SharedFeature}.java`.
Facades: `hopsworks-common/.../featurestore/share/{SharedFeatureStoreFacade,SharedFeatureGroupFacade,SharedFeatureFacade}.java`.

### 2.2 In-project: `restricted_feature_group_access` → `restricted_feature_access`

DDL: `docker/migration/sql/ddl/V46__[FSTORE-1940]_restricted_feature_access.sql`.
Engine `ndbcluster`, cascade FKs.

```
restricted_feature_group_access (id, feature_store, feature_group, granted_by,
                                 granted_on, granted_to_user,
                                 can_access_entirely DEFAULT 1)
restricted_feature_access       (id, restricted_feature_group_access,  -- FK to PARENT row
                                 feature VARCHAR(63), granted_by, granted_on,
                                 granted_to_user)
```

Semantics — a **pure allow-list**, not a deny-list:
- No parent row for (FG, user) → the restricted user has **no access** to that FG.
- Parent row with `can_access_entirely=1` → whole FG, no child rows.
- Parent row with `can_access_entirely=0` → access to exactly the features listed
  in the child `restricted_feature_access` rows.

Note the structural difference from the shared tables: `restricted_feature_access`
FKs to its **parent grant row**, not to `feature_group` — a per-feature grant cannot
exist without its per-FG parent, and deleting the parent cascades the children.

PK + event-time columns are force-added here too
(`FeatureGroupRestrictedAccessController.addPrimaryKeysAndEventTime`, L159-170).

Entities: `hopsworks-persistence/.../entity/featurestore/access/{RestrictedFeatureGroupAccess,RestrictedFeatureAccess}.java`
(parent holds `@OneToMany restrictedFeatureAccesses`).
Facades + controller: `hopsworks-common/.../featurestore/access/`.

The role: `ProjectRoleTypes.FEATURE_STORE_RESTRICTED("Feature store restricted")`
(`hopsworks-persistence/.../project/team/ProjectRoleTypes.java:48`,
`AllowedRoles.java:48`) — a fifth project team role alongside Data owner /
Data scientist / Observer. The grant path re-verifies the target user actually
holds this role (`grantRestrictedAccess`, L106-112).

---

## 3. Lifecycle: how grants are created and removed

### 3.1 Cross-project sharing (REST API)

Sub-resources of a project's feature store, all gated
`@AllowedProjectRoles({DATA_OWNER})` — **only a Data Owner of the owning project
can share**; there is **no accept/invitation flow** (unlike the legacy
`dataset_shared_with`, which had a pending + accept step). Sharing is immediate:
the metadata row and all backend permissions are applied synchronously in the call.

`hopsworks-api/.../api/featurestore/share/FeatureStoreShareResource.java`
(`.../featurestores/{fsId}/share`):
- `POST ?project={id}` — share whole store (`shared_entirely=1`)
- `DELETE ?project={id}` — owner-side revoke
- `DELETE /received ?featurestore={id}` — receiver-side revoke (Data Owner of the receiving project)
- `GET` — list shares

`FeatureGroupShareResource.java` (`.../share/featuregroups`):
- `POST /{fgId}?project={id}&feature=c1&feature=c2` — empty feature set ⇒ whole FG;
  non-empty ⇒ column-level
- `DELETE /{fgId}?project={id}`, `DELETE /{fgId}/received`, `GET /{fgId}`

Two non-REST entry points: project creation auto-shares the configured default
feature store (`ProjectService.java:725` → `shareDefaultFeatureStore`), and an
import wizard per-FG-shares all HopsFS-materialised FGs of the default project
(`FeaturestoreService.java:381` → `importDefaultMaterialisedFeatureGroups`).

Project lifecycle keeps grants in sync (`ProjectController.java`):
- member added (`:2125-2126`) → `addUserToSharedFeatureStores` /
  `addUserToSharedFeatureGroups` replays all existing shares for the new member
  (skipped entirely for `FEATURE_STORE_RESTRICTED` members);
- member removed (`:2404-2405`) → reverse;
- project deleted (`:1373`, `:1876`) → `unshareAll*`; owning-side deletions are
  handled by the DB `ON DELETE CASCADE`.

### 3.2 Restricted access (REST API)

`hopsworks-api/.../api/featurestore/access/FeatureGroupRestrictedAccessResource.java`,
a sub-resource of a feature group, all methods `@AllowedProjectRoles({DATA_OWNER})`:
- `POST grantAccess?user=<email>&feature=<set>` — no `feature` params ⇒ whole-FG
  grant; with them ⇒ per-feature grant
- `DELETE revokeAccess?user=<email>`
- `GET getUsersGrantedAccess()`

`grantRestrictedAccess` (`FeatureGroupRestrictedAccessController.java:90-137`)
writes the rows and then, if the FG is online-enabled, immediately pushes the
online GRANT (§5.1) and, if the FG has an embedding, the OpenSearch FLS role (§5.4).

---

## 4. Read-side decision logic (`AccessController` + delegates)

### 4.1 Cross-project: can project P read FG G?

`ShareFeatureGroupController.checkIfShared` (`ShareFeatureGroupController.java:498-513`):

```java
public boolean checkIfShared(Project project, Featuregroup featuregroup) {
  if (featuregroup.getFeaturestore().getProject().equals(project)) return true;   // 1. owner project
  if (sharedFeatureStoreFacade.findByFeatureStoreAndSharedEntirelyWithProject(
        featuregroup.getFeaturestore(), project).isPresent()) return true;        // 2. store shared entirely
  return sharedFeatureGroupFacade
      .findByFeatureGroupAndSharedWithProject(featuregroup, project).isPresent(); // 3. any FG share row
}
```

### 4.2 Cross-project: which columns of G can P see?

`filterSharedFeatures` (`ShareFeatureGroupController.java:532-562`), a four-level cascade:
1. owner project → all columns
2. store shared entirely → all columns
3. FG row with `shared_entirely=1` → all columns
4. else intersect with the `shared_feature` rows for (FG, project)

### 4.3 Restricted: what can user U see in FG G?

`FeatureGroupRestrictedAccessController.filterRestrictedFeatures` (L236-262):

```java
Optional<RestrictedFeatureGroupAccess> restrictedAccess =
    restrictedFeatureGroupAccessFacade.findByFeatureGroupAndUser(featuregroup, user);
if (restrictedAccess.isEmpty()) return new ArrayList<>();          // no grant → nothing
if (restrictedAccess.get().isCanAccessEntirely()) return features; // whole FG
// else intersect with granted RestrictedFeatureAccess column names
```

`AccessController.filterAvailableFeatures` (L379-390) is the unified caller-facing
API: it routes to 4.2 or 4.3 by `checkIfRestricted`. Callers such as
`OnlineFeaturegroupController` preview/query paths throw
`FEATUREGROUP_NO_ACCESSIBLE_FEATURES` when the filtered list is empty.

### 4.4 Feature views and training datasets — recursive checks (FSTORE-2026)

Feature views compose from FGs, so FV access must recurse or column shares leak
indirectly. `AccessController.hasAccess(Project, FeatureView)` (L102-124):

```java
if (featureView.getFeaturestore().getProject().equals(userProject)) return true;  // own project
if (store shared entirely) return true;
return featureView.getFeatures().stream()
    .map(TrainingDatasetFeature::getFeatureGroup).filter(Objects::nonNull)
    .distinct().allMatch(fg -> hasAccess(userProject, fg));                       // EVERY constituent FG
```

The user-aware overload `hasAccess(Project, Users, FeatureView)` (L147-172) goes
further for restricted users: it groups the FV's features by FG and requires that
**every column the FV uses** is in the user's accessible set
(`accessible.size() == fvFeatures.size()` per FG), else deny.

Training datasets are deliberately coarser at the project level:
`hasAccess(Project, TrainingDataset)` (L126-135) allows only owner or
whole-store share — TDs are not reachable via FG- or feature-level shares.
(The user-aware overload recurses via the TD's feature view.)

`verifyAccess(..., Query)` (L200-240) is the throwing variant used at query
construction: it walks the query's join tree (`join.getRightQuery()` recursion)
and raises `FEATURE_NOT_SHARED` (RESTCode 268, **HTTP 400**, `RESTCodes.java:1847`)
naming exactly which requested columns are inaccessible.
`ConstructorController.construct` calls it at L146 before building any SQL.

**HWORKS-2744 regression fix** (`e1b8b2b295`): the single-FV GET
(`FeatureViewResource.getByNameVersion`, hopsworks-api L293-304) had wrapped
`verifyAccess` in a best-effort try/catch (intended to let an owner still view a
degraded FV after a share was revoked), which silently re-served FVs to
partially-authorised callers. The fix replaces it with a hard gate:

```java
if (!accessCtrl.hasAccess(project, user, featureView)) {
  throw new FeaturestoreException(RESTCodes.FeaturestoreErrorCode.FEATURE_NOT_SHARED, ...);
}
```

`hasAccess` short-circuits `true` for own-project callers, so the degraded-owner
case still works; listing endpoints instead silently omit inaccessible FVs via
`filterAvailableFeatureViews` (L370-377).

### 4.5 Decision matrix (FV endpoint)

| Scenario | Result |
|---|---|
| Own project (even with revoked constituent shares) | Allow |
| Whole feature store shared with caller's project | Allow |
| Cross-project, all constituent FGs shared (entirely or all needed columns) | Allow |
| Cross-project, any constituent FG/column missing | **Deny** (`FEATURE_NOT_SHARED`) |
| Restricted user, all FV columns granted | Allow |
| Restricted user, any FV column not granted | **Deny** |

At the FV level access is all-or-nothing; **column filtering** to a subset happens
at the FG / query-construction layer (`filterAvailableFeatures`).

---

## 5. Enforcement backends — one metadata grant, four mechanisms

The tables in §2 are only metadata. When a row is inserted/deleted, the controllers
synchronously push real permissions to up to four backends (no timer bean in the
new path; the "timer bean" comment in V45 refers to the legacy dataset
`PermissionsFixer`).

### 5.1 Online store (RonDB/MySQL): native per-user, column-level GRANTs

- Every project member has a **per-user DB account**:
  `onlineDbUsername(project, user)` = `<project>_<user>` clipped to 32 chars
  (`OnlineFeaturestoreController.java:589`), with a random password stored as a
  Hopsworks secret and a per-user JDBC connector.
- Grant SQL (`OnlineFeaturestoreFacade.java`):
  - whole store: `GRANT SELECT ON <db>.* TO <dbUser>` (`:228`)
  - whole FG: `GRANT SELECT ON <db>.<table> TO <dbUser>` (`:240`)
  - columns: `GRANT SELECT (<col1>,<col2>) ON <db>.<table> TO <dbUser>` (`:248-260`)
  - revoke: `REVOKE ALL PRIVILEGES ON <db>.[*|<table>] FROM <dbUser>` (`:300`, `:333`)
- Grants are issued over an **admin connection** (`establishAdminConnection`,
  `:544`) when a share/grant row changes, when a member is added/removed, or on
  role change; mirrored to the secondary region when multi-region is enabled
  (async OperationLog replay).
- A `FEATURE_STORE_RESTRICTED` member's DB user is created with **zero** schema
  grants (`updateUserOnlineFeatureStoreDB`, `OnlineFeaturestoreController.java:292-302`,
  only grants for DATA_OWNER / DATA_SCIENTIST) — they accumulate only the explicit
  per-FG/per-column grants.
- Online serving reads are **not executed by Hopsworks**: `PreparedStatementBuilder`
  (hopsworks-api, `:117`) hands prepared-statement templates to the client SDK,
  which connects via the per-user JDBC connector — so JDBC-path reads run as the
  grantee and RonDB's own privilege system enforces everything.

### 5.2 Offline store (HopsFS): POSIX ACLs, whole-FG-directory only

`ShareFeatureGroupController.grantFeatureGroupAccess` (`:192-215`):

```java
// Only share the offline feature store if the entire feature group is shared.
if (featuregroup.getDataSource().getConnector() == null && features.isEmpty()) {
  grantOfflineFeatureGroupAccess(...);          // dedicated HDFS group <fsId>__<fgId>__shared + ACL on FG dir
  grantOfflineFeatureStoreExecPrivileges(...);  // READ_EXECUTE traverse on the store dir
}
```

- Whole-store share instead adds each receiving member to the FS dataset with
  `READ_ONLY` via the standard dataset ACL path
  (`ShareFeatureStoreController.addUserToFeatureStoreACL`, `:420-425`).
- **Column-level offline sharing has no filesystem enforcement** — Parquet files
  are all-or-nothing. Partial shares skip the offline branch entirely and rely on §5.3.
- Connector-backed (external) FGs also skip it — nothing on HopsFS.
- Restricted access does not drive HopsFS ACLs at all; offline access for
  restricted users is API-layer + super-user execution only.

### 5.3 ArrowFlight (offline queries): signed + encrypted super-user execution

For any query where the caller's filesystem identity is insufficient —
`AccessController.requiresSuperUserExec` (L402-406): restricted user, OR any joined
FG not shared entirely, OR on-demand FG — the offline read runs as the feature
store **super-user**, but only after app-layer authorization:

1. `ConstructorController.construct` (L146) runs `verifyAccess` over the whole
   query tree → throws `FEATURE_NOT_SHARED` on any inaccessible column.
2. `ArrowFlightController.getArrowFlightQuery` (L342-354) re-filters the column
   list through `filterAvailableFeatures` and builds the SELECT from the permitted
   columns only.
3. The full query DTO (SQL + allowed-column map + filters + connector credentials)
   is **RSA-signed** (SHA256withRSA over the serialized JSON) with the Hopsworks
   super-user key (`BaseHadoopClientsService.getMessageSignatureBase64`, L162-175)
   and the connector block holding super-user DB credentials is **encrypted**
   (AES-256-GCM + RSA-OAEP) to the Flying Duck (ArrowFlight/DuckDB) server's
   certificate (`PKIUtils.encryptText`, L66-107).
4. Flying Duck verifies the signature before privileged execution. Because the
   signature covers the entire DTO, a client cannot tamper with the SQL, column
   list, or filters; the permitted column set is fixed at sign time, and the
   client never sees the super-user credentials.

### 5.4 OpenSearch (embeddings): field-level-security roles

`OpensearchVectorDatabase.grantRestrictedAccess` (vector-db, L387-410) creates one
security role per (index, user): name `fls_<index>_<username>`, with
`index_permissions` on the FG's vector index, `allowed_actions:["read"]`, and an
`fls` field list limited to the granted embedded feature fields (`"*"` for whole
FG), then maps the OpenSearch identity to it. `EmbeddingController` maps feature
names to prefixed index fields (`toEmbeddedFeatureNames`, L457) and derives the
identity: shared-with **project name** for cross-project shares
(`shareEmbeddingFeatureGroup`, L401) vs `project__username` for restricted grants
(`grantRestrictedEmbeddingAccess`, L431). Enforcement happens inside OpenSearch —
fields outside the FLS list are stripped before results leave the cluster.

---

## 6. External FGs, migration from the legacy system

- **External / on-demand FGs** (`b622a0dbd0`): never get HopsFS ACLs; always
  require super-user ArrowFlight execution even when shared entirely (the reader
  has no credentials for the external source). Temporary super-user credentials
  are packaged into the encrypted connector block.
- **Legacy `dataset_shared_with`**: before FSTORE-1905, a feature store was shared
  as a plain HopsFS dataset (with a pending/accept flow). The V45 migration copied
  existing FS dataset shares into `shared_feature_store` with `shared_entirely=1`
  and deleted the FS-related rows from `dataset_shared_with`. The generic dataset
  share endpoint now refuses FS datasets. `dataset_shared_with` remains the
  mechanism for regular datasets/models, and the share controllers still write
  `OperationLog(SHARE_DATASET, ...)` rows into the legacy audit trail.

---

## 7. Known gaps — and what they mean for RDRS2

> **Status (2026-07, branch RONDB-1088):** the port is now complete for the
> online read path. RDRS2 resolves an API key's grants from all six tables
> (`shared_*`, `restricted_*`, plus `project_team` membership) into a
> three-tier `UserDBs` grant set (full database, metadata-visible, and
> db→table→columns), and enforces the full ladder — store-entirely,
> whole-FG, column-subset, and the restricted role — on every data-serving
> endpoint (pk-read, batch pk-read, feature-store, batch feature-store, scan,
> RonSQL). The recursive feature-view check (item 3) is implemented; the
> orphan-row fail-closed case is handled. Remaining consciously-diverged
> item: denials are surfaced as **401** rather than Hopsworks' 400 (item 6).
> The **RDRS2-side authorization architecture is documented in §9 below** —
> that section is the pointer for "how a REST client's access is decided."
> See `fg_fv_sharing_rdrs2_happy_path_plan.md` for the porting history.

1. **The RonDB REST server bypasses the MySQL GRANT enforcement.** RDRS
   authenticates with Hopsworks **API keys**, reads rows via the NDB API
   directly, and never sees the per-user MySQL GRANTs. FSTORE-1905/1940 column
   enforcement is JDBC/SDK-path only. This is the gap the RDRS2 port closes by
   option (a): read the five metadata tables (`shared_*`, `restricted_*`) and
   filter at the application layer — (b) per-user NDB identities was rejected
   given RDRS2's shared service identity. *Done for store-level `shared_*`
   grants (see status above); `restricted_*` and finer `shared_*` levels
   pending.*
2. Any RDRS2 implementation must reproduce **both** routing legs of
   `AccessController`: role check first (`FEATURE_STORE_RESTRICTED`?), then either
   the restricted allow-list or the shared cascade
   (owner → store-entirely → FG-entirely → per-feature).
3. **Recursive FV checks are mandatory** for the feature-view endpoints RDRS2
   serves: FV access = all constituent FGs/columns accessible, all-or-nothing
   deny at the FV level (else the FSTORE-2026 indirect leak reopens at the REST
   layer). Column filtering, if implemented, belongs at the FG level.
4. PK + event-time columns are always included in any column-level grant — RDRS2
   can rely on PK columns being readable whenever any column is.
5. Offline concerns (HopsFS ACLs, ArrowFlight signing) do not apply to RDRS2; only
   §5.1's metadata model and §4's decision logic need porting.
6. `FEATURE_NOT_SHARED` is surfaced as HTTP **400** (not 403) in Hopsworks — worth
   matching or consciously diverging.

---

## 8. Key files and commits

Commits (hopsworks repo):
- `c0957d9d5b` FSTORE-1905 core sharing; `baaed15b41` migration fix; `b622a0dbd0`
  external FGs; `863455155e` constructor super-user fix; `bbd3f4eee3` tests;
  `79665ccd3b` ArrowFlight query fix
- `c43ca1153b` FSTORE-1940 restricted access (revert `730a02ed85` was only on the
  4.6.1 release branch; feature is live at HEAD)
- `a5ab4408a2` FSTORE-2026 recursive FV/TD + OpenSearch FLS
- `e1b8b2b295` HWORKS-2744 append, FV deny fix

Files (hopsworks repo, `hopsworks-common` unless noted):
- Choke point: `common/util/AccessController.java`
- Sharing: `featurestore/share/{ShareFeatureStoreController,ShareFeatureGroupController,*Facade}.java`;
  REST in `hopsworks-api/.../featurestore/share/`
- Restricted: `featurestore/access/{FeatureGroupRestrictedAccessController,*Facade}.java`;
  REST in `hopsworks-api/.../featurestore/access/FeatureGroupRestrictedAccessResource.java`
- Online GRANTs: `featurestore/online/{OnlineFeaturestoreController,OnlineFeaturestoreFacade}.java`,
  `featurestore/featuregroup/online/OnlineFeaturegroupController.java`
- ArrowFlight: `arrowflight/ArrowFlightController.java`, `security/BaseHadoopClientsService.java`,
  `util/PKIUtils.java`
- Embeddings: `featurestore/embedding/EmbeddingController.java`,
  `vector-db/.../OpensearchVectorDatabase.java`
- Entities: `hopsworks-persistence/.../entity/featurestore/{share,access}/`
- DDL: `docker/migration/sql/ddl/V45__[FSTORE-1905]_column_level_permissions.sql`,
  `V46__[FSTORE-1940]_restricted_feature_access.sql`

---

## 9. RDRS2 implementation: authorization architecture (branch RONDB-1088)

> Unlike the rest of this doc, `file:line` references in this section are
> relative to `storage/ndb/rest-server2/server` in the **RonDB** repo, not the
> hopsworks repo.

This is the pointer for understanding how a RonDB REST API caller's access is
decided. The design mirrors Hopsworks' single-choke-point `AccessController`
(§1): there is **one** authorization path, and every data-serving endpoint
funnels through it. Endpoints do not implement their own access logic — they
only assemble the request(s).

### 9.1 One choke point

Every endpoint calls one of the `authenticate()` overloads in `src/api_key.cpp`,
and all of them converge on:

```
authenticate(...)                       // src/api_key.cpp — per-endpoint adapters
  -> APIKeyCache::validate_api_key(...)  // parse key, hash, expiry, then authorize
    -> APIKeyCache::find_and_validate(...)
      -> check_access(userDBs, accessReq, &ok)   // the grant ladder
```

Grants are resolved **once at cache load time** (preload / lazy load / 180s
refresh) from the six tables by the `find_*` readers in
`src/rdrs_hopsworks_dal.cpp` into a per-key `UserDBs` (`src/api_key.hpp`) with
three tiers: `userDBs` (full-database: membership or store-shared-entirely),
`visibleDBs` (feature-view metadata visibility only), and `fineGrants`
(`db -> table -> {columns}`, where an **empty** column set means the whole
table). The orphan/transient partial-grant row is dropped fail-closed in
`find_fine_grained_grants_int` so an empty set can never mean "whole table" by
accident.

`check_access` (numbered branch walkthrough in the comment above the function
in `src/api_key.cpp`) is fail-closed by construction: it defaults to deny and
only allows at three explicit points (full-db, whole-table, or all-requested-
columns-granted). Denials name the blocking object — database / table /
column(s) — the MySQL 1044 / 1142 / 1143 analogues, returned as HTTP **401**.

### 9.2 Per-endpoint request assembly

| Endpoint | Controller | How the `TableAccessRequest`(s) are built |
|---|---|---|
| pk-read | `pk_read_ctrl.cpp` | via `authenticate(PKReadParams&)` — one `(db, table)`; columns = read columns + filter/PK columns, or `nullptr` (whole row) |
| batch pk-read | `batch_pk_read_ctrl.cpp` | controller aggregates the sub-operations into one request per `(db, table)` (union of read+filter columns; whole-row if any op reads all), then `authenticate(vector<TableAccessRequest>)` |
| feature-store | `feature_store_ctrl.cpp` | via `authenticate(FeatureViewMetadata&)` — FV store visibility + every constituent FG's online table and the exact served feature columns (spine FGs: metadata-only) |
| batch feature-store | `batch_feature_store_ctrl.cpp` | the **same** `authenticate(FeatureViewMetadata&)` overload |
| scan | `scan_read_ctrl.cpp` | read columns + filter columns + index key columns → one request |
| RonSQL | `ronsql_ctrl.cpp` | parse-only pass extracts the table + every referenced column (SELECT/agg/WHERE/GROUP BY/ORDER BY) → one request |

### 9.3 Why the per-controller diffs differ

Because enforcement lives in the shared `authenticate()` overloads (§9.1), the
size of a controller's diff does **not** indicate how much authorization it
does:

- **`pk_read_ctrl.cpp` needed no change.** A single pk-read is exactly one
  `(db, table)`; its authorization is fully handled by the
  `authenticate(PKReadParams&)` overload, which this work updated to pass the
  read + filter columns (or `nullptr` for a whole-row read).
- **`batch_pk_read_ctrl.cpp` changed** only to *aggregate* many sub-operations
  into per-`(db, table)` requests before calling the same shared path — logic
  that is inherently batch-specific and has no other home.
- **`feature_store_ctrl.cpp` and `batch_feature_store_ctrl.cpp` changed
  identically** (a one-line swap from the old DB-name-list call to
  `authenticate(api_key, *metadata)`); all the real work — store visibility +
  per-constituent-FG table/column checks — is in the shared
  `authenticate(FeatureViewMetadata&)` overload, so both single and batch get
  identical enforcement.

A batch is authorized as a whole: any one denied `(db, table)` fails the entire
request.
