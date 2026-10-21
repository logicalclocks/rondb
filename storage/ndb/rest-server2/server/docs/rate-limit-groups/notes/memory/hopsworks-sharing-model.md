---
name: hopsworks-sharing-model
description: "Hopsworks cross-project sharing scouted 2026-10-01 — 4 mechanisms (dataset / shared_feature_store / shared_feature_group / shared_feature), NO accept step since Feb 2026, reader always acts as own ProjectB_user account; rate-limit billing implications"
metadata: 
  node_type: memory
  type: project
  originSessionId: accb43b6-380b-45df-9c91-4b43645871e9
  modified: 2026-10-01T12:19:04.404Z
---

# Hopsworks sharing model (scouted 2026-10-01, HEAD f873029d71)

For rate-limit context see [[rondb-978-group-rate-limits-idea]] and
[[hopsworks-satellite-clusters]] (cross-satellite sharing silently broken).

## Four mechanisms — there is NO true "project sharing"

1. **`dataset_shared_with`** — dataset-level (HDFS). Pure HDFS group membership in the
   OWNER's dataset groups (`<proj>__<ds>` write / `<proj>__<ds>__read`); no copy/symlink.
   Permissions: EDITABLE / READ_ONLY / EDITABLE_BY_OWNERS (role-bucketed). FEATURESTORE
   datasets REJECTED from this path (must use FS share endpoint). `::` in paths = UI alias.
2. **`shared_feature_store`** — whole-FS share. `shared_entirely=true`; a `false` row is
   an umbrella marker auto-created when only FGs are shared.
3. **`shared_feature_group`** — FG-level; `shared_entirely=false` ⇒ column subset in #4.
4. **`shared_feature`** — column-level (feature name string per row).

No FKs between the three FS tables (correlated by (fs|fg, shared_with_project) in app
code); NO unique constraints — dup prevention is app-level isPresent() (dup row ⇒
NonUniqueResultException). V45 migration moved FS shares OUT of dataset_shared_with.

## Workflow — unilateral, NO accept step

- **Accept/reject was REMOVED Feb 2026** (FSTORE-1946, commit ffb374fd90; V51 dropped
  `accepted`/`accepted_by` columns). A DATA_OWNER of the OWNING project shares and it
  takes effect immediately. Receiver's only control: post-hoc DELETE `/share/received`.
- REST: `/project/{id}/featurestores/share` (+ `/featuregroups/{fgId}`), all endpoints
  DATA_OWNER + JWT/ApiKey(FEATURESTORE scope).
- `DatasetRequest` flow still exists but is advisory-only (email, grants nothing).
- **Cluster default FS is auto-shared into EVERY new project**
  (ProjectService:728 → shareDefaultFeatureStore, gated on FEATURE_STORE_PROJECT_ID).

## Online-FS grants (identity = rate-limit relevant)

- FS share → `GRANT SELECT ON ownerdb.*` TO each RECIPIENT member's own
  `RecipientProject_user` account; FG share → table-level GRANT; column share →
  `GRANT SELECT (cols)`. Unshare → schema/table REVOKE (exceptions SWALLOWED, void).
- New member joins recipient project later → grants replayed automatically
  (ProjectController:2154 addUserToSharedFeatureStores/Groups; HDFS side
  HdfsUsersController.addNewMember iterates datasetSharedWithCollection).
  FEATURE_STORE_RESTRICTED members are SKIPPED from share replay.
- **CONFIRMED: a reader of shared data always acts as their OWN project account**
  (`RecipientProject_user`), never an owner-project account → bill-the-reader holds for
  SQL and matches RDRS's fv-own-project attribution. Shares are read-only by
  construction (write gates = verifyUserProjectEqualsFsProject, ownership only).
- Grant machinery warts: grant silently no-ops if account missing (the cross-satellite
  hole); every grant does schema-level revoke-then-grant (table↔schema grants can
  clobber); grant strings unquoted (revokes are backticked).
- **Superuser-exec path**: AccessController.requiresSuperUserExec — restricted role,
  PARTIALLY-shared FGs, and on-demand FGs force query execution through a superuser
  path, i.e. NOT the user's own MySQL account → those queries dodge per-user/group rate
  buckets (open question for group design).
- Self-healing: PermissionsFixer (HDFS) + TrinoController.reconcileAll replay/prune
  share state; Trino uses OS groups `<owner>__shared_<dstype>`, no Hive GRANTs.

## Final-report deltas (explore-sharing consolidated pass)

- **Cross-project online shares are ALWAYS read-only data-scientist grants** —
  shareOnlineFeatureStore hardcodes grantDataScientistPrivileges; share endpoint has no
  permission param (pre-FSTORE-1905 RO/EDITABLE choice is gone).
- Identity proof triple: grant (OnlineFeaturestoreController:432-435 — ownerDb ×
  recipient member), credentials (connector per (user, own project) only; secrets keyed
  (uid, dbUser)), server-side queries (establishUserConnection uses caller's project for
  username even against owner's db).
- **Grant state is last-writer-wins at schema scope**: every grant issues schema-level
  REVOKE first → an FG-level grant WIPES a pre-existing store-wide SELECT on the same db.
- **setupOnlineFeatureStore early-returns on onlineFeatureStoreAvailable** (re-added by
  FSTORE-2040; repo doc online-fs-mysql-users.md is STALE claiming it was removed) → no
  backfill of missing member accounts → share can silently grant nothing even
  single-cluster.
- **Restricted-member asymmetry**: FEATURE_STORE_RESTRICTED member already in recipient
  project at share time DOES get the cross-project grant
  (ProjectUtils.getProjectTeamCollection filters only airflow); one joining later does
  NOT (ProjectController:2153 skips them). HDFS consistently skips.
- `ProjectB_onlinefs` service account is a member → gets grants on shared dbs too.
- **RDRS never sees MySQL accounts**: authorizes api key → uid → project_team +
  shared_feature_store/shared_feature_group/shared_feature +
  restricted_feature_group_access/restricted_feature_access
  (rdrs_hopsworks_dal.cpp:615/857/958/1126/1203/1294/1358; find_user_databases:1770 →
  full_dbs/visible_dbs/fine_grants; shared_entirely=0 rows = visibility only, no data).
  feature_store.name IS the database name.
- Dead-code landmine: DatasetSharedWithFacade.java:169 still filters on `d.accepted` —
  a column DROPPED by V51; that query path would fail at runtime.
- Third grant mechanism (intra-project): restricted_feature_group_access /
  restricted_feature_access per-user grants (FSTORE-1940, V46).

## Authorization lookups (what RDRS-equivalent checks read)

Can P see FS? `shared_feature_store` any row. Blanket access? `shared_entirely=true`.
Read FG G? entirely-row OR `shared_feature_group`. All columns? FG row shared_entirely.
Which columns? `shared_feature`. Write anything? NEVER via share (ownership only).
TrainingDataset access: ONLY whole-FS entirely share (per-FG share never grants TD).
FeatureView: entirely-share OR all constituent FGs accessible.
