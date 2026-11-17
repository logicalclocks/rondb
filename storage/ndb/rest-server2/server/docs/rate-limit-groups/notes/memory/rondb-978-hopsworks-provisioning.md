---
name: rondb-978-hopsworks-provisioning
description: RONDB-978 Hopsworks-side provisioning scouted 2026-09-23 — drop-in via OnlinefsOperationHandler; RonDbManagementClient (mgmd client) already exists; USER cmds already on 26.02-main
metadata: 
  node_type: memory
  type: project
  originSessionId: accb43b6-380b-45df-9c91-4b43645871e9
  modified: 2026-09-23T14:30:04.174Z
---

# RONDB-978: Hopsworks-side USER provisioning — scouting results (2026-09-23)

Three parallel explorers mapped <repos>/hopsworks (HEAD 895a1739bd).
Conclusion: design doc §7's "cross-repo provisioning work" is far smaller than assumed —
**all three sub-items (provisioning, service-user exemption, reconciliation) have existing
templates**; ~1 command class + 1 handler bean + 1 reconciler bean, zero producer changes.
See [[rondb-978-stage1-rdrs-apikey]] for the RonDB side. Status doc:
rondb/.claude/RONDB-978-username-rate-limit-status.md. Hopsworks in-repo handoff doc:
hopsworks/online-fs-mysql-users.md (slightly stale line numbers, verified vs d7fe78bfab).

## Infrastructure that ALREADY EXISTS

- **`RonDbManagementClient`** (hopsworks-common/.../util/RonDbManagementClient.java, ~500
  lines, @Stateless): raw-socket mgmd text protocol, Consul discovery
  (HopsworksService.RONDB_MGMD="mgmd"), multi-region fan-out built into setQuotas,
  num_rows-counted response framing. Sole consumer: ProjectQuotasController (database
  quotas; project create → enforceDefaultRonDbQuota, delete → dropRonDbQuota at
  ProjectController:1481/1983). Companions: RonDbQuotaCommand / RonDbQuotaResponse /
  RonDbManagementClientSettings.
- **RonDB side USER command family is already merged on 26.02-main** (mgmsrv
  Services.cpp: "set user":113, "alter user", "drop user", "get user", "list user",
  "backup user"; args username/rate_per_sec/max_transaction_size/max_parallel_transactions/
  max_parallel_complex_queries; same reply shape the Java parser handles).
- **`OnlinefsOperationHandler`** CDI interface (hopsworks-common async/service framework,
  V52 DDL): implement as @Stateless @TransactionAttribute(NOT_SUPPORTED) bean → discovered
  automatically (Instance<> injection, no registration, no beans.xml, no new enum, no DDL).
  Callbacks: onlineFsDbCreateUser(projectId, userId, onlineFsDbUsername, serviceUser) /
  onlineFsDbRemoveUser / grant / revoke — the row's `online_fs_user` (varchar 200) already
  carries the EXACT clip31(ProjectName_username) identity, and `is_service_user` gives the
  §7 service-user exemption (computed in OperationHandlerHelper.isServiceUser: onlinefs,
  serving manager, airflow user). Producer = OnlineFeaturestoreController only
  (ONLINE_FS_DB_CREATE_USER at :239, REMOVE at :353/:400).
- **Timer/retry**: OperationLogTimer (@Singleton, 15s tick, payara-primary-gated,
  ASYNC_SERVICES_TIMER_* settings); 10 immediate retries then 60s·2^n backoff capped 1 day;
  row deleted only when all enabled handlers SUCCESS/SKIPPED; HandlerStatus precedence
  FAILURE > PENDING > SUCCESS > SKIPPED — return pending() when RonDB merely unreachable.
  Template: SupersetUserAccountHandler (+ TestSupersetUserAccountHandler, pure Mockito).
  isServiceEnabled() = per-cluster kill switch → matches §9 dormant-then-flip rollout.
- **Reconciliation template**: TrinoReconciler + TrinoController.reconcileAll() — 5-min
  @Singleton timer, primary gating INSIDE @Timeout body, Hazelcast topic for on-demand
  admin trigger, sorted desired-state diff (steady state = zero writes), skips service
  users + UNDER_REMOVAL. Copy for periodic USER LIST reconciliation.

## Traps

- **Multi-region double-apply**: OnlineFeaturestoreController runs every create/remove
  twice (per-region JDBC connections) but writes ONE operation_log row;
  RonDbManagementClient.setQuotas fans out to secondary region internally — mgm USER call
  must NOT sit inside the per-region loop (sync option) and the async handler must do its
  own region fan-out (or rely on the client's built-in one).
- **EJB default-method trap** (documented in SupersetUserAccountHandler:76-85): default
  interface methods get NO transaction attribute — redeclare every default method in a
  NOT_SUPPORTED handler.
- **Name-reuse hazard is framework-handled**: timer pends ops when a same-name user/project
  delete is outstanding (OperationLogFacade.hasPendingProjectDelete generation check).
- **service column is VARCHAR(20)** per entity @Size (DDL says 45) — keep getServiceName()
  short.
- **31-char clip collision surface**: users.username is varchar(10) (generated 8-char
  handle from email local part, UsersController.generateUsername), project name up to
  88/100 chars → any project name >~20 chars truncates INTO the username half; members of
  such a project share one MySQL account string and hence one rate bucket. Same collision
  already exists in Hopsworks' own GRANTs; must be documented, not fixed (byte-equality
  constraint).

## API key facts (RDRS-relevant)

- Key = `<16 alnum prefix>.<64 alnum secret>` (81 chars, one dot); stored secret =
  sha256hex(secret||salt), salt = 88-char base64; prefix UNIQUE, latin1_general_cs
  (case-sensitive). Table hopsworks.api_key, engine ndbcluster.
- **Key → exactly ONE user, NO project scoping** (no project_id column; scopes via
  api_key_scope, FEATURESTORE is the only FS scope). project_username identity is only
  determined once the target database is known → mixed-db batch problem is inherent to
  the schema, not an RDRS artifact.
- `reserved=1` marks machine-minted transient keys (Kube serving per-workload, Airflow
  per-DAG), hidden from user listing — NOT a service-user marker. Service-user-ness is
  BbcGroups membership (HOPS_SERVICE_USER/AGENT); RDRS cannot detect it from api_key alone
  — use operation_log.is_service_user at provisioning time instead.

## Customer-doc discrepancy (flagged 2026-09-23)

/private/tmp/RonDB-Rate-Limits.docx (shared with customer) presents Option A "meter by
user name" as currently available, but username identity mode is unmerged (only on
origin/RONDB-978-Cont); doc also omits the 31-char clip, and its `myproject_alice`
example hides that the real string is `<ProjectName>_<8charhandle>` clipped. Unknown
identities run unmetered (fail-open), so a mismatch is silent.
