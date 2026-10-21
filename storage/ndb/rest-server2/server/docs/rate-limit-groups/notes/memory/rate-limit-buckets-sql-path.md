---
name: rate-limit-buckets-sql-path
description: Rate-limit buckets via mysqld (2026-10-21) — MySQL account name = clip31(Project_username) decodes to (project,user); mysqld tags once per NdbTransaction with no table; kernel has (userId, tableId) at every charge point → proposal = kernel-resolved scoped USER ALIAS, zero mysqld change
metadata:
  type: project
---

# Rate-limit buckets on the SQL path (scouted 2026-10-21, two explorers, verified line refs)

Follow-up to [[rondb-978-group-rate-limits-idea]]. Question from Salman: "when a request
comes to the MySQL server, can we use the user name to identify the bucket?" Answer: YES
for the identity half; the table half must be resolved where the table is known.

## Hopsworks facts (HEAD 83963e2354)
- ONE account per project member: `clip31(ProjectName_username)`
  (OnlineFeaturestoreController.onlineDbUsername :589-595; >32 chars → substring(0,31);
  ONLINE_FEATURESTORE_USERNAME_MAX_LENGTH=32 FeaturestoreConstants:109). Host '%',
  grant NDB_STORED_USER ON *.* (Facade:169-174). DB = lowercased project name.
- Reverse mapping is DETERMINISTIC: project names ≤25 chars [A-Za-z0-9_] no "__"
  (FolderNameValidator:54-55,88), case-insensitively unique; human usernames
  [a-z0-9]{8} (UsersController:517-552); service names (onlinefs, srvmanager) have no
  "_". → split at LAST "_" = exact project name + ≥5-char username prefix; recompute
  clip31 over project_team members to match. No table stores the mapping (secrets row
  `secret_name`=account, pid_scope NULL; operation_log is a queue).
- Shared-FS reader ALWAYS connects as own project's account (grants land on
  clip31(ReceiverProject_user)). All user paths through mysqld use it: hsfs SQL
  engine, Spark/Flink/Jupyter (onlinefeaturestore connector returns caller's own
  account, backend ignores fsId), Superset, serving pods (MYSQL_USER env), backend
  preview. Admin account only for DDL/grants/trace reads/INFORMATION_SCHEMA/ndbinfo.
  onlinefs ingestion = ClusterJ (no mysqld); hsfs REST = RDRS api key.
- Collisions unguarded: project ≥24 chars + usernames sharing 5-6 char prefix → same
  account; CREATE USER IF NOT EXISTS reuses it, 2nd user's login already breaks today.
- Grant quirk: every grant first REVOKE ALL ON <db>.* → a table-level FG share wipes a
  whole-FS grant on that db (Facade:262-281). Multi-region: every op repeated.
- Service accounts: onlinefs (Data scientist) + srvmanager (Data owner) GET accounts;
  airflow is filtered from team (ProjectUtils:376-380) → no account.

## mysqld / kernel facts (rondb2 HEAD ab1549d26ed = 26.02-main+1; identical in rondb)
- ha_ndbcluster::set_user_id (ha_ndbcluster.cc:7756) tags security_context()->user()
  = bare login name, no host; called once per NdbTransaction right after
  startTransaction (:7786/7819/7853/7875). One Thd_ndb::trans per THD shared by all
  handlers → JOIN / multi-statement txn = ONE tag, decided before tables are known.
  Gated by READ-ONLY sysvar ndb_user_rate_limits (default OFF, :153/18773) + data-node
  ActivateRateLimits=true (default false, ConfigInfo.cpp:640). Internal txns untagged.
- Resolution is CLIENT-side, exact memcmp: ClusterMgr::retrieveUserId (ClusterMgr.cpp
  :1512) 1024-bucket hash filled by LIST_DATABASE is_user=1, kept by
  CREATE/DROP_DATABASE_REP + 60 s re-list. Wire = 32-bit Dbdict object id + version
  (TcKeyReq bit 29, ScanTab bit 5). Unknown name → RNIL → UNMETERED (falls back to
  table's DATABASE QUOTA, DbtcMain.cpp:3730-3746). No alias concept. ≤128 bytes,
  stored as "$name" sharing id space with DATABASE QUOTA objects (Dbdict.cpp:33182).
- Kernel HAS the table at every charge point: Dbtc admission TCKEYREQ :3726 (localTabptr
  next to userId), SCAN_TABREQ :16175, TCINDXREQ :22396; LQH
  Dblqh::update_rate_usage(tableId, userPtrI, ns) DblqhMain.cpp:40517; SPJ children carry
  own table. Kernel already maps table→DATABASE QUOTA at create (CONNECT_TABLE_DB
  Dbdict.cpp:8540-8566). Sticky first-record rule: ApiConnectRecord::m_queuedDatabasePtrI
  (:28543, :16205) → whole txn charged to the FIRST touched record (first-op-wins, same
  as RDRS batch stopgap). TC forwards userId to LQH only when sticky record is a user
  (LQHKEYREQ RI bit 9 :5684, SCAN_FRAGREQ bit 27 :19017). Query threads may lack the
  user record → fall back to db record (DblqhMain.cpp:9196-9213).
- Costs ns/op: key read 1250, write 4000, scanned row 750, +6/word read, 12/word
  written, 12/interp instr (Dblqh.hpp:4707). Pool = 50% × threads × 1e6 (Dbdict:2666).
  max_parallel_transactions is DATABASE-only; max_parallel_complex_queries never enforced.
- mysqld mapping templates: ndb_sql_metadata has NO event op (distributed via
  ndb_schema SOT_ACL_*); cleanest table+event+cache template = ndb_index_stat_head
  listener (ha_ndb_index_stat.cc:1839). Per-table hook would be start_statement
  (ha_ndbcluster.cc:7531, has m_share db/table + security ctx) but txn already tagged.
- MTR: suite ndb_quota; SQL-path user test = ndb_user_rate_limits.test (metered_user vs
  free_user, 300 SELECTs, no JOIN/multi-table coverage).

## PROPOSAL (not yet accepted by Salman): kernel-resolved scoped aliases
- Keep mysqld untouched: it already tags the account; the account decodes to
  (project,user). Move bucket resolution INTO THE KERNEL at Dbtc admission, where
  (userId, tableId) are both in hand: new non-budgeted Dbdict object kind
  `USER ALIAS <account> [ON <db>[.<table>]] FOR <bucket>`; TC follows alias → bucket
  (table rule > db rule > ALL rule; plain USER objects pay nothing). LQH/SPJ unchanged
  (receive resolved bucket id). Sticky first-record rule keeps one-bucket-per-txn.
- RDRS sync (already pulling bucket tables, PULL model) expands each bucket into alias
  rows: WHO uid → every project the user is in → clip31(project_name_username); WHO
  project → each member likewise; WHAT fg → ON db.table, store → ON db, none → ALL.
  Installs USER SET (bucket) + USER ALIAS rows idempotently on its own mgmd.
- BONUS: RDRS/ClusterJ can tag the same project-user string (username mode, parked on
  origin/RONDB-978-Cont) and let the kernel resolve → ONE resolution point for REST +
  SQL + onlinefs; RDRS's (user,data)→bucket lookup becomes optional.
- Rejected: mysqld-side (account,db,table)→bucket mapping — tag is fixed per txn before
  tables are known, needs new system table + event cache + Hopsworks-shaped logic in
  mysqld, and ClusterJ would need it again. Also rejected: encoding bucket in account
  name (account is per (project,user), bucket depends on table).
- To verify before committing: Dbdict object-count cap for $name objects (aliases =
  members × projects × data rows), alias persistence across restart + BACKUP USER,
  query-thread fallback path, Mikael sign-off on new mgm syntax + TC hot path.
- Ops prerequisite either way: ndb_user_rate_limits=ON in mysqld config (read-only
  sysvar → rondb-helm) and ActivateRateLimits=true.
