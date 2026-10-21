---
name: rondb-978-group-rate-limits-idea
description: "RONDB-978 follow-up idea (2026-09-23/24, exploratory) — group/department rate limits, Hopsworks-defined groups, one kernel bucket per group; two mapping-resolution variants (RDRS vs kernel aliases)"
metadata: 
  node_type: memory
  type: project
  originSessionId: accb43b6-380b-45df-9c91-4b43645871e9
  modified: 2026-10-02T10:00:26.212Z
---

# Group ("department") rate limits — design exploration (2026-09-23/24)

Problem Salman raised: per-identity limits don't scale to "give department X 10% of the
cluster" when the dept = many projects × members × API keys — no way to express an
aggregate budget today. Exploratory idea only; no ticket/implementation yet. Builds on
[[rondb-978-hopsworks-provisioning]].

## Decisions from the discussion

- **Groups are defined in Hopsworks** (e.g. a `rate_limit_group` table + assignment;
  simplest granularity: project → group, a project in at most one group). The table lives
  in the `hopsworks` DB = already inside RonDB, so RDRS reads it via NDB API like
  api_key/users/project_team.
- **One group = ONE kernel bucket** (`USER SET dept_x --rate-per-sec=...`). NO intra-group
  balancing / per-member sub-slices — Salman explicitly rejected control-plane budget
  rebalancing as too complicated and incompatible with RonDB's CPU-cost-slicing leaky
  bucket. A noisy member may eat the whole group budget; accepted (other groups stay
  protected, which is the actual requirement).
- Big operational win: USER entities per group, not per member → member/project churn
  provisions nothing; half-pool allocation constraint stops scaling with user count.

## The fork: where the identity mapping is RESOLVED

1. **RDRS-resolved** (v1 candidate): api-key cache extends key→user→membership resolution
   one step: target db → project → group → tag transaction with group name. Zero RonDB
   changes. HOLE: SQL traffic escapes — mysqld tags the MySQL account name
   (ha_ndbcluster::set_user_id), which becomes an unknown → unmetered identity.
2. **Kernel-resolved (aliases)** — "grouping info in RonDB": Hopsworks materializes
   member-name → group aliases into the kernel; the data nodes' existing name→ID cache
   (with periodic re-LIST refresh) resolves both mysqld's account name and RDRS's tag to
   the SAME bucket ID. Covers REST + SQL, no mysqld change, near-zero RDRS change. Alias =
   two names → one ID; aliases are cheap entries (per-member churn returns, but they're
   not budgeted entities). New mgm syntax needed (e.g. USER ALIAS).

Migration path: ship variant 1 (REST-only), add kernel aliases later — Hopsworks tables/UX
identical in both.

## Semantics that fall out

- Shared feature stores: bill the target project's group = the READER's department (same
  rule as username mode's fv-own-project attribution).
- Mixed-db batches: less problematic — one batch's ops usually share a department even
  across projects, so first-op-wins is right more often.
- Groups generalize beyond departments: "serving", "batch", "free tier".
- Ungrouped projects fall back to the configured mode (apikey / project_user / unmetered).

## Refinements (2026-09-24 session, after satellite scouting — see [[hopsworks-satellite-clusters]])

- **Group members = LOGIN USERS (hopsworks.users), not projects** (Salman's call, verified):
  every logged-in user can create API keys (ApiKeyResource.create allows HOPS_ADMIN,
  HOPS_USER, AGENT, HOPS_SERVICE_USER; key always minted for the caller), api_key.user_id
  is a non-null FK to users(uid), no project column → key→owner→group is deterministic,
  database-independent, zero churn on new keys. Projects-in-UI = sugar/secondary. SQL
  coverage open item (mysqld tags per-project account, needs account→user mapping or
  kernel alias); REST-only can ship first.
- **PULL model, not push** (Salman's direction): group+budget tables live in hopsworks DB;
  EVERY cluster's RDRS reads them over its existing metadata connection (satellites
  already have it for api keys) and blindly applies USER SET/ALTER/DROP to its own local
  mgmd. Main cluster = N=1 same code. No cluster registry, no Hopsworks→satellite-mgmd
  network path. Blind apply affordable because groups are few. Idempotent SET-then-ALTER
  makes multi-RDRS races benign; sync loop doubles as drift reconciler.
- **Per-cluster budgets** (the marketing 10%-on-A / 50%-on-B requirement): store budgets
  as PERCENT (each cluster converts using own thread count) + optional per-cluster
  override rows keyed by cluster name. Requires a new ClusterName field in RDRS config
  (config_structs_def.hpp has none today) templated by rondb-helm (default
  Release.Namespace) and set by deploy.hopsworks.ai, which also registers the name into
  hopsworks for a UI dropdown. Verified: satellites' names exist ONLY in deploy.ai's DB;
  RDRS/helm/hopsworks all name-blind today.
- **Half-pool crowding argument** (for the doc/pitch): grantable = threads×1M/2 per node;
  grants consume the pool even when idle → 1,000 identities × 10,000 fills a 20-thread
  node's 10M pool exactly; groups keep grant-count = budget-count (tens, not thousands).

## Resume prompt

`rondb/.claude/RESUME-rate-limit-buckets.md` — paste-able prompt for a fresh session
(what to read in which order, the agreed design, the running example, open next steps,
the sandbox-off textutil recipe).

## Design doc (2026-09-24, shared with team)

- HTML (styled, canonical): rondb repo
  `storage/ndb/rest-server2/server/docs/RonDB-Group-Rate-Limits.html` — §1 rate-limit
  mechanics copied from customer docx (API-key Option A/B section EXCLUDED per Salman),
  §2 too-many-identities problem + running example cast (2026-10-02 extended to 4
  projects: Marketing={Mary,Mike} owns marketing_clicks+marketing_campaigns;
  Sales={Sam} owns sales_orders; Recommender={Alice,Ava} owns recommender_features and
  CONSUMES both; Recommender2={Rosa} 2nd consumer. §1 teaching identity = neutral
  etl_jobs). TWO DEPLOYMENT SHAPES carried through doc: A=single cluster, wish "the
  Recommender team gets 20% whatever they read" (= everything-bucket);
  B=cluster-per-project, wishes "Marketing gives Recommender 20% of MY cluster / Sales
  20% of mine / Marketing gives Recommender2 30%" (= store-scoped buckets
  rec_on_marketing 20, rec2_on_marketing 30, rec_on_sales 20). KEY INSIGHT: a
  store-scoped bucket's % IS automatically % of the owner's cluster (that data exists
  only there) → NO cluster name needed for data-scoped buckets; overrides only for
  everything-buckets. 20+30=50% on cluster M = EXACTLY the half-pool grantable cap (good
  teaching moment: owner divides a finite budget). §3 FINAL MODEL (2026-10-02, Salman reverted to UNIFIED BUCKETS
  after first rejecting policies then rejecting share-column coupling): ONE concept
  rate_limit_bucket(name,budget%) + bucket_user(uid XOR project_id) + bucket_data(fg_id
  XOR feature_store_id; NO rows = "everything" = a group) + optional
  rate_limit_cluster_override — NEW TABLES ONLY, no ALTER of share tables; covers own-FG
  caps + shared-FG caps + subsets + pooling (why two-kind model was dropped: couldn't
  cap own data). Rule: bill the bucket containing BOTH caller AND data; most specific
  wins (FG>store>everything); save-time no-ties validation; buckets NEVER authorize.
  Example buckets: marketing 10 / analytics 30 / clicks_readers 5 / campaigns_alice 3 /
  clicks_own 2 = 50% exactly at half-pool cap. One-transaction-one-bucket (verified
  pk_batch_read ONE rate_limit_identity per batch, rdrs_dal.cpp:185): FV read → most
  restrictive matching; data-scoped buckets REST-only v1 (everything-buckets can cover
  SQL later). Doc: single admin-page mockup (New bucket form, empty What = group),
  4-step RDRS pipeline (only (user,data)→bucket lookup new), API-key→bucket flow
  diagram. §6 "One design, both deployment shapes" table + satellites-distribute-data
  (blind apply; location column = fail-open trap). §4 sharing &
  billing READER-PAYS (online FS only per Salman,
  no HDFS; many-shares ⇒ still one bucket; superuser-exec open item; charge lands on the
  owner-data cluster), §5 satellite gap, §6 pull-model proposal (updated 2026-10-01).
- docx: /private/tmp/RonDB-Group-Rate-Limits.docx (VOLATILE — /tmp cleared on reboot;
  regenerate via `textutil -convert docx` with sandbox DISABLED — sandboxed textutil
  fails with "couldn't communicate with helper application").
- Artifact publishing BROKEN for this user: publishes/lists fine via API
  (https://claude.ai/code/artifact/23a2072b-03bc-449c-96fd-bd3b5df6229f) but 404s in
  their browser and absent from their claude.ai/code/artifacts gallery (Hopsworks org) —
  don't rely on artifacts for sharing; give them files.

## Doc REWRITTEN 2026-10-02 (three passes, Salman's asks in order)

1. "more focus on the new Hopsworks tables" → tables became the spine.
2. "human readable, easy to read, with examples" → example rows before definitions,
   DDL to appendix, worked walkthroughs (Alice's request; what M's RDRS does).
3. "clearly discuss the examples … exactly what is in the new tables … how API key
   maps to buckets … focus on single cluster" + mid-turn "tables as images, side by
   side like ERD" → single-cluster is THE example (3 wishes: recommender ALL 20% /
   clicks_own SELECTED 2% on fg 31 / rec2_on_marketing SELECTED 5% on store 21);
   multi-cluster demoted to short §9; existing rows shown with illustrative ids
   (users 1001-1006, project 11-14, feature_store 21-24, feature_group 31-35,
   api_key 501-505 w/ 16-char prefixes); §5 traces key prefix→user_id→email→
   project_team→buckets, 8 steps (5-7 new). DB-row tables = Graphviz PNG ERDs from
   `docs/img/*.dot` (sources kept; `dot -Tpng -Gdpi=144`); textutil embeds them in
   the docx. Graphviz lesson: anchor arrows at table BORDER (no compass on the
   referenced end, key column facing the arrow) or lines cross cell text.
   Verified for this: api_key cols (id,prefix,secret,salt,name,user_id,reserved,expiry),
   project_team.team_member = EMAIL, feature_store.name = project name LOWERCASED = the ONLINE RonDB database (OnlineFeaturestoreController.getOnlineFeaturestoreDbName; `<project>_featurestore` is the OFFLINE Hive db — earlier drafts had this WRONG, fixed 2026-10-02),
   online table = `<fg>_<version>`, RDRS find_api_key by prefix.
Precise row semantics now in doc: project row = every project_team row any role incl. service accounts onlinefs/airflow/srvmanager, expanded at request time; FG row = ONE online table `<db>.<name>_<version>` (a new version is a new fg row, NOT covered); FS row = one database. `data_scope` ('ALL'|'SELECTED') column proposed (cascade-delete trap) — "Decision
needed" in doc, NOT yet accepted by Salman. Kernel facts: MAX_DB_NAME_SIZE 128
(ndb_limits.h:119), Dbdict stores USER entities as `$name` (Dbdict.cpp:33184).

## Provisioning refinement (2026-10-02) — supersedes "blind apply everywhere"

Blind-applying ALL buckets on EVERY cluster does NOT fit the half-pool cap once budgets
are realistic (cluster M carrying rec_on_marketing 20 + rec2_on_marketing 30 + sales'
and recommender's buckets would blow past 50%). FIX: each RDRS provisions only
**the buckets whose data its own cluster hosts**, plus ALL everything-buckets. It decides
by asking its OWN data connection whether the database exists — a LOCAL, always-current
fact, never a location column in Hopsworks metadata (that would be a stale-copy +
fail-open trap). So cluster M spends its grantable half on the two Marketing buckets;
cluster S reserves nothing for them.

## Rejected alternatives (from earlier in the discussion)

- Static budget split 10%/N per member: ceilings not a pool, bad utilization.
- Hopsworks dynamic budget rebalancer (usage-proportional USER ALTER loop): rejected as
  too complicated + fights CPU-slicing enforcement.
- Kernel hierarchical buckets (charge leaf+parent, cgroups-style): the "full" solution,
  superset of aliasing; not pursued for now — aliasing covers the stated need without
  sub-limits.

## OPEN DECISIONS parked 2026-10-21 (Salman: "all valid, keep in memory, bigger picture first")

Not ruled on; do not drop. Bigger picture being worked first = **SQL-path coverage via
mysqld accounts** (see [[rate-limit-buckets-sql-path]]).
1. `data_scope` ('ALL'|'SELECTED') column so FK cascade from a dropped feature group
   cannot silently widen a bucket to ALL.
2. Service accounts (onlinefs, airflow, srvmanager) in project Who rows: include/exclude.
3. Tie created by a membership change (joins a 2nd project with its own ALL bucket):
   refuse the membership change vs mark both buckets conflicting.
4. Fold the multi-cluster answer into doc §9: ALL = N% of EACH cluster; deterministic
   install order by bucket id; per-cluster visible state; percent→rate needs each
   cluster's thread count read from mgm config (new); never USER DROP on failed
   metadata read; overrides need ClusterName in RDRS config + helm + deploy.
5. Superuser-exec path for partially-shared FGs bypasses all buckets (metering hole).
6. SQL-path coverage (mysqld tags the per-project MySQL account) — NOW BEING ADDRESSED.
7. Turn doc into tickets; plus 3 unrelated Hopsworks bugs: inverted RONDB_QUOTAS_ENABLED
   check (ProjectQuotasController:309/:320), cross-satellite share grants silently no-op,
   DatasetSharedWithFacade.java:169 filters on dropped `accepted` column.

## DOC TODO (2026-10-21, Salman): explain WHY the 50% grant cap exists

Salman's chosen rationale, to be added to the doc later: **the meter only charges row
operations** (LQH ns/op: key read 1250, write 4000, scanned row 750, + per-word), so
redo logging, LCP/GCP checkpoints, send/receive threads, DDL, backups and unmetered
identities (admin account, unknown names, databases without quota) still need CPU —
half the pool is kept for them. Facts: pool = (block threads + send threads) × 1e6 µs
(Dbdict::initCommonData, Dbdict.cpp:2663-2667); CREATE cap 50%, ALTER cap 70%,
restart never refused (logs "oversubscribed"); rate 0 = unlimited and consumes no
pool; pool is PER NODE (smallest node decides). Introduced RONDB-503 f37b0d80a60
(2024-08-24), no written rationale in code/commit. Consequence for buckets: grants are
scarce → one budgeted object per bucket, aliases must be non-budgeted.

## TWO NEW DOCS written 2026-10-21 (HTML only per Salman; PDF/docx later)

Both in rondb `storage/ndb/rest-server2/server/docs/` (UNTRACKED, never committed on any
branch — same as the old doc):
- `RonDB-Rate-Limit-Buckets-Overview.html` — CUSTOMER doc: why/what/use cases (5)/all
  access paths count/admin page/what a limited team sees/50% cap + meter rationale/
  rollout/FAQ. Images: bucket-concept, coverage, multi-cluster-topology, admin-page.
- `RonDB-Rate-Limit-Buckets-Design.html` — TECHNICAL doc, supersedes
  `RonDB-Group-Rate-Limits.html` (old file left in place, delete when Salman agrees):
  architecture, tables, identity per path (REST/SQL/ClusterJ), Option A (RDRS maps) vs
  Option B (kernel USER ALIAS, proposed), kernel changes, sync loop w/ alias expansion,
  rules, multi-cluster, build list, reference + cap rationale, 8 open decisions, DDL +
  mgm syntax. Images: architecture, kernel-resolution + the 4 old ERDs.
- New .dot/.png/.svg in docs/img/: bucket-concept, coverage, multi-cluster-topology,
  architecture, kernel-resolution. Illustrative account names used in both docs:
  Marketing_mary01ab, Marketing_mike7c3d, Recommender_alice4f2, Recommender_ava3b7c,
  Recommender2_rosa9e1f.
- Shared CSS copied verbatim from the old doc (print styles included → Chrome PDF works).

UPDATE 2026-10-21 (later): both new docs REBUILT from the old HTML verbatim via
rondb/.claude/build_rate_limit_docs.py (old sections spliced, renumbered 6→8…12→15,
new §6 SQL path, §7 alias, §14 open decisions inserted; Overview reuses old §1, §2 incl.
existing-tables ERD, wishes + new-tables ERD, who-is-charged table, old §8 admin page,
lookup-maps ERD, old §11 reference text, multi-cluster ERD). Old doc
/tmp/RonDB-Group-Rate-Limits.pdf = Oct 2 print of the old HTML (identical content).

## FILE NAMES / TERMINOLOGY as of 2026-10-21 evening (supersedes earlier entries)

- TERM: "rate limit group" (Salman: customers don't know "bucket"; "rate limit" alone made
  "Sam is in no rate limit" nonsense). Tables renamed rate_limit_group /
  rate_limit_group_user / rate_limit_group_data / rate_limit_group_cluster_override
  (group_id). Example groups: recommender_team_rl / marketing_clicks_rl /
  recommender2_marketing_rl (+ hypothetical recommender2_clicks_rl); `_rl` suffix
  explained once. mgm alias syntax now `USER ALIAS SET <name> --target=<user> [--table|--database]`.
- CUSTOMER white paper: docs/Hopsworks-OnlineFS-Ratelimiting-v1.0.html → /tmp
  Hopsworks-OnlineFS-Ratelimiting-v1.0.{pdf,docx}. Rules Salman gave: title
  "Hopsworks OnlineFS Ratelimiting"; generic intro (teams introduced only in the
  scenario); white-paper structure; no "what exists today"; no companion-doc mention;
  only TWO entry points (REST server (RDRS) + MySQL server), never Spark/Flink/notebooks;
  simple budget section (20% = one fifth of each node's work; round numbers; node = 20
  CPU cores, not "typical"); no per-key example box in scenario; only the admin UI
  image (img/admin-page-customer.{svg,png}: no "bucket", no callouts, 1410 wide).
- INTERNAL design doc: docs/Hopsworks-OnlineFS-Ratelimiting-Design-v1.0.html → /tmp
  Hopsworks-OnlineFS-Ratelimiting-Design-v1.0.{pdf,docx}. Structure: 1 exists-vs-new
  table · 2 what exists today (USER metering, two entry points, Hopsworks accounts) · 3
  scenario + existing ERD · 4 new tables + rows ERD + who-charged · 5 REST matching
  (lookup-maps) · 6 SQL path + alias (kernel-resolution, architecture) + A/B table · 7
  RonDB alias · 8 sync · 9 rules · 10 admin (internal mockup w/ callouts) · 11 one
  cluster vs several (table + topology + ERD) · 12 build list w/ status · 13 reference ·
  14 open decisions · 15 DDL. Diagrams regenerated with new names under img/design/
  (originals in img/ untouched). Old Oct-2 doc RonDB-Group-Rate-Limits.html untouched.
- docx-build/build.js now takes title + footer args; extract.py handles <dl>.
