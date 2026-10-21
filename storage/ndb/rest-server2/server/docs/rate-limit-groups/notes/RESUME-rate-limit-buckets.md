# Resume prompt — RonDB rate-limit buckets design doc

Paste the block below into a fresh Claude Code session started in
`/Users/salman/code/hops/rondb`. Last worked on 2026-10-02 (a full day of doc iteration);
this file was rewritten on 2026-10-21 at the end of that session.

---

We are writing the design doc for **rate-limit buckets**: shared database budgets for
teams in Hopsworks + RonDB. It is the follow-up to RONDB-978 (per-identity rate limits;
apikey mode merged on 26.02-main, username mode parked on `origin/RONDB-978-Cont`).
The design is settled; the work now is the document itself and a few open decisions
that writing the document exposed.

## Read first, in this order

1. Memory: `~/.claude/projects/-Users-salman-code-hops-rondb/memory/rondb-978-group-rate-limits-idea.md`
   (design + full decision history, incl. the 2026-10-02 rewrite log), then
   `hopsworks-sharing-model.md`, `hopsworks-satellite-clusters.md`,
   `rondb-978-hopsworks-provisioning.md`.
2. The doc: `storage/ndb/rest-server2/server/docs/RonDB-Group-Rate-Limits.html`
   (untracked). Read it start to finish before touching it. Images are in `docs/img/`.
3. `.claude/RONDB-978-username-rate-limit-status.md` for RONDB-978 background.

Repos: `/Users/salman/code/hops/rondb` (kernel + RDRS), `/Users/salman/code/hops/hopsworks`
(metadata, UI, provisioning), `/Users/salman/code/hops/rondb-helm`,
`/Users/salman/code/hops/deploy.hopsworks.ai`.

## The agreed design (do NOT re-litigate; rejected alternatives are in the memory file)

One new concept, the **rate-limit bucket** = name + budget (% of a cluster) + WHO
(login users and/or whole projects) + WHAT (feature groups and/or feature stores).
Three new Hopsworks tables, no change to existing tables:
`rate_limit_bucket(id, name, budget_percent, data_scope, created, created_by)`,
`rate_limit_bucket_user(bucket_id, uid XOR project_id)`,
`rate_limit_bucket_data(bucket_id, feature_group_id XOR feature_store_id)`,
plus optional `rate_limit_cluster_override` (multi-cluster only, appendix).
A request is charged to the bucket containing both the caller and the data; most
specific wins (feature group > feature store > ALL); buckets never authorize. One bucket =
one kernel `USER SET` entry. Provisioning is PULL: every cluster's RDRS reads the tables
over its metadata connection and installs on its own mgmd the buckets whose data its own
cluster hosts (checked by "does the database exist on my data connection") plus all
ALL-scope buckets.

**Precise semantics now written into the doc (verified in code 2026-10-02):**
- A `project_id` Who row = every `project_team` row for that project, ANY role, resolved
  at request time from RDRS's cached membership (RDRS scans `project_team` by the user's
  EMAIL; `project_team.team_member` is an email, not a uid). Includes the service
  accounts Hopsworks adds to every project (`onlinefs`, `airflow` as data scientists,
  `srvmanager` as data owner). Membership changes write nothing to the bucket tables.
- A `feature_group_id` What row = ONE online table `<db>.<name>_<version>`. A new
  feature-group VERSION is a new `feature_group` row and is NOT covered.
  A `feature_store_id` row = one database, every table, incl. future ones.
- **Database naming (earlier drafts had this WRONG):** `feature_store.name` = project
  name lower-cased = the ONLINE RonDB database (`marketing`). `<project>_featurestore`
  is the OFFLINE Hive db. RDRS expands bucket data rows into (db, table)/(db) keys at
  cache-load time via `find_feature_group_int` + `find_feature_store_name_int`
  (rdrs_hopsworks_dal.cpp ~1560-1600), same as fine-grained grants; no parsing per request.
- Kernel facts: USER names ≤ 128 bytes (MAX_DB_NAME_SIZE), stored internally as `$name`
  (Dbdict.cpp ~33184); mgmapi has `ndb_mgm_set_user/alter_user/drop_user/get_user/list_users`.

## The running example in the doc (single cluster is THE example)

Projects: Marketing 11 {Mary 1001, Mike 1002} owns feature groups 31 `marketing_clicks`,
32 `marketing_campaigns` in store 21 `marketing`; Sales 12 {Sam 1003} owns 33
`sales_orders` in store 22 `sales`; Recommender 13 {Alice 1004, Ava 1005} owns 34 in store
23 `recommender`; Recommender2 14 {Rosa 1006} owns 35 in store 24 `recommender2`.
API keys 501/502 Alice, 503 Ava, 504 Mary, 505 Rosa (16-char prefixes shown in the ERD).
Marketing's store is shared with 13 and 14; feature group 33 is shared with 13.
Three wishes → three buckets: `recommender` ALL 20% (project 13); `clicks_own` SELECTED
2% ({1001,1002} on fg 31); `rec2_on_marketing` SELECTED 5% (project 14 on store 21).
Total 27% of the 50% cap. Multi-cluster (M/S/R, store-scoped buckets, "no row names a
cluster") is a SHORT later section only.

## Doc structure (12 sections)

1 problem+idea (Alice's two keys) · 2 teams + EXISTING rows (ERD) · 3 the new tables, one
subsection each · 4 the example as rows (ERD) + who-is-charged table · 5 API key → bucket:
the two RDRS lookup maps (ERD) + Alice's request in 8 steps (5-7 new) + other keys ·
6 save rules + delete/cascade table + "Decision needed" note · 7 sync (3 USER SET) ·
8 admin page (mockup image) · 9 later: more than one cluster (ERD) · 10 build list ·
11 kernel reference · 12 DDL appendix.
Images: `docs/img/{existing-tables,new-tables,lookup-maps,multi-cluster}.dot` → PNG via
`dot -Tpng -Gdpi=144 x.dot -o x.png` (keep .svg too); `docs/img/admin-page.svg` →
`rsvg-convert -z 1.6 admin-page.svg -o admin-page.png`. In the ERDs, grey italic
borderless cells are explanations, NOT columns (Salman asked); real columns have borders.
Graphviz lesson: anchor arrows at the table BORDER (no compass on the referenced end, key
column facing the arrow) or lines cross cell text.

## Salman's feedback on 2026-10-02 (keep the doc in this style)

- "I do not like how the doc is organized" → tables are the spine.
- "make it human readable, easy to read, with examples" → example rows BEFORE
  definitions; DDL in an appendix; worked walkthroughs; no clipped fragments
  ("Nine rows." / "Three tables in …" were called out as cryptic).
- "focus on single cluster"; "exactly what will be in the new tables"; "how api key maps
  to buckets".
- "when writing tables use images, put tables side by side like ERD diagrams".
- "rate_limit_bucket_user has hand waving, look into the meaning col" → every annotation
  must name the exact lookup/rule, and be visibly marked as not-a-column.
- He asked "does this work for multi-cluster" → answer given in chat (summarised below),
  NOT yet folded into §9.

## Open decisions / next steps — ASK which one before starting

- `data_scope` ('ALL'|'SELECTED') column: proposed in §6 "Decision needed" because FK
  cascade from a dropped feature group can empty a bucket's What list and silently widen
  it. Salman has NOT ruled on it.
- Service accounts in project rows: include or exclude (§3.2 flags it).
- Tie created by a membership change (person joins a second project that has its own
  ALL bucket): doc proposes REFUSING the membership change; alternative is marking both
  buckets conflicting. Not ruled on.
- Fold the multi-cluster discussion into §9: ALL buckets mean "N% of EACH cluster";
  per-cluster 50%-cap refusals need a deterministic install order (by bucket id) and a
  visible per-cluster state; percent→rate needs each cluster's thread count (RDRS must
  read it from mgm config, new); never `USER DROP` on a failed metadata read; request
  routing to the right RDRS and cross-cluster feature views are pre-existing platform
  gaps; overrides need `ClusterName` in RDRS config + rondb-helm + deploy.hopsworks.ai.
- Superuser-exec metering hole (partially-shared FGs bypass all buckets today).
- SQL-path coverage (mysqld tags the per-project MySQL account; only ALL buckets could
  cover SQL via an account→user mapping).
- Turn the doc into tickets (Hopsworks tables + admin page + RDRS cache/match/sync).
- Unrelated Hopsworks bugs found while scouting, each worth a ticket: inverted
  `RONDB_QUOTAS_ENABLED` check in `ProjectQuotasController:309/:320`; cross-satellite
  share grants silently no-op; `DatasetSharedWithFacade.java:169` filters on the dropped
  `accepted` column.

## Producing shareable copies

Generated outputs live in volatile `/private/tmp` and were GONE after the 2026-10-21
restart; regenerate before sharing.

- **PDF** (images embedded): headless Chrome, run with the command sandbox DISABLED:
  `"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --disable-gpu --no-first-run --no-pdf-header-footer --print-to-pdf=/private/tmp/RonDB-Group-Rate-Limits.pdf file:///Users/salman/code/hops/rondb/storage/ndb/rest-server2/server/docs/RonDB-Group-Rate-Limits.html`
- **DOCX**: `textutil -convert docx` silently DROPS every image (relative, absolute,
  data-URI — all tried). Use the builder checked into
  `storage/ndb/rest-server2/server/docs/docx-build/` (extract.py = HTML→blocks.json with
  the stdlib parser; build.js = blocks→docx with npm `docx@9`):
  ```
  cd storage/ndb/rest-server2/server/docs/docx-build
  npm install --cache ./.npmcache --no-audit --no-fund     # sandbox blocks ~/.npm; needs registry.npmjs.org
  python3 extract.py ../RonDB-Group-Rate-Limits.html blocks.json
  node build.js blocks.json ./RonDB-Group-Rate-Limits.docx
  cp RonDB-Group-Rate-Limits.docx /private/tmp/            # sandbox off if a file made outside the sandbox is in the way
  ```
  Verify: `unzip -l … | grep media/` (expect 5 PNGs) and `textutil -convert txt -stdout …`
  (sandbox off). No LibreOffice/pandoc on this box, so pages cannot be rendered; the docx
  skill's validate.py needs `defusedxml`, which pip could not fetch through the proxy.
  Word asks to update fields on open (table of contents); answer yes.
- Claude artifact links 404 for this user — share files, not artifacts.
- The customer-facing guide `/private/tmp/RonDB-Rate-Limits.docx` (section 11 derives
  from it) is also gone; ask Salman to re-supply if needed.
