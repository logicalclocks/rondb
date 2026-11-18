# Hopsworks OnlineFS rate limit groups — design docs and working notes

Follow-up to RONDB-978 (per-identity rate limits). A **rate limit group** is a named
budget (% of a RonDB cluster) tied to WHO (Hopsworks users and/or whole projects) and a
CLUSTER (one by name, or every cluster). One group = one kernel `USER SET` entry per
targeted cluster, installed by Hopsworks itself. Earlier drafts called this a
"rate-limit bucket"; the term was dropped for customers. Design version 1.0 also scoped
budgets to feature groups and feature stores and had RDRS install the objects; v2.0
(2026-11-17) removed both (the v2.0 design doc's header note records why).

Everything in this folder is self-contained: the two documents, the Oct 2 original they
grew from, every diagram with its Graphviz source, the DOCX builder, printed PDF/DOCX
copies, and the Claude working notes (memory files, resume prompt, status files) so a
new session does not have to re-read the rondb and hopsworks repos.

## Documents

| File | Audience | What it is |
|---|---|---|
| `Hopsworks-OnlineFS-Ratelimiting-v2.0.html` | customer | **Current** white paper. The feature (WHO + cluster, no data scoping), what a 20% budget means, worked scenario, admin UI mockup, planning, several clusters, FAQ. Only two entry points are named: REST server (RDRS) and MySQL server. Never mentions the design doc. |
| `Hopsworks-OnlineFS-Ratelimiting-Design-v2.0.html` | internal | **Current** design, written for a system architect in three parts of increasing depth. Part I, the design at a glance (§1-4): the feature, the end-to-end install + request flow (lifecycle diagram), the admin page mockup, the exists-vs-new map. Part II, the design (§5-12): what exists today (incl. the Hopsworks mgm client and namespace discovery), the worked scenario, the new tables (ERD), both request paths, the Hopsworks installer + reconciler, save/delete rules, one cluster vs several (cluster column). Part III, technical reference (§13-17): the single-target kernel `USER ALIAS`, enforcement mechanics, build list, open decisions, DDL appendix. The header note records what v2.0 removed from v1.0 and why. |
| `Hopsworks-OnlineFS-Ratelimiting-v1.0.html`, `Hopsworks-OnlineFS-Ratelimiting-Design-v1.0.html` | internal, historical | The data-scoped design (FG/FS `rate_limit_group_data` rows, scoped alias rules, most-specific precedence, RDRS sync loop). Superseded 2026-11-17; kept as the record, referenced by v2.0 §14. |
| `Hopsworks-OnlineFS-Ratelimiting-Variants-Explained.html` | internal, historical | Plain-words companion to the **v1.0** design doc: the two resolution variants (A, RDRS resolves; B, RonDB resolves) with five inline SVG pictures. Carries a superseded banner; the v2.0 alias is B simplified to one target. |
| `RonDB-Group-Rate-Limits.html` | internal, historical | The 2026-10-02 original (bucket terminology, `--bucket=` syntax, table names `rate_limit_bucket*`). Kept untouched as the source both v1.0 documents were spliced from. |
| `dist/*.pdf`, `dist/*.docx` | share | Printed copies of the current documents. Regenerate after editing the HTML (see below). |

Shared vocabulary of the v2.0 documents (keep it when editing):
- "rate limit group", never "bucket"; tables `rate_limit_group` (with `cluster`, NULL =
  every cluster), `rate_limit_group_user`, `rate_limit_group_cluster_state`; column `group_id`.
  No `rate_limit_group_data`, no `data_scope`, no override table (v1.0 only).
- Example groups `recommender_team_rl` (project Recommender, every cluster, 20%),
  `mary_mike_rl` (Mary+Mike, 2%), `recommender2_rl` (project Recommender2, 5%);
  multi-cluster examples `recommender_m_rl` / `recommender2_m_rl` / `recommender_s_rl`
  name namespaces `rondb-m` / `rondb-s`.
- Alias option is `--target=<group>`; an alias has exactly one target and no scope options.
- A cluster's name is its Kubernetes namespace (label `hopsworks.ai/onlinefs-cluster`;
  main = the install namespace); mgmd endpoint `headless-mgmds.<namespace>.svc:1186`.
- Budget explanation: "a data node with 20 CPU cores", never "a typical data node".
- Cap rationale: the meter only charges row operations; redo, checkpoints, sends, DDL and
  unmetered identities still need CPU, so half the pool is kept for them.

## Images

| Folder | Used by | Naming |
|---|---|---|
| `img/` | `RonDB-Group-Rate-Limits.html` (existing-tables, new-tables, lookup-maps, multi-cluster, admin-page) and the v1.0 customer doc (`admin-page-customer`) | bucket era, except `admin-page-customer` |
| `img/design/` | `Hopsworks-OnlineFS-Ratelimiting-Design-v1.0.html` (all nine figures); `existing-tables` is also reused by the v2.0 design doc | group era |
| `img/v2/` | the v2.0 documents: group-concept, lifecycle (numbered install + request flow), new-tables, kernel-resolution, architecture, multi-cluster, multi-cluster-topology, admin-page (design doc), admin-page-customer (white paper) | cluster era |

Every figure ships as `.dot` (source), `.svg` and `.png`; the admin mockups are hand-written
SVG. In the ERDs, grey italic borderless cells are explanations, not columns.

Regenerate:
```
dot -Tpng -Gdpi=144 x.dot -o x.png && dot -Tsvg x.dot -o x.svg
rsvg-convert -z 1.6 admin-page.svg -o admin-page.png
```
Graphviz lesson: anchor edges at the table border (no compass on the referenced end) or
lines cross cell text.

## Regenerating PDF and DOCX

PDF (headless Chrome; in Claude Code run it with the command sandbox disabled and use
explicit `/tmp/...` output paths):
```
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --disable-gpu \
  --no-first-run --no-pdf-header-footer \
  --print-to-pdf=/tmp/Hopsworks-OnlineFS-Ratelimiting-Design-v2.0.pdf \
  file:///<abs path>/Hopsworks-OnlineFS-Ratelimiting-Design-v2.0.html
```

DOCX (`textutil -convert docx` silently drops every image, so use the builder):
```
cd docx-build
npm install --cache ./.npmcache --no-audit --no-fund      # docx@9; sandbox blocks ~/.npm
python3 extract.py ../Hopsworks-OnlineFS-Ratelimiting-Design-v2.0.html blocks-design.json
node build.js blocks-design.json ./Hopsworks-OnlineFS-Ratelimiting-Design-v2.0.docx \
  "Hopsworks OnlineFS Ratelimiting — Design" "Hopsworks OnlineFS Ratelimiting Design v2.0 · "
python3 extract.py ../Hopsworks-OnlineFS-Ratelimiting-v2.0.html blocks.json
node build.js blocks.json ./Hopsworks-OnlineFS-Ratelimiting-v2.0.docx \
  "Hopsworks OnlineFS Ratelimiting" "Hopsworks OnlineFS Ratelimiting v2.0 · "
```
`extract.py` turns the HTML into blocks (headings, paragraphs, lists, tables, figures,
`<dl>`); `build.js` writes the DOCX with a table of contents before the first level-2
heading (Word asks to update fields on open; answer yes). Verify with
`unzip -l x.docx | grep media/` (9 images for the v2.0 design doc, 1 for the customer doc).

## Working notes (`notes/`)

| File | Content |
|---|---|
| `RESUME-rate-limit-buckets.md` | Resume prompt as of 2026-10-21: agreed design, precise semantics verified in code, the running example, doc feedback to keep, open decisions. Paths in it predate this folder. |
| `RONDB-978-username-rate-limit-status.md` | RONDB-978 username identity mode: what was built, what merged, what is parked on `origin/RONDB-978-Cont`. |
| `RONDB-978-Cont-pr-body.md` | PR text for the parked username-mode branch. |
| `memory/rondb-978-group-rate-limits-idea.md` | Full design and decision history incl. rejected alternatives, the 2026-10-02 rewrite log, the parked decisions. |
| `memory/rate-limit-buckets-sql-path.md` | SQL path findings (2026-10-21): mysqld tags `clip31(Project_username)` once per transaction without the table; the kernel has (userId, tableId) at every charge point; hence the kernel-resolved `USER ALIAS` proposal. |
| `memory/hopsworks-sharing-model.md` | The four Hopsworks sharing mechanisms, no accept step, reader always acts as own project account (bill-the-reader). |
| `memory/hopsworks-satellite-clusters.md` | Satellite RonDB topology: full cluster per satellite, RDRS dual connections, hopsworks DB only on main, gaps found. |
| `memory/rondb-978-hopsworks-provisioning.md` | Where Hopsworks-side provisioning would plug in (OnlinefsOperationHandler, RonDbManagementClient) and the traps. |
| `memory/rondb-978-stage1-rdrs-apikey.md` | RONDB-978 RDRS apikey mode: bugs fixed, branch history, what merged where. |
| `memory/RONDB-978-port.md` | Kernel port of per-user rate limits across 26.02/26.04/26.05, signal-bit assignments, upmerge rules. |
| `memory/doc-revisions-keep-old-text.md` | Editing rule: when revising these docs, splice the old HTML verbatim and extend; never condense from memory. |
| `tools/build_rate_limit_docs.py` | The one-off splice generator that produced the first drafts of the two v1.0 documents from the Oct 2 original. Reference only. |

The memory files are verbatim copies of Claude Code's project memory (frontmatter included)
at 2026-10-21.

## Code pointers (line numbers as of October 2026, 26.02)

RonDB kernel and clients:
- Dbtc admission: `TCKEYREQ` ~:3726, `SCAN_TABREQ` ~:16175, `TCINDXREQ` ~:22396 in
  `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`; sticky first-record rule via
  `ApiConnectRecord::m_queuedDatabasePtrI`.
- Dblqh charging: `update_rate_usage(tableId, userPtrI, ns)`; cost model key read 1.25 µs,
  write 4 µs, scanned row 0.75 µs, plus 6/12 ns per word.
- Dbdict: USER objects stored as `$name` (~:33184), namespace shared with DATABASE QUOTA,
  names ≤ 128 bytes.
- Client name→id cache: `ClusterMgr::retrieveUserId`. mgmapi: `ndb_mgm_set_user`,
  `ndb_mgm_alter_user`, `ndb_mgm_drop_user`, `ndb_mgm_get_user`, `ndb_mgm_list_users`.
- Pool = threads × 1e6 µs/s; create cap 50%, alter cap 70%; restart never refuses;
  rate 0 = unlimited; unknown name = unmetered.
- mysqld tagging: `ha_ndbcluster::set_user_id` tags `security_context()->user()` once per
  `NdbTransaction`; gated by read-only sysvar `ndb_user_rate_limits` (default OFF) and the
  data-node config `ActivateRateLimits`.

RDRS (`storage/ndb/rest-server2/server/src`):
- Feature-group / feature-store name expansion: `find_feature_group_int`,
  `find_feature_store_name_int` in `rdrs_hopsworks_dal.cpp` ~1560-1600.
- Username identity mode (parked): commit `aab9edbfbaf` on `origin/RONDB-978-Cont`.

Hopsworks (`<repos>/hopsworks`):
- MySQL account name: `clip31(ProjectName_username)` (OnlineFeaturestoreFacade); decodable
  to (project, user); collisions unguarded; airflow has no account.
- Provisioning hook candidates: `OnlinefsOperationHandler`, `RonDbManagementClient`
  (already speaks mgm USER/QUOTA commands, single-cluster only).
- Bugs noticed while scouting, each worth a ticket: inverted `RONDB_QUOTAS_ENABLED` check in
  `ProjectQuotasController` (~:309/:320); cross-satellite share grants silently no-op;
  `DatasetSharedWithFacade.java:169` filters on the dropped `accepted` column.

## Status and next steps

- The work is ticketed as RONDB-1158. **Design v2.0, 2026-11-17**: budgets no longer
  scope to data (WHO + cluster only), and Hopsworks installs the objects instead of an
  RDRS sync loop. The v2.0 design doc's header note records the provenance (the v1.0
  data scope existed to pin budgets to clusters; Hopsworks already had cluster
  discovery, `KubeBackupController` / HWORKS-2638). Section 14 of v2.0 carries the open
  decisions; ask before resolving any.
- RonDB per-name metering (apikey mode in RDRS, mysqld account tagging): merged on 26.02-main.
- RDRS username mode: merged onto the `RONDB-1158` branch (from `origin/RONDB-978-Cont`).
- A v1.0 implementation (scoped kernel aliases, RDRS sync loop, FG/FS admin page) exists
  as five draft PRs (rondb#1234, rondb#1235, hopsworks-ee#3776, hopsworks-front#2280,
  rondb-helm#270); it is being reshaped to v2.0. The query-thread fix (rondb#1235) is
  design-independent and stands.
- Next: rework the branches to v2.0 (shrink the kernel alias to one target, drop the
  RDRS sync series, move provisioning into hopsworks-ee, simplify the admin page and
  helm values), and refresh the JIRA description.
