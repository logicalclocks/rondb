# Hopsworks OnlineFS rate limit groups — design docs and working notes

Follow-up to RONDB-978 (per-identity rate limits). A **rate limit group** is a named
budget (% of a RonDB cluster) tied to WHO (Hopsworks users and/or whole projects) and
WHAT (feature groups and/or feature stores). One group = one kernel `USER SET` entry.
Earlier drafts called this a "rate-limit bucket"; the term was dropped for customers.

Everything in this folder is self-contained: the two documents, the Oct 2 original they
grew from, every diagram with its Graphviz source, the DOCX builder, printed PDF/DOCX
copies, and the Claude working notes (memory files, resume prompt, status files) so a
new session does not have to re-read the rondb and hopsworks repos.

## Documents

| File | Audience | What it is |
|---|---|---|
| `Hopsworks-OnlineFS-Ratelimiting-v1.0.html` | customer | White paper. The feature, what a 20% budget means, worked scenario, admin UI mockup, planning, several clusters, FAQ. Only two entry points are named: REST server (RDRS) and MySQL server. Never mentions the design doc. |
| `Hopsworks-OnlineFS-Ratelimiting-Design-v1.0.html` | internal | Detailed design: what exists vs what is new, the two entry points, Hopsworks accounts, new tables (ERD), REST-path matching, SQL-path matching and the kernel `USER ALIAS` proposal, sync loop, save/delete rules, admin page, one cluster vs several, build list, kernel reference, open decisions, DDL appendix. |
| `RonDB-Group-Rate-Limits.html` | internal, historical | The 2026-10-02 original (bucket terminology, `--bucket=` syntax, table names `rate_limit_bucket*`). Kept untouched as the source both documents were spliced from. |
| `dist/*.pdf`, `dist/*.docx` | share | Printed copies of the two v1.0 documents, built 2026-10-21. Regenerate after editing the HTML (see below). |

Shared vocabulary of the v1.0 documents (keep it when editing):
- "rate limit group", never "bucket"; tables `rate_limit_group`, `rate_limit_group_user`,
  `rate_limit_group_data`, optional `rate_limit_group_cluster_override`; column `group_id`.
- Example groups `recommender_team_rl` (project Recommender, ALL, 20%),
  `marketing_clicks_rl` (Mary+Mike on `marketing_clicks`, 2%),
  `recommender2_marketing_rl` (project Recommender2 on the Marketing store, 5%).
- Alias option is `--target=<group>`.
- Budget explanation: "a data node with 20 CPU cores", never "a typical data node".
- Cap rationale: the meter only charges row operations; redo, checkpoints, sends, DDL and
  unmetered identities still need CPU, so half the pool is kept for them.

## Images

| Folder | Used by | Naming |
|---|---|---|
| `img/` | `RonDB-Group-Rate-Limits.html` (existing-tables, new-tables, lookup-maps, multi-cluster, admin-page) and the customer doc (`admin-page-customer`) | bucket era, except `admin-page-customer` |
| `img/design/` | `Hopsworks-OnlineFS-Ratelimiting-Design-v1.0.html` (all nine figures) | group era |

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
  --print-to-pdf=/tmp/Hopsworks-OnlineFS-Ratelimiting-Design-v1.0.pdf \
  file:///<abs path>/Hopsworks-OnlineFS-Ratelimiting-Design-v1.0.html
```

DOCX (`textutil -convert docx` silently drops every image, so use the builder):
```
cd docx-build
npm install --cache ./.npmcache --no-audit --no-fund      # docx@9; sandbox blocks ~/.npm
python3 extract.py ../Hopsworks-OnlineFS-Ratelimiting-Design-v1.0.html blocks-design.json
node build.js blocks-design.json ./Hopsworks-OnlineFS-Ratelimiting-Design-v1.0.docx \
  "Hopsworks OnlineFS Ratelimiting — Design" "Hopsworks OnlineFS Ratelimiting Design v1.0 · "
python3 extract.py ../Hopsworks-OnlineFS-Ratelimiting-v1.0.html blocks.json
node build.js blocks.json ./Hopsworks-OnlineFS-Ratelimiting-v1.0.docx \
  "Hopsworks OnlineFS Ratelimiting" "Hopsworks OnlineFS Ratelimiting v1.0 · "
```
`extract.py` turns the HTML into blocks (headings, paragraphs, lists, tables, figures,
`<dl>`); `build.js` writes the DOCX with a table of contents before the first level-2
heading (Word asks to update fields on open; answer yes). Verify with
`unzip -l x.docx | grep media/` (9 images for the design doc, 1 for the customer doc).

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

Hopsworks (`/Users/salman/code/hops/hopsworks`):
- MySQL account name: `clip31(ProjectName_username)` (OnlineFeaturestoreFacade); decodable
  to (project, user); collisions unguarded; airflow has no account.
- Provisioning hook candidates: `OnlinefsOperationHandler`, `RonDbManagementClient`
  (already speaks mgm USER/QUOTA commands, single-cluster only).
- Bugs noticed while scouting, each worth a ticket: inverted `RONDB_QUOTAS_ENABLED` check in
  `ProjectQuotasController` (~:309/:320); cross-satellite share grants silently no-op;
  `DatasetSharedWithFacade.java:169` filters on the dropped `accepted` column.

## Status and next steps

- RonDB per-name metering (apikey mode in RDRS, mysqld account tagging): merged on 26.02-main.
- RDRS username mode: parked on `origin/RONDB-978-Cont`, held back until Hopsworks provisions USER entries.
- Everything else in the design doc (tables, admin page, RDRS match + sync loop, kernel `USER ALIAS`): not started.
- Open decisions: section 14 of the design doc (eight items). Ask before resolving any of them.
- Turning the design into tickets (Hopsworks tables + admin page, RDRS cache/match/sync, kernel alias) is the next piece of work.
