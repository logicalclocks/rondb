# Fine-Grained FG/Feature Sharing — Test Design (validate on real cluster, replicate in RDRS)

Method: build each sharing scenario on a **real Hopsworks cluster**
(kubeconfig `~/code/hops/helm/kubeconfig.yml`), record (a) the metadata rows
Hopsworks writes and (b) how reads behave for the grantee, then import that
data into the RDRS test fixtures and implement until RDRS shows the **same
behavior**. This doc defines the users/projects/FGs to create, the exact
grant actions, the observation protocol, and the derived RDRS test matrix.

Findings this design builds on (from the Hopsworks repo exploration):

- Hopsworks itself has **no integration tests** for FG-level shares,
  feature-level shares, or restricted access — only store-level sharing has
  Ruby specs (`featurestore_share_spec.rb`). The fine-grained logic is
  covered by Java unit tests only (`TestAccessController.java`,
  `TestShareFeatureGroupController.java`,
  `TestFeatureGroupRestrictedAccessController.java`). Our live-cluster
  recordings are therefore the *first* end-to-end validation — expect
  surprises, record everything.
- Grant-creation endpoints (all query params, empty POST bodies,
  caller must be DATA_OWNER of the producer project):
  - Store-entirely: `POST /project/{pid}/featurestores/{fsId}/share?project={granteeProjectId}`
  - Whole FG: `POST .../share/featuregroups/{fgId}?project={granteeProjectId}`
  - Feature-level: same + repeatable `&feature=f1&feature=f2`
    (PK + event-time columns are force-added server-side)
  - Restricted grant (in-project user with `FEATURE_STORE_RESTRICTED`
    project_team role): `POST .../featuregroups/{fgId}/restrictedaccess?user={email}[&feature=f1...]`
  - DELETE variants of each = unshare/revoke.
- Error contract: denial at FV/feature level = HTTP **400**, RESTCode
  **268 `FEATURE_NOT_SHARED`** (message lists the inaccessible features);
  `FEATURE_STORE_NOT_SHARED` = 11; sharing an FG when the store is already
  fully shared = 264; FG already shared = 267.
- Semantics to respect in test design:
  - `shared_*` grantee is a **project**, never a user. "Share with userb"
    always means "share with userb_project"; userb's API key then satisfies
    the check through his membership of that project (RDRS decision D1:
    any of the caller's projects may satisfy access).
  - `restricted_*` grantee is a **user** who is a member of the *producer's*
    project with role `FEATURE_STORE_RESTRICTED`. The two systems are
    mutually exclusive per (project, user): `AccessController` routes by
    role first (restricted → allow-list; everyone else → share cascade).
  - Whole-FS share short-circuits per-FG checks; an FG-level share creates a
    **placeholder** `shared_feature_store` row (`shared_entirely=0`) that
    must grant nothing store-wide.
  - FV access is recursive and all-or-nothing over all constituent
    FGs/columns (FSTORE-2026).

## 1. Test population — projects, users, content, grants

We rebuild the cluster state from scratch rather than preserving what is
there today: the existing FGs are offline-only (`online_enabled=0`) and
RDRS serves the online store exclusively, so all FGs are recreated with
`online_enabled=True` (existing projects can be reused/renamed for this;
the admin setup projects are not used at all). **Clean-slate first:** before
any setup, all existing grants are unshared, every FG is deleted, and the
userx_projects themselves are deleted (surviving projects would keep their
old `project`/`feature_store` ids, which collide with ids already used in
the RDRS seed file — e.g. today's prod store ids 67–71 overlap seeded
fsdb001..fsdb004/fsdb_isolate). The rebuild starts from empty tables.

**Auto-increment floor (avoids id remapping at import):** every table we
import uses AUTO_INCREMENT ids, and the RDRS seed file already occupies
ids up to ~5200 (training_dataset_feature), plus 88887/88888 used at
runtime by the C++ sharing test. Immediately after the wipe and BEFORE
creating anything, set the counter of every auto-increment table in the
prod `hopsworks` schema to **100000**:

```sql
SELECT CONCAT('ALTER TABLE `hopsworks`.`', c.TABLE_NAME,
              '` AUTO_INCREMENT = 100000;')
FROM information_schema.COLUMNS c
JOIN information_schema.TABLES t
  ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
WHERE c.TABLE_SCHEMA = 'hopsworks' AND c.EXTRA LIKE '%auto_increment%'
  AND (t.AUTO_INCREMENT IS NULL OR t.AUTO_INCREMENT < 100000);
-- execute the generated ALTERs
```

The `< 100000` guard skips tables whose counter is already above the floor
— concretely `feature_group_commit`, whose commit ids are epoch-millisecond
timestamps that must not be lowered. The pre-generated script for this
cluster (158 ALTERs over the 159 auto-increment tables found on Hopsworks
5.0.1, all import-critical tables verified present) is at
`docs/fine_grained_recordings/00_set_autoinc_floor.sql`. The mechanism is
proven on this cluster: an ndbcluster table altered to floor 100000 handed
out exactly id 100000 on the next insert.

Every row created from then on (users, projects, stores, FGs, FVs, keys,
share/restricted rows, subjects/schemas) is born with an id ≥ 100000 and
imports into the seed file without remapping. Database ids are still
deliberately omitted everywhere in this doc — entities are identified by
name only.

Existing today and reusable: users usera/userb/userc `@lc.com` with
projects usera_project / userb_project / userc_project. **All API keys are
created fresh** — the old ones are deleted (secrets lost); every user gets
a new key, recorded at creation time for the driver scripts. All grants
are applied fresh after the clean-slate wipe. Everything else below is
created new.

### What is needed — checklist

**Users (12):**

| user | purpose |
|---|---|
| usera | producer — owns all shared content |
| userb, userc, userd, usere, userf | cross-project consumers — grant granularity DECREASES alphabetically: userb = whole store, then progressively finer |
| userg, userh, useri | reserved — gap for future tests (users exist, unused) |
| userj, userk, userl | restricted members of usera_project — one per restricted grant shape |

**Projects (6 — only where needed; more can be added later):**

| project | content | role in tests |
|---|---|---|
| usera_project | 2 online FGs + 2 FVs (the only project with content) | everything is shared FROM here; also where the restricted users are members |
| userb_project | empty | consumer — receives the ENTIRE-store share (coarsest grant) |
| userc_project | empty | consumer — receives both FGs as whole-FG shares |
| userd_project | empty | consumer — receives one whole-FG share |
| usere_project | empty | consumer — receives whole-FG + feature-subset shares (finest grant) |
| userf_project | empty | no shares at all — outsider negative control |

The five consumer projects stay **empty on purpose**: they exist only so
their owners' API keys have a grantee project to receive shares through.
Keeping them empty means any usera data a consumer's key can read is
provably reachable *only* via the grant under test. userg..userl have no
projects (created only if a future test needs one); userj/userk/userl's
access exists solely through their `FEATURE_STORE_RESTRICTED` membership
of usera_project.

**Content (all in usera_project, all online-enabled):**

- FG `usera_customers_fg` (customer_id PK, age, country, is_premium,
  event_time)
- FG `usera_transactions_fg` (customer_id PK, num_transactions_30d,
  total_spend_30d, avg_transaction_value_30d, event_time)
- FV `usera_customers_transactions_fv` — uses ALL columns of both FGs
- FV `usera_txncount_fv` — uses only num_transactions_30d from
  transactions_fg (plus customers columns)

**API keys:** one per user, all 9 created fresh (secrets recorded at
creation).

**Grants (7):** detailed per consumer below.

### Why this population

**Why so many users?** Every "deny" test needs a user whose *only* grant is
the one under test. A user's API key is satisfied by any project they are
in (RDRS decision D1), and restricted grants are per-user — so if one user
carried two grant states, the stronger one would mask the weaker and the
deny test would silently pass for the wrong reason. On the live cluster we
could reuse users by granting and reverting between recordings, but the
RDRS fixture is one frozen snapshot: every grant state must exist
side-by-side, each on its own user.

#### The producer: usera / usera_project

All shared content lives here. usera stays the only Data owner.
Its feature store contains, after setup:

- `usera_customers_fg` — columns: customer_id (PK), age, country,
  is_premium, event_time — **recreate with online_enabled=True**
- `usera_transactions_fg` — columns: customer_id (PK),
  num_transactions_30d, total_spend_30d, avg_transaction_value_30d,
  event_time — **recreate with online_enabled=True**
- `usera_customers_transactions_fv` — joins BOTH FGs, uses ALL their
  columns (exists; recreate on the online FGs)
- `usera_txncount_fv` — **new**; joins both FGs but takes only
  `num_transactions_30d` from transactions_fg. This FV is the instrument
  for the partial-grant tests: under a grant of just num_transactions_30d
  it must work while the full FV is denied.

Nothing else is ever shared from anywhere — all grants below point at
usera_project's content.

#### Cross-project consumers (each = own user + own project + api key;
grant granularity strictly decreases from userb to userf)

1. **userb / userb_project**
   - Grant: usera's ENTIRE feature store shared with userb_project —
     the coarsest grant.
   - Tests **A1**: full-store share ⇒ userb's key reads both FVs, and
     pk-read/batch/scan work on both online FG tables.

2. **userc / userc_project**
   - Grant: BOTH FGs shared, each as a whole FG (no store share).
   - Tests **B3**: with every constituent FG shared, the full FV works —
     via the per-FG cascade, not the store short-circuit that userb takes.

3. **userd / userd_project**
   - Grant: `usera_customers_fg` shared as a WHOLE FG — and nothing else.
   - Tests **B1** (grant metadata), **B2**: the full FV is denied because
     transactions_fg is not shared, and **B4**: the `shared_entirely=0`
     placeholder row that this share creates must not open the rest of the
     store (neither to the FV endpoints nor to pk-read on the unshared
     FG's table).

4. **usere / usere_project**
   - Grant: `usera_customers_fg` whole + `usera_transactions_fg`
     RESTRICTED TO the single feature `num_transactions_30d` — the finest
     grant.
   - Tests **C1**: `usera_txncount_fv` works (all its columns are covered),
     **C2**: the full FV is denied with a message naming total_spend_30d /
     avg_transaction_value_30d, **C3**: Hopsworks wrote PK + event_time
     into `shared_feature` alongside the granted column, **C4**: what
     direct table access (pk-read analogue) yields under a column-subset
     grant.

5. **userf / userf_project**
   - Grant: NOTHING. userf is the clean outsider — the only user with no
     path to usera's store.
   - Tests **A2**: the store-level denial (what exact status/code Hopsworks
     returns to a complete stranger), on every API surface. Every other
     user has some grant, so none of them can test this.

(userg, userh, useri exist as users but have no projects/roles — reserved
for future tests.)

#### Restricted members of usera_project (users only — no own projects;
their whole purpose is membership in usera_project; api key each)

6. **userj** — added to usera_project with role FEATURE_STORE_RESTRICTED,
   given NO grants.
   - Tests **D0**: a restricted member sees nothing by default. Also the
     acceptance test for today's RDRS gap (currently he'd see everything).

7. **userk** — restricted member; granted `usera_customers_fg` in FULL
   (canAccessEntirely).
   - Tests **D2**: full FV denied — transactions_fg not granted.

8. **userl** — restricted member; granted `usera_customers_fg` in full +
   `usera_transactions_fg` PARTIALLY (only num_transactions_30d).
   - Tests **D3**: `usera_txncount_fv` works, full FV denied — the
     restricted twin of usere's cross-project case.

#### Totals

- Users: 12, all created fresh (usera..userl, incl. the reserved gap
  userg/h/i)
- Projects: 6 (`usera_project`..`userf_project`), all except usera_project
  empty; userg..userl get projects only if a future test needs one
- API keys: 12 (one per user), all created fresh; record each secret at
  creation for the driver scripts
- Grants to apply: 9, all fresh after the wipe (userb: 1 store-entirely,
  userc: 2 whole-FG, userd: 1 whole-FG, usere: 2, userk: 1, userl: 2,
  userj/userf: 0) — note the usere/userl partial grants are 1 whole-FG +
  1 feature-level each.

Transitions (A3/B5 unshare-revokes, D4 grant-to-wrong-role rejection) are
recorded live by applying and reverting — they need no extra users, and in
RDRS they are replicated with runtime NDB row insert/delete (P2 pattern).

### Producer-side layout after setup (all in usera_project)

FGs (online, PK `customer_id`, event_time `event_time`):

| FG | non-key columns |
|---|---|
| usera_customers_fg | age, country, is_premium |
| usera_transactions_fg | num_transactions_30d, total_spend_30d, avg_transaction_value_30d |

FVs:

| FV | columns used |
|---|---|
| usera_customers_transactions_fv | all of both FGs (existing design) |
| usera_txncount_fv (new) | customers: age; transactions: **num_transactions_30d only** |

The new FV is the key instrument: under a feature-level share of
`{num_transactions_30d}`, `usera_txncount_fv` must become readable while
`usera_customers_transactions_fv` (which uses `total_spend_30d` etc.) must
stay denied — same share, opposite outcomes.

## 2. Scenario matrix

Grantor is always usera (DATA_OWNER of usera_project). Every scenario is
read through **both RDRS API families**, under the reader's API key:

1. **Feature-store APIs** — `POST /feature_store` and
   `POST /batch_feature_store` on the named FVs (reference behavior:
   the Hopsworks/hsfs client and metadata endpoints);
2. **Generic APIs** — `pk-read`, `batch-pk-read` and `scan` on the online
   FG tables themselves (`db = usera_project`,
   `table = usera_customers_fg_1 / usera_transactions_fg_1`). Hopsworks
   has no direct reference for these (its online enforcement is per-user
   MySQL GRANTs, recorded at C4); the expected outcomes are the §E rules,
   which every scenario below exercises alongside its FV reads.

Per-scenario generic-API expectations: store-entirely ⇒ both tables
readable; whole-FG shares ⇒ only the shared FGs' tables readable;
feature-subset share ⇒ per the E3 decision (informed by C4); no grant /
placeholder-only ⇒ nothing readable.

Every scenario records: rows written to `shared_*`/`restricted_*`/
`project_team`, and per API surface the read outcome (status + error code
+ which features are named in the message).

### A. Store shared entirely (cross-project) — baseline, mechanism already in RDRS

| # | grant | reader | read | expected (Hopsworks ref) |
|---|---|---|---|---|
| A1 | usera's store → userb_project | userb key | both usera FVs; pk-read/batch/scan on both FG tables | allow everywhere (store-entirely opens the whole DB) |
| A2 | none for userf | userf key (outsider) | any usera FV; pk-read on any FG table | deny on every surface (record exact code — RDRS today: 401) |
| A3 | live transition: unshare A1 | userb key | any usera FV; pk-read | deny after revocation; re-share to restore the static state |

A1/A2 are *mechanically* covered by RDRS P2/P3 tests, but with a single
user (macho) and synthetic rows. What A adds: **two distinct real users'
keys**, real Hopsworks-written rows, and the recorded Hopsworks denial
contract for A2/A3 — the reference RDRS must converge to (D3: eventually
400, not 401). Verdict on the open question: yes, test store-entirely
explicitly again in this round, it is cheap and anchors the comparison.

### B. Whole-FG share (cross-project)

Static grantees: userd_project (one FG only) and userc_project (both FGs).
usera's store is NOT store-shared with either.

| # | grant | reader | read | expected (per AccessController unit tests) |
|---|---|---|---|---|
| B1 | share usera_customers_fg (whole) → userd_project | userd key | direct-FG read/metadata; pk-read on customers_fg table | grant exists; pk-read on the SHARED FG's table allowed (E1) |
| B2 | B1 only | userd key | usera_customers_transactions_fv (+ batch) | **deny 400/268** — transactions_fg not shared (FSTORE-2026 partial-FG case) |
| B3 | both FGs shared (whole each) → userc_project | userc key | usera_customers_transactions_fv (+ batch); pk-read on both FG tables | allow everywhere |
| B4 | B1 only (placeholder row check) | userd key | anything else in usera's store; pk-read/scan on transactions_fg table | deny — `shared_feature_store.shared_entirely=0` placeholder grants nothing store-wide (E2) |
| B5 | live transition: unshare customers_fg from userc_project | userc key | usera_customers_transactions_fv; pk-read on customers_fg table | deny again (removing one FG re-breaks the FV and its table access); re-share afterwards to restore the static state |

Metadata to record at B1: the placeholder `shared_feature_store` row
(`shared_entirely=0`), the `shared_feature_group` row, and that
`shared_feature` stays empty (whole-FG = no feature rows) — plus HDFS/online
side effects we can ignore for RDRS.

### C. Feature-level share (part of an FG, cross-project)

Static grantee: usere_project.

| # | grant | reader | read | expected |
|---|---|---|---|---|
| C1 | share customers_fg whole + transactions_fg `feature=num_transactions_30d` → usere_project | usere key | usera_txncount_fv (+ batch); pk-read on customers_fg table | allow — the FV's transactions columns ⊆ {num_transactions_30d} ∪ PK ∪ event_time; customers table wholly shared (E1) |
| C2 | same | usere key | usera_customers_transactions_fv (+ batch) | **deny 400/268**, message must name total_spend_30d / avg_transaction_value_30d (per testVerifyAccessSomeInaccessible) |
| C3 | same | — | inspect `shared_feature` rows | PK (customer_id) + event_time force-included: rows = {customer_id, event_time, num_transactions_30d} (per testShareFeaturesIncludePrimaryKeyEventTime) |
| C4 | same | usere key | pk-read on transactions_fg online table | record Hopsworks online behavior (per-user MySQL GRANT SELECT(cols)) — reference for whether RDRS pk-read must column-filter or deny |

### D. Restricted role (in-project user)

Setup once: create userj, userk, userl; add each to usera_project with role
`FEATURE_STORE_RESTRICTED`; create their api keys.

The restricted system has exactly **two grant shapes**, and the matrix is
organized around them:

- **full FG** — `restricted_feature_group_access` row with
  `canAccessEntirely = 1`, no child rows
  (`POST .../featuregroups/{fgId}/restrictedaccess?user={email}`, no
  `feature` params);
- **part of FG** — `restricted_feature_group_access` row with child
  `restricted_feature_access` rows, one per granted feature
  (same endpoint with `&feature=...` params).

There is no store-level restricted grant; "the whole store" is only
expressible as a full-FG grant on every FG.

| # | grant shape | grant | reader | read | expected |
|---|---|---|---|---|---|
| D0 | none | membership only (userj) | userj key | both usera FVs; pk-read/scan on both FG tables | deny on every surface — restricted member sees nothing by default |
| D1 | **full FG** | live transition: temporarily add transactions_fg full to userk (on top of D2's customers full), revert after recording | userk key | usera_customers_transactions_fv | allow (all constituent FGs granted entirely) |
| D2 | **full FG** | customers_fg full only (userk) | userk key | usera_customers_transactions_fv | deny 400/268 (FV needs transactions_fg too; testHasAccessFeatureViewRestrictedUserPartialGrantDenied) |
| D3 | **part of FG** | customers_fg full + transactions_fg `feature=num_transactions_30d` (userl) | userl key | usera_txncount_fv allow; usera_customers_transactions_fv deny; pk-read: customers table allow (E1), transactions table per E3 | granted-subset boundary, both directions under one grant |
| D4 | — | grant to a user WITHOUT the restricted role (e.g. userb, not a member) | — | grant call itself | rejected: UserException ACCESS_CONTROL ("must have FEATURE_STORE_RESTRICTED role") |
| D5 | **full FG** | userm (NEW restricted-block user, uid 100012, post-T3): restricted member of usera_project + Data owner of his OWN userm_project; customers_fg granted entirely | userm key | from userm_project scope: reach usera's store / create FV over usera FGs; from usera_project scope: FG metadata parity with F4 | **RECORDED 2026-07-17** (recording_D5.json): grants do NOT travel - usera store invisible from his own project (404/270008, A2 parity; no shared_feature_store row created for userm_project), so an own-store FV over usera FGs is impossible; inside usera_project exact userk parity (customers ok, transactions 400/270266) |

Metadata to record: D1 → `restricted_feature_group_access` rows with
`canAccessEntirely = 1` and zero `restricted_feature_access` rows; D3 →
`canAccessEntirely = 0` (or however Hopsworks encodes it) plus the child
feature rows — and specifically whether PK/event-time columns are
force-added to the child rows the way `shared_feature` does (open question
5 in §4).

**Critical current-gap test (D-RDRS):** under today's RDRS, userj — being a
`project_team` member of usera_project — gets **full database access**
(`team_role` is never read). D0 seeded into RDRS fixtures must initially
FAIL (userj can read everything) and is the acceptance test that the
fine-grained implementation flips to deny.

### E. Generic-API rules (pk-read/batch/scan — no Hopsworks reference)

RDRS's generic endpoints have no Hopsworks analogue — Hopsworks enforces
online reads via per-user MySQL GRANTs which RDRS bypasses. These rules
are the expected outcomes for the generic-API reads embedded in every
scenario above (informed by C4's recording):

| # | case | proposed rule (to confirm at implementation review) | exercised at |
|---|---|---|---|
| E1 | pk-read table of a wholly-shared FG (incl. store-entirely) | allow | A1, B1, B3, C1, D3 |
| E2 | pk-read table of an unshared FG in a partially-shared store | deny | B4, B5 |
| E3 | pk-read table of a feature-level-shared FG | mirror MySQL: allow but only shared columns readable (or deny whole-table read if it selects unshared columns) | C4, D3 |
| E4 | ronsql | stays member-only (deferred, decision D2) | — |
| E5 | no grant at all / restricted member without grants | deny | A2, A3, D0 |

### F. Consumer-created FVs on shared / restricted FGs

A consumer can build their OWN feature view — in their own feature store —
on top of foreign FGs they can access. The FV's metadata then lives in the
consumer's store while its `training_dataset_feature` rows reference FGs
whose online data lives in the producer's database: a distinct RDRS code
path (cross-store metadata resolution + authorization), and a distinct
grant question (is FV creation column-checked at creation time?). Tested
for both grant systems, reusing the existing static grants:

| # | actor (grant) | action | expected (record actual) |
|---|---|---|---|
| F1 | userc (both FGs whole) | create `userc_own_fv` in userc_project joining BOTH usera FGs, all columns; then read it (hsfs + RDRS `featureStoreName=userc_project`) | creation allowed; read serves full vectors |
| F2 | usere (customers whole + transactions{num_transactions_30d}) | create `usere_own_fv` in usere_project using only granted columns; then read it | creation allowed; read serves |
| F3 | usere | attempt an FV in usere_project selecting the UNSHARED `total_spend_30d` | record where it fails: at FV creation (400/268 FEATURE_NOT_SHARED per AccessController.verifyAccess) or later at read (SQL 1143) |
| F4 | userk (restricted, customers full) | attempt to create an FV in usera_project itself (restricted users have no own project) over customers_fg | record whether the `Feature store restricted` role may create FVs at all — the role check may deny before any column logic |
| F5 | userl (restricted, partial) | if F4 allows: FV over granted columns only, and one over `total_spend_30d` | record allow/deny per column set |

The successful consumer-side FVs (F1, F2) become part of the final-state
dump and therefore RDRS fixtures — giving the RDRS test suite native
cross-store FVs to serve.

## 2.5 T2 results — observed behavior (Hopsworks 5.0.1, recorded 2026-07-16)

All scenarios A–D and F were recorded against the live cluster; 15
recording JSONs live in `fine_grained_recordings/` (self-describing:
setup + per-test context + reads legend). The matrices above are the
hypotheses; this section is what actually happened, and it corrects
several expectations.

### The enforcement model (measured)

Online sharing is enforced **exclusively at the per-user MySQL GRANT
layer** — one ladder, identical for both grant systems:

| grant | MySQL grant observed |
|---|---|
| store entirely (userb) | `GRANT SELECT ON usera_project.*` (database-level) |
| whole FG (userc, userd, userk) | table-level grant per shared FG's online table |
| feature subset (usere, userl) | `GRANT SELECT (customer_id, event_time, num_transactions_30d)` — column-level, PK + event_time force-included |
| restricted member, no grants (userj) | nothing — not even database access |

Serving-path metadata is NOT enforced: a partially-granted consumer can
fetch the FV definition and even the prepared statements containing SQL
over unshared FGs (schema leak, accepted by Hopsworks). Denials surface
at data-read time as SQL errors: **1044** (no DB grant), **1142** (table
denied), **1143** (column denied, message names the column).

### The denial contract, by location

| situation | observed |
|---|---|
| outsider reads store metadata (A2) | **404** at `get_feature_store` — store not listed at all |
| FV read over insufficient grant (B2, C2, D2, D3) | SQL error 1142/1143 at vector fetch — **no 400/268 in the serving path** |
| FV **creation** selecting unshared/ungranted features (F3, F5) | **400 / errorCode 270268** "The feature is not shared", usrMsg names the exact features and FG — identical for shared_* and restricted_* |
| restricted user fetches ungranted FG metadata (F4 probe) | **400 / errorCode 270266** "The feature group is not shared" — stricter than cross-project, where prepared statements leak |
| restricted grant to ineligible user (D4) | **403 / errorCode 160047** "User must have the Feature store restricted role..." (not 400 as hypothesized) |

### Other findings

- The restricted_* system is a per-user mirror of the shared_* system:
  same ladder, same metadata shapes (`can_access_entirely` 1/0 + child
  feature rows), same force-inclusion of PK/event_time (open question 5:
  **yes**).
- Restricted users CAN create FVs (in usera_project — they own no
  project) within their grants (F4/F5); column checks at creation mirror
  the cross-project case exactly.
- **Restricted grants do not travel to projects the user owns** (D5,
  recorded 2026-07-17 with userm — restricted in usera_project AND owner
  of userm_project): the grant creates no share row for his project, the
  producer store is 404/270008-invisible from his own project's scope
  (A2 parity), so an FV over the producer's FGs can only ever live in the
  producer project itself (which is why userk_own_fv/userl_own_fv sit in
  usera's catalog). Inside usera_project userm behaves exactly like userk.
  The restricted-user block grows alphabetically: j, k, l, m (the g-i gap
  stays reserved for consumer-side tests).
- Consumer-created FVs land in the consumer's feature store while their
  features reference the producer's FGs — the cross-store serving path
  RDRS must support (fixtures now include 4 such FVs).
- **Prod RDRS gap, recorded live**: RDRS (member-only authz) returns 401
  for every cross-project consumer (no share support — the P2 work fixes
  store-level) and **200 for the restricted members on every surface**
  (userj/k/l get full DB access via project_team) — the security hole
  this project closes.
- hsfs 5.0.3 client bugs found: AttributeError on partially-shared FV
  provenance (`get_parent_feature_groups().accessible`), and an infinite
  hang when the online read hits SQL 1044 (error swallowed by the async
  executor). Worth reporting upstream; the recordings capture the
  underlying server behavior despite them.

### Implications for the RDRS implementation

1. Store-level denial may stay 401 (or move to 404-parity); the 400/268
   contract belongs to *metadata/creation* endpoints, which RDRS does not
   serve — decision D3 refined: for the serving path, "Hopsworks parity"
   means denying data access with an error naming the denied table/column
   (mirroring 1142/1143), not RESTCode 268.
2. The E-rules are confirmed: E1 allow (table grant exists), E2 deny (no
   grant), E3 = column-subset readable / unshared column denied (mirror
   the column grant).
3. `team_role = 'Feature store restricted'` must route to the
   restricted_* allow-list and, absent grants, deny even the member's own
   project database (D0).

## 3. Execution plan

### Phase T1 — build the cluster state
0. **Clean slate** (manual, by the cluster owner): unshare everything,
   delete all FGs, delete the userx_projects, delete all existing API
   keys. Verify: `shared_feature_store`, `shared_feature_group`,
   `shared_feature` empty; no FGs/FVs/userx projects left.
0.5 **Set the auto-increment floor**: run the information_schema-generated
   `ALTER TABLE ... AUTO_INCREMENT = 100000` over all hopsworks tables
   (see §1). Verify by creating one project and checking its id ≥ 100000.
1. Recreate the projects, then usera's 2 FGs `online_enabled=True` + both
   FVs (modified notebook, run as usera). Insert the sample rows; verify
   online tables appear in DB `usera_project` and RonDB serving works via
   hsfs.
2. Create the 6 new users (userd, usere, userf, userj, userk, userl) and
   their projects (userd/usere/userf); add userj/userk/userl to
   usera_project as `FEATURE_STORE_RESTRICTED`; create all 9 API keys and
   record the secrets.
3. Snapshot `hopsworks` metadata (baseline dump) before any grants.

### Phase T2 — record scenarios A → F on the cluster — **DONE 2026-07-16**

Completed: 15 recordings (A1/A2/A3, B1_B2_B4/B3/B5, C, D0/D1/D2/D3/D4,
F1/F2_F3/F4_F5) in `fine_grained_recordings/`, harness = t2_lib.py +
t2_scenarios.py + t2_descriptions.py; final-state dumps
(final_state_metadata.sql + final_state_usera_project_online.sql,
SET-form) taken as the T3 import source; findings in §2.5. D5 deferred
(userj owns no second project). Original plan below for reference.
For each scenario, a driver script (python, per-user API keys) that:
1. applies the grant via the REST endpoints above,
2. dumps the affected tables (`shared_feature_store`, `shared_feature_group`,
   `shared_feature`, `restricted_feature_group_access`,
   `restricted_feature_access`, `project_team`),
3. performs the reads as the designated user (hsfs `get_feature_vector`
   and/or the metadata/query endpoints) and records status/error-code/
   message verbatim,
4. reverts the grant and re-verifies denial.
Output: one recorded-behavior file per scenario, committed under
`docs/fine_grained_recordings/` — these become the assertions of the RDRS
tests.

### Phase T3 — import into RDRS fixtures
Dump the final cluster state (projects, users, project_team, feature_store,
feature_group + cached/on-demand satellites, feature_view,
training_dataset_*, serving_key, subjects/schemas, the online FG tables
with data, and all five sharing tables) and add as labeled
`INSERT ... SET` rows to `hopsworks_data.sql` (P1.5 format, mysqldump →
SET-form conversion per that file's header). Thanks to the auto-increment
floor (§1), all imported ids are ≥ 100000 and no remapping is needed —
the import gate is a mechanical check that no imported id is below the
floor and no PK collides with an existing seeded row. The online feature
tables become new test databases in the fixture tree.

### Phase T4 — write RDRS tests (failing first, then implement)
- Go integration tests, one per scenario row, using the imported users'
  API keys against `/feature_store`, `/batch_feature_store`, pk-read and
  batch endpoints. Assertions: allow/deny + denial shape from the
  recordings (per §2.5: serving denials mirror the MySQL-grant contract —
  name the denied table/column; 400/270268 belongs to creation-time
  endpoints RDRS doesn't serve); data values of allowed reads validated
  the existing way, by querying mysqld directly in the test.
- C++ api_key/DAL unit tests for the new readers
  (`shared_feature_group`, `shared_feature`, `restricted_*`), following the
  P2 pattern (runtime NDB row insert/delete).
- The D-RDRS gap test goes in first — it documents today's wrong behavior
  and gates the restricted-role implementation.

## 4. Open questions — ANSWERED during T2 (see §2.5 for detail)

1. Store-level denial (A2): **404** at `get_feature_store` — the store is
   simply not listed for a stranger. No 400/11 anywhere in the read path.
2. Where does enforcement fire? **Not** in the FV serving-metadata path
   (FV definition and prepared statements flow unenforced, even over
   unshared FGs). Enforcement = MySQL grants at data-read time, plus
   400/270268 at FV **creation** and 400/270266 on ungranted FG metadata
   for restricted users. RDRS therefore enforces at request/data time.
3. C4: **column-restricted SELECT** — `GRANT SELECT (customer_id,
   event_time, num_transactions_30d)`; reading the whole table or an
   unshared column fails (1142/1143). E3 = mirror the column grant.
4. The five metadata tables are sufficient for RDRS: the extra online
   artifacts are the per-user MySQL accounts/grants themselves, which
   RDRS bypasses by design (it replicates their *semantics* at the
   application layer).
5. PK/event-time force-inclusion in restricted_feature_access: **yes** —
   D3's child rows are exactly {customer_id, event_time,
   num_transactions_30d}, same as shared_feature.
