"""Self-contained human-readable context for every T2 recording.

write_recording() attaches READS_LEGEND plus the matching SCENARIO_CONTEXT
entry to each recording json so the files can be understood without the
design doc. Content:  producer usera / usera_project holds two online FGs
(usera_customers_fg: customer_id PK, age, country, is_premium, event_time;
usera_transactions_fg: customer_id PK, num_transactions_30d,
total_spend_30d, avg_transaction_value_30d, event_time) and two FVs
(usera_customers_transactions_fv = all columns of both FGs;
usera_txncount_fv = customer_id, age + num_transactions_30d only).
"""

SETUP = {
    "purpose":
        "Reference recordings of how a real Hopsworks cluster enforces "
        "feature-store/feature-group/feature sharing, captured to define "
        "the target behavior for the RonDB REST server (RDRS) fine-grained "
        "sharing implementation. Test design: "
        "docs/fg_fv_sharing_fine_grained_test_design.md.",
    "cluster":
        "Live Hopsworks 5.0.1 on kubernetes; REST API at "
        "https://10.115.253.130/hopsworks-api/api; online feature store = "
        "RonDB, external mysqld LoadBalancer at 10.115.253.126:3306; prod "
        "RDRS (pre-fix, member-only authorization) at "
        "https://10.115.253.124:4406. All auto-increment counters were "
        "floored to 100000 before any test data was created, so every id "
        "in these recordings is >= 100000 and imports into the RDRS test "
        "fixtures without remapping.",
    "producer": {
        "project": "usera_project (owned by usera@lc.com, Data owner; the "
                   "ONLY project with content - everything is shared FROM "
                   "here)",
        "feature_groups": {
            "usera_customers_fg":
                "online-enabled, 4 rows; columns: customer_id (PK), age, "
                "country, is_premium, event_time (event-time col); online "
                "table usera_project.usera_customers_fg_1",
            "usera_transactions_fg":
                "online-enabled, 4 rows; columns: customer_id (PK), "
                "num_transactions_30d, total_spend_30d, "
                "avg_transaction_value_30d, event_time; online table "
                "usera_project.usera_transactions_fg_1",
        },
        "feature_views": {
            "usera_customers_transactions_fv":
                "joins BOTH FGs on customer_id and selects ALL their "
                "columns - serving it needs full access to both FGs",
            "usera_txncount_fv":
                "joins both FGs but selects only customer_id, age from "
                "customers and num_transactions_30d from transactions - "
                "the instrument for partial grants: works when only "
                "num_transactions_30d is granted while the full FV fails",
        },
    },
    "users":
        "12 users usera..userl@lc.com (uids 100000..100011), all with "
        "password Pass123 and one api key each (identical scopes: "
        "FEATURESTORE PROJECT JOB KAFKA SERVING DATASET_VIEW/CREATE/"
        "DELETE). Consumers userb..userf own empty projects "
        "userb_project..userf_project (empty on purpose: anything their "
        "key can read from usera's data is provably reachable only via "
        "the grant under test). Grant granularity decreases "
        "alphabetically: userb = entire store, userc = both FGs whole, "
        "userd = one FG whole, usere = whole FG + feature subset, userf "
        "= nothing (outsider). userj/userk/userl own no projects and are "
        "members of usera_project with project_team role 'Feature store "
        "restricted': userj = no grants, userk = one FG full, userl = "
        "one FG full + feature subset. userg/h/i exist but are unused "
        "(reserved).",
    "how_reads_authenticate":
        "Every read is performed with the READER's own api key. hsfs "
        "reads log in through the reader's own project (restricted users "
        "log in through usera_project itself - they own none); RDRS "
        "reads pass the key in the X-API-KEY header; the mysql probe "
        "uses the reader's per-user online-store account "
        "(<project>_<username>) fetched via their storage connector.",
    "enforcement_model_observed":
        "Hopsworks enforces online sharing exclusively at the MySQL "
        "GRANT layer: store-entirely share => GRANT SELECT ON "
        "<producer_db>.*; whole-FG share => table-level GRANT on that "
        "FG's online table; feature-level share => column-level GRANT "
        "SELECT (PK, event_time, granted cols). Metadata endpoints "
        "(feature view, prepared statements) are NOT enforced. Denials "
        "surface as SQL errors at read time: 1044 no DB access, 1142 "
        "table denied, 1143 column denied. The restricted_* system "
        "mirrors the same ladder per-user instead of per-project.",
}

READS_LEGEND = {
    "hsfs_full_fv":
        "hopsworks python client, logged in AS THE READER with their own "
        "api key through their login project: get_feature_store("
        "'usera_project_featurestore') -> get_feature_view("
        "'usera_customers_transactions_fv', 1) -> get_feature_vector("
        "{'customer_id': 1}). This FV needs ALL columns of BOTH FGs.",
    "hsfs_narrow_fv":
        "same as hsfs_full_fv but on 'usera_txncount_fv' which only needs "
        "num_transactions_30d from usera_transactions_fg (plus customers "
        "columns).",
    "rdrs_full_fv":
        "POST /0.1.0/feature_store on the PROD RDRS server (pre-fix build "
        "with member-only authorization - recorded to document the gap, "
        "NOT the target behavior) with the reader's api key in X-API-KEY; "
        "featureStoreName=usera_project, featureViewName="
        "usera_customers_transactions_fv.",
    "rdrs_narrow_fv":
        "same as rdrs_full_fv but featureViewName=usera_txncount_fv.",
    "rdrs_batch_full_fv":
        "POST /0.1.0/batch_feature_store on prod RDRS, one entry, full FV.",
    "rdrs_pkread_customers":
        "POST /0.1.0/usera_project/usera_customers_fg_1/pk-read on prod "
        "RDRS with the reader's api key - direct online-table access.",
    "rdrs_pkread_transactions":
        "POST /0.1.0/usera_project/usera_transactions_fg_1/pk-read on "
        "prod RDRS with the reader's api key.",
    "rdrs_batch_pkread_customers":
        "POST /0.1.0/batch with two pk-read operations on the customers "
        "table, prod RDRS, reader's api key.",
    "mysql_grants_probe":
        "the enforcement layer itself: fetch the reader's per-user online-"
        "store MySQL account via THEIR storage-connector REST endpoint, "
        "then SHOW GRANTS plus four probe SELECTs against the online DB "
        "(all columns of each FG table; only the shared column; only an "
        "unshared column). Hopsworks enforces sharing online exclusively "
        "through these MySQL GRANTs.",
}

SCENARIO_CONTEXT = {
    "A1": {
        "test": "Store-entirely share - the coarsest cross-project grant.",
        "projects_involved":
            "usera_project (producer, has both FGs + both FVs) and "
            "userb_project (consumer, empty).",
        "who_shares_what_with_whom":
            "usera (Data owner of usera_project) shares his ENTIRE "
            "feature store with the PROJECT userb_project: "
            "POST /project/<usera_project>/featurestores/share"
            "?project=<userb_project>. No other grant exists for userb.",
        "who_reads":
            "userb, using userb's api key, hsfs login via userb_project.",
        "verifies":
            "one shared_feature_store row with shared_entirely=1 is "
            "written; userb can read BOTH FVs and BOTH online tables; his "
            "MySQL account gets a DATABASE-level grant "
            "(GRANT SELECT ON usera_project.*).",
    },
    "A2": {
        "test": "Outsider negative control - store-level denial.",
        "projects_involved":
            "usera_project (producer) and userf_project (consumer, empty, "
            "completely unrelated).",
        "who_shares_what_with_whom":
            "NOTHING is shared with userf_project, and userf is not a "
            "member of usera_project. userf is the only user with no path "
            "at all to usera's data.",
        "who_reads":
            "userf, using userf's api key, hsfs login via userf_project.",
        "verifies":
            "every surface denies; the hopsworks reference denies already "
            "at get_feature_store with HTTP 404 (the store is not even "
            "listed for him).",
    },
    "A3": {
        "test": "Revocation of the store-entirely share (live transition).",
        "projects_involved":
            "usera_project (producer) and userb_project (consumer holding "
            "the A1 store-entirely share).",
        "who_shares_what_with_whom":
            "usera UNSHARES the store from userb_project (DELETE "
            "/project/<usera_project>/featurestores/share"
            "?project=<userb_project>), then re-shares it to restore the "
            "A1 static state.",
        "who_reads":
            "userb, using userb's api key, once after the unshare (must "
            "deny like an outsider) and once after the re-share (must "
            "allow again).",
        "verifies":
            "unshare deletes the shared_feature_store row and revokes the "
            "database-level MySQL grant; re-share restores both.",
    },
    "B1_B2_B4": {
        "test": "Single whole-FG share; everything else must stay closed.",
        "projects_involved":
            "usera_project (producer) and userd_project (consumer, empty).",
        "who_shares_what_with_whom":
            "usera shares ONLY the feature group usera_customers_fg - as "
            "a whole FG, no feature list - with the PROJECT userd_project: "
            "POST /project/<usera_project>/featurestores/share"
            "/featuregroups/<usera_customers_fg>?project=<userd_project>. "
            "usera_transactions_fg is NOT shared with userd_project.",
        "who_reads":
            "userd, using userd's api key, hsfs login via userd_project.",
        "verifies":
            "B1: metadata = one placeholder shared_feature_store row "
            "(shared_entirely=0) + one shared_feature_group row, no "
            "shared_feature rows. B2: the full FV denies (needs the "
            "unshared transactions_fg). B4: the placeholder row opens "
            "nothing else - narrow FV and transactions table deny too. "
            "MySQL: table-level grant on the customers table only.",
    },
    "B3": {
        "test": "Both FGs shared individually - allow via per-FG cascade.",
        "projects_involved":
            "usera_project (producer) and userc_project (consumer, empty).",
        "who_shares_what_with_whom":
            "usera shares BOTH feature groups (usera_customers_fg and "
            "usera_transactions_fg), EACH as a whole FG, with the PROJECT "
            "userc_project - two separate share/featuregroups calls. The "
            "store itself is NOT shared (contrast with A1/userb).",
        "who_reads":
            "userc, using userc's api key, hsfs login via userc_project.",
        "verifies":
            "both FVs and both tables readable through the per-FG cascade; "
            "MySQL shows two TABLE-level grants and no database-level "
            "grant (the difference to the store-entirely share).",
    },
    "B5": {
        "test": "Revoking one of two FG shares re-breaks the joined FV "
                "(live transition).",
        "projects_involved":
            "usera_project (producer) and userc_project (consumer holding "
            "the B3 both-FGs share).",
        "who_shares_what_with_whom":
            "usera UNSHARES usera_customers_fg from userc_project (the "
            "transactions share stays), then re-shares it to restore B3.",
        "who_reads":
            "userc, using userc's api key, after the unshare (full FV "
            "must deny again, customers table gone) and after the "
            "re-share (all allowed again).",
        "verifies":
            "removing ONE constituent FG revokes that FG's rows/grant and "
            "breaks every FV that joins it.",
    },
    "C": {
        "test": "Feature-level share - the finest cross-project grant.",
        "projects_involved":
            "usera_project (producer) and usere_project (consumer, empty).",
        "who_shares_what_with_whom":
            "usera shares with the PROJECT usere_project: (1) "
            "usera_customers_fg as a whole FG, and (2) "
            "usera_transactions_fg RESTRICTED to the single feature "
            "num_transactions_30d (?feature=num_transactions_30d). "
            "total_spend_30d and avg_transaction_value_30d are NOT shared.",
        "who_reads":
            "usere, using usere's api key, hsfs login via usere_project.",
        "verifies":
            "C1: the narrow FV usera_txncount_fv works (its transactions "
            "columns fit the grant). C2: the full FV denies - it needs "
            "the unshared columns. C3: shared_feature rows force-include "
            "PK customer_id and event_time alongside num_transactions_30d. "
            "C4: MySQL shows a COLUMN-level grant (GRANT SELECT "
            "(customer_id, event_time, num_transactions_30d)); selecting "
            "an unshared column fails with SQL error 1143.",
    },
    "D0": {
        "test": "Restricted member with NO grants sees nothing.",
        "projects_involved":
            "usera_project only (in-project scenario - no consumer "
            "project involved).",
        "who_shares_what_with_whom":
            "NOTHING is granted. userj is a member of usera_project with "
            "project_team role 'Feature store restricted' and zero "
            "restricted_* grant rows.",
        "who_reads":
            "userj, using userj's api key, hsfs login via usera_project "
            "itself (restricted users read in-project; userj owns no "
            "project).",
        "verifies":
            "every surface denies - his MySQL account cannot even enter "
            "the usera_project database (error 1044). NOTE: prod RDRS "
            "wrongly returns 200 everywhere because userj is a "
            "project_team member - this recording documents the security "
            "gap the fine-grained RDRS work must close.",
    },
    "D1": {
        "test": "Restricted member granted ALL FGs - full access "
                "(live transition).",
        "projects_involved": "usera_project only (in-project scenario).",
        "who_shares_what_with_whom":
            "on top of userk's static grant (usera_customers_fg entirely, "
            "from D2), usera TEMPORARILY grants him usera_transactions_fg "
            "entirely too: POST /project/<usera_project>/featurestores/"
            "<fs>/featuregroups/<usera_transactions_fg>/restrictedaccess"
            "?user=userk@lc.com - then revokes it again.",
        "who_reads":
            "userk, using userk's api key, hsfs login via usera_project; "
            "once while both FGs are granted (everything must allow) and "
            "once after the revoke (back to the D2 denials).",
        "verifies":
            "with every FG granted entirely a restricted user is "
            "equivalent to a full reader; revocation takes effect "
            "immediately (grant row and MySQL table grant removed).",
    },
    "D2": {
        "test": "Restricted member granted ONE full FG.",
        "projects_involved": "usera_project only (in-project scenario).",
        "who_shares_what_with_whom":
            "usera grants the USER userk@lc.com (restricted member of "
            "usera_project) access to usera_customers_fg ENTIRELY: "
            "POST /project/<usera_project>/featurestores/<fs>/"
            "featuregroups/<usera_customers_fg>/restrictedaccess"
            "?user=userk@lc.com (no feature params). "
            "usera_transactions_fg is NOT granted.",
        "who_reads":
            "userk, using userk's api key, hsfs login via usera_project.",
        "verifies":
            "metadata = restricted_feature_group_access row with "
            "can_access_entirely=1 and no restricted_feature_access child "
            "rows; both FVs deny (both need transactions_fg); MySQL shows "
            "a table-level grant on the customers table only.",
    },
    "D3": {
        "test": "Restricted member granted one full FG + part of the "
                "other - the restricted twin of scenario C.",
        "projects_involved": "usera_project only (in-project scenario).",
        "who_shares_what_with_whom":
            "usera grants the USER userl@lc.com (restricted member): (1) "
            "usera_customers_fg entirely, and (2) usera_transactions_fg "
            "restricted to num_transactions_30d "
            "(?user=userl@lc.com&feature=num_transactions_30d).",
        "who_reads":
            "userl, using userl's api key, hsfs login via usera_project.",
        "verifies":
            "narrow FV works, full FV denies with SQL error 1143 naming "
            "total_spend_30d; metadata = can_access_entirely=0 row plus "
            "restricted_feature_access child rows force-including "
            "customer_id and event_time; MySQL column-level grant "
            "identical in shape to usere's cross-project one (C4).",
    },
    "F1": {
        "test": "Consumer creates his OWN feature view on wholly-shared "
                "foreign FGs.",
        "projects_involved":
            "usera_project (producer of the FGs) and userc_project (the "
            "consumer's own project where the new FV is created).",
        "who_shares_what_with_whom":
            "reuses userc's static B3 grant: both usera FGs shared whole "
            "with userc_project. No new grant in this scenario.",
        "who_reads":
            "userc, using userc's api key: creates userc_own_fv (version "
            "1) in userc_project's own feature store, joining BOTH usera "
            "FGs with ALL columns, then reads a vector from it; the prod-"
            "RDRS read uses featureStoreName=userc_project (the consumer "
            "store) even though the data lives in usera_project's online "
            "DB - the cross-store serving path.",
        "verifies":
            "FV creation over fully-shared foreign FGs is allowed and the "
            "consumer-side FV serves full vectors.",
    },
    "F2_F3": {
        "test": "Consumer creates his OWN FV on a partially-shared FG - "
                "within the grant vs selecting an unshared column.",
        "projects_involved":
            "usera_project (producer) and usere_project (consumer's own "
            "project).",
        "who_shares_what_with_whom":
            "reuses usere's static C grant: customers_fg whole + "
            "transactions_fg restricted to num_transactions_30d. No new "
            "grant in this scenario.",
        "who_reads":
            "usere, using usere's api key: (F2) creates usere_own_fv in "
            "usere_project selecting only granted columns (customer_id, "
            "age + num_transactions_30d) and reads it; (F3) attempts "
            "usere_own_denied_fv selecting the UNSHARED total_spend_30d.",
        "verifies":
            "F2: creation + serving work within the grant. F3: records "
            "WHERE the unshared column is rejected - at FV creation "
            "(FEATURE_NOT_SHARED 400/268 per AccessController.verifyAccess) "
            "or only later at read time (SQL 1143).",
    },
    "F4_F5": {
        "test": "Restricted users creating FVs in the producer project "
                "itself.",
        "projects_involved":
            "usera_project only - restricted users own no project, so "
            "their FVs (if allowed) land in usera_project's feature store.",
        "who_shares_what_with_whom":
            "reuses the static restricted grants: userk = customers_fg "
            "entirely (D2); userl = customers entirely + transactions"
            "{num_transactions_30d} (D3). No new grant in this scenario.",
        "who_reads":
            "userk and userl, each with their own api key, hsfs login via "
            "usera_project: userk attempts an FV over customers columns "
            "only (F4); userl attempts one within his grant and one "
            "selecting the ungranted total_spend_30d (F5).",
        "verifies":
            "whether the 'Feature store restricted' role may create FVs "
            "at all (the role check may deny before any column logic), "
            "and if so whether column checks mirror the cross-project "
            "case.",
    },
    "D4": {
        "test": "Granting restricted access to an ineligible user is "
                "rejected (no reads).",
        "projects_involved":
            "usera_project (producer); userb is NOT a member of it.",
        "who_shares_what_with_whom":
            "usera ATTEMPTS to grant restricted access on "
            "usera_customers_fg to the USER userb@lc.com, who is neither "
            "a member of usera_project nor holds the 'Feature store "
            "restricted' role.",
        "who_reads": "nobody - the recording captures the grant rejection.",
        "verifies":
            "the grant call itself fails: HTTP 403, errorCode 160047, "
            "'User must have the Feature store restricted role...'; the "
            "restricted_* tables keep only userk's and userl's rows from "
            "D2/D3, unchanged.",
    },
}
