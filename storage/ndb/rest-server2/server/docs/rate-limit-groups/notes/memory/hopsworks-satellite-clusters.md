---
name: hopsworks-satellite-clusters
description: "Satellite (\"Distributed online FS\") RonDB cluster topology scouted 2026-09-24 across deploy.hopsworks.ai / rondb-helm / hopsworks — no first-class Hopsworks concept, one magic connector; gaps list incl. quota client single-cluster + broken cross-satellite sharing"
metadata: 
  node_type: memory
  type: project
  originSessionId: accb43b6-380b-45df-9c91-4b43645871e9
  modified: 2026-09-24T11:23:37.629Z
---

# Satellite RonDB clusters — topology & Hopsworks awareness (scouted 2026-09-24)

Three explorers over deploy.hopsworks.ai (tip d185815), rondb-helm, hopsworks
(HEAD f873029d71). Relevant to [[rondb-978-group-rate-limits-idea]].

## Topology (deploy.hopsworks.ai + rondb-helm)

- Satellite = **full RonDB cluster** (own mgmd, ndbd, mysqld, RDRS, Kafka, onlinefs
  writer) in its **own k8s namespace inside the parent's EKS cluster**. One main + N
  satellites; `cluster.parent_cluster_id` self-FK in the deploy DB; satellite runs NO
  Hopsworks app, no Trino/Superset. Own NLB endpoints (RDRS 4406/5406, MySQL 3306).
- **Metadata/data split**: satellite RDRS gets TWO NDB connections — `RonDB` (data,
  local) and `RonDBMetadataCluster` → parent's mgmd via helm
  `rdrs.externalMetadataCluster.mgmds` (deploy: chart_onlinefs_satellite.go:127;
  helm: files/configs/rest_api.json; RDRS fallback: empty ⇒ metadata=data connection,
  rdrs_rondb_connection_pool.cpp:292/302). Metadata conn used for api_key lookup,
  hopsworks DAL, FS metadata cache.
- **hopsworks DB lives ONLY on the main cluster** — no replica on satellites (global
  replication in helm = unrelated active/passive DR).
- Helm has NO satellite concept; only `externalMetadataCluster` (maxItems 1, raw IP,
  no HA) + unrelated `isExternallyManaged` (BYO-RonDB, suppresses data tier). Only
  rate-limit knob: `rondbConfig.ActivateRateLimits` 0/1 in config.ini:223. NetworkPolicy
  trap: main cluster must add ingressSelectors for satellite RDRS (undocumented).
- Deploy control plane has ZERO quota/rate-limit plumbing.

## Hopsworks awareness: NO first-class concept

Called "Distributed online feature store" (commit 70e4a4dd2a, Fabio, 2025-07-28).
Mechanism = magic-named per-featurestore JDBC storage connector **`onlinefs_connector`**
(+ `__ddl_mysql_jdbc_connection` external arg for separate DDL host), stored as URL
string on hopsworks.feature_store_connector, **created out-of-band** (nothing in repo
creates it; name NOT reserved — a project Data Owner could create/repoint one!).
Override covers: admin connections (OnlineFeaturestoreFacade.establishAdminConnection:544,
creds embedded in URL), online DDL/user creation, per-user `<user>_onlinefeaturestore`
connector → client-facing JDBC URL. So per-member MySQL accounts ARE created on the
satellite's mysqlds.

**NOT covered (gaps):**
1. **RonDbManagementClient is single-cluster**: discovers one global "mgmd" service
   (NoTags), settings have no host/port; unused (host,port) overloads exist. A satellite
   project's DATABASE QUOTA goes to the WRONG (main) mgmd today.
2. **Cross-satellite sharing silently broken**: ShareFeatureStoreController:143-144
   creates borrower users on borrower's cluster, grants on owner's cluster;
   grantUserPrivileges guards `COUNT(*) FROM mysql.user` and silently no-ops when the
   user doesn't exist there. Sharing works only when both projects are on the SAME
   satellite (or users provisioned there some other way).
3. Table-size metrics read local ndbinfo → 0 for satellite tables.
4. No RDRS endpoint routing at all in Hopsworks (no API returns an RDRS address).
5. MultiRegion is a DIFFERENT concept (active/passive replica of same data; explicitly
   incompatible with onlinefs_connector, OnlineFeaturestoreFacade.java:572 comment).
6. Unrelated live bug: ProjectQuotasController:309/:320 inverted flag check —
   `if (project == null || RONDB_QUOTAS_ENABLED) return;` should be `!enabled` → default
   quotas never applied at project create, never dropped at delete (callers already gate
   on enabled=true).

## Rate-limit implications (for group buckets, see [[rondb-978-group-rate-limits-idea]])

- Buckets are per-cluster kernel state; RDRS tags transactions on the DATA connection →
  enforcement lands on the satellite holding the project, correct by construction.
- Group definitions in hopsworks DB are readable by satellite RDRS over its existing
  metadata connection → RDRS-resolved group tagging works unchanged on satellites.
- "10% for dept X" naturally means 10% of EACH cluster hosting the dept's projects
  (projects are pinned exclusively to one satellite; each cluster's CPU is a separate
  resource) — group semantics compose per-cluster without new mechanisms.
- Provisioning needs a project→mgmd mapping Hopsworks lacks: today the only satellite
  pointer is a JDBC URL string (no mgmd endpoint). Need a real cluster registry (id,
  mgmd endpoint, per-cluster creds) or extend the connector convention; then USER SET
  per group on every satellite hosting grouped projects (kernel-alias variant: aliases
  per satellite too). This registry would also fix gap 1 (quotas to wrong cluster).
