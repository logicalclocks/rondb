# Monitoring the Data Node Security Framework

This doc covers what to alert on, how to scrape the data, and ready-to-paste filter queries.

The framework's observability surface is two streams:

- **`SECURITY_EVENT:` log lines** — one per violation, structured `key=value`. Kernel-side via CMVMI → cluster log; RONDIS-side via `RONDIS_SECURITY_EVENT` macro → RONDIS stdout. Same 4-field prefix in both; a single grep finds everything.
- **`ndbinfo.security_violations` + `ndbinfo.security_violation_counts`** — SQL-queryable. `security_violations` is the static type catalog (public). `security_violation_counts` holds cumulative per-(reporting_node_id, violation_id) counts; requires `PROCESS` privilege.

The cluster log is the durable audit trail (and the only source of offender node_id for Tier B). ndbinfo is the live count dashboard.

---

## Cluster log line format

### Kernel side (emitted by QMGR via EventLogger)

```
SECURITY_EVENT: tier=A node_id=5 node_type=API violation=invalid_apiconnectptr_in_tckeyreq
SECURITY_EVENT: tier=B node_id=5 node_type=API violation=keyinfo_signal_length_mismatch
```

Fields:
- `tier` — `A` (disconnect-eligible) or `B` (log-only forever).
- `node_id` — offending NodeId as seen by the reporting data node.
- `node_type` — `DB` (data node), `API` (API/mysqld/RDRS), `MGM` (management node).
- `violation` — the reason string from `g_violation_info[]` in `ViolationType.hpp`.

There are no `source_block`, `source_line`, `window_count`, or `total_count` fields in v2.

### RONDIS side (printed to RONDIS stdout)

```
SECURITY_EVENT: tier=B node_id=0 node_type=API violation=rondis_oversize_value client=10.0.0.1:49221 worker=3
SECURITY_EVENT: tier=B node_id=0 node_type=API violation=rondis_select_out_of_range client=10.0.0.1:49221 worker=3
```

The first 4 fields are byte-for-byte identical to the kernel format. `node_id=0` indicates "no NDB NodeId at Redis-client granularity." The two appended fields provide the per-connection attribution that the cluster log line would carry for a kernel event:
- `client=<ip:port>` — the Redis client connection that triggered the violation.
- `worker=<id>` — RONDIS worker thread id.

RONDIS logs to its own stdout, not the NDB cluster log. Configure your log collector to scrape both streams.

**Tier B flood caveat:** there is no in-kernel or upstream rate limiter on `SECURITY_EVENT:` emission. A connected, authenticated client can sustain high Tier B violation rates, each emitting one line, churning the 6×1 MB rotating cluster log. Tier A self-limits (offender disconnected on first strike when `EnableSecurityDisconnect=true`). For Tier B, the worst case is forensic-integrity loss, not crash or OOM. Bound inflow via connection-level controls; monitor `ndbinfo.security_violation_counts` for persistent elevation.

---

## ndbinfo tables

### security_violations (public, static catalog)

```sql
SELECT * FROM ndbinfo.security_violations;
```

| Column | Type | Description |
|---|---|---|
| `violation_id` | int | Stable integer id (maps to `ViolationType` enum) |
| `tier` | char(1) | `A` or `B` |
| `reason` | varchar | Reason string from `g_violation_info[]` |

One row per known violation type; emitted identically by every data node. Use this to look up the id for a specific reason string or build Prometheus label mappings. The violation_id is a stable external contract — ndbinfo monitoring dashboards index by it; renumbering would corrupt historical metrics.

### security_violation_counts (PROCESS privilege required)

```sql
SELECT * FROM ndbinfo.security_violation_counts;
```

| Column | Type | Description |
|---|---|---|
| `reporting_node_id` | int | Data node that observed this count |
| `violation_id` | int | Violation type (FK into security_violations) |
| `tier` | char(1) | `A` or `B` (denormalized from catalog for convenience) |
| `reason` | varchar | Reason string (denormalized) |
| `total_count` | bigint | Cumulative count on this data node since cluster start |

One row per (reporting_node_id, violation_id) with a non-zero count. Zero-count pairs are omitted. **RONDIS violation types (ids 23–24, `rondis_oversize_value` and `rondis_select_out_of_range`) never appear here** — RONDIS bypasses QMGR and never increments `m_violationCounts[]`. They appear only in `security_violations` (the static catalog). Aggregate kernel violations across all data nodes with `SUM(total_count) GROUP BY violation_id`:

```sql
SELECT reason, tier, SUM(total_count) AS cluster_total
FROM ndbinfo.security_violation_counts
GROUP BY violation_id, reason, tier
ORDER BY cluster_total DESC;
```

---

## Alerting recommendations

| Trigger | Severity | Action |
|---|---|---|
| **Any `tier=A` line in the cluster log** | Page on-call | The cluster just disconnected a node — investigate immediately. |
| **`tier=A` from multiple distinct node_ids in a short window** | Higher-priority page | Coordinated attack or widespread client bug — both warrant immediate eyes. |
| **Sustained `tier=B` rate from one node** | Notify (not page) | Buggy client or active probing. Threshold: operator-tunable; default suggestion: >10 violations in 5 min for the same `(node_id, violation)`. |
| **`EnableSecurityDisconnect = false`** | Page | Security enforcement is off. Either a deliberate ops action or an incident response. |

Note: `violation_id` + `reporting_node_id` is the natural grouping key for Tier B. A single buggy library fires the same `(node_id, violation)` pair repeatedly; group before alerting to prevent pager flood.

The offending node_id for Tier B is in the **cluster log line**, not in ndbinfo (ndbinfo counts are per-type across all offenders — the counter array carries no offender attribution by design). To track which nodes triggered which violations, scrape the log.

---

## Log aggregation filters

### Loki

```logql
{job="ndb_cluster_log"} |= "SECURITY_EVENT:"
  | logfmt
  | tier != ""
```

Extract fields:

```logql
{job="ndb_cluster_log"} |= "SECURITY_EVENT:"
  | pattern `SECURITY_EVENT: tier=<tier> node_id=<node_id> node_type=<node_type> violation=<violation>`
```

For RONDIS lines (stdout stream):

```logql
{job="rondis_stdout"} |= "SECURITY_EVENT:"
  | pattern `SECURITY_EVENT: tier=<tier> node_id=<node_id> node_type=<node_type> violation=<violation> client=<client> worker=<worker>`
```

### Splunk

```spl
index=ndb_cluster "SECURITY_EVENT:"
  | rex "SECURITY_EVENT: tier=(?<tier>[AB]) node_id=(?<node_id>\d+) node_type=(?<node_type>\S+) violation=(?<violation>\S+)"
  | stats count by tier, node_id, node_type, violation
```

Tier A only:

```spl
index=ndb_cluster "SECURITY_EVENT: tier=A"
  | rex "node_id=(?<node_id>\d+) node_type=(?<node_type>\S+) violation=(?<violation>\S+)"
  | table _time, node_id, node_type, violation
```

### Plain grep

```bash
# All security events across all data nodes:
grep "^SECURITY_EVENT:" /var/log/ndb/ndb_*_cluster.log

# Tier A only:
grep "^SECURITY_EVENT: tier=A" /var/log/ndb/ndb_*_cluster.log

# Specific violation:
grep "violation=keyinfo_signal_length_mismatch" /var/log/ndb/ndb_*_cluster.log

# RONDIS events (separate stdout stream):
grep "^SECURITY_EVENT:" /var/log/rondis/rondis.log
```

---

## Prometheus scrape from ndbinfo

Use `mysqld_exporter` with a custom query collector targeting `ndbinfo.security_violation_counts`. Example `queries.yaml` snippet:

```yaml
ndb_security_violation_counts:
  query: |
    SELECT reporting_node_id, violation_id, reason, tier,
           total_count
    FROM ndbinfo.security_violation_counts
  metrics:
    - reporting_node_id:
        usage: LABEL
        description: Data node that observed this count
    - violation_id:
        usage: LABEL
        description: Violation type integer id (stable, never renumbered)
    - reason:
        usage: LABEL
        description: Violation reason string
    - tier:
        usage: LABEL
        description: A or B
    - total_count:
        usage: COUNTER
        description: Cumulative count since cluster start
```

Scrape interval: 30–60 s. The counter is cluster-lifetime cumulative; use Prometheus `increase()` or `rate()` to detect new activity. The scraping MySQL user needs both `SELECT` on `ndbinfo.*` AND `PROCESS` privilege (required for `security_violation_counts`).

### Example Prometheus alert rules

```yaml
groups:
  - name: ndb_security
    rules:
      - alert: NdbTierAViolation
        expr: increase(ndb_security_violation_counts_total_count{tier="A"}[5m]) > 0
        for: 0m
        labels:
          severity: page
        annotations:
          summary: "NDB Tier A violation — node {{ $labels.reporting_node_id }} violation {{ $labels.reason }}"
          description: >
            A Tier A malicious-signal violation was detected. The offending node was
            disconnected (when EnableSecurityDisconnect=true). Investigate the cluster
            log for SECURITY_EVENT lines.

      - alert: NdbTierBElevated
        expr: rate(ndb_security_violation_counts_total_count{tier="B"}[10m]) > 0.1
        for: 5m
        labels:
          severity: warn
        annotations:
          summary: "NDB Tier B violations elevated — {{ $labels.reason }}"
          description: >
            Tier B violations of type {{ $labels.reason }} exceed ~6/min averaged
            over 10 min from node {{ $labels.reporting_node_id }}. Check the cluster
            log for the offending node_id; this may be a buggy client or active probing.
```

---

## Observation mode (kill switch off)

When `EnableSecurityDisconnect=false`, the cluster detects and logs everything but disconnects nothing. Signs:

- In debug builds: `DUMP 9101 0` toggles to observation; `DUMP 9101 1` re-enables.
- In production: `config.ini` parameter at startup.
- Query current state: `ndb_config --query=EnableSecurityDisconnect --type=ndbd`.

If observation mode is on, **Tier A violations are being logged but not enforced.** Treat this as an operational alert whenever it is not an explicitly authorized rollout window.

---

## What this doesn't cover

- **Per-offender attribution in Tier B counts.** `ndbinfo.security_violation_counts` is keyed by (reporting_node_id, violation_id) — it carries no record of which remote node sent the bad signals. The offending node_id lives only in the cluster log line. To identify an offender, parse `SECURITY_EVENT:` lines.
- **Per-Redis-client attribution in ndbinfo.** RONDIS events have `node_id=0` in ndbinfo. For per-client attribution, parse the `client=` field from RONDIS stdout logs.
- **Time-series in ndbinfo.** Counts are cluster-lifetime cumulative; time-series come from your Prometheus stack (use `rate()` / `increase()` on the scraped counter). There is no in-kernel windowing.
