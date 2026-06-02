# Monitoring the Data Node Security Framework

This doc covers what to alert on, how to scrape the data, and ready-to-paste filter queries. The framework's observability surface is two streams:

- **`SECURITY_EVENT:` cluster log lines** — one per violation, structured `key=value`. Kernel-emitted (via `NDB_LE_SecurityEvent` → CMVMI → cluster log) plus RONDIS-emitted (via `RONDIS_SECURITY_EVENT` macro → RONDIS stdout). Same format in both, so a single grep finds everything.
- **`ndbinfo.security_events`** — SQL-queryable per-NodeId rolling state (cumulative + 5-min window + last-strike fields). Cluster-aggregated. Requires `SELECT` privilege on the view; restrict via `REVOKE/GRANT` to DBA accounts (the doc's "PROCESS-privilege" intent is enforced operationally, not in code — see policy doc §11.1).

The cluster log is the durable audit trail; ndbinfo is the live dashboard.

---

## Cluster log line format

```
SECURITY_EVENT: tier=B node_id=5 node_type=API violation=keyinfo_signal_length_mismatch source_block=DBTC source_line=2998 window_count=3 total_count=14
```

Fields:
- `tier` — `A` (would-disconnect) or `B` (log-only).
- `node_id` — offending NodeId (0 for RONDIS, where the offender is a Redis client below the cluster's NodeId granularity).
- `node_type` — `0`=NDB (data node), `1`=API, `2`=MGM (numeric in the kernel line; RONDIS prints `API`).
- `violation` — the reason string from `g_violation_info[]` (see `ViolationType.hpp`).
- `source_block` — block reference of the detector (`DBTC`, `RONDIS`, etc.).
- `source_line` — `__LINE__` at the detection site.
- `window_count`, `total_count` — for the offending node within the 5-min window / cumulative.

A line is emitted at most once per **`SEC_LOG_SUPPRESS_MS` (100 ms)** per offending node, except Tier A and first-strike which always emit (see policy doc §8.1). Suppressed strikes still appear in the counts of the next emitted line.

---

## Alerting recommendations

Tune to your environment. The recommended baseline:

| Trigger | Severity | Action |
|---|---|---|
| **First `tier=A` for any node in a 15-min rolling window** | Page on-call | The cluster just disconnected a node — investigate immediately. |
| **Subsequent `tier=A` from the same node within the window** | Group / suppress at alerting layer | Buggy client libraries / runaway state-machine bugs produce repeated Tier A from one node; don't silence the pager. |
| **`tier=A` from multiple distinct nodes simultaneously** | Higher-priority page | Coordinated attack or widespread client bug — both warrant immediate eyes. |
| **`current_window_count > N` for any (node_id, tier=B)** | Notify (not page) | Tier B is log-only, but elevated rates from one node mean either a buggy client or active probing. Default `N`: 10 per 5-min window. |
| **`EnableSecurityDisconnect = false`** | Page | Cluster is in observation mode — security enforcement is off. Either a deliberate ops action or a real incident. |
| **Tier C `violation=rate_limit_exceeded`** | Page | The cluster-side safety net fired — an API node was disconnected for volumetric flood. Check upstream rate limits and the offending host. |

For Tier B grouping: the `(node_id, violation)` pair is the natural grouping key. A single buggy client library will fire the same `(node_id, violation)` repeatedly; grouping prevents pager-flood.

---

## Log-aggregation filters

### Loki

```logql
{job="ndb_cluster_log"} |= "SECURITY_EVENT:" | pattern `SECURITY_EVENT: tier=<tier> node_id=<nodeid> node_type=<ntype> violation=<v> source_block=<src> source_line=<line> window_count=<wc> total_count=<tc>`
```

### Splunk

```spl
index=ndb_cluster "SECURITY_EVENT:" | rex "SECURITY_EVENT: tier=(?<tier>\S+) node_id=(?<node_id>\d+) node_type=(?<node_type>\S+) violation=(?<violation>\S+) source_block=(?<source_block>\S+) source_line=(?<source_line>\d+) window_count=(?<window_count>\d+) total_count=(?<total_count>\d+)"
```

### Plain grep / journald

```bash
# All security events:
grep "^SECURITY_EVENT:" /var/log/ndb/ndb_*_cluster.log

# Tier A only:
grep "^SECURITY_EVENT: tier=A" /var/log/ndb/ndb_*_cluster.log

# One specific violation type across the cluster:
grep "violation=tckeyreq_signal_too_short" /var/log/ndb/ndb_*_cluster.log
```

RONDIS prints to its own stdout, not the NDB cluster log. If you collect RONDIS logs separately, scrape both streams.

---

## Prometheus scrape from ndbinfo

Use `mysqld_exporter` with a custom query collector against `ndbinfo.security_events`. Example `queries.yaml` snippet:

```yaml
ndb_security_strikes:
  query: |
    SELECT node_id, total_tier_a, total_tier_b, total_disconnects,
           current_window_count, last_strike_seconds_ago
    FROM ndbinfo.security_events
  metrics:
    - node_id:
        usage: LABEL
        description: Offending NodeId
    - total_tier_a:
        usage: COUNTER
        description: Cumulative Tier A strikes
    - total_tier_b:
        usage: COUNTER
        description: Cumulative Tier B strikes
    - total_disconnects:
        usage: COUNTER
        description: Times this node has been disconnected for a Tier A violation
    - current_window_count:
        usage: GAUGE
        description: Strikes in the active 5-minute window
    - last_strike_seconds_ago:
        usage: GAUGE
        description: Age in seconds of the most recent strike
```

Scrape interval: 30–60s is plenty (the view is cheap, but it doesn't change faster than your alert cadence anyway). Don't scrape sub-second — Tier B's window granularity is 30 s, so finer resolution gains nothing.

### Example Prometheus alert rules

```yaml
groups:
  - name: ndb_security
    rules:
      - alert: NdbTierADisconnect
        expr: increase(ndb_security_strikes_total_disconnects[5m]) > 0
        for: 0m
        labels:
          severity: page
        annotations:
          summary: "NDB Tier A disconnect — node {{ $labels.node_id }}"
          description: "A node was disconnected for a Tier A malicious-signal violation. Investigate the cluster log for SECURITY_EVENT lines."

      - alert: NdbTierBElevated
        expr: rate(ndb_security_strikes_total_tier_b[10m]) > 0.1
        for: 5m
        labels:
          severity: warn
        annotations:
          summary: "NDB Tier B strikes elevated — node {{ $labels.node_id }}"
          description: "Tier B violations from node {{ $labels.node_id }} exceed 6/minute averaged over 10 min. Likely buggy client or active probing."

      - alert: NdbSecurityRateLimitFired
        expr: increase(ndb_security_strikes_total_tier_a{violation="rate_limit_exceeded"}[5m]) > 0
        for: 0m
        labels:
          severity: page
        annotations:
          summary: "NDB Tier C safety net fired — node {{ $labels.node_id }}"
          description: "Cluster-side rate-limit safety net disconnected an API node. Check upstream rate limits."
```

(The `violation` label needs to be exposed by enriching the exporter query with a join against the live log; if that's too much, drop that last rule and rely on `NdbTierADisconnect`.)

---

## Observation mode (kill switch off)

Watch for:
- `SECURITY_KILLSWITCH:` log lines (when the live-`SET` plumbing lands — currently the kill-switch state is only logged at startup via a plain info line).
- `EnableSecurityDisconnect = 0` in `SHOW CONFIG` or `ndb_config --query`.

If observation mode is on, **the cluster is no longer enforcing Tier A disconnects.** A real attack would be detected and logged but not acted on. Treat it as an operational alert at all times.

---

## What this doesn't cover

- **Per-Redis-client identity for RONDIS events.** v1 emits `node_id=0` for RONDIS violations because RONDIS connections are below NDB's NodeId granularity. If you need per-client attribution, parse `source_block=RONDIS` + the upstream RONDIS access logs (which carry client IP).
- **Per-violation-type breakdown in the live window.** `current_window_count` is per-node, not per-(node, violation_type) — see policy doc §8.1 for why. The per-type detail is in `last_violation` (most-recent) and the cluster log (full history).
- **Live-`SET` of `EnableSecurityDisconnect`.** Deferred from v1; until that lands, observation mode is opt-in at config.ini level only, set via the test-only `DUMP 9101` in debug builds (mysql-test/suite/ndb/t/ndb_security.test).
