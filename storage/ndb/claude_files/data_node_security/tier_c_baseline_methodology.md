# Tier C Baseline Measurement Methodology

The Tier C cluster-side safety net ships with `SecurityRateLimitClusterOverloadsPerSec = 0` (disabled). Operators **must measure their own baseline** before enabling it — the right threshold depends on the workload's natural variance. This doc explains how.

## Why we measure first

The Tier C check uses the per-NodeId receive-overload counter (`get_overload_count(NodeId)`) as the per-second rate metric. That counter increments when the receiving data node feels buffer pressure *from* the offending node. Legitimate workloads occasionally trigger it (bursts, batch jobs, replica catch-up). An attack flood triggers it sustainedly and at high rate. The job of the threshold is to land in between.

The recommendation is **10× the empirical peak per-second delta observed in healthy operation** — high enough that legitimate spikes don't trip it, low enough that an attack is caught well before the cluster degrades. Both halves of that statement are workload-dependent, hence the measurement.

## What to measure

For each API node id `N` reaching a data node:

```
delta_per_sec(N) = ( overload_count(N)_at_t_plus_1s − overload_count(N)_at_t ) / 1.0
```

Capture this once per second for the full representative workload (peak hours, batch windows, schema changes — whatever your cluster does at its busiest). For each API node, record:

- `peak_delta`: the maximum per-second delta observed.
- Optional: `p99_delta`, `mean_delta` if you want to characterise the distribution.

Then take the maximum `peak_delta` across all API nodes — call this `cluster_peak`.

## How to sample

There are two reasonable approaches; pick whichever fits your monitoring stack.

**Option A — scrape the transporter stats from ndbinfo.** RonDB exposes per-transporter byte and overload counts via `ndbinfo.transporters` and similar. A 1-second-cadence scraper (Prometheus / a shell loop / `mysql -e`) computing the per-NodeId delta gives you the data directly.

**Option B — read from cluster log / transporter API.** If you already capture transporter metrics into your monitoring stack, the same series carries the data.

For a one-shot measurement before enabling the safety net, a short script like the following is sufficient. It samples every second for one hour during a representative-load window.

```sql
-- Run once per second for the measurement window:
SELECT node_id, overload_count, NOW() FROM ndbinfo.transporters
 WHERE remote_node_id IN (<your API node ids>);
```

Postprocess by computing the per-(node_id, second) delta, then `MAX()` over the whole window — that's your `peak_delta` per node. Take the max across nodes for `cluster_peak`.

## Setting the threshold

```
SecurityRateLimitClusterOverloadsPerSec = 10 * cluster_peak
```

Then enable enforcement:

```bash
ndb_mgm> SET <node_id> SecurityRateLimitClusterOverloadsPerSec <value>
```

(Live-settable — no restart required.)

## Tuning afterwards

- **False positives** (legitimate workload tripping the threshold): the peak you measured wasn't actually representative. Re-measure during the offending workload (e.g. a backup, schema change, or replica catch-up) and raise the threshold accordingly. The default ratio of 10× is conservative-leaning; some workloads need 20–50×.
- **Never tripping during a real incident**: you may be under-instrumented. Cross-check with `ndbinfo.security_events` after running an adversarial test (Phase 6 test injector), and consider lowering the threshold if 10× turns out to be loose for your workload.

## Why this differs from the original design doc

The policy doc proposed two thresholds — `SecurityRateLimitClusterSignalsPerSec` and `SecurityRateLimitClusterBytesPerSec`. v1 ships **only the overload-count metric** (`SecurityRateLimitClusterOverloadsPerSec`), for two reasons:

1. **No per-NodeId signal-count counter exists** in TransporterRegistry. Adding one would require modifying the receive hot path — not in keeping with v1's lightweight, no-hot-path-overhead mandate.
2. **`get_overload_count(NodeId)` is a more meaningful "this node is overwhelming us" signal** than raw bytes/sec. Bytes/sec flags any high-volume sender, including legitimate bursty workloads; overload count only ticks up when the receiver actually feels buffer pressure. That naturally filters out the false-positive-prone "fast but handled" case.

A bytes-per-second metric (with the necessary multi-transporter aggregation) and a signals-per-second metric (with a new hot-path counter) remain as follow-up if operational evidence shows they catch attacks the overload metric misses.
