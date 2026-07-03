# CTE main JoinAgg suppressed SCAN_NEXTREQ plan

## Trigger

The observed hang was reproduced by a RonSQL query with two aggregating CTEs
and an aggregating main SELECT:

```sql
WITH order_stats AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_totalprice) AS spend
  FROM orders GROUP BY o_custkey),
recent_stats AS (
  SELECT o_custkey AS k2, COUNT(*) AS recent_cnt
  FROM orders WHERE o_orderdate >= '1998-01-01' GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(order_stats.cnt), SUM(order_stats.spend),
       SUM(recent_stats.recent_cnt)
FROM customer AS c
JOIN order_stats ON order_stats.k = c.c_custkey
JOIN recent_stats ON recent_stats.k2 = c.c_custkey
GROUP BY c.c_nationkey;
```

Debug logs show the query reaches the main SELECT phase. The CTE hash tables
already exist and the main scan is doing `CTE_LOOKUP` probes:

```text
CTE_LOOKUP result: aggStateKey=1 ...
CTE_LOOKUP result: aggStateKey=2 ...
```

The last useful DBTC trace then shows a main-scan fragment batch boundary:

```text
TC scanPtrI: 0, done: 0, scan_frag_conf_status: 0, scanFragPtrI: 3 ...
TC scanPtrI: 0, Send SCAN_TABCONF and wait for API
sendScanTabConf: suppress intermediate SCAN_TABCONF for CTE JoinAgg
```

No later `SCAN_NEXTREQ` or `JOIN_AGG_COMPLETE_REQ` appears for the main scan.

## Classification

There are three relevant DBTC paths:

1. CTE materialization phase.
   DBTC must suppress `SCAN_TABCONF`; CTE scan continuation is driven by DBSPJ.
   This is the `m_cteCurrentPhase < m_ctePhaseCount` path in
   `execSCAN_FRAGCONF()`.

2. Main SELECT with CTEs and a main aggregation program.
   DBTC must also suppress intermediate `SCAN_TABCONF` to the API. The raw
   main-scan batches are only inputs to `JoinAggInterpreter`; final rows are
   delivered after `JOIN_AGG_COMPLETE_REQ`.

3. Main SELECT without a main aggregation program, with or without CTEs.
   DBTC must send `SCAN_TABCONF` to the API, because normal API
   `SCAN_NEXTREQ` flow control is the result delivery protocol.

The trigger query is case 2. Suppressing the API-visible `SCAN_TABCONF` is
correct. The bug is that suppressing it also suppresses the only continuation
event for a delivered main-scan fragment.

## Current failure mode

`sendScanTabConf()` drains `m_queued_scan_frags`.

For each queued fragment:

- if `done`, the fragment becomes `COMPLETED`;
- if not `done`, the fragment is moved to `m_delivered_scan_frags` and marked
  `DELIVERED`.

For normal scans, the API receives `SCAN_TABCONF` containing the delivered
fragment ids. It later sends `SCAN_NEXTREQ`, and `execSCAN_NEXTREQ()` moves
those fragments back to `m_running_scan_frags` and either:

- sends `SCAN_NEXTREQ` to the same fragment when `m_scan_frag_conf_status == 0`;
- or reuses the scan fragment record for the next fragment when
  `m_scan_frag_conf_status != 0` and more fragments remain.

For case 2, `sendScanTabConf()` suppresses the API-visible signal after moving
the fragment to `DELIVERED`. Since no API `SCAN_NEXTREQ` can arrive, delivered
fragments never run again. If this happens after the last running fragment has
paused, the scan cannot reach the existing completion condition:

```cpp
m_delivered_scan_frags.isEmpty() && m_running_scan_frags.isEmpty()
```

Therefore `sendJoinAggCompleteReqs()` is never called.

## Design goal

Keep API semantics unchanged:

- do not send intermediate `SCAN_TABCONF` to the API for case 2;
- continue to deliver only final aggregate rows through `JOIN_AGG_COMPLETE`;
- do not change CTE materialization phase suppression;
- do not change non-aggregate main SELECT behavior.

But DBTC must still drive main-scan flow control internally when it suppresses
the API-visible `SCAN_TABCONF`.

## Proposed implementation

1. Factor the case-2 predicate in `sendScanTabConf()`.

   The predicate is:

   ```cpp
   scanPtr.p->m_joinAgg &&
   scanPtr.p->m_numCtes > 0 &&
   scanPtr.p->m_hasMainAggProgram &&
   !release
   ```

   It should be computed before the suppression return and reused for logging
   and continuation decisions.

2. When a queued fragment is moved to `DELIVERED` under this predicate, schedule
   an internal continuation for that exact `ScanFragRec`.

   Only newly delivered fragments from the current `sendScanTabConf()` call
   should be continued. Do not walk the full `m_delivered_scan_frags` list,
   otherwise duplicate continuations are possible.

3. Implement the internal continuation by self-sending `GSN_SCAN_NEXTREQ` to
   DBTC with one receiver id.

   The signal should contain:

   - `apiConnectPtr = scanApiRec`;
   - `stopScan = 0`;
   - current transaction ids;
   - the delivered `ScanFragRec` id after `ScanNextReq::SignalLength`.

   Prefer an asynchronous self-signal (`sendSignal(reference(), ...)`) rather
   than `EXECUTE_DIRECT`, so the queued-to-delivered list transition finishes
   before `execSCAN_NEXTREQ()` mutates the same lists.

4. Let the existing `execSCAN_NEXTREQ()` path perform all state transitions.

   This keeps the continuation rules centralized:

   - `DELIVERED` -> `RUNNING`;
   - same-fragment `SCAN_NEXTREQ` when `m_scan_frag_conf_status == 0`;
   - next-fragment `SCAN_FRAGREQ` when the previous fragment completed but more
     fragments remain.

5. Avoid misleading API-wait state in the suppressed path.

   The current log path says `Send SCAN_TABCONF and wait for API` and starts an
   API timer when all running fragments are delivered. For case 2, DBTC is not
   waiting for the API. Either skip that timer for the suppressed predicate, or
   ensure the self-sent `SCAN_NEXTREQ` clears it immediately. The preferred fix
   is to make the log and timer conditional on actually sending an API-visible
   `SCAN_TABCONF`.

6. Leave final completion unchanged.

   Once all main-scan fragments genuinely complete, `m_delivered_scan_frags`
   and `m_running_scan_frags` become empty. The existing path should enter
   `WAIT_JOIN_AGG_COMPLETE`, send `JOIN_AGG_COMPLETE_REQ`, and deliver final
   aggregate rows through `sendJoinAggScanTabConf()`.

## Review points

- The fix must not send raw main-scan `SCAN_TABCONF` to the API for case 2.
- CTE phase suppression in `execSCAN_FRAGCONF()` must remain untouched.
- Internal continuation must be per newly delivered fragment to avoid duplicate
  `SCAN_NEXTREQ`.
- Use the existing `execSCAN_NEXTREQ()` state machine rather than manually
  moving scan fragment records.
- Close and API-failure paths must remain safe if an internal continuation
  signal is already queued.

## Verification plan

1. Re-run the trigger query.

   Expected debug trace:

   - `sendScanTabConf: suppress intermediate SCAN_TABCONF for CTE JoinAgg`;
   - an internal continuation log for the delivered `ScanFragRec`;
   - `SCAN_NEXTREQ` / `send SCAN_NEXTREQ to node` or next-fragment
     `SCAN_FRAGREQ`;
   - eventually `send JOIN_AGG_COMPLETE_REQ`;
   - final aggregate `SCAN_TABCONF` to the API.

2. Re-run a CTE materialization-heavy query.

   Expected: still uses `execSCAN_FRAGCONF: suppress SCAN_TABCONF during CTE
   phase`; no API-visible intermediate `SCAN_TABCONF`.

3. Re-run a CTE main SELECT without aggregation.

   Expected: normal API-visible `SCAN_TABCONF` / `SCAN_NEXTREQ` flow remains.

4. Re-run a non-CTE join aggregation query.

   Expected: no behavior change; the new predicate is false because
   `m_numCtes == 0`.

5. Add or re-enable a focused regression after the code fix is confirmed.

   The regression should force at least one main-scan batch boundary in case 2,
   so the internal continuation path is exercised before final
   `JOIN_AGG_COMPLETE`.
