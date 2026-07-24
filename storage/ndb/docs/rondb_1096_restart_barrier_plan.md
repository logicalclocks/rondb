# RONDB-1096: Restart barrier at start phase 110

Phased development plan for making RonDB data node restarts safe under
Kubernetes rolling restarts (rondb-helm), based on a code analysis of both
this tree and the rondb-helm chart.

## Problem statement

Under rondb-helm, data nodes run as one StatefulSet per node group. All
Kubernetes probes reduce to a single check: `ndb_mgm -e "<nodeId> status"`
reporting `started` (`healthcheck.sh` in the rondb-docker image). Within one
StatefulSet the RollingUpdate controller waits for Ready before restarting
the next replica, but by default *nothing* coordinates across node groups:
`helm upgrade` stamps all StatefulSets at once and up to one node per node
group restarts concurrently.

That would be acceptable, except a restarting node deliberately kills itself
on *any* data node disconnect during all start phases 1-101 — and a graceful
stop is indistinguishable from a crash to its peers (no planned-departure
protocol; survivors run the full failure protocol on `DISCONNECT_REP`):

- `Qmgr::execDISCONNECT_REP` — QmgrMain.cpp:4942-4958, unconditional
  `progError(NDBD_EXIT_SR_OTHERNODEFAILED)` for any DB-node disconnect while
  own `startLevel < SL_STARTED`. Fires first.
- Backup kill sites: `Qmgr::failReportLab` (:5874-5892),
  `Qmgr::execPREP_FAILREQ` (:6030-6037),
  `Ndbcntr::execNODE_FAILREP` (NdbcntrMain.cpp:3540-3562, three branches).
- The mgmd-level guard (error 5063, `MgmtSrvr::stopNodes`,
  MgmtSrvr.cpp:2450-2457) is skipped with `-a`/`-f` and does not apply to
  the RonDB SIGTERM graceful-shutdown path (HOPSWORKS-2610), which sends a
  force-flagged STOP_REQ via Qmgr with a hardcoded ~27 s deadline
  (QmgrMain.cpp:3246-3275).

So a rolling restart in one node group can kill a node mid-restart in
another node group, with exit code 2308, restarting its node restart from
scratch.

## Solution

Introduce a new start phase 110 (after SUMA's phase 101, the highest phase
in use today). A node at phase 110 is fully recovered — all fragments
synced, GCP/LCP participant, SUMA bucket handover complete — but has not
yet set `NodeState::SL_STARTED`. MGM therefore still reports "starting",
which keeps the Kubernetes pod not-Ready and blocks further pod restarts.
The node parks at 110 until no other data node is in earlier start phases,
then all parked nodes complete together ("wave" release).

Verified properties this design rests on:

- A node at >= 101 is a full cluster citizen: QMGR failure protocol selects
  participants by ZRUNNING only (QmgrMain.cpp:5924-5930); arbitration's
  survivor mask counts it (computeArbitNdbMask, QmgrMain.cpp:8161-8187) so
  its node group stays represented; DBTC/SUMA/GCP failure paths have no
  SL_STARTED assumptions.
- Upgrade wall time stays ~ R x T (waves of <= 1 node per node group), not
  the G x R x T of a stop-blocking approach.
- MGM status path needs no changes: startPhase is Uint32 end-to-end
  (MgmtSrvr.cpp:3146-3149; the MAX_STARTPHASE ndbrequire is compiled out),
  and helm's healthcheck pattern-matches `*"starting"*` -> not ready.

## Stage 0 — Helm groundwork and interim mitigation (independent)

Repository: rondb-helm (lc_rondb_helm).

1. Fix the SIGTERM trap bug in `files/scripts/ndbmtds.sh`: bash delivers a
   trapped signal only after the foreground job exits, so with
   `ndbmtd --nodaemon ... | tee` in the foreground the `handle_sigterm`
   handler (MGM `deactivate`) never runs during pod termination —
   termination is effectively a 60 s wait followed by SIGKILL. Run the
   pipeline as a background job and `wait` on it so the trap fires.
2. Fix the `$!`-vs-`$?` bug in the livenessProbe MGMd guard
   (`templates/ndbd.yaml`): `NS_LOOKUP_EXIT_CODE=$!` reads the last
   background PID, not the nslookup exit code, so the "skip liveness when
   MGMd unresolvable" branch never triggers.
3. Interim mitigation until the kernel barrier ships: recommend
   `ndbmtdSequencedRollout.enabled=true` on multi-node-group clusters
   (one node group unfrozen at a time, held while any group is unhealthy).

Exit criteria: SIGTERM demonstrably produces a graceful MGM-driven stop
within the grace period on a live pod.

## Stage 1 — Kernel: the phase-110 barrier

### 1a. New start phase and recovered state

- Register NDBCNTR for STTOR phase 110 (its STTORRY phase list,
  NdbcntrMain.cpp:4127-4136); Missra dispatches it after SUMA's 101.
- Barrier applies to ST_NODE_RESTART / ST_INITIAL_NODE_RESTART only
  (system restart / initial start are already synchronized by `wait_sp`).
- Define a "recovered" node set in Ndbcntr (e.g. `c_recoveredNodeSet`)
  alongside the existing `c_cntr_startedNodeSet`/`c_startedNodeSet` split
  (Ndbcntr.hpp:445-454). Populate it by broadcasting a barrier-entry report
  to ALL NDBCNTRs, not only the master, so every node can classify peers
  (needed by Stage 2 for the tStarting/tStarted classification and by
  QMGR's is_node_started view).

### 1b. Barrier protocol (Ndbcntr master coordinated)

- New CNTR_WAITREP request type (`WaitRestartCompleted`), modeled on the
  `wait_sp` machinery (NdbcntrMain.cpp:3303-3389) but generalized:
  `wait_sp` is SR/initial-only (:3306-3315) and computes its minimum over
  `c_start.m_starting` only (:3351-3359).
- Release condition: every node N with `is_node_starting(N)` true
  (`c_cntr_startedNodeSet && !c_startedNodeSet`, NdbcntrMain.cpp:6938-6947)
  has reported barrier phase >= 110.
  - NOT `c_start.m_starting`: nodes leave it already at START_PERMREP
    (NdbcntrMain.cpp:2188-2189), early in the restart.
  - NOT any MGM-status formulation ("all nodes started or not started"):
    (a) two concurrent restarters in different node groups would deadlock at
    110, each seeing the other as "starting"; (b) a same-NG waiter queued in
    `c_start.m_waiting` (held by is_nodegroup_starting,
    NdbcntrMain.cpp:2422-2437) reports SL_STARTING phase ~1-2 indefinitely
    and must not hold the barrier — `is_node_starting()` is false for it.
- Re-evaluate the release condition on: each barrier report, each
  CNTR_START_REP, and in `execNODE_FAILREP` after the start-bookkeeping
  pruning (NdbcntrMain.cpp:3529-3537). Nothing re-evaluates there today
  (startWaitingNodes is only invoked from CNTR_START_REQ / START_PERMREP);
  without the hook, nodes parked behind a now-dead starter stay parked
  forever. A dead starting node releasing the barrier is also the desired
  "failure as opportunity" semantics.
- Master failover: parked nodes re-send the barrier report to the new
  master; a taking-over master rebuilds the wait set. (`wait_sp` has no
  such handling; must be added.)
- Version guard: new capability in ndb_version.h.in (precedent:
  `ndbd_send_started_bitmask`, :588). Under an old master, fail open —
  proceed straight to SL_STARTED as today.

### 1c. Fail-safes

- New config parameter (e.g. `RestartBarrierTimeout`, 0 = wait forever):
  on expiry the parked node proceeds to SL_STARTED unilaterally with an
  infoEvent. Backstop against a crash-looping node group freezing all
  rollouts cluster-wide.
- Exempt the parked period from `StartFailureTimeout` (ZSTARTUP check,
  NdbcntrMain.cpp:219-251): restamp `m_startTime` at barrier entry or skip
  the check at phase >= 110.
- New log events: entering barrier / released / timed out, so orchestration
  can distinguish "held at barrier" from "stuck restarting".

## Stage 2 — Kernel: survive node failures while parked

Six sites plus one rerouting, all conditioned on the recovered state
(phase >= 110 / `c_recoveredNodeSet`):

1. Three QMGR kill sites: `execDISCONNECT_REP` (QmgrMain.cpp:4942),
   `failReportLab` (:5874), `execPREP_FAILREQ` (:6030). QMGR sees the phase
   via NODE_STATE_REP, so the condition is locally available.
2. Three progError branches in `Ndbcntr::execNODE_FAILREP`
   (NdbcntrMain.cpp:3547-3562): tMasterFailed, tStarting, tStarted —
   including the starting-node-failed branch, which is exactly the case
   that should shrink the barrier set instead of killing parked nodes.
3. Reroute, do not merely de-crash: a parked node must take the full
   started-node NODE_FAILREP handling path (forwarding to QMGR/DBDIH/all
   blocks, :3577-3594), not the SL_STARTING short-circuit that only replies
   NDB_FAILCONF (:3564-3572).
4. `Dbdih::execNODE_FAILREP` master-takeover branch:
   `progError(NDBD_EXIT_MASTER_FAILURE_DURING_NR)` fires while
   `getNodeRestartInProgress()` (DbdihMain.cpp:9534-9536), still true at
   110. Without this, a master failure still kills parked nodes.
5. SUMA spurious-STTORRY hazard: the phase-101 completion condition
   (Suma.cpp:5614-5620: switchover buckets clear && SL_STARTING && handover
   nodes clear -> sendSTTORRY) remains armed at 110; a bucket switchover
   completing while parked (e.g. a peer's graceful stop) would emit a
   second STTORRY and corrupt Missra sequencing. Guard with a
   "STTORRY sent" flag.
6. Count recovered nodes as started in `Ndbcntr::is_node_started()`
   (NdbcntrMain.cpp:6949-6957). Fixes in one place:
   - the arbitration asymmetry: a parked node is in survivorNodes
     (ZRUNNING) but not in count_previously_alive_nodes
     (QmgrMain.cpp:8130-8158), skewing the partitioning majority test
     `2 x survivors vs prev_alive` toward winning without the arbitrator;
   - the tStarting/tStarted classification in Ndbcntr's failure handling.
7. Master eligibility decision: a parked node can become QMGR president /
   DIH master / DICT master (new president = min dynamic id among ZRUNNING,
   no started-ness filter, QmgrMain.cpp:6805-6825). No explicit gates block
   an SL_STARTING master, but the path is unreachable today (the node dies
   first at the sites above). Decision: accept and test (with item 6 the
   node is semantically started), rather than adding untested exclusion
   logic. Gets dedicated test coverage in Stage 3.

Exit criteria: graceful stop and hard kill of a started node while another
node is parked at 110 -> parked node survives, participates in failure
handling, and is released or keeps waiting per the barrier condition.

## Stage 3 — Test development (interleaved with Stages 1-2, gates them)

- ERROR_INSERT to stall a node at phase 110 (precedent: SUMA 13053 stalls
  phase 101, Suma.cpp:512-519) — the workhorse for the scenarios below.
- Test cases (testNodeRestart / MTR):
  1. Two node groups restarting concurrently; both park; joint release.
  2. Starting node (phase < 110) crashes while others parked -> release.
  3. Started node gracefully stopped / killed while a node is parked ->
     survival; arbitration outcome correct (node group still represented).
  4. President/DIH-master failure while a node is parked -> master takeover
     paths (LCP/GCP/DICT) with a recovered-but-starting node present, and
     the parked node itself becoming master.
  5. SUMA bucket switchover (peer graceful stop) while parked -> no
     spurious STTORRY.
  6. Barrier timeout expiry -> fail-open with event.
  7. Mixed-version rolling upgrade (old master -> barrier skipped).
- Full testNodeRestart regression: the SL_STARTING failure-handling changes
  touch the most sensitive code in the kernel; this is where the schedule
  risk lives.

## Stage 4 — Helm integration and rollout

- `healthcheck.sh` needs no change (matches `*"starting"*` -> not ready).
- Confirm probe budgets: the parked wait is bounded by the slowest
  concurrent restarter; `ndbmtdStartupProbe` (default 120 min) and the
  sequenced-rollout stall timeout may need raising, or set
  RestartBarrierTimeout below them.
- Surface the new barrier events in logs/docs so operators can distinguish
  "held at barrier" from "stuck".
- Update rondb-helm docs/data_node_upgrade_ordering.md: with the kernel
  barrier, default (non-sequenced) cross-group rollouts become safe against
  restart collisions; sequenced rollout remains for blast-radius
  containment.

## Stage 5 — Safety net for non-rollout stops (optional, recommended)

Readiness gating only covers rollouts; drains, liveness kills and manual
stops are not gated by another pod's readiness.

- Extend `Dbdih::execSTOP_PERM_REQ` (DbdihMain.cpp:27637-27724) to refuse
  (`NodeStartInProgress`) while any node is actively recovering below
  phase 110 — today the interlock (`c_nodeStartMaster.activeState`) ends at
  START_MECONF (DbdihMain.cpp:4337), before the long recovery phase. Never
  block on queued waiters. Ndbcntr's existing 100 ms retry on
  STOP_PERM_REF (NdbcntrMain.cpp:4611-4618) provides the delay behavior;
  move the wait ahead of the SL_STOPPING_1 transition so a delayed node
  keeps serving.
- Make QMGR's hardcoded 27 s graceful-shutdown deadline
  (QmgrMain.cpp:3252-3258, Qmgr.hpp:878-879) a config parameter so
  SIGTERM-initiated stops can legitimately wait; keep `stop -a`/`-f`
  unconditional-bypass semantics.
- Helm: preStop hook that waits while any node is below phase 110, with a
  matching terminationGracePeriodSeconds.

## Stage 6 — API/TC enablement at 110 (follow-up, after 1-4 are stable)

A node parked at 110 is a full replica but invisible to NDB API clients;
redundancy (not API capacity) is what makes the next pod kill safe, so this
stage is optional capacity recovery, not correctness.

- DBTC: accept remote TCSEIZEREQ when SL_STARTING && startPhase >= 110 —
  one condition at DbtcMain.cpp:2312-2313 (today refused with error 203;
  DBTC's node state is fresh via NODE_STATE_REP at every phase boundary).
- NDB API: extend the ClusterMgr alive test (ClusterMgr.cpp:1134-1136) the
  same way. API_REGCONF already carries the full NodeState including
  starting.startPhase (ApiRegSignalData.hpp:99-107), and APIs are already
  connected-but-idle at 110 (communication enabled from phase >= 8,
  QmgrMain.cpp:5337-5360). Old clients degrade gracefully: they keep
  treating the node as unavailable. Version-guard client-side only.
- MGM status intentionally unchanged — "starting, phase 110" remains the
  Kubernetes not-ready signal.

## Dependencies and sizing

Stage 0 independent. Critical path: 1 -> 2 -> 4, with Stage 3 interleaved
as the gate for each. Stage 5 depends only on the recovered-state
definition from Stage 1 and can run in parallel with Stage 4. Stage 6
strictly follows production hardening of 1-4.

Sizing: Stages 0 and 6 small; Stage 1 medium; Stages 2-3 large (node
failure handling changes plus previously-unreachable "recovered-but-
starting master" territory).
