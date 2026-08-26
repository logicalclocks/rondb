# RONDB-1096 follow-up: restart barrier under an old NDBCNTR master

Plan for making the start-phase-110 restart barrier effective during a
rolling upgrade from a pre-barrier release (e.g. 25.10.16/17 -> 25.10.19,
or 25.10.16 -> 26.2.10), where the NDBCNTR master does not support
the barrier but the restarting nodes do.

Version landing: the base barrier (RONDB-1096, Grant-only, all-members
gate) ships in 25.10.18 and 26.2.9; this mixed-version support ships
in 26.2.10 (and the next 25.10 release). The base capability gate
`ndbd_restart_phase_110_barrier()` stays at 25.10.18 / 26.2.9 — the
mixed-version support needs no capability gate of its own, since all
of its behaviors are local decisions of the restarting node and every
peer interaction it relies on (waitpoint understanding, master Grant,
successor-master competence after failover) is the base capability.
26.2.10 matters operationally, not in the kernel protocol: an upgrade
from a pre-barrier release is barrier-protected only when the target
carries the mixed-version support (25.10.16 -> 26.2.10 is smooth,
25.10.16 -> 26.2.9 is not — its restarters fail open in a mixed
cluster; 25.10.18 -> 26.2.9 is smooth, all members barrier-capable).
Orchestration that decides sequenced-vs-parallel rollouts by version
must therefore key on 26.2.10, while the helm
`rondb.supportsRestartBarrier` config helper stays at 25.10.18 /
26.2.9.

Parent feature: RONDB-1096 (`rondb_1096_restart_barrier_plan.md`).
Ticket: RONDB-XXXX (assign before first commit).

## 1. Problem statement

The barrier today requires *every* data-node member to pass
`ndbd_restart_phase_110_barrier()` (`handle_start_phase_110`,
NdbcntrMain.cpp:3557-3579); otherwise the starting node fails open and
completes its start immediately, as before RONDB-1096.

During a rolling upgrade the NDBCNTR master/president is always the
longest-lived node, so it stays on the old version until the last old
node restarts. No restart ordering avoids this: restarting the master
just hands mastership to the next-oldest (still old) node. Consequence:
**the barrier is inert for the entire upgrade from a pre-barrier
release — which is exactly the cross-node-group rollout-collision
window the feature was built for.** It only protects restarts that
happen after the cluster is homogeneous.

## 2. Why the whole-cluster gate exists today (dependency inventory)

1. **Old nodes crash on the new waitpoint.** The barrier-entry report
   is `CNTR_WAITREP(ZWAITPOINT_RESTART_BARRIER)` broadcast to all of
   `c_clusterNodes` (NdbcntrMain.cpp:3601-3607). A pre-barrier
   `execCNTR_WAITREP` takes `default: systemErrorLab` (the switch
   equivalent of :3758-3761) — the receiver dies.
2. **The master owns the release.** Only the master evaluates
   `check_restart_barrier()` (:3637-3675, early-return for non-master
   at :3638) and sends the `Grant`. An old master never releases
   anyone.
3. **The release condition uses master-local state.**
   `is_node_restarting()` (:7361-7386) excludes nodes queued in
   `c_start.m_waiting` (set only on the master in `execCNTR_START_REQ`,
   :2340) so that a queued same-node-group waiter does not deadlock the
   barrier.
4. **Old peers were not hardened.** The Stage-2 survive-failures work
   and the live-debugging fixes (DICT `dict_lock_unlock`
   NotInLockQueue no-op, DIH `setNodeRecoveryStatus` ordering, QMGR
   arbitration counting via `is_node_started()`) exist only in new
   binaries; a long-lived recovered-but-STARTING node stresses those
   paths on old survivors.

Facts that make a redesign possible (verified in tree):

- `c_recoveredNodeSet` is already maintained **on every barrier-capable
  node**: set on each `WaitFor` broadcast (:3619), cleared on
  `CM_ADD_REP` when a node begins a new restart (:2046), pruned on
  `NODE_FAILREP` (:3837). This was done for master takeover and is the
  foundation for peer evaluation.
- `CNTR_START_REP` is broadcast by **all versions** to all NDBCNTRs on
  start completion (:5749-5751, pre-existing upstream protocol), and
  new nodes track it in `c_startedNodeSet` on every node (:2223,
  pruned at :3836). So a parked node can detect completion of an *old*
  restarter without any protocol change.
- `check_restart_barrier` already treats "restarting but never
  reported" as barrier-holding (:3648-3654), i.e. old restarters
  degrade to holding the barrier until their `CNTR_START_REP`.
- A `Grant` arriving after the barrier was already left is tolerated
  (:3628-3631).

## 3. Design decision

**Peer-coordinated release, master Grant retained.** The barrier
becomes a protocol among the barrier-capable nodes only:

- The `WaitFor` broadcast goes only to barrier-capable members
  (per-receiver filter on `ndbd_restart_phase_110_barrier()`), never
  to old nodes. Removes dependency (1).
- Each **parked node evaluates the release condition itself** over
  `is_node_restarting()` / `c_recoveredNodeSet` and releases itself
  when no node is below the barrier. Removes dependency (2). The
  master `Grant` path is kept unchanged for 25.10.18 / 26.2.9 parked
  peers — the Grant-only barrier versions (they only ever park when
  all members are barrier-capable, and then a capable master exists
  to grant them) — and because a master's evaluation is sharper (it
  has `m_waiting`).
- Self-evaluation without `m_waiting` is strictly conservative: it can
  only over-wait (treat a queued waiter as restarting), never
  under-wait. The only affected scenario is a *same-node-group* second
  start while a node is parked **and** the master is old; that resolves
  via `RestartBarrierTimeout` fail-open. Dependency (3) is thereby
  accepted, not solved — closing it fully is deferred Stage 5.
- **Conservative mixed-mode failure policy** for dependency (4): while
  any old data node is a member, the parked node leaves the barrier
  (fail-open) on the **first `NODE_FAILREP`**. Healthy concurrent
  restarts — the motivating rollout-collision case — get full
  protection; the moment failure handling starts in a mixed cluster we
  revert to pre-barrier behavior instead of keeping a
  recovered-but-STARTING node alive for minutes against unhardened old
  failure paths. Once the member set is homogeneous-new, full Stage-2
  semantics (survive failures while parked) apply as today.
- **Exception: master failure with an old successor.** DBDIH already
  terminates a restarting node — recovered or not — when the
  successor master does not support the barrier
  (`Dbdih::execNODE_FAILREP`, `NDBD_EXIT_MASTER_FAILURE_DURING_NR`,
  DbdihMain.cpp:9591-9597, deliberate: an old master cannot handle a
  surviving restarting node, and the NodeRestartLock re-registration
  is gated on the successor). This is kept: in that case the parked
  node stays parked and awaits the DIH termination instead of racing
  it with a start completion; it restarts and completes on the next
  attempt. Exactly pre-barrier behavior for master failure. A welcome
  consequence: the DICT unlock-to-old-master hazard (§8) is
  unreachable — the node dies before any unlock could be sent.

  Blast radius, stated plainly: in a minimal 2-node-group cluster, an
  old-master failure while one node is parked and another restarts
  below the barrier kills three of four nodes (the killed master, the
  below-barrier restarter, the DIH-terminated parked node) and empties
  a node group — a full cluster shutdown. Observed live (2026-08-26
  MTR run against a real 25.10.16 master): node 2 first won
  arbitration with the parked node covering its group, then lost it
  ("missing node group") when the parked node terminated; the cluster
  system-restarted automatically under StopOnError=0. This is not a
  regression: pre-barrier 25.10.16 kills all concurrent restarters on
  any master failure, producing the identical outage; and once the
  successor is barrier-capable, the parked node survives and the
  cluster stays up — strictly better. It also validated the await
  branch and the old (ERROR_INSERT-built) successor surviving the
  takeover with no asserts.

A pleasant generalization falls out: a single new restarter in a mixed
cluster now also waits for concurrently restarting *old* nodes (their
`CNTR_START_REP` is version-independent), which the current code never
does. "All starting nodes are new" is not a precondition, just the
common upgrade case.

No new signal, request value, or capability bit is needed for Stages
1-4: the only signals sent are `WaitFor`/`Grant` under
`ZWAITPOINT_RESTART_BARRIER` (CntrStart.hpp:106,122), understood by
every barrier-capable version. 25.10.18 / 26.2.9 nodes interoperate
without change; they simply keep their stricter all-members gate for
their own parking decision.

### Interop matrix (park decision of a restarting node)

Legend: "base" = Grant-only barrier (25.10.18 / 26.2.9), "mixed" =
carries this change (26.2.10 / next 25.10), "old" = pre-barrier.

| Restarter | Master | Other members | Behavior |
|-----------|--------|---------------|----------|
| old       | any    | any           | never parks; holds barrier for parked peers until its CNTR_START_REP (existing semantics) |
| base      | old    | any           | fails open (its own all-members gate) — unchanged |
| base      | capable| all capable   | parks, released by master Grant — unchanged |
| mixed     | old    | some old      | **parks (new)**; WaitFor to capable members only; local release; fail-open on first NODE_FAILREP (but master failure with an old successor terminates the node in DIH); RestartBarrierTimeout backstop |
| mixed     | capable| all capable   | parks; master Grant (first) or self-release (equivalent), full failure survival |

## 4. Stage 1 — Kernel changes (all in Ndbcntr unless noted)

1. `handle_start_phase_110` (:3541-3608):
   - Replace the all-members fail-open (:3557-3579) with: compute
     `capableNodes = c_clusterNodes ∩ version-capable` (own node always
     in it), log the excluded old members if any (mixed mode), park
     regardless, and send the `WaitFor` broadcast to
     `NodeReceiverGroup(NDBCNTR, capableNodes)` instead of
     `c_clusterNodes`.
   - Keep the ST_NODE_RESTART / ST_INITIAL_NODE_RESTART filter
     (:3542-3555) unchanged.
2. `check_restart_barrier` (:3637-3675): before the master-only early
   return (:3638-3641), add self-release: if
   `m_restart_barrier_waiting && !is_any_node_below_restart_barrier()`
   (:7388-7398) then
   `leave_restart_barrier(signal, "all restarting nodes have recovered
   (self-evaluated)")`. All existing call sites (:2245, :2419, :2435,
   :3620, :4009) then cover the peer-evaluation triggers for free:
   WaitFor arrival, CNTR_START_REP arrival, NODE_FAILREP pruning.
   Note the parked-node-as-master edge (parent plan Stage 2 item 7)
   composes correctly: a parked master has `m_waiting` and evaluates
   sharply.
3. `execNODE_FAILREP` barrier hook (:3990-4010): replace the
   old-new-master special case (:3998-4007) with the mixed-mode
   policy: after the prune block, if `m_restart_barrier_waiting` and
   any remaining member of `c_clusterNodes` fails the version check,
   then (a) if the master failed and the successor is old, stay
   parked and await the DIH termination (§3 exception) — do not race
   it with a start completion; (b) otherwise
   `leave_restart_barrier(signal, "node failure in a mixed-version
   cluster")`. If the failed node was the *last* old member the
   cluster is homogeneous and the node correctly stays parked
   (`check_restart_barrier`).
4. `execCM_ADD_REP` (:2037-2058): keep the existing fail-open when an
   old node joins while parked (:2048-2057) — consistent with the
   mixed-mode failure policy (an old joiner implies an earlier old-node
   failure or a scale-out with an old binary; both are "revert to
   pre-barrier behavior" situations). Update the comment.
5. `ndb_version.h.in` (:593-627): the capability values are unchanged;
   rewrite the comment ("only used when the NDBCNTR master and the peer
   NDBCNTRs support it" no longer holds) to describe per-receiver
   gating and the mixed-mode policy.
6. Update the protocol block comment (:3450-3540) with the
   old-master flow and the mixed-mode failure policy; extend the DUMP
   71 state report (:4500 region) to print the capable/old member
   split so operators can see why a node did or did not park.
7. ~~Dbdih NodeRestartLock drop for an old successor~~ — not needed:
   the kill site at DbdihMain.cpp:9591-9597 terminates a restarting
   node (recovered included) whenever the successor master is old,
   before the DICT lock block is ever reached, so no
   `DICT_UNLOCK_ORD` can reach a pre-barrier master. No DIH change.

Explicit non-changes: `Grant` sending logic, `RestartBarrierTimeout`
(:231-257, DUMP 72 :4562), `is_node_restarting()`, STOP_PERM refusal
(DbdihMain.cpp:27781), TCSEIZEREQ/ClusterMgr serve-while-parked, helm
`RestartBarrierTimeout` version gate.

## 5. Stage 2 — Tests

1. New NDBT case `RestartBarrierSelfRelease` (testNodeRestart.cpp,
   alongside :10885-11300): homogeneous cluster, ERROR_INSERT on the
   master that suppresses `check_restart_barrier`'s Grant, verify a
   parked node self-releases once all restarters report. This
   exercises the new release path without needing two binaries.
2. New NDBT case `RestartBarrierMixedFailOpen` shape via ERROR_INSERT
   is *not* possible (version is real); mixed behavior is tested in
   MTR instead.
3. Rewrite `mysql-test/suite/ndb/t/ndb_restart_barrier_upgrade.test`:
   the current expectation ("old live data nodes disable the barrier")
   inverts. Scenarios, using the existing `have_ndbmtd_v2` old-binary
   infra and version-probe preamble:
   a. Two upgraded nodes in different node groups restart concurrently
      under the old master (one stalled below the barrier via DUMP 71,
      one parked at 110): the parked node holds, then both complete
      through the local release when the stall clears — the old
      master sends no Grant.
   b. ~~parked node waits for an old restarter~~ — not testable with
      this infra: all nodes start through one binary symlink, so any
      old node restarted after the relink comes up new; an old
      restarter cannot coexist with a parked new node in MTR. The
      hold-and-release mechanics for a never-reporting restarter are
      the same as for the stalled node in (a) (absence from
      c_recoveredNodeSet, release via CNTR_START_REP /
      NODE_FAILREP), which (a) and (c) cover.
   c. Kill an old started node while an upgraded node is parked:
      the parked node fails open and completes immediately; the
      stalled node below the barrier dies with the failure (expected
      2308, StopOnError=0 auto-restarts it); the old master survives.
      The killed old node comes back upgraded (binary link).
      Determinism: which old node (2 or 4) is master depends on the
      initial QMGR registration race, and killing the old *master*
      here instead triggers the die path and a full cluster outage
      (§3). The test therefore detects the master via `ndb_mgm -e
      show` and picks stall/park/kill roles so the kill always hits
      the old non-master, with each dead node's group covered by a
      survivor (the parked node covers the killed node's group, the
      old master covers the stalled node's group). The role node ids
      vary per run, so those phase waits use the `$_quiet_wait` mode
      added to ndb_wait_start_phase*.inc. The die path is not
      institutionalized in MTR — deliberately taking the whole
      cluster down and relying on the StopOnError=0 system restart
      asserts nothing useful — but it was validated live once (§3).
   d. Complete the upgrade, then verify full barrier semantics
      (existing tail of the test).
   The version probe now skips when the old binary is
   barrier-capable, mirroring ndbd_restart_phase_110_barrier()
   ((25.10.x, x>=18) or >= 26.2.9) — a 26.2.9 old binary would park
   in the fully-capable phases and hold through failures, breaking
   the mixed expectations.
4. Regression: all existing `RestartBarrier*` NDBT cases (:12171+) and
   MTR tests `ndb_restart_barrier`, `ndb_restart_barrier_failures`,
   `ndb_restart_barrier_dictlock` must stay green — the homogeneous
   Grant path is intentionally untouched. Full testNodeRestart sweep
   as in the parent plan (the NODE_FAILREP hook is touched again).
5. Manual/live: one real rolling upgrade 25.10.17 -> this version on a
   4-node 2-NG cluster (rondb-helm), observing park + release events
   in the logs during the cross-group rollout.

## 6. Stage 3 — Docs, helm, release notes

- rondb-helm `docs/data_node_upgrade_ordering.md`: mixed-version
  rollouts from pre-barrier releases are now barrier-protected when
  upgrading *to* >= this version; the sequenced-rollout recommendation
  for the upgrade itself can be relaxed accordingly.
- Helm chart: no gate change needed (`rondb.supportsRestartBarrier` /
  `RestartBarrierTimeout` semverCompare stays keyed at the original
  barrier versions; the parameter is honored by all parking versions).
- Release note: barrier now active during upgrades from pre-barrier
  releases; document the mixed-mode fail-open-on-failure policy and
  the same-NG-queue timeout caveat.

## 7. Stage 5 — Deferred: close the m_waiting hole

Only if the same-NG-queued-under-old-master timeout proves annoying in
practice: a new starting node broadcasts a "start pending" report to
barrier-capable peers after sending `CNTR_START_REQ` (cleared on
`CNTR_START_CONF`), and parked nodes exclude pending nodes from the
wait set — reconstructing `m_waiting` from the waiter's side.
**Requires a new `CntrWaitRep::Request` value, which a 25.10.18 /
26.2.9 `restart_barrier_rep` answers with `ndbabort()` (:3634)** — so
it needs a second capability (`ndbd_restart_barrier_v2`) and
per-receiver gating. Old queued waiters stay invisible either way
(timeout backstop remains). Not needed for the upgrade use case:
same-NG concurrent starts during a rollout imply a crash, and the
mixed-mode policy already fails open on failures.

A second deferred item: once fleets are homogeneous >= this version,
consider relaxing the fail-open-on-first-failure policy to
"fail open only while an old node remains a member" — which is what
the proposed condition already implements; no later change needed.

## 8. Residual risk (accepted, documented)

Unfixable-from-new-side old-code exposure, bounded by the mixed-mode
policy to the window between a failure event and the parked nodes'
fail-open completing (they are at phase 110; completion is
milliseconds of bookkeeping):

- QMGR arbitration counting asymmetry on an old president (parked node
  in survivors, not in previously-alive). Pre-existing in old releases
  for any node in phases 102-110 and for the whole restart window;
  the parked state widens the instantaneous exposure at the failure
  moment. Needs a simultaneous network partition to matter.
- DIH `setNodeRecoveryStatus` NODE_FAILURE_COMPLETED invariant
  (DbdihMain.cpp:5550): this is an `ndbrequire`, active in release
  builds — but it fires on the node that *itself* survives a failure
  while SL_STARTING and completes its start before that failure's
  handling completes (the NODE_FAILED half was skipped by the
  not-yet-started guard at :5443). Only barrier-capable nodes can
  survive a failure while starting, and every such binary carries the
  record-both-halves fix (commit d1d7eeb153b). Old nodes keep dying at
  their kill sites through phase 101, so their exposure stays the
  pre-existing millisecond window (phases 102-110) — unchanged by this
  feature. No mixed-version crash condition, provided the MTR test
  confirms the parked node records NODE_FAILED while parked
  (`track_failure_at_restart_barrier`).
- DICT NodeRestartLock unlock at an old successor master
  (`dict_lock_unlock` NotInLockQueue, `ndbassert(false)`):
  unreachable. The only path that could send it — a parked node
  surviving a master failure and completing under an old successor —
  is cut by the DIH kill site at DbdihMain.cpp:9591-9597, which
  terminates the node before the unlock exists to send (§3
  exception). Verified during implementation; no DIH change needed.
- Old DIH master lacks the STOP_PERM refusal below the barrier
  (Stage 5 of the parent plan): graceful-stop safety net absent in
  mixed mode; the helm preStop hook still covers rollout stops.
- Old API/MySQL nodes do not use a parked node as TC (capacity only).

## 9. Exit criteria

1. All Stage-2 tests green, including the rewritten upgrade MTR test
   against a real pre-barrier ndbmtd_v2 binary.
2. Full testNodeRestart regression green.
3. Live rolling upgrade on rondb-helm shows both parked nodes' barrier
   events during a cross-group rollout and a clean release, with the
   old master alive throughout.
4. MTR upgrade test scenario (c) passes with an ERROR_INSERT-built
   old ndbmtd_v2: the old master survives the failure handling with a
   node completing its start mid-handling, confirming the §8 analysis
   (no unlock reaches it, no old-side assert fires).
