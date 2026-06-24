# CLAUDE.md — Deadlock Discovery (RONDB-1062)

Active design + implementation plan for **proactive deadlock discovery** in RonDB.

## Goal

Today RonDB only detects deadlocks *indirectly*, by letting a lock wait until the
DBTC transaction timeout (`TransactionDeadlockDetectionTimeout`) fires and aborts
the transaction with error **266** ("Time-out in NDB, probably caused by deadlock").
This is slow (timeout-bounded) and imprecise (it may abort a transaction that was
merely slow, not deadlocked).

We add **proactive wait-for-edge reporting**: the moment DBACC has to enqueue a lock
request behind a conflicting lock, it captures the wait-for relationship
(waiter transaction → lock-owner transaction) and forwards it via a **new signal to
DBTC**. A chosen "collector" transaction (selected deterministically by the smaller
**hash of the transaction id**) accumulates these edges and runs cycle detection; on a
detected cycle it aborts the same min-hash transaction as victim — a purely local action —
retaining the externally-visible abort code (266) so existing application retry logic is
unaffected.

Deadlock detection is NP-complete in the general distributed case, so this is a
**best-effort** mechanism: 2-cycles are always caught; longer cycles are caught
opportunistically (and via "send to both" / path-pushing extensions). The existing
timeout remains as a backstop.

## Documents

| File | Contents |
|------|----------|
| `design.md` | Full design + phased implementation plan + file-by-file change checklist + open questions. **Start here.** |
| `error_reporting_plan.md` | Plan (not started) to enrich the deadlock error reported to the NDB API: optional real-deadlock indicator, tables involved, and the aborted deadlocking operation — without changing the 266/296 error code. |

## Key code anchors (verified against current tree)

- **DBACC wait-for detection point** — `storage/ndb/src/kernel/blocks/dbacc/DbaccMain.cpp:2806` (`placeWriteInLockQueue`), `:2914` (`placeReadInLockQueue`), `:3019` (`placeSerialQueue`). Both waiter (`operationRecPtr`) and blocker (`lockOwnerPtr`) are in scope.
- **ACC op → TC identity** — `Operationrec` carries `transId1/2`, `userptr` (LQH op idx), `userblockref` (LQH ref) at `Dbacc.hpp:777-783`; translate to TC via `c_lqh->get_tc_ref(userptr, tcOprec, tcBlockref)` (`Dblqh.hpp:6276`), already called under `DEB_LOCK_TRANS` in `DbaccMain.cpp:1553`.
- **DBTC abort + code retention** — `abortErrorLab` (`DbtcMain.cpp:10418`, retain guard `if (returncode==0)` at `:10438`); timeout victim path `timeOutFoundLab` (`:11333`, sets `returncode=266` at `:11362`).
- **Error code 266** — `ZTIME_OUT_ERROR` (`Dbtc.hpp:199`); ndberror table `ndberror.cpp:351`; class `TO`→`TemporaryError` → `HA_ERR_LOCK_WAIT_TIMEOUT`.
- **Config** — `TransactionDeadlockDetectionTimeout` / `CFG_DB_TRANSACTION_DEADLOCK_TIMEOUT` (`ConfigInfo.cpp:1109`), read in `DbtcMain.cpp:1485`.
- **Config (on/off switch)** — `EnableProactiveDeadlockDetection` / `CFG_DB_ENABLE_PROACTIVE_DEADLOCK_DETECTION` (=705, `CI_BOOL`, **default false** in production; `ConfigInfo.cpp` after the deadlock-timeout entry). Read into `c_proactive_deadlock_detect` in **both** DBACC (`execREAD_CONFIG_REQ`; gates the `accIsLockedLab` edge capture) and DBTC (`DbtcMain.cpp` config read; early-returns in `execDBACC_WAITFOR_REP`). When false → only the timeout backstop resolves deadlocks (pre-RONDB-1062 behaviour). **MTR turns it ON globally** via `mysql-test/include/default_ndbd.cnf` (`EnableProactiveDeadlockDetection=1`) so the deadlock suite exercises it; `ndbinfo_cluster_locks.cnf` overrides it back to 0 (that test must hold a deadlock open to observe it).
- **Runtime on/off switch (DUMP)** — `DumpStateOrd::DeadlockDetection = 16000` (`DumpStateOrd.hpp`; < `OneBlockOnly` so CMVMI fans it to all blocks, and `LocalProxy` fans it to all worker instances). `ALL DUMP 16000 1` enables, `ALL DUMP 16000 0` disables, at runtime until the next (re)start. Handled in both `Dbtc::execDUMP_STATE_ORD` and `Dbacc::execDUMP_STATE_ORD` (each sets `c_proactive_deadlock_detect` and logs via `g_eventLogger`). Same mechanism as `DUMP 2507` (the deadlock-timeout knob).
- **MGM client online switch (SET)** — `[ALL|<id>] SET EnableProactiveDeadlockDetection <0|1>` (`CommandInterpreter::executeSetEnableProactiveDeadlockDetection`, modelled on `executeSetMaxDiskWriteSpeed`). Step 1 updates the saved config (`ndb_mgm_get_configuration`→`iter.set(CFG_DB_ENABLE_PROACTIVE_DEADLOCK_DETECTION)`→`ndb_mgm_set_configuration`; only DB-node sections, bounded by `ABS_MAX_NDB_NODES`, so (re)starting nodes pick it up). Step 2 toggles running nodes via the DUMP above (`ndb_mgm_dump_state` per running DB node — it targets one node at a time). For a specific node, the id must be `!= 0` and `<= ABS_MAX_NDB_NODES`.
- **New-signal checklist** — `GlobalSignalNumbers.h:40` (`MAX_GSN`), `SignalNames.cpp`, `signaldata/*.hpp`, `Dbtc.hpp`/`DbtcInit.cpp`/`DbtcMain.cpp`. Closest template: `CteScan.hpp` `CtePhaseCompleteRep` (DBSPJ→DBTC `_REP`).
