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

## Key code anchors (verified against current tree)

- **DBACC wait-for detection point** — `storage/ndb/src/kernel/blocks/dbacc/DbaccMain.cpp:2806` (`placeWriteInLockQueue`), `:2914` (`placeReadInLockQueue`), `:3019` (`placeSerialQueue`). Both waiter (`operationRecPtr`) and blocker (`lockOwnerPtr`) are in scope.
- **ACC op → TC identity** — `Operationrec` carries `transId1/2`, `userptr` (LQH op idx), `userblockref` (LQH ref) at `Dbacc.hpp:777-783`; translate to TC via `c_lqh->get_tc_ref(userptr, tcOprec, tcBlockref)` (`Dblqh.hpp:6276`), already called under `DEB_LOCK_TRANS` in `DbaccMain.cpp:1553`.
- **DBTC abort + code retention** — `abortErrorLab` (`DbtcMain.cpp:10418`, retain guard `if (returncode==0)` at `:10438`); timeout victim path `timeOutFoundLab` (`:11333`, sets `returncode=266` at `:11362`).
- **Error code 266** — `ZTIME_OUT_ERROR` (`Dbtc.hpp:199`); ndberror table `ndberror.cpp:351`; class `TO`→`TemporaryError` → `HA_ERR_LOCK_WAIT_TIMEOUT`.
- **Config** — `TransactionDeadlockDetectionTimeout` / `CFG_DB_TRANSACTION_DEADLOCK_TIMEOUT` (`ConfigInfo.cpp:1109`), read in `DbtcMain.cpp:1485`.
- **New-signal checklist** — `GlobalSignalNumbers.h:40` (`MAX_GSN`), `SignalNames.cpp`, `signaldata/*.hpp`, `Dbtc.hpp`/`DbtcInit.cpp`/`DbtcMain.cpp`. Closest template: `CteScan.hpp` `CtePhaseCompleteRep` (DBSPJ→DBTC `_REP`).
