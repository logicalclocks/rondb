# RONDB-1062 — Deadlock error enrichment (plan)

Status: **PLAN ONLY — not started.** Resume here after compaction.

**Decisions locked (2026-06):** transport = **Option B** (new version-gated signal
`GSN_TC_DEADLOCK_REP`); API exposure = **additive accessors** (no new `NdbError`
members, no `details`-string hack). Phase A is the starting point.

## Goal

When a transaction is aborted as a *proactively detected* deadlock victim, let the
NDB API retrieve more about the error **without changing the externally visible
error code** (266 key-op / 296 scan → `HA_ERR_LOCK_WAIT_TIMEOUT` → 1205) and
without affecting existing retry logic. Add **optional** extra information:

1. **Real-deadlock indicator** — distinguish a detected cycle from a plain
   lock-wait timeout (both report 266/296 today).
2. **Tables involved** in the deadlock cycle.
3. **The deadlocking operation object** of the aborted victim, if available.

Hard constraints: primary code unchanged; old API ↔ new TC and new API ↔ old TC
both work (version/length gated); extra info is opt-in.

## Current error path (anchors, verified 2026-06; function names are the durable anchors, line numbers may drift)

- **Victim abort (set in `Dbtc::execDBACC_WAITFOR_REP`, DbtcMain.cpp ~11366–11535):**
  - key op / takeover buddy: `timeOutFoundLab(signal, apiConnectptr.i, ZTIME_OUT_ERROR=266)`
    → `returnsignal = RS_TCROLLBACKREP` (DbtcMain.cpp:10454) → GSN_TCROLLBACKREP sent at
    DbtcMain.cpp:20075–20087.
  - scan (batch, no buddy): `scanError(signal, scanptr, ZSCANTIME_OUT_ERROR=296)`
    → GSN_SCAN_TABREF (DbtcMain.cpp:6471–6479).
- **`TcRollbackRep`** (signaldata/TcRollbackRep.hpp): `connectPtr, transId[2], returnCode,
  errorData`; `SignalLength=5`. `errorData = apiConnectptr.p->errorData` (DbtcMain.cpp:20085);
  today used for indexId/fkId on constraint errors (9443/9498/9606), 0 for a deadlock.
- **`TcKeyRef`** (signaldata/TcKeyRef.hpp): `connectPtr` (= the API operation pointer),
  `transId[2], errorCode, errorData`; `SignalLength=5`. (Op-level error channel.)
- **API receive `NdbTransaction::receiveTCROLLBACKREP`** (NdbTransaction.cpp:2628):
  `theError.code = returnCode`; **iff `aSignal->getLength() == TcRollbackRep::SignalLength`**
  reads `theError.details = (char*)UintPtr(errorData)`. ⚠ the `==` check means naively
  growing TcRollbackRep silently drops `errorData` on old APIs — relax to `>=` carefully if
  extending.
- **`NdbError`** (ndbapi/NdbError.hpp): `classification, status, code, mysql_code,
  message, details(char*)`.
- **Edge signal `DeadlockWaitforRep`** (signaldata/DeadlockWaitfor.hpp): waiter/owner
  `{transId1,transId2,tcRef,tcOprec}` + `flags{CollectorIsWaiter,CollectorIsScan}`;
  `SignalLength=10`. **No table id yet.**
- **DBTC deadlock state:** `ApiConnectRecord::m_deadlock_edges[MAX_DEADLOCK_EDGES=4]`,
  `struct DeadlockEdge` (Dbtc.hpp:1300), `recordDeadlockEdge()` (Dbtc.hpp:2302 / def in
  DbtcMain.cpp ~11540), dirs `DLD_WAITS_ON=1 / DLD_WAITED_BY=2`.
- **Handles:** victim op's API pointer = `TcConnectRecord::clientData` ("SENDERS OPERATION
  POINTER", Dbtc.hpp:959); API node block ref = `ApiConnectRecord::ndbapiBlockref`
  (Dbtc.hpp:1170); txn API handle = `ApiConnectRecord::clientData` (Dbtc.hpp:1269).
- **ndberror:** 266/296 are class TO → TemporaryError → 1205 (ndberror.cpp). Keep as-is.

## Data to add per layer

1. **DBACC** (`accIsLockedLab` fan-out ~DbaccMain.cpp:2340; `describe_deadlock_endpoint`
   ~2123): the contended lock's table = `fragrecptr.p->myTableId`. Add **`contendedTableId`**
   to `DeadlockWaitforRep` (one per edge — the table the waiter and owner contend on; bump
   `SignalLength`). Optionally also fragId.
2. **DBTC** (`execDBACC_WAITFOR_REP` / `recordDeadlockEdge`): store `contendedTableId` (and
   the victim op handle) per recorded edge in `DeadlockEdge`. On cycle, the two matching
   edges (WAITS_ON + WAITED_BY for the same other-txn) give the **two involved tables**
   (dedupe if equal). The **victim deadlocking op** = the collector's waiting op in its
   WAITS_ON edge: for a key op that is `collectorTcOprec`'s `TcConnectRecord::clientData`;
   for a scan victim, report the scan (no single key op); for the takeover/buddy case the op
   is the buddy key op. Capture `clientData` at record time (store in the edge) since the
   triggering edge may be the other direction.
3. **Transport DBTC→API:** see options.
4. **NDB API:** receive + expose.

## Transport — DECIDED: Option B (new version-gated signal `GSN_TC_DEADLOCK_REP`)

DBTC sends `GSN_TC_DEADLOCK_REP` to the victim's API node (`ndbapiBlockref`)
*immediately before* the abort signal (TCROLLBACKREP / SCAN_TABREF), carrying:
`transId[2], deadlockCode (real-deadlock + reason bits), tableId1, tableId2,
victimApiOpRef (= the victim waiting op's `TcConnectRecord::clientData`; RNIL for a
pure scan victim)`.

- **Gate on the API node version** so old APIs never receive it:
  `getNodeInfo(refToNode(ndbapiBlockref)).m_version >= NDBD_DEADLOCK_DETAIL_VERSION`
  (define a new `NDBD_DEADLOCK_DETAIL_VERSION` in ndb_version.h). New TC ↔ old API: not
  sent. Old TC ↔ new API: never arrives — accessors return "no info". Both safe.
- **API side:** handle the new GSN (Ndbif.cpp dispatch), cache the payload on the matching
  `NdbTransaction` (validate by transid). The subsequent TCROLLBACKREP/SCAN_TABREF carrying
  266/296 is what the app observes; the cached detail is exposed via the accessors below.
  Clear the cache on transaction reset/close so it can't leak across reuse.
- **New-GSN checklist:** `GlobalSignalNumbers.h` (define GSN + bump `MAX_GSN`),
  `SignalNames.cpp`, new `signaldata/TcDeadlockRep.hpp` (+ a printer if other _REP have
  one), API dispatch in `Ndbif.cpp`, kernel send in `Dbtc` (DbtcInit registers no exec —
  it's send-only from TC to API). Closest template: the `DeadlockWaitforRep` addition (DBACC
  → DBTC) done earlier.

## API exposure — DECIDED: additive accessors (no new `NdbError` members)

Add to `NdbTransaction` (and keep `NdbError`/`details` untouched):
- `bool NdbTransaction::wasDeadlock() const` — true iff a `GSN_TC_DEADLOCK_REP` was received
  for this txn (the "real deadlock" indicator; absent on the plain timeout backstop).
- `int NdbTransaction::getDeadlockTableIds(Uint32 out[2]) const` — returns count (0/1/2),
  dedup'd; table ids (caller maps to names via the dictionary if desired).
- `const NdbOperation* NdbTransaction::getDeadlockOperation() const` — maps the carried
  `clientData` back to one of this txn's operations (validate it still belongs to the txn);
  returns `nullptr` for a pure scan victim or if not resolvable.
- Optionally a reason enum accessor if `deadlockCode` carries more than a single bit.

All additive (new methods only) → no ABI break, fully opt-in. Document that they are only
populated after a 266/296 abort from a *proactively detected* deadlock.

## Phasing

- **A.** Carry `contendedTableId` DBACC→DBTC (extend `DeadlockWaitforRep`; store in
  `DeadlockEdge`). Verify with `DEB_DEADLOCK` logs; no API change.
- **B.** In DBTC, on cycle assemble `{deadlockCode, table1, table2, victimOpRef}`; implement
  the chosen transport to the API node.
- **C.** API receive + expose (accessors and/or details).
- **D.** ha_ndbcluster (mysqld): surface in the `ER_LOCK_WAIT_TIMEOUT` warning/diagnostic
  (map tableId→name) — optional.
- **E.** Tests: extend `ndb_deadlock_*` and `testDeadlock` to assert the real-deadlock flag,
  correct table ids, and victim-op identification; for the mysqld path check SHOW WARNINGS.

## Open questions (transport + exposure now DECIDED — see above)

1. Tables: just the two contended tables, or also fragment/row? Dedupe when equal.
2. Scan victim: `getDeadlockOperation()` returns nullptr (no single key op); is reporting the
   `NdbScanOperation` worth it? Takeover victim → report the buddy key op (its `clientData`).
3. mysqld: does ha_ndbcluster want table *names* in the warning (Phase D)?
4. The `wasDeadlock()` flag is set only on the **proactive** path (a `GSN_TC_DEADLOCK_REP`
   arrived); the plain timeout backstop does not set it — confirm that's the desired semantics.
5. `NDBD_DEADLOCK_DETAIL_VERSION` value — pick when implementing Phase B/C.

## Resume note

Nothing implemented yet. Start at Phase A (DBACC→DBTC table id) — it is self-contained and
verifiable by logs before touching the API. The detection/abort machinery this builds on is
described in `design.md` (sections 14–16).
