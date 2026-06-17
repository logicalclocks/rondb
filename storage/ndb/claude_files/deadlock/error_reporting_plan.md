# RONDB-1062 — Deadlock error enrichment (plan)

Status: **Phases A + B DONE (2026-06).** Resume at **Phase C** (API receive + cache +
accessors).

**Decisions locked (2026-06):** transport = **Option B** (new version-gated signal
`GSN_TC_DEADLOCK_REP`); API exposure = **additive accessors** (no new `NdbError`
members, no `details`-string hack).

## Phase A — DONE (DBACC→DBTC contended table id; no API change)

The contended table now flows from the lock manager to the collector and is stored
per direction, ready for Phase B to assemble on a detected cycle:

- **`signaldata/DeadlockWaitfor.hpp`** — added `Uint32 contendedTableId`;
  `SignalLength` 10 → 11.
- **DBACC** (`DbaccMain.cpp`) — `accIsLockedLab` captures `fragrecptr.p->myTableId`
  into a local under the fragment mutex (`dl_table`) and passes it to
  `send_deadlock_waitfor(signal, waiter, owner, contendedTableId)`, which sets
  `rep->contendedTableId` and logs it. Decl updated in `Dbacc.hpp`.
- **DBTC** (`DbtcMain.cpp`) — `execDBACC_WAITFOR_REP` reads `rep->contendedTableId`
  **gated on `signal->getLength() >= SignalLength`** (rolling-upgrade safe: an old
  DBACC sends the 10-word signal → table id treated as `RNIL`). It passes the id to
  `recordDeadlockEdge(..., contendedTableId)`.
- **`DeadlockEdge`** (`Dbtc.hpp`) — added `tableIdWaitsOn` / `tableIdWaitedBy`
  (one per direction, since the two directions of a 2-cycle may contend on different
  tables). `recordDeadlockEdge` stores into the direction-matching field, clears both
  on freshness expiry, and logs both. (The slot-init loops still reset only
  `direction`, the validity gate — consistent with existing code; the table ids are
  fully (re)written when a slot is claimed.)

Verify with `DEB_DEADLOCK` logs: the `send`, `recv`, and `edge` lines now carry
`contendedTable=` / `tables waits_on=.. waited_by=..`.

## Phase B — DONE (DBTC assemble + transport to API; benign API stub)

On a detected cycle DBTC now assembles the detail and sends the new version-gated
signal to the victim's API node, just before the abort. The visible error (266/296)
is unchanged.

- **`recordDeadlockEdge` now returns the slot index** (was `bool`). The caller reads
  the slot's `direction` to test the 2-cycle (`== WAITS_ON|WAITED_BY`) and reads the
  stored `tableIdWaitsOn`/`tableIdWaitedBy`/`victimOpRef` from it. Decl + def + the one
  call site updated; the now-unused local `bothDirs` was removed from the function.
- **Victim op handle.** `DeadlockEdge` gained `victimOpRef` — the collector's own
  deadlocking op as the API operation pointer (`TcConnectRecord::clientData`). It is
  captured in `execDBACC_WAITFOR_REP` (key-op branch: `localTcPtr.p->clientData` →
  `collectorClientData`) and stored **only on the WAITS_ON direction** (where the
  collector is the waiter), so it survives even when the cycle is closed by the
  WAITED_BY report. RNIL for a scan/takeover victim (refinement deferred — see open Q2).
- **New signal `GSN_TC_DEADLOCK_REP` (980)** — `signaldata/TcDeadlockRep.hpp`
  (`apiConnectPtr, transId[2], deadlockReason{RealDeadlock bit}, tableId1, tableId2,
  victimOpRef`, `SignalLength=7`). Registered in `GlobalSignalNumbers.h` (define + bump
  `MAX_GSN` 979→980) and `SignalNames.cpp`. No printer (consistent with
  `DBACC_WAITFOR_REP`).
- **Version gate** `NDBD_DEADLOCK_DETAIL_VERSION = NDB_MAKE_VERSION(26,4,1)` +
  `ndbd_deadlock_detail_supported()` in `ndb_version.h.in` (this build's own version,
  so a same-build API receives it; older APIs never do).
- **`Dbtc::sendDeadlockDetailRep`** (DbtcMain.cpp) — gates on
  `getNodeInfo(apiNode).getType()==API && ndbd_deadlock_detail_supported(version)`,
  fills the signal (`apiConnectPtr = ndbapiConnect`, transid for validation), logs, and
  `sendSignal(ndbapiBlockref, GSN_TC_DEADLOCK_REP, …)`. Reuses `signal->theData` (the
  incoming DBACC_WAITFOR_REP is fully consumed; the following abort rebuilds it).
- **API stub** — `Ndbif.cpp trp_deliver_signal` has a `case GSN_TC_DEADLOCK_REP: break;`
  so a same-version API **silently ignores** it (no `InvalidSignal` warning / drop).
  Phase C replaces the stub with caching + accessors.

Verify with DBTC `DEB_DEADLOCK` logs: a detected cycle now logs
`send TC_DEADLOCK_REP to api … tables(t1,t2) victimOp=…` (or `skip … unsupported`
when the API is too old) immediately before the abort line.

**Remaining for Phase C:** make the API actually use the report (cache on
`NdbTransaction`, validate by transid, clear on reset/close) and expose it via the
additive accessors `wasDeadlock()` / `getDeadlockTableIds(out[2])` /
`getDeadlockOperation()` (resolve `victimOpRef`/`apiConnectPtr` back through
`void2con`). The signal carries `apiConnectPtr == ApiConnectRecord::ndbapiConnect`, so
the API resolves the txn the same way other TC→API signals do.

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

- **A. DONE.** Carry `contendedTableId` DBACC→DBTC (extended `DeadlockWaitforRep`; stored
  per direction in `DeadlockEdge` as `tableIdWaitsOn`/`tableIdWaitedBy`). Verified with
  `DEB_DEADLOCK` logs; no API change. See the "Phase A — DONE" section above.
- **B. DONE.** In DBTC `execDBACC_WAITFOR_REP`, on a detected cycle read the two table
  ids + victim op back from the cycle slot and send `GSN_TC_DEADLOCK_REP` (version-gated)
  to the API node just before the abort. `recordDeadlockEdge` now **returns the slot
  index**; `DeadlockEdge` gained `victimOpRef` (captured on WAITS_ON). New signal/GSN/
  version defined; API has a benign ignore stub. See the "Phase B — DONE" section above.
- **C.** API receive + expose. Replace the `Ndbif.cpp` stub (`case GSN_TC_DEADLOCK_REP`)
  with: resolve the txn (`void2con(rep->apiConnectPtr)`, validate `transId[2]`), cache
  `{reason, tableId1, tableId2, victimOpRef}` on `NdbTransaction`, and clear it on
  txn reset/close. Add the additive accessors (next section). Dedup tables when equal;
  resolve `victimOpRef` to an `NdbOperation*` (validate it belongs to the txn).
- **D.** ha_ndbcluster (mysqld): surface in the `ER_LOCK_WAIT_TIMEOUT` warning/diagnostic
  (map tableId→name) — optional.
- **E.** Tests: extend `ndb_deadlock_*` and `testDeadlock` to assert the real-deadlock flag,
  correct table ids, and victim-op identification; for the mysqld path check SHOW WARNINGS.

## Open questions

1. ~~Tables: just the two contended tables, or also fragment/row?~~ DONE: the signal
   carries the two contended table ids only (`tableId1`/`tableId2`); Phase C dedups when
   equal. Fragment/row not carried.
2. Scan victim: `getDeadlockOperation()` returns nullptr (no single key op); is reporting the
   `NdbScanOperation` worth it? Takeover victim → report the buddy key op (its `clientData`).
   **Phase B leaves `victimOpRef = RNIL` for scan/takeover victims** (only the plain key-op
   waiter captures it); revisit in Phase C/E if scan/takeover op reporting is wanted.
3. mysqld: does ha_ndbcluster want table *names* in the warning (Phase D)?
4. The `wasDeadlock()` flag is set only on the **proactive** path (a `GSN_TC_DEADLOCK_REP`
   arrived); the plain timeout backstop does not set it — confirm that's the desired semantics.
5. ~~`NDBD_DEADLOCK_DETAIL_VERSION` value~~ DONE: `NDB_MAKE_VERSION(26,4,1)` (this build's
   own version). If Phase C ships in a later build, bump to that build's version so the gate
   matches the build that first understands the signal.

## Resume note

Phases A + B implemented. **Resume at Phase C** (API side): replace the benign
`case GSN_TC_DEADLOCK_REP` stub in `Ndbif.cpp::trp_deliver_signal` with txn resolution +
caching on `NdbTransaction`, and add the additive accessors. The kernel already sends the
fully-populated signal on every detected cycle (gated on the API node version). The
detection/abort machinery this builds on is described in `design.md` (sections 14–16).
