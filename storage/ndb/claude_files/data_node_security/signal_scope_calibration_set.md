# Signal-Scope Calibration Set (ground truth)

Hand-verified, code-grounded SignalScope classifications used to validate the
automated GSN classification audit. Before the fan-out workflow is trusted on
the full GSN space, it is run **blind** against this set and must reproduce
these verdicts. Every entry here was traced to its actual send sites (all
`sendSignal` variants, `EXECUTE_DIRECT`, GSN-as-variable, and routed sends)
across the whole tree — including `src/ndbapi/` and `src/mgmsrv/` — not inferred
from names.

Status: 2026-07-23. Verified via 8-agent fan-out audit. **60 entries**
(39 Local + 6 Remote + 9 Management + 6 External), plus documented traps.

## Why this set exists

The pre-existing classifications (the "gold set") are all accurate but are
39 `Local` / 1 `Remote` / 0 `Management` / 0 `External`. They validate only the
easy "never on the wire" recognition. They do not exercise the wire-crossing
node-type distinctions (`Remote`/`Management`/`External`) where every dangerous
false-positive-disconnect error actually lives — especially the API boundary.
This calibration set adds verified coverage across all four scopes so passing it
means something for the decisions that matter.

## Decisive heuristics discovered while building this set

- **A literal send from `src/ndbapi/` ⇒ External.** Definitive: an API node
  physically sends it. Check `src/ndbapi/` first for any External candidate.
- **A send from `src/mgmsrv/` with no `src/ndbapi/` send ⇒ Management** (once you
  confirm a DB-node receiver).
- **A send with a destination ref built as `numberToRef(block, …, remoteNodeId)`
  (remote node) and no non-DB sender ⇒ Remote.** A signal only ever sent to
  node 0 / `getOwnNodeId()` / `reference()` is `Local`, not `Remote`.
- **`Management` is rare and error-prone.** Most "management-plane"-sounding
  signals collapse to `Remote` (DB-internal) or `External` (API-reachable). Treat
  any proposed `Management` with extra scrutiny.

## LOCAL (verified — 39 signals)

38 pre-existing `Local` classifications were audited and are all correct (high
confidence); `FSSYNCREQ` was the 39th, previously undeclared (defaulted to
External) and now fixed to `Local`. Instructive representatives:

| GSN | Why Local | Key evidence |
|---|---|---|
| GSN_CONTINUEB | canonical self-scheduled signal; all 467 send sites resolve to self/own-node | SignalData.hpp:500 |
| GSN_FSOPENREQ (+ all FS*) | NDBFS is strictly per-node; every request → local `NDBFS_REF` | FsOpenReq.hpp:241; Ndbfs.cpp userRef replies |
| GSN_STTOR / GSN_NDB_STTOR | block startup handshake, NDBCNTR → local blocks only | NdbSttor.hpp:69-70 |
| GSN_READ_CONFIG_REQ | NDBCNTR → same-node blocks | ReadConfig.hpp:45 |
| GSN_BUILDINDXREQ | internal DICT self-send (`sendSignal(reference(),…)`); NOT the API create-index (that is GSN_CREATE_INDX_REQ) | Dbdict.cpp:3673 |
| GSN_CREATE_FRAGMENTATION_REQ | only ever `EXECUTE_DIRECT` (same-node) | Dbdict.cpp:1374 |
| GSN_MALICIOUS_SIGNAL_REPORT | always to local QMGR_REF; offending nodeId is payload, not destination | SimulatedBlock.cpp:2492 |

Full Local membership: the FS family (FSOPEN/CLOSE/READ/WRITE/REMOVE/APPEND ×
REQ/CONF/REF, FSSYNCCONF/REF, FSSUSPENDORD), STTOR/STTORRY/NDB_STTOR/
NDB_STTORRY, READ_CONFIG_REQ/CONF, ALLOC_MEM_REQ/CONF/REF, BUILDINDXREQ/CONF/REF,
CREATE_FRAGMENTATION_REQ/CONF/REF, CONTINUEB, MALICIOUS_SIGNAL_REPORT.

## REMOTE (verified — 6 signals)

DB→DB over the wire, no API/MGM sender.

| GSN | Decisive remote-destination evidence |
|---|---|
| GSN_FAIL_REP | QMGR/NDBCNTR only; `calcQmgrBlockRef(remoteNode)` QmgrMain.cpp:1785; arbitrator never emits it. NOTE: was never actually scoped before — its `DECLARE_SIGNAL_SCOPE` was inside a doc comment (silently External); now a real `Remote` entry in the central table |
| GSN_LQHKEYREQ | DbtcMain.cpp:6045 to `numberToRef(DBLQH,…,nodeId)` with `refToNode != getOwnNodeId` (6039) |
| GSN_SCAN_FRAGREQ | DbtcMain.cpp:18968 remote branch (18935, `nodeId != getOwnNodeId`) |
| GSN_GCP_PREPARE | DbdihMain.cpp:401 `calcDihBlockRef(nodeId)`; sendLoopMacro over participants (17171 remote branch) |
| GSN_LCP_FRAG_ORD | DbdihMain.cpp:22255 `calcLqhBlockRef(replica procNode)` (remote) |
| GSN_COPY_FRAGREQ | DbdihMain.cpp:8451 `numberToRef(DBLQH,…,toCopyNode)` (remote source during takeover) |

## MANAGEMENT (verified — 9 signals)

MGM (and sometimes DB) senders, never API. Each was confirmed to have an MGM
send site, a DB-node exec handler, and **zero** `src/ndbapi/` senders. This
category proved rare and hard to find (see traps) — most management-sounding
signals are actually External or MGM↔MGM.

| GSN | Decisive evidence |
|---|---|
| GSN_START_ORD | MGM→DB wire send MgmtSrvr.cpp:1173; DB-internal sends also exist |
| GSN_STOP_REQ | MGM→DB MgmtSrvr.cpp:2029/2284/2512; DB→DB NdbcntrMain.cpp:4826 |
| GSN_CREATE_NODEGROUP_REQ | MGM-only sender MgmtSrvr.cpp:3512/3534 → Dbdict.cpp:24394; no DB sender at all |
| GSN_DROP_NODEGROUP_REQ | MGM-only MgmtSrvr.cpp:3609 → Dbdict.cpp:24933 |
| GSN_EVENT_SUBSCRIBE_REQ | MGM MgmtSrvr.cpp:3193/3232 (skips non-DB targets) → Cmvmi.cpp:620; cluster-log subscription |
| GSN_RESUME_REQ | MGM MgmtSrvr.cpp:3002/3011 (single-user exit) → NdbcntrMain.cpp:4215 |
| GSN_BACKUP_REQ | MGM MgmtSrvr.cpp:4994/5056 → Backup.cpp:4173; DB self-send for LCP (Backup.cpp:2787), no API |
| GSN_ALLOC_NODEID_REQ | MGM MgmtSrvr.cpp:4175 → QmgrMain.cpp:8666; DB→DB president broadcast (8764); API gets nodeid via mgmapi text protocol, not this GSN |
| GSN_ABORT_BACKUP_ORD | MGM MgmtSrvr.cpp:5189 → Backup.cpp:11043; heavy DB→DB use |

Note — `GSN_CONFIG_CHANGE_IMPL_REQ`/`_CONF`, `GSN_CONFIG_CHECK_REQ` are MGM↔MGM
(handled in `mgmsrv/ConfigManager.cpp`, never reach a DB node) → NOT Management
scope (no DB receiver). `GSN_GET_CONFIG_REQ` is ambiguous (API fetches config via
the mgmapi text protocol, not this GSN) → excluded to keep ground truth clean.

## EXTERNAL (verified — 6 signals)

An API node legitimately sends each (literal `src/ndbapi/` send site).

| GSN | Decisive ndbapi/ send site |
|---|---|
| GSN_TCKEYREQ | NdbOperationExec.cpp:208/210 (→ DBTC) |
| GSN_SCAN_TABREQ | NdbScanOperation.cpp:2836/2858 (→ DBTC) |
| GSN_CREATE_INDX_REQ | NdbDictionaryImpl.cpp:5623 (dictSignal → DBDICT) |
| GSN_API_REGREQ | ClusterMgr.cpp:492 (raw_sendSignal → QMGR on DB node) |
| GSN_SUB_START_REQ | NdbDictionaryImpl.cpp:6256 (dictSignal → DBDICT → SUMA) |
| GSN_DUMP_STATE_ORD | Ndbif.cpp:1890 via ndb_internal.cpp:43; CMVMI handler has no sender check |

## Traps and divergences (the highest-value calibration data)

These are cases where the intuitive/name-based guess is WRONG. A trustworthy
audit method must get these right.

| Item | Intuition | Truth | Lesson |
|---|---|---|---|
| GSN_DUMP_STATE_ORD | Management (mgm DUMP command) | **External** — NdbApi clients send it via `ndb_internal.cpp`; handler has no sender check | Management-looking ops are often API-reachable → classify from send sites, never names. The dangerous direction: had this been enforced as Management, API diagnostics would be disconnected. |
| GSN_ENTER_SINGLE_USER_REQ | Management (single-user mode) | **Does not exist** — single-user rides on GSN_STOP_REQ with `singleuser=1` | The audit must iterate real GSN `#define`s, not conceptual operation names. |
| GSN_SET_LOGLEVELORD | Management | **Dead** — only `_v9_4_0` exists; zero senders; no-op handler | Deprecated/`_vX_Y_Z` GSNs need a defined disposition, not a live scope. |
| GSN_FSSYNCREQ | (assumed classified) | **Unclassified → defaults to External; should be Local** | An undeclared signal silently reads as "open to all." Motivates the `Unclassified` bookkeeping state so audited-open is distinguishable from never-looked-at. |
| EVENT_SUBSCRIBE_REQ vs SUB_START_REQ | both "event subscription" | **EVENT_SUBSCRIBE_REQ = Management** (cluster log → CMVMI, MGM-only); **SUB_START_REQ = External** (NdbApi event → SUMA) | Near-synonym names, opposite scopes. The receiver block (CMVMI vs SUMA) and the actual sender decide it — not the word "subscribe." |

## How to use this set

1. Run the fan-out audit **blind** on these GSNs (strip the known answers from the
   prompt).
2. Compare the workflow's proposed scope to the truth column.
3. **Gate:** any mismatch in the *restrictive/tightening* direction (proposing
   Local/Remote/Management for something that is truly External/Management, i.e. a
   would-be false-positive disconnect) is a blocking failure — fix the method
   before the full run. A mismatch in the loosening direction is a warning.
4. Pay special attention to whether the method reproduces the four traps above,
   especially DUMP_STATE_ORD.
