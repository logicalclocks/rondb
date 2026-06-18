/*
   Copyright (c) 2003, 2025, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include <NdbCondition.h>
#include <NdbMutex.h>
#include <NdbSleep.h>
#include <NdbThread.h>
#include <NdbTick.h>
#include <ndb_global.h>
#include <NdbApi.hpp>
#include <NdbOut.hpp>
#include <NdbRestarter.hpp>
#include <NdbTest.hpp>
#include "util/require.h"

struct Opt {
  bool m_dbg;
  const char *m_scan;
  const char *m_tname;
  const char *m_xname;
  Opt() : m_dbg(true), m_scan("tx"), m_tname("T"), m_xname("X") {}
};

static void printusage() {
  Opt d;
  ndbout << "usage: testDeadlock" << endl
         << "-scan txs       scan table (t), index (x), scan<->scan deadlock"
            " (s) [" << d.m_scan << "]" << endl;
}

static Opt g_opt;

static NdbMutex *ndbout_mutex = NULL;
static Ndb_cluster_connection *g_cluster_connection = 0;
#define DBG(x)                                         \
  do {                                                 \
    if (!g_opt.m_dbg) break;                           \
    NdbMutex_Lock(ndbout_mutex);                       \
    ndbout << "line " << __LINE__ << " " << x << endl; \
    NdbMutex_Unlock(ndbout_mutex);                     \
  } while (0)

#define CHK(x)                                                        \
  do {                                                                \
    if (x) break;                                                     \
    ndbout << "line " << __LINE__ << ": " << #x << " failed" << endl; \
    return -1;                                                        \
  } while (0)

#define CHN(p, x)                                                     \
  do {                                                                \
    if (x) break;                                                     \
    ndbout << "line " << __LINE__ << ": " << #x << " failed" << endl; \
    ndbout << (p)->getNdbError() << endl;                             \
    return -1;                                                        \
  } while (0)

// threads

typedef int (*Runstep)(struct Thr &thr);

struct Thr {
  enum State { Wait, Start, Stop, Stopped, Exit };
  State m_state;
  int m_no;
  Runstep m_runstep;
  int m_ret;
  NdbMutex *m_mutex;
  NdbCondition *m_cond;
  NdbThread *m_thread;
  void *m_status;
  Ndb *m_ndb;
  NdbConnection *m_con;
  NdbScanOperation *m_scanop;
  NdbIndexScanOperation *m_indexscanop;
  //
  Thr(int no);
  ~Thr();
  int run();
  void start(Runstep runstep);
  void stop();
  void stopped();
  void lock() { NdbMutex_Lock(m_mutex); }
  void unlock() { NdbMutex_Unlock(m_mutex); }
  void wait() { NdbCondition_Wait(m_cond, m_mutex); }
  void signal() { NdbCondition_Signal(m_cond); }
  void exit();
  void join() { NdbThread_WaitFor(m_thread, &m_status); }
};

static NdbOut &operator<<(NdbOut &out, const Thr &thr) {
  out << "thr " << thr.m_no;
  return out;
}

extern "C" {
static void *runthread(void *arg);
}

Thr::Thr(int no) {
  m_state = Wait;
  m_no = no;
  m_runstep = 0;
  m_ret = 0;
  m_mutex = NdbMutex_Create();
  m_cond = NdbCondition_Create();
  require(m_mutex != 0 && m_cond != 0);
  const unsigned stacksize = 256 * 1024;
  const NDB_THREAD_PRIO prio = NDB_THREAD_PRIO_LOW;
  m_thread = NdbThread_Create(runthread, (void **)this, stacksize, "me", prio);
  if (m_thread == 0) {
    DBG("create thread failed: errno=" << errno);
    m_ret = -1;
  }
  m_status = 0;
  m_ndb = 0;
  m_con = 0;
  m_scanop = 0;
  m_indexscanop = 0;
}

Thr::~Thr() {
  if (m_thread != 0) NdbThread_Destroy(&m_thread);
  if (m_cond != 0) NdbCondition_Destroy(m_cond);
  if (m_mutex != 0) NdbMutex_Destroy(m_mutex);
}

static void *runthread(void *arg) {
  Thr &thr = *(Thr *)arg;
  thr.run();
  return 0;
}

int Thr::run() {
  DBG(*this << " run");
  while (true) {
    lock();
    while (m_state != Start && m_state != Exit) {
      wait();
    }
    if (m_state == Exit) {
      DBG(*this << " exit");
      unlock();
      break;
    }
    m_ret = (*m_runstep)(*this);
    m_state = Stopped;
    signal();
    unlock();
    if (m_ret != 0) {
      DBG(*this << " error exit");
      break;
    }
  }
  delete m_ndb;
  m_ndb = 0;
  return 0;
}

void Thr::start(Runstep runstep) {
  lock();
  m_state = Start;
  m_runstep = runstep;
  signal();
  unlock();
}

void Thr::stopped() {
  lock();
  while (m_state != Stopped) {
    wait();
  }
  m_state = Wait;
  unlock();
}

void Thr::exit() {
  lock();
  m_state = Exit;
  signal();
  unlock();
}

// general

static int runstep_connect(Thr &thr) {
  Ndb *ndb = thr.m_ndb = new Ndb(g_cluster_connection, "TEST_DB");
  CHN(ndb, ndb->init() == 0);
  CHN(ndb, ndb->waitUntilReady() == 0);
  DBG(thr << " connected");
  return 0;
}

static int runstep_starttx(Thr &thr) {
  Ndb *ndb = thr.m_ndb;
  require(ndb != 0);
  CHN(ndb, (thr.m_con = ndb->startTransaction()) != 0);
  DBG("thr " << thr.m_no << " tx started");
  return 0;
}

/*
 * WL1822 flush locks
 *
 * Table T with 3 tuples X, Y, Z.
 * Two transactions (* = lock wait).
 *
 * - tx1 reads and locks Z
 * - tx2 scans X, Y, *Z
 * - tx2 returns X, Y before lock wait on Z
 * - tx1 reads and locks *X
 * - api asks for next tx2 result
 * - LQH unlocks X via ACC or TUX [*]
 * - tx1 gets lock on X
 * - tx1 returns X to api
 * - api commits tx1
 * - tx2 gets lock on Z
 * - tx2 returns Z to api
 *
 * The point is deadlock is avoided due to [*].
 * The test is for 1 db node and 1 fragment table.
 */

static char wl1822_scantx = 0;

static const Uint32 wl1822_valA[3] = {0, 1, 2};
static const Uint32 wl1822_valB[3] = {3, 4, 5};

static Uint32 wl1822_bufA = ~0;
static Uint32 wl1822_bufB = ~0;

// map scan row to key (A) and reverse
static unsigned wl1822_r2k[3] = {0, 0, 0};
static unsigned wl1822_k2r[3] = {0, 0, 0};

static int wl1822_createtable(Thr &thr) {
  Ndb *ndb = thr.m_ndb;
  require(ndb != 0);
  NdbDictionary::Dictionary *dic = ndb->getDictionary();
  // drop T
  if (dic->getTable(g_opt.m_tname) != 0)
    CHN(dic, dic->dropTable(g_opt.m_tname) == 0);
  // create T
  NdbDictionary::Table tab(g_opt.m_tname);
  tab.setFragmentType(NdbDictionary::Object::FragAllSmall);
  {
    NdbDictionary::Column col("A");
    col.setType(NdbDictionary::Column::Unsigned);
    col.setPrimaryKey(true);
    tab.addColumn(col);
  }
  {
    NdbDictionary::Column col("B");
    col.setType(NdbDictionary::Column::Unsigned);
    col.setPrimaryKey(false);
    tab.addColumn(col);
  }
  CHN(dic, dic->createTable(tab) == 0);
  // create X
  NdbDictionary::Index ind(g_opt.m_xname);
  ind.setTable(g_opt.m_tname);
  ind.setType(NdbDictionary::Index::OrderedIndex);
  ind.setLogging(false);
  ind.addColumn("B");
  CHN(dic, dic->createIndex(ind) == 0);
  DBG("created " << g_opt.m_tname << ", " << g_opt.m_xname);
  return 0;
}

static int wl1822_insertrows(Thr &thr) {
  // insert X, Y, Z
  Ndb *ndb = thr.m_ndb;
  require(ndb != 0);
  NdbConnection *con;
  NdbOperation *op;
  for (unsigned k = 0; k < 3; k++) {
    CHN(ndb, (con = ndb->startTransaction()) != 0);
    CHN(con, (op = con->getNdbOperation(g_opt.m_tname)) != 0);
    CHN(op, op->insertTuple() == 0);
    CHN(op, op->equal("A", (char *)&wl1822_valA[k]) == 0);
    CHN(op, op->setValue("B", (char *)&wl1822_valB[k]) == 0);
    CHN(con, con->execute(Commit) == 0);
    ndb->closeTransaction(con);
  }
  DBG("inserted X, Y, Z");
  return 0;
}

static int wl1822_getscanorder(Thr &thr) {
  // cheat, table order happens to be key order in my test
  wl1822_r2k[0] = 0;
  wl1822_r2k[1] = 1;
  wl1822_r2k[2] = 2;
  wl1822_k2r[0] = 0;
  wl1822_k2r[1] = 1;
  wl1822_k2r[2] = 2;
  DBG("scan order determined");
  return 0;
}

static int wl1822_tx1_readZ(Thr &thr) {
  // tx1 read Z with exclusive lock
  NdbConnection *con = thr.m_con;
  require(con != 0);
  NdbOperation *op;
  CHN(con, (op = con->getNdbOperation(g_opt.m_tname)) != 0);
  CHN(op, op->readTupleExclusive() == 0);
  CHN(op, op->equal("A", wl1822_valA[wl1822_r2k[2]]) == 0);
  wl1822_bufB = ~0;
  CHN(op, op->getValue("B", (char *)&wl1822_bufB) != 0);
  CHN(con, con->execute(NoCommit) == 0);
  CHK(wl1822_bufB == wl1822_valB[wl1822_r2k[2]]);
  DBG("tx1 locked Z");
  return 0;
}

static int wl1822_tx2_scanXY(Thr &thr) {
  // tx2 scan X, Y with exclusive lock
  NdbConnection *con = thr.m_con;
  require(con != 0);
  NdbScanOperation *scanop = nullptr;
  NdbIndexScanOperation *indexscanop;

  if (wl1822_scantx == 't') {
    CHN(con,
        (scanop = thr.m_scanop = con->getNdbScanOperation(g_opt.m_tname)) != 0);
    DBG("tx2 scan exclusive " << g_opt.m_tname);
  }
  if (wl1822_scantx == 'x') {
    CHN(con,
        (scanop = thr.m_scanop = indexscanop = thr.m_indexscanop =
             con->getNdbIndexScanOperation(g_opt.m_xname, g_opt.m_tname)) != 0);
    DBG("tx2 scan exclusive " << g_opt.m_xname);
  }
  CHN(scanop, scanop->readTuplesExclusive(16) == 0);
  CHN(scanop, scanop->getValue("A", (char *)&wl1822_bufA) != 0);
  CHN(scanop, scanop->getValue("B", (char *)&wl1822_bufB) != 0);
  CHN(con, con->execute(NoCommit) == 0);
  unsigned row = 0;
  while (row < 2) {
    DBG("before row " << row);
    int ret;
    wl1822_bufA = wl1822_bufB = ~0;
    CHN(con, (ret = scanop->nextResult(true)) == 0);
    DBG("got row " << row << " a=" << wl1822_bufA << " b=" << wl1822_bufB);
    CHK(wl1822_bufA == wl1822_valA[wl1822_r2k[row]]);
    CHK(wl1822_bufB == wl1822_valB[wl1822_r2k[row]]);
    row++;
  }
  return 0;
}

static int wl1822_tx1_readX_commit(Thr &thr) {
  // tx1 read X with exclusive lock and commit
  NdbConnection *con = thr.m_con;
  require(con != 0);
  NdbOperation *op;
  CHN(con, (op = con->getNdbOperation(g_opt.m_tname)) != 0);
  CHN(op, op->readTupleExclusive() == 0);
  CHN(op, op->equal("A", wl1822_valA[wl1822_r2k[2]]) == 0);
  wl1822_bufB = ~0;
  CHN(op, op->getValue("B", (char *)&wl1822_bufB) != 0);
  CHN(con, con->execute(NoCommit) == 0);
  CHK(wl1822_bufB == wl1822_valB[wl1822_r2k[2]]);
  DBG("tx1 locked X");
  CHN(con, con->execute(Commit) == 0);
  DBG("tx1 commit");
  return 0;
}

static int wl1822_tx2_scanZ_close(Thr &thr) {
  // tx2 scan Z with exclusive lock and close scan
  Ndb *ndb = thr.m_ndb;
  NdbConnection *con = thr.m_con;
  NdbScanOperation *scanop = thr.m_scanop;
  require(ndb != 0 && con != 0 && scanop != 0);
  unsigned row = 2;
  while (true) {
    DBG("before row " << row);
    int ret;
    wl1822_bufA = wl1822_bufB = ~0;
    CHN(con, (ret = scanop->nextResult(true)) == 0 || ret == 1);
    if (ret == 1) break;
    DBG("got row " << row << " a=" << wl1822_bufA << " b=" << wl1822_bufB);
    CHK(wl1822_bufA == wl1822_valA[wl1822_r2k[row]]);
    CHK(wl1822_bufB == wl1822_valB[wl1822_r2k[row]]);
    row++;
  }
  ndb->closeTransaction(con);
  CHK(row == 3);
  return 0;
}

// threads are synced between each step
static Runstep wl1822_step[][2] = {
    {runstep_connect, runstep_connect},
    {wl1822_createtable, 0},
    {wl1822_insertrows, 0},
    {wl1822_getscanorder, 0},
    {runstep_starttx, runstep_starttx},
    {wl1822_tx1_readZ, 0},
    {0, wl1822_tx2_scanXY},
    {wl1822_tx1_readX_commit, wl1822_tx2_scanZ_close}};
const unsigned wl1822_stepcount = sizeof(wl1822_step) / sizeof(wl1822_step[0]);

static int wl1822_main(char scantx) {
  wl1822_scantx = scantx;
  static const unsigned thrcount = 2;
  // create threads for tx1 and tx2
  Thr *thrlist[2];
  unsigned n;
  for (n = 0; n < thrcount; n++) {
    Thr &thr = *(thrlist[n] = new Thr(1 + n));
    CHK(thr.m_ret == 0);
  }
  // run the steps
  for (unsigned i = 0; i < wl1822_stepcount; i++) {
    DBG("step " << i << " start");
    for (n = 0; n < thrcount; n++) {
      Thr &thr = *thrlist[n];
      Runstep runstep = wl1822_step[i][n];
      if (runstep != 0) thr.start(runstep);
    }
    for (n = 0; n < thrcount; n++) {
      Thr &thr = *thrlist[n];
      Runstep runstep = wl1822_step[i][n];
      if (runstep != 0) thr.stopped();
    }
  }
  // delete threads
  for (n = 0; n < thrcount; n++) {
    Thr &thr = *thrlist[n];
    thr.exit();
    thr.join();
    delete &thr;
  }
  return 0;
}

/*
 * RONDB-1062 scan <-> scan deadlock.
 *
 * Table T (single fragment) with two tuples, key A in {0,1}, ordered index X
 * on B with B in {10,20}.  Two transactions each run an EXCLUSIVE ordered
 * index scan with batch size 2, in opposite directions:
 *
 *   - tx1 scans ascending : locks row 0, then wants row 1
 *   - tx2 scans descending: locks row 1, then wants row 0  => cycle
 *
 * The cycle is made deterministic with error insert 12010 in DBTUX: a locking
 * scan that already holds a batch lock stalls before locking its next row
 * until the error insert is cleared.  We arm it, start both scans (each grabs
 * its first row and stalls, blocked inside nextResult), then clear it so both
 * proceed to the other's row and collide at the same instant.  Because both
 * scans hold their first row (a *batch* scan-lock, not taken over) when
 * requesting the second, DBACC reports a wait-for edge in each direction and a
 * single min-hash collector in DBTC resolves the scan<->scan cycle
 * proactively, aborting one scan with a lock-wait/timeout error (266/274/296)
 * while the other proceeds.  The .cnf sets a high
 * TransactionDeadlockDetectionTimeout, so prompt resolution proves proactive
 * detection rather than the timeout backstop.
 *
 * Batch size 2 (not 1) is required so the scan holds row 0's lock while it
 * waits on row 1: a batch-1 scan releases each row's lock when it advances.
 * Lock takeover is deliberately NOT used - that would convert the held locks
 * into key ops, and the scan<->key-op routing would split the two edges to
 * different collectors and never detect the cycle.
 */

static const Uint32 ss_valA[2] = {0, 1};
static const Uint32 ss_valB[2] = {10, 20};
static Uint32 ss_bufA[2] = {~0u, ~0u};  // per-thread scan read buffer
static int ss_outcome[2] = {-1, -1};    // per-thread: 0 survived, else err code
static NDB_TICKS ss_t0, ss_t1;
// RONDB-1062 deadlock error enrichment: per-thread detail read back from the
// victim transaction via the new NdbTransaction accessors (Phase C).
static bool ss_was_deadlock[2] = {false, false};
static int ss_dl_ntables[2] = {0, 0};
static Uint32 ss_dl_table[2][2] = {{~0u, ~0u}, {~0u, ~0u}};
static bool ss_dl_has_op[2] = {false, false};
static int ss_table_id = -1;  // T's NDB table id (captured in ss_main)

static int ss_createtable(Ndb *ndb) {
  require(ndb != 0);
  NdbDictionary::Dictionary *dic = ndb->getDictionary();
  if (dic->getTable(g_opt.m_tname) != 0)
    CHN(dic, dic->dropTable(g_opt.m_tname) == 0);
  NdbDictionary::Table tab(g_opt.m_tname);
  // Single fragment so both rows live on the same fragment and the two
  // opposing scans contend on the same locks.  FragSingle places the one
  // fragment correctly regardless of the number of data nodes
  // (setFragmentCount(1) fails on a multi-node cluster with a nodegroup error).
  tab.setFragmentType(NdbDictionary::Object::FragSingle);
  {
    NdbDictionary::Column col("A");
    col.setType(NdbDictionary::Column::Unsigned);
    col.setPrimaryKey(true);
    tab.addColumn(col);
  }
  {
    NdbDictionary::Column col("B");
    col.setType(NdbDictionary::Column::Unsigned);
    col.setPrimaryKey(false);
    tab.addColumn(col);
  }
  CHN(dic, dic->createTable(tab) == 0);
  NdbDictionary::Index ind(g_opt.m_xname);
  ind.setTable(g_opt.m_tname);
  ind.setType(NdbDictionary::Index::OrderedIndex);
  ind.setLogging(false);
  ind.addColumn("B");
  CHN(dic, dic->createIndex(ind) == 0);
  DBG("ss created " << g_opt.m_tname << ", " << g_opt.m_xname
                    << " (1 fragment)");
  return 0;
}

static int ss_insertrows(Ndb *ndb) {
  require(ndb != 0);
  NdbConnection *con;
  NdbOperation *op;
  for (unsigned k = 0; k < 2; k++) {
    CHN(ndb, (con = ndb->startTransaction()) != 0);
    CHN(con, (op = con->getNdbOperation(g_opt.m_tname)) != 0);
    CHN(op, op->insertTuple() == 0);
    CHN(op, op->equal("A", (char *)&ss_valA[k]) == 0);
    CHN(op, op->setValue("B", (char *)&ss_valB[k]) == 0);
    CHN(con, con->execute(Commit) == 0);
    ndb->closeTransaction(con);
  }
  DBG("ss inserted 2 rows");
  return 0;
}

// Best-effort drop of the test table (and its index) so the test leaves no
// state behind for MTR's check-testcase.  Call only after the scan
// transactions are closed (worker threads torn down) so no locks remain.
static void ss_droptable(Ndb *ndb) {
  if (ndb == 0) return;
  NdbDictionary::Dictionary *dic = ndb->getDictionary();
  if (dic->getTable(g_opt.m_tname) != 0) (void)dic->dropTable(g_opt.m_tname);
}

// Run a full exclusive ordered index scan (batch 2) to completion, recording
// the outcome.  Thread 1 ascends, thread 2 descends.  With error insert 12010
// armed the scan locks its first row, then stalls (blocked here inside
// nextResult) before the second; once the test clears the insert the two scans
// collide and one is aborted as the deadlock victim.
static int ss_run_scan(Thr &thr) {
  const bool descending = (thr.m_no == 2);
  NdbConnection *con = thr.m_con;
  require(con != 0);
  NdbIndexScanOperation *iscanop;
  CHN(con, (iscanop = con->getNdbIndexScanOperation(g_opt.m_xname,
                                                    g_opt.m_tname)) != 0);
  thr.m_indexscanop = iscanop;
  // Use the base NdbScanOperation interface: NdbIndexScanOperation overloads
  // readTuples in a way that makes the 4-argument form ambiguous.
  NdbScanOperation *scanop = thr.m_scanop = iscanop;
  Uint32 flags = NdbScanOperation::SF_OrderBy;
  if (descending) flags |= NdbScanOperation::SF_Descending;
  // LM_Exclusive, ordered, parallel=0 (all = 1 frag), batch=2 so the first
  // row's lock is retained while waiting on the second (a batch-1 scan would
  // release it on advance).
  CHN(scanop,
      scanop->readTuples(NdbScanOperation::LM_Exclusive, flags, 0, 2) == 0);
  CHN(scanop,
      scanop->getValue("A", (char *)&ss_bufA[thr.m_no - 1]) != 0);
  CHN(con, con->execute(NoCommit) == 0);
  DBG(thr << " scan open" << (descending ? " (desc)" : " (asc)"));
  // Drive the scan to completion.  nextResult blocks during the 12010 stall
  // and again on the real lock wait; it returns < 0 if this scan is the
  // deadlock victim, or 1 (no more rows) if it survived and read both rows.
  int ret;
  while ((ret = scanop->nextResult(true)) == 0) {
    DBG(thr << " scan got row A=" << ss_bufA[thr.m_no - 1]);
  }
  if (ret < 0) {
    const int idx = thr.m_no - 1;
    ss_outcome[idx] = con->getNdbError().code;
    // RONDB-1062: read back the deadlock detail reported to this (victim) txn.
    // For a scan victim getDeadlockOperation() is null (no single key op); the
    // contended table id(s) identify table T.  con is still open here.
    ss_was_deadlock[idx] = con->wasDeadlock();
    ss_dl_ntables[idx] = con->getDeadlockTableIds(ss_dl_table[idx]);
    ss_dl_has_op[idx] = (con->getDeadlockOperation() != nullptr);
    DBG(thr << " scan aborted, error " << ss_outcome[idx] << ", wasDeadlock="
            << ss_was_deadlock[idx] << " ntables=" << ss_dl_ntables[idx]
            << " hasOp=" << ss_dl_has_op[idx]);
  } else {
    ss_outcome[thr.m_no - 1] = 0;  // ret == 1: end of scan, survived
    DBG(thr << " scan completed (survivor)");
  }
  return 0;
}

// Exactly one scan must be the victim (lock-wait/timeout error), the other
// must have survived.
static int ss_verify(Thr &thr) {
  int victims = 0, survivors = 0;
  for (int i = 0; i < 2; i++) {
    const int c = ss_outcome[i];
    if (c == 0)
      survivors++;
    else if (c == 266 || c == 274 || c == 296)
      victims++;
    else {
      DBG("ss unexpected outcome[" << i << "]=" << c);
      return -1;
    }
  }
  CHK(victims == 1 && survivors == 1);
  DBG("ss scan<->scan deadlock resolved: 1 victim, 1 survivor");

  // RONDB-1062 deadlock error enrichment: the victim must carry the
  // proactively-detected detail (the timing assertion below proves it was
  // proactive, so the report must have arrived).  A scan victim has no single
  // deadlocking key op, and the contended table is T.
  for (int i = 0; i < 2; i++) {
    if (ss_outcome[i] == 0) continue;  // survivor carries no detail
    CHK(ss_was_deadlock[i]);           // a real-deadlock report was received
    CHK(ss_dl_ntables[i] >= 1);        // at least one contended table reported
    bool is_T = false;
    for (int j = 0; j < ss_dl_ntables[i]; j++)
      if ((int)ss_dl_table[i][j] == ss_table_id) is_T = true;
    CHK(is_T);                  // and it is table T
    CHK(!ss_dl_has_op[i]);      // scan victim => getDeadlockOperation() is null
    DBG("ss victim detail ok: wasDeadlock, table T reported, no key op");
  }
  return 0;
}

// Run one runstep on a thread and wait for it to finish; return its result.
static int ss_do(Thr &thr, Runstep step) {
  thr.start(step);
  thr.stopped();
  return thr.m_ret;
}

static int ss_main() {
  static const unsigned thrcount = 2;
  Thr *thrlist[2];
  for (unsigned n = 0; n < thrcount; n++) {
    Thr &thr = *(thrlist[n] = new Thr(1 + n));
    CHK(thr.m_ret == 0);
  }
  Thr &t1 = *thrlist[0];
  Thr &t2 = *thrlist[1];
  NdbRestarter restarter;
  int rc = 0;
  bool armed = false;

  // A dedicated Ndb (independent of the worker threads) owns the test table so
  // that it is always created and, at the end, dropped - leaving no NDB
  // dictionary state behind for MTR's check-testcase.
  Ndb mgmt(g_cluster_connection, "TEST_DB");
  if (mgmt.init() != 0 || mgmt.waitUntilReady() != 0) {
    ndbout << "ss: management Ndb connect failed" << endl;
    rc = -1;
  }

  // Setup: create + fill the table, then connect each worker and start a tx.
  if (rc == 0 &&
      (ss_createtable(&mgmt) != 0 || ss_insertrows(&mgmt) != 0 ||
       ss_do(t1, runstep_connect) != 0 || ss_do(t2, runstep_connect) != 0 ||
       ss_do(t1, runstep_starttx) != 0 || ss_do(t2, runstep_starttx) != 0)) {
    rc = -1;
  }

  // Remember T's table id so ss_verify can match it against the contended
  // table id reported in the deadlock detail (RONDB-1062).
  if (rc == 0) {
    const NdbDictionary::Table *t =
        mgmt.getDictionary()->getTable(g_opt.m_tname);
    if (t != nullptr) ss_table_id = t->getObjectId();
  }

  // Arm the deterministic stall (DBTUX error insert 12010) and run both scans
  // concurrently: each grabs its first row and blocks before the second.
  if (rc == 0) {
    if (restarter.insertErrorInAllNodes(12010) != 0) {
      ndbout << "ss: failed to arm error insert 12010" << endl;
      rc = -1;
    } else {
      armed = true;
      ss_t0 = NdbTick_getCurrentTicks();
      t1.start(ss_run_scan);
      t2.start(ss_run_scan);
      // The stall is a hard gate (scans wait until the insert is cleared), so
      // a generous sleep here is not a race - it just ensures both have locked
      // their first row before we release them to collide.
      NdbSleep_SecSleep(3);
      (void)restarter.insertErrorInAllNodes(0);
      armed = false;
      t1.stopped();
      t2.stopped();
      ss_t1 = NdbTick_getCurrentTicks();
      if (t1.m_ret != 0 || t2.m_ret != 0)
        rc = -1;
      else
        rc = ss_verify(t1);
      if (rc == 0) {
        const Uint64 ms = NdbTick_Elapsed(ss_t0, ss_t1).milliSec();
        ndbout << "scan<->scan deadlock resolved in " << ms << " ms" << endl;
        // A high TransactionDeadlockDetectionTimeout means prompt resolution
        // can only be proactive detection (the timeout would take many secs).
        if (ms >= 20000) {
          ndbout << "deadlock not resolved promptly - proactive detection"
                    " may have regressed (fell back to the timeout)"
                 << endl;
          rc = -1;
        }
      }
    }
  }

  if (armed) (void)restarter.insertErrorInAllNodes(0);
  // Tear down the workers first (deleting their Ndb closes the scan
  // transactions and releases all locks), then drop the table.
  for (unsigned n = 0; n < thrcount; n++) {
    thrlist[n]->exit();
    thrlist[n]->join();
    delete thrlist[n];
  }
  ss_droptable(&mgmt);
  return rc;
}

int main(int argc, char **argv) {
  ndb_init();
  if (ndbout_mutex == NULL) ndbout_mutex = NdbMutex_Create();
  while (++argv, --argc > 0) {
    const char *arg = argv[0];
    if (strcmp(arg, "-scan") == 0) {
      if (++argv, --argc > 0) {
        g_opt.m_scan = strdup(argv[0]);
        continue;
      }
    }
    printusage();
    return NDBT_ProgramExit(NDBT_WRONGARGS);
  }

  Ndb_cluster_connection con;
  con.configure_tls(opt_tls_search_path, opt_mgm_tls);
  if (con.connect(12, 5, 1) != 0) {
    return NDBT_ProgramExit(NDBT_FAILED);
  }
  g_cluster_connection = &con;

  if ((strchr(g_opt.m_scan, 't') != 0 && wl1822_main('t') == -1) ||
      (strchr(g_opt.m_scan, 'x') != 0 && wl1822_main('x') == -1) ||
      (strchr(g_opt.m_scan, 's') != 0 && ss_main() == -1)) {
    return NDBT_ProgramExit(NDBT_FAILED);
  }
  return NDBT_ProgramExit(NDBT_OK);
}

// vim: set sw=2 et:
