/*
   Copyright (c) 2011, 2026, Oracle and/or its affiliates.

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

#include <NDBT_ReturnCodes.h>
#include <ndb_version.h>
#include <random.h>
#include <Bitmask.hpp>
#include <HugoQueries.hpp>
#include <HugoQueryBuilder.hpp>
#include <HugoTransactions.hpp>
#include <NDBT_Test.hpp>
#include <NdbRestarter.hpp>
#include <NdbSchemaCon.hpp>
#include <UtilTransactions.hpp>
#include <signaldata/DbspjErr.hpp>
#include <signaldata/DictTabInfo.hpp>
#include <signaldata/DumpStateOrd.hpp>
#include "../../src/ndbapi/NdbInfo.hpp"
#include "util/require.h"

static int faultToInject = 0;

enum faultsToInject { FI_START = 17001, FI_END = 17541 };

int runLoadTable(NDBT_Context *ctx, NDBT_Step *step) {
  int records = ctx->getNumRecords();
  HugoTransactions hugoTrans(*ctx->getTab());
  if (hugoTrans.loadTable(GETNDB(step), records) != 0) {
    return NDBT_FAILED;
  }
  return NDBT_OK;
}

int runClearTable(NDBT_Context *ctx, NDBT_Step *step) {
  UtilTransactions utilTrans(*ctx->getTab());
  if (utilTrans.clearTable(GETNDB(step)) != 0) {
    return NDBT_FAILED;
  }
  return NDBT_OK;
}

static void addMask(NDBT_Context *ctx, Uint32 val, const char *name) {
  Uint32 oldValue = 0;
  do {
    oldValue = ctx->getProperty(name);
    Uint32 newValue = oldValue | val;
    if (ctx->casProperty(name, oldValue, newValue) == oldValue) return;
    NdbSleep_MilliSleep(5);
  } while (true);
}

int runLookupJoin(NDBT_Context *ctx, NDBT_Step *step) {
  int loops = ctx->getNumLoops();
  int joinlevel = ctx->getProperty("JoinLevel", 3);
  int records = ctx->getNumRecords();
  int queries = records / joinlevel;
  int until_stopped = ctx->getProperty("UntilStopped", (Uint32)0);
  Uint32 stepNo = step->getStepNo();

  int i = 0;
  HugoQueryBuilder qb(GETNDB(step), ctx->getTab(), HugoQueryBuilder::O_LOOKUP);
  qb.setJoinLevel(joinlevel);
  const NdbQueryDef *query = qb.createQuery();
  if (query == nullptr) {
    ndbout << "Failed to create NdbQueryDef" << endl;
    return NDBT_FAILED;
  }
  HugoQueries hugoTrans(*query);
  while ((i < loops || until_stopped) && !ctx->isTestStopped()) {
    g_info << i << ": ";
    if (hugoTrans.runLookupQuery(GETNDB(step), queries)) {
      g_info << endl;
      return NDBT_FAILED;
    }
    addMask(ctx, (1 << stepNo), "Running");
    i++;
  }
  g_info << endl;
  return NDBT_OK;
}

int runLookupJoinError(NDBT_Context *ctx, NDBT_Step *step) {
  int loops = ctx->getNumLoops();
  int joinlevel = ctx->getProperty("JoinLevel", 8);
  int records = ctx->getNumRecords();
  int queries = records / joinlevel;
  int until_stopped = ctx->getProperty("UntilStopped", (Uint32)0);
  Uint32 stepNo = step->getStepNo();

  int i = 0;
  HugoQueryBuilder qb(GETNDB(step), ctx->getTab(), HugoQueryBuilder::O_LOOKUP);
  qb.setJoinLevel(joinlevel);
  const NdbQueryDef *query = qb.createQuery();
  if (query == nullptr) {
    ndbout << "Failed to create NdbQueryDef" << endl;
    return NDBT_FAILED;
  }
  HugoQueries hugoTrans(*query);

  NdbRestarter restarter;
  int lookupFaults[] = {
      5078,  // Pack TCKEYREF in ROUTE_ORD and send it via SPJ.
      7240,  // DIGETNODESREQ returns error
      17001, 17005, 17006, 17008,
      17012,                // testing abort in :execDIH_SCAN_TAB_CONF
      17013,                // Simulate DbspjErr::InvalidRequest
      17020, 17021, 17022,  // lookup_send() encounter dead node -> NodeFailure
      17030, 17031, 17032,  // LQHKEYREQ reply is LQHKEYREF('Invalid..')
      17040, 17041, 17042,  // lookup_parent_row -> OutOfQueryMemory
      17043, 17050, 17051, 17052, 17053,  // parseDA -> outOfSectionMem
      17060, 17061, 17062, 17063,  // scanIndex_parent_row -> outOfSectionMem
      17070, 17071, 17072,         // lookup_send.dupsec -> outOfSectionMem
      17080, 17081, 17082,         // lookup_parent_row -> OutOfQueryMemory
      17120, 17121,                // execTRANSID_AI -> OutOfRowMemory
      17122, 17130, 17131,  // sendSignal(DIH_SCAN_GET_NODES_REQ)  -> import()
                            // failed
      7234,   // sendSignal(DIH_SCAN_GET_NODES_CONF) -> import() failed (DIH)
      17510,  // random failure when allocating section memory
      17520, 17521,  // failure (+random) from ::checkTableError()
      17534          // do_init: per-node arrays -> OutOfQueryMemory (F27)
  };
  loops = faultToInject ? 1 : sizeof(lookupFaults) / sizeof(int);

  while ((i < loops || until_stopped) && !ctx->isTestStopped()) {
    g_info << i << ": ";

    int inject_err = faultToInject ? faultToInject : lookupFaults[i];
    int randomId = rand() % restarter.getNumDbNodes();
    int nodeId = restarter.getDbNodeId(randomId);

    ndbout << "LookupJoinError: Injecting error " << inject_err << " in node "
           << nodeId << " loop " << i << endl;

    if (restarter.getNodeStatus(nodeId) != NDB_MGM_NODE_STATUS_STARTED ||
        restarter.insertErrorInNode(nodeId, inject_err) != 0) {
      ndbout << "Could not insert error in node " << nodeId << endl;
      g_info << endl;
      return NDBT_FAILED;
    }

    // It'd be better if test could differentiates failures from
    // fault injection and others.
    // We expect to fail, and it's a failure if we don't
    if (!hugoTrans.runLookupQuery(GETNDB(step), queries)) {
      g_info << "LookUpJoinError didn't fail as expected." << endl;
      // return NDBT_FAILED;
    }

    addMask(ctx, (1 << stepNo), "Running");
    i++;
  }
  restarter.insertErrorInAllNodes(0);
  g_info << endl;
  return NDBT_OK;
}

int runScanJoin(NDBT_Context *ctx, NDBT_Step *step) {
  int loops = ctx->getNumLoops();
  int joinlevel = ctx->getProperty("JoinLevel", 3);
  int until_stopped = ctx->getProperty("UntilStopped", (Uint32)0);
  Uint32 stepNo = step->getStepNo();

  int i = 0;
  HugoQueryBuilder qb(GETNDB(step), ctx->getTab(), HugoQueryBuilder::O_SCAN);
  qb.setJoinLevel(joinlevel);
  const NdbQueryDef *query = qb.createQuery();
  if (query == nullptr) {
    ndbout << "Failed to create NdbQueryDef" << endl;
    return NDBT_FAILED;
  }
  HugoQueries hugoTrans(*query);
  while ((i < loops || until_stopped) && !ctx->isTestStopped()) {
    g_info << i << ": ";
    if (hugoTrans.runScanQuery(GETNDB(step))) {
      g_info << endl;
      return NDBT_FAILED;
    }
    addMask(ctx, (1 << stepNo), "Running");
    i++;
  }
  g_info << endl;
  return NDBT_OK;
}

int runScanJoinError(NDBT_Context *ctx, NDBT_Step *step) {
  int loops = ctx->getNumLoops();
  int joinlevel = ctx->getProperty("JoinLevel", 3);
  int until_stopped = ctx->getProperty("UntilStopped", (Uint32)0);
  Uint32 stepNo = step->getStepNo();

  int i = 0;
  HugoQueryBuilder qb(GETNDB(step), ctx->getTab(), HugoQueryBuilder::O_SCAN);
  qb.setJoinLevel(joinlevel);
  const NdbQueryDef *query = qb.createQuery();
  if (query == nullptr) {
    ndbout << "Failed to create NdbQueryDef" << endl;
    return NDBT_FAILED;
  }
  HugoQueries hugoTrans(*query);

  NdbRestarter restarter;
  int scanFaults[] = {
      7240,  // DIGETNODESREQ returns error
      17002, 17004, 17005, 17006, 17008,
      17012,                // testing abort in :execDIH_SCAN_TAB_CONF
      17013,                // Simulate DbspjErr::InvalidRequest
      17020, 17021, 17022,  // lookup_send() encounter dead node -> NodeFailure
      17030, 17031, 17032,  // LQHKEYREQ reply is LQHKEYREF('Invalid..')
      17040, 17041, 17042,  // lookup_parent_row -> OutOfQueryMemory
      17043, 17050, 17051, 17052, 17053,  // parseDA -> outOfSectionMem
      17060, 17061, 17062, 17063,  // scanIndex_parent_row -> outOfSectionMem
      17070, 17071, 17072,         // lookup_send.dupsec -> outOfSectionMem
      17080, 17081, 17082,         // lookup_parent_row -> OutOfQueryMemory
      17090, 17091, 17092, 17093,  // scanIndex_send -> OutOfQueryMemory
      17100,  // scanFrag_sends invalid schema version, to get a SCAN_FRAGREF
      17110, 17111, 17112,  // scanIndex_sends invalid schema version, to get a
                            // SCAN_FRAGREF
      17120, 17121,         // execTRANSID_AI -> OutOfRowMemory
      17122, 17130, 17131,  // sendSignal(DIH_SCAN_GET_NODES_REQ) -> import()
                            // failed
      17510,                // random failure when allocating section memory
      17520, 17521,  // failure (+random) from TableRecord::checkTableError()
      17534          // do_init: per-node arrays -> OutOfQueryMemory (F27)
  };
  loops = faultToInject ? 1 : sizeof(scanFaults) / sizeof(int);

  while ((i < loops || until_stopped) && !ctx->isTestStopped()) {
    g_info << i << ": ";

    int inject_err = faultToInject ? faultToInject : scanFaults[i];
    int randomId = rand() % restarter.getNumDbNodes();
    int nodeId = restarter.getDbNodeId(randomId);

    ndbout << "ScanJoin: Injecting error " << inject_err << " in node "
           << nodeId << " loop " << i << endl;

    if (restarter.insertErrorInNode(nodeId, inject_err) != 0) {
      ndbout << "Could not insert error in node " << nodeId << endl;
      return NDBT_FAILED;
    }

    // It'd be better if test could differentiates failures from
    // fault injection and others.
    // We expect to fail, and it's a failure if we don't
    if (!hugoTrans.runScanQuery(GETNDB(step))) {
      g_info << "ScanJoinError didn't fail as expected." << endl;
      // return NDBT_FAILED;
    }

    addMask(ctx, (1 << stepNo), "Running");
    i++;
  }
  restarter.insertErrorInAllNodes(0);
  g_info << endl;
  return NDBT_OK;
}

int runJoin(NDBT_Context *ctx, NDBT_Step *step) {
  int loops = ctx->getNumLoops();
  int joinlevel = ctx->getProperty("JoinLevel", 3);
  int records = ctx->getNumRecords();
  int queries = records / joinlevel;
  int until_stopped = ctx->getProperty("UntilStopped", (Uint32)0);
  int inject_err = ctx->getProperty("ErrorCode");
  int accept_error = ctx->getProperty("AcceptError");
  Uint32 stepNo = step->getStepNo();

  // Either handle error myself, or let Hugo retry
  Uint32 maxRetries = (accept_error != 0) ? 1 : 100;

  int i = 0;
  HugoQueryBuilder qb1(GETNDB(step), ctx->getTab(), HugoQueryBuilder::O_SCAN);
  HugoQueryBuilder qb2(GETNDB(step), ctx->getTab(), HugoQueryBuilder::O_LOOKUP);
  qb1.setJoinLevel(joinlevel);
  qb2.setJoinLevel(joinlevel);
  const NdbQueryDef *q1 = qb1.createQuery();
  const NdbQueryDef *q2 = qb2.createQuery();
  if (q1 == nullptr || q2 == nullptr) {
    ndbout << "Failed to create NdbQueryDefs" << endl;
    return NDBT_FAILED;
  }
  HugoQueries hugoTrans1(*q1, maxRetries);
  HugoQueries hugoTrans2(*q2, maxRetries);
  NdbRestarter restarter;

  if (inject_err) {
    ndbout << "insertErrorInAllNodes(" << inject_err << ")" << endl;
    if (restarter.insertErrorInAllNodes(inject_err) != 0) {
      g_info << "Could not insert error in all nodes " << endl;
      return NDBT_FAILED;
    }
  }
  int ret = NDBT_OK;
  while ((i < loops || until_stopped) && !ctx->isTestStopped()) {
    g_info << i << ": ";
    if (hugoTrans1.runScanQuery(GETNDB(step))) {
      const NdbError err = hugoTrans1.getNdbError();
      if (err.code != accept_error) {
        ret = NDBT_FAILED;
        break;
      }
    }
    if (hugoTrans2.runLookupQuery(GETNDB(step), queries)) {
      const NdbError err = hugoTrans2.getNdbError();
      if (err.code != accept_error) {
        ret = NDBT_FAILED;
        break;
      }
    }
    i++;
    addMask(ctx, (1 << stepNo), "Running");
  }
  g_info << endl;
  if (inject_err) {
    restarter.insertErrorInAllNodes(0);
  }
  return ret;
}

int runAbortedJoin(NDBT_Context *ctx, NDBT_Step *step) {
  int loops = ctx->getNumLoops();
  int joinlevel = ctx->getProperty("JoinLevel", 3);
  int records = ctx->getNumRecords();
  int queries = records / joinlevel;
  int until_stopped = ctx->getProperty("UntilStopped", (Uint32)0);
  Uint32 stepNo = step->getStepNo();
  int i = 0;

  HugoQueryBuilder qb2(GETNDB(step), ctx->getTab(), HugoQueryBuilder::O_LOOKUP);
  qb2.setJoinLevel(joinlevel);
  const NdbQueryDef *q2 = qb2.createQuery();
  if (q2 == nullptr) {
    ndbout << "Failed to create NdbQueryDef" << endl;
    return NDBT_FAILED;
  }
  HugoQueries hugoTrans2(*q2, 1);  // maxRetry==1 -> Don't retry temp errors
  NdbRestarter restarter;

  while ((i < loops || until_stopped) && !ctx->isTestStopped()) {
    g_info << i << ": ";
    if (hugoTrans2.runLookupQuery(GETNDB(step), queries)) {
      const NdbError err = hugoTrans2.getNdbError();

      // Test pass as long as we don't get errors 'promoted' to nodeFailures
      if (err.code == DbspjErr::NodeFailure)  // NodeFailure == 20016
      {
        g_info << endl;
        return NDBT_FAILED;
      }
    }
    i++;
    addMask(ctx, (1 << stepNo), "Running");
  }
  g_info << endl;
  return NDBT_OK;
}

int runRestarter(NDBT_Context *ctx, NDBT_Step *step) {
  int result = NDBT_OK;
  int loops = ctx->getNumLoops();
  int waitprogress = ctx->getProperty("WaitProgress", (unsigned)0);
  int randnode = ctx->getProperty("RandNode", (unsigned)0);
  int inject_err = ctx->getProperty("RestartWithErrorCode");
  NdbRestarter restarter;
  int i = 0;
  int lastId = 0;

  if (restarter.getNumDbNodes() < 2) {
    ctx->stopTest();
    return NDBT_OK;
  }

  if (restarter.waitClusterStarted() != 0) {
    g_err << "Cluster failed to start" << endl;
    return NDBT_FAILED;
  }

  loops *= (restarter.getNumDbNodes() > 2 ? 2 : restarter.getNumDbNodes());
  if (loops < restarter.getNumDbNodes()) loops = restarter.getNumDbNodes();

  NdbSleep_MilliSleep(200);
  Uint32 running = ctx->getProperty("Running", (Uint32)0);
  while (running == 0 && !ctx->isTestStopped()) {
    NdbSleep_MilliSleep(100);
    running = ctx->getProperty("Running", (Uint32)0);
  }

  if (ctx->isTestStopped()) return NDBT_FAILED;

  while (i < loops && result != NDBT_FAILED && !ctx->isTestStopped()) {
    int id = lastId % restarter.getNumDbNodes();
    if (randnode == 1) {
      id = rand() % restarter.getNumDbNodes();
    }
    int nodeId = restarter.getDbNodeId(id);
    ndbout << "Restart node " << nodeId << endl;

    if (restarter.restartOneDbNode(nodeId, false, true, true) != 0) {
      g_err << "Failed to restartNextDbNode" << endl;
      result = NDBT_FAILED;
      break;
    }

    if (restarter.waitNodesNoStart(&nodeId, 1)) {
      g_err << "Failed to waitNodesNoStart" << endl;
      result = NDBT_FAILED;
      break;
    }

    if (waitprogress) {
      Uint32 maxwait = 60;
      ndbout_c("running: 0x%.8x", running);
      for (Uint32 checks = 0; checks < 3 && !ctx->isTestStopped(); checks++) {
        ctx->setProperty("Running", (Uint32)0);
        for (; maxwait != 0 && !ctx->isTestStopped(); maxwait--) {
          if ((ctx->getProperty("Running", (Uint32)0) & running) == running)
            goto ok;
          NdbSleep_SecSleep(1);
        }

        if (ctx->isTestStopped()) {
          g_err << "Test stopped while waiting for progress!" << endl;
          return NDBT_FAILED;
        }

        g_err << "No progress made!!" << endl;
        return NDBT_FAILED;
      ok:
        g_err << "Progress made!! " << endl;
      }
    }

    if (inject_err) {
      ndbout << "RestartWithErrorCode: (" << inject_err << ")" << endl;
      if (restarter.insertErrorInNode(nodeId, inject_err) != 0) {
        g_info << "Could not insert error in node " << nodeId << endl;
        return NDBT_FAILED;
      }
    }

    if (restarter.startNodes(&nodeId, 1)) {
      g_err << "Failed to start node" << endl;
      result = NDBT_FAILED;
      break;
    }

    if (restarter.waitClusterStarted() != 0) {
      g_err << "Cluster failed to start" << endl;
      result = NDBT_FAILED;
      break;
    }

    if (waitprogress) {
      Uint32 maxwait = 60;
      ndbout_c("running: 0x%.8x", running);
      for (Uint32 checks = 0; checks < 3 && !ctx->isTestStopped(); checks++) {
        ctx->setProperty("Running", (Uint32)0);
        for (; maxwait != 0 && !ctx->isTestStopped(); maxwait--) {
          if ((ctx->getProperty("Running", (Uint32)0) & running) == running)
            goto ok2;
          NdbSleep_SecSleep(1);
        }

        if (ctx->isTestStopped()) {
          g_err << "Test stopped while waiting for progress!" << endl;
          return NDBT_FAILED;
        }

        g_err << "No progress made!!" << endl;
        return NDBT_FAILED;
      ok2:
        g_err << "Progress made!! " << endl;
        ctx->setProperty("Running", (Uint32)0);
      }
    }

    if (inject_err) {
      if (restarter.insertErrorInNode(nodeId, 0) != 0) {
        g_info << "Could not clear error in node " << nodeId << endl;
        return NDBT_FAILED;
      }
    }

    lastId++;
    i++;
  }

  ctx->stopTest();

  return result;
}

static const int nt2StrLen = 20;

static int createNegativeSchema(NDBT_Context *ctx, NDBT_Step *step) {
  for (int i = 0; i < 2; i++) {
    NdbDictionary::Column::Type type = NdbDictionary::Column::Undefined;
    Uint32 arraySize = 0;
    const char *tabName = NULL;
    const char *ordIdxName = NULL;
    const char *unqIdxName = NULL;
    switch (i) {
      case 0:
        type = NdbDictionary::Column::Int;
        arraySize = 1;
        tabName = "nt1";
        ordIdxName = "nt1_oix";
        unqIdxName = "nt1_uix";
        break;
      case 1:
        type = NdbDictionary::Column::Varchar;
        arraySize = nt2StrLen;
        tabName = "nt2";
        ordIdxName = "nt2_oix";
        unqIdxName = "nt2_uix";
        break;
    }

    /****************************************************************
     *	Create table nt1 and attributes.
     ***************************************************************/
    NDBT_Attribute pk1("pk1", type, arraySize, true);
    NDBT_Attribute pk2("pk2", type, arraySize, true);
    NDBT_Attribute oi1("oi1", type, arraySize);
    NDBT_Attribute oi2("oi2", type, arraySize);
    NDBT_Attribute ui1("ui1", type, arraySize);
    NDBT_Attribute ui2("ui2", type, arraySize);

    NdbDictionary::Column *columns[] = {&pk1, &pk2, &oi1, &oi2, &ui1, &ui2};

    const NDBT_Table tabDef(tabName, sizeof columns / sizeof columns[0],
                            columns);

    Ndb *const ndb = step->getNdb();

    NdbDictionary::Dictionary *const dictionary = ndb->getDictionary();

    dictionary->dropTable(tabName);
    require(dictionary->createTable(tabDef) == 0);

    // Create ordered index on oi1,oi2.
    NdbDictionary::Index ordIdx(ordIdxName);
    require(ordIdx.setTable(tabName) == 0);
    ordIdx.setType(NdbDictionary::Index::OrderedIndex);
    ordIdx.setLogging(false);
    require(ordIdx.addColumn(oi1) == 0);
    require(ordIdx.addColumn(oi2) == 0);
    require(dictionary->createIndex(ordIdx, tabDef) == 0);

    // Create unique index on ui1,ui2.
    NdbDictionary::Index unqIdx(unqIdxName);
    require(unqIdx.setTable(tabName) == 0);
    unqIdx.setType(NdbDictionary::Index::UniqueHashIndex);
    unqIdx.setLogging(true);
    require(unqIdx.addColumn(ui1) == 0);
    require(unqIdx.addColumn(ui2) == 0);
    require(dictionary->createIndex(unqIdx, tabDef) == 0);
  }  // for (...
  return NDBT_OK;
}

/* Query-related error codes. Used for negative testing. */
#define QRY_TOO_FEW_KEY_VALUES 4801
#define QRY_TOO_MANY_KEY_VALUES 4802
#define QRY_OPERAND_HAS_WRONG_TYPE 4803
#define QRY_CHAR_OPERAND_TRUNCATED 4804
#define QRY_NUM_OPERAND_RANGE 4805
#define QRY_MULTIPLE_PARENTS 4806
#define QRY_UNKNOWN_PARENT 4807
#define QRY_UNRELATED_INDEX 4809
#define QRY_WRONG_INDEX_TYPE 4810
#define QRY_DEFINITION_TOO_LARGE 4812
#define QRY_RESULT_ROW_ALREADY_DEFINED 4814
#define QRY_HAS_ZERO_OPERATIONS 4815
#define QRY_ILLEGAL_STATE 4817
#define QRY_WRONG_OPERATION_TYPE 4820
#define QRY_MULTIPLE_SCAN_SORTED 4824
#define QRY_EMPTY_PROJECTION 4826

/* Various error codes that are not specific to NdbQuery. */
static const int Err_UnknownColumn = 4004;
static const int Err_WrongFieldLength = 4209;
static const int Err_InvalidRangeNo = 4286;
static const int Err_DifferentTabForKeyRecAndAttrRec = 4287;
static const int Err_KeyIsNULL = 4316;

/**
 * Context data for negative tests of api extensions.
 */
class NegativeTest {
 public:
  // Static wrapper for each test case.
  static int keyTest(NDBT_Context *ctx, NDBT_Step *step) {
    return NegativeTest(ctx, step).runKeyTest();
  }

  static int graphTest(NDBT_Context *ctx, NDBT_Step *step) {
    return NegativeTest(ctx, step).runGraphTest();
  }

  static int setBoundTest(NDBT_Context *ctx, NDBT_Step *step) {
    return NegativeTest(ctx, step).runSetBoundTest();
  }

  static int valueTest(NDBT_Context *ctx, NDBT_Step *step) {
    return NegativeTest(ctx, step).runValueTest();
  }

  static int featureDisabledTest(NDBT_Context *ctx, NDBT_Step *step) {
    return NegativeTest(ctx, step).runFeatureDisabledTest();
  }

 private:
  Ndb *m_ndb;
  NdbDictionary::Dictionary *m_dictionary;
  const NdbDictionary::Table *m_nt1Tab;
  const NdbDictionary::Index *m_nt1OrdIdx;
  const NdbDictionary::Index *m_nt1UnqIdx;
  const NdbDictionary::Table *m_nt2Tab;
  const NdbDictionary::Index *m_nt2OrdIdx;
  const NdbDictionary::Index *m_nt2UnqIdx;

  NegativeTest(NDBT_Context *ctx, NDBT_Step *step);

  // Tests
  int runKeyTest() const;
  int runGraphTest() const;
  int runSetBoundTest() const;
  int runValueTest() const;
  int runFeatureDisabledTest() const;
  // No copy.
  NegativeTest(const NegativeTest &);
  NegativeTest &operator=(const NegativeTest &);
};

NegativeTest::NegativeTest(NDBT_Context *ctx, NDBT_Step *step) {
  m_ndb = step->getNdb();
  m_dictionary = m_ndb->getDictionary();

  m_nt1Tab = m_dictionary->getTable("nt1");
  require(m_nt1Tab != NULL);

  m_nt1OrdIdx = m_dictionary->getIndex("nt1_oix", "nt1");
  require(m_nt1OrdIdx != NULL);

  m_nt1UnqIdx = m_dictionary->getIndex("nt1_uix", "nt1");
  require(m_nt1UnqIdx != NULL);

  m_nt2Tab = m_dictionary->getTable("nt2");
  require(m_nt2Tab != NULL);

  m_nt2OrdIdx = m_dictionary->getIndex("nt2_oix", "nt2");
  require(m_nt2OrdIdx != NULL);

  m_nt2UnqIdx = m_dictionary->getIndex("nt2_uix", "nt2");
  require(m_nt2UnqIdx != NULL);
}

int NegativeTest::runKeyTest() const {
  // Make key with too long strings
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const char *longTxt = "x012345678901234567890123456789";
    const NdbQueryOperand *const keyOperands[] = {
        builder->constValue(longTxt), builder->constValue(longTxt), NULL};

    if (builder->readTuple(m_nt2Tab, keyOperands) != NULL ||
        builder->getNdbError().code != QRY_CHAR_OPERAND_TRUNCATED) {
      g_err << "Lookup with truncated char values gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Make key with integer value outside column range.
  if (false)  // Temporarily disabled.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const keyOperands[] = {
        builder->constValue(1ull), builder->constValue(~0ull), NULL};

    if (builder->readTuple(m_nt1Tab, keyOperands) != NULL ||
        builder->getNdbError().code != QRY_NUM_OPERAND_RANGE) {
      g_err << "Lookup with integer value outside column range gave unexpected "
               "result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Make key with too few fields
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const keyOperands[] = {builder->constValue(1), NULL};

    if (builder->readTuple(m_nt1Tab, keyOperands) != NULL ||
        builder->getNdbError().code != QRY_TOO_FEW_KEY_VALUES) {
      g_err << "Read with too few key values gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Make key with too many fields
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const keyOperands[] = {builder->constValue(1),
                                                  builder->constValue(1),
                                                  builder->constValue(1), NULL};

    if (builder->readTuple(m_nt1Tab, keyOperands) != NULL ||
        builder->getNdbError().code != QRY_TOO_MANY_KEY_VALUES) {
      g_err << "Read with too many key values gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Make key with fields of wrong type.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const keyOperands[] = {
        builder->constValue(1), builder->constValue("xxx"), NULL};

    if (builder->readTuple(m_nt1Tab, keyOperands) != NULL ||
        builder->getNdbError().code != QRY_OPERAND_HAS_WRONG_TYPE) {
      g_err << "Read with key values of wrong type gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Make key with unknown column. Try preparing failed NdbQueryBuilder.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const keyOperands[] = {builder->constValue(1),
                                                  builder->constValue(1), NULL};

    const NdbQueryLookupOperationDef *parentOperation =
        builder->readTuple(m_nt1Tab, keyOperands);
    require(parentOperation != NULL);

    if (builder->linkedValue(parentOperation, "unknown_col") != NULL ||
        builder->getNdbError().code != Err_UnknownColumn) {
      g_err << "Link to unknown column gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    if (builder->prepare(m_ndb) != NULL) {
      g_err << "prepare() on failed query gave non-NULL result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Give too few parameter values.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const keyOperands[] = {builder->paramValue(),
                                                  builder->paramValue(), NULL};

    require(builder->readTuple(m_nt1Tab, keyOperands) != NULL);
    const NdbQueryDef *const queryDef = builder->prepare(m_ndb);
    require(queryDef != NULL);
    builder->destroy();

    const NdbQueryParamValue params[] = {Uint32(1), NdbQueryParamValue()};

    NdbTransaction *const trans = m_ndb->startTransaction();
    NdbQuery *const query = trans->createQuery(queryDef, params);

    if (query != NULL || trans->getNdbError().code != Err_KeyIsNULL) {
      g_err << "Read with too few parameter values gave unexpected result.";
      m_ndb->closeTransaction(trans);
      queryDef->destroy();
      return NDBT_FAILED;
    }
    m_ndb->closeTransaction(trans);
    queryDef->destroy();
  }

  /**
   * Check for too many parameter values currently not possible. Must decide if
   * NdbQueryParamValue with m_type==Type_NULL should be mandatory end marker or
   * used for specifying actual null values.
   */
  return NDBT_OK;
}  // NegativeTest::runKeyTest()

int NegativeTest::runGraphTest() const {
  // Try preparing empty NdbQueryBuilder
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    if (builder->prepare(m_ndb) != NULL ||
        builder->getNdbError().code != QRY_HAS_ZERO_OPERATIONS) {
      g_err << "prepare() on empty query gave non-NULL result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Make query with too many operations.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const keyOperands[] = {builder->constValue(1),
                                                  builder->constValue(1), NULL};

    const NdbQueryLookupOperationDef *const parentOperation =
        builder->readTuple(m_nt1Tab, keyOperands);
    require(parentOperation != NULL);

    const NdbQueryOperand *const childOperands[] = {
        builder->linkedValue(parentOperation, "ui1"),
        builder->linkedValue(parentOperation, "oi1"), NULL};

    for (Uint32 i = 0; i < 32; i++) {
      const NdbQueryLookupOperationDef *const childOperation =
          builder->readTuple(m_nt1Tab, childOperands);
      if (i < 31) {
        require(childOperation != NULL);
      } else if (childOperation != NULL &&
                 builder->getNdbError().code != QRY_DEFINITION_TOO_LARGE) {
        g_err << "Building query with too many operations gave unexpected "
                 "result.";
        builder->destroy();
        return NDBT_FAILED;
      }
    }
    builder->destroy();
  }

  // Make query with two root operations.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const keyOperands[] = {builder->constValue(1),
                                                  builder->constValue(1), NULL};

    const NdbQueryLookupOperationDef *const root1 =
        builder->readTuple(m_nt1Tab, keyOperands);
    require(root1 != NULL);

    if (builder->readTuple(m_nt1Tab, keyOperands) != NULL ||
        builder->getNdbError().code != QRY_UNKNOWN_PARENT) {
      g_err << "Query with two root operations gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    };
    builder->destroy();
  }

  // Try lookup on ordered index.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const keyOperands[] = {builder->constValue(1),
                                                  builder->constValue(1), NULL};

    if (builder->readTuple(m_nt1OrdIdx, m_nt1Tab, keyOperands) != NULL ||
        builder->getNdbError().code != QRY_WRONG_INDEX_TYPE) {
      g_err << "Lookup on ordered index gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Try lookup on index on wrong table.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const keyOperands[] = {builder->constValue(1),
                                                  builder->constValue(1), NULL};

    if (builder->readTuple(m_nt2OrdIdx, m_nt1Tab, keyOperands) != NULL ||
        builder->getNdbError().code != QRY_UNRELATED_INDEX) {
      g_err << "Lookup on unrelated index gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Try scanning unique index.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const boundOperands[] = {builder->constValue(1),
                                                    NULL};
    const NdbQueryIndexBound bound(boundOperands);

    if (builder->scanIndex(m_nt1UnqIdx, m_nt1Tab, &bound) != NULL ||
        builder->getNdbError().code != QRY_WRONG_INDEX_TYPE) {
      g_err << "Scan of unique index gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Try scanning index on wrong table.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const boundOperands[] = {builder->constValue(1),
                                                    NULL};
    const NdbQueryIndexBound bound(boundOperands);

    if (builder->scanIndex(m_nt2OrdIdx, m_nt1Tab, &bound) != NULL ||
        builder->getNdbError().code != QRY_UNRELATED_INDEX) {
      g_err << "Scan of unrelated index gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Try adding a scan child to a lookup root.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const keyOperands[] = {builder->constValue(1),
                                                  builder->constValue(1), NULL};

    const NdbQueryLookupOperationDef *parentOperation =
        builder->readTuple(m_nt1Tab, keyOperands);
    require(parentOperation != NULL);

    const NdbQueryOperand *const childOperands[] = {
        builder->linkedValue(parentOperation, "ui1"),
        builder->linkedValue(parentOperation, "oi1"), NULL};
    const NdbQueryIndexBound bound(childOperands);

    if (builder->scanIndex(m_nt1OrdIdx, m_nt1Tab, &bound) != NULL ||
        builder->getNdbError().code != QRY_WRONG_OPERATION_TYPE) {
      g_err << "Lookup with scan child gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  // Try adding a sorted child scan to a query.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();

    const NdbQueryTableScanOperationDef *parentOperation =
        builder->scanTable(m_nt1Tab);
    require(parentOperation != NULL);

    const NdbQueryOperand *const childOperands[] = {
        builder->linkedValue(parentOperation, "ui1"), NULL};
    const NdbQueryIndexBound bound(childOperands);
    NdbQueryOptions childOptions;
    childOptions.setOrdering(NdbQueryOptions::ScanOrdering_ascending);

    if (builder->scanIndex(m_nt1OrdIdx, m_nt1Tab, &bound, &childOptions) !=
            NULL ||
        builder->getNdbError().code != QRY_MULTIPLE_SCAN_SORTED) {
      g_err << "Query with sorted child scan gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  /**
   * Try adding a child operation with two parents that are not descendants of
   * each other (i.e. a diamond-shaped query graph).
   */
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();
    const NdbQueryOperand *const rootKey[] = {builder->constValue(1),
                                              builder->constValue(1), NULL};

    const NdbQueryLookupOperationDef *rootOperation =
        builder->readTuple(m_nt1Tab, rootKey);
    require(rootOperation != NULL);

    const NdbQueryOperand *const leftKey[] = {
        builder->linkedValue(rootOperation, "ui1"), builder->constValue(1),
        NULL};

    const NdbQueryLookupOperationDef *leftOperation =
        builder->readTuple(m_nt1Tab, leftKey);
    require(leftOperation != NULL);

    const NdbQueryOperand *const rightKey[] = {
        builder->linkedValue(rootOperation, "ui1"), builder->constValue(1),
        NULL};

    const NdbQueryLookupOperationDef *rightOperation =
        builder->readTuple(m_nt1Tab, rightKey);
    require(rightOperation != NULL);

    const NdbQueryOperand *const bottomKey[] = {
        builder->linkedValue(leftOperation, "ui1"),
        builder->linkedValue(rightOperation, "oi1"), NULL};

    if (builder->readTuple(m_nt1Tab, bottomKey) != NULL ||
        builder->getNdbError().code != QRY_MULTIPLE_PARENTS) {
      g_err << "Diamond-shaped query graph gave unexpected result.";
      builder->destroy();
      return NDBT_FAILED;
    }
    builder->destroy();
  }

  return NDBT_OK;
}  // NegativeTest::runGraphTest()

int NegativeTest::runSetBoundTest() const {
  // Test NdbQueryOperation::setBound() with too long string value.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();

    const NdbQueryIndexScanOperationDef *parentOperation =
        builder->scanIndex(m_nt2OrdIdx, m_nt2Tab);
    require(parentOperation != NULL);

    const NdbQueryDef *const queryDef = builder->prepare(m_ndb);
    require(queryDef != NULL);
    builder->destroy();

    NdbTransaction *const trans = m_ndb->startTransaction();
    NdbQuery *const query = trans->createQuery(queryDef);

    // Make bound with too long string.
    const NdbDictionary::RecordSpecification ordIdxRecSpec[] = {
        {m_nt2Tab->getColumn("oi1"), 0, 0, 0, 0}};

    const NdbRecord *const ordIdxRecord = m_dictionary->createRecord(
        m_nt2OrdIdx, ordIdxRecSpec,
        sizeof ordIdxRecSpec / sizeof ordIdxRecSpec[0],
        sizeof(NdbDictionary::RecordSpecification));
    require(ordIdxRecord != NULL);

    struct {
      Uint8 len;
      char data[nt2StrLen + 10];
    } boundRow;
    memset(boundRow.data, 'x', sizeof(boundRow.data));
    // Set string length field.
    boundRow.len = nt2StrLen + 10;

    NdbIndexScanOperation::IndexBound bound = {
        reinterpret_cast<const char *>(&boundRow),
        1,
        true,
        reinterpret_cast<const char *>(&boundRow),
        1,
        true,
        0};

    if (query->setBound(ordIdxRecord, &bound) == 0 ||
        query->getNdbError().code != Err_WrongFieldLength) {
      g_err << "Scan bound with too long string value gave unexpected result.";
      m_ndb->closeTransaction(trans);
      queryDef->destroy();
      return NDBT_FAILED;
    }

    // Set correct string length.
    boundRow.len = nt2StrLen;
    bound.range_no = 1;
    if (query->setBound(ordIdxRecord, &bound) == 0 ||
        query->getNdbError().code != QRY_ILLEGAL_STATE) {
      g_err << "setBound() in failed state gave unexpected result.";
      m_ndb->closeTransaction(trans);
      queryDef->destroy();
      return NDBT_FAILED;
    }

    m_ndb->closeTransaction(trans);
    queryDef->destroy();
  }

  // Test NdbQueryOperation::setBound() with wrong bound no.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();

    const NdbQueryIndexScanOperationDef *parentOperation =
        builder->scanIndex(m_nt1OrdIdx, m_nt1Tab);
    require(parentOperation != NULL);

    const NdbQueryDef *const queryDef = builder->prepare(m_ndb);
    require(queryDef != NULL);
    builder->destroy();

    NdbTransaction *const trans = m_ndb->startTransaction();
    NdbQuery *const query = trans->createQuery(queryDef);

    const int boundRow[] = {1, 1};

    // Make bound with wrong bound no.
    NdbIndexScanOperation::IndexBound bound = {
        reinterpret_cast<const char *>(boundRow),
        1,
        true,
        reinterpret_cast<const char *>(boundRow),
        1,
        true,
        1 /*Should be 0.*/};

    if (query->setBound(m_nt1OrdIdx->getDefaultRecord(), &bound) == 0 ||
        query->getNdbError().code != Err_InvalidRangeNo) {
      g_err << "Scan bound with wrong range no gave unexpected result.";
      m_ndb->closeTransaction(trans);
      queryDef->destroy();
      return NDBT_FAILED;
    }

    m_ndb->closeTransaction(trans);
    queryDef->destroy();
  }

  // Test NdbQueryOperation::setBound() on table scan.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();

    const NdbQueryTableScanOperationDef *parentOperation =
        builder->scanTable(m_nt1Tab);
    require(parentOperation != NULL);

    const NdbQueryDef *const queryDef = builder->prepare(m_ndb);
    require(queryDef != NULL);
    builder->destroy();

    NdbTransaction *const trans = m_ndb->startTransaction();
    NdbQuery *const query = trans->createQuery(queryDef);

    const int boundRow[] = {1, 1};

    NdbIndexScanOperation::IndexBound bound = {
        reinterpret_cast<const char *>(boundRow),
        1,
        true,
        reinterpret_cast<const char *>(boundRow),
        1,
        true,
        0};

    if (query->setBound(m_nt1OrdIdx->getDefaultRecord(), &bound) == 0 ||
        query->getNdbError().code != QRY_WRONG_OPERATION_TYPE) {
      g_err << "Scan bound on table scan gave unexpected result.";
      m_ndb->closeTransaction(trans);
      queryDef->destroy();
      return NDBT_FAILED;
    }

    m_ndb->closeTransaction(trans);
    queryDef->destroy();
  }

  // Test NdbQueryOperation::setBound() in executed query.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();

    const NdbQueryIndexScanOperationDef *parentOperation =
        builder->scanIndex(m_nt1OrdIdx, m_nt1Tab);
    require(parentOperation != NULL);

    const NdbQueryDef *const queryDef = builder->prepare(m_ndb);
    require(queryDef != NULL);
    builder->destroy();

    NdbTransaction *const trans = m_ndb->startTransaction();
    NdbQuery *const query = trans->createQuery(queryDef);

    const char *resultRow;

    require(query->getQueryOperation(0u)->setResultRowRef(
                m_nt1Tab->getDefaultRecord(), resultRow, NULL) == 0);

    require(trans->execute(NoCommit) == 0);

    const int boundRow[] = {1, 1};

    // Add bound now.
    NdbIndexScanOperation::IndexBound bound = {
        reinterpret_cast<const char *>(boundRow),
        1,
        true,
        reinterpret_cast<const char *>(boundRow),
        1,
        true,
        0};

    if (query->setBound(m_nt1OrdIdx->getDefaultRecord(), &bound) == 0 ||
        query->getNdbError().code != QRY_ILLEGAL_STATE) {
      g_err << "Adding scan bound to executed query gave unexpected result.";
      m_ndb->closeTransaction(trans);
      queryDef->destroy();
      return NDBT_FAILED;
    }

    m_ndb->closeTransaction(trans);
    queryDef->destroy();
  }

  return NDBT_OK;
}  // NegativeTest::runSetBoundTest()

int NegativeTest::runValueTest() const {
  // Test NdbQueryOperation::getValue() on an unknown column.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();

    const NdbQueryTableScanOperationDef *parentOperation =
        builder->scanTable(m_nt1Tab);
    require(parentOperation != NULL);

    const NdbQueryDef *const queryDef = builder->prepare(m_ndb);
    require(queryDef != NULL);
    builder->destroy();

    NdbTransaction *const trans = m_ndb->startTransaction();
    NdbQuery *const query = trans->createQuery(queryDef);

    if (query->getQueryOperation(0u)->getValue("unknownCol") != NULL ||
        query->getNdbError().code != Err_UnknownColumn) {
      g_err << "NdbQueryOperation::getValue() on unknown column gave "
               "unexpected result.";
      m_ndb->closeTransaction(trans);
      queryDef->destroy();
      return NDBT_FAILED;
    }

    m_ndb->closeTransaction(trans);
    queryDef->destroy();
  }

  // Try fetching results with an NdbRecord for a different table.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();

    const NdbQueryTableScanOperationDef *parentOperation =
        builder->scanTable(m_nt1Tab);
    require(parentOperation != NULL);

    const NdbQueryDef *const queryDef = builder->prepare(m_ndb);
    require(queryDef != NULL);
    builder->destroy();

    NdbTransaction *const trans = m_ndb->startTransaction();
    NdbQuery *const query = trans->createQuery(queryDef);

    const char *resultRow;

    if (query->getQueryOperation(0u)->setResultRowRef(
            m_nt2Tab->getDefaultRecord(), resultRow, NULL) == 0 ||
        query->getNdbError().code != Err_DifferentTabForKeyRecAndAttrRec) {
      g_err << "NdbQueryOperation::setResultRowRef() on wrong table gave "
               "unexpected "
               "result.";
      m_ndb->closeTransaction(trans);
      queryDef->destroy();
      return NDBT_FAILED;
    }

    m_ndb->closeTransaction(trans);
    queryDef->destroy();
  }

  // Try defining result row twice.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();

    const NdbQueryTableScanOperationDef *parentOperation =
        builder->scanTable(m_nt1Tab);
    require(parentOperation != NULL);

    const NdbQueryDef *const queryDef = builder->prepare(m_ndb);
    require(queryDef != NULL);
    builder->destroy();

    NdbTransaction *const trans = m_ndb->startTransaction();
    NdbQuery *const query = trans->createQuery(queryDef);

    const char *resultRow;

    require(query->getQueryOperation(0u)->setResultRowRef(
                m_nt1Tab->getDefaultRecord(), resultRow, NULL) == 0);

    if (query->getQueryOperation(0u)->setResultRowRef(
            m_nt1Tab->getDefaultRecord(), resultRow, NULL) == 0 ||
        query->getNdbError().code != QRY_RESULT_ROW_ALREADY_DEFINED) {
      g_err << "Defining result row twice gave unexpected result.";
      m_ndb->closeTransaction(trans);
      queryDef->destroy();
      return NDBT_FAILED;
    }

    m_ndb->closeTransaction(trans);
    queryDef->destroy();
  }

  // Test operation with empty projection.
  {
    NdbQueryBuilder *const builder = NdbQueryBuilder::create();

    const NdbQueryIndexScanOperationDef *parentOperation =
        builder->scanIndex(m_nt1OrdIdx, m_nt1Tab);
    require(parentOperation != NULL);

    const NdbQueryDef *const queryDef = builder->prepare(m_ndb);
    require(queryDef != NULL);
    builder->destroy();

    NdbTransaction *const trans = m_ndb->startTransaction();
    NdbQuery *const query = trans->createQuery(queryDef);

    // Execute without defining a projection.
    if (trans->execute(NoCommit) == 0 ||
        query->getNdbError().code != QRY_EMPTY_PROJECTION) {
      g_err << "Having operation with empty projection gave unexpected result.";
      m_ndb->closeTransaction(trans);
      queryDef->destroy();
      return NDBT_FAILED;
    }

    m_ndb->closeTransaction(trans);
    queryDef->destroy();
  }
  return NDBT_OK;
}  // NegativeTest::runValueBoundTest()

/**
 * Check that query pushdown is disabled in older versions of the code
 * (even if the API extensions are present in the code).
 */
int NegativeTest::runFeatureDisabledTest() const {
  NdbQueryBuilder *const builder = NdbQueryBuilder::create();

  const NdbQueryTableScanOperationDef *const parentOperation =
      builder->scanTable(m_nt1Tab);

  int result = NDBT_OK;

  {
    if (parentOperation == NULL) {
      g_err << "scanTable() failed: " << builder->getNdbError() << endl;
      result = NDBT_FAILED;
    } else {
      g_info << "scanTable() succeeded in version " << ndbGetOwnVersionString()
             << " as expected." << endl;
    }
  }

  builder->destroy();
  return result;
}  // NegativeTest::runFeatureDisabledTest()

static int dropNegativeSchema(NDBT_Context *ctx, NDBT_Step *step) {
  NdbDictionary::Dictionary *const dictionary = step->getNdb()->getDictionary();

  if (dictionary->dropTable("nt1") != 0) {
    g_err << "Failed to drop table nt1." << endl;
    return NDBT_FAILED;
  }
  if (dictionary->dropTable("nt2") != 0) {
    g_err << "Failed to drop table nt2." << endl;
    return NDBT_FAILED;
  }
  return NDBT_OK;
}

/**
 * Robustness tests: DBSPJ error inserts with a checked outcome, followed by
 * leak checks.
 *
 * LookupJoinError and ScanJoinError accept any outcome of an error insert
 * and check nothing afterwards. The tests below require that the query
 * either succeeds with the row counts it has without the insert, or fails
 * with one of the NDB errors expected for the insert, and that nothing is
 * left behind afterwards:
 *  - the DIH scan registrations of the test table and its indexes
 *    (ndbinfo table_distribution_status_all, current_scan_count +
 *    scan_count_wait) return to their level before the query,
 *  - the open fragment scans on them (ndbinfo frag_operations,
 *    conc_frag_scans) return to their level before the query,
 *  - DUMP 2650 (DBSPJ requests) and DUMP 2560 (DBTC join-aggregation
 *    records and closing scans) stop no data node: they abort the node on
 *    a leak.
 */
class SpjLeakCheck {
 public:
  SpjLeakCheck(NDBT_Context *ctx, NDBT_Step *step)
      : m_ndbinfo(&ctx->m_cluster_connection, "ndbinfo/"),
        m_ndb(GETNDB(step)),
        m_tab(ctx->getTab()) {}

  bool init() {
    if (!m_ndbinfo.init()) {
      g_err << "SpjLeakCheck: NdbInfo::init() failed" << endl;
      return false;
    }
    m_tableIds.clear();
    m_tableIds.push_back(m_tab->getObjectId());
    NdbDictionary::Dictionary::List list;
    if (m_ndb->getDictionary()->listIndexes(list, m_tab->getName()) != 0) {
      g_err << "SpjLeakCheck: listIndexes(" << m_tab->getName()
            << ") failed: " << m_ndb->getDictionary()->getNdbError() << endl;
      return false;
    }
    for (unsigned i = 0; i < list.count; i++) {
      m_tableIds.push_back(list.elements[i].id);
    }
    return true;
  }

  /* DIH scans registered on the test table and its indexes, all nodes */
  bool dihScans(Uint64 &sum) {
    return sumColumns("ndbinfo/table_distribution_status_all",
                      "current_scan_count", "scan_count_wait", sum);
  }

  /* Fragment scans open on the test table and its indexes, all nodes */
  bool fragScans(Uint64 &sum) {
    return sumColumns("ndbinfo/frag_operations", "conc_frag_scans", nullptr,
                      sum);
  }

  /**
   * Wait at most 'maxMillis' for the DIH scans (dih == true) or the open
   * fragment scans to come back to 'base' or below. 'last' is the last
   * value read. Returns false if ndbinfo could not be read.
   */
  bool waitBack(bool dih, Uint64 base, Uint32 maxMillis, Uint64 &last) {
    Uint32 waited = 0;
    while (true) {
      if (!(dih ? dihScans(last) : fragScans(last))) return false;
      if (last <= base || waited >= maxMillis) return true;
      NdbSleep_MilliSleep(200);
      waited += 200;
    }
  }

 private:
  bool isTestTable(Uint32 tableId) const {
    for (unsigned i = 0; i < m_tableIds.size(); i++) {
      if (m_tableIds[i] == tableId) return true;
    }
    return false;
  }

  bool sumColumns(const char *tableName, const char *col1, const char *col2,
                  Uint64 &sum) {
    sum = 0;
    const NdbInfo::Table *table = nullptr;
    if (m_ndbinfo.openTable(tableName, &table) != 0) {
      g_err << "SpjLeakCheck: openTable(" << tableName << ") failed" << endl;
      return false;
    }
    NdbInfoScanOperation *scanOp = nullptr;
    if (m_ndbinfo.createScanOperation(table, &scanOp) != 0) {
      g_err << "SpjLeakCheck: createScanOperation(" << tableName
            << ") failed" << endl;
      m_ndbinfo.closeTable(table);
      return false;
    }
    bool ok = false;
    do {
      if (scanOp->readTuples() != 0) break;
      const NdbInfoRecAttr *tableId = scanOp->getValue("table_id");
      const NdbInfoRecAttr *val1 = scanOp->getValue(col1);
      const NdbInfoRecAttr *val2 =
          (col2 != nullptr) ? scanOp->getValue(col2) : nullptr;
      if (tableId == nullptr || val1 == nullptr ||
          (col2 != nullptr && val2 == nullptr))
        break;
      if (scanOp->execute() != 0) break;
      int ret;
      while ((ret = scanOp->nextResult()) == 1) {
        if (tableId->isNULL() || !isTestTable(tableId->u_32_value())) continue;
        if (!val1->isNULL()) sum += val1->u_32_value();
        if (val2 != nullptr && !val2->isNULL()) sum += val2->u_32_value();
      }
      ok = (ret == 0);
    } while (0);
    if (!ok) {
      g_err << "SpjLeakCheck: scan of " << tableName << " failed" << endl;
    }
    m_ndbinfo.releaseScanOperation(scanOp);
    m_ndbinfo.closeTable(table);
    return ok;
  }

  NdbInfo m_ndbinfo;
  Ndb *m_ndb;
  const NdbDictionary::Table *m_tab;
  Vector<Uint32> m_tableIds;
};

static void spjGetConnectCounts(NdbRestarter &restarter, Vector<int> &counts) {
  counts.clear();
  for (int i = 0; i < restarter.getNumDbNodes(); i++) {
    counts.push_back(restarter.getNodeConnectCount(restarter.getDbNodeId(i)));
  }
}

/* True if every data node is still started and has not reconnected */
static bool spjNodesUnchanged(NdbRestarter &restarter,
                              const Vector<int> &connectCounts) {
  for (int i = 0; i < restarter.getNumDbNodes(); i++) {
    const int nodeId = restarter.getDbNodeId(i);
    if (restarter.getNodeStatus(nodeId) != NDB_MGM_NODE_STATUS_STARTED ||
        restarter.getNodeConnectCount(nodeId) != connectCounts[i]) {
      g_err << "Data node " << nodeId << " has stopped or restarted" << endl;
      return false;
    }
  }
  return true;
}

/**
 * DUMP 2650 (DBSPJ: no scan or lookup request left) and DUMP 2560 (DBTC:
 * no join-aggregation record, no scan closing) abort a data node that
 * finds a leak. Only valid on an idle cluster.
 */
static bool spjDumpLeakCheck(NdbRestarter &restarter,
                             const Vector<int> &connectCounts) {
  const int dumpSpj[] = {DumpStateOrd::SpjDumpRequests};
  const int dumpTc[] = {DumpStateOrd::TcDumpJoinAggRecords};
  if (restarter.dumpStateAllNodes(dumpSpj, 1) != 0 ||
      restarter.dumpStateAllNodes(dumpTc, 1) != 0) {
    g_err << "DUMP 2650 / 2560 could not be sent" << endl;
    return false;
  }
  NdbSleep_MilliSleep(2000);
  if (!spjNodesUnchanged(restarter, connectCounts)) {
    g_err << "A data node stopped on DUMP 2650 / 2560: a DBSPJ request or a "
             "DBTC scan was leaked (see the node's out log)"
          << endl;
    return false;
  }
  return true;
}

struct SpjFaultSpec {
  int code;
  /**
   * NDB errors the query may fail with when the insert fires, 0 terminated.
   * {0}: the query must not fail. {-1}: any error but a time-out or a node
   * failure. An insert that does not fire lets the query succeed.
   */
  int accepted[4];
};

/* Existing DBSPJ error inserts (as in LookupJoinError / ScanJoinError) */
static const SpjFaultSpec spjExistingFaults[] = {
    {17001, {20008}},
    {17002, {20008}},
    {17003, {20015}},
    {17004, {20008}},
    {17005, {20000}},
    {17006, {20003}},
    {17008, {20008}},
    {17009, {20008}},
    {17010, {20008}},
    {17012, {20008}},  // R-2: DIH scan count leaked
    {17013, {20002}},
    {17020, {20016}},
    {17021, {20016}},
    {17022, {20016}},
    {17030, {1227, 241, 20021}},
    {17031, {1227, 241, 20021}},
    {17032, {1227, 241, 20021}},
    {17040, {20008}},
    {17041, {20008}},
    {17042, {20008}},
    {17043, {20008}},
    {17050, {20006}},
    {17051, {20006}},
    {17052, {20006}},
    {17053, {20006}},
    {17060, {20006}},
    {17061, {20006}},
    {17062, {20006}},
    {17063, {20006}},
    {17070, {20006}},
    {17071, {20006}},
    {17072, {20006}},
    {17080, {20006}},
    {17081, {20006}},
    {17082, {20006}},
    {17090, {20006}},
    {17091, {20006}},  // S-19: may leave DBLQH fragment scans open
    {17092, {20006}},
    {17093, {20006}},
    {17110, {1227, 241, 20021}},
    {17111, {1227, 241, 20021}},
    {17112, {1227, 241, 20021}},
    {17120, {20015}},
    {17121, {20015}},
    {17122, {20015}},
    {17130, {20006}},
    {17131, {0}},  // Only spreads DIGETNODESREQ over CONTINUEB rounds
    {17510, {20006, 20008, 20015}},
    {17520, {20019}},
    {17521, {20019}},
    {17534, {20008}}};

/* Error inserts added for the robustness tests (R-n / S-n / P-n) */
static const SpjFaultSpec spjNewFaults[] = {
    {17535, {20006}},  // R-4/R-5/R-4b: abort in the first child batch
    {17538, {311}},    // R-6: DIGETNODESREQ error code
    {17539, {311}},    // R-3: DIGETNODESREQ error in a CONTINUEB round
    {17540, {20008}},  // P-19: buildExecPlan() error ignored
    {17541, {-1}}};    // S-2: DIH_SCAN_TAB_REF stalls until time-out

static bool spjErrorAccepted(const SpjFaultSpec &fault, int code) {
  if (fault.accepted[0] == -1) {
    static const int notAccepted[] = {266,  274,  296,  297,  4008,
                                      4010, 4012, 4025, 4028, 20016};
    for (unsigned i = 0; i < NDB_ARRAY_SIZE(notAccepted); i++) {
      if (code == notAccepted[i]) return false;
    }
    return true;
  }
  for (unsigned i = 0; i < NDB_ARRAY_SIZE(fault.accepted); i++) {
    if (fault.accepted[i] == 0) break;
    if (fault.accepted[i] == code) return true;
  }
  return false;
}

static int runSpjFaultLeakCheckList(NDBT_Context *ctx, NDBT_Step *step,
                                    const SpjFaultSpec *faults,
                                    Uint32 faultCount) {
  Ndb *ndb = GETNDB(step);
  NdbRestarter restarter;
  if (restarter.waitClusterStarted() != 0) {
    g_err << "Cluster is not started" << endl;
    return NDBT_FAILED;
  }
  SpjLeakCheck leaks(ctx, step);
  if (!leaks.init()) return NDBT_FAILED;

  HugoQueryBuilder qbScan(ndb, ctx->getTab(), HugoQueryBuilder::O_SCAN);
  HugoQueryBuilder qbLookup(ndb, ctx->getTab(), HugoQueryBuilder::O_LOOKUP);
  qbScan.setJoinLevel(3);
  qbLookup.setJoinLevel(3);
  const NdbQueryDef *queryScan = qbScan.createQuery();
  const NdbQueryDef *queryLookup = qbLookup.createQuery();
  if (queryScan == nullptr || queryLookup == nullptr) {
    g_err << "Failed to create NdbQueryDefs" << endl;
    return NDBT_FAILED;
  }
  // No retries: the first error is the outcome of the error insert
  HugoQueries hugoScan(*queryScan, 1);
  HugoQueries hugoLookup(*queryLookup, 1);
  const int lookups = 16;

  // Reference row counts without error inserts, no random close
  if (hugoScan.runScanQuery(ndb, 0) != NDBT_OK ||
      hugoLookup.runLookupQuery(ndb, lookups) != NDBT_OK) {
    g_err << "Reference queries failed" << endl;
    return NDBT_FAILED;
  }
  const Vector<Uint32> refScan(hugoScan.m_rows_found);
  const Vector<Uint32> refLookup(hugoLookup.m_rows_found);

  Uint64 dihBase = 0;
  Uint64 fragBase = 0;
  if (!leaks.waitBack(true, 0, 20000, dihBase) ||
      !leaks.waitBack(false, 0, 60000, fragBase)) {
    return NDBT_FAILED;
  }
  if (dihBase != 0 || fragBase != 0) {
    g_err << "Idle cluster has " << dihBase << " DIH scans and " << fragBase
          << " open fragment scans on the test table" << endl;
    return NDBT_FAILED;
  }
  Vector<int> connectCounts;
  spjGetConnectCounts(restarter, connectCounts);

  int result = NDBT_OK;
  for (Uint32 f = 0; f < faultCount && !ctx->isTestStopped(); f++) {
    const SpjFaultSpec &fault = faults[f];
    if (faultToInject != 0 && fault.code != faultToInject) continue;

    for (int queryType = 0; queryType < 2; queryType++) {
      const bool isScan = (queryType == 0);
      const char *what = isScan ? "scan" : "lookup";
      HugoQueries &hugo = isScan ? hugoScan : hugoLookup;
      const Vector<Uint32> &ref = isScan ? refScan : refLookup;

      ndbout << "SpjFaultLeakCheck: error " << fault.code << ", " << what
             << " query" << endl;
      if (restarter.insertErrorInAllNodes(fault.code) != 0) {
        g_err << "Could not insert error " << fault.code << endl;
        return NDBT_FAILED;
      }
      const int res = isScan ? hugo.runScanQuery(ndb, 0)
                             : hugo.runLookupQuery(ndb, lookups);
      const int errCode = hugo.getNdbError().code;
      if (restarter.insertErrorInAllNodes(0) != 0) {
        g_err << "Could not clear error " << fault.code << endl;
        return NDBT_FAILED;
      }

      if (res != NDBT_OK) {
        if (!spjErrorAccepted(fault, errCode)) {
          g_err << "Error " << fault.code << ", " << what
                << " query: unexpected error " << hugo.getNdbError() << endl;
          result = NDBT_FAILED;
        }
      } else if (!hugo.m_rows_found.equal(ref)) {
        g_err << "Error " << fault.code << ", " << what
              << " query: succeeded with wrong row counts" << endl;
        result = NDBT_FAILED;
      }

      if (!spjNodesUnchanged(restarter, connectCounts)) {
        g_err << "Error " << fault.code << ", " << what
              << " query: a data node failed" << endl;
        return NDBT_FAILED;
      }

      Uint64 last = 0;
      if (!leaks.waitBack(true, dihBase, 5000, last)) return NDBT_FAILED;
      if (last > dihBase) {
        g_err << "Error " << fault.code << ", " << what
              << " query: DIH scans leaked, " << dihBase << " -> " << last
              << endl;
        result = NDBT_FAILED;
      }
      dihBase = last;
      if (!leaks.waitBack(false, fragBase, 15000, last)) return NDBT_FAILED;
      if (last > fragBase) {
        g_err << "Error " << fault.code << ", " << what
              << " query: fragment scans left open, " << fragBase << " -> "
              << last << endl;
        result = NDBT_FAILED;
      }
      fragBase = last;
    }

    if (!spjDumpLeakCheck(restarter, connectCounts)) {
      g_err << "Leak found by DUMP after error " << fault.code << endl;
      return NDBT_FAILED;
    }
  }
  restarter.insertErrorInAllNodes(0);
  return result;
}

int runSpjFaultLeakCheck(NDBT_Context *ctx, NDBT_Step *step) {
  return runSpjFaultLeakCheckList(ctx, step, spjExistingFaults,
                                  NDB_ARRAY_SIZE(spjExistingFaults));
}

int runSpjNewFaultLeakCheck(NDBT_Context *ctx, NDBT_Step *step) {
  return runSpjFaultLeakCheckList(ctx, step, spjNewFaults,
                                  NDB_ARRAY_SIZE(spjNewFaults));
}

/**
 * Leak checks on an idle cluster after a test, e.g. after node restarts
 * during pushed joins (NF_JoinLeakCheck): no DIH scan registered and no
 * fragment scan open on the test table, no DBSPJ request / DBTC scan left
 * (DUMP 2650 / 2560), and pushed joins work.
 */
int runSpjIdleLeakCheck(NDBT_Context *ctx, NDBT_Step *step) {
  NdbRestarter restarter;
  if (restarter.waitClusterStarted() != 0) {
    g_err << "Cluster is not started" << endl;
    return NDBT_FAILED;
  }
  SpjLeakCheck leaks(ctx, step);
  if (!leaks.init()) return NDBT_FAILED;

  Uint64 dih = 0;
  Uint64 frag = 0;
  if (!leaks.waitBack(true, 0, 30000, dih) ||
      !leaks.waitBack(false, 0, 60000, frag)) {
    return NDBT_FAILED;
  }
  int result = NDBT_OK;
  if (dih != 0) {
    g_err << dih << " DIH scans are still registered on the test table"
          << endl;
    result = NDBT_FAILED;
  }
  if (frag != 0) {
    g_err << frag << " fragment scans are still open on the test table"
          << endl;
    result = NDBT_FAILED;
  }

  Vector<int> connectCounts;
  spjGetConnectCounts(restarter, connectCounts);
  if (!spjDumpLeakCheck(restarter, connectCounts)) return NDBT_FAILED;

  Ndb *ndb = GETNDB(step);
  HugoQueryBuilder qbScan(ndb, ctx->getTab(), HugoQueryBuilder::O_SCAN);
  HugoQueryBuilder qbLookup(ndb, ctx->getTab(), HugoQueryBuilder::O_LOOKUP);
  qbScan.setJoinLevel(3);
  qbLookup.setJoinLevel(3);
  const NdbQueryDef *queryScan = qbScan.createQuery();
  const NdbQueryDef *queryLookup = qbLookup.createQuery();
  if (queryScan == nullptr || queryLookup == nullptr) {
    g_err << "Failed to create NdbQueryDefs" << endl;
    return NDBT_FAILED;
  }
  HugoQueries hugoScan(*queryScan);
  HugoQueries hugoLookup(*queryLookup);
  if (hugoScan.runScanQuery(ndb, 0) != NDBT_OK ||
      hugoLookup.runLookupQuery(ndb, 16) != NDBT_OK) {
    g_err << "Pushed joins fail after the test" << endl;
    result = NDBT_FAILED;
  }
  return result;
}

NDBT_TESTSUITE(testSpj);
TESTCASE("NegativeJoin", "") {
  INITIALIZER(createNegativeSchema);
  INITIALIZER(NegativeTest::keyTest);
  INITIALIZER(NegativeTest::graphTest);
  INITIALIZER(NegativeTest::setBoundTest);
  INITIALIZER(NegativeTest::valueTest);
  FINALIZER(dropNegativeSchema);
}
TESTCASE("FeatureDisabled", "") {
  INITIALIZER(createNegativeSchema);
  INITIALIZER(NegativeTest::featureDisabledTest);
  FINALIZER(dropNegativeSchema);
}
TESTCASE("LookupJoin", "") {
  INITIALIZER(runLoadTable);
  STEP(runLookupJoin);
  VERIFIER(runClearTable);
}
TESTCASE("ScanJoin", "") {
  INITIALIZER(runLoadTable);
  STEP(runScanJoin);
  FINALIZER(runClearTable);
}
TESTCASE("MixedJoin", "") {
  INITIALIZER(runLoadTable);
  STEPS(runJoin, 6);
  FINALIZER(runClearTable);
}
TESTCASE("MixedJoin17131", "Simulate CONTINUEB for DIGETNODESREQ") {
  INITIALIZER(runLoadTable);
  TC_PROPERTY("ErrorCode", 17131);
  STEPS(runJoin, 6);
  FINALIZER(runClearTable);
}
TESTCASE("MixedJoinDiskWait", "Simulate disk wait during pushed joins") {
  INITIALIZER(runLoadTable);
  TC_PROPERTY("ErrorCode", 4035);
  STEPS(runJoin, 4);
  FINALIZER(runClearTable);
}
TESTCASE("MultiFrag_OOM",
         "'Out of LongMessageBuffer' during 'import' of MultiFrag list") {
  INITIALIZER(runLoadTable);
  TC_PROPERTY("ErrorCode", 8116);
  TC_PROPERTY("AcceptError", 218);
  STEP(runJoin);
  FINALIZER(runClearTable);
}
TESTCASE(
    "MultiFrag_OOM_rand",
    "Random 'Out of LongMessageBuffer' during 'import' of MultiFrag list") {
  INITIALIZER(runLoadTable);
  TC_PROPERTY("ErrorCode", 8117);
  TC_PROPERTY("AcceptError", 218);
  STEP(runJoin);
  FINALIZER(runClearTable);
}
TESTCASE("NF_Join", "") {
  TC_PROPERTY("UntilStopped", 1);
  TC_PROPERTY("WaitProgress", 20);
  INITIALIZER(runLoadTable);
  // STEPS(runScanJoin, 6);
  // STEPS(runLookupJoin, 6);
  STEPS(runJoin, 6);
  STEP(runRestarter);
  FINALIZER(runClearTable);
}
TESTCASE("bug#23049170",
         "Test abort() when partially connected after a node restart. "
         "Should not see misreported 'NodeFailure' error (20016) due to "
         "bug#23049170") {
  TC_PROPERTY("UntilStopped", 1);
  TC_PROPERTY("WaitProgress", 20);
  TC_PROPERTY("RestartWithErrorCode", 17530);  // OJA, note -> restarter set it
  INITIALIZER(runLoadTable);
  STEPS(runAbortedJoin, 6);
  STEP(runRestarter);
  FINALIZER(runClearTable);
}
TESTCASE("bug#23048816",
         "Test handling of out of section memory during signal receive. "
         "Should REF/abort query, not crash SPJ node") {
  INITIALIZER(runLoadTable);
  TC_PROPERTY("ErrorCode", 17531);
  TC_PROPERTY("AcceptError", 20006);
  STEP(runJoin);
  FINALIZER(runClearTable);
}
TESTCASE("LookupJoinError", "") {
  INITIALIZER(runLoadTable);
  STEP(runLookupJoinError);
  VERIFIER(runClearTable);
}
TESTCASE("ScanJoinError", "") {
  INITIALIZER(runLoadTable);
  TC_PROPERTY("NodeNumber", 2);
  STEP(runScanJoinError);
  FINALIZER(runClearTable);
}
TESTCASE("SpjFaultLeakCheck",
         "Existing DBSPJ error inserts in all data nodes, a scan and a lookup "
         "join each: the query succeeds with the reference row counts or "
         "fails with an expected error, and leaves no DIH scan, no open "
         "fragment scan and no DBSPJ request / DBTC scan (DUMP 2650 / 2560)") {
  INITIALIZER(runLoadTable);
  STEP(runSpjFaultLeakCheck);
  FINALIZER(runClearTable);
}
TESTCASE("SpjNewFaultLeakCheck",
         "As SpjFaultLeakCheck for the robustness error inserts 17535 and "
         "17538-17541 (R-3, R-4, R-5, R-6, S-2, P-19)") {
  INITIALIZER(runLoadTable);
  STEP(runSpjNewFaultLeakCheck);
  FINALIZER(runClearTable);
}
TESTCASE("NF_JoinLeakCheck",
         "NF_Join, then no DIH scan, no open fragment scan and no DBSPJ "
         "request / DBTC scan may be left on the idle cluster") {
  TC_PROPERTY("UntilStopped", 1);
  TC_PROPERTY("WaitProgress", 20);
  INITIALIZER(runLoadTable);
  STEPS(runJoin, 6);
  STEP(runRestarter);
  FINALIZER(runSpjIdleLeakCheck);
  FINALIZER(runClearTable);
}
NDBT_TESTSUITE_END(testSpj)

int main(int argc, const char **argv) {
  ndb_init();

  /* To inject a single fault, for testing fault injection.
     Add the required fault number at the end
     of the command line. */

  if (argc > 0) sscanf(argv[argc - 1], "%d", &faultToInject);
  if (faultToInject && (faultToInject < FI_START || faultToInject > FI_END)) {
    ndbout_c("Illegal fault to inject: %d. Legal range is between %d and %d",
             faultToInject, FI_START, FI_END);
    exit(1);
  }

  NDBT_TESTSUITE_INSTANCE(testSpj);
  return testSpj.execute(argc, argv);
}
