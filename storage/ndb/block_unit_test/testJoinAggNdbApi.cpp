/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

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

/*
 * testJoinAggNdbApi — Integration test for pushdown join aggregation
 *                     using the NdbQueryBuilder API.
 *
 * Tests the complete NDB API path for pushed join queries with aggregation:
 *   NdbQueryBuilder → NdbQueryDef → NdbQuery → getAggregator()
 *
 * Schema:
 *   parent_t(id INT PK, grp INT)
 *   child_t(parent_id INT PK, amount BIGINT)
 *
 * Usage: testJoinAggNdbApi -c <connect_string> [-v|--verbose]
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbSleep.h>
#include <NdbAggregator.hpp>
#include "NdbQueryBuilder.hpp"
#include "NdbQueryOperation.hpp"

#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <vector>

/* Verbose output control */
static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *PARENT_TABLE = "jagg_parent";
static const char *CHILD_TABLE = "jagg_child";

/* LINKED_COL_FLAG: bit 15 set means column comes from parent table */
static const Uint32 LINKED_COL_FLAG = 0x8000;

/* ------------------------------------------------------------------ */
/* Table setup via NDB API                                             */
/* ------------------------------------------------------------------ */

static int
createTestTables(Ndb *ndb)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();

  /* Drop if exist */
  dict->dropTable(CHILD_TABLE);
  dict->dropTable(PARENT_TABLE);

  /* parent_t(id INT PK, grp INT) */
  {
    NdbDictionary::Table tab;
    tab.setName(PARENT_TABLE);

    NdbDictionary::Column colId;
    colId.setName("id");
    colId.setType(NdbDictionary::Column::Int);
    colId.setPrimaryKey(true);
    colId.setNullable(false);
    tab.addColumn(colId);

    NdbDictionary::Column colGrp;
    colGrp.setName("grp");
    colGrp.setType(NdbDictionary::Column::Int);
    colGrp.setPrimaryKey(false);
    colGrp.setNullable(false);
    tab.addColumn(colGrp);

    if (dict->createTable(tab) != 0) {
      fprintf(stderr, "createTable(%s) failed: %s\n",
              PARENT_TABLE, dict->getNdbError().message);
      return -1;
    }
    V("Created table %s\n", PARENT_TABLE);
  }

  /* child_t(parent_id INT PK, amount BIGINT) */
  {
    NdbDictionary::Table tab;
    tab.setName(CHILD_TABLE);

    NdbDictionary::Column colPid;
    colPid.setName("parent_id");
    colPid.setType(NdbDictionary::Column::Int);
    colPid.setPrimaryKey(true);
    colPid.setNullable(false);
    tab.addColumn(colPid);

    NdbDictionary::Column colAmt;
    colAmt.setName("amount");
    colAmt.setType(NdbDictionary::Column::Bigint);
    colAmt.setPrimaryKey(false);
    colAmt.setNullable(false);
    tab.addColumn(colAmt);

    if (dict->createTable(tab) != 0) {
      fprintf(stderr, "createTable(%s) failed: %s\n",
              CHILD_TABLE, dict->getNdbError().message);
      return -1;
    }
    V("Created table %s\n", CHILD_TABLE);
  }

  return 0;
}

/* ------------------------------------------------------------------ */
/* Data insertion via NDB API                                          */
/* ------------------------------------------------------------------ */

static int
insertParentRows(Ndb *ndb, const std::vector<std::pair<Int32, Int32>> &rows)
{
  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *ptab = dict->getTable(PARENT_TABLE);
  if (ptab == nullptr) return -1;

  for (const auto &row : rows) {
    NdbTransaction *trans = ndb->startTransaction();
    if (trans == nullptr) {
      fprintf(stderr, "startTransaction: %s\n", ndb->getNdbError().message);
      return -1;
    }

    NdbOperation *op = trans->getNdbOperation(ptab);
    if (op == nullptr) {
      fprintf(stderr, "getNdbOperation: %s\n", trans->getNdbError().message);
      trans->close();
      return -1;
    }

    op->insertTuple();
    op->equal("id", row.first);
    op->setValue("grp", row.second);

    if (trans->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert parent(%d,%d): %s\n",
              row.first, row.second, trans->getNdbError().message);
      trans->close();
      return -1;
    }
    trans->close();
  }
  V("Inserted %zu parent rows\n", rows.size());
  return 0;
}

static int
insertChildRows(Ndb *ndb, const std::vector<std::pair<Int32, Int64>> &rows)
{
  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *ctab = dict->getTable(CHILD_TABLE);
  if (ctab == nullptr) return -1;

  for (const auto &row : rows) {
    NdbTransaction *trans = ndb->startTransaction();
    if (trans == nullptr) {
      fprintf(stderr, "startTransaction: %s\n", ndb->getNdbError().message);
      return -1;
    }

    NdbOperation *op = trans->getNdbOperation(ctab);
    if (op == nullptr) {
      fprintf(stderr, "getNdbOperation: %s\n", trans->getNdbError().message);
      trans->close();
      return -1;
    }

    op->insertTuple();
    op->equal("parent_id", row.first);
    op->setValue("amount", row.second);

    if (trans->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert child(%d,%lld): %s\n",
              row.first, (long long)row.second, trans->getNdbError().message);
      trans->close();
      return -1;
    }
    trans->close();
  }
  V("Inserted %zu child rows\n", rows.size());
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 1: SUM(amount) GROUP BY grp                                    */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT grp, SUM(amount)                                           */
/*   FROM parent_t JOIN child_t ON child_t.parent_id = parent_t.id     */
/*   GROUP BY grp                                                      */
/* ------------------------------------------------------------------ */

static int
testSumGroupBy(Ndb *ndb,
               const std::map<Int32, Int64> &expected)
{
  printf("Test 1: SUM(amount) GROUP BY grp ... ");
  fflush(stdout);

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *parentTab = dict->getTable(PARENT_TABLE);
  const NdbDictionary::Table *childTab = dict->getTable(CHILD_TABLE);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* Build aggregation program:
   *   GROUP BY grp (parent column → LINKED_COL_FLAG)
   *   SUM(amount)  (child column)
   */
  NdbAggregator agg(childTab);
  Int32 grpColNo = parentTab->getColumn("grp")->getColumnNo();
  agg.GroupBy(grpColNo | LINKED_COL_FLAG);
  agg.LoadColumn("amount", 0);  /* reg 0 */
  agg.Sum(0, 0);                /* agg[0] = SUM(reg 0) */
  agg.Finalize();

  if (agg.GetError().errno_ != 0) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  /* Build pushed join query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  /* Root: scan parent table */
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  if (parentOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Join key: child.parent_id = parent.id */
  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"),
    nullptr
  };

  /* Child options: attach aggregation + linked projection for grp */
  NdbQueryOptions opts;
  opts.setAggregation(agg);

  const NdbLinkedOperand *grpLink = qb->linkedValue(parentOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  opts.addLinkedProjection(grpLink);

  /* Child: lookup child_t by parent_id */
  const NdbQueryLookupOperationDef *childOp =
      qb->readTuple(childTab, joinKey, &opts);
  if (childOp == nullptr) {
    printf("FAILED (readTuple: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Prepare query definition */
  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  V("\n  Query prepared: %u operations, isScan=%d\n",
    queryDef->getNoOfOperations(), queryDef->isScanQuery());

  /* Execute query */
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction: %s)\n", ndb->getNdbError().message);
    queryDef->destroy();
    return -1;
  }

  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    printf("FAILED (createQuery: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Consume all scan batches */
  NdbQuery::NextResultOutcome outcome;
  Uint32 rowCount = 0;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
  }

  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  V("  Scan consumed %u rows\n", rowCount);

  /* Retrieve aggregated results */
  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  std::map<Int32, Int64> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    Int32 grpVal = grpCol.data_int32();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    actual[grpVal] = sumVal;
    V("  grp=%d SUM(amount)=%lld\n", grpVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Verify results */
  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end()) {
      printf("FAILED (missing group %d)\n", e.first);
      return -1;
    }
    if (it->second != e.second) {
      printf("FAILED (group %d: expected SUM=%lld, got %lld)\n",
             e.first, (long long)e.second, (long long)it->second);
      return -1;
    }
  }

  printf("OK (%zu groups)\n", actual.size());
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 2: COUNT(*), SUM(amount) — no GROUP BY                         */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT COUNT(*), SUM(amount)                                      */
/*   FROM parent_t JOIN child_t ON child_t.parent_id = parent_t.id     */
/* ------------------------------------------------------------------ */

static int
testCountSum(Ndb *ndb, Int64 expectedCount, Int64 expectedSum)
{
  printf("Test 2: COUNT(*), SUM(amount) ... ");
  fflush(stdout);

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *parentTab = dict->getTable(PARENT_TABLE);
  const NdbDictionary::Table *childTab = dict->getTable(CHILD_TABLE);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* Build aggregation: COUNT(*), SUM(amount) — no GROUP BY */
  NdbAggregator agg(childTab);
  agg.LoadColumn("amount", 0);
  agg.Count(0, 0);  /* agg[0] = COUNT */
  agg.Sum(1, 0);    /* agg[1] = SUM */
  agg.Finalize();

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);

  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"),
    nullptr
  };

  NdbQueryOptions opts;
  opts.setAggregation(agg);

  const NdbQueryLookupOperationDef *childOp =
      qb->readTuple(childTab, joinKey, &opts);
  if (childOp == nullptr) {
    printf("FAILED (readTuple: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Consume scan */
  while (query->nextResult(true) == NdbQuery::NextResult_gotRow) {}

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result record)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result countRes = rec.FetchAggregationResult();
  Int64 actualCount = countRes.data_int64();

  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  Int64 actualSum = sumRes.data_int64();

  V("\n  COUNT(*)=%lld SUM(amount)=%lld\n",
    (long long)actualCount, (long long)actualSum);

  /* Verify no more records */
  NdbAggregator::ResultRecord rec2 = resultAgg->FetchResultRecord();
  if (!rec2.end()) {
    printf("FAILED (expected single record for non-GROUP-BY, got more)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actualCount != expectedCount) {
    printf("FAILED (COUNT: expected %lld, got %lld)\n",
           (long long)expectedCount, (long long)actualCount);
    return -1;
  }
  if (actualSum != expectedSum) {
    printf("FAILED (SUM: expected %lld, got %lld)\n",
           (long long)expectedSum, (long long)actualSum);
    return -1;
  }

  printf("OK (count=%lld, sum=%lld)\n",
         (long long)actualCount, (long long)actualSum);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: Multiple groups with SUM and COUNT                          */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT grp, COUNT(*), SUM(amount)                                 */
/*   FROM parent_t JOIN child_t ON child_t.parent_id = parent_t.id     */
/*   GROUP BY grp                                                      */
/* ------------------------------------------------------------------ */

static int
testMultiAggGroupBy(Ndb *ndb,
                    const std::map<Int32, std::pair<Int64, Int64>> &expected)
{
  printf("Test 3: COUNT(*), SUM(amount) GROUP BY grp ... ");
  fflush(stdout);

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *parentTab = dict->getTable(PARENT_TABLE);
  const NdbDictionary::Table *childTab = dict->getTable(CHILD_TABLE);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* Build aggregation: GROUP BY grp, COUNT(*), SUM(amount) */
  NdbAggregator agg(childTab);
  Int32 grpColNo = parentTab->getColumn("grp")->getColumnNo();
  agg.GroupBy(grpColNo | LINKED_COL_FLAG);
  agg.LoadColumn("amount", 0);
  agg.Count(0, 0);  /* agg[0] = COUNT */
  agg.Sum(1, 0);    /* agg[1] = SUM */
  agg.Finalize();

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);

  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"),
    nullptr
  };

  NdbQueryOptions opts;
  opts.setAggregation(agg);
  const NdbLinkedOperand *grpLink = qb->linkedValue(parentOp, "grp");
  opts.addLinkedProjection(grpLink);

  const NdbQueryLookupOperationDef *childOp =
      qb->readTuple(childTab, joinKey, &opts);
  if (childOp == nullptr) {
    printf("FAILED (readTuple: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  while (query->nextResult(true) == NdbQuery::NextResult_gotRow) {}

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  std::map<Int32, std::pair<Int64, Int64>> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    Int32 grpVal = grpCol.data_int32();

    NdbAggregator::Result countRes = rec.FetchAggregationResult();
    Int64 countVal = countRes.data_int64();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    actual[grpVal] = {countVal, sumVal};
    V("\n  grp=%d COUNT=%lld SUM=%lld", grpVal,
      (long long)countVal, (long long)sumVal);
  }
  V("\n");

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end()) {
      printf("FAILED (missing group %d)\n", e.first);
      return -1;
    }
    if (it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("FAILED (group %d: expected COUNT=%lld SUM=%lld, "
             "got COUNT=%lld SUM=%lld)\n",
             e.first,
             (long long)e.second.first, (long long)e.second.second,
             (long long)it->second.first, (long long)it->second.second);
      return -1;
    }
  }

  printf("OK (%zu groups)\n", actual.size());
  return 0;
}

/* ------------------------------------------------------------------ */
/* Cleanup                                                             */
/* ------------------------------------------------------------------ */

static int
dropTestTables(Ndb *ndb)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->dropTable(CHILD_TABLE);
  dict->dropTable(PARENT_TABLE);
  V("Dropped test tables\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

static void
usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s -c <connect_string> [-v|--verbose] [-h|--help]\n"
          "\n"
          "Options:\n"
          "  -c  NDB management server connect string (default: localhost:1186)\n"
          "  -v, --verbose  Show detailed progress output\n"
          "  -h, --help     Show this help\n",
          prog);
}

int main(int argc, char **argv)
{
  const char *connectString = "localhost:1186";

  /* Parse arguments */
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      connectString = argv[++i];
    } else if (strcmp(argv[i], "-v") == 0 ||
               strcmp(argv[i], "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(argv[i], "-h") == 0 ||
               strcmp(argv[i], "--help") == 0) {
      usage(argv[0]);
      return 0;
    } else {
      fprintf(stderr, "Unknown option: %s\n", argv[i]);
      usage(argv[0]);
      return 1;
    }
  }

  printf("=== testJoinAggNdbApi ===\n");
  printf("Connect string: %s\n\n", connectString);

  ndb_init();

  int exitCode = 0;

  {
    /* Scoping block: all NDB objects must be destroyed before ndb_end() */
    Ndb_cluster_connection clusterConn(connectString);
    if (clusterConn.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Cannot connect to cluster mgm: %s\n", connectString);
      ndb_end(0);
      return 1;
    }

    if (clusterConn.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready within 30s\n");
      ndb_end(0);
      return 1;
    }

    Ndb ndb(&clusterConn, "TEST_DB");
    if (ndb.init() != 0) {
      fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
      ndb_end(0);
      return 1;
    }

    /* Create test tables */
    if (createTestTables(&ndb) != 0) {
      exitCode = 1;
      goto cleanup;
    }

    /*
     * Test data:
     *   parent: (1,1), (2,1), (3,2), (4,2), (5,3)
     *   child:  (1,100), (2,200), (3,300), (4,400), (5,500)
     *
     * Join result (parent_id = id):
     *   (id=1, grp=1, amount=100)
     *   (id=2, grp=1, amount=200)
     *   (id=3, grp=2, amount=300)
     *   (id=4, grp=2, amount=400)
     *   (id=5, grp=3, amount=500)
     *
     * GROUP BY grp:
     *   grp=1: SUM=300, COUNT=2
     *   grp=2: SUM=700, COUNT=2
     *   grp=3: SUM=500, COUNT=1
     *
     * Totals: COUNT=5, SUM=1500
     */
    {
      std::vector<std::pair<Int32, Int32>> parentRows = {
        {1, 1}, {2, 1}, {3, 2}, {4, 2}, {5, 3}
      };
      if (insertParentRows(&ndb, parentRows) != 0) {
        exitCode = 1;
        goto cleanup;
      }

      std::vector<std::pair<Int32, Int64>> childRows = {
        {1, 100}, {2, 200}, {3, 300}, {4, 400}, {5, 500}
      };
      if (insertChildRows(&ndb, childRows) != 0) {
        exitCode = 1;
        goto cleanup;
      }
    }

    /* Test 1: SUM(amount) GROUP BY grp */
    {
      std::map<Int32, Int64> expected = {
        {1, 300}, {2, 700}, {3, 500}
      };
      if (testSumGroupBy(&ndb, expected) != 0) {
        exitCode = 1;
      }
    }

    /* Test 2: COUNT(*), SUM(amount) — no GROUP BY */
    if (testCountSum(&ndb, 5, 1500) != 0) {
      exitCode = 1;
    }

    /* Test 3: COUNT(*), SUM(amount) GROUP BY grp */
    {
      std::map<Int32, std::pair<Int64, Int64>> expected = {
        {1, {2, 300}}, {2, {2, 700}}, {3, {1, 500}}
      };
      if (testMultiAggGroupBy(&ndb, expected) != 0) {
        exitCode = 1;
      }
    }

cleanup:
    dropTestTables(&ndb);
  }

  ndb_end(0);

  printf("\n%s\n", exitCode == 0 ? "All tests PASSED" : "Some tests FAILED");
  return exitCode;
}
