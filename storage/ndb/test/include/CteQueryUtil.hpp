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

/**
 * CteQueryUtil - CTE query helpers shared by the node-failure tests
 * (testNodeRestart) and the block-level protocol tests
 * (block_unit_test).  See node_failure_test_plan.md, section 2.1.
 *
 * Header-only.  Provides two tables, a loader, and one query runner
 * covering the shapes the node-failure cases need:
 *
 *   LookupMain  CTE0 = GROUP BY grp, SUM(val) over cte_nf_src;
 *               main = scan cte_nf_src -> lookupCte(CTE0, key = grp).
 *               Every source row joins its own group: expect `rows` rows.
 *   ScanRoot    CTE0 as above; main = scanCte(CTE0) -> rows to the API.
 *               Expect `groups` rows.
 *   FeedChain   CTE0 as above; CTE1 = GROUP BY grp, SUM(total) over
 *               scanCte(CTE0), so each source node feeds its CTE0
 *               partition into its CTE1 state with the aggregation feed
 *               continuation; main = scan cte_nf_src -> lookupCte(CTE1).
 *               Expect `rows` rows.
 *   ScanAggMain No CTE: scan cte_nf_src -> scan its ordered PRIMARY
 *               index with child.pk = parent.pk. The child carries
 *               COUNT(*), SUM(val), so its SCAN_FRAGREQ feeds the state.
 *               Requires the SQL-created PRIMARY index. Expect
 *               aggCount = rows, aggSum = sum of 0..rows-1.
 *   OuterAggMain No CTE: scan cte_nf_src -> LEFT JOIN readTuple(pk = nk)
 *               carrying COUNT(*), SUM(val of the joined row).  nk is
 *               NULL for every fourth row (a NULL key, fed through
 *               JOIN_AGG_NULL_ROW_REQ) and equals pk otherwise.  Expect
 *               aggCount = rows, aggSum = sum of i for i % 4 != 3.
 * The last two exist for the parking cases (node_failure_test_plan.md
 * section 6): every consumer signal a held SETUP can park.
 *
 * The runner calls an optional hook once the transaction coordinator
 * is known and before execute(), so a test can arm an error insert on
 * another node for the whole query.  It can also stop after the first
 * delivered row and close the query, calling a second hook right
 * before the close goes out (the CLOSING_SCAN cases).  It
 * reports how long the close took, and can run the main scan with a
 * small batch so that every fragment is still mid-scan (and so has a
 * close to answer) when the first row reaches the API.
 */

#ifndef CTE_QUERY_UTIL_HPP
#define CTE_QUERY_UTIL_HPP

#include <NdbApi.hpp>
#include <NdbAggregator.hpp>
#include <NdbTick.h>
#include "../../src/ndbapi/NdbQueryBuilder.hpp"
#include "../../src/ndbapi/NdbQueryOperation.hpp"

namespace CteQueryUtil {

static const char *const SRC_TABLE = "cte_nf_src";
static const char *const VIRT_TABLE = "cte_nf_virtual";

enum Shape {
  LookupMain = 0,
  ScanRoot = 1,
  FeedChain = 2,
  ScanAggMain = 3,
  OuterAggMain = 4
};

struct Options {
  Shape shape;
  bool closeAfterFirstBatch;
  /* Called right before the close when closeAfterFirstBatch is set.
   * Return false to report a failure instead of closing. */
  bool (*beforeClose)(void *arg, Uint32 tcNodeId);
  /* Called after startTransaction() and before execute(), with the
   * transaction coordinator's node id.  Return false to fail the run. */
  bool (*beforeExecute)(void *arg, Uint32 tcNodeId);
  void *arg;
  /* Batch rows for the main scan of LookupMain (0 = API default).  Must
   * be at least the source table's fragment count. */
  Uint32 mainBatchRows;
  /* Data node to use as transaction coordinator (0 = the API's choice). */
  Uint32 tcNodeId;
  /* CTE0's aggregate leaf looks up pk = grp instead of pk = pk, so the
   * feeds of a scanned row go to the node holding row `grp`, spread over
   * the cluster, instead of staying on the scanning node.  Same result
   * rows; used by the parking cases, which need remote feeds. */
  bool crossNodeLeaf;
  Options()
      : shape(LookupMain), closeAfterFirstBatch(false), beforeClose(nullptr),
        beforeExecute(nullptr), arg(nullptr), mainBatchRows(0), tcNodeId(0),
        crossNodeLeaf(false) {}
};

struct Result {
  Uint32 tcNodeId;   // transaction coordinator after execute()
  Uint64 rows;       // rows fetched before completion or close
  Uint64 queryMillis;  // wall time from execute() to the end of fetching
  Uint64 closeMillis;  // wall time spent in NdbQuery::close()
  int ndbError;      // NDB error code on a runtime failure
  int closeError;    // NDB error observed after explicit query close
  const char *failedAt;
  Int64 aggCount;    // ScanAggMain / OuterAggMain: COUNT(*)
  Int64 aggSum;      // ScanAggMain / OuterAggMain: SUM(val)
  Result()
      : tcNodeId(0), rows(0), queryMillis(0), closeMillis(0), ndbError(0),
        closeError(0), failedAt(""), aggCount(0), aggSum(0) {}
};

/* SUM(val) an aggregating main shape must return over `rows` loaded
 * rows: ScanAggMain adds every row, OuterAggMain every row whose nk is
 * not NULL (the NULL-extended rows are counted but add no val). */
static inline Int64 expectedAggSum(Uint32 rows, Shape shape) {
  Int64 sum = 0;
  for (Uint32 i = 0; i < rows; i++) {
    if (shape == OuterAggMain && i % 4 == 3) continue;
    sum += i;
  }
  return sum;
}

/* Whether a completed run (rc == 0) returned the result loadTable(rows,
 * groups) implies for the shape: one row per source row (LookupMain,
 * FeedChain), one per populated group (ScanRoot), or COUNT(*) = rows
 * with the sum above (ScanAggMain, OuterAggMain). */
static inline bool resultMatches(Shape shape, const Result &res, Uint32 rows,
                                 Uint32 groups) {
  switch (shape) {
    case LookupMain:
    case FeedChain:
      return res.rows == rows;
    case ScanRoot:
      return res.rows == (groups < rows ? groups : rows);
    case ScanAggMain:
    case OuterAggMain:
      return res.aggCount == (Int64)rows &&
             res.aggSum == expectedAggSum(rows, shape);
  }
  return false;
}

static inline void dropTables(Ndb *ndb) {
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  (void)dict->dropTable(SRC_TABLE);
  (void)dict->dropTable(VIRT_TABLE);
}

/* cte_nf_src(pk INT PK, grp INT, val BIGINT, nk INT NULL) and the CTE
 * projection descriptor cte_nf_virtual(grp INT PK, total BIGINT).  A
 * block test that runs beside live mysqlds must create the same tables
 * through MySQL instead (see testCteProtocol) and only call loadTable. */
static inline int createTables(Ndb *ndb) {
  dropTables(ndb);
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  {
    NdbDictionary::Table tab(SRC_TABLE);
    NdbDictionary::Column pk("pk");
    pk.setType(NdbDictionary::Column::Int);
    pk.setPrimaryKey(true);
    pk.setNullable(false);
    tab.addColumn(pk);
    NdbDictionary::Column grp("grp");
    grp.setType(NdbDictionary::Column::Int);
    grp.setNullable(false);
    tab.addColumn(grp);
    NdbDictionary::Column val("val");
    val.setType(NdbDictionary::Column::Bigint);
    val.setNullable(false);
    tab.addColumn(val);
    NdbDictionary::Column nk("nk");
    nk.setType(NdbDictionary::Column::Int);
    nk.setNullable(true);
    tab.addColumn(nk);
    if (dict->createTable(tab) != 0) return -1;
  }
  {
    NdbDictionary::Table tab(VIRT_TABLE);
    NdbDictionary::Column grp("grp");
    grp.setType(NdbDictionary::Column::Int);
    grp.setPrimaryKey(true);
    grp.setNullable(false);
    tab.addColumn(grp);
    NdbDictionary::Column total("total");
    total.setType(NdbDictionary::Column::Bigint);
    total.setNullable(false);
    tab.addColumn(total);
    if (dict->createTable(tab) != 0) return -1;
  }
  return 0;
}

/* pk = i, grp = i % groups, val = i, nk = NULL when i % 4 == 3 and i
 * otherwise, for i in [0, rows). */
static inline int loadTable(Ndb *ndb, Uint32 rows, Uint32 groups) {
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *tab = dict->getTable(SRC_TABLE);
  if (tab == nullptr || groups == 0) return -1;
  for (Uint32 i = 0; i < rows;) {
    NdbTransaction *trans = ndb->startTransaction();
    if (trans == nullptr) return -1;
    for (Uint32 n = 0; i < rows && n < 256; i++, n++) {
      NdbOperation *op = trans->getNdbOperation(tab);
      if (op == nullptr || op->insertTuple() != 0 ||
          op->equal("pk", (Int32)i) != 0 ||
          op->setValue("grp", (Int32)(i % groups)) != 0 ||
          op->setValue("val", (Int64)i) != 0 ||
          ((i % 4 == 3) ? op->setValue("nk", (const char *)nullptr)
                        : op->setValue("nk", (Int32)i)) != 0) {
        trans->close();
        return -1;
      }
    }
    if (trans->execute(NdbTransaction::Commit) != 0) {
      trans->close();
      return -1;
    }
    trans->close();
  }
  return 0;
}

/* Returns 0 on success, -1 on a runtime failure (res.ndbError set, which
 * a node-failure test usually expects), -2 on a build failure. */
static inline int runQuery(Ndb *ndb, const Options &opt, Result &res) {
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(VIRT_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    res.failedAt = "getTable";
    return -2;
  }

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    res.failedAt = "cteAgg";
    return -2;
  }

  /* FeedChain's CTE 1: GROUP BY grp, SUM(total) over the CTE 0 rows the
   * CTE scan feeds (linked columns of the virtual table). */
  NdbAggregator chainAgg(virtTab);
  if (opt.shape == FeedChain) {
    const NdbDictionary::Column *grpCol = virtTab->getColumn("grp");
    const NdbDictionary::Column *totalCol = virtTab->getColumn("total");
    if (grpCol == nullptr || totalCol == nullptr ||
        !chainAgg.GroupByLinked(0, grpCol) ||
        !chainAgg.LoadLinkedColumn(1, 0, totalCol) ||
        !chainAgg.Sum(0, 0) || !chainAgg.Finalize()) {
      res.failedAt = "chainAgg";
      return -2;
    }
  }

  /* ScanAggMain / OuterAggMain: the main aggregation COUNT(*), SUM(val).
   * Register 0 holds the constant 1 so NULL-extended rows still count. */
  const bool aggMain = (opt.shape == ScanAggMain || opt.shape == OuterAggMain);
  NdbAggregator mainAgg(srcTab);
  if (aggMain &&
      (!mainAgg.LoadUint64(1, 0) || !mainAgg.LoadColumn("val", 1) ||
       !mainAgg.Count(0, 0) || !mainAgg.Sum(1, 1) || !mainAgg.Finalize())) {
    res.failedAt = "mainAgg";
    return -2;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    res.failedAt = "NdbQueryBuilder::create";
    return -2;
  }

  /* CTE 0: scan src -> self-lookup on pk carrying the aggregation. */
  if (!aggMain) {
    if (qb->beginCteSubtree(0) == nullptr) {
      res.failedAt = "beginCteSubtree";
      qb->destroy();
      return -2;
    }
    const NdbQueryTableScanOperationDef *cteScanOp = qb->scanTable(srcTab);
    if (cteScanOp == nullptr) {
      res.failedAt = "cte scanTable";
      qb->destroy();
      return -2;
    }
    const NdbQueryOperand *cteJoinKey[] = {
        qb->linkedValue(cteScanOp, opt.crossNodeLeaf ? "grp" : "pk"),
        nullptr};
    NdbQueryOptions cteLeafOpts;
    cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    cteLeafOpts.setAggregation(cteAgg);
    if (qb->readTuple(srcTab, cteJoinKey, &cteLeafOpts) == nullptr) {
      res.failedAt = "cte readTuple";
      qb->destroy();
      return -2;
    }
    qb->endCteSubtree();
    if (qb->defineCte(0, srcTab, cteAgg) != 0) {
      res.failedAt = "defineCte";
      qb->destroy();
      return -2;
    }
  }

  /* FeedChain's CTE 1: a CTE scan of CTE 0 as the aggregate leaf. */
  Uint32 mainCteId = 0;
  if (opt.shape == FeedChain) {
    if (qb->beginCteSubtree(1) == nullptr) {
      res.failedAt = "beginCteSubtree 1";
      qb->destroy();
      return -2;
    }
    NdbQueryOptions feedOpts;
    feedOpts.setAggregation(chainAgg);
    if (qb->scanCte(0, 2, virtTab, &feedOpts) == nullptr) {
      res.failedAt = "cte1 scanCte";
      qb->destroy();
      return -2;
    }
    qb->endCteSubtree();
    if (qb->defineCte(1, virtTab, chainAgg, /* depMask */ 1) != 0) {
      res.failedAt = "defineCte 1";
      qb->destroy();
      return -2;
    }
    mainCteId = 1;
  }

  /* Main query. */
  if (opt.shape == LookupMain || opt.shape == FeedChain) {
    const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
    if (mainScanOp == nullptr) {
      res.failedAt = "main scanTable";
      qb->destroy();
      return -2;
    }
    const NdbQueryOperand *cteKey[] = {qb->linkedValue(mainScanOp, "grp"),
                                       nullptr};
    NdbQueryOptions lookupOpts;
    lookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    if (qb->lookupCte(mainCteId, 2, virtTab, cteKey, &lookupOpts) == nullptr) {
      res.failedAt = "lookupCte";
      qb->destroy();
      return -2;
    }
  } else if (opt.shape == ScanAggMain) {
    // A main aggregate leaf cannot be the root. Use a child index scan
    // that finds exactly the parent's row and carries the aggregation.
    const NdbDictionary::Index *pkIndex = dict->getIndex("PRIMARY", SRC_TABLE);
    if (pkIndex == nullptr) {
      res.failedAt = "getIndex PRIMARY";
      res.ndbError = dict->getNdbError().code;
      qb->destroy();
      return -2;
    }
    const NdbQueryTableScanOperationDef *root = qb->scanTable(srcTab);
    if (root == nullptr) {
      res.failedAt = "main scanTable";
      res.ndbError = qb->getNdbError().code;
      qb->destroy();
      return -2;
    }
    const NdbQueryOperand *key[] = {qb->linkedValue(root, "pk"), nullptr};
    NdbQueryIndexBound bound(key);
    NdbQueryOptions scanOpts;
    scanOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    scanOpts.setAggregation(mainAgg);
    if (qb->scanIndex(pkIndex, srcTab, &bound, &scanOpts) == nullptr) {
      res.failedAt = "agg scanIndex";
      res.ndbError = qb->getNdbError().code;
      qb->destroy();
      return -2;
    }
  } else if (opt.shape == OuterAggMain) {
    /* scan src -> LEFT JOIN readTuple(pk = nk) as the aggregate leaf:
     * nk NULL rows take the JOIN_AGG_NULL_ROW_REQ path. */
    const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
    if (mainScanOp == nullptr) {
      res.failedAt = "main scanTable";
      qb->destroy();
      return -2;
    }
    const NdbQueryOperand *leafKey[] = {qb->linkedValue(mainScanOp, "nk"),
                                        nullptr};
    NdbQueryOptions leafOpts;   /* MatchAll: LEFT JOIN */
    leafOpts.setAggregation(mainAgg);
    if (qb->readTuple(srcTab, leafKey, &leafOpts) == nullptr) {
      res.failedAt = "outer readTuple";
      qb->destroy();
      return -2;
    }
  } else {
    if (qb->scanCte(0, 2, virtTab) == nullptr) {
      res.failedAt = "scanCte";
      qb->destroy();
      return -2;
    }
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    res.failedAt = "prepare";
    res.ndbError = qb->getNdbError().code;
    qb->destroy();
    return -2;
  }
  qb->destroy();

  NdbTransaction *trans = opt.tcNodeId != 0
                              ? ndb->startTransaction(opt.tcNodeId, 0)
                              : ndb->startTransaction();
  if (trans == nullptr) {
    res.failedAt = "startTransaction";
    res.ndbError = ndb->getNdbError().code;
    queryDef->destroy();
    return -1;
  }
  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    res.failedAt = "createQuery";
    res.ndbError = trans->getNdbError().code;
    trans->close();
    queryDef->destroy();
    return -1;
  }
  /* Register the projected columns so the main rows are fetched. */
  const Uint32 nOps = queryDef->getNoOfOperations();
  if (opt.shape == LookupMain || opt.shape == FeedChain) {
    NdbQueryOperation *mainOp = query->getQueryOperation(nOps - 2);
    NdbQueryOperation *lookupOp = query->getQueryOperation(nOps - 1);
    if (mainOp != nullptr) {
      (void)mainOp->getValue("grp");
      if (opt.mainBatchRows != 0 &&
          mainOp->setBatchSize(opt.mainBatchRows) != 0) {
        res.failedAt = "setBatchSize";
        res.ndbError = query->getNdbError().code;
        trans->close();
        queryDef->destroy();
        return -2;
      }
    }
    if (lookupOp != nullptr) {
      (void)lookupOp->getValue("grp");
      (void)lookupOp->getValue("total");
    }
  } else if (aggMain) {
    /* No getValue on an aggregating query's operations: it would shift
     * the linked-attribute positions (block_unit_test/CLAUDE.md). */
  } else {
    NdbQueryOperation *mainOp = query->getQueryOperation(nOps - 1);
    if (mainOp != nullptr) {
      (void)mainOp->getValue("grp");
      (void)mainOp->getValue("total");
    }
  }

  /* The coordinator is chosen at startTransaction(); publish it before
   * the query goes out so a hook can pick a victim among the others. */
  res.tcNodeId = trans->getConnectedNodeId();
  if (opt.beforeExecute != nullptr &&
      !opt.beforeExecute(opt.arg, res.tcNodeId)) {
    res.failedAt = "beforeExecute";
    trans->close();
    queryDef->destroy();
    return -2;
  }
  const NDB_TICKS execStart = NdbTick_getCurrentTicks();
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    res.failedAt = "execute";
    res.ndbError = trans->getNdbError().code != 0
                       ? trans->getNdbError().code
                       : query->getNdbError().code;
    res.queryMillis =
        NdbTick_Elapsed(execStart, NdbTick_getCurrentTicks()).milliSec();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  int rc = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    res.rows++;
    if (opt.closeAfterFirstBatch) break;
  }
  res.queryMillis =
      NdbTick_Elapsed(execStart, NdbTick_getCurrentTicks()).milliSec();
  if (opt.closeAfterFirstBatch && outcome == NdbQuery::NextResult_gotRow) {
    if (opt.beforeClose != nullptr && !opt.beforeClose(opt.arg, res.tcNodeId)) {
      res.failedAt = "beforeClose";
      rc = -2;
    }
    /* The close sends SCAN_NEXTREQ(close) and waits for EndOfData. */
  } else if (outcome == NdbQuery::NextResult_error) {
    res.failedAt = "nextResult";
    res.ndbError = query->getNdbError().code;
    rc = -1;
  }
  if (aggMain && rc == 0) {
    NdbAggregator *agg = query->getAggregator();
    if (agg == nullptr) {
      res.failedAt = "getAggregator";
      rc = -2;
    } else {
      NdbAggregator::ResultRecord rec = agg->FetchResultRecord();
      if (rec.end()) {
        res.failedAt = "no aggregation result";
        rc = -2;
      } else {
        res.aggCount = rec.FetchAggregationResult().data_int64();
        res.aggSum = rec.FetchAggregationResult().data_int64();
      }
    }
  }
  const NDB_TICKS closeStart = NdbTick_getCurrentTicks();
  query->close();
  res.closeMillis =
      NdbTick_Elapsed(closeStart, NdbTick_getCurrentTicks()).milliSec();
  res.closeError = query->getNdbError().code;
  trans->close();
  queryDef->destroy();
  return rc;
}

}  // namespace CteQueryUtil

#endif  // CTE_QUERY_UTIL_HPP
