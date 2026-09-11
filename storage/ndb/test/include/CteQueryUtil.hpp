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
 *
 * The runner can stop after the first delivered row and close the
 * query, calling a hook first so a test can arm an error insert and
 * publish the transaction coordinator before the close goes out.  It
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

enum Shape { LookupMain = 0, ScanRoot = 1 };

struct Options {
  Shape shape;
  bool closeAfterFirstBatch;
  /* Called right before the close when closeAfterFirstBatch is set.
   * Return false to report a failure instead of closing. */
  bool (*beforeClose)(void *arg, Uint32 tcNodeId);
  void *arg;
  /* Batch rows for the main scan of LookupMain (0 = API default).  Must
   * be at least the source table's fragment count. */
  Uint32 mainBatchRows;
  Options()
      : shape(LookupMain), closeAfterFirstBatch(false), beforeClose(nullptr),
        arg(nullptr), mainBatchRows(0) {}
};

struct Result {
  Uint32 tcNodeId;   // transaction coordinator after execute()
  Uint64 rows;       // rows fetched before completion or close
  Uint64 closeMillis;  // wall time spent in NdbQuery::close()
  int ndbError;      // NDB error code on a runtime failure
  const char *failedAt;
  Result()
      : tcNodeId(0), rows(0), closeMillis(0), ndbError(0), failedAt("") {}
};

static inline void dropTables(Ndb *ndb) {
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  (void)dict->dropTable(SRC_TABLE);
  (void)dict->dropTable(VIRT_TABLE);
}

/* cte_nf_src(pk INT PK, grp INT, val BIGINT) and the CTE projection
 * descriptor cte_nf_virtual(grp INT PK, total BIGINT). */
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

/* pk = i, grp = i % groups, val = i for i in [0, rows). */
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
          op->setValue("val", (Int64)i) != 0) {
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

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    res.failedAt = "NdbQueryBuilder::create";
    return -2;
  }

  /* CTE 0: scan src -> self-lookup on pk carrying the aggregation. */
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
  const NdbQueryOperand *cteJoinKey[] = {qb->linkedValue(cteScanOp, "pk"),
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

  /* Main query. */
  if (opt.shape == LookupMain) {
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
    if (qb->lookupCte(0, 2, virtTab, cteKey, &lookupOpts) == nullptr) {
      res.failedAt = "lookupCte";
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
    qb->destroy();
    return -2;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
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
  if (opt.shape == LookupMain) {
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
  } else {
    NdbQueryOperation *mainOp = query->getQueryOperation(nOps - 1);
    if (mainOp != nullptr) {
      (void)mainOp->getValue("grp");
      (void)mainOp->getValue("total");
    }
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    res.failedAt = "execute";
    res.ndbError = trans->getNdbError().code != 0
                       ? trans->getNdbError().code
                       : query->getNdbError().code;
    trans->close();
    queryDef->destroy();
    return -1;
  }
  res.tcNodeId = trans->getConnectedNodeId();

  int rc = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    res.rows++;
    if (opt.closeAfterFirstBatch) break;
  }
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
  const NDB_TICKS closeStart = NdbTick_getCurrentTicks();
  query->close();
  res.closeMillis =
      NdbTick_Elapsed(closeStart, NdbTick_getCurrentTicks()).milliSec();
  trans->close();
  queryDef->destroy();
  return rc;
}

}  // namespace CteQueryUtil

#endif  // CTE_QUERY_UTIL_HPP
