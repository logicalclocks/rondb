/*
 * Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#ifndef NDB_QUERY_AGGREGATION_HPP
#define NDB_QUERY_AGGREGATION_HPP

/**
 * @file NdbQueryAggregation.hpp
 *
 * Pushdown Join Aggregation API (RONDB-733)
 *
 * This header documents the user-facing API for defining and executing
 * pushdown join queries with aggregation using NdbQueryBuilder. The
 * aggregation is computed on the data nodes, so only the final aggregated
 * results are sent back to the API — not the intermediate join rows.
 *
 *
 * == OVERVIEW ==
 *
 * Pushdown join aggregation extends NdbQueryBuilder with the ability to
 * attach an NdbAggregator program to a leaf operation in a pushed join
 * query. The kernel (DBLQH) executes the aggregation as rows are produced
 * by the join, grouping and merging results before sending them to the API.
 *
 * Schema example:
 *   CREATE TABLE orders (
 *     o_id INT PRIMARY KEY,
 *     o_custkey INT,
 *     o_totalprice DECIMAL(12,2)
 *   ) ENGINE=NDB;
 *
 *   CREATE TABLE lineitem (
 *     l_orderkey INT,
 *     l_linenumber INT,
 *     l_quantity INT,
 *     l_extendedprice DECIMAL(12,2),
 *     PRIMARY KEY (l_orderkey, l_linenumber),
 *     INDEX idx_orderkey (l_orderkey)
 *   ) ENGINE=NDB;
 *
 * SQL equivalent:
 *   SELECT o_custkey, SUM(l_quantity), COUNT(*)
 *   FROM orders
 *   JOIN lineitem ON l_orderkey = o_id
 *   GROUP BY o_custkey;
 *
 *
 * == QUERY CONSTRUCTION ==
 *
 * Step 1: Create the NdbAggregator program.
 *
 *   The NdbAggregator defines what to GROUP BY and what aggregate functions
 *   to compute. Columns referenced via LoadColumn() refer to the child
 *   (leaf) table. To group by or aggregate a column from the parent table,
 *   use addLinkedProjection() to ensure it is projected through the join,
 *   then reference it with the LINKED_COL_FLAG (bit 15) in LoadColumn().
 *
 *   const NdbDictionary::Table *lineitemTab = dict->getTable("lineitem");
 *   const NdbDictionary::Table *ordersTab = dict->getTable("orders");
 *
 *   NdbAggregator agg(lineitemTab);
 *
 *   // GROUP BY o_custkey (parent column — use LINKED_COL_FLAG)
 *   // Column ID with bit 15 set indicates a linked (parent) column
 *   agg.GroupBy(ordersTab->getColumn("o_custkey")->getColumnNo() | 0x8000);
 *
 *   // SUM(l_quantity) — child column, direct reference
 *   agg.LoadColumn("l_quantity", kReg1);
 *   agg.Sum(0, kReg1);
 *
 *   // COUNT(*)
 *   agg.LoadUint64(1, kReg2);
 *   agg.Count(1, kReg2);
 *
 *   agg.Finalize();
 *
 *
 * Step 2: Build the pushed join query with NdbQueryBuilder.
 *
 *   NdbQueryBuilder *qb = NdbQueryBuilder::create();
 *
 *   // Root operation: scan the parent table (orders)
 *   const NdbQueryTableScanOperationDef *parentOp =
 *       qb->scanTable(ordersTab);
 *
 *   // Link child to parent via foreign key
 *   const NdbQueryOperand *joinKey[] = {
 *       qb->linkedValue(parentOp, "o_id"),
 *       nullptr  // NULL-terminated
 *   };
 *
 *   // Child operation options: attach aggregation
 *   NdbQueryOptions opts;
 *   opts.setAggregation(agg);
 *
 *   // Ensure parent projects o_custkey for GROUP BY
 *   const NdbLinkedOperand *custkey_link =
 *       qb->linkedValue(parentOp, "o_custkey");
 *   opts.addLinkedProjection(custkey_link);
 *
 *   // Child operation: lookup lineitem by l_orderkey
 *   const NdbQueryLookupOperationDef *childOp =
 *       qb->readTuple(lineitemTab, joinKey, &opts, "lineitem_agg");
 *
 *   // Prepare the query definition (reusable)
 *   const NdbQueryDef *queryDef = qb->prepare(ndb);
 *   qb->destroy();
 *
 *
 * Step 3: Execute the query.
 *
 *   NdbTransaction *trans = ndb->startTransaction();
 *   NdbQuery *query = trans->createQuery(queryDef);
 *   trans->execute(NdbTransaction::NoCommit);
 *
 *
 * Step 4: Retrieve aggregated results.
 *
 *   NdbAggregator *resultAgg = query->getAggregator();
 *   resultAgg->PrepareResults();
 *
 *   for (;;) {
 *     NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
 *     if (rec.end()) break;
 *
 *     // Fetch GROUP BY columns
 *     NdbAggregator::Column groupCol = rec.FetchGroupbyColumn();
 *     Int32 custkey = groupCol.data_int32();
 *
 *     // Fetch aggregate results
 *     NdbAggregator::Result sumQty = rec.FetchAggregationResult();
 *     Int64 totalQty = sumQty.data_int64();
 *
 *     NdbAggregator::Result count = rec.FetchAggregationResult();
 *     Int64 rowCount = count.data_int64();
 *
 *     printf("custkey=%d  sum(qty)=%lld  count=%lld\n",
 *            custkey, totalQty, rowCount);
 *   }
 *
 *   query->close();
 *   trans->close();
 *
 *
 * == API REFERENCE ==
 *
 * --- NdbQueryOptions ---
 *
 * int setAggregation(const NdbAggregator &agg);
 *
 *   Attach an aggregation program to this operation. The operation
 *   becomes the "aggregate leaf" of the pushed join query. Only one
 *   operation in the query may have aggregation attached, and it must
 *   not be the root operation.
 *
 *   The NdbAggregator must be Finalize()'d before calling this method.
 *   The aggregator's buffer is deep-copied internally, so the
 *   NdbAggregator object may be destroyed after this call returns.
 *
 *   @param agg  A finalized NdbAggregator defining the aggregation program.
 *   @return 0 if ok, -1 on error.
 *
 *
 * int addLinkedProjection(const NdbLinkedOperand *operand);
 *
 *   Request that a linked column from a parent operation be included in
 *   the SPJ projection. This is required when the aggregation program
 *   references parent columns (e.g., GROUP BY on a parent column).
 *
 *   The linked operand is created via NdbQueryBuilder::linkedValue() and
 *   refers to a specific column of a parent operation.
 *
 *   @param operand  A linked operand from NdbQueryBuilder::linkedValue().
 *   @return 0 if ok, -1 on error.
 *
 *
 * --- NdbQuery ---
 *
 * NdbAggregator *getAggregator() const;
 *
 *   Returns the NdbAggregator associated with this query's aggregation,
 *   or nullptr if the query does not use aggregation.
 *
 *   After the query has been executed and all scan batches consumed,
 *   call PrepareResults() on the returned aggregator, then iterate
 *   with FetchResultRecord().
 *
 *
 * --- NdbAggregator (existing API, used with queries) ---
 *
 * The NdbAggregator class (NdbAggregator.hpp) provides both the program
 * builder interface and the result retrieval interface.
 *
 * Program Building:
 *   - GroupBy(col_id)           Group by a column (use col_id | 0x8000
 *                               for linked parent columns)
 *   - LoadColumn(col_id, reg)   Load column value into register
 *   - LoadUint64(val, reg)      Load constant into register
 *   - Sum(agg_id, reg)          Accumulate sum from register
 *   - Count(agg_id, reg)        Count non-null values
 *   - Max(agg_id, reg)          Track maximum value
 *   - Min(agg_id, reg)          Track minimum value
 *   - Finalize()                Complete program construction
 *
 * Result Retrieval (after query execution):
 *   - PrepareResults()          Prepare internal state for iteration
 *   - FetchResultRecord()       Returns next ResultRecord (check .end())
 *
 * ResultRecord:
 *   - FetchGroupbyColumn()      Returns next Column (group key value)
 *   - FetchAggregationResult()  Returns next Result (aggregate value)
 *   - end()                     True if no more records
 *
 *
 * == LINKED COLUMNS (PARENT TABLE REFERENCES) ==
 *
 * When the aggregation program needs to reference columns from a parent
 * table (e.g., GROUP BY on a parent column), two mechanisms work together:
 *
 * 1. addLinkedProjection(): Tells SPJ to include the parent column in
 *    the data sent to the child operation at the data node level.
 *
 * 2. LINKED_COL_FLAG (0x8000): When calling GroupBy() or LoadColumn()
 *    with a column ID, set bit 15 to indicate the column comes from the
 *    parent's projected data rather than the child's own columns.
 *
 *    agg.GroupBy(parent_col_id | 0x8000);   // Group by parent column
 *    agg.LoadColumn(parent_col_id | 0x8000, kReg1);  // Load parent column
 *
 *
 * == MULTIPLE RECEIVERS (HASH-PARTITIONED ROUTING) ==
 *
 * For aggregation queries, the NDB API allocates multiple NdbReceiver
 * objects. Aggregate result groups are hash-routed across receivers:
 * each group key is hashed and mapped to exactly one receiver via
 * receiverIds[hash(key) % N].
 *
 * This means:
 *   - Each receiver gets a disjoint subset of groups
 *   - The same group key from different data nodes always arrives at
 *     the same receiver
 *   - No cross-receiver merge is needed
 *   - ProcessRes() merges partial results within each receiver
 *
 * The number of receivers and the routing are handled internally by the
 * NDB API — the user does not need to manage receivers directly.
 *
 *
 * == ORDERED INDEX SCAN WITH BOUNDS ==
 *
 * Aggregation can be combined with index scan bounds on the root
 * operation. For example, scanning orders within a date range:
 *
 *   const NdbDictionary::Index *dateIdx =
 *       dict->getIndex("idx_orderdate", "orders");
 *
 *   const NdbQueryOperand *low[] = {
 *       qb->constValue("1994-01-01"), nullptr};
 *   const NdbQueryOperand *high[] = {
 *       qb->constValue("1994-12-31"), nullptr};
 *   NdbQueryIndexBound bound(low, true, high, true);
 *
 *   const NdbQueryIndexScanOperationDef *parentOp =
 *       qb->scanIndex(dateIdx, ordersTab, &bound);
 *
 * The bounds and aggregation program are packed into SCAN_TABREQ
 * Section 2 with a header format:
 *   Word 0:              boundsLen
 *   Words 1..boundsLen:  bounds data
 *   Remaining words:     aggregation program
 *
 * DBTC splits these apart: bounds go to scanKeyInfoPtr, the remainder
 * goes to the aggregation program section. This is transparent to the
 * user.
 *
 *
 * == SCAN PARALLELISM ==
 *
 * For aggregation queries, scan parallelism is communicated explicitly
 * in DATA word 15 of SCAN_TABREQ, since Section 0 now carries receiver
 * IDs rather than encoding parallelism by its size. The NDB API sets
 * this automatically based on the configured parallelism.
 *
 *
 * == RESTRICTIONS ==
 *
 * - Exactly 0 or 1 operation in a query may have aggregation attached.
 * - The aggregate operation must not be the root operation.
 * - The NdbAggregator must be Finalize()'d before setAggregation().
 * - Aggregation results are not available until all scan batches have
 *   been consumed (SCAN_TABCONF with EndOfData).
 * - Maximum 128 GROUP BY columns (MAX_AGG_N_GROUPBY_COLS).
 * - Maximum 256 aggregate results (MAX_AGG_N_RESULTS).
 * - Aggregation program must fit in 1024 words (MAX_AGG_PROGRAM_WORD_SIZE).
 *
 *
 * == ERROR HANDLING ==
 *
 * - NdbAggregator::GetError() returns the last error from program
 *   building (e.g., unsupported column type, program too large).
 * - NdbQuery::getNdbError() returns errors from query execution.
 * - If setAggregation() is called with a non-finalized aggregator,
 *   it returns -1.
 *
 *
 * == COMPLETE EXAMPLE ==
 *
 * This example computes SUM(l_quantity) grouped by o_custkey for all
 * orders, equivalent to:
 *   SELECT o_custkey, SUM(l_quantity)
 *   FROM orders JOIN lineitem ON l_orderkey = o_id
 *   GROUP BY o_custkey;
 *
 * @code
 * #include <NdbApi.hpp>
 * #include <NdbAggregator.hpp>
 * #include "NdbQueryBuilder.hpp"
 * #include "NdbQueryOperation.hpp"
 *
 * void run_aggregation(Ndb *ndb) {
 *   const NdbDictionary::Dictionary *dict = ndb->getDictionary();
 *   const NdbDictionary::Table *ordersTab = dict->getTable("orders");
 *   const NdbDictionary::Table *lineitemTab = dict->getTable("lineitem");
 *
 *   // 1. Build aggregation program
 *   NdbAggregator agg(lineitemTab);
 *   int custkey_col = ordersTab->getColumn("o_custkey")->getColumnNo();
 *   agg.GroupBy(custkey_col | 0x8000);  // GROUP BY parent column
 *   agg.LoadColumn("l_quantity", kReg1);
 *   agg.Sum(0, kReg1);                 // SUM(l_quantity)
 *   agg.Finalize();
 *
 *   // 2. Build pushed join query
 *   NdbQueryBuilder *qb = NdbQueryBuilder::create();
 *   const NdbQueryTableScanOperationDef *parentOp =
 *       qb->scanTable(ordersTab);
 *
 *   const NdbQueryOperand *joinKey[] = {
 *       qb->linkedValue(parentOp, "o_id"), nullptr
 *   };
 *
 *   NdbQueryOptions opts;
 *   opts.setAggregation(agg);
 *   opts.addLinkedProjection(
 *       qb->linkedValue(parentOp, "o_custkey"));
 *
 *   qb->readTuple(lineitemTab, joinKey, &opts);
 *
 *   const NdbQueryDef *queryDef = qb->prepare(ndb);
 *   qb->destroy();
 *
 *   // 3. Execute
 *   NdbTransaction *trans = ndb->startTransaction();
 *   NdbQuery *query = trans->createQuery(queryDef);
 *   trans->execute(NdbTransaction::NoCommit);
 *
 *   // Consume all scan batches
 *   while (query->nextResult(true) == NdbQuery::NextResult_gotRow) {
 *     // Scan rows are consumed but not used directly —
 *     // aggregated results are collected internally
 *   }
 *
 *   // 4. Retrieve aggregated results
 *   NdbAggregator *result = query->getAggregator();
 *   result->PrepareResults();
 *
 *   for (;;) {
 *     NdbAggregator::ResultRecord rec = result->FetchResultRecord();
 *     if (rec.end()) break;
 *
 *     NdbAggregator::Column grp = rec.FetchGroupbyColumn();
 *     NdbAggregator::Result sum = rec.FetchAggregationResult();
 *
 *     printf("custkey=%d  sum_qty=%lld\n",
 *            grp.data_int32(), sum.data_int64());
 *   }
 *
 *   query->close();
 *   trans->close();
 *   queryDef->destroy();
 * }
 * @endcode
 */

#endif  // NDB_QUERY_AGGREGATION_HPP
