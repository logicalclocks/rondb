/*
   Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.

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
 * @file ha_ndbcluster_push_agg.h
 *
 * Aggregation pushdown for NDB pushed joins and single-table queries.
 *
 * When a fully-pushed join query or a single-table query has GROUP BY +
 * aggregate functions (COUNT, SUM, MIN, MAX), the aggregation can be
 * pushed to the data nodes via the NdbAggregator program. This file
 * declares the functions for detecting pushable aggregation, building
 * the aggregation program, rebuilding the NdbQueryDef with aggregation
 * attached, and fetching aggregate results.
 *
 * All substantial aggregation pushdown logic lives in
 * ha_ndbcluster_push_agg.cc. Existing files receive only small hooks
 * that call into these functions.
 */

#ifndef HA_NDBCLUSTER_PUSH_AGG_H
#define HA_NDBCLUSTER_PUSH_AGG_H

struct AccessPath;
class THD;
class JOIN;
class ha_ndbcluster;
class ndb_pushed_builder_ctx;
class NdbQueryOptions;
class NdbScanOperation;

/**
 * The code of a pushed condition is attached to a lookup operation of a
 * pushed join only if it is smaller than this number of words.  A larger
 * condition is evaluated by ha_ndbcluster (create_pushed_join()), which
 * can't be done when the aggregation is pushed.
 */
constexpr unsigned int NDB_MAX_LOOKUP_FILTER_WORDS = 64;

/**
 * Entry point for aggregation pushdown, called from
 * ndbcluster_push_to_engine() after make_pushed_join(), and after the
 * conditions on the tables not pushed as join children were prepared for
 * being pushed: an aggregation is pushed only if every condition is
 * completely pushed.
 *
 * Pushes the aggregation of the pushed join (ndb_push_aggregation()) if
 * 'push_join_aggregate', else that of a single table
 * (ndb_push_single_table_aggregation()) if 'push_table_aggregate'.
 * Neither is pushed when the plan takes its output order from the index
 * order of the table access (F30), or when the access reads several ranges
 * in one execution (F10, E-12).
 *
 * Marks the root handler as reading a pushed aggregation, and clears
 * the mark on the other handlers of the builder: push_to_engine() is called
 * several times while the optimizer explores plans, and an earlier call may
 * have marked a handler that is now a child in a wider pushed join.
 *
 * ha_ndbcluster reads a pushed aggregation unordered: its groups come back
 * in NdbAggregator's order, and there are no root rows to merge sort (and
 * no index rows for the sorted merge to compare).  It reads it through the
 * default MRR implementation, as its own multi_range_read_next() expects
 * table rows: the aggregation is pushed only for a single range, read by
 * read_range_first() / read_range_next().
 *
 * @return true if an aggregation was pushed
 */
bool ndb_push_aggregation_to_engine(THD *thd, const AccessPath *root_path,
                                    const JOIN *join,
                                    ndb_pushed_builder_ctx &builder,
                                    bool push_join_aggregate,
                                    bool allow_outer_join,
                                    bool push_table_aggregate);

/**
 * Aggregation pushdown for a pushed join.
 * Called from ndb_push_aggregation_to_engine().
 *
 * Checks whether aggregation can be pushed, builds the NdbAggregator
 * program, rebuilds the NdbQueryDef with aggregation attached, and
 * replaces the pushed join definition on handlers.
 *
 * @return true if aggregation was pushed, false otherwise
 */
bool ndb_push_aggregation(THD *thd, const JOIN *join,
                          ndb_pushed_builder_ctx &builder,
                          bool allow_outer_join);

/**
 * Apply aggregation options to the leaf table during build_query().
 * Called from ndb_pushed_builder_ctx::build_query() when m_aggregator is set.
 *
 * For the leaf table (last in join scope), calls setAggregation() on its
 * NdbQueryOptions and adds linked projections for GROUP BY columns from
 * parent tables.
 *
 * @param builder  The builder context with m_aggregator set
 * @param tab_no   The current table number being built
 * @param options  The NdbQueryOptions being populated for this table
 */
void ndb_apply_aggregation_options(ndb_pushed_builder_ctx &builder,
                                   unsigned int tab_no,
                                   NdbQueryOptions *options);

/**
 * Strip NESTED_LOOP_JOINs whose inner side is a pushed-join child,
 * returning the root table scan path. Used by AccessPath surgery
 * in fixup_pushed_access_paths() to remove pushed-join-child NLJs
 * when aggregation is pushed.
 */
AccessPath *strip_pushed_child_nljs(AccessPath *path);

/**
 * Fetch the next pushed aggregate result.
 * Called from ha_ndbcluster::fetch_next_pushed() when in aggregate mode.
 *
 * On first call, drains the scan to completion (consuming all batches),
 * calls PrepareResults() on the NdbAggregator, then starts returning
 * aggregate result rows.
 *
 * Each returned row populates the MySQL row buffer with GROUP BY column
 * values and sets Item_sum pushed values for aggregate results.
 *
 * @return 0 (NextResult_gotRow) for row found,
 *         NextResult_scanComplete when done, or error code
 */
int ndb_fetch_pushed_aggregate(ha_ndbcluster *handler);

/**
 * Detect whether a single-table aggregate query can be pushed.
 * Called from ndb_push_aggregation_to_engine() when no join aggregation
 * was pushed.
 *
 * Validates that the query has pushable aggregate functions and
 * GROUP BY columns, all referencing the single table.
 *
 * @return true if aggregation was pushed, false otherwise
 */
bool ndb_push_single_table_aggregation(THD *thd, const JOIN *join,
                                       const ndb_pushed_builder_ctx &builder);

/**
 * Attach aggregation program to scan, execute DoAggregation() to drain
 * the scan and merge per-fragment results, then return the first
 * aggregate result row.
 *
 * Called from ha_ndbcluster::full_table_scan() when m_stm_aggregator is set.
 *
 * @return 0 for row found, or NDB error mapped to MySQL error code
 */
int ndb_start_stm_aggregate_scan(ha_ndbcluster *handler,
                                 NdbScanOperation *op);

/**
 * Fetch the next single-table pushed aggregate result.
 * Called from ha_ndbcluster::rnd_next() on subsequent calls after
 * ndb_start_stm_aggregate_scan() returned the first row.
 *
 * @return 0 for row found, HA_ERR_END_OF_FILE when done, or error code
 */
int ndb_fetch_stm_aggregate(ha_ndbcluster *handler);

#endif  // HA_NDBCLUSTER_PUSH_AGG_H
