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
 * Aggregation pushdown for NDB pushed joins.
 *
 * When a fully-pushed join query has GROUP BY + aggregate functions
 * (COUNT, SUM, MIN, MAX), the aggregation can be pushed to the data
 * nodes via the NdbAggregator program. This file declares the functions
 * for detecting pushable aggregation, building the aggregation program,
 * modifying the AccessPath tree, and fetching aggregate results.
 *
 * All substantial aggregation pushdown logic lives in
 * ha_ndbcluster_push_agg.cc. Existing files receive only small hooks
 * that call into these functions.
 */

#ifndef HA_NDBCLUSTER_PUSH_AGG_H
#define HA_NDBCLUSTER_PUSH_AGG_H

class THD;
class JOIN;
class ha_ndbcluster;
class ndb_pushed_builder_ctx;

/**
 * Entry point for aggregation pushdown analysis.
 * Called from ndbcluster_push_to_engine() after make_pushed_join() succeeds.
 *
 * Finds the pushed join (if any) from the builder context, checks whether
 * aggregation can be pushed, builds the NdbAggregator program, and returns
 * true if aggregation was successfully pushed.
 *
 * @return true if aggregation is pushed (caller should remove AGGREGATE
 *         AccessPath), false otherwise
 */
bool ndb_push_aggregation(THD *thd, const JOIN *join,
                          const ndb_pushed_builder_ctx &builder);

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

#endif  // HA_NDBCLUSTER_PUSH_AGG_H
