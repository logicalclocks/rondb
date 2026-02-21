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
class ndb_pushed_builder_ctx;

/**
 * Entry point for aggregation pushdown analysis.
 * Called from ndbcluster_push_to_engine() after make_pushed_join() succeeds.
 *
 * Finds the pushed join (if any) from the builder context, checks whether
 * aggregation can be pushed, and in later phases will build the NdbAggregator
 * program and modify the AccessPath tree.
 */
void ndb_push_aggregation(THD *thd, const JOIN *join,
                          const ndb_pushed_builder_ctx &builder);

#endif  // HA_NDBCLUSTER_PUSH_AGG_H
