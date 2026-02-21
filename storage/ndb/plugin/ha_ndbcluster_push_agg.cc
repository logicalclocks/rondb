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
 * @file ha_ndbcluster_push_agg.cc
 *
 * Aggregation pushdown for NDB pushed joins — implementation.
 *
 * Contains all logic for detecting pushable aggregation queries,
 * building NdbAggregator programs, modifying the AccessPath tree,
 * and fetching aggregate results.
 */

#include "storage/ndb/plugin/ha_ndbcluster_push_agg.h"

#include "sql/table.h"
#include "storage/ndb/plugin/ha_ndbcluster.h"
#include "storage/ndb/plugin/ha_ndbcluster_push.h"

void ndb_push_aggregation(THD *, const JOIN *,
                          const ndb_pushed_builder_ctx &builder) {
  // Find a pushed join root by scanning builder tables.
  // Use the public handler API (member_of_pushed_join) to detect roots.
  bool has_pushed_join = false;
  for (uint i = 0; i < builder.m_table_count; i++) {
    const TABLE *tab = builder.m_tables[i].get_table();
    if (tab != nullptr && tab->file->member_of_pushed_join() != nullptr) {
      has_pushed_join = true;
      break;
    }
  }
  if (!has_pushed_join) return;

  // Stub — Phase 2 will add aggregation detection here.
  // Phase 3+ will build aggregation program and modify AccessPath.
}
