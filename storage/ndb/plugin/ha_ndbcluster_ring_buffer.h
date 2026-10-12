/* Copyright (c) 2022, 2025, Hopsworks and/or its affiliates.

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
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/*
  Ring Buffer Table - support code extracted from ha_ndbcluster.cc to keep
  that file's delta against upstream minimal. Member function bodies
  (ha_ndbcluster::ndb_ring_buffer_write_row and ::flush_ring_buffer_batch)
  live in the corresponding .cc file alongside these helpers - they are
  declared in ha_ndbcluster.h.
*/

#ifndef HA_NDBCLUSTER_RING_BUFFER_H
#define HA_NDBCLUSTER_RING_BUFFER_H

#include <cstdint>
#include <string>

/*
  NdbDictionary is a class (not a namespace) with nested Table/Column.
  The public API below takes NdbDictionary::Table* by pointer - we need
  the full declaration for that to work from both consumer TUs.
*/
#include "storage/ndb/include/ndbapi/NdbDictionary.hpp"

class Item;
struct TABLE;
class THD;
class KEY;
struct NDB_Modifier;

namespace ndb_ring_buffer {

/**
  On-disk size of the Ring_meta row payload (the VARBINARY `ring_meta`
  column stored at ring_idx=0). Also enforced as the minimum user-declared
  length of the ring_meta column.
*/
constexpr std::uint32_t META_SIZE = 32;

/**
  Parsed form of the MAX_ROWS_PER_PK table comment modifier.
  - `is_off` is true when the user spelled "off" (case-insensitive);
    `size` / column names are undefined in that case.
  - Otherwise `size`, `idx_col_name`, `meta_col_name` are filled; when the
    `@idx@meta` suffix is omitted, column names default to "ring_idx" /
    "ring_meta".
*/
struct Spec {
  bool is_off = false;
  std::uint32_t size = 0;
  std::string idx_col_name;
  std::string meta_col_name;
};

/**
  Parse the MAX_ROWS_PER_PK modifier. Caller must have confirmed
  `mod->m_found`. Returns nullptr on success (fills `*out`), or a static
  error string the caller routes to its own error sink.
*/
const char *parse_spec(const NDB_Modifier *mod, Spec *out);

/**
  Validate the ring-buffer columns against the MySQL Field* view of the
  table (used during CREATE / pre-apply ALTER). Enforces type, default,
  nullability, last-PK-column, and the "no NOT NULL BLOB/TEXT user cols"
  rule. Returns nullptr on success, static error string otherwise.
*/
const char *validate_columns_mysql(const TABLE *table, const Spec &spec);

/**
  Locate the ring-buffer columns in an NDB dictionary table and stamp the
  ring-buffer metadata (size + column numbers) onto `new_tab`. Used by the
  inplace ALTER path. Returns nullptr on success, static error string
  otherwise.
*/
const char *apply_columns_ndb(NdbDictionary::Table *new_tab,
                              const Spec &spec);

/**
  Check whether a DELETE on a ring-buffer table is allowed.
  Requires a WHERE clause where every Item_field references a PK-prefix
  column (not ring_idx, not any non-PK column). Accepts @a cond == nullptr;
  delete_where_pins_prefixes() rejects it.
*/
bool delete_where_allowed(const TABLE *table, unsigned ring_idx_field_index,
                          const Item *cond);

/**
  Check that the WHERE names the rings it deletes: an AND of conditions
  that give every PK-prefix column with `=` (or, for one column, `IN`) and
  a value fixed for the statement, compared in the column's domain, and
  nothing else. The DELETE locks the meta row of each such prefix before
  it scans; ranges, OR, other conditions and a missing WHERE could not be
  locked or could delete part of a ring, and are rejected (TRUNCATE TABLE
  clears the table).
*/
bool delete_where_pins_prefixes(const TABLE *table,
                                unsigned ring_idx_field_index,
                                const Item *cond);

/** Error text when delete_where_pins_prefixes() fails */
constexpr const char *DELETE_NOT_PINNED_MSG =
    "DELETE on ring-buffer table must give every PK-prefix column with = or "
    "IN; use TRUNCATE TABLE to clear the table";

/**
  Check the DELETE statement shape. The WHERE walker alone cannot see
  LIMIT, ORDER BY, or the multi-table DELETE form - each of which can
  remove a strict subset of a ring and corrupt or orphan the meta row.
*/
bool delete_statement_shape_allowed(const THD *thd);

/**
  Decide whether a scan on a ring-buffer-capable table should surface the
  meta rows (ring_idx=0). True when the session flag is on, when a DELETE
  has been validated (start_bulk_delete/ndb_delete_row), or when ALTER TABLE
  is copying rows from a ring-buffer table.
*/
bool show_meta_active(THD *thd, bool is_ring_buffer, bool delete_allowed);

/**
  Block creation of a secondary index over a ring-buffer internal column
  (ring_idx / ring_meta). No-op when the table is not a ring buffer or the
  index is the primary key. Returns 0 on allow, HA_ERR_* on reject (emits
  error or warning via @a thd depending on the originating SQL command).
*/
int check_index_columns(THD *thd, const NdbDictionary::Table *ndbtab,
                        const KEY *key_info, bool is_primary_key_index);

/**
  Accessor for the `ring_buffer_show_meta` THD system variable, defined
  next to its MYSQL_THDVAR_BOOL declaration in ha_ndbcluster.cc. Exposed so
  show_meta_active() (and any future ring-buffer code living outside
  ha_ndbcluster.cc) can read it without duplicating the THDVAR macro.
*/
bool thdvar_show_meta(THD *thd);

}  // namespace ndb_ring_buffer

#endif  // HA_NDBCLUSTER_RING_BUFFER_H
