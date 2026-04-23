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
  Ring Buffer Table — support code extracted from ha_ndbcluster.cc to keep
  that file's delta against upstream minimal. Member function bodies
  (ha_ndbcluster::ndb_ring_buffer_write_row and ::flush_ring_buffer_batch)
  live in the corresponding .cc file alongside these helpers — they are
  declared in ha_ndbcluster.h.
*/

#ifndef HA_NDBCLUSTER_RING_BUFFER_H
#define HA_NDBCLUSTER_RING_BUFFER_H

#include <cstdint>

class Item;
class TABLE;

namespace ndb_ring_buffer {

/**
  On-disk size of the Ring_meta row payload (the VARBINARY `ring_meta`
  column stored at ring_idx=0). Also enforced as the minimum user-declared
  length of the ring_meta column.
*/
constexpr std::uint32_t META_SIZE = 32;

/**
  Check whether a DELETE on a ring-buffer table is allowed.
  Requires a WHERE clause where every Item_field references a PK-prefix
  column (not ring_idx, not any non-PK column). Accepts @a cond == nullptr
  (bare DELETE: full-table clear, always allowed).
*/
bool delete_where_allowed(const TABLE *table, unsigned ring_idx_field_index,
                          const Item *cond);

}  // namespace ndb_ring_buffer

#endif  // HA_NDBCLUSTER_RING_BUFFER_H
