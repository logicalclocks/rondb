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

#include <ndb_global.h>

#include "GlobalData.hpp"
#include "NodeStartLog.hpp"

/**
 * The nsl_*() cross-block reads declared in NodeStartLog.hpp. Each one
 * dispatches through GlobalData::theNodeStartLogHooks, filled by the
 * owning block's constructor, and returns a neutral value while the
 * owner is not registered (before the blocks exist, in ndbd without a
 * proxy, or in a unit test that does not construct the owner). See the
 * comment on GlobalData::NodeStartLogHooks for why these are pointers.
 */

Uint64 nsl_lqh_copy_row_ops_total() {
  const auto f = globalData.theNodeStartLogHooks.lqh_copy_row_ops_total;
  return (f != nullptr) ? f() : 0;
}

bool nsl_dict_restart_progress(Uint32 &pass, Uint32 &passes, Uint32 &object,
                               Uint32 &last_object) {
  const auto f = globalData.theNodeStartLogHooks.dict_restart_progress;
  return (f != nullptr) && f(pass, passes, object, last_object);
}

Uint64 nsl_dih_sr_metadata_start() {
  const auto f = globalData.theNodeStartLogHooks.dih_sr_metadata_start;
  return (f != nullptr) ? f() : 0;
}

bool nsl_dih_performed_copy_phase() {
  const auto f = globalData.theNodeStartLogHooks.dih_performed_copy_phase;
  return (f != nullptr) && f();
}

bool nsl_dih_wait_lcp_reported() {
  const auto f = globalData.theNodeStartLogHooks.dih_wait_lcp_reported;
  return (f != nullptr) && f();
}

bool nsl_dih_sr_receiving_tables(Uint64 &sub_start, Uint32 &tables) {
  sub_start = 0;
  tables = 0;
  const auto f = globalData.theNodeStartLogHooks.dih_sr_receiving_tables;
  return (f != nullptr) && f(sub_start, tables);
}

Uint64 nsl_lqh_proxy_undo_dd_start() {
  const auto f = globalData.theNodeStartLogHooks.lqh_proxy_undo_dd_start;
  return (f != nullptr) ? f() : 0;
}

Uint32 nsl_lqh_proxy_redo_prepare_done(Uint32 &ldms_with_log_parts) {
  ldms_with_log_parts = 0;
  const auto f = globalData.theNodeStartLogHooks.lqh_proxy_redo_prepare_done;
  return (f != nullptr) ? f(ldms_with_log_parts) : 0;
}

Uint32 nsl_cntr_local_lcp_barrier(Uint32 &ldms_done, Uint32 &ldms,
                                  Uint32 &gci_needed, Uint32 &gci_done) {
  ldms_done = 0;
  ldms = 0;
  gci_needed = 0;
  gci_done = 0;
  const auto f = globalData.theNodeStartLogHooks.cntr_local_lcp_barrier;
  return (f != nullptr) ? f(ldms_done, ldms, gci_needed, gci_done) : 0;
}
