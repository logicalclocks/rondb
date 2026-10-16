/*
 * Copyright (C) 2026 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_PROCESS_SETUP_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_PROCESS_SETUP_HPP_

/*
 * RONDB-1144: the process-wide memory setting (THP opt-out) that must be
 * in place before the first thread exists. Kept out of main.cc so the single call site
 * merges cleanly between release branches whose main.cc differ.
 */

struct ProcessSetupResult {
  bool thp_disabled;   // prctl(PR_SET_THP_DISABLE) succeeded
  int errno_thp;       // errno when thp_disabled is false, else 0
};

/*
 * Opt the process out of transparent huge pages. Must run before any
 * thread is created: before Linux 5.0 the flag only applied to mappings
 * created after the call.
 * Idempotent. On platforms without the facility the flag is false and the
 * errno is 0.
 */
ProcessSetupResult rdrs_process_setup();

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_PROCESS_SETUP_HPP_
