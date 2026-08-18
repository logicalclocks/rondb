/*
 * Copyright (c) 2026, Hopsworks and/or its affiliates.
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

#include "rate_limit.hpp"
#include "logger.hpp"

#include <mutex>
#include <unordered_set>

/*
RONDB-978: see the declaration in rate_limit.hpp. This runs on the api key
cache load path, which is already doing NDB round trips, so the lock and the
set cost nothing measurable here - and nothing at all on the request path,
which no longer performs this check.

The set only ever grows, but it is bounded by the number of distinct
databases the deployment serves, and only databases that actually lack an
identity are ever inserted - on a correctly provisioned cluster it stays
empty.
*/
void report_unmetered_database(const std::string &db) {
  static std::mutex reported_lock;
  static std::unordered_set<std::string> reported;

  {
    std::lock_guard<std::mutex> guard(reported_lock);
    if (!reported.insert(db).second) {
      return;
    }
  }
  rdrs_logger::warn(
    "Rate limiting: requests to database '" + db + "' run unmetered. No rate "
    "limit identity could be resolved: the API key's owner is neither a "
    "member of that project nor a recipient of a share of it. Reported once "
    "per database.");
}
