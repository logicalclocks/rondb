/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef STORAGE_NDB_SRC_RONSQL_RONSQLPREPARERINTERNAL_HPP
#define STORAGE_NDB_SRC_RONSQL_RONSQLPREPARERINTERNAL_HPP 1

/*
 * T-16: the classification of NDB errors in RonSQLPreparer.cpp, which
 * ErrorClassUnitTest runs without a cluster.  Only RonSQLPreparer.cpp and
 * that test include this header.
 */

#include "RonSQLCommon.hpp"

namespace ronsql_preparer_internal
{

// RONDB-978: the caller is over its USER rate limit or quota.
bool is_rate_limit_error(int ndb_error_code);

// X-8: no data node can serve this API node now (4009, 4035-4041), or a
// cluster shutdown or single user mode (281, 299).
bool is_cluster_unavailable_error(const NdbError& err);

// A temporary error, a lock wait timeout or an unavailable cluster: the
// statement may run again (unless output already went out).
bool is_retryable_ndb_error(const NdbError& err);

// X-8: an error that ends the statement without the statement or RonSQL
// being at fault (a retryable error, or an unknown result).
bool is_resource_ndb_error(const NdbError& err);

/*
 * What RonSQLPreparer::handle_ronsql_exception does with the NDB error of
 * the open transaction when the statement failed with a
 * std::runtime_error.  The flags are the markers it writes on the err
 * stream between "Error handling: SRE,te" and the action.
 */
struct NdbErrorVerdict
{
  enum class Action
  {
    RATE_LIMIT,  // RonSQLRateLimitError, never retried ("->RLE")
    RETRY,       // RonSQLRetryableError ("->RRE")
    FAIL         // RonSQLPermanentError of error_class ("->RPE")
  };
  Action action;
  // FAIL: the class of the permanent error.
  RonSQLErrorClass error_class;
  // ",nu": a retryable error that needs no schema reload.
  bool retryable_error;
  // ",od": the error would be retried, but output already went out.
  bool output_started;
};

// Whether handle_ronsql_exception reloads the schema (",rl") before the
// verdict: a schema error that is not a rate limit rejection.
bool ndb_error_reloads_schema(const NdbError& err,
                              bool has_rate_limit_identity);

// The verdict.  schema_changed is what the reload found, read only when
// ndb_error_reloads_schema(); output_started is
// RonSQLPreparer::retry_would_repeat_output().
NdbErrorVerdict ndb_error_verdict(const NdbError& err,
                                  bool has_rate_limit_identity,
                                  bool schema_changed,
                                  bool output_started);

}  // namespace ronsql_preparer_internal

#endif
