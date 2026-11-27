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

/*
 * T-16: the error classification of RonSQL, run without a cluster:
 *  - ronsql_classify_message, which gives the class (and in RDRS the HTTP
 *    status, see rest-server2/server/test/ronsql_http_code_test.cpp) of a
 *    permanent error by its wording, so that rewording a message can
 *    change the status;
 *  - the NDB error predicates and the verdict of
 *    RonSQLPreparer::handle_ronsql_exception on the error of the open
 *    transaction (RonSQLPreparerInternal.hpp), with errors as the NDB API
 *    classifies them (ndberror.cpp).
 */

#include "RonSQLPreparerInternal.hpp"

#include <cstdio>
#include <cstring>
#include <string>

#include <my_base.h>

using ronsql_preparer_internal::is_cluster_unavailable_error;
using ronsql_preparer_internal::is_rate_limit_error;
using ronsql_preparer_internal::is_resource_ndb_error;
using ronsql_preparer_internal::is_retryable_ndb_error;
using ronsql_preparer_internal::ndb_error_reloads_schema;
using ronsql_preparer_internal::ndb_error_verdict;
using ronsql_preparer_internal::NdbErrorVerdict;

typedef RonSQLErrorClass EC;
typedef NdbErrorVerdict::Action Act;

static int g_failures = 0;

#define CHECK(condition)                                               \
  do {                                                                 \
    if (!(condition)) {                                                \
      fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #condition);     \
      g_failures++;                                                    \
    }                                                                  \
  } while (0)

static const char*
action_name(Act action)
{
  switch (action)
  {
  case Act::RATE_LIMIT: return "RATE_LIMIT";
  case Act::RETRY: return "RETRY";
  case Act::FAIL: return "FAIL";
  }
  return "?";
}

/*
 * Error classes
 */

static void
test_class_names()
{
  CHECK(strcmp(ronsql_error_class_name(EC::SYNTAX), "syntax") == 0);
  CHECK(strcmp(ronsql_error_class_name(EC::SEMANTIC), "semantic") == 0);
  CHECK(strcmp(ronsql_error_class_name(EC::UNSUPPORTED), "unsupported") == 0);
  CHECK(strcmp(ronsql_error_class_name(EC::LIMIT), "limit") == 0);
  CHECK(strcmp(ronsql_error_class_name(EC::RESOURCE), "resource") == 0);
  CHECK(strcmp(ronsql_error_class_name(EC::INTERNAL), "internal") == 0);
}

struct MessageCase
{
  const char* message;
  RonSQLErrorClass expected;
};

/*
 * Messages RonSQL throws without a class, and the class their wording
 * gives.  The rules are ordered: a bug marker, then syntax, resource and
 * limit markers, then the "not supported" family, then the user-error
 * vocabulary; anything else is internal.
 */
static const MessageCase message_cases[] = {
  {nullptr, EC::INTERNAL},
  {"", EC::INTERNAL},
  {"Something odd happened.", EC::INTERNAL},
  {"Failed writing aggregation program. Please report a bug.",
   EC::INTERNAL},
  {"Aggregation over a type that is not supported. Please report a bug.",
   EC::INTERNAL},
  {"Bug in RonSQLPreparer::encode_constant", EC::INTERNAL},
  {"INTERNAL: inconsistent state", EC::INTERNAL},
  {"Syntax error in SQL statement: unexpected FROM", EC::SYNTAX},
  // Before "exceeded" (limit).
  {"Parser stack exceeded its maximum depth.", EC::SYNTAX},
  {"Empty input", EC::SYNTAX},
  {"Out of memory while processing the query.", EC::RESOURCE},
  {"No NDB object", EC::RESOURCE},
  {"Pass-through ORDER BY result too large.", EC::LIMIT},
  {"CTE body LIMIT value too large.", EC::LIMIT},
  {"Too many tables in the join.", EC::LIMIT},
  {"A subquery returned more than 1000 rows.", EC::LIMIT},
  {"The response exceeds the configured size.", EC::LIMIT},
  // Before "not supported".
  {"Too many columns: not supported.", EC::LIMIT},
  {"Aggregation over a column of this type is not supported.",
   EC::UNSUPPORTED},
  {"RonSQL feature not implemented: LIKE on BLOB", EC::UNSUPPORTED},
  {"Unsupported column type in comparison condition.", EC::UNSUPPORTED},
  {"GROUP BY on a BLOB or TEXT column is not supported.", EC::UNSUPPORTED},
  {"Integer type column compared to an integer literal out of range.",
   EC::SEMANTIC},
  {"Could not find column (column names are case sensitive).",
   EC::SEMANTIC},
  {"Failed to get table. Note that RonSQL only supports tables with "
   "ENGINE=NDB.",
   EC::SEMANTIC},
  {"Failed converting float literal to DECIMAL", EC::SEMANTIC},
  {"Unknown column 'x'", EC::SEMANTIC},
  {"Ambiguous column name.", EC::SEMANTIC},
  {"Tried to EXPLAIN with explain mode set to FORBID.", EC::SEMANTIC},
  {"Ungrouped column in SELECT list.", EC::SEMANTIC},
};

static void
test_classify_message()
{
  for (size_t i = 0; i < sizeof(message_cases) / sizeof(message_cases[0]);
       i++)
  {
    const MessageCase& c = message_cases[i];
    const RonSQLErrorClass cls = ronsql_classify_message(c.message);
    if (cls != c.expected)
    {
      fprintf(stderr, "FAIL: \"%s\" is %s, expected %s\n",
              c.message != nullptr ? c.message : "(null)",
              ronsql_error_class_name(cls),
              ronsql_error_class_name(c.expected));
      g_failures++;
    }
    if (c.message == nullptr) continue;
    // The constructors without a class classify the same way.
    const RonSQLPermanentError from_c_string(c.message);
    const RonSQLPermanentError from_string{std::string(c.message)};
    CHECK(from_c_string.error_class() == c.expected);
    CHECK(from_string.error_class() == c.expected);
    CHECK(from_string.ndb_error_code() == 0);
  }
  // A class given explicitly is kept, whatever the wording.
  const RonSQLPermanentError explicit_class(EC::LIMIT, "Unknown column",
                                            1860);
  CHECK(explicit_class.error_class() == EC::LIMIT);
  CHECK(explicit_class.ndb_error_code() == 1860);
  CHECK(strcmp(explicit_class.error_class_name(), "limit") == 0);
}

static void
test_ronsql_bug()
{
  bool thrown = false;
  try
  {
    ronsql_require_bug(1 + 1 == 3, "Arithmetic broke.");
  }
  catch (const RonSQLPermanentError& e)
  {
    thrown = true;
    CHECK(e.error_class() == EC::INTERNAL);
    CHECK(strcmp(e.what(), "Arithmetic broke. Please report a bug.") == 0);
  }
  CHECK(thrown);
  thrown = false;
  try
  {
    ronsql_require_bug(1 + 1 == 2, "Arithmetic broke.");
  }
  catch (const RonSQLPermanentError&)
  {
    thrown = true;
  }
  CHECK(!thrown);
}

/*
 * NDB errors
 */

static NdbError
ndb_error(int code, NdbError::Status status,
          NdbError::Classification classification, int mysql_code)
{
  NdbError err;
  err.code = code;
  err.status = status;
  err.classification = classification;
  err.mysql_code = mysql_code;
  return err;
}

static const NdbError::Status TMP = NdbError::TemporaryError;
static const NdbError::Status PRM = NdbError::PermanentError;
static const NdbError::Status UNK = NdbError::UnknownResult;
static const int DMEC = -1;  // no MySQL code, as in ndberror.cpp

static void
test_ndb_predicates()
{
  // RONDB-978: the four rate limit and quota rejections.
  CHECK(is_rate_limit_error(243));
  CHECK(is_rate_limit_error(2203));
  CHECK(is_rate_limit_error(247));
  CHECK(is_rate_limit_error(248));
  CHECK(!is_rate_limit_error(0));
  CHECK(!is_rate_limit_error(266));
  CHECK(!is_rate_limit_error(410));

  const NdbError::Classification UR = NdbError::UnknownResultError;
  const NdbError::Classification AE = NdbError::ApplicationError;
  // X-8: the cluster cannot serve this API node now.
  const int unavailable[] = {281, 299, 4009, 4035, 4036, 4037, 4038, 4039,
                             4040, 4041};
  for (size_t i = 0; i < sizeof(unavailable) / sizeof(unavailable[0]); i++)
  {
    const int code = unavailable[i];
    const bool app = (code == 281 || code == 299);
    const NdbError err = ndb_error(code, app ? PRM : UNK, app ? AE : UR,
                                   HA_ERR_NO_CONNECTION);
    CHECK(is_cluster_unavailable_error(err));
    CHECK(is_retryable_ndb_error(err));
    CHECK(is_resource_ndb_error(err));
  }
  // The other unknown results are not retried, but are a resource error.
  const NdbError receive_failed = ndb_error(4008, UNK, UR, DMEC);
  CHECK(!is_cluster_unavailable_error(receive_failed));
  CHECK(!is_retryable_ndb_error(receive_failed));
  CHECK(is_resource_ndb_error(receive_failed));
  // A temporary error, and a lock wait timeout whatever its status.
  const NdbError deadlock =
      ndb_error(266, TMP, NdbError::TimeoutExpired, HA_ERR_LOCK_WAIT_TIMEOUT);
  CHECK(is_retryable_ndb_error(deadlock));
  CHECK(is_resource_ndb_error(deadlock));
  const NdbError lock_wait = ndb_error(9999, PRM, AE, HA_ERR_LOCK_WAIT_TIMEOUT);
  CHECK(is_retryable_ndb_error(lock_wait));
  // The statement's own errors are neither.
  const NdbError overflow = ndb_error(1860, PRM, AE, DMEC);
  CHECK(!is_retryable_ndb_error(overflow));
  CHECK(!is_resource_ndb_error(overflow));
  const NdbError data_memory = ndb_error(827, PRM, NdbError::InsufficientSpace,
                                         HA_ERR_RECORD_FILE_FULL);
  CHECK(!is_retryable_ndb_error(data_memory));
  CHECK(!is_resource_ndb_error(data_memory));
}

struct VerdictCase
{
  const char* name;
  int code;
  NdbError::Status status;
  NdbError::Classification classification;
  int mysql_code;
  bool has_rate_limit_identity;
  bool schema_changed;
  bool output_started;
  // Expected
  bool reloads_schema;
  NdbErrorVerdict::Action action;
  RonSQLErrorClass error_class;  // FAIL only
  bool retryable_error;          // ",nu"
  bool output_marker;            // ",od"
};

typedef NdbError NE;

/*
 * The verdict of handle_ronsql_exception, error by error.  The status,
 * classification and MySQL code of each NDB error are those of
 * ndberror.cpp; 9999 is not an NDB error.
 */
static const VerdictCase verdict_cases[] = {
  // Temporary errors are retried (",nu"); after output a retry would
  // repeat it, so they are resource errors (",nu,od", X-8).
  {"266 deadlock", 266, TMP, NE::TimeoutExpired, HA_ERR_LOCK_WAIT_TIMEOUT,
   false, false, false, false, Act::RETRY, EC::INTERNAL, true, false},
  {"266 deadlock after output", 266, TMP, NE::TimeoutExpired,
   HA_ERR_LOCK_WAIT_TIMEOUT, false, false, true, false, Act::FAIL,
   EC::RESOURCE, true, true},
  {"4010 node failure", 4010, TMP, NE::NodeRecoveryError,
   HA_ERR_LOCK_WAIT_TIMEOUT, false, false, false, false, Act::RETRY,
   EC::INTERNAL, true, false},
  {"4010 node failure after output", 4010, TMP, NE::NodeRecoveryError,
   HA_ERR_LOCK_WAIT_TIMEOUT, false, false, true, false, Act::FAIL,
   EC::RESOURCE, true, true},
  {"233 temporary resource", 233, TMP, NE::TemporaryResourceError, DMEC,
   false, false, false, false, Act::RETRY, EC::INTERNAL, true, false},
  {"410 REDO overload", 410, TMP, NE::OverloadError, DMEC, false, false,
   false, false, Act::RETRY, EC::INTERNAL, true, false},
  {"9999 permanent lock wait timeout", 9999, PRM, NE::ApplicationError,
   HA_ERR_LOCK_WAIT_TIMEOUT, false, false, false, false, Act::RETRY,
   EC::INTERNAL, true, false},
  // RONDB-978: a rate limit rejection is reported as such, only for a
  // tagged statement, and never retried, also after output.
  {"243 rate limit, tagged", 243, TMP, NE::OverloadError, DMEC, true, false,
   false, false, Act::RATE_LIMIT, EC::INTERNAL, false, false},
  {"243 rate limit, not tagged", 243, TMP, NE::OverloadError, DMEC, false,
   false, false, false, Act::RETRY, EC::INTERNAL, true, false},
  {"2203 rate limit, tagged, after output", 2203, TMP, NE::OverloadError,
   DMEC, true, false, true, false, Act::RATE_LIMIT, EC::INTERNAL, false,
   false},
  {"247 quota, tagged", 247, TMP, NE::OverloadError, DMEC, true, false,
   false, false, Act::RATE_LIMIT, EC::INTERNAL, false, false},
  {"248 quota, tagged", 248, TMP, NE::OverloadError, DMEC, true, false,
   false, false, Act::RATE_LIMIT, EC::INTERNAL, false, false},
  // X-8: an unavailable cluster is retried; other unknown results are
  // resource errors.
  {"4009 no data node", 4009, UNK, NE::UnknownResultError,
   HA_ERR_NO_CONNECTION, false, false, false, false, Act::RETRY,
   EC::INTERNAL, true, false},
  {"4041 single user mode", 4041, UNK, NE::UnknownResultError,
   HA_ERR_NO_CONNECTION, false, false, false, false, Act::RETRY,
   EC::INTERNAL, true, false},
  {"281 cluster shutdown", 281, PRM, NE::ApplicationError,
   HA_ERR_NO_CONNECTION, false, false, false, false, Act::RETRY,
   EC::INTERNAL, true, false},
  {"299 single user mode", 299, PRM, NE::ApplicationError, DMEC, false,
   false, false, false, Act::RETRY, EC::INTERNAL, true, false},
  {"281 cluster shutdown after output", 281, PRM, NE::ApplicationError,
   HA_ERR_NO_CONNECTION, false, false, true, false, Act::FAIL, EC::RESOURCE,
   true, true},
  {"4008 receive failed", 4008, UNK, NE::UnknownResultError, DMEC, false,
   false, false, false, Act::FAIL, EC::RESOURCE, false, false},
  {"4012 ndbd time-out", 4012, UNK, NE::UnknownResultError, DMEC, false,
   false, false, false, Act::FAIL, EC::RESOURCE, false, false},
  // The statement's own errors, by classification.
  {"1860 arithmetic overflow", 1860, PRM, NE::ApplicationError, DMEC, false,
   false, false, false, Act::FAIL, EC::SEMANTIC, false, false},
  // An aggregation group larger than a query memory chunk fails at once;
  // out of query memory is retried.
  {"1873 aggregation group too large", 1873, PRM, NE::ApplicationError,
   DMEC, false, false, false, false, Act::FAIL, EC::SEMANTIC, false, false},
  {"1870 aggregation out of query memory", 1870, TMP,
   NE::TemporaryResourceError, DMEC, false, false, false, false, Act::RETRY,
   EC::INTERNAL, true, false},
  {"626 no such row", 626, PRM, NE::NoDataFound, HA_ERR_KEY_NOT_FOUND,
   false, false, false, false, Act::FAIL, EC::SEMANTIC, false, false},
  {"827 out of DataMemory", 827, PRM, NE::InsufficientSpace,
   HA_ERR_RECORD_FILE_FULL, false, false, false, false, Act::FAIL,
   EC::RESOURCE, false, false},
  {"4003 not implemented", 4003, PRM, NE::FunctionNotImplemented, DMEC,
   false, false, false, false, Act::FAIL, EC::UNSUPPORTED, false, false},
  {"4000 memory allocation", 4000, PRM, NE::InternalError, DMEC, false,
   false, false, false, Act::FAIL, EC::INTERNAL, false, false},
  {"721 object exists", 721, PRM, NE::SchemaObjectExists,
   HA_ERR_TABLE_EXIST, false, false, false, false, Act::FAIL, EC::INTERNAL,
   false, false},
  // A schema error reloads the schema (",rl"), and is retried when it
  // changed and no output went out.
  {"241 schema changed", 241, PRM, NE::SchemaError,
   HA_ERR_TABLE_DEF_CHANGED, false, true, false, true, Act::RETRY,
   EC::INTERNAL, false, false},
  {"241 schema changed, tagged", 241, PRM, NE::SchemaError,
   HA_ERR_TABLE_DEF_CHANGED, true, true, false, true, Act::RETRY,
   EC::INTERNAL, false, false},
  {"241 schema unchanged", 241, PRM, NE::SchemaError,
   HA_ERR_TABLE_DEF_CHANGED, false, false, false, true, Act::FAIL,
   EC::SEMANTIC, false, false},
  {"241 schema changed after output", 241, PRM, NE::SchemaError,
   HA_ERR_TABLE_DEF_CHANGED, false, true, true, true, Act::FAIL,
   EC::SEMANTIC, false, true},
};

static void
test_ndb_error_verdict()
{
  for (size_t i = 0; i < sizeof(verdict_cases) / sizeof(verdict_cases[0]);
       i++)
  {
    const VerdictCase& c = verdict_cases[i];
    const NdbError err =
        ndb_error(c.code, c.status, c.classification, c.mysql_code);
    const bool reloads =
        ndb_error_reloads_schema(err, c.has_rate_limit_identity);
    const NdbErrorVerdict v = ndb_error_verdict(
        err, c.has_rate_limit_identity, c.schema_changed, c.output_started);
    bool ok = reloads == c.reloads_schema && v.action == c.action &&
              v.retryable_error == c.retryable_error &&
              v.output_started == c.output_marker;
    if (c.action == Act::FAIL && v.error_class != c.error_class)
      ok = false;
    if (!ok)
    {
      fprintf(stderr,
              "FAIL %s: reloads %d, %s %s, nu %d, od %d; expected "
              "reloads %d, %s %s, nu %d, od %d\n",
              c.name, reloads ? 1 : 0, action_name(v.action),
              ronsql_error_class_name(v.error_class),
              v.retryable_error ? 1 : 0, v.output_started ? 1 : 0,
              c.reloads_schema ? 1 : 0, action_name(c.action),
              ronsql_error_class_name(c.error_class),
              c.retryable_error ? 1 : 0, c.output_marker ? 1 : 0);
      g_failures++;
    }
  }
}

int
main()
{
  test_class_names();
  test_classify_message();
  test_ronsql_bug();
  test_ndb_predicates();
  test_ndb_error_verdict();
  if (g_failures != 0)
  {
    printf("FAIL: %d checks failed\n", g_failures);
    return 1;
  }
  printf("OK\n");
  return 0;
}
