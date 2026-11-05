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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301,
 * USA.
 */

// Parser allocation failures need no cluster: normal preparation uses EXPLAIN,
// while ParseOnly uses the authorization path's usual ALLOW mode.
#include "db_operations/ronsql/ronsql_operation.hpp"
#include "storage/ndb/src/ronsql/RonSQLPreparer.hpp"

#include <gtest/gtest.h>
#include <NdbMutex.h>
#include <ndb_init.h>

#include <cstddef>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

// Owned by main.cc in the server binary.
NdbMutex *globalConfigsMutex = nullptr;

namespace {

using AllocationHook = RonSQLAllocationTestHook;
using AllocationSite = AllocationHook::Site;

constexpr const char *kOomMessage = "Out of memory while processing the query.";

enum class EntryPoint { Normal, ParseOnly, RestOperation };

struct Request {
  ArenaMalloc arena{RonSQLExecParams::ARENA_MALLOC_PAGE_SIZE};
  std::string sql;
  std::ostringstream out;
  std::ostringstream err;
  RonSQLExecParams params;

  explicit Request(const std::string &query, AllocationHook *hook = nullptr,
                   Ndb *ndb = nullptr)
      : sql(query) {
    sql.append(2, '\0');
    params.sql_buffer = &sql[0];
    params.sql_len = sql.size();
    params.amalloc = &arena;
    params.ndb = ndb;
    params.allocation_test_hook = hook;
    params.explain_mode = ndb ? RonSQLExecParams::ExplainMode::ALLOW
                             : RonSQLExecParams::ExplainMode::FORCE;
    params.output_format = RonSQLExecParams::OutputFormat::TEXT;
    params.out_stream = &out;
    params.err_stream = &err;
  }
};

struct Outcome {
  HTTP_CODE http_code;
  std::string error_class;
  std::string message;
};

void expect_resource_failure(const Outcome &failure) {
  EXPECT_EQ(503, failure.http_code) << failure.message;
  EXPECT_EQ("resource", failure.error_class);
  EXPECT_NE(std::string::npos, failure.message.find(kOomMessage))
      << failure.message;
}

Outcome run_request(Request &request, EntryPoint entry) {
  if (entry == EntryPoint::RestOperation) {
    const RS_Status status = ronsql_op(request.params);
    return {status.http_code,
            request.params.error_class ? request.params.error_class : "",
            status.message};
  }
  try {
    if (entry == EntryPoint::ParseOnly) {
      request.params.explain_mode = RonSQLExecParams::ExplainMode::ALLOW;
      RonSQLPreparer parser(request.params, RonSQLPreparer::ParseOnly{"test"});
    } else {
      RonSQLPreparer executor(request.params);
      executor.execute();
    }
    return {SUCCESS, "", ""};
  } catch (const RonSQLPermanentError &error) {
    // Use the mapper shared with the authorization controller. Other exception
    // types deliberately escape and fail the test.
    return {ronsql_http_code_for(error.error_class(), error.what()),
            error.error_class_name(), error.what()};
  }
}

std::string nested_query(size_t depth) {
  std::string sql = "SELECT COUNT(*) FROM oom_parser WHERE ";
  for (size_t i = 0; i < depth; ++i) sql += "NOT ";
  return sql + "id = 1;";
}

class RonSQLParserAllocationFailure
    : public ::testing::TestWithParam<EntryPoint> {
 protected:
  void fail_each_allocation(const std::string &sql, AllocationSite site,
                            size_t minimum_allocations) {
    size_t allocation_count = 0;
    {
      AllocationHook hook{site};
      Request request(sql, &hook);
      const Outcome result = run_request(request, GetParam());
      ASSERT_EQ(200, result.http_code) << result.message << request.err.str();
      ASSERT_GE(hook.attempts, minimum_allocations);
      allocation_count = hook.attempts;
    }

    for (size_t fail_on = 1; fail_on <= allocation_count; ++fail_on) {
      SCOPED_TRACE(::testing::Message()
                   << "allocation " << fail_on << " of " << allocation_count);
      AllocationHook hook{site, fail_on};
      {
        Request request(sql, &hook);
        const Outcome failure = run_request(request, GetParam());
        expect_resource_failure(failure);
        EXPECT_TRUE(request.out.str().empty());
        ASSERT_EQ(fail_on, hook.attempts)
            << "The failure must return without retrying the request.";
      }  // Reclaim the arena even when the preparer constructor threw.

      // Retry explicitly in a fresh request with the same one-shot hook.
      // This also checks recovery after partial scanner/parser construction.
      {
        Request request(sql, &hook);
        const Outcome recovered = run_request(request, GetParam());
        ASSERT_EQ(200, recovered.http_code)
            << recovered.message << request.err.str();
      }
      EXPECT_EQ(fail_on + allocation_count, hook.attempts);

      // An independent request must not inherit or modify the prior hook.
      {
        Request request(sql);
        const Outcome independent = run_request(request, GetParam());
        ASSERT_EQ(200, independent.http_code)
            << independent.message << request.err.str();
      }
      EXPECT_EQ(fail_on + allocation_count, hook.attempts);
    }
  }
};

TEST_P(RonSQLParserAllocationFailure, LexerInitialization) {
  // Scanner state, scan-buffer state, and the initial buffer stack.
  fail_each_allocation("SELECT COUNT(*) FROM oom_parser WHERE id = 1;",
                       AllocationSite::LEXER, 3);
}

TEST_P(RonSQLParserAllocationFailure, ParserStackGrowth) {
  // More than one relocation, but comfortably below YYMAXDEPTH.
  fail_each_allocation(nested_query(1200), AllocationSite::PARSER_STACK, 2);
}

TEST_P(RonSQLParserAllocationFailure, DepthLimitIsNotOutOfMemory) {
  AllocationHook hook{AllocationSite::PARSER_STACK};  // Count, never fail.
  Request request(nested_query(12000), &hook);
  const Outcome failure = run_request(request, GetParam());
  EXPECT_GT(hook.attempts, 0U);
  EXPECT_EQ(400, failure.http_code) << failure.message;
  // Preserve the existing classification of a parser depth-limit rejection.
  EXPECT_EQ("syntax", failure.error_class);
  EXPECT_NE(std::string::npos,
            failure.message.find("Parser stack exceeded its maximum depth."))
      << failure.message;
  EXPECT_EQ(std::string::npos, failure.message.find(kOomMessage));
  EXPECT_TRUE(request.out.str().empty());
}

INSTANTIATE_TEST_SUITE_P(
    EntryPoints, RonSQLParserAllocationFailure,
    ::testing::Values(EntryPoint::Normal, EntryPoint::ParseOnly,
                      EntryPoint::RestOperation),
    [](const ::testing::TestParamInfo<EntryPoint> &info) {
      switch (info.param) {
        case EntryPoint::Normal: return "Normal";
        case EntryPoint::ParseOnly: return "ParseOnly";
        case EntryPoint::RestOperation: return "RestOperation";
      }
      return "Unknown";
    });

// A flat list longer than YYMAXDEPTH must still fit the initial parser stack.
// ParseOnly avoids schema access and semantic GROUP BY column limits.
void expect_flat_list_uses_initial_stack(const char *clause) {
  std::string sql =
      std::string("SELECT COUNT(*) FROM oom_parser ") + clause + " id";
  for (size_t i = 1; i < 12000; ++i) sql += ", id";
  sql += ";";

  AllocationHook hook{AllocationSite::PARSER_STACK, 1};
  Request request(sql, &hook);
  const Outcome result = run_request(request, EntryPoint::ParseOnly);
  EXPECT_EQ(200, result.http_code) << result.message << request.err.str();
  EXPECT_EQ(0U, hook.attempts)
      << "Flat column lists must not grow the parser stack.";
}

TEST(RonSQLParserListStack, GroupByUsesInitialStack) {
  expect_flat_list_uses_initial_stack("GROUP BY");
}

TEST(RonSQLParserListStack, OrderByUsesInitialStack) {
  expect_flat_list_uses_initial_stack("ORDER BY");
}

struct DecimalCase {
  const char *name;
  const char *predicate;
  const char *expected_output;
  Uint64 transactions;
};

// The fixture is created by ronsql_parser_oom.test. Subqueries finish their
// own transaction before the outer query opens one and encodes the result.
const DecimalCase kDecimalCases[] = {
    {"SignedInteger", "amount = -12", "n\n1\n", 1},
    {"SignedFloat", "amount = 12.50", "n\n1\n", 1},
    {"UnsignedInteger", "unsigned_amount = 12", "n\n1\n", 1},
    {"UnsignedFloat", "unsigned_amount = 12.50", "n\n1\n", 1},
    {"SubqueryInteger",
     "amount > (SELECT MIN(id) FROM ronsql_oom_values)", "n\n2\n", 2},
    {"SubqueryFloat",
     "amount = (SELECT MIN(unsigned_amount) FROM ronsql_oom_values)",
     "n\n1\n", 2},
};

std::string decimal_query(const DecimalCase &test_case) {
  // Force filter encoding after start_transaction(), avoiding index bounds.
  return std::string("SELECT COUNT(*) AS n FROM ronsql_oom_values "
                     "IGNORE INDEX (amount_idx) WHERE ") +
         test_case.predicate + ";";
}

constexpr const char *kPreparationQuery =
    "SELECT COUNT(*) AS n FROM ronsql_oom_values FORCE INDEX (amount_idx) "
    "WHERE amount IN (-12, 12.50);";

struct TransactionCounts {
  Uint64 started;
  Uint64 closed;

  explicit TransactionCounts(const Ndb &ndb)
      : started(ndb.getClientStat(Ndb::TransStartCount)),
        closed(ndb.getClientStat(Ndb::TransCloseCount)) {}

  void expect_closed(const Ndb &ndb, Uint64 expected_starts) const {
    const Uint64 starts = ndb.getClientStat(Ndb::TransStartCount) - started;
    EXPECT_EQ(expected_starts, starts);
    EXPECT_EQ(starts, ndb.getClientStat(Ndb::TransCloseCount) - closed);
  }
};

TEST(RonSQLDecimalParseOnly, DoesNotEncode) {
  auto check = [](const std::string &sql) {
    AllocationHook hook{AllocationSite::DECIMAL_BUFFER, 1};
    Request request(sql, &hook);  // No Ndb object or schema access.
    const Outcome result = run_request(request, EntryPoint::ParseOnly);
    EXPECT_EQ(200, result.http_code) << result.message << request.err.str();
    EXPECT_EQ(0U, hook.attempts);
    EXPECT_TRUE(request.out.str().empty());
  };
  for (const DecimalCase &test_case : kDecimalCases) {
    SCOPED_TRACE(test_case.name);
    check(decimal_query(test_case));
  }
  check(kPreparationQuery);
}

class RonSQLDecimalAllocationFailure : public ::testing::Test {
 protected:
  static std::unique_ptr<Ndb_cluster_connection> cluster;
  static std::unique_ptr<Ndb> ndb;

  static void SetUpTestSuite() {
    const char *connect_string = std::getenv("NDB_CONNECTSTRING");
    ASSERT_NE(nullptr, connect_string)
        << "Run through ronsql.ronsql_parser_oom, or supply its NDB fixture.";
    ASSERT_NE('\0', *connect_string);
    cluster = std::make_unique<Ndb_cluster_connection>(connect_string);
    ASSERT_EQ(0, cluster->connect(4, 5, 0));
    ASSERT_EQ(0, cluster->wait_until_ready(30, 30));
    auto candidate = std::make_unique<Ndb>(cluster.get(), "test");
    ASSERT_EQ(0, candidate->init())
        << candidate->getNdbError().message;
    ASSERT_NE(nullptr,
              candidate->getDictionary()->getTable("ronsql_oom_values"))
        << candidate->getDictionary()->getNdbError().message;
    ndb = std::move(candidate);
  }

  static void TearDownTestSuite() {
    ndb.reset();
    cluster.reset();
  }

  void SetUp() override { ASSERT_NE(nullptr, ndb.get()); }

  void expect_success(const std::string &sql, const char *expected_output,
                      EntryPoint entry, Uint64 transactions,
                      AllocationHook *hook = nullptr) {
    const TransactionCounts before(*ndb);
    Request request(sql, hook, ndb.get());
    const Outcome result = run_request(request, entry);
    ASSERT_EQ(200, result.http_code) << result.message << request.err.str();
    EXPECT_EQ(expected_output, request.out.str());
    before.expect_closed(*ndb, transactions);
  }
};

std::unique_ptr<Ndb_cluster_connection> RonSQLDecimalAllocationFailure::cluster;
std::unique_ptr<Ndb> RonSQLDecimalAllocationFailure::ndb;

TEST_F(RonSQLDecimalAllocationFailure, PreparationFailures) {
  size_t preparation_allocations = 0;
  {
    AllocationHook hook{AllocationSite::DECIMAL_BUFFER};
    Request request(kPreparationQuery, &hook, ndb.get());
    const TransactionCounts before(*ndb);
    RonSQLPreparer executor(request.params);
    before.expect_closed(*ndb, 0);
    preparation_allocations = hook.attempts;
    // IN-list deduplication encodes both the integer and floating literal.
    ASSERT_GE(preparation_allocations, 2U);
    executor.execute();
    EXPECT_EQ("n\n2\n", request.out.str());
    before.expect_closed(*ndb, 1);
  }

  for (EntryPoint entry : {EntryPoint::Normal, EntryPoint::RestOperation}) {
    SCOPED_TRACE(entry == EntryPoint::Normal ? "Normal" : "RestOperation");
    for (size_t fail_on = 1; fail_on <= preparation_allocations; ++fail_on) {
      SCOPED_TRACE(fail_on);
      AllocationHook hook{AllocationSite::DECIMAL_BUFFER, fail_on};
      {
        const TransactionCounts before(*ndb);
        Request request(kPreparationQuery, &hook, ndb.get());
        const Outcome failure = run_request(request, entry);
        expect_resource_failure(failure);
        EXPECT_TRUE(request.out.str().empty());
        ASSERT_EQ(fail_on, hook.attempts) << "Unexpected automatic retry.";
        before.expect_closed(*ndb, 0);
      }
      ASSERT_NO_FATAL_FAILURE(
          expect_success(kPreparationQuery, "n\n2\n", entry, 1, &hook));
    }
  }
}

TEST_F(RonSQLDecimalAllocationFailure, ExecutionFailureClosesTransaction) {
  for (const DecimalCase &test_case : kDecimalCases) {
    SCOPED_TRACE(test_case.name);
    const std::string sql = decimal_query(test_case);
    AllocationHook hook{AllocationSite::DECIMAL_BUFFER};
    {
      Request request(sql, &hook, ndb.get());
      RonSQLPreparer executor(request.params);
      ASSERT_EQ(0U, hook.attempts);
      const TransactionCounts before(*ndb);
      hook.fail_on = 1;
      bool failed = false;
      try {
        executor.execute();
      } catch (const RonSQLPermanentError &error) {
        failed = true;
        EXPECT_EQ(RonSQLErrorClass::RESOURCE, error.error_class());
        EXPECT_STREQ(kOomMessage, error.what());
        EXPECT_EQ(503, ronsql_http_code_for(error.error_class(), error.what()));
      }
      EXPECT_TRUE(failed) << "Execution did not report the injected failure.";
      EXPECT_EQ(1U, hook.attempts);
      EXPECT_TRUE(request.out.str().empty());
      // Check before executor's destructor can hide missing error cleanup.
      // Two starts for scalar cases prove the outer transaction opened too.
      before.expect_closed(*ndb, test_case.transactions);
    }
    ASSERT_NO_FATAL_FAILURE(expect_success(
        sql, test_case.expected_output, EntryPoint::Normal,
        test_case.transactions, &hook));
  }
}

TEST_F(RonSQLDecimalAllocationFailure, RestFailuresAreNotRetried) {
  for (const DecimalCase &test_case : kDecimalCases) {
    SCOPED_TRACE(test_case.name);
    const std::string sql = decimal_query(test_case);
    AllocationHook baseline{AllocationSite::DECIMAL_BUFFER};
    ASSERT_NO_FATAL_FAILURE(expect_success(
        sql, test_case.expected_output, EntryPoint::RestOperation,
        test_case.transactions, &baseline));
    ASSERT_GE(baseline.attempts, 1U);
    for (size_t fail_on = 1; fail_on <= baseline.attempts; ++fail_on) {
      SCOPED_TRACE(fail_on);
      AllocationHook hook{AllocationSite::DECIMAL_BUFFER, fail_on};
      {
        const TransactionCounts before(*ndb);
        Request request(sql, &hook, ndb.get());
        const Outcome failure = run_request(request, EntryPoint::RestOperation);
        expect_resource_failure(failure);
        EXPECT_TRUE(request.out.str().empty());
        ASSERT_EQ(fail_on, hook.attempts) << "Unexpected automatic retry.";
        before.expect_closed(*ndb, test_case.transactions);
      }
      ASSERT_NO_FATAL_FAILURE(expect_success(
          sql, test_case.expected_output, EntryPoint::RestOperation,
          test_case.transactions, &hook));
      EXPECT_EQ(fail_on + baseline.attempts, hook.attempts);
    }
  }
}

}  // namespace

int main(int argc, char **argv) {
  if (ndb_init() != 0) return 1;
  ::testing::InitGoogleTest(&argc, argv);
  const int result = RUN_ALL_TESTS();
  ndb_end(0);
  return result;
}
