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

// T-16: the HTTP status RDRS answers a permanent RonSQL error with, for
// every error class, and for the wording of the messages RonSQL throws
// without a class (ronsql_classify_message): rewording such a message can
// change its status.  Cluster-free; linked into ronsql_oom_test, whose
// main() initializes the NDB client library.
#include "db_operations/ronsql/ronsql_operation.hpp"

#include <gtest/gtest.h>

#include <cstddef>

namespace {

int http_code(RonSQLErrorClass cls, const char *what) {
  return static_cast<int>(ronsql_http_code_for(cls, what));
}

TEST(RonSQLHttpCode, EveryClass) {
  EXPECT_EQ(400, http_code(RonSQLErrorClass::SYNTAX, "Syntax error"));
  EXPECT_EQ(400, http_code(RonSQLErrorClass::SEMANTIC, "Unknown column"));
  EXPECT_EQ(400, http_code(RonSQLErrorClass::UNSUPPORTED, "Not supported"));
  // A limit of the query's shape is 400; a result or response that is too
  // large is 413.
  EXPECT_EQ(400, http_code(RonSQLErrorClass::LIMIT, "Too many tables"));
  EXPECT_EQ(400, http_code(RonSQLErrorClass::LIMIT, nullptr));
  EXPECT_EQ(413, http_code(RonSQLErrorClass::LIMIT,
                           "Pass-through ORDER BY result too large."));
  EXPECT_EQ(413, http_code(RonSQLErrorClass::LIMIT, "Response too large"));
  // Only a LIMIT error is 413 for that wording.
  EXPECT_EQ(500, http_code(RonSQLErrorClass::INTERNAL,
                           "Pass-through ORDER BY result too large."));
  EXPECT_EQ(503, http_code(RonSQLErrorClass::RESOURCE,
                           "Out of memory while processing the query."));
  EXPECT_EQ(500, http_code(RonSQLErrorClass::INTERNAL,
                           "Something broke. Please report a bug."));
  // A class the mapping does not know is a server error.
  EXPECT_EQ(500, http_code(static_cast<RonSQLErrorClass>(99), "x"));
}

struct WordingCase {
  const char *message;
  int status;
};

TEST(RonSQLHttpCode, MessageWording) {
  const WordingCase cases[] = {
      {"Syntax error in SQL statement: unexpected FROM", 400},
      {"Parser stack exceeded its maximum depth.", 400},
      {"Could not find column (column names are case sensitive).", 400},
      {"Integer type column compared to an integer literal out of range.",
       400},
      {"Aggregation over a column of this type is not supported.", 400},
      {"CTE body LIMIT value too large.", 400},
      {"A subquery returned more than 1000 rows.", 400},
      {"Pass-through ORDER BY result too large.", 413},
      {"Out of memory while processing the query.", 503},
      {"No NDB object", 503},
      {"Failed writing aggregation program. Please report a bug.", 500},
      {"Something odd happened.", 500},
  };
  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
    SCOPED_TRACE(cases[i].message);
    // The class the one-argument constructor gives, as RDRS sees it.
    const RonSQLPermanentError error(cases[i].message);
    EXPECT_EQ(cases[i].status, http_code(error.error_class(), error.what()));
  }
}

}  // namespace
