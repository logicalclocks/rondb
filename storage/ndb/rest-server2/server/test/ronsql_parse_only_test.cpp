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

// Cluster-free checks of the inventory used before REST authorization.
// These assertions concern ParseOnly, not full execution support.
#include "storage/ndb/src/ronsql/RonSQLPreparer.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <initializer_list>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace {

struct ParseOnlyRequest {
  ArenaMalloc arena{RonSQLExecParams::ARENA_MALLOC_PAGE_SIZE};
  std::string sql;
  std::ostringstream out;
  std::ostringstream err;
  RonSQLExecParams params;

  explicit ParseOnlyRequest(const char *query) : sql(query) {
    sql.append(2, '\0');
    params.sql_buffer = &sql[0];
    params.sql_len = sql.size();
    params.amalloc = &arena;
    params.ndb = nullptr;
    params.explain_mode = RonSQLExecParams::ExplainMode::ALLOW;
    params.output_format = RonSQLExecParams::OutputFormat::TEXT;
    params.out_stream = &out;
    params.err_stream = &err;
  }
};

struct ExpectedAccessTable {
  const char *name;
  std::initializer_list<const char *> columns;
};

void expect_access_inventory(
    const char *query, std::initializer_list<ExpectedAccessTable> expected) {
  SCOPED_TRACE(query);
  ParseOnlyRequest request(query);
  RonSQLPreparer parser(request.params, RonSQLPreparer::ParseOnly{"test"});
  const auto &tables = parser.get_accessed_tables();
  const auto &columns = parser.get_accessed_columns();
  std::vector<std::string> actual_tables, expected_tables;
  using TableColumn = std::pair<std::string, std::string>;
  std::vector<TableColumn> actual_columns, expected_columns;

  for (Uint32 i = 0; i < tables.size(); ++i)
    actual_tables.emplace_back(tables[i].str, tables[i].len);
  for (Uint32 i = 0; i < columns.size(); ++i) {
    const auto &column = columns[i];
    ASSERT_LT(column.table_idx, tables.size());
    const auto &table = tables[column.table_idx];
    actual_columns.emplace_back(std::string(table.str, table.len),
                                std::string(column.column.str,
                                            column.column.len));
  }
  for (const auto &table : expected) {
    expected_tables.emplace_back(table.name);
    for (const char *column : table.columns)
      expected_columns.emplace_back(table.name, column);
  }

  // Ignore ordering, but keep duplicates so duplicate inventory entries fail.
  std::sort(actual_tables.begin(), actual_tables.end());
  std::sort(expected_tables.begin(), expected_tables.end());
  std::sort(actual_columns.begin(), actual_columns.end());
  std::sort(expected_columns.begin(), expected_columns.end());
  EXPECT_EQ(expected_tables, actual_tables);
  EXPECT_EQ(expected_columns, actual_columns);
  EXPECT_TRUE(request.out.str().empty()) << request.out.str();
  EXPECT_TRUE(request.err.str().empty()) << request.err.str();
}

void expect_parse_only_rejection(const char *query, const char *diagnostic) {
  SCOPED_TRACE(query);
  ParseOnlyRequest request(query);
  try {
    RonSQLPreparer parser(request.params, RonSQLPreparer::ParseOnly{"test"});
    FAIL() << "Expected ParseOnly to reject the statement";
  } catch (const RonSQLPermanentError &error) {
    EXPECT_EQ(RonSQLErrorClass::UNSUPPORTED, error.error_class());
    EXPECT_NE(std::string::npos, std::string(error.what()).find(diagnostic))
        << error.what();
  }
}

TEST(RonSQLParseOnlyInventory, CountStarStillRequiresTableAccess) {
  expect_access_inventory("SELECT COUNT(*) FROM inv_orders;",
                          {{"inv_orders", {}}});
}

TEST(RonSQLParseOnlyInventory, CaseAndHavingOperandsAreIncluded) {
  expect_access_inventory(
      "SELECT o.customer_id, "
      "SUM(CASE WHEN o.flag = 1 THEN o.amount ELSE o.discount END) "
      "FROM inv_orders AS o WHERE o.active_flag = 1 GROUP BY o.customer_id "
      "HAVING SUM(o.tax) > 0;",
      {{"inv_orders",
        {"customer_id", "flag", "amount", "discount", "active_flag", "tax"}}});
}

TEST(RonSQLParseOnlyInventory, ProjectionOrderByColumnIsIncluded) {
  expect_access_inventory(
      "SELECT o.id FROM inv_orders AS o WHERE o.active_flag = 1 "
      "ORDER BY o.sort_key LIMIT 5;",
      {{"inv_orders", {"id", "active_flag", "sort_key"}}});
}

TEST(RonSQLParseOnlyInventory, JoinIncludesBothOnOperands) {
  expect_access_inventory(
      "SELECT SUM(o.amount) FROM inv_customers AS c "
      "JOIN inv_orders AS o ON o.customer_id = c.id "
      "AND o.tenant_id = c.tenant_id WHERE c.region = 1 AND o.state = 2;",
      {{"inv_customers", {"id", "tenant_id", "region"}},
       {"inv_orders", {"amount", "customer_id", "tenant_id", "state"}}});
}

TEST(RonSQLParseOnlyInventory, UnqualifiedJoinColumnsCoverBothTables) {
  // Without a dictionary, either table may own amount or flag.
  expect_access_inventory(
      "SELECT SUM(amount) FROM inv_customers AS c "
      "JOIN inv_orders AS o ON o.customer_id = c.id WHERE flag = 1;",
      {{"inv_customers", {"amount", "flag", "id"}},
       {"inv_orders", {"amount", "flag", "customer_id"}}});
}

TEST(RonSQLParseOnlyInventory, SelfJoinDeduplicatesTablesAndColumns) {
  expect_access_inventory(
      "SELECT SUM(b.amount) FROM inv_orders AS a "
      "JOIN inv_orders AS b ON b.parent_id = a.id "
      "WHERE a.id > 0 AND b.amount > 0;",
      {{"inv_orders", {"amount", "parent_id", "id"}}});
}

TEST(RonSQLParseOnlyInventory, NestedSubqueriesKeepQualifiedReferences) {
  expect_access_inventory(
      "SELECT COUNT(*) FROM inv_outer AS o WHERE EXISTS "
      "(SELECT i.id FROM inv_inner AS i WHERE i.owner_id = o.id AND EXISTS "
      "(SELECT j.id FROM inv_leaf AS j "
      "WHERE j.inner_id = i.id AND j.flag = 1));",
      {{"inv_outer", {"id"}},
       {"inv_inner", {"id", "owner_id"}},
       {"inv_leaf", {"id", "inner_id", "flag"}}});
}

TEST(RonSQLParseOnlyInventory, UnqualifiedSubqueryColumnsCoverOuterScope) {
  // Unqualified subquery references may be correlated.
  expect_access_inventory(
      "SELECT COUNT(*) FROM inv_outer AS o WHERE EXISTS "
      "(SELECT marker FROM inv_inner AS i WHERE shared_key = o.id);",
      {{"inv_outer", {"marker", "shared_key", "id"}},
       {"inv_inner", {"marker", "shared_key"}}});
}

TEST(RonSQLParseOnlyInventory, ReusedAliasConservativelyCoversBothScopes) {
  // The inventory intentionally matches a qualifier across all scopes.
  expect_access_inventory(
      "SELECT SUM(x.amount) FROM inv_orders AS x WHERE EXISTS "
      "(SELECT x.id FROM inv_audit AS x WHERE x.owner_id = 7);",
      {{"inv_orders", {"amount", "id", "owner_id"}},
       {"inv_audit", {"amount", "id", "owner_id"}}});
}

TEST(RonSQLParseOnlyInventory, ChainedCtesExposeOnlyBaseTables) {
  expect_access_inventory(
      "WITH inv_totals AS (SELECT o.customer_id AS k, SUM(o.amount) AS total "
      "FROM inv_orders AS o GROUP BY o.customer_id), "
      "inv_rollup AS (SELECT t.k AS k, SUM(t.total) AS rolled "
      "FROM inv_totals AS t GROUP BY t.k) "
      "SELECT c.id, SUM(r.rolled) FROM inv_customers AS c "
      "JOIN inv_rollup AS r ON r.k = c.id GROUP BY c.id;",
      {{"inv_orders", {"customer_id", "amount"}},
       {"inv_customers", {"id"}}});
}

TEST(RonSQLParseOnlyInventory, CteBodyColumnsDoNotLeakIntoMainScope) {
  expect_access_inventory(
      "WITH inv_totals AS (SELECT customer_id AS k, SUM(amount) AS total "
      "FROM inv_orders WHERE flag = 1 GROUP BY customer_id) "
      "SELECT SUM(t.total) FROM inv_customers AS c "
      "JOIN inv_totals AS t ON t.k = c.id;",
      {{"inv_orders", {"customer_id", "amount", "flag"}},
       {"inv_customers", {"id"}}});
}

TEST(RonSQLParseOnlyInventory, UnusedCteStillContributesItsInventory) {
  // Authorization records the statement as written, including unused CTEs.
  expect_access_inventory(
      "WITH inv_unused AS (SELECT SUM(secret) AS s FROM inv_hidden) "
      "SELECT COUNT(*) FROM inv_visible;",
      {{"inv_hidden", {"secret"}}, {"inv_visible", {}}});
}

TEST(RonSQLParseOnlyInventory, CteNameIsNotVisibleInsideItsOwnBody) {
  expect_access_inventory(
      "WITH inv_source AS (SELECT id FROM inv_source) "
      "SELECT COUNT(*) FROM inv_source;",
      {{"inv_source", {"id"}}});
}

TEST(RonSQLParseOnlyInventory, LaterCteNameStillNamesABaseTable) {
  expect_access_inventory(
      "WITH inv_first AS (SELECT id FROM inv_later), "
      "inv_later AS (SELECT id FROM inv_source) "
      "SELECT COUNT(*) FROM inv_first;",
      {{"inv_later", {"id"}}, {"inv_source", {"id"}}});
}

TEST(RonSQLParseOnlyInventory, ScalarCteWithoutFromKeepsBodyInventory) {
  expect_access_inventory(
      "WITH inv_total AS (SELECT SUM(amount) AS total FROM inv_orders) "
      "SELECT MAX(inv_total.total);",
      {{"inv_orders", {"amount"}}});
}

TEST(RonSQLParseOnlyInventory, InAndHavingScalarSubqueriesAreIncluded) {
  expect_access_inventory(
      "SELECT SUM(o.amount) FROM inv_orders AS o "
      "WHERE o.customer_id IN "
      "(SELECT c.id FROM inv_customers AS c WHERE c.region = 1) "
      "HAVING SUM(o.tax) > "
      "(SELECT MAX(a.limit_value) FROM inv_audit AS a);",
      {{"inv_orders", {"amount", "customer_id", "tax"}},
       {"inv_customers", {"id", "region"}},
       {"inv_audit", {"limit_value"}}});
}

TEST(RonSQLParseOnlyInventory, QuotedNamesAndCurrentDatabaseAreAccepted) {
  expect_access_inventory(
      "SELECT SUM(`a``lias`.`va``lue`) "
      "FROM test.`ta``ble` AS `a``lias`;",
      {{"ta`ble", {"va`lue"}}});
}

TEST(RonSQLParseOnlyInventory, UnknownQualifierFallsBackToScope) {
  // Name resolution may reject this later; inventory must not omit amount.
  expect_access_inventory(
      "SELECT SUM(missing.amount) FROM inv_orders AS o;",
      {{"inv_orders", {"amount"}}});
}

TEST(RonSQLParseOnlyRejection, CrossDatabaseMainTable) {
  expect_parse_only_rejection(
      "SELECT COUNT(*) FROM other.inv_orders;",
      "Cross-database table references are not supported.");
}

TEST(RonSQLParseOnlyRejection, CrossDatabaseCteBody) {
  expect_parse_only_rejection(
      "WITH inv_totals AS (SELECT SUM(amount) AS total FROM other.inv_orders) "
      "SELECT SUM(total) FROM inv_totals;",
      "Cross-database table references are not supported.");
}

TEST(RonSQLParseOnlyRejection, CrossDatabaseSubquery) {
  expect_parse_only_rejection(
      "SELECT COUNT(*) FROM inv_orders AS o WHERE EXISTS "
      "(SELECT a.id FROM other.inv_audit AS a);",
      "Cross-database table references are not supported.");
}

TEST(RonSQLParseOnlyRejection, DatabaseQualifiedCteReference) {
  expect_parse_only_rejection(
      "WITH inv_totals AS (SELECT SUM(amount) AS total FROM inv_orders) "
      "SELECT COUNT(*) FROM test.inv_totals;",
      "is qualified with a database, so it names the stored table");
}

TEST(RonSQLParseOnlyRejection, CteReferenceInScalarSubquery) {
  expect_parse_only_rejection(
      "WITH inv_totals AS (SELECT SUM(amount) AS total FROM inv_orders) "
      "SELECT COUNT(*) FROM inv_customers "
      "WHERE id > (SELECT MAX(total) FROM inv_totals);",
      "A subquery cannot read the CTE inv_totals.");
}

TEST(RonSQLParseOnlyRejection, CteReferenceInNestedSubquery) {
  expect_parse_only_rejection(
      "WITH inv_totals AS (SELECT SUM(amount) AS total FROM inv_orders) "
      "SELECT COUNT(*) FROM inv_customers WHERE EXISTS "
      "(SELECT a.id FROM inv_audit AS a "
      "WHERE a.id IN (SELECT total FROM inv_totals));",
      "A subquery cannot read the CTE inv_totals.");
}

TEST(RonSQLParseOnlyRejection, JoinOnMustNameJoinedTableOnLeft) {
  expect_parse_only_rejection(
      "SELECT SUM(o.amount) FROM inv_customers AS c "
      "JOIN inv_orders AS o ON c.id = o.customer_id;",
      "does not have a column of the joined table o on the left.");
}

}  // namespace
