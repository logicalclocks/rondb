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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_TEST_RESOURCES_SEED_CHECK_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_TEST_RESOURCES_SEED_CHECK_HPP_

#include "../../src/config_structs.hpp"

#include <mysql.h>
#include <cstdlib>
#include <iostream>
#include <string>

/*
 * Fail fast when the test databases have not been seeded. The C++ tests do
 * not create any schema or data themselves - the Go test suite does
 * (test_go, InitialiseTesting / testutils.CreateDatabases), and it creates
 * the empty `sentinel` database as the seeded marker. Without this check a
 * missing seed surfaces as dozens of misleading assertion failures (e.g.
 * "API key found in cache but is invalid").
 *
 * The probe mirrors the Go SentinelDbExists() check. It must go through a
 * MySQL connection: `sentinel` contains no tables, and an empty database
 * has no footprint visible to the NDB API.
 */
inline void RequireSeededTestDatabases() {
  auto &mySQL = globalConfigs.testing.mySQL;
  MYSQL *conn = mysql_init(nullptr);
  // Force TCP: libmysqlclient treats host "localhost" as a unix-socket
  // connection, but the config carries a TCP endpoint (the Go test driver
  // connects with an explicit tcp() DSN for the same reason).
  unsigned int protocol = MYSQL_PROTOCOL_TCP;
  mysql_options(conn, MYSQL_OPT_PROTOCOL, &protocol);
  if (mysql_real_connect(conn,
                         mySQL.servers[0].IP.c_str(),
                         mySQL.user.c_str(),
                         mySQL.password.c_str(),
                         nullptr,
                         mySQL.servers[0].port,
                         nullptr,
                         0) == nullptr) {
    std::cerr << "FATAL: cannot connect to mysqld at "
              << mySQL.servers[0].IP << ":" << mySQL.servers[0].port
              << " to verify the test databases are seeded: "
              << mysql_error(conn) << std::endl
              << "Is the test cluster running? Start it with:" << std::endl
              << "  cd build && ./mysql-test/mtr --suite rdrs2-golang"
              << " --start-and-exit" << std::endl;
    mysql_close(conn);
    std::exit(2);
  }
  const char *query = "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA"
                      " WHERE SCHEMA_NAME = 'sentinel'";
  bool seeded = false;
  if (mysql_query(conn, query) == 0) {
    MYSQL_RES *result = mysql_store_result(conn);
    if (result != nullptr) {
      seeded = mysql_num_rows(result) > 0;
      mysql_free_result(result);
    }
  }
  mysql_close(conn);
  if (!seeded) {
    std::cerr << "FATAL: test databases are not seeded (sentinel DB missing)."
              << " Run a golang test to seed the database" << std::endl
              << "e.g.:" << std::endl
              << "  cd test_go && ./script.sh test"
              << " hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore"
              << std::endl;
    std::exit(2);
  }
}

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_TEST_RESOURCES_SEED_CHECK_HPP_
