/*
 * Copyright (C) 2024 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_TEST_RESOURCES_TEST_CONSTANTS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_TEST_RESOURCES_TEST_CONSTANTS_HPP_

#include <string>

/*
 * Constants describing the seeded test databases. The C++ tests do NOT seed
 * the cluster themselves - the schemas and data are created by the Go test
 * suite (test_go/resources/testdbs, applied by InitialiseTesting /
 * testutils.CreateDatabases). Run any Go integration test package first;
 * these values must match those fixtures.
 */

// Test database names (see test_go/resources/testdbs/fixed/*.sql)
const std::string DB001 = "db001";
const std::string DB002 = "db002";
const std::string FSDB001 = "fsdb001";
const std::string FSDB002 = "fsdb002";

// Clear-text secret of the seeded test api keys; the hashed form is stored in
// hopsworks.api_key by test_go/resources/testdbs/dynamic/hopsworks_api_key.sql
const std::string HopsworksAPIKey_SECRET =
    "ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub";

// Number of additional seeded api keys (ids 2..513, prefixes "%016d");
// must match HopsworksAPIKey_ADDITIONAL_KEYS in test_go/resources/testdbs/embeddings.go
const int HopsworksAPIKey_ADDITIONAL_KEYS = 512;

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_TEST_RESOURCES_TEST_CONSTANTS_HPP_
