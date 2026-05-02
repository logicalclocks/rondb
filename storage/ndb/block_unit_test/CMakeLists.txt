# Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation. The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have either included with
# the program or referenced in the documentation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/kernel
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/kernel/signaldata
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/util
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/mgmapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/test/include
)

SET(BLOCK_UNIT_TEST_TARGETS
  testJoinAgg
  benchJoinAgg
  testCaseAgg
  bench_q12_tpch
  testJoinAggSpj
  bench_q12_dbtc
  load_tpch
  bench_q9_dbtc
)

NDB_ADD_EXECUTABLE(testJoinAgg testJoinAgg.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testCteLookup testCteLookup.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testCtePhase6 testCtePhase6.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testCteDbtc testCteDbtc.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(benchJoinAgg benchJoinAgg.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testCaseAgg testCaseAgg.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q12_tpch bench_q12_tpch.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testJoinAggSpj testJoinAggSpj.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testOuterJoinAgg testOuterJoinAgg.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q12_dbtc bench_q12_dbtc.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(load_tpch load_tpch.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
TARGET_LINK_LIBRARIES(load_tpch ndbgeneral)
NDB_ADD_EXECUTABLE(bench_q9_dbtc bench_q9_dbtc.cpp NDBTEST NDBCLIENT MYSQLCLIENT)

# testJoinAggNdbApi needs NdbQueryBuilder.hpp from src/ndbapi
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi)
NDB_ADD_EXECUTABLE(testJoinAggNdbApi testJoinAggNdbApi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testCteNdbApi testCteNdbApi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testCteNdbApiFilter testCteNdbApiFilter.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testCteNdbApiOuterJoin testCteNdbApiOuterJoin.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testJoinAggIdempotency testJoinAggIdempotency.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testInterpreterTypedRegs testInterpreterTypedRegs.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testOuterJoinAggNdbApi testOuterJoinAggNdbApi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testMultiOuterJoinAggNdbApi testMultiOuterJoinAggNdbApi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testJoinAggScanScan testJoinAggScanScan.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testStarJoinAgg testStarJoinAgg.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testStarJoinAggSpj testStarJoinAggSpj.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(testStarJoinAggNdbApi testStarJoinAggNdbApi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q9_ndbapi bench_q9_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q4_ndbapi bench_q4_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_minmax_ndbapi bench_minmax_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q3_ndbapi bench_q3_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_nogroup_ndbapi bench_nogroup_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q5_ndbapi bench_q5_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q2_ndbapi bench_q2_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q10_ndbapi bench_q10_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q11_ndbapi bench_q11_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_orderscan_ndbapi bench_orderscan_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_datescan_ndbapi bench_datescan_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)

FOREACH(target ${BLOCK_UNIT_TEST_TARGETS})
  SET_TARGET_PROPERTIES(${target} PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
ENDFOREACH()
