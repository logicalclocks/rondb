cmake_minimum_required(VERSION 3.10)

project(rdrs2_tests VERSION 1.0)

enable_testing()

include_directories(${GMOCK_INCLUDE_DIRS})
include_directories("../src")

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(
  ${PROJECT_SOURCE_DIR}
  ${BASE64_INCLUDE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/mgmapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/rondis/include
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/util
  ${CMAKE_SOURCE_DIR}/include)
INCLUDE_DIRECTORIES(${AVROCPP_INCLUDE_DIR})

NDB_ADD_EXECUTABLE(api_key_test api_key_test.cpp
                   STATIC_NDBCLIENT)

NDB_ADD_EXECUTABLE(error_handling_test error_handling_test.cpp
                   STATIC_NDBCLIENT)

NDB_ADD_EXECUTABLE(reconnect_test reconnect_test.cpp
                   STATIC_NDBCLIENT)

NDB_ADD_EXECUTABLE(
  feature_store_test
  feature_store_test.cpp
  STATIC_NDBCLIENT
)

# Probe-listener tests. probe_server_http_test is cluster-free (pure HTTP
# conformance of the dedicated probe port); probe_server_cluster_test needs
# a running cluster like the other tests here.
NDB_ADD_EXECUTABLE(probe_server_http_test probe_server_http_test.cpp
                   STATIC_NDBCLIENT)

NDB_ADD_EXECUTABLE(probe_server_cluster_test probe_server_cluster_test.cpp
                   STATIC_NDBCLIENT)

# RONDB-1144: process startup settings (THP opt-out,
# untouched parse buffers). Cluster-free; the observations are Linux-only.
NDB_ADD_EXECUTABLE(process_setup_test process_setup_test.cpp
                   STATIC_NDBCLIENT)

TARGET_LINK_LIBRARIES(
  process_setup_test
  gtest_main
  ndbgeneral
  rdrs2_lib
  ${GTEST_LIBRARIES}
  pthread
)

# RONDB-1144: the request/response buffer pools (cap, shrink, stats,
# memory-limit detection) and the compile-time buffer minimums checked
# against the real encoders. Both cluster-free.
NDB_ADD_EXECUTABLE(buffer_manager_test buffer_manager_test.cpp
                   STATIC_NDBCLIENT)

NDB_ADD_EXECUTABLE(limits_test limits_test.cpp
                   STATIC_NDBCLIENT)

# RONDB-1144: the buffer chain and the response writer's spill path.
NDB_ADD_EXECUTABLE(buffer_chain_test buffer_chain_test.cpp
                   STATIC_NDBCLIENT)

TARGET_LINK_LIBRARIES(
  buffer_chain_test
  gtest_main
  ndbgeneral
  rdrs2_lib
  ${GTEST_LIBRARIES}
)

TARGET_LINK_LIBRARIES(
  buffer_manager_test
  gtest_main
  ndbgeneral
  rdrs2_lib
  ${GTEST_LIBRARIES}
)

TARGET_LINK_LIBRARIES(
  limits_test
  gtest_main
  ndbgeneral
  rdrs2_lib
  ${GTEST_LIBRARIES}
)

TARGET_LINK_LIBRARIES(
	api_key_test
	gtest_main
        ndbgeneral
	rdrs2_lib
	${GTEST_LIBRARIES}
)

TARGET_LINK_LIBRARIES(
	error_handling_test
	gtest_main
	rdrs2_lib
	${GTEST_LIBRARIES}
)

TARGET_LINK_LIBRARIES(
	reconnect_test
	gtest_main
	ndbgeneral
	rdrs2_lib
	${GTEST_LIBRARIES}
)

TARGET_LINK_LIBRARIES(
  feature_store_test
  gtest_main
  rdrs2_lib
  ${GTEST_LIBRARIES}
  pthread
)

# Both probe tests define their own main() (they need setup before gtest
# runs), so they link gtest only, not gtest_main.
TARGET_LINK_LIBRARIES(
  probe_server_http_test
  ndbgeneral
  rdrs2_lib
  ${GTEST_LIBRARIES}
)

TARGET_LINK_LIBRARIES(
  probe_server_cluster_test
  ndbgeneral
  rdrs2_lib
  ${GTEST_LIBRARIES}
)

# The C++ tests do not seed the cluster and embed no SQL fixtures. The test
# databases are created by the Go test suite from
# test_go/resources/testdbs (the single source of truth) - run any Go
# integration test package before running these binaries.

add_test(NAME process_setup_test COMMAND process_setup_test)
add_test(NAME buffer_manager_test COMMAND buffer_manager_test)
add_test(NAME limits_test COMMAND limits_test)
add_test(NAME buffer_chain_test COMMAND buffer_chain_test)
add_test(NAME api_key_test COMMAND api_key_test)
add_test(NAME feature_store_test COMMAND feature_store_test)
add_test(NAME error_handling_test COMMAND error_handling_test)
add_test(NAME reconnect_test COMMAND reconnect_test)
add_test(NAME probe_server_http_test COMMAND probe_server_http_test)
add_test(NAME probe_server_cluster_test COMMAND probe_server_cluster_test)
