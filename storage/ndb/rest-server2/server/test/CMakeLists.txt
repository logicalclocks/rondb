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

NDB_ADD_EXECUTABLE(
  feature_store_test
  feature_store_test.cpp
  STATIC_NDBCLIENT
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
  feature_store_test
  gtest_main
  rdrs2_lib
  ${GTEST_LIBRARIES}
  pthread
)

# The C++ tests do not seed the cluster and embed no SQL fixtures. The test
# databases are created by the Go test suite from
# test_go/resources/testdbs (the single source of truth) - run any Go
# integration test package before running these binaries.

add_test(NAME api_key_test COMMAND api_key_test)
add_test(NAME feature_store_test COMMAND feature_store_test)
add_test(NAME error_handling_test COMMAND error_handling_test)
