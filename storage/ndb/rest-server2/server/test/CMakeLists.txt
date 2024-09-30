cmake_minimum_required(VERSION 3.10)

project(rdrs2_tests VERSION 1.0)

enable_testing()

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_DROGON_CMAKE_MODULES_DIR})
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_DROGON_CMAKE_MODULES_DIR_64})
find_package(Drogon CONFIG REQUIRED)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_SIMDJSON_CMAKE_MODULES_DIR})
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_SIMDJSON_CMAKE_MODULES_DIR_64})
find_package(simdjson REQUIRED)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_AVRO_CMAKE_MODULES_DIR})
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_AVRO_CMAKE_MODULES_DIR_64})
find_package(Jsoncpp REQUIRED)

find_path(AVROCPP_INCLUDE_DIR avro/ValidSchema.hh
  PATHS /usr/local/include /usr/include /opt/homebrew/include 
)

find_library(AVROCPP_LIBRARIES NAMES avrocpp
  PATHS /usr/local/lib /usr/lib /opt/homebrew/lib
)

if(AVROCPP_INCLUDE_DIR AND AVROCPP_LIBRARIES)
    message(STATUS "Found avrocpp: ${AVROCPP_LIBRARIES}")
else()
  message(STATUS "avrocpp NOT FOUND")
  message("Please refer to the latest Avro C++ Installing instructions: https://avro.apache.org/docs/1.12.0/api/cpp/html/")
  if(APPLE)
      message("or  brew install avro-cpp")
  endif()
  message(FATAL_ERROR "Failed to configure rdrs2. avrocpp NOT FOUND")
endif()

include_directories(${GMOCK_INCLUDE_DIRS})
include_directories("../src")

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(
  ${PROJECT_SOURCE_DIR}
  ${BASE64_INCLUDE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/mgmapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/util
  ${CMAKE_SOURCE_DIR}/include)
INCLUDE_DIRECTORIES(${RDRS_DROGON_INCLUDE_DIR})
#INCLUDE_DIRECTORIES(${RDRS_SIMDJSON_DIR})
INCLUDE_DIRECTORIES(${RDRS_SIMDJSON_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${AVROCPP_INCLUDE_DIR})
LINK_DIRECTORIES(${RDRS_DROGON_LIB_DIR})

NDB_ADD_EXECUTABLE(api_key_test api_key_test.cpp
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
  feature_store_test
  gtest_main
  rdrs2_lib
  ${GTEST_LIBRARIES}
  pthread
)

set(RESOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/resources")
set(TESTDBS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/resources/testdbs")
set(GENERATED_HEADER "${CMAKE_CURRENT_BINARY_DIR}/embedded_content.hpp")

file(GLOB_RECURSE SQL_FILES RELATIVE "${TESTDBS_DIR}" "${TESTDBS_DIR}/**/*.sql")

set(SQL_CONTENT_ENTRIES "")

foreach(SQL_FILE ${SQL_FILES})
    file(READ "${TESTDBS_DIR}/${SQL_FILE}" CONTENTS)

    string(REPLACE "\"" "\\\"" CONTENTS "${CONTENTS}")
    string(REPLACE "\\" "\\\\" CONTENTS "${CONTENTS}")

    set(SQL_CONTENT_ENTRIES "${SQL_CONTENT_ENTRIES}    {\"${SQL_FILE}\", R\"SQL(${CONTENTS})SQL\"},\n")
endforeach()

configure_file("${RESOURCE_DIR}/embedded_content.hpp.in" "${GENERATED_HEADER}" @ONLY)

include_directories("${CMAKE_CURRENT_BINARY_DIR}")

add_test(NAME api_key_test COMMAND api_key_test)
add_test(NAME feature_store_test COMMAND feature_store_test)
