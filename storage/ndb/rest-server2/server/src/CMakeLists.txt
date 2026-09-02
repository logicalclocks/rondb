# Copyright (c) 2023, 2026, Hopsworks and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have either included with
# the program or referenced in the documentation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

file(GLOB_RECURSE SRC
        api_key.cpp
        base_ctrl.cpp
        batch_feature_store_ctrl.cpp
        batch_pk_delete_ctrl.cpp
        batch_pk_read_ctrl.cpp
        batch_pk_write_ctrl.cpp
        buffer_manager.cpp
        config_structs.cpp
        connection.cpp
        encoding.cpp
        encoding_helper.cpp
        feature_store_ctrl.cpp
        feature_store_data_structs.cpp
        feature_util.cpp
        feature_store/feature_store.cpp
        fs_cache.cpp
        health_ctrl.cpp
        json_parser.cpp
        json_printer.cpp
        logger.cpp
        main.cc
        metadata.cpp
        metrics.cpp
        mystring.cpp
        ndb_api_helper.cpp
        operations_feature_store.cpp
        schema_cache.cpp
        ping_ctrl.cpp
        pk_data_structs.cpp
        pk_read_ctrl.cpp
        pk_read_ctrl.cpp
        prometheus_ctrl.cpp
        rdrs_dal.cpp
        rdrs_hopsworks_dal.cpp
        rdrs_rondb_connection.cpp
        rdrs_rondb_connection_pool.cpp
        retry_handler.cpp
        ronsql_ctrl.cpp
        tls_util.cpp
        db_operations/pk/common.cpp
        db_operations/pk/pk_base_operation.cpp
        db_operations/pk/pk_batch_base_operation.cpp
        db_operations/pk/pkd_operation.cpp
        db_operations/pk/pkr_operation.cpp
        db_operations/pk/pkr_request.cpp
        db_operations/pk/pkw_operation.cpp
        db_operations/pk/pkr_response.cpp
        db_operations/ronsql/ronsql_operation.cpp
        ttl_purge.cpp
        ttl_purge_ctrl.cpp
        scan_read_ctrl.cpp
        scan_metrics.cpp
        slow_scan_ctrl.cpp
)
NDB_ADD_EXECUTABLE(${PROJECT_NAME} ${SRC} STATIC_NDBCLIENT)

# Create symlink bin/rdrs -> bin/rdrs2
ADD_CUSTOM_COMMAND(TARGET ${PROJECT_NAME} POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E cmake_symlink_executable
          $<TARGET_FILE:${PROJECT_NAME}>
          $<TARGET_FILE_DIR:${PROJECT_NAME}>/rdrs)

find_library(OSSP_UUID_LIBRARY NAMES ossp-uuid
  PATHS /lib64 /usr/lib64
)

find_library(UUID_LIBRARY NAMES uuid
  PATHS /lib64 /usr/lib64
)

find_package(Threads REQUIRED)

#prometheus-cpp
find_package(prometheus-cpp CONFIG QUIET)
if (NOT prometheus-cpp_FOUND)
  if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
    message(FATAL_ERROR "Error: prometheus-cpp was not found. Please install prometheus-cpp. \n\t\t brew install prometheus-cpp \n\t\t or install from src https://github.com/jupp0r/prometheus-cpp")
  else()
    message(FATAL_ERROR "Error: prometheus-cpp was not found. Please install prometheus-cpp. \n\t\t brew install prometheus-cpp https://github.com/jupp0r/prometheus-cpp")
  endif()
endif()

if(NOT DEFINED prometheus-cpp_VERSION)
  message(FATAL_ERROR "prometheus-cpp_VERSION is not defined")
endif()

TARGET_INCLUDE_DIRECTORIES(${PROJECT_NAME} PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}
  ${BASE64_INCLUDE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/extra/rapidjson/include
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/mgmapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/rondis/include
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/ronsql
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/util
  ${CMAKE_SOURCE_DIR}/include
  )

TARGET_LINK_LIBRARIES(${PROJECT_NAME}
                      base64
                      drogon
                      trantor
                      simdjson
                      prometheus-cpp::push
                      ndbgeneral
                      rdrs_string
                      mysqlclient
                      ronsql
                      rondis
                      libavro)

# Install rdrs2 binary
INSTALL(TARGETS ${PROJECT_NAME}
    DESTINATION ${INSTALL_BINDIR} COMPONENT Server)

# Install rdrs symlink
INSTALL(FILES "$<TARGET_FILE_DIR:${PROJECT_NAME}>/rdrs"
    DESTINATION ${INSTALL_BINDIR} COMPONENT Server)

# The test library must not contain main(); linking it next to gtest_main
# would otherwise give a duplicate _main whenever an archive member that
# main.cc.o satisfies gets pulled in.
set(SRC_LIB ${SRC})
list(REMOVE_ITEM SRC_LIB ${CMAKE_CURRENT_SOURCE_DIR}/main.cc)
add_library(rdrs2_lib STATIC ${SRC_LIB})

target_include_directories(rdrs2_lib PUBLIC
	${CMAKE_CURRENT_SOURCE_DIR}
	${PROJECT_SOURCE_DIR}
  ${BASE64_INCLUDE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/extra/rapidjson/include
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/mgmapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/rondis/include
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/ronsql
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/util
  ${CMAKE_SOURCE_DIR}/include
  )

# todo-ronsql is ronsql needed for rdrs2_lib? It's used in ../test/api_key_test
TARGET_LINK_LIBRARIES(rdrs2_lib
                      base64
                      drogon
                      trantor
                      simdjson
                      prometheus-cpp::push
                      ndbclient_so
                      ndbgeneral
                      rdrs_string
                      mysqlclient
                      ronsql
                      libavro)

# Install (bundle) prometheus-cpp libraries. Linux only:
# there prometheus-cpp is built from source without a build type, so the
# imported targets expose IMPORTED_LOCATION_NOCONFIG, and rdrs2 resolves the
# bundled copies via RPATH/SONAME from the install's lib directory.
# On macOS prometheus-cpp comes from Homebrew (IMPORTED_LOCATION_RELEASE, so
# the lookup below returns NOTFOUND) and rdrs2 references the dylibs by
# absolute path, so bundled copies would never be loaded anyway.
IF(NOT APPLE)
  get_target_property(lib_core_path prometheus-cpp::core IMPORTED_LOCATION_NOCONFIG)
  get_target_property(lib_push_path prometheus-cpp::push IMPORTED_LOCATION_NOCONFIG)
  INSTALL(FILES ${lib_core_path} DESTINATION lib
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                    GROUP_READ GROUP_EXECUTE
                    WORLD_READ WORLD_EXECUTE
  )
  INSTALL(FILES ${lib_push_path} DESTINATION lib
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                    GROUP_READ GROUP_EXECUTE
                    WORLD_READ WORLD_EXECUTE
  )
  EXECUTE_PROCESS(
        COMMAND dirname "${lib_core_path}" OUTPUT_VARIABLE PROMETHEUS_CPP_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE)
  INSTALL(FILES "${PROMETHEUS_CPP_DIR}/libprometheus-cpp-core.so.1.3" DESTINATION lib)
  INSTALL(FILES "${PROMETHEUS_CPP_DIR}/libprometheus-cpp-push.so.1.3" DESTINATION lib)
ENDIF()

# Install uuid library. Linux only: on macOS find_library picks up a static
# libuuid.a (nothing to bundle) and the hardcoded .so names do not exist.
if(UUID_LIBRARY AND NOT APPLE)
    message(STATUS "Found uuid_library: ${UUID_LIBRARY}")
    EXECUTE_PROCESS(
      COMMAND readlink -f "${UUID_LIBRARY}" OUTPUT_VARIABLE UUID_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(
      COMMAND dirname "${UUID_LIBRARY}" OUTPUT_VARIABLE UUID_VERSION_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    INSTALL(FILES ${UUID_VERSION} DESTINATION lib)
    INSTALL(FILES "${UUID_VERSION_DIR}/libuuid.so.1" DESTINATION lib
      PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                  GROUP_READ GROUP_EXECUTE
                  WORLD_READ WORLD_EXECUTE
    )
endif()

# Install ossp-uuid library. Linux only, see uuid comment above.
if(OSSP_UUID_LIBRARY AND NOT APPLE)
    message(STATUS "Found ossp_uuid_library: ${OSSP_UUID_LIBRARY}")
    EXECUTE_PROCESS(
      COMMAND readlink -f "${OSSP_UUID_LIBRARY}" OUTPUT_VARIABLE OSSP_UUID_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(
      COMMAND dirname "${OSSP_UUID_LIBRARY}" OUTPUT_VARIABLE OSSP_UUID_VERSION_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    INSTALL(FILES ${OSSP_UUID_VERSION} DESTINATION lib)
    INSTALL(FILES "${OSSP_UUID_VERSION_DIR}/libossp-uuid.so.16" DESTINATION lib
      PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                  GROUP_READ GROUP_EXECUTE
                  WORLD_READ WORLD_EXECUTE
    )
endif()

