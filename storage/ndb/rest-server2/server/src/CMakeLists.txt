if(NOT WITH_RDRS)
  MESSAGE(STATUS "Skip building REST Server") 
  RETURN()
endif()

file(GLOB_RECURSE SRC
	*.cc
	*.c
	*.cpp
	*.h
)
add_executable(${PROJECT_NAME} ${SRC})
add_dependencies(${PROJECT_NAME} DROGON_IS_BUILD SIMDJSON_IS_BUILD)

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_DROGON_CMAKE_MODULES_DIR})
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_DROGON_CMAKE_MODULES_DIR_64})
find_package(Drogon CONFIG REQUIRED)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_SIMDJSON_CMAKE_MODULES_DIR})
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_SIMDJSON_CMAKE_MODULES_DIR_64})
find_package(simdjson REQUIRED)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_AVRO_CMAKE_MODULES_DIR})
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_AVRO_CMAKE_MODULES_DIR_64})

find_path(AVROCPP_INCLUDE_DIR avro/ValidSchema.hh
  PATHS /usr/local/include /usr/include
)

find_library(AVROCPP_LIBRARIES NAMES avrocpp
  PATHS /usr/local/lib /usr/lib
)

if(AVROCPP_INCLUDE_DIR AND AVROCPP_LIBRARIES)
    message(STATUS "Found avrocpp: ${AVROCPP_LIBRARIES}")
else()
  message(STATUS "avrocpp NOT FOUND")
  message("Please refer to the latest Avro C++ Installing instructions: https://avro.apache.org/docs/1.12.0/api/cpp/html/")
  message(FATAL_ERROR "Failed to configure rdrs2. avrocpp NOT FOUND")
endif()

find_package(Jsoncpp REQUIRED)
find_package(Threads REQUIRED)

IF(CRYPTO_CUSTOM_LIBRARY) 
  SET(RDRS_CRYPTO_LIBRARY ${CRYPTO_CUSTOM_LIBRARY})
ELSE()
  SET(RDRS_CRYPTO_LIBRARY ${CRYPTO_LIBRARY})
ENDIF()

IF(OPENSSL_CUSTOM_LIBRARY) 
  SET(RDRS_OPENSSL_LIBRARY ${OPENSSL_CUSTOM_LIBRARY})
ELSE()
  SET(RDRS_OPENSSL_LIBRARY ${OPENSSL_LIBRARY})
ENDIF()

if(OPENSSL_INCLUDE_DIR)
  MESSAGE(STATUS "RDRS Server Using OpenSSL Configured by RonDB") 
  MESSAGE(STATUS "RDRS Server OPENSSL_INCLUDE_DIR ${OPENSSL_INCLUDE_DIR}") 
  MESSAGE(STATUS "RDRS Server RDRS_OPENSSL_LIBRARY ${RDRS_OPENSSL_LIBRARY}") 
  MESSAGE(STATUS "RDRS Server RDRS_CRYPTO_LIBRARY ${RDRS_CRYPTO_LIBRARY}") 
  INCLUDE_DIRECTORIES(${OPENSSL_INCLUDE_DIR})
else()
  message(FATAL_ERROR "Requested OpenSSL TLS provider but OpenSSL was not found")
endif()

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
LINK_DIRECTORIES(${RDRS_DROGON_LIB_DIR})
INCLUDE_DIRECTORIES(${AVROCPP_INCLUDE_DIR})

target_link_libraries(${PROJECT_NAME}
                      base64
                      ${AVROCPP_LIBRARIES}
                      Jsoncpp_lib
                      Drogon::Drogon
                      Trantor::Trantor
                      simdjson::simdjson
                      ${RDRS_OPENSSL_LIBRARY}
                      ${RDRS_CRYPTO_LIBRARY}
                      ndbclient_so
                      ndbgeneral
                      rdrs_string)

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME} 
   DESTINATION ${INSTALL_BINDIR} COMPONENT Development
   PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

add_library(rdrs2_lib STATIC ${SRC})

target_include_directories(rdrs2_lib PUBLIC 
	${CMAKE_CURRENT_SOURCE_DIR} 
	${PROJECT_SOURCE_DIR}
  ${BASE64_INCLUDE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/mgmapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/util
  ${CMAKE_SOURCE_DIR}/include)
target_include_directories(rdrs2_lib PUBLIC ${RDRS_DROGON_INCLUDE_DIR})
#target_include_directories(rdrs2_lib PUBLIC ${RDRS_SIMDJSON_DIR})
target_include_directories(rdrs2_lib PUBLIC ${RDRS_SIMDJSON_INCLUDE_DIR})
target_include_directories(rdrs2_lib PUBLIC ${RDRS_AVRO_INCLUDE_DIR})
target_link_directories(rdrs2_lib PUBLIC ${RDRS_DROGON_LIB_DIR})

target_link_libraries(rdrs2_lib
                      base64
                      ${AVROCPP_LIBRARIES}
                      Jsoncpp_lib
                      Drogon::Drogon
                      Trantor::Trantor
                      simdjson::simdjson
                      ${RDRS_OPENSSL_LIBRARY}
                      ${RDRS_CRYPTO_LIBRARY}
                      ndbclient_so
                      ndbgeneral
                      rdrs_string
                      mysqlclient)
