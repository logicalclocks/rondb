project (rdrs2 LANGUAGES CXX C VERSION 1.0.0)

set(CMAKE_MODULE_PATH ${DROGON_CMAKE_MODULES_DIR})

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Jsoncpp REQUIRED)
find_package(Threads REQUIRED)

INCLUDE_DIRECTORIES(${SIMDJSON_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DROGON_INCLUDE_DIR} ${DROGON_ORM_INCLUDE_DIR} ${DROGON_INCLUDE_DIR_BUILD} ${DROGON_NOSQL_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${TRANTOR_INCLUDE_DIR})
INCLUDE_DIRECTORIES(SYSTEM ${BOOST_PATCHES_DIR} ${BOOST_INCLUDE_DIR})

include(${SIMDJSON_INCLUDE_DIR}/cmake/simdjson-props.cmake)
include(${SIMDJSON_INCLUDE_DIR}/cmake/implementation-flags.cmake)
# include(${SIMDJSON_INCLUDE_DIR}/cmake/exception-flags.cmake)
# include(${SIMDJSON_INCLUDE_DIR}/cmake/handle-deprecations.cmake)
# include(${SIMDJSON_INCLUDE_DIR}/cmake/developer-options.cmake)

file(GLOB SRC
	*.cc
	*.c
	*.cpp
	*.h
)

# -g for debug, -03 for release
# target_compile_options(${PROJECT_NAME} PUBLIC -O3)
add_executable(${PROJECT_NAME} ${SRC})
target_link_libraries(${PROJECT_NAME} PRIVATE Jsoncpp_lib ${DROGON_LIBRARY} ${SIMDJSON_LIBRARY} ${TRANTOR_LIBRARY})
