project (rdrs2 LANGUAGES CXX C VERSION 1.0.0)

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${RDRS_DROGON_CMAKE_MODULES_DIR})
find_package(Drogon CONFIG REQUIRED)


set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Jsoncpp REQUIRED)
find_package(Threads REQUIRED)
# find_package(OpenSSL REQUIRED)

INCLUDE_DIRECTORIES(${RDRS_DROGON_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${RDRS_SIMDJSON_DIR})
INCLUDE_DIRECTORIES(${RDRS_SIMDJSON_INCLUDE_DIR})
LINK_DIRECTORIES(${RDRS_DROGON_LIB_DIR})


file(GLOB SRC
	*.cc
	*.c
	*.cpp
	*.h
)

# -g for debug, -03 for release
# target_compile_options(${PROJECT_NAME} PUBLIC -O3)
add_executable(${PROJECT_NAME} ${SRC})
target_link_libraries(${PROJECT_NAME}  Jsoncpp_lib Drogon::Drogon Trantor::Trantor  simdjson)
#OpenSSL::SSL OpenSSL::Crypto
