# This file is part of the RonDB REST API Server
# Copyright (c) 2023 Hopsworks AB
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

SET(DROGON_VERSION 1.9.7) # See ../README.txt for how to upgrade

set(TRANTOR_USE_TLS "openssl" CACHE STRING "Select TLS library for Trantor" FORCE)

if(OPENSSL_CUSTOM_LIBRARY)
    # Point OPENSSL_ROOT_DIR to custom OpenSSL path so Drogon's/trantor's
    # find_package(OpenSSL) picks them up.
    set(OPENSSL_ROOT_DIR "${OPENSSL_CUSTOM_LIBRARY}" CACHE PATH "Custom OpenSSL root" FORCE)
endif()

# We don't necessarily need these, but we want to catch any compiler errors after patches.
set(BUILD_EXAMPLES ON CACHE BOOL "Build Drogon examples" FORCE)
set(BUILD_TESTING ON CACHE BOOL "Enable Drogon tests" FORCE)

# Define Jsoncpp_lib target
set(jsoncpp_SOURCE_DIR "${SIMDJSON_SOURCE_DIR}/dependencies/jsoncppdist")
add_library(Jsoncpp_lib STATIC ${jsoncpp_SOURCE_DIR}/jsoncpp.cpp)
target_include_directories(Jsoncpp_lib PUBLIC ${jsoncpp_SOURCE_DIR})
target_compile_options(Jsoncpp_lib PRIVATE -Wno-implicit-fallthrough)

# Add drogon subdirectory
add_subdirectory(drogon-${DROGON_VERSION})

# Add include directories (necessary since drogon isn't built standalone)
target_include_directories(unittest PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/drogon-${DROGON_VERSION}/lib")

# Suppress warnings. This is necessary since RonDB has much stricter compiler
# flags than drogon.
#
# If a build fails due to a warning (raised to an error) in a drogon/trantor
# source file, add the corresponding -Wno-<warning> to the PRIVATE compile
# options below. If it still doesn't work, move it to the corresponding PUBLIC
# compile options.
#
target_compile_options(drogon PRIVATE
    -Wno-parentheses
    -Wno-range-loop-construct
    -Wno-type-limits
    -Wno-undef
    -Wno-unused-but-set-variable
    -Wno-unused-parameter
)
target_compile_options(drogon PUBLIC
    -Wno-deprecated-declarations
    -Wno-missing-field-initializers
    -Wno-missing-format-attribute
    -Wno-overloaded-virtual
    -Wno-sign-compare
    -Wno-unused-function
    -Wno-unused-variable
)

IF(NOT APPLE)
  target_compile_options(trantor PRIVATE
      -Wno-logical-op
  )
ENDIF()

IF(APPLE)
target_compile_options(trantor PUBLIC
    -Wno-cast-qual
    -Wno-extra-semi
    -Wno-ignored-qualifiers
    -Wno-suggest-override
    -Wno-unused-parameter
    -Wno-documentation
    -Wno-deprecated-copy-with-user-provided-dtor
    -Wno-inconsistent-missing-destructor-override
    -Wno-deprecated-copy-with-dtor
    -Wno-mismatched-tags
    -Wno-header-hygiene
    -Wno-shadow-field
)
ELSE()
target_compile_options(trantor PUBLIC
    -Wno-cast-qual
    -Wno-extra-semi
    -Wno-ignored-qualifiers
    -Wno-suggest-override
    -Wno-unused-parameter
    -Wno-documentation
)
ENDIF()
