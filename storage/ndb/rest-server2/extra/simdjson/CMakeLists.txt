# This file is part of the RonDB REST API Server
# Copyright (c) 2023,2025 Hopsworks AB
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

SET(SIMDJSON_VERSION "3.12.0") # See ../README.txt for how to upgrade

set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

# Save SIMDJSON_SOURCE_DIR so that drogon can find jsoncpp
SET(SIMDJSON_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/simdjson-${SIMDJSON_VERSION}
    CACHE INTERNAL "")

# Add simdjson subdirectory
add_subdirectory("simdjson-${SIMDJSON_VERSION}")

target_compile_options(simdjson PRIVATE
    -Wno-documentation
)
