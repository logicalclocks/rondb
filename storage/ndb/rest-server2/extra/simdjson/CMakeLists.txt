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

SET(SIMDJSON_VERSION "3.3.0" CACHE INTERNAL "Simdjson Version" FORCE)
SET(RDRS_SIMDJSON_DIR ${CMAKE_CURRENT_SOURCE_DIR}/simdjson-${SIMDJSON_VERSION}  CACHE INTERNAL "Simdjson dir" FORCE)
SET(RDRS_SIMDJSON_INCLUDE_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/simdjson-${SIMDJSON_VERSION}/include  CACHE INTERNAL "Simdjson include dir" FORCE)
add_subdirectory(simdjson-3.3.0)
