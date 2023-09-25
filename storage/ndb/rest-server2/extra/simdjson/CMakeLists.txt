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
SET(RDRS_SIMDJSON_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/simdjson-${SIMDJSON_VERSION}  CACHE INTERNAL "Simdjson src dir" FORCE)
SET(RDRS_SIMDJSON_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/simdjson-${SIMDJSON_VERSION}  CACHE INTERNAL "Simdjson binary dir" FORCE)
SET(RDRS_SIMDJSON_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/simdjson-${SIMDJSON_VERSION}/install_dir  CACHE INTERNAL "Simdjson install dir" FORCE)
#SET(RDRS_SIMDJSON_INCLUDE_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/simdjson-${SIMDJSON_VERSION}/include  CACHE INTERNAL "Simdjson include dir" FORCE)

if(WITH_RDRS)
  FILE(WRITE "${RDRS_SIMDJSON_BINARY_DIR}/builder.sh"  "
    if ! [ -f built ]; then
      cmake \
        -S${RDRS_SIMDJSON_SOURCE_DIR} \
        -B${RDRS_SIMDJSON_BINARY_DIR} \
        -DCMAKE_INSTALL_PREFIX=${RDRS_SIMDJSON_INSTALL_DIR} \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
      make -s -j $(nproc) 
      make install
      touch built
    else
      echo Skipping Simdjson web framework as it is already built. Delete ${RDRS_SIMDJSON_INSTALL_DIR}/built to rebuilt it
    fi"
  )

  EXECUTE_PROCESS(COMMAND bash builder.sh WORKING_DIRECTORY ${RDRS_SIMDJSON_BINARY_DIR})
  CONFIGURE_FILE(${RDRS_SIMDJSON_BINARY_DIR}/built ${RDRS_SIMDJSON_BINARY_DIR}/built COPYONLY)
  ADD_CUSTOM_TARGET(SIMDJSON_IS_BUILD DEPENDS ${RDRS_SIMDJSON_BINARY_DIR}/built)

  SET(RDRS_SIMDJSON_CMAKE_MODULES_DIR ${RDRS_SIMDJSON_INSTALL_DIR}/lib/cmake CACHE INTERNAL "Simdjson cmake module dir" FORCE)
  SET(RDRS_SIMDJSON_CMAKE_MODULES_DIR_64 ${RDRS_SIMDJSON_INSTALL_DIR}/lib64/cmake CACHE INTERNAL "Simdjson cmake module dir" FORCE)
  SET(RDRS_SIMDJSON_INCLUDE_DIR   ${RDRS_SIMDJSON_INSTALL_DIR}/include  CACHE INTERNAL "(Simdjson include dir" FORCE)

endif()
