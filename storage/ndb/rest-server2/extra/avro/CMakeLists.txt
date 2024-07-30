# This file is part of the RonDB REST API Server
# Copyright (c) 2024 Hopsworks AB
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

if(NOT WITH_RDRS)
  MESSAGE(STATUS "Skip building Avro for REST Server")
  RETURN()
endif()

SET(AVRO_VERSION "1.11.3")
set(RDRS_AVRO_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/avro-${AVRO_VERSION})
set(RDRS_AVRO_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/avro-release-1.11.3/lang/c++)
set(RDRS_AVRO_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/avro-${AVRO_VERSION}/install_dir)

FILE(WRITE "${RDRS_AVRO_BINARY_DIR}/builder.sh"  "
  set -e
  if ! [ -f built ]; then
    cmake \
      -S${RDRS_AVRO_SOURCE_DIR} \
      -B${RDRS_AVRO_BINARY_DIR} \
      -DCMAKE_INSTALL_PREFIX=${RDRS_AVRO_INSTALL_DIR} \
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    make -s -j $(nproc)
    make install
    touch built
  else
    echo Skipping Avro C++ as it is already built. Delete ${RDRS_AVRO_BINARY_DIR}/built to rebuilt it
  fi"
)

EXECUTE_PROCESS(COMMAND bash builder.sh WORKING_DIRECTORY ${RDRS_AVRO_BINARY_DIR} COMMAND_ERROR_IS_FATAL ANY)
ADD_CUSTOM_TARGET(AVRO_IS_BUILD DEPENDS ${RDRS_AVRO_BINARY_DIR}/built)

SET(RDRS_AVRO_CMAKE_MODULES_DIR ${RDRS_AVRO_INSTALL_DIR}/lib/cmake CACHE INTERNAL "Avro cmake module dir" FORCE)
SET(RDRS_AVRO_CMAKE_MODULES_DIR_64 ${RDRS_AVRO_INSTALL_DIR}/lib64/cmake CACHE INTERNAL "Avro cmake module dir" FORCE)
SET(RDRS_AVRO_INCLUDE_DIR   ${RDRS_AVRO_INSTALL_DIR}/include  CACHE INTERNAL "Avro include dir" FORCE)
