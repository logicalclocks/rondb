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

if(NOT WITH_RDRS)
  MESSAGE(STATUS "Skip building Drogon for REST Server") 
  RETURN()
endif()

SET(DROGON_VERSION 1.9.7)
set(RDRS_DROGON_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/drogon-${DROGON_VERSION})
set(RDRS_DROGON_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/drogon-${DROGON_VERSION})
set(RDRS_DROGON_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/drogon-${DROGON_VERSION}/install_dir)

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

MESSAGE(STATUS "RDRS DROGON RDRS_OPENSSL_LIBRARY ${RDRS_OPENSSL_LIBRARY}")
MESSAGE(STATUS "RDRS DROGON RDRS_OPENSSL_INCLUDE_DIR ${OPENSSL_INCLUDE_DIR}")
MESSAGE(STATUS "RDRS DROGON RDRS_CRYPTO_LIBRARY  ${RDRS_CRYPTO_LIBRARY}")

FILE(WRITE "${RDRS_DROGON_BINARY_DIR}/builder.sh"  "
  set -e
  if ! [ -f built ]; then
    cmake \
      -S${RDRS_DROGON_SOURCE_DIR} \
      -B${RDRS_DROGON_BINARY_DIR} \
      -DCMAKE_INSTALL_PREFIX=${RDRS_DROGON_INSTALL_DIR} \
      -DRDRS_OPENSSL_LIBRARY=${RDRS_OPENSSL_LIBRARY} \
      -DRDRS_CRYPTO_LIBRARY=${RDRS_CRYPTO_LIBRARY} \
      -DRDRS_OPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR} \
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    make -s -j $(nproc) 
    make install
    touch built
  else
      echo Skipping Drogon web framework as it is already built. Delete ${RDRS_DROGON_BINARY_DIR}/built to rebuilt it
  fi"
)

EXECUTE_PROCESS(COMMAND bash builder.sh WORKING_DIRECTORY ${RDRS_DROGON_BINARY_DIR} COMMAND_ERROR_IS_FATAL ANY)
ADD_CUSTOM_TARGET(DROGON_IS_BUILD DEPENDS ${RDRS_DROGON_BINARY_DIR}/built)

SET(RDRS_DROGON_CMAKE_MODULES_DIR ${RDRS_DROGON_INSTALL_DIR}/lib/cmake CACHE INTERNAL "Drogon cmake module dir" FORCE)
SET(RDRS_DROGON_CMAKE_MODULES_DIR_64 ${RDRS_DROGON_INSTALL_DIR}/lib64/cmake CACHE INTERNAL "Drogon cmake module dir" FORCE)
SET(RDRS_DROGON_INCLUDE_DIR   ${RDRS_DROGON_INSTALL_DIR}/include  CACHE INTERNAL "(Drogon include dir" FORCE)
SET(RDRS_DROGON_LIB_DIR   ${RDRS_DROGON_INSTALL_DIR}/lib  CACHE INTERNAL "(Drogon lib dir" FORCE)

