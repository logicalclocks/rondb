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

SET(DROGON_VERSION 1.8.4)
set(RDRS_DROGON_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/drogon-${DROGON_VERSION})
set(RDRS_DROGON_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/drogon-${DROGON_VERSION})
set(RDRS_DROGON_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/drogon-${DROGON_VERSION}/install_dir)


if(WITH_RDRS)

  EXECUTE_PROCESS(COMMAND cmake -S${RDRS_DROGON_SOURCE_DIR} -B${RDRS_DROGON_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${RDRS_DROGON_INSTALL_DIR}
                      WORKING_DIRECTORY ${RDRS_DROGON_SOURCE_DIR})
   FILE(WRITE "${RDRS_DROGON_BINARY_DIR}/builder.sh"  "
   make -s -j $(nproc) 
   make -s install"
   )
  EXECUTE_PROCESS(COMMAND bash builder.sh 
                      WORKING_DIRECTORY ${RDRS_DROGON_BINARY_DIR})


  # add_custom_target(drogon ALL 
                      # COMMAND bash builder.sh 
                      # WORKING_DIRECTORY ${RDRS_DROGON_BINARY_DIR}
                      # COMMENT "Building Drogon Web Framework")

  SET(RDRS_DROGON_CMAKE_MODULES_DIR ${RDRS_DROGON_INSTALL_DIR}/lib/cmake CACHE INTERNAL "Drogon cmake module dir" FORCE)
  SET(RDRS_DROGON_INCLUDE_DIR   ${RDRS_DROGON_INSTALL_DIR}/include  CACHE INTERNAL "(Drogon include dir" FORCE)
  SET(RDRS_DROGON_LIB_DIR   ${RDRS_DROGON_INSTALL_DIR}/lib  CACHE INTERNAL "(Drogon lib dir" FORCE)

  # SET(DROGON_ORM_INCLUDE_DIR   ${RDRS_DROGON_INSTALL_DIR}/orm_lib/inc  CACHE INTERNAL "(Drogon orm include dir" FORCE)
  # SET(DROGON_NOSQL_INCLUDE_DIR ${RDRS_DROGON_SOURCE_DIR}/nosql_lib/redis/inc CACHE INTERNAL "Drogon nosql include dir" FORCE)
  # SET(DROGON_INCLUDE_DIR_BUILD ${RDRS_DROGON_BINARY_DIR}/extra/drogon/${DROGON_RELEASE}/exports CACHE INTERNAL "Drogon build include dir" FORCE)
  # SET(DROGON_LIBRARY drogon CACHE INTERNAL "Bundled drogon library" FORCE)
    
   # INSTALL(FILES ${RDRS_OUTPUT_DIR}/rdrs 
  #       DESTINATION ${INSTALL_BINDIR} COMPONENT Development
  #       PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
endif()
