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
  MESSAGE( "============> Source DIR ${RDRS_DROGON_SOURCE_DIR}" ) 
  MESSAGE( "============> Binary DIR ${RDRS_DROGON_BINARY_DIR}" ) 
  MESSAGE( "============> Install DIR ${RDRS_DROGON_INSTALL_DIR}" ) 

   add_custom_target(drogon ALL 
                      COMMAND ${CMAKE_MAKE_PROGRAM} RDRS_DROGON_SOURCE_DIR=${RDRS_DROGON_SOURCE_DIR}  RDRS_DROGON_INSTALL_DIR=${RDRS_DROGON_INSTALL_DIR} RDRS_DROGON_BINARY_DIR=${RDRS_DROGON_BINARY_DIR}
                      WORKING_DIRECTORY ${RDRS_DROGON_SOURCE_DIR}
                      COMMENT "Building Drogon Web Framework")
    
   # INSTALL(FILES ${RDRS_OUTPUT_DIR}/rdrs 
  #       DESTINATION ${INSTALL_BINDIR} COMPONENT Development
  #       PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
endif()
