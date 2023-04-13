#
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

project (rdrs)

if(WITH_RDRS)
  find_program(GO_EXECUTABLE NAMES go)
  if (NOT GO_EXECUTABLE)
    message(FATAL_ERROR "Install Golang")
  endif()

  add_custom_target(rdrs ALL 
                     COMMAND ${CMAKE_MAKE_PROGRAM} RDRS_LIB_DIR=${RDRS_CLIENT_LIB_DIR}  OUTPUT_DIR=${RDRS_OUTPUT_DIR}
                     WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                     COMMENT "Building RonDB REST API Server"
                     DEPENDS rdrclient)
  
  INSTALL(FILES ${RDRS_OUTPUT_DIR}/rdrs 
       DESTINATION ${INSTALL_BINDIR} COMPONENT Development
       PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
endif()
