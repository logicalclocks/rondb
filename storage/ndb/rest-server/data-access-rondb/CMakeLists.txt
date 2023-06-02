
# Copyright (C) 2022 Hopsworks AB 
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

project (rdrclient)

if(WITH_RDRS) 

  INCLUDE_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}
    ${BASE64_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi
    ${CMAKE_SOURCE_DIR}/storage/ndb/src/mgmapi
    ${CMAKE_SOURCE_DIR}/storage/ndb/include/util
    ${CMAKE_SOURCE_DIR}/include)
  INCLUDE_DIRECTORIES(SYSTEM ${BOOST_PATCHES_DIR} ${BOOST_INCLUDE_DIR})
  
  file(GLOB_RECURSE RDRS_LIB_SOURCE ${PROJECT_SOURCE_DIR}/src/*)
  
  ADD_LIBRARY(rdrclient SHARED ${RDRS_LIB_SOURCE})
  
  TARGET_LINK_LIBRARIES(rdrclient ndbclient_so rdrs_string pthread base64)

  SET_TARGET_PROPERTIES(rdrclient PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/library_output_directory)

  INSTALL(TARGETS rdrclient
      DESTINATION ${INSTALL_LIBDIR} COMPONENT Development)
endif()
