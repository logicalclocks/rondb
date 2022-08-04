
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
  include_directories (${CMAKE_SOURCE_DIR}/include)
  include_directories(${PROJECT_SOURCE_DIR})
  
  INCLUDE_DIRECTORIES(
    ${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi
    ${CMAKE_SOURCE_DIR}/storage/ndb/src/mgmapi
    ${CMAKE_SOURCE_DIR}/storage/ndb/include/util
    ${CMAKE_SOURCE_DIR}/include)
  INCLUDE_DIRECTORIES(SYSTEM ${BOOST_PATCHES_DIR} ${BOOST_INCLUDE_DIR})
  
  link_directories(${RONDB_DIR}/lib)
  
  file(GLOB_RECURSE RDRS_LIB_SOURCE ${PROJECT_SOURCE_DIR}/src/*)
  
  add_library(rdrclient SHARED ${RDRS_LIB_SOURCE})
  
  TARGET_LINK_LIBRARIES(rdrclient ndbclient_so rdrs_string pthread)
  INSTALL(TARGETS rdrclient
      DESTINATION ${INSTALL_LIBDIR} COMPONENT Development)
endif()
