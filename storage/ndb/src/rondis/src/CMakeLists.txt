# Copyright (c) 2025, 2025, Hopsworks and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

INCLUDE_DIRECTORIES(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/rondis/include
  ${CMAKE_SOURCE_DIR}/storage/ndb/include
  ${CMAKE_SOURCE_DIR}/include
)

ADD_CONVENIENCE_LIBRARY(rondis
  commands.cc
  common.cc
  db_operations.cc
  dispatch_thread.cc
  interpreted_code.cc
  mysql_conn.cc
  mysql_protocol.cc
  pink_conn.cc
  pink_epoll.cc
  pink_thread.cc
  pink_util.cc
  redis_conn.cc
  redis_parser.cc
  rondb.cc
  rondis_thread.cc
  server_socket.cc
  server_thread.cc
  table_definitions.cc
  worker_thread.cc
  LINK_LIBRARIES mysqlclient
)

NDB_ADD_EXECUTABLE(rondisd rondis.cc)
TARGET_LINK_LIBRARIES(rondisd rondis)
TARGET_LINK_LIBRARIES(rondisd ndbclient_so)
