# Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
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
  # Since we generate header files RonSQLParser.y.hpp and RonSQLLexer.l.hpp into
  # the binary dir, we need to include it in the include path
  ${CMAKE_CURRENT_BINARY_DIR}
  )

# Generate parser
FIND_PACKAGE(BISON REQUIRED)
ADD_CUSTOM_COMMAND(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/RonSQLParser.y.cpp ${CMAKE_CURRENT_BINARY_DIR}/RonSQLParser.y.hpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/RonSQLParser.y ${CMAKE_CURRENT_SOURCE_DIR}/build_parser.sh
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build_parser.sh ${BISON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/RonSQLParser.y ${CMAKE_CURRENT_BINARY_DIR}/RonSQLParser.y.hpp ${CMAKE_CURRENT_BINARY_DIR}/RonSQLParser.y.cpp
  )

# Generate lexer
FIND_PACKAGE(FLEX REQUIRED)
ADD_CUSTOM_COMMAND(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/RonSQLLexer.l.cpp ${CMAKE_CURRENT_BINARY_DIR}/RonSQLLexer.l.hpp
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/RonSQLLexer.l ${CMAKE_CURRENT_SOURCE_DIR}/build_lexer.sh
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build_lexer.sh ${FLEX_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/RonSQLLexer.l ${CMAKE_CURRENT_BINARY_DIR}/RonSQLLexer.l.hpp ${CMAKE_CURRENT_BINARY_DIR}/RonSQLLexer.l.cpp
  )

ADD_CONVENIENCE_LIBRARY(ronsql
  AggregationAPICompiler.cpp
  ArenaAllocator.cpp
  LexString.cpp
  ResultPrinter.cpp
  RonSQLPreparer.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/RonSQLParser.y.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/RonSQLLexer.l.cpp
  )

# todo check that ronsql_cli is included in the tarball
NDB_ADD_EXECUTABLE(ronsql_cli ronsql_cli.cpp)
TARGET_LINK_LIBRARIES(ronsql_cli ronsql)
TARGET_LINK_LIBRARIES(ronsql_cli ndbclient_so)
TARGET_LINK_LIBRARIES(ronsql_cli mytime) # Needed for DATE_SUB etc.

# todo check that KeywordsUnitTest is not included in tarball
NDB_ADD_TEST(KeywordsUnitTest KeywordsUnitTest.cpp LIBS ronsql ndbclient_so)
