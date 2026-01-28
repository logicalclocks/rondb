# Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have either included with
# the program or referenced in the documentation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# rondb-cli - RonDB command-line interface tool
# This is a Go-based tool that provides unified access to Rondis and MySQL

# Find Go compiler
find_program(GO_EXECUTABLE go)

if(GO_EXECUTABLE)
  message(STATUS "Found Go: ${GO_EXECUTABLE}")

  # Get Go version
  execute_process(
    COMMAND ${GO_EXECUTABLE} version
    OUTPUT_VARIABLE GO_VERSION_OUTPUT
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  message(STATUS "Go version: ${GO_VERSION_OUTPUT}")

  # Define the output binary location
  set(RONDB_CLI_BINARY "${CMAKE_CURRENT_BINARY_DIR}/rondb")
  set(RONDB_CLI_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

  # Collect all Go source files for dependency tracking
  file(GLOB_RECURSE RONDB_CLI_GO_SOURCES
    "${RONDB_CLI_SOURCE_DIR}/*.go"
    "${RONDB_CLI_SOURCE_DIR}/go.mod"
    "${RONDB_CLI_SOURCE_DIR}/go.sum"
  )

  # Custom command to build rondb-cli
  add_custom_command(
    OUTPUT ${RONDB_CLI_BINARY}
    COMMAND ${CMAKE_COMMAND} -E env
            GOFLAGS=-mod=readonly
            ${GO_EXECUTABLE} build -o ${RONDB_CLI_BINARY} .
    WORKING_DIRECTORY ${RONDB_CLI_SOURCE_DIR}
    DEPENDS ${RONDB_CLI_GO_SOURCES}
    COMMENT "Building rondb-cli..."
    VERBATIM
  )

  # Custom target that depends on the binary
  add_custom_target(rondb-cli ALL
    DEPENDS ${RONDB_CLI_BINARY}
  )

  # Copy the binary to bin/ directory for use during development/testing
  add_custom_command(TARGET rondb-cli POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${RONDB_CLI_BINARY}
            ${CMAKE_BINARY_DIR}/bin/rondb
    COMMENT "Copying rondb-cli to bin/"
  )

  # Install the binary to the bin directory
  install(PROGRAMS ${RONDB_CLI_BINARY}
    DESTINATION ${INSTALL_BINDIR}
    COMPONENT Server
  )

  message(STATUS "rondb-cli will be built and installed to ${INSTALL_BINDIR}")

else()
  message(STATUS "Go not found - rondb-cli will not be built")
  message(STATUS "To build rondb-cli, install Go from https://go.dev/dl/")
endif()
