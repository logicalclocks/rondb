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
  set(RONDB_CLI_RUNTIME_BINARY "${CMAKE_BINARY_DIR}/runtime_output_directory/rondb")

  set(RONDB_CLI_RONSQL_BENCH_TIMEOUT_SECONDS "120")
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(RONDB_CLI_RONSQL_BENCH_TIMEOUT_SECONDS "300")
  endif()

  # rondb-cli is rebuilt on every build and `go build` decides what is stale.
  #
  # Compilation is cached by Go, so an unchanged build only relinks from the
  # cached package archives (a few seconds for this binary) and produces
  # byte-identical output; that is cheap next to the C++ build, and it is the
  # only arrangement that cannot go stale.  The previous rule — an OUTPUT with
  # DEPENDS on a configure-time file(GLOB ... CONFIGURE_DEPENDS) — let make
  # judge staleness from a snapshot of the source list: a Go file added after
  # configuration was only noticed when cmake_check_build_system happened to
  # re-run the glob (top-level make targets only), and the copy into
  # runtime_output_directory (the binary MTR runs) was a POST_BUILD step that
  # fired only when make thought the target was out of date.  Both produced
  # stale MTR binaries during RONDB-1121 E7 (see phase_e7.md §2).
  #
  # copy_if_different refreshes runtime_output_directory/rondb only when the
  # content changed, so its timestamp is stable across unchanged builds.
  add_custom_target(rondb-cli ALL
    COMMAND ${CMAKE_COMMAND} -E env
            GOFLAGS=-mod=readonly
            ${GO_EXECUTABLE} build
              -ldflags "-X github.com/logicalclocks/rondb/tools/rondb-cli/internal/shell.ronsqlBenchTimeoutSeconds=${RONDB_CLI_RONSQL_BENCH_TIMEOUT_SECONDS}"
              -o ${RONDB_CLI_BINARY} .
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/runtime_output_directory
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${RONDB_CLI_BINARY} ${RONDB_CLI_RUNTIME_BINARY}
    WORKING_DIRECTORY ${RONDB_CLI_SOURCE_DIR}
    COMMENT "Building rondb-cli (go build is incremental; copied to runtime_output_directory/ for MTR)"
    VERBATIM
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
