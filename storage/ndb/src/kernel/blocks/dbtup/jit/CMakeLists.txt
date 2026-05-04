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

# RONDB-1056 Phase 0: JIT arena substrate.
#
# Builds a small static library `ndb_jit_arena` consumed by the
# Phase 0 standalone smoke test (storage/ndb/test/jit_proto/). Has no
# NDB dependencies. Will be linked into ndbblocks in Phase 4 when the
# JIT engine wires into AggInterpreter.
#
# macOS x86_64 is explicitly excluded — the feature is not built on
# that platform per the §1 / §6 / §16 decisions in plan.md.

IF(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
  MESSAGE(STATUS "RONDB-1056: JIT arena excluded on macOS x86_64")
  RETURN()
ENDIF()

ADD_LIBRARY(ndb_jit_arena STATIC
  jit_arena.c
)

# The .inc.c platform bodies are #included from jit_arena.c, not
# compiled standalone — so they are listed only as headers/sources to
# get them into IDE indexes.
SET_TARGET_PROPERTIES(ndb_jit_arena PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED YES
  C_EXTENSIONS YES
)

TARGET_INCLUDE_DIRECTORIES(ndb_jit_arena PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
)

# No external libraries needed — pure libc + libpthread on macOS for
# pthread_jit_write_protect_np.
IF(APPLE)
  FIND_PACKAGE(Threads REQUIRED)
  TARGET_LINK_LIBRARIES(ndb_jit_arena PUBLIC Threads::Threads)
ENDIF()

# ---------------------------------------------------------------------------
# RONDB-1056 Phase 1: copy-and-patch JIT engine.
#
# Pure C, links the arena substrate. On non-x86_64 architectures the
# entry points are stub-only; the test binaries will still link, but
# jit1_compile returns NULL with errno=ENOTSUP so the microbench
# falls back gracefully.
# ---------------------------------------------------------------------------

ADD_LIBRARY(ndb_jit1 STATIC
  jit1.c
)
SET_TARGET_PROPERTIES(ndb_jit1 PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED YES
  C_EXTENSIONS YES
)
TARGET_INCLUDE_DIRECTORIES(ndb_jit1 PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
)
TARGET_LINK_LIBRARIES(ndb_jit1 PUBLIC ndb_jit_arena)

# ---------------------------------------------------------------------------
# RONDB-1056 Phase 1: stencils.o build target.
#
# stencils_src.c is compiled with clang (not the project's CC) using a
# specific set of flags, into stencils.o. The .o is NOT linked into
# anything — it's an artefact the developer runs `objdump -d -r` on,
# then hand-copies bytes + relocations into stencils_x86_64.h per
# phase_1_implementation.md §7. Phase 2 replaces this with an
# automated extractor.
#
# We require clang explicitly (gcc produces incompatible codegen
# because preserve_none is clang-only). If clang is not available the
# target is skipped with a status message and the rest of the build
# carries on; the developer needs clang only when regenerating
# stencils.
# ---------------------------------------------------------------------------

FIND_PROGRAM(NDB_JIT_CLANG clang
             DOC "clang for RONDB-1056 Phase 1 stencil compilation")

IF(NDB_JIT_CLANG)
  MESSAGE(STATUS "RONDB-1056: stencils will be compiled with ${NDB_JIT_CLANG}")

  SET(NDB_JIT_STENCIL_FLAGS
    -O2
    -fno-asynchronous-unwind-tables
    -ffreestanding
    -fno-stack-protector
    -fno-pic
    -std=c11
    -Wall
    -Wextra
    -I${CMAKE_CURRENT_SOURCE_DIR}
  )

  ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/stencils.o
    COMMAND ${NDB_JIT_CLANG} ${NDB_JIT_STENCIL_FLAGS}
            -c ${CMAKE_CURRENT_SOURCE_DIR}/stencils_src.c
            -o ${CMAKE_CURRENT_BINARY_DIR}/stencils.o
    DEPENDS
      ${CMAKE_CURRENT_SOURCE_DIR}/stencils_src.c
      ${CMAKE_CURRENT_SOURCE_DIR}/jit1.h
      ${CMAKE_CURRENT_SOURCE_DIR}/bytecode1.h
      ${CMAKE_CURRENT_SOURCE_DIR}/jit_arena.h
    COMMENT "[RONDB-1056 Phase 1] compiling stencils.o with clang"
    VERBATIM
  )

  ADD_CUSTOM_TARGET(ndb_jit1_stencils ALL
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/stencils.o)
ELSE()
  MESSAGE(STATUS "RONDB-1056: clang not found; Phase 1 stencils.o target skipped. "
                 "Install clang (Linux: dnf/apt install clang; macOS: Apple clang via "
                 "Xcode CLT or `brew install llvm`) to enable the `ndb_jit1_stencils` "
                 "target.")
ENDIF()
