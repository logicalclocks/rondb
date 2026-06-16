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

# jit_codemem.c (Phase 8) is the node-global free-capable code-memory
# manager layered on the arena substrate; it lives in the same static
# library since it is part of the executable-memory substrate.
ADD_LIBRARY(ndb_jit_arena STATIC
  jit_arena.c
  jit_codemem.c
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

# libpthread: macOS for pthread_jit_write_protect_np; all platforms for
# the code-memory manager's per-size-class mutexes + pthread_once.
FIND_PACKAGE(Threads REQUIRED)
TARGET_LINK_LIBRARIES(ndb_jit_arena PUBLIC Threads::Threads)

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
# RONDB-1056 Phase 4: NDB aggregation bytecode bridge.
#
# Translates an NDB Uint32-word aggregation program into our internal
# Program/Op format. Pure C, no NDB-side dependencies — DBTUP code
# includes only ndb_jit_bridge.h.
# ---------------------------------------------------------------------------

ADD_LIBRARY(ndb_jit_bridge STATIC
  ndb_jit_bridge.c
)
SET_TARGET_PROPERTIES(ndb_jit_bridge PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED YES
  C_EXTENSIONS YES
)
TARGET_INCLUDE_DIRECTORIES(ndb_jit_bridge PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
)

# ---------------------------------------------------------------------------
# RONDB-1056 Phase 2: stencil extractor — host tool.
#
# Reads an ELF .o produced by clang from stencils_src.c, walks the
# symbol table for op_* entries, and emits a C header with per-
# stencil byte arrays + Hole tables. Pure C, no third-party deps,
# builds and runs on macOS hosts as well as Linux. Used by the
# regen-stencils target (added once stencils_src.c is restructured
# to consume the generated headers).
# ---------------------------------------------------------------------------

ADD_EXECUTABLE(ndb_jit_extract_stencils
  extract_stencils/extract_stencils.c
)
SET_TARGET_PROPERTIES(ndb_jit_extract_stencils PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED YES
  C_EXTENSIONS YES
)
TARGET_INCLUDE_DIRECTORIES(ndb_jit_extract_stencils PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/extract_stencils
)

# ---------------------------------------------------------------------------
# RONDB-1056 Phase 2: magic-byte collision audit — host tool.
#
# Independent verification that each MAGIC_* declared in hole_kinds.h
# appears exactly the expected number of times in the generated stencil
# headers (1x in its declaring stencil on aarch64, 0x everywhere else).
#
# The extractor and the audit run independent chain-detection logic; if
# the extractor ever silently swallows a chain (e.g., due to a clang
# codegen surprise that confuses the magic decoder), the audit run is
# meant to flag it.
#
# Will be wired into the regen-stencils target in Day 3 AM.
# ---------------------------------------------------------------------------

ADD_EXECUTABLE(ndb_jit_audit_magics
  extract_stencils/audit_magics.c
)
SET_TARGET_PROPERTIES(ndb_jit_audit_magics PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED YES
  C_EXTENSIONS YES
)
TARGET_INCLUDE_DIRECTORIES(ndb_jit_audit_magics PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/extract_stencils
)

# ---------------------------------------------------------------------------
# RONDB-1056 Phase 2: regen-stencils — pinned-clang regeneration target.
#
# The default `cmake --build` does NOT run the extractor; the committed
# stencils_x86_64.h / stencils_arm64.h are consumed directly. This
# target is invoked explicitly when stencil bytes need to be refreshed
# (after editing stencils_src.c or rolling the clang pin):
#
#   cmake -DNDB_JIT_CLANG=/path/to/upstream/clang ..
#   cmake --build . --target regen-stencils
#
# The upstream clang requirement is enforced by extract_stencils/regen.cmake's
# preflight (rejects Apple clang, requires the exact pinned version).
# See jit/extract_stencils/README.md for the full extraction recipe.
#
# Pinned clang version. Updated as a deliberate three-place change
# (this var, plan.md §2 clang pin, jit/extract_stencils/README.md).
# ---------------------------------------------------------------------------

SET(NDB_JIT_CLANG_VERSION "20.1.8"
    CACHE STRING "Required upstream LLVM clang version for stencil regeneration")

# The Phase 1 ndb_jit1_stencils target (a one-shot stencils.o for
# manual objdump) is removed in Phase 2 — the extractor + regen-stencils
# target replaces its purpose.

FIND_PROGRAM(NDB_JIT_CLANG clang
             DOC "Upstream LLVM clang for RONDB-1056 stencil regeneration")

IF(NDB_JIT_CLANG)
  MESSAGE(STATUS "RONDB-1056: regen-stencils will use ${NDB_JIT_CLANG} "
                 "(pinned to ${NDB_JIT_CLANG_VERSION}; preflight runs at target time)")
ELSE()
  MESSAGE(STATUS "RONDB-1056: clang not found on PATH. The regen-stencils "
                 "target will fail; set -DNDB_JIT_CLANG=/path/to/upstream/clang "
                 "(macOS: brew install llvm@20 then "
                 "/opt/homebrew/opt/llvm@20/bin/clang; "
                 "Linux: dnf/apt install clang).")
ENDIF()

ADD_CUSTOM_TARGET(regen-stencils
  COMMAND ${CMAKE_COMMAND}
          -P "${CMAKE_CURRENT_SOURCE_DIR}/extract_stencils/regen.cmake"
          --
          "${NDB_JIT_CLANG}"
          "${NDB_JIT_CLANG_VERSION}"
          "${CMAKE_CURRENT_SOURCE_DIR}/stencils_src.c"
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${CMAKE_CURRENT_BINARY_DIR}/regen_tmp"
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "$<TARGET_FILE:ndb_jit_extract_stencils>"
          "$<TARGET_FILE:ndb_jit_audit_magics>"
  DEPENDS
    ndb_jit_extract_stencils
    ndb_jit_audit_magics
    ${CMAKE_CURRENT_SOURCE_DIR}/stencils_src.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hole_kinds.h
    ${CMAKE_CURRENT_SOURCE_DIR}/jit1.h
    ${CMAKE_CURRENT_SOURCE_DIR}/bytecode1.h
    ${CMAKE_CURRENT_SOURCE_DIR}/jit_arena.h
  COMMENT "[RONDB-1056] regenerating stencil headers (clang ${NDB_JIT_CLANG_VERSION})"
  VERBATIM
)

# ---------------------------------------------------------------------------
# RONDB-1056 Phase 2: extractor-tests — host-tool unit tests.
#
# CLI errors, malformed input, idempotency, audit pass/fail. Runs the
# committed extractor + audit binaries against the committed
# stencils_*.h headers (and a freshly compiled stencils_<arch>.o for
# byte-equivalence). 11 cases; harness lives in
# extract_stencils/tests/run_tests.cmake.
#
# Like regen-stencils, this depends on a working clang on PATH (or
# overridden via NDB_JIT_CLANG). The harness uses the same flags as
# regen.cmake so test failure points at extractor / audit regressions
# rather than at compile-flag drift.
# ---------------------------------------------------------------------------

ADD_CUSTOM_TARGET(extractor-tests
  COMMAND ${CMAKE_COMMAND}
          -P "${CMAKE_CURRENT_SOURCE_DIR}/extract_stencils/tests/run_tests.cmake"
          --
          "$<TARGET_FILE:ndb_jit_extract_stencils>"
          "$<TARGET_FILE:ndb_jit_audit_magics>"
          "${NDB_JIT_CLANG}"
          "${CMAKE_CURRENT_SOURCE_DIR}/stencils_src.c"
          "${CMAKE_CURRENT_SOURCE_DIR}"
          "${CMAKE_CURRENT_BINARY_DIR}/extractor_test_tmp"
          "${CMAKE_CURRENT_SOURCE_DIR}/stencils_x86_64.h"
          "${CMAKE_CURRENT_SOURCE_DIR}/stencils_arm64.h"
  DEPENDS
    ndb_jit_extract_stencils
    ndb_jit_audit_magics
  COMMENT "[RONDB-1056] running extractor + audit unit tests"
  VERBATIM
)
