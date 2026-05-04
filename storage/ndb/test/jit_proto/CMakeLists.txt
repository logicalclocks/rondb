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

# RONDB-1056 Phase 0 standalone test binaries.
#
# These are tiny C programs exercising only the JIT arena substrate.
# No NDB dependency; no signal-handling; no DBTUP wiring. They live
# in their own CMake target so the platform-specific quirks that come
# with mmap/MAP_JIT/SELinux can be debugged in isolation before any
# bytecode or stencil work begins.

IF(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
  MESSAGE(STATUS "RONDB-1056: jit_proto excluded on macOS x86_64")
  RETURN()
ENDIF()

ADD_EXECUTABLE(proto_smoke proto_smoke.c)
SET_TARGET_PROPERTIES(proto_smoke PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED YES
  C_EXTENSIONS YES
)
TARGET_LINK_LIBRARIES(proto_smoke PRIVATE ndb_jit_arena)

# proto_hardened: Linux only; verifies dual-mapping accepts where
# RWX would not.
IF(NOT APPLE)
  ADD_EXECUTABLE(proto_hardened proto_hardened.c)
  SET_TARGET_PROPERTIES(proto_hardened PROPERTIES
    C_STANDARD 11
    C_STANDARD_REQUIRED YES
    C_EXTENSIONS YES
  )
  TARGET_LINK_LIBRARIES(proto_hardened PRIVATE ndb_jit_arena)
ENDIF()

# proto_icache_bench: ARM64 only — measures __builtin___clear_cache /
# sys_icache_invalidate cost. On x86_64 the binary is built but
# returns a "cache coherent, free" notice immediately.
ADD_EXECUTABLE(proto_icache_bench proto_icache_bench.c)
SET_TARGET_PROPERTIES(proto_icache_bench PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED YES
  C_EXTENSIONS YES
)
TARGET_LINK_LIBRARIES(proto_icache_bench PRIVATE ndb_jit_arena)
