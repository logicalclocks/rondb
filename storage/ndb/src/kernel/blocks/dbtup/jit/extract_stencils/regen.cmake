# Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
#
# RONDB-1056 Phase 2 — stencil regeneration driver.
#
# Invoked as:
#   cmake -P regen.cmake -- \
#         <clang_path> <required_version> <stencils_src_c> <include_dir> \
#         <tmp_dir> <committed_dir> <extractor_bin> <audit_bin>
#
# Pipeline (per arch):
#   1. Preflight: clang --version must report "clang version X.Y.Z"
#      with the required X.Y.Z, and must NOT be Apple clang.
#   2. Compile stencils_src.c -> stencils_<arch>.o.
#   3. Run the extractor -> <committed_dir>/stencils_<arch>.h.new.
#   4. Run the magic-byte collision audit on the .new header.
#   5. Diff against the committed header. Identical -> delete .new.
#      Different -> copy over the committed file and print
#      REGENERATED so the caller knows to commit.

cmake_minimum_required(VERSION 3.16)

# ---------------------------------------------------------------------------
# Argument parsing.
# ---------------------------------------------------------------------------

# Args after the first `--` start at CMAKE_ARGV4
# (cmake, -P, regen.cmake, --, ...). Read positionally.
if(CMAKE_ARGC LESS 12)
  message(FATAL_ERROR
    "regen.cmake: expected 8 positional args after `--`, got ${CMAKE_ARGC} "
    "total. See the script header for usage.")
endif()

set(CLANG_PATH       "${CMAKE_ARGV4}")
set(REQUIRED_VERSION "${CMAKE_ARGV5}")
set(SRC_C            "${CMAKE_ARGV6}")
set(INCLUDE_DIR      "${CMAKE_ARGV7}")
set(TMP_DIR          "${CMAKE_ARGV8}")
set(COMMITTED_DIR    "${CMAKE_ARGV9}")
set(EXTRACTOR        "${CMAKE_ARGV10}")
set(AUDIT            "${CMAKE_ARGV11}")

if(NOT CLANG_PATH OR CLANG_PATH STREQUAL "NDB_JIT_CLANG-NOTFOUND")
  message(FATAL_ERROR
    "regen.cmake: NDB_JIT_CLANG is unset. Configure with:\n"
    "  cmake -DNDB_JIT_CLANG=/path/to/upstream/clang ..\n"
    "macOS contributors: brew install llvm@20, then use\n"
    "  /opt/homebrew/opt/llvm@20/bin/clang\n"
    "Linux contributors: dnf install clang / apt install clang.")
endif()

file(MAKE_DIRECTORY "${TMP_DIR}")

# ---------------------------------------------------------------------------
# 1. Clang version preflight.
# ---------------------------------------------------------------------------

execute_process(
  COMMAND "${CLANG_PATH}" --version
  OUTPUT_VARIABLE CLANG_VERSION_OUT
  ERROR_VARIABLE  CLANG_VERSION_ERR
  RESULT_VARIABLE CLANG_VERSION_RC
)
if(NOT CLANG_VERSION_RC EQUAL 0)
  message(FATAL_ERROR
    "regen.cmake: `${CLANG_PATH} --version` failed (rc=${CLANG_VERSION_RC}):\n"
    "${CLANG_VERSION_ERR}")
endif()

# First line only; downstream lines vary by build (Target/Thread model/etc.).
string(REGEX MATCH "^[^\n]*" CLANG_FIRST_LINE "${CLANG_VERSION_OUT}")

if(CLANG_FIRST_LINE MATCHES "Apple clang")
  message(FATAL_ERROR
    "regen.cmake: ${CLANG_PATH} reports `${CLANG_FIRST_LINE}`.\n"
    "Apple clang is NOT acceptable for stencil regeneration — the\n"
    "version mapping to upstream LLVM is undocumented and codegen can\n"
    "differ in ways the extractor doesn't tolerate.\n"
    "Install upstream LLVM ${REQUIRED_VERSION}:\n"
    "  brew install llvm@20\n"
    "  cmake -DNDB_JIT_CLANG=/opt/homebrew/opt/llvm@20/bin/clang ..")
endif()

# Accept "clang version X.Y.Z" or "Homebrew clang version X.Y.Z" — the
# Homebrew prefix is the only blessed deviation.
if(NOT CLANG_FIRST_LINE MATCHES "clang version ${REQUIRED_VERSION}")
  message(FATAL_ERROR
    "regen.cmake: ${CLANG_PATH} reports `${CLANG_FIRST_LINE}`.\n"
    "Required: clang version ${REQUIRED_VERSION} (Homebrew prefix accepted).\n"
    "If you need to roll the pin to a different version, update it in\n"
    "three places before regenerating:\n"
    "  - jit/CMakeLists.txt   (NDB_JIT_CLANG_VERSION)\n"
    "  - claude_files/compiled_interpreter/plan.md  (§2 clang pin)\n"
    "  - jit/extract_stencils/README.md\n"
    "All stencil bytes will shift; expect to also commit regenerated headers.")
endif()

message(STATUS "regen.cmake: clang preflight OK — ${CLANG_FIRST_LINE}")

# ---------------------------------------------------------------------------
# 2. Per-arch compile + extract + audit + diff.
# ---------------------------------------------------------------------------

# Project flags must match Phase 1's manual extraction recipe so the
# resulting bytes are reproducible.
set(STENCIL_FLAGS
  -O2 -fno-asynchronous-unwind-tables -ffreestanding
  -fno-stack-protector -fno-pic -std=c11 -Wall -Wextra
  "-I${INCLUDE_DIR}"
)

function(do_arch ARCH_LABEL TARGET_TRIPLE COMMITTED_NAME)
  set(OBJ   "${TMP_DIR}/stencils_${ARCH_LABEL}.o")
  set(NEW_H "${TMP_DIR}/${COMMITTED_NAME}.new")
  set(CUR_H "${COMMITTED_DIR}/${COMMITTED_NAME}")

  message(STATUS "regen.cmake: [${ARCH_LABEL}] compiling stencils_src.c -> ${OBJ}")
  execute_process(
    COMMAND "${CLANG_PATH}"
            "--target=${TARGET_TRIPLE}"
            ${STENCIL_FLAGS}
            -c "${SRC_C}"
            -o "${OBJ}"
    RESULT_VARIABLE RC
    ERROR_VARIABLE  ERR
  )
  if(NOT RC EQUAL 0)
    message(FATAL_ERROR
      "regen.cmake: [${ARCH_LABEL}] clang compile failed (rc=${RC}):\n${ERR}")
  endif()

  message(STATUS "regen.cmake: [${ARCH_LABEL}] extracting -> ${NEW_H}")
  execute_process(
    COMMAND "${EXTRACTOR}" "${OBJ}" "${ARCH_LABEL}" "${NEW_H}"
    RESULT_VARIABLE RC
    ERROR_VARIABLE  ERR
  )
  if(NOT RC EQUAL 0)
    message(FATAL_ERROR
      "regen.cmake: [${ARCH_LABEL}] extractor failed (rc=${RC}):\n${ERR}")
  endif()

  message(STATUS "regen.cmake: [${ARCH_LABEL}] auditing magic-byte collisions")
  execute_process(
    COMMAND "${AUDIT}" "${NEW_H}" "${ARCH_LABEL}"
    RESULT_VARIABLE RC
    ERROR_VARIABLE  AUDIT_ERR
  )
  # Audit prints its diagnostics on stderr; surface them whether
  # pass or fail.
  if(AUDIT_ERR)
    message("${AUDIT_ERR}")
  endif()
  if(NOT RC EQUAL 0)
    message(FATAL_ERROR
      "regen.cmake: [${ARCH_LABEL}] magic-byte audit FAILED (rc=${RC}). "
      "See diagnostics above. Most likely cause: a new opcode was "
      "added but its MAGIC_* declaration wasn't registered in "
      "audit_magics.c's kMagicToStencil[]. Less likely: clang folded "
      "two chains together — regenerate the affected MAGIC_* with a "
      "fresh high-entropy value.")
  endif()

  if(EXISTS "${CUR_H}")
    execute_process(
      COMMAND ${CMAKE_COMMAND} -E compare_files "${NEW_H}" "${CUR_H}"
      RESULT_VARIABLE DIFF_RC
      ERROR_QUIET OUTPUT_QUIET
    )
    if(DIFF_RC EQUAL 0)
      message(STATUS
        "regen.cmake: [${ARCH_LABEL}] up-to-date (no change to ${COMMITTED_NAME})")
      file(REMOVE "${NEW_H}")
      return()
    endif()
  endif()

  configure_file("${NEW_H}" "${CUR_H}" COPYONLY)
  file(REMOVE "${NEW_H}")
  message(STATUS
    "regen.cmake: [${ARCH_LABEL}] REGENERATED ${COMMITTED_NAME} — "
    "remember to `git add` + commit it.")
endfunction()

do_arch(x86_64 "x86_64-pc-linux-gnu" "stencils_x86_64.h")
do_arch(arm64  "aarch64-linux-gnu"   "stencils_arm64.h")

message(STATUS "regen.cmake: done.")
