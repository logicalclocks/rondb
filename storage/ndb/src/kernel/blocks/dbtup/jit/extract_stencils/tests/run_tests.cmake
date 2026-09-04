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
# RONDB-1056 Phase 2 — extractor + audit unit tests.
#
# Invoked as:
#   cmake -P run_tests.cmake -- \
#         <extractor_bin> <audit_bin> <clang_path> <stencils_src_c> \
#         <include_dir> <tmp_dir> <committed_x86_h> <committed_arm_h>
#
# 11 test cases covering CLI errors, malformed input, idempotency,
# audit pass/fail. The harness is independent of the regen-stencils
# target — runs straight off committed sources.

cmake_minimum_required(VERSION 3.16)

# ---------------------------------------------------------------------------
# Argument parsing.
# ---------------------------------------------------------------------------

if(CMAKE_ARGC LESS 12)
  message(FATAL_ERROR
    "run_tests.cmake: expected 8 positional args after `--`, got ${CMAKE_ARGC} total.")
endif()

set(EXTRACTOR        "${CMAKE_ARGV4}")
set(AUDIT            "${CMAKE_ARGV5}")
set(CLANG_PATH       "${CMAKE_ARGV6}")
set(SRC_C            "${CMAKE_ARGV7}")
set(INCLUDE_DIR      "${CMAKE_ARGV8}")
set(TMP_DIR          "${CMAKE_ARGV9}")
set(COMMITTED_X86_H  "${CMAKE_ARGV10}")
set(COMMITTED_ARM_H  "${CMAKE_ARGV11}")

file(MAKE_DIRECTORY "${TMP_DIR}")

# ---------------------------------------------------------------------------
# Harness state.
# ---------------------------------------------------------------------------

set(N_PASS 0)
set(N_FAIL 0)
set(FAIL_LOG "")

# Run a command, capture exit code + stderr, assert exit matches expected.
# Usage: assert_exit(<test_name> <expected_rc> COMMAND ...)
function(assert_exit TEST_NAME EXPECTED_RC)
  set(opts "")
  set(one_value "")
  set(multi_value COMMAND)
  cmake_parse_arguments(A "${opts}" "${one_value}" "${multi_value}" ${ARGN})

  execute_process(
    COMMAND ${A_COMMAND}
    RESULT_VARIABLE RC
    OUTPUT_VARIABLE OUT
    ERROR_VARIABLE  ERR
  )
  if(RC EQUAL EXPECTED_RC)
    math(EXPR n "${N_PASS} + 1")
    set(N_PASS "${n}" PARENT_SCOPE)
    message(STATUS "  PASS  ${TEST_NAME} (rc=${RC})")
  else()
    math(EXPR n "${N_FAIL} + 1")
    set(N_FAIL "${n}" PARENT_SCOPE)
    set(MSG "${FAIL_LOG}\n[${TEST_NAME}] expected rc=${EXPECTED_RC}, got rc=${RC}")
    if(ERR)
      set(MSG "${MSG}\n  stderr: ${ERR}")
    endif()
    set(FAIL_LOG "${MSG}" PARENT_SCOPE)
    message(STATUS "  FAIL  ${TEST_NAME} (expected rc=${EXPECTED_RC}, got rc=${RC})")
  endif()
endfunction()

# Like assert_exit, but also asserts stderr contains <substring>.
function(assert_exit_stderr TEST_NAME EXPECTED_RC NEEDLE)
  set(opts "")
  set(one_value "")
  set(multi_value COMMAND)
  cmake_parse_arguments(A "${opts}" "${one_value}" "${multi_value}" ${ARGN})

  execute_process(
    COMMAND ${A_COMMAND}
    RESULT_VARIABLE RC
    OUTPUT_VARIABLE OUT
    ERROR_VARIABLE  ERR
  )
  set(rc_ok 0)
  if(RC EQUAL EXPECTED_RC)
    set(rc_ok 1)
  endif()
  set(needle_ok 0)
  string(FIND "${ERR}" "${NEEDLE}" pos)
  if(NOT pos EQUAL -1)
    set(needle_ok 1)
  endif()

  if(rc_ok AND needle_ok)
    math(EXPR n "${N_PASS} + 1")
    set(N_PASS "${n}" PARENT_SCOPE)
    message(STATUS "  PASS  ${TEST_NAME} (rc=${RC}, found `${NEEDLE}`)")
  else()
    math(EXPR n "${N_FAIL} + 1")
    set(N_FAIL "${n}" PARENT_SCOPE)
    set(why "")
    if(NOT rc_ok)
      set(why "${why}rc=${RC} (want ${EXPECTED_RC}); ")
    endif()
    if(NOT needle_ok)
      set(why "${why}stderr lacks `${NEEDLE}`; ")
    endif()
    set(MSG "${FAIL_LOG}\n[${TEST_NAME}] ${why}\n  stderr: ${ERR}")
    set(FAIL_LOG "${MSG}" PARENT_SCOPE)
    message(STATUS "  FAIL  ${TEST_NAME} (${why})")
  endif()
endfunction()

# ---------------------------------------------------------------------------
# Compile production stencils for both arches (used by happy-path tests).
# ---------------------------------------------------------------------------

set(STENCIL_FLAGS
  -O2 -fno-asynchronous-unwind-tables -ffreestanding
  -fno-stack-protector -fno-pic -std=c11 -Wall -Wextra
  "-I${INCLUDE_DIR}"
)

function(compile_arch ARCH_LABEL TARGET_TRIPLE OUT_OBJ)
  message(STATUS "compiling stencils_src.c for ${ARCH_LABEL}")
  execute_process(
    COMMAND "${CLANG_PATH}"
            "--target=${TARGET_TRIPLE}"
            ${STENCIL_FLAGS}
            -c "${SRC_C}"
            -o "${OUT_OBJ}"
    RESULT_VARIABLE RC
    ERROR_VARIABLE  ERR
  )
  if(NOT RC EQUAL 0)
    message(FATAL_ERROR
      "run_tests.cmake: clang compile of stencils_src.c for ${ARCH_LABEL} failed (rc=${RC}):\n${ERR}")
  endif()
endfunction()

set(OBJ_X86 "${TMP_DIR}/stencils_x86_64.o")
set(OBJ_ARM "${TMP_DIR}/stencils_arm64.o")
compile_arch(x86_64 "x86_64-pc-linux-gnu" "${OBJ_X86}")
compile_arch(arm64  "aarch64-linux-gnu"  "${OBJ_ARM}")

# ---------------------------------------------------------------------------
# Extract → produce headers we'll use for byte-equivalence + audit tests.
# ---------------------------------------------------------------------------

set(GEN_X86 "${TMP_DIR}/gen_x86_64.h")
set(GEN_ARM "${TMP_DIR}/gen_arm64.h")

execute_process(
  COMMAND "${EXTRACTOR}" "${OBJ_X86}" "x86_64" "${GEN_X86}"
  RESULT_VARIABLE RC ERROR_VARIABLE ERR
)
if(NOT RC EQUAL 0)
  message(FATAL_ERROR "extractor x86_64 setup failed: ${ERR}")
endif()
execute_process(
  COMMAND "${EXTRACTOR}" "${OBJ_ARM}" "arm64" "${GEN_ARM}"
  RESULT_VARIABLE RC ERROR_VARIABLE ERR
)
if(NOT RC EQUAL 0)
  message(FATAL_ERROR "extractor arm64 setup failed: ${ERR}")
endif()

# Helper: read the content of a generated header, return only `bytes_*[]`
# arrays (stripping comments which carry hex literals). We use this for
# byte-equivalence assertions.
function(read_bytes_only PATH OUT_VAR)
  file(READ "${PATH}" CONTENT)
  string(REGEX MATCHALL
         "static const uint8_t bytes_[A-Za-z0-9_]+\\[\\][^}]*}"
         CHUNKS "${CONTENT}")
  set(joined "")
  foreach(chunk IN LISTS CHUNKS)
    string(APPEND joined "${chunk}\n")
  endforeach()
  string(REGEX MATCHALL "0x[0-9a-fA-F][0-9a-fA-F]" hex_bytes "${joined}")
  string(REPLACE ";" "\n" hex_str "${hex_bytes}")
  set(${OUT_VAR} "${hex_str}" PARENT_SCOPE)
endfunction()

# ---------------------------------------------------------------------------
# Test cases.
# ---------------------------------------------------------------------------

message(STATUS "extractor-tests: starting (13 cases)")

# T1: extractor with no args → exit 2 (usage)
assert_exit_stderr("T1 extractor_no_args" 2 "usage" COMMAND "${EXTRACTOR}")

# T2: audit_magics with no args → exit 2 (usage)
assert_exit_stderr("T2 audit_no_args" 2 "usage" COMMAND "${AUDIT}")

# T3: extractor with bogus arch → die() returns exit 1
assert_exit_stderr("T3 extractor_bad_arch" 1 "unknown arch"
                    COMMAND "${EXTRACTOR}" "${OBJ_X86}" "ppc64" "${TMP_DIR}/junk.h")

# T4: extractor on a 16-byte zero file (not an ELF) → exit 1 with "bad magic"
file(WRITE "${TMP_DIR}/junk.bin" "")
file(APPEND "${TMP_DIR}/junk.bin" "abcdefghijklmnop")  # 16 bytes
assert_exit_stderr("T4 extractor_truncated_elf" 1 "ELF"
                    COMMAND "${EXTRACTOR}" "${TMP_DIR}/junk.bin" "x86_64" "${TMP_DIR}/junk.h")

# T5: audit with bogus arch → exit 2 (die from arg parsing)
assert_exit_stderr("T5 audit_bad_arch" 2 "unknown arch"
                    COMMAND "${AUDIT}" "${GEN_X86}" "ppc64")

# T6: extractor x86_64 idempotency — bytes match committed header.
read_bytes_only("${GEN_X86}"        BYTES_GEN_X86)
read_bytes_only("${COMMITTED_X86_H}" BYTES_CUR_X86)
if(BYTES_GEN_X86 STREQUAL BYTES_CUR_X86)
  math(EXPR n "${N_PASS} + 1")
  set(N_PASS "${n}")
  message(STATUS "  PASS  T6 extractor_x86_64_idempotent (bytes match committed)")
else()
  math(EXPR n "${N_FAIL} + 1")
  set(N_FAIL "${n}")
  set(FAIL_LOG "${FAIL_LOG}\n[T6] generated x86_64 bytes differ from committed ${COMMITTED_X86_H}")
  message(STATUS "  FAIL  T6 extractor_x86_64_idempotent (bytes differ)")
endif()

# T7: extractor arm64 idempotency — bytes match committed header.
read_bytes_only("${GEN_ARM}"        BYTES_GEN_ARM)
read_bytes_only("${COMMITTED_ARM_H}" BYTES_CUR_ARM)
if(BYTES_GEN_ARM STREQUAL BYTES_CUR_ARM)
  math(EXPR n "${N_PASS} + 1")
  set(N_PASS "${n}")
  message(STATUS "  PASS  T7 extractor_arm64_idempotent (bytes match committed)")
else()
  math(EXPR n "${N_FAIL} + 1")
  set(N_FAIL "${n}")
  set(FAIL_LOG "${FAIL_LOG}\n[T7] generated arm64 bytes differ from committed ${COMMITTED_ARM_H}")
  message(STATUS "  FAIL  T7 extractor_arm64_idempotent (bytes differ)")
endif()

# T8: audit committed x86_64 header → exit 0 (PASS)
assert_exit("T8 audit_x86_64_pass" 0
            COMMAND "${AUDIT}" "${COMMITTED_X86_H}" "x86_64")

# T9: audit committed arm64 header → exit 0 (PASS)
assert_exit("T9 audit_arm64_pass" 0
            COMMAND "${AUDIT}" "${COMMITTED_ARM_H}" "arm64")

# T10: corrupt arm64 header by injecting a real movz/movk chain for
# MAGIC_LCI_VAL (0x223975d389209953) into op_skip's bytes; audit must FAIL.
file(READ "${COMMITTED_ARM_H}" ARM_TXT)
# Real chain bytes lifted from op_load_const_int (offsets 4..19):
# 0x68 2a 93 d2  - movz x8, #0x9953
# 0x08 24 b1 f2  - movk x8, #0x8920, lsl #16
# 0x68 ba ce f2  - movk x8, #0x75d3, lsl #32
# 0x28 47 e4 f2  - movk x8, #0x2239, lsl #48
string(REPLACE
  "static const uint8_t bytes_op_skip[] = {"
  "static const uint8_t bytes_op_skip[] = { 0x68, 0x2a, 0x93, 0xd2, 0x08, 0x24, 0xb1, 0xf2, 0x68, 0xba, 0xce, 0xf2, 0x28, 0x47, 0xe4, 0xf2,"
  ARM_CORRUPT "${ARM_TXT}")
file(WRITE "${TMP_DIR}/arm64_corrupt.h" "${ARM_CORRUPT}")
assert_exit_stderr("T10 audit_arm64_detects_injected_chain" 1 "VIOLATION"
                    COMMAND "${AUDIT}" "${TMP_DIR}/arm64_corrupt.h" "arm64")

# T11: corrupt x86_64 header by injecting an 8-byte LE literal of
# MAGIC_LCI_VAL into op_skip's bytes; audit must FAIL.
file(READ "${COMMITTED_X86_H}" X86_TXT)
# 0x223975d389209953 LE = 0x53 0x99 0x20 0x89 0xd3 0x75 0x39 0x22
string(REPLACE
  "static const uint8_t bytes_op_skip[] = {"
  "static const uint8_t bytes_op_skip[] = { 0x53, 0x99, 0x20, 0x89, 0xd3, 0x75, 0x39, 0x22,"
  X86_CORRUPT "${X86_TXT}")
file(WRITE "${TMP_DIR}/x86_64_corrupt.h" "${X86_CORRUPT}")
assert_exit_stderr("T11 audit_x86_64_detects_injected_literal" 1 "VIOLATION"
                    COMMAND "${AUDIT}" "${TMP_DIR}/x86_64_corrupt.h" "x86_64")

# T12 — x86_64 op_load_const_int must carry its int64 immediate through
# an 8-byte hole (HOLE_64 → `movabs r64, imm64`, R_X86_64_64). A width-4
# hole here is the sign-extending `mov qword [..], imm32` fold that
# truncated every non-int32 constant on Linux (2026-09-04: 1.0 → 0.0).
file(READ "${GEN_X86}" GEN_X86_TXT)
string(REGEX MATCH
  "holes_op_load_const_int\\[\\] = {[^}]*HK_OP_IMM, \\.width = 8"
  LCI_IMM64 "${GEN_X86_TXT}")
if(LCI_IMM64)
  math(EXPR N_PASS "${N_PASS} + 1")
  message(STATUS "  PASS  T12 x86_64_load_const_int_imm64_hole")
else()
  math(EXPR N_FAIL "${N_FAIL} + 1")
  set(FAIL_LOG "${FAIL_LOG}\n[T12 x86_64_load_const_int_imm64_hole] op_load_const_int's HK_OP_IMM hole is not width 8 in ${GEN_X86}")
  message(STATUS "  FAIL  T12 x86_64_load_const_int_imm64_hole")
endif()

# T13 — x86_64 op_load_const_uint32 must not store its immediate with
# `mov r/m64, imm32` (REX.WB 0x49 0xc7: sign-extends, so [2^31, 2^32)
# came out negative). HOLE_U32 forces `mov r32, imm32` + a register store.
string(REGEX MATCH
  "static const uint8_t bytes_op_load_const_uint32\\[\\][^}]*}"
  LCU32_BYTES "${GEN_X86_TXT}")
if(LCU32_BYTES AND NOT LCU32_BYTES MATCHES "0x49, 0xc7")
  math(EXPR N_PASS "${N_PASS} + 1")
  message(STATUS "  PASS  T13 x86_64_load_const_uint32_zero_extends")
else()
  math(EXPR N_FAIL "${N_FAIL} + 1")
  set(FAIL_LOG "${FAIL_LOG}\n[T13 x86_64_load_const_uint32_zero_extends] bytes_op_load_const_uint32 missing or still uses mov r/m64, imm32 (0x49 0xc7) in ${GEN_X86}")
  message(STATUS "  FAIL  T13 x86_64_load_const_uint32_zero_extends")
endif()

# ---------------------------------------------------------------------------
# Summary.
# ---------------------------------------------------------------------------

math(EXPR TOTAL "${N_PASS} + ${N_FAIL}")
message(STATUS "extractor-tests: ${N_PASS}/${TOTAL} passed")

if(N_FAIL GREATER 0)
  message(FATAL_ERROR
    "extractor-tests: ${N_FAIL} failure(s):${FAIL_LOG}")
endif()
