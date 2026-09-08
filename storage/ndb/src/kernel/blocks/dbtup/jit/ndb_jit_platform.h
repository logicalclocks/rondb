/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

/*
 * RONDB-1056 — does this build target have a JIT backend?
 *
 * The compiled interpreter needs pre-extracted stencils
 * (stencils_x86_64.h / stencils_arm64.h) and an executable-memory
 * backend (jit_arena_linux / jit_arena_macos). Both exist for x86_64
 * and aarch64 on Linux, and for aarch64 on macOS; macOS x86_64 is
 * excluded by project decision (plan.md §1/§6/§16). Every other CPU
 * (RISC-V is next) builds the JIT tree as a stub: the arch-neutral
 * parts (bridge, program cache, code-memory manager) compile as usual,
 * jit1_compile() fails with ENOTSUP, and the data node refuses
 * CompiledInterpreter=AUTO|ON at config read (DblqhProxy) and at runtime
 * SET (Cmvmi), so the interpreter is the only engine there.
 *
 * Plain C: shared by the C JIT sources and the C++ kernel blocks.
 */
#ifndef NDB_JIT_PLATFORM_H
#define NDB_JIT_PLATFORM_H

#if (defined(__x86_64__) && !defined(__APPLE__)) || defined(__aarch64__)
#  define NDB_JIT_HAVE_BACKEND 1
#else
#  define NDB_JIT_HAVE_BACKEND 0
#endif

#endif /* NDB_JIT_PLATFORM_H */
