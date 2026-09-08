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
 * RONDB-1056 — stencil table for targets WITHOUT a JIT backend
 * (NDB_JIT_HAVE_BACKEND == 0, see ndb_jit_platform.h). Not generated.
 *
 * Every entry is empty (no bytes, no holes) so jit1.c compiles
 * unchanged; jit1_compile() never reaches the emit pass on such a
 * target — it fails with ENOTSUP before the admission walk.
 */
#ifndef NDB_JIT_STENCILS_NONE_H
#define NDB_JIT_STENCILS_NONE_H

#include <stddef.h>
#include <stdint.h>
#include "bytecode1.h"
#include "hole_kinds.h"

static const Stencil g_stencils[OP_KIND_MAX + 1] = { { 0 } };

#endif /* NDB_JIT_STENCILS_NONE_H */
