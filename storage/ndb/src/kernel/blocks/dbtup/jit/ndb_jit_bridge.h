/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

/*
 * RONDB-1056 Phase 4 — NDB aggregation bytecode bridge.
 *
 * Translates an NDB aggregation program (Uint32-word wire format,
 * top 6 bits = opcode, lower 26 bits = operands per kOp* layout)
 * into our internal `Program` struct that jit1_compile consumes.
 *
 * Pure C. Does NOT depend on any NDB headers — callers include
 * only this file and bytecode1.h. The bridge's input is just a
 * uint32_t* array; NDB types like `Register` or `Uint32` are not
 * referenced.
 *
 * Phase 4 narrow coverage: 7 type-specialised NDB opcodes are
 * supported (LoadConst-bigint, LoadCol-bigint, Mov, Plus/Minus/Mul-
 * bigint, SumBigint), plus implicit program-end → OP_EXIT.
 * Everything else (including kOpEmbeddedInterp, kOpDiv*, kOpMod,
 * kOpSkip, all double / max / min / count variants, generic
 * kOpPlus / kOpSum without type specialisation, kOpSetRegNull)
 * returns JIT_BRIDGE_UNSUPPORTED_OP. Phase 5 expands.
 */

#ifndef NDB_JIT_BRIDGE_H
#define NDB_JIT_BRIDGE_H

#include "bytecode1.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Why a translation failed. Append-only — future opcodes / failure
 * modes add new values at the end so existing callers' switch
 * statements stay valid. */
typedef enum {
  JIT_BRIDGE_OK              = 0,
  JIT_BRIDGE_UNSUPPORTED_OP  = 1,   /* opcode has no JIT mapping */
  JIT_BRIDGE_NON_BIGINT      = 2,   /* opcode references a non-bigint type */
  JIT_BRIDGE_PROG_TOO_LARGE  = 3,   /* > BC_MAX_OPS instructions translated */
  JIT_BRIDGE_MALFORMED       = 4,   /* truncated word stream */
  JIT_BRIDGE_REG_OUT_OF_RANGE = 5,  /* register index ≥ BC_MAX_REGS */
} JitBridgeReason;

typedef struct {
  JitBridgeReason reason;
  uint32_t        offending_word;   /* index into ndb_prog[] */
  uint32_t        offending_op;     /* the kOp* value (bits 31-26) */
} JitBridgeError;

/* Translate `ndb_prog[0..n_words)` into `out_prog`.
 *
 *   On JIT_BRIDGE_OK: out_prog->n_ops is set, out_prog->ops[] is
 *   populated with translated instructions plus a trailing
 *   OP_EXIT, ready for jit1_compile.
 *
 *   On any other return: out_prog is left untouched. out_err (if
 *   non-NULL) is populated with the offending word index and
 *   opcode value for caller logging.
 *
 * Caller responsibility: verify the columns referenced by
 * kOpLoadCol are non-null bigint at the SETUP-record level.
 * Phase 4 admission cannot do this from the bytecode alone
 * (column type info lives in the column descriptor, which is
 * attached to the SETUP record alongside the bytecode). The
 * bridge accepts kOpLoadCol unconditionally; column-type
 * validation is a Day 2/3 integration concern.
 *
 * Single-pass forward walk; cost is O(n_words), no allocation. */
JitBridgeReason ndb_jit_bridge_translate(const uint32_t *ndb_prog,
                                          uint32_t       n_words,
                                          Program       *out_prog,
                                          JitBridgeError *out_err);

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_BRIDGE_H */
