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
 * Current coverage: type-specialised bigint hot ops from Phase 4
 * plus the Phase 5 embedded filter/row-disposition subset
 * (BRANCH_ATTR_*_NULL, READ_LINKED_TO_MEM, BRANCH_LINKED_*_NULL,
 * LOAD_CONST16, WRITE_INTERPRETER_OUTPUT slot 0, EXIT_OK,
 * EXIT_REFUSE). Everything else returns JIT_BRIDGE_UNSUPPORTED_OP.
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
  JIT_BRIDGE_EMBEDDED_TOO_LARGE = 6,/* embedded block exceeds Phase 5.0 cap */
  JIT_BRIDGE_EMBEDDED_BACKWARD = 7, /* embedded block has a backward branch */
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

/* Phase 7: translate a standalone SCAN_FRAGREQ scan-filter interpreter
 * program into the internal Program/Op[] form. Wired into DBTUP via
 * Dbtup::scanCopyAttrinfo (compile) + Dbtup::interpreterStartLab (per-row
 * dispatch); see phase_7_implementation.md.
 *
 * Reuses the embedded-interpreter subset BRANCH_ATTR_*_NULL, EXIT_OK,
 * EXIT_REFUSE. EXIT_OK lowers to OP_EXIT (the accept terminator) and
 * EXIT_REFUSE to OP_FILTER_REJECT_EXIT; a trailing OP_EXIT covers a
 * fall-off-the-end accept. Linked ops (READ_LINKED_TO_MEM,
 * BRANCH_LINKED_*_NULL) are rejected here — they need a join-aggregation
 * context a standalone scan filter doesn't have.
 *
 * out_reject_code (nullable) receives the program's EXIT_REFUSE code
 * (theInstruction >> 16 — the same field the interpreter's handleExitRefuse
 * reads), so the runtime can TUPKEY_abort with the program's actual code
 * instead of a hardcoded one. A boolean WHERE filter rejects with one
 * uniform code; a program whose EXIT_REFUSE words carry differing codes is
 * rejected (JIT_BRIDGE_UNSUPPORTED_OP) since one value can't represent them.
 * 0 is written when the filter has no reject path. */
JitBridgeReason ndb_jit_bridge_translate_scan_filter(
    const uint32_t *filter_prog,
    uint32_t       n_words,
    Program       *out_prog,
    JitBridgeError *out_err,
    uint32_t       *out_reject_code);

#ifdef NDB_JIT_BRIDGE_TESTING
JitBridgeReason ndb_jit_bridge_translate_embedded_for_test(
    const uint32_t *emb_prog,
    uint32_t       emb_len,
    Program       *out_prog,
    JitBridgeError *out_err,
    uint32_t       outer_word_pos);
#endif

/* Diagnostic helpers shared by DBLQH setup logging and unit tests.
 * The dump functions emit one already-formatted line per callback
 * invocation; callers decide whether that goes to g_eventLogger,
 * stdout, or a test failure buffer. */
typedef void (*NdbJitBridgeDumpFn)(void *ctx, const char *line);

const char *ndb_jit_bridge_reason_name(JitBridgeReason reason);
const char *ndb_jit_bridge_agg_op_name(uint32_t op);
const char *ndb_jit_bridge_emb_op_name(uint32_t op);
const char *ndb_jit_bridge_jit_op_name(uint8_t kind);

void ndb_jit_bridge_dump_input(const uint32_t *header,
                               uint32_t       header_words,
                               const uint32_t *body,
                               uint32_t       body_words,
                               NdbJitBridgeDumpFn dump,
                               void          *ctx);

void ndb_jit_bridge_dump_program(const Program *prog,
                                 NdbJitBridgeDumpFn dump,
                                 void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_BRIDGE_H */
