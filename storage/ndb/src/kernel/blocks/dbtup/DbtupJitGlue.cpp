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

#include "DbtupJitGlue.hpp"
#include "AggInterpreterBase.hpp"
#include "JoinAggInterpreter.hpp"
#include "AggInterpreter.hpp"   /* for AlignedType / IsUnsigned helpers */

#include <ndb_global.h>
#include <my_byteorder.h>      /* sint8korr / sint4korr / ... */
#include <AttributeDescriptor.hpp>  /* column type decode for READ_ATTR */

#include <atomic>              /* observability counters */
#include <cstdlib>             /* malloc / free for cache products */
#include <cstring>
#include <ctime>               /* rate-limited fallback logging */

#ifndef _WIN32
#include <pthread.h>           /* pthread_once for the crash-handler install */
#include <signal.h>            /* sigaction (JIT-CRASH interposer) */
#include <unistd.h>            /* write() from the signal handler */
#if defined(__linux__)
#include <ucontext.h>          /* faulting-PC extraction */
#elif defined(__APPLE__)
#include <sys/ucontext.h>
#endif
#endif /* !_WIN32 */

extern "C" {
#include "jit/ndb_jit_bridge.h"   /* ndb_jit_bridge_translate_scan_filter */
#include "jit/jit_progcache.h"    /* program-reuse cache (Phase 8 Slice 3) */
}

#ifndef ZAGG_MATH_OVERFLOW
#define ZAGG_MATH_OVERFLOW 1860
#endif

/* ------------------------------------------------------------------ */
/* RONDB-1056 Phase 8 — CompiledInterpreter config gate.              */
/*                                                                    */
/* Node-global JIT mode from the CompiledInterpreter config param. Set */
/* once at config read (DblqhProxy::execREAD_CONFIG_REQ) before any    */
/* scan/aggregation traffic, then only read on the compile path — so a */
/* plain word needs no synchronisation. Defaults to enabled (AUTO) so  */
/* a compile before config read (none expected) still works. 0 is OFF  */
/* (NDB_COMPILED_INTERPRETER_OFF); AUTO/ON are enabled.                */
static Uint32 g_jit_mode = 1 /* NDB_COMPILED_INTERPRETER_AUTO */;

void dbtup_jit_set_mode(Uint32 mode) { g_jit_mode = mode; }

bool dbtup_jit_enabled() { return g_jit_mode != 0 /* != OFF */; }

/* ------------------------------------------------------------------ */
/* Phase 8 — observability counters (ndbinfo.jit) + fallback logging. */
/* ------------------------------------------------------------------ */

/* rows_executed is bumped once per row on the JIT execution hot path,
 * so it must not be one shared atomic — a contended fetch_add across
 * LDM threads would cost a real slice of the ~11 ns/row JIT budget.
 * Each thread claims a cache-line-padded slot on first use and does a
 * plain load+store (single writer per slot, no RMW); the stats reader
 * sums every slot. The slot claim wraps at NJT_MAX_ROW_SLOTS — two
 * threads sharing a slot after a wrap can lose increments, which is
 * stats-grade acceptable (real thread counts never approach the cap). */
struct alignas(64) JitRowSlot {
  std::atomic<Uint64> rows{0};
};
static constexpr unsigned NJT_MAX_ROW_SLOTS = 256;
static JitRowSlot g_jit_row_slots[NJT_MAX_ROW_SLOTS];
static std::atomic<unsigned> g_jit_row_slot_next{0};

static inline void jit_count_row() {
  static thread_local unsigned slot =
      g_jit_row_slot_next.fetch_add(1, std::memory_order_relaxed) %
      NJT_MAX_ROW_SLOTS;
  JitRowSlot &js = g_jit_row_slots[slot];
  js.rows.store(js.rows.load(std::memory_order_relaxed) + 1,
                std::memory_order_relaxed);
}

/* Compile-frequency counters — plain relaxed atomics are fine here. */
static std::atomic<Uint64> g_jit_fallback_count{0};
static std::atomic<Uint64> g_jit_compile_ns_total{0};

/* Rate limit for the production fallback log line. Implicit scans
 * (e.g. the EXIT_OK_LAST table-stats scan) fall back deliberately on
 * every occurrence, so an unthrottled log would flood the cluster log. */
static constexpr time_t NJT_FALLBACK_LOG_PERIOD_S = 10;
static std::atomic<time_t> g_jit_fallback_last_log{0};

void dbtup_jit_note_fallback(const char *path, int reason, Uint32 detail) {
  const Uint64 n =
      g_jit_fallback_count.fetch_add(1, std::memory_order_relaxed) + 1;
  const time_t now = time(nullptr);
  time_t last = g_jit_fallback_last_log.load(std::memory_order_relaxed);
  if (now - last < NJT_FALLBACK_LOG_PERIOD_S) {
    return;
  }
  if (!g_jit_fallback_last_log.compare_exchange_strong(
          last, now, std::memory_order_relaxed)) {
    return;   /* another thread just logged */
  }
  g_eventLogger->info(
      "RONDB-1056 JIT fallback: %s rejected (reason=%d detail=%u) — the "
      "program runs on the interpreter. %llu JIT fallbacks since node "
      "start (logged at most every %d s).",
      path, reason, (unsigned)detail, (unsigned long long)n,
      (int)NJT_FALLBACK_LOG_PERIOD_S);
}

void dbtup_jit_note_compile_ns(Uint64 ns) {
  g_jit_compile_ns_total.fetch_add(ns, std::memory_order_relaxed);
}

#ifdef ERROR_INSERT
static bool dbtup_jit_trace_start(AggInterpreterBase *agg,
                                  Dbtup *block_tup,
                                  Uint32 *row_no,
                                  Uint32 *limit) {
  if (agg == nullptr ||
      !agg->jitTraceEnabledForJit(block_tup, limit)) {
    return false;
  }
  Uint64 processed = agg->processed_rows();
  Uint32 current = processed > ~Uint32(0)
                     ? ~Uint32(0)
                     : static_cast<Uint32>(processed);
  if (current > *limit) {
    return false;
  }
  *row_no = current;
  return true;
}

static void dbtup_jit_trace_accs(const char *stage,
                                 Uint32 row_no,
                                 const int64_t *accs,
                                 Uint32 n_accs) {
  for (Uint32 i = 0; i < n_accs; i++) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u %s acc[%u]=%lld",
        row_no, stage, i, (long long)accs[i]);
  }
}
#endif

/* ------------------------------------------------------------------ */
/* Cold-call helpers.                                                 */
/* ------------------------------------------------------------------ */

extern "C" void
ndb_jit_h_load_col(JitState *s, uint32_t col_id, uint32_t dst_reg) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  /* Admission guarantees ctx is set. If it isn't, the dispatch path
   * is broken — fail fast. ndbrequire requires a JAM context only
   * available inside blocks, so we use direct null-checks + abort
   * here. ctx->agg is NOT required: the scan-filter path
   * (dbtup_jit_invoke_scan_filter) leaves it null and reaches the row
   * through block_tup + req_struct, same as the aggregation path. */
  if (ctx == nullptr ||
      ctx->block_tup == nullptr || ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_load_col: JitState.ctx is malformed (col_id=%u)",
        col_id);
    abort();
  }

  /* Read buffer: 1 word AttributeHeader + up to 2 words (8 bytes)
   * for a BIGINT. 4 words gives breathing room for alignment.
   *
   * readSingleAttribute is private on Dbtup; the glue reaches it via
   * the public Dbtup::readSingleAttributeForJit forwarder (the same
   * call AggInterpreterBase::readAttributeForJit makes), so this
   * helper no longer depends on an AggInterpreter instance. */
  Uint32 read_buf[4];

  int ret = ctx->block_tup->readSingleAttributeForJit(
      ctx->req_struct, col_id, read_buf,
      sizeof(read_buf) / sizeof(Uint32));
  if (ret < 0) {
    /* Column read failed — flag the row for interpreter fallback so
     * the interpreter path produces its normal error handling for it.
     * (Pre-5A this abort()ed.) */
    s->row_fallback = 1;
    s->regs_i64[dst_reg] = 0;
    return;
  }

  AttributeHeader *header =
      reinterpret_cast<AttributeHeader *>(&read_buf[0]);
  if (header->isNULL()) {
    /* NULL column value. The bridge cannot see nullability (it only
     * sees bytecode) and the SQL planner pushes aggregation over
     * nullable columns, so this is a NORMAL runtime condition — not an
     * admission bug. JIT registers have no null tracking until Phase
     * 5D, so flag the row: the glue discards this row's JIT run and
     * the caller re-runs it on the interpreter, whose register null
     * flags give the exact semantics (SUM/COUNT null-skip,
     * ZREGISTER_INIT_ERROR on null comparisons). Pre-5A this
     * abort()ed — a production crash for SUM(nullable_col) with any
     * NULL row. */
    s->row_fallback = 1;
    s->regs_i64[dst_reg] = 0;
    return;
  }

  /* Decode by the column's DECLARED type, mirroring the interpreter's
   * handleReadAttrIntoReg descriptor inspection. This matters because
   * the embedded READ_ATTR wire format carries no type: decoding an
   * INT column's 4-byte cell as 8 bytes reads a garbage high word (the
   * 5A join-CASE all-ELSE bug). The outer kOpLoadCol path is only
   * admitted for declared-BIGINT programs, so it always lands in the
   * BIGINT case below — behaviour unchanged.
   *
   * Types the signed-i64 register model cannot represent exactly take
   * the per-row interpreter fallback: BIGUNSIGNED (values >= 2^63
   * would misorder under the hot stencils' signed compare), FLOAT /
   * DOUBLE (until Phase 5C), strings, and pseudo columns. Narrower
   * unsigned ints zero-extend to non-negative i64 and compare
   * correctly. */
  Uint32 type_id = NDB_TYPE_UNDEFINED;
  if (likely(col_id < ctx->req_struct->tablePtrP->m_no_of_attributes)) {
    const Uint32 attrDesc1 =
        ctx->req_struct->tablePtrP->tabDescriptor[col_id * ZAD_SIZE];
    type_id = AttributeDescriptor::getType(attrDesc1);
  }
  const char *data = reinterpret_cast<const char *>(&read_buf[1]);
  Int64 value;
  switch (type_id) {
    case NDB_TYPE_TINYINT:
      value = (Int64)*reinterpret_cast<const Int8 *>(data);
      break;
    case NDB_TYPE_TINYUNSIGNED:
      value = (Int64)(Uint64)*reinterpret_cast<const Uint8 *>(data);
      break;
    case NDB_TYPE_SMALLINT:
      value = (Int64)(Int16)sint2korr(data);
      break;
    case NDB_TYPE_SMALLUNSIGNED:
      value = (Int64)(Uint64)uint2korr(data);
      break;
    case NDB_TYPE_MEDIUMINT:
      value = (Int64)sint3korr(data);
      break;
    case NDB_TYPE_MEDIUMUNSIGNED:
      value = (Int64)(Uint64)uint3korr(data);
      break;
    case NDB_TYPE_INT:
      value = (Int64)sint4korr(data);
      break;
    case NDB_TYPE_UNSIGNED:
      value = (Int64)(Uint64)uint4korr(data);
      break;
    case NDB_TYPE_BIGINT:
      value = (Int64)sint8korr(data);
      break;
    default:
      /* Not representable in the signed-i64 register model — re-run
       * the row on the interpreter (typed registers there handle it). */
      s->row_fallback = 1;
      s->regs_i64[dst_reg] = 0;
      return;
  }
  s->regs_i64[dst_reg] = value;

#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=load_col col=%u dst=r%u "
        "value=%lld",
        ctx->trace_row_no, col_id, dst_reg,
        (long long)s->regs_i64[dst_reg]);
  }
#endif
}

/* ndb_jit_h_load_col_f64 — Phase 5C-2 cold-call load for declared
 * FLOAT/DOUBLE columns (OP_LOAD_COL_NDB_F64). Same read path as
 * ndb_jit_h_load_col; the double's BIT PATTERN is stored into
 * regs_i64[dst_reg] (f64 values live bit-cast in the i64 register
 * file — the f64 stencils reinterpret on use). FLOAT promotes to
 * double, mirroring the interpreter's floatget load. NULL values and
 * any declared type other than FLOAT/DOUBLE (the bridge admits by the
 * wire type, so a mismatch here means the program no longer matches
 * the schema) take the per-row interpreter fallback. */
extern "C" void
ndb_jit_h_load_col_f64(JitState *s, uint32_t col_id, uint32_t dst_reg) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr ||
      ctx->block_tup == nullptr || ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_load_col_f64: JitState.ctx is malformed (col_id=%u)",
        col_id);
    abort();
  }

  /* 1 word AttributeHeader + 8 bytes for a DOUBLE. */
  Uint32 read_buf[4];
  int ret = ctx->block_tup->readSingleAttributeForJit(
      ctx->req_struct, col_id, read_buf,
      sizeof(read_buf) / sizeof(Uint32));
  if (ret < 0) {
    s->row_fallback = 1;
    s->regs_i64[dst_reg] = 0;
    return;
  }

  AttributeHeader *header =
      reinterpret_cast<AttributeHeader *>(&read_buf[0]);
  if (header->isNULL()) {
    /* Same per-row fallback as the i64 load — registers have no null
     * tracking until Phase 5D. */
    s->row_fallback = 1;
    s->regs_i64[dst_reg] = 0;
    return;
  }

  Uint32 type_id = NDB_TYPE_UNDEFINED;
  if (likely(col_id < ctx->req_struct->tablePtrP->m_no_of_attributes)) {
    const Uint32 attrDesc1 =
        ctx->req_struct->tablePtrP->tabDescriptor[col_id * ZAD_SIZE];
    type_id = AttributeDescriptor::getType(attrDesc1);
  }
  const uchar *data = reinterpret_cast<const uchar *>(&read_buf[1]);
  double value;
  switch (type_id) {
    case NDB_TYPE_FLOAT:
      value = (double)floatget(data);
      break;
    case NDB_TYPE_DOUBLE:
      value = doubleget(data);
      break;
    default:
      s->row_fallback = 1;
      s->regs_i64[dst_reg] = 0;
      return;
  }
  Int64 bits;
  std::memcpy(&bits, &value, sizeof(bits));
  s->regs_i64[dst_reg] = bits;

#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=load_col_f64 col=%u dst=r%u "
        "value=%f",
        ctx->trace_row_no, col_id, dst_reg, value);
  }
#endif
}

/* ndb_jit_h_load_col_nb — Phase 5D-1 NULL-BRANCHING load
 * (OP_LOAD_COL_NDB_NB). Decodes exactly like ndb_jit_h_load_col, but
 * a NULL column value RETURNS 1 — the stencil then takes its branch,
 * skipping the loaded register's whole consumer chain (the
 * interpreter kernels' null-skip), so NULL rows stay on the JIT
 * instead of the per-row fallback. Read errors and declared types
 * the signed-i64 model cannot represent keep the row_fallback
 * defense (return 0 — the blob continues, the glue discards the
 * row). */
extern "C" int
ndb_jit_h_load_col_nb(JitState *s, uint32_t col_id, uint32_t dst_reg) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr ||
      ctx->block_tup == nullptr || ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_load_col_nb: JitState.ctx is malformed (col_id=%u)",
        col_id);
    abort();
  }

  Uint32 read_buf[4];
  int ret = ctx->block_tup->readSingleAttributeForJit(
      ctx->req_struct, col_id, read_buf,
      sizeof(read_buf) / sizeof(Uint32));
  if (ret < 0) {
    s->row_fallback = 1;
    s->regs_i64[dst_reg] = 0;
    return 0;
  }

  AttributeHeader *header =
      reinterpret_cast<AttributeHeader *>(&read_buf[0]);
  if (header->isNULL()) {
    /* The whole point: take the null branch, stay on the JIT. */
    s->regs_i64[dst_reg] = 0;
#ifdef ERROR_INSERT
    if (ctx->trace_enabled) {
      g_eventLogger->info(
          "ERROR_INSERT 4063: row=%u helper=load_col_nb col=%u dst=r%u "
          "NULL -> branch",
          ctx->trace_row_no, col_id, dst_reg);
    }
#endif
    return 1;
  }

  Uint32 type_id = NDB_TYPE_UNDEFINED;
  if (likely(col_id < ctx->req_struct->tablePtrP->m_no_of_attributes)) {
    const Uint32 attrDesc1 =
        ctx->req_struct->tablePtrP->tabDescriptor[col_id * ZAD_SIZE];
    type_id = AttributeDescriptor::getType(attrDesc1);
  }
  const char *data = reinterpret_cast<const char *>(&read_buf[1]);
  Int64 value;
  switch (type_id) {
    case NDB_TYPE_TINYINT:
      value = (Int64)*reinterpret_cast<const Int8 *>(data);
      break;
    case NDB_TYPE_TINYUNSIGNED:
      value = (Int64)(Uint64)*reinterpret_cast<const Uint8 *>(data);
      break;
    case NDB_TYPE_SMALLINT:
      value = (Int64)(Int16)sint2korr(data);
      break;
    case NDB_TYPE_SMALLUNSIGNED:
      value = (Int64)(Uint64)uint2korr(data);
      break;
    case NDB_TYPE_MEDIUMINT:
      value = (Int64)sint3korr(data);
      break;
    case NDB_TYPE_MEDIUMUNSIGNED:
      value = (Int64)(Uint64)uint3korr(data);
      break;
    case NDB_TYPE_INT:
      value = (Int64)sint4korr(data);
      break;
    case NDB_TYPE_UNSIGNED:
      value = (Int64)(Uint64)uint4korr(data);
      break;
    case NDB_TYPE_BIGINT:
      value = (Int64)sint8korr(data);
      break;
    default:
      s->row_fallback = 1;
      s->regs_i64[dst_reg] = 0;
      return 0;
  }
  s->regs_i64[dst_reg] = value;

#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=load_col_nb col=%u dst=r%u "
        "value=%lld",
        ctx->trace_row_no, col_id, dst_reg,
        (long long)s->regs_i64[dst_reg]);
  }
#endif
  return 0;
}

/* ndb_jit_h_load_col_f64_nb / _u64_nb — Phase 5D-2 null-branching
 * siblings of the f64/u64 loads: NULL returns 1 (the stencil takes
 * its branch, skipping the consumer chain); read errors and
 * unexpected declared types keep the row_fallback defense. */
extern "C" int
ndb_jit_h_load_col_f64_nb(JitState *s, uint32_t col_id,
                          uint32_t dst_reg) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr ||
      ctx->block_tup == nullptr || ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_load_col_f64_nb: JitState.ctx is malformed "
        "(col_id=%u)", col_id);
    abort();
  }

  Uint32 read_buf[4];
  int ret = ctx->block_tup->readSingleAttributeForJit(
      ctx->req_struct, col_id, read_buf,
      sizeof(read_buf) / sizeof(Uint32));
  if (ret < 0) {
    s->row_fallback = 1;
    s->regs_i64[dst_reg] = 0;
    return 0;
  }

  AttributeHeader *header =
      reinterpret_cast<AttributeHeader *>(&read_buf[0]);
  if (header->isNULL()) {
    s->regs_i64[dst_reg] = 0;
    return 1;
  }

  Uint32 type_id = NDB_TYPE_UNDEFINED;
  if (likely(col_id < ctx->req_struct->tablePtrP->m_no_of_attributes)) {
    const Uint32 attrDesc1 =
        ctx->req_struct->tablePtrP->tabDescriptor[col_id * ZAD_SIZE];
    type_id = AttributeDescriptor::getType(attrDesc1);
  }
  const uchar *data = reinterpret_cast<const uchar *>(&read_buf[1]);
  double value;
  switch (type_id) {
    case NDB_TYPE_FLOAT:
      value = (double)floatget(data);
      break;
    case NDB_TYPE_DOUBLE:
      value = doubleget(data);
      break;
    default:
      s->row_fallback = 1;
      s->regs_i64[dst_reg] = 0;
      return 0;
  }
  Int64 bits;
  std::memcpy(&bits, &value, sizeof(bits));
  s->regs_i64[dst_reg] = bits;
  return 0;
}

extern "C" int
ndb_jit_h_load_col_u64_nb(JitState *s, uint32_t col_id,
                          uint32_t dst_reg) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr ||
      ctx->block_tup == nullptr || ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_load_col_u64_nb: JitState.ctx is malformed "
        "(col_id=%u)", col_id);
    abort();
  }

  Uint32 read_buf[4];
  int ret = ctx->block_tup->readSingleAttributeForJit(
      ctx->req_struct, col_id, read_buf,
      sizeof(read_buf) / sizeof(Uint32));
  if (ret < 0) {
    s->row_fallback = 1;
    s->regs_i64[dst_reg] = 0;
    return 0;
  }

  AttributeHeader *header =
      reinterpret_cast<AttributeHeader *>(&read_buf[0]);
  if (header->isNULL()) {
    s->regs_i64[dst_reg] = 0;
    return 1;
  }

  Uint32 type_id = NDB_TYPE_UNDEFINED;
  if (likely(col_id < ctx->req_struct->tablePtrP->m_no_of_attributes)) {
    const Uint32 attrDesc1 =
        ctx->req_struct->tablePtrP->tabDescriptor[col_id * ZAD_SIZE];
    type_id = AttributeDescriptor::getType(attrDesc1);
  }
  if (type_id != NDB_TYPE_BIGUNSIGNED) {
    s->row_fallback = 1;
    s->regs_i64[dst_reg] = 0;
    return 0;
  }
  const char *data = reinterpret_cast<const char *>(&read_buf[1]);
  s->regs_i64[dst_reg] = (Int64)(Uint64)uint8korr(data);
  return 0;
}

/* ndb_jit_h_load_col_u64 — Phase 5C-3 cold-call load for declared
 * BIGUNSIGNED columns (OP_LOAD_COL_NDB_U64). The u64 value's bits are
 * stored into regs_i64[dst_reg]; the u64 consumer stencils
 * (SUM_U64_CHECKED, MIN/MAX_U64) reinterpret them unsigned. Kept
 * separate from the signed helper so a schema drift cannot feed u64
 * bits (which misorder under signed compares for values >= 2^63) into
 * a signed-contract site: only a descriptor-BIGUNSIGNED column loads
 * here; anything else takes the per-row fallback. */
extern "C" void
ndb_jit_h_load_col_u64(JitState *s, uint32_t col_id, uint32_t dst_reg) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr ||
      ctx->block_tup == nullptr || ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_load_col_u64: JitState.ctx is malformed (col_id=%u)",
        col_id);
    abort();
  }

  /* 1 word AttributeHeader + 8 bytes for a BIGUNSIGNED. */
  Uint32 read_buf[4];
  int ret = ctx->block_tup->readSingleAttributeForJit(
      ctx->req_struct, col_id, read_buf,
      sizeof(read_buf) / sizeof(Uint32));
  if (ret < 0) {
    s->row_fallback = 1;
    s->regs_i64[dst_reg] = 0;
    return;
  }

  AttributeHeader *header =
      reinterpret_cast<AttributeHeader *>(&read_buf[0]);
  if (header->isNULL()) {
    /* Same per-row fallback as the other loads — registers have no
     * null tracking until Phase 5D. */
    s->row_fallback = 1;
    s->regs_i64[dst_reg] = 0;
    return;
  }

  Uint32 type_id = NDB_TYPE_UNDEFINED;
  if (likely(col_id < ctx->req_struct->tablePtrP->m_no_of_attributes)) {
    const Uint32 attrDesc1 =
        ctx->req_struct->tablePtrP->tabDescriptor[col_id * ZAD_SIZE];
    type_id = AttributeDescriptor::getType(attrDesc1);
  }
  if (type_id != NDB_TYPE_BIGUNSIGNED) {
    s->row_fallback = 1;
    s->regs_i64[dst_reg] = 0;
    return;
  }
  const char *data = reinterpret_cast<const char *>(&read_buf[1]);
  s->regs_i64[dst_reg] = (Int64)(Uint64)uint8korr(data);

#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=load_col_u64 col=%u dst=r%u "
        "value=%llu",
        ctx->trace_row_no, col_id, dst_reg,
        (unsigned long long)(Uint64)s->regs_i64[dst_reg]);
  }
#endif
}

/* ndb_jit_h_branch_attr_null — Phase 5.0 cold-call branch helper.
 *
 * Used by both op_branch_attr_eq_null (want_null=1) and
 * op_branch_attr_ne_null (want_null=0). Reads the column's
 * AttributeHeader via the same readAttributeForJit path as
 * ndb_jit_h_load_col, checks isNULL(), and returns:
 *   1 → take the branch (e.g., for IS NULL: column is null AND
 *       want_null=1, so the embedded EXIT_REFUSE landing pad —
 *       sorry actually the semantics are flipped: see the bridge
 *       — `WHERE c IS NULL` emits BRANCH_ATTR_NE_NULL +offset to
 *       EXIT_REFUSE, so taking the branch here means rejecting
 *       the row).
 *   0 → fall through.
 *
 * Lifetime: ctx is stack-local in dbtup_jit_invoke; this helper
 * runs synchronously inside that invocation, so ctx is always
 * valid. */
extern "C" int
ndb_jit_h_branch_attr_null(JitState *s,
                            uint32_t attr_id,
                            uint32_t want_null) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  /* ctx->agg is NOT required — see ndb_jit_h_load_col. The scan-filter
   * path leaves agg null and reads through block_tup + req_struct. */
  if (ctx == nullptr ||
      ctx->block_tup == nullptr || ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_branch_attr_null: JitState.ctx is malformed "
        "(attr_id=%u)", attr_id);
    abort();
  }

  /* Read just the AttributeHeader; the value bytes that follow
   * don't matter for a null check. 4 words still gives breathing
   * room for the readAttributes path's worst-case header
   * size. */
  Uint32 read_buf[4];
  int ret = ctx->block_tup->readSingleAttributeForJit(
      ctx->req_struct, attr_id, read_buf,
      sizeof(read_buf) / sizeof(Uint32));
  if (ret < 0) {
    /* Column read failed — same Phase 4 policy: panic. Phase 5+
     * wires this into JoinAggInterpreter's error path so a row
     * can be skipped cleanly. */
    g_eventLogger->error(
        "ndb_jit_h_branch_attr_null: readAttributes failed for "
        "attr_id=%u (rc=%d)", attr_id, ret);
    abort();
  }

  AttributeHeader *header =
      reinterpret_cast<AttributeHeader *>(&read_buf[0]);
  bool is_null = header->isNULL();
  int take_branch = (is_null == (want_null != 0)) ? 1 : 0;
#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=branch_attr_null attr=%u "
        "want_null=%u is_null=%u take=%u",
        ctx->trace_row_no, attr_id, want_null, is_null ? 1 : 0,
        take_branch);
  }
#endif
  return take_branch;
}

/* ndb_jit_h_branch_attr_op_arg — Phase 7 cold-call branch helper for
 * BRANCH_ATTR_OP_ARG (WHERE col <op> literal). The whole instruction is
 * read from the program buffer: ctx->prog_buf + inst_word_off points at the
 * instruction's word 0, and Dbtup::evalBranchColForJit decodes it
 * (cond / nulls / attrId / inline literal), reads the column, and compares
 * via the type's NdbSqlUtil comparator — mirroring the interpreter's
 * handleBranchAttrOp. Returns 1 to take the branch, 0 to fall through. */
extern "C" int
ndb_jit_h_branch_attr_op_arg(JitState *s, uint32_t inst_word_off) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr || ctx->block_tup == nullptr ||
      ctx->req_struct == nullptr || ctx->prog_buf == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_branch_attr_op_arg: JitState.ctx is malformed "
        "(inst_word_off=%u)", inst_word_off);
    abort();
  }
  int rc = ctx->block_tup->evalBranchColForJit(
      ctx->req_struct, ctx->prog_buf + inst_word_off, ctx->param_buf);
  if (rc < 0) {
    /* Read failure / unsupported type or condition. The bridge's admission
     * is meant to keep these off the JIT path; if one surfaces, fail fast
     * (same policy as the other helpers) rather than silently mis-filter. */
    g_eventLogger->error(
        "ndb_jit_h_branch_attr_op_arg: evalBranchColForJit failed "
        "(inst_word_off=%u, rc=%d)", inst_word_off, rc);
    abort();
  }
#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=branch_attr_op_arg off=%u take=%d",
        ctx->trace_row_no, inst_word_off, rc);
  }
#endif
  return rc;
}

/* ndb_jit_h_read_linked_to_mem — Phase 5.1a cold-call helper.
 *
 * Used by op_load_linked_to_mem to populate
 * ctx->block_tup->cheapMemory[0] from the row's linked-attr buffer
 * at the patched position. Delegates to Dbtup::readLinkedToMemBuffer
 * so the buffer-walk logic is shared one-to-one with NDB's
 * interpreter READ_LINKED_TO_MEM handler — no drift risk. */
extern "C" void
ndb_jit_h_read_linked_to_mem(JitState *s, uint32_t position) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr || ctx->join_agg == nullptr ||
      ctx->block_tup == nullptr || ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_read_linked_to_mem: JitState.ctx is malformed "
        "(position=%u)", position);
    abort();
  }
  /* Routes through JoinAggInterpreter::readLinkedToMemForJit since
   * Dbtup::cheapMemory is private — JoinAggInterpreter is friend of
   * Dbtup so it can reach the buffer + the static walk routine. */
  JoinAggInterpreter *join_agg = ctx->join_agg;
  join_agg->readLinkedToMemForJit(ctx->block_tup, ctx->req_struct,
                                  position);
#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    AttributeHeader ah(join_agg->cheapMemoryHeaderForJit(ctx->block_tup));
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=read_linked_to_mem "
        "position=%u is_null=%u bytes=%u",
        ctx->trace_row_no, position, ah.isNULL() ? 1 : 0,
        ah.getByteSize());
  }
#endif
}

/* ndb_jit_h_branch_linked_null — Phase 5.1a cold-call branch helper.
 *
 * Returns 1 to take the branch, 0 to fall through. Both
 * op_branch_linked_eq_null (want_null=1) and op_branch_linked_ne_null
 * (want_null=0) share this helper. Inspects the AttributeHeader at
 * cheapMemory[0] which a preceding op_load_linked_to_mem populated. */
extern "C" int
ndb_jit_h_branch_linked_null(JitState *s, uint32_t want_null) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr || ctx->join_agg == nullptr ||
      ctx->block_tup == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_branch_linked_null: JitState.ctx is malformed");
    abort();
  }
  JoinAggInterpreter *join_agg = ctx->join_agg;
  AttributeHeader ah(join_agg->cheapMemoryHeaderForJit(ctx->block_tup));
  bool is_null = ah.isNULL();
  int take_branch = (is_null == (want_null != 0)) ? 1 : 0;
#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=branch_linked_null "
        "want_null=%u is_null=%u take=%u",
        ctx->trace_row_no, want_null, is_null ? 1 : 0,
        take_branch);
  }
#endif
  return take_branch;
}

/* ------------------------------------------------------------------ */
/* Helper registration.                                               */
/* ------------------------------------------------------------------ */

extern "C" void dbtup_jit_register_helpers(void) {
  /* The cast through (void(*)(void)) is safe — the helper-registry
   * stores generic JitHelperFn pointers and the engine's
   * HK_COLDCALL patcher only uses the function's address, not its
   * signature. The stencil source's extern declarations are what
   * enforce the call ABI at codegen time. */
  jit1_register_helper("ndb_jit_h_load_col",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_load_col));
  jit1_register_helper("ndb_jit_h_load_col_f64",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_load_col_f64));
  jit1_register_helper("ndb_jit_h_load_col_u64",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_load_col_u64));
  jit1_register_helper("ndb_jit_h_load_col_nb",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_load_col_nb));
  jit1_register_helper("ndb_jit_h_load_col_f64_nb",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_load_col_f64_nb));
  jit1_register_helper("ndb_jit_h_load_col_u64_nb",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_load_col_u64_nb));
  jit1_register_helper("ndb_jit_h_branch_attr_null",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_branch_attr_null));
  jit1_register_helper("ndb_jit_h_branch_attr_op_arg",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_branch_attr_op_arg));
  jit1_register_helper("ndb_jit_h_read_linked_to_mem",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_read_linked_to_mem));
  jit1_register_helper("ndb_jit_h_branch_linked_null",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_branch_linked_null));
}

/* ------------------------------------------------------------------ */
/* Per-row dispatch.                                                  */
/* ------------------------------------------------------------------ */

Int32 dbtup_jit_invoke(AggInterpreterBase *agg,
                       Dbtup *block_tup,
                       Dbtup::KeyReqStruct *req_struct,
                       JitEntry            entry_fn,
                       AggResItem         *agg_res_ptr,
                       Uint32              n_agg_results,
                       JoinAggInterpreter *join_agg) {
  /* Build the per-row context on the stack. JitState.ctx points
   * at this; helpers consult it during the JIT'd code's execution
   * and never retain pointers into it. */
  dbtup_jit_call_ctx ctx;
  ctx.agg        = agg;
  ctx.join_agg   = join_agg;
  ctx.block_tup  = block_tup;
  ctx.req_struct = req_struct;
  ctx.prog_buf   = nullptr;  /* aggregation path emits no BRANCH_ATTR_OP_ARG */
  ctx.param_buf  = nullptr;
#ifdef ERROR_INSERT
  ctx.trace_enabled = dbtup_jit_trace_start(agg, block_tup,
                                            &ctx.trace_row_no,
                                            &ctx.trace_limit);
#endif

  jit_count_row();

  JitState s;
  std::memset(&s, 0, sizeof(s));
  s.ctx = &ctx;

  /* Read accumulators into s.acc_i64. Phase 4 narrow: non-null
   * BIGINT only. Cap at BC_MAX_ACCS — bridge admission rejects
   * programs that would exceed this.
   *
   * Phase 5B: value_initialized tells the MIN/MAX stencils whether the
   * accumulator holds a real value (the interpreter's first-row-
   * initialize check on AggResItem::type) — a fresh slot's copy-in
   * value of 0 must never win a comparison. Per row: the grouped path
   * points agg_res_ptr at a different group record each row. */
  if (n_agg_results > BC_MAX_ACCS) n_agg_results = BC_MAX_ACCS;
  for (Uint32 i = 0; i < n_agg_results; i++) {
    s.acc_i64[i] = agg_res_ptr[i].value.val_int64;
    s.value_initialized[i] =
        (agg_res_ptr[i].type != NDB_TYPE_UNDEFINED &&
         !agg_res_ptr[i].is_null) ? 1 : 0;
  }

#ifdef ERROR_INSERT
  if (ctx.trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u/%u jit invoke entry=%p "
        "n_agg_results=%u",
        ctx.trace_row_no, ctx.trace_limit,
        reinterpret_cast<void *>(entry_fn), n_agg_results);
    dbtup_jit_trace_accs("before", ctx.trace_row_no,
                         s.acc_i64, n_agg_results);
  }
#endif

  /* Run the JIT'd program. */
  entry_fn(&s);

  if (s.row_fallback != 0) {
    /* A helper hit a condition the JIT can't represent (NULL column
     * value). Discard everything from this run — no writeback — and
     * tell the caller to re-run the row on the interpreter. */
    return NDB_JIT_ROW_FALLBACK;
  }
  if (s.row_overflowed != 0) {
    return ZAGG_MATH_OVERFLOW;
  }

#ifdef ERROR_INSERT
  if (ctx.trace_enabled) {
    dbtup_jit_trace_accs("after", ctx.trace_row_no,
                         s.acc_i64, n_agg_results);
    for (Uint32 i = 0; i < BC_MAX_REGS; i++) {
      g_eventLogger->info(
          "ERROR_INSERT 4063: row=%u after reg[%u]=%lld",
          ctx.trace_row_no, i, (long long)s.regs_i64[i]);
    }
  }
#endif

  /* Write accumulators back only for aggregate results updated by this
   * row. Rejected rows leave NULL metadata intact for SUM/MIN/MAX
   * semantics over an empty input. value_unsigned mirrors the
   * interpreter's per-kernel result signedness: COUNT produces an
   * unsigned BIGINT (Count() inits is_unsigned=true and asserts it),
   * SUM a signed one. */
  for (Uint32 i = 0; i < n_agg_results; i++) {
    if (s.value_updated[i] != 0) {
      if (s.value_double[i] != 0) {
        /* Phase 5C-2: a double accumulator (SUM/MIN/MAX_F64). The
         * acc slot holds the double's bit pattern. */
        agg_res_ptr[i].type        = NDB_TYPE_DOUBLE;
        agg_res_ptr[i].is_unsigned = false;
        agg_res_ptr[i].is_null     = false;
        std::memcpy(&agg_res_ptr[i].value.val_double, &s.acc_i64[i],
                    sizeof(double));
      } else {
        agg_res_ptr[i].type        = NDB_TYPE_BIGINT;
        agg_res_ptr[i].is_unsigned = (s.value_unsigned[i] != 0);
        agg_res_ptr[i].is_null     = false;
        agg_res_ptr[i].value.val_int64 = s.acc_i64[i];
      }
    }
  }

  return 0;
}

/* ------------------------------------------------------------------ */
/* Phase 7/8 — scan-filter compile (reuse cache) + per-row invoke.    */
/* ------------------------------------------------------------------ */

/* A compiled scan filter's product carried by the reuse cache: the
 * jit1 handle (freed on eviction) plus the program's EXIT_REFUSE reject
 * code, which the per-row reject path needs on every use (hit or miss). */
struct ScanFilterProduct {
  Jit1Prog *jp;
  Uint32    reject_code;
};

/* Reuse-cache compile callback (a cache MISS). Translates the NDB
 * scan-filter wire format and compiles into the code-memory manager.
 * The bridge admits only the supported subset (BRANCH_ATTR_* /
 * comparison predicates / EXIT_OK / EXIT_REFUSE); anything else returns
 * != JIT_BRIDGE_OK and we refuse (-1) so the scan stays on the
 * interpreter. Returns 0 and fills *out on success. */
static int scan_filter_compile_cb(void *ctx, const uint8_t *key,
                                  uint32_t key_len, NdbJitProgItem *out) {
  (void)ctx;
  const Uint32 *prog = reinterpret_cast<const Uint32 *>(key);
  const Uint32 n_words = key_len / (Uint32)sizeof(Uint32);
  Program p;
  JitBridgeError berr;
  Uint32 reject_code = 0;
  JitBridgeReason brc = ndb_jit_bridge_translate_scan_filter(
      prog, n_words, &p, &berr, &reject_code);
  if (brc != JIT_BRIDGE_OK) {
    dbtup_jit_note_fallback("scan-filter bridge", (int)brc,
                            berr.offending_op);
    return -1;
  }
  Jit1Timing jt;
  Jit1Prog *jp = jit1_compile(ndb_jit_codemem_global(), &p, &jt);
  if (jp == nullptr) {
    dbtup_jit_note_fallback("scan-filter compile",
                            (int)jit1_last_admit_error()->reason, 0);
    return -1;
  }
  dbtup_jit_note_compile_ns(jt.total_ns);
  ScanFilterProduct *sfp =
      static_cast<ScanFilterProduct *>(malloc(sizeof(ScanFilterProduct)));
  if (sfp == nullptr) {
    jit1_free(jp);
    dbtup_jit_note_fallback("scan-filter product-alloc", 0, 0);
    return -1;
  }
  sfp->jp = jp;
  sfp->reject_code = reject_code;
  out->entry_fn = reinterpret_cast<void *>(jit1_entry(jp));
  out->user = sfp;
  return 0;
}

/* Reuse-cache destroy callback (last release of an entry): free the
 * compiled blob's code-memory slot and the product. */
static void scan_filter_destroy_cb(void *ctx, NdbJitProgItem *item) {
  (void)ctx;
  ScanFilterProduct *sfp = static_cast<ScanFilterProduct *>(item->user);
  if (sfp != nullptr) {
    jit1_free(sfp->jp);
    free(sfp);
  }
}

/* Node-global scan-filter reuse cache. Lazily created; C++11 magic-static
 * init is thread-safe across LDM threads. Never destroyed (node-lived),
 * matching the code-memory manager. */
static NdbJitProgCache *scan_filter_cache() {
  static NdbJitProgCache *cache = ndb_jit_progcache_create(
      scan_filter_compile_cb, scan_filter_destroy_cb, /*cb_ctx=*/nullptr);
  return cache;
}

void *dbtup_jit_compile_scan_filter(const Uint32 *filter_prog,
                                    Uint32        n_words,
                                    Uint32       *out_reject_code,
                                    void        **out_cache_handle) {
  if (out_reject_code != nullptr) {
    *out_reject_code = 0;
  }
  if (out_cache_handle != nullptr) {
    *out_cache_handle = nullptr;
  }
  if (!dbtup_jit_enabled()) {
    return nullptr;   /* CompiledInterpreter=OFF -> run on the interpreter */
  }
  dbtup_jit_install_crash_handler();
  if (filter_prog == nullptr || n_words == 0) {
    return nullptr;
  }
  NdbJitProgCache *cache = scan_filter_cache();
  if (cache == nullptr) {
    return nullptr;
  }

  /* Acquire the compiled form, keyed on the exact bytecode words. On a
   * hit this bumps the refcount and returns the shared blob; on a miss
   * scan_filter_compile_cb translates + compiles. nullptr => not
   * JIT-eligible / OOM => caller runs the interpreter. */
  NdbJitProgItem item;
  NjpEntry *handle = ndb_jit_progcache_acquire(
      cache, reinterpret_cast<const uint8_t *>(filter_prog),
      n_words * (Uint32)sizeof(Uint32), /*pinned=*/0, &item);
  if (handle == nullptr) {
    return nullptr;
  }

  const ScanFilterProduct *sfp =
      static_cast<const ScanFilterProduct *>(item.user);
  if (out_reject_code != nullptr) {
    *out_reject_code = sfp->reject_code;
  }
  if (out_cache_handle != nullptr) {
    *out_cache_handle = handle;
  }
  return item.entry_fn;
}

void dbtup_jit_release_scan_filter(void *cache_handle) {
  if (cache_handle == nullptr) {
    return;
  }
  ndb_jit_progcache_release(scan_filter_cache(),
                            static_cast<NjpEntry *>(cache_handle));
}

/* ------------------------------------------------------------------ */
/* Phase 8 Slice 3c — standalone aggregation reuse cache.             */
/* ------------------------------------------------------------------ */

/* Agg programs need no per-row reject code, so the product is just the
 * jit1 handle (freed on eviction). */
static int agg_compile_cb(void *ctx, const uint8_t *key, uint32_t key_len,
                          NdbJitProgItem *out) {
  (void)ctx;
  const Uint32 *prog = reinterpret_cast<const Uint32 *>(key);
  const Uint32 n_words = key_len / (Uint32)sizeof(Uint32);
  Program p;
  JitBridgeError berr;
  JitBridgeReason brc = ndb_jit_bridge_translate(prog, n_words, &p, &berr);
  if (brc != JIT_BRIDGE_OK) {
    dbtup_jit_note_fallback("aggregation bridge", (int)brc,
                            berr.offending_op);
    return -1;
  }
  Jit1Timing jt;
  Jit1Prog *jp = jit1_compile(ndb_jit_codemem_global(), &p, &jt);
  if (jp == nullptr) {
    dbtup_jit_note_fallback("aggregation compile",
                            (int)jit1_last_admit_error()->reason, 0);
    return -1;
  }
  dbtup_jit_note_compile_ns(jt.total_ns);
  out->entry_fn = reinterpret_cast<void *>(jit1_entry(jp));
  out->user = jp;   /* Jit1Prog* directly; jit1_free on destroy */
  return 0;
}

static void agg_destroy_cb(void *ctx, NdbJitProgItem *item) {
  (void)ctx;
  jit1_free(static_cast<Jit1Prog *>(item->user));
}

/* Node-global aggregation reuse cache (separate from the scan-filter
 * cache — different bytecode format). Lazy magic-static init; node-lived. */
static NdbJitProgCache *agg_cache() {
  static NdbJitProgCache *cache = ndb_jit_progcache_create(
      agg_compile_cb, agg_destroy_cb, /*cb_ctx=*/nullptr);
  return cache;
}

void *dbtup_jit_compile_agg(const Uint32 *agg_prog, Uint32 n_words,
                            void **out_cache_handle, bool pinned) {
  if (out_cache_handle != nullptr) {
    *out_cache_handle = nullptr;
  }
  if (!dbtup_jit_enabled()) {
    return nullptr;   /* CompiledInterpreter=OFF -> run on the interpreter */
  }
  dbtup_jit_install_crash_handler();
  if (agg_prog == nullptr || n_words == 0) {
    return nullptr;
  }
  NdbJitProgCache *cache = agg_cache();
  if (cache == nullptr) {
    return nullptr;
  }
  /* Phase 8 Slice 4: `pinned` comes from the program's
   * AGG_PROG_FLAG_REUSABLE header bit (RonSQL / prepared statements).
   * A pinned entry is retained at refcount 0, so the next execution of
   * the identical program is a cache hit instead of a recompile; the
   * cache upgrades an existing entry to pinned and never downgrades.
   * Bounded by the code-memory cap: on OOM new compiles fail and fall
   * back (a memory-pressure sweep is future work). */
  NdbJitProgItem item;
  NjpEntry *handle = ndb_jit_progcache_acquire(
      cache, reinterpret_cast<const uint8_t *>(agg_prog),
      n_words * (Uint32)sizeof(Uint32), pinned ? 1 : 0, &item);
  if (handle == nullptr) {
    return nullptr;
  }
  if (out_cache_handle != nullptr) {
    *out_cache_handle = handle;
  }
  return item.entry_fn;
}

void dbtup_jit_release_agg(void *cache_handle) {
  if (cache_handle == nullptr) {
    return;
  }
  ndb_jit_progcache_release(agg_cache(),
                            static_cast<NjpEntry *>(cache_handle));
}

/* ------------------------------------------------------------------ */
/* Phase 8 — node-global JIT statistics (NDBINFO).                    */
/* ------------------------------------------------------------------ */

void dbtup_jit_get_stats(NdbJitStats *out) {
  if (out == nullptr) {
    return;
  }
  NdbJitCodeMem *mem = ndb_jit_codemem_global();
  out->code_reserved_bytes = ndb_jit_codemem_reserved_bytes(mem);
  out->code_used_bytes = ndb_jit_codemem_inuse_bytes(mem);
  out->code_slots_live = ndb_jit_codemem_live_slots(mem);

  /* Sum across both reuse caches (scan filters + aggregation). */
  NdbJitProgCache *sf = scan_filter_cache();
  NdbJitProgCache *ag = agg_cache();
  out->programs_compiled = ndb_jit_progcache_compile_count(sf) +
                           ndb_jit_progcache_compile_count(ag);
  out->programs_reused = ndb_jit_progcache_hit_count(sf) +
                         ndb_jit_progcache_hit_count(ag);
  out->programs_cached = ndb_jit_progcache_live_count(sf) +
                         ndb_jit_progcache_live_count(ag);

  out->programs_fallback =
      g_jit_fallback_count.load(std::memory_order_relaxed);
  out->compile_ns_total =
      g_jit_compile_ns_total.load(std::memory_order_relaxed);
  Uint64 rows = 0;
  for (unsigned i = 0; i < NJT_MAX_ROW_SLOTS; i++) {
    rows += g_jit_row_slots[i].rows.load(std::memory_order_relaxed);
  }
  out->rows_executed = rows;
}

/* ------------------------------------------------------------------ */
/* Phase 8 — crash diagnosis (SIGSEGV interposer + DUMP).             */
/*                                                                    */
/* JIT'd code has no symbols: a fault inside a blob lands at a bare   */
/* PC that neither the stacktrace printer nor gdb can name. The        */
/* interposer below catches the fatal signal FIRST, maps the faulting  */
/* PC through jit1_describe_pc (lock-free over the live-program        */
/* registry), logs the JIT-CRASH line, and then chains to whatever     */
/* handler ndbd installed at startup (handler_error -> ErrorReporter)  */
/* so the node's normal crash path is unchanged. Installed lazily at   */
/* the first JIT compile: a node running CompiledInterpreter=OFF (or   */
/* one that never compiles) never touches signal handling at all —     */
/* and catchsigs() runs long before any query traffic, so the previous */
/* action we capture is always ndbd's own handler.                     */
/* ------------------------------------------------------------------ */

#ifndef _WIN32

static const int g_jit_crash_signals[] = {SIGSEGV, SIGBUS, SIGILL, SIGFPE};
static const int g_n_jit_crash_signals =
    (int)(sizeof(g_jit_crash_signals) / sizeof(g_jit_crash_signals[0]));
static struct sigaction g_jit_prev_action[
    sizeof(g_jit_crash_signals) / sizeof(g_jit_crash_signals[0])];

/* Faulting instruction pointer from the ucontext the kernel hands an
 * SA_SIGINFO handler. Per-platform; nullptr where unknown (the handler
 * then just chains without a JIT-CRASH line). */
static const void *jit_crash_pc_from_ucontext(void *uctx) {
  if (uctx == nullptr) {
    return nullptr;
  }
#if defined(__linux__) && defined(__x86_64__)
  const ucontext_t *uc = static_cast<const ucontext_t *>(uctx);
  return reinterpret_cast<const void *>(uc->uc_mcontext.gregs[REG_RIP]);
#elif defined(__linux__) && defined(__aarch64__)
  const ucontext_t *uc = static_cast<const ucontext_t *>(uctx);
  return reinterpret_cast<const void *>(uc->uc_mcontext.pc);
#elif defined(__APPLE__) && defined(__x86_64__)
  const ucontext_t *uc = static_cast<const ucontext_t *>(uctx);
  return reinterpret_cast<const void *>(uc->uc_mcontext->__ss.__rip);
#elif defined(__APPLE__) && defined(__aarch64__)
  const ucontext_t *uc = static_cast<const ucontext_t *>(uctx);
#if defined(arm_thread_state64_get_pc)
  return reinterpret_cast<const void *>(
      arm_thread_state64_get_pc(uc->uc_mcontext->__ss));
#else
  return reinterpret_cast<const void *>(uc->uc_mcontext->__ss.__pc);
#endif
#else
  return nullptr;
#endif
}

extern "C" void dbtup_jit_crash_handler(int signum, siginfo_t *info,
                                        void *uctx) {
  char line[256];
  const void *pc = jit_crash_pc_from_ucontext(uctx);
  if (pc != nullptr && jit1_describe_pc(pc, line, sizeof(line))) {
    /* Raw write first (async-signal-safe), then the event logger so the
     * line reaches the cluster log. The logger is not signal-safe, but
     * ndbd's own handler_error logs from this context too and the
     * process is going down either way — the write() already saved the
     * diagnosis if the logger deadlocks. */
    ssize_t wr = write(STDERR_FILENO, line, std::strlen(line));
    wr = write(STDERR_FILENO, "\n", 1);
    (void)wr;
    g_eventLogger->error("%s", line);
  }

  /* Chain to the previously installed handler (ndbd's handler_error),
   * preserving the node's normal crash path exactly. */
  const struct sigaction *prev = nullptr;
  for (int i = 0; i < g_n_jit_crash_signals; i++) {
    if (g_jit_crash_signals[i] == signum) {
      prev = &g_jit_prev_action[i];
      break;
    }
  }
  if (prev != nullptr && (prev->sa_flags & SA_SIGINFO) != 0 &&
      prev->sa_sigaction != nullptr) {
    prev->sa_sigaction(signum, info, uctx);
    return;
  }
  if (prev != nullptr && (prev->sa_flags & SA_SIGINFO) == 0 &&
      prev->sa_handler != SIG_DFL && prev->sa_handler != SIG_IGN) {
    prev->sa_handler(signum);
    return;
  }
  /* No previous handler: restore the default action and re-raise so the
   * OS produces the normal termination/core. */
  signal(signum, SIG_DFL);
  raise(signum);
}

static void dbtup_jit_install_crash_handler_once(void) {
  for (int i = 0; i < g_n_jit_crash_signals; i++) {
    struct sigaction sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = dbtup_jit_crash_handler;
    sa.sa_flags = SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    if (sigaction(g_jit_crash_signals[i], &sa, &g_jit_prev_action[i]) != 0) {
      /* Install failed for this signal — treat "previous" as default so
       * a fault still terminates via re-raise. */
      std::memset(&g_jit_prev_action[i], 0, sizeof(g_jit_prev_action[i]));
    }
  }
}

void dbtup_jit_install_crash_handler() {
  static pthread_once_t once = PTHREAD_ONCE_INIT;
  pthread_once(&once, dbtup_jit_install_crash_handler_once);
}

#else /* _WIN32 */

void dbtup_jit_install_crash_handler() {}

#endif /* !_WIN32 */

static void jit_dump_emit_line(void *arg, const char *line) {
  (void)arg;
  g_eventLogger->info("%s", line);
}

void dbtup_jit_dump_programs() {
  jit1_registry_dump(jit_dump_emit_line, nullptr);
}

bool dbtup_jit_invoke_scan_filter(Dbtup *block_tup,
                                  Dbtup::KeyReqStruct *req_struct,
                                  JitEntry             entry_fn,
                                  const Uint32        *prog_buf,
                                  const Uint32        *param_buf) {
  /* Per-row context: no aggregation instance. The cold-call helpers
   * (ndb_jit_h_load_col / ndb_jit_h_branch_attr_null /
   * ndb_jit_h_branch_attr_op_arg) reach the row through
   * block_tup->readSingleAttributeForJit, so agg / join_agg stay null.
   * prog_buf is the exec-region base so the OP_ARG helper can read the
   * instruction + its inline literal by offset. */
  dbtup_jit_call_ctx ctx;
  ctx.agg        = nullptr;
  ctx.join_agg   = nullptr;
  ctx.block_tup  = block_tup;
  ctx.req_struct = req_struct;
  ctx.prog_buf   = prog_buf;
  ctx.param_buf  = param_buf;
#ifdef ERROR_INSERT
  ctx.trace_enabled = false;   /* 4063 row trace is aggregation-only for now */
  ctx.trace_row_no  = 0;
  ctx.trace_limit   = 0;
#endif

  jit_count_row();

  JitState s;
  std::memset(&s, 0, sizeof(s));
  s.ctx = &ctx;

  entry_fn(&s);

  /* A scan filter keeps the row unless OP_FILTER_REJECT_EXIT set the
   * reject flag. The admitted NULL-branch subset performs no
   * arithmetic, so row_overflowed cannot legitimately be set here;
   * treat any unexpected overflow defensively as "reject" so a
   * miscompiled program can never leak a row past the filter. */
  if (s.row_overflowed != 0) {
    return false;
  }
  return s.row_filter_rejected == 0;
}
