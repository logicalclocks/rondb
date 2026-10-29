/*
 * Copyright (C) 2023, 2025 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#include "encoding.hpp"
#include "constants.hpp"
#include "rdrs_dal.hpp"
#include "buffer_manager.hpp"
#include <my_compiler.h>
#include "db_operations/pk/pkr_request.hpp"
#include <ArenaMalloc.hpp>
#include <EventLogger.hpp>
#include <util/require.h>
#include "buffer_sizes.hpp"
#include "buffer_chain.hpp"
#include "error_strings.h"
#include "status.hpp"

#include <cstring>
#include <string>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_ENC 1
//#define DEBUG_ENC_RESP 1
#endif

#ifdef DEBUG_ENC
#define DEB_ENC(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_ENC(...) do { } while (0)
#endif

#ifdef DEBUG_ENC_RESP
#define DEB_ENC_RESP(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_ENC_RESP(...) do { } while (0)
#endif

/**
 * Build memory structure for Primary Key request. This is stored in
 * a serial buffer, the PKRRequest object is used as a way to easily
 * access this serial buffer making sure that the user of the serial
 * buffer is hidden from the internal logic of it.
 *
 * Here is the memory structure explained. All things are stored aligned
 * at 4 byte alignment, offsets, names and values.
 *
 * All names are stored null terminated as well as with a length word.
 *
 * The input data comes from the JSON request.
 *
 * ---------------------------------------
 * | Length of Database name             |
 * ---------------------------------------
 * | Database name                       |
 * ---------------------------------------
 * | Length of Table name                |
 * ---------------------------------------
 * | Table name                          |
 * ---------------------------------------
 * | Number of Primary key columns       |
 * ---------------------------------------
 * | Array of Tuple offset PK col 0..n-1 |
 * |                                     |
 * | Tuple offset points to name + value |
 * | offsets below.                      |
 * ---------------------------------------
 * ## Array of the following for each PK
 * ## column.
 * ---------------------------------------
 * | Offset of PK name column            |
 * | Offset of PK value                  |
 * ---------------------------------------
 * | Length of PK name                   |
 * |--------------------------------------
 * | PK column name                      |
 * |--------------------------------------
 * | PK value length                     |
 * ---------------------------------------
 * | PK Value                            |
 * ---------------------------------------
 * 
 * ---------------------------------------
 * | Number of read columns              |
 * ---------------------------------------
 * | Array of read column offsets 0..m-1 |
 * ---------------------------------------
 * ## Array of the following for each column
 * ## read. The above array points to the
 * ## offset of the start of this for
 * ## each read column.
 * ---------------------------------------
 * | Data type of read column            |
 * ---------------------------------------
 * | Length of read column name          |
 * ---------------------------------------
 * | Read column name                    |
 * ---------------------------------------
 */ 
/* Upper bound of what create_native_request writes for params: every
 * string as a length word, the bytes and a terminator, 4-aligned, plus the
 * pointer tables, plus the column-name reservation when no columns are
 * named. Mirrors the encoder below; limits_test only checks that the worst
 * case fits MIN_REQ_BUFFER. */
Uint32 native_request_upper_bound(const PKReadParams &params) {
  Uint64 total = PK_REQ_HEADER_END;
  total += align_word(ADDRESS_SIZE + params.path.db.size() + 1);
  total += align_word(ADDRESS_SIZE + params.path.table.size() + 1);
  total += ADDRESS_SIZE + params.filters.size() * ADDRESS_SIZE;
  for (const auto &filter : params.filters) {
    total += 2 * ADDRESS_SIZE;
    total += align_word(ADDRESS_SIZE + filter.column.size() + 1);
    total += align_word(ADDRESS_SIZE + filter.value.size() + 1);
  }
  if (!params.operationId.empty()) {
    total += align_word(ADDRESS_SIZE + params.operationId.size() + 1);
  }
  if (!params.readColumns.empty()) {
    total += ADDRESS_SIZE + params.readColumns.size() * ADDRESS_SIZE;
    for (const auto &col : params.readColumns) {
      total += align_word(ADDRESS_SIZE + col.column.size() + 1) + ADDRESS_SIZE;
    }
  } else {
    total += REQ_COLNAME_RESERVE;
  }
  return total > 0xFFFFFFFFULL ? 0xFFFFFFFFU : static_cast<Uint32>(total);
}

RS_Status create_native_request(PKReadParams &pkReadParams,
                                RS_Buffer &view,
                                Uint32 &consumed) {
  const Uint32 bound = native_request_upper_bound(pkReadParams);
  if (unlikely(bound > view.size)) {
    return CRS_Status(
      static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
      (std::string("operation too large for request buffer: needs up to ") +
       std::to_string(bound) + " bytes, " + std::to_string(view.size) +
       " available").c_str()).status;
  }
  Uint32 *reqBuff = reinterpret_cast<Uint32 *>(view.buffer);
  Uint32 next_head = 0;
  Uint32 *buf = reqBuff;
  Uint32 head = PK_REQ_HEADER_END;
  Uint32 dbOffset = head;
  EN_Status status = {};
  head = copy_str_to_buffer(pkReadParams.path.db, reqBuff, head, status);
  if (unlikely(head == 0)) {
    return CRS_Status(status.http_code, status.message).status;
  }
  Uint32 tableOffset = head;
  head = copy_str_to_buffer(pkReadParams.path.table, reqBuff, head, status);
  if (unlikely(head == 0)) {
    return CRS_Status(status.http_code, status.message).status;
  }
  // PK Filters
  Uint32 pkOffset = head;
  buf[head / ADDRESS_SIZE] = Uint32(pkReadParams.filters.size());
  head += ADDRESS_SIZE;
  Uint32 kvi = head / ADDRESS_SIZE;
  // index for storing offsets for each key/value pair
  // skip for N number of offsets one for each key/value pair
  head += Uint32(pkReadParams.filters.size()) * ADDRESS_SIZE;
  for (auto filter : pkReadParams.filters) {
    Uint32 tupleOffset = head;
    head += 8;
    Uint32 keyNameOffset = head;
    head = copy_str_to_buffer(filter.column, reqBuff, head, status);
    if (unlikely(head == 0)) {
      return CRS_Status(status.http_code, status.message).status;
    }
    Uint32 valueOffset = head;
    head = copy_ndb_str_to_buffer(filter.value, reqBuff, head, status);
    if (unlikely(head == 0)) {
      return CRS_Status(status.http_code, status.message).status;
    }
    buf[kvi] = tupleOffset;
    kvi++;
    buf[tupleOffset / ADDRESS_SIZE] = keyNameOffset;
    buf[tupleOffset / ADDRESS_SIZE + 1] = valueOffset;
  }
  // Operation ID
  Uint32 op_id_offset = 0;
  if (likely(!pkReadParams.operationId.empty())) {
    op_id_offset = head;
    head = copy_str_to_buffer(pkReadParams.operationId, reqBuff, head, status);
    if (unlikely(head == 0)) {
      return CRS_Status(status.http_code, status.message).status;
    }
  }
  // Read Columns
  Uint32 readColsOffset = 0;
  if (likely(!pkReadParams.readColumns.empty())) {
    readColsOffset = head;
    buf[head / ADDRESS_SIZE] = (Uint32)(pkReadParams.readColumns.size());
    head += ADDRESS_SIZE;
    Uint32 rci = head / ADDRESS_SIZE;
    head += Uint32(pkReadParams.readColumns.size()) * ADDRESS_SIZE;
    for (auto col : pkReadParams.readColumns) {
      buf[rci] = head;
      rci++;
      // return type not used for the moment
      Uint32 drt = DEFAULT_DRT;
      /*
      if (!col.returnType.empty()) {
        drt = data_return_type(col.returnType);
        if (drt == UINT32_MAX) {
          return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
            "Invalid return type").status;
        }
      }
      */
      buf[head / ADDRESS_SIZE] = drt;
      head += ADDRESS_SIZE;
      // col name
      head = copy_str_to_buffer(col.column, reqBuff, head, status);
      if (unlikely(head == 0)) {
        return CRS_Status(status.http_code, status.message).status;
      }
    }
    next_head += head;
  } else {
    /**
     * Need to reserve space in request buffer for column names of all
     * table columns.
     */
    /* REQ_COLNAME_RESERVE is what addReadColumns and addReadColumnName
     * write for every table column: an offset array, then per column a
     * type word, a length word, the name and its terminator, 4-aligned.
     * The old estimate was about 6% short of that layout (RONDB-1144);
     * limits_test checks the two against each other. */
    next_head += head + REQ_COLNAME_RESERVE;
  }
  // request buffer header
  buf[PK_REQ_OP_TYPE_IDX] = (Uint32)(RDRS_PK_REQ_ID);
  buf[PK_REQ_CAPACITY_IDX] = (Uint32)(view.size);
  buf[PK_REQ_LENGTH_IDX] = (Uint32)(head);
  consumed = next_head;
  buf[PK_REQ_FLAGS_IDX] = (Uint32)(0);
  buf[PK_REQ_DB_IDX] = (Uint32)(dbOffset);
  buf[PK_REQ_TABLE_IDX] = (Uint32)(tableOffset);
  buf[PK_REQ_PK_COLS_IDX] = (Uint32)(pkOffset);
  buf[PK_REQ_READ_COLS_IDX] = (Uint32)(readColsOffset);
  buf[PK_REQ_OP_ID_IDX] = (Uint32)(op_id_offset);
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK), "OK").status;
}

/* The segments of a value whose bytes continue past its home view: the tail
 * of the home view from addr, then whole continuation buffers of the chain
 * in order, until len bytes are covered. Valid because the writer fills
 * each buffer to its last byte before spilling (RONDB-1144). */
static RS_Status build_segments(ArenaMalloc *amalloc,
                                const RS_BufferChain &chain,
                                Uint32 op_index,
                                Uint32 addr,
                                Uint32 len,
                                const RS_Segment **segs_out,
                                Uint32 *nsegs_out) {
  const RS_Buffer &home = chain.view(op_index);
  const Uint32 max_segs = 2 + len / chain.owner(0).size + 1;
  RS_Segment *segs = static_cast<RS_Segment *>(
    amalloc->alloc_bytes(max_segs * sizeof(RS_Segment), 8));
  if (unlikely(segs == nullptr)) {
    std::string msg = "Failed to allocate memory";
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k500InternalServerError),
      msg.c_str(), msg).status;
  }
  Uint32 nsegs = 0;
  Uint64 remaining = len;
  Uint64 virt = addr;          // virtual stream offset of the next byte
  Uint64 base = 0;             // virtual offset where the current region starts
  if (virt < home.size) {
    const Uint32 take = static_cast<Uint32>(
      std::min<Uint64>(home.size - virt, remaining));
    segs[nsegs++] = RS_Segment{home.buffer + virt, take};
    remaining -= take;
    virt += take;
  }
  base = home.size;
  Uint32 owner = chain.owner_of(op_index) + 1;
  while (remaining > 0) {
    if (unlikely(owner >= chain.owner_count() || nsegs >= max_segs)) {
      std::string msg = "internal server error. spilled value runs past"
        " the buffer chain";
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k500InternalServerError),
        msg.c_str(), msg).status;
    }
    const RS_Buffer &buf = chain.owner(owner);
    if (virt >= base + buf.size) {
      base += buf.size;
      owner++;
      continue;
    }
    const Uint32 start = static_cast<Uint32>(virt - base);
    const Uint32 take = static_cast<Uint32>(
      std::min<Uint64>(buf.size - start, remaining));
    segs[nsegs++] = RS_Segment{buf.buffer + start, take};
    remaining -= take;
    virt += take;
    base += buf.size;
    owner++;
  }
  *segs_out = segs;
  *nsegs_out = nsegs;
  return RS_OK;
}

RS_Status process_pkread_response(ArenaMalloc *amalloc,
                                  const RS_BufferChain &resp_chain,
                                  Uint32 op_index,
                                  RS_Buffer *reqBuff,
                                  PKReadResponseJSON &response) {
  void *respBuff = resp_chain.view(op_index).buffer;
  DEB_ENC("process_pk_read_response, respBuff: %p", respBuff);
  PKRRequest req = PKRRequest(reqBuff);
  Uint32 *buf = (Uint32 *)(respBuff);
  Uint32 responseType = buf[PK_RESP_OP_TYPE_IDX];
  Uint32 colCount = req.ReadColumnsCount();
  ResultView *result_view = (ResultView*)
    amalloc->alloc_bytes(colCount * sizeof(ResultView), 8);
  response.init(colCount, result_view);
  if (unlikely(result_view == nullptr)) {
    std::string msg = "Failed to allocate memory";
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k500InternalServerError),
      msg.c_str(), msg).status;
  }
  if (responseType != RDRS_PK_RESP_ID) {
    std::string msg = "internal server error. Wrong response type";
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k500InternalServerError),
      msg.c_str(), msg).status;
  }
  // some sanity checks
  Uint32 capacity   = buf[PK_RESP_CAPACITY_IDX];
  Uint32 dataLength = buf[PK_RESP_LENGTH_IDX];

  if (capacity < dataLength) {
    std::string message = "internal server error. response buffer"
      " may be corrupt. ";
    message += "Buffer capacity: " + std::to_string(capacity) +
               ", data length: " + std::to_string(dataLength);
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k500InternalServerError),
      message.c_str(), message).status;
  }
  Uint32 opIDX = buf[PK_RESP_OP_ID_IDX];
  if (opIDX != 0) {
    const char* opIDXPtr = (const char*)respBuff + opIDX;
    DEB_ENC("Set OperationID: %s", opIDXPtr);
    response.setOperationID(opIDXPtr, (Uint32)strlen(opIDXPtr));
  }

  Int32 status = (Int32)(buf[PK_RESP_OP_STATUS_IDX]);
  response.setStatusCode(static_cast<drogon::HttpStatusCode>(status));
  if (status == drogon::HttpStatusCode::k200OK) {
    Uint32 colIDX = buf[PK_RESP_COLS_IDX];
    UintPtr colIDXPtr = (UintPtr)respBuff + (UintPtr)colIDX;
    Uint32 colCount = *(Uint32 *)colIDXPtr;
    for (Uint32 i = 0; i < colCount; i++) {
      Uint32 *colHeaderStart = reinterpret_cast<Uint32 *>(
        reinterpret_cast<UintPtr>(respBuff) + colIDX + ADDRESS_SIZE +
          i * 4 * ADDRESS_SIZE);

      const char* name = req.ReadColumnName(i);
      Uint32 name_len = req.ReadColumnNameLen(i);
      Uint32 value_len = colHeaderStart[0];
      Uint32 value_add = colHeaderStart[1];
      Uint32 isNull = colHeaderStart[2];
      Uint32 dataType = colHeaderStart[3];
      if (isNull == 0) {
        char * value = (reinterpret_cast<char*>(respBuff) + value_add);
        bool quoted = dataType != RDRS_INTEGER_DATATYPE &&
                      dataType != RDRS_FLOAT_DATATYPE;
        /* A blob or text value may continue past the home view into the
         * chain (RONDB-1144): hand it over as segments. */
        if (static_cast<Uint64>(value_add) + value_len > dataLength) {
          const RS_Segment *segs = nullptr;
          Uint32 nsegs = 0;
          RS_Status seg_status = build_segments(amalloc, resp_chain, op_index,
                                               value_add, value_len,
                                               &segs, &nsegs);
          if (unlikely(seg_status.http_code != SUCCESS)) {
            return seg_status;
          }
          response.setColumnData(i, name, name_len, value, value_len, quoted,
                                 dataType, segs, nsegs);
          continue;
        }
        DEB_ENC("setColumnData(%u) name: %s, name_len: %u,"
                " value_len: %u, quoted: %u, value: %s",
          i,
          name,
          name_len,
          value_len,
          quoted,
          std::string(value, value_len).c_str());
        response.setColumnData(i,
                               name,
                               name_len,
                               value,
                               value_len,
                               quoted,
                               dataType);
      } else {
        const char *null_value = "null";
        Uint32 null_value_len = strlen(null_value);
        response.setColumnData(i,
                               name,
                               name_len,
                               null_value,
                               null_value_len,
                               false,
                               RDRS_STRING_DATATYPE);
      }
    }
  }
  std::string message = "";
  Uint32 messageIDX = buf[PK_RESP_OP_MESSAGE_IDX];
  if (messageIDX != 0) {
    UintPtr messageIDXPtr = (UintPtr)respBuff + (UintPtr)messageIDX;
    message = std::string((char *)messageIDXPtr);
    response.addSizeJsonMessage();
#ifdef DEBUG_ENC_RESP
    if (status != 200) {
      DEB_ENC_RESP("message: %s", message.c_str());
    }
#endif
  }
#ifdef DEBUG_ENC_RESP
  if (status != 200) {
    DEB_ENC_RESP("status: %d", status);
  }
#endif
  DEB_ENC("OperationID: %s, view: %s, len_str: %u, len_view: %u",
          response.getOperationIdString().c_str(),
          response.getOperationID().data(),
          (Uint32)response.getOperationIdString().size(),
          (Uint32)response.getOperationID().size());
  return CRS_Status(static_cast<HTTP_CODE>(status), message.c_str()).status;
}
