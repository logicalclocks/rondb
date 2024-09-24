/*
 * Copyright (C) 2023 Hopsworks AB
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

#include <cstring>
#include <string>

RS_Status create_native_request(PKReadParams &pkReadParams, void *reqBuff, void * /*respBuff*/) {
  uint32_t *buf = (uint32_t *)(reqBuff);

  uint32_t head = PK_REQ_HEADER_END;

  uint32_t dbOffset = head;

  EN_Status status = copy_str_to_buffer(pkReadParams.path.db, reqBuff, head);

  if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
    head = status.retValue;
  } else {
    return CRS_Status(status.http_code, status.message).status;
  }

  uint32_t tableOffset = head;

  status = copy_str_to_buffer(pkReadParams.path.table, reqBuff, head);

  if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
    head = status.retValue;
  } else {
    return CRS_Status(status.http_code, status.message).status;
  }

  // PK Filters
  head = align_word(head);

  uint32_t pkOffset        = head;
  buf[head / ADDRESS_SIZE] = uint32_t(pkReadParams.filters.size());
  head += ADDRESS_SIZE;

  uint32_t kvi = head / ADDRESS_SIZE;  // index for storing offsets for each key/value pair
  // skip for N number of offsets one for each key/value pair
  head += uint32_t(pkReadParams.filters.size()) * ADDRESS_SIZE;

  for (auto filter : pkReadParams.filters) {
    head = align_word(head);

    uint32_t tupleOffset = head;

    head += 8;

    uint32_t keyOffset = head;

    status = copy_str_to_buffer(filter.column, reqBuff, head);

    if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
      head = status.retValue;
    } else {
      return CRS_Status(status.http_code, status.message).status;
    }

    uint32_t value_offset = head;

    status = copy_ndb_str_to_buffer(filter.value, reqBuff, head);

    if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
      head = status.retValue;
    } else {
      return CRS_Status(status.http_code, status.message).status;
    }

    buf[kvi] = tupleOffset;
    kvi++;
    buf[tupleOffset / ADDRESS_SIZE]     = keyOffset;
    buf[tupleOffset / ADDRESS_SIZE + 1] = value_offset;
  }

  // Read Columns
  head                    = align_word(head);
  uint32_t readColsOffset = 0;
  if (!pkReadParams.readColumns.empty()) {
    readColsOffset           = head;
    buf[head / ADDRESS_SIZE] = (uint32_t)(pkReadParams.readColumns.size());
    head += ADDRESS_SIZE;

    uint32_t rci = head / ADDRESS_SIZE;
    head += uint32_t(pkReadParams.readColumns.size()) * ADDRESS_SIZE;

    for (auto col : pkReadParams.readColumns) {
      head = align_word(head);

      buf[rci] = head;
      rci++;

      // return type
      uint32_t drt = DEFAULT_DRT;
      if (!col.returnType.empty()) {
        drt = data_return_type(col.returnType);
        if (drt == UINT32_MAX) {
          return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                            "Invalid return type")
              .status;
        }
      }

      buf[head / ADDRESS_SIZE] = drt;
      head += ADDRESS_SIZE;

      // col name
      status = copy_str_to_buffer(col.column, reqBuff, head);

      if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
        head = status.retValue;
      } else {
        return CRS_Status(status.http_code, status.message).status;
      }
    }
  }

  // Operation ID
  uint32_t op_id_offset = 0;
  if (!pkReadParams.operationId.empty()) {
    op_id_offset = head;

    status = copy_str_to_buffer(pkReadParams.operationId, reqBuff, head);

    if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
      head = status.retValue;
    } else {
      return CRS_Status(status.http_code, status.message).status;
    }
  }

  // request buffer header
  buf[PK_REQ_OP_TYPE_IDX]   = (uint32_t)(RDRS_PK_REQ_ID);
  buf[PK_REQ_CAPACITY_IDX]  = (uint32_t)(globalConfigs.internal.respBufferSize);
  buf[PK_REQ_LENGTH_IDX]    = (uint32_t)(head);
  buf[PK_REQ_FLAGS_IDX]     = (uint32_t)(0);  // FIXME TODO fill in. is_grpc, is_http ...
  buf[PK_REQ_DB_IDX]        = (uint32_t)(dbOffset);
  buf[PK_REQ_TABLE_IDX]     = (uint32_t)(tableOffset);
  buf[PK_REQ_PK_COLS_IDX]   = (uint32_t)(pkOffset);
  buf[PK_REQ_READ_COLS_IDX] = (uint32_t)(readColsOffset);
  buf[PK_REQ_OP_ID_IDX]     = (uint32_t)(op_id_offset);

  return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK), "OK").status;
}

RS_Status process_pkread_response(void *respBuff, PKReadResponseJSON &response) {
  uint32_t *buf = (uint32_t *)(respBuff);

  uint32_t responseType = buf[PK_RESP_OP_TYPE_IDX];

  if (responseType != RDRS_PK_RESP_ID) {
    std::string msg = "internal server error. Wrong response type";
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k500InternalServerError),
                      msg.c_str(), msg)
        .status;
  }

  // some sanity checks
  uint32_t capacity   = buf[PK_RESP_CAPACITY_IDX];
  uint32_t dataLength = buf[PK_RESP_LENGTH_IDX];

  if (capacity < dataLength) {
    std::string message = "internal server error. response buffer may be corrupt. ";
    message += "Buffer capacity: " + std::to_string(capacity) +
               ", data length: " + std::to_string(dataLength);
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k500InternalServerError),
                      message.c_str(), message)
        .status;
  }

  uint32_t opIDX = buf[PK_RESP_OP_ID_IDX];
  if (opIDX != 0) {
    uintptr_t opIDXPtr = (uintptr_t)respBuff + (uintptr_t)opIDX;
    std::string goOpID = std::string((char *)opIDXPtr);
    response.setOperationID(goOpID);
  }

  int32_t status = (int32_t)(buf[PK_RESP_OP_STATUS_IDX]);
  response.setStatusCode(static_cast<drogon::HttpStatusCode>(status));

  if (status == drogon::HttpStatusCode::k200OK) {
    uint32_t colIDX     = buf[PK_RESP_COLS_IDX];
    uintptr_t colIDXPtr = (uintptr_t)respBuff + (uintptr_t)colIDX;
    uint32_t colCount   = *(uint32_t *)colIDXPtr;

    for (uint32_t i = 0; i < colCount; i++) {
      uint32_t *colHeaderStart = reinterpret_cast<uint32_t *>(
          reinterpret_cast<uintptr_t>(respBuff) + colIDX + ADDRESS_SIZE + i * 4 * ADDRESS_SIZE);

      uint32_t colHeader[4];
      for (uint32_t j = 0; j < 4; j++) {
        colHeader[j] = colHeaderStart[j];
      }

      uint32_t nameAdd = colHeader[0];
      std::string name = std::string((char *)(reinterpret_cast<uintptr_t>(respBuff) + nameAdd));

      uint32_t valueAdd = colHeader[1];

      uint32_t isNull = colHeader[2];

      uint32_t dataType = colHeader[3];

      if (isNull == 0) {
        std::string value = std::string((char *)(reinterpret_cast<uintptr_t>(respBuff) + valueAdd));

        std::string quotedValue = quote_if_string(dataType, value);
        response.setColumnData(name, std::vector<char>(quotedValue.begin(), quotedValue.end()));
      } else {
        response.setColumnData(name, std::vector<char>());
      }
    }
  }

  std::string message = "";
  uint32_t messageIDX = buf[PK_RESP_OP_MESSAGE_IDX];
  if (messageIDX != 0) {
    uintptr_t messageIDXPtr = (uintptr_t)respBuff + (uintptr_t)messageIDX;
    message                 = std::string((char *)messageIDXPtr);
  }
  return CRS_Status(static_cast<HTTP_CODE>(status), message.c_str()).status;
}
