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

#include "rdrs_dal.h"
#include "db_operations/pk/pkr_operation.hpp"
#include "db_operations/ronsql/ronsql_operation.hpp"
#include "rdrs_dal.hpp"
#include "rdrs_rondb_connection_pool.hpp"
#include "ndb_api_helper.hpp"
#include "retry_handler.hpp"
#include "status.hpp"
#include "logger.hpp"
#include "pk_data_structs.hpp"
#include "scan_metrics.hpp"

#include <storage/ndb/include/ndb_global.h>
#include <util/require.h>
#include <mgmapi.h>
#include <my_base.h>
#include <unistd.h>
#include <NdbApi.hpp>
#include <cstdlib>
#include <cstring>
#include <EventLogger.hpp>

#include <rapidjson/fwd.h>
#include <rapidjson/document.h>      // rapidjson::Document
#include <rapidjson/prettywriter.h>  // rapidjson::PrettyWriter
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>  // rapidjson::Writer
#include "my_byteorder.h"

#include <my_time.h>
#include <decimal_utils.hpp>
#include <decimal.h>
#include <libbase64.h>
#include "rdrs_const.h"

extern EventLogger *g_eventLogger;

#include "storage/ndb/src/ronsql/RonSQLCommon.hpp"
#include "string_with_len.h"

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_DAL 1
#endif

#ifdef DEBUG_DAL
#define DEB_TRACE() do { \
  printf("rdrs_dal.cpp:%d\n", __LINE__); \
  fflush(stdout); \
} while (0)
#else
#define DEB_TRACE() do { } while (0)
#endif


RDRSRonDBConnectionPool *rdrsRonDBConnectionPool = nullptr;

RS_Status init(unsigned int numThreads, unsigned int num_data_connections) {
  // disable buffered stdout
  setbuf(stdout, NULL);

  // Initialize NDB Connection and Object Pool
  rdrsRonDBConnectionPool = new RDRSRonDBConnectionPool();
  RS_Status status = rdrsRonDBConnectionPool->Init(numThreads,
                                                   num_data_connections);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RS_Status add_data_connection(const char *connection_string,
                              unsigned int connection_pool_size,
                              unsigned int *node_ids,
                              unsigned int node_ids_len,
                              unsigned int connection_retries,
                              unsigned int connection_retry_delay_in_sec) {

  RS_Status status = rdrsRonDBConnectionPool->AddConnections(
    connection_string,
    connection_pool_size,
    node_ids,
    node_ids_len,
    connection_retries,
    connection_retry_delay_in_sec);

  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RS_Status add_metadata_connection(const char *connection_string,
                                  unsigned int connection_pool_size,
                                  unsigned int *node_ids,
                                  unsigned int node_ids_len,
                                  unsigned int connection_retries,
                                  unsigned int connection_retry_delay_in_sec) {

  RS_Status status = rdrsRonDBConnectionPool->AddMetaConnections(
    connection_string,
    connection_pool_size,
    node_ids,
    node_ids_len,
    connection_retries,
    connection_retry_delay_in_sec);

  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RS_Status set_data_cluster_op_retry_props(
  const unsigned int retry_cont,
  const unsigned int rety_initial_delay,
  const unsigned int jitter) {
  DATA_CONN_OP_RETRY_COUNT = retry_cont;
  DATA_CONN_OP_RETRY_INITIAL_DELAY_IN_MS = rety_initial_delay;
  DATA_CONN_OP_RETRY_JITTER_IN_MS = jitter;
  return RS_OK;
}

RS_Status set_metadata_cluster_op_retry_props(
  const unsigned int retry_cont,
  const unsigned int rety_initial_delay,
  const unsigned int jitter) {
  METADATA_CONN_OP_RETRY_COUNT = retry_cont;
  METADATA_CONN_OP_RETRY_INITIAL_DELAY_IN_MS = rety_initial_delay;
  METADATA_CONN_OP_RETRY_JITTER_IN_MS = jitter;
  return RS_OK;
}

RS_Status shutdown_connection() {
  rdrsRonDBConnectionPool->shutdown();
  delete rdrsRonDBConnectionPool;
  return RS_OK;
}

RS_Status reconnect() {
  return rdrsRonDBConnectionPool->Reconnect();
}

RS_Status pk_batch_read(void *amalloc_void,
                        unsigned int no_req,
                        bool is_batch,
                        RS_Buffer *req_buffs,
                        RS_Buffer *resp_buffs,
                        unsigned int threadIndex) {
  ArenaMalloc *amalloc = (ArenaMalloc*)amalloc_void;
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetNdbObject(&ndb_object,
                                                           threadIndex);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  DATA_OP_RETRY_HANDLER(
    BatchKeyOperations pkread;
    status = pkread.perform_operation(amalloc,
                                      no_req,
                                      is_batch,
                                      req_buffs,
                                      resp_buffs,
                                      ndb_object);
  )
  rdrsRonDBConnectionPool->ReturnNdbObject(ndb_object,
                                           &status,
                                           threadIndex);
  return status;
}

RS_Status ronsql_dal(const char* database,
                     RonSQLExecParams* ep,
                     unsigned int threadIndex) {
  assert(ep != nullptr);
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetNdbObject(&ndb_object,
                                                           threadIndex);
  if (unlikely(status.http_code != SUCCESS)) {
    DEB_TRACE();
    return status;
  }

  assert(ep->ndb == NULL);
  assert(ndb_object != NULL);
  ep->ndb = ndb_object;
  std::string saved_database_name = ndb_object->getDatabaseName();
  ndb_object->setDatabaseName(database);
  DEB_TRACE();
  status = ronsql_op(*ep);
  DEB_TRACE();
  ndb_object->setDatabaseName(saved_database_name.c_str());
  ep->ndb = NULL;
  rdrsRonDBConnectionPool->ReturnNdbObject(ndb_object,
                                           &status,
                                           threadIndex);
  DEB_TRACE();
  return status;
}

/**
 * Returns statistis about RonDB connection
 */
RS_Status get_rondb_stats(RonDB_Stats *stats) {
  RonDB_Stats ret = rdrsRonDBConnectionPool->GetStats();
  stats->ndb_objects_created = ret.ndb_objects_created;
  stats->ndb_objects_deleted = ret.ndb_objects_deleted;
  stats->ndb_objects_count = ret.ndb_objects_count;
  stats->ndb_objects_available = ret.ndb_objects_available;
  stats->connection_state = ret.connection_state;
  stats->is_reconnection_in_progress = ret.is_reconnection_in_progress;
  stats->is_shutdown = ret.is_shutdown;
  stats->is_shutting_down = ret.is_shutting_down;
  return RS_OK;
}

int get_num_ready_data_nodes() {
  return rdrsRonDBConnectionPool->GetMinReadyDataNodes();
}

void*
get_rdrs_ndb_object(int thread_index) {
  Ndb *ndb_object  = nullptr;
  (void)rdrsRonDBConnectionPool->GetNdbObject(&ndb_object,
                                              thread_index);
  return (void*)ndb_object;
}

void
return_rdrs_ndb_object(void *ndb_object, int thread_index) {
  RS_Status status = RS_OK;
  rdrsRonDBConnectionPool->ReturnNdbObject((Ndb*)ndb_object,
                                           &status,
                                           thread_index);
}
CRS_Status CRS_Status::SUCCESS = CRS_Status(HTTP_CODE::SUCCESS);

class Bitmap {
 public:
   Bitmap(int n_cols)
     : n_bytes_((n_cols + 7) / 8), bitmap_(nullptr) {
   }

   ~Bitmap() {
     delete[] bitmap_;
   }

   Bitmap(const Bitmap&) = delete;
   Bitmap& operator=(const Bitmap&) = delete;

   bool Init() {
     if (!bitmap_) {
       bitmap_ = new(std::nothrow) unsigned char[n_bytes_];
       if (!bitmap_) {
         return false;
       }
       memset(bitmap_, 0, n_bytes_);
     }
     return true;
   }

   void SetBit(int col) {
     if (col < 0 || col >= n_bytes_ * 8) {
       return;
     }
     int idx = col / 8;
     int offset = col & 7;
     bitmap_[idx] |= (static_cast<unsigned char>(1) << offset);
     // std::cout << "bitmap: "
     //   << static_cast<int>(bitmap_[idx]) << std::endl;
   }

   bool GetBit(int col) const {
     if (col < 0 || col >= n_bytes_ * 8) {
       return false;
     }
     int idx = col / 8;
     int offset = col & 7;
     return (bitmap_[idx] & (static_cast<unsigned char>(1) << offset)) != 0;
   }

   const unsigned char* bitmap() const {
     return bitmap_;
   }

 private:
   int n_bytes_;
   unsigned char* bitmap_;
};

// Helper function to unpack a 3-byte DATE value into MYSQL_TIME
// Matches MySQL's Field_newdate::get_date_internal() in sql/field.cc
static inline void my_unpack_date(MYSQL_TIME *l_time, const void *d) {
  uchar b[4];
  memcpy(b, d, 3);
  b[3] = 0;
  uint w = (uint)uint3korr(b);
  l_time->day = (w & 31);
  w >>= 5;
  l_time->month = (w & 15);
  w >>= 4;
  l_time->year = w;
  l_time->time_type = MYSQL_TIMESTAMP_DATE;
}

RS_Status GenerateBinary(Node& node, std::vector<uint8_t>& bin) {
  RS_Status status = RS_OK;
  assert(node.col != nullptr);
  bin.clear();
  switch(node.col->getType()) {
    case NdbDictionary::Column::Tinyint: {
      int64_t val;
      if (node.value.kind == Node::ParsedValue::Kind::INT64) {
        val = node.value.i64;
      } else if (node.value.kind == Node::ParsedValue::Kind::UINT64 &&
                 node.value.u64 <= static_cast<uint64_t>(INT8_MAX)) {
        val = static_cast<int64_t>(node.value.u64);
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        char* endptr = nullptr;
        val = strtoll(node.value.s.c_str(), &endptr, 10);
        if (endptr == node.value.s.c_str() || *endptr != '\0') {
          status = RS_CLIENT_ERROR("Invalid value for TINYINT column. Column: " +
              std::string(node.col->getName()));
          break;
        }
      } else {
        status = RS_CLIENT_ERROR("Invalid value for TINYINT column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      if (val < INT8_MIN || val > INT8_MAX) {
        status = RS_CLIENT_ERROR("Value out of range for TINYINT column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      int8_t x = val;
      bin.push_back(*reinterpret_cast<int8_t*>(&x));
      break;
    }
    case NdbDictionary::Column::Smallint: {
      int64_t val;
      if (node.value.kind == Node::ParsedValue::Kind::INT64) {
        val = node.value.i64;
      } else if (node.value.kind == Node::ParsedValue::Kind::UINT64 &&
                 node.value.u64 <= static_cast<uint64_t>(INT16_MAX)) {
        val = static_cast<int64_t>(node.value.u64);
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        char* endptr = nullptr;
        val = strtoll(node.value.s.c_str(), &endptr, 10);
        if (endptr == node.value.s.c_str() || *endptr != '\0') {
          status = RS_CLIENT_ERROR("Invalid value for SMALLINT column. Column: " +
              std::string(node.col->getName()));
          break;
        }
      } else {
        status = RS_CLIENT_ERROR("Invalid value for SMALLINT column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      if (val < INT16_MIN || val > INT16_MAX) {
        status = RS_CLIENT_ERROR("Value out of range for SMALLINT column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      int16_t x = val;
      bin.resize(sizeof(int16_t));
      int2store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Mediumint: {
      // MEDIUMINT is 3-byte signed: -8388608 to 8388607
      int64_t val;
      if (node.value.kind == Node::ParsedValue::Kind::INT64) {
        val = node.value.i64;
      } else if (node.value.kind == Node::ParsedValue::Kind::UINT64 &&
                 node.value.u64 <= 8388607) {
        val = static_cast<int64_t>(node.value.u64);
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        char* endptr = nullptr;
        val = strtoll(node.value.s.c_str(), &endptr, 10);
        if (endptr == node.value.s.c_str() || *endptr != '\0') {
          status = RS_CLIENT_ERROR("Invalid value for MEDIUMINT column. Column: " +
              std::string(node.col->getName()));
          break;
        }
      } else {
        status = RS_CLIENT_ERROR("Invalid value for MEDIUMINT column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      if (val < -8388608 || val > 8388607) {
        status = RS_CLIENT_ERROR("Value out of range for MEDIUMINT column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      int32_t x = val;
      bin.resize(3);
      int3store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Int: {
      int64_t val;
      if (node.value.kind == Node::ParsedValue::Kind::INT64) {
        val = node.value.i64;
      } else if (node.value.kind == Node::ParsedValue::Kind::UINT64 &&
                 node.value.u64 <= static_cast<uint64_t>(INT32_MAX)) {
        val = static_cast<int64_t>(node.value.u64);
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        char* endptr = nullptr;
        val = strtoll(node.value.s.c_str(), &endptr, 10);
        if (endptr == node.value.s.c_str() || *endptr != '\0') {
          status = RS_CLIENT_ERROR("Invalid value for INT column. Column: " +
              std::string(node.col->getName()));
          break;
        }
      } else {
        status = RS_CLIENT_ERROR("Invalid value for INT column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      if (val < INT32_MIN || val > INT32_MAX) {
        status = RS_CLIENT_ERROR("Value out of range for INT column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      int32_t x = val;
      bin.resize(sizeof(int32_t));
      int4store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Bigint: {
      int64_t val;
      if (node.value.kind == Node::ParsedValue::Kind::INT64) {
        val = node.value.i64;
      } else if (node.value.kind == Node::ParsedValue::Kind::UINT64 &&
                 node.value.u64 <= static_cast<uint64_t>(INT64_MAX)) {
        val = static_cast<int64_t>(node.value.u64);
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        char* endptr = nullptr;
        errno = 0;
        val = strtoll(node.value.s.c_str(), &endptr, 10);
        if (endptr == node.value.s.c_str() || *endptr != '\0' || errno == ERANGE) {
          status = RS_CLIENT_ERROR("Invalid or out of range value for BIGINT column. Column: " +
              std::string(node.col->getName()));
          break;
        }
      } else {
        status = RS_CLIENT_ERROR("Invalid value for BIGINT column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      bin.resize(sizeof(int64_t));
      int8store(bin.data(), val);
      break;
    }
    case NdbDictionary::Column::Tinyunsigned: {
      // JSON parser may store positive values as i64, so check both
      uint64_t val;
      if (node.value.kind == Node::ParsedValue::Kind::UINT64) {
        val = node.value.u64;
      } else if (node.value.kind == Node::ParsedValue::Kind::INT64 && node.value.i64 >= 0) {
        val = static_cast<uint64_t>(node.value.i64);
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        char* endptr = nullptr;
        errno = 0;
        val = strtoull(node.value.s.c_str(), &endptr, 10);
        if (endptr == node.value.s.c_str() || *endptr != '\0' ||
            errno == ERANGE || node.value.s[0] == '-') {
          status = RS_CLIENT_ERROR("Invalid unsigned value. Column: " +
              std::string(node.col->getName()));
          break;
        }
      } else {
        status = RS_CLIENT_ERROR("Invalid unsigned value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      if (val > UINT8_MAX) {
        status = RS_CLIENT_ERROR("Value out of range for TINYINT UNSIGNED column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      uint8_t x = val;
      bin.push_back(*reinterpret_cast<uint8_t*>(&x));
      break;
    }
    case NdbDictionary::Column::Smallunsigned: {
      uint64_t val;
      if (node.value.kind == Node::ParsedValue::Kind::UINT64) {
        val = node.value.u64;
      } else if (node.value.kind == Node::ParsedValue::Kind::INT64 && node.value.i64 >= 0) {
        val = static_cast<uint64_t>(node.value.i64);
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        char* endptr = nullptr;
        errno = 0;
        val = strtoull(node.value.s.c_str(), &endptr, 10);
        if (endptr == node.value.s.c_str() || *endptr != '\0' ||
            errno == ERANGE || node.value.s[0] == '-') {
          status = RS_CLIENT_ERROR("Invalid unsigned value. Column: " +
              std::string(node.col->getName()));
          break;
        }
      } else {
        status = RS_CLIENT_ERROR("Invalid unsigned value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      if (val > UINT16_MAX) {
        status = RS_CLIENT_ERROR("Value out of range for SMALLINT UNSIGNED column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      uint16_t x = val;
      bin.resize(sizeof(uint16_t));
      int2store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Mediumunsigned: {
      // MEDIUMINT UNSIGNED is 3-byte: 0 to 16777215
      uint64_t val;
      if (node.value.kind == Node::ParsedValue::Kind::UINT64) {
        val = node.value.u64;
      } else if (node.value.kind == Node::ParsedValue::Kind::INT64 && node.value.i64 >= 0) {
        val = static_cast<uint64_t>(node.value.i64);
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        char* endptr = nullptr;
        errno = 0;
        val = strtoull(node.value.s.c_str(), &endptr, 10);
        if (endptr == node.value.s.c_str() || *endptr != '\0' ||
            errno == ERANGE || node.value.s[0] == '-') {
          status = RS_CLIENT_ERROR("Invalid unsigned value. Column: " +
              std::string(node.col->getName()));
          break;
        }
      } else {
        status = RS_CLIENT_ERROR("Invalid unsigned value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      if (val > 16777215) {
        status = RS_CLIENT_ERROR("Value out of range for MEDIUMINT UNSIGNED column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      uint32_t x = val;
      bin.resize(3);
      int3store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Unsigned: {
      uint64_t val;
      if (node.value.kind == Node::ParsedValue::Kind::UINT64) {
        val = node.value.u64;
      } else if (node.value.kind == Node::ParsedValue::Kind::INT64 && node.value.i64 >= 0) {
        val = static_cast<uint64_t>(node.value.i64);
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        char* endptr = nullptr;
        errno = 0;
        val = strtoull(node.value.s.c_str(), &endptr, 10);
        if (endptr == node.value.s.c_str() || *endptr != '\0' ||
            errno == ERANGE || node.value.s[0] == '-') {
          status = RS_CLIENT_ERROR("Invalid unsigned value. Column: " +
              std::string(node.col->getName()));
          break;
        }
      } else {
        status = RS_CLIENT_ERROR("Invalid unsigned value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      if (val > UINT32_MAX) {
        status = RS_CLIENT_ERROR("Value out of range for INT UNSIGNED column. Column: " +
            std::string(node.col->getName()));
        break;
      }
      uint32_t x = val;
      bin.resize(sizeof(uint32_t));
      int4store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Bigunsigned: {
      uint64_t val;
      if (node.value.kind == Node::ParsedValue::Kind::UINT64) {
        val = node.value.u64;
      } else if (node.value.kind == Node::ParsedValue::Kind::INT64 && node.value.i64 >= 0) {
        val = static_cast<uint64_t>(node.value.i64);
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        char* endptr = nullptr;
        errno = 0;
        val = strtoull(node.value.s.c_str(), &endptr, 10);
        if (endptr == node.value.s.c_str() || *endptr != '\0' ||
            errno == ERANGE || node.value.s[0] == '-') {
          status = RS_CLIENT_ERROR("Invalid unsigned value. Column: " +
              std::string(node.col->getName()));
          break;
        }
      } else {
        status = RS_CLIENT_ERROR("Invalid unsigned value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      bin.resize(sizeof(int64_t));
      int8store(bin.data(), val);
      break;
    }
    case NdbDictionary::Column::Varchar: {
      if (node.value.kind != Node::ParsedValue::Kind::STRING) {
        status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_VALUE_TYPE_MISMATCH)) +
            " Expecting string. Column: " + std::string(node.col->getName()));
        break;
      }
      if (node.value.s.size() > node.col->getLength()) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " The provided string is too long. Column: " +
            std::string(node.col->getName()));
        break;
      }
      assert(node.value.s.size() <= (uint16_t)(0xFF));
      uint8_t len = node.value.s.size();
      bin.resize(len + 1);
      bin[0] = len;
      memcpy(&bin[1], node.value.s.data(), len);
      break;
    }
    case NdbDictionary::Column::Longvarchar: {
      if (node.value.kind != Node::ParsedValue::Kind::STRING) {
        status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_VALUE_TYPE_MISMATCH)) +
            " Expecting string. Column: " + std::string(node.col->getName()));
        break;
      }
      if (node.value.s.size() > node.col->getLength()) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " The provided string is too long. Column: " +
            std::string(node.col->getName()));
        break;
      }
      assert(node.value.s.size() <= (uint16_t)(0xFFFF));
      uint16_t len = node.value.s.size();
      bin.resize(len + 2);
      int2store(bin.data(), len);
      memcpy(&bin[2], node.value.s.data(), len);
      break;
    }
    case NdbDictionary::Column::Timestamp2: {
      if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        uint32_t precision = node.col->getPrecision();
        MYSQL_TIME lTime;
        MYSQL_TIME_STATUS time_status;
        bool ret = str_to_datetime(node.value.s.data(), node.value.s.length(), &lTime, 0, &time_status);
        if (unlikely(ret != 0)) {
          status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) +
            std::string(" Column: ") + std::string(node.col->getName()));
          break;
        }
        time_t epoch = 0;
        struct tm time_info;
        time_info.tm_year = lTime.year - 1900;  // tm_year is years since 1900
        time_info.tm_mon = lTime.month - 1;     // tm_mon is 0-based
        time_info.tm_mday = lTime.day;
        time_info.tm_hour = lTime.hour;
        time_info.tm_min = lTime.minute;
        time_info.tm_sec = lTime.second;
        time_info.tm_isdst = -1; // Daylight saving time
        epoch = timegm(&time_info);
        // 1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
        if (unlikely(epoch <= 0 || epoch > 2147483647)) {
          status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) + std::string(" Column: ") +
            std::string(node.col->getName()));
          break;
        }
        int warnings = 0;
        my_datetime_adjust_frac(&lTime, precision, &warnings, true);
        if (unlikely(warnings != 0)) {
          status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) +
            std::string(" Column: ") + std::string(node.col->getName()));
          break;
        }
        // On Mac timeval.tv_usec is Int32 and on linux it is Int64.
        // Inorder to be compatible we cast l_time.second_part to Int32
        // This will not create problems as only six digit nanoseconds
        // are stored in Timestamp2
        my_timeval myTV{epoch, (Int32)lTime.second_part};
        // Timestamp2 size: 4 bytes + 0-3 bytes for fractional seconds
        // precision 0: 4 bytes, 1-2: 5 bytes, 3-4: 6 bytes, 5-6: 7 bytes
        int timestamp_size = 4 + (precision + 1) / 2;
        bin.resize(timestamp_size);
        my_timestamp_to_binary(&myTV, (uchar *)bin.data(), precision);
        break;
      } else {
        status = RS_CLIENT_ERROR("Timestamp2 column requires string value. Column: " +
            std::string(node.col->getName()));
      }
      break;
    }
    case NdbDictionary::Column::Decimalunsigned: {
      // Check for negative value in unsigned decimal
      if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        if (unlikely(node.value.s.find('-') != std::string::npos)) {
          status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) +
              " Expecting DECIMAL UNSIGNED. Column: " + std::string(node.col->getName()));
          break;
        }
      } else if (node.value.kind == Node::ParsedValue::Kind::INT64) {
        if (unlikely(node.value.i64 < 0)) {
          status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) +
              " Expecting DECIMAL UNSIGNED. Column: " + std::string(node.col->getName()));
          break;
        }
      } else if (node.value.kind == Node::ParsedValue::Kind::DOUBLE) {
        if (unlikely(node.value.d < 0)) {
          status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) +
              " Expecting DECIMAL UNSIGNED. Column: " + std::string(node.col->getName()));
          break;
        }
      }
      [[fallthrough]];
    }
    case NdbDictionary::Column::Decimal: {
      // Uses decimal_str2bin() which wraps MySQL's str2my_decimal() + my_decimal2binary()
      // Convert numeric values to string for decimal processing
      std::string decimalStr;
      if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        decimalStr = node.value.s;
      } else if (node.value.kind == Node::ParsedValue::Kind::DOUBLE) {
        // Use high precision for double to string conversion
        char buf[64];
        snprintf(buf, sizeof(buf), "%.15g", node.value.d);
        decimalStr = buf;
      } else if (node.value.kind == Node::ParsedValue::Kind::INT64) {
        decimalStr = std::to_string(node.value.i64);
      } else if (node.value.kind == Node::ParsedValue::Kind::UINT64) {
        decimalStr = std::to_string(node.value.u64);
      } else {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) +
            " Decimal column requires numeric or string value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      int precision = node.col->getPrecision();
      int scale = node.col->getScale();
      // Use actual column binary size based on precision and scale
      int binSize = decimal_bin_size(precision, scale);
      bin.resize(binSize);
      int err = decimal_str2bin(decimalStr.data(), decimalStr.length(),
                                precision, scale, bin.data(), bin.size());
      if (unlikely(err != E_DEC_OK && err != E_DEC_TRUNCATED)) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) +
            " Expecting Decimal with Precision: " + std::to_string(precision) +
            " and Scale: " + std::to_string(scale) +
            ". Column: " + std::string(node.col->getName()));
      }
      break;
    }
    case NdbDictionary::Column::Date: {
      // Uses my_date_to_binary() from MySQL (my_time.h)
      if (node.value.kind != Node::ParsedValue::Kind::STRING) {
        status = RS_CLIENT_ERROR("Date column requires string value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      MYSQL_TIME lTime;
      MYSQL_TIME_STATUS time_status;
      bool ret = str_to_datetime(node.value.s.data(), node.value.s.length(),
                                 &lTime, 0, &time_status);
      if (unlikely(ret != 0)) {
        status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) +
            std::string(" Column: ") + std::string(node.col->getName()));
        break;
      }
      // Date should not have time components
      if (unlikely(lTime.hour != 0 || lTime.minute != 0 || lTime.second != 0 ||
                   lTime.second_part != 0)) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " Expecting only date data. Column: " + std::string(node.col->getName()));
        break;
      }
      bin.resize(3);  // DATE is stored in 3 bytes
      my_date_to_binary(&lTime, (uchar *)bin.data());
      break;
    }
    case NdbDictionary::Column::Datetime2: {
      // Uses TIME_to_longlong_datetime_packed() + my_datetime_packed_to_binary() from MySQL
      if (node.value.kind != Node::ParsedValue::Kind::STRING) {
        status = RS_CLIENT_ERROR("Datetime2 column requires string value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      MYSQL_TIME lTime;
      MYSQL_TIME_STATUS time_status;
      bool ret = str_to_datetime(node.value.s.data(), node.value.s.length(),
                                 &lTime, 0, &time_status);
      if (unlikely(ret != 0)) {
        status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) +
            std::string(" Column: ") + std::string(node.col->getName()));
        break;
      }
      uint32_t precision = node.col->getPrecision();
      int warnings = 0;
      my_datetime_adjust_frac(&lTime, precision, &warnings, true);
      if (unlikely(warnings != 0)) {
        status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) +
            std::string(" Column: ") + std::string(node.col->getName()));
        break;
      }
      longlong numericDateTime = TIME_to_longlong_datetime_packed(lTime);
      // Datetime2 size: 5 bytes + 0-3 bytes for fractional seconds
      // precision 0: 5 bytes, 1-2: 6 bytes, 3-4: 7 bytes, 5-6: 8 bytes
      int datetime_size = 5 + (precision + 1) / 2;
      bin.resize(datetime_size);
      my_datetime_packed_to_binary(numericDateTime, (uchar *)bin.data(), precision);
      break;
    }
    case NdbDictionary::Column::Time2: {
      // Uses str_to_time() + TIME_to_longlong_time_packed() + my_time_packed_to_binary() from MySQL
      if (node.value.kind != Node::ParsedValue::Kind::STRING) {
        status = RS_CLIENT_ERROR("Time2 column requires string value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      MYSQL_TIME lTime;
      MYSQL_TIME_STATUS time_status;
      // Use str_to_time() for TIME values (not str_to_datetime)
      bool ret = str_to_time(node.value.s.data(), node.value.s.length(),
                             &lTime, &time_status, 0);
      if (unlikely(ret != 0)) {
        status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) +
            std::string(" Column: ") + std::string(node.col->getName()));
        break;
      }
      uint32_t precision = node.col->getPrecision();
      int warnings = 0;
      my_datetime_adjust_frac(&lTime, precision, &warnings, true);
      if (unlikely(warnings != 0)) {
        status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) +
            std::string(" Column: ") + std::string(node.col->getName()));
        break;
      }
      longlong numericTime = TIME_to_longlong_time_packed(lTime);
      // Time2 size: 3 bytes + 0-3 bytes for fractional seconds
      int time_size = 3 + (precision + 1) / 2;
      bin.resize(time_size);
      my_time_packed_to_binary(numericTime, (uchar *)bin.data(), precision);
      break;
    }
    case NdbDictionary::Column::Year: {
      // Year 1901-2155 stored as (year - 1900) in 1 byte
      // Matches MySQL's Field_year::store() in sql/field.cc
      int64_t yearValue = 0;
      if (node.value.kind == Node::ParsedValue::Kind::INT64) {
        yearValue = node.value.i64;
      } else if (node.value.kind == Node::ParsedValue::Kind::UINT64) {
        yearValue = static_cast<int64_t>(node.value.u64);
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        char *endptr = nullptr;
        errno = 0;
        yearValue = strtoll(node.value.s.data(), &endptr, 10);
        if (errno != 0 || endptr != node.value.s.data() + node.value.s.length()) {
          status = RS_CLIENT_ERROR("Invalid year value. Column: " +
              std::string(node.col->getName()));
          break;
        }
      } else {
        status = RS_CLIENT_ERROR("Year column requires integer or string value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      // Validate year range: 0 or 1901-2155
      if (yearValue != 0 && (yearValue < 1901 || yearValue > 2155)) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) +
            " Year value out of range [1901-2155]. Column: " +
            std::string(node.col->getName()));
        break;
      }
      bin.resize(1);
      if (yearValue == 0) {
        bin[0] = 0;
      } else {
        bin[0] = static_cast<uint8_t>(yearValue - 1900);
      }
      break;
    }
    case NdbDictionary::Column::Char: {
      // Fixed-length character string, zero-padded for NDB operations
      // Matches common.cpp SetOperationPKCol() implementation
      if (node.value.kind != Node::ParsedValue::Kind::STRING) {
        status = RS_CLIENT_ERROR("Char column requires string value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      int colMaxLen = node.col->getSizeInBytes();
      if (static_cast<int>(node.value.s.size()) > colMaxLen) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " String length exceeds column size. Column: " +
            std::string(node.col->getName()));
        break;
      }
      bin.resize(colMaxLen);
      memcpy(bin.data(), node.value.s.data(), node.value.s.size());
      // Zero-pad the remaining bytes (NDB operations expect zero-padded CHAR)
      if (node.value.s.size() < static_cast<size_t>(colMaxLen)) {
        memset(bin.data() + node.value.s.size(), 0, colMaxLen - node.value.s.size());
      }
      break;
    }
    case NdbDictionary::Column::Float: {
      // Check if this is a PK column - hash indexes on float are not supported
      if (node.col->getPrimaryKey()) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNSUPPORTED_HASH_INDEX)) +
            " Column: " + std::string(node.col->getName()));
        break;
      }
      // Float stored as 4 bytes little-endian
      // Matches MySQL's Field_float::store() using float4store()
      double dval = 0.0;
      if (node.value.kind == Node::ParsedValue::Kind::DOUBLE) {
        dval = node.value.d;
      } else if (node.value.kind == Node::ParsedValue::Kind::INT64) {
        dval = static_cast<double>(node.value.i64);
      } else if (node.value.kind == Node::ParsedValue::Kind::UINT64) {
        dval = static_cast<double>(node.value.u64);
      } else {
        status = RS_CLIENT_ERROR("Float column requires numeric value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      float fval = static_cast<float>(dval);
      bin.resize(sizeof(float));
      float4store(bin.data(), fval);
      break;
    }
    case NdbDictionary::Column::Double: {
      // Check if this is a PK column - hash indexes on double are not supported
      if (node.col->getPrimaryKey()) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNSUPPORTED_HASH_INDEX)) +
            " Column: " + std::string(node.col->getName()));
        break;
      }
      // Double stored as 8 bytes little-endian
      // Matches MySQL's Field_double::store() using float8store()
      double dval = 0.0;
      if (node.value.kind == Node::ParsedValue::Kind::DOUBLE) {
        dval = node.value.d;
      } else if (node.value.kind == Node::ParsedValue::Kind::INT64) {
        dval = static_cast<double>(node.value.i64);
      } else if (node.value.kind == Node::ParsedValue::Kind::UINT64) {
        dval = static_cast<double>(node.value.u64);
      } else {
        status = RS_CLIENT_ERROR("Double column requires numeric value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      bin.resize(sizeof(double));
      float8store(bin.data(), dval);
      break;
    }
    case NdbDictionary::Column::Binary: {
      // Fixed-length binary, input is base64 encoded
      // Matches common.cpp SetOperationPKCol() implementation
      if (node.value.kind != Node::ParsedValue::Kind::STRING) {
        status = RS_CLIENT_ERROR("Binary column requires base64 string value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      int colMaxLen = node.col->getSizeInBytes();

      // Pre-validate: base64 decoded size is at most (input_len * 3) / 4
      size_t maxDecodedLen = (node.value.s.length() * 3) / 4 + 3;  // +3 for safety
      if (unlikely(maxDecodedLen > static_cast<size_t>(colMaxLen) + 16)) {
        // Input is way too large - reject before decoding
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " Base64 input too large for column size. Column: " +
            std::string(node.col->getName()));
        break;
      }

      // Use a temporary buffer for decoding to avoid overflow
      std::vector<char> tempBuf(maxDecodedLen);
      size_t outlen = 0;
      int result = base64_decode(node.value.s.data(), node.value.s.length(),
                                 tempBuf.data(), &outlen, 0);
      if (unlikely(result == 0)) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " Error decoding base64. Column: " +
            std::string(node.col->getName()));
        break;
      } else if (unlikely(result == -1)) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " Base64 decode error: codec not available. Column: " +
            std::string(node.col->getName()));
        break;
      }
      if (unlikely(static_cast<int>(outlen) > colMaxLen)) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " Decoded data length exceeds column size. Column: " +
            std::string(node.col->getName()));
        break;
      }

      // Copy to output buffer with zero padding
      bin.resize(colMaxLen);
      memcpy(bin.data(), tempBuf.data(), outlen);
      if (outlen < static_cast<size_t>(colMaxLen)) {
        memset(bin.data() + outlen, 0, colMaxLen - outlen);
      }
      break;
    }
    case NdbDictionary::Column::Varbinary:
      [[fallthrough]];
    case NdbDictionary::Column::Longvarbinary: {
      // Variable-length binary with 1 or 2 byte length prefix
      // Input is base64 encoded
      // Matches common.cpp SetOperationPKCol() implementation
      if (node.value.kind != Node::ParsedValue::Kind::STRING) {
        status = RS_CLIENT_ERROR("Varbinary column requires base64 string value. Column: " +
            std::string(node.col->getName()));
        break;
      }
      int colDataLen = node.col->getLength();  // max data length (without prefix)
      int prefixLen = (node.col->getType() == NdbDictionary::Column::Varbinary) ? 1 : 2;

      // Pre-validate: base64 decoded size is at most (input_len * 3) / 4
      size_t maxDecodedLen = (node.value.s.length() * 3) / 4 + 3;  // +3 for safety
      if (unlikely(maxDecodedLen > static_cast<size_t>(colDataLen) + 16)) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " Base64 input too large for column size. Column: " +
            std::string(node.col->getName()));
        break;
      }

      // Use a temporary buffer for decoding to avoid overflow
      std::vector<char> tempBuf(maxDecodedLen);
      size_t outlen = 0;
      int result = base64_decode(node.value.s.data(), node.value.s.length(),
                                 tempBuf.data(), &outlen, 0);
      if (unlikely(result == 0)) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " Error decoding base64. Column: " +
            std::string(node.col->getName()));
        break;
      } else if (unlikely(result == -1)) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " Base64 decode error: codec not available. Column: " +
            std::string(node.col->getName()));
        break;
      }
      if (unlikely(static_cast<int>(outlen) > colDataLen)) {
        status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            " Decoded data length exceeds column size. Column: " +
            std::string(node.col->getName()));
        break;
      }

      // Set the length prefix and copy data
      bin.resize(prefixLen + outlen);
      if (prefixLen == 1) {
        bin[0] = static_cast<uint8_t>(outlen);
      } else {
        bin[0] = static_cast<uint8_t>(outlen & 0xFF);
        bin[1] = static_cast<uint8_t>((outlen >> 8) & 0xFF);
      }
      memcpy(bin.data() + prefixLen, tempBuf.data(), outlen);
      break;
    }
    case NdbDictionary::Column::Bit: {
      // Bit column - accept integer or base64 string
      // NDB stores BIT as 32-bit words and compares them in native (little-endian) order
      Uint32 bitLen = node.col->getLength();
      Uint32 logicalByteLen = (bitLen + 7) / 8;  // Logical bytes needed for bits
      // Use NDB's actual record size for filter operations (word-aligned)
      Uint32 recordByteLen = node.col->getSizeInBytesForRecord();

      // Initialize buffer to zero and fill with value in little-endian format
      bin.resize(recordByteLen, 0);

      if (node.value.kind == Node::ParsedValue::Kind::INT64 ||
          node.value.kind == Node::ParsedValue::Kind::UINT64) {
        // Convert integer to little-endian bytes directly into bin
        uint64_t val = (node.value.kind == Node::ParsedValue::Kind::INT64)
                           ? static_cast<uint64_t>(node.value.i64)
                           : node.value.u64;
        for (Uint32 i = 0; i < recordByteLen && i < 8; i++) {
          bin[i] = (val >> (i * 8)) & 0xFF;
        }
      } else if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        // Decode base64
        size_t maxDecodedLen = (node.value.s.length() * 3) / 4 + 3;
        std::vector<char> tempBuf(maxDecodedLen);
        size_t outlen = 0;
        int result = base64_decode(node.value.s.data(), node.value.s.length(),
                                   tempBuf.data(), &outlen, 0);
        if (unlikely(result == 0)) {
          status = RS_CLIENT_ERROR("Error decoding base64 for Bit column. Column: " +
              std::string(node.col->getName()));
          break;
        }
        if (unlikely(outlen > logicalByteLen)) {
          status = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
              " Decoded data too large for Bit column. Column: " +
              std::string(node.col->getName()));
          break;
        }
        // Copy decoded bytes directly (already in little-endian from base64)
        memcpy(bin.data(), tempBuf.data(), outlen);
      } else {
        status = RS_CLIENT_ERROR("Bit column requires integer or base64 string. Column: " +
            std::string(node.col->getName()));
        break;
      }
      break;
    }
    default: {
      status = RS_CLIENT_ERROR("Unsupported column type for filter/index. Column: " +
          std::string(node.col->getName()) + " Type: " +
          std::to_string(node.col->getType()));
      break;
    }
  }
  return status;
}

void ClearFilterColumns(std::shared_ptr<FilterNode>& node) {
  if (node == nullptr) {
    return;
  }
  if (node->type != FilterNode::Type::LOGIC) {
    node->col = nullptr;
  } else {
    for (auto& child : node->children) {
      ClearFilterColumns(child);
    }
  }
}

RS_Status BindFilterColumns(std::shared_ptr<FilterNode>& node,
                            const NdbDictionary::Table* table) {
  RS_Status status = RS_OK;
  if (node == nullptr) {
    return status;
  }
  if (node->type != FilterNode::Type::LOGIC) {
    const NdbDictionary::Column *column = table->getColumn(node->column.c_str());
    if (column == nullptr) {
      status = RS_CLIENT_404_WITH_MSG_ERROR(
        "The column used in filter doesn't exist in table");
      return status;
    }
    assert(node->col == nullptr);
    node->col = column;
  } else {
    for (auto& child : node->children) {
      status = BindFilterColumns(child, table);
      if (status.http_code != HTTP_CODE::SUCCESS) {
        break;
      }
    }
  }
  return status;
}

RS_Status CompileFilter(std::shared_ptr<FilterNode>& node,
                        NdbScanFilter* filter) {
  RS_Status status = RS_OK;
  if (node == nullptr) {
    return status;
  }
  if (node->type == FilterNode::Type::LOGIC) {
    DEB_SCAN("  filter->begin(" << node->group << ")" << std::endl);
    if (filter->begin(node->group) == -1) {
      return RS_SERVER_ERROR(
          std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)) +
          " filter->begin() failed");
    }
  } else {
    assert(node->col != nullptr);
    switch (node->type) {
      case FilterNode::Type::COMPARE:
        status = GenerateBinary(*node, node->binary);
        if (status.http_code != HTTP_CODE::SUCCESS) {
          return status;
        }
        DEB_SCAN_BLOCK(
          std::cout << "  filter->cmp(" << node->cond << ", "
                    << node->col->getAttrId() << ", ";
          std::cout << "[" << node->binary.size() << "]";
          for (auto byte : node->binary) {
            std::cout << std::hex << std::setw(2) << std::setfill('0')
                      << static_cast<int>(byte) << ' ';
          }
          std::cout << std::dec << ")" << std::endl;
        );
        if (filter->cmp(node->cond, node->col->getAttrId(),
                        node->binary.data(), node->binary.size()) == -1) {
          return RS_SERVER_ERROR(
              std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)) +
              " filter->cmp() failed for column: " +
              std::string(node->col->getName()));
        }
        break;
      case FilterNode::Type::IS_NULL:
        DEB_SCAN("  filter->isnull(" << node->col->getAttrId() << ")" << std::endl);
        if (filter->isnull(node->col->getAttrId()) == -1) {
          return RS_SERVER_ERROR(
              std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)) +
              " filter->isnull() failed for column: " +
              std::string(node->col->getName()));
        }
        break;
      case FilterNode::Type::IS_NOT_NULL:
        DEB_SCAN("  filter->isnotnull(" << node->col->getAttrId() << ")" << std::endl);
        if (filter->isnotnull(node->col->getAttrId()) == -1) {
          return RS_SERVER_ERROR(
              std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)) +
              " filter->isnotnull() failed for column: " +
              std::string(node->col->getName()));
        }
        break;
      default:
        status = RS_CLIENT_ERROR(
            "Invalid filter node type");
        return status;
    }
  }

  for (auto& child : node->children) {
    status = CompileFilter(child, filter);
    if (status.http_code != HTTP_CODE::SUCCESS) {
      return status;
    }
  }
  if (node->type == FilterNode::Type::LOGIC) {
    DEB_SCAN("  filter->end()" << std::endl);
    if (filter->end() == -1) {
      return RS_SERVER_ERROR(
          std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)) +
          " filter->end() failed");
    }
  }
  return status;
}

RS_Status BindIndexColumns(IndexScanParams& index_params,
                            const NdbDictionary::Table* table,
                            const NdbDictionary::Index* index) {
  RS_Status status = RS_OK;
  
  assert(table != nullptr);
  assert(index != nullptr);
  assert(!index_params.columns.empty());
  if (index_params.columns.size() != index->getNoOfColumns()) {
      status = RS_CLIENT_ERROR(
        "key_columns don't match the index columns");
      return status;
  }
  for (int i = 0; i < index->getNoOfColumns(); i++) {
    const NdbDictionary::Column* column = index->getColumn(i);
    if (std::string(column->getName()) != index_params.columns[i]) {
        status = RS_CLIENT_ERROR(
          "key_columns don't match the index columns");
        return status;
    }
  }

  assert(index_params.cols.empty());
  for (auto& column_name : index_params.columns) {
    const NdbDictionary::Column *column = table->getColumn(column_name.c_str());
    if (column == nullptr) {
      status = RS_CLIENT_404_WITH_MSG_ERROR(
        "The column used in index doesn't exist in table");
      return status;
    }
    index_params.cols.push_back(column);
  }
  return status;
}


typedef rapidjson::UTF8<char> RJ_Encoding;
typedef rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator> RJ_Allocator;
typedef rapidjson::GenericDocument<RJ_Encoding, RJ_Allocator,
                                   rapidjson::CrtAllocator>
    RJ_Document;
typedef rapidjson::GenericValue<RJ_Encoding, RJ_Allocator> RJ_Value;
typedef rapidjson::GenericStringBuffer<RJ_Encoding, rapidjson::CrtAllocator>
    RJ_StringBuffer;
typedef rapidjson::PrettyWriter<RJ_StringBuffer, RJ_Encoding, RJ_Encoding,
                                RJ_Allocator, 0>
    RJ_PrettyWriter;

using RJ_Writer = rapidjson::Writer<RJ_StringBuffer, RJ_Encoding, RJ_Encoding,
                                    RJ_Allocator, 0>;
void WriteColumnData2Json(RJ_Writer& writer, Uint32 attrType, const NdbDictionary::Column* col,
                          const char* binary) {
  if (unlikely(binary == nullptr)) {
    return;
  }
  Uint16 varchar_len = 0;
  Int64 value_int64 = 0;
  Uint64 value_uint64 = 0;
  float value_float = 0.0;
  double value_double = 0.0;
  const char* field = binary;
  switch(attrType) {
    case NdbDictionary::Column::Tinyint:
      value_int64 = *reinterpret_cast<const int8_t*>(field);
      writer.Int64(value_int64);
      DEB_SCAN(value_int64);
      break;
    case NdbDictionary::Column::Tinyunsigned:
      value_uint64 = *reinterpret_cast<const uint8_t*>(field);
      writer.Uint64(value_uint64);
      DEB_SCAN(value_uint64);
      break;
    case NdbDictionary::Column::Smallint:
      value_int64 = sint2korr(field);
      writer.Int64(value_int64);
      DEB_SCAN(value_int64);
      break;
    case NdbDictionary::Column::Smallunsigned:
      value_uint64 = uint2korr(field);
      writer.Uint64(value_uint64);
      DEB_SCAN(value_uint64);
      break;
    case NdbDictionary::Column::Mediumint:
      value_int64 = sint3korr(field);
      writer.Int64(value_int64);
      DEB_SCAN(value_int64);
      break;
    case NdbDictionary::Column::Mediumunsigned:
      value_uint64 = uint3korr(field);
      writer.Uint64(value_uint64);
      DEB_SCAN(value_uint64);
      break;
    case NdbDictionary::Column::Int:
      value_int64 = *reinterpret_cast<const int32_t*>(field);
      writer.Int64(value_int64);
      DEB_SCAN(value_int64);
      break;
    case NdbDictionary::Column::Unsigned:
      value_uint64 = *reinterpret_cast<const uint32_t*>(field);
      writer.Uint64(value_uint64);
      DEB_SCAN(value_uint64);
      break;
    case NdbDictionary::Column::Bigint:
      value_int64 = *reinterpret_cast<const int64_t*>(field);
      writer.Int64(value_int64);
      DEB_SCAN(value_int64);
      break;
    case NdbDictionary::Column::Bigunsigned:
      value_uint64 = *reinterpret_cast<const uint64_t*>(field);
      writer.Uint64(value_uint64);
      DEB_SCAN(value_uint64);
      break;
    case NdbDictionary::Column::Float:
      value_float = *reinterpret_cast<const float*>(field);
      writer.Double(value_float);
      DEB_SCAN(value_float);
      break;
    case NdbDictionary::Column::Double:
      value_double = *reinterpret_cast<const double*>(field);
      writer.Double(value_double);
      DEB_SCAN(value_double);
      break;
    case NdbDictionary::Column::Varchar:
      varchar_len = *reinterpret_cast<const uint8_t*>(field);
      writer.String(field + 1, varchar_len);
      DEB_SCAN("[" << varchar_len << "] "
        << std::string(field + 1, varchar_len));
      break;
    case NdbDictionary::Column::Longvarchar:
      varchar_len = *reinterpret_cast<const uint16_t*>(field);
      writer.String(field + 2, varchar_len);
      DEB_SCAN("[" << varchar_len << "] "
        << std::string(field + 2, varchar_len));
      break;
    case NdbDictionary::Column::Timestamp2: {
      ///< 4 bytes + 0-3 fraction
      uint32_t precision = col->getPrecision();
      my_timeval myTV{};
      my_timestamp_from_binary(&myTV, (const unsigned char *)field, precision);
      Int64 epochIn = myTV.m_tv_sec;
      time_t stdtime(epochIn);
      struct tm time_info;
      gmtime_r(&stdtime, &time_info);
      MYSQL_TIME lTime  = {};
      lTime.year        = time_info.tm_year + 1900;
      lTime.month       = time_info.tm_mon + 1;
      lTime.day         = time_info.tm_mday;
      lTime.hour        = time_info.tm_hour;
      lTime.minute      = time_info.tm_min;
      lTime.second      = time_info.tm_sec;
      lTime.second_part = myTV.m_tv_usec;
      lTime.time_type   = MYSQL_TIMESTAMP_DATETIME;
      char to[MAX_DATE_STRING_REP_LENGTH];
      memset(to, 0, MAX_DATE_STRING_REP_LENGTH);
      my_TIME_to_str(lTime, to, precision);
      std::string time_str(to);
      writer.String(time_str.data(), time_str.length());
      DEB_SCAN(time_str);
      break;
    }
    case NdbDictionary::Column::Decimal:
    case NdbDictionary::Column::Decimalunsigned: {
      // Uses decimal_bin2str() which wraps MySQL's bin2decimal() + decimal2string()
      char decStr[DECIMAL_MAX_STR_LEN_IN_BYTES];
      int precision = col->getPrecision();
      int scale = col->getScale();
      int binLen = col->getSizeInBytesForRecord();
      decimal_bin2str((void*)field, binLen, precision, scale, decStr, DECIMAL_MAX_STR_LEN_IN_BYTES);
      writer.String(decStr);
      DEB_SCAN(decStr);
      break;
    }
    case NdbDictionary::Column::Date: {
      ///< 3 bytes - Precision down to 1 day
      // Uses my_unpack_date() which matches MySQL's Field_newdate::get_date_internal()
      MYSQL_TIME lTime = {};
      my_unpack_date(&lTime, field);
      char to[MAX_DATE_STRING_REP_LENGTH];
      my_date_to_str(lTime, to);
      writer.String(to);
      DEB_SCAN(to);
      break;
    }
    case NdbDictionary::Column::Datetime2: {
      ///< 5 bytes plus 0-3 fraction
      // Uses my_datetime_packed_from_binary() + TIME_from_longlong_datetime_packed() from MySQL
      uint precision = col->getPrecision();
      longlong numericDate =
          my_datetime_packed_from_binary((const unsigned char *)field, precision);
      MYSQL_TIME lTime;
      TIME_from_longlong_datetime_packed(&lTime, numericDate);
      char to[MAX_DATE_STRING_REP_LENGTH];
      my_TIME_to_str(lTime, to, precision);
      writer.String(to);
      DEB_SCAN(to);
      break;
    }
    case NdbDictionary::Column::Time2: {
      ///< 3 bytes + 0-3 fraction
      // Uses my_time_packed_from_binary() + TIME_from_longlong_time_packed() from MySQL
      uint precision = col->getPrecision();
      longlong numericTime =
          my_time_packed_from_binary((const unsigned char *)field, precision);
      MYSQL_TIME lTime;
      TIME_from_longlong_time_packed(&lTime, numericTime);
      char to[MAX_DATE_STRING_REP_LENGTH];
      my_TIME_to_str(lTime, to, precision);
      writer.String(to);
      DEB_SCAN(to);
      break;
    }
    case NdbDictionary::Column::Year: {
      ///< Year 1901-2155 (1 byte)
      // Matches MySQL's Field_year::val_int() in sql/field.cc
      Int32 year = static_cast<uint8_t>(field[0]);
      if (year != 0) {
        year += 1900;
      }
      writer.Int(year);
      DEB_SCAN(year);
      break;
    }
    case NdbDictionary::Column::Char: {
      ///< Fixed-length character string (ArrayTypeFixed)
      // Data may be padded with spaces - trim trailing spaces to match MySQL behavior
      Uint32 colLen = col->getLength();
      // Find actual string length by removing trailing spaces
      while (colLen > 0 && field[colLen - 1] == ' ') {
        colLen--;
      }
      writer.String(field, colLen);
      DEB_SCAN("[" << colLen << "] " << std::string(field, colLen));
      break;
    }
    case NdbDictionary::Column::Binary: {
      ///< Fixed-length binary (ArrayTypeFixed)
      // Returns base64 encoded string
      Uint32 colLen = col->getLength();
      // Base64 output size: 4 * ceil(input_size / 3) + 1 for null terminator
      size_t base64_len = 4 * ((colLen + 2) / 3) + 1;
      char* base64_buf = new char[base64_len];
      size_t outlen = 0;
      base64_encode(field, colLen, base64_buf, &outlen, 0);
      writer.String(base64_buf, outlen);
      DEB_SCAN("[binary:" << colLen << "] base64_len=" << outlen);
      delete[] base64_buf;
      break;
    }
    case NdbDictionary::Column::Varbinary:
      [[fallthrough]];
    case NdbDictionary::Column::Longvarbinary: {
      ///< Variable-length binary with 1 or 2 byte length prefix
      // Returns base64 encoded string
      const char *dataStart = nullptr;
      Uint32 attrBytes = 0;
      const NdbDictionary::Column::ArrayType arrayType = col->getArrayType();
      switch (arrayType) {
        case NdbDictionary::Column::ArrayTypeFixed:
          dataStart = field;
          attrBytes = col->getLength();
          break;
        case NdbDictionary::Column::ArrayTypeShortVar:
          dataStart = field + 1;
          attrBytes = static_cast<Uint8>(field[0]);
          break;
        case NdbDictionary::Column::ArrayTypeMediumVar:
          dataStart = field + 2;
          attrBytes = static_cast<Uint8>(field[0]) +
                      (static_cast<Uint8>(field[1]) << 8);
          break;
        default:
          writer.String("Error: unknown array type");
          return;
      }
      // Base64 output size: 4 * ceil(input_size / 3) + 1 for null terminator
      size_t base64_len = 4 * ((attrBytes + 2) / 3) + 1;
      char* base64_buf = new char[base64_len];
      size_t outlen = 0;
      base64_encode(dataStart, attrBytes, base64_buf, &outlen, 0);
      writer.String(base64_buf, outlen);
      DEB_SCAN("[varbinary:" << attrBytes << "] base64_len=" << outlen);
      delete[] base64_buf;
      break;
    }
    case NdbDictionary::Column::Bit: {
      ///< Bit field
      // Matches pkr_operation.cpp implementation: reverse byte order, return as base64
      Uint32 bitLen = col->getLength();
      Uint32 byteLen = bitLen / 8;
      Uint32 bitsInLastByte = bitLen % 8;
      Uint32 lastMask = 0xFF;
      if (bitsInLastByte != 0) {
        byteLen += 1;
        lastMask = ((1 << bitsInLastByte) - 1);
      }
      // Reverse byte order (NDB stores in big-endian, we want little-endian for output)
      char reversed[BIT_MAX_SIZE_IN_BYTES];
      const Uint8* src = reinterpret_cast<const Uint8*>(field);
      int i = 0;
      for (int j = byteLen - 1; j >= 0; j--) {
        if (j == static_cast<int>(byteLen - 1)) {
          reversed[i++] = src[j] & lastMask;
        } else {
          reversed[i++] = src[j];
        }
      }
      // Base64 encode
      size_t base64_len = 4 * ((byteLen + 2) / 3) + 1;
      char* base64_buf = new char[base64_len];
      size_t outlen = 0;
      base64_encode(reversed, byteLen, base64_buf, &outlen, 0);
      writer.String(base64_buf, outlen);
      DEB_SCAN("[bit:" << bitLen << "] bytes=" << byteLen << " base64_len=" << outlen);
      delete[] base64_buf;
      break;
    }
    default:
      DEB_SCAN("Unexpected column type: " << attrType);
      writer.String("Unexpected column type");
      break;
  }
  return;
}

RS_Status CompileIndexRanges(const NdbTransaction* transaction,
                             NdbIndexScanOperation* operation,
                             const NdbRecord* index_rec,
                             IndexScanParams& index_params) {
  RS_Status status = RS_OK;
  assert(index_rec);
  int bound_num = index_params.ranges.size() * 2;
  Uint32 index_rec_len = NdbDictionary::getRecordRowLength(index_rec);
  assert(index_params.index_recs_buffer == nullptr);
  index_params.index_recs_buffer = new char[bound_num * index_rec_len];
  memset(index_params.index_recs_buffer, 0, bound_num * index_rec_len);
  char* buffer = index_params.index_recs_buffer;
  size_t buf_idx = 0;

  Uint32 range_no = 0;
  for (auto& range : index_params.ranges) {
    NdbIndexScanOperation::IndexBound bound;
    char* row_ptr = &buffer[buf_idx];
    DEB_SCAN(">>>LOWER bound: " << std::endl);
    if (range.lower != std::nullopt) {
      IndexBound& lower = range.lower.value();
      bound.low_inclusive = lower.inclusive;
      bound.low_key_count = lower.values.size();
      Uint32 curr_pos = 0;
      for (auto& node : lower.values) {
        node.col = index_params.cols[curr_pos];
        RS_Status status = GenerateBinary(node, node.binary);
        if (status.http_code != HTTP_CODE::SUCCESS) {
          return status;
        }
        Uint32 curr_attrId = node.col->getAttrId();
        char* field = NdbDictionary::getValuePtr(index_rec, row_ptr, curr_attrId);
        DEB_SCAN("curr_pos: " << curr_pos << ", curr_attrId: " << curr_attrId
          << ", col: " << node.col->getName()
          << ", node: " << node.value.ToString()
          << ", offset: " << field - row_ptr
          << ", binary_size: " << node.binary.size()
          << std::endl);
        if (node.value.kind == Node::ParsedValue::Kind::NULLVAL) {
          Uint32 nullbit_byte_offset = 0;
          Uint32 nullbit_bit_in_byte = 0;
          bool ret = NdbDictionary::getNullBitOffset(index_rec, curr_attrId,
              nullbit_byte_offset,
              nullbit_bit_in_byte);
          if (!ret) {
            return RS_SERVER_ERROR("Failed to get null bit offset for index column");
          }
          row_ptr[nullbit_byte_offset] |= (1 << nullbit_bit_in_byte);
        } else {
          memcpy(field, node.binary.data(), node.binary.size());
        }
        curr_pos++;
      }
      bound.low_key = row_ptr;
      DEB_SCAN("low_inclusive: " << bound.low_inclusive << std::endl);
      DEB_SCAN("low_key_count: " << bound.low_key_count << std::endl);
      DEB_SCAN_BLOCK(
        std::cout << "Lower bound binary: " << std::endl;
        for (size_t i = 0; i < index_rec_len; i++) {
          unsigned char c = static_cast<unsigned char>(bound.low_key[i]);
          std::cout << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(c) << ' ';
        }
        std::cout << std::dec << std::endl;
      );
      DEB_SCAN("<<<" << std::endl);
    } else {
      bound.low_key_count = 0;
      bound.low_key = nullptr;
      bound.low_inclusive = true;
      DEB_SCAN("Empty Lower bound" << std::endl);
    }
    buf_idx += (index_rec_len);
    row_ptr = &buffer[buf_idx];

    DEB_SCAN(">>>UPPER bound: " << std::endl);
    if (range.upper != std::nullopt) {
      IndexBound& upper = range.upper.value();
      bound.high_inclusive = upper.inclusive;
      bound.high_key_count = upper.values.size();
      Uint32 curr_pos = 0;
      for (auto& node : upper.values) {
        node.col = index_params.cols[curr_pos];
        RS_Status status = GenerateBinary(node, node.binary);
        if (status.http_code != HTTP_CODE::SUCCESS) {
          return status;
        }
        Uint32 curr_attrId = node.col->getAttrId();
        char* field = NdbDictionary::getValuePtr(index_rec, row_ptr, curr_attrId);
        DEB_SCAN("curr_pos: " << curr_pos << ", curr_attrId: " << curr_attrId
          << ", col: " << node.col->getName()
          << ", node: " << node.value.ToString()
          << ", offset: " << field - row_ptr
          << ", binary_size: " << node.binary.size()
          << std::endl);
        if (node.value.kind == Node::ParsedValue::Kind::NULLVAL) {
          Uint32 nullbit_byte_offset = 0;
          Uint32 nullbit_bit_in_byte = 0;
          bool ret = NdbDictionary::getNullBitOffset(index_rec, curr_attrId,
              nullbit_byte_offset,
              nullbit_bit_in_byte);
          if (!ret) {
            return RS_SERVER_ERROR("Failed to get null bit offset for index column");
          }
          row_ptr[nullbit_byte_offset] |= (1 << nullbit_bit_in_byte);
        } else {
          memcpy(field, node.binary.data(), node.binary.size());
        }
        curr_pos++;
      }
      bound.high_key = row_ptr;
      DEB_SCAN("high_inclusive: " << bound.high_inclusive << std::endl);
      DEB_SCAN("high_key_count: " << bound.high_key_count << std::endl);
      DEB_SCAN_BLOCK(
        std::cout << "Upper bound binary: " << std::endl;
        for (size_t i = 0; i < index_rec_len; i++) {
          unsigned char c = static_cast<unsigned char>(bound.high_key[i]);
          std::cout << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(c) << ' ';
        }
        std::cout << std::dec << std::endl;
      );
      DEB_SCAN("<<<" << std::endl);
    } else {
      bound.high_key_count = 0;
      bound.high_key = nullptr;
      bound.high_inclusive = true;
      DEB_SCAN("Empty Upper bound" << std::endl);
    }
    buf_idx += (index_rec_len);

    bound.range_no = range_no;
    range_no++;
    if (operation->setBound(index_rec, bound)) {
      RS_Status err = RS_RONDB_SERVER_ERROR(
          transaction->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to setBound.") +
          std::string(" Index: ") + index_params.name);
      return err;
    }
  }
  return status;
}

// RAII guard for NdbTransaction - ensures transaction is closed on scope exit
class TransactionGuard {
 public:
  TransactionGuard(Ndb* ndb, NdbTransaction* txn)
      : ndb_(ndb), transaction_(txn) {}

  ~TransactionGuard() {
    if (transaction_ != nullptr) {
      ndb_->closeTransaction(transaction_);
    }
  }

  // Disable copy
  TransactionGuard(const TransactionGuard&) = delete;
  TransactionGuard& operator=(const TransactionGuard&) = delete;

 private:
  Ndb* ndb_;
  NdbTransaction* transaction_;
};

RS_Status perform_scan(ScanReadParams& scan_params, Ndb* ndb_object, void* json_str_buf,
                       uint64_t* rows_fetched_out, ScanPhaseTiming* timing) {
  // Clear the JSON buffer in case this is a retry
  RJ_StringBuffer* buffer = (RJ_StringBuffer*)json_str_buf;
  buffer->Clear();

  // Timing setup
  bool timing_enabled = (timing != nullptr);
  NDB_TICKS phase_start;
  if (timing_enabled) {
    phase_start = NdbTick_getCurrentTicks();
  }

  std::string db = std::string(scan_params.path.db);
  if (unlikely(ndb_object->setDatabaseName(db.c_str()))) {
    RS_Status err = RS_CLIENT_404_WITH_MSG_ERROR(
      std::string(rdrsErrorMessage(ERROR_DB_TABLE_NOT_EXIST)) +
      std::string(" Database: ") +
      db);
    return err;
  }
  const NdbDictionary::Dictionary *dict = ndb_object->getDictionary();
  const NdbDictionary::Table* table = dict->getTable(scan_params.path.table.c_str());
  if (unlikely(table == nullptr)) {
    if (unlikely(!ndb_dict_object_missing(dict->getNdbError().code))) {
      /* Dictionary lookup failed (e.g. cluster unavailable); the table may
       * exist, so report the real NDB error instead of 404. */
      return RS_RONDB_SERVER_ERROR(dict->getNdbError(),
        std::string(rdrsErrorMessage(ERROR_TABLE_METADATA_READ_FAILED)) +
        std::string(" Database: ") + db +
        std::string(" Table: ") + scan_params.path.table);
    }
    RS_Status err = RS_CLIENT_404_WITH_MSG_ERROR(
      std::string(rdrsErrorMessage(ERROR_DB_TABLE_NOT_EXIST)) +
      std::string(" Database: ") + db +
      std::string(" Table: ") + scan_params.path.table);
    return err;
  }

  const NdbDictionary::Index* index = nullptr;

  std::vector<const NdbDictionary::Column*> read_columns;
  Bitmap read_set(table->getNoOfColumns());
  read_set.Init();
  bool read_cols_provided = true;
  if (scan_params.readColumns.empty()) {
    read_cols_provided = false;
    for (int i = 0; i < table->getNoOfColumns(); i++) {
      const NdbDictionary::Column *column = table->getColumn(i);
      if (unlikely(column == nullptr)) {
        return RS_SERVER_ERROR("Failed to get column at index " + std::to_string(i));
      }
      read_columns.push_back(column);
    }
  } else {
    for (const auto& col : scan_params.readColumns) {
      std::string col_name = std::string(col.column);
      const NdbDictionary::Column *column = table->getColumn(col_name.c_str());
      if (unlikely(column == nullptr)) {
        RS_Status err = RS_CLIENT_404_WITH_MSG_ERROR(
          std::string(rdrsErrorMessage(ERROR_COLUMN_NOT_EXIST)) +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table +
          std::string(" Column: ") + col_name);
        return err;
      }
      read_columns.push_back(column);
      read_set.SetBit(column->getAttrId());
    }
  }

  const NdbRecord* table_rec = table->getDefaultRecord();
  if (unlikely(table_rec == nullptr)) {
    RS_Status err = RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
        std::string("Failed to get NdbRecord.") +
        std::string(" Database: ") + db +
        std::string(" Table: ") + scan_params.path.table);
    return err;
  }

  const NdbRecord* index_rec = nullptr;

  // End of preparation phase
  if (timing_enabled) {
    timing->preparation_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
    phase_start = NdbTick_getCurrentTicks();
  }

  NdbTransaction *transaction = ndb_object->startTransaction();
  if (unlikely(transaction == nullptr)) {
    RS_Status err = RS_RONDB_SERVER_ERROR(
        ndb_object->getNdbError(),
        std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
        std::string("Failed to start transaction.") +
        std::string(" Database: ") + db +
        std::string(" Table: ") + scan_params.path.table);
    return err;
  }

  // Guard will automatically close transaction when function exits
  TransactionGuard txn_guard(ndb_object, transaction);

  // End of start_transaction phase
  if (timing_enabled) {
    timing->start_transaction_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
    phase_start = NdbTick_getCurrentTicks();
  }

  NdbInterpretedCode filter_code(*table_rec);
  NdbScanFilter filter(&filter_code);

  if (scan_params.filterRoot) {
    RS_Status err = BindFilterColumns(scan_params.filterRoot, table);
    if (err.http_code != HTTP_CODE::SUCCESS) {
      return err;
    }

    DEB_SCAN(std::endl);
    DEB_SCAN(">>>>>> Compiling PHYSICAL Scan Filter" << std::endl);
    if (scan_params.filterRoot->type != FilterNode::Type::LOGIC) {
      DEB_SCAN("  filter->begin(" << FilterNode::Group::AND << ")" << std::endl);
      if (unlikely(filter.begin(FilterNode::Group::AND) == -1)) {
        return RS_SERVER_ERROR(
            std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)) +
            " filter->begin() failed");
      }
    }
    err = CompileFilter(scan_params.filterRoot, &filter);
    if (err.http_code != HTTP_CODE::SUCCESS) {
      return err;
    }
    if (scan_params.filterRoot->type != FilterNode::Type::LOGIC) {
      DEB_SCAN("  filter->end()" << std::endl);
      if (unlikely(filter.end() == -1)) {
        return RS_SERVER_ERROR(
            std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)) +
            " filter->end() failed");
      }
    }
    DEB_SCAN("<<<<<<" << std::endl);
  }

  // End of compile_filter phase
  if (timing_enabled) {
    timing->compile_filter_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
    phase_start = NdbTick_getCurrentTicks();
  }

  NdbScanOperation::ScanOptions scan_options;
  scan_options.optionsPresent = 0;
	if (scan_params.limit < 384 /* NDBAPI DEF_BATCH_SIZE */) {
    scan_options.batch = scan_params.limit;
    scan_options.optionsPresent |= NdbScanOperation::ScanOptions::SO_BATCH;
  }
  Uint32 scan_flags = 0;
  RS_Status status = RS_OK;

  if (scan_params.index != std::nullopt) {
    // Index scan
    IndexScanParams& index_params = scan_params.index.value();
    index = dict->getIndex(index_params.name.c_str(), *table);
    if (unlikely(index == nullptr)) {
      const NdbError &dictErr = dict->getNdbError();
      if (dictErr.code == 241 /* Invalid schema object version */) {
        // The cached base table is stale and its id was reused by a recreated
        // sibling table, so the index would otherwise resolve to the wrong
        // table. getIndex() now reports 241 instead of returning that stale
        // index. Surface the real NDB error (rather than masking it as a plain
        // 404) so scan_read()'s HandleSchemaErrors()/retry path unloads the
        // stale schema and retries against the recreated table.
        RS_Status err = RS_RONDB_SERVER_ERROR(
          dictErr,
          std::string(rdrsErrorMessage(ERROR_INDEX_NOT_EXIST)) +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table +
          std::string(" Index: ") + index_params.name);
        return err;
      }
      RS_Status err = RS_CLIENT_404_WITH_MSG_ERROR(
        std::string(rdrsErrorMessage(ERROR_INDEX_NOT_EXIST)) +
        std::string(" Database: ") + db +
        std::string(" Table: ") + scan_params.path.table +
        std::string(" Index: ") + index_params.name);
      return err;
    }

    RS_Status err = BindIndexColumns(index_params, table, index);
    if (err.http_code != HTTP_CODE::SUCCESS) {
      return err;
    }

    if (!index_params.ranges.empty()) {
      scan_flags |= (NdbScanOperation::SF_MultiRange |
                    NdbScanOperation::SF_ReadRangeNo);
    }

    if (index_params.order != IndexScanParams::Order::NO_ORDER) {
      // SF_OrderByFull (vs SF_OrderBy) lets NDB auto-add the index key columns
      // into the result mask, so callers can list only the columns they want
      // in readColumns without hitting NDB error 4341.
      scan_flags |= NdbScanOperation::SF_OrderByFull;
      if (index_params.order == IndexScanParams::Order::DESC) {
        scan_flags |= NdbScanOperation::SF_Descending;
      }
    }
    if (scan_params.filterRoot) {
      scan_options.optionsPresent |= NdbScanOperation::ScanOptions::SO_INTERPRETED;
      scan_options.interpretedCode = &filter_code;
    }
    if (scan_flags != 0) {
      scan_options.optionsPresent |= NdbScanOperation::ScanOptions::SO_SCANFLAGS;
      scan_options.scan_flags = scan_flags;
    }

    index_rec = index->getDefaultRecord();
    NdbIndexScanOperation* operation = transaction->scanIndex(index_rec, table_rec,
        NdbOperation::LockMode::LM_CommittedRead,
        read_cols_provided ? read_set.bitmap() : nullptr,
        nullptr,
        &scan_options, sizeof(NdbScanOperation::ScanOptions));
    if (unlikely(operation == nullptr)) {
      RS_Status err = RS_RONDB_SERVER_ERROR(
          transaction->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to start scanIndex operation.") +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table +
          std::string(" Index: ") + index_params.name);
      return err;
    }

    // End of scan_index_setup phase
    if (timing_enabled) {
      timing->scan_index_setup_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
      phase_start = NdbTick_getCurrentTicks();
    }

    if (!index_params.ranges.empty()) {
      DEB_SCAN(std::endl);
      DEB_SCAN(">>>>>> Compiling PHYSICAL index ranges" << std::endl);
      RS_Status err = CompileIndexRanges(transaction, operation,
                                         index_rec, index_params);
      if (err.http_code != HTTP_CODE::SUCCESS) {
        return err;
      }
      DEB_SCAN("<<<<<<" << std::endl);
      DEB_SCAN(std::endl);

      // End of compile_index_range phase
      if (timing_enabled) {
        timing->compile_index_range_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
        phase_start = NdbTick_getCurrentTicks();
      }
    }

    if (unlikely(transaction->execute(NdbTransaction::NoCommit) != 0)) {
      RS_Status err = RS_RONDB_SERVER_ERROR(
          transaction->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to execute transaction.") +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table);
      return err;
    }

    // End of execute phase
    if (timing_enabled) {
      timing->execute_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
      phase_start = NdbTick_getCurrentTicks();
    }

    Uint32 table_rec_len = NdbDictionary::getRecordRowLength(table_rec);
    assert(scan_params.table_rec_buffer == nullptr);
    scan_params.table_rec_buffer = new char[table_rec_len];
    memset(scan_params.table_rec_buffer, 0, table_rec_len);

    const char* row_ptr = scan_params.table_rec_buffer;
    int rc = 0;
    DEB_SCAN("Rows: " << std::endl);

    RJ_StringBuffer* buffer = (RJ_StringBuffer*)json_str_buf;
    RJ_Writer writer(*buffer);
    writer.StartObject();
    writer.Key("data");
    writer.StartArray();
    uint64_t rows = 0;

    // Add json init time to json_serialize_us
    if (timing_enabled) {
      timing->json_serialize_us += NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
    }

    // Timing for fetch loop
    NDB_TICKS next_result_start, json_start;
    if (timing_enabled) {
      next_result_start = NdbTick_getCurrentTicks();
    }

    while ((rc = operation->nextResult(reinterpret_cast<const char **>(&row_ptr),
            true, false)) == 0) {
      // Accumulate nextResult time
      if (unlikely(timing_enabled)) {
        timing->next_result_us += NdbTick_Elapsed(next_result_start, NdbTick_getCurrentTicks()).microSec();
        json_start = NdbTick_getCurrentTicks();
      }

      if (rows >= scan_params.limit) {
        break;
      }
      rows++;
      writer.StartObject();
      for (auto& column : read_columns) {
        writer.Key(column->getName());

        Uint32 attrId = column->getAttrId();
        Uint32 attrType = column->getType();
        DEB_SCAN("  [" << attrId << "]: ");
        bool is_null = NdbDictionary::isNull(table_rec, row_ptr, attrId);
        if (unlikely(is_null)) {
          DEB_SCAN("NULL");
          writer.Null();
        } else {
          const char* field = NdbDictionary::getValuePtr(table_rec, row_ptr, attrId);
          WriteColumnData2Json(writer, attrType, column, field);
        }
      }
      writer.EndObject();
      DEB_SCAN(std::endl);

      // Accumulate JSON serialization time, start timing for next nextResult
      if (unlikely(timing_enabled)) {
        timing->json_serialize_us += NdbTick_Elapsed(json_start, NdbTick_getCurrentTicks()).microSec();
        next_result_start = NdbTick_getCurrentTicks();
      }
    }

    // Accumulate final nextResult time (the one that returned non-zero or hit limit)
    if (unlikely(timing_enabled)) {
      timing->next_result_us += NdbTick_Elapsed(next_result_start, NdbTick_getCurrentTicks()).microSec();
      timing->rows_fetched = rows;
      phase_start = NdbTick_getCurrentTicks();
    }

    writer.EndArray();
    writer.Key("rows");
    writer.Uint64(rows);
    writer.EndObject();

    // Add json finalize time to json_serialize_us
    if (timing_enabled) {
      timing->json_serialize_us += NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
    }

    // Output rows_fetched for Prometheus metrics
    if (rows_fetched_out != nullptr) {
      *rows_fetched_out = rows;
    }

    if (unlikely(rc == -1)) {
      status = RS_RONDB_SERVER_ERROR(
          transaction->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to read tuple.") +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table);
    }

    // Time operation->close()
    if (timing_enabled) {
      phase_start = NdbTick_getCurrentTicks();
    }
    operation->close();
    if (timing_enabled) {
      timing->close_operation_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
    }
  } else {
    // Table scan
    NdbScanOperation* operation = nullptr;
    if (scan_params.filterRoot) {
      scan_options.optionsPresent |= NdbScanOperation::ScanOptions::SO_INTERPRETED;
      scan_options.interpretedCode = &filter_code;
    }
    operation = transaction->scanTable(table_rec,
        NdbOperation::LockMode::LM_CommittedRead,
        read_cols_provided ? read_set.bitmap() : nullptr,
        &scan_options, sizeof(NdbScanOperation::ScanOptions));
    if (unlikely(operation == nullptr)) {
      RS_Status err = RS_RONDB_SERVER_ERROR(
          transaction->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to start scanTable operation.") +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table);
      return err;
    }

    // End of scan_index_setup phase (for table scan)
    if (timing_enabled) {
      timing->scan_index_setup_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
      phase_start = NdbTick_getCurrentTicks();
    }

    if (unlikely(transaction->execute(NdbTransaction::NoCommit) != 0)) {
      RS_Status err = RS_RONDB_SERVER_ERROR(
          transaction->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to execute transaction.") +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table);
      return err;
    }

    // End of execute phase (for table scan)
    if (timing_enabled) {
      timing->execute_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
      phase_start = NdbTick_getCurrentTicks();
    }

    Uint32 table_rec_len = NdbDictionary::getRecordRowLength(table_rec);
    scan_params.table_rec_buffer = new char[table_rec_len];
    memset(scan_params.table_rec_buffer, 0, table_rec_len);

    const char* row_ptr = scan_params.table_rec_buffer;
    int rc = 0;
    DEB_SCAN("Rows: " << std::endl);

    RJ_StringBuffer* buffer = (RJ_StringBuffer*)json_str_buf;
    RJ_Writer writer(*buffer);
    writer.StartObject();
    writer.Key("data");
    writer.StartArray();
    uint64_t rows = 0;

    // Add json init time to json_serialize_us
    if (timing_enabled) {
      timing->json_serialize_us += NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
    }

    // Timing for fetch loop (table scan)
    NDB_TICKS next_result_start, json_start;
    if (timing_enabled) {
      next_result_start = NdbTick_getCurrentTicks();
    }

    while ((rc = operation->nextResult(reinterpret_cast<const char **>(&row_ptr),
            true, false)) == 0) {
      // Accumulate nextResult time
      if (unlikely(timing_enabled)) {
        timing->next_result_us += NdbTick_Elapsed(next_result_start, NdbTick_getCurrentTicks()).microSec();
        json_start = NdbTick_getCurrentTicks();
      }

      if (rows >= scan_params.limit) {
        break;
      }
      rows++;
      writer.StartObject();
      for (auto& column : read_columns) {
        writer.Key(column->getName());

        Uint32 attrId = column->getAttrId();
        Uint32 attrType = column->getType();
        DEB_SCAN("  [" << attrId << "]: ");
        bool is_null = NdbDictionary::isNull(table_rec, row_ptr, attrId);
        if (unlikely(is_null)) {
          DEB_SCAN("NULL");
          writer.Null();
        } else {
          const char* field = NdbDictionary::getValuePtr(table_rec, row_ptr, attrId);
          WriteColumnData2Json(writer, attrType, column, field);
        }
      }
      writer.EndObject();
      DEB_SCAN(std::endl);

      // Accumulate JSON serialization time, start timing for next nextResult
      if (unlikely(timing_enabled)) {
        timing->json_serialize_us += NdbTick_Elapsed(json_start, NdbTick_getCurrentTicks()).microSec();
        next_result_start = NdbTick_getCurrentTicks();
      }
    }

    // Accumulate final nextResult time
    if (unlikely(timing_enabled)) {
      timing->next_result_us += NdbTick_Elapsed(next_result_start, NdbTick_getCurrentTicks()).microSec();
      timing->rows_fetched = rows;
      phase_start = NdbTick_getCurrentTicks();
    }

    writer.EndArray();
    writer.Key("rows");
    writer.Uint64(rows);
    writer.EndObject();

    // Add json finalize time to json_serialize_us
    if (timing_enabled) {
      timing->json_serialize_us += NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
    }

    // Output rows_fetched for Prometheus metrics
    if (rows_fetched_out != nullptr) {
      *rows_fetched_out = rows;
    }

    if (unlikely(rc == -1)) {
      status = RS_RONDB_SERVER_ERROR(
          transaction->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to read tuple.") +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table);
    }

    // Time operation->close()
    if (timing_enabled) {
      phase_start = NdbTick_getCurrentTicks();
    }
    operation->close();
    if (timing_enabled) {
      timing->close_operation_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
    }
  }

  return status;
}

void ResetScanParams(ScanReadParams& scan_params) {
  // Clear cached column pointers in filter tree
  ClearFilterColumns(scan_params.filterRoot);

  // Clear cached index column pointers and buffer
  if (scan_params.index != std::nullopt) {
    scan_params.index.value().cols.clear();
    if (scan_params.index.value().index_recs_buffer != nullptr) {
      delete[] scan_params.index.value().index_recs_buffer;
      scan_params.index.value().index_recs_buffer = nullptr;
    }
  }

  // Clear cached table record buffer
  if (scan_params.table_rec_buffer != nullptr) {
    delete[] scan_params.table_rec_buffer;
    scan_params.table_rec_buffer = nullptr;
  }
}

RS_Status scan_read(ScanReadParams& scan_params, unsigned int threadIndex, void* doc,
                    uint64_t* rows_fetched_out, ScanPhaseTiming* timing) {
  bool timing_enabled = (timing != nullptr);
  NDB_TICKS phase_start;

  // Time GetNdbObject
  if (timing_enabled) {
    phase_start = NdbTick_getCurrentTicks();
  }

  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetNdbObject(&ndb_object,
                                                           threadIndex);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }

  if (timing_enabled) {
    timing->get_ndb_object_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
  }

  DATA_OP_RETRY_HANDLER(
    status = perform_scan(scan_params, ndb_object, doc, rows_fetched_out, timing);
    HandleSchemaErrors(ndb_object,
                       status,
                       {std::make_tuple(std::string(scan_params.path.db),
                                        std::string(scan_params.path.table))});
    if (CanRetryOperation(status)) {
      ResetScanParams(scan_params);
    }
  )

  // Time ReturnNdbObject
  if (timing_enabled) {
    phase_start = NdbTick_getCurrentTicks();
  }

  rdrsRonDBConnectionPool->ReturnNdbObject(ndb_object,
                                           &status,
                                           threadIndex);

  if (timing_enabled) {
    timing->return_ndb_object_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
  }

  return status;
}
