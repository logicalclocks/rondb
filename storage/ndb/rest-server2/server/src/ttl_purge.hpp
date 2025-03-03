/*
 * Copyright (C) 2024, 2025 Hopsworks and/or its affiliates
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_TTL_PURGE_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_TTL_PURGE_HPP_

#include <atomic>
#include <thread>
#include <string>
#include <map>

#include <NdbApi.hpp>
#include "NdbThread.h"

class TTLPurger {
 public:
  static constexpr const char* kSchemaEventName = "REPL$mysql/ndb_schema";
  static constexpr const char* kSystemDBName = "mysql";
  static constexpr const char* kSchemaTableName = "ndb_schema";
  static constexpr const char* kSchemaResTabName = "ndb_schema_result";
  static constexpr const char* kTTLPurgeNodesTabName = "ttl_purge_nodes";
  static constexpr const char* kTTLPurgeIndexName = "ttl_index";
  static constexpr int kNoEventCol = 10;
  static constexpr int kLeaseSeconds = 20;
  static constexpr Uint32 kPurgeBatchSize = 5;
  static constexpr Uint32 kPurgeBatchSizePerIncr = 5;
  static constexpr Uint32 kMaxPurgeBatchSize = 50;
  static constexpr Uint32 kPurgeThresholdTime = 1000000;  // 1 second
  static constexpr int kMaxTrxRetryTimes = 10;
  static constexpr const char* kEventColNames[kNoEventCol] = {
    "db",
    "name",
    "slock",
    "query",
    "node_id",
    "epoch",
    "id",
    "version",
    "type",
    "schema_op_id"
  };
  bool Init();
  static TTLPurger* CreateTTLPurger();
  bool Run();
  ~TTLPurger();
  void SchemaWatcherJob();
  void PurgeWorkerJob();

 private:
  TTLPurger();
  static void* _SchemaWatcherJob(void* arg);
  static void* _PurgeWorkerJob(void* arg);
  Ndb* watcher_ndb_;
  Ndb* worker_ndb_;
  std::atomic<bool> exit_;

  typedef struct TTLInfo {
    Int32 table_id;
    Uint32 ttl_sec;
    Uint32 col_no;
    Uint32 part_id = {0};                   // Only valid in local ttl cache
    Uint32 batch_size = {kPurgeBatchSize};  // Only valid in local ttl cache
  } TTLInfo;
  std::map<std::string, TTLInfo> ttl_cache_;
  std::mutex mutex_;
  std::atomic<bool> cache_updated_;
  bool UpdateLocalCache(const std::string& db,
                        const std::string& table,
                        const NdbDictionary::Table* tab);
  bool UpdateLocalCache(const std::string& db,
                        const std::string& table,
                        const std::string& new_table,
                        const NdbDictionary::Table* tab);
  static char* GetEventName(
                        NdbDictionary::Event::TableEvent event_type,
                        char* name_buf);
  bool DropDBLocalCache(const std::string& db_str,
                        NdbDictionary::Dictionary* dict);

  std::atomic<bool> purge_worker_asks_for_retry_;
  bool schema_watcher_running_;
  NdbThread* schema_watcher_;

  bool GetShard(Int32* shard, Int32* n_purge_nodes, bool update_objects);
  static Int64 GetNow(unsigned char* encoded_now, bool timestamp);
  bool UpdateLease(const unsigned char* encoded_now);
  bool IsNodeAlive(const unsigned char* encoded_last_active);
  Uint32 AdjustBatchSize(Uint32 curr_batch_size,
                         Uint32 deleted_rows,
                         Uint64 used_time);
  bool purge_worker_running_;
  NdbThread* purge_worker_;
  std::atomic<bool> purge_worker_exit_;
  std::map<Int32, std::map<Uint32, Int64>> purged_pos_;
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_TTL_PURGE_HPP_
