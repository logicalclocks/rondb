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
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <string>
#include <map>
#include <vector>
#include <cstdint>

#include <NdbApi.hpp>
#include "NdbThread.h"

// Configuration structure for TTL purge
// Lock strategy: Use shared_mutex for config access
// - Purge worker: reads config once at start of each round (shared_lock)
// - API: reads with shared_lock, writes with unique_lock
// This minimizes contention: one brief shared_lock per ~1.5s round
struct TTLPurgeConfig {
  bool enabled = true;
  Uint32 min_batch_size = 5;
  Uint32 max_batch_size = 50;
  Uint32 sleep_interval_ms = 1500;
};

// Runtime status for TTL purge
struct TTLPurgeStatus {
  enum class State {
    kStopped,     // Not started yet
    kRunning,     // Actively purging
    kPaused,      // Enabled but temporarily paused (e.g., no TTL tables)
    kDisabled,    // Disabled via config
    kError        // Error state
  };
  State state = State::kStopped;
  bool schema_watcher_running = false;
  bool purge_worker_running = false;
  std::string current_table;     // "db/table" being processed
  Uint32 current_partition = 0;
};

// Metrics for TTL purge
struct TTLPurgeMetrics {
  Uint32 tables_count = 0;
  Uint64 rows_purged_total = 0;
  Uint64 rows_purged_last_round = 0;
  Uint64 last_round_duration_ms = 0;
  Uint64 last_purge_time_epoch_ms = 0;  // Unix timestamp in milliseconds
  Uint64 rounds_completed = 0;
};

// Per-table metrics
struct TTLTableMetrics {
  std::string database;
  std::string table;
  Int32 table_id = 0;
  Uint32 ttl_sec = 0;
  Uint32 ttl_column_no = 0;
  Uint32 current_partition = 0;
  Uint32 partition_count = 0;
  Uint32 current_batch_size = 0;
  Uint64 rows_purged = 0;
  Uint64 last_purge_time_epoch_ms = 0;
};

class TTLPurger {
 public:
  static constexpr const char* kSchemaEventName = "REPL$mysql/ndb_schema";
  static constexpr const char* kSystemDBName = "mysql";
  static constexpr const char* kSchemaTableName = "ndb_schema";
  static constexpr const char* kSchemaResTabName = "ndb_schema_result";
  static constexpr const char* kTTLPurgeNodesTabName = "ttl_purge_nodes";

  static constexpr const char* kTTLPurgeCtrlTabName = "ttl_purge_ctrl";
  static constexpr const char* kPurgeCtrlKey = "ctrl_id";
  static constexpr const char* kPurgeCtrlValue = "value";
  static constexpr int kPurgeCtrlPurgeWindowId = 1;

  static constexpr const char* kTTLPurgeIndexName = "ttl_index";
  static constexpr int kNoEventCol = 10;
  static constexpr int kLeaseSeconds = 20;
  static constexpr Uint32 kPurgeThresholdTime = 1000000;  // 1 second
  static constexpr int kMaxTrxRetryTimes = 10;
  // Default config values
  static constexpr Uint32 kDefaultMinBatchSize = 5;
  static constexpr Uint32 kDefaultMaxBatchSize = 50;
  static constexpr Uint32 kDefaultSleepIntervalMs = 1500;
  static constexpr Uint32 kBatchSizePerIncr = 5;
  static constexpr Uint32 kDisabledCheckIntervalMs = 2000;
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

  // Config getters/setters (thread-safe)
  TTLPurgeConfig GetConfig() const;
  void SetConfig(const TTLPurgeConfig& config);
  bool IsEnabled() const;
  void SetEnabled(bool enabled);

  // Status and metrics getters (thread-safe)
  TTLPurgeStatus GetStatus() const;
  TTLPurgeMetrics GetMetrics() const;
  // Paginated table metrics - fixed cost regardless of total tables
  std::vector<TTLTableMetrics> GetTableMetrics(Uint32 offset, Uint32 limit,
                                               Uint32* total) const;
  // Get single table metrics, returns false if not found
  bool GetTableMetrics(const std::string& db, const std::string& table,
                       TTLTableMetrics* out) const;

 private:
  TTLPurger();
  static void* _SchemaWatcherJob(void* arg);
  static void* _PurgeWorkerJob(void* arg);
  Ndb* watcher_ndb_;
  Ndb* worker_ndb_;
  std::atomic<bool> exit_;

  typedef struct TTLInfo {
    Int32 table_id = 0;
    Uint32 ttl_sec = 0;
    Uint32 col_no = 0;
    Uint32 part_id = {0};                       // Only valid in local ttl cache
    Uint32 batch_size = {kDefaultMinBatchSize}; // Only valid in local ttl cache
    bool part_id_offset_applied = {false};      // Only valid in local ttl cache
  } TTLInfo;
  // TTL table cache (protected by mutex_)
  // Schema watcher: updates on DDL events
  // Purge worker: copies to local cache at start of round
  std::map<std::string, TTLInfo> ttl_cache_;
  std::mutex mutex_;
  std::atomic<bool> cache_updated_;

  /*
   * LOCK ORDER (to avoid deadlocks):
   *   mutex_ → table_metrics_mutex_ → metrics_mutex_
   *
   * When acquiring multiple locks, always follow this order.
   * Current usage patterns:
   *   - Schema watcher: mutex_ → table_metrics_mutex_ (in UpdateLocalCache)
   *   - Purge worker (cache copy): mutex_ → metrics_mutex_
   *   - Purge worker (UpdateRoundMetrics): table_metrics_mutex_ → metrics_mutex_
   *   - API threads: individual locks only (no nesting)
   *
   * NEVER acquire mutex_ while holding table_metrics_mutex_ or metrics_mutex_.
   */
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
  bool GetPurgeWindow(Uint32* purge_window, bool update_objects);
  static Int64 GetNow(unsigned char* encoded_now, bool timestamp);
  bool UpdateLease(const unsigned char* encoded_now);
  bool IsNodeAlive(const unsigned char* encoded_last_active);
  Uint32 AdjustBatchSize(Uint32 curr_batch_size,
                         Uint32 deleted_rows,
                         Uint64 used_time,
                         Uint32 min_batch,
                         Uint32 max_batch);
  bool purge_worker_running_;
  NdbThread* purge_worker_;
  std::atomic<bool> purge_worker_exit_;
  std::map<Int32, std::map<Uint32, Int64>> purged_pos_;

  // Config (protected by config_mutex_)
  // Use shared_mutex: multiple readers (API GET, purge worker) can proceed
  // concurrently, only writers (API PUT) need exclusive access
  mutable std::shared_mutex config_mutex_;
  TTLPurgeConfig config_;

  // Status and global metrics (protected by metrics_mutex_)
  // Separate from config to avoid contention
  // API: copy out with shared_lock, build JSON outside lock
  // Purge worker: accumulate locally, update once per round with unique_lock
  mutable std::shared_mutex metrics_mutex_;
  TTLPurgeStatus status_;
  TTLPurgeMetrics metrics_;

  // Per-table metrics (protected by table_metrics_mutex_)
  // Purge worker: accumulate locally, update once per round
  mutable std::shared_mutex table_metrics_mutex_;
  std::map<std::string, TTLTableMetrics> table_metrics_;

  // Helper methods for updating status/metrics from worker threads
  void UpdateStatus(TTLPurgeStatus::State state);
  void UpdateCurrentTable(const std::string& table, Uint32 partition);
  // Update all metrics at once at end of round (single lock acquisition)
  void UpdateRoundMetrics(Uint64 rows_purged_this_round, Uint64 duration_ms,
                          const std::map<std::string, TTLTableMetrics>& table_updates);
  // Cleanup stale table metrics
  void RemoveTableMetrics(const std::string& key);      // "db/table"
  void RemoveDBTableMetrics(const std::string& db);     // All tables in db
  // Update TTL fields in table_metrics_ (for immediate API visibility on ALTER)
  void UpdateTableMetricsTTL(const std::string& key, Uint32 ttl_sec,
                             Uint32 ttl_col_no);
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_TTL_PURGE_HPP_
