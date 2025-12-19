/*
 * Copyright (C) 2024, 2025 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_SCAN_METRICS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_SCAN_METRICS_HPP_

#include <ndb_types.h>
#include <NdbTick.h>
#include <string>
#include <vector>
#include <mutex>
#include <chrono>

// Configuration - can be changed at compile time or made runtime configurable later
extern bool g_scan_timing_enabled;
extern Uint64 g_slow_scan_threshold_us;
extern Uint32 g_slow_scan_buffer_size;

// Histogram bucket boundaries (microseconds)
// Buckets: 0-100, 100-500, 500-1k, 1k-2k, 2k-5k, 5k-10k,
//          10k-20k, 20k-50k, 50k-100k, 100k-500k, 500k-1M, >1M
static constexpr Uint32 SCAN_HISTOGRAM_BUCKETS = 12;
static constexpr Uint64 SCAN_BUCKET_BOUNDS[SCAN_HISTOGRAM_BUCKETS] = {
  100, 500, 1000, 2000, 5000, 10000,
  20000, 50000, 100000, 500000, 1000000, UINT64_MAX
};

// Max threads supported for per-thread stats (fixed size to avoid dynamic allocation)
static constexpr Uint32 MAX_SCAN_THREADS = 128;

// Get bucket index for a latency value (binary search pattern - max 4 comparisons)
inline Uint32 getScanBucketIndex(Uint64 us) {
  if (us <= 5000) {
    if (us <= 500) return (us <= 100) ? 0 : 1;
    if (us <= 2000) return (us <= 1000) ? 2 : 3;
    return 4;
  } else {
    if (us <= 50000) {
      if (us <= 10000) return 5;
      return (us <= 20000) ? 6 : 7;
    }
    if (us <= 500000) return (us <= 100000) ? 8 : 9;
    return (us <= 1000000) ? 10 : 11;
  }
}

// Per-thread scan statistics (no mutex needed - each thread writes only to its own)
struct PerThreadScanStats {
  Uint64 total_count = 0;
  Uint64 sum_us = 0;      // For computing average
  Uint64 max_us = 0;      // P100
  Uint64 buckets[SCAN_HISTOGRAM_BUCKETS] = {0};

  void record(Uint64 total_us) {
    total_count++;
    sum_us += total_us;
    if (total_us > max_us) max_us = total_us;
    buckets[getScanBucketIndex(total_us)]++;
  }

  void clear() {
    total_count = 0;
    sum_us = 0;
    max_us = 0;
    for (Uint32 i = 0; i < SCAN_HISTOGRAM_BUCKETS; i++) {
      buckets[i] = 0;
    }
  }
};

// Global per-thread statistics array (fixed size, no dynamic allocation)
extern PerThreadScanStats g_per_thread_scan_stats[MAX_SCAN_THREADS];

// Aggregated statistics for reporting
struct AggregatedScanStats {
  Uint64 total_count = 0;
  Uint64 sum_us = 0;
  Uint64 max_us = 0;
  Uint64 buckets[SCAN_HISTOGRAM_BUCKETS] = {0};

  Uint64 getAvg() const {
    return (total_count > 0) ? (sum_us / total_count) : 0;
  }

  // Get percentile from histogram (returns upper bound of bucket containing percentile)
  Uint64 getPercentile(double p) const {
    if (total_count == 0) return 0;
    Uint64 target = (Uint64)(total_count * p / 100.0);
    if (target == 0) target = 1;
    Uint64 cumulative = 0;
    for (Uint32 i = 0; i < SCAN_HISTOGRAM_BUCKETS; i++) {
      cumulative += buckets[i];
      if (cumulative >= target) {
        return SCAN_BUCKET_BOUNDS[i];
      }
    }
    return max_us;
  }

  Uint64 getP80() const { return getPercentile(80.0); }
  Uint64 getP90() const { return getPercentile(90.0); }
  Uint64 getP95() const { return getPercentile(95.0); }
  Uint64 getP99() const { return getPercentile(99.0); }
  Uint64 getP100() const { return max_us; }
};

// Aggregate stats from all threads
AggregatedScanStats getAggregatedScanStats();

// Clear stats for all threads
void clearAllScanStats();

// Timing for each scan phase (in microseconds)
struct ScanPhaseTiming {
  Uint64 json_parse_us = 0;           // Step 1: json.scan_parse
  Uint64 validation_us = 0;           // Step 2: validation (db/table/columns/filter/index)
  Uint64 get_ndb_object_us = 0;       // Step 3.0: GetNdbObject from pool
  Uint64 preparation_us = 0;          // Step 3.1: setup before startTransaction
  Uint64 start_transaction_us = 0;    // Step 3.2: startTransaction()
  Uint64 compile_filter_us = 0;       // Step 3.3: BindFilterColumns + CompileFilter
  Uint64 scan_index_setup_us = 0;     // Step 3.4: scanIndex() or scanTable()
  Uint64 compile_index_range_us = 0;  // Step 3.5: CompileIndexRanges
  Uint64 execute_us = 0;              // Step 3.6: transaction->execute()
  Uint64 next_result_us = 0;          // Step 3.7a: sum of nextResult() calls
  Uint64 json_serialize_us = 0;       // Step 3.7b: all JSON work (init + serialize + finalize)
  Uint64 close_operation_us = 0;      // Step 3.8: operation->close()
  Uint64 return_ndb_object_us = 0;    // Step 3.9: ReturnNdbObject to pool
  Uint64 callback_us = 0;             // Step 4: callback()
  Uint64 total_us = 0;                // Total operation time

  // Context
  Uint64 rows_fetched = 0;
  Uint64 limit = 0;
  std::string database;
  std::string table;
  std::string index_name;
  bool has_filter = false;
  bool is_index_scan = false;
};

// Slow scan entry for the circular buffer
struct SlowScanEntry {
  ScanPhaseTiming timing;
  Uint64 timestamp_ms;    // Unix timestamp in milliseconds
  Uint32 thread_id;
};

// Thread-safe circular buffer for slow queries
class SlowScanBuffer {
public:
  explicit SlowScanBuffer(size_t capacity);
  ~SlowScanBuffer();

  // Add entry to buffer (thread-safe)
  void add(const SlowScanEntry& entry);

  // Get all entries (thread-safe, returns copy)
  std::vector<SlowScanEntry> getAll() const;

  // Clear all entries (thread-safe)
  void clear();

  // Get current count in buffer
  size_t count() const;

  // Get total count since last reset (includes overwritten entries)
  Uint64 totalCount() const;

private:
  mutable std::mutex mutex_;
  std::vector<SlowScanEntry> buffer_;
  size_t capacity_;
  size_t head_ = 0;
  size_t count_ = 0;
  Uint64 total_count_ = 0;  // Total slow scans since last reset
};

// Global slow scan buffer
extern SlowScanBuffer* g_slow_scan_buffer;

// Initialize scan metrics (call at startup)
void initScanMetrics();

// Cleanup scan metrics (call at shutdown)
void cleanupScanMetrics();

// Record slow scan if exceeds threshold
void maybeRecordSlowScan(const ScanPhaseTiming& timing, Uint32 thread_id);

// Get current timestamp in milliseconds
inline Uint64 getCurrentTimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_SCAN_METRICS_HPP_
