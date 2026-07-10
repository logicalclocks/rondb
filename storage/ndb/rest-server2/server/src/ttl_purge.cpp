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
#include <algorithm>
#include <utility>
#include <vector>
#include <random>
#include <set>
#include <cstdlib>

#include "src/rdrs_rondb_connection_pool.hpp"
#include "src/ttl_purge.hpp"
#include "src/status.hpp"
#include "storage/ndb/plugin/ndb_schema_dist.h"
#include "include/my_murmur3.h"
#include "include/my_systime.h"
#include "include/my_time.h"
#include "include/myisampack.h"
#include "sql/tzfile.h"
#include "NdbSleep.h"

#include <EventLogger.hpp>
extern EventLogger *g_eventLogger;
#ifdef DEBUG_EVENT
#define DEB_EVENT(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_EVENT(...) do { } while (0)
#endif

namespace {
/*
 * TTL related
 * Lock-free UTC epoch-seconds -> broken-down MYSQL_TIME; twin of
 * Dbtup::ttl_utc_sec_to_TIME in DbtupExecQuery.cpp. glibc's gmtime_r() takes
 * the process-global tzset_lock on every call (even for UTC); this uses only
 * in-tree calendar arithmetic (get_date_from_daynr), so the purge worker never
 * touches that lock. Field-for-field equivalent to MySQL's
 * sec_to_TIME(out, t, 0) (the Time_zone_offset / my_tz_OFFSET0 path).
 */
inline void ttl_utc_sec_to_TIME(time_t t, MYSQL_TIME *out) {
  /* calc_daynr(1970, 1, 1) == 719528 (days from year 0 to the Unix epoch) */
  const int64_t EPOCH_DAYNR = 719528;
  int64_t days = t / 86400;
  int32_t secs = static_cast<int32_t>(t % 86400);
  if (secs < 0) { /* t < 0: normalize into [0, 86400) */
    secs += 86400;
    days -= 1;
  }
  unsigned int year, month, day;
  get_date_from_daynr(days + EPOCH_DAYNR, &year, &month, &day);
  out->neg = false;
  out->second_part = 0;
  out->year = year;
  out->month = month;
  out->day = day;
  out->hour = secs / 3600;
  out->minute = (secs % 3600) / 60;
  out->second = secs % 60;
  out->time_zone_displacement = 0;
  out->time_type = MYSQL_TIMESTAMP_DATETIME;
}

/*
 * Partition-level shard ownership (sharded mode, i.e. mysql.ttl_purge_nodes
 * in use): shard s owns partition p of a table with hash h iff
 * (h + p) % n_nodes == s. Per table the owned counts across shards differ by
 * at most one (maximal evenness), and the hash offset rotates which shards
 * carry the remainder partitions from table to table, so the aggregate load
 * spreads evenly. Ownership is a pure function of (part_count, hash, n_nodes,
 * shard) recomputed every round, so partition reorganizations and purge-node
 * arrivals/departures re-scatter automatically; distinct shards never overlap,
 * so purge scans never contend on rows.
 *
 * The smallest owned partition id is first = (s - h) mod n_nodes; owned ids
 * are first, first + n_nodes, first + 2*n_nodes, ... below part_count.
 */
inline Uint32 FirstOwnedPartition(Uint32 table_hash, Uint32 n_nodes,
                                  Uint32 shard) {
  return (shard + n_nodes - (table_hash % n_nodes)) % n_nodes;
}

/*
 * Position *part_id on the smallest owned partition >= its current value
 * (wrapping to the first owned one when past the end). Returns false when
 * this shard owns no partition of the table (n_nodes > part_count and the
 * table's hash maps this shard past the last partition).
 */
bool AlignToOwnedPartition(Uint32* part_id, Uint32 part_count,
                           Uint32 table_hash, Uint32 n_nodes, Uint32 shard) {
  if (part_count == 0 || n_nodes == 0) {
    return false;
  }
  Uint32 first = FirstOwnedPartition(table_hash, n_nodes, shard);
  if (first >= part_count) {
    return false;
  }
  Uint32 p = *part_id;
  Uint32 cand;
  if (p <= first) {
    cand = first;
  } else {
    cand = first + ((p - first + n_nodes - 1) / n_nodes) * n_nodes;
    if (cand >= part_count) {
      cand = first;
    }
  }
  *part_id = cand;
  return true;
}

/*
 * The next owned partition strictly after part_id, wrapping to the first
 * owned one. Caller must have established ownership of >= 1 partition via
 * AlignToOwnedPartition with the same (part_count, hash, n_nodes, shard).
 */
Uint32 NextOwnedPartition(Uint32 part_id, Uint32 part_count,
                          Uint32 table_hash, Uint32 n_nodes, Uint32 shard) {
  Uint32 first = FirstOwnedPartition(table_hash, n_nodes, shard);
  Uint32 next = (part_id < first)
                    ? first
                    : first + (((part_id - first) / n_nodes) + 1) * n_nodes;
  if (next >= part_count) {
    next = first;
  }
  return next;
}

/*
 * Advance a table's rotation pointer to the next partition this node may
 * purge: the next owned one in sharded mode (shard >= 0), the next one
 * plainly otherwise. Used for the normal post-batch rotation and to back
 * off to a different partition after a lock timeout.
 */
Uint32 AdvancePartition(Uint32 part_id, Uint32 part_count, Uint32 table_hash,
                        Int32 n_nodes, Int32 shard) {
  if (shard >= 0 && n_nodes > 0) {
    return NextOwnedPartition(part_id, part_count, table_hash,
                              static_cast<Uint32>(n_nodes),
                              static_cast<Uint32>(shard));
  }
  return part_count > 0 ? (part_id + 1) % part_count : 0;
}
}  // namespace

TTLPurger::TTLPurger() :
  watcher_ndb_(nullptr), worker_ndb_(nullptr),
  exit_(false), cache_updated_(false),
  purge_worker_asks_for_retry_(false),
  schema_watcher_running_(false), schema_watcher_(nullptr),
  purge_worker_running_(false), purge_worker_(nullptr),
  purge_worker_exit_(false) {
    ttl_cache_.clear();
}

extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;
TTLPurger::~TTLPurger() {
  exit_ = true;
  if (schema_watcher_running_) {
    assert(schema_watcher_ != nullptr);
    void* status;
    NdbThread_WaitFor(schema_watcher_, &status);
    NdbThread_Destroy(&schema_watcher_);
    schema_watcher_ = nullptr;
    schema_watcher_running_ = false;
  }
  assert(purge_worker_exit_ == true && purge_worker_ == nullptr &&
         purge_worker_running_ == false);
}

bool TTLPurger::Init() {
  RS_Status status = rdrsRonDBConnectionPool->
                       GetTTLSchemaWatcherNdbObject(&watcher_ndb_);
  if (status.http_code != SUCCESS) {
    watcher_ndb_ = nullptr;
    return false;
  }

  status = rdrsRonDBConnectionPool->
                       GetTTLPurgeWorkerNdbObject(&worker_ndb_);
  if (status.http_code != SUCCESS) {
    worker_ndb_ = nullptr;
    rdrsRonDBConnectionPool->ReturnTTLSchemaWatcherNdbObject(
                               watcher_ndb_, &status);
    watcher_ndb_ = nullptr;
    return false;
  }
  return true;
}

TTLPurger* TTLPurger::CreateTTLPurger() {
  TTLPurger* ttl_purger = new TTLPurger();
  if (!ttl_purger->Init()) {
    delete ttl_purger;
    ttl_purger = nullptr;
  }
  return ttl_purger;
}

static void RandomSleep(int lower_bound, int upper_bound) {
  if (lower_bound > upper_bound) {
    std::swap(lower_bound, upper_bound);
  }
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dist(lower_bound, upper_bound);

  int sleep_duration = dist(gen);
  NdbSleep_MilliSleep(sleep_duration);
}

void* TTLPurger::_PurgeWorkerJob(void* arg) {
  errno = 0;
  TTLPurger* p_this = static_cast<TTLPurger*>(arg);
  p_this->PurgeWorkerJob();
  return nullptr;
}

static constexpr int NDB_INVALID_SCHEMA_OBJECT = 241;
void TTLPurger::SchemaWatcherJob() {
  bool init_event_succ = false;
  NdbDictionary::Dictionary* dict = nullptr;
  const NdbDictionary::Table* schema_tab = nullptr;
  const NdbDictionary::Table* schema_res_tab = nullptr;
  NdbEventOperation* ev_op = nullptr;
  NdbEventOperation* op = nullptr;
  NdbDictionary::Dictionary::List list;
  // NDB VARBINARY on-wire format: 1-byte length prefix + data.
  // setValue("message", ...) reads the length byte and then that many data
  // bytes from the buffer; a bare C string literal is mis-sized.
  const char message_buf[] = {6, 'A', 'P', 'I', '_', 'O', 'K'};
#ifdef DEBUG_EVENT
  Uint32 event_nums = 0;
#endif
  [[maybe_unused]] char event_name_buf[128];
  char slock_buf_pre[32];
  char slock_buf[32];
  // Declared before the retry/err labels so the early `goto err` sites do not
  // jump over an initialized scalar (ill-formed). Reset before each poll loop.
  Uint64 last_reconcile_us = 0;

  g_eventLogger->info("[TTL SWatcher] Started");
retry:
  init_event_succ = false;
  dict = nullptr;
  schema_tab = nullptr;
  schema_res_tab = nullptr;
  ev_op = nullptr;
  op = nullptr;
  // Init event
  do {
    if (watcher_ndb_ == nullptr) {
      RS_Status status = rdrsRonDBConnectionPool->
                           GetTTLSchemaWatcherNdbObject(&watcher_ndb_);
      if (status.http_code != SUCCESS) {
        g_eventLogger->warning("[TTL SWatcher] Failed to get schema "
                               "watcher's NdbObject. Retry...");
        watcher_ndb_ = nullptr;
        goto err;
      }
    }
    if (worker_ndb_ == nullptr) {
      RS_Status status = rdrsRonDBConnectionPool->
                           GetTTLPurgeWorkerNdbObject(&worker_ndb_);
      if (status.http_code != SUCCESS) {
        g_eventLogger->warning("[TTL SWatcher] Failed to get purge "
                               "worker's NdbObject. Retry...");
        worker_ndb_ = nullptr;
        goto err;
      }
    }

    if (watcher_ndb_->setDatabaseName(kSystemDBName) != 0) {
      g_eventLogger->warning("[TTL SWatcher] Failed to select system database: "
                            "%s, error: %d(%s). Retry...",
                             kSystemDBName,
                             watcher_ndb_->getNdbError().code,
                             watcher_ndb_->getNdbError().message);
      goto err;
    }

    dict = watcher_ndb_->getDictionary();
    dict->invalidateTable(kSchemaTableName);
    schema_tab = dict->getTable(kSchemaTableName);
    if (schema_tab == nullptr) {
      g_eventLogger->warning("[TTL SWatcher] Failed to get system table: %s"
                             ", error: %d(%s). Retry...",
                             kSchemaTableName,
                             dict->getNdbError().code,
                             dict->getNdbError().message);
      goto err;
    }
    dict->invalidateTable(kSchemaResTabName);
    schema_res_tab = dict->getTable(kSchemaResTabName);
    if (schema_res_tab == nullptr) {
      g_eventLogger->warning("[TTL SWatcher] Failed to get system table: %s"
                             ", error: %d(%s). Retry...",
                             kSchemaResTabName,
                             dict->getNdbError().code,
                             dict->getNdbError().message);
      goto err;
    }

    NdbDictionary::Event my_event(kSchemaEventName);
    my_event.setTable(*schema_tab);
    my_event.addTableEvent(NdbDictionary::Event::TE_ALL);
    my_event.mergeEvents(true);
    my_event.setReportOptions(NdbDictionary::Event::ER_ALL |
                              NdbDictionary::Event::ER_SUBSCRIBE |
                              NdbDictionary::Event::ER_DDL);
    const int n_cols = schema_tab->getNoOfColumns();
    for (int i = 0; i < n_cols; i++) {
      my_event.addEventColumn(i);
    }

    if (dict->createEvent(my_event)) {
      if (dict->getNdbError().classification != NdbError::SchemaObjectExists) {
        g_eventLogger->warning("[TTL SWatcher] Failed to create event"
                               ", error: %d(%s). Retry...",
                               dict->getNdbError().code,
                               dict->getNdbError().message);
        goto err;
      }
    }
    NdbDictionary::Event_ptr ev(dict->getEvent(kSchemaEventName));
    if (ev) {
      init_event_succ = true;
    } else {
      if (dict->getNdbError().code == NDB_INVALID_SCHEMA_OBJECT &&
          dict->dropEvent(my_event.getName(), 1)) {
        g_eventLogger->warning("[TTL SWatcher] Failed to drop the old event"
                               ", error: %d(%s). Retry...",
                               dict->getNdbError().code,
                               dict->getNdbError().message);
        goto err;
      }
      g_eventLogger->warning("[TTL SWatcher] Failed to get the event"
                             ", error: %d(%s). "
                             "Dropped the old one and retry...",
                             dict->getNdbError().code,
                             dict->getNdbError().message);
    }
  } while (!exit_ && !init_event_succ);

  // Create event operation
  if ((ev_op = watcher_ndb_->createEventOperation(kSchemaEventName))
       == nullptr) {
    g_eventLogger->warning("[TTL SWatcher] Failed to create event operation"
                           ", error: %d(%s). Retry...",
                           watcher_ndb_->getNdbError().code,
                           watcher_ndb_->getNdbError().message);
    goto err;
  }
  ev_op->mergeEvents(true);
  typedef union {
    NdbRecAttr* ra;
    NdbBlob* bh;
  } RA_BH;
  RA_BH rec_attr_pre[kNoEventCol];
  RA_BH rec_attr[kNoEventCol];
  for (int i = 0; i < kNoEventCol; i++) {
    if (i != 3) {
      rec_attr_pre[i].ra = ev_op->getPreValue(kEventColNames[i]);
      rec_attr[i].ra = ev_op->getValue(kEventColNames[i]);
    } else {
      rec_attr_pre[i].bh = ev_op->getPreBlobHandle(kEventColNames[i]);
      rec_attr[i].bh = ev_op->getBlobHandle(kEventColNames[i]);
    }
  }
  if (ev_op->execute()) {
    g_eventLogger->warning("[TTL SWatcher] Failed to execute event operation"
                           ", error: %d(%s). Retry...",
                           ev_op->getNdbError().code,
                           ev_op->getNdbError().message);
    goto err;
  }

  // The PurgeWorker requested a retry from the beginning.
  // Invalidate the previous table objects to avoid using outdated ones.
  if (!ttl_cache_.empty()) {
    for (auto iter = ttl_cache_.begin(); iter != ttl_cache_.end(); iter++) {
      auto pos = iter->first.find('/');
      if (pos != std::string::npos) {
        std::string db = iter->first.substr(0, pos);
        if (watcher_ndb_->setDatabaseName(db.c_str()) != 0) {
          g_eventLogger->warning("[TTL SWatcher-] Failed to select database: %s"
                                 ", error: %d(%s). Retry...",
                                 db.c_str(),
                                 watcher_ndb_->getNdbError().code,
                                 watcher_ndb_->getNdbError().message);
          continue;
        }
        g_eventLogger->info("[TTL SWatcher] Remove[4] TTL of table %s "
                             "in cache: [%u, %u@%u]",
                             iter->first.c_str(), iter->second.table_id,
                             iter->second.ttl_sec, iter->second.col_no);
        if (pos + 1 < iter->first.length()) {
          std::string table = iter->first.substr(pos + 1);
          dict->invalidateTable(table.c_str());
        }
      }
    }
  }

  // Fetch tables - clear both caches to avoid stale entries
  ttl_cache_.clear();
  {
    // Also clear table_metrics_ to stay in sync with ttl_cache_
    // Tables will be re-added via UpdateLocalCache during the scan below
    const std::lock_guard<std::shared_mutex> lock(table_metrics_mutex_);
    table_metrics_.clear();
  }
  // No need to reset metrics_.tables_count here;
  // GetMetrics() computes it on-the-fly from table_metrics_.size()
  list.clear();
  if (dict->listObjects(list, NdbDictionary::Object::UserTable) != 0) {
    g_eventLogger->warning("[TTL SWatcher] Failed to list objects"
                           ", error: %d(%s). Retry...",
                           dict->getNdbError().code,
                           dict->getNdbError().message);
    goto err;
  }
  for (uint i = 0; i < list.count; i++) {
    NdbDictionary::Dictionary::List::Element& elmt = list.elements[i];

    const char* db_str = elmt.database;
    assert(elmt.schema == std::string("def"));  // always "<db>/def/<name>"
    const char *table_str = elmt.name;
    if (strcmp(db_str, "mysql") == 0) {
      continue;
    }
    const NdbDictionary::Table* tab = nullptr;
    FetchResult fr = FetchTableForDiscovery(watcher_ndb_, dict, db_str,
                                            table_str, &tab);
    if (fr == FetchResult::kRestart) {
      goto err;
    }
    if (fr == FetchResult::kSkipTable) {
      // Broken/unreadable table: skip it instead of stalling the entire init
      // scan (which would loop forever and never spawn the purge worker). The
      // periodic reconcile retries it later.
      continue;
    }
    UpdateLocalCache(db_str, table_str, tab);
  }

  assert(!purge_worker_running_);
  // Set it to true to make purge worker load cache
  cache_updated_ = true;
  purge_worker_exit_ = false;
  purge_worker_ = NdbThread_Create(TTLPurger::_PurgeWorkerJob,
                                     (NDB_THREAD_ARG *)this,
                                     0, "PurgeWorker",
                                     NDB_THREAD_PRIO_MEAN);
  purge_worker_running_ = true;

  last_reconcile_us = my_micro_time();
  // Main schema_watcher_ task
  while (!exit_) {
    int res = watcher_ndb_->pollEvents(1000);  // wait for event or 1000 ms
    if (res > 0) {
      while ((op = watcher_ndb_->nextEvent())) {
        if (op->hasError()) {
          g_eventLogger->warning("[TTL SWatcher] Get an event error on "
                                 "handling event"
                                 ", error: %d(%s). Retry...",
                                 op->getNdbError().code,
                                 op->getNdbError().message);
          goto err;
        }
#ifdef DEBUG_EVENT
        event_nums++;
        DEB_EVENT("EVENT [%u]: %s, GCI = %llu",
                  event_nums,
                  GetEventName(op->getEventType(), event_name_buf),
                  op->getGCI());
#endif
        char* ptr_pre = nullptr;
        char* ptr = nullptr;
        std::string db_str_pre;
        std::string db_str;
        std::string table_str_pre;
        std::string table_str;
        std::string query_str_pre;
        std::string query_str;
        Uint32 node_id = 0;
        Uint32 type = 0;
        [[maybe_unused]] Uint32 id = 0;
        Uint32 schema_op_id = 0;
        NdbTransaction* trans = nullptr;
        NdbOperation* top = nullptr;
        bool clear_slock = false;
        bool trx_succ = false;
        Uint32 trx_failure_times = 0;
        bool cache_updated = false;
        DEB_EVENT("----------------------------");
        switch (op->getEventType()) {
          case NdbDictionary::Event::TE_CLUSTER_FAILURE:
          case NdbDictionary::Event::TE_CREATE:
          case NdbDictionary::Event::TE_ALTER:
          case NdbDictionary::Event::TE_DROP:
          case NdbDictionary::Event::TE_STOP:
          case NdbDictionary::Event::TE_INCONSISTENT:
          case NdbDictionary::Event::TE_OUT_OF_MEMORY:
            // Retry from beginning
            goto err;
          case NdbDictionary::Event::TE_DELETE:
            /*
             * A row delete on ndb_schema is the coordinator cleaning up a
             * COMPLETED schema operation -- there is nothing to parse, apply
             * or acknowledge (mysqld participants ignore these deletes too).
             * Crucially, the after-image RecAttrs are NOT populated for a
             * delete event: they still hold the PREVIOUS event's values, or
             * nothing at all right after a (re)subscribe. Falling through to
             * the parser acted on that stale/uninitialized data (type,
             * node_id, schema_op_id, db/table names): in the normal op
             * sequence the stale type happened to be SOT_CLEAR_SLOCK from
             * the coordinator's final update, which accidentally skipped the
             * ACK; but a cleanup delete without that predecessor, or as the
             * first event seen, could write a junk ndb_schema_result row,
             * rerun a cache update for a stale table (a failing getTable
             * there restarts the whole watcher), or build names from
             * uninitialized length bytes.
             */
            break;
          case NdbDictionary::Event::TE_INSERT:
          case NdbDictionary::Event::TE_UPDATE:
            for (int l = 0; l < kNoEventCol; l++) {
              ptr_pre = rec_attr_pre[l].ra->aRef();
              ptr = rec_attr[l].ra->aRef();
              switch (l) {
                case 0:
                  db_str_pre = std::string(ptr_pre + 1,
                      rec_attr_pre[l].ra->u_8_value());
                  db_str = std::string(ptr + 1,
                      rec_attr[l].ra->u_8_value());
                  DEB_EVENT("  db: %s[%u] -> %s[%u]",
                             db_str_pre.c_str(),
                             rec_attr_pre[l].ra->u_8_value(),
                             db_str.c_str(),
                             rec_attr[l].ra->u_8_value());
                  break;
                case 1:
                  table_str_pre = std::string(ptr_pre + 1,
                      rec_attr_pre[l].ra->u_8_value());
                  table_str = std::string(ptr + 1,
                      rec_attr[l].ra->u_8_value());
                  DEB_EVENT("  table: %s[%u] -> %s[%u]",
                            db_str_pre.c_str(),
                            rec_attr_pre[l].ra->u_8_value(),
                            db_str.c_str(),
                            rec_attr[l].ra->u_8_value());
                  break;
                case 2:
                  {
                  std::string info_buf;
                  memset(slock_buf_pre, 0, 32);
                  memcpy(slock_buf_pre, rec_attr_pre[l].ra->aRef(), 32);
                  info_buf = "  slock: ";
                  for (int i = 0; i < 32; i++) {
                    info_buf += std::to_string(
                                static_cast<unsigned int>(slock_buf_pre[i]));
                    info_buf += " ";
                  }
                  DEB_EVENT("%s", info_buf.c_str());
                  info_buf = "       ->";
                  memset(slock_buf, 0, 32);
                  memcpy(slock_buf, rec_attr[l].ra->aRef(), 32);
                  for (int i = 0; i < 32; i++) {
                    info_buf += std::to_string(
                                static_cast<unsigned int>(slock_buf[i]));
                    info_buf += " ";
                  }
                  DEB_EVENT("%s", info_buf.c_str());
                  }
                  break;
                case 3:
                  {
                    int blob_is_null = 0;
                    Uint64 blob_len = 0;
                    rec_attr_pre[l].bh->getNull(blob_is_null);
                    rec_attr_pre[l].bh->getLength(blob_len);
                    if (blob_is_null == 0 && blob_len != 0) {
                      Uint32 read_len = static_cast<Uint32>(blob_len);
                      query_str_pre.resize(read_len, '\0');
                      rec_attr_pre[l].bh->readData(query_str_pre.data(),
                                          read_len);
                      DEB_EVENT("  query: [%llu]%s",
                                blob_len,
                                query_str_pre.c_str());
                    } else {
                      DEB_EVENT("  query: [0]");
                    }
                    DEB_EVENT("       ->");
                    blob_is_null = 0;
                    blob_len = 0;
                    rec_attr[l].bh->getNull(blob_is_null);
                    rec_attr[l].bh->getLength(blob_len);
                    if (blob_is_null == 0 && blob_len != 0) {
                      Uint32 read_len = static_cast<Uint32>(blob_len);
                      query_str.resize(read_len, '\0');
                      rec_attr[l].bh->readData(query_str.data(), read_len);
                      DEB_EVENT("         [%llu]%s",
                                blob_len,
                                query_str.c_str());
                    } else {
                      DEB_EVENT("         [0]");
                    }
                    break;
                  }
                case 4:
                  node_id = rec_attr[l].ra->u_32_value();
                  DEB_EVENT("  node_id: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            node_id);
                  break;
                case 5:
                  DEB_EVENT("  epoch: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            rec_attr[l].ra->u_32_value());
                  break;
                case 6:
                  id = rec_attr[l].ra->u_32_value();
                  DEB_EVENT("  id: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            id);
                  break;
                case 7:
                  DEB_EVENT("  version: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            rec_attr[l].ra->u_32_value());
                  break;
                case 8:
                  // SCHEMA_OP_TYPE
                  type = rec_attr[l].ra->u_32_value();
                  DEB_EVENT("  type: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            type);
                  break;
                case 9:
                  schema_op_id = rec_attr[l].ra->u_32_value();
                  DEB_EVENT("  schema_op_id: %u -> %u",
                            rec_attr_pre[l].ra->u_32_value(),
                            schema_op_id);
                  break;
                default:
                  break;
              }
            }
            DEB_EVENT("----------------------------");

            // Check event and update local cache in nessary
            clear_slock = false;
            cache_updated = false;
            switch (type) {
              case SCHEMA_OP_TYPE::SOT_RENAME_TABLE:
                {
                  std::string new_table_str;
                  auto pos = query_str_pre.find(db_str);
                  if (pos != std::string::npos) {
                    pos += db_str.length();
                    assert(query_str_pre.at(pos) == '/');
                    pos += 1;
                    new_table_str = query_str_pre.substr(pos);
                  }
                  if (watcher_ndb_->setDatabaseName(db_str.c_str()) != 0) {
                    g_eventLogger->warning("[TTL SWatcher] Failed to select "
                                           "database: %s"
                                           ", error: %d(%s). Retry...",
                                           db_str.c_str(),
                                           watcher_ndb_->getNdbError().code,
                                           watcher_ndb_->getNdbError().message);
                    goto err;
                  }
                  dict->invalidateTable(table_str.c_str());
                  const NdbDictionary::Table* tab = dict->getTable(
                      new_table_str.c_str());
                  if (tab == nullptr) {
                    g_eventLogger->warning("[TTL SWatcher] Failed to get table:"
                                           " %s, error: %d(%s). Retry...",
                                           new_table_str.c_str(),
                                           dict->getNdbError().code,
                                           dict->getNdbError().message);
                    goto err;
                  }
                  const std::lock_guard<std::mutex> lock(mutex_);
                  cache_updated = UpdateLocalCache(db_str, table_str,
                                                    new_table_str, tab);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_DROP_TABLE:
                {
                  if (watcher_ndb_->setDatabaseName(db_str.c_str()) != 0) {
                    g_eventLogger->warning("[TTL SWatcher] Failed to select "
                                           "database: %s"
                                           ", error: %d(%s). Retry...",
                                           db_str.c_str(),
                                           watcher_ndb_->getNdbError().code,
                                           watcher_ndb_->getNdbError().message);
                    goto err;
                  }
                  dict->invalidateTable(table_str.c_str());
                  const std::lock_guard<std::mutex> lock(mutex_);
                  cache_updated = UpdateLocalCache(db_str, table_str, nullptr);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_DROP_DB:
                {
                  if (watcher_ndb_->setDatabaseName(db_str.c_str()) != 0) {
                    g_eventLogger->warning("[TTL SWatcher] Failed to select "
                                           "database: %s"
                                           ", error: %d(%s). Retry...",
                                           db_str.c_str(),
                                           watcher_ndb_->getNdbError().code,
                                           watcher_ndb_->getNdbError().message);
                    goto err;
                  }
                  const std::lock_guard<std::mutex> lock(mutex_);
                  cache_updated = DropDBLocalCache(db_str, dict);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_CREATE_TABLE:
              case SCHEMA_OP_TYPE::SOT_ALTER_TABLE_COMMIT:
              case SCHEMA_OP_TYPE::SOT_ONLINE_ALTER_TABLE_COMMIT:
                {
                  if (watcher_ndb_->setDatabaseName(db_str.c_str()) != 0) {
                    g_eventLogger->warning("[TTL SWatcher] Failed to select "
                                           "database: %s"
                                           ", error: %d(%s). Retry...",
                                           db_str.c_str(),
                                           watcher_ndb_->getNdbError().code,
                                           watcher_ndb_->getNdbError().message);
                    goto err;
                  }
                  dict->invalidateTable(table_str.c_str());
                  const NdbDictionary::Table* tab = dict->getTable(
                      table_str.c_str());
                  if (tab == nullptr) {
                    g_eventLogger->warning("[TTL SWatcher] Failed to get table:"
                                           " %s, error: %d(%s). Retry...",
                                           table_str.c_str(),
                                           dict->getNdbError().code,
                                           dict->getNdbError().message);
                    goto err;
                  }
                  const std::lock_guard<std::mutex> lock(mutex_);
                  cache_updated = UpdateLocalCache(db_str, table_str, tab);
                  break;
                }
              case SCHEMA_OP_TYPE::SOT_CLEAR_SLOCK:
                clear_slock = true;
                break;
              default:
                break;
            }

            // Only purge worker can set cache_updated_ to false;
            if (cache_updated) {
              // TODO(Zhao) Is it better to put it after
              // notify ndb_schema_result?
              cache_updated_ = true;
            }

            if (clear_slock) {
              continue;
            }

            trx_succ = false;
            trx_failure_times = 0;
            do {
              trans = watcher_ndb_->startTransaction();
              if (trans == nullptr) {
                g_eventLogger->warning("[TTL SWatcher] Failed to start "
                                       "transaction"
                                       ", error: %d(%s). Retry...",
                                       watcher_ndb_->getNdbError().code,
                                       watcher_ndb_->getNdbError().message);
                goto trx_err;
              }
              top = trans->getNdbOperation(schema_res_tab);
              if (top == nullptr) {
                g_eventLogger->warning("[TTL SWatcher] Failed to get the Ndb "
                                       "operation"
                                       ", error: %d(%s). Retry...",
                                       trans->getNdbError().code,
                                       trans->getNdbError().message);
                goto trx_err;
              }
              if (top->insertTuple() != 0 ||
                  /*Ndb_schema_result_table::COL_NODEID*/
                  top->equal("nodeid", node_id) != 0 ||
                  /*Ndb_schema_result_table::COL_SCHEMA_OP_ID*/
                  top->equal("schema_op_id", schema_op_id) != 0 ||
                  /*Ndb_schema_result_table::COL_PARTICIPANT_NODEID*/
                  top->equal("participant_nodeid",
                                watcher_ndb_->getNodeId()) != 0 ||
                  /*Ndb_schema_result_table::COL_RESULT*/
                  top->setValue("result", 0) != 0 ||
                  /*Ndb_schema_result_table::COL_MESSAGE*/
                  top->setValue("message", message_buf) != 0) {
                g_eventLogger->warning("[TTL SWatcher] Failed to insert tuple "
                                       ", error: %d(%s). Retry...",
                                       top->getNdbError().code,
                                       top->getNdbError().message);
                goto trx_err;
              }
              if (trans->execute(NdbTransaction::Commit,
                    NdbOperation::DefaultAbortOption,
                    1 /*force send*/) != 0) {
                g_eventLogger->warning("[TTL SWatcher] Failed to the execute "
                                       "transaction"
                                       ", error: %d(%s). Retry...",
                                       trans->getNdbError().code,
                                       trans->getNdbError().message);
                goto trx_err;
              } else {
                trx_succ = true;
              }
trx_err:
              if (trans != nullptr) {
                watcher_ndb_->closeTransaction(trans);
              }
              if (!trx_succ) {
                trx_failure_times++;
                if (trx_failure_times > 10) {
                  goto err;
                } else {
                  if (exit_) {
                    goto err;
                  }
                  sleep(1);
                }
              }
            } while (!trx_succ);
            break;
          default:
            break;
        }
      }
    } else if (purge_worker_asks_for_retry_) {
      g_eventLogger->warning("[TTL SWatcher] Purge worker asks for retry");
      purge_worker_asks_for_retry_ = false;
      goto err;
    } else if (res < 0) {
      g_eventLogger->warning("[TTL SWatcher] Failed to poll event "
                             ", error: %d(%s). Retry...",
                             watcher_ndb_->getNdbError().code,
                             watcher_ndb_->getNdbError().message);
      goto err;
    }

    // Periodic reconcile: discover TTL tables created out-of-band (ndb_restore,
    // NdbAPI/ClusterJ) and prune ones dropped the same way -- neither emits an
    // ndb_schema event. Timer-gated; runs on this (watcher) thread so it is
    // serialized with the event handling above.
    {
      Uint32 reconcile_sec = GetConfig().reconcile_interval_sec;
      if (reconcile_sec > 0) {
        Uint64 now_us = my_micro_time();
        if (now_us - last_reconcile_us >=
            static_cast<Uint64>(reconcile_sec) * 1000000ULL) {
          last_reconcile_us = now_us;
          if (ReconcileTables(dict) == ReconcileResult::kRestart) {
            goto err;
          }
        }
      }
    }
  }
err:
  if (ev_op != nullptr) {
    watcher_ndb_->dropEventOperation(ev_op);
  }
  ev_op = nullptr;
  op = nullptr;
  if (dict != nullptr) {
    dict->dropEvent(kSchemaEventName);
  }
  // Stop purge worker
  purge_worker_exit_ = true;
  if (purge_worker_running_) {
    assert(purge_worker_ != nullptr);
    void* status;
    NdbThread_WaitFor(purge_worker_, &status);
    NdbThread_Destroy(&purge_worker_);
    purge_worker_ = nullptr;
    purge_worker_running_ = false;
  }
  // Return 2 NdbObjects
  RS_Status status = RS_OK;
  rdrsRonDBConnectionPool->ReturnTTLSchemaWatcherNdbObject(
                             watcher_ndb_, &status);
  rdrsRonDBConnectionPool->ReturnTTLPurgeWorkerNdbObject(
                             worker_ndb_, &status);
  watcher_ndb_ = nullptr;
  worker_ndb_ = nullptr;

  if (!exit_) {
    sleep(2);
    goto retry;
  }
  g_eventLogger->info("[TTL SWatcher] Exited");
  return;
}

TTLPurger::FetchResult TTLPurger::FetchTableForDiscovery(
    Ndb* ndb, NdbDictionary::Dictionary* dict, const std::string& db,
    const std::string& table, const NdbDictionary::Table** out) {
  *out = nullptr;

  // Test-only fault injection (inert unless the env var is set): simulate a
  // permanent getTable failure for a named "db/table" to exercise the
  // skip-and-continue path deterministically. Mirrors the getenv() precedent
  // in main.cc; unset => no effect in production.
  const char* fail_tab = std::getenv("RDRS_TTL_PURGE_FAIL_GETTABLE");
  if (fail_tab != nullptr && (db + "/" + table) == fail_tab) {
    g_eventLogger->warning("[TTL SWatcher] (debug) Simulated getTable failure "
                           "for %s.%s -- skipping table",
                           db.c_str(), table.c_str());
    return FetchResult::kSkipTable;
  }

  // A per-table failure must not abort the whole scan (which would loop
  // forever on a single broken table, never spawning the purge worker).
  // Only a TemporaryError (connection/overload -- where every table would
  // fail) warrants restarting the watcher; permanent/not-found errors are
  // table-local, so skip and let the periodic reconcile retry the table.
  if (ndb->setDatabaseName(db.c_str()) != 0) {
    const NdbError& err = ndb->getNdbError();
    bool temp = err.status == NdbError::TemporaryError;
    g_eventLogger->warning("[TTL SWatcher] Failed to select database: %s"
                           ", error: %d(%s). %s",
                           db.c_str(), err.code, err.message,
                           temp ? "Retry..." : "Skipping table...");
    return temp ? FetchResult::kRestart : FetchResult::kSkipTable;
  }

  const NdbDictionary::Table* tab = dict->getTable(table.c_str());
  if (tab == nullptr) {
    const NdbError& err = dict->getNdbError();
    bool temp = err.status == NdbError::TemporaryError;
    g_eventLogger->warning("[TTL SWatcher] Failed to get table: %s"
                           ", error: %d(%s). %s",
                           table.c_str(), err.code, err.message,
                           temp ? "Retry..." : "Skipping table...");
    return temp ? FetchResult::kRestart : FetchResult::kSkipTable;
  }

  *out = tab;
  return FetchResult::kOk;
}

TTLPurger::ReconcileResult TTLPurger::ReconcileTables(
    NdbDictionary::Dictionary* dict) {
  NdbDictionary::Dictionary::List list;
  if (dict->listObjects(list, NdbDictionary::Object::UserTable) != 0) {
    const NdbError& err = dict->getNdbError();
    g_eventLogger->warning("[TTL SWatcher] Reconcile: failed to list objects"
                           ", error: %d(%s). Skipping this cycle.",
                           err.code, err.message);
    // Never prune on an incomplete listing.
    return err.status == NdbError::TemporaryError ? ReconcileResult::kRestart
                                                  : ReconcileResult::kOk;
  }

  // Names present in the cluster right now (excluding the system db), used to
  // discover new TTL tables and to prune ones dropped out-of-band.
  std::set<std::string> present;
  bool changed = false;

  for (uint i = 0; i < list.count; i++) {
    NdbDictionary::Dictionary::List::Element& elmt = list.elements[i];
    const char* db_str = elmt.database;
    const char* table_str = elmt.name;
    if (strcmp(db_str, "mysql") == 0) {
      continue;
    }
    std::string key = std::string(db_str) + "/" + table_str;
    present.insert(key);

    // Tracked tables are re-fetched every pass too: an out-of-band NdbAPI
    // alterTable can change the TTL metadata (ttl_sec/col_no) IN PLACE --
    // same table id, no ndb_schema event -- so an id compare alone would
    // leave the cache stale until an unrelated watcher restart. The fetched
    // metadata is compared below so an unchanged table stays a no-op.
    bool tracked = false;
    TTLInfo cached;
    {
      const std::lock_guard<std::mutex> lock(mutex_);
      auto it = ttl_cache_.find(key);
      if (it != ttl_cache_.end()) {
        tracked = true;
        cached = it->second;
      }
    }

    const NdbDictionary::Table* tab = nullptr;
    FetchResult fr = FetchTableForDiscovery(watcher_ndb_, dict, db_str,
                                            table_str, &tab);
    if (fr == FetchResult::kRestart) {
      return ReconcileResult::kRestart;
    }
    if (fr == FetchResult::kSkipTable) {
      continue;
    }
    if (!tracked && !tab->isTTLEnabled()) {
      continue;  // untracked non-TTL table: nothing to track
    }
    if (tracked && tab->isTTLEnabled() &&
        cached.table_id == tab->getTableId() &&
        cached.ttl_sec == tab->getTTLSec() &&
        cached.col_no == tab->getTTLColumnNo()) {
      continue;  // tracked and unchanged
    }
    // Real change: a new TTL table, an out-of-band recreate/alter (id or TTL
    // metadata differs), or a tracked table that came back non-TTL (drop).
    // UpdateLocalCache handles each case. NOTE it returns true
    // unconditionally for tracked entries, hence the no-op pre-check above:
    // the pass must stay change-sensitive, or a quiet reconcile would set
    // cache_updated_ every interval and force pointless worker reloads.
    {
      const std::lock_guard<std::mutex> lock(mutex_);
      if (UpdateLocalCache(db_str, table_str, tab)) {
        changed = true;
        g_eventLogger->info("[TTL SWatcher] Reconcile: %s out-of-band TTL "
                            "table %s",
                            tracked ? "refreshed" : "discovered",
                            key.c_str());
      }
    }
  }

  // Prune TTL tables that vanished out-of-band from the purge cache (this is
  // what stops the worker from trying to purge them). Safe only because the
  // listing above completed successfully.
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = ttl_cache_.begin(); it != ttl_cache_.end();) {
      if (present.find(it->first) == present.end()) {
        g_eventLogger->info("[TTL SWatcher] Reconcile: pruning vanished TTL "
                            "table %s from cache", it->first.c_str());
        it = ttl_cache_.erase(it);
        changed = true;
      } else {
        ++it;
      }
    }
  }
  // Sweep per-table metrics for tables no longer present. This reclaims the
  // pruned tables' entries AND any that a concurrent purge-worker round
  // re-inserted (via UpdateRoundMetrics) just after a prune -- otherwise the
  // /tables API would report a vanished table forever. Keyed by the same
  // "db/table" as ttl_cache_; only entries absent from the successful listing
  // are removed.
  {
    const std::lock_guard<std::shared_mutex> lock(table_metrics_mutex_);
    for (auto it = table_metrics_.begin(); it != table_metrics_.end();) {
      if (present.find(it->first) == present.end()) {
        it = table_metrics_.erase(it);
      } else {
        ++it;
      }
    }
  }

  if (changed) {
    cache_updated_ = true;
  }
  return ReconcileResult::kOk;
}

bool TTLPurger::UpdateLocalCache(const std::string& db,
                                 const std::string& table,
                                 const NdbDictionary::Table* tab) {
  bool updated = false;
  auto iter = ttl_cache_.find(db + "/" + table);
  if (tab != nullptr) {
    if (iter != ttl_cache_.end()) {
      if (tab->isTTLEnabled()) {
        if (iter->second.table_id != tab->getTableId()) {
          g_eventLogger->info("[TTL SWatcher] Catching the ID of the TTL table "
              "%s.%s changed from %u to %u, updating local cache",
              db.c_str(), table.c_str(),
              iter->second.table_id, tab->getTableId());
          iter->second.table_id = tab->getTableId();
        }
        g_eventLogger->info("[TTL SWatcher] Update TTL of table %s.%s "
                            "in cache: [%u, %u@%u] -> [%u, %u@%u]",
                            db.c_str(), table.c_str(),
                            iter->second.table_id, iter->second.ttl_sec,
                            iter->second.col_no,
                            tab->getTableId(), tab->getTTLSec(),
                            tab->getTTLColumnNo());
        iter->second.ttl_sec = tab->getTTLSec();
        iter->second.col_no = tab->getTTLColumnNo();
        // Also update table_metrics_ if entry exists (for immediate API visibility)
        UpdateTableMetricsTTL(db + "/" + table, tab->getTTLSec(),
                              tab->getTTLColumnNo());
      } else {
        g_eventLogger->info("[TTL SWatcher] Remove[1] TTL of table %s.%s "
                             "in cache: [%u, %u@%u]",
                             db.c_str(), table.c_str(), iter->second.table_id,
                             iter->second.ttl_sec, iter->second.col_no);
        ttl_cache_.erase(iter);
        // Clean up stale table metrics
        RemoveTableMetrics(db + "/" + table);
      }
      updated = true;
    } else {
      if (tab->isTTLEnabled()) {
        g_eventLogger->info("[TTL SWatcher] Insert TTL of table %s.%s "
                             "in cache: [%u, %u@%u]",
                             db.c_str(), table.c_str(), tab->getTableId(),
                             tab->getTTLSec(), tab->getTTLColumnNo());
        ttl_cache_.insert({db + "/" + table, {tab->getTableId(),
                           tab->getTTLSec(), tab->getTTLColumnNo()}});
        // Also add to table_metrics_ for immediate API visibility
        // (without waiting for purge worker to process it first)
        // Lock ordering: mutex_ -> table_metrics_mutex_ (consistent with existing code)
        {
          const std::lock_guard<std::shared_mutex> lock(table_metrics_mutex_);
          std::string key = db + "/" + table;
          if (table_metrics_.find(key) == table_metrics_.end()) {
            TTLTableMetrics metrics;
            metrics.database = db;
            metrics.table = table;
            metrics.table_id = tab->getTableId();
            metrics.ttl_sec = tab->getTTLSec();
            metrics.ttl_column_no = tab->getTTLColumnNo();
            table_metrics_[key] = metrics;
          }
        }
        updated = true;
      } else {
        // check mysql.ttl_purge_nodes
        // TODO(zhao): handle ttl_purge_tables as well
        if (db == kSystemDBName &&
           (table == kTTLPurgeNodesTabName || table == kTTLPurgeCtrlTabName)) {
          updated = true;
        }
      }
    }
  } else {
    if (iter != ttl_cache_.end()) {
      g_eventLogger->info("[TTL SWatcher] Remove[2] TTL of table %s.%s "
                           "in cache: [%u, %u@%u]",
                           db.c_str(), table.c_str(), iter->second.table_id,
                           iter->second.ttl_sec, iter->second.col_no);
      ttl_cache_.erase(iter);
      // Clean up stale table metrics
      RemoveTableMetrics(db + "/" + table);
      updated = true;
    } else {
      // check mysql.ttl_purge_nodes
      // TODO(zhao): handle ttl_purge_tables as well
      if (db == kSystemDBName &&
           (table == kTTLPurgeNodesTabName || table == kTTLPurgeCtrlTabName)) {
        updated = true;
      }
    }
  }
  return updated;
}

bool TTLPurger::UpdateLocalCache(const std::string& db,
                                 const std::string& table,
                                 const std::string& new_table,
                                 const NdbDictionary::Table* tab) {
  // 1. Remove old table
  bool ret = UpdateLocalCache(db, table, nullptr);
  assert(ret);
  // 2. Insert new table
  ret = UpdateLocalCache(db, new_table, tab);
  assert(ret);
  return ret;
}

char* TTLPurger::GetEventName(NdbDictionary::Event::TableEvent event_type,
                              char* name_buf) {
  switch (event_type) {
    case NdbDictionary::Event::TE_INSERT:
      strcpy(name_buf, "TE_INSERT");
      break;
    case NdbDictionary::Event::TE_DELETE:
      strcpy(name_buf, "TE_DELETE");
      break;
    case NdbDictionary::Event::TE_UPDATE:
      strcpy(name_buf, "TE_UPDATE");
      break;
    case NdbDictionary::Event::TE_SCAN:
      strcpy(name_buf, "TE_SCAN");
      break;
    case NdbDictionary::Event::TE_DROP:
      strcpy(name_buf, "TE_DROP");
      break;
    case NdbDictionary::Event::TE_ALTER:
      strcpy(name_buf, "TE_ALTER");
      break;
    case NdbDictionary::Event::TE_CREATE:
      strcpy(name_buf, "TE_CREATE");
      break;
    case NdbDictionary::Event::TE_GCP_COMPLETE:
      strcpy(name_buf, "TE_GCP_COMPLETE");
      break;
    case NdbDictionary::Event::TE_CLUSTER_FAILURE:
      strcpy(name_buf, "TE_CLUSTER_FAILURE");
      break;
    case NdbDictionary::Event::TE_STOP:
      strcpy(name_buf, "TE_STOP");
      break;
    case NdbDictionary::Event::TE_NODE_FAILURE:
      strcpy(name_buf, "TE_NODE_FAILURE");
      break;
    case NdbDictionary::Event::TE_SUBSCRIBE:
      strcpy(name_buf, "TE_SUBSCRIBE");
      break;
    case NdbDictionary::Event::TE_UNSUBSCRIBE:
      strcpy(name_buf, "TE_UNSUBSCRIBE");
      break;
    case NdbDictionary::Event::TE_EMPTY:
      strcpy(name_buf, "TE_EMPTY");
      break;
    case NdbDictionary::Event::TE_INCONSISTENT:
      strcpy(name_buf, "TE_INCONSISTENT");
      break;
    case NdbDictionary::Event::TE_OUT_OF_MEMORY:
      strcpy(name_buf, "TE_OUT_OF_MEMEORY");
      break;
    case NdbDictionary::Event::TE_ALL:
      strcpy(name_buf, "TE_ALL");
      break;
    default:
      strcpy(name_buf, "UNKNOWN");
      break;
  }
  return name_buf;
}

bool TTLPurger::DropDBLocalCache(const std::string& db_str,
                                 NdbDictionary::Dictionary* dict) {
  assert(dict != nullptr);
  bool updated = false;
  for (auto iter = ttl_cache_.begin(); iter != ttl_cache_.end();) {
    auto pos = iter->first.find('/');
    if (pos != std::string::npos) {
      std::string db = iter->first.substr(0, pos);
      if (db == db_str) {
        g_eventLogger->info("[TTL SWatcher] Remove[3] TTL of table %s "
                             "in cache: [%u, %u@%u]",
                             iter->first.c_str(), iter->second.table_id,
                             iter->second.ttl_sec, iter->second.col_no);
        if (pos + 1 < iter->first.length()) {
          std::string table = iter->first.substr(pos + 1);
          dict->invalidateTable(table.c_str());
        }
        iter = ttl_cache_.erase(iter);
        updated = true;
        continue;
      }
    }
    iter++;
  }
  // Clean up all table metrics for this database (single lock acquisition)
  if (updated) {
    RemoveDBTableMetrics(db_str);
  }
  return updated;
}

enum SpecialShardVal {
  kShardNotPurger = -2,
  kShardNosharding = -1,
  kShardFirst = 0
};

// sec_since_epoch()/mon_starts/LEAPS_THRU_END_OF were removed: their only use
// was encoding the purge high-water mark as the index-scan lower bound, which
// is now always the infimum (see PurgeWorkerJob). GetNow() handles all the
// now/threshold encoding the purger needs.

void TTLPurger::PurgeWorkerJob() {
  bool purge_trx_started = false;
  bool update_objects = false;
  std::map<std::string, TTLInfo> local_ttl_cache;
  Int32 shard = -1;
  Int32 n_purge_nodes = 0;
  unsigned char encoded_now[8] = {0};
  std::string log_buf;
  size_t pos = 0;
  std::string db_str;
  std::string table_str;
  Uint32 ttl_col_no = 0;
  int check = 0;
  int table_id = 0;
  Uint32 hash_val = 0;
  Uint32 deletedRows = 0;
  int trx_failure_times = 0;
  std::map<std::string, TTLInfo>::iterator iter;
  std::map<Int32, std::map<Uint32, Int64>>::iterator purge_tab_iter;
  std::map<Uint32, Int64>::iterator purge_part_iter;

  // Initialized up front: the round-start cache_updated_ walk dereferences
  // dict, and idle rounds (disabled / not-a-purge-node / outside the active
  // window) `continue` before the later per-round assignment -- leaving it
  // null exactly when a schema event arrives during an idle stretch.
  NdbDictionary::Dictionary* dict = worker_ndb_->getDictionary();
  const NdbDictionary::Table* ttl_tab = nullptr;
  const NdbDictionary::Index* ttl_index = nullptr;
  Uint64 start_time = 0;
  Uint64 end_time = 0;
  bool sleep_between_each_round = true;
  NdbTransaction* trans = nullptr;
  NdbScanOperation* scan_op = nullptr;
  Int64 packed_last = 0;
  unsigned char encoded_last[8] = {0};
  unsigned char encoded_threshold[8] = {0};
  unsigned char encoded_curr_purge[8] = {0};
  MYSQL_TIME datetime = {};
  Int64 packed_now = 0;
  NdbRecAttr* rec_attr[3] = {nullptr, nullptr, nullptr};
  bool use_index = false;
  Uint32 purge_window = 0;
  PurgeCtrlSettings purge_ctrl;
  // Effective daily active window for the current round: the cluster-wide
  // ttl_purge_ctrl window when valid, else the per-node config window,
  // else -1/-1 (no window). Resolved once per round by the gate below.
  Int32 eff_win_start = -1;
  Int32 eff_win_end = -1;
  // Daily active-window bookkeeping: -1 = not yet evaluated, 0 = outside,
  // 1 = inside (or no window configured). Used to log transitions once.
  int last_window_state = -1;
  // Transition-only logging for the not-a-purge-node idle state
  bool was_not_purger = false;
  // True when the previous purging round ended still saturated (some table
  // finished a full batch at max size, or the window closed mid-round):
  // leaving the window in that state means a backlog likely remains.
  bool last_round_saturated = false;
  bool window_closed_mid_round = false;

  g_eventLogger->info("[TTL PWorker] Started");
  // Reset status from potential previous kError state
  // The actual state (kRunning/kPaused/kDisabled) will be set in the main loop
  UpdateStatus(TTLPurgeStatus::State::kPaused);
  purged_pos_.clear();
  Uint64 round_start_time = 0;
  TTLPurgeConfig local_config;  // Local copy of config for this round
  // Local accumulation for metrics - updated once per round to minimize locking
  Uint64 local_rows_purged = 0;
  std::map<std::string, TTLTableMetrics> local_table_metrics;
  int pre_trx_failures = 0;  // Tracks pre-transaction errors across rounds
  do {
    // Reset local accumulators at start of each round
    local_rows_purged = 0;
    local_table_metrics.clear();
    window_closed_mid_round = false;
    // Read config once at the start of each round (single shared_lock)
    // This minimizes lock contention - only one brief lock per ~1.5s round
    local_config = GetConfig();

    // Check if purging is enabled
    if (!local_config.enabled) {
      UpdateStatus(TTLPurgeStatus::State::kDisabled);
      NdbSleep_MilliSleep(kDisabledCheckIntervalMs);
      continue;
    }

    round_start_time = my_micro_time();
    purge_trx_started = false;
    update_objects = false;
    if (cache_updated_) {
      for (iter = local_ttl_cache.begin(); iter != local_ttl_cache.end();
           iter++) {
        pos = iter->first.find('/');
        assert(pos != std::string::npos);
        db_str = iter->first.substr(0, pos);
        assert(pos + 1 < iter->first.length());
        table_str = iter->first.substr(pos + 1);
        if (worker_ndb_->setDatabaseName(db_str.c_str()) != 0) {
          g_eventLogger->warning("[TTL PWorker] Failed to select "
              "database: %s"
              ", error: %d(%s). Retry...",
              db_str.c_str(),
              worker_ndb_->getNdbError().code,
              worker_ndb_->getNdbError().message);
	  goto round_err;
        }
        /*
         * Notice:
         * Based on the comment below,
         * here we need to call invalidateIndex() for ttl_index, the reason is
         * removeCachedTable() just decrease the reference count of the table
         * object in the global list, it won't remove the object even the counter
         * becomes to 0. But invalidateIndex() will set the object to DROP and
         * remove it if the counter is 0. Since we don't call invalidateIndex
         * in main thread(it's a major different with other normal table objects),
         * so here we need to call invalidateIndex()
         */
        dict->invalidateIndex(kTTLPurgeIndexName, table_str.c_str());
        /*
         * Notice:
         * Purge thread can only call removeCachedXXX to remove its
         * thread local cached table object and decrease the reference
         * count of the global cached table object.
         * If we call invalidateTable() and following by getTable() here,
         * Purge thread will invalidate the global cached table object
         * and generate a new version of table object, which will make
         * the main thread's following invalidateTable() + getTable() gets
         * this table object, stops the chance to get the latest one from
         * data nodes.
         */
        dict->removeCachedTable(table_str.c_str());
      }
      std::map<std::string, TTLInfo> prev_local;
      prev_local.swap(local_ttl_cache);
      purged_pos_.clear();
      const std::lock_guard<std::mutex> lock(mutex_);
      local_ttl_cache = ttl_cache_;
      // Carry the worker-local rotation state forward for tables unchanged
      // across the reload (same db/table key AND same table_id). The shared
      // cache's entries always hold part_id = 0 / offset-not-applied, so
      // without this every TTL-relevant schema event would restart the
      // rotation at the first partition (nodeId offset or, in sharded mode,
      // partition 0) and starve the high partitions under TTL-DDL churn.
      // ttl_sec/col_no deliberately come from the fresh shared entry. The
      // offset flag is carried verbatim: a never-visited table (flag still
      // false) must still get its initial nodeId%partition_count offset.
      for (auto& kv : local_ttl_cache) {
        auto old_it = prev_local.find(kv.first);
        if (old_it != prev_local.end() &&
            old_it->second.table_id == kv.second.table_id) {
          kv.second.part_id = old_it->second.part_id;
          kv.second.batch_size = old_it->second.batch_size;
          kv.second.part_id_offset_applied =
              old_it->second.part_id_offset_applied;
        }
      }
      cache_updated_ = false;
      update_objects = true;
      g_eventLogger->info("[TTL PWorker] Detected cache updated, "
                           "reloaded %lu TTL tables",
                           local_ttl_cache.size());
      // No need to update metrics_.tables_count here;
      // GetMetrics() computes it on-the-fly from table_metrics_.size()
    }

    shard = kShardNosharding;
    n_purge_nodes = 0;
    if (GetShard(&shard, &n_purge_nodes, update_objects) == false) {
      g_eventLogger->info("[TTL PWorker] Failed to get shard, "
                          "error: %u(%s). Retry...",
                          watcher_ndb_->getNdbError().code,
                          watcher_ndb_->getNdbError().message);
      goto round_err;
    }
    if (shard == kShardNotPurger) {
      // Log only on the transition: this branch repeats every ~2s on every
      // non-purging node, which on a large RDRS fleet floods the logs.
      if (!was_not_purger) {
        g_eventLogger->info("[TTL PWorker] Not the configured purging node, "
                            "skip purging...");
        was_not_purger = true;
      }
      if (purge_worker_exit_) {
        break;
      }
      sleep(2);
      continue;
    }
    if (was_not_purger) {
      was_not_purger = false;
      g_eventLogger->info("[TTL PWorker] Became an active purging node");
    }

    if (GetPurgeCtrl(&purge_ctrl, update_objects) == false) {
      // GetPurgeCtrl already logged the precise failure; worker_ndb_'s
      // top-level error here may be stale and watcher_ndb_ belongs to the
      // other thread, so add no error fields
      g_eventLogger->info("[TTL PWorker] Failed to get purge control "
                          "settings. Retry...");
      goto round_err;
    }
    purge_window = purge_ctrl.purge_lag_sec;

    // Daily active-window gate (UTC). The effective window is the
    // cluster-wide one from mysql.ttl_purge_ctrl when valid, else the
    // per-node config window (.TTLPurge.ActiveWindow / REST config API),
    // else none. When now is outside the effective window, idle this round
    // WITHOUT refreshing the lease (the gate sits above UpdateLease on
    // purpose): like a disabled node, an out-of-window node ages out of the
    // active purge set after kLeaseSeconds, so in sharded mode its owned
    // partitions redistribute to in-window nodes instead of going unpurged
    // -- essential when per-node windows differ. At window open, the first
    // round may see stale peer leases (GetShard always counts self), giving
    // a transient 1-2 rounds of ownership overlap until every node has
    // re-leased; overlap is benign (idempotent expired-row deletes plus the
    // 296/499 partition back-off). Hard-stop semantics: a backlog never
    // extends purging past the window close (see the matching mid-round
    // check in the table loop).
    {
      const char* win_source = "";
      eff_win_start = -1;
      eff_win_end = -1;
      if (HasActiveWindow(purge_ctrl.win_start_min, purge_ctrl.win_end_min)) {
        eff_win_start = purge_ctrl.win_start_min;
        eff_win_end = purge_ctrl.win_end_min;
        win_source = "ttl_purge_ctrl";
      } else if (HasActiveWindow(local_config.active_window_start_min,
                                 local_config.active_window_end_min)) {
        eff_win_start = local_config.active_window_start_min;
        eff_win_end = local_config.active_window_end_min;
        win_source = "config";
      }
      UpdateActiveWindowStatus(eff_win_start, eff_win_end, win_source);
      bool in_window =
          eff_win_start < 0 ||
          InActiveWindow(eff_win_start, eff_win_end,
                         (time_t)(my_micro_time() / 1000000));
      if (!in_window) {
        if (last_window_state != 0) {
          g_eventLogger->info("[TTL PWorker] Outside the purge active window "
                              "[%02d:%02d-%02d:%02d) UTC (from %s), "
                              "purging paused",
                              eff_win_start / 60, eff_win_start % 60,
                              eff_win_end / 60, eff_win_end % 60,
                              win_source);
          if (last_round_saturated) {
            g_eventLogger->warning(
                "[TTL PWorker] The purge active window closed while purging "
                "was still saturated; an expired-row backlog likely remains. "
                "Consider a wider window or more purge nodes.");
          }
        }
        last_window_state = 0;
        UpdateStatus(TTLPurgeStatus::State::kOutsideWindow);
        UpdateCurrentTable("", 0);
        if (purge_worker_exit_) {
          break;
        }
        NdbSleep_MilliSleep(kDisabledCheckIntervalMs);
        continue;
      }
      if (eff_win_start >= 0 && last_window_state == 0) {
        g_eventLogger->info("[TTL PWorker] Entered the purge active window "
                            "[%02d:%02d-%02d:%02d) UTC (from %s), "
                            "purging resumed",
                            eff_win_start / 60, eff_win_start % 60,
                            eff_win_end / 60, eff_win_end % 60,
                            win_source);
      }
      last_window_state = 1;
    }

    GetNow(encoded_now, false);
    if (shard >= kShardFirst && !UpdateLease(encoded_now)) {
      g_eventLogger->warning("[TTL PWorker] Failed to update the lease");
      goto round_err;
    }

    if (local_ttl_cache.empty()) {
      // No TTL table is found
      UpdateStatus(TTLPurgeStatus::State::kPaused);
      UpdateCurrentTable("", 0);
      if (purge_worker_exit_) {
        break;
      }
      sleep(2);
      continue;
    }

    UpdateStatus(TTLPurgeStatus::State::kRunning);
    sleep_between_each_round = true;
    dict = worker_ndb_->getDictionary();
    for (iter = local_ttl_cache.begin(); iter != local_ttl_cache.end();) {
      if (purge_worker_exit_) {
        break;
      }
      // Honor a disable observed mid-round. The worker is the sole writer of
      // status_, so it reports kDisabled exactly when it actually stops purging
      // -- never earlier (the config setter must not write status while we are
      // still working) and never as late as the next round boundary (which can
      // be a long round on a big cluster). Re-checked per table, since the
      // purger does ~one batch per table per round.
      if (!IsEnabled()) {
        UpdateStatus(TTLPurgeStatus::State::kDisabled);
        break;
      }
      // Hard stop at active-window close, even mid-round: a backlog must
      // never extend purging into the hours the window is meant to protect.
      // Checked before EACH table's batch, so the overrun past close is
      // bounded by one already-open scan transaction: up to the table's
      // current batch_size rows plus NDB timeout behavior. Interrupting the
      // in-flight scan instead would roll back its deletes -- same data-node
      // work, zero rows purged -- for a seconds-scale gain against
      // multi-hour windows. Pure local arithmetic (no NDB access); the next
      // round's gate logs the transition and publishes kOutsideWindow.
      if (eff_win_start >= 0 &&
          !InActiveWindow(eff_win_start, eff_win_end,
                          (time_t)(my_micro_time() / 1000000))) {
        window_closed_mid_round = true;  // tables were still pending
        UpdateStatus(TTLPurgeStatus::State::kOutsideWindow);
        break;
      }
      purge_trx_started = false;
      {
        GetNow(encoded_now, false);
        if (shard >= kShardFirst && !UpdateLease(encoded_now)) {
          g_eventLogger->warning("[TTL PWorker] Failed to update the lease[2]");
	  goto table_err;
        }
      }
      // Note: cache_updated_ is only checked at round start (line 1067).
      // Checking here caused round starvation: tables later in iteration
      // order would never be processed if schema events kept arriving.

      start_time = my_micro_time();

      log_buf = "[TTL PWorker] Processing " + iter->first + ": ";

      pos = iter->first.find('/');
      assert(pos != std::string::npos);
      db_str = iter->first.substr(0, pos);
      assert(pos + 1 < iter->first.length());
      table_str = iter->first.substr(pos + 1);
      ttl_col_no = iter->second.col_no;
      check = 0;
      deletedRows = 0;
      trx_failure_times = 0;

      if (worker_ndb_->setDatabaseName(db_str.c_str()) != 0) {
        g_eventLogger->warning("[TTL PWorker] Failed to select "
            "database: %s"
            ", error: %d(%s). Retry...",
            db_str.c_str(),
            worker_ndb_->getNdbError().code,
            worker_ndb_->getNdbError().message);
	goto table_err;
      }
      ttl_tab = dict->getTable(table_str.c_str());
      if (ttl_tab == nullptr) {
        const NdbError& gt_err = dict->getNdbError();
        if (gt_err.code == 723) {
          // 723 = no such table: it was dropped out-of-band mid-round and
          // cannot be purged until re-created (a create event or the
          // reconcile re-adds it then), so drop it from this round's local
          // cache instead of burning retries into a full worker escalation.
          // ONLY the explicit not-found code qualifies: broader classes
          // (e.g. NdbError::PermanentError) include transient conditions
          // like schema-version races (241), and erasing a live table here
          // has no re-add path while reconcile is disabled.
          g_eventLogger->info("[TTL PWorker] Table %s is gone (error %d), "
                              "removing from the local purge cache",
                              iter->first.c_str(), gt_err.code);
          RemoveTableMetrics(iter->first);
          purge_trx_started = false;
          iter = local_ttl_cache.erase(iter);
          continue;
        }
        g_eventLogger->warning("[TTL PWorker] Failed to get table: "
                              "%s, error: %d(%s). Retry...",
                               table_str.c_str(),
                               gt_err.code,
                               gt_err.message);
	goto table_err;
      }
      table_id = ttl_tab->getTableId();
      hash_val = murmur3_32(reinterpret_cast<unsigned char*>(&table_id),
                                             sizeof(int), 0);
      if (shard == kShardNosharding &&
          !iter->second.part_id_offset_applied &&
          ttl_tab->getPartitionCount() > 1) {
        iter->second.part_id =
          worker_ndb_->getNodeId() % ttl_tab->getPartitionCount();
        iter->second.part_id_offset_applied = true;
      }
      {
        // A carried part_id can exceed the partition count if it shrank
        // (e.g. a reorganize) since the last visit: wrap instead of
        // scanning a nonexistent partition (the old assert here was
        // compiled out in release builds anyway).
        Uint32 part_count = ttl_tab->getPartitionCount();
        if (part_count > 0 && iter->second.part_id >= part_count) {
          iter->second.part_id %= part_count;
        }
      }
      if (shard >= kShardFirst && n_purge_nodes > 0) {
        // Sharded mode: partition-level ownership (see the helpers above).
        // Every active purge node works each TTL table, but on a disjoint,
        // evenly-scattered subset of its partitions, so purge scans never
        // contend with each other and a table's purge throughput scales
        // with the number of active purge nodes. Recomputed every round
        // from the live partition count and active-node set.
        if (!AlignToOwnedPartition(&iter->second.part_id,
                                   ttl_tab->getPartitionCount(), hash_val,
                                   static_cast<Uint32>(n_purge_nodes),
                                   static_cast<Uint32>(shard))) {
          // More purge nodes than partitions and this table's hash maps
          // this node past the last partition: nothing to do here.
          ++iter;
          continue;
        }
      }
      // Published after the wrap/ownership fix-ups above so /status shows
      // the partition this scan will actually use
      UpdateCurrentTable(iter->first, iter->second.part_id);
      log_buf += ("[P" + std::to_string(iter->second.part_id) +
                 "/" +
                 std::to_string(ttl_tab->getPartitionCount()) + "]");

      log_buf += ("[BS: " + std::to_string(iter->second.batch_size) + "]");

      trx_failure_times = 0;
retry_trx:
      if (purge_worker_exit_) {
        break;
      }
      /*
       * Transaction may be failed by the schema changing,
       * here we getTable() to get the latest NdbObject(the
       * previous has already been removed by removeCachedObject()
       * in the 'err' handling
       */
      ttl_tab = dict->getTable(table_str.c_str());
      if (ttl_tab == nullptr) {
        const NdbError& gt_err = dict->getNdbError();
        if (gt_err.code == 723) {
          // Same 723-only classified removal as the first per-table fetch
          // above; this is the retry_trx refetch, reached when the table
          // vanished MID-BATCH (the transaction failed and
          // purge_trx_started sent table_err back here) -- precisely the
          // case that previously retried kMaxTrxRetryTimes times and then
          // killed the worker.
          g_eventLogger->info("[TTL PWorker] Table %s is gone (error %d), "
                              "removing from the local purge cache",
                              iter->first.c_str(), gt_err.code);
          RemoveTableMetrics(iter->first);
          if (trans != nullptr) {
            worker_ndb_->closeTransaction(trans);
            trans = nullptr;
          }
          purge_trx_started = false;
          iter = local_ttl_cache.erase(iter);
          continue;
        }
        g_eventLogger->warning("[TTL PWorker] Failed to get table: "
                              "%s, error: %d(%s). Retry...",
                               table_str.c_str(),
                               gt_err.code,
                               gt_err.message);
	goto table_err;
      }
      trans = worker_ndb_->startTransaction();
      if (trans == nullptr) {
        g_eventLogger->warning("[TTL PWorker] Failed to start "
                               "transaction"
                               ", error: %d(%s). Retry...",
                               worker_ndb_->getNdbError().code,
                               worker_ndb_->getNdbError().message);
	goto table_err;
      }
      purge_trx_started = true;

      use_index = false;
      ttl_index = dict->getIndex(kTTLPurgeIndexName, table_str.c_str());
      if (ttl_index != nullptr) {
        const NdbDictionary::Column* ttl_col_index = ttl_index->getColumn(0);
        if (ttl_col_index != nullptr &&
              (ttl_col_index->getType() == NdbDictionary::Column::Datetime2 ||
              ttl_col_index->getType() == NdbDictionary::Column::Timestamp2)) {
        const NdbDictionary::Column* ttl_col_table =
               ttl_tab->getColumn(ttl_col_index->getName());
          if (ttl_col_table != nullptr &&
               (ttl_col_table->getType() == NdbDictionary::Column::Datetime2 ||
                ttl_col_table->getType() ==
                                          NdbDictionary::Column::Timestamp2) &&
               ttl_col_table->getColumnNo() == static_cast<int>(ttl_col_no)) {
            use_index = true;
          }
        }
      }

      check = 0;
      deletedRows = 0;
      if (use_index) {
        // Found index on ttl column, use it
        log_buf += "[INDEX scan]";
        const NdbDictionary::Column* ttl_col_index = ttl_index->getColumn(0);
        assert(ttl_col_index != nullptr &&
              (ttl_col_index->getType() == NdbDictionary::Column::Datetime2 ||
              ttl_col_index->getType() == NdbDictionary::Column::Timestamp2));
        const NdbDictionary::Column* ttl_col_table =
               ttl_tab->getColumn(ttl_col_index->getName());
        assert(ttl_col_table != nullptr &&
               (ttl_col_table->getType() == NdbDictionary::Column::Datetime2 ||
                ttl_col_table->getType() ==
                                          NdbDictionary::Column::Timestamp2) &&
               ttl_col_table->getColumnNo() == static_cast<int>(ttl_col_no));
        bool type_timestamp = (ttl_col_index->getType() ==
                               NdbDictionary::Column::Timestamp2);

        NdbIndexScanOperation* index_scan_op =
          trans->getNdbIndexScanOperation(ttl_index);
        if (index_scan_op == nullptr) {
          g_eventLogger->warning("[TTL PWorker] Failed to start get index "
                                 "scan operations on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
	  goto table_err;
        }
        index_scan_op->setPartitionId(iter->second.part_id);
        index_scan_op->setTTLPurgeWindowSize(purge_window);
        /* Index Scan */
        Uint32 scanFlags =
         /*NdbScanOperation::SF_OrderBy |
          *NdbScanOperation::SF_MultiRange |
          */
          NdbScanOperation::SF_KeyInfo |
          NdbScanOperation::SF_OnlyExpiredScan;

        if (index_scan_op->readTuples(NdbOperation::LM_Exclusive,
              scanFlags,
              1,                        // parallel
              iter->second.batch_size)  // batch
              != 0) {
          g_eventLogger->warning("[TTL PWorker] Failed to readTuples "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
	  goto table_err;
        }

        log_buf += "-[";
        // purged_pos_ high-water mark: read ONLY for diagnostics (logged
        // below) and to position purge_tab_iter for the post-commit update.
        // It is NO LONGER used as the scan lower bound -- a row inserted with
        // an old ttl_col (below this mark) would otherwise be skipped forever,
        // so we always scan from the infimum.
        packed_last = 0;
        purge_tab_iter = purged_pos_.find(iter->second.table_id);
        if (purge_tab_iter != purged_pos_.end()) {
          purge_part_iter = purge_tab_iter->second.find(iter->second.part_id);
          if (purge_part_iter != purge_tab_iter->second.end()) {
            packed_last = purge_part_iter->second;
          }
        }
        if (packed_last != 0) {
          TIME_from_longlong_datetime_packed(&datetime, packed_last);
          log_buf += "hwm:" +
                     std::to_string(TIME_to_ulonglong_datetime(datetime));
        } else {
          log_buf += "hwm:INF";
        }
        // Lower bound = infimum: always scan the whole expired prefix.
        memset(encoded_last, 0, 8);
        // Upper bound = expiry threshold = now - (ttl_sec + purge_window).
        // The kernel adds purge_window to ttl_sec when judging expiry
        // (DbtupExecQuery), so this bounds the index scan to exactly the rows
        // SF_OnlyExpiredScan accepts -- instead of [infimum, now) = every row.
        packed_now = GetNow(encoded_threshold, type_timestamp,
                            Uint64(iter->second.ttl_sec) + purge_window);
        TIME_from_longlong_datetime_packed(&datetime, packed_now);
        log_buf += " --- expire<=" +
                   std::to_string(TIME_to_ulonglong_datetime(datetime)) + "]";

        if (index_scan_op->setBound(ttl_col_index->getName(),
                            NdbIndexScanOperation::BoundLE,
                            encoded_last)) {
          g_eventLogger->warning("[TTL PWorker] Failed to setBound "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
	  goto table_err;
        }
        if (index_scan_op->setBound(ttl_col_index->getName(),
                            NdbIndexScanOperation::BoundGE, encoded_threshold)) {
          g_eventLogger->warning("[TTL PWorker] Failed to setBound "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
	  goto table_err;
        }
        rec_attr[0] = index_scan_op->getValue(ttl_col_no);
        if (rec_attr[0] == nullptr) {
          g_eventLogger->warning("[TTL PWorker] Failed to getValue "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
	  goto table_err;
        }
        if (trans->execute(NdbTransaction::NoCommit) != 0) {
          g_eventLogger->warning("[TTL PWorker] Failed to execute transaction "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
	  goto table_err;
        }
        memset(encoded_curr_purge, 0, 8);
        /*
         * Sleeping here can produce error
         * 296(Time-out in NDB, probably caused by deadlock),
         * which is handled below.
         *
         * sleep(XXX);
         */
        while ((check = index_scan_op->nextResult(true)) == 0) {
          do {
            memset(encoded_curr_purge, 0, 8);
            if (type_timestamp) {
              my_timeval timestamp;
              my_timestamp_from_binary(&timestamp,
                  reinterpret_cast<const unsigned char*>(rec_attr[0]->aRef()),
                  0);
              /*
               * TTL related
               * Lock-free UTC conversion (see ttl_utc_sec_to_TIME) keeps the
               * per-row purge loop off glibc's global tzset_lock.
               */
              const time_t tmp_t = (time_t)timestamp.m_tv_sec;
              MYSQL_TIME tmp;
              ttl_utc_sec_to_TIME(tmp_t, &tmp);
              my_datetime_packed_to_binary(
                  TIME_to_longlong_datetime_packed(tmp),
                  encoded_curr_purge, 0);
            } else {
              memcpy(encoded_curr_purge, rec_attr[0]->aRef(),
                     rec_attr[0]->get_size_in_bytes());
            }
            // std::cerr << "Get a expired row: timestamp = ["
            //   << rec_attr[0]->get_size_in_bytes() << "]";
            // for (Uint32 i = 0; i < rec_attr[0]->get_size_in_bytes(); i++) {
            //   std::cerr << std::hex
            //     << static_cast<unsigned int>(rec_attr[0]->aRef()[i])
            //     << " ";
            // }
            // std::cerr << std::endl;
            if (index_scan_op->deleteCurrentTuple() != 0) {
              g_eventLogger->warning("[TTL PWorker] Failed to deleteTuple "
                                     "on table %s"
                                     ", error: %d(%s). Retry...",
                                     ttl_tab->getName(),
                                     trans->getNdbError().code,
                                     trans->getNdbError().message);
	      goto table_err;
            }
            deletedRows++;
          } while ((check = index_scan_op->nextResult(false)) == 0);

          if (check == -1) {
            g_eventLogger->warning("[TTL PWorker] Failed to execute "
                                   "transaction[2] on table %s"
                                   ", error: %d(%s). Retry...",
                                   ttl_tab->getName(),
                                   trans->getNdbError().code,
                                   trans->getNdbError().message);
	    goto table_err;
          }
          break;
        }
        if (check == -1) {
          g_eventLogger->warning("[TTL PWorker] Failed to nextResult(true) "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
          if (trans->getNdbError().code == 296) {
            /*
             * if the TransactionInactiveTimeout is set too small,
             * error 296(Time-out in NDB, probably caused by deadlock)
             * may happen, change the batch size to the minimum and retry
             */
            iter->second.batch_size = local_config.min_batch_size;
            g_eventLogger->warning("[TTL PWorker] Changed the purgine batch "
                                   "size of table %s to the minimum size %u, "
                                   "Retry...",
                                   ttl_tab->getName(),
                                   iter->second.batch_size);
            // Another purge worker is likely on this partition right now
            // (lock wait / scan takeover). Back off to the next partition
            // this node may purge instead of piling onto the contended one;
            // the skipped partition is revisited on a later rotation or
            // drained by the contending node.
            iter->second.part_id = AdvancePartition(
                iter->second.part_id, ttl_tab->getPartitionCount(), hash_val,
                n_purge_nodes, shard);
          }
	  goto table_err;
        }
        /**
         * Commit all prepared operations
         */
        /*
         * Sleeping here can produce error
         * 499(Scan take over error)
         * which is handled below.
         *
         * sleep(XXX);
         */
        if (trans->execute(NdbTransaction::Commit) == -1) {
          g_eventLogger->warning("[TTL PWorker] Failed to commit transaction "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
          if (trans->getNdbError().code == 499) {
            /*
             * if the TransactionInactiveTimeout is set too small,
             * error 499(Scan take over error) may happen,
             * change the batch size to the minimum and retry
             */
            iter->second.batch_size = local_config.min_batch_size;
            g_eventLogger->warning("[TTL PWorker] Changed the purgine batch "
                                   "size of table %s to the minimum size %u, "
                                   "Retry...",
                                   ttl_tab->getName(),
                                   iter->second.batch_size);
            // Another purge worker is likely on this partition right now
            // (lock wait / scan takeover). Back off to the next partition
            // this node may purge instead of piling onto the contended one;
            // the skipped partition is revisited on a later rotation or
            // drained by the contending node.
            iter->second.part_id = AdvancePartition(
                iter->second.part_id, ttl_tab->getPartitionCount(), hash_val,
                n_purge_nodes, shard);
          }
	  goto table_err;
        } else if (*reinterpret_cast<Int64*>(encoded_curr_purge) != 0) {
          packed_last = my_datetime_packed_from_binary(encoded_curr_purge, 0);
          if (purge_tab_iter != purged_pos_.end()) {
            purge_tab_iter->second[iter->second.part_id] = packed_last;
          } else {
            purged_pos_[iter->second.table_id][iter->second.part_id]
                                                         = packed_last;
          }
        }
      } else if (dict->getNdbError().code == 4243 || !use_index) {
        // Can't find the index on ttl column, use table instead
        log_buf += "[TABLE scan]";
        scan_op = trans->getNdbScanOperation(ttl_tab);
        if (scan_op == nullptr) {
          g_eventLogger->warning("[TTL PWorker] Failed to start get scan "
                                 "operations on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
	  goto table_err;
        }
        scan_op->setPartitionId(iter->second.part_id);
        scan_op->setTTLPurgeWindowSize(purge_window);
        Uint32 scanFlags = NdbScanOperation::SF_OnlyExpiredScan;
        if (scan_op->readTuples(NdbOperation::LM_Exclusive, scanFlags,
                                1, iter->second.batch_size) != 0) {
          g_eventLogger->warning("[TTL PWorker] Failed to readTuples "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
	  goto table_err;
        }
        rec_attr[0] = scan_op->getValue(ttl_col_no);
        if (rec_attr[0] == nullptr) {
          g_eventLogger->warning("[TTL PWorker] Failed to getValue "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
	  goto table_err;
        }
        if (trans->execute(NdbTransaction::NoCommit) != 0) {
          g_eventLogger->warning("[TTL PWorker] Failed to execute transaction "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
	  goto table_err;
        }
        /*
         * Sleeping here can produce error
         * 296(Time-out in NDB, probably caused by deadlock),
         * which is handled below.
         *
         * sleep(XXX);
         */
        while ((check = scan_op->nextResult(true)) == 0) {
          do {
            // std::cerr << "Get a expired row: timestamp = ["
            //   << rec_attr[0]->get_size_in_bytes() << "]";
            // for (Uint32 i = 0; i < rec_attr[0]->get_size_in_bytes(); i++) {
            //   std::cerr << std::hex
            //     << static_cast<unsigned int>(rec_attr[0]->aRef()[i])
            //     << " ";
            // }
            // std::cerr << std::endl;
            if (scan_op->deleteCurrentTuple() != 0) {
              g_eventLogger->warning("[TTL PWorker] Failed to deleteTuple "
                                     "on table %s"
                                     ", error: %d(%s). Retry...",
                                     ttl_tab->getName(),
                                     trans->getNdbError().code,
                                     trans->getNdbError().message);
	      goto table_err;
            }
            deletedRows++;
          } while ((check = scan_op->nextResult(false)) == 0);

          if (check == -1) {
            g_eventLogger->warning("[TTL PWorker] Failed to execute "
                                   "transaction[2] on table %s"
                                   ", error: %d(%s). Retry...",
                                   ttl_tab->getName(),
                                   trans->getNdbError().code,
                                   trans->getNdbError().message);
	    goto table_err;
          }

          break;
        }

        if (check == -1) {
          g_eventLogger->warning("[TTL PWorker] Failed to nextResult(true) "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
          if (trans->getNdbError().code == 296) {
            /*
             * if the TransactionInactiveTimeout is set too small,
             * error 296(Time-out in NDB, probably caused by deadlock)
             * may happen, change the batch size to the minimum and retry
             */
            iter->second.batch_size = local_config.min_batch_size;
            g_eventLogger->warning("[TTL PWorker] Changed the purgine batch "
                                   "size of table %s to the minimum size %u, "
                                   "Retry...",
                                   ttl_tab->getName(),
                                   iter->second.batch_size);
            // Another purge worker is likely on this partition right now
            // (lock wait / scan takeover). Back off to the next partition
            // this node may purge instead of piling onto the contended one;
            // the skipped partition is revisited on a later rotation or
            // drained by the contending node.
            iter->second.part_id = AdvancePartition(
                iter->second.part_id, ttl_tab->getPartitionCount(), hash_val,
                n_purge_nodes, shard);
          }
	  goto table_err;
        }
        /**
         * Commit all prepared operations
         */
        /*
         * Sleeping here can produce error
         * 499(Scan take over error)
         * which is handled below.
         *
         * sleep(XXX);
         */
        if (trans->execute(NdbTransaction::Commit) == -1) {
          g_eventLogger->warning("[TTL PWorker] Failed to commit transaction "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
          if (trans->getNdbError().code == 499) {
            /*
             * if the TransactionInactiveTimeout is set too small,
             * error 499(Scan take over error) may happen,
             * change the batch size to the minimum and retry
             */
            iter->second.batch_size = local_config.min_batch_size;
            g_eventLogger->warning("[TTL PWorker] Changed the purgine batch "
                                   "size of table %s to the minimum size %u, "
                                   "Retry...",
                                   ttl_tab->getName(),
                                   iter->second.batch_size);
            // Another purge worker is likely on this partition right now
            // (lock wait / scan takeover). Back off to the next partition
            // this node may purge instead of piling onto the contended one;
            // the skipped partition is revisited on a later rotation or
            // drained by the contending node.
            iter->second.part_id = AdvancePartition(
                iter->second.part_id, ttl_tab->getPartitionCount(), hash_val,
                n_purge_nodes, shard);
          }
	  goto table_err;
        }
      } else {
        g_eventLogger->warning("[TTL PWorker] Failed to get Table/Index "
                               "object on table %s"
                               ", error: %d(%s). Retry...",
                               table_str.c_str(),
                               dict->getNdbError().code,
                               dict->getNdbError().message);
	goto table_err;
      }
      worker_ndb_->closeTransaction(trans);
      trans = nullptr;
      log_buf += " Purged " + std::to_string(deletedRows) + " rows";
#ifdef DEBUG_EVENT
      g_eventLogger->info("%s", log_buf.c_str());
#endif
      end_time = my_micro_time();

      iter->second.batch_size = AdjustBatchSize(iter->second.batch_size,
                                                deletedRows,
                                                end_time - start_time,
                                                local_config.min_batch_size,
                                                local_config.max_batch_size);
      if (sleep_between_each_round &&
          iter->second.batch_size == local_config.max_batch_size) {
        // At least 1 table finished its batch purging in the max size,
        // so don't sleep and start the next round as soon as possible
        sleep_between_each_round = false;
      }

      iter->second.part_id = AdvancePartition(
          iter->second.part_id, ttl_tab->getPartitionCount(), hash_val,
          n_purge_nodes, shard);

      // Accumulate metrics locally (no locking during round)
      local_rows_purged += deletedRows;
      {
        std::string key = db_str + "/" + table_str;
        auto it = local_table_metrics.find(key);
        if (it == local_table_metrics.end()) {
          TTLTableMetrics tm;
          tm.database = db_str;
          tm.table = table_str;
          tm.table_id = iter->second.table_id;
          tm.ttl_sec = iter->second.ttl_sec;
          tm.ttl_column_no = iter->second.col_no;
          tm.current_partition = iter->second.part_id;
          tm.partition_count = ttl_tab->getPartitionCount();
          tm.current_batch_size = iter->second.batch_size;
          tm.rows_purged = deletedRows;
          local_table_metrics[key] = tm;
        } else {
          it->second.current_partition = iter->second.part_id;
          it->second.current_batch_size = iter->second.batch_size;
          it->second.rows_purged += deletedRows;
        }
      }

	      // Finish 1 batch
	      // keep the ttl_tab in local table cache ?
	      ++iter;
	      continue;
table_err:
      if (trans != nullptr) {
        worker_ndb_->closeTransaction(trans);
        trans = nullptr;
      }
      trx_failure_times++;
      if (purge_worker_exit_) {
        break;
      }
      sleep(1);
      if (trx_failure_times > kMaxTrxRetryTimes) {
        g_eventLogger->warning("[TTL PWorker] Has retried for %d times..."
                               "Quit and notify schema worker",
                               kMaxTrxRetryTimes);
        for (auto iter = local_ttl_cache.begin();
            iter != local_ttl_cache.end(); iter++) {
          auto pos = iter->first.find('/');
          if (pos != std::string::npos) {
            std::string db = iter->first.substr(0, pos);
            if (worker_ndb_->setDatabaseName(db.c_str()) != 0) {
              g_eventLogger->warning("[TTL SWatcher-] "
                  "Failed to select database: %s"
                  ", error: %d(%s). Retry...",
                  db.c_str(),
                  worker_ndb_->getNdbError().code,
                  worker_ndb_->getNdbError().message);
              continue;
            }
            g_eventLogger->info("[TTL SWatcher] Remove[5] TTL of table %s "
                "in cache: [%u, %u@%u]",
                iter->first.c_str(), iter->second.table_id,
                iter->second.ttl_sec, iter->second.col_no);
            if (pos + 1 < iter->first.length()) {
              std::string table = iter->first.substr(pos + 1);
              /*
               * FIX for error 241 infinite loop after restart:
               *
               * Use invalidateTable() here instead of removeCachedTable().
               *
               * removeCachedTable() only decrements refCount but does NOT
               * mark m_impl->m_status as Invalid. When getTable() is called
               * later, GlobalDictCache::get() sees status=OK and returns
               * the SAME stale cached object, causing error 241 loop.
               *
               * invalidateTable() marks m_impl->m_status as Invalid, so
               * next getTable() will retrieve fresh metadata from data nodes.
               *
               * The race condition concern at lines 1066-1077 does NOT apply
               * here because purge worker EXITS immediately after this
               * cleanup (line 1759) without calling getTable(). When schema
               * watcher restarts purge worker, the new worker calls getTable()
               * and gets fresh objects since they are marked Invalid.
               *
               * ORIGINAL CODE (for potential revert):
               * dict->removeCachedTable(table.c_str());
               */
              dict->invalidateTable(table.c_str());
              dict->invalidateIndex(kTTLPurgeIndexName, table.c_str());
            }
          }
        }
        purge_worker_asks_for_retry_ = true;
        purge_worker_exit_ = true;
        UpdateStatus(TTLPurgeStatus::State::kError);
        break;
      } else if (purge_trx_started) {
        dict->removeCachedTable(table_str.c_str());
        goto retry_trx;
      } else {
        // Pre-transaction error: skip this table and try the next one.
        // Accumulate failures in a do-while scoped counter so that
        // persistent errors eventually trigger a full restart.
        pre_trx_failures++;
        if (pre_trx_failures > kMaxTrxRetryTimes) {
          g_eventLogger->warning("[TTL PWorker] Pre-transaction errors "
                                 "exceeded %d times... "
                                 "Quit and notify schema worker",
                                 kMaxTrxRetryTimes);
          for (auto it = local_ttl_cache.begin();
              it != local_ttl_cache.end(); it++) {
            auto p = it->first.find('/');
            if (p != std::string::npos) {
              std::string db = it->first.substr(0, p);
              if (worker_ndb_->setDatabaseName(db.c_str()) != 0) {
                continue;
              }
              if (p + 1 < it->first.length()) {
                std::string table = it->first.substr(p + 1);
                dict->invalidateTable(table.c_str());
                dict->invalidateIndex(kTTLPurgeIndexName, table.c_str());
              }
            }
          }
          purge_worker_asks_for_retry_ = true;
          purge_worker_exit_ = true;
          UpdateStatus(TTLPurgeStatus::State::kError);
          break;
        }
		++iter;
		continue;  // skip to next table
	      }
    }
    // Round completed without pre-trx escalation, reset the counter
    pre_trx_failures = 0;
    // Remember whether this round ended still saturated (feeds the
    // backlog warning when the active window closes).
    last_round_saturated = window_closed_mid_round || !sleep_between_each_round;
    // Finish 1 round - update all metrics with single lock acquisition
    {
      Uint64 round_end_time = my_micro_time();
      Uint64 duration_ms = (round_end_time - round_start_time) / 1000;
      UpdateRoundMetrics(local_rows_purged, duration_ms, local_table_metrics);
    }
    if (sleep_between_each_round) {
      // Sleep with randomness to avoid contention between multiple RDRS nodes
      // Use configured interval as center, with ±33% variance
      int variance = static_cast<int>(local_config.sleep_interval_ms / 3);
      int lower = static_cast<int>(local_config.sleep_interval_ms) - variance;
      int upper = static_cast<int>(local_config.sleep_interval_ms) + variance;
      if (lower < 100) lower = 100;  // Minimum 100ms
      RandomSleep(lower, upper);
    }
    continue;
round_err:
    if (trans != nullptr) {
      worker_ndb_->closeTransaction(trans);
      trans = nullptr;
    }
    if (purge_worker_exit_) {
      break;
    }
    // Pre-round failures (cache_updated_ walk / GetShard / GetPurgeCtrl /
    // pre-loop UpdateLease) share the do-while-scoped pre_trx_failures
    // counter with table_err's pre-trx branch so persistent failures
    // eventually escalate instead of sleeping silently forever. The counter
    // is reset to 0 only after a fully successful round (see end of the
    // table for-loop). We deliberately skip the per-table dictionary
    // invalidation that table_err performs on escalation: most round_err
    // failures are before selecting a specific user TTL table, and the
    // cache_updated_ walk already failed while trying to refresh cached
    // objects, so escalation only asks the schema worker to restart.
    pre_trx_failures++;
    if (pre_trx_failures > kMaxTrxRetryTimes) {
      g_eventLogger->warning("[TTL PWorker] Pre-round errors exceeded %d "
                             "times... Quit and notify schema worker",
                             kMaxTrxRetryTimes);
      purge_worker_asks_for_retry_ = true;
      purge_worker_exit_ = true;
      UpdateStatus(TTLPurgeStatus::State::kError);
      break;
    }
    sleep(1);
    continue;
  } while (!purge_worker_exit_);

  // No need to return PurgeWorker NdbObject here, SchemaWatch will do that.
  g_eventLogger->info("[TTL PWorker] Exited");
  return;
}

bool TTLPurger::GetShard(Int32* shard, Int32* n_purge_nodes,
                         bool update_objects) {
  *shard = kShardNosharding;
  *n_purge_nodes = 0;
  if (worker_ndb_->setDatabaseName(kSystemDBName) != 0) {
    g_eventLogger->warning("[TTL PWorker] Failed to select system database: "
                          "%s, error: %d(%s). Retry...",
                           kSystemDBName,
                           worker_ndb_->getNdbError().code,
                           worker_ndb_->getNdbError().message);
    return false;
  }
  NdbDictionary::Dictionary* dict = worker_ndb_->getDictionary();
  if (update_objects) {
    dict->removeCachedTable(kTTLPurgeNodesTabName);
  }
  const NdbDictionary::Table* tab = dict->getTable(kTTLPurgeNodesTabName);
  if (tab == nullptr) {
    if (dict->getNdbError().code == 723) {
      // Purging nodes configuration table is not found, no sharding
      return true;
    } else {
      g_eventLogger->warning("[TTL PWorker] Failed to get table: "
                            "%s, error: %d(%s). Retry...",
                             kTTLPurgeNodesTabName,
                             dict->getNdbError().code,
                             dict->getNdbError().message);
      return false;
    }
  }
  NdbRecAttr* rec_attr[3];
  NdbTransaction* trans = nullptr;
  NdbScanOperation* scan_op = nullptr;
  Int32 n_nodes = 0;;
  std::vector<Int32> purge_nodes;
  size_t pos = 0;
  int check = 0;
  std::string log_buf = "[TTL PWorker] ";
  std::string active_nodes = "[";
  std::string inactive_nodes = "[";

  trans = worker_ndb_->startTransaction();
  if (trans == nullptr) {
    g_eventLogger->warning("[TTL PWorker] Failed to start "
                           "transaction"
                           ", error: %d(%s). Retry...",
                           worker_ndb_->getNdbError().code,
                           worker_ndb_->getNdbError().message);
    goto err;
  }
  scan_op = trans->getNdbScanOperation(tab);
  if (scan_op == nullptr) {
    g_eventLogger->warning("[TTL PWorker] Failed to start get scan "
                           "operations on table %s"
                           ", error: %d(%s). Retry...",
                           tab->getName(),
                           trans->getNdbError().code,
                           trans->getNdbError().message);
    goto err;
  }
  if (scan_op->readTuples(NdbOperation::LM_CommittedRead) != 0) {
    g_eventLogger->warning("[TTL PWorker] Failed to readTuples "
                           "on table %s"
                           ", error: %d(%s). Retry...",
                           tab->getName(),
                           trans->getNdbError().code,
                           trans->getNdbError().message);
    goto err;
  }

  rec_attr[0] = scan_op->getValue("node_id");
  if (rec_attr[0] == nullptr) {
    g_eventLogger->warning("[TTL PWorker] Failed to getValue "
                           "on table %s"
                           ", error: %d(%s). Retry...",
                           tab->getName(),
                           trans->getNdbError().code,
                           trans->getNdbError().message);
    goto err;
  }
  rec_attr[1] = scan_op->getValue("last_active");
  if (rec_attr[1] == nullptr) {
    g_eventLogger->warning("[TTL PWorker] Failed to getValue "
                           "on table %s"
                           ", error: %d(%s). Retry...",
                           tab->getName(),
                           trans->getNdbError().code,
                           trans->getNdbError().message);
    goto err;
  }
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    g_eventLogger->warning("[TTL PWorker] Failed to execute transaction "
                           "on table %s"
                           ", error: %d(%s). Retry...",
                           tab->getName(),
                           trans->getNdbError().code,
                           trans->getNdbError().message);
    goto err;
  }
  n_nodes = 0;
  purge_nodes.clear();
  pos = 0;
  while ((check = scan_op->nextResult(true)) == 0) {
    do {
      if (rec_attr[0]->int32_value() != worker_ndb_->getNodeId() &&
          (rec_attr[1]->isNULL() ||
           !IsNodeAlive(reinterpret_cast<unsigned char*>(
              rec_attr[1]->aRef())))) {
        inactive_nodes += (std::to_string(rec_attr[0]->int32_value()) + " ");
        continue;
      }
      n_nodes++;
      purge_nodes.push_back(rec_attr[0]->int32_value());
    } while ((check = scan_op->nextResult(false)) == 0);
    if (check == -1) {
      break;
    }
  }
  if (check == -1) {
    g_eventLogger->warning("[TTL PWorker] Failed to scan table %s"
                           ", error: %d(%s). Retry...",
                           tab->getName(),
                           scan_op->getNdbError().code,
                           scan_op->getNdbError().message);
    goto err;
  }

  std::sort(purge_nodes.begin(), purge_nodes.end());
  if (!purge_nodes.empty()) {
    for (auto iter : purge_nodes) {
      active_nodes += (std::to_string(iter) + " ");
      if (worker_ndb_->getNodeId() == iter) {
        *shard = pos;
      }
      pos++;
    }
  }
  if (!purge_nodes.empty() && *shard == -1) {
    // if the current node id is not in the purging nodes list,
    // set shard to -2 to tell the purge thread sleep
    *shard = kShardNotPurger;
  }
  *n_purge_nodes = n_nodes;
  worker_ndb_->closeTransaction(trans);
  if (active_nodes.length() > 1) {
    active_nodes[active_nodes.length() - 1] = ']';
  } else {
    active_nodes += "]";
  }
  if (inactive_nodes.length() > 1) {
    inactive_nodes[inactive_nodes.length() - 1] = ']';
  } else {
    inactive_nodes += "]";
  }
  log_buf += ("Shard: [" + std::to_string(*shard) +
              "/" + std::to_string(n_nodes) + "]");
  log_buf += (", Active purging nodes: " + active_nodes);
  log_buf += (", Inactive purging nodes: " + inactive_nodes);
#ifdef DEBUG_EVENT
  g_eventLogger->info("%s", log_buf.c_str());
#endif
  return true;

err:
  if (trans != nullptr) {
    worker_ndb_->closeTransaction(trans);
  }
  return false;
}

bool TTLPurger::HasActiveWindow(Int32 start_min, Int32 end_min) {
  return start_min >= 0 && start_min < kMinutesPerDay &&
         end_min >= 0 && end_min < kMinutesPerDay &&
         start_min != end_min;
}

bool TTLPurger::InActiveWindow(Int32 start_min, Int32 end_min,
                               time_t now_utc) {
  Int32 minute_of_day = static_cast<Int32>((now_utc % 86400) / 60);
  if (start_min < end_min) {
    return minute_of_day >= start_min && minute_of_day < end_min;
  }
  // start > end: the daily window wraps past midnight
  return minute_of_day >= start_min || minute_of_day < end_min;
}

bool TTLPurger::GetPurgeCtrl(PurgeCtrlSettings* settings,
                             bool update_objects) {
  PurgeCtrlSettings old = *settings;
  PurgeCtrlSettings fresh;  // defaults: no lag, no active window
  if (worker_ndb_->setDatabaseName(kSystemDBName) != 0) {
    g_eventLogger->warning("[TTL PWorker] Failed to select system database: "
                          "%s, error: %d(%s). Retry...",
                           kSystemDBName,
                           worker_ndb_->getNdbError().code,
                           worker_ndb_->getNdbError().message);
    return false;
  }
  NdbDictionary::Dictionary* dict = worker_ndb_->getDictionary();
  if (update_objects) {
    dict->removeCachedTable(kTTLPurgeCtrlTabName);
  }
  const NdbDictionary::Table* tab = dict->getTable(kTTLPurgeCtrlTabName);
  if (tab == nullptr) {
    if (dict->getNdbError().code != 723) {
      g_eventLogger->warning("[TTL PWorker] Failed to get table: "
                            "%s, error: %d(%s). Retry...",
                             kTTLPurgeCtrlTabName,
                             dict->getNdbError().code,
                             dict->getNdbError().message);
      return false;
    }
    // Purge control configuration table not found — defaults apply
  } else {
    NdbTransaction* trans = worker_ndb_->startTransaction();
    if (trans == nullptr) {
      g_eventLogger->warning("[TTL PWorker] Failed to start "
                             "transaction"
                             ", error: %d(%s). Retry...",
                             worker_ndb_->getNdbError().code,
                             worker_ndb_->getNdbError().message);
      return false;
    }
    // One committed read per ctrl row, batched in a single transaction.
    // Reads default to AO_IgnoreError, so an absent row surfaces as a
    // per-operation 626 (leaving that setting at its default) instead of
    // failing the whole execute.
    const int ctrl_ids[3] = {kPurgeCtrlPurgeWindowId,
                             kPurgeCtrlActiveWinStartId,
                             kPurgeCtrlActiveWinEndId};
    const NdbOperation* ops[3] = {nullptr, nullptr, nullptr};
    NdbRecAttr* vals[3] = {nullptr, nullptr, nullptr};
    for (int i = 0; i < 3; i++) {
      NdbOperation* op = trans->getNdbOperation(tab);
      if (op == nullptr ||
          op->readTuple(NdbOperation::LM_CommittedRead) != 0 ||
          op->equal(kPurgeCtrlKey, ctrl_ids[i]) != 0 ||
          (vals[i] = op->getValue(kPurgeCtrlValue)) == nullptr) {
        g_eventLogger->warning("[TTL PWorker] Failed to prepare read "
                               "[%s = %d] on table %s"
                               ", error: %d(%s). Retry...",
                               kPurgeCtrlKey, ctrl_ids[i], tab->getName(),
                               trans->getNdbError().code,
                               trans->getNdbError().message);
        worker_ndb_->closeTransaction(trans);
        return false;
      }
      ops[i] = op;
    }
    if (trans->execute(NdbTransaction::Commit) != 0) {
      g_eventLogger->warning("[TTL PWorker] Failed to execute transaction "
                             "on table %s"
                             ", error: %d(%s). Retry...",
                             tab->getName(),
                             trans->getNdbError().code,
                             trans->getNdbError().message);
      worker_ndb_->closeTransaction(trans);
      return false;
    }
    Int32 raw[3] = {0, -1, -1};  // defaults: lag 0, window unset
    for (int i = 0; i < 3; i++) {
      const NdbError& op_err = ops[i]->getNdbError();
      if (op_err.code == 626) {
        continue;  // row absent: keep the default
      }
      if (op_err.code != 0) {
        g_eventLogger->warning("[TTL PWorker] Failed to read [%s = %d] "
                               "on table %s, error: %d(%s). Retry...",
                               kPurgeCtrlKey, ctrl_ids[i], tab->getName(),
                               op_err.code, op_err.message);
        worker_ndb_->closeTransaction(trans);
        return false;
      }
      if (!vals[i]->isNULL()) {
        raw[i] = vals[i]->int32_value();
      }
    }
    worker_ndb_->closeTransaction(trans);
    if (raw[0] < 0) {
      g_eventLogger->warning("[TTL PWorker] Negtive purge window size %d "
                             "is set in the ttl_purge_ctrl, using 0 instead",
                             raw[0]);
      raw[0] = 0;
    }
    fresh.purge_lag_sec = static_cast<Uint32>(raw[0]);
    fresh.win_start_min = raw[1];
    fresh.win_end_min = raw[2];
  }
  *settings = fresh;

  // Change logging: this runs every round, so log only on transitions.
  if (old.purge_lag_sec != settings->purge_lag_sec) {
    g_eventLogger->info("[TTL PWorker] purge window size changed from %u "
                        "to %u seconds",
                        old.purge_lag_sec, settings->purge_lag_sec);
  }
  if (old.win_start_min != settings->win_start_min ||
      old.win_end_min != settings->win_end_min) {
    if (HasActiveWindow(settings->win_start_min, settings->win_end_min)) {
      g_eventLogger->info("[TTL PWorker] purge active window set to "
                          "[%02d:%02d-%02d:%02d) UTC",
                          settings->win_start_min / 60,
                          settings->win_start_min % 60,
                          settings->win_end_min / 60,
                          settings->win_end_min % 60);
    } else if (settings->win_start_min >= 0 || settings->win_end_min >= 0) {
      g_eventLogger->warning("[TTL PWorker] Invalid purge active window "
                             "(start=%d, end=%d): both must be in [0, 1439] "
                             "minutes and start != end; ignoring it (any "
                             "per-node config window still applies)",
                             settings->win_start_min, settings->win_end_min);
    } else {
      g_eventLogger->info("[TTL PWorker] purge active window cleared, "
                          "purging around the clock");
    }
  }
  return true;
}

Int64 TTLPurger::GetNow(unsigned char* encoded_now, bool timestamp,
                        Uint64 minus_sec) {
  assert(encoded_now != nullptr);
  Int64 packed_now = 0;
  memset(encoded_now, 0, 8);
  MYSQL_TIME curr_dt;
  time_t t_now = (time_t)my_micro_time() / 1000000; /* second */
  // Callers computing the TTL expiry threshold pass
  // minus_sec = ttl_sec + purge_window; encode (now - minus_sec). Clamp at 0
  // so an oversized TTL cannot wrap into a future instant (which would
  // re-open the unbounded [infimum, now) scan).
  t_now = (t_now > (time_t)minus_sec) ? (t_now - (time_t)minus_sec) : 0;
  /*
   * TTL related
   * Lock-free UTC conversion (see ttl_utc_sec_to_TIME) avoids glibc's
   * global tzset_lock.
   */
  ttl_utc_sec_to_TIME(t_now, &curr_dt);
  packed_now = TIME_to_longlong_datetime_packed(curr_dt);

  if (timestamp) {
    mi_int4store(encoded_now, t_now);
  } else {
    my_datetime_packed_to_binary(packed_now, encoded_now, 0);
  }
  return packed_now;
}

bool TTLPurger::UpdateLease(const unsigned char* encoded_now) {
  NdbDictionary::Dictionary* dict = nullptr;
  const NdbDictionary::Table* tab = nullptr;
  NdbTransaction* trans = nullptr;
  NdbOperation *op = nullptr;
  if (worker_ndb_->setDatabaseName(kSystemDBName) != 0) {
    g_eventLogger->warning("[TTL PWorker] Failed to select system database: "
                          "%s, error: %d(%s). Retry...",
                           kSystemDBName,
                           worker_ndb_->getNdbError().code,
                           worker_ndb_->getNdbError().message);
    goto err;
  }
  dict = worker_ndb_->getDictionary();
  tab = dict->getTable(kTTLPurgeNodesTabName);
  if (tab == nullptr) {
    if (dict->getNdbError().code == 723) {
      /*
       * Purging nodes configuration table is not found,
       * no need to update lease
       */
      return true;
    } else {
      g_eventLogger->warning("[TTL PWorker] Failed to get table: "
                            "%s, error: %d(%s). Retry...",
                             kTTLPurgeNodesTabName,
                             dict->getNdbError().code,
                             dict->getNdbError().message);
      goto err;
    }
  }

  trans = worker_ndb_->startTransaction();
  if (trans == nullptr) {
    g_eventLogger->warning("[TTL PWorker] Failed to start "
                           "transaction"
                           ", error: %d(%s). Retry...",
                           worker_ndb_->getNdbError().code,
                           worker_ndb_->getNdbError().message);
    goto err;
  }
  op = trans->getNdbOperation(tab);
  if (op == nullptr) {
    g_eventLogger->warning("[TTL PWorker] Failed to get the Ndb "
                           "operation on table %s"
                           ", error: %d(%s). Retry...",
                           tab->getName(),
                           trans->getNdbError().code,
                           trans->getNdbError().message);
    goto err;
  }
  if (op->updateTuple() != 0 ||
      op->equal("node_id", worker_ndb_->getNodeId()) != 0 ||
      op->setValue("last_active",
                   reinterpret_cast<const char*>(encoded_now)) != 0) {
    g_eventLogger->warning("[TTL PWorker] Failed to prepare update on table %s"
                           ", error: %d(%s). Retry...",
                           tab->getName(),
                           op->getNdbError().code,
                           op->getNdbError().message);
    goto err;
  }

  if (trans->execute(NdbTransaction::Commit) != 0) {
    if (trans->getNdbError().code != 626 /*not found*/) {
      g_eventLogger->warning("[TTL PWorker] Failed to commit transaction "
                             "on table %s"
                             ", error: %d(%s). Retry...",
                             tab->getName(),
                             trans->getNdbError().code,
                             trans->getNdbError().message);
      goto err;
    }
  }
  worker_ndb_->closeTransaction(trans);
  return true;
err:
  if (trans != nullptr) {
    worker_ndb_->closeTransaction(trans);
  }
  return false;
}

bool TTLPurger::IsNodeAlive(const unsigned char* encoded_last_active) {
  assert(encoded_last_active != nullptr);
  Uint64 packed_last_active =
          my_datetime_packed_from_binary(encoded_last_active, 0);
  MYSQL_TIME last_active_dt;
  TIME_from_longlong_datetime_packed(&last_active_dt, packed_last_active);
  // Add lease seconds
  Interval interval;
  memset(&interval, 0, sizeof(interval));
  interval.second = kLeaseSeconds;
  date_add_interval(&last_active_dt, INTERVAL_SECOND, interval, nullptr);

  MYSQL_TIME curr_dt;
  time_t t_now = (time_t)my_micro_time() / 1000000; /* second */
  /*
   * TTL related
   * Lock-free UTC conversion (see ttl_utc_sec_to_TIME) avoids glibc's
   * global tzset_lock.
   */
  ttl_utc_sec_to_TIME(t_now, &curr_dt);

  int res = my_time_compare(last_active_dt, curr_dt);
  if (res >= 0) {
    return true;
  } else {
    return false;
  }
}

void* TTLPurger::_SchemaWatcherJob(void* arg) {
  errno = 0;
  TTLPurger* p_this = static_cast<TTLPurger*>(arg);
  p_this->SchemaWatcherJob();
  return nullptr;
}

bool TTLPurger::Run() {
  if (!schema_watcher_running_) {
    assert(schema_watcher_ == nullptr);
    assert(!purge_worker_running_);
    schema_watcher_ = NdbThread_Create(TTLPurger::_SchemaWatcherJob,
                                       (NDB_THREAD_ARG *)this,
                                       0, "SchemaWatcher",
                                       NDB_THREAD_PRIO_MEAN);
    schema_watcher_running_ = true;
  }
  return true;
}

Uint32 TTLPurger::AdjustBatchSize(Uint32 curr_batch_size,
                                 Uint32 deleted_rows,
                                 Uint64 used_time,
                                 Uint32 min_batch,
                                 Uint32 max_batch) {
  // Use provided config values (read once per round to minimize lock contention)
  if (deleted_rows == curr_batch_size && used_time < kPurgeThresholdTime) {
      if (curr_batch_size + kBatchSizePerIncr <= max_batch) {
        // Increase
        return curr_batch_size + kBatchSizePerIncr;
      } else {
        // Keep as max
        return max_batch;
      }
  } else if (curr_batch_size > min_batch &&
             curr_batch_size - kBatchSizePerIncr >= min_batch) {
    // Decrease
    return curr_batch_size - kBatchSizePerIncr;
  } else {
    // Keep as min
    return min_batch;
  }
}

// ============================================================================
// Config, Status, and Metrics API implementation
// ============================================================================
// Lock strategy:
// - Readers (API GET, purge worker config read): use std::shared_lock
// - Writers (API PUT, purge worker status/metrics update): use std::lock_guard
// This allows concurrent reads while writes have exclusive access.

TTLPurgeConfig TTLPurger::GetConfig() const {
  const std::shared_lock<std::shared_mutex> lock(config_mutex_);
  return config_;
}

void TTLPurger::SetConfig(const TTLPurgeConfig& config) {
  const std::lock_guard<std::shared_mutex> lock(config_mutex_);
  bool was_enabled = config_.enabled;
  config_ = config;

  // Log config change
  if (was_enabled != config.enabled) {
    g_eventLogger->info("[TTL PWorker] TTL purge %s via API",
                        config.enabled ? "enabled" : "disabled");
  }
}

bool TTLPurger::IsEnabled() const {
  const std::shared_lock<std::shared_mutex> lock(config_mutex_);
  return config_.enabled;
}

void TTLPurger::SetEnabled(bool enabled) {
  const std::lock_guard<std::shared_mutex> lock(config_mutex_);
  if (config_.enabled != enabled) {
    config_.enabled = enabled;
    g_eventLogger->info("[TTL PWorker] TTL purge %s via API",
                        enabled ? "enabled" : "disabled");
  }
}

TTLPurgeStatus TTLPurger::GetStatus() const {
  TTLPurgeStatus status;
  {
    const std::shared_lock<std::shared_mutex> lock(metrics_mutex_);
    status = status_;
  }
  // These are atomic or only set at startup, safe to read without lock
  status.schema_watcher_running = schema_watcher_running_;
  status.purge_worker_running = purge_worker_running_;
  return status;
}

TTLPurgeMetrics TTLPurger::GetMetrics() const {
  TTLPurgeMetrics result;
  {
    const std::shared_lock<std::shared_mutex> lock(metrics_mutex_);
    result = metrics_;
  }
  // Compute tables_count on-the-fly from table_metrics_ (always accurate)
  // Sequential lock acquisition (no nesting) avoids deadlock risk
  {
    const std::shared_lock<std::shared_mutex> lock(table_metrics_mutex_);
    result.tables_count = static_cast<Uint32>(table_metrics_.size());
  }
  return result;
}

std::vector<TTLTableMetrics> TTLPurger::GetTableMetrics(
    Uint32 offset, Uint32 limit, Uint32* total) const {
  std::vector<TTLTableMetrics> result;
  const std::shared_lock<std::shared_mutex> lock(table_metrics_mutex_);

  *total = static_cast<Uint32>(table_metrics_.size());
  if (offset >= *total) {
    return result;
  }

  // Only copy the requested page - fixed cost regardless of total tables
  result.reserve(std::min(limit, *total - offset));
  auto it = table_metrics_.begin();
  std::advance(it, offset);
  for (Uint32 i = 0; i < limit && it != table_metrics_.end(); ++i, ++it) {
    result.push_back(it->second);
  }
  return result;
}

bool TTLPurger::GetTableMetrics(const std::string& db, const std::string& table,
                                TTLTableMetrics* out) const {
  std::string key = db + "/" + table;
  const std::shared_lock<std::shared_mutex> lock(table_metrics_mutex_);
  auto it = table_metrics_.find(key);
  if (it == table_metrics_.end()) {
    return false;
  }
  *out = it->second;
  return true;
}

void TTLPurger::UpdateStatus(TTLPurgeStatus::State state) {
  const std::lock_guard<std::shared_mutex> lock(metrics_mutex_);
  status_.state = state;
}

void TTLPurger::UpdateCurrentTable(const std::string& table, Uint32 partition) {
  const std::lock_guard<std::shared_mutex> lock(metrics_mutex_);
  status_.current_table = table;
  status_.current_partition = partition;
}

void TTLPurger::UpdateActiveWindowStatus(Int32 start_min, Int32 end_min,
                                         const char* source) {
  const std::lock_guard<std::shared_mutex> lock(metrics_mutex_);
  status_.active_window_start_min = start_min;
  status_.active_window_end_min = end_min;
  status_.active_window_source = source;
}

void TTLPurger::UpdateRoundMetrics(
    Uint64 rows_purged_this_round,
    Uint64 duration_ms,
    const std::map<std::string, TTLTableMetrics>& table_updates) {
  Uint64 now_ms = static_cast<Uint64>(my_micro_time() / 1000);

  // Update global metrics (single lock)
  {
    const std::lock_guard<std::shared_mutex> lock(metrics_mutex_);
    metrics_.rows_purged_total += rows_purged_this_round;
    metrics_.rows_purged_last_round = rows_purged_this_round;
    metrics_.last_round_duration_ms = duration_ms;
    metrics_.last_purge_time_epoch_ms = now_ms;
    metrics_.rounds_completed++;
  }

  // Update per-table metrics (single lock)
  if (!table_updates.empty()) {
    const std::lock_guard<std::shared_mutex> lock(table_metrics_mutex_);
    for (const auto& [key, update] : table_updates) {
      auto it = table_metrics_.find(key);
      if (it == table_metrics_.end()) {
        // New table
        table_metrics_[key] = update;
        table_metrics_[key].last_purge_time_epoch_ms = now_ms;
      } else {
        // Existing table - merge updates
        it->second.current_partition = update.current_partition;
        it->second.partition_count = update.partition_count;
        it->second.current_batch_size = update.current_batch_size;
        it->second.rows_purged += update.rows_purged;
        it->second.last_purge_time_epoch_ms = now_ms;
      }
    }
    // tables_count is computed on-the-fly in GetMetrics() from
    // table_metrics_.size(), no need to sync here
  }
}

void TTLPurger::RemoveTableMetrics(const std::string& key) {
  const std::lock_guard<std::shared_mutex> lock(table_metrics_mutex_);
  table_metrics_.erase(key);
}

void TTLPurger::RemoveDBTableMetrics(const std::string& db) {
  const std::lock_guard<std::shared_mutex> lock(table_metrics_mutex_);
  std::string prefix = db + "/";
  for (auto it = table_metrics_.begin(); it != table_metrics_.end(); ) {
    if (it->first.compare(0, prefix.size(), prefix) == 0) {
      it = table_metrics_.erase(it);
    } else {
      ++it;
    }
  }
}

void TTLPurger::UpdateTableMetricsTTL(const std::string& key,
                                      Uint32 ttl_sec, Uint32 ttl_col_no) {
  const std::lock_guard<std::shared_mutex> lock(table_metrics_mutex_);
  auto it = table_metrics_.find(key);
  if (it != table_metrics_.end()) {
    it->second.ttl_sec = ttl_sec;
    it->second.ttl_column_no = ttl_col_no;
  }
  // If entry doesn't exist, it will be created on next purge round
}
