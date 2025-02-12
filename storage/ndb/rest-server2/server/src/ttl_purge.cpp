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
  const char* message_buf = "API_OK";
#ifdef DEBUG_EVENT
  Uint32 event_nums = 0;
#endif
  [[maybe_unused]] char event_name_buf[128];
  char slock_buf_pre[32];
  char slock_buf[32];

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

  // Fetch tables
  ttl_cache_.clear();
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
    if (watcher_ndb_->setDatabaseName(db_str) != 0) {
      g_eventLogger->warning("[TTL SWatcher] Failed to select database: %s"
                             ", error: %d(%s). Retry...",
                             db_str,
                             watcher_ndb_->getNdbError().code,
                             watcher_ndb_->getNdbError().message);
      goto err;
    }
    const NdbDictionary::Table* tab = dict->getTable(
        table_str);
    if (tab == nullptr) {
      g_eventLogger->warning("[TTL SWatcher] Failed to get table: %s"
                             ", error: %d(%s). Retry...",
                             table_str,
                             dict->getNdbError().code,
                             dict->getNdbError().message);
      goto err;
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
          case NdbDictionary::Event::TE_INSERT:
          case NdbDictionary::Event::TE_UPDATE:
          case NdbDictionary::Event::TE_DELETE:
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
  RS_Status status;
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
      } else {
        g_eventLogger->info("[TTL SWatcher] Remove[1] TTL of table %s.%s "
                             "in cache: [%u, %u@%u]",
                             db.c_str(), table.c_str(), iter->second.table_id,
                             iter->second.ttl_sec, iter->second.col_no);
        ttl_cache_.erase(iter);
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
        updated = true;
      } else {
        // check mysql.ttl_purge_nodes
        // TODO(zhao): handle ttl_purge_tables as well
        if (db == kSystemDBName && table == kTTLPurgeNodesTabName) {
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
      updated = true;
    } else {
      // check mysql.ttl_purge_nodes
      // TODO(zhao): handle ttl_purge_tables as well
      if (db == kSystemDBName && table == kTTLPurgeNodesTabName) {
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
  return updated;
}

enum SpecialShardVal {
  kShardNotPurger = -2,
  kShardNosharding = -1,
  kShardFirst = 0
};

/*
static const uint mon_lengths[2][MONS_PER_YEAR] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};
static const uint year_lengths[2] = {DAYS_PER_NYEAR, DAYS_PER_LYEAR};
*/
static const uint mon_starts[2][MONS_PER_YEAR] = {
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335}};
#define LEAPS_THRU_END_OF(y) ((y) / 4 - (y) / 100 + (y) / 400)
static my_time_t sec_since_epoch(int year, int mon, int mday, int hour, int min,
                                 int sec) {
  assert(mon > 0 && mon < 13 && year <= 9999);
  my_time_t days = year * DAYS_PER_NYEAR - EPOCH_YEAR * DAYS_PER_NYEAR +
                   LEAPS_THRU_END_OF(year - 1) -
                   LEAPS_THRU_END_OF(EPOCH_YEAR - 1);
  days += mon_starts[isleap(year)][mon - 1];
  days += mday - 1;

  const my_time_t result =
      ((days * HOURS_PER_DAY + hour) * MINS_PER_HOUR + min) * SECS_PER_MIN +
      sec;
  return result;
}
static my_time_t sec_since_epoch(const MYSQL_TIME &mt) {
  return sec_since_epoch(static_cast<int>(mt.year), static_cast<int>(mt.month),
                         static_cast<int>(mt.day), static_cast<int>(mt.hour),
                         static_cast<int>(mt.minute),
                         static_cast<int>(mt.second));
}

void TTLPurger::PurgeWorkerJob() {
  bool purge_trx_started = false;
  bool update_objects = false;
  std::map<std::string, TTLInfo> local_ttl_cache;
  Int32 shard = -1;
  Int32 n_purge_nodes = 0;
  unsigned char encoded_now[8];
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

  NdbDictionary::Dictionary* dict = nullptr;
  const NdbDictionary::Table* ttl_tab = nullptr;
  const NdbDictionary::Index* ttl_index = nullptr;
  Uint64 start_time = 0;
  Uint64 end_time = 0;
  bool sleep_between_each_round = true;
  NdbTransaction* trans = nullptr;
  NdbScanOperation* scan_op = nullptr;
  Int64 packed_last = 0;
  unsigned char encoded_last[8];
  unsigned char encoded_curr_purge[8];
  MYSQL_TIME datetime;
  Int64 packed_now = 0;
  NdbRecAttr* rec_attr[3];
  bool use_index = false;

  g_eventLogger->info("[TTL PWorker] Started");
  purged_pos_.clear();
  do {
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
          goto err;
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
      local_ttl_cache.clear();
      purged_pos_.clear();
      const std::lock_guard<std::mutex> lock(mutex_);
      local_ttl_cache = ttl_cache_;
      cache_updated_ = false;
      update_objects = true;
      g_eventLogger->info("[TTL PWorker] Detected cache updated, "
                           "reloaded %lu TTL tables",
                           local_ttl_cache.size());
    }

    shard = kShardNosharding;
    n_purge_nodes = 0;
    if (GetShard(&shard, &n_purge_nodes, update_objects) == false) {
      g_eventLogger->info("[TTL PWorker] Failed to get shard, "
                          "error: %u(%s). Retry...",
                          watcher_ndb_->getNdbError().code,
                          watcher_ndb_->getNdbError().message);
      goto err;
    }
    if (shard == kShardNotPurger) {
      g_eventLogger->info("Not the configured purging node, skip purging...");
      sleep(2);
      continue;
    }

    GetNow(encoded_now, false);
    if (shard >= kShardFirst && !UpdateLease(encoded_now)) {
      g_eventLogger->warning("[TTL PWorker] Failed to update the lease");
      goto err;
    }

    if (local_ttl_cache.empty()) {
      // No TTL table is found
      sleep(2);
      continue;
    }

    sleep_between_each_round = true;
    dict = worker_ndb_->getDictionary();
    for (iter = local_ttl_cache.begin(); iter != local_ttl_cache.end();
         iter++) {
      if (purge_worker_exit_) {
        break;
      }
      purge_trx_started = false;
      {
        GetNow(encoded_now, false);
        if (shard >= kShardFirst && !UpdateLease(encoded_now)) {
          g_eventLogger->warning("[TTL PWorker] Failed to update the lease[2]");
          goto err;
        }
      }
      if (cache_updated_) {
        break;
      }

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
        goto err;
      }
      ttl_tab = dict->getTable(table_str.c_str());
      if (ttl_tab == nullptr) {
        g_eventLogger->warning("[TTL PWorker] Failed to get table: "
                              "%s, error: %d(%s). Retry...",
                               table_str.c_str(),
                               dict->getNdbError().code,
                               dict->getNdbError().message);
        goto err;
      }
      table_id = ttl_tab->getTableId();
      hash_val = murmur3_32(reinterpret_cast<unsigned char*>(&table_id),
                                             sizeof(int), 0);
        if (shard >= kShardFirst && n_purge_nodes > 0 &&
          hash_val % n_purge_nodes != static_cast<Uint32>(shard)) {
        continue;
      }
      log_buf += ("[P" + std::to_string(iter->second.part_id) +
                 "/" +
                 std::to_string(ttl_tab->getPartitionCount()) + "]");
      assert(iter->second.part_id < ttl_tab->getPartitionCount());

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
        g_eventLogger->warning("[TTL PWorker] Failed to get table: "
                              "%s, error: %d(%s). Retry...",
                               table_str.c_str(),
                               dict->getNdbError().code,
                               dict->getNdbError().message);
        goto err;
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
          goto err;
        }
        index_scan_op->setPartitionId(iter->second.part_id);
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
          goto err;
        }

        log_buf += "-[";
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
          log_buf += std::to_string(TIME_to_ulonglong_datetime(datetime));
          if (type_timestamp) {
            MYSQL_TIME tmp;
            TIME_from_longlong_datetime_packed(&tmp, packed_last);
            my_timeval tm = {sec_since_epoch(tmp), 0};
            my_timestamp_to_binary(&tm, encoded_last, 0);
          } else {
            my_datetime_packed_to_binary(packed_last, encoded_last, 0);
          }
        } else {
          memset(encoded_last, 0, 8);
          log_buf += "INF";
        }
        log_buf += " --- ";
        packed_now = GetNow(encoded_now, type_timestamp);
        TIME_from_longlong_datetime_packed(&datetime, packed_now);
        log_buf += std::to_string(TIME_to_ulonglong_datetime(datetime));
        log_buf += ")";

        if (index_scan_op->setBound(ttl_col_index->getName(),
                            NdbIndexScanOperation::BoundLE,
                            encoded_last)) {
          g_eventLogger->warning("[TTL PWorker] Failed to setBound "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
          goto err;
        }
        if (index_scan_op->setBound(ttl_col_index->getName(),
                            NdbIndexScanOperation::BoundGT, encoded_now)) {
          g_eventLogger->warning("[TTL PWorker] Failed to setBound "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
          goto err;
        }
        rec_attr[0] = index_scan_op->getValue(ttl_col_no);
        if (rec_attr[0] == nullptr) {
          g_eventLogger->warning("[TTL PWorker] Failed to getValue "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
          goto err;
        }
        if (trans->execute(NdbTransaction::NoCommit) != 0) {
          g_eventLogger->warning("[TTL PWorker] Failed to execute transaction "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
          goto err;
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
              struct tm tmp_tm;
              const time_t tmp_t = (time_t)timestamp.m_tv_sec;
              gmtime_r(&tmp_t, &tmp_tm);
              MYSQL_TIME tmp;
              if (tmp_tm.tm_year <= 0) {
                tmp.year = 0;
                tmp.month = 0;
                tmp.day = 0;
                tmp.hour = 0;
                tmp.minute = 0;
                tmp.second = 0;
                tmp.second_part = 0;
                tmp.time_type = MYSQL_TIMESTAMP_DATETIME;
              }
              localtime_to_TIME(&tmp, &tmp_tm);
              tmp.time_type = MYSQL_TIMESTAMP_DATETIME;
              if (tmp.second == 60 || tmp.second == 61) {
                tmp.second = 59;
              }
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
              goto err;
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
            goto err;
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
            iter->second.batch_size = kPurgeBatchSize;
            g_eventLogger->warning("[TTL PWorker] Changed the purgine batch "
                                   "size of table %s to the minimum size %u, "
                                   "Retry...",
                                   ttl_tab->getName(),
                                   iter->second.batch_size);
          }
          goto err;
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
            iter->second.batch_size = kPurgeBatchSize;
            g_eventLogger->warning("[TTL PWorker] Changed the purgine batch "
                                   "size of table %s to the minimum size %u, "
                                   "Retry...",
                                   ttl_tab->getName(),
                                   iter->second.batch_size);
          }
          goto err;
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
          goto err;
        }
        scan_op->setPartitionId(iter->second.part_id);
        Uint32 scanFlags = NdbScanOperation::SF_OnlyExpiredScan;
        if (scan_op->readTuples(NdbOperation::LM_Exclusive, scanFlags,
                                1, iter->second.batch_size) != 0) {
          g_eventLogger->warning("[TTL PWorker] Failed to readTuples "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
          goto err;
        }
        rec_attr[0] = scan_op->getValue(ttl_col_no);
        if (rec_attr[0] == nullptr) {
          g_eventLogger->warning("[TTL PWorker] Failed to getValue "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
          goto err;
        }
        if (trans->execute(NdbTransaction::NoCommit) != 0) {
          g_eventLogger->warning("[TTL PWorker] Failed to execute transaction "
                                 "on table %s"
                                 ", error: %d(%s). Retry...",
                                 ttl_tab->getName(),
                                 trans->getNdbError().code,
                                 trans->getNdbError().message);
          goto err;
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
              goto err;
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
            goto err;
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
            iter->second.batch_size = kPurgeBatchSize;
            g_eventLogger->warning("[TTL PWorker] Changed the purgine batch "
                                   "size of table %s to the minimum size %u, "
                                   "Retry...",
                                   ttl_tab->getName(),
                                   iter->second.batch_size);
          }
          goto err;
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
            iter->second.batch_size = kPurgeBatchSize;
            g_eventLogger->warning("[TTL PWorker] Changed the purgine batch "
                                   "size of table %s to the minimum size %u, "
                                   "Retry...",
                                   ttl_tab->getName(),
                                   iter->second.batch_size);
          }
          goto err;
        }
      } else {
        g_eventLogger->warning("[TTL PWorker] Failed to get Table/Index "
                               "object on table %s"
                               ", error: %d(%s). Retry...",
                               table_str.c_str(),
                               dict->getNdbError().code,
                               dict->getNdbError().message);
        goto err;
      }
      worker_ndb_->closeTransaction(trans);
      trans = nullptr;
      log_buf += " Purged " + std::to_string(deletedRows) + " rows";
      g_eventLogger->info("%s", log_buf.c_str());
      end_time = my_micro_time();

      iter->second.batch_size = AdjustBatchSize(iter->second.batch_size,
                                                deletedRows,
                                                end_time - start_time);
      if (sleep_between_each_round &&
          iter->second.batch_size == kMaxPurgeBatchSize) {
        // At least 1 table finished its batch purging in the max size,
        // so don't sleep and start the next round as soon as possible
        sleep_between_each_round = false;
      }

      iter->second.part_id =
        ((iter->second.part_id + 1) % ttl_tab->getPartitionCount());
      // Finish 1 batch
      // keep the ttl_tab in local table cache ?
      continue;
err:
      if (trans != nullptr) {
        worker_ndb_->closeTransaction(trans);
        trans = nullptr;
      }
      trx_failure_times++;
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
              dict->removeCachedTable(table.c_str());
              dict->invalidateIndex(kTTLPurgeIndexName, table.c_str());
            }
          }
        }
        purge_worker_asks_for_retry_ = true;
        purge_worker_exit_ = true;
        break;
      } else if (purge_trx_started) {
        dict->removeCachedTable(table_str.c_str());
        goto retry_trx;
      } else {
        // retry from begining
        break;  // jump out from for-loop
      }
    }
    // Finish 1 round
    if (sleep_between_each_round) {
      // Sleep for 1000 - 2000 ms after finishing each round
      RandomSleep(1000, 2000);
    }
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
  bool check = 0;
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
  g_eventLogger->info("%s", log_buf.c_str());
  return true;

err:
  if (trans != nullptr) {
    worker_ndb_->closeTransaction(trans);
  }
  return false;
}

Int64 TTLPurger::GetNow(unsigned char* encoded_now, bool timestamp) {
  assert(encoded_now != nullptr);
  Int64 packed_now = 0;
  memset(encoded_now, 0, 8);
  MYSQL_TIME curr_dt;
  struct tm tmp_tm;
  time_t t_now = (time_t)my_micro_time() / 1000000; /* second */
  gmtime_r(&t_now, &tmp_tm);
  curr_dt.neg = false;
  curr_dt.second_part = 0;
  curr_dt.year = ((tmp_tm.tm_year + 1900) % 10000);
  curr_dt.month = tmp_tm.tm_mon + 1;
  curr_dt.day = tmp_tm.tm_mday;
  curr_dt.hour = tmp_tm.tm_hour;
  curr_dt.minute = tmp_tm.tm_min;
  curr_dt.second = tmp_tm.tm_sec;
  curr_dt.time_zone_displacement = 0;
  curr_dt.time_type = MYSQL_TIMESTAMP_DATETIME;
  if (curr_dt.second == 60 || curr_dt.second == 61) {
    curr_dt.second = 59;
  }
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
  op->updateTuple();
  op->equal("node_id", worker_ndb_->getNodeId());
  op->setValue("last_active", reinterpret_cast<const char*>(encoded_now));

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
  struct tm tmp_tm;
  time_t t_now = (time_t)my_micro_time() / 1000000; /* second */
  gmtime_r(&t_now, &tmp_tm);
  curr_dt.neg = false;
  curr_dt.second_part = 0;
  curr_dt.year = ((tmp_tm.tm_year + 1900) % 10000);
  curr_dt.month = tmp_tm.tm_mon + 1;
  curr_dt.day = tmp_tm.tm_mday;
  curr_dt.hour = tmp_tm.tm_hour;
  curr_dt.minute = tmp_tm.tm_min;
  curr_dt.second = tmp_tm.tm_sec;
  curr_dt.time_zone_displacement = 0;
  curr_dt.time_type = MYSQL_TIMESTAMP_DATETIME;
  if (curr_dt.second == 60 || curr_dt.second == 61) {
    curr_dt.second = 59;
  }

  int res = my_time_compare(last_active_dt, curr_dt);
  if (res >= 0) {
    return true;
  } else {
    return false;
  }
}

void* TTLPurger::_SchemaWatcherJob(void* arg) {
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
                                 Uint64 used_time) {
  if (deleted_rows == curr_batch_size && used_time < kPurgeThresholdTime) {
      if (curr_batch_size + kPurgeBatchSizePerIncr <= kMaxPurgeBatchSize) {
        // Increase
        return curr_batch_size + kPurgeBatchSizePerIncr;
      } else {
        // Keep as max
        assert(curr_batch_size == kMaxPurgeBatchSize);
        return curr_batch_size;
      }
  } else if (curr_batch_size - kPurgeBatchSizePerIncr >= kPurgeBatchSize) {
    // Decrease
    return curr_batch_size - kPurgeBatchSizePerIncr;
  } else {
    // Keep as min
    assert(curr_batch_size == kPurgeBatchSize);
    return curr_batch_size;
  }
}
