/*
 * Copyright (C) 2024 Hopsworks AB
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

#include "../src/api_key.hpp"
#include "../src/config_structs.hpp"
#include "connection.hpp"
#include "resources/seed_check.hpp"
#include "resources/test_constants.hpp"
#include "rdrs_dal.h"
#include "rdrs_hopsworks_dal.h"
#include "rdrs_rondb_connection.hpp"
#include "rdrs_rondb_connection_pool.hpp"
#include <NdbMutex.h>
#include <my_time.h>

#include <functional>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <NdbSleep.h>
#include <ctime>
#include <random>

NdbMutex *globalConfigsMutex = nullptr;
template <typename T> class SafeQueue {
 private:
  std::queue<T> queue_;
  std::mutex mutex_;
  std::condition_variable cond_;

 public:
  void push(T value) {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push(std::move(value));
    cond_.notify_one();
  }

  T pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [this] { return !queue_.empty(); });
    T value = std::move(queue_.front());
    queue_.pop();
    return value;
  }
};

APIKeyCache *apiKeyCachePtr = nullptr;

extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;
const std::string HOPSWORKS_TEST_API_KEY =
    "bkYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub";

// NDB varchar helpers: prepare length-prefixed buffer for setValue()
static void prepare_short_varchar(char *buf, const char *str, size_t len) {
  buf[0] = (char)len;
  memcpy(buf + 1, str, len);
}

static void prepare_medium_varchar(char *buf, const char *str, size_t len) {
  buf[0] = (char)(len & 0xFF);
  buf[1] = (char)((len >> 8) & 0xFF);
  memcpy(buf + 2, str, len);
}

// Insert a row into hopsworks.api_key via NDB API (for event tests)
// Encode a unix epoch as the 5-byte packed DATETIME(0) NDB storage format
static void encode_datetime(time_t epoch, unsigned char *buf) {
  struct tm local_tm;
  localtime_r(&epoch, &local_tm);
  MYSQL_TIME mysql_time;
  memset(&mysql_time, 0, sizeof(mysql_time));
  mysql_time.year = local_tm.tm_year + 1900;
  mysql_time.month = local_tm.tm_mon + 1;
  mysql_time.day = local_tm.tm_mday;
  mysql_time.hour = local_tm.tm_hour;
  mysql_time.minute = local_tm.tm_min;
  mysql_time.second = local_tm.tm_sec;
  mysql_time.time_type = MYSQL_TIMESTAMP_DATETIME;
  longlong packed = TIME_to_longlong_datetime_packed(mysql_time);
  my_datetime_packed_to_binary(packed, buf, 0);
}

static bool ndb_insert_api_key(int id,
                                const char *prefix,
                                const char *secret,
                                const char *salt,
                                const char *name,
                                int user_id,
                                time_t expiry_epoch = 0) {
  Ndb *ndb = nullptr;
  RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
  if (rs.http_code != SUCCESS) {
    std::cerr << "Failed to get NDB object" << std::endl;
    return false;
  }

  if (ndb->setDatabaseName(HOPSWORKS) != 0) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  const NdbDictionary::Table *tab = ndb->getDictionary()->getTable(API_KEY);
  if (tab == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbTransaction *tx = ndb->startTransaction();
  if (tx == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbOperation *op = tx->getNdbOperation(tab);
  if (op == nullptr || op->insertTuple() != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // PK: id (int)
  if (op->equal("id", id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // prefix (varchar(45) - short var: 1-byte length prefix)
  char prefix_buf[API_KEY_PREFIX_SIZE];
  memset(prefix_buf, 0, sizeof(prefix_buf));
  prepare_short_varchar(prefix_buf, prefix, strlen(prefix));
  if (op->setValue("prefix", prefix_buf) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // secret (varchar(512) - medium var: 2-byte length prefix)
  char secret_buf[API_KEY_SECRET_SIZE];
  memset(secret_buf, 0, sizeof(secret_buf));
  prepare_medium_varchar(secret_buf, secret, strlen(secret));
  if (op->setValue("secret", secret_buf) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // salt (varchar(256) - medium var: 2-byte length prefix)
  char salt_buf[API_KEY_SALT_SIZE];
  memset(salt_buf, 0, sizeof(salt_buf));
  prepare_medium_varchar(salt_buf, salt, strlen(salt));
  if (op->setValue("salt", salt_buf) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // name (varchar(45) - short var: 1-byte length prefix)
  char name_buf[API_KEY_NAME_SIZE];
  memset(name_buf, 0, sizeof(name_buf));
  prepare_short_varchar(name_buf, name, strlen(name));
  if (op->setValue("name", name_buf) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // user_id (int)
  if (op->setValue("user_id", user_id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // created (timestamp - 4-byte seconds since epoch)
  Uint32 now = (Uint32)time(nullptr);
  if (op->setValue("created", now) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // modified (timestamp)
  if (op->setValue("modified", now) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // reserved (tinyint, DEFAULT 0)
  if (op->setValue("reserved", (Int32)0) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // expiry (nullable DATETIME) - left NULL (= never expires) unless given
  if (expiry_epoch != 0) {
    unsigned char expiry_buf[8];
    memset(expiry_buf, 0, sizeof(expiry_buf));
    encode_datetime(expiry_epoch, expiry_buf);
    if (op->setValue("expiry", (const char *)expiry_buf) != 0) {
      ndb->closeTransaction(tx);
      rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
      return false;
    }
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    std::cerr << "NDB insert failed: " << tx->getNdbError().code
              << " " << tx->getNdbError().message << std::endl;
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  ndb->closeTransaction(tx);
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  return true;
}

// Update the expiry column of a row in hopsworks.api_key by PK (id) via NDB API
static bool ndb_update_api_key_expiry(int id, time_t expiry_epoch) {
  Ndb *ndb = nullptr;
  RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
  if (rs.http_code != SUCCESS) {
    std::cerr << "Failed to get NDB object" << std::endl;
    return false;
  }

  if (ndb->setDatabaseName(HOPSWORKS) != 0) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  const NdbDictionary::Table *tab = ndb->getDictionary()->getTable(API_KEY);
  if (tab == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbTransaction *tx = ndb->startTransaction();
  if (tx == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbOperation *op = tx->getNdbOperation(tab);
  if (op == nullptr || op->updateTuple() != 0 || op->equal("id", id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  unsigned char expiry_buf[8];
  memset(expiry_buf, 0, sizeof(expiry_buf));
  encode_datetime(expiry_epoch, expiry_buf);
  if (op->setValue("expiry", (const char *)expiry_buf) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    std::cerr << "NDB update failed: " << tx->getNdbError().code
              << " " << tx->getNdbError().message << std::endl;
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  ndb->closeTransaction(tx);
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  return true;
}

// Update the secret column of a row in hopsworks.api_key by PK (id) via NDB API
static bool ndb_update_api_key_secret(int id, const char *new_secret) {
  Ndb *ndb = nullptr;
  RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
  if (rs.http_code != SUCCESS) {
    std::cerr << "Failed to get NDB object" << std::endl;
    return false;
  }

  if (ndb->setDatabaseName(HOPSWORKS) != 0) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  const NdbDictionary::Table *tab = ndb->getDictionary()->getTable(API_KEY);
  if (tab == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbTransaction *tx = ndb->startTransaction();
  if (tx == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbOperation *op = tx->getNdbOperation(tab);
  if (op == nullptr || op->updateTuple() != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // PK: id
  if (op->equal("id", id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // secret (varchar(512) - medium var: 2-byte length prefix)
  char secret_buf[API_KEY_SECRET_SIZE];
  memset(secret_buf, 0, sizeof(secret_buf));
  prepare_medium_varchar(secret_buf, new_secret, strlen(new_secret));
  if (op->setValue("secret", secret_buf) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    std::cerr << "NDB update failed: " << tx->getNdbError().code
              << " " << tx->getNdbError().message << std::endl;
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  ndb->closeTransaction(tx);
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  return true;
}

// Delete a row from hopsworks.api_key by primary key (id) via NDB API
static bool ndb_delete_api_key_by_id(int id) {
  Ndb *ndb = nullptr;
  RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
  if (rs.http_code != SUCCESS) {
    std::cerr << "Failed to get NDB object" << std::endl;
    return false;
  }

  if (ndb->setDatabaseName(HOPSWORKS) != 0) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  const NdbDictionary::Table *tab = ndb->getDictionary()->getTable(API_KEY);
  if (tab == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbTransaction *tx = ndb->startTransaction();
  if (tx == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbOperation *op = tx->getNdbOperation(tab);
  if (op == nullptr || op->deleteTuple() != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (op->equal("id", id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    std::cerr << "NDB delete failed: " << tx->getNdbError().code
              << " " << tx->getNdbError().message << std::endl;
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  ndb->closeTransaction(tx);
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  return true;
}

// Insert a row into hopsworks.shared_feature_store via NDB API
// (for feature-store sharing tests)
static bool ndb_insert_shared_feature_store(int id,
                                            int feature_store_id,
                                            int shared_with_project,
                                            int shared_entirely) {
  Ndb *ndb = nullptr;
  RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
  if (rs.http_code != SUCCESS) {
    std::cerr << "Failed to get NDB object" << std::endl;
    return false;
  }

  if (ndb->setDatabaseName(HOPSWORKS) != 0) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  const NdbDictionary::Table *tab =
      ndb->getDictionary()->getTable(SHARED_FEATURE_STORE);
  if (tab == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbTransaction *tx = ndb->startTransaction();
  if (tx == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbOperation *op = tx->getNdbOperation(tab);
  if (op == nullptr || op->insertTuple() != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // PK: id (int)
  if (op->equal("id", id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // feature_store (int, FK -> feature_store.id)
  if (op->setValue("feature_store", feature_store_id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // shared_by (int, FK -> users.uid): the api key user macho
  if (op->setValue("shared_by", (Int32)10000) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // shared_on (timestamp - 4-byte seconds since epoch)
  Uint32 now = (Uint32)time(nullptr);
  if (op->setValue("shared_on", now) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // shared_with_project (int, FK -> project.id)
  if (op->setValue("shared_with_project", shared_with_project) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // shared_entirely (tinyint)
  if (op->setValue("shared_entirely", (Int32)shared_entirely) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    std::cerr << "NDB insert failed: " << tx->getNdbError().code
              << " " << tx->getNdbError().message << std::endl;
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  ndb->closeTransaction(tx);
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  return true;
}

// Delete a row from hopsworks.shared_feature_store by primary key (id)
static bool ndb_delete_shared_feature_store(int id) {
  Ndb *ndb = nullptr;
  RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
  if (rs.http_code != SUCCESS) {
    std::cerr << "Failed to get NDB object" << std::endl;
    return false;
  }

  if (ndb->setDatabaseName(HOPSWORKS) != 0) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  const NdbDictionary::Table *tab =
      ndb->getDictionary()->getTable(SHARED_FEATURE_STORE);
  if (tab == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbTransaction *tx = ndb->startTransaction();
  if (tx == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbOperation *op = tx->getNdbOperation(tab);
  if (op == nullptr || op->deleteTuple() != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (op->equal("id", id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    std::cerr << "NDB delete failed: " << tx->getNdbError().code
              << " " << tx->getNdbError().message << std::endl;
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  ndb->closeTransaction(tx);
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  return true;
}

// Shared plumbing for the fine-grained grant-table rows: insert one row
// with integer PK 'id'; set_columns sets the non-PK columns on the
// operation and returns 0 on success.
static bool ndb_insert_row(const char *table_name,
                           int id,
                           const std::function<int(NdbOperation*)> &set_columns) {
  Ndb *ndb = nullptr;
  RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
  if (rs.http_code != SUCCESS) {
    std::cerr << "Failed to get NDB object" << std::endl;
    return false;
  }
  bool ok = false;
  NdbTransaction *tx = nullptr;
  do {
    if (ndb->setDatabaseName(HOPSWORKS) != 0) break;
    const NdbDictionary::Table *tab =
        ndb->getDictionary()->getTable(table_name);
    if (tab == nullptr) break;
    tx = ndb->startTransaction();
    if (tx == nullptr) break;
    NdbOperation *op = tx->getNdbOperation(tab);
    if (op == nullptr || op->insertTuple() != 0) break;
    if (op->equal("id", id) != 0) break;
    if (set_columns(op) != 0) break;
    if (tx->execute(NdbTransaction::Commit) != 0) {
      std::cerr << "NDB insert into " << table_name << " failed: "
                << tx->getNdbError().code << " "
                << tx->getNdbError().message << std::endl;
      break;
    }
    ok = true;
  } while (false);
  if (tx != nullptr) {
    ndb->closeTransaction(tx);
  }
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  return ok;
}

// Delete one row by integer PK 'id' (fine-grained grant tables)
static bool ndb_delete_row(const char *table_name, int id) {
  Ndb *ndb = nullptr;
  RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
  if (rs.http_code != SUCCESS) {
    std::cerr << "Failed to get NDB object" << std::endl;
    return false;
  }
  bool ok = false;
  NdbTransaction *tx = nullptr;
  do {
    if (ndb->setDatabaseName(HOPSWORKS) != 0) break;
    const NdbDictionary::Table *tab =
        ndb->getDictionary()->getTable(table_name);
    if (tab == nullptr) break;
    tx = ndb->startTransaction();
    if (tx == nullptr) break;
    NdbOperation *op = tx->getNdbOperation(tab);
    if (op == nullptr || op->deleteTuple() != 0) break;
    if (op->equal("id", id) != 0) break;
    if (tx->execute(NdbTransaction::Commit) != 0) break;
    ok = true;
  } while (false);
  if (tx != nullptr) {
    ndb->closeTransaction(tx);
  }
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  return ok;
}

// hopsworks.shared_feature_group: FG shared with a project, whole
// (shared_entirely = 1) or feature-wise (0, columns in shared_feature)
static bool ndb_insert_shared_feature_group(int id,
                                            int feature_store_id,
                                            int feature_group_id,
                                            int shared_with_project,
                                            int shared_entirely) {
  return ndb_insert_row(SHARED_FEATURE_GROUP, id, [&](NdbOperation *op) {
    Uint32 now = (Uint32)time(nullptr);
    return op->setValue("feature_store", feature_store_id) |
           op->setValue("feature_group", feature_group_id) |
           op->setValue("shared_by", (Int32)10000) |
           op->setValue("shared_on", now) |
           op->setValue("shared_with_project", shared_with_project) |
           op->setValue("shared_entirely", (Int32)shared_entirely);
  });
}

// hopsworks.shared_feature: one column of a feature-wise FG share
static bool ndb_insert_shared_feature(int id,
                                      int feature_group_id,
                                      const char *feature,
                                      int shared_with_project) {
  return ndb_insert_row(SHARED_FEATURE, id, [&](NdbOperation *op) {
    char feature_buf[SHARED_FEATURE_NAME_SIZE];
    memset(feature_buf, 0, sizeof(feature_buf));
    prepare_short_varchar(feature_buf, feature, strlen(feature));
    Uint32 now = (Uint32)time(nullptr);
    return op->setValue("feature_group", feature_group_id) |
           op->setValue("feature", feature_buf) |
           op->setValue("shared_by", (Int32)10000) |
           op->setValue("shared_on", now) |
           op->setValue("shared_with_project", shared_with_project);
  });
}

// hopsworks.restricted_feature_group_access: per-user FG grant, whole
// (can_access_entirely = 1) or feature-wise (0, columns in
// restricted_feature_access)
static bool ndb_insert_restricted_feature_group_access(int id,
                                                       int feature_store_id,
                                                       int feature_group_id,
                                                       int granted_to_user,
                                                       int can_access_entirely) {
  return ndb_insert_row(RESTRICTED_FEATURE_GROUP_ACCESS, id,
                        [&](NdbOperation *op) {
    Uint32 now = (Uint32)time(nullptr);
    return op->setValue("feature_store", feature_store_id) |
           op->setValue("feature_group", feature_group_id) |
           op->setValue("granted_by", (Int32)10000) |
           op->setValue("granted_on", now) |
           op->setValue("granted_to_user", granted_to_user) |
           op->setValue("can_access_entirely", (Int32)can_access_entirely);
  });
}

class APIKeyTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    printf("Set up TestSuite\n");
    RequireSeededTestDatabases();
    RS_Status status = RonDBConnection::init_rondb_connection(globalConfigs.ronDB,
                                                              globalConfigs.ronDBMetadataCluster,
                                                              4);
    if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
  }

  static void TearDownTestSuite() {
    stop_api_key_cache();
    RS_Status status = RonDBConnection::shutdown_rondb_connection();
    if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
    apiKeyCachePtr = nullptr;
  }
};

class MyEnvironment : public ::testing::Environment {
 public:
  ~MyEnvironment() override {}

  // Override this to define how to set up the environment.
  void SetUp() override
  {
  }

  // Override this to define how to tear down the environment.
  void TearDown() override
  {
  }
};

void test_key(APIKeyCache *cache) {
  const std::string existentDB = DB001;
  const std::string existentDB2 = DB002;
  const std::string fakeDB = "test3";

  std::string apiKey =
      "bkYjEz6OTZyevbqT.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub";
  RS_Status status = cache->validate_api_key(apiKey, {existentDB});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Wrong prefix was falsely validated";

  apiKey = "bkYjEz6OTZyevbqT";
  status = cache->validate_api_key(apiKey, std::vector<std::string_view>{});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Missing secret was falsely validated";

  apiKey = "bkYjEz6OTZyevbq.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub";
  status = cache->validate_api_key(apiKey, std::vector<std::string_view>{});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Wrong length prefix was falsely validated";

  // correct api key but wrong db. this api key can not access test3 db
  status = cache->validate_api_key(HOPSWORKS_TEST_API_KEY, {fakeDB});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Inexistent database was falsely validated";

  // correct api key
  status = cache->validate_api_key(HOPSWORKS_TEST_API_KEY, {existentDB});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "No error expected; error: " << status.message;

  // no errors
  status = cache->validate_api_key(HOPSWORKS_TEST_API_KEY, {existentDB, existentDB2});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "No error expected; error: " << status.message;
}

TEST_F(APIKeyTest, TestAPIKey1) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }
  test_key(apiKeyCachePtr);
  stop_api_key_cache();
}

// Verify preload populates cache and preloaded keys validate
TEST_F(APIKeyTest, TestPreload) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  apiKeyCachePtr->preload_all_keys();
  EXPECT_GT(apiKeyCachePtr->size(), 0u)
      << "Cache should have preloaded entries";

  // Verify the known test key works after preload (no lazy load needed)
  RS_Status status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {DB001});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Preloaded key should validate: " << status.message;

  // Verify unauthorized DB still fails
  status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {"nonexistent_db"});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Unauthorized DB should be rejected";

  // Verify generated keys validate after preload
  std::ostringstream oss;
  oss << std::setw(16) << std::setfill('0') << 0 << "." << HopsworksAPIKey_SECRET;
  std::string generatedKey = oss.str();
  status = apiKeyCachePtr->validate_api_key(generatedKey, {DB001});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Preloaded generated key should validate: " << status.message;

  stop_api_key_cache();
}

void test_update_cache_every_n_seconds(APIKeyCache *cache,
                                       const AllConfigs &conf) {
  std::string apiKey = HOPSWORKS_TEST_API_KEY;
  std::vector<std::string_view> databases = {DB001, DB002};

  RS_Status status = cache->validate_api_key(apiKey, databases);
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "No error expected; error: " << status.message;

  auto lastUpdated1 = cache->last_updated(apiKey);
  // waiting 2 * cacheRefreshIntervalMS to ensure the update trigger has run
  NdbSleep_MilliSleep(2 * (conf.security.apiKey.cacheRefreshIntervalMS));
  auto lastUpdated2 = cache->last_updated(apiKey);
  EXPECT_NE(lastUpdated1, lastUpdated2) << "Cache entry was not updated";

  status = cache->validate_api_key(apiKey, databases);
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "No error expected; error: " << status.message;
}

// Check that cache is updated every N secs
TEST_F(APIKeyTest, TestAPIKeyCache1) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  // To speed up the tests
  conf.security.apiKey.cacheRefreshIntervalMS       = 1000;
  auto status = AllConfigs::set_all(conf);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << status.message;
  }
  apiKeyCachePtr->start_background_threads();
  test_update_cache_every_n_seconds(apiKeyCachePtr, conf);
  stop_api_key_cache();
}

void test_update_cache_every_n_seconds_unauthorized(APIKeyCache *cache,
                                                    const AllConfigs &conf) {
  std::string apiKey    = HOPSWORKS_TEST_API_KEY;
  const std::string db3 = "test3";

  RS_Status status = cache->validate_api_key(apiKey, {db3});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Database should not exist. Expected test to fail";

  auto lastUpdated1 = cache->last_updated(apiKey);
  // waiting 2 * cacheRefreshIntervalMS to ensure the update trigger has run
  NdbSleep_MilliSleep(2 * (conf.security.apiKey.cacheRefreshIntervalMS));
  auto lastUpdated2 = cache->last_updated(apiKey);
  EXPECT_NE(lastUpdated1, lastUpdated2) << "Cache entry was not updated";
}

// check that cache is updated every N secs even if the user is not authorized to access a DB
TEST_F(APIKeyTest, TestAPIKeyCache2) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  // To speed up the tests
  conf.security.apiKey.cacheRefreshIntervalMS       = 1000;
  auto status = AllConfigs::set_all(conf);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << status.message;
  }
  apiKeyCachePtr->start_background_threads();
  test_update_cache_every_n_seconds_unauthorized(apiKeyCachePtr, conf);
  stop_api_key_cache();
}

void test_load(APIKeyCache *cache, const AllConfigs &conf) {
  SafeQueue<bool> ch;
  int numOps = 150;

  std::vector<std::thread> producers;
  producers.reserve(numOps);
  for (int i = 0; i < numOps; ++i) {
    producers.push_back(std::thread([&ch, &cache] {
      NDB_TICKS now = NdbTick_getCurrentTicks();
      Uint32 now_uint32 = (Uint32)now.getUint64();
      std::mt19937 generator(now_uint32);
      std::uniform_int_distribution<int>
        distribution(0, HopsworksAPIKey_ADDITIONAL_KEYS - 1);
      int apiKeyNum = distribution(generator);
      std::ostringstream oss;
      oss << std::setw(16) << std::setfill('0') << apiKeyNum << "." << HopsworksAPIKey_SECRET;
      std::string apiKey = oss.str();

      auto DB = DB001;

      RS_Status status = cache->validate_api_key(apiKey, {DB});
      if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
        std::cout << "validation failed for api key: " << apiKey << "; error: " << status.message
                  << std::endl;
        ch.push(false);
      } else {
        ch.push(true);
      }
    }));
  }

  for (auto &producer : producers) {
    producer.join();
  }

	bool pass = true;
	int failCount = 0;
	for (int i = 0; i < numOps; ++i) {
		bool val = ch.pop();
		if (!val) {
			pass = false;
			failCount++;
		}
	}

  if (!pass) {
    EXPECT_EQ(failCount, 0) << failCount << " key validations failed" << std::endl;
  }

  // Valid keys exist in DB, so they should NOT be evicted — they get refreshed.
  // Wait for a refresh cycle and verify entries are still cached.
  NdbSleep_MilliSleep(2 * conf.security.apiKey.cacheRefreshIntervalMS);

  EXPECT_GT(cache->size(), 0u)
      << "Valid keys should remain cached (refreshed, not evicted)";
}

// Test load. Generate lots of api key requests
TEST_F(APIKeyTest, TestAPIKeyCache3) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  // To speed up the tests
  conf.security.apiKey.cacheRefreshIntervalMS       = 1000;
  auto status = AllConfigs::set_all(conf);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << status.message;
  }
  apiKeyCachePtr->start_background_threads();
  test_load(apiKeyCachePtr, conf);
  stop_api_key_cache();
}

void test_load_with_bad_keys(APIKeyCache *cache, const AllConfigs &conf) {
  std::vector<std::string> badKeys = {
      "fvoHJCjkpof4WezF.4eed386ceb310e9976932cb279de2dab70c24a1ceb396e99dd29df3a1348f42e",
      "vNizYZEsK7Ip1AEt.3eb997b041460f59b90094bd7d07b30e385c1c72961a440b74f9dccf5dd467b2",
      "ASLMR5E2fZj99Urc.4beb809a7191cfb3f301467ead5cf9be537b42f3535d0b0a3262a2de14f4972c",
      "gvTjnkN9sT4f8QKP.0cbc2086d518d57f676c1876e98d01e2672a373cc7e6f46358759bb81cf70f34",
      "uIZsxwh0iS0ChVO7.5ae7a415290ade873b52c8002bfb52d9dce4d5e5e4a78e9d4413208171348917",
      "uLlYekGkIb9yRfMh.9cd7beab8983364db2cf78c68ba253026e782eac4795f4d76520236097ea30df",
      "8OHWKsjFL2ek4I1a.41a50648077779db9e106d7218d5fa67b9fda35ce08d3aa3a661a24fcf0c9499",
      "ha3Vwv76A00xXRbZ.e77ec23d18045492deefcf5566b75170a9ecf5c928c86a4891f9964a73cab537",
      "pws6Pt15Kb0Y6z1l.b34c14ba29320e8b3018abe7fa3af4c6b112c2c842e0734ed8a2f5ef5cd6881d",
      "tukhMkclDr5RZdhI.04f736fe234d93f4e9082c11ebdea670830c473e1e4a4e9cd8d97d59719fffd7",
      "zPZaJdxZ1SmzoXHE.827f25efe88e8d11f471c45ceedaee60c3b221fa92e26af02b8da98c8d767a54",
      "ImpGCVclSGHU7ENR.357a422b918617611847bc30a8c5663e0353860f5904931ea947e25718aa8997",
      "84q6WR9bxFYsuEin.202baf9ab9fca52404b69d00de9e7f6f30c1c04ef7d85b2258fa2c73799fe808",
      "EejFn4BA7M1QgGbF.1865aeeb01ce392444291d0a0ffe00b73e61b50b8f22be0ac94a7b991c1a5d7d",
      "CK8zsPaQYmtw5ZsL.a23f9207b5f525c0a2abe941eb716e98d6b061ccb3e44347bb9ce950fdbf3c89",
      "6ILEK6QkvLcztbEy.74f73637ae78e00136028b1254bf11669ebfe8fa26d47527daaf7bc4d0645d7a",
      "4yIbzqCZkugp5R9w.4166d6cd74a81f63e1da70c2f7dd7504d575d112be5a152bff4656d2d82189a3",
      "fIt8oIE8rIaHYLYD.e25466322dabc65f20a40623997a9a46c0fdebee77b9106021c8c0bf1dd29799",
      "iSHXZKLzVu0tYmtD.2ae442c50f6d650ea57d2f6d622416520751a8a53420344d57765a16b9d62436",
      "ArExPZKJfLAJA4y1.7953b49f0a777064f6f5cc3c0671777716b50355228bb18b4d8b9a1fae7ac185",
      "pGgsePMZcnylvYE1.ec0cf03ff4adc48ece53eb2648d7c3af47f9d9758207420ef825d2b5c22daf48",
      "dUoyC2RkLVhXBhKy.93b4b371637571ca90199640c64de38d3a25a4439e6bd535b1ba72b74665c24e",
      "eueiglqaGyTYTnxR.be28d684c25e076ec8dba8fe4bfa90c4e47b92413dd96ad72e58ed3386f5b561",
      "14KAZwpg2WEBd495.863ebf824a434d8be02cf0b271f311519553e822029c3629a7c2ada725c19b28",
      "rexNdYKxC5tnSL4l.e9b3b6916afbe43197957dace3d27e7a46e54885a0528751558e3bc551afc1ba",
      "O1HZWxhysrY9Gyzm.aa69b2998f98978a67a381ae1676d34d1f7840755e690751fe89c8f3bfa675a0",
      "P7TPSTmZqvrcjr0i.1a39245511be48308032a53febd2c87aa1ab36f9d12035a7da823ab2cff686f4",
      "imvrGrF8xXcv56zQ.4d32e9a1c5c9fe63cb90aa633ff6a296ec5b247aba6146f2fefcfe3edd052022",
      "YQxxA7asFLjDY9x0.0a23d2a1a51df84c949e1ee6ae0ea1fb3318fd09672e416c935450b230cf17ef",
      "hA7AaiWER7p28qMq.643bf6192b31f8b265f6994d5164cc1b2ff8e2c7cc8fd36b002895e2b5e0e7bf",
      "OnIM4JFEfh1Tc6ch.1333f69c2dfd4ef135fe57e138ca5900728145023d3e5eb86b9730f6f4e8ca94",
      "OIjBkc4nQzd5SuHz.b48344e10b505f9305c6a61189595ec2e627a207b5440321c9911ca52cbe3d16",
      "uoiI97cfErZxeXw8.eb7c6aecf770dd535dff036ef72320bf579d4ea606152ad6ab2716b2134f971c",
      "BcA1y9pD57bqnHPd.f230a76e8f8f77b4671e544dac5eb43944d9e6e5f56bfeafe775715715f57b4b",
      "qPmErPtUzAzBdOT9.38d95dbfc76735c17f955f964e92194fc124c9788ed7cedcb99ab1389ceac92c",
      "G9WcEVdVBtzqEynP.b21fefa7b3432afcacfae759378d43448819e9164d79e4722d206f791c9663e8",
      "O3RE8z7yfzzvkLwo.f3551462e8527e32cdb315039927765d93f19d9408ce781316746af99ce134c0",
      "UtZSTaFo6IK5gIuC.e9fda4f5b56b28f6c4deecb8c774db18571f577e0864cc994e4ce00ffe17a7c7",
      "xdCROBN8T5d7t8dr.9846f9940299c7a2703e37a12932c88590f3ca89ad029d85b9b59916c2eb902c",
      "anpA9GY4kpafqhdO.d90892a2d8c016b6ffc8601b1ba33fdd13b48d6fdd6ca7803146ccb330cc44d6",
      "vENzHsp96BhTKxtF.048866d815f193d81e7d5c3bdd4ed538eb9593f119785886400ddb78201a205b",
      "hLBB0nDBpimZLcQN.76ce9177012e0295396c1cb8c15c7c2b91113b463bfbe7c7675de7912458558f",
      "YLlLUBDyUDopgPY3.458e9b6247234511b3ab325448c7ab34ab9ea2e629f9408fa9eed163f2a1d639",
      "0xUzLhYchZHHTMll.ad9855a6717dfd87c34527772d39074592b5207727bf2b07a98b2c28755eecb5",
      "ovPMBJunjS51C963.b974ce48886da6cf54c6302da39c13d709e190edcdd65700b8eb48cd5183a488",
      "pd8cxgroWc4T1Vd6.af85e2aa4fa580f8d8b99c255124a7ec1c7c42be4cbdb9c16468cd879843992c",
      "dxnLAe5cNkp48Vyt.1c2b1bb75b0f1a24207b6b1de635e308b65e05fb52eb77f21c6b9d9d2c5aba6b",
      "GtyBzOVXEGBRqlGf.1339a00fbf24c7ce677d6b40bf1009594c2a0b682af3f542ce71bd6fb84e779b",
      "63b9DnA2A6R9CRV7.48f7bb59fb0db3fdc6d2818b4ade575c1cb8813b9033bd977eca7225dc188bd0",
      "0WhhpThaapAuVp2Y.5960341683854056c4eeae2e602398d0439c1b5062e0708695f0ce381dcd2f06",
      "CCoCcTeCQeanHKAR.fcb734f7856079bc5253f039ca1e94b8027a419c5a57ae032a45a40c2c911ff9",
      "s1GtULCFzsEmFkWQ.e0214b01fe437b0b0eff5884dd76099f6e1b68dfa6584b2d5ea3cc376bf02204",
      "r2QT2DgVFv8ko5Ol.e8dd5fbcf1d0cb4a0b882fd106a204d4462adc00e242d000021f659fa7cbd87b",
      "iMZkMSMHQGkvtoGe.67ab1785c3b3d7059f1362b0b4a3a5446b2bb2d069ad146eee6f4d1a65020b08",
      "QEHHKBVmYaKNZOGK.79d6f65520e05119cfb698ce256348796217188071ef6f0e56c0fc8f75bae3c3",
      "mRog42LN7DWoXF8v.4240ff3c6844cb59a50070fc8baa14358a9820a3d142ba962180ced2856b9894",
      "IFTU5sCXXgUXT8TM.f5504e74563805978804a507eb55a6e111e83abe8f7897a64ac84918fd7185a1",
      "geWmjrtRW2PwvHnX.46387e49e624828aab1f70004f4e161da62c2fbcb65353681b2b9d6891b5e7cd",
      "P9aQKTyQ4MEqgpT7.67c1bfaaae9c36e681bd509c3f5605e3c8adb680a6e6a2f216e6b14eabc4f599",
      "WDxm9Cc5j74Rx2Q1.858edb638c1f4295f1b62353dd2c875b330b40e5b9dedbf0489dc76482c50beb",
  };

  SafeQueue<bool> ch;
  int numOps = 1000;

  std::vector<std::thread> producers;
  producers.reserve(numOps);
  for (int i = 0; i < numOps; ++i) {
    producers.push_back(std::thread([&ch, &cache, &badKeys] {
      NDB_TICKS now = NdbTick_getCurrentTicks();
      Uint32 now_uint32 = (Uint32)now.getUint64();
      std::mt19937 generator(now_uint32);
      std::string apiKey = badKeys[generator() % badKeys.size()];

      auto DB = DB001;

      RS_Status status = cache->validate_api_key(apiKey, {DB});
      if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
        ch.push(true);
      } else {
        // TODO log
        ch.push(false);
      }
    }));
  }

  for (auto &producer : producers) {
    producer.join();
  }

	bool pass = true;
	int failCount = 0;
	for (int i = 0; i < numOps; ++i) {
		bool val = ch.pop();
		if (!val) {
			pass = false;
			failCount++;
		}
	}

  if (!pass) {
    EXPECT_EQ(failCount, 0) << failCount << " key validations failed" << std::endl;
  }

  // Wait for refresh cycle + invalid entry TTL (hardcoded 5s) to pass
  NdbSleep_MilliSleep(2 * (conf.security.apiKey.cacheRefreshIntervalMS) + 5000);

  std::string cacheContent = cache->to_string();
  EXPECT_EQ(cache->size(), 0) << "Cache was not cleared. Expected 0. Got " << cache->size()
                              << " entries in the cache: " << cacheContent << std::endl;
}

// Test load. Generate lots of bad request for API Keys
TEST_F(APIKeyTest, TestAPIKeyCache4) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  // To speed up the tests
  conf.security.apiKey.cacheRefreshIntervalMS       = 1000;
  auto status = AllConfigs::set_all(conf);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << status.message;
  }
  apiKeyCachePtr->start_background_threads();
  test_load_with_bad_keys(apiKeyCachePtr, conf);
  stop_api_key_cache();
}

// Test that NDB event INSERT is detected by the event watcher
TEST_F(APIKeyTest, TestEventInsert) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  const int test_id = 99999;
  const char *test_prefix = "EVT_INSERT_TEST1";  // 16 chars
  const char *test_secret =
      "709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4";
  const char *test_salt =
      "1/1TxiaiIB01rIcY2E36iuwKP6fm2GzBaNaQqOVGMhH0AvcIlIzaUIw0fMDjKNLa0OWxAOrfTSPqAolpI/n+ug==";
  const int test_user_id = 10000;


  conf.security.apiKey.cacheRefreshIntervalMS       = 10000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  // Cleanup leftover from any previous failed run (before preload)
  ndb_delete_api_key_by_id(test_id);

  apiKeyCachePtr->preload_all_keys();
  unsigned size_before = apiKeyCachePtr->size();

  apiKeyCachePtr->start_background_threads();

  // Give event watcher time to subscribe
  NdbSleep_MilliSleep(2000);

  // Insert the row via NDB
  ASSERT_TRUE(ndb_insert_api_key(test_id, test_prefix, test_secret, test_salt,
                                  "evt_test", test_user_id))
      << "Failed to insert test API key via NDB";

  // Wait for event watcher to detect the INSERT (polls every 1s)
  NdbSleep_MilliSleep(3000);

  // Verify the cache grew
  EXPECT_GT(apiKeyCachePtr->size(), size_before)
      << "Cache should have gained an entry from the event INSERT";

  // Verify the key validates
  std::string fullKey = std::string(test_prefix) + "." + HopsworksAPIKey_SECRET;
  RS_Status status = apiKeyCachePtr->validate_api_key(fullKey, {DB001});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Event-inserted key should validate: " << status.message;

  // Verify unauthorized DB still fails
  status = apiKeyCachePtr->validate_api_key(fullKey, {"nonexistent_db"});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Unauthorized DB should be rejected for event-inserted key";

  // Cleanup: delete the inserted row
  ASSERT_TRUE(ndb_delete_api_key_by_id(test_id))
      << "Failed to cleanup test API key";

  stop_api_key_cache();
}

// Test that NDB event DELETE is detected by the event watcher
TEST_F(APIKeyTest, TestEventDelete) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }


  conf.security.apiKey.cacheRefreshIntervalMS       = 10000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  apiKeyCachePtr->preload_all_keys();
  apiKeyCachePtr->start_background_threads();

  // Give event watcher time to subscribe
  NdbSleep_MilliSleep(2000);

  // Use generated key #0 (id=2, prefix="0000000000000000")
  const int delete_id = 2;
  const char *delete_prefix = "0000000000000000";
  std::string fullKey = std::string(delete_prefix) + "." + HopsworksAPIKey_SECRET;

  // Verify key IS in cache (preloaded) and validates
  RS_Status status = apiKeyCachePtr->validate_api_key(fullKey, {DB001});
  ASSERT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Preloaded key should validate before delete: " << status.message;

  unsigned size_before = apiKeyCachePtr->size();

  // Delete the row via NDB
  ASSERT_TRUE(ndb_delete_api_key_by_id(delete_id))
      << "Failed to delete API key via NDB";

  // Wait for event watcher to detect the DELETE
  NdbSleep_MilliSleep(3000);

  // The event watcher marks deleted entries as IS_INVALID (the refresh thread
  // handles actual eviction). Verify the key no longer validates.
  status = apiKeyCachePtr->validate_api_key(fullKey, {DB001});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Deleted key should fail validation";

  // Cleanup: re-insert the deleted key so other test runs still work
  const char *test_secret =
      "709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4";
  const char *test_salt =
      "1/1TxiaiIB01rIcY2E36iuwKP6fm2GzBaNaQqOVGMhH0AvcIlIzaUIw0fMDjKNLa0OWxAOrfTSPqAolpI/n+ug==";
  ASSERT_TRUE(ndb_insert_api_key(delete_id, delete_prefix, test_secret,
                                  test_salt, "name0", 10000))
      << "Failed to re-insert deleted API key for cleanup";

  stop_api_key_cache();
}

// Test that NDB event UPDATE is detected by the event watcher and cache entry is updated
TEST_F(APIKeyTest, TestEventUpdate) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }


  conf.security.apiKey.cacheRefreshIntervalMS       = 10000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  apiKeyCachePtr->preload_all_keys();
  apiKeyCachePtr->start_background_threads();

  // Give event watcher time to subscribe
  NdbSleep_MilliSleep(2000);

  // Use generated key #1 (id=3, prefix="0000000000000001")
  const int update_id = 3;
  const char *update_prefix = "0000000000000001";
  std::string fullKey = std::string(update_prefix) + "." + HopsworksAPIKey_SECRET;

  // Verify key validates before update
  RS_Status status = apiKeyCachePtr->validate_api_key(fullKey, {DB001});
  ASSERT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Preloaded key should validate before update: " << status.message;

  // Update secret to something different — this makes SHA256(client_secret + salt) != new_secret
  const char *bogus_secret =
      "0000000000000000000000000000000000000000000000000000000000000000";
  ASSERT_TRUE(ndb_update_api_key_secret(update_id, bogus_secret))
      << "Failed to update API key secret via NDB";

  // Wait for event watcher to detect the UPDATE
  NdbSleep_MilliSleep(3000);

  // Verify the key no longer validates (hash mismatch: stored secret changed)
  status = apiKeyCachePtr->validate_api_key(fullKey, {DB001});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Key should fail validation after secret was updated in DB";

  // Cleanup: restore original secret
  const char *original_secret =
      "709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4";
  ASSERT_TRUE(ndb_update_api_key_secret(update_id, original_secret))
      << "Failed to restore original API key secret";

  stop_api_key_cache();
}

// Test that keys not in cache fall back to lazy loading
TEST_F(APIKeyTest, TestLazyLoadFallback) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }


  conf.security.apiKey.cacheRefreshIntervalMS       = 10000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  // Do NOT preload and do NOT start event watcher — only start refresh thread
  // so lazy-load is the only path for discovering keys

  // The HOPSWORKS_TEST_API_KEY is already in the DB but not in cache
  EXPECT_EQ(apiKeyCachePtr->size(), 0u)
      << "Cache should be empty before any validation";

  // Validate — should trigger lazy load path (update_cache)
  RS_Status status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {DB001});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Lazy load should validate key from DB: " << status.message;

  // Verify the key is now in cache
  EXPECT_GT(apiKeyCachePtr->size(), 0u)
      << "Cache should have an entry after lazy load";

  // Second validation should hit cache (no lazy load)
  status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {DB001});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Cached key should validate on second call: " << status.message;

  // Unauthorized DB still fails
  status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {"nonexistent_db"});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Unauthorized DB should be rejected even after lazy load";

  stop_api_key_cache();
}

// Test that calling preload_all_keys() twice doesn't create duplicate entries
TEST_F(APIKeyTest, TestPreloadIdempotent) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }


  conf.security.apiKey.cacheRefreshIntervalMS       = 10000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  apiKeyCachePtr->preload_all_keys();
  unsigned size_after_first = apiKeyCachePtr->size();
  EXPECT_GT(size_after_first, 0u)
      << "Cache should have entries after first preload";

  // Preload again — load_single_key should skip already-cached prefixes
  apiKeyCachePtr->preload_all_keys();
  unsigned size_after_second = apiKeyCachePtr->size();

  EXPECT_EQ(size_after_first, size_after_second)
      << "Second preload should not create duplicate entries. "
      << "First: " << size_after_first << ", Second: " << size_after_second;

  // Verify keys still validate after double preload
  RS_Status status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {DB001});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Key should still validate after double preload: " << status.message;

  stop_api_key_cache();
}

// Test that a valid prefix with a wrong secret returns "bad API key"
// (not "not authorized to access DB"), verifying that the hash check
// runs before DB-access results are revealed (Bug 3 fix).
TEST_F(APIKeyTest, TestHashMismatch) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  conf.security.apiKey.cacheRefreshIntervalMS = 10000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  // Preload so prefix "0000000000000000" is in cache with valid DB list
  apiKeyCachePtr->preload_all_keys();

  // Construct key with valid prefix but bogus secret
  std::string badKey = std::string("0000000000000000") +
      ".AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

  // Validate against an authorized DB — should fail with "bad API key"
  // (not "not authorized to access DB")
  RS_Status status = apiKeyCachePtr->validate_api_key(badKey, {DB001});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Bad secret should be rejected";
  EXPECT_NE(std::string(status.message).find("bad API key"), std::string::npos)
      << "Error should be 'bad API key', got: " << status.message;

  stop_api_key_cache();
}

// Test concurrent validation of the SAME key from many threads.
// Exercises the lock-free SHA256 path: multiple threads hold lock_shared()
// on the same partition, copy secret+salt, release lock, then compute
// SHA256 in parallel without blocking each other.
TEST_F(APIKeyTest, TestConcurrentSameKey) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  conf.security.apiKey.cacheRefreshIntervalMS = 10000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  // Preload so the key is already cached — all threads hit the in-memory path
  apiKeyCachePtr->preload_all_keys();

  const int numThreads = 100;
  SafeQueue<bool> results;

  std::vector<std::thread> threads;
  threads.reserve(numThreads);
  for (int i = 0; i < numThreads; ++i) {
    threads.push_back(std::thread([&results] {
      // All threads validate the SAME key against the SAME database
      RS_Status status =
          apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {DB001});
      results.push(status.http_code ==
                   static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK));
    }));
  }

  for (auto &t : threads) {
    t.join();
  }

  int failCount = 0;
  for (int i = 0; i < numThreads; ++i) {
    if (!results.pop()) {
      failCount++;
    }
  }
  EXPECT_EQ(failCount, 0)
      << failCount << " of " << numThreads
      << " concurrent same-key validations failed";

  stop_api_key_cache();
}

// Test concurrent lazy-load: multiple threads validate the same key
// simultaneously when it's NOT in cache. One thread enters IS_VALIDATING
// (does the DB lookup), others wait on the condition variable.
TEST_F(APIKeyTest, TestConcurrentLazyLoad) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  conf.security.apiKey.cacheRefreshIntervalMS = 10000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  // Do NOT preload — cache is empty, all threads hit the lazy-load path
  EXPECT_EQ(apiKeyCachePtr->size(), 0u) << "Cache should be empty";

  const int numThreads = 50;
  SafeQueue<bool> results;

  // All threads validate the SAME key — one will create the IS_VALIDATING
  // entry, others will wait on the condition variable until it's resolved.
  std::vector<std::thread> threads;
  threads.reserve(numThreads);
  for (int i = 0; i < numThreads; ++i) {
    threads.push_back(std::thread([&results] {
      RS_Status status =
          apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {DB001});
      results.push(status.http_code ==
                   static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK));
    }));
  }

  for (auto &t : threads) {
    t.join();
  }

  int failCount = 0;
  for (int i = 0; i < numThreads; ++i) {
    if (!results.pop()) {
      failCount++;
    }
  }
  EXPECT_EQ(failCount, 0)
      << failCount << " of " << numThreads
      << " concurrent lazy-load validations failed";

  // Only one cache entry should exist (no duplicates from concurrent inserts)
  EXPECT_EQ(apiKeyCachePtr->size(), 1u)
      << "Concurrent lazy-load should create exactly one cache entry";

  stop_api_key_cache();
}

// Test that the refresh thread evicts a key that was deleted from the DB.
// Uses lazy-load (not preload) so only one entry is in cache,
// keeping refresh cycles fast.
TEST_F(APIKeyTest, TestRefreshEviction) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  // Use a test key that we can insert/delete without affecting other tests.
  const int test_id = 99998;
  const char *test_prefix = "RFSH_EVICT_TEST1";  // 16 chars
  const char *test_secret =
      "709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4";
  const char *test_salt =
      "1/1TxiaiIB01rIcY2E36iuwKP6fm2GzBaNaQqOVGMhH0AvcIlIzaUIw0fMDjKNLa0OWxAOrfTSPqAolpI/n+ug==";
  const int test_user_id = 10000;

  // Cleanup leftover from any previous failed run
  ndb_delete_api_key_by_id(test_id);

  // Insert the test key
  ASSERT_TRUE(ndb_insert_api_key(test_id, test_prefix, test_secret,
                                  test_salt, "evict_test", test_user_id))
      << "Failed to insert test API key";

  // Fast refresh interval so the test completes quickly
  conf.security.apiKey.cacheRefreshIntervalMS = 1000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  // Do NOT preload — lazy-load just this one key to keep cache small
  std::string fullKey = std::string(test_prefix) + "." + HopsworksAPIKey_SECRET;
  RS_Status status = apiKeyCachePtr->validate_api_key(fullKey, {DB001});
  ASSERT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Test key should validate via lazy load: " << status.message;
  ASSERT_EQ(apiKeyCachePtr->size(), 1u) << "Only the test key should be cached";

  // Start refresh thread (+ event watcher)
  apiKeyCachePtr->start_background_threads();

  // Give event watcher time to subscribe
  NdbSleep_MilliSleep(2000);

  // Delete the key from DB
  ASSERT_TRUE(ndb_delete_api_key_by_id(test_id))
      << "Failed to delete test API key from DB";

  // Wait for: event watcher to detect DELETE and mark IS_INVALID (~2s) +
  // invalid entry TTL (5s) + refresh thread to evict (~2s)
  NdbSleep_MilliSleep(10000);

  // The refresh thread should have evicted the deleted key
  EXPECT_EQ(apiKeyCachePtr->size(), 0u)
      << "Cache should be empty after refresh eviction";

  stop_api_key_cache();
}

// Test that preload_all_keys() picks up new entries inserted during an
// event gap (simulates what happens on event watcher reconnect).
TEST_F(APIKeyTest, TestReconnectPreloadPicksUpNewEntry) {
  apiKeyCachePtr = start_api_key_cache();

  // Preload existing keys
  apiKeyCachePtr->preload_all_keys();
  unsigned initial_size = apiKeyCachePtr->size();
  ASSERT_GT(initial_size, 0u) << "Preload should have loaded some keys";

  // Insert a new API key into DB (simulating INSERT during event gap)
  const int test_id = 99997;
  const char *test_prefix = "RECONNECT_TEST01";  // 16 chars
  const char *test_secret =
      "709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4";
  const char *test_salt =
      "1/1TxiaiIB01rIcY2E36iuwKP6fm2GzBaNaQqOVGMhH0AvcIlIzaUIw0fMDjKNLa0OWxAOrfTSPqAolpI/n+ug==";
  const int test_user_id = 10000;

  ndb_delete_api_key_by_id(test_id);  // cleanup from previous runs
  ASSERT_TRUE(ndb_insert_api_key(test_id, test_prefix, test_secret,
                                  test_salt, "reconnect_test", test_user_id))
      << "Failed to insert test API key";

  // Preload again (this is what the event watcher does on reconnect)
  apiKeyCachePtr->preload_all_keys();
  EXPECT_EQ(apiKeyCachePtr->size(), initial_size + 1)
      << "Preload should pick up the new key";

  // Verify the new key is usable
  std::string fullKey = std::string(test_prefix) + "." + HopsworksAPIKey_SECRET;
  RS_Status status = apiKeyCachePtr->validate_api_key(fullKey, {DB001});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Reconnect-preloaded key should validate: " << status.message;

  // Cleanup
  ndb_delete_api_key_by_id(test_id);
  stop_api_key_cache();
}

// Test that the refresh thread evicts a key that was deleted from DB during
// an event gap.  On reconnect the event watcher wakes the refresh thread
// (NdbCondition_Broadcast) so it runs immediately instead of waiting for
// the full refresh interval.  This test simulates that scenario:
//   1. Preload a key into cache
//   2. Delete it from DB (no event watcher running → DELETE is missed)
//   3. Start background threads (refresh + event watcher)
//   4. The refresh thread discovers the key is gone and evicts it
TEST_F(APIKeyTest, TestReconnectRefreshEvictsMissedDelete) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();

  // Fast refresh so the test completes quickly
  conf.security.apiKey.cacheRefreshIntervalMS = 1000;
  auto confStatus = AllConfigs::set_all(conf);
  ASSERT_EQ(confStatus.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK));

  const int test_id = 99995;
  const char *test_prefix = "RCONN_DEL_TEST01";  // 16 chars
  const char *test_secret =
      "709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4";
  const char *test_salt =
      "1/1TxiaiIB01rIcY2E36iuwKP6fm2GzBaNaQqOVGMhH0AvcIlIzaUIw0fMDjKNLa0OWxAOrfTSPqAolpI/n+ug==";
  const int test_user_id = 10000;

  // Insert test key and preload it into cache
  ndb_delete_api_key_by_id(test_id);
  ASSERT_TRUE(ndb_insert_api_key(test_id, test_prefix, test_secret,
                                  test_salt, "rconn_del", test_user_id))
      << "Failed to insert test API key";
  apiKeyCachePtr->preload_all_keys();

  // Verify key validates
  std::string fullKey = std::string(test_prefix) + "." + HopsworksAPIKey_SECRET;
  RS_Status status = apiKeyCachePtr->validate_api_key(fullKey, {DB001});
  ASSERT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Preloaded key should validate: " << status.message;

  unsigned size_before = apiKeyCachePtr->size();

  // Delete from DB while no event watcher is running (simulates missed DELETE)
  ASSERT_TRUE(ndb_delete_api_key_by_id(test_id))
      << "Failed to delete test key from DB";

  // Start background threads — the refresh thread will discover the key
  // is gone from DB and evict it.  (The event watcher can't help here
  // because the DELETE happened before it subscribed.)
  apiKeyCachePtr->start_background_threads();

  // Wait for: refresh cycle (1s) to find key missing from DB,
  // mark IS_INVALID, then invalid entry TTL (5s) to expire,
  // then next refresh cycle (1s) to evict.  Add margin.
  NdbSleep_MilliSleep(9000);

  EXPECT_EQ(apiKeyCachePtr->size(), size_before - 1)
      << "Refresh thread should have evicted the deleted key";

  // Key should no longer validate
  status = apiKeyCachePtr->validate_api_key(fullKey, {DB001});
  EXPECT_NE(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Deleted key should not validate after refresh eviction";

  stop_api_key_cache();
}

// End-to-end reconnect test: forces the event watcher to tear down and
// reconnect, then verifies that a key inserted during the gap is picked
// up by the reconnect preload.
TEST_F(APIKeyTest, TestEndToEndReconnect) {
  apiKeyCachePtr = start_api_key_cache();
  apiKeyCachePtr->preload_all_keys();
  apiKeyCachePtr->start_background_threads();

  // Wait for event watcher to subscribe
  NdbSleep_MilliSleep(2000);

  const int test_id = 99993;
  const char *test_prefix = "E2E_RECONNECT_01";  // 16 chars
  const char *test_secret =
      "709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4";
  const char *test_salt =
      "1/1TxiaiIB01rIcY2E36iuwKP6fm2GzBaNaQqOVGMhH0AvcIlIzaUIw0fMDjKNLa0OWxAOrfTSPqAolpI/n+ug==";
  const int test_user_id = 10000;

  ndb_delete_api_key_by_id(test_id);

  // Force the event watcher to disconnect.  This triggers the full
  // err: path (tear down subscription → backoff sleep → retry:).
  apiKeyCachePtr->force_reconnect();

  // Wait for the watcher to enter backoff sleep (1s), then insert during
  // the gap so the INSERT event is missed.
  NdbSleep_MilliSleep(500);

  ASSERT_TRUE(ndb_insert_api_key(test_id, test_prefix, test_secret,
                                  test_salt, "e2e_rconn", test_user_id))
      << "Failed to insert test API key";

  // Wait for: remaining backoff (0.5s) + reconnect setup + preload
  NdbSleep_MilliSleep(5000);

  // Verify the key was picked up by reconnect preload
  std::string fullKey = std::string(test_prefix) + "." + HopsworksAPIKey_SECRET;
  RS_Status status = apiKeyCachePtr->validate_api_key(fullKey, {DB001});
  EXPECT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Reconnect preload should have picked up the new key: "
      << status.message;

  // Cleanup
  ndb_delete_api_key_by_id(test_id);
  stop_api_key_cache();
}

// Test cross-project feature-store sharing (hopsworks.shared_feature_store):
// a store shared entirely with one of the key user's projects becomes an
// allowed database; placeholder rows (shared_entirely = 0) and shares with
// other projects grant nothing; deleting the share revokes access.
// fsdb_isolate is the only seeded store the key user cannot already reach.
// There is no NDB event watcher on shared_feature_store — changes propagate
// via the refresh thread, so the test waits ~3 refresh cycles after each
// change.
TEST_F(APIKeyTest, TestSharedFeatureStoreAccess) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  // Fast refresh so share changes propagate quickly
  conf.security.apiKey.cacheRefreshIntervalMS = 1000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  const int share_id = 88888;
  const int other_share_id = 88887;
  // Cleanup leftovers from any previous failed run
  ndb_delete_shared_feature_store(share_id);
  ndb_delete_shared_feature_store(other_share_id);

  // Baseline (lazy load): member DBs accessible, the isolated store not
  RS_Status status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {DB001});
  ASSERT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Member DB should be accessible: " << status.message;
  status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {FSDB_ISOLATE});
  ASSERT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Unshared store must be rejected";

  apiKeyCachePtr->start_background_threads();

  // Share the isolated store entirely with the key user's home project
  ASSERT_TRUE(ndb_insert_shared_feature_store(share_id,
                                              FSDB_ISOLATE_STORE_ID,
                                              HOME_PROJECT_ID,
                                              1))
      << "Failed to insert shared_feature_store row via NDB";
  NdbSleep_MilliSleep(3000);
  status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {FSDB_ISOLATE});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Store shared entirely should be accessible: " << status.message;
  // Member and shared DBs in the same request
  status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {DB001, FSDB_ISOLATE});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Member DB + shared store together should be accessible: " << status.message;

  // Unshare: access revoked on the next refresh
  ASSERT_TRUE(ndb_delete_shared_feature_store(share_id))
      << "Failed to delete shared_feature_store row via NDB";
  NdbSleep_MilliSleep(3000);
  status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {FSDB_ISOLATE});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Unshared store should be rejected again";

  // A placeholder row (shared_entirely = 0, created by Hopsworks when only
  // individual feature groups are shared) grants nothing; neither does a
  // share with a project the key user is not a member of
  ASSERT_TRUE(ndb_insert_shared_feature_store(share_id,
                                              FSDB_ISOLATE_STORE_ID,
                                              HOME_PROJECT_ID,
                                              0))
      << "Failed to insert placeholder share row via NDB";
  ASSERT_TRUE(ndb_insert_shared_feature_store(other_share_id,
                                              FSDB_ISOLATE_STORE_ID,
                                              FSDB_ISOLATE_PROJECT_ID,
                                              1))
      << "Failed to insert foreign-project share row via NDB";
  NdbSleep_MilliSleep(3000);
  status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {FSDB_ISOLATE});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Placeholder / foreign-project shares must not grant access";

  // Member access unaffected throughout
  status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY, {DB001});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Member DB should still be accessible: " << status.message;

  // Cleanup
  ASSERT_TRUE(ndb_delete_shared_feature_store(share_id))
      << "Failed to cleanup placeholder share row";
  ASSERT_TRUE(ndb_delete_shared_feature_store(other_share_id))
      << "Failed to cleanup foreign-project share row";

  stop_api_key_cache();
}

// Fine-grained sharing (RONDB-1088): shared_feature_group / shared_feature
// grant table- and column-level access to another project's online tables;
// restricted_feature_group_access is the per-user mirror. Targets the
// usera_project entities of the imported fixture
// (fine_grained_sharing_data.sql, ids >= 100000) - a store the key user
// macho has no access to. Like shared_feature_store, the grant tables have
// no NDB event watcher: changes propagate via the refresh thread.
TEST_F(APIKeyTest, TestFineGrainedSharedAccess) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  // Fast refresh so grant changes propagate quickly
  conf.security.apiKey.cacheRefreshIntervalMS = 1000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  // Producer entities of the fine-grained sharing fixture
  const std::string_view usera_db = "usera_project";
  const std::string_view customers_table = "usera_customers_fg_1";
  const std::string_view transactions_table = "usera_transactions_fg_1";
  const int usera_store_id = 100000;
  const int customers_fg_id = 100000;
  const int transactions_fg_id = 100001;

  const int fg_share_id = 88886;         // customers FG shared whole
  const int fg_subset_share_id = 88885;  // transactions FG shared feature-wise
  const int feature_share_id1 = 88884;
  const int feature_share_id2 = 88883;
  const int restricted_id = 88882;       // transactions granted to the user
  // Cleanup leftovers from any previous failed run
  ndb_delete_row(SHARED_FEATURE_GROUP, fg_share_id);
  ndb_delete_row(SHARED_FEATURE_GROUP, fg_subset_share_id);
  ndb_delete_row(SHARED_FEATURE, feature_share_id1);
  ndb_delete_row(SHARED_FEATURE, feature_share_id2);
  ndb_delete_row(RESTRICTED_FEATURE_GROUP_ACCESS, restricted_id);

  auto table_request = [](const std::string_view &db,
                          const std::string_view &table,
                          const std::vector<std::string_view> *columns) {
    TableAccessRequest accessReq;
    accessReq.db = db;
    accessReq.table = table;
    accessReq.columns = columns;
    return std::vector<TableAccessRequest>{accessReq};
  };

  // Baseline (lazy load): no access to the producer's tables or database
  RS_Status status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY, table_request(usera_db, customers_table, nullptr));
  ASSERT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Unshared table must be rejected";

  apiKeyCachePtr->start_background_threads();

  // Whole-FG share: the FG's table becomes readable, nothing else
  ASSERT_TRUE(ndb_insert_shared_feature_group(fg_share_id,
                                              usera_store_id,
                                              customers_fg_id,
                                              HOME_PROJECT_ID,
                                              1))
      << "Failed to insert shared_feature_group row via NDB";
  NdbSleep_MilliSleep(3000);
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY, table_request(usera_db, customers_table, nullptr));
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Wholly shared FG table should be accessible: " << status.message;
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY, table_request(usera_db, transactions_table, nullptr));
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "FG share must not open the store's other tables";
  status = apiKeyCachePtr->validate_api_key(HOPSWORKS_TEST_API_KEY,
                                            {usera_db});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "FG share must not grant database-level access";

  // Feature-wise share: only the shared columns are readable
  ASSERT_TRUE(ndb_insert_shared_feature_group(fg_subset_share_id,
                                              usera_store_id,
                                              transactions_fg_id,
                                              HOME_PROJECT_ID,
                                              0))
      << "Failed to insert feature-wise shared_feature_group row via NDB";
  ASSERT_TRUE(ndb_insert_shared_feature(feature_share_id1,
                                        transactions_fg_id,
                                        "customer_id",
                                        HOME_PROJECT_ID))
      << "Failed to insert shared_feature row via NDB";
  ASSERT_TRUE(ndb_insert_shared_feature(feature_share_id2,
                                        transactions_fg_id,
                                        "num_transactions_30d",
                                        HOME_PROJECT_ID))
      << "Failed to insert shared_feature row via NDB";
  NdbSleep_MilliSleep(3000);
  const std::vector<std::string_view> granted_columns =
    {"customer_id", "num_transactions_30d"};
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY,
    table_request(usera_db, transactions_table, &granted_columns));
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Shared columns should be accessible: " << status.message;
  const std::vector<std::string_view> ungranted_columns = {"total_spend_30d"};
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY,
    table_request(usera_db, transactions_table, &ungranted_columns));
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Unshared column must be rejected";
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY, table_request(usera_db, transactions_table, nullptr));
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Whole-row read of a column-shared table must be rejected";

  // Restricted per-user grant: same ladder, keyed by user instead of project
  ASSERT_TRUE(ndb_insert_restricted_feature_group_access(restricted_id,
                                                         usera_store_id,
                                                         transactions_fg_id,
                                                         10000 /*macho*/,
                                                         1))
      << "Failed to insert restricted_feature_group_access row via NDB";
  NdbSleep_MilliSleep(3000);
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY, table_request(usera_db, transactions_table, nullptr));
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Restricted whole-FG grant should open the table: " << status.message;

  // Revoking the restricted grant drops back to the column subset
  ASSERT_TRUE(ndb_delete_row(RESTRICTED_FEATURE_GROUP_ACCESS, restricted_id))
      << "Failed to delete restricted_feature_group_access row via NDB";
  NdbSleep_MilliSleep(3000);
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY, table_request(usera_db, transactions_table, nullptr));
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Revoked restricted grant must reject whole-row reads again";

  // Cleanup
  ASSERT_TRUE(ndb_delete_row(SHARED_FEATURE_GROUP, fg_share_id));
  ASSERT_TRUE(ndb_delete_row(SHARED_FEATURE_GROUP, fg_subset_share_id));
  ASSERT_TRUE(ndb_delete_row(SHARED_FEATURE, feature_share_id1));
  ASSERT_TRUE(ndb_delete_row(SHARED_FEATURE, feature_share_id2));

  stop_api_key_cache();
}

// A feature-wise share (shared_entirely/can_access_entirely = 0) whose
// column rows are missing must grant NOTHING. Hopsworks commits the parent
// and child rows in separate transactions and validates the feature names
// only after the parent is committed, so a parent row without children is
// reachable both mid-write and permanently (share request with a bad
// feature name). Resolving it as an empty column set would mean "whole
// table granted" - a privilege escalation.
TEST_F(APIKeyTest, TestOrphanSubsetGrantFailsClosed) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  // Fast refresh so grant changes propagate quickly
  conf.security.apiKey.cacheRefreshIntervalMS = 1000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  // Producer entities of the fine-grained sharing fixture
  const std::string_view usera_db = "usera_project";
  const std::string_view transactions_table = "usera_transactions_fg_1";
  const int usera_store_id = 100000;
  const int transactions_fg_id = 100001;

  const int orphan_share_id = 88881;       // subset share without columns
  const int orphan_restricted_id = 88880;  // restricted grant without columns
  const int late_feature_id = 88879;       // column row committed later
  // Cleanup leftovers from any previous failed run
  ndb_delete_row(SHARED_FEATURE_GROUP, orphan_share_id);
  ndb_delete_row(RESTRICTED_FEATURE_GROUP_ACCESS, orphan_restricted_id);
  ndb_delete_row(SHARED_FEATURE, late_feature_id);

  auto table_request = [](const std::string_view &db,
                          const std::string_view &table,
                          const std::vector<std::string_view> *columns) {
    TableAccessRequest accessReq;
    accessReq.db = db;
    accessReq.table = table;
    accessReq.columns = columns;
    return std::vector<TableAccessRequest>{accessReq};
  };
  const std::vector<std::string_view> one_column = {"num_transactions_30d"};

  // Baseline (lazy load): no access to the producer's table
  RS_Status status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY,
    table_request(usera_db, transactions_table, nullptr));
  ASSERT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Unshared table must be rejected";

  apiKeyCachePtr->start_background_threads();

  // Orphan feature-wise share: parent row committed, column rows absent
  ASSERT_TRUE(ndb_insert_shared_feature_group(orphan_share_id,
                                              usera_store_id,
                                              transactions_fg_id,
                                              HOME_PROJECT_ID,
                                              0))
      << "Failed to insert orphan shared_feature_group row via NDB";
  NdbSleep_MilliSleep(3000);
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY,
    table_request(usera_db, transactions_table, nullptr));
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Orphan subset share must not grant whole-row reads";
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY,
    table_request(usera_db, transactions_table, &one_column));
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Orphan subset share must not grant any column";

  // Orphan restricted grant: same fail-closed rule on the per-user ladder
  ASSERT_TRUE(ndb_insert_restricted_feature_group_access(orphan_restricted_id,
                                                         usera_store_id,
                                                         transactions_fg_id,
                                                         10000 /*macho*/,
                                                         0))
      << "Failed to insert orphan restricted_feature_group_access row via NDB";
  NdbSleep_MilliSleep(3000);
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY,
    table_request(usera_db, transactions_table, &one_column));
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Orphan restricted grant must not grant any column";

  // The writer finishes: the column row lands and only that column opens up
  ASSERT_TRUE(ndb_insert_shared_feature(late_feature_id,
                                        transactions_fg_id,
                                        "num_transactions_30d",
                                        HOME_PROJECT_ID))
      << "Failed to insert shared_feature row via NDB";
  NdbSleep_MilliSleep(3000);
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY,
    table_request(usera_db, transactions_table, &one_column));
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Completed share should grant the shared column: " << status.message;
  status = apiKeyCachePtr->validate_api_key(
    HOPSWORKS_TEST_API_KEY,
    table_request(usera_db, transactions_table, nullptr));
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Completed subset share must not grant whole-row reads";

  // Cleanup
  ASSERT_TRUE(ndb_delete_row(SHARED_FEATURE_GROUP, orphan_share_id));
  ASSERT_TRUE(ndb_delete_row(RESTRICTED_FEATURE_GROUP_ACCESS,
                             orphan_restricted_id));
  ASSERT_TRUE(ndb_delete_row(SHARED_FEATURE, late_feature_id));

  stop_api_key_cache();
}

// api_key.expiry enforcement, mirroring Hopsworks
// (ApiKeyUtilities.getApiKeyCheckingExpiry): NULL never expires, otherwise
// the key is rejected once expiry is strictly before now. Also covers
// expiry changes on an already-cached key propagating via the background
// threads (api_key UPDATE event / refresh sweep).
TEST_F(APIKeyTest, TestAPIKeyExpiry) {
  apiKeyCachePtr = start_api_key_cache();
  AllConfigs conf = AllConfigs::get_all();
  if (!conf.security.apiKey.useHopsworksAPIKeys) {
    std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
  }

  // Fast refresh so expiry changes propagate quickly
  conf.security.apiKey.cacheRefreshIntervalMS = 1000;
  auto confStatus = AllConfigs::set_all(conf);
  if (confStatus.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to set config: " << confStatus.message;
  }

  const int past_id = 99996;
  const int future_id = 99995;
  const int null_id = 99994;
  const char *past_prefix = "EXPIRY_PAST_TEST";    // 16 chars
  const char *future_prefix = "EXPIRY_FUTR_TEST";  // 16 chars
  const char *null_prefix = "EXPIRY_NULL_TEST";    // 16 chars
  const char *test_secret =
      "709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4";
  const char *test_salt =
      "1/1TxiaiIB01rIcY2E36iuwKP6fm2GzBaNaQqOVGMhH0AvcIlIzaUIw0fMDjKNLa0OWxAOrfTSPqAolpI/n+ug==";
  const int test_user_id = 10000;
  const time_t now = time(nullptr);

  // Cleanup leftovers from any previous failed run
  ndb_delete_api_key_by_id(past_id);
  ndb_delete_api_key_by_id(future_id);
  ndb_delete_api_key_by_id(null_id);

  ASSERT_TRUE(ndb_insert_api_key(past_id, past_prefix, test_secret,
                                 test_salt, "expiry_past", test_user_id,
                                 now - 3600))
      << "Failed to insert expired API key";
  ASSERT_TRUE(ndb_insert_api_key(future_id, future_prefix, test_secret,
                                 test_salt, "expiry_future", test_user_id,
                                 now + 3600))
      << "Failed to insert not-yet-expired API key";
  ASSERT_TRUE(ndb_insert_api_key(null_id, null_prefix, test_secret,
                                 test_salt, "expiry_null", test_user_id))
      << "Failed to insert never-expiring API key";

  // Expired key: correct secret, but rejected with the Hopsworks message
  std::string pastKey = std::string(past_prefix) + "." + HopsworksAPIKey_SECRET;
  RS_Status status = apiKeyCachePtr->validate_api_key(pastKey, {DB001});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Expired API key must be rejected";
  EXPECT_NE(std::string(status.message).find("expired"), std::string::npos)
      << "Denial should name the expiry, got: " << status.message;

  // Expiry in the future: works like any valid key
  std::string futureKey =
      std::string(future_prefix) + "." + HopsworksAPIKey_SECRET;
  status = apiKeyCachePtr->validate_api_key(futureKey, {DB001});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Not-yet-expired API key should validate: " << status.message;

  // NULL expiry: never expires
  std::string nullKey = std::string(null_prefix) + "." + HopsworksAPIKey_SECRET;
  status = apiKeyCachePtr->validate_api_key(nullKey, {DB001});
  EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "API key without expiry should validate: " << status.message;

  // Shortening a cached key's expiry into the past must revoke it via the
  // background threads (UPDATE event or refresh sweep)
  apiKeyCachePtr->start_background_threads();
  NdbSleep_MilliSleep(2000);  // let the event watcher subscribe
  ASSERT_TRUE(ndb_update_api_key_expiry(future_id, now - 60))
      << "Failed to update API key expiry via NDB";
  NdbSleep_MilliSleep(3000);
  status = apiKeyCachePtr->validate_api_key(futureKey, {DB001});
  EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
      << "Key expired after caching must be rejected";
  EXPECT_NE(std::string(status.message).find("expired"), std::string::npos)
      << "Denial should name the expiry, got: " << status.message;

  // Cleanup
  ASSERT_TRUE(ndb_delete_api_key_by_id(past_id));
  ASSERT_TRUE(ndb_delete_api_key_by_id(future_id));
  ASSERT_TRUE(ndb_delete_api_key_by_id(null_id));

  stop_api_key_cache();
}

int main(int argc, char **argv) {
  ndb_init();
  globalConfigsMutex = NdbMutex_Create();

  // Load config from RDRS_CONFIG_FILE (needed for correct NDB connection settings)
  std::string configFile;
  const char *env_config_file_path = std::getenv("RDRS_CONFIG_FILE");
  if (env_config_file_path != nullptr) {
    configFile = env_config_file_path;
  }
  RS_Status status = AllConfigs::init(configFile);
  if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    std::cerr << "Error loading config: " << status.message << std::endl;
    return 1;
  }

  testing::InitGoogleTest(&argc, argv);
  testing::Environment* const my_env =
    testing::AddGlobalTestEnvironment(new MyEnvironment);
  (void)my_env;
  int rc = RUN_ALL_TESTS();
  NdbMutex_Destroy(globalConfigsMutex);
  ndb_end(0);
  return rc;
}
