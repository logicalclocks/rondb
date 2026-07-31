/*
 * Copyright (C) 2023, 2024 Hopsworks AB
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

#include "rdrs_hopsworks_dal.h"
#include "rdrs_dal.hpp"
#include "error_strings.h"
#include "logger.hpp"
#include "rdrs_rondb_connection_pool.hpp"
#include "db_operations/pk/common.hpp"
#include "rdrs_const.h"
#include "retry_handler.hpp"
#include "ndb_api_helper.hpp"

#include <my_time.h>

#include <cstring>
#include <map>
#include <set>
#include <string>
#include <iostream>
#include <vector>
#include <unistd.h>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_DAL 1
#endif

#ifdef DEBUG_DAL
#define DEB_DAL(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_DAL(...) do { } while (0)
#endif


// RonDB connection pool
extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;

/*
 * api_key.expiry is a nullable DATETIME (wall-clock, no timezone). Hopsworks
 * reads it via JPA / Connector-J 8.x into a java.util.Date using the app
 * server's JVM DEFAULT timezone, then compares it against now
 * (ApiKeyUtilities.getApiKeyCheckingExpiry: expiry.before(new Date())). The
 * Hopsworks deployment pins no timezone - no connectionTimeZone, no
 * -Duser.timezone, and the helm chart sets no TZ - so the app server container
 * defaults to UTC and the stored wall-clock is authored in UTC.
 *
 * We convert it the same way with pure integer arithmetic via calc_daynr(),
 * which is lock-free. mktime()/timegm() are deliberately avoided: they take a
 * process-wide timezone lock (tzset state) and, with tm_isdst=-1, resolve DST
 * ambiguously across gaps. Converted to epoch seconds once at key load time so
 * validation is a plain integer comparison.
 *
 * NOTE: this assumes the Hopsworks app server runs in UTC (the deployment
 * default). Running it in a non-UTC timezone would shift stored expiries by
 * that offset - matching Hopsworks would then require converting in the same
 * timezone instead of UTC.
 */
long long datetime_attr_to_epoch(const NdbRecAttr *attr, unsigned precision) {
  if (attr->isNULL()) {
    return 0;
  }
  longlong packed = my_datetime_packed_from_binary(
    (const unsigned char *)attr->aRef(), precision);
  MYSQL_TIME mysql_time;
  TIME_from_longlong_datetime_packed(&mysql_time, packed);
  long long days = (long long)calc_daynr(mysql_time.year, mysql_time.month,
                                         mysql_time.day) -
                   (long long)calc_daynr(1970, 1, 1);
  return days * 86400LL +
         (long long)mysql_time.hour * 3600LL +
         (long long)mysql_time.minute * 60LL +
         (long long)mysql_time.second;
}

RS_Status find_api_key_int(Ndb *ndb_object,
                           const char *prefix,
                           HopsworksAPIKey *api_key) {
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;

  RS_Status status = select_table(ndb_object,
                                  HOPSWORKS,
                                  API_KEY,
                                  &table_dict);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = start_transaction(ndb_object, &tx);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  const char* index_name = "prefix_UNIQUE";
  status = get_index_scan_op(ndb_object,
                             tx,
                             table_dict,
                             index_name,
                             &scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  status = read_tuples(ndb_object, scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  int col_id = table_dict->getColumn("prefix")->getColumnNo();
  Uint32 col_size = (Uint32)table_dict->getColumn("prefix")->getSizeInBytes();
  if (unlikely(col_size != API_KEY_PREFIX_SIZE)) {
    ndb_object->closeTransaction(tx);
    return RS_SERVER_ERROR(
      "hopsworks.api_key table has wrong schema: prefix column size mismatch "
      "(expected latin1 charset)");
  }
  size_t prefix_len = strlen(prefix);
  if (unlikely(prefix_len >
              (col_size - bytes_for_ndb_str_len(API_KEY_PREFIX_SIZE)))) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR("Wrong length of the search key");
  }
  // Note: api_key is varchar column.
  char cmp_str[API_KEY_PREFIX_SIZE];
  memcpy(cmp_str + bytes_for_ndb_str_len(API_KEY_PREFIX_SIZE),
         prefix,
         prefix_len);
  cmp_str[0] = static_cast<char>(prefix_len);

  NdbScanFilter filter(scanOp);
  if (unlikely(filter.begin(NdbScanFilter::AND) < 0 ||
               filter.cmp(NdbScanFilter::COND_EQ,
                          col_id,
                          cmp_str,
                          API_KEY_PREFIX_SIZE) < 0 ||
               filter.end() < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  NdbRecAttr *user_id = scanOp->getValue("user_id");
  NdbRecAttr *secret = scanOp->getValue("secret");
  NdbRecAttr *salt = scanOp->getValue("salt");
  NdbRecAttr *name = scanOp->getValue("name");
  NdbRecAttr *expiry = scanOp->getValue("expiry");
  // getColumn() is nullptr on a pre-V73 schema without api_key.expiry; the
  // getValue() above is then nullptr too and the check below reports it.
  const NdbDictionary::Column *expiry_col = table_dict->getColumn("expiry");
  unsigned expiry_prec = expiry_col != nullptr ? expiry_col->getPrecision() : 0;

  assert(API_KEY_SECRET_SIZE ==
         (Uint32)table_dict->getColumn("secret")->getSizeInBytes());
  assert(API_KEY_SALT_SIZE ==
         (Uint32)table_dict->getColumn("salt")->getSizeInBytes());
  assert(API_KEY_NAME_SIZE ==
         (Uint32)table_dict->getColumn("name")->getSizeInBytes());

  if (unlikely(user_id == nullptr ||
               secret == nullptr ||
               salt == nullptr ||
               name == nullptr ||
               expiry == nullptr)) {
    err = scanOp->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  int count  = 0;
  bool check = 0;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      count++;
      if (unlikely(count > 1)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR("Wrong API Prefix");
      }
      Uint32 name_attr_bytes;
      const char *name_data_start = nullptr;
      if (unlikely(GetByteArray(
                     name, &name_data_start, &name_attr_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
      }
      Uint32 salt_attr_bytes;
      const char *salt_data_start = nullptr;
      if (unlikely(GetByteArray(
                     salt, &salt_data_start, &salt_attr_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
      }
      Uint32 secret_attr_bytes;
      const char *secret_data_start = nullptr;
      if (unlikely(GetByteArray(
                     secret, &secret_data_start, &secret_attr_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
      }
      // <= because we want to leave one byte for '\0'
      // sizes of char arrays are set to accommodate additional '\0'
      if (unlikely(sizeof(api_key->secret) <= secret_attr_bytes ||
                   sizeof(api_key->name) <= name_attr_bytes ||
                   sizeof(api_key->salt) <= salt_attr_bytes)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUFFER_TOO_SMALL)));
      }
      memcpy(api_key->name, name_data_start, name_attr_bytes);
      api_key->name[name_attr_bytes] = '\0';

      memcpy(api_key->secret, secret_data_start, secret_attr_bytes);
      api_key->secret[secret_attr_bytes] = '\0';

      memcpy(api_key->salt, salt_data_start, salt_attr_bytes);
      api_key->salt[salt_attr_bytes] = '\0';

      api_key->user_id = user_id->int32_value();
      api_key->expiry_epoch = datetime_attr_to_epoch(expiry, expiry_prec);
    } while ((check = scanOp->nextResult(false)) == 0);
  }
  NdbError error = scanOp->getNdbError();
  ndb_object->closeTransaction(tx);
  if (unlikely(error.code != 4120 /*Scan already complete*/)) {
    return RS_RONDB_SERVER_ERROR(
      error, "Failed Reading API Key. Fn find_api_key_int");
  }
  if (unlikely(count == 0)) {
    return RS_CLIENT_404_ERROR();
  }
  return RS_OK;
}

RS_Status find_api_key(const char *prefix, HopsworksAPIKey *api_key) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb_object);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  METADATA_OP_RETRY_HANDLER(
   status = find_api_key_int(ndb_object, prefix, api_key);
   HandleSchemaErrors(ndb_object,
                      status,
                      {std::make_tuple(HOPSWORKS, API_KEY)});
  )
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb_object, &status);
  return status;
}

RS_Status find_all_api_keys_int(Ndb *ndb_object,
                                std::vector<HopsworksAPIKeyEntry> *keys) {
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;

  RS_Status status = select_table(ndb_object,
                                  HOPSWORKS,
                                  API_KEY,
                                  &table_dict);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = start_transaction(ndb_object, &tx);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  scanOp = tx->getNdbScanOperation(table_dict);
  if (unlikely(scanOp == nullptr)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err,
      std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  if (unlikely(scanOp->readTuples(NdbOperation::LM_CommittedRead) != 0)) {
    err = scanOp->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err,
      std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }

  NdbRecAttr *prefix_attr = scanOp->getValue("prefix");
  NdbRecAttr *secret_attr = scanOp->getValue("secret");
  NdbRecAttr *salt_attr = scanOp->getValue("salt");
  NdbRecAttr *user_id_attr = scanOp->getValue("user_id");
  NdbRecAttr *expiry_attr = scanOp->getValue("expiry");
  // getColumn() is nullptr on a pre-V73 schema without api_key.expiry; the
  // getValue() above is then nullptr too and the check below reports it.
  const NdbDictionary::Column *expiry_col = table_dict->getColumn("expiry");
  unsigned expiry_prec = expiry_col != nullptr ? expiry_col->getPrecision() : 0;

  if (unlikely(prefix_attr == nullptr ||
               secret_attr == nullptr ||
               salt_attr == nullptr ||
               user_id_attr == nullptr ||
               expiry_attr == nullptr)) {
    err = scanOp->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err,
      std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err,
      std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  bool check;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      Uint32 prefix_bytes;
      const char *prefix_start = nullptr;
      if (unlikely(GetByteArray(
                     prefix_attr, &prefix_start, &prefix_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(
          std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
      }

      Uint32 secret_bytes;
      const char *secret_start = nullptr;
      if (unlikely(GetByteArray(
                     secret_attr, &secret_start, &secret_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(
          std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
      }

      Uint32 salt_bytes;
      const char *salt_start = nullptr;
      if (unlikely(GetByteArray(
                     salt_attr, &salt_start, &salt_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(
          std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
      }

      keys->push_back({
        std::string(prefix_start, prefix_bytes),
        std::string(secret_start, secret_bytes),
        std::string(salt_start, salt_bytes),
        user_id_attr->int32_value(),
        datetime_attr_to_epoch(expiry_attr, expiry_prec)
      });
    } while ((check = scanOp->nextResult(false)) == 0);
  }
  NdbError error = scanOp->getNdbError();
  ndb_object->closeTransaction(tx);
  if (unlikely(error.code != 4120 /*Scan already complete*/)) {
    return RS_RONDB_SERVER_ERROR(
      error, "Failed Reading API Keys. Fn find_all_api_keys_int");
  }
  return RS_OK;
}

RS_Status find_all_api_keys(std::vector<HopsworksAPIKeyEntry> *keys) {
  Ndb *ndb_object = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb_object);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  METADATA_OP_RETRY_HANDLER(
    keys->clear();
    status = find_all_api_keys_int(ndb_object, keys);
    HandleSchemaErrors(ndb_object,
                       status,
                       {std::make_tuple(HOPSWORKS, API_KEY)});
  )
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb_object, &status);
  return status;
}

static RS_Status find_api_key_by_id_int(Ndb *ndb_object,
                                         int id,
                                         HopsworksAPIKeyEntry *entry) {
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;

  RS_Status status = select_table(ndb_object,
                                  HOPSWORKS,
                                  API_KEY,
                                  &table_dict);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = start_transaction(ndb_object, &tx);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }

  NdbOperation *op = tx->getNdbOperation(table_dict);
  if (unlikely(op == nullptr)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err,
      std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  if (unlikely(op->readTuple(NdbOperation::LM_CommittedRead) != 0)) {
    err = op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err,
      std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  if (unlikely(op->equal("id", id) != 0)) {
    err = op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err,
      std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }

  NdbRecAttr *prefix_attr = op->getValue("prefix");
  NdbRecAttr *secret_attr = op->getValue("secret");
  NdbRecAttr *salt_attr   = op->getValue("salt");
  NdbRecAttr *user_id_attr = op->getValue("user_id");
  NdbRecAttr *expiry_attr = op->getValue("expiry");
  // getColumn() is nullptr on a pre-V73 schema without api_key.expiry; the
  // getValue() above is then nullptr too and the check below reports it.
  const NdbDictionary::Column *expiry_col = table_dict->getColumn("expiry");
  unsigned expiry_prec = expiry_col != nullptr ? expiry_col->getPrecision() : 0;

  if (unlikely(prefix_attr == nullptr ||
               secret_attr == nullptr ||
               salt_attr == nullptr ||
               user_id_attr == nullptr ||
               expiry_attr == nullptr)) {
    err = op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err,
      std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  if (unlikely(tx->execute(NdbTransaction::Commit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    if (err.code == 626) {  // Tuple did not exist
      return RS_CLIENT_404_ERROR();
    }
    return RS_RONDB_SERVER_ERROR(err,
      std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }

  Uint32 prefix_bytes;
  const char *prefix_start = nullptr;
  if (unlikely(GetByteArray(prefix_attr, &prefix_start, &prefix_bytes) != 0)) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR(
      std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  Uint32 secret_bytes;
  const char *secret_start = nullptr;
  if (unlikely(GetByteArray(secret_attr, &secret_start, &secret_bytes) != 0)) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR(
      std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  Uint32 salt_bytes;
  const char *salt_start = nullptr;
  if (unlikely(GetByteArray(salt_attr, &salt_start, &salt_bytes) != 0)) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR(
      std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }

  entry->prefix = std::string(prefix_start, prefix_bytes);
  entry->secret = std::string(secret_start, secret_bytes);
  entry->salt = std::string(salt_start, salt_bytes);
  entry->user_id = user_id_attr->int32_value();
  entry->expiry_epoch = datetime_attr_to_epoch(expiry_attr, expiry_prec);

  ndb_object->closeTransaction(tx);
  return RS_OK;
}

RS_Status find_api_key_by_id(int id, HopsworksAPIKeyEntry *entry) {
  Ndb *ndb_object = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb_object);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  METADATA_OP_RETRY_HANDLER(
    status = find_api_key_by_id_int(ndb_object, id, entry);
    HandleSchemaErrors(ndb_object,
                       status,
                       {std::make_tuple(HOPSWORKS, API_KEY)});
  )
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb_object, &status);
  return status;
}

RS_Status find_user_int(Ndb *ndb_object,
                        Uint32 uid,
                        HopsworksUsers *users) {
  // FIX ME: Use batch PK lookups instead of Index Scan Op
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;

  RS_Status status = select_table(ndb_object, HOPSWORKS, USERS, &table_dict);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = start_transaction(ndb_object, &tx);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  const char* index_name = "PRIMARY";
  status = get_index_scan_op(ndb_object,
                             tx,
                             table_dict,
                             index_name,
                             &scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  status = read_tuples(ndb_object, scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  int col_id = table_dict->getColumn("uid")->getColumnNo();
  NdbScanFilter filter(scanOp);
  if (unlikely(filter.begin(NdbScanFilter::AND) < 0 ||
               filter.eq(col_id, uid) < 0 ||
               filter.end() < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  NdbRecAttr *email = scanOp->getValue("email");
  if (unlikely(email == nullptr)) {
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  assert(USERS_EMAIL_SIZE ==
         (Uint32)table_dict->getColumn("email")->getSizeInBytes());

  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  bool check = 0;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      Uint32 email_attr_bytes;
      const char *email_data_start = nullptr;
      if (unlikely(GetByteArray(
                     email, &email_data_start, &email_attr_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
      }
      if (unlikely(sizeof(users->email) < email_attr_bytes)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUFFER_TOO_SMALL)));
      }
      memcpy(users->email, email_data_start, email_attr_bytes);
      users->email[email_attr_bytes] = 0;
    } while ((check = scanOp->nextResult(false)) == 0);
  }
  // check for errors happened during the reading process
  NdbError error = scanOp->getNdbError();

  // As we are at the end we will first close the transaction and then deal
  // with the error.
  ndb_object->closeTransaction(tx);

  if (unlikely(error.code != 4120 /*Scan already complete*/)) {
    return RS_RONDB_SERVER_ERROR(
      error, "Failed Reading API Key. Fn find_user_int");
  }
  return RS_OK;
}

RS_Status find_user(Ndb *ndb_object, Uint32 uid, HopsworksUsers *users) {
  return find_user_int(ndb_object, uid, users);
}

RS_Status find_project_team_int(
  Ndb *ndb_object,
  HopsworksUsers *users,
  std::vector<HopsworksProjectTeam> *project_team_vec) {
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;

  RS_Status status = select_table(ndb_object,
                                  HOPSWORKS,
                                  PROJECT_TEAM,
                                  &table_dict);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = start_transaction(ndb_object, &tx);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  const char* index_name = "team_member";
  status = get_index_scan_op(ndb_object,
                             tx,
                             table_dict,
                             index_name,
                             &scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  status = read_tuples(ndb_object, scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  int col_id = table_dict->getColumn("team_member")->getColumnNo();
  Uint32 col_size =
    (Uint32)table_dict->getColumn("team_member")->getSizeInBytes();
  if (unlikely(col_size != PROJECT_TEAM_TEAM_MEMBER_SIZE)) {
    ndb_object->closeTransaction(tx);
    return RS_SERVER_ERROR(
      "hopsworks.project_team table has wrong schema: team_member column size "
      "mismatch (expected latin1 charset)");
  }

  size_t email_len = strlen(users->email);
  if (unlikely(email_len >
      (col_size - bytes_for_ndb_str_len(PROJECT_TEAM_TEAM_MEMBER_SIZE)))) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR("Wrong length of the search key");
  }
  // Note: project_team is varchar column.
  char cmp_str[PROJECT_TEAM_TEAM_MEMBER_SIZE];
  memcpy(cmp_str + bytes_for_ndb_str_len(PROJECT_TEAM_TEAM_MEMBER_SIZE),
         users->email,
         email_len);
  cmp_str[0] = static_cast<char>(email_len);

  NdbScanFilter filter(scanOp);
  if (unlikely(filter.begin(NdbScanFilter::AND) < 0 ||
               filter.cmp(NdbScanFilter::COND_EQ,
                          col_id,
                          cmp_str,
                          PROJECT_TEAM_TEAM_MEMBER_SIZE) < 0 ||
               filter.end() < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  NdbRecAttr *project_id = scanOp->getValue("project_id");
  NdbRecAttr *team_role = scanOp->getValue("team_role");
  if (unlikely(project_id == nullptr || team_role == nullptr)) {
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  assert(PROJECT_TEAM_TEAM_ROLE_SIZE ==
         (Uint32)table_dict->getColumn("team_role")->getSizeInBytes());
  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  bool check;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      HopsworksProjectTeam project_team;
      project_team.project_id = project_id->int32_value();
      Uint32 role_attr_bytes;
      const char *role_data_start = nullptr;
      if (unlikely(GetByteArray(
                     team_role, &role_data_start, &role_attr_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
      }
      if (unlikely(sizeof(project_team.team_role) < role_attr_bytes)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUFFER_TOO_SMALL)));
      }
      memcpy(project_team.team_role, role_data_start, role_attr_bytes);
      project_team.team_role[role_attr_bytes] = '\0';
      project_team_vec->push_back(project_team);
    } while ((check = scanOp->nextResult(false)) == 0);
  }
  // check for errors happened during the reading process
  NdbError error = scanOp->getNdbError();

  // As we are at the end we will first close the transaction and then deal
  // with the error.
  ndb_object->closeTransaction(tx);
  if (unlikely(error.code != 4120 /*Scan already complete*/)) {
    return RS_RONDB_SERVER_ERROR(
      error, "Failed Reading API Key. Fn find_project_team_int");
  }
  return RS_OK;
}

RS_Status find_project_team(
  Ndb *ndb_object,
  HopsworksUsers *users,
  std::vector<HopsworksProjectTeam> *project_team_vec) {
  return find_project_team_int(ndb_object, users, project_team_vec);
}

RS_Status find_projects_int(
  Ndb *ndb_object,
  std::vector<HopsworksProjectTeam> *project_team_vec,
  std::vector<HopsworksProject> *project_vec) {

  // FIX ME: Use batch PK lookups instead of Index Scan Op
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;
  RS_Status status = select_table(ndb_object, HOPSWORKS, PROJECT, &table_dict);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = start_transaction(ndb_object, &tx);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  const char* index_name = "PRIMARY";
  status = get_index_scan_op(ndb_object,
                             tx,
                             table_dict,
                             index_name,
                             &scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  status = read_tuples(ndb_object, scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  int col_id = table_dict->getColumn("id")->getColumnNo();
  NdbScanFilter filter(scanOp);
  if (unlikely(filter.begin(NdbScanFilter::OR) < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  for (Uint32 i = 0; i < project_team_vec->size(); i++) {
    if (unlikely(filter.eq(col_id,
                 (Uint32)(*project_team_vec)[i].project_id) < 0)) {
      err = filter.getNdbError();
      ndb_object->closeTransaction(tx);
      return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
    }
  }
  if (unlikely(filter.end() < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  NdbRecAttr *projectname = scanOp->getValue("projectname");
  if (unlikely(projectname == nullptr)) {
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  assert(PROJECT_PROJECTNAME_SIZE ==
         (Uint32)table_dict->getColumn("projectname")->getSizeInBytes());

  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  bool check = 0;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      HopsworksProject project;
      Uint32 projectname_attr_bytes;
      const char *projectname_data_start = nullptr;
      if (unlikely(GetByteArray(projectname,
                                &projectname_data_start,
                                &projectname_attr_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
      }

      if (unlikely(sizeof(project.projectname) < projectname_attr_bytes)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUFFER_TOO_SMALL)));
      }

      memcpy(project.projectname,
             projectname_data_start,
             projectname_attr_bytes);
      project.projectname[projectname_attr_bytes] = '\0';
      project_vec->push_back(project);

    } while ((check = scanOp->nextResult(false)) == 0);
  }
  // check for errors happened during the reading process
  NdbError error = scanOp->getNdbError();

  // As we are at the end we will first close the transaction and then deal
  // with the error.
  ndb_object->closeTransaction(tx);
  if (unlikely(error.code != 4120 /*Scan already complete*/)) {
    return RS_RONDB_SERVER_ERROR(
      error, "Failed Reading API Key. Fn find_projects_int");
  }
  return RS_OK;
}

RS_Status find_projects_vec(
  Ndb *ndb_object,
  std::vector<HopsworksProjectTeam> *project_team_vec,
  std::vector<HopsworksProject> *project_vec) {
  return find_projects_int(ndb_object, project_team_vec, project_vec);
}

/*
 * SELECT feature_store, shared_entirely FROM hopsworks.shared_feature_store
 * WHERE shared_with_project IN ({project ids})
 *
 * Rows with shared_entirely = 1 grant full access to the store's database.
 * Rows with shared_entirely = 0 are listing placeholders created by
 * Hopsworks when only individual feature groups are shared: they grant no
 * data access, but they DO make the store visible for feature-view
 * metadata resolution, so they are returned separately. The flag is read
 * per row instead of pushed into the scan filter to keep the filter to the
 * proven int-column pattern.
 */
RS_Status find_shared_feature_store_ids_int(
  Ndb *ndb_object,
  std::vector<HopsworksProjectTeam> *project_team_vec,
  std::vector<int> *shared_store_ids,
  std::vector<int> *placeholder_store_ids) {

  if (project_team_vec->empty()) {
    return RS_OK;
  }
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;
  RS_Status status = select_table(ndb_object,
                                  HOPSWORKS,
                                  SHARED_FEATURE_STORE,
                                  &table_dict);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = start_transaction(ndb_object, &tx);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  const char* index_name = "PRIMARY";
  status = get_index_scan_op(ndb_object,
                             tx,
                             table_dict,
                             index_name,
                             &scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  status = read_tuples(ndb_object, scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  int col_id = table_dict->getColumn("shared_with_project")->getColumnNo();
  NdbScanFilter filter(scanOp);
  if (unlikely(filter.begin(NdbScanFilter::OR) < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  for (Uint32 i = 0; i < project_team_vec->size(); i++) {
    if (unlikely(filter.eq(col_id,
                 (Uint32)(*project_team_vec)[i].project_id) < 0)) {
      err = filter.getNdbError();
      ndb_object->closeTransaction(tx);
      return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
    }
  }
  if (unlikely(filter.end() < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  NdbRecAttr *feature_store_id = scanOp->getValue("feature_store");
  NdbRecAttr *shared_entirely = scanOp->getValue("shared_entirely");
  if (unlikely(feature_store_id == nullptr || shared_entirely == nullptr)) {
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  bool check;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      if (shared_entirely->int8_value() == 1) {
        shared_store_ids->push_back(feature_store_id->int32_value());
      } else {
        placeholder_store_ids->push_back(feature_store_id->int32_value());
      }
    } while ((check = scanOp->nextResult(false)) == 0);
  }
  // check for errors happened during the reading process
  NdbError error = scanOp->getNdbError();

  // As we are at the end we will first close the transaction and then deal
  // with the error.
  ndb_object->closeTransaction(tx);
  if (unlikely(error.code != 4120 /*Scan already complete*/)) {
    return RS_RONDB_SERVER_ERROR(
      error, "Failed Reading API Key. Fn find_shared_feature_store_ids_int");
  }
  return RS_OK;
}

/*
 * SELECT name FROM hopsworks.feature_store WHERE id = {fs_id}
 *
 * The feature store name is the database name. A missing row sets *found to
 * false instead of failing: the sfs_fs_fk ON DELETE CASCADE removes share
 * rows with the store, so a dangling share id can only be a scan/delete
 * race and the store is simply not accessible.
 */
RS_Status find_feature_store_name_int(Ndb *ndb_object,
                                      int fs_id,
                                      HopsworksProject *store_db,
                                      bool *found) {
  *found = false;
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbOperation *ndb_op;

  RS_Status status = select_table(ndb_object,
                                  HOPSWORKS,
                                  FEATURE_STORE,
                                  &table_dict);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = start_transaction(ndb_object, &tx);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = get_op(ndb_object, tx, FEATURE_STORE, &ndb_op);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  status = read_tuple(ndb_object, ndb_op);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  if (unlikely(ndb_op->equal("id", fs_id) != 0)) {
    err = ndb_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_EQUAL_FAILED)));
  }
  NdbRecAttr *name = ndb_op->getValue("name");
  if (unlikely(name == nullptr)) {
    err = ndb_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  assert(FEATURE_STORE_NAME_SIZE ==
         (Uint32)table_dict->getColumn("name")->getSizeInBytes());
  if (unlikely(tx->execute(NdbTransaction::Commit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  if (ndb_op->getNdbError().classification == NdbError::NoDataFound) {
    ndb_object->closeTransaction(tx);
    return RS_OK;
  }
  Uint32 name_attr_bytes;
  const char *name_data_start = nullptr;
  if (unlikely(GetByteArray(name, &name_data_start, &name_attr_bytes) != 0)) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  if (unlikely(sizeof(store_db->projectname) < name_attr_bytes)) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUFFER_TOO_SMALL)));
  }
  memcpy(store_db->projectname, name_data_start, name_attr_bytes);
  store_db->projectname[name_attr_bytes] = '\0';
  *found = true;
  ndb_object->closeTransaction(tx);
  return RS_OK;
}

/*
 * Resolve feature store ids to database names, appending to db_names.
 * Missing stores are skipped (FK-cascade race, see
 * find_feature_store_name_int).
 */
static RS_Status append_store_names_int(
  Ndb *ndb_object,
  std::vector<int> *store_ids,
  std::vector<std::string> *db_names) {

  for (Uint32 i = 0; i < store_ids->size(); i++) {
    HopsworksProject store_db;
    bool found = false;
    RS_Status status = find_feature_store_name_int(ndb_object,
                                                   (*store_ids)[i],
                                                   &store_db,
                                                   &found);
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
    if (found) {
      db_names->push_back(store_db.projectname);
    }
  }
  return RS_OK;
}

/*
 * Shared boilerplate of the grant-table scans: table dict + transaction +
 * PRIMARY index scan op with read_tuples. On error everything opened so
 * far is closed. The caller must close the transaction.
 */
static RS_Status open_grant_scan_int(
  Ndb *ndb_object,
  const char *table_name,
  const NdbDictionary::Table **table_dict,
  NdbTransaction **tx,
  NdbScanOperation **scanOp) {

  RS_Status status = select_table(ndb_object,
                                  HOPSWORKS,
                                  table_name,
                                  table_dict);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = start_transaction(ndb_object, tx);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  const char* index_name = "PRIMARY";
  status = get_index_scan_op(ndb_object,
                             *tx,
                             *table_dict,
                             index_name,
                             scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(*tx);
    return status;
  }
  status = read_tuples(ndb_object, *scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(*tx);
    return status;
  }
  return RS_OK;
}

// Raw grant rows, joined to table/database names in
// find_fine_grained_grants_int
struct SharedFeatureGroupRow {
  int feature_group_id;
  int shared_with_project;
  bool shared_entirely;
};

struct SharedFeatureRow {
  int feature_group_id;
  int shared_with_project;
  char feature[SHARED_FEATURE_NAME_SIZE];
};

struct RestrictedFeatureGroupRow {
  int id;
  int feature_group_id;
  bool can_access_entirely;
};

struct RestrictedFeatureRow {
  int restricted_feature_group_access;
  char feature[SHARED_FEATURE_NAME_SIZE];
};

/*
 * SELECT feature_group, shared_with_project, shared_entirely
 * FROM hopsworks.shared_feature_group
 * WHERE shared_with_project IN ({member project ids})
 */
static RS_Status find_shared_feature_groups_int(
  Ndb *ndb_object,
  std::vector<HopsworksProjectTeam> *project_team_vec,
  std::vector<SharedFeatureGroupRow> *rows) {

  if (project_team_vec->empty()) {
    return RS_OK;
  }
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;
  RS_Status status = open_grant_scan_int(ndb_object,
                                         SHARED_FEATURE_GROUP,
                                         &table_dict,
                                         &tx,
                                         &scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  int col_id = table_dict->getColumn("shared_with_project")->getColumnNo();
  NdbScanFilter filter(scanOp);
  if (unlikely(filter.begin(NdbScanFilter::OR) < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  for (Uint32 i = 0; i < project_team_vec->size(); i++) {
    if (unlikely(filter.eq(col_id,
                 (Uint32)(*project_team_vec)[i].project_id) < 0)) {
      err = filter.getNdbError();
      ndb_object->closeTransaction(tx);
      return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
    }
  }
  if (unlikely(filter.end() < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  NdbRecAttr *feature_group = scanOp->getValue("feature_group");
  NdbRecAttr *shared_with_project = scanOp->getValue("shared_with_project");
  NdbRecAttr *shared_entirely = scanOp->getValue("shared_entirely");
  if (unlikely(feature_group == nullptr || shared_with_project == nullptr ||
               shared_entirely == nullptr)) {
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  bool check;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      SharedFeatureGroupRow row;
      row.feature_group_id = feature_group->int32_value();
      row.shared_with_project = shared_with_project->int32_value();
      row.shared_entirely = shared_entirely->int8_value() == 1;
      rows->push_back(row);
    } while ((check = scanOp->nextResult(false)) == 0);
  }
  NdbError error = scanOp->getNdbError();
  ndb_object->closeTransaction(tx);
  if (unlikely(error.code != 4120 /*Scan already complete*/)) {
    return RS_RONDB_SERVER_ERROR(
      error, "Failed Reading API Key. Fn find_shared_feature_groups_int");
  }
  return RS_OK;
}

/*
 * SELECT feature_group, shared_with_project, feature
 * FROM hopsworks.shared_feature
 * WHERE shared_with_project IN ({member project ids})
 */
static RS_Status find_shared_features_int(
  Ndb *ndb_object,
  std::vector<HopsworksProjectTeam> *project_team_vec,
  std::vector<SharedFeatureRow> *rows) {

  if (project_team_vec->empty()) {
    return RS_OK;
  }
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;
  RS_Status status = open_grant_scan_int(ndb_object,
                                         SHARED_FEATURE,
                                         &table_dict,
                                         &tx,
                                         &scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  int col_id = table_dict->getColumn("shared_with_project")->getColumnNo();
  NdbScanFilter filter(scanOp);
  if (unlikely(filter.begin(NdbScanFilter::OR) < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  for (Uint32 i = 0; i < project_team_vec->size(); i++) {
    if (unlikely(filter.eq(col_id,
                 (Uint32)(*project_team_vec)[i].project_id) < 0)) {
      err = filter.getNdbError();
      ndb_object->closeTransaction(tx);
      return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
    }
  }
  if (unlikely(filter.end() < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  NdbRecAttr *feature_group = scanOp->getValue("feature_group");
  NdbRecAttr *shared_with_project = scanOp->getValue("shared_with_project");
  NdbRecAttr *feature = scanOp->getValue("feature");
  if (unlikely(feature_group == nullptr || shared_with_project == nullptr ||
               feature == nullptr)) {
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  assert(SHARED_FEATURE_NAME_SIZE ==
         (Uint32)table_dict->getColumn("feature")->getSizeInBytes());
  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  bool check;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      SharedFeatureRow row;
      row.feature_group_id = feature_group->int32_value();
      row.shared_with_project = shared_with_project->int32_value();
      Uint32 feature_attr_bytes;
      const char *feature_data_start = nullptr;
      if (unlikely(GetByteArray(
                     feature, &feature_data_start, &feature_attr_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
      }
      if (unlikely(sizeof(row.feature) < feature_attr_bytes)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUFFER_TOO_SMALL)));
      }
      memcpy(row.feature, feature_data_start, feature_attr_bytes);
      row.feature[feature_attr_bytes] = '\0';
      rows->push_back(row);
    } while ((check = scanOp->nextResult(false)) == 0);
  }
  NdbError error = scanOp->getNdbError();
  ndb_object->closeTransaction(tx);
  if (unlikely(error.code != 4120 /*Scan already complete*/)) {
    return RS_RONDB_SERVER_ERROR(
      error, "Failed Reading API Key. Fn find_shared_features_int");
  }
  return RS_OK;
}

/*
 * SELECT id, feature_group, can_access_entirely
 * FROM hopsworks.restricted_feature_group_access
 * WHERE granted_to_user = {uid}
 */
static RS_Status find_restricted_feature_groups_int(
  Ndb *ndb_object,
  int uid,
  std::vector<RestrictedFeatureGroupRow> *rows) {

  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;
  RS_Status status = open_grant_scan_int(ndb_object,
                                         RESTRICTED_FEATURE_GROUP_ACCESS,
                                         &table_dict,
                                         &tx,
                                         &scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  int col_id = table_dict->getColumn("granted_to_user")->getColumnNo();
  NdbScanFilter filter(scanOp);
  if (unlikely(filter.begin(NdbScanFilter::AND) < 0 ||
               filter.eq(col_id, (Uint32)uid) < 0 ||
               filter.end() < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  NdbRecAttr *row_id = scanOp->getValue("id");
  NdbRecAttr *feature_group = scanOp->getValue("feature_group");
  NdbRecAttr *can_access_entirely = scanOp->getValue("can_access_entirely");
  if (unlikely(row_id == nullptr || feature_group == nullptr ||
               can_access_entirely == nullptr)) {
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  bool check;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      RestrictedFeatureGroupRow row;
      row.id = row_id->int32_value();
      row.feature_group_id = feature_group->int32_value();
      row.can_access_entirely = can_access_entirely->int8_value() == 1;
      rows->push_back(row);
    } while ((check = scanOp->nextResult(false)) == 0);
  }
  NdbError error = scanOp->getNdbError();
  ndb_object->closeTransaction(tx);
  if (unlikely(error.code != 4120 /*Scan already complete*/)) {
    return RS_RONDB_SERVER_ERROR(
      error,
      "Failed Reading API Key. Fn find_restricted_feature_groups_int");
  }
  return RS_OK;
}

/*
 * SELECT restricted_feature_group_access, feature
 * FROM hopsworks.restricted_feature_access
 * WHERE granted_to_user = {uid}
 */
static RS_Status find_restricted_features_int(
  Ndb *ndb_object,
  int uid,
  std::vector<RestrictedFeatureRow> *rows) {

  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;
  RS_Status status = open_grant_scan_int(ndb_object,
                                         RESTRICTED_FEATURE_ACCESS,
                                         &table_dict,
                                         &tx,
                                         &scanOp);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  int col_id = table_dict->getColumn("granted_to_user")->getColumnNo();
  NdbScanFilter filter(scanOp);
  if (unlikely(filter.begin(NdbScanFilter::AND) < 0 ||
               filter.eq(col_id, (Uint32)uid) < 0 ||
               filter.end() < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_FILTER_FAILED)));
  }
  NdbRecAttr *rfga_id = scanOp->getValue("restricted_feature_group_access");
  NdbRecAttr *feature = scanOp->getValue("feature");
  if (unlikely(rfga_id == nullptr || feature == nullptr)) {
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  assert(SHARED_FEATURE_NAME_SIZE ==
         (Uint32)table_dict->getColumn("feature")->getSizeInBytes());
  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  bool check;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      RestrictedFeatureRow row;
      row.restricted_feature_group_access = rfga_id->int32_value();
      Uint32 feature_attr_bytes;
      const char *feature_data_start = nullptr;
      if (unlikely(GetByteArray(
                     feature, &feature_data_start, &feature_attr_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
      }
      if (unlikely(sizeof(row.feature) < feature_attr_bytes)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUFFER_TOO_SMALL)));
      }
      memcpy(row.feature, feature_data_start, feature_attr_bytes);
      row.feature[feature_attr_bytes] = '\0';
      rows->push_back(row);
    } while ((check = scanOp->nextResult(false)) == 0);
  }
  NdbError error = scanOp->getNdbError();
  ndb_object->closeTransaction(tx);
  if (unlikely(error.code != 4120 /*Scan already complete*/)) {
    return RS_RONDB_SERVER_ERROR(
      error, "Failed Reading API Key. Fn find_restricted_features_int");
  }
  return RS_OK;
}

/*
 * SELECT name, version, feature_store_id FROM hopsworks.feature_group
 * WHERE id = {fg_id}
 *
 * A missing row sets *found to false instead of failing: the grant tables
 * cascade-delete with the feature group, so a dangling id can only be a
 * scan/delete race and the grant is simply skipped.
 */
static RS_Status find_feature_group_int(Ndb *ndb_object,
                                        int fg_id,
                                        char *name /*FEATURE_GROUP_NAME_SIZE*/,
                                        int *version,
                                        int *feature_store_id,
                                        bool *found) {
  *found = false;
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbOperation *ndb_op;

  RS_Status status = select_table(ndb_object,
                                  HOPSWORKS,
                                  FEATURE_GROUP,
                                  &table_dict);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = start_transaction(ndb_object, &tx);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = get_op(ndb_object, tx, FEATURE_GROUP, &ndb_op);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  status = read_tuple(ndb_object, ndb_op);
  if (unlikely(status.http_code != SUCCESS)) {
    ndb_object->closeTransaction(tx);
    return status;
  }
  if (unlikely(ndb_op->equal("id", fg_id) != 0)) {
    err = ndb_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_SET_EQUAL_FAILED)));
  }
  NdbRecAttr *name_attr = ndb_op->getValue("name");
  NdbRecAttr *version_attr = ndb_op->getValue("version");
  NdbRecAttr *feature_store_id_attr = ndb_op->getValue("feature_store_id");
  if (unlikely(name_attr == nullptr || version_attr == nullptr ||
               feature_store_id_attr == nullptr)) {
    err = ndb_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  assert(FEATURE_GROUP_NAME_SIZE ==
         (Uint32)table_dict->getColumn("name")->getSizeInBytes());
  if (unlikely(tx->execute(NdbTransaction::Commit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
  }
  if (ndb_op->getNdbError().classification == NdbError::NoDataFound) {
    ndb_object->closeTransaction(tx);
    return RS_OK;
  }
  Uint32 name_attr_bytes;
  const char *name_data_start = nullptr;
  if (unlikely(GetByteArray(name_attr, &name_data_start, &name_attr_bytes) != 0)) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
  }
  if (unlikely(FEATURE_GROUP_NAME_SIZE < name_attr_bytes + 1)) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUFFER_TOO_SMALL)));
  }
  memcpy(name, name_data_start, name_attr_bytes);
  name[name_attr_bytes] = '\0';
  *version = version_attr->int32_value();
  *feature_store_id = feature_store_id_attr->int32_value();
  *found = true;
  ndb_object->closeTransaction(tx);
  return RS_OK;
}

/*
 * Assemble the table/column-level grants of the user: feature groups
 * shared whole or feature-wise with the user's (non-restricted) member
 * projects, plus the user's own restricted_* grants. Feature group ids
 * are joined to (database, online table) via feature_group and
 * feature_store; grants whose FG or store vanished mid-scan are skipped.
 * Multiple grants on the same table merge: whole-table wins over any
 * column list, column lists union.
 */
static RS_Status find_fine_grained_grants_int(
  Ndb *ndb_object,
  int uid,
  std::vector<HopsworksProjectTeam> *member_team_vec,
  HopsworksUserGrants *grants) {

  std::vector<SharedFeatureGroupRow> shared_fgs;
  RS_Status status = find_shared_feature_groups_int(ndb_object,
                                                    member_team_vec,
                                                    &shared_fgs);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  std::vector<SharedFeatureRow> shared_features;
  status = find_shared_features_int(ndb_object,
                                    member_team_vec,
                                    &shared_features);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  std::vector<RestrictedFeatureGroupRow> restricted_fgs;
  status = find_restricted_feature_groups_int(ndb_object,
                                              uid,
                                              &restricted_fgs);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  std::vector<RestrictedFeatureRow> restricted_features;
  if (!restricted_fgs.empty()) {
    status = find_restricted_features_int(ndb_object,
                                          uid,
                                          &restricted_features);
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
  }

  // fg id -> (db, online table); fs id -> db name (lookup caches)
  std::map<int, std::pair<std::string, std::string>> fg_location;
  std::map<int, std::string> fs_names;
  auto locate_fg = [&](int fg_id, bool *fg_found) -> RS_Status {
    *fg_found = false;
    auto it = fg_location.find(fg_id);
    if (it != fg_location.end()) {
      *fg_found = !it->second.first.empty();
      return RS_OK;
    }
    char fg_name[FEATURE_GROUP_NAME_SIZE];
    int fg_version = 0;
    int fs_id = 0;
    bool found = false;
    RS_Status st = find_feature_group_int(ndb_object, fg_id, fg_name,
                                          &fg_version, &fs_id, &found);
    if (unlikely(st.http_code != SUCCESS)) {
      return st;
    }
    if (found) {
      auto fs_it = fs_names.find(fs_id);
      if (fs_it == fs_names.end()) {
        HopsworksProject store_db;
        bool fs_found = false;
        st = find_feature_store_name_int(ndb_object, fs_id, &store_db,
                                         &fs_found);
        if (unlikely(st.http_code != SUCCESS)) {
          return st;
        }
        fs_it = fs_names.emplace(fs_id,
                                 fs_found ? store_db.projectname : "").first;
      }
      if (!fs_it->second.empty()) {
        std::string table_name =
          std::string(fg_name) + "_" + std::to_string(fg_version);
        fg_location[fg_id] = {fs_it->second, table_name};
        *fg_found = true;
        return RS_OK;
      }
      found = false;
    }
    fg_location[fg_id] = {"", ""}; // negative cache: skip this grant
    return RS_OK;
  };

  // (db, table) -> merged grant; whole-table wins over any column list
  struct MergedGrant {
    bool whole_table = false;
    std::set<std::string> columns;
  };
  std::map<std::pair<std::string, std::string>, MergedGrant> merged;
  auto add_grant = [&](int fg_id, bool whole,
                       const std::vector<const char*> &columns) -> RS_Status {
    bool fg_found = false;
    RS_Status st = locate_fg(fg_id, &fg_found);
    if (unlikely(st.http_code != SUCCESS)) {
      return st;
    }
    if (!fg_found) {
      return RS_OK;
    }
    MergedGrant &grant = merged[fg_location[fg_id]];
    if (whole) {
      grant.whole_table = true;
      grant.columns.clear();
    } else if (!grant.whole_table) {
      for (const char *col : columns) {
        grant.columns.insert(col);
      }
    }
    return RS_OK;
  };

  for (const SharedFeatureGroupRow &fg_row : shared_fgs) {
    std::vector<const char*> columns;
    if (!fg_row.shared_entirely) {
      for (const SharedFeatureRow &f_row : shared_features) {
        if (f_row.feature_group_id == fg_row.feature_group_id &&
            f_row.shared_with_project == fg_row.shared_with_project) {
          columns.push_back(f_row.feature);
        }
      }
      if (columns.empty()) {
        // A consistent feature-subset share always has at least one
        // shared_feature row (Hopsworks force-adds the primary key).
        // Zero rows means the share is mid-write (Hopsworks commits the
        // tables in separate transactions) or its writer failed after
        // committing only the parent row. Grant nothing: an empty column
        // set would mean the whole table is granted.
        continue;
      }
    }
    status = add_grant(fg_row.feature_group_id, fg_row.shared_entirely,
                       columns);
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
  }
  for (const RestrictedFeatureGroupRow &fg_row : restricted_fgs) {
    std::vector<const char*> columns;
    if (!fg_row.can_access_entirely) {
      for (const RestrictedFeatureRow &f_row : restricted_features) {
        if (f_row.restricted_feature_group_access == fg_row.id) {
          columns.push_back(f_row.feature);
        }
      }
      if (columns.empty()) {
        // Same fail-closed rule as for shared feature groups above; the
        // window is wider here because Hopsworks commits each
        // restricted_feature row in its own transaction.
        continue;
      }
    }
    status = add_grant(fg_row.feature_group_id, fg_row.can_access_entirely,
                       columns);
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
  }

  for (const auto &entry : merged) {
    HopsworksFineGrant grant;
    grant.db = entry.first.first;
    grant.table = entry.first.second;
    if (!entry.second.whole_table) {
      grant.columns.assign(entry.second.columns.begin(),
                           entry.second.columns.end());
    }
    grants->fine_grants.push_back(std::move(grant));
  }
  return RS_OK;
}

/*
 * All access of the user, from both grant systems.
 *
 * Full-database access = the user's own projects plus feature stores
 * shared entirely with any of those projects. Members holding the
 * 'Feature store restricted' project role get NO member access: their
 * project database is only visible (feature-view metadata) and their data
 * access comes exclusively from the restricted_* rows. Membership grants
 * access to the user's own projects only; Hopsworks never self-shares a
 * store, so both sources are needed.
 */
RS_Status find_user_databases_int(
  Ndb *ndb_object,
  int uid,
  HopsworksUserGrants *grants) {

  HopsworksUsers user;
  RS_Status status = find_user(ndb_object, (Uint32)uid, &user);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  std::vector<HopsworksProjectTeam> team_vec;
  status = find_project_team(ndb_object, &user, &team_vec);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  std::vector<HopsworksProjectTeam> member_vec;
  std::vector<HopsworksProjectTeam> restricted_vec;
  for (const HopsworksProjectTeam &team : team_vec) {
    if (strcmp(team.team_role, FEATURE_STORE_RESTRICTED_ROLE) == 0) {
      restricted_vec.push_back(team);
    } else {
      member_vec.push_back(team);
    }
  }
  std::vector<HopsworksProject> project_vec;
  if (!member_vec.empty()) {
    status = find_projects_vec(ndb_object, &member_vec, &project_vec);
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
    for (const HopsworksProject &project : project_vec) {
      grants->full_dbs.push_back(project.projectname);
    }
  }
  if (!restricted_vec.empty()) {
    project_vec.clear();
    status = find_projects_vec(ndb_object, &restricted_vec, &project_vec);
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
    for (const HopsworksProject &project : project_vec) {
      grants->visible_dbs.push_back(project.projectname);
    }
  }
  std::vector<int> shared_store_ids;
  std::vector<int> placeholder_store_ids;
  status = find_shared_feature_store_ids_int(ndb_object,
                                             &member_vec,
                                             &shared_store_ids,
                                             &placeholder_store_ids);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = append_store_names_int(ndb_object,
                                  &shared_store_ids,
                                  &grants->full_dbs);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = append_store_names_int(ndb_object,
                                  &placeholder_store_ids,
                                  &grants->visible_dbs);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return find_fine_grained_grants_int(ndb_object, uid, &member_vec, grants);
}

RS_Status find_user_databases(int uid, HopsworksUserGrants *grants) {

  Ndb *ndb_object = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb_object);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  METADATA_OP_RETRY_HANDLER(
    grants->full_dbs.clear();
    grants->visible_dbs.clear();
    grants->fine_grants.clear();
    status = find_user_databases_int(ndb_object, uid, grants);
    HandleSchemaErrors(ndb_object, status, {
      std::make_tuple(HOPSWORKS, USERS),
      std::make_tuple(HOPSWORKS, PROJECT_TEAM),
      std::make_tuple(HOPSWORKS, PROJECT),
      std::make_tuple(HOPSWORKS, SHARED_FEATURE_STORE),
      std::make_tuple(HOPSWORKS, SHARED_FEATURE_GROUP),
      std::make_tuple(HOPSWORKS, SHARED_FEATURE),
      std::make_tuple(HOPSWORKS, RESTRICTED_FEATURE_GROUP_ACCESS),
      std::make_tuple(HOPSWORKS, RESTRICTED_FEATURE_ACCESS),
      std::make_tuple(HOPSWORKS, FEATURE_GROUP),
      std::make_tuple(HOPSWORKS, FEATURE_STORE)});
  )
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb_object, &status);
  return status;
}
