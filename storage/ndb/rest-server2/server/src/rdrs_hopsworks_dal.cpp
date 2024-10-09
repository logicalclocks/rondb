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

#include <cstring>
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
#define DEB_DAL(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_DAL(arglist) do { } while (0)
#endif


// RonDB connection pool
extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;

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
  assert(col_size == API_KEY_PREFIX_SIZE);
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
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }
  NdbRecAttr *user_id = scanOp->getValue("user_id");
  NdbRecAttr *secret = scanOp->getValue("secret");
  NdbRecAttr *salt = scanOp->getValue("salt");
  NdbRecAttr *name = scanOp->getValue("name");

  assert(API_KEY_SECRET_SIZE ==
         (Uint32)table_dict->getColumn("secret")->getSizeInBytes());
  assert(API_KEY_SALT_SIZE ==
         (Uint32)table_dict->getColumn("salt")->getSizeInBytes());
  assert(API_KEY_NAME_SIZE ==
         (Uint32)table_dict->getColumn("name")->getSizeInBytes());

  if (unlikely(user_id == nullptr ||
               secret == nullptr ||
               salt == nullptr ||
               name == nullptr)) {
    err = scanOp->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_019);
  }
  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_009);
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
        return RS_CLIENT_ERROR(ERROR_019);
      }
      Uint32 salt_attr_bytes;
      const char *salt_data_start = nullptr;
      if (unlikely(GetByteArray(
                     salt, &salt_data_start, &salt_attr_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(ERROR_019);
      }
      Uint32 secret_attr_bytes;
      const char *secret_data_start = nullptr;
      if (unlikely(GetByteArray(
                     secret, &secret_data_start, &secret_attr_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(ERROR_019);
      }
      // <= because we want to leave one byte for '\0'
      // sizes of char arrays are set to accommodate additional '\0'
      if (unlikely(sizeof(api_key->secret) <= secret_attr_bytes ||
                   sizeof(api_key->name) <= name_attr_bytes ||
                   sizeof(api_key->salt) <= salt_attr_bytes)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(ERROR_021);
      }
      memcpy(api_key->name, name_data_start, name_attr_bytes);
      api_key->name[name_attr_bytes] = '\0';

      memcpy(api_key->secret, secret_data_start, secret_attr_bytes);
      api_key->secret[secret_attr_bytes] = '\0';

      memcpy(api_key->salt, salt_data_start, salt_attr_bytes);
      api_key->salt[salt_attr_bytes] = '\0';

      api_key->user_id = user_id->int32_value();
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
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }
  NdbRecAttr *email = scanOp->getValue("email");
  if (unlikely(email == nullptr)) {
    return RS_RONDB_SERVER_ERROR(err, ERROR_019);
  }
  assert(USERS_EMAIL_SIZE ==
         (Uint32)table_dict->getColumn("email")->getSizeInBytes());

  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_009);
  }
  bool check = 0;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      Uint32 email_attr_bytes;
      const char *email_data_start = nullptr;
      if (unlikely(GetByteArray(
                     email, &email_data_start, &email_attr_bytes) != 0)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(ERROR_019);
      }
      if (unlikely(sizeof(users->email) < email_attr_bytes)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(ERROR_021);
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
  assert(col_size == PROJECT_TEAM_TEAM_MEMBER_SIZE);

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
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }
  NdbRecAttr *project_id = scanOp->getValue("project_id");
  if (unlikely(project_id == nullptr)) {
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_019);
  }
  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_009);
  }
  bool check;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      HopsworksProjectTeam project_team;
      project_team.project_id = project_id->int32_value();
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
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }
  for (Uint32 i = 0; i < project_team_vec->size(); i++) {
    if (unlikely(filter.eq(col_id,
                 (Uint32)(*project_team_vec)[i].project_id) < 0)) {
      err = filter.getNdbError();
      ndb_object->closeTransaction(tx);
      return RS_RONDB_SERVER_ERROR(err, ERROR_031);
    }
  }
  if (unlikely(filter.end() < 0)) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }
  NdbRecAttr *projectname = scanOp->getValue("projectname");
  if (unlikely(projectname == nullptr)) {
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_019);
  }
  assert(PROJECT_PROJECTNAME_SIZE ==
         (Uint32)table_dict->getColumn("projectname")->getSizeInBytes());

  if (unlikely(tx->execute(NdbTransaction::NoCommit) != 0)) {
    err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_009);
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
        return RS_CLIENT_ERROR(ERROR_019);
      }

      if (unlikely(sizeof(project.projectname) < projectname_attr_bytes)) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(ERROR_021);
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

RS_Status find_all_projects_int(
  Ndb *ndb_object,
  int uid,
  std::vector<HopsworksProject> *project_vec) {

  HopsworksUsers user;
  RS_Status status = find_user(ndb_object, (Uint32)uid, &user);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  std::vector<HopsworksProjectTeam> project_team_vec;
  status = find_project_team(ndb_object, &user, &project_team_vec);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  status = find_projects_vec(ndb_object, &project_team_vec, project_vec);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RS_Status find_all_projects(int uid, char ***projects, int *count) {

  RS_Status status;
  std::vector<HopsworksProject> project_vec;

  Ndb *ndb_object = nullptr;
  status = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb_object);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  METADATA_OP_RETRY_HANDLER(
    project_vec.clear();
    status = find_all_projects_int(ndb_object, uid, &project_vec);
    HandleSchemaErrors(ndb_object, status, {
      std::make_tuple(HOPSWORKS, USERS),
      std::make_tuple(HOPSWORKS, PROJECT_TEAM),
      std::make_tuple(HOPSWORKS, PROJECT)});
  )
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb_object, &status);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  size_t malloc_size = 0;
  malloc_size += (project_vec.size() * sizeof(char*));
  for (Uint32 i = 0; i < project_vec.size(); i++) {
    size_t name_size = (strlen(project_vec[i].projectname) + 1) * sizeof(char);
    DEB_DAL(("i = %u, db: %s, len(db name): %u",
             i, project_vec[i].projectname, (Uint32)name_size));
    malloc_size += name_size;
  }
  *projects = (char **)malloc(malloc_size);
  if (unlikely(*projects == nullptr)) {
    return CRS_Status(HTTP_CODE::SERVER_ERROR,
      "Failed to allocate database names in API key").status;
  }
  *count = project_vec.size();
  char **ease = *projects;
  char *str_ptr = (char*)*projects;
  str_ptr += (project_vec.size() * sizeof(char*));
  for (Uint32 i = 0; i < project_vec.size(); i++) {
    ease[i] = str_ptr;
    size_t name_size = (strlen(project_vec[i].projectname) + 1) * sizeof(char);
    memcpy(str_ptr, project_vec[i].projectname, name_size);
    str_ptr += name_size;
  }
  return RS_OK;
}
