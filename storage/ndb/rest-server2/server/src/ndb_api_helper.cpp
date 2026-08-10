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

#include "ndb_api_helper.hpp"
#include "error_strings.h"
#include "status.hpp"
#include <my_compiler.h>

#include <iostream>
#include <string>

// Internal NDB dictionary impl: clearing the sticky dictionary error has
// no public API, so drive it through the impl accessor (same pattern as
// rdrs_rondb_connection_pool.cpp).
#include "NdbDictionaryImpl.hpp"

void ndb_dict_clear_error(const NdbDictionary::Dictionary *dict) {
  /* getTable() does not clear the dictionary error on entry, and a few of
   * its null-return paths set no error at all. Pooled Ndb objects live
   * for the process lifetime, so without this the 404-vs-500 decision
   * after a failed lookup could read an error left over from an earlier,
   * unrelated dictionary call. m_error is mutable by design ("allow
   * update error from const methods"). */
  NdbDictionaryImpl::getImpl(*dict).m_error.code = 0;
}

bool ndb_dict_object_missing(int dict_error_code) {
  /* Callers clear the dictionary error before the lookup
   * (ndb_dict_clear_error), so code 0 means the lookup failed without
   * reporting why (a rare allocation-failure path) - keep the historic
   * 404 for it rather than inventing a server error. */
  switch (dict_error_code) {
  case 0:     /* dictionary reports no error at all */
  case 709:   /* No such table existed */
  case 723:   /* No such table existed */
  case 4377:  /* Invalid schema/database name in the request path */
    return true;
  default:
    return false;
  }
}

bool ndb_error_cluster_unavailable(int error_code) {
  /* Only the states where the API node has lost its transporter
   * connection to EVERY data node. Those are the states that strand the
   * NDB API in CS_waiting_for_clean_cache (so only a full reconnection
   * recovers), and losing the last data node is also what guarantees
   * TE_CLUSTER_FAILURE was delivered to the long-lived event subscribers,
   * so they hand their Ndb objects back and the teardown converges.
   *
   * Deliberately NOT included, although
   * ClusterMgr::is_cluster_completely_unavailable() can report them:
   * 4036/4038/4039/4041 (alive nodes exist - transporters are up, the
   * NDB API recovers by itself) and 4037 (nodes starting up - once one
   * reaches STARTED this becomes 4035 if the connection is stuck). */
  switch (error_code) {
  case 4009:  /* Cluster failure - no data node reachable */
  case 4035:  /* Cluster temporary unavailable - none alive */
  case 4040:  /* No data node ever connected */
    return true;
  default:
    return false;
  }
}

RS_Status select_table(Ndb *ndb_object,
                       const char *database_str,
                       const char *table_str,
                       const NdbDictionary::Table **table_dict) {
  /* Every caller passes internal metadata table names (hopsworks.*,
   * feature store tables), never client input: a missing table here is a
   * broken/absent schema, i.e. a server-side problem, not a client
   * error. */
  if (unlikely(ndb_object->setCatalogName(database_str) != 0)) {
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_DB_TABLE_NOT_EXIST)) +
      std::string(" Database: ") + std::string(database_str) +
      std::string(". Table: ") + std::string(table_str));
  }
  const NdbDictionary::Dictionary *dict = ndb_object->getDictionary();
  ndb_dict_clear_error(dict);
  *table_dict = dict->getTable(table_str);
  if (unlikely(*table_dict == nullptr)) {
    if (unlikely(!ndb_dict_object_missing(dict->getNdbError().code))) {
      /* The dictionary lookup itself failed (e.g. cluster unavailable).
       * The table may well exist, so report the real NDB error instead of
       * pretending the table is missing. */
      return RS_RONDB_SERVER_ERROR(dict->getNdbError(),
        std::string(rdrsErrorMessage(ERROR_TABLE_METADATA_READ_FAILED)) +
        std::string(" Database: ") + std::string(database_str) +
        std::string(". Table: ") + std::string(table_str));
    }
    return RS_SERVER_ERROR(
    std::string(rdrsErrorMessage(ERROR_DB_TABLE_NOT_EXIST)) +
    std::string(" Database: ") + std::string(database_str) +
    std::string(". Table: ") + std::string(table_str));
  }
  return RS_OK;
}

RS_Status start_transaction(Ndb *ndb_object, NdbTransaction **tx) {
  NdbError err;
  *tx = ndb_object->startTransaction();
  if (unlikely(*tx == nullptr)) {
    err = ndb_object->getNdbError();
    return RS_RONDB_SERVER_ERROR(err, 
        std::string(rdrsErrorMessage(ERROR_TRANSACTION_START_FAILED)));
  }
  return RS_OK;
}

RS_Status get_index_scan_op(Ndb *ndb_object,
                            NdbTransaction *tx,
                            const NdbDictionary::Table *table_dict,
                            const char *index_name,
                            NdbScanOperation **scanOp) {
  NdbError err;
  const NdbDictionary::Dictionary *dict = ndb_object->getDictionary();
  const NdbDictionary::Index *index =
    dict->getIndex(index_name, table_dict->getName());

  if (unlikely(index == nullptr)) {
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_LOAD_INDEX_FAILED)) + 
      std::string(" Index: ") + std::string(index_name));
  }
  *scanOp = tx->getNdbIndexScanOperation(index, table_dict);
  if (unlikely(*scanOp == nullptr)) {
    err = tx->getNdbError();
    return RS_RONDB_SERVER_ERROR(err,
        std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)));
  }
  return RS_OK;
}

RS_Status get_scan_op(Ndb *ndb_object,
                      NdbTransaction *tx,
                      const char *table_name,
                      NdbScanOperation **scanOp) {

  *scanOp = tx->getNdbScanOperation(table_name);
  if (unlikely(*scanOp == nullptr)) {
    NdbError err = ndb_object->getNdbError();
    return RS_RONDB_SERVER_ERROR(err, 
        std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)));
  }
  return RS_OK;
}

RS_Status read_tuples(Ndb *ndb_object, NdbScanOperation *scanOp) {
  NdbError err;
  if (unlikely(scanOp->readTuples(NdbOperation::LM_Exclusive) != 0)) {
    err = ndb_object->getNdbError();
    return RS_RONDB_SERVER_ERROR(err, 
        std::string(rdrsErrorMessage(ERROR_SET_LOCK_MODE_FAILED_TUPLES)));
  }
  return RS_OK;
}

RS_Status read_tuple(Ndb *ndb_object, NdbOperation *ndbOp) {
  NdbError err;
  if (unlikely(ndbOp->readTuple(NdbOperation::LM_Exclusive) != 0)) {
    err = ndb_object->getNdbError();
    return RS_RONDB_SERVER_ERROR(err, 
        std::string(rdrsErrorMessage(ERROR_SET_LOCK_MODE_FAILED)));
  }
  return RS_OK;
}

RS_Status get_op(Ndb *ndb_object, NdbTransaction *tx, const char *table_name,
                 NdbOperation **ndbOp) {
  *ndbOp = tx->getNdbOperation(table_name);
  if (unlikely(*ndbOp == nullptr)) {
    NdbError err = ndb_object->getNdbError();
    return RS_RONDB_SERVER_ERROR(err, 
        std::string(rdrsErrorMessage(ERROR_READ_OPERATION_FAILED)));
  }
  return RS_OK;
}
