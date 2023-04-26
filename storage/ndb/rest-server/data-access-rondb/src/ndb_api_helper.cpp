/*
 * Copyright (C) 2023 Hopsworks AB
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

#include "src/ndb_api_helper.hpp"
#include <iostream>
#include <string>
#include "src/error-strings.h"
#include "src/status.hpp"

RS_Status select_table(Ndb *ndb_object, const char *database_str, const char *table_str,
                       const NdbDictionary::Table **table_dict) {
  if (ndb_object->setCatalogName(database_str) != 0) {
    return RS_CLIENT_ERROR(ERROR_011 + std::string(" Database: ") + std::string(database_str) +
                           std::string(". Table: ") + std::string(table_str));
  }

  const NdbDictionary::Dictionary *dict = ndb_object->getDictionary();
  *table_dict                           = dict->getTable(table_str);

  if (*table_dict == nullptr) {
    return RS_CLIENT_ERROR(ERROR_011 + std::string(" Database: ") + std::string(database_str) +
                           std::string(". Table: ") + std::string(table_str));
  }
  return RS_OK;
}

RS_Status start_transaction(Ndb *ndb_object, NdbTransaction **tx) {
  NdbError err;
  *tx = ndb_object->startTransaction();
  if (*tx == nullptr) {
    err = ndb_object->getNdbError();
    return RS_RONDB_SERVER_ERROR(err, ERROR_005);
  }
  return RS_OK;
}

RS_Status get_index_scan_op(Ndb *ndb_object, NdbTransaction *tx,
                            const NdbDictionary::Table *table_dict, const char *index_name,
                            NdbScanOperation **scanOp) {
  NdbError err;
  const NdbDictionary::Dictionary *dict = ndb_object->getDictionary();
  const NdbDictionary::Index *index     = dict->getIndex(index_name, table_dict->getName());

  if (index == nullptr) {
    return RS_SERVER_ERROR(ERROR_032 + std::string(" Index: ") + std::string(index_name));
  }

  *scanOp = tx->getNdbIndexScanOperation(index);
  if (*scanOp == nullptr) {
    err = ndb_object->getNdbError();
    return RS_RONDB_SERVER_ERROR(err, ERROR_029);
  }
  return RS_OK;
}

RS_Status get_scan_op(Ndb *ndb_object, NdbTransaction *tx, const char *table_name,
                      NdbScanOperation **scanOp) {

  *scanOp = tx->getNdbScanOperation(table_name);
  if (*scanOp == nullptr) {
    NdbError err = ndb_object->getNdbError();
    return RS_RONDB_SERVER_ERROR(err, ERROR_029);
  }
  return RS_OK;
}

RS_Status read_tuples(Ndb *ndb_object, NdbScanOperation *scanOp) {
  NdbError err;
  if (scanOp->readTuples(NdbOperation::LM_Exclusive) != 0) {
    err = ndb_object->getNdbError();
    return RS_RONDB_SERVER_ERROR(err, ERROR_030);
  }
  return RS_OK;
}

