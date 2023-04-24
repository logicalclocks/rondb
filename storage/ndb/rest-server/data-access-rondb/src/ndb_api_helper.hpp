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

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_NDB_API_HELPER_H_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_NDB_API_HELPER_H_

#include "src/rdrs-dal.h"
#include <NdbApi.hpp>

RS_Status start_transaction(Ndb *ndb_object, NdbTransaction **tx);

RS_Status select_table(Ndb *ndb_object, const char *database_str, const char *table_str,
                       const NdbDictionary::Table **table_dict);

RS_Status get_index_scan_op(Ndb *ndb_object, NdbTransaction *tx,
                            const NdbDictionary::Table *table_dict, const char *index_name,
                            NdbScanOperation **scanOp);

RS_Status read_tuples(Ndb *ndb_object, NdbScanOperation *scanOp);

#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_NDB_API_HELPER_H_
