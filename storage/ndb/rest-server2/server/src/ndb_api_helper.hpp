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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_NDB_API_HELPER_H_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_NDB_API_HELPER_H_

#include "rdrs_dal.h"

#include <NdbApi.hpp>

/**
 * Clear the dictionary's sticky error. Call before a getTable() whose
 * error will be used to classify a failure: the NDB API never resets it
 * on entry, and pooled Ndb objects keep it across unrelated requests.
 */
void ndb_dict_clear_error(const NdbDictionary::Dictionary *dict);

/**
 * Classify a dictionary lookup failure (getTable() returned nullptr).
 * Returns true when the dictionary positively reports that the object does
 * not exist. Returns false when the lookup itself failed (e.g. no data
 * node was available to answer) and the object may well exist, in which
 * case the failure must be reported as a server error, never as 404.
 *
 * @param dict_error_code dict->getNdbError().code after the failed lookup
 */
bool ndb_dict_object_missing(int dict_error_code);

/**
 * True for NdbError codes meaning this API node has lost connectivity to
 * every data node - the states only a full reconnection of the cluster
 * connection can recover from. Used to trigger reconnection; states with
 * alive data nodes (single node failure, nodes stopping, single user
 * mode, version mismatch) must not tear the connection down.
 */
bool ndb_error_cluster_unavailable(int error_code);

RS_Status start_transaction(Ndb *ndb_object, NdbTransaction **tx);

RS_Status select_table(Ndb *ndb_object,
                       const char *database_str,
                       const char *table_str,
                       const NdbDictionary::Table **table_dict);

RS_Status get_index_scan_op(Ndb *ndb_object,
                            NdbTransaction *tx,
                            const NdbDictionary::Table *table_dict,
                            const char *index_name,
                            NdbScanOperation **scanOp);

RS_Status get_scan_op(Ndb *ndb_object,
                      NdbTransaction *tx,
                      const char *table_name,
                      NdbScanOperation **scanOp);

RS_Status get_op(Ndb *ndb_object,
                 NdbTransaction *tx,
                 const char *table_name,
                 NdbOperation **ndbOp);

RS_Status read_tuples(Ndb *ndb_object, NdbScanOperation *scanOp);

RS_Status read_tuple(Ndb *ndb_object, NdbOperation *ndbOp);

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_NDB_API_HELPER_H_
