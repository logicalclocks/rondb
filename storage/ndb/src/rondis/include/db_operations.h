/*
   Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>
#include "table_definitions.h"

#ifndef STRING_DB_OPERATIONS_H
#define STRING_DB_OPERATIONS_H

const Uint32 ROWS_PER_READ = 2;

/* Callback function setup for DELETE MODULE */
void prepare_delete_value_transaction(struct KeyStorage *key_storage);
void commit_complex_delete_transaction(struct KeyStorage *key_storage);
void prepare_complex_delete_transaction(struct KeyStorage *key_storage);
void prepare_simple_delete_transaction(struct KeyStorage *key_storage);

/* Setup operation record for DELETE MODULE */
int prepare_complex_delete_row(std::string *response,
                               const NdbDictionary::Table *tab,
                               struct KeyStorage *key_storage);
int prepare_simple_delete_row(std::string *response,
                              const NdbDictionary::Table *tab,
                              KeyStorage *key_storage);


/* Callback function setup for SET MODULE */
void commit_write_value_transaction(struct KeyStorage *key_store);
void prepare_write_value_transaction(struct KeyStorage *key_store);
void prepare_write_transaction(struct KeyStorage *key_store);
void commit_simple_write_transaction(struct KeyStorage *key_storage);

/* Setup operation record for SET MODULE */
int prepare_delete_value_row(std::string *response,
                             struct KeyStorage *key_store,
                             Uint32 ordinal,
                             Uint32 database_id);
int prepare_set_value_row(std::string *response,
                          KeyStorage *key_store);
int write_data_to_key_op(std::string *response,
                         const NdbDictionary::Table *tab,
                         KeyStorage *key_store,
                         Uint64 redis_key_id,
                         bool commit_flag,
                         Uint32 row_state,
                         Uint32 database_id);

/* Callback function setup for GET MODULE */
void prepare_read_value_transaction(struct KeyStorage *key_store);
void commit_read_value_transaction(struct KeyStorage *key_store);
void prepare_read_transaction(struct KeyStorage *key_storage);
void commit_simple_read_transaction(struct KeyStorage *key_storage);

/* Setup operation record for GET MODULE */
int prepare_get_value_row(std::string *response,
                          KeyStorage *key_store,
                          bool is_set_command,
                          struct value_table *value_row,
                          Uint32 database_id);
int prepare_get_key_row(std::string *response,
                        KeyStorage *key_store,
                        bool is_set_command,
                        Uint32 database_id);
int prepare_get_simple_key_row(std::string *response,
                               const Uint32,
                               NdbTransaction *trans,
                               struct key_table *key_row,
                               Uint32 database_id);

void execute_set_range_simple(std::string *response,
                              KeyStorage *key_store,
                              const NdbDictionary::Table *tab,
                              Uint32 database_id,
                              Uint32 start,
                              Uint32 end);

int write_key_row_setrange(std::string *response,
                           KeyStorage *key_store,
                           const NdbDictionary::Table *tab,
                           Uint32 database_id,
                           Uint32 start,
                           Uint32 end,
                           Uint32 &old_tot_value_len);

int write_value_row_setrange(std::string *response,
                             KeyStorage *key_store,
                             Uint32 row_id,
                              const NdbDictionary::Dictionary *dict,
                             Uint32 start_zero_index,
                             Uint32 end_zero_index,
                             Uint32 start_write_index,
                             Uint32 end_write_index,
                             const char *start_write_ptr,
                             Uint32 database_id,
                             bool last_row);

/**
 * INCR and DECR MODULE
 * --------------------
 */
void incr_decr_key_row(std::string *response,
                       Ndb *ndb,
                       const NdbDictionary::Table *tab,
                       NdbTransaction *trans,
                       struct key_table *key_row,
                       bool incr_flag,
                       Uint64 inc_dec_value,
                       int worker_id);

/**
 * Uinique key MODULE for Rondis
 * -----------------------------
 */
int rondb_get_rondb_key(const NdbDictionary::Table *tab,
                        Uint64 &key_id,
                        Ndb *ndb,
                        std::string *response);

int rondb_get_redis_key_id(Ndb *ndb,
                           Uint64 &redis_key_id,
                           const char *key_str,
                           Uint32 key_len,
                           std::string *response,
                           Uint32 database_id);
#endif
