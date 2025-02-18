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

#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>
#include "rondb.h"

#ifndef STRING_TABLE_DEFINITIONS_H
#define STRING_TABLE_DEFINITIONS_H

/*
    NdbRecords are used for serialization. They map columns of a table to fields in a struct.
    For each table we interact with, we define:
    - one NdbRecord defining the columns to filter the row we want to read
    - one NdbRecord defining the columns we want to fetch
*/


#define MAX_KEY_VALUE_LEN 3000

/*
    HSET KEY TABLE
*/
#define HSET_KEY_TABLE_NAME "hset_keys"

int init_hset_key_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_hset_key_record[MAX_NUM_DATABASES];
extern NdbRecord *entire_hset_key_record[MAX_NUM_DATABASES];

#define HSET_KEY_TABLE_COL_redis_key "redis_key"
#define HSET_KEY_TABLE_COL_redis_key_id "redis_key_id"

struct hset_key_table
{
    Uint64 redis_key_id;
    char redis_key[MAX_KEY_VALUE_LEN + 2];
};

/*
    KEY AND FIELD TABLE
*/

#define KEY_TABLE_NAME "string_keys"
#define INLINE_VALUE_LEN 4096

int init_key_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_key_record[MAX_NUM_DATABASES];
extern NdbRecord *entire_key_record[MAX_NUM_DATABASES];

/*
    Doing this instead of reflection; Keep these the same
    as the field names in the key_table struct.
*/
#define KEY_TABLE_COL_redis_key_id "redis_key_id"
#define KEY_TABLE_COL_redis_key "redis_key"
#define KEY_TABLE_COL_rondb_key "rondb_key"
#define KEY_TABLE_COL_expiry_date "expiry_date"
#define KEY_TABLE_COL_value_data_type "value_data_type"
#define KEY_TABLE_COL_tot_value_len "tot_value_len"
#define KEY_TABLE_COL_num_rows "num_rows"
#define KEY_TABLE_COL_value_start "value_start"

struct key_table
{
    Uint32 null_bits;
    Uint64 redis_key_id;
    Uint64 rondb_key;
    Int32 expiry_date;
    Uint32 value_data_type;
    Uint32 tot_value_len;
    // Technically implicit
    Uint32 num_rows;
    char redis_key[MAX_KEY_VALUE_LEN + 4];
    char value_start[INLINE_VALUE_LEN + 4];
};

/*
    VALUE TABLE
*/

#define VALUE_TABLE_NAME "string_values"
#define EXTENSION_VALUE_LEN 29500

int init_value_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_value_record[MAX_NUM_DATABASES];
extern NdbRecord *entire_value_record[MAX_NUM_DATABASES];

/*
    Doing this instead of reflection; Keep these the same
    as the field names in the value_table struct.
*/
#define VALUE_TABLE_COL_rondb_key "rondb_key"
#define VALUE_TABLE_COL_ordinal "ordinal"
#define VALUE_TABLE_COL_value "value"
#define VALUE_TABLE_COL_expiry_date "expiry_date"

struct value_table
{
    Uint64 rondb_key;
    Uint32 ordinal;
    Int32 expiry_date;
    char value[EXTENSION_VALUE_LEN + 2];
};

/*
    SHARED/EXPORT
*/

int init_record(NdbDictionary::Dictionary *dict,
                const NdbDictionary::Table *tab,
                std::map<const NdbDictionary::Column *,
                std::pair<size_t, int>> column_info_map,
                NdbRecord *&record);

int init_string_records(NdbDictionary::Dictionary *dict, Uint32 database_id);

enum KeyState {
    /* m_value_size undefined */
    NotCompleted = 0,
    /* Use m_error_code */
    CompletedFailed = 1,
    /* Use m_value_size */
    CompletedSuccess = 2,
    /* Use m_num_rows */
    MultiRow = 3,
    /* Use m_num_rows */
    CompletedMultiRowSuccess = 4,
    MultiRowRWValue = 5,
    MultiRowRWValueSent = 6,
    MultiRowRWAll = 7,
    CompletedMultiRow = 8,
    CompletedReadError = 9
};

#define MAX_PARALLEL_KEY_OPS 1024
#define MAX_VALUES_TO_WRITE 4
#define STRING_REDIS_KEY_ID 0
#define MAX_PARALLEL_VALUE_RWS 2
#define MAX_OUTSTANDING_BYTES (512 * 1024)
#define DELETE_BYTES 2000

enum SetType {
  IsWrite = 0,
  IsInsert = 1,
  IsUpdate = 2
};

struct GetControl;
struct KeyStorage {
    struct GetControl *m_get_ctrl;
    NdbTransaction *m_trans;
    NdbRecAttr *m_rec_attr_prev_num_rows;
    NdbRecAttr *m_rec_attr_rondb_key;
    NdbRecAttr *m_rec_attr_expiry_date;
    char *m_value_ptr;
    const char *m_key_str;
    Uint64 m_rondb_key;
    Uint32 m_key_len;
    char m_header_buf[20];
    bool m_close_flag;
    bool m_keep_ttl;
    bool m_set_ttl;
    Uint32 m_header_len;
    Uint32 m_index;
    Uint32 m_first_value_row;
    Uint32 m_current_pos;
    Uint32 m_num_rows;
    Uint32 m_num_rw_rows;
    Uint32 m_num_current_rw_rows;
    Uint32 m_prev_num_rows;
    Int64 m_expire_at;
    union {
        Uint32 m_value_size;
        Uint32 m_error_code;
    };
    enum KeyState m_key_state;
    enum SetType m_set_type;
    struct key_table m_key_row;
    char m_key_buf[16];
};

class Ndb;
struct GetControl {
    Ndb *m_ndb;
    bool m_is_set_command;
    struct KeyStorage *m_key_store;
    struct value_table *m_value_rows;
    Uint32 m_next_value_row;
    Uint32 m_num_transactions;
    Uint32 m_num_keys_requested;
    Uint32 m_num_keys_outstanding;
    Uint32 m_num_bytes_outstanding;
    Uint32 m_num_keys_completed_first_pass;
    Uint32 m_num_keys_multi_rows;
    Uint32 m_num_keys_failed;
    Uint32 m_num_read_errors;
    Uint32 m_error_code;
    Uint32 m_database_id;
};
#endif
