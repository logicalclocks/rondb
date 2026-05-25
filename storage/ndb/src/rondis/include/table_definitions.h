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

// Phase 1.10c.1: Source of fresh hash redis_key_id values. HSET
// pre-allocates from this table's AUTO_INCREMENT (via
// Ndb::getAutoIncrementValue, batch=1024) and writes the explicit
// value into hset_keys.redis_key_id only for hash-owned rows.
// Strings keep redis_key_id IS NULL.
#define HSET_KEY_ID_SEQUENCE_TABLE_NAME "hset_key_id_sequence"

int init_hset_key_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_hset_key_record[MAX_NUM_DATABASES];
extern NdbRecord *entire_hset_key_record[MAX_NUM_DATABASES];

#define HSET_KEY_TABLE_COL_redis_key "redis_key"
#define HSET_KEY_TABLE_COL_redis_key_id "redis_key_id"
#define HSET_KEY_TABLE_COL_field_count "field_count"
#define HSET_KEY_TABLE_COL_expiry_date "expiry_date"

struct hset_key_table
{
    // Phase 1.10c.1: redis_key_id is also nullable. NULL = string row
    // (registered by SET / MSET); non-null = hash row.
    // Bit assignment in null_bits (per init_hset_key_records):
    //   bit 0 -> redis_key_id
    //   bit 1 -> expiry_date
    Uint32 null_bits;
    Uint64 redis_key_id;
    Int32 expiry_date;
    Uint32 field_count;
    char redis_key[MAX_KEY_VALUE_LEN + 2];
};

/*
    KEY AND FIELD TABLE
*/

#define KEY_TABLE_NAME "string_keys"
#define INLINE_VALUE_LEN 4096

int init_key_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_key_record[MAX_NUM_DATABASES];
// Phase 1.10c.7b: NdbRecord on string_keys's PRIMARY ordered
// index, used by run_hset_replace_hash_scan_delete's scanIndex
// to walk only rows with redis_key_id == old_id.
extern NdbRecord *pk_key_index_record[MAX_NUM_DATABASES];
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
    Uint64 rondb_key;
    Int32 expiry_date;
    Uint32 value_data_type;
    Uint32 tot_value_len;
    // Technically implicit
    Uint32 num_rows;
    /**
     * redis_key_id and redis_key must be in this order and next
     * to each other to ensure startTransaction with hint works.
     */
    Uint64 redis_key_id;
    char redis_key[MAX_KEY_VALUE_LEN + 8];
    char value_start[INLINE_VALUE_LEN + 8];
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
    Uint32 null_bits;
    Uint64 rondb_key;
    Uint32 ordinal;
    Int32 expiry_date;
    char value[EXTENSION_VALUE_LEN + 8];
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
    CompletedMultiRowSuccessCommit = 5,
    MultiRowRWValue = 6,
    MultiRowRWValueSent = 7,
    MultiRowRWAll = 8,
    CompletedMultiRow = 9,
    CompletedReadError = 10,
    // The SET write tripped an NX / XX guard (NDB 6000 sentinel from
    // the interpreted-code program). Not a failure: Redis-canonical
    // reply is the nil bulk string. Only ever set on the single-key
    // plain SET path (MSET / HSET do not expose conditional flags).
    CompletedConditionalFail = 11
};

#define MAX_PARALLEL_KEY_OPS 256
#define MAX_VALUES_TO_WRITE 4
#define STRING_REDIS_KEY_ID 0
#define MAX_PARALLEL_VALUE_RWS 2
#define MAX_OUTSTANDING_BYTES (512 * 1024)
#define MAX_REDIS_ROW_SIZE (512 * 1024)
#define DELETE_BYTES 2000

enum SetType {
  IsWrite = 0,
  IsInsert = 1,
  IsUpdate = 2,
  IsGet = 3
};

struct GetControl;
struct KeyStorage {
    struct GetControl *m_get_ctrl;
    NdbTransaction *m_trans;
    NdbRecAttr *m_rec_attr_prev_num_rows;
    NdbRecAttr *m_rec_attr_rondb_key;
    NdbRecAttr *m_rec_attr_expiry_date;
    // Receives OUTPUT_INDEX_3 from the SET write interpreter program:
    // 1 on the INSERT branch (new field), 0 on UPDATE. Aggregated into
    // GetControl::m_num_new_fields for the HSET reply (C10).
    NdbRecAttr *m_rec_attr_new_field;
    // Phase 1.10c.7b: per-key dual-claim's OUTPUT_INDEX_0 = old
    // hset_keys.redis_key_id captured during set_simple_rows
    // Phase A. After Phase B's NoCommit drain, if non-zero we run
    // run_hset_replace_hash_scan_delete on this trans before
    // dispatching the commit. nullptr for HSET (set_rows_hset uses
    // a different lock-claim op).
    NdbRecAttr *m_rec_attr_string_claim_old_id;
    NdbRecAttr *m_rec_attr_string_claim_old_expiry;
    union {
      char *m_value_ptr;
      const char *m_const_value_ptr;
    };
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
    // Phase 1.0.3 single-trans HDEL: per-field op handle (the
    // Phase-2 deleteTuple-with-readback) and "row was found" flag
    // set by the Phase-2 callback. The deleteTuple is staged
    // (NoCommit) and reads back num_rows + rondb_key before the
    // delete applies. m_num_rows is reused to carry the ext-row
    // count when a field has overflow; m_rondb_key carries the
    // row's rondb_key for ext-row PK addressing in Phase 3.
    const NdbOperation *m_hdel_phase2_op;
    bool m_hdel_field_present;
    bool m_del_logically_absent;
    Int64 m_expire_at;
    union {
        Uint32 m_get_value_size;
        Uint32 m_error_code;
    };
    Uint32 m_set_value_size;
    enum KeyState m_key_state;
    enum KeyState m_get_key_state;
    enum SetType m_set_type;
    struct key_table m_key_row;
    char m_key_buf[16];
};

class Ndb;
struct GetControl {
    Ndb *m_ndb;
    bool m_is_set_command;
    bool m_get_cmd_part;
    int m_worker_id;
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
    // Running count of fields/rows whose SET write actually inserted
    // rather than overwrote. Used by HSET's Redis-canonical reply
    // (C10) - incremented by write_callback when OUTPUT_INDEX_3 from
    // the interpreter program is 1.
    Uint32 m_num_new_fields;
    Uint32 m_num_read_errors;
    Uint32 m_error_code;
    Uint32 m_database_id;
    // Set by rondb_mset when the batch is a hash write (is_hmset /
    // rondb_hset_command); used by the single-trans HSET state
    // machine (Phase 1.0.2d). Points into argv[1]'s backing storage,
    // so lifetime spans the batch. Empty for plain SET/MSET batches.
    const char *m_hash_name_ptr;
    Uint32 m_hash_name_len;
    const NdbDictionary::Table *m_hset_key_tab;
    // State for set_rows_hset's three-phase pipeline.
    // Pre-allocated id (from getAutoIncrementValue) handed to Phase 1's
    // interpreter; written into hset_keys on the INSERT branch,
    // discarded on the UPDATE branch.
    Uint64 m_hset_prealloc_id;
    // Captured by Phase 1's callback from the lock-claim op's
    // OUTPUT_INDEX_0 (existing redis_key_id, or m_hset_prealloc_id
    // if INSERT branch ran) and OUTPUT_INDEX_1 (existing field_count,
    // or 0 if INSERT). Phase 2 uses redis_key_id to PK-address each
    // field_row write; Phase 3 uses field_count + delta to write the
    // new count.
    Uint64 m_hset_redis_key_id;
    Uint32 m_hset_field_count_pre;
    // NdbRecAttr handles for Phase 1's three output values.
    // m_rec_attr_hset_was_string is OUTPUT_INDEX_2: 1 if the
    // existing hset_keys row was a string and got claimed as a
    // hash by Phase 1's UPDATE-on-string branch (Phase 1.10c.7a),
    // 0 otherwise. The callback copies it into
    // m_hset_was_string_replaced and Phase 1.5 dispatches the
    // string-row + ext-row deletes when set.
    NdbRecAttr *m_rec_attr_hset_id;
    NdbRecAttr *m_rec_attr_hset_field_count;
    NdbRecAttr *m_rec_attr_hset_was_string;
    bool m_hset_was_string_replaced;
    // Phase 2 / Phase 3 chunk-window. set_rows_hset breaks an
    // N-field HSET into MAX_PARALLEL_KEY_OPS-sized batches so a
    // single NoCommit submission does not overrun NDB's per-trans
    // op buffer. The Phase-2 callback iterates only
    // [chunk_start, chunk_start+chunk_count) of m_key_store; the
    // Phase-3 ack callback uses no per-key state.
    Uint32 m_hset_phase_chunk_start;
    Uint32 m_hset_phase_chunk_count;
    // Phase 1.0.3 single-trans HDEL: deleted-field count carried
    // from Phase 2's classifier callback to Phase 3's commit, so
    // the field_count bump uses delta = -m_num_deleted_fields.
    Uint32 m_num_deleted_fields;
    // HDEL Phase 1's lock-read on hset_keys(key) projects into this
    // buffer; the callback consumes m_hset_redis_key_id and
    // m_hset_field_count_pre from it on success. m_hdel_phase1_op
    // is the per-op handle used to read the per-op error code (so
    // we can distinguish 626 "no row" from a real failure under
    // the default AO_IgnoreError that NDB sets for reads).
    struct hset_key_table m_hset_lock_read_buf;
    const NdbOperation *m_hdel_phase1_op;
};
#endif
