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

#include <unordered_map>
#include <mutex>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdarg.h>
#include "redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "common.h"
#include "db_operations.h"
#include "table_definitions.h"
#include "interpreted_code.h"
#include "include/my_systime.h"
#include "include/myisampack.h"

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_DEL_CMD 1
//#define DEBUG_KS 1
//#define DEBUG_HSET_KEY 1
//#define DEBUG_MSET 1
//#define DEBUG_INCR 1
//#define DEBUG_SETRANGE 1
#endif

#ifdef DEBUG_SETRANGE
#define DEB_SETRANGE(arglist) do { printf arglist ; fflush(stdout); } while (0)
#else
#define DEB_SETRANGE(arglist)
#endif

#ifdef DEBUG_DEL_CMD
#define DEB_DEL_CMD(arglist) do { printf arglist ; fflush(stdout); } while (0)
#else
#define DEB_DEL_CMD(arglist)
#endif

#ifdef DEBUG_INCR
#define DEB_INCR(arglist) do { printf arglist ; } while (0)
#else
#define DEB_INCR(arglist)
#endif

#ifdef DEBUG_KS
#define DEB_KS(arglist) do { printf arglist ; } while (0)
#else
#define DEB_KS(arglist)
#endif

#ifdef DEBUG_HSET_KEY
#define DEB_HSET_KEY(arglist) do { printf arglist ; } while (0)
#else
#define DEB_HSET_KEY(arglist)
#endif

#ifdef DEBUG_MSET
#define DEB_MSET(arglist) do { printf arglist ; } while (0)
#else
#define DEB_MSET(arglist)
#endif

NdbRecord *pk_hset_key_record[MAX_NUM_DATABASES] =
{
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr
};
NdbRecord *entire_hset_key_record[MAX_NUM_DATABASES] =
{
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr
};
NdbRecord *pk_key_record[MAX_NUM_DATABASES] =
{
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr
};
NdbRecord *entire_key_record[MAX_NUM_DATABASES] =
{
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr
};
NdbRecord *pk_value_record[MAX_NUM_DATABASES] =
{
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr
};
NdbRecord *entire_value_record[MAX_NUM_DATABASES] =
{
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr
};

static void
delete_value_callback(int result, NdbTransaction *trans, void *aObject) {
  struct KeyStorage *key_store = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_store->m_get_ctrl;
  assert(get_ctrl->m_num_transactions > 0);
  assert(trans == key_store->m_trans);
  assert(key_store->m_key_state == KeyState::MultiRowRWValueSent);
  (void)result;
  int code = trans->getNdbError().code;
  if (code != 0) {
    DEB_DEL_CMD(("Key %u had error: %d\n", key_store->m_index, code));
    key_store->m_key_state = KeyState::CompletedFailed;
    get_ctrl->m_num_keys_failed++;
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = code;
    }
    key_store->m_close_flag = true;
  } else {
    key_store->m_key_state = KeyState::MultiRowRWValue;
  }
  Uint32 bytes_outstanding =
    key_store->m_num_current_rw_rows * DELETE_BYTES;
  assert(get_ctrl->m_num_bytes_outstanding >= bytes_outstanding);
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_bytes_outstanding -= bytes_outstanding;
  get_ctrl->m_num_keys_outstanding--;
  DEB_DEL_CMD(("Key %u Complex Delete value, key_state: %u\n",
    key_store->m_index,
    key_store->m_key_state));
}

void prepare_delete_value_transaction(struct KeyStorage *key_storage) {
  key_storage->m_trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                                             &delete_value_callback,
                                             (void*)key_storage);
}

static void
commit_complex_delete_callback(int result,
                               NdbTransaction *trans,
                               void *aObject) {
  struct KeyStorage *key_store = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_store->m_get_ctrl;
  assert(get_ctrl->m_num_transactions > 0);
  assert(trans == key_store->m_trans);
  assert(key_store->m_key_state == KeyState::MultiRowRWAll);
  (void)result;
  int code = trans->getNdbError().code;
  if (code != 0) {
    DEB_DEL_CMD(("Key %u had error: %d\n", key_store->m_index, code));
    key_store->m_key_state = KeyState::CompletedFailed;
    get_ctrl->m_num_keys_failed++;
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = code;
    }
  } else {
    key_store->m_key_state = KeyState::CompletedSuccess;
    assert(get_ctrl->m_num_keys_multi_rows > 0);
    get_ctrl->m_num_keys_multi_rows--;
  }
  key_store->m_close_flag = true;
  Uint32 bytes_outstanding =
    key_store->m_num_current_rw_rows * DELETE_BYTES;
  assert(get_ctrl->m_num_bytes_outstanding >= bytes_outstanding);
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_bytes_outstanding -= bytes_outstanding;
  get_ctrl->m_num_keys_outstanding--;
  DEB_DEL_CMD(("Key %u Commit Complex Delete string_values, key_state: %u\n",
    key_store->m_index,
    key_store->m_key_state));
}

void commit_complex_delete_transaction(struct KeyStorage *key_storage) {
  key_storage->m_trans->executeAsynchPrepare(NdbTransaction::Commit,
                                             &commit_complex_delete_callback,
                                             (void*)key_storage);
}

static void
complex_delete_callback(int result, NdbTransaction *trans, void *aObject) {
  struct KeyStorage *key_store = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_store->m_get_ctrl;
  assert(get_ctrl->m_num_transactions > 0);
  assert(trans == key_store->m_trans);
  assert(key_store->m_key_state == KeyState::MultiRow);
  (void)result;
  int code = trans->getNdbError().code;
  if (code != 0) {
    DEB_KS(("Key %u had error: %d\n", key_store->m_index, code));
    if (code == READ_ERROR) {
      key_store->m_key_state = KeyState::CompletedReadError;
      assert(get_ctrl->m_num_keys_multi_rows > 0);
      get_ctrl->m_num_keys_multi_rows--;
      get_ctrl->m_num_read_errors++;
    } else {
      key_store->m_key_state = KeyState::CompletedFailed;
      get_ctrl->m_num_keys_failed++;
      if (get_ctrl->m_error_code == 0) {
        get_ctrl->m_error_code = code;
      }
    }
    key_store->m_close_flag = true;
  } else {
    Uint32 num_rows = key_store->m_key_row.num_rows;
    Uint64 rondb_key = key_store->m_key_row.rondb_key;
    if (num_rows == 0) {
      key_store->m_key_state = KeyState::CompletedMultiRow;
    } else {
      key_store->m_num_rows = num_rows;
      key_store->m_rondb_key = rondb_key;
      key_store->m_key_state = KeyState::MultiRowRWValue;
    }
  }
  assert(get_ctrl->m_num_bytes_outstanding >= DELETE_BYTES);
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_bytes_outstanding -= DELETE_BYTES;
  get_ctrl->m_num_keys_outstanding--;
  DEB_KS(("Key %u Complex Delete string_keys, key_state: %u\n",
    key_store->m_index,
    key_store->m_key_state));
}

int prepare_complex_delete_row(std::string *response,
                               [[maybe_unused]]/*todo remove?*/
                               const NdbDictionary::Table *tab,
                               struct KeyStorage *key_storage) {
  struct key_table *key_row = &key_storage->m_key_row;
  /* Set primary key row already done */

  /* Read rondb_key, tot_value_len, num_rows */
  const Uint32 mask = 0x34;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  Uint32 database_id = key_storage->m_get_ctrl->m_database_id;

  const NdbOperation *del_op = key_storage->m_trans->deleteTuple(
    pk_key_record[database_id],
    (const char *)key_row,
    entire_key_record[database_id],
    (char *)key_row,
    mask_ptr);
  if (del_op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               key_storage->m_trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  return 0;
}

void prepare_complex_delete_transaction(struct KeyStorage *key_storage) {
  key_storage->m_trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                                             &complex_delete_callback,
                                             (void*)key_storage);
}

int prepare_simple_delete_row(std::string *response,
                              const NdbDictionary::Table *tab,
                              KeyStorage *key_storage) {
  struct key_table *key_row = &key_storage->m_key_row;
  /* Set primary key row done already in setup_one_transaction */

  Uint32 database_id = key_storage->m_get_ctrl->m_database_id;
  Uint32 code_buffer[64];
  NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer) / sizeof(code_buffer[0]));
  int ret_code = simple_delete_key_row_code(response, code, tab);
  if (ret_code != 0) {
    return RONDB_INTERNAL_ERROR;
  }
  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
  opts.interpretedCode = &code;

  const NdbOperation *del_op = key_storage->m_trans->deleteTuple(
    pk_key_record[database_id],
    (const char *)key_row,
    entire_key_record[database_id],
    nullptr,
    nullptr,
    &opts,
    sizeof(opts));
  if (del_op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               key_storage->m_trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  return 0;
}

static void
simple_delete_callback(int result, NdbTransaction *trans, void *aObject) {
  struct KeyStorage *key_store = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_store->m_get_ctrl;
  assert(get_ctrl->m_num_transactions > 0);
  assert(trans == key_store->m_trans);
  assert(key_store->m_key_state == KeyState::NotCompleted);
  (void)result;
  int code = trans->getNdbError().code;
  if (code != 0) {
    DEB_DEL_CMD(("Key %u had error: %d\n", key_store->m_index, code));
    key_store->m_key_state = KeyState::CompletedFailed;
    if (code == RONDB_KEY_NOT_NULL_ERROR) {
      key_store->m_key_state = KeyState::MultiRow;
      get_ctrl->m_num_keys_multi_rows++;
    } else if (code == READ_ERROR) {
      key_store->m_key_state = KeyState::CompletedReadError;
      get_ctrl->m_num_keys_completed_first_pass++;
      get_ctrl->m_num_read_errors++;
    } else {
      get_ctrl->m_num_keys_failed++;
      if (get_ctrl->m_error_code == 0) {
        get_ctrl->m_error_code = code;
      }
    }
  } else {
    key_store->m_key_state = KeyState::CompletedSuccess;
    get_ctrl->m_num_keys_completed_first_pass++;
  }
  key_store->m_close_flag = true;
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
  DEB_DEL_CMD(("Key %u Simple Delete, key_state: %u\n",
    key_store->m_index,
    key_store->m_key_state));
}

void prepare_simple_delete_transaction(struct KeyStorage *key_storage) {
  key_storage->m_trans->executeAsynchPrepare(NdbTransaction::Commit,
                                             &simple_delete_callback,
                                             (void*)key_storage);
}

/**
 * SET MODULE
 * ----------
 */
int prepare_delete_value_row(std::string *response,
                             struct KeyStorage *key_store,
                             Uint32 ordinal,
                             Uint32 database_id) {
  struct value_table value_row;
  value_row.ordinal = ordinal;
  value_row.rondb_key = key_store->m_rondb_key;
  DEB_MSET(("Key: %u, delete value row with rondb_key: %llu and ordinal: %u\n",
    key_store->m_index, key_store->m_rondb_key, ordinal));
  const NdbOperation *delete_op = key_store->m_trans->deleteTuple(
    pk_value_record[database_id],
    (const char *)&value_row,
    entire_value_record[database_id]);

  if (delete_op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               key_store->m_trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  return 0;
}

static void
write_commit_callback(int result, NdbTransaction *trans, void *aObject) {
  struct KeyStorage *key_storage = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_storage->m_get_ctrl;
  (void)result;
  assert(trans == key_storage->m_trans);
  assert(get_ctrl->m_num_transactions > 0);
  assert(key_storage->m_key_state == KeyState::MultiRowRWAll);
  int code = trans->getNdbError().code;
  if (code != 0) {
    key_storage->m_key_state = KeyState::CompletedFailed;
    get_ctrl->m_num_keys_failed++;
    DEB_HSET_KEY(("key %u write commit had ERROR: %d\n",
      key_storage->m_index, code));
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = code;
    }
  } else {
    key_storage->m_key_state = KeyState::CompletedSuccess;
    assert(get_ctrl->m_num_keys_multi_rows > 0);
    get_ctrl->m_num_keys_multi_rows--;
    DEB_HSET_KEY(("key %u write commit succeeded\n",
      key_storage->m_index));
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
  key_storage->m_close_flag = true;
}

void commit_write_value_transaction(struct KeyStorage *key_store) {
  key_store->m_trans->executeAsynchPrepare(NdbTransaction::Commit,
                                           &write_commit_callback,
                                           (void*)key_store);
}

int prepare_set_value_row(std::string *response,
                          KeyStorage *key_store) {
  struct value_table value_row;
  Uint32 database_id = key_store->m_get_ctrl->m_database_id;
  Uint32 remaining = key_store->m_set_value_size - key_store->m_current_pos;
  Uint32 len = std::min((Uint32)EXTENSION_VALUE_LEN, remaining);
  memcpy(&value_row.value[2],
         &key_store->m_value_ptr[key_store->m_current_pos],
         len);
  set_length(&value_row.value[0], len);
  value_row.expiry_date = key_store->m_expire_at;
  value_row.ordinal = key_store->m_num_rw_rows;
  value_row.rondb_key = key_store->m_rondb_key;
  DEB_MSET(("Set value key: %u, rondb_key: %llu, ordinal: %u,"
            " expiry_date: %u\n",
            key_store->m_index,
            key_store->m_rondb_key,
            key_store->m_num_rw_rows,
            value_row.expiry_date));
  key_store->m_num_rw_rows++;
  key_store->m_current_pos += len;
  /* Mask means writing all columns. */
  const Uint32 mask = 0xF;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  const NdbOperation *write_op = key_store->m_trans->writeTuple(
    pk_value_record[database_id],
    (const char *)&value_row,
    entire_value_record[database_id],
    (char *)&value_row,
    mask_ptr);
  if (write_op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               key_store->m_trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  return 0;
}

static void
write_value_callback(int result, NdbTransaction *trans, void *aObject) {
  struct KeyStorage *key_storage = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_storage->m_get_ctrl;
  (void)result;
  assert(trans == key_storage->m_trans);
  assert(get_ctrl->m_num_transactions > 0);
  assert(key_storage->m_key_state == KeyState::MultiRowRWValueSent);
  int code = trans->getNdbError().code;
  if (code != 0) {
    key_storage->m_key_state = KeyState::CompletedFailed;
    get_ctrl->m_num_keys_failed++;
    DEB_HSET_KEY(("key %u write value had ERROR: %d\n",
      key_storage->m_index, code));
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = code;
    }
    key_storage->m_close_flag = true;
  } else {
    key_storage->m_key_state = KeyState::MultiRowRWValue;
    DEB_HSET_KEY(("key %u write value succeeded\n",
      key_storage->m_index));
    assert(get_ctrl->m_num_bytes_outstanding >=
      (key_storage->m_num_current_rw_rows * sizeof(struct value_table)));
    get_ctrl->m_num_bytes_outstanding -=
      (key_storage->m_num_current_rw_rows * sizeof(struct value_table));
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
}

void prepare_write_value_transaction(struct KeyStorage *key_store) {
  key_store->m_trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                                           &write_value_callback,
                                           (void*)key_store);
}

// Phase 1.0.2d Phase-1 op. Stages a writeTuple on hset_keys(key)
// with the lock-claim interpreter (init_hset_lock_claim_code),
// records the OUTPUT_INDEX_0/1 NdbRecAttr handles on get_ctrl so
// the Phase-1 callback can read them. Caller is responsible for
// pre-allocating prealloc_id from getAutoIncrementValue and for
// running executeAsynchPrepare(NoCommit, hset_phase1_callback)
// after this returns.
int add_hset_lock_claim_op(NdbTransaction *trans,
                           const NdbDictionary::Table *tab_hset,
                           const char *hash_name,
                           Uint32 hash_name_len,
                           Uint64 prealloc_id,
                           struct GetControl *get_ctrl,
                           Uint32 database_id,
                           std::string *response) {
  struct hset_key_table key_row;
  key_row.null_bits = 0;
  memcpy(&key_row.redis_key[2], hash_name, hash_name_len);
  set_length(&key_row.redis_key[0], hash_name_len);

  Uint32 code_buffer[32];
  NdbInterpretedCode code(tab_hset,
                          &code_buffer[0],
                          sizeof(code_buffer) / sizeof(code_buffer[0]));
  int ret_code = init_hset_lock_claim_code(response,
                                           &code,
                                           tab_hset,
                                           prealloc_id);
  if (ret_code != 0) {
    return ret_code;
  }

  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
  opts.optionsPresent |=
    NdbOperation::OperationOptions::OO_INTERPRETED_INSERT;
  opts.interpretedCode = &code;

  NdbOperation::GetValueSpec getvals[2];
  getvals[0].appStorage = nullptr;
  getvals[0].recAttr = nullptr;
  getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
  getvals[1].appStorage = nullptr;
  getvals[1].recAttr = nullptr;
  getvals[1].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_1;
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
  opts.numExtraGetFinalValues = 2;
  opts.extraGetFinalValues = getvals;

  const Uint32 mask = 0x1;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  const NdbOperation *op = trans->writeTuple(
    pk_hset_key_record[database_id],
    (const char *)&key_row,
    entire_hset_key_record[database_id],
    (char *)&key_row,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to add hset_keys lock-claim op",
                               trans->getNdbError());
    return -1;
  }
  get_ctrl->m_rec_attr_hset_id = getvals[0].recAttr;
  get_ctrl->m_rec_attr_hset_field_count = getvals[1].recAttr;
  return 0;
}

int add_hset_field_count_set_op(NdbTransaction *trans,
                                const NdbDictionary::Table *tab_hset,
                                const char *hash_name,
                                Uint32 hash_name_len,
                                Uint32 new_count,
                                Uint32 database_id,
                                std::string *response) {
  // tab_hset is unused for this plain updateTuple (the
  // entire_hset_key_record / pk_hset_key_record globals carry the
  // schema knowledge). Kept in the signature for symmetry with
  // add_hset_lock_claim_op / add_hset_field_count_bump_op.
  (void)tab_hset;
  struct hset_key_table key_row;
  key_row.null_bits = 0;
  memcpy(&key_row.redis_key[2], hash_name, hash_name_len);
  set_length(&key_row.redis_key[0], hash_name_len);
  key_row.field_count = new_count;

  // Mask: only field_count column is updated. The schema layout
  // for hset_keys (per init_hset_key_records read_all_column_map)
  // orders columns as redis_key (PK) [0], redis_key_id [1],
  // field_count [2], expiry_date [3] - the mask is keyed by the
  // record's column index in entire_hset_key_record. We rely on
  // the NDB API treating PK columns as auto-included for an
  // updateTuple, so mask only needs to include field_count.
  // To stay robust against record-spec reordering, use the full
  // record but only set the field_count column via mask bit 2.
  const Uint32 mask = 0x4; // bit 2 = field_count
  const unsigned char *mask_ptr = (const unsigned char *)&mask;

  const NdbOperation *op = trans->updateTuple(
    pk_hset_key_record[database_id],
    (const char *)&key_row,
    entire_hset_key_record[database_id],
    (char *)&key_row,
    mask_ptr);
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to add hset_keys field_count set op",
                               trans->getNdbError());
    return -1;
  }
  return 0;
}

// Phase 1.5 EXPIRE-family helpers. Each opens its own trans,
// commits a single plain updateTuple writing only expiry_date,
// and closes. Returns the trans-level NdbError code
// (0 = applied, 626 = row missing, other = real error).
//
// The buffer's expiry_date is set to the mi_int4-encoded value
// matching the rondis SET ... EX path (see write_data_to_key_op:
// `key_row.expiry_date = key_store->m_expire_at` where m_expire_at
// was filled by generate_expire_at via mi_int4store). NDB then
// stores the bytes verbatim; mi_sint4korr at read time recovers
// the native epoch seconds. Mask 0x8 (bit 3 = expiry_date in both
// records, per declaration order matching std::map iteration).
//
// Caller passes m_expire_at_encoded (the Int64 produced by
// generate_expire_at - low 4 bytes are the mi_int4 BE bytes).
int update_expiry_string_row(Ndb *ndb,
                             const char *key_str,
                             Uint32 key_len,
                             Int64 m_expire_at_encoded,
                             Uint32 database_id,
                             std::string *response) {
  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *tab = dict->getTable(KEY_TABLE_NAME);
  if (tab == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TABLE_OBJECT,
                               dict->getNdbError());
    return -1;
  }
  struct key_table key_row;
  key_row.null_bits = 0;
  key_row.redis_key_id = STRING_REDIS_KEY_ID;
  memcpy(&key_row.redis_key[2], key_str, key_len);
  memset(&key_row.redis_key[2 + key_len], 0, 3);
  set_length((char*)&key_row.redis_key[0], key_len);
  key_row.expiry_date = (Int32)m_expire_at_encoded;

  NdbTransaction *trans = ndb->startTransaction(
    tab, (const char*)&key_row.redis_key_id, key_len + 10);
  if (trans == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TXN_OBJECT,
                               ndb->getNdbError());
    return -1;
  }
  // bit 7 = expiry_date in string_keys NdbRecord. The
  // std::map<column_ptr,...> iteration order does not match source
  // declaration order for this 8-column table (column pointers
  // happen to land out of order); empirically derived from
  // write_data_to_key_op which drops bit 7 (mask 0xFB -> 0x7B) when
  // set_ttl is false to leave expiry_date alone.
  const Uint32 mask = 0x80;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  const NdbOperation *op = trans->updateTuple(
    pk_key_record[database_id],
    (const char *)&key_row,
    entire_key_record[database_id],
    (char *)&key_row,
    mask_ptr);
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               trans->getNdbError());
    ndb->closeTransaction(trans);
    return -1;
  }
  trans->execute(NdbTransaction::Commit, NdbOperation::AO_IgnoreError);
  int code_val = trans->getNdbError().code;
  if (code_val != 0 && code_val != 626) {
    assign_ndb_err_to_response(response,
                               "Failed to commit expire update",
                               trans->getNdbError());
  }
  ndb->closeTransaction(trans);
  return code_val;
}

int update_expiry_hset_row(Ndb *ndb,
                           const char *key_str,
                           Uint32 key_len,
                           Int64 m_expire_at_encoded,
                           Uint32 database_id,
                           std::string *response) {
  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *tab = dict->getTable(HSET_KEY_TABLE_NAME);
  if (tab == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TABLE_OBJECT,
                               dict->getNdbError());
    return -1;
  }
  struct hset_key_table key_row;
  key_row.null_bits = 0;
  memcpy(&key_row.redis_key[2], key_str, key_len);
  set_length(&key_row.redis_key[0], key_len);
  key_row.expiry_date = (Int32)m_expire_at_encoded;

  NdbTransaction *trans = ndb->startTransaction(
    tab, (const char*)&key_row.redis_key[0], key_len + 2);
  if (trans == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TXN_OBJECT,
                               ndb->getNdbError());
    return -1;
  }
  // bit 3 = expiry_date in hset_keys NdbRecord. add_hset_field_count_set_op
  // uses mask 0x4 (bit 2 = field_count); declaration order matches
  // std::map iteration for this 4-column table, so expiry_date is
  // at bit 3 (the last column).
  const Uint32 mask = 0x8;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  const NdbOperation *op = trans->updateTuple(
    pk_hset_key_record[database_id],
    (const char *)&key_row,
    entire_hset_key_record[database_id],
    (char *)&key_row,
    mask_ptr);
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               trans->getNdbError());
    ndb->closeTransaction(trans);
    return -1;
  }
  trans->execute(NdbTransaction::Commit, NdbOperation::AO_IgnoreError);
  int code_val = trans->getNdbError().code;
  if (code_val != 0 && code_val != 626) {
    assign_ndb_err_to_response(response,
                               "Failed to commit expire update",
                               trans->getNdbError());
  }
  ndb->closeTransaction(trans);
  return code_val;
}

// Phase 1.0.2d Phase-1 callback. Fires once per HSET trans, after
// the lock-claim op's NoCommit response arrives. Captures the two
// outputs (redis_key_id, field_count) from the interpreter into
// GetControl. Errors propagate via m_num_keys_failed; the outer
// state machine (set_rows_hset) checks that flag after the drain.
static void
hset_phase1_callback(int result, NdbTransaction *trans, void *aObject) {
  struct GetControl *get_ctrl = (struct GetControl*)aObject;
  (void)result;
  assert(get_ctrl->m_num_transactions > 0);
  int code = trans->getNdbError().code;
  if (code != 0) {
    get_ctrl->m_num_keys_failed++;
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = code;
    }
  } else {
    get_ctrl->m_hset_redis_key_id =
      get_ctrl->m_rec_attr_hset_id->u_64_value();
    get_ctrl->m_hset_field_count_pre =
      (Uint32)get_ctrl->m_rec_attr_hset_field_count->u_64_value();
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
}

void prepare_hset_phase1_transaction(struct GetControl *get_ctrl,
                                     NdbTransaction *trans) {
  trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                              &hset_phase1_callback,
                              (void*)get_ctrl);
}

// Phase 1.0.2d/f Phase-2 chunk callback. Fires once per Phase-2
// NoCommit submission. Iterates only the keys in the current
// chunk window (m_hset_phase_chunk_start ..
// m_hset_phase_chunk_start + m_hset_phase_chunk_count) and pulls
// the per-op interpreter outputs from the NdbRecAttr handles
// stashed by write_data_to_key_op for those keys. set_rows_hset
// loops Phase-2 chunks until every field has been written.
static void
hset_phase2_callback(int result, NdbTransaction *trans, void *aObject) {
  struct GetControl *get_ctrl = (struct GetControl*)aObject;
  (void)result;
  assert(get_ctrl->m_num_transactions > 0);
  int code = trans->getNdbError().code;
  Uint32 chunk_start = get_ctrl->m_hset_phase_chunk_start;
  Uint32 chunk_end = chunk_start + get_ctrl->m_hset_phase_chunk_count;
  if (code != 0) {
    if (code == RONDB_CONDITIONAL_STORE_NOT_MET) {
      // HSETNX-style guard (NX/XX) tripped on at least one field
      // in this chunk. HSET / HMSET don't expose conditional flags
      // so this only fires on HSETNX. Mark the entire batch
      // ConditionalFail (the trans aborts on commit anyway).
      Uint32 num_keys = get_ctrl->m_num_keys_requested;
      for (Uint32 i = 0; i < num_keys; i++) {
        get_ctrl->m_key_store[i].m_key_state =
          KeyState::CompletedConditionalFail;
      }
    } else {
      get_ctrl->m_num_keys_failed++;
      if (get_ctrl->m_error_code == 0) {
        get_ctrl->m_error_code = code;
      }
      Uint32 num_keys = get_ctrl->m_num_keys_requested;
      for (Uint32 i = 0; i < num_keys; i++) {
        get_ctrl->m_key_store[i].m_key_state = KeyState::CompletedFailed;
      }
    }
  } else {
    for (Uint32 i = chunk_start; i < chunk_end; i++) {
      struct KeyStorage *ks = &get_ctrl->m_key_store[i];
      ks->m_prev_num_rows =
        (Uint32)ks->m_rec_attr_prev_num_rows->u_64_value();
      ks->m_rondb_key =
        (Uint64)ks->m_rec_attr_rondb_key->u_64_value();
      // KEEPTTL is not exposed by HSET so OUTPUT_INDEX_2 is never
      // requested for the hash-write path. Skip the m_keep_ttl
      // branch.
      if (ks->m_rec_attr_new_field != nullptr &&
          ks->m_rec_attr_new_field->u_64_value() != 0) {
        get_ctrl->m_num_new_fields++;
      }
      ks->m_current_pos = INLINE_VALUE_LEN;
      ks->m_key_state = KeyState::MultiRowRWValue;
    }
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
}

void prepare_hset_phase2_transaction(struct GetControl *get_ctrl,
                                     NdbTransaction *trans) {
  trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                              &hset_phase2_callback,
                              (void*)get_ctrl);
}

// Phase 1.0.2f Phase-3 chunk-ack callback. Fires once per Phase-3
// NoCommit submission carrying ext-row writes / deletes.
// Acknowledges the chunk; no per-key state to read (ext-row ops
// don't emit interpreter outputs we care about). The final Phase-3
// submission is a Commit that uses hset_phase3_callback instead.
static void
hset_phase_chunk_callback(int result,
                          NdbTransaction *trans,
                          void *aObject) {
  struct GetControl *get_ctrl = (struct GetControl*)aObject;
  (void)result;
  assert(get_ctrl->m_num_transactions > 0);
  int code = trans->getNdbError().code;
  if (code != 0) {
    get_ctrl->m_num_keys_failed++;
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = code;
    }
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
}

void prepare_hset_phase_chunk_transaction(struct GetControl *get_ctrl,
                                          NdbTransaction *trans) {
  trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                              &hset_phase_chunk_callback,
                              (void*)get_ctrl);
}

// Phase 1.0.2d Phase-3 callback. Fires once when the Commit
// response arrives (with any ext-row ops + field_count update
// folded in). Marks every key in the batch as terminal so the
// reply builder can run.
static void
hset_phase3_callback(int result, NdbTransaction *trans, void *aObject) {
  struct GetControl *get_ctrl = (struct GetControl*)aObject;
  (void)result;
  assert(get_ctrl->m_num_transactions > 0);
  int code = trans->getNdbError().code;
  Uint32 num_keys = get_ctrl->m_num_keys_requested;
  if (code != 0) {
    get_ctrl->m_num_keys_failed++;
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = code;
    }
    for (Uint32 i = 0; i < num_keys; i++) {
      get_ctrl->m_key_store[i].m_key_state = KeyState::CompletedFailed;
      get_ctrl->m_key_store[i].m_close_flag = true;
    }
  } else {
    for (Uint32 i = 0; i < num_keys; i++) {
      get_ctrl->m_key_store[i].m_key_state = KeyState::CompletedSuccess;
      get_ctrl->m_key_store[i].m_close_flag = true;
    }
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
}

void prepare_hset_phase3_transaction(struct GetControl *get_ctrl,
                                     NdbTransaction *trans) {
  trans->executeAsynchPrepare(NdbTransaction::Commit,
                              &hset_phase3_callback,
                              (void*)get_ctrl);
}

// Phase 1.0.3 single-trans HDEL helpers. The shape mirrors HSET's
// three-phase pipeline (Phase 1 lock-claim, Phase 2 chunked
// per-field probes, Phase 3 commit-with-deletes-and-bump) but the
// Phase 1 op is a read (no auto-INSERT of hset_keys), Phase 2 is a
// read pass that classifies each field as inline / ext-row /
// missing, and Phase 3's per-field ops are deletes rather than
// writes.

// Phase 1: take an X-lock on hset_keys(key) by reading the row
// LM_Exclusive. Projects redis_key_id (column index 1) and
// field_count (column index 2) into get_ctrl->m_hset_lock_read_buf.
// On NoDataFound (626) the read fails individually; the
// trans-level error is 0 because reads default to AO_IgnoreError.
// hdel_phase1_callback inspects the per-op error to distinguish
// "hash exists, X-lock held" from "hash never existed".
int add_hdel_lock_read_op(NdbTransaction *trans,
                          const NdbDictionary::Table *tab_hset,
                          const char *hash_name,
                          Uint32 hash_name_len,
                          struct GetControl *get_ctrl,
                          Uint32 database_id,
                          std::string *response) {
  (void)tab_hset;
  struct hset_key_table *key_row = &get_ctrl->m_hset_lock_read_buf;
  key_row->null_bits = 0;
  memcpy(&key_row->redis_key[2], hash_name, hash_name_len);
  set_length(&key_row->redis_key[0], hash_name_len);

  // Mask 0x6 = bits 1,2 (redis_key_id, field_count). PK is read
  // implicitly by the PK lookup. Skip expiry_date (bit 3) — HDEL
  // doesn't consult it.
  const Uint32 mask = 0x6;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;

  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_BATCH_SAFE_FLAG;

  const NdbOperation *op = trans->readTuple(
    pk_hset_key_record[database_id],
    (const char *)key_row,
    entire_hset_key_record[database_id],
    (char *)key_row,
    NdbOperation::LM_Exclusive,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to add hset_keys lock-read op",
                               trans->getNdbError());
    return -1;
  }
  get_ctrl->m_hdel_phase1_op = op;
  return 0;
}

static void
hdel_phase1_callback(int result, NdbTransaction *trans, void *aObject) {
  struct GetControl *get_ctrl = (struct GetControl*)aObject;
  (void)result;
  assert(get_ctrl->m_num_transactions > 0);
  // Trans-level error first: a non-626 error here is a real
  // failure (op-build error, schema mismatch, network) and
  // should fail the batch. NoDataFound on the read does NOT
  // bubble up to trans level (default AO_IgnoreError for reads).
  int trans_code = trans->getNdbError().code;
  if (trans_code != 0 && trans_code != 626) {
    get_ctrl->m_num_keys_failed++;
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = trans_code;
    }
    assert(get_ctrl->m_num_keys_outstanding > 0);
    get_ctrl->m_num_keys_outstanding--;
    return;
  }
  // Per-op error: 626 means the hash row does not exist. Signal
  // the missing-hash case via m_hset_redis_key_id == 0 (reserved
  // STRING_REDIS_KEY_ID is never assigned to a hash, so 0 is a
  // safe "no row" sentinel). The outer state machine skips
  // Phase 2/3 and replies :0\r\n.
  int op_code =
    (get_ctrl->m_hdel_phase1_op != nullptr)
    ? get_ctrl->m_hdel_phase1_op->getNdbError().code
    : 0;
  if (op_code == 626) {
    get_ctrl->m_hset_redis_key_id = 0;
    get_ctrl->m_hset_field_count_pre = 0;
  } else if (op_code != 0) {
    get_ctrl->m_num_keys_failed++;
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = op_code;
    }
  } else {
    get_ctrl->m_hset_redis_key_id =
      get_ctrl->m_hset_lock_read_buf.redis_key_id;
    get_ctrl->m_hset_field_count_pre =
      get_ctrl->m_hset_lock_read_buf.field_count;
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
}

void prepare_hdel_phase1_transaction(struct GetControl *get_ctrl,
                                     NdbTransaction *trans) {
  trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                              &hdel_phase1_callback,
                              (void*)get_ctrl);
}

// Phase 2: per-field deleteTuple on string_keys(redis_key_id,
// field) that also reads back the row's rondb_key + num_rows
// before the delete is applied. NDB's deleteTuple accepts a
// result_record / result_mask just like readTuple; the row's
// pre-delete column values are projected into key_store->m_key_row.
// AO_IgnoreError per-op so a NoDataFound (626) on a field that
// doesn't exist does not abort the trans; hdel_phase2_callback
// walks per-op errors via the stashed NdbOperation* to classify
// each field as inline / ext-row / missing.
//
// The deleteTuples are staged (NoCommit), so the row is locked
// for delete but not yet applied. Phase 3 queues per-ordinal
// string_values deletes for ext fields and folds the field_count
// bump into the Commit, which applies every staged delete
// atomically.
int add_hdel_field_delete_op(KeyStorage *key_store,
                             Uint32 database_id,
                             std::string *response) {
  struct key_table *key_row = &key_store->m_key_row;
  // PK is set up by the caller (hash redis_key_id + field name).
  // Mask 0x64 = rondb_key (bit 2) + tot_value_len (bit 5) +
  // num_rows (bit 6). Phase 3 reads num_rows to decide how many
  // string_values ordinals need cleanup, and rondb_key for the
  // ext-row PK.
  const Uint32 mask = 0x64;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;

  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_ABORTOPTION;
  opts.abortOption = NdbOperation::AO_IgnoreError;
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_BATCH_SAFE_FLAG;

  const NdbOperation *op = key_store->m_trans->deleteTuple(
    pk_key_record[database_id],
    (const char *)key_row,
    entire_key_record[database_id],
    (char *)key_row,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               key_store->m_trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  key_store->m_hdel_phase2_op = op;
  return 0;
}

// NDB's executeAsynchPrepare fires one batch-level callback per
// submission, not one per op. Per-field results (success vs 626 vs
// real error) are pulled out of each field's stashed NdbOperation*
// (m_hdel_phase2_op) inside the batch callback below - that is the
// "per-row callback" equivalent in NDB's async API. There is no
// need for a dedicated per-string_keys-delete callback: the
// per-op error is captured at NoCommit time here, and the actual
// row delete applies atomically at Phase 3's Commit alongside the
// ext-row deletes and the field_count bump.
static void
hdel_phase2_callback(int result, NdbTransaction *trans, void *aObject) {
  struct GetControl *get_ctrl = (struct GetControl*)aObject;
  (void)result;
  assert(get_ctrl->m_num_transactions > 0);
  int trans_code = trans->getNdbError().code;
  if (trans_code != 0 && trans_code != 626) {
    get_ctrl->m_num_keys_failed++;
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = trans_code;
    }
    assert(get_ctrl->m_num_keys_outstanding > 0);
    get_ctrl->m_num_keys_outstanding--;
    return;
  }
  Uint32 chunk_start = get_ctrl->m_hset_phase_chunk_start;
  Uint32 chunk_end = chunk_start + get_ctrl->m_hset_phase_chunk_count;
  for (Uint32 i = chunk_start; i < chunk_end; i++) {
    struct KeyStorage *ks = &get_ctrl->m_key_store[i];
    int op_code = (ks->m_hdel_phase2_op != nullptr)
                  ? ks->m_hdel_phase2_op->getNdbError().code
                  : 0;
    if (op_code == 0) {
      ks->m_hdel_field_present = true;
      // m_key_row was populated by the read; copy out the bits
      // Phase 3 needs.
      ks->m_num_rows = ks->m_key_row.num_rows;
      ks->m_rondb_key = ks->m_key_row.rondb_key;
      get_ctrl->m_num_deleted_fields++;
    } else if (op_code == 626) {
      ks->m_hdel_field_present = false;
      ks->m_num_rows = 0;
      ks->m_rondb_key = 0;
    } else {
      get_ctrl->m_num_keys_failed++;
      if (get_ctrl->m_error_code == 0) {
        get_ctrl->m_error_code = op_code;
      }
      ks->m_hdel_field_present = false;
    }
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
}

void prepare_hdel_phase2_transaction(struct GetControl *get_ctrl,
                                     NdbTransaction *trans) {
  trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                              &hdel_phase2_callback,
                              (void*)get_ctrl);
}

// Phase 3: commit. Like hset_phase3_callback, marks every key
// terminal so the caller can emit the integer reply. Differs from
// the HSET version in tolerating a 626 at trans level: Phase 2's
// per-op deleteTuples on missing fields surface 626 (under
// AO_IgnoreError) and the tolerated code is still visible on the
// trans's NdbError at the post-Commit callback. Treat 626 as
// "no real failure", same as hdel_phase1_callback /
// hdel_phase2_callback do.
static void
hdel_phase3_callback(int result, NdbTransaction *trans, void *aObject) {
  struct GetControl *get_ctrl = (struct GetControl*)aObject;
  (void)result;
  assert(get_ctrl->m_num_transactions > 0);
  int code = trans->getNdbError().code;
  Uint32 num_keys = get_ctrl->m_num_keys_requested;
  if (code != 0 && code != 626) {
    get_ctrl->m_num_keys_failed++;
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = code;
    }
    for (Uint32 i = 0; i < num_keys; i++) {
      get_ctrl->m_key_store[i].m_key_state = KeyState::CompletedFailed;
      get_ctrl->m_key_store[i].m_close_flag = true;
    }
  } else {
    for (Uint32 i = 0; i < num_keys; i++) {
      get_ctrl->m_key_store[i].m_key_state = KeyState::CompletedSuccess;
      get_ctrl->m_key_store[i].m_close_flag = true;
    }
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
}

void prepare_hdel_phase3_transaction(struct GetControl *get_ctrl,
                                     NdbTransaction *trans) {
  trans->executeAsynchPrepare(NdbTransaction::Commit,
                              &hdel_phase3_callback,
                              (void*)get_ctrl);
}

// Append an interpretedUpdate op on the same NdbTransaction that
// adjusts hset_keys.field_count by delta. Used by HDEL (Phase
// 1.0.3) with delta<0 to fold a deferred decrement into the
// Commit op that also carries the field-row deletes. The
// cross-server invariant (Phase 1.0.3) keeps the hset_keys row
// present forever, so updateTuple is safe.
int
add_hset_field_count_bump_op(NdbTransaction *trans,
                             const NdbDictionary::Table *tab_hset,
                             const char *hash_name,
                             Uint32 hash_name_len,
                             Int64 delta,
                             Uint32 database_id,
                             std::string *response) {
  struct hset_key_table key_row;
  key_row.null_bits = 0;
  memcpy(&key_row.redis_key[2], hash_name, hash_name_len);
  set_length(&key_row.redis_key[0], hash_name_len);

  Uint32 code_buffer[16];
  NdbInterpretedCode code(tab_hset,
                          &code_buffer[0],
                          sizeof(code_buffer) / sizeof(code_buffer[0]));
  int ret_code = init_hset_field_count_bump_code(response,
                                                 &code,
                                                 tab_hset,
                                                 delta);
  if (ret_code != 0) {
    return ret_code;
  }

  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
  opts.interpretedCode = &code;

  // updateTuple (not writeTuple): if the hset_keys row is missing
  // something has gone wrong upstream. Fail the trans rather than
  // writing a row with uninitialized non-PK columns.
  //
  // mask=0 (zero, NOT nullptr): nullptr would tell NDB to write
  // every non-PK column from key_row, including the unset
  // redis_key_id and field_count fields - that would clobber the
  // existing row's redis_key_id with stack garbage. With an
  // explicit zero mask, only the interpreter writes
  // (init_hset_field_count_bump_code's write_attr on field_count),
  // and the existing redis_key_id is preserved.
  const Uint32 mask = 0;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  const NdbOperation *op = trans->updateTuple(
    pk_hset_key_record[database_id],
    (const char *)&key_row,
    entire_hset_key_record[database_id],
    (char *)&key_row,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to add hset_keys bump op",
                               trans->getNdbError());
    return -1;
  }
  return 0;
}

static void
write_callback(int result, NdbTransaction *trans, void *aObject) {
  struct KeyStorage *key_storage = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_storage->m_get_ctrl;
  (void)result;
  assert(trans == key_storage->m_trans);
  assert(get_ctrl->m_num_transactions > 0);
  int code = trans->getNdbError().code;
  if (code != 0) {
    if (code == RONDB_CONDITIONAL_STORE_NOT_MET) {
      // NX / XX guard tripped in the write interpreter program
      // (C7 / C8). Not a real failure - the response builder will
      // emit Redis-canonical nil ($-1\r\n) for this key.
      key_storage->m_key_state = KeyState::CompletedConditionalFail;
      get_ctrl->m_num_keys_completed_first_pass++;
    } else {
      key_storage->m_key_state = KeyState::CompletedFailed;
      get_ctrl->m_num_keys_failed++;
      DEB_HSET_KEY(("key %u had ERROR: %d\n", key_storage->m_index, code));
      if (get_ctrl->m_error_code == 0) {
        get_ctrl->m_error_code = code;
      }
    }
    assert(get_ctrl->m_num_keys_outstanding > 0);
    get_ctrl->m_num_keys_outstanding--;
    key_storage->m_close_flag = true;
  } else {
    key_storage->m_prev_num_rows =
      (Uint32)key_storage->m_rec_attr_prev_num_rows->u_64_value();
    key_storage->m_rondb_key =
      (Uint32)key_storage->m_rec_attr_rondb_key->u_64_value();
    if (key_storage->m_keep_ttl == true &&
        key_storage->m_num_rows > 0) {
      Uint32 expiry_date =
        (Uint32)key_storage->m_rec_attr_expiry_date->u_64_value();
      key_storage->m_expire_at = (Int64)expiry_date;
    }
    // OUTPUT_INDEX_3 from the interpreter: 1 on INSERT, 0 on UPDATE.
    // Aggregate for the HSET new-field-count reply (C10). For
    // is_hmset batches (Phase 1.0.2d), this is also the source for
    // the field_count delta written in Phase 3.
    if (key_storage->m_rec_attr_new_field != nullptr &&
        key_storage->m_rec_attr_new_field->u_64_value() != 0) {
      get_ctrl->m_num_new_fields++;
    }
    key_storage->m_current_pos = INLINE_VALUE_LEN;
    key_storage->m_key_state = KeyState::MultiRowRWValue;
    assert(get_ctrl->m_num_keys_outstanding > 0);
    get_ctrl->m_num_keys_outstanding--;
    DEB_HSET_KEY(("key %u simple write succeeded, prev_num_rows: %u"
                  ", rondb_key: %llu\n",
      key_storage->m_index,
      key_storage->m_prev_num_rows,
      key_storage->m_rondb_key));
  }
}

void prepare_write_transaction(struct KeyStorage *key_store) {
  key_store->m_trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                                           &write_callback,
                                           (void*)key_store);
}

void commit_write_transaction(struct KeyStorage *key_store) {
  key_store->m_trans->executeAsynchPrepare(NdbTransaction::Commit,
                                           &write_callback,
                                           (void*)key_store);
}

int write_data_to_key_op(std::string *response,
                         const NdbDictionary::Table *tab,
                         KeyStorage *key_store,
                         Uint64 redis_key_id,
                         Uint32 row_state,
                         Uint32 database_id) {
  struct key_table key_row;
  NdbTransaction *trans = key_store->m_trans;
  Uint32 mask = 0xFB;
  key_row.null_bits = 0;
  memcpy(&key_row.redis_key[2],
         key_store->m_key_str,
         key_store->m_key_len);
  set_length(&key_row.redis_key[0], key_store->m_key_len);
  key_row.redis_key_id = redis_key_id;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  key_row.tot_value_len = key_store->m_set_value_size;
  key_row.num_rows = key_store->m_num_rows;
  key_row.value_data_type = row_state;
  if (key_store->m_set_ttl) {
    key_row.expiry_date = key_store->m_expire_at;
  } else {
    mask = 0x7B;
  }
  Uint32 this_value_len = key_store->m_set_value_size;
  if (this_value_len > INLINE_VALUE_LEN) {
    this_value_len = INLINE_VALUE_LEN;
  }

  DEB_MSET(("PK is %s with len: %u\n",
    key_store->m_key_str, key_store->m_key_len));
  DEB_MSET(("Set value to %s with len: %u\n",
    key_store->m_value_ptr, this_value_len));

  memcpy(&key_row.value_start[2], key_store->m_value_ptr, this_value_len);
  set_length(&key_row.value_start[0], this_value_len);

  Uint32 code_buffer[64];
  NdbInterpretedCode code(tab,
                          &code_buffer[0],
                          sizeof(code_buffer) / sizeof(code_buffer[0]));
  int ret_code = write_key_row_no_commit(response, code, tab, key_store);
  if (ret_code != 0) {
    return ret_code;
  }
  // Prepare the interpreted program to be part of the write
  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
  opts.optionsPresent |=
    NdbOperation::OperationOptions::OO_INTERPRETED_INSERT;
  opts.interpretedCode = &code;

  // getvals[] carries the extra-final-values we want the data node to
  // return from the interpreted-code OUTPUT channels:
  //   INDEX_0 prev_num_rows, INDEX_1 rondb_key, INDEX_2 expiry_date
  //   (only when KEEPTTL), INDEX_3 new-field flag (always).
  NdbOperation::GetValueSpec getvals[4];
  getvals[0].appStorage = nullptr;
  getvals[0].recAttr = nullptr;
  getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
  getvals[1].appStorage = nullptr;
  getvals[1].recAttr = nullptr;
  getvals[1].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_1;
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
  Uint32 num_vals = 2;
  if (key_store->m_keep_ttl == true &&
      key_store->m_num_rows > 0) {
    getvals[num_vals].appStorage = nullptr;
    getvals[num_vals].recAttr = nullptr;
    getvals[num_vals].column =
      NdbDictionary::Column::READ_INTERPRETER_OUTPUT_2;
    num_vals++;
  }
  getvals[num_vals].appStorage = nullptr;
  getvals[num_vals].recAttr = nullptr;
  getvals[num_vals].column =
    NdbDictionary::Column::READ_INTERPRETER_OUTPUT_3;
  num_vals++;
  opts.numExtraGetFinalValues = num_vals;
  opts.extraGetFinalValues = getvals;

  /* Define the actual operation to be sent to RonDB data node. */
  const NdbOperation *op = trans->writeTuple(
    pk_key_record[database_id],
    (const char *)&key_row,
    entire_key_record[database_id],
    (char *)&key_row,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to create NdbOperation",
                               trans->getNdbError());
    return -1;
  }
  key_store->m_rec_attr_prev_num_rows = getvals[0].recAttr;
  key_store->m_rec_attr_rondb_key = getvals[1].recAttr;
  key_store->m_rec_attr_expiry_date = nullptr;
  // Walk the layout assembled above: INDEX_2 is present only when
  // the KEEPTTL branch ran, and INDEX_3 always comes last.
  Uint32 getvals_slot = 2;
  if (key_store->m_keep_ttl == true && key_store->m_num_rows > 0) {
    key_store->m_rec_attr_expiry_date = getvals[getvals_slot].recAttr;
    getvals_slot++;
  }
  key_store->m_rec_attr_new_field = getvals[getvals_slot].recAttr;
  return 0;
}

/**
 * GET MODULE
 * ----------
 */
static void
value_callback(int result, NdbTransaction *trans, void *aObject) {
  struct KeyStorage *key_store = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_store->m_get_ctrl;
  assert(trans == key_store->m_trans);
  assert(get_ctrl->m_num_transactions > 0);
  (void)result;
  if (key_store->m_key_state == KeyState::CompletedMultiRowSuccessCommit) {
    /* Only commit of Locked Read performed here */
    key_store->m_close_flag = true;
    assert(get_ctrl->m_num_keys_multi_rows > 0);
    get_ctrl->m_num_keys_multi_rows--;
    key_store->m_key_state = KeyState::CompletedSuccess;
    assert(get_ctrl->m_num_keys_outstanding > 0);
    get_ctrl->m_num_keys_outstanding--;
    return;
  }
  assert(key_store->m_key_state == KeyState::MultiRowRWValueSent ||
         key_store->m_key_state == KeyState::MultiRowRWAll);
  int code = trans->getNdbError().code;
  if (code != 0) {
    DEB_KS(("Key %u had error %d reading value\n",
      key_store->m_index, code));
    key_store->m_key_state = KeyState::CompletedFailed;
    get_ctrl->m_num_keys_completed_first_pass++;
    get_ctrl->m_num_keys_failed++;
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = code;
    }
    key_store->m_close_flag = true;
    assert(get_ctrl->m_num_keys_multi_rows > 0);
    get_ctrl->m_num_keys_multi_rows--;
  } else {
    Uint32 current_pos = key_store->m_current_pos;
    char *complex_value = key_store->m_value_ptr;
    for (Uint32 i = 0; i < key_store->m_num_current_rw_rows; i++) {
      Uint32 inx = key_store->m_first_value_row + i;
      struct value_table *value_row = &get_ctrl->m_value_rows[inx];
      Uint32 value_len = get_length((char*)&value_row->value[0]);
      Uint32 calc_pos = INLINE_VALUE_LEN +
        (value_row->ordinal * EXTENSION_VALUE_LEN);
      assert(calc_pos == current_pos);
      assert(calc_pos + value_len <= key_store->m_get_value_size);
      memcpy(&complex_value[calc_pos], &value_row->value[2], value_len);
      Uint32 old_pos = current_pos;
      (void)old_pos;
      current_pos += value_len;
      DEB_KS(("Read value of %u bytes, new pos: %u old_pos: %u (%u), key: %u\n",
        value_len, current_pos, old_pos, calc_pos, key_store->m_index));
    }
    key_store->m_current_pos = current_pos;
    if (key_store->m_num_rows == key_store->m_num_rw_rows) {
      key_store->m_key_state = KeyState::CompletedMultiRow;
      assert(get_ctrl->m_num_keys_multi_rows > 0);
      get_ctrl->m_num_keys_multi_rows--;
      if (get_ctrl->m_is_set_command == false) {
        key_store->m_close_flag = true;
      }
    } else {
      key_store->m_key_state = KeyState::MultiRowRWValue;
    }
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
  Uint32 sz = sizeof(struct value_table) * key_store->m_num_current_rw_rows;
  assert(get_ctrl->m_num_bytes_outstanding >= sz);
  get_ctrl->m_num_bytes_outstanding -= sz;
  key_store->m_num_current_rw_rows = 0;
  DEB_KS(("Read value for key %u, key_state: %u, keys %u and"
          " bytes %u out, num_read_rows: %u\n",
          key_store->m_index,
          key_store->m_key_state,
          get_ctrl->m_num_keys_outstanding,
          get_ctrl->m_num_bytes_outstanding,
          key_store->m_num_rw_rows));
}

void prepare_read_value_transaction(struct KeyStorage *key_store) {
  key_store->m_trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                                           &value_callback,
                                           (void*)key_store);
}

void commit_read_value_transaction(struct KeyStorage *key_store) {
  key_store->m_trans->executeAsynchPrepare(NdbTransaction::Commit,
                                           &value_callback,
                                           (void*)key_store);
}

int prepare_get_value_row(std::string *response,
                          KeyStorage *key_store,
                          bool is_set_command,
                          struct value_table *value_row,
                          Uint32 database_id) {
  /**
   * Mask and options means simply reading all columns
   * except primary key columns. In this case only the
   * value column is read. We read the ordinal column
   * as well to ensure that we don't rely on order of
   * signals arriving. Normally they should be arriving
   * in order, but it is safer to not rely on that.
   *
   * We use SimpleRead to ensure that DBTC is aborted if
   * something goes wrong with the read, the row should
   * never be locked since we hold a lock on the key row
   * at this point.
   */
  NdbTransaction *trans = key_store->m_trans;
  const Uint32 mask = 0xC;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_BATCH_SAFE_FLAG;
  const NdbOperation *read_op = trans->readTuple(
    pk_value_record[database_id],
    (const char *)value_row,
    entire_value_record[database_id],
    (char *)value_row,
    is_set_command ?
      NdbOperation::LM_Exclusive : NdbOperation::LM_SimpleRead,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (read_op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  return 0;
}

// Phase 1.10b: filter expired string_keys rows out of GET-shape
// reads (GET / MGET / GETRANGE / STRLEN). The mask 0xFC used by
// prepare_get_key_row / prepare_get_simple_key_row already projects
// expiry_date, so this is a pure post-read C-side check in the
// same shape as filter_expired_probe in commands.cc. Bit position
// for expiry_date in null_bits is bit 1 (0x2), matching the probe
// path's m_string_buf decode at commands.cc:1185 (string_keys'
// std::map column ordering, see feedback_ndb_record_column_index).
bool key_row_is_expired(const struct key_table *key_row) {
  if ((key_row->null_bits & 0x2) != 0) return false;
  Int32 expiry_seconds =
    mi_sint4korr((const unsigned char*)&key_row->expiry_date);
  if (expiry_seconds == g_max_expire_at) return false;
  Int64 now_seconds = (Int64)my_micro_time() / 1000000;
  return expiry_seconds <= now_seconds;
}

static void
read_callback(int result, NdbTransaction *trans, void *aObject) {
  struct KeyStorage *key_store = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_store->m_get_ctrl;
  assert(trans == key_store->m_trans);
  assert(get_ctrl->m_num_transactions > 0);
  assert(key_store->m_key_state == KeyState::MultiRow);
  (void)result;
  int code = trans->getNdbError().code;
  if (code != 0) {
    DEB_KS(("Key %u had error: %d\n", key_store->m_index, code));
    key_store->m_key_state = KeyState::CompletedFailed;
    get_ctrl->m_num_keys_completed_first_pass++;
    if (code != READ_ERROR) {
      get_ctrl->m_num_keys_failed++;
      if (get_ctrl->m_error_code == 0) {
        get_ctrl->m_error_code = code;
      }
    }
    key_store->m_close_flag = true;
    assert(get_ctrl->m_num_keys_multi_rows > 0);
    get_ctrl->m_num_keys_multi_rows--;
  } else if (key_store->m_key_row.num_rows > 0) {
    key_store->m_key_state = KeyState::MultiRowRWValue;
    key_store->m_get_value_size = key_store->m_key_row.tot_value_len;
    key_store->m_num_rows = key_store->m_key_row.num_rows;
    key_store->m_rondb_key = key_store->m_key_row.rondb_key;
    DEB_KS(("LockRead Key %u with size: %u, num_rows: %u"
            ", key_state: %u\n",
      key_store->m_index,
      key_store->m_get_value_size,
      key_store->m_num_rows,
      key_store->m_key_state));
  } else {
    key_store->m_key_state = KeyState::CompletedMultiRowSuccess;
    Uint32 value_len =
      get_length((char*)&key_store->m_key_row.value_start[0]);
    assert(value_len == key_store->m_key_row.tot_value_len);
    key_store->m_get_value_size = value_len;
    key_store->m_rondb_key = 0;
    DEB_KS(("LockRead Key %u completed, no value rows\n",
      key_store->m_index));
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
  Uint32 sz = (sizeof(struct key_table) - MAX_KEY_VALUE_LEN);
  assert(get_ctrl->m_num_bytes_outstanding >= sz);
  get_ctrl->m_num_bytes_outstanding -= sz;
  DEB_KS(("Key %u, keys %u and bytes %u out\n",
    key_store->m_index,
    get_ctrl->m_num_keys_outstanding,
    get_ctrl->m_num_bytes_outstanding));
}

int prepare_get_key_row(std::string *response,
                        KeyStorage *key_store,
                        bool is_set_command,
                        Uint32 database_id) {
  /**
   * Mask and options means simply reading all columns
   * except primary key columns.
   */
  struct key_table *key_row = &key_store->m_key_row;
  NdbTransaction *trans = key_store->m_trans;
  const Uint32 mask = 0xFC;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_BATCH_SAFE_FLAG;

  const NdbOperation *read_op = trans->readTuple(
    pk_key_record[database_id],
    (const char *)key_row,
    entire_key_record[database_id],
    (char *)key_row,
    is_set_command ?
      NdbOperation::LM_Exclusive : NdbOperation::LM_Read,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (read_op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  return 0;
}

void prepare_read_transaction(struct KeyStorage *key_storage) {
  key_storage->m_trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                                             &read_callback,
                                             (void*)key_storage);
}

int prepare_get_simple_key_row(std::string *response,
                               const Uint32 mask,
                               NdbTransaction *trans,
                               struct key_table *key_row,
                               Uint32 database_id) {
  /**
   * Mask and options means simply reading all columns
   * except primary key columns.
   */
  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_BATCH_SAFE_FLAG;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  const NdbOperation *read_op = trans->readTuple(
    pk_key_record[database_id],
    (const char *)key_row,
    entire_key_record[database_id],
    (char *)key_row,
    NdbOperation::LM_CommittedRead,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (read_op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  return 0;
}

static void
simple_read_callback(int result, NdbTransaction *trans, void *aObject) {
  struct KeyStorage *key_storage = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_storage->m_get_ctrl;
  (void)result;
  assert(trans == key_storage->m_trans);
  assert(get_ctrl->m_num_transactions > 0);
  int code = trans->getNdbError().code;
  if (code != 0) {
    key_storage->m_key_state = KeyState::CompletedFailed;
    get_ctrl->m_num_keys_completed_first_pass++;
    if (code == READ_ERROR) {
      DEB_HSET_KEY(("key %u had READ_ERROR\n", key_storage->m_index));
    } else {
      get_ctrl->m_num_keys_failed++;
      DEB_HSET_KEY(("key %u had ERROR: %d\n", key_storage->m_index, code));
      if (get_ctrl->m_error_code == 0) {
        get_ctrl->m_error_code = code;
      }
    }
  } else if (key_row_is_expired(&key_storage->m_key_row)) {
    // Phase 1.10b: NDB lazy GC may leave expiry_date < now rows
    // physically present. Treat as missing - same shape as the
    // READ_ERROR (626) branch above (CompletedFailed without a
    // num_keys_failed bump). Reply layer emits $-1 for GET, nil
    // entry for MGET, +0 for STRLEN, $0 for GETRANGE.
    key_storage->m_key_state = KeyState::CompletedFailed;
    get_ctrl->m_num_keys_completed_first_pass++;
    DEB_HSET_KEY(("key %u was expired (expiry_date < now)\n",
      key_storage->m_index));
  } else if (key_storage->m_key_row.num_rows > 0) {
    key_storage->m_key_state = KeyState::MultiRow;
    get_ctrl->m_num_keys_multi_rows++;
    DEB_HSET_KEY(("key %u required multi-row handling: num_rows: %u\n",
      key_storage->m_index, key_storage->m_key_row.num_rows));
  } else {
    key_storage->m_key_state = KeyState::CompletedSuccess;
    Uint32 value_len =
      get_length((char*)&key_storage->m_key_row.value_start[0]);
    assert(value_len == key_storage->m_key_row.tot_value_len);
    key_storage->m_get_value_size = value_len;
    get_ctrl->m_num_keys_completed_first_pass++;
    DEB_HSET_KEY(("key %u was read, size: %u\n",
      key_storage->m_index, value_len));
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
  key_storage->m_close_flag = true;
}

void commit_simple_read_transaction(struct KeyStorage *key_storage) {
  key_storage->m_trans->executeAsynchPrepare(NdbTransaction::Commit,
                                             &simple_read_callback,
                                             (void*)key_storage);
}

/**
 * INCR and DECR MODULE
 * --------------------
 */
void incr_decr_key_row(std::string *response,
                       [[maybe_unused]]/*todo remove?*/ Ndb *ndb,
                       const NdbDictionary::Table *tab,
                       NdbTransaction *trans,
                       struct key_table *key_row,
                       bool incr_flag,
                       Uint64 inc_dec_value,
                       int worker_id) {
  /**
   * The mask specifies which columns is to be updated after the interpreter
   * has finished. The values are set in the key_row.
   * We have 7 columns, we will update tot_value_len in interpreter, same with
   * value_start.
   *
   * The rest, redis_key, rondb_key, value_data_type, num_rows and expiry_date
   * are updated through final update.
   */
  const Uint32 mask = 0xAB;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  // redis_key already set as this is the Primary key
  key_row->null_bits = 1; // Set rondb_key to NULL, first NULL column
  key_row->num_rows = 0;
  key_row->value_data_type = 0;
  key_row->expiry_date = -1;

  Uint32 code_buffer[128];
  NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer) / sizeof(code_buffer[0]));
  if (initNdbCodeIncrDecr(response,
                          &code,
                          tab,
                          incr_flag,
                          inc_dec_value) != 0)
    return;

  // Prepare the interpreted program to be part of the write
  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED_INSERT;
  opts.interpretedCode = &code;

  /**
   * Prepare to get the final value of the Redis row after INCR is finished
   * This is performed by the reading the pseudo column that is reading the
   * output index written in interpreter program.
   */
  NdbOperation::GetValueSpec getvals[1];
  getvals[0].appStorage = nullptr;
  getvals[0].recAttr = nullptr;
  getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
  opts.numExtraGetFinalValues = 1;
  opts.extraGetFinalValues = getvals;

  if (get_dirty_incr_decr_flag(worker_id))
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_DIRTY_FLAG;

  /* Define the actual operation to be sent to RonDB data node. */
  Uint32 database_id = get_current_database(worker_id);
  const NdbOperation *op = trans->writeTuple(
    pk_key_record[database_id],
    (const char *)key_row,
    entire_key_record[database_id],
    (char *)key_row,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to create NdbOperation",
                               trans->getNdbError());
    return;
  }

  /* Send to RonDB and execute the INCR operation */
  int exec_rc = trans->execute(NdbTransaction::Commit,
                               NdbOperation::AbortOnError);
  const NdbError &ndb_err = trans->getNdbError();
  if (exec_rc != 0 || ndb_err.code != 0) {
    if (ndb_err.code == RONDB_KEY_NOT_NULL_ERROR) {
      assign_ndb_err_to_response(response,
                                 FAILED_INCR_KEY_MULTI_ROW,
                                 ndb_err);
      return;
    }
    if (ndb_err.code == RONDB_INTERP_INVALID_INT64) {
      // STR_TO_INT64 in the interpreted-code program could not parse
      // the stored value as an Int64. Redis-canonical reply for
      // INCR/DECR/HINCR/HDECR on a non-numeric string.
      assign_generic_err_to_response(response, FAILED_INCRBY_DECRBY_PARAMETER);
      return;
    }
    if (ndb_err.code == RONDB_INTERP_CALC_OVERFLOW) {
      // ADD_REG_REG / SUB_REG_REG overflowed Int64. Redis-canonical
      // reply for INCR/DECR/HINCR/HDECR crossing INT64 boundaries.
      assign_generic_err_to_response(response, FAILED_INCRBY_DECRBY_OVERFLOW);
      return;
    }
    assign_ndb_err_to_response(response,
                               FAILED_INCR_KEY,
                               ndb_err);
    return;
  }
  /* Retrieve the returned new value as an Int64 value */
  NdbRecAttr *recAttr = getvals[0].recAttr;
  Int64 new_incremented_value = recAttr->int64_value();
  DEB_INCR(("INCR/DECR success, new value: %lld\n", new_incremented_value));
  /* Send the return message to Redis client. The buffer needs to fit
   * ":<int64>\r\n" which is up to 24 bytes for INT64_MIN; use 32 for
   * safety. Previously char[20] truncated replies at |v| >= 10^18,
   * stripping the trailing \r\n and closing the connection (C5c). */
  char header_buf[32];
  snprintf(header_buf, sizeof(header_buf), ":%lld\r\n", new_incremented_value);
  response->append(header_buf);
}

/**
 * RONDIS Unique Key ids
 * ---------------------
 */
static
int get_unique_redis_key_id(const NdbDictionary::Table *tab,
                            Ndb *ndb,
                            Uint64 &redis_key_id,
                            std::string *response) {

  if (ndb->getAutoIncrementValue(tab, redis_key_id, unsigned(1024)) != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to get autoincrement value",
                               ndb->getNdbError());
    return -1;
  }
  return 0;
}

// Phase 1.0.4: redis_key_id_hash is a process-wide cache shared by
// every Rondis worker thread. The map is accessed both by the
// HGET-style read commands (which call rondb_get_redis_key_id
// directly) and was historically accessed by HSET / HDEL too,
// though Phase 1.0.2d / 1.0.3 moved those to in-trans lock-claim
// reads. Concurrent unsynchronized find / insert on a
// std::unordered_map is undefined behavior; guard with a mutex.
//
// The mutex is NOT held across NDB calls (get_unique_redis_key_id,
// write_hset_key_table) - those can take many milliseconds and
// holding the mutex would serialize every read command in the
// process. Pattern: lookup under lock, drop lock, do NDB work on
// a miss, re-acquire lock for the insert. A racing thread that
// won the insert wins; the loser uses the winner's id and
// discards its own prealloc (write_hset_key_table is an UPSERT,
// so both threads end up with the same row's redis_key_id - the
// loser's prealloc is harmlessly skipped).
static std::mutex redis_key_id_hash_mutex;
std::unordered_map<std::string, Uint64> redis_key_id_hash;
int rondb_get_redis_key_id(Ndb *ndb,
                           Uint64 &redis_key_id,
                           const char *key_str,
                           Uint32 key_len,
                           std::string *response,
                           Uint32 database_id) {
  std::string std_key_str = std::string(key_str, key_len);
  {
    std::lock_guard<std::mutex> guard(redis_key_id_hash_mutex);
    auto it = redis_key_id_hash.find(std_key_str);
    if (it != redis_key_id_hash.end()) {
      redis_key_id = it->second;
      DEB_HSET_KEY(("Found local redis_key_id = %llu for key: %s\n",
        redis_key_id, key_str));
      return 0;
    }
  }
  /* Cache miss; resolve via NDB without holding the mutex. */
  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  if (dict == nullptr) {
    assign_ndb_err_to_response(response, FAILED_GET_DICT, ndb->getNdbError());
    return -1;
  }
  const NdbDictionary::Table *tab = dict->getTable(HSET_KEY_TABLE_NAME);
  if (tab == nullptr) {
    assign_ndb_err_to_response(response,
                                FAILED_CREATE_TABLE_OBJECT,
                                dict->getNdbError());
    return -1;
  }
  int ret_code = get_unique_redis_key_id(tab,
                                         ndb,
                                         redis_key_id,
                                         response);
  if (ret_code < 0) {
    DEB_HSET_KEY(("Failed get_unique_redis_key_id, err: %d\n", ret_code));
    return -1;
  }
  ret_code = write_hset_key_table(ndb,
                                  tab,
                                  std_key_str,
                                  redis_key_id,
                                  response,
                                  database_id);
  if (ret_code < 0) {
    DEB_HSET_KEY(("Failed write_hset_key_table, err: %d\n", ret_code));
    return -1;
  }
  /* Reacquire lock for the insert. If another thread populated the
   * entry while we were doing NDB work, use that thread's id (both
   * threads UPSERTed the same hset_keys row, so the ids match).
   */
  {
    std::lock_guard<std::mutex> guard(redis_key_id_hash_mutex);
    auto [it, inserted] =
      redis_key_id_hash.emplace(std_key_str, redis_key_id);
    if (!inserted) {
      redis_key_id = it->second;
    }
  }
  DEB_HSET_KEY(("Created redis_key_id = %llu for key: %s\n",
    redis_key_id, key_str));
  return 0;
}

int rondb_get_rondb_key(const NdbDictionary::Table *tab,
                        Uint64 &rondb_key,
                        Ndb *ndb,
                        std::string *response) {
  if (ndb->getAutoIncrementValue(tab, rondb_key, unsigned(1024)) != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to get autoincrement value",
                               ndb->getNdbError());
    return -1;
  }
  return 0;
}

void execute_set_range_simple(std::string *response,
                              KeyStorage *key_store,
                              const NdbDictionary::Table *tab,
                              Uint32 database_id,
                              Uint32 start,
                              Uint32 end) {
  struct key_table key_row;
  NdbTransaction *trans = key_store->m_trans;
  Uint32 mask = 0x3;
  key_row.null_bits = 0;
  memcpy(&key_row.redis_key[2],
         key_store->m_key_str,
         key_store->m_key_len);
  set_length(&key_row.redis_key[0], key_store->m_key_len);
  key_row.redis_key_id = STRING_REDIS_KEY_ID;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;

  Uint32 code_buffer[64];
  NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer) / sizeof(code_buffer[0]));
  int ret_code = simple_write_key_row_setrange(code,
                                               tab,
                                               key_store,
                                               start,
                                               end);
  if (ret_code != 0) {
    assign_err_to_response(response,
                           "Failed to create interpreted code",
                           ret_code);
    return;
  }
  // Prepare the interpreted program to be part of the write
  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
  opts.optionsPresent |=
    NdbOperation::OperationOptions::OO_INTERPRETED_INSERT;
  opts.interpretedCode = &code;

  NdbOperation::GetValueSpec getvals[1];
  getvals[0].appStorage = nullptr;
  getvals[0].recAttr = nullptr;
  getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
  opts.numExtraGetFinalValues = 1;
  opts.extraGetFinalValues = getvals;

  /* Define the actual operation to be sent to RonDB data node. */
  const NdbOperation *op = trans->writeTuple(
    pk_key_record[database_id],
    (const char *)&key_row,
    entire_key_record[database_id],
    (char *)&key_row,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to create NdbOperation",
                               trans->getNdbError());
    return;
  }
  if (key_store->m_trans->execute(NdbTransaction::Commit) != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to execute SETRANGE simple command",
                               trans->getNdbError());
    return;
  }
  Uint32 new_len = (Uint32)getvals[0].recAttr->u_64_value();
  char buf[20];
  [[maybe_unused]] int len =
  snprintf(buf, sizeof(buf), "+%u\r\n", new_len);
  response->append(buf);
  return;
}

int write_key_row_setrange(std::string *response,
                           KeyStorage *key_store,
                           const NdbDictionary::Table *tab,
                           Uint32 database_id,
                           Uint32 start,
                           Uint32 end,
                           Uint32 &old_tot_value_len) {
  struct key_table key_row;
  NdbTransaction *trans = key_store->m_trans;
  Uint32 mask = 0x3;
  key_row.null_bits = 0;
  memcpy(&key_row.redis_key[2],
         key_store->m_key_str,
         key_store->m_key_len);
  set_length(&key_row.redis_key[0], key_store->m_key_len);
  key_row.redis_key_id = STRING_REDIS_KEY_ID;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;

  Uint32 code_buffer[64];
  NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer) / sizeof(code_buffer[0]));
  int ret_code = write_key_row_setrange_int(code,
                                            tab,
                                            key_store,
                                            start,
                                            end,
                                            key_store->m_rondb_key);
  if (ret_code != 0) {
    assign_err_to_response(response,
                           "Failed to create interpreted code",
                           ret_code);
    return -1;
  }
  // Prepare the interpreted program to be part of the write
  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
  opts.optionsPresent |=
    NdbOperation::OperationOptions::OO_INTERPRETED_INSERT;
  opts.interpretedCode = &code;

  NdbOperation::GetValueSpec getvals[2];
  getvals[0].appStorage = nullptr;
  getvals[0].recAttr = nullptr;
  getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
  getvals[1].appStorage = nullptr;
  getvals[1].recAttr = nullptr;
  getvals[1].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_1;
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
  opts.numExtraGetFinalValues = 2;
  opts.extraGetFinalValues = getvals;

  /* Define the actual operation to be sent to RonDB data node. */
  const NdbOperation *op = trans->writeTuple(
    pk_key_record[database_id],
    (const char *)&key_row,
    entire_key_record[database_id],
    (char *)&key_row,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to create NdbOperation",
                               trans->getNdbError());
    return -1;
  }
  if (key_store->m_trans->execute(NdbTransaction::NoCommit) != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to execute SETRANGE command",
                               trans->getNdbError());
    return -1;
  }
  old_tot_value_len = (Uint32)getvals[0].recAttr->u_64_value();
  key_store->m_rondb_key = getvals[1].recAttr->u_64_value();
  DEB_SETRANGE(("old_tot_value_len: %u, rondb_key: %llu\n",
    old_tot_value_len,
    key_store->m_rondb_key));
  return 0;
}

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
                             bool last_row) {
  struct value_table value_row;
  value_row.ordinal = row_id;
  value_row.rondb_key = key_store->m_rondb_key;
  /* Mask means writing all columns. */
  const Uint32 mask = 0x3;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  const NdbDictionary::Table *value_tab = dict->getTable(VALUE_TABLE_NAME);
  if (value_tab == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TABLE_OBJECT,
                               dict->getNdbError());
    return -1;
  }
  Uint32 code_buffer[64];
  NdbInterpretedCode code(value_tab, &code_buffer[0], sizeof(code_buffer) / sizeof(code_buffer[0]));
  int ret_code = write_value_row_setrange_int(code,
                                              value_tab,
                                              start_zero_index,
                                              end_zero_index,
                                              start_write_index,
                                              end_write_index,
                                              start_write_ptr);
  if (ret_code != 0) {
    assign_err_to_response(response,
                           "Failed to create interpreted code",
                           ret_code);
    return -1;
  }
  // Prepare the interpreted program to be part of the write
  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
  opts.optionsPresent |=
    NdbOperation::OperationOptions::OO_INTERPRETED_INSERT;
  opts.interpretedCode = &code;

  const NdbOperation *write_op = key_store->m_trans->writeTuple(
    pk_value_record[database_id],
    (const char *)&value_row,
    entire_value_record[database_id],
    (char *)&value_row,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (write_op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               key_store->m_trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  if (key_store->m_trans->execute(
    last_row ? NdbTransaction::Commit : NdbTransaction::NoCommit) != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to execute SETRANGE command",
                               key_store->m_trans->getNdbError());
    return -1;
  }
  return 0;
}
