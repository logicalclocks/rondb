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

//#define DEBUG_DEL_CMD 1
//#define DEBUG_KS 1
//#define DEBUG_HSET_KEY 1
//#define DEBUG_MSET 1
//#define DEBUG_INCR 1

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
  NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer));
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

static void
write_callback(int result, NdbTransaction *trans, void *aObject) {
  struct KeyStorage *key_storage = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_storage->m_get_ctrl;
  (void)result;
  assert(trans == key_storage->m_trans);
  assert(get_ctrl->m_num_transactions > 0);
  int code = trans->getNdbError().code;
  if (code != 0) {
    key_storage->m_key_state = KeyState::CompletedFailed;
    get_ctrl->m_num_keys_failed++;
    DEB_HSET_KEY(("key %u had ERROR: %d\n", key_storage->m_index, code));
    if (get_ctrl->m_error_code == 0) {
      get_ctrl->m_error_code = code;
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

static void
simple_write_callback(int result, NdbTransaction *trans, void *aObject) {
  struct KeyStorage *key_store = (struct KeyStorage*)aObject;
  struct GetControl *get_ctrl = key_store->m_get_ctrl;
  (void)result;
  DEB_HSET_KEY(("result = %d\n", result));
  assert(trans == key_store->m_trans);
  assert(get_ctrl->m_num_transactions > 0);
  int code = trans->getNdbError().code;
  if (code != 0 || result != 0) {
    if (code == RESTRICT_VALUE_ROWS_ERROR) {
      key_store->m_key_state = KeyState::MultiRow;
      get_ctrl->m_num_keys_multi_rows++;
      DEB_HSET_KEY(("key %u had RESTRICT_VALUE_ROWS_ERROR\n",
        key_store->m_index));
    } else {
      key_store->m_key_state = KeyState::CompletedFailed;
      get_ctrl->m_num_keys_failed++;
      DEB_HSET_KEY(("key %u had ERROR: %d\n", key_store->m_index, code));
      if (get_ctrl->m_error_code == 0) {
        get_ctrl->m_error_code = code;
      }
    }
  } else {
    get_ctrl->m_num_keys_completed_first_pass++;
    key_store->m_key_state = KeyState::CompletedSuccess;
    DEB_HSET_KEY(("key %u simple write succeeded\n",
      key_store->m_index));
  }
  assert(get_ctrl->m_num_keys_outstanding > 0);
  get_ctrl->m_num_keys_outstanding--;
  key_store->m_close_flag = true;
}

void commit_simple_write_transaction(struct KeyStorage *key_store) {
  key_store->m_trans->executeAsynchPrepare(NdbTransaction::Commit,
                                           &simple_write_callback,
                                           (void*)key_store);
}

int write_data_to_key_op(std::string *response,
                         const NdbDictionary::Table *tab,
                         KeyStorage *key_store,
                         Uint64 redis_key_id,
                         bool commit_flag,
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
  NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer));
  int ret_code = 0;
  if (commit_flag) {
    ret_code = write_key_row_commit(response, code, tab, key_store);
  } else {
    ret_code = write_key_row_no_commit(response,
                                       code,
                                       tab,
                                       key_store);
  }
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

  NdbOperation::GetValueSpec getvals[3];
  if (commit_flag) {
    getvals[0].appStorage = nullptr;
    getvals[0].recAttr = nullptr;
    getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
    opts.numExtraGetFinalValues = 1;
    opts.extraGetFinalValues = getvals;
  } else {
    getvals[0].appStorage = nullptr;
    getvals[0].recAttr = nullptr;
    getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
    getvals[1].appStorage = nullptr;
    getvals[1].recAttr = nullptr;
    getvals[1].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_1;
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
    if (key_store->m_keep_ttl == true &&
        key_store->m_num_rows > 0) {
      getvals[1].appStorage = nullptr;
      getvals[1].recAttr = nullptr;
      getvals[1].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_2;
      opts.numExtraGetFinalValues = 3;
    } else {
      opts.numExtraGetFinalValues = 2;
    }
    opts.extraGetFinalValues = getvals;
  }
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
  key_store->m_rec_attr_expiry_date = getvals[2].recAttr;
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
  if (key_store->m_key_state == KeyState::CompletedMultiRowSuccess) {
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
  const NdbOperation *read_op = trans->readTuple(
    pk_value_record[database_id],
    (const char *)value_row,
    entire_value_record[database_id],
    (char *)value_row,
    is_set_command ?
      NdbOperation::LM_Exclusive : NdbOperation::LM_SimpleRead,
    mask_ptr);
  if (read_op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  return 0;
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

  const NdbOperation *read_op = trans->readTuple(
    pk_key_record[database_id],
    (const char *)key_row,
    entire_key_record[database_id],
    (char *)key_row,
    is_set_command ?
      NdbOperation::LM_Exclusive : NdbOperation::LM_Read,
    mask_ptr);
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
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  const NdbOperation *read_op = trans->readTuple(
    pk_key_record[database_id],
    (const char *)key_row,
    entire_key_record[database_id],
    (char *)key_row,
    NdbOperation::LM_CommittedRead,
    mask_ptr);
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
  NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer));
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
  if (trans->execute(NdbTransaction::Commit,
                     NdbOperation::AbortOnError) != 0 ||
      trans->getNdbError().code != 0) {
    if (trans->getNdbError().code == RONDB_KEY_NOT_NULL_ERROR) {
      DEB_INCR(("RONDB_KEY_NOT_NULL_ERROR\n"));
      assign_ndb_err_to_response(response,
                                 FAILED_INCR_KEY_MULTI_ROW,
                                 trans->getNdbError());
      return;
    }
    DEB_INCR(("INCR_DECR_ERROR: %d\n", trans->getNdbError().code));
    assign_ndb_err_to_response(response,
                               FAILED_INCR_KEY,
                               trans->getNdbError());
    return;
  }
  /* Retrieve the returned new value as an Int64 value */
  NdbRecAttr *recAttr = getvals[0].recAttr;
  Int64 new_incremented_value = recAttr->int64_value();
  DEB_INCR(("INCR/DECR success, new value: %lld\n", new_incremented_value));
  /* Send the return message to Redis client */
  char header_buf[20];
  [[maybe_unused]]/*todo remove?*/ int header_len =
    snprintf(header_buf,
      sizeof(header_buf),
      ":%lld\r\n",
      new_incremented_value);
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

std::unordered_map<std::string, Uint64> redis_key_id_hash;
int rondb_get_redis_key_id(Ndb *ndb,
                           Uint64 &redis_key_id,
                           const char *key_str,
                           Uint32 key_len,
                           std::string *response,
                           Uint32 database_id) {
  std::string std_key_str = std::string(key_str, key_len);
  auto it = redis_key_id_hash.find(std_key_str);
  if (it == redis_key_id_hash.end()) {
    /* Found no redis_key_id in local hash */
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
    redis_key_id_hash[std_key_str] = redis_key_id;
    DEB_HSET_KEY(("Created redis_key_id = %llu for key: %s\n",
      redis_key_id, key_str));
  } else {
    redis_key_id = it->second;
    /* Found local redis_key_id */
    DEB_HSET_KEY(("Found local redis_key_id = %llu for key: %s\n",
      redis_key_id, key_str));
  }
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
  NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer));
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
  NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer));
  int ret_code = write_key_row_setrange(code,
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
  if (key_store->m_trans->execute(NdbTransaction::Commit) != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to execute SETRANGE command",
                               trans->getNdbError());
    return -1;
  }
  old_tot_value_len = (Uint32)getvals[0].recAttr->u_64_value();
  key_store->m_rondb_key = getvals[1].recAttr->u_64_value();
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
                             Uint32 database_id) {
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
  NdbInterpretedCode code(value_tab, &code_buffer[0], sizeof(code_buffer));
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
  return 0;
}
