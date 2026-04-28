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
   GNU General Public License for more details.
  
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
   USA.
 */

#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <algorithm>
#include "redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>
#include <assert.h>
#include <random>

#include "db_operations.h"
#include "commands.h"
#include "common.h"
#include "table_definitions.h"
#include "include/my_systime.h"
#include "include/myisampack.h"

#define RAND_CONSTANT 10000

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
#define DEBUG_MGET_CMD 1
#define DEBUG_MSET_CMD 1
#define DEBUG_DEL_CMD 1
#define DEBUG_HSET_KEY 1
#define DEBUG_INCR 1
#define DEBUG_RAND_KEY 1
#define DEBUG_TTL 1
#define DEBUG_SETRANGE 1
#endif

#ifdef DEBUG_SETRANGE
#define DEB_SETRANGE(arglist) do { printf arglist ; } while (0)
#else
#define DEB_SETRANGE(arglist)
#endif

#ifdef DEBUG_RAND_KEY
#define DEB_RAND_KEY(arglist) do { printf arglist ; } while (0)
#else
#define DEB_RAND_KEY(arglist)
#endif

#ifdef DEBUG_INCR
#define DEB_INCR(arglist) do { printf arglist ; } while (0)
#else
#define DEB_INCR(arglist)
#endif

#ifdef DEBUG_MGET_CMD
#define DEB_MGET_CMD(arglist) do { printf arglist ; } while (0)
#else
#define DEB_MGET_CMD(arglist)
#endif

#ifdef DEBUG_MSET_CMD
#define DEB_MSET_CMD(arglist) do { printf arglist ; } while (0)
#else
#define DEB_MSET_CMD(arglist)
#endif

#ifdef DEBUG_DEL_CMD
#define DEB_DEL_CMD(arglist) do { printf arglist ; } while (0)
#else
#define DEB_DEL_CMD(arglist)
#endif

#ifdef DEBUG_HSET_KEY
#define DEB_HSET_KEY(arglist) do { printf arglist ; } while (0)
#else
#define DEB_HSET_KEY(arglist)
#endif

#ifdef DEBUG_TTL
#define DEB_TTL(arglist) do { printf arglist ; } while (0)
#else
#define DEB_TTL(arglist)
#endif

static int
rondb_get_func(Ndb *ndb,
               const NdbDictionary::Table *tab,
               std::string *response,
               Uint64 redis_key_id,
               struct GetControl *get_ctrl,
               KeyStorage *key_storage,
               Uint32 num_keys);

static int rondb_get_response(std::string *response,
                              KeyStorage *key_storage,
                              Uint32 num_keys);

/**
 * GENERIC SUPPORT MODULE
 * ----------------------
 */
static
bool setup_metadata(
  Ndb *ndb,
  std::string *response,
  const NdbDictionary::Dictionary **ret_dict,
  const NdbDictionary::Table **ret_tab) {
  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  if (dict == nullptr) {
    assign_ndb_err_to_response(response, FAILED_GET_DICT, ndb->getNdbError());
    return false;
  }
  const NdbDictionary::Table *tab = dict->getTable(KEY_TABLE_NAME);
  if (tab == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TABLE_OBJECT,
                               dict->getNdbError());
    return false;
  }
  *ret_tab = tab;
  *ret_dict = dict;
  return true;
}

static
bool setup_one_transaction(Ndb *ndb,
                           std::string *response,
                           Uint64 redis_key_id,
                           KeyStorage *key_store,
                           const NdbDictionary::Table *tab) {
  struct key_table *key_row = &key_store->m_key_row;
  const char *key_str = key_store->m_key_str;
  Uint32 key_len = key_store->m_key_len;
  if (key_len > MAX_KEY_VALUE_LEN) {
    assign_generic_err_to_response(response, REDIS_KEY_TOO_LARGE);
    return false;
  }
  key_row->redis_key_id = redis_key_id;
  memcpy(&key_row->redis_key[2], key_str, key_len);
  memset(&key_row->redis_key[2 + key_len], 0, 3);
  set_length((char*)&key_row->redis_key[0], key_len);
  NdbTransaction *trans =
    ndb->startTransaction(tab,
                          (const char*)&key_row->redis_key_id,
                          key_len + 10);
  if (trans == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TXN_OBJECT,
                               ndb->getNdbError());
    return false;
  }
  key_store->m_trans = trans;
  return true;
}

bool setup_transaction(
    Ndb *ndb,
    std::string *response,
    Uint64 redis_key_id,
    struct KeyStorage *key_store,
    const NdbDictionary::Dictionary **ret_dict,
    const NdbDictionary::Table **ret_tab) {
  if (!setup_metadata(ndb,
                      response,
                      ret_dict,
                      ret_tab)) {
      return false;
  }
  return setup_one_transaction(ndb,
                               response,
                               redis_key_id,
                               key_store,
                               *ret_tab);
}

static void
close_finished_transactions(KeyStorage *key_storage,
                            GetControl *get_ctrl,
                            Uint32 loop_count,
                            Uint32 current_index) {
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    if (key_storage[inx].m_close_flag) {
      DEB_DEL_CMD(("Close finished transaction from key: %u\n",
        key_storage[i].m_index));
      assert(get_ctrl->m_num_transactions > 0);
      get_ctrl->m_num_transactions--;
      get_ctrl->m_ndb->closeTransaction(key_storage[i].m_trans);
      key_storage[i].m_trans = nullptr;
    }
  }
}

static void
close_transactions(KeyStorage *key_storage,
                   GetControl *get_ctrl) {
  Uint32 loop_count = get_ctrl->m_num_keys_requested;
  for (Uint32 i = 0; i < loop_count; i++) {
    if (key_storage[i].m_trans != nullptr) {
      DEB_DEL_CMD(("Close transaction from key: %u\n",
        key_storage[i].m_index));
      assert(get_ctrl->m_num_transactions > 0);
      get_ctrl->m_num_transactions--;
      get_ctrl->m_ndb->closeTransaction(key_storage[i].m_trans);
      key_storage[i].m_trans = nullptr;
    }
  }
}

static void
rand_key(struct KeyStorage *key_store,
         const char **key_str,
         Uint32 &key_len) {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<Uint32> dist(0, UINT32_MAX);
  Uint32 rand_number = dist(gen);
  rand_number = rand_number % RAND_CONSTANT;
  char *new_key_str = &key_store->m_key_buf[0];
  Uint32 new_len = snprintf(new_key_str,
                            16,
                            "key:%u",
                            rand_number);
  *key_str = new_key_str;
  key_len = new_len;
  DEB_RAND_KEY(("Change to use key: %s\n", new_key_str));
  return;
}

static int
execute_ndb(Ndb *ndb, int min_finished, int line) {
  (void)line;
  int finished = ndb->sendPollNdb(100, min_finished);
  return finished;
}

static Int64 get_current_unix_time() {
  Int64 now = (Int64)my_micro_time() / 1000000;
  return now;
}

static bool get_int64(std::string opt_val,
                      std::string *response,
                      Int64 *ret_value) {
  // Previously used std::stoll, which silently accepts a trailing
  // non-digit suffix ("1.5" -> 1, "42abc" -> 42). strtoll with an
  // end-pointer check rejects that, matching the Redis-canonical
  // "value is not an integer or out of range" semantics and the
  // stricter pattern already used in INCRBY/HINCRBY (C14).
  const char *p = opt_val.c_str();
  const char *end = p + opt_val.size();
  char *ep = nullptr;
  errno = 0;
  Int64 val = strtoll(p, &ep, 10);
  if (errno == EINVAL || errno == ERANGE || ep != end || ep == p) {
    assign_generic_err_to_response(response, REDIS_INVALID_INTEGER);
    return false;
  }
  *ret_value = val;
  return true;
}

const Int32 g_max_expire_at = 0x7FFFFFFF;
static void generate_expire_at(Int64* binary, Int64 ttl) {
  *binary = 0;
  time_t now = (time_t)my_micro_time() / 1000000;
  if (ttl != -1) {
    now += ttl;
    if (now > g_max_expire_at) {
      now = g_max_expire_at;
    }
  } else {
    now = g_max_expire_at;
  }
  mi_int4store(binary, now);
}

/**
 * RELEASE MODULE
 * --------------
 */
void release_mset(struct GetControl *get_ctrl) {
  struct KeyStorage *key_storage = get_ctrl->m_key_store;
  close_transactions(key_storage, get_ctrl);
  for (Uint32 i = 0; i < get_ctrl->m_num_keys_requested; i++) {
    if (key_storage[i].m_trans != nullptr) {
      get_ctrl->m_ndb->closeTransaction(key_storage[i].m_trans);
      assert(get_ctrl->m_num_transactions > 0);
      get_ctrl->m_num_transactions--;
    }
  }
  assert(get_ctrl->m_num_transactions == 0);
  free(get_ctrl);
  free(key_storage);
}

void release_del(struct GetControl *get_ctrl) {
  struct KeyStorage *key_storage = get_ctrl->m_key_store;
  close_transactions(key_storage, get_ctrl);
  for (Uint32 i = 0; i < get_ctrl->m_num_keys_requested; i++) {
    if (key_storage[i].m_trans != nullptr) {
      get_ctrl->m_ndb->closeTransaction(key_storage[i].m_trans);
      assert(get_ctrl->m_num_transactions > 0);
      get_ctrl->m_num_transactions--;
    }
  }
  assert(get_ctrl->m_num_transactions == 0);
  free(get_ctrl);
  free(key_storage);
}

void release_mget(struct GetControl *get_ctrl) {
  struct KeyStorage *key_storage = get_ctrl->m_key_store;
  close_transactions(key_storage, get_ctrl);
  for (Uint32 i = 0; i < get_ctrl->m_num_keys_requested; i++) {
    if (key_storage[i].m_trans != nullptr) {
      get_ctrl->m_ndb->closeTransaction(key_storage[i].m_trans);
      get_ctrl->m_num_transactions--;
    }
    if (key_storage[i].m_value_ptr != nullptr) {
      free(key_storage[i].m_value_ptr);
    }
  }
  if (get_ctrl->m_value_rows != nullptr) {
    free(get_ctrl->m_value_rows);
  }
  assert(get_ctrl->m_num_transactions == 0);
  free(get_ctrl);
  free(key_storage);
}

/**
 * DELETE MODULE
 * -------------
 */
static int send_value_delete(std::string *response,
                             struct KeyStorage *key_store,
                             struct GetControl *get_ctrl) {
  Uint32 i = 0;
  do {
    [[maybe_unused]]/*todo remove?*/ int ret_code =
      prepare_delete_value_row(response,
                               key_store,
                               key_store->m_num_rw_rows,
                               get_ctrl->m_database_id);
    key_store->m_num_rw_rows++;
    i++;
    if (key_store->m_num_rw_rows == key_store->m_num_rows) {
      key_store->m_key_state = KeyState::MultiRowRWAll;
      key_store->m_num_current_rw_rows = i;
      get_ctrl->m_num_bytes_outstanding += (i * DELETE_BYTES);
      DEB_DEL_CMD(("Commit send value delete: Key %u, last row:%u"
                   ", num_rows: %u, key_state: %u\n",
                   key_store->m_index,
                   key_store->m_num_rw_rows,
                   key_store->m_num_current_rw_rows,
                   key_store->m_key_state));
      commit_complex_delete_transaction(key_store);
      return 0;
    }
    assert(key_store->m_num_rw_rows < key_store->m_num_rows);
    if (get_ctrl->m_num_bytes_outstanding > MAX_OUTSTANDING_BYTES) {
      key_store->m_key_state = KeyState::MultiRowRWValueSent;
      key_store->m_num_current_rw_rows = i;
      get_ctrl->m_num_bytes_outstanding += (i * DELETE_BYTES);
      DEB_DEL_CMD(("Prepare send value delete: Key %u, last row:%u"
                   ", num_rows: %u, key_state: %u\n",
                   key_store->m_index,
                   key_store->m_num_rw_rows,
                   key_store->m_num_current_rw_rows,
                   key_store->m_key_state));
      prepare_delete_value_transaction(key_store);
      return 0;
    }
  } while (1);
  return 0;
}

static int send_next_delete_batch(std::string *response,
                                 struct KeyStorage *key_storage,
                                 struct GetControl *get_ctrl,
                                 Uint32 current_index,
                                 Uint32 loop_count,
                                 Uint32 &current_finished) {
  if (get_ctrl->m_num_keys_multi_rows == 0) {
    return 0;
  }
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    if (get_ctrl->m_num_bytes_outstanding > MAX_OUTSTANDING_BYTES) {
      assert(get_ctrl->m_num_keys_outstanding > 0);
      return 0;
    }
    if (key_storage[inx].m_key_state == KeyState::CompletedMultiRow) {
      commit_complex_delete_transaction(&key_storage[inx]);
      key_storage[inx].m_key_state = KeyState::MultiRowRWAll;
      key_storage[inx].m_num_current_rw_rows = 0;
      get_ctrl->m_num_keys_outstanding++;
      DEB_DEL_CMD(("Commit with no value rows"));
    } else if (key_storage[inx].m_key_state == KeyState::MultiRowRWValue) {
      assert(key_storage[inx].m_num_rows > key_storage[inx].m_num_rw_rows);
      get_ctrl->m_num_keys_outstanding++;
      int ret_code = send_value_delete(response,
                                       &key_storage[inx],
                                       get_ctrl);
      if (ret_code != 0) return 1;
      assert(current_finished > 0);
      current_finished--;
    }
  }
  return 0;
}

static int del_complex_rows(Ndb *ndb,
                            const NdbDictionary::Table *tab,
                            std::string *response,
                            Uint64 redis_key_id,
                            struct KeyStorage *key_storage,
                            struct GetControl *get_ctrl,
                            Uint32 loop_count,
                            Uint32 current_index) {
  Uint32 num_complex_deletes = 0;
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    if (key_storage[inx].m_key_state == KeyState::MultiRow) {
      num_complex_deletes++;
      DEB_DEL_CMD(("Start complex delete of key id %u\n", inx));
      if (!setup_one_transaction(ndb,
                                 response,
                                 redis_key_id,
                                 &key_storage[inx],
                                 tab)) {
        return 1;
      }
      get_ctrl->m_num_transactions++;
      int ret_code = prepare_complex_delete_row(response,
                                                tab,
                                                &key_storage[inx]);
      if (ret_code != 0) {
        return 1;
      }
      prepare_complex_delete_transaction(&key_storage[inx]);
    } else {
      DEB_DEL_CMD(("No complex delete of key: %u\n", inx));
    }
  }
  DEB_DEL_CMD(("num_complex_deletes: %u, multi_rows: %u\n",
    num_complex_deletes,
    get_ctrl->m_num_keys_multi_rows));
  assert(num_complex_deletes == get_ctrl->m_num_keys_multi_rows);
  Uint32 current_finished_in_loop = 0;
  get_ctrl->m_num_keys_outstanding = num_complex_deletes;
  get_ctrl->m_num_bytes_outstanding = num_complex_deletes * DELETE_BYTES;
  do {
    /**
     * Now send off all prepared and wait for at least one to complete.
     * We cannot wait for multiple ones since we could then run into
     * deadlock issues. The transactions are independent of each other,
     * so if one of them has to wait for a lock, it should not stop
     * other transactions from progressing.
     */
    DEB_DEL_CMD(("Call sendPollNdb with %u keys, %u keys out and %u bytes"
                 " out, current_finished_in_loop: %u\n",
                 get_ctrl->m_num_keys_multi_rows,
                 get_ctrl->m_num_keys_outstanding,
                 get_ctrl->m_num_bytes_outstanding,
                 current_finished_in_loop));
    int min_finished = 1;
    int finished = execute_ndb(ndb, min_finished, __LINE__);
    DEB_DEL_CMD(("Finished serving %u keys, prepare next batch"
                 ", current_finished_in_loop: %u, ndb: %p\n",
      finished, current_finished_in_loop, ndb));
    current_finished_in_loop += finished;
    if (get_ctrl->m_num_keys_failed > 0) return 0;
    int ret_code = send_next_delete_batch(response,
                                          key_storage,
                                          get_ctrl,
                                          current_index,
                                          loop_count,
                                          current_finished_in_loop);
    DEB_DEL_CMD(("Next delete batch sent, keys out: %u,"
                 "current_finished_in_loop: %u\n",
      get_ctrl->m_num_keys_outstanding, current_finished_in_loop));
    if (ret_code != 0) return 1;
    assert(finished > 0);
  } while (current_finished_in_loop < num_complex_deletes);
  return 0;
}

static int del_simple_rows(Ndb *ndb,
                           const NdbDictionary::Table *tab,
                           std::string *response,
                           Uint64 redis_key_id,
                           struct KeyStorage *key_storage,
                           struct GetControl *get_ctrl,
                           Uint32 loop_count,
                           Uint32 current_index) {
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    DEB_DEL_CMD(("Try simple del key: %u\n", inx));
    if (!setup_one_transaction(ndb,
                               response,
                               redis_key_id,
                               &key_storage[inx],
                               tab)) {
      return 1;
    }
    get_ctrl->m_num_transactions++;
    get_ctrl->m_num_keys_outstanding++;
    int ret_code = prepare_simple_delete_row(response,
                                             tab,
                                             &key_storage[inx]);
    if (ret_code != 0) {
      return 1;
    }
    prepare_simple_delete_transaction(&key_storage[inx]);
  }
  Uint32 current_finished_in_loop = 0;
  assert(loop_count >= get_ctrl->m_num_keys_multi_rows);
  Uint32 count_finished = loop_count;
  do {
    /**
     * Now send off all prepared and wait for all to complete.
     * Since each transaction is independent and only takes one
     * Exclusive lock there is no risk for deadlock.
     *
     * In the future when we can run it in one transaction we will
     * avoid deadlocks by sorting the rows AND by using a single
     * partition in the table 'string_keys'.
     */
    int min_finished = 1;
    int finished = execute_ndb(ndb, min_finished, __LINE__);
    assert(finished >= 0);
    current_finished_in_loop += finished;
  } while (current_finished_in_loop < count_finished);
  assert(get_ctrl->m_num_keys_outstanding == 0);
  return 0;
}

static
void rondb_del(Ndb *ndb,
              const pink::RedisCmdArgsType &argv,
              std::string *response,
              Uint64 redis_key_id,
              Uint32 worker_id)
{
  Uint32 arg_index_start = (redis_key_id == STRING_REDIS_KEY_ID) ? 1 : 2;
  Uint32 num_keys = argv.size() - arg_index_start;
  assert(num_keys > 0);
  const NdbDictionary::Dictionary *dict;
  const NdbDictionary::Table *tab = nullptr;
  struct KeyStorage *key_storage;
  key_storage = (struct KeyStorage*)malloc(
    sizeof(struct KeyStorage) * num_keys);
  if (key_storage == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    return;
  }
  struct GetControl *get_ctrl = (struct GetControl*)
    malloc(sizeof(struct GetControl));
  if (get_ctrl == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    free(get_ctrl);
    return;
  }
  get_ctrl->m_ndb = ndb;
  get_ctrl->m_key_store = key_storage;
  get_ctrl->m_value_rows = nullptr;
  get_ctrl->m_next_value_row = 0;
  get_ctrl->m_num_transactions = 0;
  get_ctrl->m_num_keys_requested = num_keys;
  get_ctrl->m_num_keys_outstanding = 0;
  get_ctrl->m_num_bytes_outstanding = 0;
  get_ctrl->m_num_keys_completed_first_pass = 0;
  get_ctrl->m_num_keys_multi_rows = 0;
  get_ctrl->m_num_keys_failed = 0;
  get_ctrl->m_num_read_errors = 0;
  get_ctrl->m_error_code = 0;
  get_ctrl->m_database_id = get_current_database(worker_id);
  for (Uint32 i = 0; i < num_keys; i++) {
    Uint32 arg_index_key = i + arg_index_start;
    key_storage[i].m_index = i;
    key_storage[i].m_close_flag = false;
    key_storage[i].m_get_ctrl = get_ctrl;
    key_storage[i].m_trans = nullptr;
    key_storage[i].m_key_str = argv[arg_index_key].c_str();
    key_storage[i].m_key_len = argv[arg_index_key].size();
    DEB_DEL_CMD(("DEL key: %u, key_str: %s, key_len: %u\n",
      i, key_storage[i].m_key_str, key_storage[i].m_key_len));
    key_storage[i].m_value_ptr = nullptr;
    key_storage[i].m_get_value_size = 0;
    key_storage[i].m_set_value_size = 0;
    key_storage[i].m_header_len = 0;
    key_storage[i].m_first_value_row = 0;
    key_storage[i].m_current_pos = 0;
    key_storage[i].m_num_rows = 0;
    key_storage[i].m_num_rw_rows = 0;
    key_storage[i].m_num_current_rw_rows = 0;
    key_storage[i].m_rondb_key = 0;
    key_storage[i].m_rec_attr_prev_num_rows = nullptr;
    key_storage[i].m_rec_attr_rondb_key = nullptr;
    key_storage[i].m_key_state = KeyState::NotCompleted;
  }
  if (!setup_metadata(ndb,
                      response,
                      &dict,
                      &tab)) {
    release_del(get_ctrl);
    return;
  }
  Uint32 current_index = 0;
  do {
    Uint32 loop_count = std::min(num_keys - current_index,
                                 (Uint32)MAX_PARALLEL_KEY_OPS);
    int ret_code = del_simple_rows(ndb,
                                   tab,
                                   response,
                                   redis_key_id,
                                   key_storage,
                                   get_ctrl,
                                   loop_count,
                                   current_index);
    if (ret_code != 0) {
      release_del(get_ctrl);
      return;
    }
    DEB_DEL_CMD(("%u keys, %u multi rows, %u completed\n",
                 num_keys,
                 get_ctrl->m_num_keys_multi_rows,
                 get_ctrl->m_num_keys_completed_first_pass));
    /**
     * We have finished the initial round of simple GETs. Now time
     * to handle those that require multi-row GETs. Since we used
     * an optimistic approach we need to start this from scratch
     * again for these new GETs.
     */
    close_finished_transactions(key_storage,
                                get_ctrl,
                                loop_count,
                                current_index);
    assert(get_ctrl->m_num_transactions == 0);
    assert(get_ctrl->m_num_keys_outstanding == 0);
    if (get_ctrl->m_num_keys_multi_rows > 0 &&
        get_ctrl->m_num_keys_failed == 0) {
      int ret_code = del_complex_rows(ndb,
                                      tab,
                                      response,
                                      redis_key_id,
                                      key_storage,
                                      get_ctrl,
                                      loop_count,
                                      current_index);
      if (ret_code != 0) {
        release_del(get_ctrl);
        return;
      }
    }
    current_index += loop_count;
  } while (current_index < num_keys && get_ctrl->m_num_keys_failed == 0);
  /**
   * We are done with the writing process, now it is time to report the
   * result.
   */
  if (get_ctrl->m_num_keys_failed > 0) {
    assign_err_to_response(response,
                           FAILED_EXECUTE_DEL,
                           get_ctrl->m_error_code);
    release_del(get_ctrl);
    return;
  }
  assert(get_ctrl->m_num_keys_requested >= get_ctrl->m_num_read_errors);
  Uint32 deleted_rows =
    get_ctrl->m_num_keys_requested - get_ctrl->m_num_read_errors;
  char buf[20];
  snprintf(buf,
           sizeof(buf),
           ":%u\r\n",
           deleted_rows);
  response->append(&buf[0]);
  release_del(get_ctrl);
  return;
}

void rondb_del_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int worker_id) {
  DEB_DEL_CMD(("DEL command with %lu parameters, first_key: %s\n",
               argv.size(),
               argv[1].c_str()));
  rondb_del(ndb, argv, response, STRING_REDIS_KEY_ID, worker_id);
}

// Phase 1.1 EXISTS / Phase 1.2 TYPE / Phase 1.3 TTL / Phase 1.4 PTTL
// shared probe pipeline. Two-pass batched probe: first string_keys
// (STRING_REDIS_KEY_ID, key), then hset_keys(key) for keys that
// missed pass 1.
//
// Each probe gets its own NDB transaction for parallelism: with
// LM_CommittedRead reads there is no lock-coordination benefit to
// batching keys on a shared trans, and per-key trans can route to
// distinct partitions in parallel - significantly better
// scale-up under MGET-shape workloads. Callbacks are per-trans
// (one per probe) and write directly into probe->m_present /
// m_match_kind based on the trans-level error code.
//
// Per-key state lives in a small heap-allocated array; no
// KeyStorage overload (KeyStorage is much wider than EXISTS needs).
struct probe_drain_state {
  Uint32 m_outstanding;
  int m_error_code;
};

struct exists_probe {
  const char *m_key_str;
  Uint32 m_key_len;
  bool m_present;
  // Phase 1.2: which pass matched. 0 = none, 1 = string, 2 = hash.
  // EXISTS only reads m_present; TYPE consults m_match_kind to
  // emit +string\r\n / +hash\r\n / +none\r\n.
  Uint8 m_match_kind;
  // Per-trans state (one trans per probe per pass; reused across
  // passes for the same probe). m_drain points at the per-pass
  // shared counter so the callback can decrement it.
  NdbTransaction *m_trans;
  bool m_is_pass2;
  struct probe_drain_state *m_drain;
  // Pass 1: PK buffer for string_keys.
  struct key_table m_string_buf;
  // Pass 2: PK buffer + field_count read-back for hset_keys.
  struct hset_key_table m_hset_buf;
};

#define EXISTS_MATCH_NONE   0
#define EXISTS_MATCH_STRING 1
#define EXISTS_MATCH_HASH   2

static int add_exists_string_probe_op(NdbTransaction *trans,
                                      struct exists_probe *probe,
                                      Uint32 database_id,
                                      std::string *response) {
  struct key_table *kr = &probe->m_string_buf;
  kr->null_bits = 0;
  kr->redis_key_id = STRING_REDIS_KEY_ID;
  memcpy(&kr->redis_key[2], probe->m_key_str, probe->m_key_len);
  memset(&kr->redis_key[2 + probe->m_key_len], 0, 3);
  set_length((char*)&kr->redis_key[0], probe->m_key_len);

  // Project all non-PK columns. The same mask prepare_get_key_row
  // uses, so we know it works against the actual std::map iteration
  // order of init_key_records' read_all_column_map. EXISTS / TYPE
  // ignore the projected values; TTL / PTTL read expiry_date from
  // probe->m_string_buf via mi_sint4korr. NDB's NdbRecord readTuple
  // also requires at least one non-PK column projected (mask = 0
  // crashes the worker under LM_CommittedRead).
  const Uint32 mask = 0xFC;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;

  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_ABORTOPTION;
  opts.abortOption = NdbOperation::AO_IgnoreError;
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_BATCH_SAFE_FLAG;

  const NdbOperation *op = trans->readTuple(
    pk_key_record[database_id],
    (const char *)kr,
    entire_key_record[database_id],
    (char *)kr,
    NdbOperation::LM_CommittedRead,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  return 0;
}

static int add_exists_hset_probe_op(NdbTransaction *trans,
                                    struct exists_probe *probe,
                                    Uint32 database_id,
                                    std::string *response) {
  struct hset_key_table *kr = &probe->m_hset_buf;
  kr->null_bits = 0;
  memcpy(&kr->redis_key[2], probe->m_key_str, probe->m_key_len);
  memset(&kr->redis_key[2 + probe->m_key_len], 0, 3);
  set_length(&kr->redis_key[0], probe->m_key_len);
  kr->field_count = 0;

  // Project all non-PK columns of hset_keys (redis_key_id,
  // field_count, expiry_date). Mask 0xE = bits 1,2,3.
  // add_hset_field_count_set_op uses mask 0x4 successfully for
  // field_count, so the std::map iteration order matches source
  // declaration order: bit 0 = redis_key (PK), bit 1 = redis_key_id,
  // bit 2 = field_count, bit 3 = expiry_date.
  const Uint32 mask = 0xE;
  const unsigned char *mask_ptr = (const unsigned char *)&mask;

  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_ABORTOPTION;
  opts.abortOption = NdbOperation::AO_IgnoreError;
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_BATCH_SAFE_FLAG;

  const NdbOperation *op = trans->readTuple(
    pk_hset_key_record[database_id],
    (const char *)kr,
    entire_hset_key_record[database_id],
    (char *)kr,
    NdbOperation::LM_CommittedRead,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (op == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_GET_OP,
                               trans->getNdbError());
    return RONDB_INTERNAL_ERROR;
  }
  return 0;
}

// Per-probe callback: fires once per per-key trans completion.
// Trans-level error code drives the classification:
//   0   -> Pass 1: m_match_kind = STRING, m_present = true.
//          Pass 2: read field_count from buffer; m_match_kind =
//                  HASH and m_present = true only if > 0
//                  (post-1.0.3 cross-server invariant - row may
//                  be present with field_count == 0).
//   626 -> row not found; leave probe untouched (Pass 2 will
//          probe hset_keys for it).
//   any other -> hard failure; record on shared drain state.
static void
probe_callback(int result, NdbTransaction *trans, void *aObject) {
  struct exists_probe *probe = (struct exists_probe*)aObject;
  (void)result;
  int code = trans->getNdbError().code;
  if (code == 0) {
    if (probe->m_is_pass2) {
      if (probe->m_hset_buf.field_count > 0) {
        probe->m_present = true;
        probe->m_match_kind = EXISTS_MATCH_HASH;
      }
    } else {
      probe->m_present = true;
      probe->m_match_kind = EXISTS_MATCH_STRING;
    }
  } else if (code != 626) {
    if (probe->m_drain->m_error_code == 0) {
      probe->m_drain->m_error_code = code;
    }
  }
  assert(probe->m_drain->m_outstanding > 0);
  probe->m_drain->m_outstanding--;
}

// Close every per-probe trans in [start, end). Used both as
// post-drain cleanup and as setup-error-path cleanup. Idempotent
// via m_trans = nullptr after close.
static void close_probe_trans(Ndb *ndb,
                              struct exists_probe *probes,
                              Uint32 start, Uint32 end) {
  for (Uint32 i = start; i < end; i++) {
    if (probes[i].m_trans != nullptr) {
      ndb->closeTransaction(probes[i].m_trans);
      probes[i].m_trans = nullptr;
    }
  }
}

// Phase 1.1 / 1.2 / 1.3 shared two-pass probe pipeline.
// Per-key trans for parallelism: each key opens its own trans
// hinted on its PK, all are submitted async, then drained
// together via execute_ndb. Chunked at MAX_PARALLEL_KEY_OPS to
// cap fan-out per drain cycle.
static int run_exists_probes(Ndb *ndb,
                             struct exists_probe *probes,
                             Uint32 num_keys,
                             Uint32 database_id,
                             const NdbDictionary::Table *string_tab,
                             std::string *response) {
  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *hset_tab = dict->getTable(HSET_KEY_TABLE_NAME);
  if (hset_tab == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TABLE_OBJECT,
                               dict->getNdbError());
    return 1;
  }

  Uint32 idx = 0;
  while (idx < num_keys) {
    Uint32 chunk = std::min(num_keys - idx, (Uint32)MAX_PARALLEL_KEY_OPS);
    Uint32 chunk_end = idx + chunk;

    // Pass 1: per-key trans on string_keys, submitted in parallel.
    struct probe_drain_state drain1;
    drain1.m_outstanding = 0;
    drain1.m_error_code = 0;
    for (Uint32 i = idx; i < chunk_end; i++) {
      struct exists_probe *p = &probes[i];
      struct key_table *kr = &p->m_string_buf;
      kr->null_bits = 0;
      kr->redis_key_id = STRING_REDIS_KEY_ID;
      memcpy(&kr->redis_key[2], p->m_key_str, p->m_key_len);
      memset(&kr->redis_key[2 + p->m_key_len], 0, 3);
      set_length((char*)&kr->redis_key[0], p->m_key_len);

      p->m_trans = ndb->startTransaction(
        string_tab,
        (const char*)&kr->redis_key_id,
        p->m_key_len + 10);
      if (p->m_trans == nullptr) {
        assign_ndb_err_to_response(response,
                                   FAILED_CREATE_TXN_OBJECT,
                                   ndb->getNdbError());
        // Drain anything already in flight, then close all.
        while (drain1.m_outstanding > 0) {
          execute_ndb(ndb, 1, __LINE__);
        }
        close_probe_trans(ndb, probes, idx, chunk_end);
        return 1;
      }
      p->m_is_pass2 = false;
      p->m_drain = &drain1;
      if (add_exists_string_probe_op(p->m_trans, p,
                                     database_id, response) != 0) {
        while (drain1.m_outstanding > 0) {
          execute_ndb(ndb, 1, __LINE__);
        }
        close_probe_trans(ndb, probes, idx, chunk_end);
        return 1;
      }
      drain1.m_outstanding++;
      p->m_trans->executeAsynchPrepare(NdbTransaction::Commit,
                                       &probe_callback,
                                       (void*)p);
    }
    while (drain1.m_outstanding > 0) {
      execute_ndb(ndb, 1, __LINE__);
    }
    close_probe_trans(ndb, probes, idx, chunk_end);
    if (drain1.m_error_code != 0) {
      assign_err_to_response(response,
                             "Pass 1 per-op error",
                             drain1.m_error_code);
      return 1;
    }

    // Pass 2: per-key trans on hset_keys for missers, in parallel.
    struct probe_drain_state drain2;
    drain2.m_outstanding = 0;
    drain2.m_error_code = 0;
    for (Uint32 i = idx; i < chunk_end; i++) {
      struct exists_probe *p = &probes[i];
      if (p->m_present) continue;
      struct hset_key_table *kr = &p->m_hset_buf;
      kr->null_bits = 0;
      memcpy(&kr->redis_key[2], p->m_key_str, p->m_key_len);
      memset(&kr->redis_key[2 + p->m_key_len], 0, 3);
      set_length(&kr->redis_key[0], p->m_key_len);
      kr->field_count = 0;

      p->m_trans = ndb->startTransaction(
        hset_tab,
        (const char*)&kr->redis_key[0],
        p->m_key_len + 2);
      if (p->m_trans == nullptr) {
        assign_ndb_err_to_response(response,
                                   FAILED_CREATE_TXN_OBJECT,
                                   ndb->getNdbError());
        while (drain2.m_outstanding > 0) {
          execute_ndb(ndb, 1, __LINE__);
        }
        close_probe_trans(ndb, probes, idx, chunk_end);
        return 1;
      }
      p->m_is_pass2 = true;
      p->m_drain = &drain2;
      if (add_exists_hset_probe_op(p->m_trans, p,
                                   database_id, response) != 0) {
        while (drain2.m_outstanding > 0) {
          execute_ndb(ndb, 1, __LINE__);
        }
        close_probe_trans(ndb, probes, idx, chunk_end);
        return 1;
      }
      drain2.m_outstanding++;
      p->m_trans->executeAsynchPrepare(NdbTransaction::Commit,
                                       &probe_callback,
                                       (void*)p);
    }
    while (drain2.m_outstanding > 0) {
      execute_ndb(ndb, 1, __LINE__);
    }
    close_probe_trans(ndb, probes, idx, chunk_end);
    if (drain2.m_error_code != 0) {
      assign_err_to_response(response,
                             "Pass 2 per-op error",
                             drain2.m_error_code);
      return 1;
    }
    idx += chunk;
  }
  return 0;
}

void rondb_exists_command(Ndb *ndb,
                          const pink::RedisCmdArgsType &argv,
                          std::string *response,
                          int worker_id) {
  Uint32 num_keys = argv.size() - 1;
  assert(num_keys > 0);
  struct exists_probe *probes = (struct exists_probe*)
    malloc(sizeof(struct exists_probe) * num_keys);
  if (probes == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    return;
  }
  for (Uint32 i = 0; i < num_keys; i++) {
    probes[i].m_key_str = argv[i + 1].c_str();
    probes[i].m_key_len = argv[i + 1].size();
    probes[i].m_present = false;
    probes[i].m_match_kind = EXISTS_MATCH_NONE;
    probes[i].m_trans = nullptr;
  }

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  if (dict == nullptr) {
    assign_ndb_err_to_response(response, FAILED_GET_DICT, ndb->getNdbError());
    free(probes);
    return;
  }
  const NdbDictionary::Table *string_tab = dict->getTable(KEY_TABLE_NAME);
  const NdbDictionary::Table *hset_tab = dict->getTable(HSET_KEY_TABLE_NAME);
  if (string_tab == nullptr || hset_tab == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TABLE_OBJECT,
                               dict->getNdbError());
    free(probes);
    return;
  }
  (void)hset_tab;
  Uint32 database_id = get_current_database(worker_id);
  if (run_exists_probes(ndb, probes, num_keys,
                        database_id, string_tab, response) != 0) {
    free(probes);
    return;
  }
  Uint32 found_count = 0;
  for (Uint32 i = 0; i < num_keys; i++) {
    if (probes[i].m_present) found_count++;
  }
  free(probes);
  char buf[20];
  snprintf(buf, sizeof(buf), ":%u\r\n", found_count);
  response->append(&buf[0]);
}

// Phase 1.2: TYPE key. Single-key two-probe variant of EXISTS.
// Reuses run_exists_probes with num_keys = 1; reply word is
// chosen from probes[0].m_match_kind.
void rondb_type_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id) {
  struct exists_probe probe;
  probe.m_key_str = argv[1].c_str();
  probe.m_key_len = argv[1].size();
  probe.m_present = false;
  probe.m_match_kind = EXISTS_MATCH_NONE;
  probe.m_trans = nullptr;

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  if (dict == nullptr) {
    assign_ndb_err_to_response(response, FAILED_GET_DICT, ndb->getNdbError());
    return;
  }
  const NdbDictionary::Table *string_tab = dict->getTable(KEY_TABLE_NAME);
  const NdbDictionary::Table *hset_tab = dict->getTable(HSET_KEY_TABLE_NAME);
  if (string_tab == nullptr || hset_tab == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TABLE_OBJECT,
                               dict->getNdbError());
    return;
  }
  (void)hset_tab;
  Uint32 database_id = get_current_database(worker_id);
  if (run_exists_probes(ndb, &probe, 1,
                        database_id, string_tab, response) != 0) {
    return;
  }
  switch (probe.m_match_kind) {
    case EXISTS_MATCH_STRING:
      response->append("+string\r\n");
      break;
    case EXISTS_MATCH_HASH:
      response->append("+hash\r\n");
      break;
    default:
      response->append("+none\r\n");
      break;
  }
}

// Phase 1.3 / 1.4: TTL / PTTL key. Reply:
//   :-2\r\n  - key missing OR row expired (expiry_date < now)
//   :-1\r\n  - key has no TTL (expiry_date IS NULL or
//              g_max_expire_at sentinel)
//   :N\r\n   - seconds (TTL) or milliseconds (PTTL) remaining
//
// Single-key variant of EXISTS that additionally reads
// expiry_date from the projected probe buffer. The probe masks
// were widened to include expiry_date (bit 3 in both records),
// so EXISTS / TYPE pay one extra projected column for the shared
// pipeline - cheap. expiry_date is stored in NDB TIMESTAMP format
// (4 bytes); read via mi_sint4korr for portability.
static void rondb_ttl_or_pttl(Ndb *ndb,
                              const pink::RedisCmdArgsType &argv,
                              std::string *response,
                              int worker_id,
                              bool millis) {
  struct exists_probe probe;
  probe.m_key_str = argv[1].c_str();
  probe.m_key_len = argv[1].size();
  probe.m_present = false;
  probe.m_match_kind = EXISTS_MATCH_NONE;
  probe.m_trans = nullptr;

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  if (dict == nullptr) {
    assign_ndb_err_to_response(response, FAILED_GET_DICT, ndb->getNdbError());
    return;
  }
  const NdbDictionary::Table *string_tab = dict->getTable(KEY_TABLE_NAME);
  if (string_tab == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TABLE_OBJECT,
                               dict->getNdbError());
    return;
  }
  Uint32 database_id = get_current_database(worker_id);
  if (run_exists_probes(ndb, &probe, 1,
                        database_id, string_tab, response) != 0) {
    return;
  }

  // Missing key (or hash with field_count == 0): :-2\r\n.
  if (probe.m_match_kind == EXISTS_MATCH_NONE) {
    response->append(":-2\r\n");
    return;
  }
  // Pull expiry_date from the right buffer. Null bit positions
  // differ per init_*_records column_map: string_keys expiry_date
  // = bit 1 of null_bits, hset_keys expiry_date = bit 0. Once the
  // pointer + null check are picked, the mi_sint4korr decode is
  // the same for both tables.
  bool expiry_is_null;
  const Int32 *expiry_ptr;
  if (probe.m_match_kind == EXISTS_MATCH_STRING) {
    expiry_is_null = (probe.m_string_buf.null_bits & 0x2) != 0;
    expiry_ptr = &probe.m_string_buf.expiry_date;
  } else {
    expiry_is_null = (probe.m_hset_buf.null_bits & 0x1) != 0;
    expiry_ptr = &probe.m_hset_buf.expiry_date;
  }
  Int32 expiry_seconds = mi_sint4korr((const unsigned char*)expiry_ptr);
  if (expiry_is_null || expiry_seconds == g_max_expire_at) {
    response->append(":-1\r\n");
    return;
  }
  Int64 now_seconds = (Int64)my_micro_time() / 1000000;
  if (expiry_seconds <= now_seconds) {
    // Expired but not yet GC'd. Phase 1.10 will hide such rows
    // from GET/HGET reads; TTL's reply for the expired-but-present
    // case is decided here.
    response->append(":-2\r\n");
    return;
  }
  Int64 remaining = (Int64)expiry_seconds - now_seconds;
  if (millis) remaining *= 1000;
  char buf[32];
  snprintf(buf, sizeof(buf), ":%lld\r\n", (long long)remaining);
  response->append(&buf[0]);
}

void rondb_ttl_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int worker_id) {
  rondb_ttl_or_pttl(ndb, argv, response, worker_id, /*millis=*/false);
}

void rondb_pttl_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id) {
  rondb_ttl_or_pttl(ndb, argv, response, worker_id, /*millis=*/true);
}

// Phase 1.5: EXPIRE key seconds. Reply :1 if applied, :0 if missing.
// Rejects seconds <= 0 (Redis-canonical, also avoids generate_expire_at's
// -1 sentinel). Probes string_keys then hset_keys via run_exists_probes
// to learn the type, then issues a single interpretedUpdateTuple writing
// expiry_date on the appropriate table.
void rondb_expire_command(Ndb *ndb,
                          const pink::RedisCmdArgsType &argv,
                          std::string *response,
                          int worker_id) {
  Int64 ttl_seconds = 0;
  if (get_int64(argv[2], response, &ttl_seconds) == false) return;
  if (ttl_seconds <= 0) {
    assign_generic_err_to_response(response, REDIS_INVALID_EXPIRE_TIME);
    return;
  }

  struct exists_probe probe;
  probe.m_key_str = argv[1].c_str();
  probe.m_key_len = argv[1].size();
  probe.m_present = false;
  probe.m_match_kind = EXISTS_MATCH_NONE;
  probe.m_trans = nullptr;

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  if (dict == nullptr) {
    assign_ndb_err_to_response(response, FAILED_GET_DICT, ndb->getNdbError());
    return;
  }
  const NdbDictionary::Table *string_tab = dict->getTable(KEY_TABLE_NAME);
  if (string_tab == nullptr) {
    assign_ndb_err_to_response(response,
                               FAILED_CREATE_TABLE_OBJECT,
                               dict->getNdbError());
    return;
  }
  Uint32 database_id = get_current_database(worker_id);
  if (run_exists_probes(ndb, &probe, 1,
                        database_id, string_tab, response) != 0) {
    return;
  }
  if (probe.m_match_kind == EXISTS_MATCH_NONE) {
    response->append(":0\r\n");
    return;
  }

  // Use the same encoding as the SET ... EX write path. m_expire_at
  // ends up holding mi_int4 BE bytes of (now + ttl) in its low 4
  // bytes; the helper assigns to key_row.expiry_date via Int64
  // truncation, which writes those BE bytes verbatim into the
  // buffer at the column's offset. NDB stores them, mi_sint4korr
  // (TTL read path) recovers the native epoch seconds.
  Int64 m_expire_at = 0;
  generate_expire_at(&m_expire_at, ttl_seconds);

  int err;
  if (probe.m_match_kind == EXISTS_MATCH_STRING) {
    err = update_expiry_string_row(ndb,
                                   probe.m_key_str,
                                   probe.m_key_len,
                                   m_expire_at,
                                   database_id,
                                   response);
  } else {
    err = update_expiry_hset_row(ndb,
                                 probe.m_key_str,
                                 probe.m_key_len,
                                 m_expire_at,
                                 database_id,
                                 response);
  }
  if (err == 0) {
    response->append(":1\r\n");
  } else if (err == 626) {
    // Row vanished between probe and update (concurrent DEL /
    // HDEL-of-last-field). Redis-canonical reply for missing key.
    response->append(":0\r\n");
  }
  // Other errors: response already populated by update_expiry_*_row.
}

// Forward decl: set_rows_hdel is defined below set_rows_hset (where it
// can sit next to its mirror) but rondb_hdel_command needs to call it.
static int set_rows_hdel(Ndb *ndb,
                         const NdbDictionary::Table *tab_string_keys,
                         std::string *response,
                         struct KeyStorage *key_storage,
                         struct GetControl *get_ctrl,
                         Uint32 num_fields);

void rondb_hdel_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id) {
  DEB_DEL_CMD(("HDEL command with %lu parameters", argv.size()));
  // Phase 1.0.3: HDEL is a single-trans pipeline that mirrors HSET
  // (Phase 1.0.2d) - one NDB transaction for the whole batch, taking
  // hset_keys(key)'s X-lock as the trans's first op so concurrent
  // HSET / HDEL on the same hash are mutually serialized cluster-
  // wide. argv[0] = "HDEL", argv[1] = hash name, argv[2..] = fields.
  Uint32 num_fields = argv.size() - 2;
  assert(num_fields > 0);
  struct KeyStorage *key_storage = (struct KeyStorage*)
    malloc(sizeof(struct KeyStorage) * num_fields);
  if (key_storage == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    return;
  }
  struct GetControl *get_ctrl = (struct GetControl*)
    malloc(sizeof(struct GetControl));
  if (get_ctrl == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    free(key_storage);
    return;
  }
  get_ctrl->m_ndb = ndb;
  get_ctrl->m_key_store = key_storage;
  get_ctrl->m_value_rows = nullptr;
  get_ctrl->m_next_value_row = 0;
  get_ctrl->m_num_transactions = 0;
  get_ctrl->m_num_keys_requested = num_fields;
  get_ctrl->m_num_keys_outstanding = 0;
  get_ctrl->m_num_bytes_outstanding = 0;
  get_ctrl->m_num_keys_completed_first_pass = 0;
  get_ctrl->m_num_keys_multi_rows = 0;
  get_ctrl->m_num_keys_failed = 0;
  get_ctrl->m_num_new_fields = 0;
  get_ctrl->m_num_read_errors = 0;
  get_ctrl->m_error_code = 0;
  get_ctrl->m_is_set_command = false;
  get_ctrl->m_get_cmd_part = false;
  get_ctrl->m_worker_id = worker_id;
  get_ctrl->m_database_id = get_current_database(worker_id);
  get_ctrl->m_hash_name_ptr = argv[1].c_str();
  get_ctrl->m_hash_name_len = argv[1].size();
  get_ctrl->m_hset_key_tab = nullptr;
  get_ctrl->m_hset_prealloc_id = 0;
  get_ctrl->m_hset_redis_key_id = 0;
  get_ctrl->m_hset_field_count_pre = 0;
  get_ctrl->m_rec_attr_hset_id = nullptr;
  get_ctrl->m_rec_attr_hset_field_count = nullptr;
  get_ctrl->m_hset_phase_chunk_start = 0;
  get_ctrl->m_hset_phase_chunk_count = 0;
  get_ctrl->m_num_deleted_fields = 0;
  get_ctrl->m_hdel_phase1_op = nullptr;
  for (Uint32 i = 0; i < num_fields; i++) {
    key_storage[i].m_index = i;
    key_storage[i].m_close_flag = false;
    key_storage[i].m_get_ctrl = get_ctrl;
    key_storage[i].m_trans = nullptr;
    key_storage[i].m_key_str = argv[i + 2].c_str();
    key_storage[i].m_key_len = argv[i + 2].size();
    key_storage[i].m_value_ptr = nullptr;
    key_storage[i].m_get_value_size = 0;
    key_storage[i].m_set_value_size = 0;
    key_storage[i].m_header_len = 0;
    key_storage[i].m_first_value_row = 0;
    key_storage[i].m_current_pos = 0;
    key_storage[i].m_num_rows = 0;
    key_storage[i].m_num_rw_rows = 0;
    key_storage[i].m_num_current_rw_rows = 0;
    key_storage[i].m_rondb_key = 0;
    key_storage[i].m_rec_attr_prev_num_rows = nullptr;
    key_storage[i].m_rec_attr_rondb_key = nullptr;
    key_storage[i].m_rec_attr_expiry_date = nullptr;
    key_storage[i].m_rec_attr_new_field = nullptr;
    key_storage[i].m_hdel_phase2_op = nullptr;
    key_storage[i].m_hdel_field_present = false;
    key_storage[i].m_key_state = KeyState::NotCompleted;
  }

  const NdbDictionary::Dictionary *dict;
  const NdbDictionary::Table *tab_string_keys = nullptr;
  if (!setup_metadata(ndb, response, &dict, &tab_string_keys)) {
    release_del(get_ctrl);
    return;
  }
  const NdbDictionary::Table *hset_tab =
    dict->getTable(HSET_KEY_TABLE_NAME);
  if (hset_tab == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to get hset_keys table",
                               dict->getNdbError());
    release_del(get_ctrl);
    return;
  }
  get_ctrl->m_hset_key_tab = hset_tab;

  int ret_code = set_rows_hdel(ndb,
                               tab_string_keys,
                               response,
                               key_storage,
                               get_ctrl,
                               num_fields);
  if (ret_code != 0) {
    release_del(get_ctrl);
    return;
  }
  if (get_ctrl->m_num_keys_failed > 0) {
    assign_err_to_response(response,
                           FAILED_EXECUTE_DEL,
                           get_ctrl->m_error_code);
    release_del(get_ctrl);
    return;
  }
  // Reply is the count of fields actually deleted (Redis-canonical
  // HDEL semantics). Missing-hash case lands here with
  // m_num_deleted_fields == 0 -> :0\r\n.
  char buf[20];
  snprintf(buf, sizeof(buf), ":%u\r\n", get_ctrl->m_num_deleted_fields);
  response->append(&buf[0]);
  release_del(get_ctrl);
}

/**
 * SET MODULE
 * ----------
 */
static int send_delete_write(std::string *response,
                             struct KeyStorage *key_store,
                             struct GetControl *get_ctrl) {
  assert(key_store->m_num_rows < key_store->m_prev_num_rows);
  for (Uint32 i = key_store->m_num_rows;
       i < key_store->m_prev_num_rows;
       i++) {
    int ret_code = prepare_delete_value_row(response,
                                            key_store,
                                            i,
                                            get_ctrl->m_database_id);
    if (ret_code != 0) return 1;
  }
  Uint32 num_delete_rows = key_store->m_prev_num_rows - key_store->m_num_rows;
  Uint32 bytes_outstanding = DELETE_BYTES * num_delete_rows;
  get_ctrl->m_num_bytes_outstanding += bytes_outstanding;
  get_ctrl->m_num_keys_outstanding++;
  commit_write_value_transaction(key_store);
  key_store->m_key_state = KeyState::MultiRowRWAll;
  DEB_MSET_CMD(("Prepare send value delete: Key %u, prev rows: %u"
                ", num_rows: %u, key_state: %u\n",
                key_store->m_index,
                key_store->m_prev_num_rows,
                key_store->m_num_rows,
                key_store->m_key_state));
  return 0;
}

static int send_value_write(std::string *response,
                            struct KeyStorage *key_store,
                            struct GetControl *get_ctrl) {
  Uint32 i = 0;
  do {
    int ret_code = prepare_set_value_row(response,
                                         key_store);
    if (ret_code != 0) return 1;
    i++;
  } while (i < MAX_PARALLEL_VALUE_RWS &&
           key_store->m_num_rw_rows < key_store->m_num_rows);
  key_store->m_num_current_rw_rows = i;
  get_ctrl->m_num_bytes_outstanding += i * sizeof(struct value_table);
  get_ctrl->m_num_keys_outstanding++;
  if (key_store->m_num_rw_rows == key_store->m_num_rows &&
    key_store->m_prev_num_rows <= key_store->m_num_rows) {
    commit_write_value_transaction(key_store);
    key_store->m_key_state = KeyState::MultiRowRWAll;
  } else {
    prepare_write_value_transaction(key_store);
    key_store->m_key_state = KeyState::MultiRowRWValueSent;
  }
  DEB_MSET_CMD(("Prepare send value write: Key %u, rw rows: %u"
                ", num_rows: %u, num_rw_rows: %u, key_state: %u\n",
                key_store->m_index,
                key_store->m_num_current_rw_rows,
                key_store->m_num_rows,
                key_store->m_num_rw_rows,
                key_store->m_key_state));
  return 0;
}

static int send_next_write_batch(std::string *response,
                                 struct KeyStorage *key_storage,
                                 struct GetControl *get_ctrl,
                                 Uint32 current_index,
                                 Uint32 loop_count,
                                 Uint32 &current_finished) {
  if (get_ctrl->m_num_keys_multi_rows == 0) {
    DEB_DEL_CMD(("No multi rows left, Line: %u\n", __LINE__));
    return 0;
  }
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    if (get_ctrl->m_num_bytes_outstanding > MAX_OUTSTANDING_BYTES) {
      assert(get_ctrl->m_num_keys_outstanding > 0);
      return 0;
    }
    if (key_storage[inx].m_key_state == KeyState::MultiRowRWValue) {
      if (key_storage[inx].m_num_rows == 0 &&
        key_storage[inx].m_prev_num_rows == 0) {
        get_ctrl->m_num_keys_outstanding++;
        commit_write_value_transaction(&key_storage[inx]);
        assert(current_finished > 0);
        current_finished--;
        DEB_DEL_CMD(("Commit with no value rows"));
        key_storage[inx].m_key_state = KeyState::MultiRowRWAll;
        continue;
      }
      if (key_storage[inx].m_num_rows > 0 &&
          key_storage[inx].m_num_rows > key_storage[inx].m_num_rw_rows) {
        int ret_code = send_value_write(response,
                                        &key_storage[inx],
                                        get_ctrl);
        if (ret_code != 0) return 1;
        assert(current_finished > 0);
        current_finished--;
      } else {
        int ret_code = send_delete_write(response,
                                         &key_storage[inx],
                                         get_ctrl);
        if (ret_code != 0) return 1;
        assert(current_finished > 0);
        current_finished--;
      }
    }
  }
  return 0;
}

// Unified HSET / MSET / SET write pipeline. All keys go through:
//   Phase A - submit NoCommit on string_keys via the complex
//             interpreter (write_key_row_no_commit).
//   Phase B - drain the NoCommit callbacks; every successful key
//             lands in MultiRowRWValue with its trans still open.
//   Phase C - dispatch: fast-path commit for inline-only keys
//             (m_num_rows == 0 && m_prev_num_rows == 0), or queue
//             extension-row writes / deletes for keys that need
//             them, via send_next_write_batch.
//   Phase D - drain terminal commit callbacks (with intermediate
//             dispatch rounds for keys that needed ext-row writes).
//
// Replaces the old split of set_simple_rows + set_complex_rows with
// their simple-path abort-and-retry-via-6000 pattern. The split was
// unsafe after the interpreter-less pre-filter came out, because
// write_data_to_key_op silently truncates values > INLINE_VALUE_LEN
// into value_start without the interpreter catching the mismatch;
// unifying on the complex interpreter closes that hole.
static int set_rows(Ndb *ndb,
                    const NdbDictionary::Table *tab,
                    std::string *response,
                    Uint64 redis_key_id,
                    struct KeyStorage *key_storage,
                    struct GetControl *get_ctrl,
                    Uint32 loop_count,
                    Uint32 current_index) {
  // Phase A - submit NoCommit for every key.
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    key_storage[inx].m_rondb_key = 0;
    Uint32 value_len = key_storage[inx].m_set_value_size;
    if (value_len > INLINE_VALUE_LEN) {
      // Compute extension-row layout and allocate a fresh rondb_key
      // so write_data_to_key_op writes the correct num_rows and so
      // send_next_write_batch picks the ext-rows branch instead of
      // the inline fast-path commit.
      Uint32 extended_value_len = value_len - INLINE_VALUE_LEN;
      Uint32 num_value_rows = extended_value_len / EXTENSION_VALUE_LEN;
      if (extended_value_len % EXTENSION_VALUE_LEN != 0) {
        num_value_rows++;
      }
      if (rondb_get_rondb_key(tab,
                              key_storage[inx].m_rondb_key,
                              ndb,
                              response) != 0) {
        return 1;
      }
      key_storage[inx].m_num_rows = num_value_rows;
      DEB_HSET_KEY(("key %u large value, num_rows: %u, rondb_key: %llu\n",
        inx, num_value_rows, key_storage[inx].m_rondb_key));
    }
    if (!(get_ctrl->m_get_cmd_part &&
          key_storage[inx].m_trans != nullptr)) {
      // Either no get-phase (plain SET / MSET) or the get-phase read
      // closed the transaction because the key did not exist
      // (SET .. GET on a non-existent key - C9). In both cases we
      // need a fresh transaction for the write.
      if (!setup_one_transaction(ndb,
                                 response,
                                 redis_key_id,
                                 &key_storage[inx],
                                 tab)) {
        return 1;
      }
      get_ctrl->m_num_transactions++;
    }
    Uint32 row_state = 0;
    int ret_code = write_data_to_key_op(response,
                                        tab,
                                        &key_storage[inx],
                                        redis_key_id,
                                        row_state,
                                        get_ctrl->m_database_id);
    if (ret_code != 0) {
      return 1;
    }
    prepare_write_transaction(&key_storage[inx]);
  }

  // Phase B - drain NoCommit callbacks. write_callback decrements
  // m_num_keys_outstanding on both success and error paths.
  get_ctrl->m_num_keys_outstanding = loop_count;
  Uint32 current_finished_in_loop = 0;
  do {
    int min_finished = 1;
    int finished = execute_ndb(ndb, min_finished, __LINE__);
    assert(finished >= 0);
    current_finished_in_loop += finished;
  } while (current_finished_in_loop < loop_count);

  if (get_ctrl->m_num_keys_failed > 0) return 0;

  // Phase C - count dispatchable keys. Error keys (CompletedFailed,
  // CompletedConditionalFail) stay on the side; only MultiRowRWValue
  // keys need a commit or ext-row write.
  Uint32 num_dispatch = 0;
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    if (key_storage[inx].m_key_state == KeyState::MultiRowRWValue) {
      num_dispatch++;
    }
  }
  get_ctrl->m_num_keys_multi_rows = num_dispatch;
  if (num_dispatch == 0) {
    return 0;
  }
  get_ctrl->m_num_keys_outstanding = num_dispatch;
  get_ctrl->m_num_bytes_outstanding =
    num_dispatch * (sizeof(struct key_table) - MAX_KEY_VALUE_LEN);
  // Credit the NoCommit callbacks that already fired in Phase B
  // (send_next_write_batch decrements this counter for each key it
  // dispatches). Without the credit the dispatch-consumes-credit
  // assertion inside send_next_write_batch would trip.
  current_finished_in_loop = num_dispatch;

  // First dispatch sweep: queue commit / ext-write ops on each
  // MultiRowRWValue trans so Phase D's execute_ndb has pending
  // callbacks to wait for.
  int ret_code = send_next_write_batch(response,
                                       key_storage,
                                       get_ctrl,
                                       current_index,
                                       loop_count,
                                       current_finished_in_loop);
  if (ret_code != 0) return 1;

  // Phase D - drain terminal callbacks. Ext-row paths loop through
  // extra dispatch rounds until their final commit fires.
  do {
    DEB_MSET_CMD(("Call sendPollNdb with %u keys, %u keys out and %u bytes"
                  " out, current_finished_in_loop: %u\n",
                  get_ctrl->m_num_keys_multi_rows,
                  get_ctrl->m_num_keys_outstanding,
                  get_ctrl->m_num_bytes_outstanding,
                  current_finished_in_loop));
    int min_finished = 1;
    int finished = execute_ndb(ndb, min_finished, __LINE__);
    assert(finished >= 0);
    current_finished_in_loop += finished;
    if (get_ctrl->m_num_keys_failed > 0) return 0;
    ret_code = send_next_write_batch(response,
                                     key_storage,
                                     get_ctrl,
                                     current_index,
                                     loop_count,
                                     current_finished_in_loop);
    if (ret_code != 0) return 1;
  } while (current_finished_in_loop < num_dispatch);
  return 0;
}

// Phase 1.0.2d single-trans HSET / HMSET pipeline. One NDB
// transaction per HSET command:
//   Phase 1 (NoCommit, 1 op) - writeTuple hset_keys(key) with
//     init_hset_lock_claim_code; takes X-lock + reads or writes
//     redis_key_id and field_count. Drains.
//   Phase 2 (NoCommit, N ops) - writeTuple per field on string_keys
//     with the existing complex interpreter. All on the shared
//     trans. Drains. Field-row locks acquired only after Phase 1's
//     X-lock is fully held in the cluster, so no cross-LDM deadlock
//     with concurrent HSET / HDEL on the same hash.
//   Phase 3 (Commit, with optional ext-row ops + field_count update
//     folded into the same submission) - one round-trip.
//
// Phase 1.0.2e: large values (each field's value > INLINE_VALUE_LEN)
// are supported by staging ext-row writes/deletes on the same trans
// in Phase 3 alongside the field_count update.
static int set_rows_hset(Ndb *ndb,
                         const NdbDictionary::Table *tab_string_keys,
                         std::string *response,
                         struct KeyStorage *key_storage,
                         struct GetControl *get_ctrl,
                         Uint32 num_fields) {
  Uint64 prealloc_id = 0;
  if (ndb->getAutoIncrementValue(get_ctrl->m_hset_key_tab,
                                 prealloc_id, unsigned(1024)) != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to pre-allocate redis_key_id",
                               ndb->getNdbError());
    return 1;
  }
  get_ctrl->m_hset_prealloc_id = prealloc_id;

  // Open trans hinted at hset_keys(key) so the TC lives on that
  // partition's data node; the PK we're about to lock is on the
  // same partition.
  struct hset_key_table hset_pk_buf;
  hset_pk_buf.null_bits = 0;
  memcpy(&hset_pk_buf.redis_key[2],
         get_ctrl->m_hash_name_ptr,
         get_ctrl->m_hash_name_len);
  set_length(&hset_pk_buf.redis_key[0], get_ctrl->m_hash_name_len);
  NdbTransaction *trans = ndb->startTransaction(
    get_ctrl->m_hset_key_tab,
    (const char*)&hset_pk_buf.redis_key[0],
    get_ctrl->m_hash_name_len + 2);
  if (trans == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to start HSET transaction",
                               ndb->getNdbError());
    return 1;
  }
  // Every key_storage[i] shares this trans for the duration of the
  // HSET. write_data_to_key_op queues against key_store->m_trans,
  // so this assignment is enough to make all field writes land on
  // the same trans without modifying the helper. The aliases are
  // nulled at every exit so close_transactions doesn't double-close
  // the shared handle.
  for (Uint32 i = 0; i < num_fields; i++) {
    key_storage[i].m_trans = trans;
  }
  get_ctrl->m_num_transactions++;
  // Local lambda emulation: a label-based dealias-and-return helper
  // is awkward in C++; use a small block at every return site
  // instead. Pattern: dealias_and_return = goto-style with a flag.
  bool dealias_pending = true;
#define HSET_DEALIAS_RETURN(rc) do {                                  \
    if (dealias_pending) {                                            \
      for (Uint32 _i = 1; _i < num_fields; _i++) {                    \
        key_storage[_i].m_trans = nullptr;                            \
      }                                                               \
      dealias_pending = false;                                        \
    }                                                                 \
    return (rc);                                                      \
  } while (0)

  // Phase 1: lock claim + redis_key_id / field_count read.
  if (add_hset_lock_claim_op(trans,
                             get_ctrl->m_hset_key_tab,
                             get_ctrl->m_hash_name_ptr,
                             get_ctrl->m_hash_name_len,
                             prealloc_id,
                             get_ctrl,
                             get_ctrl->m_database_id,
                             response) != 0) {
    HSET_DEALIAS_RETURN(1);
  }
  get_ctrl->m_num_keys_outstanding = 1;
  prepare_hset_phase1_transaction(get_ctrl, trans);
  while (get_ctrl->m_num_keys_outstanding > 0) {
    int finished = execute_ndb(ndb, 1, __LINE__);
    assert(finished >= 0);
  }
  if (get_ctrl->m_num_keys_failed > 0) {
    HSET_DEALIAS_RETURN(0);
  }

  // Phase 2: chunked field writes. An HSET of N fields submits N
  // writeTuples on the shared trans. To avoid overrunning NDB's
  // per-trans op buffer for very large N, break the field set into
  // chunks of MAX_PARALLEL_KEY_OPS and submit a NoCommit per chunk.
  // Pre-compute the extension-row layout for fields that overflow
  // value_start so the complex interpreter writes the correct
  // num_rows and so Phase 3 knows how many ext rows to stage.
  Uint32 phase2_idx = 0;
  while (phase2_idx < num_fields) {
    Uint32 chunk_count =
      std::min(num_fields - phase2_idx, (Uint32)MAX_PARALLEL_KEY_OPS);
    for (Uint32 i = phase2_idx; i < phase2_idx + chunk_count; i++) {
      key_storage[i].m_rondb_key = 0;
      Uint32 value_len = key_storage[i].m_set_value_size;
      if (value_len > INLINE_VALUE_LEN) {
        Uint32 extended_value_len = value_len - INLINE_VALUE_LEN;
        Uint32 num_value_rows = extended_value_len / EXTENSION_VALUE_LEN;
        if (extended_value_len % EXTENSION_VALUE_LEN != 0) {
          num_value_rows++;
        }
        if (rondb_get_rondb_key(tab_string_keys,
                                key_storage[i].m_rondb_key,
                                ndb,
                                response) != 0) {
          HSET_DEALIAS_RETURN(1);
        }
        key_storage[i].m_num_rows = num_value_rows;
      }
      int wret = write_data_to_key_op(response,
                                      tab_string_keys,
                                      &key_storage[i],
                                      get_ctrl->m_hset_redis_key_id,
                                      /* row_state */ 0,
                                      get_ctrl->m_database_id);
      if (wret != 0) {
        HSET_DEALIAS_RETURN(1);
      }
    }
    get_ctrl->m_hset_phase_chunk_start = phase2_idx;
    get_ctrl->m_hset_phase_chunk_count = chunk_count;
    get_ctrl->m_num_keys_outstanding = 1;
    prepare_hset_phase2_transaction(get_ctrl, trans);
    while (get_ctrl->m_num_keys_outstanding > 0) {
      int finished = execute_ndb(ndb, 1, __LINE__);
      assert(finished >= 0);
    }
    if (get_ctrl->m_num_keys_failed > 0) {
      HSET_DEALIAS_RETURN(0);
    }
    phase2_idx += chunk_count;
  }

  // Phase 3: chunked ext-row writes / deletes, then Commit folding
  // in the field_count update. Per field, after Phase 2's callback
  // populated m_prev_num_rows / m_rondb_key:
  //   m_num_rows > 0       -> write ordinals 0..m_num_rows-1,
  //                            overwriting any existing ext rows.
  //   m_prev_num_rows >    -> delete surplus ordinals
  //     m_num_rows             m_num_rows..m_prev_num_rows-1.
  // Tally ops as we queue them; when a chunk fills
  // MAX_PARALLEL_KEY_OPS, submit NoCommit + drain. The final
  // submission is a Commit that also carries the field_count
  // updateTuple.
  Uint32 ops_in_chunk = 0;
  for (Uint32 i = 0; i < num_fields; i++) {
    struct KeyStorage *ks = &key_storage[i];
    while (ks->m_num_rw_rows < ks->m_num_rows) {
      if (prepare_set_value_row(response, ks) != 0) {
        HSET_DEALIAS_RETURN(1);
      }
      if (++ops_in_chunk >= MAX_PARALLEL_KEY_OPS) {
        get_ctrl->m_num_keys_outstanding = 1;
        prepare_hset_phase_chunk_transaction(get_ctrl, trans);
        while (get_ctrl->m_num_keys_outstanding > 0) {
          int finished = execute_ndb(ndb, 1, __LINE__);
          assert(finished >= 0);
        }
        if (get_ctrl->m_num_keys_failed > 0) {
          HSET_DEALIAS_RETURN(0);
        }
        ops_in_chunk = 0;
      }
    }
    for (Uint32 ord = ks->m_num_rows; ord < ks->m_prev_num_rows; ord++) {
      if (prepare_delete_value_row(response,
                                   ks,
                                   ord,
                                   get_ctrl->m_database_id) != 0) {
        HSET_DEALIAS_RETURN(1);
      }
      if (++ops_in_chunk >= MAX_PARALLEL_KEY_OPS) {
        get_ctrl->m_num_keys_outstanding = 1;
        prepare_hset_phase_chunk_transaction(get_ctrl, trans);
        while (get_ctrl->m_num_keys_outstanding > 0) {
          int finished = execute_ndb(ndb, 1, __LINE__);
          assert(finished >= 0);
        }
        if (get_ctrl->m_num_keys_failed > 0) {
          HSET_DEALIAS_RETURN(0);
        }
        ops_in_chunk = 0;
      }
    }
  }
  if (get_ctrl->m_num_new_fields > 0) {
    Uint32 new_count = get_ctrl->m_hset_field_count_pre +
                       get_ctrl->m_num_new_fields;
    if (add_hset_field_count_set_op(trans,
                                    get_ctrl->m_hset_key_tab,
                                    get_ctrl->m_hash_name_ptr,
                                    get_ctrl->m_hash_name_len,
                                    new_count,
                                    get_ctrl->m_database_id,
                                    response) != 0) {
      HSET_DEALIAS_RETURN(1);
    }
  }
  get_ctrl->m_num_keys_outstanding = 1;
  prepare_hset_phase3_transaction(get_ctrl, trans);
  while (get_ctrl->m_num_keys_outstanding > 0) {
    int finished = execute_ndb(ndb, 1, __LINE__);
    assert(finished >= 0);
  }
  HSET_DEALIAS_RETURN(0);
#undef HSET_DEALIAS_RETURN
}

// Phase 1.0.3 single-trans HDEL state machine. Mirrors set_rows_hset's
// three-phase shape on a single shared NdbTransaction:
//   Phase 1: X-locked readTuple on hset_keys(key) - no INSERT side
//            effect on miss; hash-not-found is a valid result that
//            short-circuits to :0\r\n.
//   Phase 2: per-field deleteTuple on string_keys with read-back of
//            num_rows + rondb_key (NDB's deleteTuple takes a
//            result_record / result_mask just like readTuple, so
//            the row's pre-delete columns project into m_key_row
//            before the staged delete applies). AO_IgnoreError
//            per-op so 626 on a missing field does not abort the
//            trans; the callback classifies each field as inline /
//            ext-row / missing. Chunked by MAX_PARALLEL_KEY_OPS.
//   Phase 3: per-ordinal deleteTuple on string_values for fields
//            that had overflow rows, plus
//            add_hset_field_count_bump_op(delta = -deleted_count)
//            folded into the Commit. The string_keys deletes from
//            Phase 2 are still held under NoCommit and apply
//            atomically at this Commit. Chunked when ops fill the
//            per-trans op buffer.
//
// Returns 0 on success or "expected failure" (caller inspects
// get_ctrl->m_num_keys_failed and m_hset_redis_key_id to decide
// the reply); 1 on hard failure.
static int set_rows_hdel(Ndb *ndb,
                         const NdbDictionary::Table *tab_string_keys,
                         std::string *response,
                         struct KeyStorage *key_storage,
                         struct GetControl *get_ctrl,
                         Uint32 num_fields) {
  struct hset_key_table hset_pk_buf;
  hset_pk_buf.null_bits = 0;
  memcpy(&hset_pk_buf.redis_key[2],
         get_ctrl->m_hash_name_ptr,
         get_ctrl->m_hash_name_len);
  set_length(&hset_pk_buf.redis_key[0], get_ctrl->m_hash_name_len);
  NdbTransaction *trans = ndb->startTransaction(
    get_ctrl->m_hset_key_tab,
    (const char*)&hset_pk_buf.redis_key[0],
    get_ctrl->m_hash_name_len + 2);
  if (trans == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to start HDEL transaction",
                               ndb->getNdbError());
    return 1;
  }
  for (Uint32 i = 0; i < num_fields; i++) {
    key_storage[i].m_trans = trans;
  }
  get_ctrl->m_num_transactions++;
  get_ctrl->m_hdel_phase1_op = nullptr;
  get_ctrl->m_num_deleted_fields = 0;

  // Same dealias-and-return pattern as set_rows_hset: every key
  // shares one trans, so close_finished_transactions must not
  // double-close. Null out the aliases at every exit.
  bool dealias_pending = true;
#define HDEL_DEALIAS_RETURN(rc) do {                                    \
    if (dealias_pending) {                                              \
      for (Uint32 _i = 1; _i < num_fields; _i++) {                      \
        key_storage[_i].m_trans = nullptr;                              \
      }                                                                 \
      dealias_pending = false;                                          \
    }                                                                   \
    return (rc);                                                        \
  } while (0)

  // Phase 1: X-locked read on hset_keys(key).
  if (add_hdel_lock_read_op(trans,
                            get_ctrl->m_hset_key_tab,
                            get_ctrl->m_hash_name_ptr,
                            get_ctrl->m_hash_name_len,
                            get_ctrl,
                            get_ctrl->m_database_id,
                            response) != 0) {
    HDEL_DEALIAS_RETURN(1);
  }
  get_ctrl->m_num_keys_outstanding = 1;
  prepare_hdel_phase1_transaction(get_ctrl, trans);
  while (get_ctrl->m_num_keys_outstanding > 0) {
    int finished = execute_ndb(ndb, 1, __LINE__);
    assert(finished >= 0);
  }
  // Hash row didn't exist - nothing to delete, no field_count to
  // touch, no Commit needed. The trans aborts cleanly when closed.
  if (get_ctrl->m_hset_redis_key_id == 0) {
    HDEL_DEALIAS_RETURN(0);
  }

  // Phase 2: chunked per-field probes. Set up each key's m_key_row
  // PK (redis_key_id from Phase 1's read + per-field name), queue a
  // readTuple, submit NoCommit, drain. The classifier callback
  // populates m_hdel_field_present + m_num_rows + m_rondb_key per
  // field and increments m_num_deleted_fields.
  Uint32 phase2_idx = 0;
  while (phase2_idx < num_fields) {
    Uint32 chunk_count =
      std::min(num_fields - phase2_idx, (Uint32)MAX_PARALLEL_KEY_OPS);
    for (Uint32 i = phase2_idx; i < phase2_idx + chunk_count; i++) {
      struct key_table *kr = &key_storage[i].m_key_row;
      kr->null_bits = 0;
      kr->redis_key_id = get_ctrl->m_hset_redis_key_id;
      memcpy(&kr->redis_key[2],
             key_storage[i].m_key_str,
             key_storage[i].m_key_len);
      memset(&kr->redis_key[2 + key_storage[i].m_key_len], 0, 3);
      set_length((char*)&kr->redis_key[0], key_storage[i].m_key_len);
      if (add_hdel_field_delete_op(&key_storage[i],
                                   get_ctrl->m_database_id,
                                   response) != 0) {
        HDEL_DEALIAS_RETURN(1);
      }
    }
    get_ctrl->m_hset_phase_chunk_start = phase2_idx;
    get_ctrl->m_hset_phase_chunk_count = chunk_count;
    get_ctrl->m_num_keys_outstanding = 1;
    prepare_hdel_phase2_transaction(get_ctrl, trans);
    while (get_ctrl->m_num_keys_outstanding > 0) {
      int finished = execute_ndb(ndb, 1, __LINE__);
      assert(finished >= 0);
    }
    if (get_ctrl->m_num_keys_failed > 0) {
      HDEL_DEALIAS_RETURN(0);
    }
    phase2_idx += chunk_count;
  }

  // Phase 3: Phase 2 already staged the per-field deleteTuples on
  // string_keys (they're held under NoCommit and apply at this
  // Commit). All Phase 3 has to do is queue per-ordinal deleteTuples
  // on string_values for fields with overflow, and fold the
  // field_count bump into the Commit op. Chunk via the existing
  // hset_phase_chunk helper when ops_in_chunk fills the per-trans
  // buffer.
  (void)tab_string_keys;
  Uint32 ops_in_chunk = 0;
  for (Uint32 i = 0; i < num_fields; i++) {
    struct KeyStorage *ks = &key_storage[i];
    if (!ks->m_hdel_field_present) continue;
    for (Uint32 ord = 0; ord < ks->m_num_rows; ord++) {
      if (prepare_delete_value_row(response,
                                   ks,
                                   ord,
                                   get_ctrl->m_database_id) != 0) {
        HDEL_DEALIAS_RETURN(1);
      }
      if (++ops_in_chunk >= MAX_PARALLEL_KEY_OPS) {
        get_ctrl->m_num_keys_outstanding = 1;
        prepare_hset_phase_chunk_transaction(get_ctrl, trans);
        while (get_ctrl->m_num_keys_outstanding > 0) {
          int finished = execute_ndb(ndb, 1, __LINE__);
          assert(finished >= 0);
        }
        if (get_ctrl->m_num_keys_failed > 0) {
          HDEL_DEALIAS_RETURN(0);
        }
        ops_in_chunk = 0;
      }
    }
  }
  if (get_ctrl->m_num_deleted_fields > 0) {
    Int64 delta = -(Int64)get_ctrl->m_num_deleted_fields;
    if (add_hset_field_count_bump_op(trans,
                                     get_ctrl->m_hset_key_tab,
                                     get_ctrl->m_hash_name_ptr,
                                     get_ctrl->m_hash_name_len,
                                     delta,
                                     get_ctrl->m_database_id,
                                     response) != 0) {
      HDEL_DEALIAS_RETURN(1);
    }
  }
  get_ctrl->m_num_keys_outstanding = 1;
  prepare_hdel_phase3_transaction(get_ctrl, trans);
  while (get_ctrl->m_num_keys_outstanding > 0) {
    int finished = execute_ndb(ndb, 1, __LINE__);
    assert(finished >= 0);
  }
  HDEL_DEALIAS_RETURN(0);
#undef HDEL_DEALIAS_RETURN
}

static
void rondb_mset(Ndb *ndb,
               const pink::RedisCmdArgsType &argv,
               std::string *response,
               bool is_hmset,
               Uint64 redis_key_id,
               bool set_command,
               int worker_id) {
  Int64 ttl = -1;
  bool keep_ttl = false;
  Uint32 num_keys = 2;
  enum SetType set_type = IsWrite;
  bool set_ttl = false;
  bool get_cmd_part = false;
  const char *empty_str = "";
  size_t arg_size = argv.size();
  Uint32 arg_index_start = (redis_key_id == STRING_REDIS_KEY_ID) ? 1 : 2;
  if (set_command &&
      redis_key_id == STRING_REDIS_KEY_ID &&
      arg_size > 3) {
    Uint32 arg_index = 3;
    const char *arg = argv[arg_index].c_str();
    if (strcasecmp(arg, "nx") == 0) {
      set_type = IsInsert;
      arg_index++;
      arg = empty_str;
    } else if (strcasecmp(arg, "xx") == 0) {
      set_type = IsUpdate;
      arg_index++;
      arg = empty_str;
    }
    if (arg == empty_str && arg_index < arg_size) {
      arg = argv[arg_index].c_str();
    }
    if (strcasecmp(arg, "get") == 0) {
      arg_index++;
      arg = empty_str;
      get_cmd_part = true;
    }
    if (arg == empty_str && arg_index < (arg_size + 1)) {
      arg = argv[arg_index].c_str();
    }
    if (strcasecmp(arg, "ex") == 0 && argv.size() > (arg_index + 1)) {
      set_ttl = true;
      std::string opt_val = argv[arg_index + 1];
      if (get_int64(opt_val, response, &ttl) == false) return;
      if (ttl <= 0) {
        // Redis-canonical for EX <= 0 (C12). Also avoids collision
        // with generate_expire_at's ttl==-1 "never expires" sentinel.
        assign_generic_err_to_response(response, REDIS_INVALID_EXPIRE_TIME);
        return;
      }
      arg_index += 2;
      DEB_TTL(("ex: ttl: %lld\n", ttl));
    } else if (strcasecmp(arg, "px") == 0 && argv.size() > (arg_index + 1)) {
      set_ttl = true;
      std::string opt_val = argv[arg_index + 1];
      if (get_int64(opt_val, response, &ttl) == false) return;
      if (ttl <= 0) {
        // Reject before the ceil-division below (PX 0 / PX -1 would
        // otherwise survive conversion or collide with the sentinel).
        assign_generic_err_to_response(response, REDIS_INVALID_EXPIRE_TIME);
        return;
      }
      //Convert to seconds
      ttl = (ttl + 999) / 1000;
      arg_index += 2;
      DEB_TTL(("px: ttl: %lld\n", ttl));
    } else if (strcasecmp(arg, "exat") == 0 && argv.size() > (arg_index + 1)) {
      set_ttl = true;
      Int64 now = get_current_unix_time();
      std::string opt_val = argv[arg_index + 1];
      if (get_int64(opt_val, response, &ttl) == false) return;
      if (now >= ttl) {
        /* Already expired */
        ttl = 0;
      } else {
        ttl -= now;
      }
      DEB_TTL(("exat: ttl: %lld\n", ttl));
      arg_index += 2;
    } else if (strcasecmp(arg, "pxat") == 0 && argv.size() > (arg_index + 1)) {
      set_ttl = true;
      Int64 now = get_current_unix_time();
      std::string opt_val = argv[arg_index + 1];
      if (get_int64(opt_val, response, &ttl) == false) return;
      ttl = (ttl + Int64(999)) / Int64(1000);
      if (now >= ttl) {
        /* Already expired */
        ttl = 0;
      } else {
        ttl -= now;
      }
      DEB_TTL(("pxat: ttl: %lld\n", ttl));
      arg_index += 2;
    } else if (strcasecmp(arg, "keepttl") == 0 && argv.size() > arg_index) {
      set_ttl = false;
      keep_ttl = true;
      arg_index += 1;
      DEB_TTL(("keep_ttl set\n"));
    }
    if (arg_index != arg_size) {
      assign_generic_err_to_response(response, REDIS_SYNTAX_ERROR);
      return;
    }
  } else {
    num_keys = arg_size - arg_index_start;
  }
  assert((num_keys & 1) == 0);
  assert(num_keys > 0);
  num_keys = num_keys / 2;
  const NdbDictionary::Dictionary *dict;
  const NdbDictionary::Table *tab = nullptr;
  struct KeyStorage *key_storage;
  key_storage = (struct KeyStorage*)malloc(
    sizeof(struct KeyStorage) * num_keys);
  if (key_storage == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    return;
  }
  struct GetControl *get_ctrl = (struct GetControl*)
    malloc(sizeof(struct GetControl));
  if (get_ctrl == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    free(get_ctrl);
    return;
  }
  get_ctrl->m_ndb = ndb;
  get_ctrl->m_key_store = key_storage;
  get_ctrl->m_value_rows = nullptr;
  get_ctrl->m_next_value_row = 0;
  get_ctrl->m_num_transactions = 0;
  get_ctrl->m_num_keys_requested = num_keys;
  get_ctrl->m_num_keys_outstanding = 0;
  get_ctrl->m_num_bytes_outstanding = 0;
  get_ctrl->m_num_keys_completed_first_pass = 0;
  get_ctrl->m_num_keys_multi_rows = 0;
  get_ctrl->m_num_keys_failed = 0;
  get_ctrl->m_num_new_fields = 0;
  get_ctrl->m_num_read_errors = 0;
  get_ctrl->m_error_code = 0;
  get_ctrl->m_is_set_command = set_command;
  get_ctrl->m_get_cmd_part = get_cmd_part;
  get_ctrl->m_worker_id = worker_id;
  get_ctrl->m_database_id = get_current_database(worker_id);
  get_ctrl->m_hash_name_ptr = nullptr;
  get_ctrl->m_hash_name_len = 0;
  get_ctrl->m_hset_key_tab = nullptr;
  get_ctrl->m_hset_prealloc_id = 0;
  get_ctrl->m_hset_redis_key_id = 0;
  get_ctrl->m_hset_field_count_pre = 0;
  get_ctrl->m_rec_attr_hset_id = nullptr;
  get_ctrl->m_rec_attr_hset_field_count = nullptr;
  get_ctrl->m_hset_phase_chunk_start = 0;
  get_ctrl->m_hset_phase_chunk_count = 0;
  for (Uint32 i = 0; i < num_keys; i++) {
    Uint32 arg_index_key = (2 * i) + arg_index_start;
    Uint32 arg_index_val = ((2 * i) + 1) + arg_index_start;
    key_storage[i].m_index = i;
    key_storage[i].m_close_flag = false;
    key_storage[i].m_get_ctrl = get_ctrl;
    key_storage[i].m_trans = nullptr;
    const char *key_str = argv[arg_index_key].c_str();
    Uint32 key_len = argv[arg_index_key].size();
    if (memcmp(key_str, "key:__rand_int__", 16) == 0) {
      rand_key(&key_storage[i], &key_str, key_len);
    }
    key_storage[i].m_key_str = key_str;
    key_storage[i].m_key_len = key_len;

    // todo fix memory handling for m_value_ptr
    key_storage[i].m_value_ptr = (char*)malloc(argv[arg_index_val].size() + 1);
    if (key_storage[i].m_value_ptr == nullptr) {
      assign_generic_err_to_response(response, FAILED_MALLOC);
      return;
    }
    memcpy(key_storage[i].m_value_ptr,
           argv[arg_index_val].c_str(),
           argv[arg_index_val].size());
    key_storage[i].m_value_ptr[argv[arg_index_val].size()] = '\0';
    key_storage[i].m_set_value_size = argv[arg_index_val].size();

    key_storage[i].m_header_len = 0;
    key_storage[i].m_first_value_row = 0;
    key_storage[i].m_current_pos = 0;
    key_storage[i].m_num_rows = 0;
    key_storage[i].m_num_rw_rows = 0;
    key_storage[i].m_num_current_rw_rows = 0;
    key_storage[i].m_rondb_key = 0;
    key_storage[i].m_rec_attr_prev_num_rows = nullptr;
    key_storage[i].m_rec_attr_rondb_key = nullptr;
    key_storage[i].m_key_state = KeyState::NotCompleted;
    key_storage[i].m_set_type = set_type;
    key_storage[i].m_keep_ttl = keep_ttl;
    key_storage[i].m_set_ttl = set_ttl;
    generate_expire_at(&(key_storage[i].m_expire_at), ttl);
  }
  if (!setup_metadata(ndb,
                      response,
                      &dict,
                      &tab)) {
    release_mset(get_ctrl);
    return;
  }
  // is_hmset distinguishes the +OK reply shape (HMSET) from the
  // count-of-new-fields reply shape (HSET), but BOTH commands flow
  // through rondb_hset_command and need the single-trans hash path.
  // rondb_hset_command always passes redis_key_id != STRING_REDIS_KEY_ID
  // (sentinel 1); plain MSET/SET pass STRING_REDIS_KEY_ID (0). So
  // "redis_key_id != STRING || is_hmset" is the right gate for the
  // hash path.
  const bool is_hash_command =
    (redis_key_id != STRING_REDIS_KEY_ID) || is_hmset;
  if (is_hash_command) {
    // Phase 1.0.2b: stash the hash name and the hset_keys table
    // pointer so set_rows_hset can address the hash row.
    // argv[1] lives for the entire batch so no copy is needed.
    const NdbDictionary::Table *hset_tab =
      dict->getTable(HSET_KEY_TABLE_NAME);
    if (hset_tab == nullptr) {
      assign_ndb_err_to_response(response,
                                 "Failed to get hset_keys table",
                                 dict->getNdbError());
      release_mset(get_ctrl);
      return;
    }
    get_ctrl->m_hset_key_tab = hset_tab;
    get_ctrl->m_hash_name_ptr = argv[1].c_str();
    get_ctrl->m_hash_name_len = argv[1].size();
  }
  // Phase 1.0.2d: HSET / HMSET take a single-trans path that
  // resolves redis_key_id in-trans, takes hset_keys(key) X-lock
  // before any field-row op runs, and commits all field writes
  // atomically. No batching loop; one trans for the whole HSET.
  // The reply-shape switch below (+OK vs count-of-new-fields) is
  // shared with the per-key MSET path so we fall through to it.
  bool hset_done = false;
  if (is_hash_command) {
    int ret_code = set_rows_hset(ndb,
                                 tab,
                                 response,
                                 key_storage,
                                 get_ctrl,
                                 num_keys);
    if (ret_code != 0) {
      release_mset(get_ctrl);
      return;
    }
    hset_done = true;
  }
  if (get_cmd_part) {
    int ret_code = rondb_get_func(ndb,
                                  tab,
                                  response,
                                  redis_key_id,
                                  get_ctrl,
                                  key_storage,
                                  num_keys);
    if (ret_code != 0) {
      release_mset(get_ctrl);
      return;
    }
    for (Uint32 i = 0; i < num_keys; i++) {
      key_storage[i].m_get_key_state = key_storage[i].m_key_state;
      key_storage[i].m_key_state = KeyState::MultiRow;
    }
    get_ctrl->m_num_keys_multi_rows = 0;
  }
  DEB_MSET_CMD(("MSET of %u keys\n", num_keys));
  if (!hset_done) {
    Uint32 current_index = 0;
    do {
      Uint32 loop_count = std::min(num_keys - current_index,
                                   (Uint32)MAX_PARALLEL_KEY_OPS);
      int ret_code = set_rows(ndb,
                              tab,
                              response,
                              redis_key_id,
                              key_storage,
                              get_ctrl,
                              loop_count,
                              current_index);
      if (ret_code != 0) {
        release_mset(get_ctrl);
        return;
      }
      // After set_rows, every key is in a terminal state with
      // m_close_flag set: CompletedSuccess (via write_commit_callback),
      // CompletedFailed, or CompletedConditionalFail. Close their
      // transactions now so we don't hold open handles across batches.
      close_finished_transactions(key_storage,
                                  get_ctrl,
                                  loop_count,
                                  current_index);
      DEB_MSET_CMD(("%u keys, %u dispatched, %u completed\n",
                    num_keys,
                    get_ctrl->m_num_keys_multi_rows,
                    get_ctrl->m_num_keys_completed_first_pass));
      current_index += loop_count;
    } while (current_index < num_keys && get_ctrl->m_num_keys_failed == 0);
  }
  /**
   * We are done with the writing process, now it is time to report the
   * result.
   */
  if (get_ctrl->m_num_keys_failed > 0) {
    assign_err_to_response(response,
                           FAILED_EXECUTE_MSET,
                           get_ctrl->m_error_code);
    release_mset(get_ctrl);
    return;
  }
  if (get_cmd_part) {
    for (Uint32 i = 0; i < num_keys; i++) {
      key_storage[i].m_key_state = key_storage[i].m_get_key_state;
    }
    int ret_code = rondb_get_response(response,
                                      key_storage,
                                      num_keys);
    if (ret_code != 0) {
      assign_generic_err_to_response(response, FAILED_MALLOC);
    }
    release_mset(get_ctrl);
    return;
  }
  // SET ... NX / XX without GET: when the conditional guard tripped,
  // the write callback set CompletedConditionalFail. Emit Redis-
  // canonical nil (C7 / C8). NX / XX are only exposed through the
  // single-key SET path, so only key 0 can carry this state here.
  if (num_keys == 1 &&
      key_storage[0].m_key_state == KeyState::CompletedConditionalFail) {
    response->append(REDIS_NO_SUCH_KEY);
    release_mset(get_ctrl);
    return;
  }
  if (redis_key_id == STRING_REDIS_KEY_ID || is_hmset) {
    // MSET / SET / HMSET all return the simple-string +OK reply.
    response->append("+OK\r\n");
  } else {
    // HSET returns the count of fields that did not previously exist
    // and were therefore added (C10). The interpreter writes
    // OUTPUT_INDEX_3 = 1 on its INSERT branch and 0 on UPDATE; the
    // write callbacks aggregate into m_num_new_fields.
    char buf[20];
    snprintf(buf,
             sizeof(buf),
             ":%u\r\n",
             get_ctrl->m_num_new_fields);
    response->append(&buf[0]);
  }
  release_mset(get_ctrl);
  return;
}

void rondb_set_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int worker_id)
{
  rondb_mset(ndb, argv, response, false,
             STRING_REDIS_KEY_ID, true, worker_id);
}

void rondb_mset_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id)
{
  rondb_mset(ndb, argv, response, false,
             STRING_REDIS_KEY_ID, false, worker_id);
}

// HSET / HMSET share the hash-write implementation but diverge on
// reply shape: HSET returns the count of newly-added fields, HMSET
// always returns +OK (C10 / C11). The is_hmset flag selects between
// them at the response site inside rondb_mset.
void rondb_hset_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        bool is_hmset,
                        int worker_id)
{
  // Phase 1.0.2d: redis_key_id is resolved inside set_rows_hset's
  // Phase 1 (the hset_keys lock-claim writeTuple), not pre-called.
  // The redis_key_id parameter to rondb_mset is unused for the
  // is_hmset branch but the reply-shape switch later in rondb_mset
  // tests redis_key_id == STRING_REDIS_KEY_ID (==0) to distinguish
  // plain SET/MSET from HSET; pass a non-zero sentinel so HSET
  // (is_hmset=false) routes to the count-of-new-fields reply, not
  // the +OK reply.
  rondb_mset(ndb, argv, response, is_hmset,
             /* redis_key_id sentinel */ 1, false, worker_id);
}

/**
 * GET MODULE
 * ----------
 */
static int send_value_read(std::string *response,
                           struct KeyStorage *key_store,
                           struct GetControl *get_ctrl) {
  if (key_store->m_num_rw_rows == 0) {
    /* Before we read the data we need to allocate memory for the row */
    key_store->m_value_ptr = (char*)
      malloc(key_store->m_get_value_size);
    if (key_store->m_value_ptr == nullptr) {
      assign_generic_err_to_response(response, FAILED_MALLOC);
      return 1;
    }
    key_store->m_first_value_row = get_ctrl->m_next_value_row;
    get_ctrl->m_next_value_row += MAX_PARALLEL_VALUE_RWS;

    /* Copy row from key_row to complex value now that we allocated it */
    Uint32 value_len = get_length((char*)
      &key_store->m_key_row.value_start[0]);
      key_store->m_current_pos = value_len;
      assert(value_len == INLINE_VALUE_LEN);
      memcpy(key_store->m_value_ptr,
             &key_store->m_key_row.value_start[2],
             value_len);
      DEB_MGET_CMD(("Copied from key_row %u bytes for key %u\n",
        value_len, key_store->m_index));
  }
  Uint32 i = 0;
  Uint32 value_row_index = key_store->m_first_value_row;
  do {
    struct value_table *value_row =
      &get_ctrl->m_value_rows[value_row_index + i];
      value_row->rondb_key = key_store->m_key_row.rondb_key;
    value_row->ordinal = key_store->m_num_rw_rows;
    int ret_code = prepare_get_value_row(response,
                                         key_store,
                                         get_ctrl->m_is_set_command,
                                         value_row,
                                         get_ctrl->m_database_id);
    if (ret_code != 0) return 1;
    i++;
    key_store->m_num_rw_rows++;
  } while (i < MAX_PARALLEL_VALUE_RWS &&
           key_store->m_num_rw_rows < key_store->m_num_rows);
  key_store->m_num_current_rw_rows = i;
  get_ctrl->m_num_bytes_outstanding += i * sizeof(struct value_table);
  get_ctrl->m_num_keys_outstanding++;
  if (key_store->m_num_rw_rows == key_store->m_num_rows &&
      get_ctrl->m_is_set_command == false) {
    commit_read_value_transaction(key_store);
    key_store->m_key_state = KeyState::MultiRowRWAll;
  } else {
    prepare_read_value_transaction(key_store);
    key_store->m_key_state = KeyState::MultiRowRWValueSent;
  }
  DEB_MGET_CMD(("Prepare send value read: Key %u, read rows: %u"
                ", num_rows: %u, num_read_rows: %u, key_state: %u\n",
                key_store->m_index,
                key_store->m_num_current_rw_rows,
                key_store->m_num_rows,
                key_store->m_num_rw_rows,
                key_store->m_key_state));
  return 0;
}

static int send_next_read_batch(std::string *response,
                                struct KeyStorage *key_storage,
                                struct GetControl *get_ctrl,
                                Uint32 current_index,
                                Uint32 loop_count,
                                Uint32 &current_finished) {
  if (get_ctrl->m_num_keys_multi_rows == 0) {
    return 0;
  }
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    if (key_storage[inx].m_key_state == KeyState::MultiRowRWValue) {
      if (get_ctrl->m_num_bytes_outstanding > MAX_OUTSTANDING_BYTES) {
        return 0;
      }
      int ret_code = send_value_read(response,
                                     &key_storage[inx],
                                     get_ctrl);
      if (ret_code != 0) return 1;
      assert(current_finished > 0);
      current_finished--;
    } else if (key_storage[inx].m_key_state ==
               KeyState::CompletedMultiRowSuccess &&
               get_ctrl->m_is_set_command == false) {
      get_ctrl->m_num_keys_outstanding++;
      commit_read_value_transaction(&key_storage[inx]);
      DEB_MGET_CMD(("i: %u, current_finished: %u, outstanding: %u\n",
        i, current_finished, get_ctrl->m_num_keys_outstanding));
      assert(current_finished > 0);
      current_finished--;
      key_storage[inx].m_key_state = KeyState::CompletedMultiRowSuccessCommit;
    }
  }
  return 0;
}

static int get_complex_rows(Ndb *ndb,
                            const NdbDictionary::Table *tab,
                            std::string *response,
                            Uint64 redis_key_id,
                            struct KeyStorage *key_storage,
                            struct GetControl *get_ctrl,
                            Uint32 loop_count,
                            Uint32 current_index) {
  Uint32 num_complex_reads = 0;
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    if (key_storage[inx].m_key_state == KeyState::MultiRow) {
      num_complex_reads++;
      DEB_MGET_CMD(("Start complex read of key id %u\n", inx));
      if (!setup_one_transaction(ndb,
                                 response,
                                 redis_key_id,
                                 &key_storage[inx],
                                 tab)) {
        return 1;
      }
      get_ctrl->m_num_transactions++;
      if (prepare_get_key_row(response,
                              &key_storage[inx],
                              get_ctrl->m_is_set_command,
                              get_ctrl->m_database_id) != 0) {
        return 1;
      }
      prepare_read_transaction(&key_storage[inx]);
    }
  }
  get_ctrl->m_value_rows = (struct value_table*)malloc(
    num_complex_reads * MAX_PARALLEL_VALUE_RWS *
    sizeof(struct value_table));
  if (get_ctrl->m_value_rows == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    return 1;
  }
  assert(num_complex_reads == get_ctrl->m_num_keys_multi_rows);
  Uint32 current_finished_in_loop = 0;
  get_ctrl->m_num_keys_outstanding = num_complex_reads;
  get_ctrl->m_num_bytes_outstanding =
    loop_count * (sizeof(struct key_table) - MAX_KEY_VALUE_LEN);
  do {
    /**
     * Now send off all prepared and wait for at least one to complete.
     * We cannot wait for multiple ones since we could then run into
     * deadlock issues. The transactions are independent of each other,
     * so if one of them has to wait for a lock, it should not stop
     * other transactions from progressing.
     */
    DEB_MGET_CMD(("Call sendPollNdb with %u keys, %u keys out and %u bytes"
                  " out\n",
                  get_ctrl->m_num_keys_multi_rows,
                  get_ctrl->m_num_keys_outstanding,
                  get_ctrl->m_num_bytes_outstanding));
    int min_finished = 1;
    int finished = execute_ndb(ndb, min_finished, __LINE__);
    assert(finished >= 0);
    current_finished_in_loop += finished;
    DEB_MGET_CMD(("Finished serving %u keys, prepare next batch,"
                  " current_index: %u\n",
      finished, current_index));
    if (get_ctrl->m_num_keys_failed > 0) return 0;
    int ret_code = send_next_read_batch(response,
                                        key_storage,
                                        get_ctrl,
                                        current_index,
                                        loop_count,
                                        current_finished_in_loop);
    if (ret_code != 0) return 1;
  } while (current_finished_in_loop < num_complex_reads);
  return 0;
}

static int get_simple_rows(Ndb *ndb,
                           const NdbDictionary::Table *tab,
                           std::string *response,
                           Uint64 redis_key_id,
                           struct KeyStorage *key_storage,
                           struct GetControl *get_ctrl,
                           Uint32 loop_count,
                           Uint32 current_index) {
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    if (!setup_one_transaction(ndb,
                               response,
                               redis_key_id,
                               &key_storage[inx],
                               tab)) {
      return 1;
    }
    get_ctrl->m_num_transactions++;
    if (prepare_get_simple_key_row(response,
                                   Uint32(0xFC),
                                   key_storage[inx].m_trans,
                                   &key_storage[inx].m_key_row,
                                   get_ctrl->m_database_id) != 0) {
      return 1;
    }
    commit_simple_read_transaction(&key_storage[inx]);
  }
  Uint32 current_finished_in_loop = 0;
  get_ctrl->m_num_keys_outstanding = loop_count;
  do {
    /**
     * Now send off all prepared and wait for all to complete.
     * Since we are using CommitedRead there is no risk of
     * deadlocks by waiting for all to complete here.
     */
    int min_finished = 1;
    int finished = execute_ndb(ndb, min_finished, __LINE__);
    assert(finished >= 0);
    current_finished_in_loop += finished;
  } while (current_finished_in_loop < loop_count);
  return 0;
}

/**
 * According to the REDIS documentation the MGET command is atomic and
 * cannot deliver partial data. Thus if any error occurs in the process
 * of executing this command, it will report the entire command as failed.
 * Rows not found is perfectly ok and will be returned as nil rows.
 *
 * The default implementation in Rondis is such that we execute multiple
 * GET commands and thus the result isn't atomic.
 *
 * The path to achieve atomic behaviour of the MGET command is by ensuring
 * that the 'string_keys' table only have a single partition. In addition
 * to avoid deadlocks the keys will be sorted and sent off in sorted order.
 *
 * In addition we will skip the step where we run the get_simple_rows since
 * we have to run the entire operation in a single transaction. RonDB will
 * in this case not allow the data nodes to use query threads if there is
 * more than one operation sent in the request.
 *
 * Once we have finished the first part where we perform locked reads of
 * the 'string_keys' we can proceed as usual with the normal procedure
 * that uses ReadCommitted for the reads of the 'string_values' table.
 *
 * We will use the same approach for implementing MSET such that it
 * becomes atomic.
 *
 * HMGET isn't documented as being atomic. However since they share the
 * same code path as MGET the above principles will also be allowed to
 * be used for the HMGET and HSET operations. Default behaviour is still
 * that MGET, MSET, HSET and HMGET acts as a number of independent GET
 * and SET commands in Rondis.
 */
static int
rondb_get_func(Ndb *ndb,
               const NdbDictionary::Table *tab,
               std::string *response,
               Uint64 redis_key_id,
               struct GetControl *get_ctrl,
               KeyStorage *key_storage,
               Uint32 num_keys) {
  DEB_MGET_CMD(("MGET of %u keys, simple flag: %u, worker_id: %u\n",
    num_keys,
    get_opt_small_values_flag(get_ctrl->m_worker_id),
    get_ctrl->m_worker_id));
  Uint32 current_index = 0;
  do {
    Uint32 loop_count = std::min(num_keys - current_index,
                                 (Uint32)MAX_PARALLEL_KEY_OPS);
    if (get_opt_small_values_flag(get_ctrl->m_worker_id) &&
        get_ctrl->m_is_set_command == false) {
      int ret_code = get_simple_rows(ndb,
                                     tab,
                                     response,
                                     redis_key_id,
                                     key_storage,
                                     get_ctrl,
                                     loop_count,
                                     current_index);
      if (ret_code != 0) {
        return -1;
      }
      DEB_MGET_CMD(("%u keys, %u multi rows, %u completed\n",
                    num_keys,
                    get_ctrl->m_num_keys_multi_rows,
                    get_ctrl->m_num_keys_completed_first_pass));
      /**
       * We have finished the initial round of simple GETs. Now time
       * to handle those that require multi-row GETs. Since we used
       * an optimistic approach we need to start this from scratch
       * again for these new GETs.
       */
      close_finished_transactions(key_storage,
                                  get_ctrl,
                                  loop_count,
                                  current_index);
      assert(get_ctrl->m_num_transactions == 0);
      assert(get_ctrl->m_num_keys_outstanding == 0);
    } else {
      get_ctrl->m_num_keys_multi_rows = num_keys;
      for (Uint32 i = current_index; i < current_index + loop_count; i++) {
        key_storage[i].m_key_state = KeyState::MultiRow;
      }
    }
    if (get_ctrl->m_num_keys_multi_rows > 0 &&
        get_ctrl->m_num_keys_failed == 0) {
      int ret_code = get_complex_rows(ndb,
                                      tab,
                                      response,
                                      redis_key_id,
                                      key_storage,
                                      get_ctrl,
                                      loop_count,
                                      current_index);
      if (ret_code != 0) {
        return -1;
      }
    }
    current_index += loop_count;
  } while (current_index < num_keys && get_ctrl->m_num_keys_failed == 0);
  close_finished_transactions(key_storage,
                              get_ctrl,
                              num_keys,
                              0);
  /**
   * We are done with the reading process, now it is time to report the
   * result based on the KeyStorage array.
   */
  if (get_ctrl->m_num_keys_failed > 0) {
    assign_err_to_response(response,
                           FAILED_EXECUTE_MGET,
                           get_ctrl->m_error_code);
    return-1;
  }
  return 0;
}

static int rondb_get_response(std::string *response,
                              KeyStorage *key_storage,
                              Uint32 num_keys) {
  Uint64 tot_bytes = 0;
  for (Uint32 i = 0; i < num_keys; i++) {
    struct KeyStorage *key_store = &key_storage[i];
    if (key_store->m_key_state == KeyState::CompletedFailed) {
      /* Report found NULL */
      key_store->m_header_len = (Uint32)snprintf(
        key_store->m_header_buf,
        sizeof(key_store->m_header_buf),
        "$-1");
      DEB_MGET_CMD(("Key id %u was NULL, len: %u\n",
        i, key_store->m_header_len));
    } else {
      tot_bytes += key_store->m_get_value_size;
      key_store->m_header_len = (Uint32)snprintf(
        key_store->m_header_buf,
        sizeof(key_store->m_header_buf),
        "$%u\r\n",
        key_store->m_get_value_size);
      DEB_MGET_CMD(("Key id %u was of size %u, len: %u\n",
        i, key_store->m_get_value_size, key_store->m_header_len));
    }
    tot_bytes += 2;
    tot_bytes += key_store->m_header_len;
  }
  char header_buf[20];
  Uint32 header_len = 0;
  /*
   * For single-key GET/HGET, return bulk string directly without array wrapper.
   * For multi-key MGET/HMGET, wrap in array format.
   */
  if (num_keys > 1) {
    header_len = (Uint32)snprintf(header_buf,
                                  sizeof(header_buf),
                                  "*%u\r\n",
                                  num_keys);
    tot_bytes += (Uint32)header_len;
  }
  try {
    response->reserve(tot_bytes);
  } catch (const std::exception &e) {
    return -1;
  }
  if (header_len > 0) {
    response->append((const char*)&header_buf[0], header_len);
  }
  for (Uint32 i = 0; i < num_keys; i++) {
    struct KeyStorage *key_store = &key_storage[i];
    response->append((const char*)&key_store->m_header_buf[0],
                     key_store->m_header_len);
    if (key_store->m_key_state == KeyState::CompletedSuccess ||
        key_store->m_key_state == KeyState::CompletedMultiRowSuccess) {
      response->append((const char*)&key_store->m_key_row.value_start[2],
                       key_store->m_get_value_size);
    }
    else if (key_store->m_key_state == KeyState::CompletedMultiRow) {
      response->append((const char*)key_store->m_value_ptr,
                       key_store->m_get_value_size);
    } else {
      assert(key_store->m_key_state == KeyState::CompletedFailed);
    }
    response->append("\r\n");
  }
  return 0;
}

static int init_mget(struct KeyStorage **ret_key_storage,
                     struct GetControl **ret_get_ctrl,
                     const pink::RedisCmdArgsType &argv,
                     std::string *response,
                     Uint32 num_keys,
                     Ndb *ndb,
                     int worker_id,
                     Uint32 arg_index_start) {
  struct KeyStorage *key_storage;
  key_storage = (struct KeyStorage*)malloc(
    sizeof(struct KeyStorage) * num_keys);
  if (key_storage == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    return -1;
  }
  struct GetControl *get_ctrl = (struct GetControl*)
    malloc(sizeof(struct GetControl));
  if (get_ctrl == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    free(key_storage);
    return -1;
  }
  get_ctrl->m_ndb = ndb;
  get_ctrl->m_key_store = key_storage;
  get_ctrl->m_value_rows = nullptr;
  get_ctrl->m_next_value_row = 0;
  get_ctrl->m_num_transactions = 0;
  get_ctrl->m_num_keys_requested = num_keys;
  get_ctrl->m_num_keys_outstanding = 0;
  get_ctrl->m_num_bytes_outstanding = 0;
  get_ctrl->m_num_keys_completed_first_pass = 0;
  get_ctrl->m_num_keys_multi_rows = 0;
  get_ctrl->m_num_keys_failed = 0;
  get_ctrl->m_num_read_errors = 0;
  get_ctrl->m_error_code = 0;
  get_ctrl->m_is_set_command = false;
  get_ctrl->m_get_cmd_part = false;
  get_ctrl->m_worker_id = worker_id;
  get_ctrl->m_database_id = get_current_database(worker_id);
  for (Uint32 i = 0; i < num_keys; i++) {
    Uint32 arg_index = i + arg_index_start;
    key_storage[i].m_index = i;
    key_storage[i].m_close_flag = false;
    key_storage[i].m_get_ctrl = get_ctrl;
    key_storage[i].m_trans = nullptr;
    const char *key_str = argv[arg_index].c_str();
    Uint32 key_len = argv[arg_index].size();
    if (memcmp(key_str, "key:__rand_int__", 16) == 0) {
      rand_key(&key_storage[i], &key_str, key_len);
    }
    key_storage[i].m_key_str = key_str;
    key_storage[i].m_key_len = key_len;
    key_storage[i].m_value_ptr = nullptr;
    key_storage[i].m_get_value_size = 0;
    key_storage[i].m_set_value_size = 0;

    key_storage[i].m_header_len = 0;
    key_storage[i].m_first_value_row = 0;
    key_storage[i].m_current_pos = 0;
    key_storage[i].m_num_rows = 0;
    key_storage[i].m_num_rw_rows = 0;
    key_storage[i].m_num_current_rw_rows = 0;
    key_storage[i].m_key_state = KeyState::NotCompleted;
    key_storage[i].m_set_type = IsGet;
    key_storage[i].m_keep_ttl = false;
    key_storage[i].m_set_ttl = false;
    key_storage[i].m_expire_at = 0;
  }
  *ret_key_storage = key_storage;
  *ret_get_ctrl = get_ctrl;
  return 0;
}

static
void rondb_mget(Ndb *ndb,
               const pink::RedisCmdArgsType &argv,
               std::string *response,
               Uint64 redis_key_id,
               int worker_id) {
  Uint32 arg_index_start = (redis_key_id == STRING_REDIS_KEY_ID) ? 1 : 2;
  Uint32 num_keys = argv.size() - arg_index_start;
  assert(num_keys > 0);
  const NdbDictionary::Dictionary *dict;
  const NdbDictionary::Table *tab = nullptr;
  struct KeyStorage *key_storage;
  struct GetControl *get_ctrl;
  if (init_mget(&key_storage,
                &get_ctrl,
                argv,
                response,
                num_keys,
                ndb,
                worker_id,
                arg_index_start) < 0) {
    return;
  }
  if (!setup_metadata(ndb,
                      response,
                      &dict,
                      &tab)) {
    release_mget(get_ctrl);
    return;
  }
  int ret_code = rondb_get_func(ndb,
                                tab,
                                response,
                                redis_key_id,
                                get_ctrl,
                                key_storage,
                                num_keys);
  if (ret_code < 0) {
    release_mget(get_ctrl);
    return;
  }
  ret_code = rondb_get_response(response,
                                key_storage,
                                num_keys);
  if (ret_code != 0) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
  }
  release_mget(get_ctrl);
}

void rondb_get_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int worker_id) {
  rondb_mget(ndb, argv, response, STRING_REDIS_KEY_ID, worker_id);
}

void rondb_mget_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id) {
  rondb_mget(ndb, argv, response, STRING_REDIS_KEY_ID, worker_id);
}

void rondb_hget_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id) {
  Uint64 redis_key_id;
  int ret_code = rondb_get_redis_key_id(ndb,
                                       redis_key_id,
                                       argv[1].c_str(),
                                       argv[1].size(),
                                       response,
                                       get_current_database(worker_id));
  if (ret_code != 0) {
      return;
  }
  rondb_mget(ndb, argv, response, redis_key_id, worker_id);
}

void rondb_hmget_command(Ndb *ndb,
                         const pink::RedisCmdArgsType &argv,
                         std::string *response,
                         int worker_id) {
  Uint64 redis_key_id;
  int ret_code = rondb_get_redis_key_id(ndb,
                                        redis_key_id,
                                        argv[1].c_str(),
                                        argv[1].size(),
                                        response,
                                        get_current_database(worker_id));
  if (ret_code != 0) {
      return;
  }
  rondb_mget(ndb, argv, response, redis_key_id, worker_id);
}

/**
 * INCR and DECR module
 * --------------------
 */
static
void rondb_incr_decr(
    Ndb *ndb,
    const pink::RedisCmdArgsType &argv,
    std::string *response,
    Uint64 redis_key_id,
    bool incr_flag,
    Int64 inc_dec_value,
    int worker_id) {
  Uint32 arg_index_start = (redis_key_id == STRING_REDIS_KEY_ID) ? 1 : 2;
  const NdbDictionary::Dictionary *dict;
  const NdbDictionary::Table *tab = nullptr;
  KeyStorage key_store;
  const char *key_str = argv[arg_index_start].c_str();
  Uint32 key_len = argv[arg_index_start].size();
  key_store.m_key_str = key_str;
  key_store.m_key_len = key_len;
  if (!setup_transaction(ndb,
                         response,
                         redis_key_id,
                         &key_store,
                         &dict,
                         &tab))
    return;

  Uint64 unsigned_value = Uint64(inc_dec_value);
  if (inc_dec_value < 0) {
    unsigned_value = Uint64(-inc_dec_value);
    if (incr_flag)
      incr_flag = false;
    else
      incr_flag = true;
  }
  DEB_INCR(("INCR redis_key_id: %llu, incr_flag: %u, val: %llu\n",
    redis_key_id, incr_flag, unsigned_value));
  incr_decr_key_row(response,
                    ndb,
                    tab,
                    key_store.m_trans,
                    &key_store.m_key_row,
                    incr_flag,
                    unsigned_value,
                    worker_id);
  ndb->closeTransaction(key_store.m_trans);
  return;
}

void rondb_incr_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id) {
  DEB_INCR(("INCR command\n"));
  rondb_incr_decr(ndb,
                  argv,
                  response,
                  STRING_REDIS_KEY_ID,
                  true,
                  1,
                  worker_id);
}

void rondb_incrby_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id) {
  DEB_INCR(("INCRBY command\n"));
  char *end_ptr = nullptr;
  const char *val_ptr = argv[2].c_str();
  const char *memory_end = val_ptr + argv[2].size();
  errno = 0;
  Int64 val = strtoll(val_ptr,
                      &end_ptr,
                      10);
  if (errno == EINVAL || errno == ERANGE || end_ptr != memory_end) {
    assign_err_to_response(response,
                           FAILED_INCRBY_DECRBY_PARAMETER,
                           0);
    return;
  }
  rondb_incr_decr(ndb,
                  argv,
                  response,
                  STRING_REDIS_KEY_ID,
                  true,
                  val,
                  worker_id);
}

void rondb_decr_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id) {
  DEB_INCR(("DECR command\n"));
  rondb_incr_decr(ndb,
                  argv,
                  response,
                  STRING_REDIS_KEY_ID,
                  false,
                  1,
                  worker_id);
}

void rondb_decrby_command(Ndb *ndb,
                          const pink::RedisCmdArgsType &argv,
                          std::string *response,
                          int worker_id) {
  DEB_INCR(("DECRBY command\n"));
  char *end_ptr = nullptr;
  const char *val_ptr = argv[2].c_str();
  const char *memory_end = val_ptr + argv[2].size();
  errno = 0;
  Int64 val = strtoll(val_ptr,
                      &end_ptr,
                      10);
  if (errno == EINVAL || errno == ERANGE || end_ptr != memory_end) {
    assign_err_to_response(response,
                           FAILED_INCRBY_DECRBY_PARAMETER,
                           0);
    return;
  }
  rondb_incr_decr(ndb,
                  argv,
                  response,
                  STRING_REDIS_KEY_ID,
                  false,
                  val,
                  worker_id);
}

void rondb_hincr_command(Ndb *ndb,
                         const pink::RedisCmdArgsType &argv,
                         std::string *response,
                         int worker_id) {
  DEB_INCR(("HINCR command\n"));
  Uint64 redis_key_id;
  int ret_code = rondb_get_redis_key_id(ndb,
                                       redis_key_id,
                                       argv[1].c_str(),
                                       argv[1].size(),
                                       response,
                                       get_current_database(worker_id));
  if (ret_code != 0) {
      return;
  }
  rondb_incr_decr(ndb,
                  argv,
                  response,
                  redis_key_id,
                  true,
                  1,
                  worker_id);
}

void rondb_hincrby_command(Ndb *ndb,
                           const pink::RedisCmdArgsType &argv,
                           std::string *response,
                           int worker_id) {
  DEB_INCR(("HINCRBY command\n"));
  Uint64 redis_key_id;
  int ret_code = rondb_get_redis_key_id(ndb,
                                       redis_key_id,
                                       argv[1].c_str(),
                                       argv[1].size(),
                                       response,
                                       get_current_database(worker_id));
  if (ret_code != 0) {
      return;
  }
  char *end_ptr = nullptr;
  const char *val_ptr = argv[3].c_str();
  const char *memory_end = val_ptr + argv[3].size();
  errno = 0;
  Int64 val = strtoll(val_ptr,
                      &end_ptr,
                      10);
  if (errno == EINVAL || errno == ERANGE || end_ptr != memory_end) {
    assign_err_to_response(response,
                           FAILED_INCRBY_DECRBY_PARAMETER,
                           0);
    return;
  }
  rondb_incr_decr(ndb,
                  argv,
                  response,
                  redis_key_id,
                  true,
                  val,
                  worker_id);
}

void rondb_hdecr_command(Ndb *ndb,
                         const pink::RedisCmdArgsType &argv,
                         std::string *response,
                         int worker_id) {
  DEB_INCR(("HDECR command\n"));
  Uint64 redis_key_id;
  int ret_code = rondb_get_redis_key_id(ndb,
                                       redis_key_id,
                                       argv[1].c_str(),
                                       argv[1].size(),
                                       response,
                                       get_current_database(worker_id));
  if (ret_code != 0) {
      return;
  }
  rondb_incr_decr(ndb,
                  argv,
                  response,
                  redis_key_id,
                  false,
                  1,
                  worker_id);
}

void rondb_hdecrby_command(Ndb *ndb,
                           const pink::RedisCmdArgsType &argv,
                           std::string *response,
                           int worker_id) {
  DEB_INCR(("HDECRBY command\n"));
  Uint64 redis_key_id;
  int ret_code = rondb_get_redis_key_id(ndb,
                                       redis_key_id,
                                       argv[1].c_str(),
                                       argv[1].size(),
                                       response,
                                       get_current_database(worker_id));
  if (ret_code != 0) {
      return;
  }
  char *end_ptr = nullptr;
  const char *val_ptr = argv[3].c_str();
  const char *memory_end = val_ptr + argv[3].size();
  errno = 0;
  Int64 val = strtoll(val_ptr,
                      &end_ptr,
                      10);
  if (errno == EINVAL || errno == ERANGE || end_ptr != memory_end) {
    assign_err_to_response(response,
                           FAILED_INCRBY_DECRBY_PARAMETER,
                           0);
    return;
  }
  rondb_incr_decr(ndb,
                  argv,
                  response,
                  redis_key_id,
                  false,
                  val,
                  worker_id);
}

void rondb_strlen_command(Ndb *ndb,
                          const pink::RedisCmdArgsType &argv,
                          std::string *response,
                          int worker_id) {
  const NdbDictionary::Dictionary *dict;
  const NdbDictionary::Table *tab = nullptr;
  Uint32 database_id = get_current_database(worker_id);
  KeyStorage *key_store;
  key_store = (struct KeyStorage*)malloc(sizeof(struct KeyStorage));
  if (key_store == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    return;
  }

  const char *key_str = argv[1].c_str();
  Uint32 key_len = argv[1].size();
  if (memcmp(key_str, "key:__rand_int__", 16) == 0) {
    rand_key(key_store, &key_str, key_len);
  }
  key_store->m_key_str = key_str;
  key_store->m_key_len = key_len;
  if (!setup_transaction(ndb,
                         response,
                         STRING_REDIS_KEY_ID,
                         key_store,
                         &dict,
                         &tab)) {
    free(key_store);
    return;
  }
  // Initialize tot_value_len to 0 before executing - if the key doesn't exist,
  // NDB may return success without populating the row, leaving it uninitialized
  key_store->m_key_row.tot_value_len = 0;
  int ret_code = prepare_get_simple_key_row(response,
                                            Uint32(0x10),
                                            key_store->m_trans,
                                            &key_store->m_key_row,
                                            database_id);
  if (ret_code != 0) {
    ndb->closeTransaction(key_store->m_trans);
    free(key_store);
    return;
  }
  if (key_store->m_trans->execute(NdbTransaction::Commit) != 0) {
    key_store->m_key_row.tot_value_len = 0;
  }
  char buf[20];
  Uint32 ret_len = (Uint32)snprintf(buf,
                                    sizeof(buf),
                                    "+%u\r\n",
                                    key_store->m_key_row.tot_value_len);
  ndb->closeTransaction(key_store->m_trans);
  free(key_store);
  try {
    response->reserve(ret_len);
  } catch (const std::exception &e) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    return;
  }
  response->append(buf, ret_len);
  return;
}

void rondb_getrange_command(Ndb *ndb,
                            const pink::RedisCmdArgsType &argv,
                            std::string *response,
                            int worker_id) {
  Uint32 arg_index_start = 1;
  Uint32 num_keys = 1;
  const NdbDictionary::Dictionary *dict;
  const NdbDictionary::Table *tab = nullptr;
  struct KeyStorage *key_store;
  struct GetControl *get_ctrl;
  if (init_mget(&key_store,
                &get_ctrl,
                argv,
                response,
                num_keys,
                ndb,
                worker_id,
                arg_index_start) < 0) {
    return;
  }
  if (!setup_metadata(ndb,
                      response,
                      &dict,
                      &tab)) {
    release_mget(get_ctrl);
    return;
  }
  Int64 start, end;
  if (get_int64(argv[2], response, &start) == false) return;
  if (get_int64(argv[3], response, &end) == false) return;
  int ret_code = rondb_get_func(ndb,
                                tab,
                                response,
                                STRING_REDIS_KEY_ID,
                                get_ctrl,
                                key_store,
                                num_keys);
  if (ret_code < 0) {
    release_mget(get_ctrl);
    return;
  }
  Int64 tot_len = Int64(key_store->m_get_value_size);
  if (start < 0) {
    if (tot_len <= (-start)) {
      start = 0;
    } else {
      start = tot_len + start;
    }
  } else if (start >= tot_len) {
    start = tot_len;
  }
  if (end < 0) {
    if (tot_len <= (-end)) {
      end = start;
    } else {
      end = tot_len + end;
    }
  } else if (end >= tot_len) {
    end = tot_len - 1;
  }
  Uint32 ret_len;
  const char *value_ptr = nullptr;
  char buf[20];
  if (start > end ||
      key_store->m_key_state == KeyState::CompletedFailed) {
    /* Report found NULL */
    ret_len = 0;
  } else if (key_store->m_key_state == KeyState::CompletedSuccess ||
             key_store->m_key_state == KeyState::CompletedMultiRowSuccess) {
    value_ptr = &key_store->m_key_row.value_start[2];
    ret_len = Uint32((end - start + 1));
  } else {
    assert(key_store->m_key_state == KeyState::CompletedMultiRow);
    value_ptr = key_store->m_value_ptr;
    ret_len = Uint32((end - start + 1));
  }
  Uint32 header_len = (Uint32)snprintf(buf,
                                       sizeof(buf),
                                       "$%u\r\n",
                                       ret_len);
  Uint32 tot_ret_len = ret_len + (2 + header_len);
  try {
    response->reserve(tot_ret_len);
  } catch (const std::exception &e) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    return;
  }
  response->append((const char*)&buf[0], header_len);
  if (value_ptr)
    response->append(&value_ptr[start], ret_len);
  response->append("\r\n");
  return;
}

void rondb_setrange_command(Ndb *ndb,
                            const pink::RedisCmdArgsType &argv,
                            std::string *response,
                            int worker_id) {
  Int64 offset;
  if (get_int64(argv[2], response, &offset) == false) return;

  if (offset < 0) {
    assign_generic_err_to_response(response, REDIS_OFFSET_OUT_OF_RANGE);
    return;
  }
  if (offset > MAX_REDIS_ROW_SIZE) {
    assign_generic_err_to_response(response, REDIS_OFFSET_OUT_OF_RANGE);
    return;
  }
  Uint32 start = Uint32(offset);
  const NdbDictionary::Dictionary *dict;
  const NdbDictionary::Table *tab = nullptr;
  Uint32 database_id = get_current_database(worker_id);
  KeyStorage *key_store;
  key_store = (struct KeyStorage*)malloc(sizeof(struct KeyStorage));
  if (key_store == nullptr) {
    assign_generic_err_to_response(response, FAILED_MALLOC);
    return;
  }
  const char *key_str = argv[1].c_str();
  Uint32 key_len = argv[1].size();
  if (memcmp(key_str, "key:__rand_int__", 16) == 0) {
    rand_key(key_store, &key_str, key_len);
  }
  key_store->m_key_str = key_str;
  key_store->m_key_len = key_len;

  const char *value_str = argv[3].c_str();
  key_store->m_const_value_ptr = value_str;
  key_store->m_set_value_size = argv[3].size();
  Uint32 end = key_store->m_set_value_size + start;
  if (end > MAX_REDIS_ROW_SIZE) {
    assign_generic_err_to_response(response, REDIS_OFFSET_OUT_OF_RANGE);
    return;
  }
  if (!setup_transaction(ndb,
                         response,
                         STRING_REDIS_KEY_ID,
                         key_store,
                         &dict,
                         &tab)) {
    free(key_store);
    return;
  }
  if (start < INLINE_VALUE_LEN && end <= INLINE_VALUE_LEN) {
    /* The SETRANGE command only affects the STRING_keys table */
    execute_set_range_simple(response,
                             key_store,
                             tab,
                             database_id,
                             start,
                             end);
    ndb->closeTransaction(key_store->m_trans);
    free(key_store);
    return;
  }
  Uint32 old_tot_value_len = 0;
  if (rondb_get_rondb_key(tab,
                          key_store->m_rondb_key,
                          ndb,
                          response) != 0) {
    ndb->closeTransaction(key_store->m_trans);
    free(key_store);
    return;
  }

  int ret_code = write_key_row_setrange(response,
                                        key_store,
                                        tab,
                                        database_id,
                                        start,
                                        end,
                                        old_tot_value_len);
  if (ret_code != 0) {
    ndb->closeTransaction(key_store->m_trans);
    free(key_store);
    return;
  }
  DEB_SETRANGE(("old_tot_value_len: %u, start: %u, end: %u\n",
    old_tot_value_len,
    start,
    end));
  Uint32 min_num_rows =
    1 + ((end - INLINE_VALUE_LEN) / EXTENSION_VALUE_LEN);
  Uint32 start_index = INLINE_VALUE_LEN;
  bool zero_required = false;
  if (start > old_tot_value_len && start > start_index) {
    /* Zeroing is required from old_total_value_len to start */
    zero_required = true;
  }
  const char *start_write_ptr = value_str;
  for (Uint32 i = 0; i < min_num_rows; i++) {
    /**
     * Default to writing all zeroes
     * If old total value len is larger than start it means we have
     * not introduced any zero filling passages, this is signalled
     * by having start_zero_index == end_zero_index.
     * If start happened in an earlier value row there is also no
     * need for a zeroing effort here.
     */
    Uint32 end_index = start_index + EXTENSION_VALUE_LEN;
    Uint32 start_zero_index = start_index;
    Uint32 end_zero_index = end_index;
    if (zero_required == false) {
      if (start > end_index) {
        /* No need to touch this row */
        start_index = end_index;
        continue;
      }
      start_zero_index = 0;
      end_zero_index = 0;
    } else {
      if (old_tot_value_len > end_index) {
        /* No zeroing required in this round */
        if (start > end_index) {
          /* No need to touch this row */
          start_index = end_index;
          continue;
        }
        start_zero_index = 0;
        end_zero_index = 0;
      } else {
        if (old_tot_value_len < start_index) {
          start_zero_index = 0;
        } else {
          start_zero_index = old_tot_value_len - start_index;
        }
        if (start > end_index) {
          end_zero_index = EXTENSION_VALUE_LEN;
        } else {
          end_zero_index = start - start_index;
          zero_required = false;
        }
      }
    }
    Uint32 start_write_index = 0;
    Uint32 end_write_index = 0;
    if (start <= end_index) {
      if (start < start_index) {
        start_write_index = 0;
      } else {
        start_write_index = start - start_index;
      }
      if (end < end_index) {
        end_write_index = end - start_index;
      } else {
        end_write_index = EXTENSION_VALUE_LEN;
      }
    }
    assert(start_zero_index != end_zero_index ||
           start_write_index != end_write_index);
    bool last_row = (i == (min_num_rows - 1));
    DEB_SETRANGE(("write_value_row_setrange: zero[%u,%u]"
                  " write[%u,%u], last_row: %u\n",
      start_zero_index,
      end_zero_index,
      start_write_index,
      end_write_index,
      last_row));

    ret_code = write_value_row_setrange(
      response,
      key_store,
      i,
      dict,
      start_zero_index,
      end_zero_index,
      start_write_index,
      end_write_index,
      start_write_ptr,
      database_id,
      last_row);
    if (ret_code != 0) {
      ndb->closeTransaction(key_store->m_trans);
      free(key_store);
      return;
    }
    start_index = end_index;
    if (start_write_index != end_write_index) {
      start_write_ptr += (end_write_index - start_write_index);
    }
  }
  Uint32 tot_value_len = std::max(old_tot_value_len, end);
  char buf[20];
  Uint32 header_len = (Uint32)snprintf(buf,
                                       sizeof(buf),
                                       "+%u\r\n",
                                       tot_value_len);
  response->append((const char*)&buf[0], header_len);
  ndb->closeTransaction(key_store->m_trans);
  free(key_store);
  return;
}
