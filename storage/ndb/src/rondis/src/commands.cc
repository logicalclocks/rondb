/*
   Copyright (C) 2024, 2025 Hopsworks AB
 
   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; either version 2
   of the License, or (at your option) any later version.
  
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

//#define DEBUG_MGET_CMD 1
//#define DEBUG_MSET_CMD 1
//#define DEBUG_DEL_CMD 1
//#define DEBUG_HSET_KEY 1
//#define DEBUG_INCR 1
//#define DEBUG_RAND_KEY 1

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
                               key_store->m_num_rw_rows);
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
              Uint32 database_id)
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
  get_ctrl->m_database_id = database_id;
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
    key_storage[i].m_value_size = 0;
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

void rondb_hdel_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id) {
  DEB_DEL_CMD(("HDEL command with %lu parameters", argv.size()));
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
  rondb_del(ndb, argv, response, redis_key_id, worker_id);
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
                                            i);
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

static int set_complex_rows(Ndb *ndb,
                            const NdbDictionary::Table *tab,
                            std::string *response,
                            Uint64 redis_key_id,
                            struct KeyStorage *key_storage,
                            struct GetControl *get_ctrl,
                            Uint32 loop_count,
                            Uint32 current_index) {
  Uint32 num_complex_writes = 0;
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    if (key_storage[inx].m_key_state == KeyState::MultiRow) {
      num_complex_writes++;
      DEB_MGET_CMD(("Start complex write of key id %u\n", inx));
      if (!setup_one_transaction(ndb,
                                 response,
                                 redis_key_id,
                                 &key_storage[inx],
                                 tab)) {
        return 1;
      }
      get_ctrl->m_num_transactions++;
      Uint32 row_state = 0;
      int ret_code = write_data_to_key_op(response,
                                          tab,
                                          &key_storage[inx],
                                          redis_key_id,
                                          false,
                                          row_state,
                                          get_ctrl->m_database_id);
      if (ret_code != 0) {
        return 1;
      }
      prepare_write_transaction(&key_storage[inx]);
    }
  }
  assert(num_complex_writes == get_ctrl->m_num_keys_multi_rows);
  Uint32 current_finished_in_loop = 0;
  get_ctrl->m_num_keys_outstanding = num_complex_writes;
  get_ctrl->m_num_bytes_outstanding =
    num_complex_writes * (sizeof(struct key_table) - MAX_KEY_VALUE_LEN);
  do {
    /**
     * Now send off all prepared and wait for at least one to complete.
     * We cannot wait for multiple ones since we could then run into
     * deadlock issues. The transactions are independent of each other,
     * so if one of them has to wait for a lock, it should not stop
     * other transactions from progressing.
     */
    DEB_MGET_CMD(("Call sendPollNdb with %u keys, %u keys out and %u bytes"
                  " out, current_finished_in_loop: %u\n",
                  get_ctrl->m_num_keys_multi_rows,
                  get_ctrl->m_num_keys_outstanding,
                  get_ctrl->m_num_bytes_outstanding,
                  current_finished_in_loop));
    int min_finished = 1;
    int finished = execute_ndb(ndb, min_finished, __LINE__);
    assert(finished >= 0);
    current_finished_in_loop += finished;
    DEB_MSET_CMD(("Finished serving %u keys, %u remain,"
                  " prepare next batch\n",
      finished, current_finished_in_loop));
    if (get_ctrl->m_num_keys_failed > 0) return 0;
    int ret_code = send_next_write_batch(response,
                                         key_storage,
                                         get_ctrl,
                                         current_index,
                                         loop_count,
                                         current_finished_in_loop);
    if (ret_code != 0) return 1;
  } while (current_finished_in_loop < num_complex_writes);
  return 0;
}

static int set_simple_rows(Ndb *ndb,
                           const NdbDictionary::Table *tab,
                           std::string *response,
                           Uint64 redis_key_id,
                           struct KeyStorage *key_storage,
                           struct GetControl *get_ctrl,
                           Uint32 loop_count,
                           Uint32 current_index) {
  for (Uint32 i = 0; i < loop_count; i++) {
    Uint32 inx = current_index + i;
    key_storage[inx].m_rondb_key = 0;
    Uint32 value_len = key_storage[inx].m_value_size;
    if (value_len > INLINE_VALUE_LEN) {
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
      key_storage[inx].m_key_state = KeyState::MultiRow;
      get_ctrl->m_num_keys_multi_rows++;
      DEB_HSET_KEY(("key %u requires complex write, rondb_key: %llu\n",
        inx, key_storage[inx].m_rondb_key));
      continue;
    }
    DEB_MSET_CMD(("Try simple write with value_len: %u\n", value_len));
    if (!setup_one_transaction(ndb,
                               response,
                               redis_key_id,
                               &key_storage[inx],
                               tab)) {
      return 1;
    }
    get_ctrl->m_num_transactions++;
    Uint32 row_state = 0;
    int ret_code = write_data_to_key_op(response,
                                        tab,
                                        &key_storage[inx],
                                        redis_key_id,
                                        true,
                                        row_state,
                                        get_ctrl->m_database_id);
    if (ret_code != 0) {
      return 1;
    }
    prepare_simple_write_transaction(&key_storage[inx]);
  }
  Uint32 current_finished_in_loop = 0;
  assert(loop_count >= get_ctrl->m_num_keys_multi_rows);
 Uint32 count_finished = loop_count - get_ctrl->m_num_keys_multi_rows;
  get_ctrl->m_num_keys_outstanding = count_finished;
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
  return 0;
}

static
void rondb_mset(Ndb *ndb,
               const pink::RedisCmdArgsType &argv,
               std::string *response,
               Uint64 redis_key_id,
               bool set_command,
               int worker_id) {
  Int64 ttl = -1;
  bool keep_ttl = false;
  Uint32 num_keys = 2;
  enum SetType set_type = IsWrite;
  bool set_ttl = false;
  Uint32 arg_index_start = (redis_key_id == STRING_REDIS_KEY_ID) ? 1 : 2;
  if (set_command &&
      redis_key_id == STRING_REDIS_KEY_ID &&
      argv.size() > 3) {
    Uint32 arg_index = 3;
    const char *arg = argv[arg_index].c_str();
    if (strcasecmp(arg, "nx") == 0) {
      set_type = IsInsert;
      arg_index++;
      arg = argv[arg_index].c_str();
    } else if (strcasecmp(arg, "xx") == 0) {
      set_type = IsUpdate;
      arg_index++;
      arg = argv[arg_index].c_str();
    }
    if (strcasecmp(arg, "get") == 0) {
      arg_index++; //Ignore GET for now
      arg = argv[arg_index].c_str();
    }
    if (strcasecmp(arg, "ex") == 0 && argv.size() > (arg_index + 1)) {
      set_ttl = true;
      std::string opt_val = argv[arg_index + 1];
      ttl = std::stoi(opt_val);
      if (ttl > 0) {
      } else {
        char error_message[256];
        snprintf(error_message,
                 sizeof(error_message),
                 REDIS_INVALID_TTL,
                 argv[0].c_str());
        assign_generic_err_to_response(response, error_message);
        return;
      }
      arg_index += 2;
      arg = argv[arg_index].c_str();
    } else if (strcasecmp(arg, "px") == 0 && argv.size() > (arg_index + 1)) {
      set_ttl = true;
      std::string opt_val = argv[arg_index + 1];
      ttl = std::stoi(opt_val);
      if (ttl > 0) {
        //Convert to seconds
        ttl = (ttl + 999) / 1000;
      } else {
        char error_message[256];
        snprintf(error_message,
                 sizeof(error_message),
                 REDIS_INVALID_TTL,
                 argv[0].c_str());
        assign_generic_err_to_response(response, error_message);
        return;
      }
      arg_index += 2;
      arg = argv[arg_index].c_str();
    }
    if (strcasecmp(arg, "exat") == 0 && argv.size() > (arg_index + 1)) {
      set_ttl = true;
      arg_index += 2; // Ignore for now
    } else if (strcasecmp(arg, "pxat") == 0 && argv.size() > (arg_index + 1)) {
      set_ttl = true;
      arg_index += 2; // Ignore for now
    } else if (strcasecmp(arg, "keepttl") == 0 && argv.size() >
                 arg_index) {
      set_ttl = false;
      keep_ttl = true;
      arg_index += 1;
    }
    if (arg_index != argv.size()) {
      assign_generic_err_to_response(response, REDIS_SYNTAX_ERROR);
      return;
    }
  } else {
    num_keys = argv.size() - arg_index_start;
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
  get_ctrl->m_num_read_errors = 0;
  get_ctrl->m_error_code = 0;
  get_ctrl->m_database_id = get_current_database(worker_id);
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
    if(key_storage[i].m_value_ptr == nullptr) {
      abort();
    }
    memcpy(key_storage[i].m_value_ptr,
           argv[arg_index_val].c_str(),
           argv[arg_index_val].size());
    key_storage[i].m_value_ptr[argv[arg_index_val].size()] = '\0';
    key_storage[i].m_value_size = argv[arg_index_val].size();

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
  DEB_MSET_CMD(("MSET of %u keys\n", num_keys));
  Uint32 current_index = 0;
  do {
    Uint32 loop_count = std::min(num_keys - current_index,
                                 (Uint32)MAX_PARALLEL_KEY_OPS);
    if (get_opt_small_values_flag(worker_id)) {
      int ret_code = set_simple_rows(ndb,
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
      close_finished_transactions(key_storage,
                                  get_ctrl,
                                  loop_count,
                                  current_index);
      DEB_MSET_CMD(("%u keys, %u multi rows, %u completed\n",
                    num_keys,
                    get_ctrl->m_num_keys_multi_rows,
                    get_ctrl->m_num_keys_completed_first_pass));
    }
    /**
     * We have finished the initial round of simple SETs. Now time
     * to handle those that require multi-row SETs. Since we used
     * an optimistic approach we need to start this from scratch
     * again for these new SETs.
     */
    assert(get_ctrl->m_num_transactions == 0);
    assert(get_ctrl->m_num_keys_outstanding == 0);
    if (get_ctrl->m_num_keys_multi_rows > 0 &&
      get_ctrl->m_num_keys_failed == 0) {
      int ret_code = set_complex_rows(ndb,
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
    }
    current_index += loop_count;
  } while (current_index < num_keys && get_ctrl->m_num_keys_failed == 0);
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
  if (redis_key_id == STRING_REDIS_KEY_ID) {
    response->append("+OK\r\n");
  } else {
    char buf[20];
    snprintf(buf,
             sizeof(buf),
             ":%u\r\n",
             get_ctrl->m_num_keys_requested);
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
  rondb_mset(ndb, argv, response, STRING_REDIS_KEY_ID, true, worker_id);
}

void rondb_mset_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id)
{
  rondb_mset(ndb, argv, response, STRING_REDIS_KEY_ID, false, worker_id);
}

void rondb_hset_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id)
{
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
  rondb_mset(ndb, argv, response, redis_key_id, false, worker_id);
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
      malloc(key_store->m_value_size);
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
                                         key_store->m_trans,
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
  if (key_store->m_num_rw_rows == key_store->m_num_rows) {
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
               KeyState::CompletedMultiRowSuccess) {
      get_ctrl->m_num_keys_outstanding++;
      commit_read_value_transaction(&key_storage[inx]);
      assert(current_finished > 0);
      current_finished--;
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
                              key_storage[inx].m_trans,
                              &key_storage[inx].m_key_row,
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
    DEB_MGET_CMD(("Finished serving %u keys, prepare next batch\n",
      finished));
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
                                   tab,
                                   key_storage[inx].m_trans,
                                   &key_storage[inx].m_key_row,
                                   get_ctrl->m_database_id) != 0) {
      return 1;
    }
    prepare_simple_read_transaction(&key_storage[inx]);
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
    key_storage[i].m_header_len = 0;
    key_storage[i].m_first_value_row = 0;
    key_storage[i].m_current_pos = 0;
    key_storage[i].m_num_rows = 0;
    key_storage[i].m_num_rw_rows = 0;
    key_storage[i].m_num_current_rw_rows = 0;
    key_storage[i].m_value_size = 0;
    key_storage[i].m_key_state = KeyState::NotCompleted;
  }
  if (!setup_metadata(ndb,
                      response,
                      &dict,
                      &tab)) {
    release_mget(get_ctrl);
    return;
  }
  DEB_MGET_CMD(("MGET of %u keys\n", num_keys));
  Uint32 current_index = 0;
  do {
    Uint32 loop_count = std::min(num_keys - current_index,
                                 (Uint32)MAX_PARALLEL_KEY_OPS);
    if (get_opt_small_values_flag(worker_id)) {
      int ret_code = get_simple_rows(ndb,
                                     tab,
                                     response,
                                     redis_key_id,
                                     key_storage,
                                     get_ctrl,
                                     loop_count,
                                     current_index);
      if (ret_code != 0) {
        release_mget(get_ctrl);
        return;
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
        release_mget(get_ctrl);
        return;
      }
    }
    current_index += loop_count;
  } while (current_index < num_keys && get_ctrl->m_num_keys_failed == 0);
  /**
   * We are done with the reading process, now it is time to report the
   * result based on the KeyStorage array.
   */
  if (get_ctrl->m_num_keys_failed > 0) {
    assign_err_to_response(response,
                           FAILED_EXECUTE_MGET,
                           get_ctrl->m_error_code);
    release_mget(get_ctrl);
    return;
  }
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
      tot_bytes += key_store->m_value_size;
      key_store->m_header_len = (Uint32)snprintf(
        key_store->m_header_buf,
        sizeof(key_store->m_header_buf),
        "$%u\r\n",
        key_store->m_value_size);
      DEB_MGET_CMD(("Key id %u was of size %u, len: %u\n",
        i, key_store->m_value_size, key_store->m_header_len));
    }
    tot_bytes += 2;
    tot_bytes += key_store->m_header_len;
  }
  char header_buf[20];
  Uint32 header_len = (Uint32)snprintf(header_buf,
                                       sizeof(header_buf),
                                       "*%u\r\n",
                                       num_keys);
  tot_bytes += (Uint32)header_len;
  response->reserve(tot_bytes);
  response->append((const char*)&header_buf[0], header_len);
  for (Uint32 i = 0; i < num_keys; i++) {
    struct KeyStorage *key_store = &key_storage[i];
    response->append((const char*)&key_store->m_header_buf[0],
                     key_store->m_header_len);
    if (key_store->m_key_state == KeyState::CompletedSuccess ||
        key_store->m_key_state == KeyState::CompletedMultiRowSuccess) {
      response->append((const char*)&key_store->m_key_row.value_start[2],
                       key_store->m_value_size);
    }
    else if (key_store->m_key_state == KeyState::CompletedMultiRow) {
      response->append((const char*)key_store->m_value_ptr,
                       key_store->m_value_size);
    } else {
      assert(key_store->m_key_state == KeyState::CompletedFailed);
    }
    response->append("\r\n");
  }
  release_mget(get_ctrl);
  return;
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
                           1);
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
                           1);
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
                           1);
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
                           1);
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
