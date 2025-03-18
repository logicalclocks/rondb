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

#include "common.h"
#include "commands.h"
#include "interpreted_code.h"
#include "table_definitions.h"
#include <assert.h>
#include <algorithm>

//#define DEBUG_MSET_CMD 1

#ifdef DEBUG_MSET_CMD
#define DEB_MSET_CMD(arglist) do { printf arglist ; } while (0)
#else
#define DEB_MSET_CMD(arglist)
#endif

// Define the interpreted program for the INCR operation
int initNdbCodeIncrDecr(std::string *response,
                        NdbInterpretedCode *code,
                        const NdbDictionary::Table *tab,
                        bool incr_flag,
                        Uint64 inc_dec_value)
{
  const NdbDictionary::Column *value_start_col =
    tab->getColumn(KEY_TABLE_COL_value_start);
  const NdbDictionary::Column *tot_value_len_col =
    tab->getColumn(KEY_TABLE_COL_tot_value_len);
  const NdbDictionary::Column *rondb_key_col =
    tab->getColumn(KEY_TABLE_COL_rondb_key);

  code->load_const_u16(REG0, MEMORY_OFFSET_LEN_BYTES);
  code->load_const_u16(REG6, MEMORY_OFFSET_START);
  code->load_op_type(REG1); // Read operation type into register 1
  code->branch_eq_const(REG1, RONDB_INSERT, LABEL2); // Inserts go to label 1

  /**
   * The first 4 bytes of the memory must be kept for the Attribute header
   * REG0 Memory offset == 4
   * REG1 Memory offset == 6
   * REG2 Size of value_start
   * REG3 Size of value_start without length bytes
   * REG4 Old integer value after conversion
   * REG5 New integer value after increment
   * REG6 Memory offset == 0
   * REG7 Value of rondb_key (should be NULL)
   */
  /* UPDATE code */
  code->read_attr(REG7, rondb_key_col);
  code->branch_eq_null(REG7, LABEL0);
  code->interpret_exit_nok(RONDB_KEY_NOT_NULL_ERROR);
  code->def_label(LABEL0);
  code->read_full(value_start_col, REG6, REG2); // Read value_start column
  code->load_const_u16(REG1, MEMORY_OFFSET_STRING);
  code->sub_const_reg(REG3, REG2, NUM_LEN_BYTES);
  code->str_to_int64(REG4, REG1, REG3); // Convert string to number

  code->def_label(LABEL1);
  code->load_const_u64(REG5, inc_dec_value);
  if (incr_flag) {
    code->add_reg(REG5, REG4, REG5);
  } else {
    code->sub_reg(REG5, REG4, REG5);
  }
  code->int64_to_str(REG3, REG1, REG5);           // Convert number to string
  code->add_const_reg(REG2, REG3, NUM_LEN_BYTES); // New value_start length
  code->write_size_mem(REG3, REG0); // Write back length bytes in memory

  code->write_interpreter_output(REG5, OUTPUT_INDEX_0);
  code->write_from_mem(value_start_col, REG6, REG2);  // Write to column
  code->write_attr(tot_value_len_col, REG3);
  code->interpret_exit_ok();

  /* INSERT code */
  code->def_label(LABEL2);
  code->load_const_u16(REG4, 0);
  code->load_const_u16(REG1, MEMORY_OFFSET_STRING);
  code->branch_label(LABEL1);

  // Program end, now compile code
  int ret_code = code->finalise();
  if (ret_code != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to create Interpreted code",
                               code->getNdbError());
    return -1;
  }
  return 0;
}

int write_hset_key_table(Ndb *ndb,
                         const NdbDictionary::Table *tab,
                         std::string std_key_str,
                         Uint64 & redis_key_id,
                         std::string *response,
                         Uint32 database_id) {
  /* Prepare primary key */
  struct hset_key_table key_row;
  const char *key_str = std_key_str.c_str();
  Uint32 key_len = std_key_str.size();
  set_length(&key_row.redis_key[0], key_len);
  memcpy(&key_row.redis_key[2], key_str, key_len);

  const Uint32 mask = 0x1; // Write primary key
  const unsigned char *mask_ptr = (const unsigned char *)&mask;
  const NdbDictionary::Column *redis_key_id_col =
    tab->getColumn(HSET_KEY_TABLE_COL_redis_key_id);
  Uint32 code_buffer[64];
  NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer));
  code.load_op_type(REG1); // Read operation type into register 1
  code.branch_eq_const(REG1, RONDB_INSERT, LABEL0); // Inserts go to label 0
  /* UPDATE */
  code.read_attr(REG7, redis_key_id_col);
  code.write_interpreter_output(REG7, OUTPUT_INDEX_0);
  code.interpret_exit_ok();

  /* INSERT */
  code.def_label(LABEL0);
  code.load_const_u64(REG7, redis_key_id);
  code.write_attr(redis_key_id_col, REG7);
  code.write_interpreter_output(REG7, OUTPUT_INDEX_0);
  code.interpret_exit_ok();

  // Program end, now compile code
  int ret_code = code.finalise();
  if (ret_code != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to create Interpreted code",
                               code.getNdbError());
    return -1;
  }
  // Prepare the interpreted program to be part of the write
  NdbOperation::OperationOptions opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED_INSERT;
  opts.interpretedCode = &code;

  NdbOperation::GetValueSpec getvals[1];
  getvals[0].appStorage = nullptr;
  getvals[0].recAttr = nullptr;
  getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
  opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
  opts.numExtraGetFinalValues = 1;
  opts.extraGetFinalValues = getvals;

  /* Start a transaction */
  NdbTransaction *trans =
    ndb->startTransaction(tab,
                          (const char*)&key_row.redis_key_id,
                          key_len + 2);
  if (trans == nullptr) {
    assign_ndb_err_to_response(response,
                               "Failed to create transaction object",
                               ndb->getNdbError());
    return -1;
  }
  /* Define the actual operation to be sent to RonDB data node. */
  const NdbOperation *op = trans->writeTuple(
    pk_hset_key_record[database_id],
    (const char *)&key_row,
    entire_hset_key_record[database_id],
    (char *)&key_row,
    mask_ptr,
    &opts,
    sizeof(opts));
  if (op == nullptr) {
    ndb->closeTransaction(trans);
    assign_ndb_err_to_response(response,
                               "Failed to create NdbOperation",
                               trans->getNdbError());
    return -1;
  }
  if (trans->execute(NdbTransaction::Commit,
                     NdbOperation::AbortOnError) != 0 ||
      trans->getNdbError().code != 0) {
    ndb->closeTransaction(trans);
    assign_ndb_err_to_response(response,
                               FAILED_HSET_KEY,
                               trans->getNdbError());
    return -1;
  }
  /* Retrieve the returned new value as an Uint64 value */
  NdbRecAttr *recAttr = getvals[0].recAttr;
  redis_key_id = recAttr->u_64_value();
  ndb->closeTransaction(trans);
  return 0;
}

int write_key_row_no_commit(std::string *response,
                            NdbInterpretedCode &code,
                            const NdbDictionary::Table *tab,
                            KeyStorage *key_store) {
  const NdbDictionary::Column *num_rows_col =
    tab->getColumn(KEY_TABLE_COL_num_rows);
  const NdbDictionary::Column *rondb_key_col =
    tab->getColumn(KEY_TABLE_COL_rondb_key);
  code.load_op_type(REG1); // Read operation type into register 1
  code.branch_eq_const(REG1, RONDB_INSERT, LABEL3); // Inserts go to label3
  /* UPDATE */
  if (key_store->m_set_type == IsInsert) {
    code.interpret_exit_nok(6000);
  } else {
    code.load_const_null(REG3);
    if (key_store->m_keep_ttl == false &&
        key_store->m_set_ttl == false) {
      const NdbDictionary::Column *expiry_date_col =
        tab->getColumn(KEY_TABLE_COL_expiry_date);
      code.write_attr(expiry_date_col, REG3);
    } else if (key_store->m_keep_ttl == true &&
               key_store->m_num_rows > 0) {
      const NdbDictionary::Column *expiry_date_col =
        tab->getColumn(KEY_TABLE_COL_expiry_date);
      code.read_attr(REG7, expiry_date_col);
      code.write_interpreter_output(REG7, OUTPUT_INDEX_2);
    }
    code.read_attr(REG7, num_rows_col);
    code.read_attr(REG6, rondb_key_col);
    code.load_const_u64(REG5, key_store->m_rondb_key);
    code.load_const_u16(REG4, 0);
    code.write_interpreter_output(REG7, OUTPUT_INDEX_0);
    code.branch_eq_null(REG6, LABEL0);
    code.branch_eq_const(REG5, Uint16(0), LABEL1);

    /* prev_num_rows > 0 and num_rows > 0 */
    code.write_interpreter_output(REG6, OUTPUT_INDEX_1);
    code.interpret_exit_ok();

    /* rondb_key NULL => prev_num_rows == 0 */
    code.def_label(LABEL0);
    code.branch_eq_const(REG5, Uint16(0), LABEL2);

    /* prev_num_rows == 0 and num_rows > 0 */
    code.write_interpreter_output(REG5, OUTPUT_INDEX_1);
    code.write_attr(rondb_key_col, REG5);
    code.interpret_exit_ok();

    code.def_label(LABEL1);
    /* prev_num_rows > 0 and num_rows == 0 */
    code.write_interpreter_output(REG6, OUTPUT_INDEX_1);
    code.write_attr(rondb_key_col, REG3);
    code.interpret_exit_ok();

    code.def_label(LABEL2);
    /* prev_num_rows == 0 and num_rows == 0 */
    code.write_interpreter_output(REG4, OUTPUT_INDEX_1);
    code.interpret_exit_ok();
  }
  /* INSERT */
  code.def_label(LABEL3);
  if (key_store->m_set_type == IsUpdate) {
    code.interpret_exit_nok(6000);
  } else {
    code.load_const_u16(REG7, 0);
    if (key_store->m_rondb_key != 0) {
      /* Write rondb_key, we have multi row and it is an INSERT */
      code.load_const_u64(REG6, key_store->m_rondb_key);
      code.write_attr(rondb_key_col, REG6);
      code.write_interpreter_output(REG6, OUTPUT_INDEX_1);
    } else {
      code.write_interpreter_output(REG7, OUTPUT_INDEX_1);
    }
    code.write_interpreter_output(REG7, OUTPUT_INDEX_0);
    code.interpret_exit_ok();
  }
  // Program end, now compile code
  int ret_code = code.finalise();
  if (ret_code != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to create Interpreted code",
                               code.getNdbError());
    return -1;
  }
  return 0;
}

int write_key_row_commit(std::string *response,
                         NdbInterpretedCode &code,
                         const NdbDictionary::Table *tab,
                         KeyStorage *key_store) {
  const NdbDictionary::Column *num_rows_col =
    tab->getColumn(KEY_TABLE_COL_num_rows);
  code.load_op_type(REG1);         // Read operation type into register 1
  code.branch_ne_const(REG1, RONDB_INSERT, LABEL0); // Updates go to label 0
  /* INSERT */
  if (key_store->m_set_type == IsUpdate) {
    code.interpret_exit_nok(6000);
  } else {
    code.interpret_exit_ok();
  }
  /* UPDATE */
  code.def_label(LABEL0);
  if (key_store->m_set_type == IsInsert) {
    DEB_MSET_CMD(("IsInsert on existing row\n"));
    code.interpret_exit_nok(6000);
  } else {
    if (key_store->m_keep_ttl == false &&
        key_store->m_set_ttl == false) {
      const NdbDictionary::Column *expiry_date_col =
        tab->getColumn(KEY_TABLE_COL_expiry_date);
      code.load_const_null(REG3);
      code.write_attr(expiry_date_col, REG3);
    }
    code.read_attr(REG7, num_rows_col);
    code.branch_eq_const(REG7, 0, LABEL1);
    code.interpret_exit_nok(6000);
    code.def_label(LABEL1);
    code.interpret_exit_ok();
  }

  // Program end, now compile code
  int ret_code = code.finalise();
  if (ret_code != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to create Interpreted code",
                               code.getNdbError());
    return -1;
  }
  return 0;
}

int simple_delete_key_row_code(std::string *response,
                               NdbInterpretedCode &code,
                               const NdbDictionary::Table *tab) {
  const NdbDictionary::Column *num_rows_col =
    tab->getColumn(KEY_TABLE_COL_num_rows);
  code.read_attr(REG7, num_rows_col);
  code.branch_eq_const(REG7, 0, LABEL0);
  code.interpret_exit_nok(6000);
  code.def_label(LABEL0);
  code.interpret_exit_ok();

  // Program end, now compile code
  int ret_code = code.finalise();
  if (ret_code != 0) {
    assign_ndb_err_to_response(response,
                               "Failed to create Interpreted code",
                               code.getNdbError());
    return -1;
  }
  return 0;
}

int simple_write_key_row_setrange(NdbInterpretedCode &code,
                                  const NdbDictionary::Table *tab,
                                  KeyStorage *key_store,
                                  Uint32 start,
                                  Uint32 end) {
  const NdbDictionary::Column *num_rows_col =
    tab->getColumn(KEY_TABLE_COL_num_rows);
  const NdbDictionary::Column *value_data_type_col =
    tab->getColumn(KEY_TABLE_COL_value_data_type);
  const NdbDictionary::Column *tot_value_len_col =
    tab->getColumn(KEY_TABLE_COL_tot_value_len);
  const NdbDictionary::Column *value_start_col =
    tab->getColumn(KEY_TABLE_COL_value_start);
  const NdbDictionary::Column *expiry_date_col =
    tab->getColumn(KEY_TABLE_COL_expiry_date);

  /**
   * We construct the memory area to write value_start column in the
   * following manner.
   *
   * Byte 0 - 3:
   * -----------
   * This area is used by the write_from_mem instruction to load the
   * attribute header into. Thus we should not store any data here.
   *
   * Byte 4 - 5:
   * -----------
   * The value_start column is a VARBINARY(4096) column. This means
   * it will have 2 length bytes stored in little-endian format.
   * We set those bytes using write_size_mem instruction.
   *
   * Byte 6 - end of data
   * --------------------
   * Here we will construct the actual data to store in the column.
   */
  code.load_const_u16(REG0, 0);
  code.load_const_u16(REG1, 6);
  code.load_const_u16(REG2, end);
  code.load_const_u16(REG3, start);
  /**
   * REG0 = Offset 0 where write_from_mem memory starts
   * REG1 = Offset 6 where column data starts
   * REG2 = end variable where copying ends
   * REG3 = start variable where we start copying data from memory
   */
  code.load_op_type(REG4);         // Read operation type into register 1
  code.branch_ne_const(REG4, RONDB_INSERT, LABEL0); // Updates go to label 0
  /* INSERT */
  code.write_attr(num_rows_col, REG0);
  code.write_attr(value_data_type_col, REG0);
  code.write_attr(tot_value_len_col, REG2);
  if (start > 0) {
    /* Need to zero area from 0 to start */
    code.bzero(REG1, REG3);
  }
  code.move_reg(REG7, REG2);
  code.branch_label(LABEL2);

  /* UPDATE */
  code.def_label(LABEL0);
  /**
   * Start by reading value_start column into memory
   * We will put the length of the current data into REG7.
   */
  code.load_const_null(REG5);
  code.write_attr(expiry_date_col, REG5);
  code.read_full(value_start_col, REG0, REG7);
  code.sub_const_reg(REG7, REG7, Uint16(2));
  /**
   * No extension of row size is required
   * if end of data >= end
   */
  code.branch_ge(REG7, REG2, LABEL2);

  /* end is after end of current data, new tot_value_len */
  code.write_attr(tot_value_len_col, REG2);
  /**
   * No need to zero any memory area
   * if end of data >= start
   */
  code.branch_ge(REG7, REG3, LABEL1);

  /* Zero area after end of data until before start */
  code.add_reg(REG4, REG1, REG7);
  code.sub_reg(REG5, REG3, REG7);
  code.bzero(REG4, REG5);

  code.def_label(LABEL1);
  code.move_reg(REG7, REG2);

  code.def_label(LABEL2);
  /**
   * REG0 = Offset 0 where write_from_mem memory starts
   * REG1 = Offset 6 where column data starts
   * REG3 = start variable where we start copying data from memory
   * REG7 = Total size of column data after write_from_mem
   */
  code.add_reg(REG6, REG1, REG3);
  code.load_const_mem(REG6, //Offset to copy to
                      REG5, // m_set_value_size will be set here
                      Uint16(key_store->m_set_value_size),
                      key_store->m_value_ptr);
  code.load_const_u16(REG6, 4);
  code.write_size_mem(REG7, REG6);
  code.write_interpreter_output(REG7, OUTPUT_INDEX_0);
  /* Length is size of data + 2 length bytes */
  code.add_const_reg(REG7, REG7, Uint16(2));
  code.write_from_mem(value_start_col, REG0, REG7);
  code.interpret_exit_ok();

  // Program end, now compile code
  int ret_code = code.finalise();
  if (ret_code != 0) {
    return code.getNdbError().code;
  }
  return 0;
}

int write_key_row_setrange_int(NdbInterpretedCode &code,
                               const NdbDictionary::Table *tab,
                               KeyStorage *key_store,
                               Uint32 start,
                               Uint32 end,
                               Uint64 rondb_key) {
  const NdbDictionary::Column *num_rows_col =
    tab->getColumn(KEY_TABLE_COL_num_rows);
  const NdbDictionary::Column *value_data_type_col =
    tab->getColumn(KEY_TABLE_COL_value_data_type);
  const NdbDictionary::Column *tot_value_len_col =
    tab->getColumn(KEY_TABLE_COL_tot_value_len);
  const NdbDictionary::Column *value_start_col =
    tab->getColumn(KEY_TABLE_COL_value_start);
  const NdbDictionary::Column *expiry_date_col =
    tab->getColumn(KEY_TABLE_COL_expiry_date);
  const NdbDictionary::Column *rondb_key_col =
    tab->getColumn(KEY_TABLE_COL_rondb_key);

  Uint32 min_num_rows =
    1 + ((end - INLINE_VALUE_LEN) / EXTENSION_VALUE_LEN);
  /**
   * We construct the memory area to write value_start column in the
   * following manner.
   *
   * Byte 0 - 3:
   * -----------
   * This area is used by the write_from_mem instruction to load the
   * attribute header into. Thus we should not store any data here.
   *
   * Byte 4 - 5:
   * -----------
   * The value_start column is a VARBINARY(4096) column. This means
   * it will have 2 length bytes stored in little-endian format.
   * We set those bytes using write_size_mem instruction.
   *
   * Byte 6 - end of data
   * --------------------
   * Here we will construct the actual data to store in the column.
   */
  code.load_const_u16(REG0, 0);
  code.load_const_u16(REG1, 6);
  code.load_const_u16(REG2, INLINE_VALUE_LEN);
  code.load_const_u32(REG3, std::min(Uint32(INLINE_VALUE_LEN), start));
  code.load_const_u32(REG4, end);
  /**
   * REG0 = Offset 0 where write_from_mem memory starts
   * REG1 = Offset 6 where column data starts
   * REG2 = end variable where copying ends
   * REG3 = start variable where we start copying data from memory
   */
  code.load_op_type(REG5);         // Read operation type into register 1
  code.branch_ne_const(REG5, RONDB_INSERT, LABEL0); // Updates go to label 0
  /* INSERT */
  code.load_const_u64(REG7, rondb_key);
  code.load_const_u16(REG5, min_num_rows);
  code.write_attr(tot_value_len_col, REG4);
  code.write_attr(num_rows_col, REG5);
  code.write_attr(value_data_type_col, REG0);
  code.write_attr(rondb_key_col, REG7);
  code.bzero(REG1, REG2);
  code.load_const_u16(REG5, 0);
  code.branch_label(LABEL3);

  /* UPDATE */
  code.def_label(LABEL0);
  /**
   * Start by reading value_start column into memory
   * We will put the length of the current data into REG7.
   */
  code.read_attr(REG7, rondb_key_col);
  code.branch_ne_null(REG7, LABEL1);

  code.load_const_u64(REG7, rondb_key);
  code.write_attr(rondb_key_col, REG7);

  code.def_label(LABEL1);
  code.load_const_null(REG5);
  code.write_attr(expiry_date_col, REG5);
  code.read_attr(REG5, tot_value_len_col);
  code.branch_le(REG4, REG5, LABEL2);

  code.write_attr(tot_value_len_col, REG4);

  code.def_label(LABEL2);
  code.read_full(value_start_col, REG0, REG6);
  code.sub_const_reg(REG6, REG6, Uint16(2));
  code.branch_ge(REG6, REG3, LABEL3);

  /* end is after end of current data, new tot_value_len */
  /**
   * No need to zero any memory area
   * if end of data >= start
   */

  /* Zero area after end of data until before start */
  code.add_reg(REG4, REG1, REG6);
  code.sub_reg(REG6, REG3, REG6);
  code.bzero(REG4, REG6);

  code.def_label(LABEL3);
  /**
   * REG0 = Offset 0 where write_from_mem memory starts
   * REG1 = Offset 6 where column data starts
   * REG2 = Total size of column data after write_from_mem
   * REG3 = start variable where we start copying data from memory
   * REG4 = Not used
   * REG5 = old_tot_value_len
   * REG6 = Not used
   * REG7 = rondb_key
   */
  code.write_interpreter_output(REG5, OUTPUT_INDEX_0);
  code.write_interpreter_output(REG7, OUTPUT_INDEX_1);
  if (start < INLINE_VALUE_LEN) {
    code.add_reg(REG6, REG1, REG3);
    Uint32 size_load = (INLINE_VALUE_LEN - start);
    code.load_const_mem(REG6, //Offset to copy to
                        REG4, // m_set_value_size will be set here
                        Uint16(size_load),
                        key_store->m_value_ptr);
  }
  code.load_const_u16(REG6, 4);
  code.write_size_mem(REG2, REG6);
  /* Length is size of data + 2 length bytes */
  code.add_const_reg(REG2, REG2, Uint16(2));
  code.write_from_mem(value_start_col, REG0, REG2);
  code.interpret_exit_ok();

  // Program end, now compile code
  int ret_code = code.finalise();
  if (ret_code != 0) {
    return code.getNdbError().code;
  }
  return 0;
}

int write_value_row_setrange_int(NdbInterpretedCode &code,
                           const NdbDictionary::Table *value_tab,
                           Uint32 start_zero_index,
                           Uint32 end_zero_index,
                           Uint32 start_write_index,
                           Uint32 end_write_index,
                           const char *start_write_ptr) {
  const NdbDictionary::Column *value_col =
    value_tab->getColumn(VALUE_TABLE_COL_value);
  Uint32 write_len = 0;
  Uint32 start_index = 0;
  /* Position of memory used to write partial columns */
  code.load_const_u16(REG0, 0);
  /* Start position of actual data to be partially written to row */
  code.load_const_u16(REG1, 10);
  /* Memory position of length bytes in Interpreter memory */
  code.load_const_u16(REG2, 8);
  if (start_zero_index != end_zero_index) {
    start_index = start_zero_index;
    assert(end_zero_index > start_zero_index);
    write_len += (end_zero_index - start_zero_index);
    code.load_const_u16(REG3, write_len);
    code.bzero(REG1, REG3);
    code.add_reg(REG1, REG1, REG3);
  }
  if (start_write_index != end_write_index) {
    if (start_zero_index == end_zero_index) {
      start_index = start_write_index;
    }
    assert(end_write_index > start_write_index);
    Uint32 write_value_len = (end_write_index - start_write_index);
    write_len += write_value_len;
    code.load_const_mem(REG1,
                        REG4,
                        Uint16(write_value_len),
                        start_write_ptr);
  }
  code.load_const_u16(REG3, write_len);
  code.write_size_mem(REG3, REG2);
  /* Length is size of data + 2 length bytes */
  code.add_const_reg(REG3, REG3, Uint16(2));
  code.load_const_u16(REG4, start_index + 2);
  code.write_partial_from_mem(value_col, REG0, REG3, REG4);
  code.interpret_exit_ok();
  // Program end, now compile code
  int ret_code = code.finalise();
  if (ret_code != 0) {
    return code.getNdbError().code;
  }
  return 0;
}
