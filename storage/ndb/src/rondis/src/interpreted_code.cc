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
                            Uint64 rondb_key) {
  const NdbDictionary::Column *num_rows_col =
    tab->getColumn(KEY_TABLE_COL_num_rows);
  const NdbDictionary::Column *rondb_key_col =
    tab->getColumn(KEY_TABLE_COL_rondb_key);
  code.load_op_type(REG1); // Read operation type into register 1
  code.branch_eq_const(REG1, RONDB_INSERT, LABEL3); // Inserts go to label3
  /* UPDATE */
  code.read_attr(REG7, num_rows_col);
  code.read_attr(REG6, rondb_key_col);
  code.load_const_u64(REG5, rondb_key);
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
  code.load_const_null(REG3);
  code.write_attr(rondb_key_col, REG3);
  code.interpret_exit_ok();

  code.def_label(LABEL2);
  /* prev_num_rows == 0 and num_rows == 0 */
  code.write_interpreter_output(REG4, OUTPUT_INDEX_1);
  code.interpret_exit_ok();

  /* INSERT */
  code.def_label(LABEL3);
  code.load_const_u16(REG7, 0);
  if (rondb_key != 0) {
    /* Write rondb_key, we have multi row and it is an INSERT */
    code.load_const_u64(REG6, rondb_key);
    code.write_attr(rondb_key_col, REG6);
    code.write_interpreter_output(REG6, OUTPUT_INDEX_1);
  } else {
    code.write_interpreter_output(REG7, OUTPUT_INDEX_1);
  }
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
  return 0;
}

int write_key_row_commit(std::string *response,
                         NdbInterpretedCode &code,
                         const NdbDictionary::Table *tab) {
  const NdbDictionary::Column *num_rows_col = tab->getColumn(KEY_TABLE_COL_num_rows);
  code.load_op_type(REG1);                          // Read operation type into register 1
  code.branch_eq_const(REG1, RONDB_INSERT, LABEL0); // Inserts go to label 0
  /* UPDATE */
  code.read_attr(REG7, num_rows_col);
  code.branch_eq_const(REG7, 0, LABEL0);
  code.interpret_exit_nok(6000);

  /* INSERT */
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
