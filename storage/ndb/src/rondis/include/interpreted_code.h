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

#ifndef STRING_INTERPRETED_CODE_H
#define STRING_INTERPRETED_CODE_H

#define RONDB_INSERT 2
#define RONDB_UPDATE 1
#define REG0 0
#define REG1 1
#define REG2 2
#define REG3 3
#define REG4 4
#define REG5 5
#define REG6 6
#define REG7 7
#define LABEL0 0
#define LABEL1 1
#define LABEL2 2
#define LABEL3 3

#define MEMORY_OFFSET_START 0
#define MEMORY_OFFSET_LEN_BYTES 4
#define MEMORY_OFFSET_STRING 6
#define NUM_LEN_BYTES 2
#define INCREMENT_VALUE 1
#define OUTPUT_INDEX_0 0
#define OUTPUT_INDEX_1 1
#define OUTPUT_INDEX_2 2
#define RONDB_KEY_NOT_NULL_ERROR 6000

#define INITIAL_INT_VALUE 1
#define INITIAL_INT_STRING '1'
#define INITIAL_INT_STRING_LEN 1
#define INITIAL_INT_STRING_LEN_WITH_LEN_BYTES 3

int initNdbCodeIncrDecr(std::string *response,
                        NdbInterpretedCode *code,
                        const NdbDictionary::Table *tab,
                        bool incr_flag,
                        Uint64 inc_dec_value);
int write_hset_key_table(Ndb *ndb,
                         const NdbDictionary::Table *tab,
                         std::string std_key_str,
                         Uint64 & redis_key_id,
                         std::string *response,
                         Uint32 database_id);
int write_key_row_commit(std::string *response,
                         NdbInterpretedCode &code,
                         const NdbDictionary::Table *tab,
                         KeyStorage *key_store);
int write_key_row_no_commit(std::string *response,
                            NdbInterpretedCode &code,
                            const NdbDictionary::Table *tab,
                            KeyStorage *key_store);
int simple_delete_key_row_code(std::string *response,
                               NdbInterpretedCode &code,
                               const NdbDictionary::Table *tab);
int simple_write_key_row_setrange(NdbInterpretedCode &code,
                                  const NdbDictionary::Table *tab,
                                  KeyStorage *key_store,
                                  Uint32 start,
                                  Uint32 end);
int write_key_row_setrange(NdbInterpretedCode &code,
                           const NdbDictionary::Table *tab,
                           KeyStorage *key_store,
                           Uint32 start,
                           Uint32 end,
                           Uint64 rondb_key);
int write_value_row_setrange_int(NdbInterpretedCode &code,
                                 const NdbDictionary::Table *value_tab,
                                 Uint32 start_zero_index,
                                 Uint32 end_zero_index,
                                 Uint32 start_write_index,
                                 Uint32 end_write_index,
                                 const char *start_write_ptr);
#endif
