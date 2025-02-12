/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

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

#ifndef RONDIS_COMMON_H
#define RONDIS_COMMON_H

#define RONDIS_MAX_CONNECTIONS 2

#define REDIS_DB_NAME "redis"

#define RESTRICT_VALUE_ROWS_ERROR 6000

#define RONDB_INTERNAL_ERROR 2
#define READ_ERROR 626

int write_formatted(char *buffer, int bufferSize, const char *format, ...);
void assign_err_to_response(std::string *response, const char *app_str, int code);
void assign_ndb_err_to_response(std::string *response, const char *app_str, NdbError error);
void assign_generic_err_to_response(std::string *response, const char *app_str);
void set_length(char* buf, Uint32 key_len);
Uint32 get_length(char* buf);

// NDB API error messages
#define FAILED_GET_DICT "Failed to get NdbDict"
#define FAILED_CREATE_TABLE_OBJECT "Failed to create table object"
#define FAILED_CREATE_TXN_OBJECT "Failed to create transaction object"
#define FAILED_EXEC_TXN "Failed to execute transaction"
#define FAILED_READ_KEY "Failed to read key"
#define FAILED_INCR_KEY "Failed to increment key"
#define FAILED_HSET_KEY "Failed to find key"
#define FAILED_INCR_KEY_MULTI_ROW "Failed to increment key, multi-row value"
#define FAILED_GET_OP "Failed to get NdbOperation object"
#define FAILED_DEFINE_OP "Failed to define RonDB operation"
#define FAILED_EXECUTE_MGET "Failed to execute MGET operation"
#define FAILED_EXECUTE_MSET "Failed to execute MSET operation"
#define FAILED_EXECUTE_DEL "Failed to execute DEL operation"
#define FAILED_MALLOC "Failed to allocate memory for operation"
#define FAILED_INCRBY_DECRBY_PARAMETER "Wrong parameter, should be Int64"
#define FAILED_SELECT_COMMAND "Wrong parameters to SELECT command"

// Redis errors
#define REDIS_UNKNOWN_COMMAND "unknown command '%s'"
#define REDIS_WRONG_NUMBER_OF_ARGS "wrong number of arguments for '%s' command"
#define REDIS_NO_SUCH_KEY "$-1\r\n"
#define REDIS_KEY_TOO_LARGE "key is too large (3000 bytes max)"
#define REDIS_SYNTAX_ERROR "syntax error"
#define REDIS_INVALID_TTL "invalid expire time in '%s' command"
#endif
