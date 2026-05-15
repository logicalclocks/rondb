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

// Worker-id tracing: every Rondis worker thread sets g_dbg_worker_id
// at the dispatcher entry (rondb_redis_handler) so all DEB_* lines
// fired further down the call stack can prefix themselves with
// "[w<id>] ". Thread-local so concurrent workers don't clobber each
// other.
extern thread_local int g_dbg_worker_id;

// NDB error code behind the most recent error reply written by the
// assign_*_to_response helpers; 0 for a non-NDB error or no error.
// rondb_redis_handler reads it to decide whether to retry a temporary
// NDB error (e.g. 266, deadlock timeout).
extern thread_local int g_last_ndb_error_code;

// DEB_PREFIX is the leading "[w<id>] " on every DEB_* line. Each
// DEB_* macro emits two printf calls (prefix + arglist) so existing
// DEB_*(("text\n", arg)) call sites keep their format unchanged
// while gaining the worker tag automatically.
#define DEB_PREFIX() printf("[w%d] ", g_dbg_worker_id)

// DEB_CMD: command-level start / end trace, used by the dispatcher
// (rondb_redis_handler). Defined here so all .cc files share the
// same DEBUG_CMD compile flag.
#if (defined(VM_TRACE) || defined(ERROR_INSERT))
#define DEBUG_CMD 1
#endif
#ifdef DEBUG_CMD
#define DEB_CMD(arglist) \
  do { DEB_PREFIX(); printf arglist ; fflush(stdout); } while (0)
#else
#define DEB_CMD(arglist)
#endif

#define RONDIS_MAX_CONNECTIONS 2

#define REDIS_DB_NAME "redis"

#define RONDB_INTERNAL_ERROR 2
#define READ_ERROR 626

int write_formatted(char *buffer, int bufferSize, const char *format, ...);
void assign_err_to_response(std::string *response, const char *app_str, int code);
void assign_ndb_err_to_response(std::string *response, const char *app_str, NdbError error);
void assign_generic_err_to_response(std::string *response, const char *app_str);
void assign_class_err_to_response(std::string *response, const char *class_msg);
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
#define FAILED_INCRBY_DECRBY_PARAMETER "value is not an integer or out of range"
#define FAILED_SELECT_COMMAND "Wrong parameter to SELECT command"
#define FAILED_SELECT_NO_SUCH_DATABASE "The database selected doesn't exist"

// NDB interpreted-code runtime error codes that we surface specially.
// Mirrors entries in storage/ndb/src/kernel/blocks/dbtup/Dbtup.hpp.
// - INVALID_INT64: STR_TO_INT64 cannot parse the stored value as Int64
//   (INCR/DECR/HINCR/HDECR on a non-numeric string).
// - CALC_OVERFLOW:  ADD_REG_REG / SUB_REG_REG detected overflow
//   (INCR/DECR at the INT64_MAX / INT64_MIN boundary).
#define RONDB_INTERP_INVALID_INT64 853
#define RONDB_INTERP_CALC_OVERFLOW 854

// Sentinel emitted by the SET-write interpreted-code program via
// interpret_exit_nok(6000) when an NX or XX guard is tripped. Rondis
// translates this to a nil reply ($-1\r\n) - it is not a real error,
// just "the conditional store could not proceed". See interpreted_code.cc
// lines ~229 (NX-existing) and ~279 (XX-missing).
#define RONDB_CONDITIONAL_STORE_NOT_MET 6000

// Phase 1.10c.1: emitted by init_hset_string_claim_code when a SET
// finds the hset_keys row already taken by a hash (redis_key_id != 0).
// Rondis translates this to "-WRONGTYPE Operation against a key
// holding the wrong kind of value\r\n". Until the silent-replace
// phase (1.10c.6), cross-type writes error rather than overwrite.
#define RONDB_WRONGTYPE 6010

// Redis-canonical error strings we reuse for overflow.
#define FAILED_INCRBY_DECRBY_OVERFLOW \
  "increment or decrement would overflow"

// Redis-canonical WRONGTYPE message. Emitted by SET when the name
// is currently used as a hash (and, in Phase 1.10c.3, by HSET when
// the name is a string).
#define REDIS_WRONGTYPE_VALUE \
  "WRONGTYPE Operation against a key holding the wrong kind of value"

// Redis errors
#define REDIS_UNKNOWN_COMMAND "unknown command '%s'"
#define REDIS_WRONG_NUMBER_OF_ARGS "wrong number of arguments for '%s' command"
#define REDIS_NO_SUCH_KEY "$-1\r\n"
#define REDIS_KEY_TOO_LARGE "key is too large (3000 bytes max)"
#define REDIS_MAX_VALUE_LEN 33586
#define REDIS_VALUE_TOO_LARGE "value too large (33586 bytes max)"
#define REDIS_SYNTAX_ERROR "syntax error"
#define REDIS_INVALID_INTEGER "value is not an integer or out of range"
#define REDIS_OFFSET_OUT_OF_RANGE "offset is out of range"
#define REDIS_INVALID_EXPIRE_TIME "invalid expire time in set"
#endif
