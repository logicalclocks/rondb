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
#include <stdarg.h>
#include "redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "common.h"

// Per-thread worker id used by every DEB_* line (via DEB_PREFIX in
// common.h). Set at the dispatcher entry rondb_redis_handler before
// any DEB_* call fires; -1 means "not in a command" (e.g. setup
// path) so DEB_* lines emitted there carry [w-1].
thread_local int g_dbg_worker_id = -1;

// NDB error code behind the most recent error reply (see common.h).
thread_local int g_last_ndb_error_code = 0;

//#define DEBUG_ERROR 1

void assign_ndb_err_to_response(
    std::string *response,
    const char *app_str,
    NdbError error)
{
    char buf[512];
    snprintf(buf, sizeof(buf), "-ERR %s; NDB(%u) %s\r\n",
      app_str, error.code, error.message);
    g_last_ndb_error_code = error.code;
#ifdef DEBUG_ERROR
    std::cout << buf;
#endif
    response->assign(buf);
}

void assign_err_to_response(
    std::string *response,
    const char *app_str,
    int code)
{
    char buf[512];
    if (code == 0) {
        // Non-NDB error: don't leak a bogus "; NDB(0)" suffix into
        // replies the user sees (C15). Callers that have no NDB
        // error context now pass 0 instead of the placeholder 1.
        snprintf(buf, sizeof(buf), "-ERR %s\r\n", app_str);
    } else {
        snprintf(buf, sizeof(buf), "-ERR %s; NDB(%u)\r\n", app_str, code);
    }
    g_last_ndb_error_code = code;
#ifdef DEBUG_ERROR
    std::cout << buf;
#endif
    response->assign(buf);
}

void assign_generic_err_to_response(
    std::string *response,
    const char *app_str)
{
    char buf[512];
    snprintf(buf, sizeof(buf), "-ERR %s\r\n", app_str);
    // Non-NDB error: nothing to retry.
    g_last_ndb_error_code = 0;
#ifdef DEBUG_ERROR
    std::cout << buf;
#endif
    response->assign(buf);
}

// Class-prefixed error reply (no `-ERR ` prefix). Used for RESP error
// classes that carry their own keyword: WRONGTYPE, MOVED, ASK, etc.
// Caller passes the full class message, e.g.
// "WRONGTYPE Operation against a key holding the wrong kind of value".
void assign_class_err_to_response(
    std::string *response,
    const char *class_msg)
{
    char buf[512];
    snprintf(buf, sizeof(buf), "-%s\r\n", class_msg);
    // Class error (WRONGTYPE, MOVED, ...): not an NDB temporary error.
    g_last_ndb_error_code = 0;
#ifdef DEBUG_ERROR
    std::cout << buf;
#endif
    response->assign(buf);
}

void set_length(char *buf, Uint32 key_len)
{
    Uint8 *ptr = (Uint8 *)buf;
    ptr[0] = (Uint8)(key_len & 255);
    ptr[1] = (Uint8)(key_len >> 8);
}

Uint32 get_length(char *buf)
{
    Uint8 *ptr = (Uint8 *)buf;
    Uint8 low = ptr[0];
    Uint8 high = ptr[1];
    Uint32 len32 = Uint32(low) + Uint32(256) * Uint32(high);
    return len32;
}
