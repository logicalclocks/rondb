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

#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>
#include "db_operations.h"

#ifndef STRING_COMMANDS_H
#define STRING_COMMANDS_H
/*
    All STRING commands:
    https://redis.io/docs/latest/commands/?group=string

    STYLE GUIDE:
    From a RonDB perspective, this file is where transactions are created and
    removed again. The data operations inbetween are done in db_operations.
    This avoids redundant calls to `ndb->closeTransaction(trans);` and therefore
    also reduces the risk to forget calling this function (causing memory
    leaks).

    The db_operations level however handles most of the low-level NdbError
    handling. Most importantly, it writes Ndb error messages to the response
    string. This may however change in the future, since this causes redundancy.
*/
void rondb_get_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int worker_id);

void rondb_mget_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id);

void rondb_set_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int worker_id);

void rondb_del_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int worker_id);

void rondb_hdel_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id);

void rondb_mset_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id);

void rondb_incr_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id);

void rondb_incrby_command(Ndb *ndb,
                          const pink::RedisCmdArgsType &argv,
                          std::string *response,
                          int worker_id);

void rondb_decr_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id);

void rondb_decrby_command(Ndb *ndb,
                          const pink::RedisCmdArgsType &argv,
                          std::string *response,
                          int worker_id);

void rondb_hget_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int worker_id);

void rondb_hmget_command(Ndb *ndb,
                         const pink::RedisCmdArgsType &argv,
                         std::string *response,
                         int worker_id);

void rondb_hset_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int worker_id);

void rondb_hincr_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id);

void rondb_hincrby_command(Ndb *ndb,
                           const pink::RedisCmdArgsType &argv,
                           std::string *response,
                           int worker_id);

void rondb_hdecr_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id);

void rondb_hdecrby_command(Ndb *ndb,
                           const pink::RedisCmdArgsType &argv,
                           std::string *response,
                           int worker_id);

void rondb_strlen_command(Ndb *ndb,
                          const pink::RedisCmdArgsType &argv,
                          std::string *response,
                          int worker_id);

void rondb_getrange_command(Ndb *ndb,
                            const pink::RedisCmdArgsType &argv,
                            std::string *response,
                            int worker_id);
#endif
