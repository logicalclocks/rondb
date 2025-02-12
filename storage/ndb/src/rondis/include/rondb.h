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

#include <stdio.h>
#include "redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#ifndef RONDIS_RONDB_H
#define RONDIS_RONDB_H

#define MAX_NUM_DATABASES 16
extern std::vector<Ndb *> ndb_objects;

int initialize_ndb_objects(const char *connect_string, int num_ndb_objects);

int setup_rondb(const char *connect_string, int num_ndb_objects);

void setup_ndb_connection_for_rondis(
 void* (*get_ndb_object_func_ptr)(int),
 void (*return_ndb_object_func_ptr)(void*, int),
 void (*exit_func)(void),
 int first_thread_id,
 int num_threads,
 int *database_index,
 int num_databases,
 bool *dirty_incr_decr_flag);

void rondb_end();

int rondb_redis_handler(const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int fd);

void set_current_database(int index, int database_index);
#endif
