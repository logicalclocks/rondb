/*
 * Copyright (C) 2023 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RETRY_HANDLER_HPP_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RETRY_HANDLER_HPP_

#include <ndb_types.h>

extern Uint32 OP_RETRY_COUNT;
extern Uint32 OP_RETRY_INITIAL_DELAY_IN_MS; 
extern Uint32 OP_RETRY_JITTER_IN_MS;

// expects one or more lines of code that set a variable "status" of type RS_STATUS

#define RETRY_HANDLER(my_src)                                                                      \
  Uint32 orc = 0;                                                                                  \
  do {                                                                                             \
    my_src                                                                                         \
    orc++;                                                                                         \
    if (status.http_code == SUCCESS || orc > OP_RETRY_COUNT || !CanRetryOperation(status)) {       \
      break;                                                                                       \
    }                                                                                              \
    usleep(ExponentialDelayWithJitter(orc, OP_RETRY_INITIAL_DELAY_IN_MS, OP_RETRY_JITTER_IN_MS) *  \
           1000);                                                                                  \
    LOG_DEBUG("Retrying failed operation. Code: "+std::to_string(status.code));                    \
  } while (true);

#endif  //STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RETRY_HANDLER_HPP_
