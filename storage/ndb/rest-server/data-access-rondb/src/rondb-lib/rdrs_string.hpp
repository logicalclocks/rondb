/*
 * Copyright (C) 2022 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RONDB_LIB_RDRS_STRING_HPP_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RONDB_LIB_RDRS_STRING_HPP_

#include <stdint.h>
#include <cstring>
#include <string>
#include <NdbApi.hpp>

// function defined in RonDB lib
size_t convert_to_printable(char *to, size_t to_len, const char *from, size_t from_len,
                            const CHARSET_INFO *from_cs, size_t nbytes = 0);

size_t well_formed_copy_nchars(const CHARSET_INFO *to_cs, char *to, size_t to_length,
                               const CHARSET_INFO *from_cs, const char *from, size_t from_length,
                               size_t nchars, const char **well_formed_error_pos,
                               const char **cannot_convert_error_pos, const char **from_end_pos);
#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RONDB_LIB_RDRS_STRING_HPP_
