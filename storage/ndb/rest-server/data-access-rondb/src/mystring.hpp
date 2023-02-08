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

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_MYSTRING_HPP_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_MYSTRING_HPP_

#include <string>
#include <ndb_types.h>
/*!
    @brief calculates the extra space to escape a JSON string
    @param[in] s  the string to escape
    @return the number of characters required to escape string @a s
    @complexity Linear in the length of string @a s.
    */
std::size_t extra_space(const Int8 *str, std::size_t length) noexcept;

const Int8 *escape_string(const Int8 *str, std::size_t *length) noexcept;

#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_MYSTRING_HPP_
