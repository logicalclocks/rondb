/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef STORAGE_NDB_SRC_RONSQL_LEXSTRING_HPP
#define STORAGE_NDB_SRC_RONSQL_LEXSTRING_HPP 1

#include <cstddef>
#include <cstring>
#include <iostream>
#include "ArenaMalloc.hpp"

class LexCString;

class LexString
{
public:
  const char *str;
  size_t len;
  LexString() = default;
  LexString(const char* str, size_t len);
  LexString(const LexCString& other);
  LexString& operator= (const LexString& other) = default;
  ~LexString() = default;
  friend std::ostream& operator<< (std::ostream& out, const LexString& ls);
  bool operator== (const LexString& other) const;
  LexString concat(const LexString other, ArenaMalloc* allocator) const;
  LexCString to_LexCString(ArenaMalloc* allocator) const;
};

class LexCString
{
public:
  const char *str;
  size_t len;
  LexCString() = default;
  LexCString(const char* str, size_t len);
  LexCString(const LexCString& other) = default;
  LexCString& operator= (const LexCString& other) = default;
  ~LexCString() = default;
  bool operator== (const LexCString& other) const;
  const char* c_str() const;
};

#endif
