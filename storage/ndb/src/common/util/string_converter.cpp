/*
   Copyright (c) 2003, 2023, Oracle and/or its affiliates.
   Copyright (c) 2023, 2024, Hopsworks and/or its affiliates.

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

#include <ndb_global.h>
bool convert_string_to_uint64(const char* s, 
                              Uint64& val,
                              bool allow_negative)
{
  if (s == nullptr)
    return false;

  while (*s != '\0' && isspace(*s)) s++;
  const bool negative = (*s == '-');

  union {
    signed long long vs;
    unsigned long long vu;
  };
  char* p;
  constexpr int log10base = 0;
  errno = 0;
  if (negative)
  {
    if (!allow_negative)
    {
      return false;
    }
    vs = std::strtoll(s, &p, log10base);
    if ((vs == LLONG_MIN || vs == LLONG_MAX) && errno == ERANGE)
    {
      return false;
    }
  }
  else
  {
    vu = std::strtoull(s, &p, log10base);
    if (vu == ULLONG_MAX && errno == ERANGE)
    {
      return false;
    }
  }
  if (p == s)
    return false;

  int mul = 0;

  switch(*p)
  {
  case '\0':
    break;
  case 'k':
  case 'K':
    mul = 10;
    p++;
    break;
  case 'M':
    mul = 20;
    p++;
    break;
  case 'G':
    mul = 30;
    p++;
    break;
  case 'T':
    mul = 40;
    p++;
    break;
  default:
    return false;
  }
  if (*p != '\0')
    return false;
  if (negative)
  {
    Int64 v = (vs << mul);
    if ((v >> mul) != vs)
      return false;
    val = v;
  }
  else
  {
    Uint64 v = (vu << mul);
    if ((v >> mul) != vu)
      return false;
    val = v;
  }
  return true;
}
