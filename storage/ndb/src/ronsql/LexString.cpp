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

#include "LexString.hpp"

/*
 * LexString is a pointer and length representation of a string. LexCString is
 * the same, except it also guarantees the presence of a terminating NUL byte.
 * Note that the length is that of the contents in both cases, not including
 * the terminating NUL.
 */

LexString::LexString(const char* str, size_t len):
  str(str),
  len(len)
{}
LexString::LexString(const LexCString& other):
  str(other.str),
  len(other.len)
{}

std::ostream&
operator<< (std::ostream& os, const LexString& ls)
{
  os.write(ls.str, ls.len);
  return os;
}

/*
 * Byte by byte comparison. This can still return false for two equivalent UTF-8
 * strings if:
 * 1) The strings are not validated and e.g. one contains an overlong encoded
 *    character.
 * 2) The strings are not normalized to the same degree as your definition for
 *    "equivalent". For example, the grapheme[†] "ä", i.e. the shape that looks
 *    like a small character "a" with two dots above it, can be produced by the
 *    code point U+00e4 (LATIN SMALL LETTER A WITH DIAERESIS), or by a sequence
 *    of the two code points U+0061 (LATIN SMALL LETTER A) and U+0308 (COMBINING
 *    DIAERESIS). Thus, two strings can look the same when printed, yet compare
 *    as unequal. Unicode normalization[‡] is the process of transforming
 *    strings to a canonical form, so that strings that look the same when
 *    printed (are more likely to) compare as equal.
 *    [†] https://unicode.org/glossary/#grapheme
 *    [‡] https://unicode.org/glossary/#normalization
 */
bool
LexString::operator== (const LexString& other) const
{
  if (len != other.len) return false;
  if (len == 0) return true;
  return memcmp(str, other.str, len) == 0;
}
bool
LexCString::operator== (const LexCString& other) const
{
  if (len != other.len) return false;
  if (len == 0) return true;
  return memcmp(str, other.str, len) == 0;
}

/*
 * Return a concatenation of two LexStrings. The lifetime of the returned
 * LexString will end when the lifetime of either argument or the allocator
 * ends.
 */
LexString
LexString::concat(const LexString other, ArenaAllocator* allocator) const
{
  if (this->str == NULL || this->len == 0)
  {
    assert(this->str == NULL && this->len == 0);
    return other;
  }
  size_t concatenated_len = this->len + other.len;
  // It's possible that concatenated_str == this->str. The lifetime of the
  // returned LexString will end when the lifetime of either argument or the
  // allocator ends.
  char* concatenated_str = static_cast<char*>
    (allocator->realloc(this->str, concatenated_len, this->len));
  memcpy(&concatenated_str[this->len], other.str, other.len);
  return LexString{concatenated_str, concatenated_len};
}

/*
 * Convert to LexCString, the lifetime of which will end when the lifetime of
 * the LexString or the allocator ends.
 *
 * WARNING: The LexString must not contain a NUL byte.
 */
LexCString
LexString::to_LexCString(ArenaAllocator* allocator) const
{
  if (this->str == NULL || this->len == 0)
  {
    assert(this->str == NULL && this->len == 0);
    return LexCString{ NULL, 0 };
  }
  char* c_str = static_cast<char*>
    (allocator->realloc(this->str, this->len + 1, this->len));
  c_str[this->len] = '\0';
  return LexCString{ c_str, this->len };
}

const char*
LexCString::c_str() const
{
  return this->str;
}
