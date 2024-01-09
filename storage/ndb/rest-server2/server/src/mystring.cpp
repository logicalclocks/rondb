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

#include "mystring.hpp"

#include <ndb_types.h>
#include <stdint.h>
#include <cstring>
#include <string>

/*!
    @brief calculates the extra space to escape a JSON string
    @param[in] s  the string to escape
    @return the number of characters required to escape string @a s
    @complexity Linear in the length of string @a s.
    */
std::size_t extra_space(const std::string &s) noexcept {
  std::size_t result = 0;

  for (const auto &c : s) {
    Int8 ci = (Int8)c;
    switch (ci) {
    case '"':
    case '\\':
    case '\b':
    case '\f':
    case '\n':
    case '\r':
    case '\t': {
      // from c (1 byte) to \x (2 bytes)
      result += 1;
      break;
    }

    default: {
      if (ci >= 0x00 && ci <= 0x1f) {
        // from c (1 byte) to \uxxxx (6 bytes)
        result += 5;
      }
      break;
    }
    }
  }

  return result;
}

// https://github.com/nlohmann/json/blob/ec7a1d834773f9fee90d8ae908a0c9933c5646fc/src/json.hpp#L4604-L4697
/*!
    @brief escape a string
    Escape a string by replacing certain special characters by a sequence of an
    escape character (backslash) and another character and other control
    characters by a sequence of "\u" followed by a four-digit hex
    representation.
    @param[in] s  the string to escape
    @return  the escaped string
    @complexity Linear in the length of string @a s.
    */
std::string escape_string(const std::string &s) noexcept {
  const auto space = extra_space(s);
  if (space == 0) {
    return s;
  }

  // create a result string of necessary size
  std::string result(s.size() + space, '\\');
  std::size_t pos = 0;

  for (size_t i = 0; i < s.size(); i++) {
    const char c = s[i];
    Int8 ci      = (Int8)c;
    switch (ci) {
    // quotation mark (0x22)
    case '"': {
      result[pos + 1] = '"';
      pos += 2;
      break;
    }

    // reverse solidus (0x5c)
    case '\\': {
      // nothing to change
      pos += 2;
      break;
    }

    // backspace (0x08)
    case '\b': {
      result[pos + 1] = 'b';
      pos += 2;
      break;
    }

    // formfeed (0x0c)
    case '\f': {
      result[pos + 1] = 'f';
      pos += 2;
      break;
    }

    // newline (0x0a)
    case '\n': {
      result[pos + 1] = 'n';
      pos += 2;
      break;
    }

    // carriage return (0x0d)
    case '\r': {
      result[pos + 1] = 'r';
      pos += 2;
      break;
    }

    // horizontal tab (0x09)
    case '\t': {
      result[pos + 1] = 't';
      pos += 2;
      break;
    }

    default: {
      if (ci >= 0x00 && ci <= 0x1f) {
        int len = 7;  // print character c as \uxxxx. +1 or null character
        snprintf(&result[pos + 1], len, "u%04x", static_cast<int>(c));
        pos += 6;
        // overwrite trailing null character
        if (i + 1 != s.size()) {
          result[pos] = '\\';
        }
      } else {
        // all other characters are added as-is
        result[pos++] = c;
      }
      break;
    }
    }
  }

  return result;
}

std::string unescape_string(const std::string &s) noexcept {
  std::string result;
  result.reserve(s.size());

  for (size_t i = 0; i < s.size(); ++i) {
    char c = s[i];

    if (c == '\\' && i + 1 < s.size()) {
      char next = s[i + 1];
      switch (next) {
      case '"':
        result += '"';
        break;
      case '\\':
        result += '\\';
        break;
      case 'b':
        result += '\b';
        break;
      case 'f':
        result += '\f';
        break;
      case 'n':
        result += '\n';
        break;
      case 'r':
        result += '\r';
        break;
      case 't':
        result += '\t';
        break;
      case 'u': {
        if (i + 5 < s.size()) {
          // Convert the next 4 characters after "\u" from hex to char
          std::string hex = s.substr(i + 2, 4);
          int ch;
          sscanf(hex.c_str(), "%x", &ch);
          result += static_cast<char>(ch);
          i += 4;  // Skip over the hex digits
        }
        break;
      }
      default:
        result += next;
      }
      i++;  // Skip the next character as it's part of an escape sequence
    } else {
      result += c;
    }
  }

  return result;
}
