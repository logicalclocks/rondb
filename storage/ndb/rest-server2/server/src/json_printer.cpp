/*
 * Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
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

#include "json_printer.hpp"
#include "config_structs.hpp"

#include <iostream>

/*
 * Printing utilities
 */

#define INDENT_INCREASE 2

#define DEFINE_PRINTER(ValueDatatype, ...) \
  void printJson(ValueDatatype& value, \
                 std::ostream& out, \
                 [[maybe_unused]] Uint32 indent) __VA_ARGS__

#define INDENT() std::string(indent, ' ')
#define INDENT_INC() std::string(indent + INDENT_INCREASE, ' ')

/*
 * End of printing utilities
 */

/*
 * Printers for simple datatypes
 */

DEFINE_PRINTER(bool, { out << (value ? "true" : "false"); })
DEFINE_PRINTER(Uint64, { out << value; })
DEFINE_PRINTER(Int64, { out << value; })
DEFINE_PRINTER(Uint16, { out << value; })
DEFINE_PRINTER(Uint32, { out << value; })
DEFINE_PRINTER(Int32, { out << value; })

DEFINE_PRINTER(std::string, {
  out << '"';
  for (char c : value) {
    switch (c) {
    case '"': out << "\\\""; break;
    case '\\': out << "\\\\"; break;
    case '\b': out << "\\b"; break;
    case '\f': out << "\\f"; break;
    case '\n': out << "\\n"; break;
    case '\r': out << "\\r"; break;
    case '\t': out << "\\t"; break;
    default:
      if (c < 0x20) {
        constexpr const char* hexdigits = "0123456789abcdef";
        out << "\\u00" << hexdigits[(c >> 4) & 0xf] << hexdigits[c & 0xf];
        break;
      }
      out << c;
      break;
    }
  }
  out << '"';
})

/*
 * Printers for the config structs.
 */

#define CLASS(NAME, ...) \
  DEFINE_PRINTER(NAME, { \
    bool is_first_field = true; \
    out << "{"; \
    __VA_ARGS__ \
    out << '\n' << INDENT() << "}"; \
  })
#define CM(DATATYPE, VARIABLENAME, JSONKEYNAME, INITEXPR) \
  out << (is_first_field ? "\n" : ",\n") \
      << INDENT_INC() << "\"" << #JSONKEYNAME << "\": "; \
  printJson(value.VARIABLENAME, out, indent + INDENT_INCREASE); \
  is_first_field = false;
#define PROBLEM(CONDITION, MESSAGE)
#define CLASSDEFS(...)
#define VECTOR(DATATYPE) \
  DEFINE_PRINTER(std::vector<DATATYPE>, { \
    Uint32 len = value.size(); \
    out << "[\n"; \
    for (Uint32 i = 0; i < len; i++) { \
      out << INDENT_INC(); \
      printJson(value[i], out, indent + INDENT_INCREASE); \
      if (i < len - 1) { \
        out << ","; \
      } \
      out << "\n"; \
    } \
    out << INDENT() << "]"; \
  })

#include "config_structs_def.hpp"

#undef CLASS
#undef CM
#undef PROBLEM
#undef CLASSDEFS
#undef VECTOR
