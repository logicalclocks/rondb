// Copyright (c) 2023, 2024, Oracle and/or its affiliates.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA.

#include "mysql/serialization/archive_text.h"

/// @file

namespace mysql::serialization {

std::string Archive_text::get_raw_data() { return m_stream.str(); }

void Archive_text::put_field_separator() {
  if (is_good() == true) {
    m_stream << std::endl;
    if (m_stream.good() == false) {
      m_error = Serialization_error(
          __FILE__, __LINE__, "Unable to write data to the stream",
          Serialization_error_type::archive_write_error);
    }
  }
}

void Archive_text::put_entry_separator() {
  if (is_good() == true) {
    m_stream << " ";
    if (m_stream.good() == false) {
      m_error = Serialization_error(
          __FILE__, __LINE__, "Unable to write data to the stream",
          Serialization_error_type::archive_write_error);
    }
  }
}

void Archive_text::put_level_separator() {
  if (is_good() == true) {
    m_stream << "  ";
    if (m_stream.good() == false) {
      m_error = Serialization_error(
          __FILE__, __LINE__, "Unable to write data to the stream",
          Serialization_error_type::archive_write_error);
    }
  }
}

}  // namespace mysql::serialization
