/*
 * Copyright (c) 2026, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_CAPPED_OSTREAM_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_CAPPED_OSTREAM_HPP_

#include <cstddef>
#include <ostream>
#include <streambuf>
#include <string>
#include <utility>

/*
 * CappedOStream — an std::ostream accumulating into a std::string with a
 * hard byte cap (cap == 0 means unlimited).  Once a write would push the
 * accumulated size past the cap, the write is refused: xsputn()/overflow()
 * report failure, the ostream machinery sets badbit, and every later write
 * silently no-ops while the `exceeded` flag latches.  Deliberately NO
 * exceptions: producers that never inspect stream state (RonSQL's result
 * printers write via operator<< only and check nothing) keep running with
 * BOUNDED memory, and nothing escapes into catch-all handlers that abort
 * the process (db_operations/ronsql/ronsql_operation.cpp).  The caller
 * checks exceeded() after the producer returns and converts it into a
 * clean error; on success take() moves the body out without a copy
 * (pairs with HttpResponse::setBody(std::string&&)).
 *
 * Used to enforce Internal.MaxRespSize on the /ronsql endpoint (see
 * config_structs_def.hpp).
 */
class CappedStringBuf : public std::streambuf {
 public:
  CappedStringBuf(size_t cap, size_t reserve_hint)
      : m_cap(cap), m_exceeded(false) {
    if (reserve_hint > 0) {
      size_t r = reserve_hint;
      if (m_cap != 0 && m_cap < r) r = m_cap;
      m_data.reserve(r);
    }
  }
  bool exceeded() const { return m_exceeded; }
  const std::string &str() const { return m_data; }
  std::string take() { return std::move(m_data); }
  size_t size() const { return m_data.size(); }

 protected:
  std::streamsize xsputn(const char *s, std::streamsize n) override {
    if (m_exceeded) return 0;
    if (m_cap != 0 && m_data.size() + static_cast<size_t>(n) > m_cap) {
      m_exceeded = true;
      return 0;  // short write => the ostream sets badbit
    }
    m_data.append(s, static_cast<size_t>(n));
    return n;
  }
  int_type overflow(int_type ch) override {
    if (traits_type::eq_int_type(ch, traits_type::eof())) {
      return traits_type::not_eof(ch);
    }
    if (m_exceeded || (m_cap != 0 && m_data.size() >= m_cap)) {
      m_exceeded = true;
      return traits_type::eof();  // => badbit
    }
    m_data.push_back(traits_type::to_char_type(ch));
    return ch;
  }

 private:
  std::string m_data;
  size_t m_cap;
  bool m_exceeded;
};

class CappedOStream : public std::ostream {
 public:
  explicit CappedOStream(size_t cap, size_t reserve_hint = 64 * 1024)
      : std::ostream(nullptr), m_buf(cap, reserve_hint) {
    rdbuf(&m_buf);
  }
  bool exceeded() const { return m_buf.exceeded(); }
  const std::string &str() const { return m_buf.str(); }
  std::string take() { return m_buf.take(); }
  size_t size() const { return m_buf.size(); }

 private:
  CappedStringBuf m_buf;
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_CAPPED_OSTREAM_HPP_
