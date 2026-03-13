/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
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

#include "mysql_protocol.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <netdb.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace mysql_protocol {

bool read_exact(int fd, char* buf, size_t len) {
  size_t total = 0;
  while (total < len) {
    ssize_t n = read(fd, buf + total, len - total);
    if (n > 0) {
      total += n;
    } else if (n == 0) {
      return false;
    } else {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        if (errno != EINTR) usleep(1000);
        continue;
      }
      return false;
    }
  }
  return true;
}

bool read_packet(int fd, std::string& out) {
  char header[HEADER_SIZE];
  if (!read_exact(fd, header, HEADER_SIZE)) {
    return false;
  }
  uint32_t payload_len = packet_length(header);
  out.append(header, HEADER_SIZE);
  if (payload_len > 0) {
    size_t offset = out.size();
    out.resize(offset + payload_len);
    if (!read_exact(fd, &out[offset], payload_len)) {
      return false;
    }
  }
  return true;
}

bool write_all(int fd, const char* data, size_t len) {
  size_t total = 0;
  while (total < len) {
    ssize_t n = write(fd, data + total, len - total);
    if (n > 0) {
      total += n;
    } else if (n < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        if (errno != EINTR) usleep(1000);
        continue;
      }
      return false;
    }
  }
  return true;
}

uint64_t read_lenenc_int(const char*& pos, const char* end) {
  if (pos >= end) return 0;
  uint8_t first = (uint8_t)*pos++;
  if (first < 0xFB) {
    return first;
  } else if (first == 0xFC) {
    if (pos + 2 > end) return 0;
    uint64_t val = (uint64_t)(uint8_t)pos[0] |
                   ((uint64_t)(uint8_t)pos[1] << 8);
    pos += 2;
    return val;
  } else if (first == 0xFD) {
    if (pos + 3 > end) return 0;
    uint64_t val = (uint64_t)(uint8_t)pos[0] |
                   ((uint64_t)(uint8_t)pos[1] << 8) |
                   ((uint64_t)(uint8_t)pos[2] << 16);
    pos += 3;
    return val;
  } else if (first == 0xFE) {
    if (pos + 8 > end) return 0;
    uint64_t val = 0;
    for (int i = 0; i < 8; i++) {
      val |= ((uint64_t)(uint8_t)pos[i] << (i * 8));
    }
    pos += 8;
    return val;
  }
  // 0xFF is not a valid lenenc first byte
  return 0;
}

uint16_t extract_ok_status_flags(const char* pkt, size_t pkt_len) {
  // OK packet layout (after header):
  //   marker(1) = 0x00, affected_rows(lenenc), last_insert_id(lenenc),
  //   status_flags(2), warnings(2), ...
  if (pkt_len < HEADER_SIZE + 1) return 0;
  uint8_t marker = (uint8_t)pkt[HEADER_SIZE];
  if (marker != OK_MARKER) return 0;

  const char* pos = pkt + HEADER_SIZE + 1;
  const char* end = pkt + pkt_len;

  // Skip affected_rows (lenenc int)
  read_lenenc_int(pos, end);
  // Skip last_insert_id (lenenc int)
  read_lenenc_int(pos, end);

  // Read status_flags (2 bytes, little-endian)
  if (pos + 2 > end) return 0;
  uint16_t status_flags = (uint16_t)(uint8_t)pos[0] |
                           ((uint16_t)(uint8_t)pos[1] << 8);
  return status_flags;
}

static bool is_eof_packet(const char* pkt_start, uint32_t payload_len) {
  // EOF packet: marker 0xFE with payload length <= 5 bytes
  if (payload_len >= 1 && payload_len <= 5) {
    uint8_t marker = (uint8_t)pkt_start[HEADER_SIZE];
    return marker == EOF_MARKER;
  }
  return false;
}

static bool is_ok_packet(const char* pkt_start, uint32_t payload_len) {
  if (payload_len >= 1) {
    uint8_t marker = (uint8_t)pkt_start[HEADER_SIZE];
    return marker == OK_MARKER;
  }
  return false;
}

static bool is_err_packet(const char* pkt_start, uint32_t payload_len) {
  if (payload_len >= 1) {
    uint8_t marker = (uint8_t)pkt_start[HEADER_SIZE];
    return marker == ERR_MARKER;
  }
  return false;
}

bool read_response(int fd, std::string& out, uint32_t client_capabilities) {
  // Read the first packet
  size_t start_offset = out.size();
  if (!read_packet(fd, out)) {
    return false;
  }

  const char* first_pkt = out.data() + start_offset;
  uint32_t first_payload_len = packet_length(first_pkt);

  if (first_payload_len == 0) return true;

  uint8_t first_byte = (uint8_t)first_pkt[HEADER_SIZE];
  if (first_byte == OK_MARKER) return true;
  if (first_byte == ERR_MARKER) return true;
  if (first_byte == EOF_MARKER && first_payload_len <= 5) return true;
  if (first_byte == LOCAL_INFILE_MARKER) return true;

  // Result set: first packet is column_count (lenenc int)
  const char* pos = first_pkt + HEADER_SIZE;
  const char* end = first_pkt + HEADER_SIZE + first_payload_len;
  uint64_t column_count = read_lenenc_int(pos, end);

  bool deprecate_eof = (client_capabilities & CLIENT_DEPRECATE_EOF) != 0;

  // Read column definition packets
  if (!deprecate_eof) {
    while (true) {
      size_t pkt_start = out.size();
      if (!read_packet(fd, out)) return false;
      uint32_t plen = packet_length(out.data() + pkt_start);
      if (is_eof_packet(out.data() + pkt_start, plen)) break;
      if (is_err_packet(out.data() + pkt_start, plen)) return true;
    }
  } else {
    // With CLIENT_DEPRECATE_EOF, read exactly column_count definition packets
    for (uint64_t i = 0; i < column_count; i++) {
      if (!read_packet(fd, out)) return false;
    }
  }

  // Read row packets until EOF/OK/ERR
  while (true) {
    size_t pkt_start = out.size();
    if (!read_packet(fd, out)) return false;
    uint32_t plen = packet_length(out.data() + pkt_start);

    if (is_err_packet(out.data() + pkt_start, plen)) return true;

    if (!deprecate_eof) {
      if (is_eof_packet(out.data() + pkt_start, plen)) return true;
    } else {
      // With CLIENT_DEPRECATE_EOF, the termination packet is an OK_Packet
      // with header 0x00 or 0xFE (any payload length — not limited to 5).
      if (is_ok_packet(out.data() + pkt_start, plen)) return true;
      uint8_t m = (uint8_t)(out.data() + pkt_start)[HEADER_SIZE];
      if (m == EOF_MARKER) return true;
    }
  }
}

int connect_to_backend(const char* host, uint16_t port) {
  struct addrinfo hints;
  struct addrinfo* result = nullptr;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  char port_str[8];
  snprintf(port_str, sizeof(port_str), "%u", port);

  int ret = getaddrinfo(host, port_str, &hints, &result);
  if (ret != 0) {
    printf("MySQL router: getaddrinfo(%s:%u) failed: %s\n",
           host, port, gai_strerror(ret));
    return -1;
  }

  int fd = -1;
  for (struct addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
    fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (fd < 0) continue;

    if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
      break;
    }
    close(fd);
    fd = -1;
  }
  freeaddrinfo(result);

  if (fd < 0) {
    printf("MySQL router: connect to backend %s:%u failed: %s\n",
           host, port, strerror(errno));
    return -1;
  }

  int flag = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

  return fd;
}

void write_lenenc_int(std::string& out, uint64_t val) {
  if (val < 251) {
    out.push_back((char)(uint8_t)val);
  } else if (val < (1 << 16)) {
    out.push_back((char)0xFC);
    out.push_back((char)(val & 0xFF));
    out.push_back((char)((val >> 8) & 0xFF));
  } else if (val < (1 << 24)) {
    out.push_back((char)0xFD);
    out.push_back((char)(val & 0xFF));
    out.push_back((char)((val >> 8) & 0xFF));
    out.push_back((char)((val >> 16) & 0xFF));
  } else {
    out.push_back((char)0xFE);
    for (int i = 0; i < 8; i++) {
      out.push_back((char)((val >> (i * 8)) & 0xFF));
    }
  }
}

void write_lenenc_string(std::string& out, const char* s, size_t len) {
  write_lenenc_int(out, len);
  out.append(s, len);
}

void write_packet_header(char* buf, uint32_t payload_len, uint8_t seq) {
  buf[0] = (char)(payload_len & 0xFF);
  buf[1] = (char)((payload_len >> 8) & 0xFF);
  buf[2] = (char)((payload_len >> 16) & 0xFF);
  buf[3] = (char)seq;
}

void build_ok_packet(std::string& out, uint8_t seq) {
  // Minimal OK packet: marker(1) + affected_rows(1) + last_insert_id(1)
  //                    + status_flags(2) + warnings(2) = 7 bytes
  uint32_t payload_len = 7;
  char header[HEADER_SIZE];
  write_packet_header(header, payload_len, seq);
  out.append(header, HEADER_SIZE);
  out.push_back((char)OK_MARKER);  // marker
  out.push_back(0);                // affected_rows = 0
  out.push_back(0);                // last_insert_id = 0
  out.push_back(0x02);             // status: SERVER_STATUS_AUTOCOMMIT
  out.push_back(0);                // status high byte
  out.push_back(0);                // warnings low
  out.push_back(0);                // warnings high
}

void build_eof_packet(std::string& out, uint8_t seq) {
  // EOF packet: marker(1) + warnings(2) + status_flags(2) = 5 bytes
  uint32_t payload_len = 5;
  char header[HEADER_SIZE];
  write_packet_header(header, payload_len, seq);
  out.append(header, HEADER_SIZE);
  out.push_back((char)EOF_MARKER);
  out.push_back(0);     // warnings low
  out.push_back(0);     // warnings high
  out.push_back(0x02);  // status: SERVER_STATUS_AUTOCOMMIT
  out.push_back(0);     // status high byte
}

// Build a column definition packet (COM_QUERY response, protocol 41)
static void build_column_def_packet(std::string& out, uint8_t seq,
                                    const char* name, size_t name_len) {
  std::string payload;
  // catalog "def"
  write_lenenc_string(payload, "def", 3);
  // schema (empty)
  write_lenenc_string(payload, "", 0);
  // table (empty)
  write_lenenc_string(payload, "", 0);
  // org_table (empty)
  write_lenenc_string(payload, "", 0);
  // name
  write_lenenc_string(payload, name, name_len);
  // org_name (same as name)
  write_lenenc_string(payload, name, name_len);
  // length of fixed-length fields [0c]
  payload.push_back(0x0C);
  // character_set: utf8_general_ci = 33 (0x21)
  payload.push_back(0x21);
  payload.push_back(0x00);
  // column_length: 255 (arbitrary, for display)
  payload.push_back((char)0xFF);
  payload.push_back(0x00);
  payload.push_back(0x00);
  payload.push_back(0x00);
  // column_type: MYSQL_TYPE_VAR_STRING = 253
  payload.push_back((char)0xFD);
  // flags: 0
  payload.push_back(0x00);
  payload.push_back(0x00);
  // decimals: 0
  payload.push_back(0x00);
  // filler
  payload.push_back(0x00);
  payload.push_back(0x00);

  char header[HEADER_SIZE];
  write_packet_header(header, (uint32_t)payload.size(), seq);
  out.append(header, HEADER_SIZE);
  out.append(payload);
}

// Build a row packet from tab-separated values
static void build_row_packet(std::string& out, uint8_t seq,
                             const std::vector<std::string>& values) {
  std::string payload;
  for (const auto& val : values) {
    if (val == "NULL" || val == "null") {
      payload.push_back((char)0xFB);  // NULL marker
    } else {
      write_lenenc_string(payload, val.data(), val.size());
    }
  }
  char header[HEADER_SIZE];
  write_packet_header(header, (uint32_t)payload.size(), seq);
  out.append(header, HEADER_SIZE);
  out.append(payload);
}

// Split a line by tab character
static std::vector<std::string> split_tab(const char* start, const char* end) {
  std::vector<std::string> result;
  const char* p = start;
  const char* field_start = p;
  while (p < end) {
    if (*p == '\t') {
      result.emplace_back(field_start, p - field_start);
      field_start = p + 1;
    }
    p++;
  }
  result.emplace_back(field_start, p - field_start);
  return result;
}

uint8_t build_result_set_from_tsv(const std::string& tsv,
                                  std::string& out,
                                  uint8_t start_seq) {
  uint8_t seq = start_seq;

  if (tsv.empty()) {
    // Empty result — return OK packet
    build_ok_packet(out, seq++);
    return seq;
  }

  // Parse TSV: first line is headers, rest are rows
  std::vector<std::vector<std::string>> rows;
  std::vector<std::string> headers;

  const char* data = tsv.data();
  size_t data_len = tsv.size();
  const char* line_start = data;

  for (size_t i = 0; i <= data_len; i++) {
    if (i == data_len || data[i] == '\n') {
      if (i > 0 && data[i - 1] == '\r') {
        // CRLF
        if (line_start < data + i - 1) {
          auto fields = split_tab(line_start, data + i - 1);
          if (headers.empty()) {
            headers = std::move(fields);
          } else {
            rows.push_back(std::move(fields));
          }
        }
      } else {
        if (line_start < data + i) {
          auto fields = split_tab(line_start, data + i);
          if (headers.empty()) {
            headers = std::move(fields);
          } else {
            rows.push_back(std::move(fields));
          }
        }
      }
      line_start = data + i + 1;
    }
  }

  if (headers.empty()) {
    build_ok_packet(out, seq++);
    return seq;
  }

  uint64_t column_count = headers.size();

  // 1. Column count packet
  {
    std::string payload;
    write_lenenc_int(payload, column_count);
    char header[HEADER_SIZE];
    write_packet_header(header, (uint32_t)payload.size(), seq++);
    out.append(header, HEADER_SIZE);
    out.append(payload);
  }

  // 2. Column definition packets
  for (const auto& col_name : headers) {
    build_column_def_packet(out, seq++, col_name.data(), col_name.size());
  }

  // 3. EOF after column definitions
  build_eof_packet(out, seq++);

  // 4. Row packets
  for (const auto& row : rows) {
    build_row_packet(out, seq++, row);
  }

  // 5. Final EOF
  build_eof_packet(out, seq++);

  return seq;
}

void build_err_packet(std::string& out, uint8_t seq, uint16_t error_code,
                      const char* sql_state, const char* message) {
  size_t msg_len = strlen(message);
  // payload: 1 (marker) + 2 (error code) + 1 (#) + 5 (sql state) + msg
  uint32_t payload_len = 1 + 2 + 1 + 5 + (uint32_t)msg_len;

  // Header
  char header[HEADER_SIZE];
  header[0] = (char)(payload_len & 0xFF);
  header[1] = (char)((payload_len >> 8) & 0xFF);
  header[2] = (char)((payload_len >> 16) & 0xFF);
  header[3] = (char)seq;
  out.append(header, HEADER_SIZE);

  // ERR marker
  out.push_back((char)ERR_MARKER);

  // Error code (little-endian)
  out.push_back((char)(error_code & 0xFF));
  out.push_back((char)((error_code >> 8) & 0xFF));

  // SQL state marker + state
  out.push_back('#');
  out.append(sql_state, 5);

  // Message
  out.append(message, msg_len);
}

} // namespace mysql_protocol
