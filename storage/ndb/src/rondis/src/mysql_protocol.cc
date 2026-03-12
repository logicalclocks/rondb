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
#include <sys/socket.h>
#include <unistd.h>

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
      if (errno == EINTR) continue;
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
      if (errno == EINTR) continue;
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

  if (first_payload_len == 0) {
    return true;
  }

  // Check what type of response this is
  uint8_t first_byte = (uint8_t)first_pkt[HEADER_SIZE];

  // OK packet
  if (first_byte == OK_MARKER) {
    return true;
  }

  // ERR packet
  if (first_byte == ERR_MARKER) {
    return true;
  }

  // EOF packet (standalone)
  if (first_byte == EOF_MARKER && first_payload_len <= 5) {
    return true;
  }

  // LOCAL INFILE request - not supported in proxy, just pass through
  if (first_byte == LOCAL_INFILE_MARKER) {
    return true;
  }

  // Result set: first packet is column_count (lenenc int)
  const char* pos = first_pkt + HEADER_SIZE;
  const char* end = first_pkt + HEADER_SIZE + first_payload_len;
  uint64_t column_count = read_lenenc_int(pos, end);
  (void)column_count;

  bool deprecate_eof = (client_capabilities & CLIENT_DEPRECATE_EOF) != 0;

  // Read column definition packets
  // Each column definition is one packet, terminated by EOF (or no EOF if
  // CLIENT_DEPRECATE_EOF)
  if (!deprecate_eof) {
    // Read column definitions until EOF
    while (true) {
      size_t pkt_start = out.size();
      if (!read_packet(fd, out)) {
        return false;
      }
      uint32_t plen = packet_length(out.data() + pkt_start);
      if (is_eof_packet(out.data() + pkt_start, plen)) {
        break;
      }
      if (is_err_packet(out.data() + pkt_start, plen)) {
        return true;
      }
    }
  } else {
    // With CLIENT_DEPRECATE_EOF, read exactly column_count definition packets
    for (uint64_t i = 0; i < column_count; i++) {
      if (!read_packet(fd, out)) {
        return false;
      }
    }
  }

  // Read row packets until EOF/OK/ERR
  while (true) {
    size_t pkt_start = out.size();
    if (!read_packet(fd, out)) {
      return false;
    }
    uint32_t plen = packet_length(out.data() + pkt_start);

    if (is_err_packet(out.data() + pkt_start, plen)) {
      return true;
    }

    if (!deprecate_eof) {
      if (is_eof_packet(out.data() + pkt_start, plen)) {
        return true;
      }
    } else {
      // With CLIENT_DEPRECATE_EOF, rows end with an OK packet (0x00 or 0xFE)
      if (is_ok_packet(out.data() + pkt_start, plen)) {
        return true;
      }
      if (is_eof_packet(out.data() + pkt_start, plen)) {
        return true;
      }
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
