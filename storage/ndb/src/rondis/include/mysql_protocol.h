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

#ifndef MYSQL_PROTOCOL_H
#define MYSQL_PROTOCOL_H

#include <cstdint>
#include <string>

namespace mysql_protocol {

// Packet header: 3-byte length + 1-byte sequence number
static constexpr int HEADER_SIZE = 4;
static constexpr int MAX_PACKET_SIZE = (1 << 24) - 1; // 16MB - 1

// Command bytes
static constexpr uint8_t COM_QUIT = 0x01;
static constexpr uint8_t COM_INIT_DB = 0x02;
static constexpr uint8_t COM_QUERY = 0x03;
static constexpr uint8_t COM_PING = 0x0E;

// Response markers
static constexpr uint8_t OK_MARKER = 0x00;
static constexpr uint8_t ERR_MARKER = 0xFF;
static constexpr uint8_t EOF_MARKER = 0xFE;
static constexpr uint8_t LOCAL_INFILE_MARKER = 0xFB;

// CLIENT_DEPRECATE_EOF capability flag (bit 24)
static constexpr uint32_t CLIENT_DEPRECATE_EOF = (1UL << 24);

// Read exactly `len` bytes from fd into buf. Returns false on error/EOF.
bool read_exact(int fd, char* buf, size_t len);

// Read one complete MySQL packet (header + payload) from fd.
// Appends to `out`. Returns false on error.
bool read_packet(int fd, std::string& out);

// Write data to fd. Returns false on error.
bool write_all(int fd, const char* data, size_t len);

// Read a complete MySQL response from fd (may be multi-packet for result sets).
// Appends all packets to `out`. Returns false on error.
bool read_response(int fd, std::string& out, uint32_t client_capabilities);

// Connect to backend mysqld. Returns fd or -1 on error.
int connect_to_backend(const char* host, uint16_t port);

// Parse packet length from 3-byte header (little-endian).
inline uint32_t packet_length(const char* header) {
  return (uint32_t)(uint8_t)header[0] |
         ((uint32_t)(uint8_t)header[1] << 8) |
         ((uint32_t)(uint8_t)header[2] << 16);
}

// Parse length-encoded integer at pos, advance pos. Returns value.
uint64_t read_lenenc_int(const char*& pos, const char* end);

// Build a MySQL ERR packet and append to out.
void build_err_packet(std::string& out, uint8_t seq, uint16_t error_code,
                      const char* sql_state, const char* message);

} // namespace mysql_protocol
#endif
