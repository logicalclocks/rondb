/*
 * Copyright (C) 2026 Hopsworks AB
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

#include "buffer_chain.hpp"

#include <algorithm>

#include "buffer_manager.hpp"
#include "buffer_sizes.hpp"
#include "config_structs.hpp"
#include "status.hpp"

#include <util/require.h>

namespace {

constexpr Uint32 kNoOwner = 0xFFFFFFFFU;

Uint32 align4(Uint32 bytes) { return (bytes + 3U) & ~3U; }

RS_Status pool_exhausted() {
  return __RS_ERROR(static_cast<HTTP_CODE>(503), -1, -1, -1, -1,
                    "buffer pool exhausted", __LINE__, __MYFILENAME__);
}

}  // namespace

RS_Status RS_BufferChain::check_pool_can_hold(Uint64 bytes) {
  const Uint64 pool_bytes =
    static_cast<Uint64>(rsBufferArrayManager.max_buffers()) *
    rsBufferArrayManager.resp_buffer_bytes();
  if (bytes > pool_bytes) {
    return pool_exhausted();
  }
  return RS_OK;
}

RS_BufferChain::RS_BufferChain(Kind kind, Uint32 num_ops)
    : m_kind(kind), m_num_ops(num_ops) {
  m_views.assign(num_ops, RS_Buffer{nullptr, 0});
  m_view_owner.assign(num_ops, kNoOwner);
}

RS_BufferChain::~RS_BufferChain() {
  for (auto &buffer : m_owners) {
    if (m_kind == REQ) {
      rsBufferArrayManager.return_req_buffer(buffer);
    } else {
      rsBufferArrayManager.return_resp_buffer(buffer);
    }
  }
}

Uint32 RS_BufferChain::start_reserve() const {
  const Uint32 override_value = globalConfigs.internal.startReserveOverride;
  if (override_value != 0) {
    return override_value;
  }
  return m_kind == REQ ? REQ_START_RESERVE : RESP_START_RESERVE;
}

Uint32 RS_BufferChain::remaining() const {
  if (m_owners.empty()) {
    return 0;
  }
  return m_owners[m_cur].size - m_head;
}

bool RS_BufferChain::starts_at_offset_zero(Uint32 index) const {
  const Uint32 owner_index = m_view_owner[index];
  return owner_index != kNoOwner &&
         m_views[index].buffer == m_owners[owner_index].buffer;
}

RS_Status RS_BufferChain::take_owner() {
  RS_Buffer buffer = m_kind == REQ ? rsBufferArrayManager.get_req_buffer()
                                   : rsBufferArrayManager.get_resp_buffer();
  if (buffer.buffer == nullptr) {
    return pool_exhausted();
  }
  m_owners.push_back(buffer);
  m_cur = static_cast<Uint32>(m_owners.size() - 1);
  m_head = 0;
  return RS_OK;
}

RS_Status RS_BufferChain::take_first() {
  require(m_owners.empty() && m_num_ops > 0);
  RS_Status status = take_owner();
  if (status.http_code != SUCCESS) {
    return status;
  }
  m_views[0] = RS_Buffer{m_owners[0].buffer, m_owners[0].size};
  m_view_owner[0] = 0;
  return RS_OK;
}

RS_Status RS_BufferChain::next_op_view(Uint32 index, Uint32 need) {
  require(index > 0 && index < m_num_ops && !m_owners.empty());
  if (remaining() < std::max(start_reserve(), need)) {
    RS_Status status = take_owner();
    if (status.http_code != SUCCESS) {
      return status;
    }
  }
  const RS_Buffer &owner = m_owners[m_cur];
  m_views[index] = RS_Buffer{owner.buffer + m_head, owner.size - m_head};
  m_view_owner[index] = m_cur;
  return RS_OK;
}

void RS_BufferChain::advance(Uint32 bytes) {
  require(!m_owners.empty());
  m_head = align4(m_head + bytes);
  require(m_head <= m_owners[m_cur].size);
}

RS_Status RS_BufferChain::take_continuation(Uint32 *owner_index) {
  RS_Status status = take_owner();
  if (status.http_code != SUCCESS) {
    return status;
  }
  *owner_index = m_cur;
  return RS_OK;
}

RS_Status RS_BufferChain::restart_op_in_fresh_buffer(Uint32 index) {
  require(index < m_num_ops);
  RS_Status status = take_owner();
  if (status.http_code != SUCCESS) {
    return status;
  }
  const RS_Buffer &owner = m_owners[m_cur];
  m_views[index] = RS_Buffer{owner.buffer, owner.size};
  m_view_owner[index] = m_cur;
  return RS_OK;
}

void RS_BufferChain::reset_to_first_buffer() {
  if (m_owners.empty()) {
    return;
  }
  for (size_t i = 1; i < m_owners.size(); i++) {
    if (m_kind == REQ) {
      rsBufferArrayManager.return_req_buffer(m_owners[i]);
    } else {
      rsBufferArrayManager.return_resp_buffer(m_owners[i]);
    }
  }
  m_owners.resize(1);
  m_cur = 0;
  m_head = 0;
  for (Uint32 i = 0; i < m_num_ops; i++) {
    m_views[i] = RS_Buffer{nullptr, 0};
    m_view_owner[i] = kNoOwner;
  }
  m_views[0] = RS_Buffer{m_owners[0].buffer, m_owners[0].size};
  m_view_owner[0] = 0;
}

void RS_BufferChain::finish_op(Uint32 index, Uint32 home_len, bool spilled,
                               Uint32 tail_off) {
  require(index < m_num_ops && m_view_owner[index] != kNoOwner);
  if (spilled) {
    /* The tail is the last continuation owner; the next view starts after
     * the spilled bytes in it. */
    m_head = align4(tail_off);
  } else {
    const Uint32 owner_index = m_view_owner[index];
    const Uint32 view_start = static_cast<Uint32>(
        m_views[index].buffer - m_owners[owner_index].buffer);
    m_cur = owner_index;
    m_head = align4(view_start + home_len);
  }
  require(m_head <= m_owners[m_cur].size);
}
