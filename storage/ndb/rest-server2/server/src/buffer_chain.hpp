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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_CHAIN_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_CHAIN_HPP_

#include "rdrs_dal.h"

#include <ndb_types.h>

#include <vector>

/*
 * RONDB-1144: the buffers one HTTP request takes from a pool, in the order
 * it took them, and where each operation's request or response starts in
 * them.
 *
 * Vocabulary: an owner is a whole pool buffer held by this chain. A view is
 * the region one operation writes: its start inside an owner and the room
 * left after it. Every address inside a response is relative to its view,
 * which is what lets many responses share an owner.
 *
 * Placement (rule 2 of the design): a new view starts in the current owner
 * only when at least start_reserve() bytes are free there, so the fixed
 * part of a response always fits in one buffer. A blob or text value that
 * outgrows its view continues into continuation owners taken with
 * take_continuation() (rule 3); a response that overflows anyway is
 * rewritten from the start of a fresh owner (rule 5). The destructor
 * returns every owner, so a controller never releases buffers by hand.
 *
 * Owner entries are never mutated by callers; views may be (the
 * controllers set each operation's view size to its encoded length), which
 * is why the two live in separate arrays.
 */
struct RS_BufferChain {
  enum Kind { REQ, RESP };

  RS_BufferChain(Kind kind, Uint32 num_ops);
  ~RS_BufferChain();
  RS_BufferChain(const RS_BufferChain &) = delete;
  RS_BufferChain &operator=(const RS_BufferChain &) = delete;

  /* Take the first owner; the view of operation 0 starts at its offset 0.
   * 503 "buffer pool exhausted" when the pool refuses. */
  RS_Status take_first();
  /* Place the view of operation index (> 0) at the current head, or at
   * offset 0 of a fresh owner when less than start_reserve() is free.
   * need is what the operation is already known to require (the request
   * encoder computes its upper bound before writing); when more than the
   * reserve, it is what has to be free instead. The response side cannot
   * know its size in advance and relies on the reserve plus rule 5. */
  RS_Status next_op_view(Uint32 index, Uint32 need = 0);
  /* Request side: the encoder wrote bytes at the view; move the head. */
  void advance(Uint32 bytes);
  /* Response side: append an owner for a spilled value and make it the
   * tail; on success *owner_index is its index. */
  RS_Status take_continuation(Uint32 *owner_index);
  /* Rule 5: re-place the view of operation index at offset 0 of a fresh
   * owner. The old bytes stay where they are and are simply unused. */
  RS_Status restart_op_in_fresh_buffer(Uint32 index);
  /* create_response calls this before writing, so a retried attempt
   * starts clean: return every owner but the first and start over from its
   * offset 0. */
  void reset_to_first_buffer();
  /* Response side: the writer closed the response of operation index.
   * home_len is what it wrote in its view; when it spilled, tail_off is the
   * write offset in the last continuation owner, which becomes the head. */
  void finish_op(Uint32 index, Uint32 home_len, bool spilled, Uint32 tail_off);

  const RS_Buffer &view(Uint32 index) const { return m_views[index]; }
  /* The array of views, for callers that index operations by position.
   * Stable: the array never grows after construction. */
  RS_Buffer *views() { return m_views.data(); }
  Uint32 num_ops() const { return m_num_ops; }

  const RS_Buffer &owner(Uint32 owner_index) const { return m_owners[owner_index]; }
  Uint32 owner_count() const { return static_cast<Uint32>(m_owners.size()); }
  Uint32 owner_of(Uint32 index) const { return m_view_owner[index]; }
  /* Owner index of the tail, where the head is. */
  Uint32 cur() const { return m_cur; }
  Uint32 head() const { return m_head; }
  Uint32 remaining() const;
  bool starts_at_offset_zero(Uint32 index) const;
  Uint32 start_reserve() const;

 private:
  RS_Status take_owner();

  Kind m_kind;
  Uint32 m_num_ops;
  Uint32 m_cur = 0;
  Uint32 m_head = 0;
  std::vector<RS_Buffer> m_owners;
  std::vector<RS_Buffer> m_views;
  std::vector<Uint32> m_view_owner;
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_CHAIN_HPP_
