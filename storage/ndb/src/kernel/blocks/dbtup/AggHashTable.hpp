/*
 * Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#ifndef AGGHASHTABLE_H_
#define AGGHASHTABLE_H_

#include <cstring>
#include "ndb_types.h"
#include "NdbAggregationCommon.hpp"
#include <NdbSqlUtil.hpp>
#include "util/rondb_hash.hpp"

#define AGG_EVICT_NEEDED 1
#define MEM_CHUNK_SIZE 32768

struct MemChunk {
  char* data;
  Uint32 capacity;
  Uint32 used;
  Uint32 live_groups;
  MemChunk* next;
  MemChunk* prev;
  char* group_list;         // singly-linked list of live groups in this chunk
};

/*
 * Per-column type info for type-aware GROUP BY hashing and comparison.
 * Populated once from table descriptors on first ProcessRec call.
 */
struct GBColTypeInfo {
  Uint32 typeId;
  const CHARSET_INFO *cs;          // nullptr for non-string types
  NdbSqlUtil::Cmp *cmpFn;         // from NdbSqlUtil::getType(typeId)
  Uint32 maxBytes;                 // AttributeDescriptor::getSizeInBytes
};

/* Query-memory allocation for GBHashTable growth (bucket segments and
 * their directory).  Defined in AggInterpreterBase.cpp on the kernel
 * pool (RG_QUERY_MEMORY); nullptr when the pool is exhausted. */
void* agg_gb_segment_alloc(size_t bytes, Uint32 thread_id);
void agg_gb_segment_free(void* ptr);

constexpr Uint32 agg_gb_log2u(Uint32 v) {
  return v <= 1 ? 0 : 1 + agg_gb_log2u(v >> 1);
}

/*
 * Chaining hash table for group-by lookup, grown by linear hashing.
 *
 * The table starts with BUCKET_COUNT buckets held inline (segment 0) —
 * a small aggregation costs no allocation.  Past a load factor of one
 * (more entries than buckets) every insert splits one bucket: bucket
 * `m_split` of the current round moves the entries whose next hash bit
 * is set to the new bucket `m_split + round size`.  When every bucket of
 * the round has split the table has doubled and a new round starts.
 * Each insert therefore does at most one bounded split (the entries of
 * one chain), never a whole-table rehash: no long pause on the LDM
 * thread, however many groups the table grows to.
 *
 * Buckets live in fixed segments of BUCKET_COUNT pointers (8 KB for the
 * join table) reached through a directory; segments never move, so a
 * bucket's address (and an Iterator's prev link) stays valid as the
 * table grows.  Segments and the directory come from query memory; when
 * an allocation fails, or MAX_SEGMENTS is reached, growth stops and the
 * table keeps working with longer chains.  release() returns them.
 *
 * Splits only move entries to HIGHER bucket indices, so a walk in
 * bucket order that restarts or resumes by bucket index never misses
 * an entry while the table grows (it may meet a moved entry twice).
 * Entries do not store their hash: a split recomputes the hash of the
 * chain it moves (callers pass the per-thread strnxfrm scratch).
 *
 * Buckets use the upper 32 bits of hashKeyFull: the lower bits pick the
 * owner node (hash % nodes) for charset keys, and taking bucket bits
 * from the same end would leave half of each owner's buckets empty.
 *
 * Group data layout (GROUP_LINK_OVERHEAD = 24 bytes prepended):
 *   [chunk_next(8)] [hash_next(8)] [key_len(4)] [chunk_offset(4)]
 * Data pointer (from allocGroupData) points past this header.
 * hash_next links entries within the same bucket.
 */
template<Uint32 BUCKET_COUNT>
class GBHashTable {
  static_assert(BUCKET_COUNT >= 2 &&
                    (BUCKET_COUNT & (BUCKET_COUNT - 1)) == 0,
                "BUCKET_COUNT must be a power of two");

 public:
  static const Uint32 HASH_NEXT_OFFSET = sizeof(char*);
  static const Uint32 KEY_LEN_OFFSET = 2 * sizeof(char*);
  static const Uint32 OVERHEAD = 24;
  /* Buckets per segment and the segment index shift. */
  static constexpr Uint32 SEG_SHIFT = agg_gb_log2u(BUCKET_COUNT);
  static constexpr Uint32 SEG_MASK = BUCKET_COUNT - 1;
  /* Growth cap: BUCKET_COUNT * MAX_SEGMENTS buckets (4 M for the join
   * table, 32 MB of bucket segments). */
  static constexpr Uint32 MAX_SEGMENTS = 4096;
  static constexpr Uint32 INITIAL_DIR_CAPACITY = 16;

  /* Key length of a group record given its DATA pointer (the pointer
   * past the link header, as stored in iterators and candidate lists).
   * Used by the ORDER BY/LIMIT finalize's comparator, which holds only
   * data pointers (cte_orderby_limit_plan.md). */
  static Uint32 dataKeyLen(const char* data_ptr) {
    return *reinterpret_cast<const Uint32*>(
        data_ptr - OVERHEAD + KEY_LEN_OFFSET);
  }

  class Iterator {
    friend class GBHashTable;
    GBHashTable* m_ht;
    Uint32 m_bucket;
    char** m_prev_link;
    char* m_raw;
   public:
    Iterator() : m_ht(nullptr), m_bucket(0), m_prev_link(nullptr),
                 m_raw(nullptr) {}
    Iterator(GBHashTable* ht, Uint32 bucket, char** prev_link, char* raw)
      : m_ht(ht), m_bucket(bucket), m_prev_link(prev_link), m_raw(raw) {}
    bool valid() const { return m_raw != nullptr; }
    char* data() const { return m_raw + OVERHEAD; }
    Uint32 keyLen() const {
      return *reinterpret_cast<Uint32*>(m_raw + KEY_LEN_OFFSET);
    }
    Uint32 bucket() const { return m_bucket; }
    char* raw() const { return m_raw; }
  };

  GBHashTable()
    : m_dir(nullptr), m_dir_capacity(0), m_nsegs(1), m_size(0),
      m_bucket_count(BUCKET_COUNT), m_low_mask(BUCKET_COUNT - 1),
      m_split(0), m_first_hint(0), m_thread_id(0), m_grow_stopped(false),
      m_col_types(nullptr), m_n_gb_cols(0) {
    memset(m_buckets, 0, sizeof(m_buckets));
  }

  /* Empty table with the initial geometry; `thread_id` is the
   * interpreter's allocation thread for growth segments. */
  void init(Uint32 thread_id) {
    release();
    m_thread_id = thread_id;
    memset(m_buckets, 0, sizeof(m_buckets));
  }

  /* Forget every entry (callers free the group data themselves) and
   * return to the initial geometry. */
  void clear() {
    release();
    memset(m_buckets, 0, sizeof(m_buckets));
  }

  /* Free the growth segments and the directory and return to the
   * initial geometry.  Entries are not touched: call it on an empty
   * table (teardown, or clear()).  Bounded: at most MAX_SEGMENTS frees. */
  void release() {
    if (m_dir != nullptr) {
      for (Uint32 s = 1; s < m_nsegs; s++) {
        agg_gb_segment_free(m_dir[s]);
      }
      agg_gb_segment_free(m_dir);
      m_dir = nullptr;
    }
    m_dir_capacity = 0;
    m_nsegs = 1;
    m_size = 0;
    m_bucket_count = BUCKET_COUNT;
    m_low_mask = BUCKET_COUNT - 1;
    m_split = 0;
    m_first_hint = 0;
    m_grow_stopped = false;
  }

  /* D26: find()/insert()/erase()/insertRaw()/hashKey()/hashKeyFull() all
   * REQUIRE a caller-supplied per-LDM-thread strnxfrm scratch buffer
   * (Dbtup::getAggXfrmBuf).  This hash table is shared across LDM threads, so
   * the buffer must never be a member of it — making the param mandatory (no
   * default) means the compiler rejects any call that would compute a group-key
   * hash in a buffer another thread could clobber. */
  char* find(const char* key, Uint32 key_len,
             uchar* xfrm_buf, Uint32 xfrm_buf_len) const {
    Uint32 b = hashKey(key, key_len, xfrm_buf, xfrm_buf_len);
    return findInBucket(b, key, key_len);
  }

  void insert(char* data_ptr, Uint32 key_len,
              uchar* xfrm_buf, Uint32 xfrm_buf_len) {
    Uint32 b = hashKey(data_ptr, key_len, xfrm_buf, xfrm_buf_len);
    linkInBucket(b, data_ptr - OVERHEAD);
    maybeGrow(xfrm_buf, xfrm_buf_len);
  }

  void erase(char* data_ptr, Uint32 key_len,
             uchar* xfrm_buf, Uint32 xfrm_buf_len) {
    char* raw = data_ptr - OVERHEAD;
    Uint32 b = hashKey(data_ptr, key_len, xfrm_buf, xfrm_buf_len);
    char** prev = &bucketRef(b);
    while (*prev != nullptr) {
      if (*prev == raw) {
        *prev = hashNext(raw);
        m_size--;
        return;
      }
      prev = &hashNext(*prev);
    }
  }

  /* The walk starts at m_first_hint, a lower bound on the first
   * non-empty bucket, so loops that restart from begin() after erasing
   * a slice of groups (result send, teardown) do not rescan the emptied
   * front of a grown table. */
  Iterator begin() {
    for (Uint32 b = m_first_hint; b < m_bucket_count; b++) {
      char*& head = bucketRef(b);
      if (head != nullptr) {
        m_first_hint = b;
        return Iterator(this, b, &head, head);
      }
    }
    m_first_hint = m_bucket_count;
    return Iterator(this, m_bucket_count, nullptr, nullptr);
  }

  Iterator begin() const {
    return const_cast<GBHashTable*>(this)->begin();
  }

  /* The first entry in bucket order at or after `bucket` — resumes a
   * sliced walk that saved its bucket index.  Entries only move to
   * higher buckets as the table grows, so every entry that was at or
   * after `bucket` when the walk paused is still there. */
  Iterator beginAt(Uint32 bucket) {
    if (bucket < m_first_hint) bucket = m_first_hint;
    for (Uint32 b = bucket; b < m_bucket_count; b++) {
      char*& head = bucketRef(b);
      if (head != nullptr) {
        return Iterator(this, b, &head, head);
      }
    }
    return Iterator(this, m_bucket_count, nullptr, nullptr);
  }

  /**
   * Construct an iterator at a saved position (bucket + raw pointer).
   * Used for CTE scan / AVG-finalize / LIMIT-finalize resume — the
   * entry at `raw` must still be live in `bucket`'s chain (nothing may
   * erase IT between save and restore; erasing OTHER entries is fine).
   * m_prev_link is reconstructed by walking the bucket chain, so the
   * resumed iterator supports eraseAndNext() — the CTE LIMIT
   * truncation resumes mid-bucket and erases (a nullptr prev_link here
   * segfaulted on the first resumed erase slice; found by
   * ronsql_large_cte Q6 at 100k groups).  If `raw` is not found in the
   * chain the iterator falls back to a read-only one (prev_link
   * nullptr), preserving the old behavior.  The table must not grow
   * between save and restore (a split can move `raw` to a higher
   * bucket); every saved-position walk runs in a window where the
   * table is immutable.
   */
  Iterator iteratorAt(Uint32 bucket, char* raw) {
    char** prev = &bucketRef(bucket);
    while (*prev != nullptr) {
      if (*prev == raw) {
        return Iterator(this, bucket, prev, raw);
      }
      prev = &hashNext(*prev);
    }
    return Iterator(this, bucket, nullptr, raw);
  }

  void next(Iterator& it) const {
    char* nxt = hashNext(it.m_raw);
    if (nxt != nullptr) {
      it.m_prev_link = &hashNext(it.m_raw);
      it.m_raw = nxt;
      return;
    }
    GBHashTable* self = const_cast<GBHashTable*>(this);
    for (Uint32 b = it.m_bucket + 1; b < m_bucket_count; b++) {
      char*& head = self->bucketRef(b);
      if (head != nullptr) {
        it.m_bucket = b;
        it.m_prev_link = &head;
        it.m_raw = head;
        return;
      }
    }
    it.m_bucket = m_bucket_count;
    it.m_prev_link = nullptr;
    it.m_raw = nullptr;
  }

  void eraseAndNext(Iterator& it) {
    char* nxt = hashNext(it.m_raw);
    *it.m_prev_link = nxt;
    m_size--;
    if (nxt != nullptr) {
      it.m_raw = nxt;
      return;
    }
    for (Uint32 b = it.m_bucket + 1; b < m_bucket_count; b++) {
      char*& head = bucketRef(b);
      if (head != nullptr) {
        it.m_bucket = b;
        it.m_prev_link = &head;
        it.m_raw = head;
        return;
      }
    }
    it.m_bucket = m_bucket_count;
    it.m_prev_link = nullptr;
    it.m_raw = nullptr;
  }

  Uint32 size() const { return m_size; }
  bool empty() const { return m_size == 0; }
  Uint32 bucketCount() const { return m_bucket_count; }

  /* True when both tables map every hash to the same bucket. */
  bool sameGeometry(const GBHashTable& other) const {
    return m_low_mask == other.m_low_mask && m_split == other.m_split;
  }

  /* Unlink and return the first entry in bucket order (its data
   * pointer, and its bucket in *bucket_out), or nullptr when empty.
   * Drains in slices without rescanning emptied buckets; the table
   * must not grow while it is being drained. */
  char* popNext(Uint32* bucket_out) {
    for (Uint32 b = m_first_hint; b < m_bucket_count; b++) {
      char*& head = bucketRef(b);
      if (head != nullptr) {
        char* raw = head;
        head = hashNext(raw);
        m_size--;
        m_first_hint = b;
        if (bucket_out != nullptr) *bucket_out = b;
        return raw + OVERHEAD;
      }
    }
    m_first_hint = m_bucket_count;
    return nullptr;
  }

  char* popBucketHead(Uint32 b) {
    char*& head = bucketRef(b);
    char* raw = head;
    if (raw == nullptr) return nullptr;
    head = hashNext(raw);
    m_size--;
    return raw + OVERHEAD;
  }

  void insertRaw(char* data_ptr,
                 uchar* xfrm_buf, Uint32 xfrm_buf_len) {
    char* raw = data_ptr - OVERHEAD;
    Uint32 key_len = *reinterpret_cast<Uint32*>(raw + KEY_LEN_OFFSET);
    Uint32 b = hashKey(data_ptr, key_len, xfrm_buf, xfrm_buf_len);
    linkInBucket(b, raw);
    maybeGrow(xfrm_buf, xfrm_buf_len);
  }

  /* insertRaw into bucket `b`, which the caller computed with hashKey()
   * on this table's current geometry (no insert in between). */
  void insertRawInBucket(Uint32 b, char* data_ptr,
                         uchar* xfrm_buf, Uint32 xfrm_buf_len) {
    linkInBucket(b, data_ptr - OVERHEAD);
    maybeGrow(xfrm_buf, xfrm_buf_len);
  }

  bool bucketEmpty(Uint32 b) const { return bucketHead(b) == nullptr; }

  void setTypeMeta(const GBColTypeInfo *types, Uint32 nCols) {
    m_col_types = types;
    m_n_gb_cols = nCols;
  }

  /* D26: the strnxfrm scratch buffer is NOT a member of this (thread-shared)
   * hash table — every hash-computing method REQUIRES the caller to pass a
   * per-LDM-thread buffer (Dbtup::getAggXfrmBuf).  No default: the compiler
   * forbids any call that would otherwise fall back to a shared buffer and
   * race a concurrent thread's hash computation. */
  Uint64 hashKeyFull(const char* key, Uint32 len,
                     uchar* xfrm_buf, Uint32 xfrm_buf_len) const;
  /* The bucket of a key on the current geometry. */
  Uint32 hashKey(const char* key, Uint32 len,
                 uchar* xfrm_buf, Uint32 xfrm_buf_len) const;
  char* findInBucket(Uint32 b, const char* key, Uint32 key_len) const;

 private:
  char* m_buckets[BUCKET_COUNT];   // segment 0
  char*** m_dir;                   // segments; nullptr until the first growth
  Uint32 m_dir_capacity;
  Uint32 m_nsegs;                  // segments in use, segment 0 included
  Uint32 m_size;
  Uint32 m_bucket_count;           // (m_low_mask + 1) + m_split
  Uint32 m_low_mask;               // bucket mask of the current round
  Uint32 m_split;                  // next bucket of the round to split
  mutable Uint32 m_first_hint;     // lower bound on the first non-empty bucket
  Uint32 m_thread_id;              // allocation thread for segments
  bool m_grow_stopped;             // allocation failed or MAX_SEGMENTS reached
  const GBColTypeInfo *m_col_types;
  Uint32 m_n_gb_cols;

  static char*& hashNext(char* raw) {
    return *reinterpret_cast<char**>(raw + HASH_NEXT_OFFSET);
  }

  char*& bucketRef(Uint32 b) {
    if (b < BUCKET_COUNT) return m_buckets[b];
    return m_dir[b >> SEG_SHIFT][b & SEG_MASK];
  }

  char* bucketHead(Uint32 b) const {
    if (b < BUCKET_COUNT) return m_buckets[b];
    return m_dir[b >> SEG_SHIFT][b & SEG_MASK];
  }

  /* Linear hashing address: the low bits of the round, one more bit for
   * the buckets of the round that have already split. */
  Uint32 bucketOf(Uint32 h) const {
    Uint32 b = h & m_low_mask;
    if (b < m_split) {
      b = h & ((m_low_mask << 1) | 1);
    }
    return b;
  }

  static Uint32 bucketHash(Uint64 full) {
    return static_cast<Uint32>(full >> 32);
  }

  void linkInBucket(Uint32 b, char* raw) {
    char*& head = bucketRef(b);
    hashNext(raw) = head;
    head = raw;
    m_size++;
    if (b < m_first_hint) m_first_hint = b;
  }

  void maybeGrow(uchar* xfrm_buf, Uint32 xfrm_buf_len) {
    if (m_size <= m_bucket_count || m_grow_stopped) {
      return;
    }
    splitOne(xfrm_buf, xfrm_buf_len);
  }

  /* One more segment (and a larger directory when full).  False when
   * query memory is exhausted or the cap is reached. */
  bool addSegment() {
    if (m_nsegs >= MAX_SEGMENTS) {
      return false;
    }
    if (m_nsegs >= m_dir_capacity) {
      Uint32 cap = (m_dir_capacity == 0) ? INITIAL_DIR_CAPACITY
                                         : 2 * m_dir_capacity;
      if (cap > MAX_SEGMENTS) cap = MAX_SEGMENTS;
      char*** dir = static_cast<char***>(
          agg_gb_segment_alloc(cap * sizeof(char**), m_thread_id));
      if (dir == nullptr) {
        return false;
      }
      if (m_dir != nullptr) {
        memcpy(dir, m_dir, m_nsegs * sizeof(char**));
        agg_gb_segment_free(m_dir);
      } else {
        dir[0] = m_buckets;
      }
      m_dir = dir;
      m_dir_capacity = cap;
    }
    /* Not cleared: each bucket is written by the split that creates it,
     * and buckets at or past m_bucket_count are never read. */
    char** seg = static_cast<char**>(
        agg_gb_segment_alloc(BUCKET_COUNT * sizeof(char*), m_thread_id));
    if (seg == nullptr) {
      return false;
    }
    m_dir[m_nsegs++] = seg;
    return true;
  }

  /* Split bucket m_split into itself and m_split + round size (the next
   * bucket index, m_bucket_count): entries whose hash has the round's
   * high bit set move to the new bucket, the others stay. */
  void splitOne(uchar* xfrm_buf, Uint32 xfrm_buf_len) {
    const Uint32 new_b = m_bucket_count;
    if ((new_b & SEG_MASK) == 0 && !addSegment()) {
      m_grow_stopped = true;
      return;
    }
    const Uint32 high_bit = m_low_mask + 1;
    char*& old_head = bucketRef(m_split);
    char* chain = old_head;
    char* keep = nullptr;
    char* move = nullptr;
    while (chain != nullptr) {
      char* nxt = hashNext(chain);
      const Uint32 key_len =
          *reinterpret_cast<Uint32*>(chain + KEY_LEN_OFFSET);
      const Uint32 h = bucketHash(
          hashKeyFull(chain + OVERHEAD, key_len, xfrm_buf, xfrm_buf_len));
      if ((h & high_bit) != 0) {
        hashNext(chain) = move;
        move = chain;
      } else {
        hashNext(chain) = keep;
        keep = chain;
      }
      chain = nxt;
    }
    old_head = keep;
    bucketRef(new_b) = move;
    m_bucket_count++;
    if (++m_split == high_bit) {
      /* Every bucket of the round split: the table has doubled. */
      m_split = 0;
      m_low_mask = (m_low_mask << 1) | 1;
    }
  }
};

// Type aliases for the two use cases
#define AGG_HASH_BUCKET_COUNT 256
#define JOIN_AGG_HASH_BUCKET_COUNT 1024

using AggGBHashTable = GBHashTable<AGG_HASH_BUCKET_COUNT>;
using JoinGBHashTable = GBHashTable<JOIN_AGG_HASH_BUCKET_COUNT>;

/*
 * Template definitions for hashKey and findInBucket.
 * Must be in the header since the class is a template.
 */
/**
 * hashKeyFull — compute the full Uint64 hash of a GROUP BY key.
 *
 * For keys with complex character sets, normalizes each column via
 * strnxfrm_hash before hashing. For binary/simple types, hashes raw bytes.
 * This full hash is used for distribution (node selection, receiver routing)
 * where the bucket mask must not be applied.
 */
template<Uint32 BUCKET_COUNT>
Uint64 GBHashTable<BUCKET_COUNT>::hashKeyFull(const char* key,
                                              Uint32 len,
                                              uchar* xfrm_buf,
                                              Uint32 xfrm_buf_len) const {
  if (m_col_types == nullptr) {
    return rondb_xxhash_std(key, len);
  }

  /* D26: hash strictly into the caller-supplied per-LDM-thread scratch
   * (xfrm_buf).  strnxfrm_hash is deterministic, so the hash value is identical
   * regardless of which (sufficiently large) buffer is used — stored and
   * looked-up keys still land in the same bucket, but no buffer is shared
   * across threads. */
  // Type-aware path: hash column-by-column
  Uint64 hash = 0;
  const Uint32* p = reinterpret_cast<const Uint32*>(key);
  const Uint32* end = reinterpret_cast<const Uint32*>(key + len);
  for (Uint32 i = 0; i < m_n_gb_cols && p < end; i++) {
    AttributeHeader ah(*p);
    Uint32 dataSize = ah.getDataSize();
    if (m_col_types[i].cs != nullptr && dataSize > 0) {
      // Collation-aware: normalize via strnxfrm_hash, then hash
      const uchar* src = reinterpret_cast<const uchar*>(p + 1);
      Uint32 byteSize = ah.getByteSize();
      Uint32 lb, srcLen;
      NdbSqlUtil::get_var_length(m_col_types[i].typeId, src, byteSize,
                                 lb, srcLen);
      Uint32 maxBytes = m_col_types[i].maxBytes;
      Uint32 defLen = maxBytes - lb;
      int n = NdbSqlUtil::strnxfrm_hash(m_col_types[i].cs,
                                         m_col_types[i].typeId,
                                         xfrm_buf, xfrm_buf_len,
                                         src + lb, srcLen, defLen);
      if (n > 0) {
        Uint64 colHash = rondb_xxhash_std(
            reinterpret_cast<const char*>(xfrm_buf), n);
        hash ^= colHash + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
      }
    } else {
      // Non-collation or NULL: hash the data bytes only (skip AH).
      // findInBucket also compares only p+1 (the value portion), so
      // the attrId in the AH must not contribute to the hash — otherwise
      // a lookup key produced against a different table (e.g. scanCte
      // feeding a pushdown-joined lookupCte via a virtual table) would
      // hash to a different bucket than the stored key even though the
      // value bytes are identical.
      if (dataSize > 0) {
        Uint32 byteSize = ah.getByteSize();
        Uint64 colHash = rondb_xxhash_std(
            reinterpret_cast<const char*>(p + 1), byteSize);
        hash ^= colHash + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
      } else {
        // NULL column: fold a fixed sentinel to keep NULLs distinguishable.
        Uint64 colHash = 0xDEADBEEFDEADBEEFULL;
        hash ^= colHash + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
      }
    }
    p += 1 + dataSize;
  }
  return hash;
}

template<Uint32 BUCKET_COUNT>
Uint32 GBHashTable<BUCKET_COUNT>::hashKey(const char* key, Uint32 len,
                                          uchar* xfrm_buf,
                                          Uint32 xfrm_buf_len) const {
  return bucketOf(bucketHash(hashKeyFull(key, len, xfrm_buf, xfrm_buf_len)));
}

template<Uint32 BUCKET_COUNT>
char* GBHashTable<BUCKET_COUNT>::findInBucket(Uint32 b, const char* key,
                                              Uint32 key_len) const {
  if (m_col_types == nullptr) {
    // Raw comparison path (no type metadata)
    for (char* raw = bucketHead(b); raw != nullptr;
         raw = hashNext(raw)) {
      char* d = raw + OVERHEAD;
      Uint32 kl = *reinterpret_cast<Uint32*>(raw + KEY_LEN_OFFSET);
      if (kl == key_len && memcmp(d, key, key_len) == 0) {
        return d;
      }
    }
    return nullptr;
  }

  // Type-aware comparison path: compare column-by-column using cmpFn
  for (char* raw = bucketHead(b); raw != nullptr;
       raw = hashNext(raw)) {
    char* d = raw + OVERHEAD;
    Uint32 kl = *reinterpret_cast<Uint32*>(raw + KEY_LEN_OFFSET);
    if (kl != key_len) continue;

    const Uint32* p1 = reinterpret_cast<const Uint32*>(key);
    const Uint32* p2 = reinterpret_cast<const Uint32*>(d);
    const Uint32* p1_end = reinterpret_cast<const Uint32*>(key + key_len);
    bool match = true;
    for (Uint32 i = 0; i < m_n_gb_cols && p1 < p1_end; i++) {
      AttributeHeader ah1(*p1);
      AttributeHeader ah2(*p2);
      Uint32 ds1 = ah1.getDataSize();
      Uint32 ds2 = ah2.getDataSize();
      if (ds1 == 0 && ds2 == 0) {
        // Both NULL — considered equal
        p1 += 1;
        p2 += 1;
        continue;
      }
      if (ds1 != ds2) {
        match = false;
        break;
      }
      if (m_col_types[i].cs != nullptr) {
        int cmp = (*m_col_types[i].cmpFn)(
            m_col_types[i].cs,
            p1 + 1, ah1.getByteSize(),
            p2 + 1, ah2.getByteSize());
        if (cmp != 0) {
          match = false;
          break;
        }
      } else {
        // Raw comparison for non-string types
        if (memcmp(p1 + 1, p2 + 1, ds1 * sizeof(Uint32)) != 0) {
          match = false;
          break;
        }
      }
      p1 += 1 + ds1;
      p2 += 1 + ds2;
    }
    if (match) return d;
  }
  return nullptr;
}

#endif  // AGGHASHTABLE_H_
