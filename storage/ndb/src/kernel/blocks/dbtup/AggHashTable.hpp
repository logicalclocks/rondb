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

/*
 * Chaining hash table for group-by lookup.  Templatized on BUCKET_COUNT
 * so AggInterpreter uses 256 buckets (~2KB) and JoinAggInterpreter uses
 * 1024 buckets (~8KB).
 *
 * Group data layout (GROUP_LINK_OVERHEAD = 24 bytes prepended):
 *   [chunk_next(8)] [hash_next(8)] [key_len(4)] [chunk_offset(4)]
 * Data pointer (from allocGroupData) points past this header.
 * hash_next links entries within the same bucket.
 */
template<Uint32 BUCKET_COUNT>
class GBHashTable {
 public:
  static const Uint32 HASH_NEXT_OFFSET = sizeof(char*);
  static const Uint32 KEY_LEN_OFFSET = 2 * sizeof(char*);
  static const Uint32 OVERHEAD = 24;

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
    : m_size(0), m_bucket_count(BUCKET_COUNT),
      m_bucket_mask(BUCKET_COUNT - 1),
      m_col_types(nullptr), m_n_gb_cols(0),
      m_xfrm_buf(nullptr), m_xfrm_buf_len(0) {
    memset(m_buckets, 0, sizeof(m_buckets));
  }

  void init(Uint32 bucket_count) {
    m_bucket_count = bucket_count;
    m_bucket_mask = bucket_count - 1;
    m_size = 0;
    memset(m_buckets, 0, bucket_count * sizeof(char*));
  }

  void clear() {
    memset(m_buckets, 0, m_bucket_count * sizeof(char*));
    m_size = 0;
  }

  char* find(const char* key, Uint32 key_len) const {
    Uint32 b = hashKey(key, key_len);
    return findInBucket(b, key, key_len);
  }

  void insert(char* data_ptr, Uint32 key_len) {
    char* raw = data_ptr - OVERHEAD;
    Uint32 b = hashKey(data_ptr, key_len);
    hashNext(raw) = m_buckets[b];
    m_buckets[b] = raw;
    m_size++;
  }

  void erase(char* data_ptr, Uint32 key_len) {
    char* raw = data_ptr - OVERHEAD;
    Uint32 b = hashKey(data_ptr, key_len);
    char** prev = &m_buckets[b];
    while (*prev != nullptr) {
      if (*prev == raw) {
        *prev = hashNext(raw);
        m_size--;
        return;
      }
      prev = &hashNext(*prev);
    }
  }

  Iterator begin() {
    for (Uint32 b = 0; b < m_bucket_count; b++) {
      if (m_buckets[b] != nullptr) {
        return Iterator(this, b, &m_buckets[b], m_buckets[b]);
      }
    }
    return Iterator(this, m_bucket_count, nullptr, nullptr);
  }

  Iterator begin() const {
    GBHashTable* self = const_cast<GBHashTable*>(this);
    for (Uint32 b = 0; b < m_bucket_count; b++) {
      if (m_buckets[b] != nullptr) {
        return Iterator(self, b,
                        const_cast<char**>(&m_buckets[b]), m_buckets[b]);
      }
    }
    return Iterator(self, m_bucket_count, nullptr, nullptr);
  }

  /**
   * Construct an iterator at a saved position (bucket + raw pointer).
   * Used for CTE scan resume — the hash table must be immutable between
   * the save and restore. Does not support eraseAndNext() since
   * m_prev_link is not reconstructed.
   */
  Iterator iteratorAt(Uint32 bucket, char* raw) {
    return Iterator(this, bucket, nullptr, raw);
  }

  void next(Iterator& it) const {
    char* nxt = hashNext(it.m_raw);
    if (nxt != nullptr) {
      it.m_prev_link = &hashNext(it.m_raw);
      it.m_raw = nxt;
      return;
    }
    for (Uint32 b = it.m_bucket + 1; b < m_bucket_count; b++) {
      if (m_buckets[b] != nullptr) {
        it.m_bucket = b;
        it.m_prev_link = const_cast<char**>(&m_buckets[b]);
        it.m_raw = m_buckets[b];
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
      if (m_buckets[b] != nullptr) {
        it.m_bucket = b;
        it.m_prev_link = &m_buckets[b];
        it.m_raw = m_buckets[b];
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

  char* popBucketHead(Uint32 b) {
    char* raw = m_buckets[b];
    if (raw == nullptr) return nullptr;
    m_buckets[b] = hashNext(raw);
    m_size--;
    return raw + OVERHEAD;
  }

  void insertRaw(char* data_ptr) {
    char* raw = data_ptr - OVERHEAD;
    Uint32 key_len = *reinterpret_cast<Uint32*>(raw + KEY_LEN_OFFSET);
    Uint32 b = hashKey(data_ptr, key_len);
    hashNext(raw) = m_buckets[b];
    m_buckets[b] = raw;
    m_size++;
  }

  bool bucketEmpty(Uint32 b) const { return m_buckets[b] == nullptr; }

  void setTypeMeta(const GBColTypeInfo *types, Uint32 nCols,
                   uchar *xfrm_buf, Uint32 xfrm_buf_len) {
    m_col_types = types;
    m_n_gb_cols = nCols;
    m_xfrm_buf = xfrm_buf;
    m_xfrm_buf_len = xfrm_buf_len;
  }

  Uint64 hashKeyFull(const char* key, Uint32 len) const;
  Uint32 hashKey(const char* key, Uint32 len) const;
  char* findInBucket(Uint32 b, const char* key, Uint32 key_len) const;

 private:
  char* m_buckets[BUCKET_COUNT];
  Uint32 m_size;
  Uint32 m_bucket_count;
  Uint32 m_bucket_mask;
  const GBColTypeInfo *m_col_types;
  Uint32 m_n_gb_cols;
  uchar *m_xfrm_buf;              // scratch buffer for strnxfrm_hash
  Uint32 m_xfrm_buf_len;          // size in bytes

  static char*& hashNext(char* raw) {
    return *reinterpret_cast<char**>(raw + HASH_NEXT_OFFSET);
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
                                              Uint32 len) const {
  if (m_col_types == nullptr) {
    return rondb_xxhash_std(key, len);
  }

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
                                         m_xfrm_buf, m_xfrm_buf_len,
                                         src + lb, srcLen, defLen);
      if (n > 0) {
        Uint64 colHash = rondb_xxhash_std(reinterpret_cast<const char*>(m_xfrm_buf),
                                          n);
        hash ^= colHash + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
      }
    } else {
      // Non-collation or NULL: hash raw [AH + data] bytes
      Uint32 colWords = 1 + dataSize;
      Uint64 colHash = rondb_xxhash_std(reinterpret_cast<const char*>(p),
                                        colWords * sizeof(Uint32));
      hash ^= colHash + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    }
    p += 1 + dataSize;
  }
  return hash;
}

template<Uint32 BUCKET_COUNT>
Uint32 GBHashTable<BUCKET_COUNT>::hashKey(const char* key, Uint32 len) const {
  return static_cast<Uint32>(hashKeyFull(key, len)) & m_bucket_mask;
}

template<Uint32 BUCKET_COUNT>
char* GBHashTable<BUCKET_COUNT>::findInBucket(Uint32 b, const char* key,
                                              Uint32 key_len) const {
  if (m_col_types == nullptr) {
    // Raw comparison path (no type metadata)
    for (char* raw = m_buckets[b]; raw != nullptr;
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
  for (char* raw = m_buckets[b]; raw != nullptr;
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
